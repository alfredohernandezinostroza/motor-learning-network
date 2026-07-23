"""Topic <-> citation-community composition & connectivity analysis.

For each semantic *topic* (the BERTopic ``topic`` node attribute produced by the
embedding pipeline) this measures how the topic's papers distribute over the
citation-graph *communities* (the ``cpm_communities_at_res={RESOLUTION}`` Leiden
attribute, the same one ``detect_missing_links.py`` uses), and how well those
papers cite each other.

The headline output is the **citation-gap recommender**: pairs of communities
that share a topic but barely cite each other -- "these two work on the same
subject yet don't cite one another". Where ``detect_missing_links.py`` finds
candidate gaps at the *paper-pair* level, this module finds them at the
*community-pair* level and adds the structural context (how concentrated the
topic is, how internally connected, how much its communities integrate).

Ported from the companion Mariana-Embedding-Space-Analysis project
(``scripts/topic_community_analysis.py``) into this repo's Hamilton-DAG idiom,
reusing igraph for graph I/O and the repo's CPM-community attribute convention
instead of the original's hand-rolled GraphML parser and website artefacts.

Metrics per topic
-----------------
Composition (over the topic's papers):
  n_papers, n_communities, n_communities_major (>= MAJOR_SHARE of the topic),
  dominant_community + dominant_share, community_entropy (normalized Shannon,
  0 = one community, 1 = evenly spread), effective_n_communities (exp(H)),
  community_breakdown.
Connectivity (citation edges as undirected, restricted to the topic):
  internal_edges, boundary_edges, internal_edge_ratio, mean_internal_degree,
  lcc_fraction (largest connected component of the induced subgraph).
Community integration -- do the topic's communities actually cite each other?
  cross_community_edges, cross_community_edge_ratio, community_integration
  (that ratio normalized by the Gini-Simpson chance baseline; None for a
  single-community topic).
Citation-gap recommender:
  disconnected_pairs / n_disconnected_pairs -- community pairs flagged when both
  are substantial and the modularity-null expected citations are non-trivial yet
  observed ~ 0.

Outputs (data/graph_level_data/topic_community/):
  topic_community_metrics.json        keyed by topic id
  topic_community_metrics.csv         flat per-topic table
  topic_disconnected_communities.csv  one row per flagged community pair
"""

import sys
import json
import math
import logging
from pathlib import Path
from typing import Final, Optional
from collections import Counter, defaultdict

import numpy as np
import pandas as pd
import igraph as ig

from hamilton.function_modifiers import dataloader, datasaver, unpack_fields
from hamilton.io import utils
from hamilton_sdk import adapters
from hamilton import driver
import hamilton.log_setup

from motor_learning_network.constants import (
    GRAPH_LEVEL_DATA_PATH,
    FIGURES_PATH,
    DEFAULT_UI_PROJECT_ID,
    DEFAULT_UI_USERNAME,
    TEAM_NAME,
)

###################
##   Constants   ##
###################
CURRENT_FILE_NAME = Path(__file__).stem
hamilton.log_setup.setup_logging(logging.INFO)
logger = logging.getLogger(__name__)

EXECUTE = True

# Which CPM resolution's communities define a "citation community". Kept aligned
# with detect_missing_links.py so the two modules describe the same partition.
RESOLUTION: Final[float] = 0.005
COMMUNITY_ATTR: Final[str] = f"cpm_communities_at_res={RESOLUTION}"
TOPIC_ATTR: Final[str] = "topic"
OUTLIER_TOPIC: Final[int] = -1        # BERTopic's unassigned/outlier label

# A community counts as "major" within a topic if it holds at least this share
# of the topic's papers.
MAJOR_SHARE: Final[float] = 0.10
# Communities kept in the per-topic breakdown.
BREAKDOWN_TOP_N: Final[int] = 6

# ── "Citation gap" recommender thresholds ────────────────────────────────────
# A community pair within a topic is flagged when each side has >= PAIR_MIN_PAPERS
# papers in the topic, the modularity-null expected citations between them are
# >= PAIR_MIN_EXPECTED, and observed <= PAIR_MAX_RATIO * expected.
PAIR_MIN_PAPERS: Final[int] = 15
PAIR_MIN_EXPECTED: Final[float] = 3.0
PAIR_MAX_RATIO: Final[float] = 0.20
DISCONNECTED_TOP_N: Final[int] = 5

OUTPUT_DIR: Final[Path] = GRAPH_LEVEL_DATA_PATH / "topic_community"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


#####################
##  Aux Functions  ##
#####################
def _int_attr(graph: ig.Graph, attr: str) -> np.ndarray:
    """Read a vertex attribute stored as float/str and return an int array."""
    return np.array([int(float(v)) for v in graph.vs[attr]])


class UnionFind:
    """Minimal union-find for largest-connected-component (no networkx dep)."""

    def __init__(self):
        self.parent: dict[int, int] = {}

    def find(self, x: int) -> int:
        p = self.parent.setdefault(x, x)
        while p != x:
            self.parent[x] = self.parent[p]  # path halving
            x = self.parent[x]
            p = self.parent[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb


def _normalized_entropy(counts: list[int]) -> tuple[float, float]:
    """Return (normalized Shannon entropy in [0, 1], effective number = exp(H))."""
    total = sum(counts)
    if total <= 0 or len(counts) <= 1:
        return 0.0, 1.0 if counts else 0.0
    h = 0.0
    for c in counts:
        if c > 0:
            p = c / total
            h -= p * math.log(p)
    effective = math.exp(h)
    norm = h / math.log(len(counts))  # log(k) is max entropy for k buckets
    return norm, effective


def _find_disconnected_pairs(counts, deg, pair_edges, e_int, names) -> list[dict]:
    """Community pairs in a topic that barely cite each other.

    For each pair of communities (each with >= PAIR_MIN_PAPERS papers in the
    topic) compare observed inter-community citations against the modularity-null
    expectation ``deg_a * deg_b / (2 * e_int)``. Flag pairs whose expected links
    are >= PAIR_MIN_EXPECTED but observed <= PAIR_MAX_RATIO * expected. Sorted by
    the size of the gap (expected - observed), descending.
    """
    if e_int <= 0:
        return []
    sig = sorted((c for c, sz in counts.items() if sz >= PAIR_MIN_PAPERS),
                 key=lambda c: counts[c], reverse=True)
    out = []
    for i in range(len(sig)):
        for j in range(i + 1, len(sig)):
            a, b = (sig[i], sig[j]) if sig[i] < sig[j] else (sig[j], sig[i])
            expected = deg[a] * deg[b] / (2 * e_int)
            if expected < PAIR_MIN_EXPECTED:
                continue
            observed = pair_edges.get((a, b), 0)
            if observed > PAIR_MAX_RATIO * expected:
                continue
            out.append({
                "community_a": int(a), "name_a": names.get(a, f"Community {a}"),
                "size_a": int(counts[a]),
                "community_b": int(b), "name_b": names.get(b, f"Community {b}"),
                "size_b": int(counts[b]),
                "observed_edges": int(observed),
                "expected_edges": round(expected, 1),
            })
    out.sort(key=lambda p: p["expected_edges"] - p["observed_edges"], reverse=True)
    return out


def _compute_topic_community_metrics(
    graph: ig.Graph,
    topic: np.ndarray,
    community: np.ndarray,
    community_names: Optional[dict[int, str]] = None,
) -> tuple[dict, list[dict]]:
    """Core computation: per-topic composition/connectivity metrics + the flat
    citation-gap table. Kept as a plain function (graph + label arrays in) so it
    is unit-testable on a tiny synthetic igraph, mirroring test_bc_and_cocitation.
    """
    names = community_names or {}
    n_vertices = graph.vcount()

    # Composition: community mix within each topic.
    topic_comm_counts: dict[int, Counter] = defaultdict(Counter)
    topic_size: Counter = Counter()
    for v in range(n_vertices):
        t = int(topic[v])
        topic_size[t] += 1
        topic_comm_counts[t][int(community[v])] += 1

    # Connectivity: internal vs boundary edges, per-topic component structure,
    # and the per-community-pair internal citation counts for the recommender.
    internal_edges: Counter = Counter()
    boundary_edges: Counter = Counter()
    cross_internal_edges: Counter = Counter()
    topic_pair_edges: dict[int, Counter] = defaultdict(Counter)
    topic_comm_degree: dict[int, Counter] = defaultdict(Counter)
    uf = UnionFind()  # only same-topic endpoints are ever unioned
    for e in graph.es:
        s_idx, t_idx = e.source, e.target
        ts, tt = int(topic[s_idx]), int(topic[t_idx])
        if ts == tt:
            internal_edges[ts] += 1
            uf.union(s_idx, t_idx)
            cu, cv = int(community[s_idx]), int(community[t_idx])
            topic_comm_degree[ts][cu] += 1
            topic_comm_degree[ts][cv] += 1
            if cu != cv:
                cross_internal_edges[ts] += 1
                pair = (cu, cv) if cu < cv else (cv, cu)
                topic_pair_edges[ts][pair] += 1
        else:
            boundary_edges[ts] += 1
            boundary_edges[tt] += 1

    # Largest connected component per topic over its induced (internal) subgraph.
    comp_sizes: dict[int, Counter] = defaultdict(Counter)
    for v in range(n_vertices):
        comp_sizes[int(topic[v])][uf.find(v)] += 1

    metrics: dict[str, dict] = {}
    all_disconnected: list[dict] = []
    for t in sorted(topic_size):
        n = topic_size[t]
        counts = topic_comm_counts[t]
        ordered = counts.most_common()
        comm_count_list = [c for _, c in ordered]
        ent_norm, eff = _normalized_entropy(comm_count_list)
        dom_comm, dom_n = ordered[0]
        n_major = sum(1 for c in comm_count_list if c / n >= MAJOR_SHARE)

        ie = internal_edges[t]
        be = boundary_edges[t]
        ratio = ie / (ie + be) if (ie + be) > 0 else 0.0
        lcc = max(comp_sizes[t].values()) if comp_sizes[t] else 0

        ce = cross_internal_edges[t]
        cross_ratio = ce / ie if ie > 0 else 0.0
        expected_cross = 1.0 - sum((c / n) ** 2 for c in comm_count_list)
        integration = round(cross_ratio / expected_cross, 4) if expected_cross > 1e-9 else None

        breakdown = [
            {"community": int(cid), "name": names.get(cid, f"Community {cid}"),
             "count": int(c), "share": round(c / n, 4)}
            for cid, c in ordered[:BREAKDOWN_TOP_N]
        ]

        disconnected = [] if t == OUTLIER_TOPIC else _find_disconnected_pairs(
            counts, topic_comm_degree[t], topic_pair_edges[t], ie, names)

        metrics[str(t)] = {
            "n_papers": int(n),
            "n_communities": int(len(counts)),
            "n_communities_major": int(n_major),
            "dominant_community": int(dom_comm),
            "dominant_community_name": names.get(dom_comm, f"Community {dom_comm}"),
            "dominant_share": round(dom_n / n, 4),
            "community_entropy": round(ent_norm, 4),
            "effective_n_communities": round(eff, 2),
            "internal_edges": int(ie),
            "boundary_edges": int(be),
            "internal_edge_ratio": round(ratio, 4),
            "mean_internal_degree": round(2 * ie / n, 3) if n else 0.0,
            "lcc_fraction": round(lcc / n, 4) if n else 0.0,
            "cross_community_edges": int(ce),
            "cross_community_edge_ratio": round(cross_ratio, 4),
            "community_integration": integration,
            "n_disconnected_pairs": len(disconnected),
            "disconnected_pairs": disconnected[:DISCONNECTED_TOP_N],
            "community_breakdown": breakdown,
        }
        for p in disconnected:
            all_disconnected.append({"topic": int(t), **p})

    return metrics, all_disconnected


##################
##     Main     ##
##################
def _main() -> int:
    # Building the HamiltonTracker validates against a local UI server; only
    # construct it when the UI adapter below is actually enabled.
    # UI_CONFIG = adapters.HamiltonTracker(
    #     project_id=DEFAULT_UI_PROJECT_ID,
    #     username=DEFAULT_UI_USERNAME,
    #     dag_name=CURRENT_FILE_NAME,
    #     tags={"environment": "DEV", "team": TEAM_NAME, "version": "0.1"},
    # )
    inputs = dict(
        citation_network_path=GRAPH_LEVEL_DATA_PATH / "citation_network_with_topics_new.graphml",
    )
    outputs = [
        "save_topic_community_metrics_json",
        "save_topic_community_metrics_csv",
        "save_disconnected_communities_csv",
    ]
    import __main__
    dr = (
        driver.Builder()
        .with_modules(__main__)
        # .with_adapters(UI_CONFIG)
        .build()
    )
    dr.validate_execution(outputs, inputs=inputs)
    dr.display_all_functions(
        FIGURES_PATH / f"{CURRENT_FILE_NAME}_all_functions.png",
        keep_dot=True, deduplicate_inputs=True,
    )
    dr.visualize_execution(
        outputs, inputs=inputs,
        output_file_path=FIGURES_PATH / f"{CURRENT_FILE_NAME}.png",
        keep_dot=False, deduplicate_inputs=True,
    )
    if EXECUTE:
        dr.execute(outputs, inputs=inputs)
    return 0


#########################
##    DAG Definition   ##
#########################
@dataloader()
def citation_network(citation_network_path: Path) -> tuple[ig.Graph, dict]:
    graph = ig.Graph.Read_GraphML(str(citation_network_path))
    metadata = utils.get_file_metadata(citation_network_path)
    return graph, metadata


def topic_labels(citation_network: ig.Graph) -> np.ndarray:
    """Per-vertex semantic topic (BERTopic ``topic`` attribute)."""
    return _int_attr(citation_network, TOPIC_ATTR)


def community_labels(citation_network: ig.Graph) -> np.ndarray:
    """Per-vertex citation community at the configured CPM resolution."""
    return _int_attr(citation_network, COMMUNITY_ATTR)


@unpack_fields("topic_community_metrics", "disconnected_communities")
def analyze_topic_communities(
    citation_network: ig.Graph,
    topic_labels: np.ndarray,
    community_labels: np.ndarray,
) -> tuple[dict, list[dict]]:
    metrics, disconnected = _compute_topic_community_metrics(
        citation_network, topic_labels, community_labels)
    logger.info(
        "topic_community_analysis: %d topics, %d citation-gap pairs flagged",
        len(metrics), len(disconnected),
    )
    return metrics, disconnected


@datasaver()
def save_topic_community_metrics_json(topic_community_metrics: dict) -> dict:
    path = OUTPUT_DIR / "topic_community_metrics.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(topic_community_metrics, f, ensure_ascii=False, separators=(",", ":"))
    return utils.get_file_metadata(path)


@datasaver()
def save_topic_community_metrics_csv(topic_community_metrics: dict) -> dict:
    csv_cols = [
        "topic", "n_papers", "n_communities", "n_communities_major",
        "dominant_community", "dominant_community_name", "dominant_share",
        "community_entropy", "effective_n_communities", "internal_edges",
        "boundary_edges", "internal_edge_ratio", "mean_internal_degree",
        "lcc_fraction", "cross_community_edges", "cross_community_edge_ratio",
        "community_integration",
    ]
    rows = [{"topic": int(t), **{k: m[k] for k in csv_cols[1:]}}
            for t, m in topic_community_metrics.items()]
    path = OUTPUT_DIR / "topic_community_metrics.csv"
    pd.DataFrame(rows, columns=csv_cols).sort_values(
        "n_papers", ascending=False).to_csv(path, index=False)
    return utils.get_file_metadata(path)


@datasaver()
def save_disconnected_communities_csv(disconnected_communities: list[dict]) -> dict:
    pair_cols = ["topic", "community_a", "name_a", "size_a", "community_b",
                 "name_b", "size_b", "observed_edges", "expected_edges"]
    path = OUTPUT_DIR / "topic_disconnected_communities.csv"
    pd.DataFrame(disconnected_communities, columns=pair_cols).to_csv(path, index=False)
    return utils.get_file_metadata(path)


if __name__ == "__main__":
    sys.exit(_main())
