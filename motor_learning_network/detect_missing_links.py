"""Detect candidate *missing citations*: pairs of papers that are on the same
BERTopic topic (text view) but sit in different citation-network communities
(structure view) and do not cite each other.

This is the quantitative core of Step 3b in ``references/next-steps/next_steps_v2.md``.
It does two things:

1. **Null-model sanity gate** (`null_model_report`): shows that topic co-membership
   predicts citation linkage far in excess of chance, by permuting topic labels over
   nodes while holding the graph fixed. If this failed, the whole premise would be dead.

2. **Candidate enumeration + structural ranking** (`candidate_pairs`): for a given CPM
   resolution, enumerate same-topic / cross-community / unlinked pairs and rank them by
   bibliographic coupling (shared references) and co-citation (shared citers) — the
   structure-only predictors of Step 3b(iii). No text embeddings required, which matters
   because the raw SPECTER2/Gemini vectors are not available in this repo.

Ranking is a *structure-only* proxy here; the LLM pair-adjudication (Steps 1-2) and any
embedding-similarity signal are layered on top later.
"""
import sys
import json
import logging
import itertools
from pathlib import Path
from typing import Final

import numpy as np
import pandas as pd
import igraph as ig

from hamilton.function_modifiers import dataloader, datasaver
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

# Which CPM resolution's communities define "different citation cluster".
RESOLUTION: Final[float] = 0.005
COMMUNITY_ATTR: Final[str] = f"cpm_communities_at_res={RESOLUTION}"
OUTLIER_TOPIC: Final[int] = -1        # BERTopic's unassigned/outlier label
N_PERMUTATIONS: Final[int] = 200
PERMUTATION_SEED: Final[int] = 0
TOP_K_SAVED: Final[int] = 50_000      # cap on candidate rows written to disk

OUTPUT_DIR: Final[Path] = GRAPH_LEVEL_DATA_PATH / "missing_links"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

#####################
##  Aux Functions  ##
#####################
def _int_attr(graph: ig.Graph, attr: str) -> np.ndarray:
    """Read a vertex attribute that is stored as float/str and return int array."""
    return np.array([int(float(v)) for v in graph.vs[attr]])


def _undirected_edge_set(graph: ig.Graph) -> set[tuple[int, int]]:
    edges: set[tuple[int, int]] = set()
    for e in graph.es:
        a, b = e.source, e.target
        edges.add((a, b) if a < b else (b, a))
    return edges

##################
##     Main     ##
##################
def _main() -> int:
    UI_CONFIG = adapters.HamiltonTracker(
        project_id=DEFAULT_UI_PROJECT_ID,
        username=DEFAULT_UI_USERNAME,
        dag_name=CURRENT_FILE_NAME,
        tags={"environment": "DEV", "team": TEAM_NAME, "version": "0.1"},
    )
    inputs = dict(
        citation_network_path=GRAPH_LEVEL_DATA_PATH
        / "citation_network_with_topics_new.graphml"
    )
    outputs = ["save_candidate_pairs", "save_null_model_report"]

    import __main__
    dr = (
        driver.Builder()
        .with_modules(__main__)
        .with_adapters(UI_CONFIG)
        .build()
    )
    dr.validate_execution(outputs, inputs=inputs)
    dr.display_all_functions(
        FIGURES_PATH / f"{CURRENT_FILE_NAME}_all_functions.png",
        keep_dot=True,
        deduplicate_inputs=True,
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


def null_model_report(citation_network: ig.Graph) -> dict:
    """Permute topic labels over nodes (graph fixed) and count citation edges whose
    endpoints share a real topic. The observed count vs this null quantifies how much
    topic co-membership predicts linkage."""
    topic = _int_attr(citation_network, "topic")
    edges = np.array([(e.source, e.target) for e in citation_network.es])
    src, dst = edges[:, 0], edges[:, 1]
    real = topic != OUTLIER_TOPIC

    observed = int(np.sum((topic[src] == topic[dst]) & real[src] & real[dst]))

    rng = np.random.default_rng(PERMUTATION_SEED)
    null = np.empty(N_PERMUTATIONS)
    shuffled = topic.copy()
    for k in range(N_PERMUTATIONS):
        rng.shuffle(shuffled)
        r = shuffled != OUTLIER_TOPIC
        null[k] = np.sum((shuffled[src] == shuffled[dst]) & r[src] & r[dst])

    mu, sd = float(null.mean()), float(null.std())
    report = {
        "resolution": RESOLUTION,
        "n_nodes": citation_network.vcount(),
        "n_edges": citation_network.ecount(),
        "observed_linked_same_topic": observed,
        "null_mean": mu,
        "null_sd": sd,
        "null_max": float(null.max()),
        "enrichment_vs_null": observed / mu if mu else float("inf"),
        "z_score": (observed - mu) / sd if sd else float("inf"),
        "n_permutations": N_PERMUTATIONS,
    }
    logger.info("Null-model report: %s", report)
    return report


def candidate_pairs(citation_network: ig.Graph) -> pd.DataFrame:
    """Enumerate same-topic / cross-community / unlinked pairs and rank by structural
    coupling. Bibliographic coupling = shared out-neighbours (shared references);
    co-citation = shared in-neighbours (shared citing papers)."""
    g = citation_network
    topic = _int_attr(g, "topic")
    comm = _int_attr(g, COMMUNITY_ATTR)
    names = g.vs["name"]
    year = _int_attr(g, "year") if "year" in g.vs.attributes() else np.zeros(g.vcount(), int)

    out_nbrs = [set(g.successors(i)) for i in range(g.vcount())]
    in_nbrs = [set(g.predecessors(i)) for i in range(g.vcount())]
    edges = _undirected_edge_set(g)

    def linked(i: int, j: int) -> bool:
        return (i, j) in edges if i < j else (j, i) in edges

    rows = []
    real_topics = sorted(set(topic[topic != OUTLIER_TOPIC]))
    for t in real_topics:
        members = np.where(topic == t)[0]
        for i, j in itertools.combinations(members, 2):
            if comm[i] == comm[j] or linked(i, j):
                continue
            coupling = len(out_nbrs[i] & out_nbrs[j])
            cocitation = len(in_nbrs[i] & in_nbrs[j])
            if coupling == 0 and cocitation == 0:
                continue  # no structural evidence at all -> not a useful candidate
            rows.append(
                (names[i], names[j], t, int(comm[i]), int(comm[j]),
                 int(year[i]), int(year[j]), coupling, cocitation)
            )

    df = pd.DataFrame(
        rows,
        columns=["doi_a", "doi_b", "topic", "community_a", "community_b",
                 "year_a", "year_b", "bib_coupling", "co_citation"],
    )
    df["score"] = df["bib_coupling"] + df["co_citation"]
    df = df.sort_values("score", ascending=False).reset_index(drop=True)
    logger.info(
        "candidate_pairs: %d pairs with structural evidence (of same-topic/cross-comm/unlinked)",
        len(df),
    )
    return df


@datasaver()
def save_null_model_report(null_model_report: dict) -> dict:
    path = OUTPUT_DIR / f"null_model_report_res={RESOLUTION}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(null_model_report, f, indent=2)
    return utils.get_file_metadata(path)


@datasaver()
def save_candidate_pairs(candidate_pairs: pd.DataFrame) -> dict:
    path = OUTPUT_DIR / f"candidate_missing_links_res={RESOLUTION}.parquet"
    candidate_pairs.head(TOP_K_SAVED).to_parquet(path, index=False)
    return utils.get_file_metadata(path)


if __name__ == "__main__":
    sys.exit(_main())
