"""Clustering-quality metrics for the neighbor-only Leiden/CPM communities
detected by `detect_neighbor_communities.py`.

Replicates `community_quality_metrics.py`'s methodology exactly, on the
neighbor-only graph instead of the core corpus -- reuses that module's
private helpers directly rather than re-implementing them (they're generic
over `graph`/`membership`/`resolution`, no dependency on which graph). See
that module's docstring for what each metric means and why the naive
aggregates are misleading here (singleton-community dominance, etc.); the
same caveats apply unchanged.

Scoped to RESOLUTIONS = [0.003..0.009] (see that constant's comment) rather
than detect_neighbor_communities.py's full 36-value sweep -- this stage's
per-resolution cost (significance + 5x cross-seed stability reruns) is high
enough on this graph that the full sweep isn't practical, and this is the
same plateau already validated as the right operating range on the core
corpus for the Complex Networks submission.

Outputs (data/graph_level_data/neighbor_communities/):
  neighbor_citation_network_with_community_metrics.graphml
  quality_metrics_per_community.parquet
  quality_metrics_per_partition.parquet
"""

import logging
import math
from pathlib import Path
import sys
from typing import Final

from hamilton import driver
from hamilton.function_modifiers import dataloader, datasaver, group, parameterize, source, value
from hamilton.io import utils
import hamilton.log_setup
from hamilton_sdk import adapters
import igraph as ig
import networkx as nx
import numpy as np
import pandas as pd

from motor_learning_network.community_quality_metrics import (
    PLATEAU_NMI_THRESHOLD,
    STABILITY_SEEDS,
    _community_attribute_name,
    _community_surprise,
    _constant_potts_model_score,
    _cross_seed_stability,
    _directed_community_edge_counts,
    _directed_conductance,
    _directed_internal_edge_density,
    _directed_modularity,
    _directed_surprise,
    _intra_community_edge_fraction,
    _parallel_edge_count,
    _reciprocal_edge_pair_count,
    _resolution_plateau_flags,
    _self_loop_count,
    _significance,
    _summarize_community_metrics,
)
from motor_learning_network.constants import (
    DEFAULT_UI_PROJECT_ID,
    DEFAULT_UI_USERNAME,
    FIGURES_PATH,
    GRAPH_LEVEL_DATA_PATH,
    TEAM_NAME,
)

###################
##   Constants   ##
###################
CURRENT_FILE_NAME = Path(__file__).stem
hamilton.log_setup.setup_logging(logging.INFO)
logger = logging.getLogger(__name__)

EXECUTE = True

# Scoped to the resolution plateau the Complex Networks conference submission
# objectively identified on the CORE corpus (cross-seed NMI > 0.89 across
# 0.003-0.009; final reported value gamma=0.003 -- see the
# conference-submission-workstream memory), not the full 36-resolution
# sweep. Per-resolution cost here is dominated by _significance (undirected
# KL-divergence over every community) and _cross_seed_stability (5 extra
# Leiden reruns) -- both expensive enough on this graph (31,774 vertices,
# 542,195 edges) that a single resolution took >10 minutes in testing, so
# the full 36-value sweep (as run for detect_neighbor_communities.py, which
# has no such per-resolution cost) is not run through this stage.
RESOLUTIONS: Final[list[float]] = [0.003, 0.004, 0.005, 0.006, 0.007, 0.008, 0.009]

INPUT_GRAPHML: Final[Path] = (
    GRAPH_LEVEL_DATA_PATH
    / "neighbor_communities"
    / "neighbor_citation_network_with_communities.graphml"
)
OUTPUT_DIR: Final[Path] = GRAPH_LEVEL_DATA_PATH / "neighbor_communities"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_GRAPHML: Final[Path] = (
    OUTPUT_DIR / "neighbor_citation_network_with_community_metrics.graphml"
)
PER_COMMUNITY_PARQUET: Final[Path] = OUTPUT_DIR / "quality_metrics_per_community.parquet"
PER_PARTITION_PARQUET: Final[Path] = OUTPUT_DIR / "quality_metrics_per_partition.parquet"

##################
##     Main     ##
##################


def _main() -> int:
    inputs = dict(
        citation_network_path=INPUT_GRAPHML,
        n_iterations=10,
        stability_seeds=list(STABILITY_SEEDS),
    )
    outputs = [
        "save_citation_network_with_community_metrics",
        "save_per_community_quality_metrics",
        "save_per_partition_quality_metrics",
    ]

    import __main__

    UI_CONFIG = adapters.HamiltonTracker(
        project_id=DEFAULT_UI_PROJECT_ID,
        username=DEFAULT_UI_USERNAME,
        dag_name=CURRENT_FILE_NAME,
        tags={"environment": "DEV", "team": TEAM_NAME, "version": "0.1"},
    )
    dr = driver.Builder().with_modules(__main__).with_adapters(UI_CONFIG).build()

    dr.validate_execution(outputs, inputs=inputs)
    dr.display_all_functions(
        FIGURES_PATH / f"{CURRENT_FILE_NAME}_all_functions.png",
        keep_dot=True,
        deduplicate_inputs=True,
    )
    dr.visualize_execution(
        outputs,
        inputs=inputs,
        output_file_path=FIGURES_PATH / f"{CURRENT_FILE_NAME}.png",
        keep_dot=False,
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
    return graph, utils.get_file_metadata(citation_network_path)


def undirected_networkx_graph(citation_network: ig.Graph) -> nx.Graph:
    return citation_network.to_networkx().to_undirected()


def parallel_edge_count(citation_network: ig.Graph) -> int:
    count = _parallel_edge_count(citation_network)
    logger.info(f"neighbor-only network has {count} parallel edges.")
    return count


def self_loop_count(citation_network: ig.Graph) -> int:
    count = _self_loop_count(citation_network)
    logger.info(f"neighbor-only network has {count} self-loops.")
    return count


def reciprocal_edge_pair_count(citation_network: ig.Graph) -> int:
    count = _reciprocal_edge_pair_count(citation_network)
    logger.info(f"neighbor-only network has {count} reciprocal edge pairs.")
    return count


@parameterize(
    **{f"community_membership_at_resolution_{r}": {"resolution": value(r)} for r in RESOLUTIONS}
)
def community_membership_for_resolution(
    citation_network: ig.Graph, resolution: float
) -> np.ndarray:
    attribute_name = _community_attribute_name(resolution)
    return np.array([int(float(v)) for v in citation_network.vs[attribute_name]])


@parameterize(
    community_memberships_by_resolution={
        "memberships": group(
            *[source(f"community_membership_at_resolution_{r}") for r in RESOLUTIONS]
        )
    }
)
def community_memberships_by_resolution(memberships: list[np.ndarray]) -> list[np.ndarray]:
    return memberships


def resolution_plateau_flags(
    community_memberships_by_resolution: list[np.ndarray],
) -> dict[float, dict]:
    return _resolution_plateau_flags(
        community_memberships_by_resolution, RESOLUTIONS, PLATEAU_NMI_THRESHOLD
    )


@parameterize(
    **{
        f"community_quality_metrics_at_resolution_{r}": {
            "resolution": value(r),
            "community_membership": source(f"community_membership_at_resolution_{r}"),
        }
        for r in RESOLUTIONS
    }
)
def community_quality_metrics_for_resolution(
    citation_network: ig.Graph,
    undirected_networkx_graph: nx.Graph,
    resolution: float,
    community_membership: np.ndarray,
    resolution_plateau_flags: dict[float, dict],
    n_iterations: int,
    stability_seeds: list[int],
) -> dict:
    total_directed_edges = citation_network.ecount()
    n_vertices = citation_network.vcount()
    edge_counts = _directed_community_edge_counts(citation_network, community_membership)
    total_internal_edges = sum(c["internal_directed_edge_count"] for c in edge_counts.values())

    per_community = []
    for community_id, counts in edge_counts.items():
        conductances = _directed_conductance(counts, total_directed_edges)
        per_community.append(
            {
                "resolution": resolution,
                "community_id": community_id,
                "community_size": counts["size"],
                "internal_directed_edge_count": counts["internal_directed_edge_count"],
                "boundary_edge_count": counts["boundary_edge_count"],
                "out_boundary_edge_count": counts["out_boundary_edge_count"],
                "in_boundary_edge_count": counts["in_boundary_edge_count"],
                "conductance": conductances["conductance"],
                "conductance_out": conductances["conductance_out"],
                "conductance_in": conductances["conductance_in"],
                "internal_edge_density": _directed_internal_edge_density(counts),
                "internal_edge_surprise": _community_surprise(
                    counts["internal_directed_edge_count"],
                    counts["size"],
                    n_vertices,
                    total_directed_edges,
                ),
            }
        )

    stability = _cross_seed_stability(
        citation_network, resolution, n_iterations, community_membership, tuple(stability_seeds)
    )
    plateau = resolution_plateau_flags[resolution]

    per_partition = {
        "resolution": resolution,
        "number_of_communities": len(edge_counts),
        "modularity": _directed_modularity(citation_network, community_membership),
        "constant_potts_model_score": _constant_potts_model_score(
            citation_network, community_membership, resolution
        ),
        "surprise": _directed_surprise(
            n_vertices,
            total_directed_edges,
            [c["size"] for c in edge_counts.values()],
            total_internal_edges,
        ),
        "significance": _significance(undirected_networkx_graph, community_membership),
        "mean_internal_edge_density": float(
            np.mean([m["internal_edge_density"] for m in per_community])
        )
        if per_community
        else 0.0,
        "intra_community_edge_fraction": _intra_community_edge_fraction(
            total_internal_edges, total_directed_edges
        ),
        **_summarize_community_metrics(per_community),
        **stability,
        **plateau,
    }
    logger.info(
        "resolution=%s: %d communities, modularity=%.4f, constant_potts_model_score=%.2f, "
        "surprise=%.2f, significance=%.2f",
        resolution,
        len(edge_counts),
        per_partition["modularity"],
        per_partition["constant_potts_model_score"],
        per_partition["surprise"],
        per_partition["significance"],
    )
    return {
        "resolution": resolution,
        "per_community": per_community,
        "per_partition": per_partition,
    }


@parameterize(
    community_quality_metrics_all_resolutions={
        "bundles": group(
            *[source(f"community_quality_metrics_at_resolution_{r}") for r in RESOLUTIONS]
        )
    }
)
def community_quality_metrics_all_resolutions(bundles: list[dict]) -> list[dict]:
    return bundles


def per_community_quality_metrics_df(
    community_quality_metrics_all_resolutions: list[dict],
) -> pd.DataFrame:
    rows = [
        row
        for bundle in community_quality_metrics_all_resolutions
        for row in bundle["per_community"]
    ]
    return pd.DataFrame(rows)


def per_partition_quality_metrics_df(
    community_quality_metrics_all_resolutions: list[dict],
) -> pd.DataFrame:
    rows = [bundle["per_partition"] for bundle in community_quality_metrics_all_resolutions]
    return pd.DataFrame(rows)


def citation_network_with_community_metrics(
    citation_network: ig.Graph,
    community_quality_metrics_all_resolutions: list[dict],
    community_memberships_by_resolution: list[np.ndarray],
    reciprocal_edge_pair_count: int,
    parallel_edge_count: int,
    self_loop_count: int,
) -> ig.Graph:
    graph = citation_network.copy()
    graph["metrics_edge_directedness"] = "directed"
    graph["significance_edge_directedness"] = "undirected_no_standard_directed_definition"
    graph["reciprocal_edge_pair_count"] = int(reciprocal_edge_pair_count)
    graph["parallel_edge_count"] = int(parallel_edge_count)
    graph["self_loop_count"] = int(self_loop_count)

    node_metric_names = [
        "community_size",
        "conductance",
        "conductance_out",
        "conductance_in",
        "internal_edge_density",
        "internal_edge_surprise",
        "internal_directed_edge_count",
        "boundary_edge_count",
    ]

    for bundle, membership in zip(
        community_quality_metrics_all_resolutions, community_memberships_by_resolution
    ):
        resolution = bundle["resolution"]
        suffix = f"_at_res={resolution}"

        for key, metric_value in bundle["per_partition"].items():
            if key == "resolution":
                continue
            if isinstance(metric_value, float) and math.isnan(metric_value):
                continue
            if isinstance(metric_value, bool):
                metric_value = int(metric_value)
            graph[f"{key}{suffix}"] = metric_value

        lookup = {row["community_id"]: row for row in bundle["per_community"]}
        for metric_name in node_metric_names:
            graph.vs[f"{metric_name}{suffix}"] = [lookup[int(c)][metric_name] for c in membership]

    return graph


@datasaver()
def save_citation_network_with_community_metrics(
    citation_network_with_community_metrics: ig.Graph,
) -> dict:
    citation_network_with_community_metrics.write(OUTPUT_GRAPHML)
    return utils.get_file_metadata(OUTPUT_GRAPHML)


@datasaver()
def save_per_community_quality_metrics(per_community_quality_metrics_df: pd.DataFrame) -> dict:
    per_community_quality_metrics_df.to_parquet(PER_COMMUNITY_PARQUET)
    return utils.get_file_metadata(PER_COMMUNITY_PARQUET)


@datasaver()
def save_per_partition_quality_metrics(per_partition_quality_metrics_df: pd.DataFrame) -> dict:
    per_partition_quality_metrics_df.to_parquet(PER_PARTITION_PARQUET)
    return utils.get_file_metadata(PER_PARTITION_PARQUET)


if __name__ == "__main__":
    sys.exit(_main())
