"""Merge the neighbor-only community assignments + quality metrics (at the
7-resolution plateau scored by `neighbor_community_quality_metrics.py`) onto
the laid-out, combined core+neighbor graph from
`layout_expanded_citation_network.py`, so a single graphml carries both the
halo layout and the neighbor communities for one Gephi load.

Every transferred column is prefixed `neighbor_`. This is not cosmetic: the
neighbor-only detection (`detect_neighbor_communities.py`) is a SEPARATE
Leiden/CPM run from the core corpus's own community detection, over a
disjoint vertex/edge set (the neighbor-only induced subgraph). A neighbor's
community id "12" at res=0.003 and a core paper's community id "12" at the
SAME res=0.003 (already on the base graph, from
`get_network_communities_and_stats.py`) are NOT the same community -- they
come from two independent clustering runs that just happen to reuse small
integer ids. Writing them into the same `cpm_communities_at_res=<r>` column
would silently conflate two unrelated partitions under one name; the
`neighbor_` prefix keeps every transferred column distinguishable from the
base graph's own (core-only) columns of the same metric/resolution.

Core vertices get no value (nan) for every `neighbor_*` column; this mirrors
how neighbor vertices already carry nan on the base graph's own
`cpm_communities_at_res=<r>` columns (core-only, from the original
detection).

Output: data/graph_level_data/citation_network_expanded_with_layout_and_neighbor_communities.graphml
"""

import logging
from pathlib import Path
import sys
from typing import Final

from hamilton import driver
from hamilton.function_modifiers import dataloader, datasaver
from hamilton.io import utils
import hamilton.log_setup
from hamilton_sdk import adapters
import igraph as ig
import pandas as pd

from motor_learning_network.constants import (
    DEFAULT_UI_PROJECT_ID,
    DEFAULT_UI_USERNAME,
    FIGURES_PATH,
    GRAPH_LEVEL_DATA_PATH,
    TEAM_NAME,
)
from motor_learning_network.neighbor_community_quality_metrics import RESOLUTIONS

###################
##   Constants   ##
###################
CURRENT_FILE_NAME = Path(__file__).stem
hamilton.log_setup.setup_logging(logging.INFO)
logger = logging.getLogger(__name__)

EXECUTE = True

LAYOUT_GRAPHML: Final[Path] = (
    GRAPH_LEVEL_DATA_PATH / "citation_network_expanded_with_layout.graphml"
)
NEIGHBOR_METRICS_GRAPHML: Final[Path] = (
    GRAPH_LEVEL_DATA_PATH
    / "neighbor_communities"
    / "neighbor_citation_network_with_community_metrics.graphml"
)
OUTPUT_GRAPHML: Final[Path] = (
    GRAPH_LEVEL_DATA_PATH
    / "citation_network_expanded_with_layout_and_neighbor_communities.graphml"
)

# The per-vertex quality-metric columns neighbor_community_quality_metrics.py
# writes onto each scored resolution, in addition to the community
# assignment itself (handled separately since it's renamed, not just prefixed).
NEIGHBOR_VERTEX_METRIC_NAMES: Final[list[str]] = [
    "community_size",
    "conductance",
    "conductance_out",
    "conductance_in",
    "internal_edge_density",
    "internal_edge_surprise",
    "internal_directed_edge_count",
    "boundary_edge_count",
]

#####################
##  Aux Functions  ##
#####################


def _neighbor_vertex_metrics_df(
    neighbor_graph: ig.Graph, resolutions: list[float]
) -> pd.DataFrame:
    """One row per neighbor DOI; columns are every transferred vertex-level
    attribute, already renamed to their `neighbor_`-prefixed target name."""
    data: dict[str, list] = {"doi": neighbor_graph.vs["name"]}
    for resolution in resolutions:
        data[f"neighbor_cpm_communities_at_res={resolution}"] = neighbor_graph.vs[
            f"cpm_communities_at_res={resolution}"
        ]
        for metric in NEIGHBOR_VERTEX_METRIC_NAMES:
            data[f"neighbor_{metric}_at_res={resolution}"] = neighbor_graph.vs[
                f"{metric}_at_res={resolution}"
            ]
    return pd.DataFrame(data).set_index("doi")


def _merge_neighbor_vertex_attributes(
    base_graph: ig.Graph, neighbor_metrics_df: pd.DataFrame
) -> ig.Graph:
    """Left-join `neighbor_metrics_df` onto `base_graph` by vertex name;
    core vertices (absent from the neighbor-only index) get nan."""
    ordered = neighbor_metrics_df.reindex(base_graph.vs["name"])
    for column in ordered.columns:
        base_graph.vs[column] = ordered[column].tolist()
    return base_graph


def _neighbor_graph_level_attributes(neighbor_graph: ig.Graph, resolutions: list[float]) -> dict:
    """Every resolution-tagged partition-level scalar
    (`neighbor_community_quality_metrics.py`'s per-partition metrics --
    modularity, surprise, significance, plateau flags, stability, ...), for
    just the given resolutions, renamed to its `neighbor_`-prefixed key."""
    suffixes = tuple(f"_at_res={r}" for r in resolutions)
    return {
        f"neighbor_{key}": neighbor_graph[key]
        for key in neighbor_graph.attributes()
        if key.endswith(suffixes)
    }


def _merge_neighbor_graph_attributes(
    base_graph: ig.Graph, neighbor_graph_attributes: dict
) -> ig.Graph:
    for key, value in neighbor_graph_attributes.items():
        base_graph[key] = value
    return base_graph


##################
##     Main     ##
##################


def _main() -> int:
    inputs = dict(
        layout_graphml_path=LAYOUT_GRAPHML,
        neighbor_metrics_graphml_path=NEIGHBOR_METRICS_GRAPHML,
        output_graphml_path=OUTPUT_GRAPHML,
    )
    outputs = ["save_merged_citation_network"]

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
        FIGURES_PATH / f"{CURRENT_FILE_NAME}_all_functions.png", keep_dot=True
    )
    dr.visualize_execution(
        outputs,
        inputs=inputs,
        output_file_path=FIGURES_PATH / f"{CURRENT_FILE_NAME}.png",
        keep_dot=False,
    )

    if EXECUTE:
        dr.execute(outputs, inputs=inputs)
    return 0


#########################
##    DAG Definition   ##
#########################


@dataloader()
def base_graph(layout_graphml_path: Path) -> tuple[ig.Graph, dict]:
    graph = ig.Graph.Read_GraphML(str(layout_graphml_path))
    return graph, utils.get_file_metadata(layout_graphml_path)


@dataloader()
def neighbor_graph(neighbor_metrics_graphml_path: Path) -> tuple[ig.Graph, dict]:
    graph = ig.Graph.Read_GraphML(str(neighbor_metrics_graphml_path))
    return graph, utils.get_file_metadata(neighbor_metrics_graphml_path)


def neighbor_vertex_metrics_df(neighbor_graph: ig.Graph) -> pd.DataFrame:
    return _neighbor_vertex_metrics_df(neighbor_graph, RESOLUTIONS)


def neighbor_graph_level_attributes(neighbor_graph: ig.Graph) -> dict:
    return _neighbor_graph_level_attributes(neighbor_graph, RESOLUTIONS)


def merged_citation_network(
    base_graph: ig.Graph,
    neighbor_vertex_metrics_df: pd.DataFrame,
    neighbor_graph_level_attributes: dict,
) -> ig.Graph:
    graph = _merge_neighbor_vertex_attributes(base_graph, neighbor_vertex_metrics_df)
    graph = _merge_neighbor_graph_attributes(graph, neighbor_graph_level_attributes)
    logger.info(
        f"Merged {len(neighbor_vertex_metrics_df.columns)} neighbor vertex columns and "
        f"{len(neighbor_graph_level_attributes)} neighbor graph-level attributes onto the layout graph."
    )
    return graph


@datasaver()
def save_merged_citation_network(
    merged_citation_network: ig.Graph, output_graphml_path: Path
) -> dict:
    merged_citation_network.write(output_graphml_path)
    return utils.get_file_metadata(output_graphml_path)


if __name__ == "__main__":
    sys.exit(_main())
