"""Detect Leiden/CPM communities for the neighbor papers only, replicating
the same methodology used for the core corpus (`get_network_communities_and_stats.py`
-- frozen, not modified here) across the identical 36-resolution, 3-band
sweep: `leidenalg.find_partition(graph, CPMVertexPartition, resolution_parameter=r,
seed=0, n_iterations=10)`.

The 36-value RESOLUTIONS list (`community_resolution_bands.py`'s single
source of truth on the `worktree-community-connectivity-metrics` branch) is
re-derived here rather than imported, since that module lives only on a
different, unmerged branch -- verified directly against the three
`citation_network_full{,_high_res,_low_res}.graphml` files' own
`cpm_communities_at_res=*` columns: low 0.001-0.009 (9 values), mid
0.01-0.19 (19 values), high 0.2-0.9 (8 values).

Runs on the NEIGHBOR-ONLY induced subgraph (`is_original_node == False`,
and only the neighbor -> neighbor edges among them from
resolve_neighbor_reference_edges.py) as its own standalone network -- not
the combined core+neighbor graph. Same algorithm and parameters as the
frozen script, just a different input graph and no low-degree filtering
(that step exists in the frozen script but is dead code there -- its
result is discarded, never wired to the actual clustering -- so it isn't
part of "the pipeline" as actually run).

Outputs:
  data/graph_level_data/neighbor_communities/communities_per_resolution.parquet
      one row per neighbor DOI, one column per resolution
      (cpm_communities_at_res=<r>), matching the core corpus's convention.
  data/graph_level_data/neighbor_communities/neighbor_citation_network_with_communities.graphml
      the neighbor-only induced subgraph (31,774 vertices, 542,195 edges)
      with those community columns as vertex attributes.
"""

import logging
from pathlib import Path
import sys
from typing import Final

import cdlib
from hamilton import driver
from hamilton.function_modifiers import dataloader, datasaver, group, parameterize, source, value
from hamilton.io import utils
import hamilton.log_setup
from hamilton_sdk import adapters
import igraph as ig
import leidenalg
import pandas as pd

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

# Same three disjoint bands as the core corpus's three graphml files --
# verified 2026-09-10 by reading their cpm_communities_at_res=* columns
# directly (see the memory note on community-connectivity-metrics-module).
LOW_RESOLUTIONS: Final[list[float]] = [round(i * 0.001, 3) for i in range(1, 10)]
MID_RESOLUTIONS: Final[list[float]] = [round(i * 0.01, 3) for i in range(1, 20)]
HIGH_RESOLUTIONS: Final[list[float]] = [round(i * 0.1, 3) for i in range(2, 10)]
RESOLUTIONS: Final[list[float]] = LOW_RESOLUTIONS + MID_RESOLUTIONS + HIGH_RESOLUTIONS

SEED: Final[int] = 0
N_ITERATIONS: Final[int] = 10

INPUT_GRAPHML: Final[Path] = (
    GRAPH_LEVEL_DATA_PATH / "citation_network_expanded_with_reference_edges.graphml"
)
OUTPUT_DIR: Final[Path] = GRAPH_LEVEL_DATA_PATH / "neighbor_communities"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
COMMUNITIES_PARQUET: Final[Path] = OUTPUT_DIR / "communities_per_resolution.parquet"
OUTPUT_GRAPHML: Final[Path] = OUTPUT_DIR / "neighbor_citation_network_with_communities.graphml"

#####################
##  Aux Functions  ##
#####################


def _community_attribute_name(resolution: float) -> str:
    return f"cpm_communities_at_res={resolution}"


def _neighbor_subgraph(graph: ig.Graph) -> ig.Graph:
    neighbor_indices = [v.index for v in graph.vs if v["is_original_node"] is False]
    return graph.induced_subgraph(neighbor_indices)


def _leiden_cpm_communities(
    graph: ig.Graph, resolution: float, n_iterations: int, seed: int
) -> cdlib.NodeClustering:
    partition = leidenalg.find_partition(
        graph,
        leidenalg.CPMVertexPartition,
        resolution_parameter=resolution,
        initial_membership=None,
        weights=None,
        node_sizes=None,
        seed=seed,
        n_iterations=n_iterations,
    )
    communities = [graph.vs[x]["name"] for x in partition]
    return cdlib.NodeClustering(
        communities,
        graph,
        "CPM",
        method_parameters={
            "initial_membership": None,
            "weights": None,
            "node_sizes": None,
            "resolution_parameter": resolution,
            "n_iterations": n_iterations,
        },
    )


def _communities_per_resolution_df(communities: list[cdlib.NodeClustering]) -> pd.DataFrame:
    doi_to_community_series = [pd.Series(c.to_node_community_map()) for c in communities]
    doi_to_community_df = pd.concat(doi_to_community_series, axis=1)
    doi_to_community_df.columns = [
        _community_attribute_name(c.method_parameters["resolution_parameter"]) for c in communities
    ]
    return doi_to_community_df.map(lambda x: x[0])


def _attach_communities(graph: ig.Graph, communities_per_resolution_df: pd.DataFrame) -> ig.Graph:
    ordered = communities_per_resolution_df.loc[graph.vs["name"]]
    for column in ordered.columns:
        graph.vs[column] = ordered[column].tolist()
    return graph


##################
##     Main     ##
##################


def _main() -> int:
    inputs = dict(
        input_graphml_path=INPUT_GRAPHML,
        n_iterations=N_ITERATIONS,
        seed=SEED,
        communities_parquet_path=COMMUNITIES_PARQUET,
        output_graphml_path=OUTPUT_GRAPHML,
    )
    outputs = ["save_communities_parquet", "save_neighbor_citation_network_with_communities"]

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
def expanded_graph(input_graphml_path: Path) -> tuple[ig.Graph, dict]:
    graph = ig.Graph.Read_GraphML(str(input_graphml_path))
    return graph, utils.get_file_metadata(input_graphml_path)


def neighbor_subgraph(expanded_graph: ig.Graph) -> ig.Graph:
    subgraph = _neighbor_subgraph(expanded_graph)
    logger.info(
        f"Neighbor-only subgraph: {subgraph.vcount()} vertices, {subgraph.ecount()} edges."
    )
    return subgraph


@parameterize(**{f"leiden_with_resolution_{r}": {"resolution": value(r)} for r in RESOLUTIONS})
def leiden_cpm_communities(
    neighbor_subgraph: ig.Graph, resolution: float, n_iterations: int, seed: int
) -> cdlib.NodeClustering:
    logger.info(f"Running Leiden CPM at resolution={resolution} on the neighbor-only subgraph.")
    communities = _leiden_cpm_communities(neighbor_subgraph, resolution, n_iterations, seed)
    logger.info(f"  -> {len(communities.communities)} communities.")
    return communities


@parameterize(
    communities_per_resolution_df={
        "communities": group(*[source(f"leiden_with_resolution_{r}") for r in RESOLUTIONS])
    }
)
def communities_per_resolution_df(communities: list[cdlib.NodeClustering]) -> pd.DataFrame:
    return _communities_per_resolution_df(communities)


def neighbor_subgraph_with_communities(
    neighbor_subgraph: ig.Graph, communities_per_resolution_df: pd.DataFrame
) -> ig.Graph:
    return _attach_communities(neighbor_subgraph, communities_per_resolution_df)


@datasaver()
def save_communities_parquet(
    communities_per_resolution_df: pd.DataFrame, communities_parquet_path: Path
) -> dict:
    communities_per_resolution_df.to_parquet(communities_parquet_path)
    return utils.get_file_metadata(communities_parquet_path)


@datasaver()
def save_neighbor_citation_network_with_communities(
    neighbor_subgraph_with_communities: ig.Graph, output_graphml_path: Path
) -> dict:
    neighbor_subgraph_with_communities.write(output_graphml_path)
    return utils.get_file_metadata(output_graphml_path)


if __name__ == "__main__":
    sys.exit(_main())
