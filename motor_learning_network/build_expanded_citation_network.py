"""Assemble the expanded citation network: the existing giant-component graph
plus the external "neighbor" papers identified and fetched by
get_neighbor_metadata.py, connected by the citation edges from in-graph papers
to those neighbors.

Purely additive: every original vertex/edge and its attributes (including the
cpm_communities_at_res=* columns and the ForceAtlas2 x/y layout) are carried
over unchanged. New neighbor vertices get no community assignment and no
layout -- they were never community-detected or laid out -- and every vertex
is tagged `is_original_node` so downstream code can filter back to the
original graph.

Output: data/graph_level_data/citation_network_expanded_with_neighbors.graphml
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
    PROCESSED_DATA_PATH,
    TEAM_NAME,
)
from motor_learning_network.get_neighbor_metadata import _normalize_doi

###################
##   Constants   ##
###################
CURRENT_FILE_NAME = Path(__file__).stem
hamilton.log_setup.setup_logging(logging.INFO)
logger = logging.getLogger(__name__)

EXECUTE = True

BASE_GRAPHML: Final[Path] = GRAPH_LEVEL_DATA_PATH / "citation_network_full_low_res.graphml"
NEIGHBOR_METADATA_PATH: Final[Path] = PROCESSED_DATA_PATH / "neighbor_metadata.parquet"
REFERENCES_PATH: Final[Path] = PROCESSED_DATA_PATH / "updated_references.parquet"
OUTPUT_GRAPHML: Final[Path] = (
    GRAPH_LEVEL_DATA_PATH / "citation_network_expanded_with_neighbors.graphml"
)

# Attributes copied from neighbor_metadata_df onto new vertices. Deliberately
# excludes the layout (x, y) and cpm_communities_at_res=* columns that live on
# the original graph -- neighbor papers were never laid out or
# community-detected, and leaving those unset is a meaningful signal.
NEIGHBOR_METADATA_ATTRIBUTES: Final[list[str]] = [
    "title",
    "authors",
    "abstract",
    "keywords",
    "journal",
    "source_database",
    "year",
]

#####################
##  Aux Functions  ##
#####################


def _neighbor_edges_from_references(
    references_df: pd.DataFrame, graph_dois: set[str], kept_external_dois: set[str]
) -> list[tuple[str, str]]:
    """Directed edges (citing_doi -> cited_doi) from in-graph papers to the
    kept external neighbor DOIs. Mirrors _build_edges_from_references in
    build_citation_network.py, but targets the kept-external set instead of
    valid_dois -- the original in-graph edges are already baked into the base
    graph loaded from graphml, so this only adds the new ones."""
    edges = []
    for row in references_df.itertuples(index=False):
        citing = row.citing_doi
        if not citing or citing not in graph_dois:
            continue
        cited_dois = row.cited_dois
        if cited_dois is None:
            continue
        for cited in cited_dois:
            if not cited:
                continue
            cited = _normalize_doi(cited)
            if cited in kept_external_dois:
                edges.append((citing, cited))
    return edges


def _add_neighbor_vertices(graph: ig.Graph, neighbor_metadata_df: pd.DataFrame) -> ig.Graph:
    """Append one new vertex per fetched neighbor DOI and tag every vertex
    (original and new) with `is_original_node`."""
    original_count = graph.vcount()
    neighbor_count = len(neighbor_metadata_df)

    graph.add_vertices(neighbor_count)
    graph.vs[original_count:]["name"] = neighbor_metadata_df["doi"].tolist()
    for attribute in NEIGHBOR_METADATA_ATTRIBUTES:
        graph.vs[original_count:][attribute] = neighbor_metadata_df[attribute].tolist()
    graph.vs["is_original_node"] = [True] * original_count + [False] * neighbor_count
    return graph


def _add_neighbor_edges(
    graph: ig.Graph,
    references_df: pd.DataFrame,
    graph_dois: set[str],
    kept_external_dois: set[str],
) -> ig.Graph:
    name_to_index = {name: idx for idx, name in enumerate(graph.vs["name"])}
    doi_edges = _neighbor_edges_from_references(references_df, graph_dois, kept_external_dois)
    index_edges = [
        (name_to_index[citing], name_to_index[cited])
        for citing, cited in doi_edges
        if citing in name_to_index and cited in name_to_index
    ]
    graph.add_edges(index_edges)
    logger.info(
        f"Added {len(index_edges)} neighbor edges to {len(kept_external_dois)} neighbor papers."
    )
    return graph


##################
##     Main     ##
##################


def _main() -> int:
    inputs = dict(
        base_graphml_path=BASE_GRAPHML,
        neighbor_metadata_path=NEIGHBOR_METADATA_PATH,
        references_path=REFERENCES_PATH,
        output_graphml_path=OUTPUT_GRAPHML,
    )
    outputs = ["save_expanded_citation_network"]

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
def base_graph(base_graphml_path: Path) -> tuple[ig.Graph, dict]:
    graph = ig.Graph.Read_GraphML(str(base_graphml_path))
    return graph, utils.get_file_metadata(base_graphml_path)


def graph_dois(base_graph: ig.Graph) -> set[str]:
    return {_normalize_doi(name) for name in base_graph.vs["name"]}


@dataloader()
def neighbor_metadata_df(neighbor_metadata_path: Path) -> tuple[pd.DataFrame, dict]:
    df = pd.read_parquet(neighbor_metadata_path)
    return df, utils.get_file_metadata(neighbor_metadata_path)


def kept_external_dois(neighbor_metadata_df: pd.DataFrame) -> set[str]:
    return set(neighbor_metadata_df["doi"])


@dataloader()
def references_df(references_path: Path) -> tuple[pd.DataFrame, dict]:
    df = pd.read_parquet(references_path)
    return df, utils.get_file_metadata(references_path)


def expanded_citation_network(
    base_graph: ig.Graph,
    neighbor_metadata_df: pd.DataFrame,
    references_df: pd.DataFrame,
    graph_dois: set[str],
    kept_external_dois: set[str],
) -> ig.Graph:
    graph = _add_neighbor_vertices(base_graph, neighbor_metadata_df)
    graph = _add_neighbor_edges(graph, references_df, graph_dois, kept_external_dois)
    logger.info(f"Expanded citation network: {graph.vcount()} vertices, {graph.ecount()} edges.")
    return graph


@datasaver()
def save_expanded_citation_network(
    expanded_citation_network: ig.Graph, output_graphml_path: Path
) -> dict:
    expanded_citation_network.write(output_graphml_path)
    return utils.get_file_metadata(output_graphml_path)


if __name__ == "__main__":
    sys.exit(_main())
