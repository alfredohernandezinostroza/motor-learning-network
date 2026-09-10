"""Resolve each neighbor paper's own OpenAlex reference list
(`referenced_openalex_ids`, captured by get_neighbor_metadata.py but never
turned into edges) into additional citation edges on the expanded graph.

citation_network_expanded_with_neighbors.graphml is strictly bipartite
(core -> neighbor only) because it's built purely from OUR corpus's own
outbound references. This module adds the two edge kinds that were missing:

  - neighbor -> neighbor: a neighbor paper citing another neighbor paper.
    Free -- every neighbor's own OpenAlex ID is already known
    (neighbor_metadata.parquet's `openalex_id` column), so this is a plain
    ID lookup, no fetching required.
  - neighbor -> core: a neighbor paper citing back into our original corpus.
    Needs the core papers' OpenAlex IDs, which we've never fetched (the core
    corpus came from Scopus/EBSCO/MEDLINE/WoS, not OpenAlex) -- one more
    batched OpenAlex fetch, by DOI, reusing get_neighbor_metadata.py's fetch.

Deliberately NOT resolving the ~520k other referenced OpenAlex IDs that point
to papers outside the expanded graph entirely -- doing so would mean a
two-hop crawl (tens of thousands of new nodes, ~11,000 more API requests)
and is out of scope for "connect what's already in the graph."

Outputs:
  data/processed/core_openalex_ids.parquet
      doi -> openalex_id for the original (core) corpus.
  data/graph_level_data/citation_network_expanded_with_reference_edges.graphml
      citation_network_expanded_with_neighbors.graphml plus neighbor-neighbor
      and neighbor-core edges. Vertices and existing edges are unchanged.
"""

import logging
from pathlib import Path
import sys
from typing import Final

from hamilton import driver
from hamilton.function_modifiers import config, dataloader, datasaver
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
from motor_learning_network.get_neighbor_metadata import _fetch_openalex_metadata, _normalize_doi

###################
##   Constants   ##
###################
CURRENT_FILE_NAME = Path(__file__).stem
hamilton.log_setup.setup_logging(logging.INFO)
logger = logging.getLogger(__name__)

EXECUTE = True

EXPANDED_GRAPHML: Final[Path] = (
    GRAPH_LEVEL_DATA_PATH / "citation_network_expanded_with_neighbors.graphml"
)
NEIGHBOR_METADATA_PATH: Final[Path] = PROCESSED_DATA_PATH / "neighbor_metadata.parquet"
CORE_OPENALEX_IDS_PATH: Final[Path] = PROCESSED_DATA_PATH / "core_openalex_ids.parquet"
OUTPUT_GRAPHML: Final[Path] = (
    GRAPH_LEVEL_DATA_PATH / "citation_network_expanded_with_reference_edges.graphml"
)

CORE_OPENALEX_ID_COLUMNS: Final[list[str]] = ["doi", "openalex_id"]

#####################
##  Aux Functions  ##
#####################


def _build_openalex_id_index(
    neighbor_metadata_df: pd.DataFrame, core_openalex_ids_df: pd.DataFrame
) -> dict[str, str]:
    """OpenAlex work ID -> DOI, covering every vertex currently in the
    expanded graph (neighbors + core)."""
    index: dict[str, str] = {}
    index.update(zip(neighbor_metadata_df["openalex_id"], neighbor_metadata_df["doi"]))
    index.update(zip(core_openalex_ids_df["openalex_id"], core_openalex_ids_df["doi"]))
    index.pop(None, None)
    return index


def _resolve_reference_edges(
    neighbor_metadata_df: pd.DataFrame, openalex_id_to_doi: dict[str, str]
) -> list[tuple[str, str]]:
    """Directed edges (neighbor_doi -> target_doi) from each neighbor's own
    reference list, restricted to targets already in the expanded graph
    (another neighbor or a core paper); self-loops dropped."""
    edges = []
    for row in neighbor_metadata_df.itertuples(index=False):
        citing = row.doi
        for openalex_id in row.referenced_openalex_ids:
            target = openalex_id_to_doi.get(openalex_id)
            if target and target != citing:
                edges.append((citing, target))
    return edges


def _add_reference_edges(graph: ig.Graph, edges: list[tuple[str, str]]) -> ig.Graph:
    name_to_index = {name: idx for idx, name in enumerate(graph.vs["name"])}
    index_edges = [
        (name_to_index[citing], name_to_index[target])
        for citing, target in edges
        if citing in name_to_index and target in name_to_index
    ]
    graph.add_edges(index_edges)
    logger.info(f"Added {len(index_edges)} reference-resolved edges (of {len(edges)} candidates).")
    return graph


##################
##     Main     ##
##################


def _main() -> int:
    core_openalex_ids_on_disk = CORE_OPENALEX_IDS_PATH.is_file()
    logger.info(f"Core OpenAlex IDs on disk: {core_openalex_ids_on_disk}")

    inputs = dict(
        expanded_graphml_path=EXPANDED_GRAPHML,
        neighbor_metadata_path=NEIGHBOR_METADATA_PATH,
        core_openalex_ids_path=CORE_OPENALEX_IDS_PATH,
        output_graphml_path=OUTPUT_GRAPHML,
    )
    outputs = ["save_expanded_citation_network_with_reference_edges", "save_core_openalex_ids"]

    import __main__

    UI_CONFIG = adapters.HamiltonTracker(
        project_id=DEFAULT_UI_PROJECT_ID,
        username=DEFAULT_UI_USERNAME,
        dag_name=CURRENT_FILE_NAME,
        tags={"environment": "DEV", "team": TEAM_NAME, "version": "0.1"},
    )
    dr = (
        driver.Builder()
        .with_modules(__main__)
        .with_config(dict(core_openalex_ids_on_disk=core_openalex_ids_on_disk))
        .with_adapters(UI_CONFIG)
        .build()
    )

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
def expanded_graph(expanded_graphml_path: Path) -> tuple[ig.Graph, dict]:
    graph = ig.Graph.Read_GraphML(str(expanded_graphml_path))
    return graph, utils.get_file_metadata(expanded_graphml_path)


def core_dois(expanded_graph: ig.Graph) -> list[str]:
    return [_normalize_doi(v["name"]) for v in expanded_graph.vs if v["is_original_node"] is True]


@dataloader()
def neighbor_metadata_df(neighbor_metadata_path: Path) -> tuple[pd.DataFrame, dict]:
    df = pd.read_parquet(neighbor_metadata_path)
    return df, utils.get_file_metadata(neighbor_metadata_path)


@config.when(core_openalex_ids_on_disk=True)
@dataloader()
def loaded_core_openalex_ids_df(core_openalex_ids_path: Path) -> tuple[pd.DataFrame, dict]:
    logger.info(f"Loading previously fetched core OpenAlex IDs from {core_openalex_ids_path}...")
    df = pd.read_parquet(core_openalex_ids_path)
    return df, utils.get_file_metadata(core_openalex_ids_path)


@config.when(core_openalex_ids_on_disk=True)
def core_dois_to_query__with_loaded(
    core_dois: list[str], loaded_core_openalex_ids_df: pd.DataFrame
) -> list[str]:
    already_fetched = set(loaded_core_openalex_ids_df["doi"])
    query_dois = [doi for doi in core_dois if doi not in already_fetched]
    logger.info(
        f"Querying {len(query_dois)} out of {len(core_dois)} core DOIs; the rest are already fetched."
    )
    return query_dois


@config.when_not(core_openalex_ids_on_disk=True)
def core_dois_to_query__all(core_dois: list[str]) -> list[str]:
    return core_dois


def fetched_core_metadata_df(core_dois_to_query: list[str]) -> pd.DataFrame:
    fetched_df, _errors_df = _fetch_openalex_metadata(core_dois_to_query)
    return fetched_df


def core_openalex_ids_df(
    fetched_core_metadata_df: pd.DataFrame,
    loaded_core_openalex_ids_df: pd.DataFrame = pd.DataFrame(columns=CORE_OPENALEX_ID_COLUMNS),
) -> pd.DataFrame:
    combined = pd.concat(
        [loaded_core_openalex_ids_df, fetched_core_metadata_df[CORE_OPENALEX_ID_COLUMNS]],
        ignore_index=True,
    )
    return combined.drop_duplicates(subset="doi", keep="last")


@datasaver()
def save_core_openalex_ids(
    core_openalex_ids_path: Path, core_openalex_ids_df: pd.DataFrame
) -> dict:
    core_openalex_ids_df.to_parquet(core_openalex_ids_path)
    return utils.get_file_metadata(core_openalex_ids_path)


def openalex_id_to_doi(
    neighbor_metadata_df: pd.DataFrame, core_openalex_ids_df: pd.DataFrame
) -> dict[str, str]:
    return _build_openalex_id_index(neighbor_metadata_df, core_openalex_ids_df)


def reference_edges(
    neighbor_metadata_df: pd.DataFrame, openalex_id_to_doi: dict[str, str]
) -> list[tuple[str, str]]:
    return _resolve_reference_edges(neighbor_metadata_df, openalex_id_to_doi)


def expanded_citation_network_with_reference_edges(
    expanded_graph: ig.Graph, reference_edges: list[tuple[str, str]]
) -> ig.Graph:
    return _add_reference_edges(expanded_graph, reference_edges)


@datasaver()
def save_expanded_citation_network_with_reference_edges(
    expanded_citation_network_with_reference_edges: ig.Graph, output_graphml_path: Path
) -> dict:
    expanded_citation_network_with_reference_edges.write(output_graphml_path)
    return utils.get_file_metadata(output_graphml_path)


if __name__ == "__main__":
    sys.exit(_main())
