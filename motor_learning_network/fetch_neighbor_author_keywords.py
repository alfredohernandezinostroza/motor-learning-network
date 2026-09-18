"""Fetch genuine author-supplied keywords for the neighbor papers from
PubMed, and disambiguate the existing `keywords` vertex attribute on the
neighbor graphs -- which up to now silently held OpenAlex's algorithmically
-assigned Topic labels, not real author keywords (OpenAlex has no
author-keyword field at all: what it calls "topics" comes from a BERT
classifier trained on citation-network clusters, see
https://help.openalex.org/data/topics/).

Source: PubMed's `<KeywordList><Keyword>` XML element, fetched via the
`pymedx` library already used elsewhere in this repo (`get_pubmed_dataset.py`)
-- confirmed by reading pymedx's own source that `.keywords` extracts from
`.//Keyword` (author-selected), not `.//MeshHeading` (indexer-assigned MeSH).
Looked up by the `pubmed_id` column `get_neighbor_metadata.py` already
captured from OpenAlex's cross-reference (88.3% coverage, 28,060/31,774
papers) -- no new ID-resolution step needed.

For each neighbor paper: the OLD `keywords` value (really an OpenAlex Topic
label) moves to a new `openalex_topics` attribute; `keywords` is then set to
the real PubMed author keywords ("" where unavailable -- either no PMID, no
KeywordList on the PubMed record, or the query simply found nothing). Core
papers are untouched: their `keywords` was already genuine.

Outputs:
  data/processed/neighbor_author_keywords.parquet
      doi -> author_keywords ("|"-joined), has_author_keywords (bool)
  data/graph_level_data/citation_network_expanded_with_layout_and_author_keywords.graphml
      citation_network_expanded_with_layout.graphml with the keywords/topics
      split applied to neighbor vertices only (core untouched).
  data/graph_level_data/neighbor_communities/neighbor_citation_network_with_author_keywords.graphml
      neighbor_citation_network_with_community_keywords.graphml with the
      split applied (every vertex is a neighbor on this graph).

merge_neighbor_communities_into_layout.py's LAYOUT_GRAPHML/NEIGHBOR_METRICS_GRAPHML
constants were repointed at these two outputs -- rerun that module after this
one to propagate into the final combined Gephi file.
"""

import logging
from pathlib import Path
import sys
import time
from typing import Final

from hamilton import driver
from hamilton.function_modifiers import dataloader, datasaver
from hamilton.io import utils
import hamilton.log_setup
from hamilton_sdk import adapters
import igraph as ig
import pandas as pd
from pymedx import PubMed

from motor_learning_network.constants import (
    DEFAULT_UI_PROJECT_ID,
    DEFAULT_UI_USERNAME,
    EMAIL,
    FIGURES_PATH,
    GRAPH_LEVEL_DATA_PATH,
    PROCESSED_DATA_PATH,
    TEAM_NAME,
)

###################
##   Constants   ##
###################
CURRENT_FILE_NAME = Path(__file__).stem
hamilton.log_setup.setup_logging(logging.INFO)
logger = logging.getLogger(__name__)

EXECUTE = True

# NCBI's polite-use limit without an API key is 3 requests/sec; each batch
# issues one ESearch + one EFetch, so pacing between batches (not within
# pymedx's own internal 250-per-EFetch chunking) keeps well under that.
PMID_BATCH_SIZE: Final[int] = 150
REQUEST_SLEEP_SECONDS: Final[float] = 0.4
MAX_RETRIES_PER_BATCH: Final[int] = 3

NEIGHBOR_METADATA_PATH: Final[Path] = PROCESSED_DATA_PATH / "neighbor_metadata.parquet"
AUTHOR_KEYWORDS_PARQUET: Final[Path] = PROCESSED_DATA_PATH / "neighbor_author_keywords.parquet"

LAYOUT_GRAPHML: Final[Path] = (
    GRAPH_LEVEL_DATA_PATH / "citation_network_expanded_with_layout.graphml"
)
NEIGHBOR_COMMUNITIES_GRAPHML: Final[Path] = (
    GRAPH_LEVEL_DATA_PATH
    / "neighbor_communities"
    / "neighbor_citation_network_with_community_keywords.graphml"
)
OUTPUT_LAYOUT_GRAPHML: Final[Path] = (
    GRAPH_LEVEL_DATA_PATH / "citation_network_expanded_with_layout_and_author_keywords.graphml"
)
OUTPUT_NEIGHBOR_GRAPHML: Final[Path] = (
    GRAPH_LEVEL_DATA_PATH
    / "neighbor_communities"
    / "neighbor_citation_network_with_author_keywords.graphml"
)

#####################
##  Aux Functions  ##
#####################


def _pmids_to_query(neighbor_metadata_df: pd.DataFrame) -> list[str]:
    pmids = neighbor_metadata_df["pubmed_id"].dropna().astype(str).unique().tolist()
    return sorted(pmids)


def _pmid_batches(pmids: list[str], batch_size: int) -> list[list[str]]:
    return [pmids[i : i + batch_size] for i in range(0, len(pmids), batch_size)]


def _pubmed_query_for_pmids(pmids: list[str]) -> str:
    return " OR ".join(f"{pmid}[pmid]" for pmid in pmids)


def _extract_author_keywords(articles) -> dict[str, str]:
    """{pmid: "kw1|kw2|..."} for every fetched article that actually has a
    non-empty PubMed KeywordList; articles without one are simply absent
    (caller treats absence as "no author keywords available")."""
    result: dict[str, str] = {}
    for art in articles:
        pmid = getattr(art, "pubmed_id", None)
        keywords = getattr(art, "keywords", None) or []
        if pmid and keywords:
            result[str(pmid).strip()] = "|".join(keywords)
    return result


def _fetch_author_keywords_by_pmid(pmids: list[str]) -> dict[str, str]:
    pubmed = PubMed(tool="motor-learning-network", email=EMAIL)
    batches = _pmid_batches(pmids, PMID_BATCH_SIZE)
    result: dict[str, str] = {}

    for batch_index, batch in enumerate(batches):
        query = _pubmed_query_for_pmids(batch)
        for attempt in range(1, MAX_RETRIES_PER_BATCH + 1):
            try:
                articles = list(pubmed.query(query, max_results=len(batch)))
                result.update(_extract_author_keywords(articles))
                break
            except Exception as exc:
                logger.warning(
                    f"Batch {batch_index + 1}/{len(batches)} attempt {attempt} failed: {exc}"
                )
                if attempt == MAX_RETRIES_PER_BATCH:
                    logger.error(
                        f"Batch {batch_index + 1}/{len(batches)} failed after "
                        f"{MAX_RETRIES_PER_BATCH} attempts, skipping ({len(batch)} PMIDs lost)."
                    )
                else:
                    time.sleep(REQUEST_SLEEP_SECONDS * attempt)
        if (batch_index + 1) % 10 == 0 or batch_index + 1 == len(batches):
            logger.info(
                f"Fetched {batch_index + 1}/{len(batches)} batches, "
                f"{len(result)} PMIDs with author keywords so far."
            )
        time.sleep(REQUEST_SLEEP_SECONDS)

    return result


def _author_keywords_df(
    neighbor_metadata_df: pd.DataFrame, author_keywords_by_pmid: dict[str, str]
) -> pd.DataFrame:
    df = neighbor_metadata_df[["doi", "pubmed_id"]].copy()
    df["author_keywords"] = (
        df["pubmed_id"].astype(str).map(author_keywords_by_pmid).fillna("")
    )
    df["has_author_keywords"] = df["author_keywords"] != ""
    return df[["doi", "author_keywords", "has_author_keywords"]]


def _split_openalex_topics_from_keywords(
    graph: ig.Graph, author_keywords_by_doi: dict[str, str]
) -> ig.Graph:
    """For every neighbor vertex: move the current `keywords` value into a
    new `openalex_topics` attribute, then set `keywords` to the real
    PubMed-sourced author keywords ("" where unavailable). Core vertices
    (absent entirely on the neighbor-only graph) are left untouched -- their
    `keywords` was already genuine author keywords, not OpenAlex topics."""
    has_core = "is_original_node" in graph.vs.attributes()
    is_neighbor = (
        [v is False for v in graph.vs["is_original_node"]]
        if has_core
        else [True] * graph.vcount()
    )
    old_keywords = graph.vs["keywords"]
    names = graph.vs["name"]

    graph.vs["openalex_topics"] = [
        old_keywords[i] if is_neighbor[i] else "" for i in range(graph.vcount())
    ]
    graph.vs["keywords"] = [
        author_keywords_by_doi.get(names[i], "") if is_neighbor[i] else old_keywords[i]
        for i in range(graph.vcount())
    ]
    return graph


##################
##     Main     ##
##################


def _main() -> int:
    inputs = dict(
        neighbor_metadata_path=NEIGHBOR_METADATA_PATH,
        author_keywords_parquet_path=AUTHOR_KEYWORDS_PARQUET,
        layout_graphml_path=LAYOUT_GRAPHML,
        neighbor_communities_graphml_path=NEIGHBOR_COMMUNITIES_GRAPHML,
        output_layout_graphml_path=OUTPUT_LAYOUT_GRAPHML,
        output_neighbor_graphml_path=OUTPUT_NEIGHBOR_GRAPHML,
    )
    outputs = [
        "save_author_keywords_parquet",
        "save_layout_graph_with_author_keywords",
        "save_neighbor_graph_with_author_keywords",
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
def neighbor_metadata_df(neighbor_metadata_path: Path) -> tuple[pd.DataFrame, dict]:
    df = pd.read_parquet(neighbor_metadata_path)
    return df, utils.get_file_metadata(neighbor_metadata_path)


def pmids_to_query(neighbor_metadata_df: pd.DataFrame) -> list[str]:
    pmids = _pmids_to_query(neighbor_metadata_df)
    logger.info(f"{len(pmids)} distinct PubMed IDs to query for author keywords.")
    return pmids


def author_keywords_by_pmid(pmids_to_query: list[str]) -> dict[str, str]:
    result = _fetch_author_keywords_by_pmid(pmids_to_query)
    logger.info(
        f"Fetched author keywords for {len(result)}/{len(pmids_to_query)} PMIDs "
        f"({len(result) / len(pmids_to_query) * 100:.1f}%)."
    )
    return result


def author_keywords_df(
    neighbor_metadata_df: pd.DataFrame, author_keywords_by_pmid: dict[str, str]
) -> pd.DataFrame:
    return _author_keywords_df(neighbor_metadata_df, author_keywords_by_pmid)


def author_keywords_by_doi(author_keywords_df: pd.DataFrame) -> dict[str, str]:
    return author_keywords_df.set_index("doi")["author_keywords"].to_dict()


@datasaver()
def save_author_keywords_parquet(
    author_keywords_df: pd.DataFrame, author_keywords_parquet_path: Path
) -> dict:
    author_keywords_df.to_parquet(author_keywords_parquet_path)
    return utils.get_file_metadata(author_keywords_parquet_path)


@dataloader()
def layout_graph(layout_graphml_path: Path) -> tuple[ig.Graph, dict]:
    graph = ig.Graph.Read_GraphML(str(layout_graphml_path))
    return graph, utils.get_file_metadata(layout_graphml_path)


@dataloader()
def neighbor_communities_graph(neighbor_communities_graphml_path: Path) -> tuple[ig.Graph, dict]:
    graph = ig.Graph.Read_GraphML(str(neighbor_communities_graphml_path))
    return graph, utils.get_file_metadata(neighbor_communities_graphml_path)


def layout_graph_with_author_keywords(
    layout_graph: ig.Graph, author_keywords_by_doi: dict[str, str]
) -> ig.Graph:
    return _split_openalex_topics_from_keywords(layout_graph, author_keywords_by_doi)


def neighbor_graph_with_author_keywords(
    neighbor_communities_graph: ig.Graph, author_keywords_by_doi: dict[str, str]
) -> ig.Graph:
    return _split_openalex_topics_from_keywords(neighbor_communities_graph, author_keywords_by_doi)


@datasaver()
def save_layout_graph_with_author_keywords(
    layout_graph_with_author_keywords: ig.Graph, output_layout_graphml_path: Path
) -> dict:
    layout_graph_with_author_keywords.write(output_layout_graphml_path)
    return utils.get_file_metadata(output_layout_graphml_path)


@datasaver()
def save_neighbor_graph_with_author_keywords(
    neighbor_graph_with_author_keywords: ig.Graph, output_neighbor_graphml_path: Path
) -> dict:
    neighbor_graph_with_author_keywords.write(output_neighbor_graphml_path)
    return utils.get_file_metadata(output_neighbor_graphml_path)


if __name__ == "__main__":
    sys.exit(_main())
