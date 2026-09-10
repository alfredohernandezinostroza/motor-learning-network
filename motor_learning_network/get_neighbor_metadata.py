"""Identify external reference "neighbors" of the citation network and fetch
their metadata from OpenAlex.

Every paper in the network's giant component (citation_network_full_low_res.graphml)
already has its full outbound reference list stored in updated_references.parquet
(citing_doi -> cited_dois), including references to papers outside the network --
build_citation_network.py just discards those when it filters edges down to
valid_dois. This module instead counts, for each such external DOI, how many
distinct in-graph papers cite it, keeps the ones cited by at least
MIN_IN_GRAPH_CITERS, and fetches OpenAlex metadata for the kept set so
build_expanded_citation_network.py can attach them to the graph as new vertices.

Outputs (data/processed/):
  neighbor_metadata.parquet         one row per successfully fetched external DOI,
                                     the same columns as clean_unified_database.parquet
                                     (minus the leftover `index` column) plus
                                     `openalex_id` and `referenced_openalex_ids`
                                     (that neighbor's own outbound reference list, as
                                     OpenAlex work IDs -- the OpenAlex response already
                                     carries this alongside the descriptive metadata, so
                                     capturing it here is free; resolving those IDs to
                                     DOIs/graph edges is a separate follow-up step).
                                     source_database="OpenAlex".
  neighbor_metadata_errors.parquet  DOIs OpenAlex had no record for, or that
                                     failed to fetch -- retried on the next run.
"""

from collections import Counter
import logging
from pathlib import Path
import sys
import time
from typing import Final

from hamilton import driver
from hamilton.function_modifiers import config, dataloader, datasaver, unpack_fields
from hamilton.io import utils
import hamilton.log_setup
from hamilton_sdk import adapters
import igraph as ig
import pandas as pd
import requests
from tqdm import tqdm

from motor_learning_network.constants import (
    DEFAULT_UI_PROJECT_ID,
    DEFAULT_UI_USERNAME,
    EMAIL,
    FIGURES_PATH,
    GRAPH_LEVEL_DATA_PATH,
    OPENALEX_API_KEY,
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

# Keep an external DOI as a "neighbor" only if at least this many distinct
# in-graph papers cite it -- filters out one-off references (textbooks,
# unrelated-field methods citations) while keeping recurring literature.
# Chosen with Alfredo: 361,336 raw external DOIs -> 31,896 at this threshold.
MIN_IN_GRAPH_CITERS: Final[int] = 5

# OpenAlex's `doi:` filter accepts an OR-list; keep chunks well under its limit.
OPENALEX_DOI_BATCH_SIZE: Final[int] = 50
OPENALEX_REQUEST_PAUSE_SECONDS: Final[float] = 0.1

INPUT_GRAPHML: Final[Path] = GRAPH_LEVEL_DATA_PATH / "citation_network_full_low_res.graphml"
REFERENCES_PATH: Final[Path] = PROCESSED_DATA_PATH / "updated_references.parquet"
NEIGHBOR_METADATA_PATH: Final[Path] = PROCESSED_DATA_PATH / "neighbor_metadata.parquet"
NEIGHBOR_METADATA_ERRORS_PATH: Final[Path] = (
    PROCESSED_DATA_PATH / "neighbor_metadata_errors.parquet"
)

# Same column set as clean_unified_database.parquet (minus the leftover `index`
# column), plus openalex_id/referenced_openalex_ids -- the neighbor's own OpenAlex
# identity and outbound reference list, absent from the main corpus (which has no
# OpenAlex IDs) but free to capture here since it's already in the API response.
METADATA_COLUMNS: Final[list[str]] = [
    "doi",
    "title",
    "authors",
    "abstract",
    "keywords",
    "journal",
    "source_database",
    "pubmed_id",
    "year",
    "openalex_id",
    "referenced_openalex_ids",
]

#####################
##  Aux Functions  ##
#####################


def _normalize_doi(raw_doi: str) -> str:
    return raw_doi.strip().lower().removeprefix("https://doi.org/")


def _external_citation_counts(references_df: pd.DataFrame, graph_dois: set[str]) -> Counter:
    """Count, for each DOI cited by an in-graph paper but not itself in the
    graph, how many distinct in-graph papers cite it."""
    in_graph_rows = references_df[references_df["citing_doi"].isin(graph_dois)]
    counts: Counter = Counter()
    for cited_dois in in_graph_rows["cited_dois"]:
        if cited_dois is None:
            continue
        distinct_cited = {_normalize_doi(d) for d in cited_dois if d}
        distinct_cited -= graph_dois
        counts.update(distinct_cited)
    return counts


def _reconstruct_abstract_from_inverted_index(inverted_index: dict[str, list[int]] | None) -> str:
    """OpenAlex stores abstracts as {word: [positions]} to sidestep publisher
    copyright on raw abstract text. Standard reconstruction: place each word at
    every position it occupies, then join the words in position order."""
    if not inverted_index:
        return ""
    position_to_word: dict[int, str] = {}
    for word, positions in inverted_index.items():
        for position in positions:
            position_to_word[position] = word
    return " ".join(position_to_word[i] for i in sorted(position_to_word))


def _strip_openalex_prefix(openalex_url: str) -> str:
    return openalex_url.removeprefix("https://openalex.org/")


def _openalex_work_to_row(work: dict) -> dict:
    """Map one OpenAlex `works` record to the shared metadata schema. `authors`
    joins display names (OpenAlex doesn't expose split first/last name parts,
    so this is an approximation of the "Last, First" format used elsewhere).
    `referenced_openalex_ids` is this work's own outbound reference list --
    the OpenAlex response carries it alongside the descriptive fields, same
    call, no extra request."""
    authors = "|".join(
        authorship.get("author", {}).get("display_name") or ""
        for authorship in work.get("authorships") or []
    )
    location = work.get("primary_location") or work.get("host_venue") or {}
    source = location.get("source") or location
    keywords = "|".join(
        entry.get("display_name") or ""
        for entry in (work.get("topics") or work.get("concepts") or [])
    )
    pmid = (work.get("ids") or {}).get("pmid")
    if pmid:
        pmid = pmid.removeprefix("https://pubmed.ncbi.nlm.nih.gov/").rstrip("/")
    openalex_id = work.get("id")
    return {
        "doi": _normalize_doi(work.get("doi") or ""),
        "title": work.get("title") or "",
        "authors": authors,
        "abstract": _reconstruct_abstract_from_inverted_index(work.get("abstract_inverted_index")),
        "keywords": keywords,
        "journal": (source or {}).get("display_name") or "",
        "source_database": "OpenAlex",
        "pubmed_id": pmid,
        "year": work.get("publication_year"),
        "openalex_id": _strip_openalex_prefix(openalex_id) if openalex_id else None,
        "referenced_openalex_ids": tuple(
            _strip_openalex_prefix(ref) for ref in work.get("referenced_works") or []
        ),
    }


def _chunked(items: list[str], size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _fetch_openalex_metadata(dois_to_query: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch OpenAlex metadata for `dois_to_query` in batches, via the `doi:`
    OR-filter. Returns (fetched_metadata_df, error_metadata_df)."""
    fetched_rows = []
    error_rows = []
    api_call = "https://api.openalex.org/works"
    for batch in tqdm(list(_chunked(dois_to_query, OPENALEX_DOI_BATCH_SIZE))):
        params = {
            "filter": "doi:" + "|".join(batch),
            "per-page": OPENALEX_DOI_BATCH_SIZE,
            "mailto": EMAIL,
        }
        if OPENALEX_API_KEY:
            params["api_key"] = OPENALEX_API_KEY
        try:
            result = requests.get(api_call, params=params)
        except Exception as e:
            logger.warning(f"Request error for batch starting {batch[0]}: {e}")
            error_rows.extend(
                {"doi": doi, "error_message": f"request error: {e}"} for doi in batch
            )
            continue
        if result.status_code == 401 and "api_key" in params:
            # OpenAlex's premium key can be rejected independently of the free
            # "polite pool" (mailto-only) route -- fall back rather than fail
            # the whole batch, since the polite pool alone is enough at our volume.
            logger.warning("OpenAlex API key rejected (401); retrying this batch without it.")
            params.pop("api_key")
            result = requests.get(api_call, params=params)
        if result.status_code != 200:
            logger.warning(f"Status {result.status_code} for batch starting {batch[0]}")
            error_rows.extend(
                {"doi": doi, "error_message": f"status code {result.status_code}"} for doi in batch
            )
            time.sleep(OPENALEX_REQUEST_PAUSE_SECONDS)
            continue
        works = result.json().get("results", [])
        found_dois = set()
        for work in works:
            row = _openalex_work_to_row(work)
            if row["doi"]:
                found_dois.add(row["doi"])
            fetched_rows.append(row)
        error_rows.extend(
            {"doi": doi, "error_message": "no OpenAlex record found"}
            for doi in batch
            if doi not in found_dois
        )
        time.sleep(OPENALEX_REQUEST_PAUSE_SECONDS)
    fetched_metadata_df = pd.DataFrame(fetched_rows, columns=METADATA_COLUMNS)
    error_metadata_df = pd.DataFrame(error_rows, columns=["doi", "error_message"])
    return fetched_metadata_df, error_metadata_df


##################
##     Main     ##
##################


def _main() -> int:
    metadata_on_disk = NEIGHBOR_METADATA_PATH.is_file()
    logger.info(f"Neighbor metadata on disk: {metadata_on_disk}")

    inputs = dict(
        citation_network_path=INPUT_GRAPHML,
        references_path=REFERENCES_PATH,
        neighbor_metadata_path=NEIGHBOR_METADATA_PATH,
        neighbor_metadata_errors_path=NEIGHBOR_METADATA_ERRORS_PATH,
    )
    outputs = ["save_neighbor_metadata", "save_neighbor_metadata_errors"]

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
        .with_config(dict(metadata_on_disk=metadata_on_disk))
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
def citation_network(citation_network_path: Path) -> tuple[ig.Graph, dict]:
    graph = ig.Graph.Read_GraphML(str(citation_network_path))
    return graph, utils.get_file_metadata(citation_network_path)


def graph_dois(citation_network: ig.Graph) -> set[str]:
    return {_normalize_doi(name) for name in citation_network.vs["name"]}


@dataloader()
def references_df(references_path: Path) -> tuple[pd.DataFrame, dict]:
    df = pd.read_parquet(references_path)
    return df, utils.get_file_metadata(references_path)


def external_citation_counts(references_df: pd.DataFrame, graph_dois: set[str]) -> Counter:
    return _external_citation_counts(references_df, graph_dois)


def kept_external_dois(external_citation_counts: Counter) -> list[str]:
    kept = sorted(
        doi for doi, count in external_citation_counts.items() if count >= MIN_IN_GRAPH_CITERS
    )
    logger.info(
        f"{len(kept)} external DOIs cited by >= {MIN_IN_GRAPH_CITERS} in-graph papers "
        f"(out of {len(external_citation_counts)} raw external DOIs)."
    )
    return kept


@config.when(metadata_on_disk=True)
@dataloader()
def loaded_neighbor_metadata_df(neighbor_metadata_path: Path) -> tuple[pd.DataFrame, dict]:
    logger.info(f"Loading previously fetched neighbor metadata from {neighbor_metadata_path}...")
    df = pd.read_parquet(neighbor_metadata_path)
    return df, utils.get_file_metadata(neighbor_metadata_path)


@config.when(metadata_on_disk=True)
def dois_to_query__with_loaded_metadata(
    kept_external_dois: list[str], loaded_neighbor_metadata_df: pd.DataFrame
) -> list[str]:
    already_fetched = set(loaded_neighbor_metadata_df["doi"])
    query_dois = [doi for doi in kept_external_dois if doi not in already_fetched]
    logger.info(
        f"Querying {len(query_dois)} out of {len(kept_external_dois)} DOIs; the rest are already fetched."
    )
    return query_dois


@config.when_not(metadata_on_disk=True)
def dois_to_query__all(kept_external_dois: list[str]) -> list[str]:
    return kept_external_dois


@unpack_fields("fetched_metadata_df", "error_metadata_df")
def fetch_openalex_metadata(dois_to_query: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    return _fetch_openalex_metadata(dois_to_query)


@datasaver()
def save_neighbor_metadata(
    neighbor_metadata_path: Path,
    fetched_metadata_df: pd.DataFrame,
    loaded_neighbor_metadata_df: pd.DataFrame = pd.DataFrame(columns=METADATA_COLUMNS),
) -> dict:
    combined = pd.concat([loaded_neighbor_metadata_df, fetched_metadata_df], ignore_index=True)
    combined = combined.drop_duplicates(subset="doi", keep="last")
    combined.to_parquet(neighbor_metadata_path)
    return utils.get_file_metadata(neighbor_metadata_path)


@datasaver()
def save_neighbor_metadata_errors(
    neighbor_metadata_errors_path: Path, error_metadata_df: pd.DataFrame
) -> dict:
    error_metadata_df.to_parquet(neighbor_metadata_errors_path)
    return utils.get_file_metadata(neighbor_metadata_errors_path)


if __name__ == "__main__":
    sys.exit(_main())
