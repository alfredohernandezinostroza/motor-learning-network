"""Fetch author-supplied keywords for the neighbor papers from Scopus, and
layer them over the PubMed keywords `fetch_neighbor_author_keywords.py`
already collected.

Why a second source at all: PubMed's `<KeywordList>` only covers 14.6% of our
neighbours (4,088/28,060 queried PMIDs), because NLM only began requiring
depositors to supply one around 2013 -- a hard recency gate, not a random
miss. Scopus has no such gate and reaches ~65%, including 1999- and
2005-vintage papers.

Scopus's keywords are also the *same kind of object* as the core corpus's:
both are `|`-separated Title case (`Cerebellum|Motor learning|Synaptic
plasticity`), because the core corpus was itself assembled from Scopus/WoS
exports. PubMed's are MeSH-flavoured uppercase with subheadings
(`DEPRESSION/psychology|PSYCHOLOGICAL TESTS`) -- genuine author keywords, but
a different surface form. `keywords_source` records the provenance per paper
so the PubMed-only rows can be filtered downstream if that casing difference
ever fragments the TF-IDF vocabulary in `neighbor_community_keywords.py`.

ENTITLEMENT: this module only works from an institutional network. Elsevier
gates `authkeywords` by IP, and from our own address every view above
STANDARD returns `401 AUTHORIZATION_ERROR`; through the JHU tunnel the same
requests return 200 with keywords populated. Run it as:

    INSTITUTIONAL_PROXY_URL=socks5h://127.0.0.1:11080 \
        pixi run python -m motor_learning_network.fetch_neighbor_scopus_keywords

The exit IP is verified before any quota is spent -- see
`_verify_proxy_exit_ip` for why that guard is not optional.

Outputs:
  data/processed/neighbor_scopus_keywords_checkpoint.parquet
      resumable fetch state: every DOI queried, with "" for confirmed-empty.
  data/processed/neighbor_scopus_keywords.parquet
      doi -> scopus_keywords, pubmed_keywords, author_keywords, keywords_source
  data/graph_level_data/citation_network_expanded_with_layout_and_scopus_keywords.graphml
  data/graph_level_data/neighbor_communities/neighbor_citation_network_with_scopus_keywords.graphml

Rerun merge_neighbor_communities_into_layout.py afterwards to propagate into
the final combined Gephi file.
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
import requests

from motor_learning_network.constants import (
    DEFAULT_UI_PROJECT_ID,
    DEFAULT_UI_USERNAME,
    FIGURES_PATH,
    GRAPH_LEVEL_DATA_PATH,
    INSTITUTIONAL_PROXY_URL,
    PROCESSED_DATA_PATH,
    SCOPUS_API_KEY,
    TEAM_NAME,
)

###################
##   Constants   ##
###################
CURRENT_FILE_NAME = Path(__file__).stem
hamilton.log_setup.setup_logging(logging.INFO)
logger = logging.getLogger(__name__)

EXECUTE = True

SCOPUS_SEARCH_URL: Final[str] = "https://api.elsevier.com/content/search/scopus"

# JHU exits from more than one range: an observed session came out of
# 162.129.x, not the 128.220.x that vpn.jh.edu itself resolves to. A
# single-prefix check would reject a perfectly good tunnel.
JHU_IP_PREFIXES: Final[tuple[str, ...]] = ("128.220.", "162.129.")

# view=COMPLETE is the only Search view carrying authkeywords, and it caps
# `count` at 25. Measured lossless against AbstractRetrieval view=FULL on a
# 40-DOI sample (26 identical / 9 both-empty / 0 discrepancies) while costing
# 1/25th the quota: 1,271 requests for all 31,774 neighbours against Search's
# 20,000/week, versus 31,774 against AbstractRetrieval's 10,000/week -- which
# would have taken over three weeks.
DOI_BATCH_SIZE: Final[int] = 25
REQUEST_SLEEP_SECONDS: Final[float] = 0.15
MAX_RETRIES_PER_BATCH: Final[int] = 3
CHECKPOINT_EVERY_BATCHES: Final[int] = 20

KEYWORD_DIVIDING_CHARACTER: Final[str] = "|"

# Provenance values for the `keywords_source` vertex attribute.
SOURCE_SCOPUS: Final[str] = "scopus"
SOURCE_PUBMED: Final[str] = "pubmed"
SOURCE_CORE_CORPUS: Final[str] = "original_corpus"
SOURCE_NONE: Final[str] = ""

NEIGHBOR_METADATA_PATH: Final[Path] = PROCESSED_DATA_PATH / "neighbor_metadata.parquet"
PUBMED_KEYWORDS_PATH: Final[Path] = PROCESSED_DATA_PATH / "neighbor_author_keywords.parquet"
CHECKPOINT_PATH: Final[Path] = (
    PROCESSED_DATA_PATH / "neighbor_scopus_keywords_checkpoint.parquet"
)
SCOPUS_KEYWORDS_PARQUET: Final[Path] = PROCESSED_DATA_PATH / "neighbor_scopus_keywords.parquet"

LAYOUT_GRAPHML: Final[Path] = (
    GRAPH_LEVEL_DATA_PATH / "citation_network_expanded_with_layout_and_author_keywords.graphml"
)
NEIGHBOR_GRAPHML: Final[Path] = (
    GRAPH_LEVEL_DATA_PATH
    / "neighbor_communities"
    / "neighbor_citation_network_with_author_keywords.graphml"
)
OUTPUT_LAYOUT_GRAPHML: Final[Path] = (
    GRAPH_LEVEL_DATA_PATH / "citation_network_expanded_with_layout_and_scopus_keywords.graphml"
)
OUTPUT_NEIGHBOR_GRAPHML: Final[Path] = (
    GRAPH_LEVEL_DATA_PATH
    / "neighbor_communities"
    / "neighbor_citation_network_with_scopus_keywords.graphml"
)

#####################
##  Aux Functions  ##
#####################


def _proxies(proxy_url: str) -> dict | None:
    return {"http": proxy_url, "https": proxy_url} if proxy_url else None


def _verify_proxy_exit_ip(
    proxy_url: str, expected_prefixes: tuple[str, ...] = JHU_IP_PREFIXES
) -> tuple[bool, str]:
    """Confirm the tunnel is up and exiting where we think it is.

    This guard is load-bearing, not defensive boilerplate. Without the tunnel
    Elsevier still answers HTTP 200 -- it just omits `authkeywords` silently,
    which is indistinguishable from a paper that genuinely has none. A dropped
    tunnel would therefore bank 31,774 false negatives that look exactly like
    real data, and nothing downstream could tell the difference.
    """
    if not proxy_url:
        return False, "INSTITUTIONAL_PROXY_URL is not set"
    try:
        response = requests.get(
            "https://api.ipify.org", proxies=_proxies(proxy_url), timeout=30
        )
    except Exception as error:  # noqa: BLE001
        return False, f"proxy unreachable: {type(error).__name__}"
    exit_ip = response.text.strip()
    if not exit_ip.startswith(expected_prefixes):
        return False, f"tunnel exits from {exit_ip}, not an institutional range"
    return True, exit_ip


def _batch_query(dois: list[str]) -> str:
    """Quoting is required, not stylistic: 9.2% of our neighbour DOIs are
    Wiley SICI-style containing parentheses and angle brackets, e.g.
    `10.1002/(sici)1096-9861(19971020)387:2<167::aid-cne1>3.0.co;2-z`. Unquoted,
    Scopus parses those parentheses as boolean-query grouping and the batch
    silently returns the wrong papers."""
    return " OR ".join(f'DOI("{doi}")' for doi in dois)


def _doi_batches(dois: list[str], batch_size: int) -> list[list[str]]:
    return [dois[i : i + batch_size] for i in range(0, len(dois), batch_size)]


def _keywords_from_entry(entry: dict) -> str:
    """Normalise Scopus's two shapes for `authkeywords` into one `|`-joined
    string. The Search API returns a pre-joined string; AbstractRetrieval
    returns a list of `{"$": ...}` dicts (and a bare dict when there is
    exactly one). Casing is deliberately left alone -- Scopus's Title case
    already matches the core corpus's own keyword convention."""
    raw = entry.get("authkeywords")
    if not raw:
        return ""
    if isinstance(raw, dict):
        items = raw.get("author-keyword", [])
        if isinstance(items, dict):
            items = [items]
        values = [item.get("$", "").strip() for item in items]
    else:
        values = [part.strip() for part in str(raw).split(KEYWORD_DIVIDING_CHARACTER)]
    return KEYWORD_DIVIDING_CHARACTER.join(value for value in values if value)


def _load_checkpoint(checkpoint_path: Path) -> dict[str, str]:
    if not Path(checkpoint_path).exists():
        return {}
    df = pd.read_parquet(checkpoint_path)
    return dict(zip(df["doi"], df["scopus_keywords"].fillna("")))


def _save_checkpoint(checkpoint_path: Path, found: dict[str, str], queried: set[str]) -> None:
    """Persist confirmed-empties alongside hits. Without the empties, a resume
    would re-query every keyword-less paper on every run and the fetch would
    never converge."""
    rows = [{"doi": doi, "scopus_keywords": found.get(doi, "")} for doi in sorted(queried)]
    pd.DataFrame(rows).to_parquet(checkpoint_path, index=False)


def _fetch_scopus_keywords(
    dois: list[str], checkpoint_path: Path, proxy_url: str
) -> dict[str, str]:
    already = _load_checkpoint(checkpoint_path)
    queried = set(already)
    found = {doi: keywords for doi, keywords in already.items() if keywords}
    todo = [doi for doi in dois if doi not in queried]
    logger.info(
        f"{len(dois)} neighbour DOIs; {len(queried)} already queried "
        f"({len(found)} with keywords); {len(todo)} remaining."
    )

    # Nothing left to fetch means nothing to authorise: a fully-checkpointed
    # rerun is pure local file work, so it must not demand a live tunnel. The
    # tunnel's DSID cookie expires after a few hours, and requiring it here
    # would make every later rerun -- re-deriving the graphs, changing how the
    # sources are combined -- hostage to a credential that has nothing to do
    # with the work being done.
    if not todo:
        logger.info("Every DOI is already checkpointed; skipping Scopus entirely.")
        return found

    ok, detail = _verify_proxy_exit_ip(proxy_url)
    if not ok:
        raise RuntimeError(
            f"Institutional tunnel not usable ({detail}), and {len(todo)} DOIs "
            "still need fetching. Scopus gates authkeywords by IP; without the "
            "tunnel they would all come back silently keyword-less. Bring the "
            "tunnel up and set INSTITUTIONAL_PROXY_URL."
        )
    logger.info(f"Tunnel verified, exiting from {detail}.")

    headers = {"X-ELS-APIKey": SCOPUS_API_KEY, "Accept": "application/json"}
    proxies = _proxies(proxy_url)
    batches = _doi_batches(todo, DOI_BATCH_SIZE)
    for batch_index, batch in enumerate(batches, start=1):
        for attempt in range(1, MAX_RETRIES_PER_BATCH + 1):
            try:
                response = requests.get(
                    SCOPUS_SEARCH_URL,
                    headers=headers,
                    params={
                        "query": _batch_query(batch),
                        "view": "COMPLETE",
                        "count": DOI_BATCH_SIZE,
                    },
                    proxies=proxies,
                    timeout=90,
                )
                if response.status_code != 200:
                    raise RuntimeError(
                        f"HTTP {response.status_code}: {response.text[:200]}"
                    )
                for entry in response.json()["search-results"].get("entry", []):
                    doi = (entry.get("prism:doi") or "").strip().lower()
                    keywords = _keywords_from_entry(entry)
                    if doi and keywords:
                        found[doi] = keywords
                # Mark the whole batch queried, including DOIs Scopus does not
                # index at all -- otherwise they are retried on every resume.
                queried.update(batch)
                break
            except Exception as error:  # noqa: BLE001
                logger.warning(
                    f"Batch {batch_index}/{len(batches)} attempt {attempt}: {error}"
                )
                if attempt == MAX_RETRIES_PER_BATCH:
                    logger.error(
                        f"Batch {batch_index} failed permanently ({len(batch)} DOIs skipped)."
                    )
                else:
                    time.sleep(REQUEST_SLEEP_SECONDS * 4 * attempt)

        if batch_index % CHECKPOINT_EVERY_BATCHES == 0 or batch_index == len(batches):
            _save_checkpoint(checkpoint_path, found, queried)
            logger.info(
                f"Batch {batch_index}/{len(batches)} | queried {len(queried)} | "
                f"with keywords {len(found)} "
                f"({len(found) / max(len(queried), 1) * 100:.1f}%)"
            )
        time.sleep(REQUEST_SLEEP_SECONDS)

    _save_checkpoint(checkpoint_path, found, queried)
    return found


def _combined_keywords_df(
    neighbor_metadata_df: pd.DataFrame,
    scopus_keywords_by_doi: dict[str, str],
    pubmed_keywords_df: pd.DataFrame,
) -> pd.DataFrame:
    """One row per neighbour DOI, with Scopus preferred and PubMed as the
    fallback. Preference order is not arbitrary: Scopus covers ~4.5x more
    papers and shares the core corpus's surface form, so using it first both
    maximises coverage and minimises how many rows carry the odd formatting.
    """
    df = neighbor_metadata_df[["doi"]].copy()
    df["doi"] = df["doi"].astype(str).str.strip().str.lower()

    pubmed = pubmed_keywords_df[["doi", "author_keywords"]].copy()
    pubmed["doi"] = pubmed["doi"].astype(str).str.strip().str.lower()
    pubmed = pubmed.rename(columns={"author_keywords": "pubmed_keywords"})
    pubmed = pubmed.drop_duplicates(subset="doi")

    df = df.merge(pubmed, on="doi", how="left")
    df["pubmed_keywords"] = df["pubmed_keywords"].fillna("")
    df["scopus_keywords"] = df["doi"].map(scopus_keywords_by_doi).fillna("")

    has_scopus = df["scopus_keywords"] != ""
    has_pubmed = df["pubmed_keywords"] != ""
    df["author_keywords"] = df["pubmed_keywords"].where(~has_scopus, df["scopus_keywords"])
    df["keywords_source"] = SOURCE_NONE
    df.loc[has_pubmed & ~has_scopus, "keywords_source"] = SOURCE_PUBMED
    df.loc[has_scopus, "keywords_source"] = SOURCE_SCOPUS

    return df[["doi", "scopus_keywords", "pubmed_keywords", "author_keywords", "keywords_source"]]


def _apply_keywords_to_graph(
    graph: ig.Graph,
    author_keywords_by_doi: dict[str, str],
    keywords_source_by_doi: dict[str, str],
) -> ig.Graph:
    """Overwrite `keywords` on neighbour vertices with the combined value and
    record provenance in `keywords_source`. Core vertices keep their existing
    `keywords` (already genuine author keywords from the source databases) and
    are tagged `original_corpus`.

    Missing values are "" rather than None throughout: igraph's GraphML writer
    stringifies a bare Python None in a *string* vertex attribute as the
    literal text "None" (unlike float NaN, which round-trips as a real missing
    value), so a None here would surface as visible "None" labels in Gephi.
    """
    has_core = "is_original_node" in graph.vs.attributes()
    is_neighbor = (
        [value is False for value in graph.vs["is_original_node"]]
        if has_core
        else [True] * graph.vcount()
    )
    existing_keywords = graph.vs["keywords"]
    names = graph.vs["name"]

    graph.vs["keywords"] = [
        author_keywords_by_doi.get(names[index], "")
        if is_neighbor[index]
        else existing_keywords[index]
        for index in range(graph.vcount())
    ]
    graph.vs["keywords_source"] = [
        keywords_source_by_doi.get(names[index], SOURCE_NONE)
        if is_neighbor[index]
        else SOURCE_CORE_CORPUS
        for index in range(graph.vcount())
    ]
    return graph


##################
##     Main     ##
##################


def _main() -> int:
    inputs = dict(
        neighbor_metadata_path=NEIGHBOR_METADATA_PATH,
        pubmed_keywords_path=PUBMED_KEYWORDS_PATH,
        checkpoint_path=CHECKPOINT_PATH,
        proxy_url=INSTITUTIONAL_PROXY_URL,
        scopus_keywords_parquet_path=SCOPUS_KEYWORDS_PARQUET,
        layout_graphml_path=LAYOUT_GRAPHML,
        neighbor_graphml_path=NEIGHBOR_GRAPHML,
        output_layout_graphml_path=OUTPUT_LAYOUT_GRAPHML,
        output_neighbor_graphml_path=OUTPUT_NEIGHBOR_GRAPHML,
    )
    outputs = [
        "save_scopus_keywords_parquet",
        "save_layout_graph_with_scopus_keywords",
        "save_neighbor_graph_with_scopus_keywords",
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


@dataloader()
def pubmed_keywords_df(pubmed_keywords_path: Path) -> tuple[pd.DataFrame, dict]:
    df = pd.read_parquet(pubmed_keywords_path)
    return df, utils.get_file_metadata(pubmed_keywords_path)


def neighbor_dois(neighbor_metadata_df: pd.DataFrame) -> list[str]:
    """Lowercased, because Scopus echoes `prism:doi` back in its own casing;
    without normalising, a case difference would file the answer under a key
    that never matches the DOI we asked about and the paper would look
    keyword-less."""
    return (
        neighbor_metadata_df["doi"]
        .dropna()
        .astype(str)
        .str.strip()
        .str.lower()
        .unique()
        .tolist()
    )


def scopus_keywords_by_doi(
    neighbor_dois: list[str], checkpoint_path: Path, proxy_url: str
) -> dict[str, str]:
    result = _fetch_scopus_keywords(neighbor_dois, checkpoint_path, proxy_url)
    logger.info(
        f"Scopus author keywords for {len(result)}/{len(neighbor_dois)} neighbours "
        f"({len(result) / max(len(neighbor_dois), 1) * 100:.1f}%)."
    )
    return result


def combined_keywords_df(
    neighbor_metadata_df: pd.DataFrame,
    scopus_keywords_by_doi: dict[str, str],
    pubmed_keywords_df: pd.DataFrame,
) -> pd.DataFrame:
    df = _combined_keywords_df(neighbor_metadata_df, scopus_keywords_by_doi, pubmed_keywords_df)
    counts = df["keywords_source"].value_counts().to_dict()
    logger.info(f"Keyword provenance across {len(df)} neighbours: {counts}")
    return df


def author_keywords_by_doi(combined_keywords_df: pd.DataFrame) -> dict[str, str]:
    return combined_keywords_df.set_index("doi")["author_keywords"].to_dict()


def keywords_source_by_doi(combined_keywords_df: pd.DataFrame) -> dict[str, str]:
    return combined_keywords_df.set_index("doi")["keywords_source"].to_dict()


@datasaver()
def save_scopus_keywords_parquet(
    combined_keywords_df: pd.DataFrame, scopus_keywords_parquet_path: Path
) -> dict:
    combined_keywords_df.to_parquet(scopus_keywords_parquet_path, index=False)
    return utils.get_file_metadata(scopus_keywords_parquet_path)


@dataloader()
def layout_graph(layout_graphml_path: Path) -> tuple[ig.Graph, dict]:
    graph = ig.Graph.Read_GraphML(str(layout_graphml_path))
    return graph, utils.get_file_metadata(layout_graphml_path)


@dataloader()
def neighbor_graph(neighbor_graphml_path: Path) -> tuple[ig.Graph, dict]:
    graph = ig.Graph.Read_GraphML(str(neighbor_graphml_path))
    return graph, utils.get_file_metadata(neighbor_graphml_path)


def layout_graph_with_scopus_keywords(
    layout_graph: ig.Graph,
    author_keywords_by_doi: dict[str, str],
    keywords_source_by_doi: dict[str, str],
) -> ig.Graph:
    return _apply_keywords_to_graph(
        layout_graph, author_keywords_by_doi, keywords_source_by_doi
    )


def neighbor_graph_with_scopus_keywords(
    neighbor_graph: ig.Graph,
    author_keywords_by_doi: dict[str, str],
    keywords_source_by_doi: dict[str, str],
) -> ig.Graph:
    return _apply_keywords_to_graph(
        neighbor_graph, author_keywords_by_doi, keywords_source_by_doi
    )


@datasaver()
def save_layout_graph_with_scopus_keywords(
    layout_graph_with_scopus_keywords: ig.Graph, output_layout_graphml_path: Path
) -> dict:
    layout_graph_with_scopus_keywords.write(output_layout_graphml_path)
    return utils.get_file_metadata(output_layout_graphml_path)


@datasaver()
def save_neighbor_graph_with_scopus_keywords(
    neighbor_graph_with_scopus_keywords: ig.Graph, output_neighbor_graphml_path: Path
) -> dict:
    neighbor_graph_with_scopus_keywords.write(output_neighbor_graphml_path)
    return utils.get_file_metadata(output_neighbor_graphml_path)


if __name__ == "__main__":
    sys.exit(_main())
