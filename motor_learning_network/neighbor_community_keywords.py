"""Top-differentiating-keywords-per-community naming for the neighbor-only
Leiden/CPM communities, replicating the "modified TF-IDF" methodology from
`find_keywords_per_cluster_noverlap.py` (frozen, not modified here) -- each
community's aggregated (synonym-canonicalized) keyword list is treated as one
"document"; a `TfidfVectorizer` is fit across all of a resolution's community
documents; a corrected IDF (`_correct_tfidf`, which strips sklearn's `+1`
smoothing constant) re-weights the raw TF-IDF scores; the top-N keywords per
community by corrected score become that community's name.

Two deliberate departures from the original script, both scoped to what this
task actually needs (attaching community names as data, not rendering plots):

  - Only the text-computation chain is replicated (filter -> canonical
    corpus -> TF-IDF -> top keywords). The original's Voronoi/word-cloud
    plotting machinery (`_aggregate_top_scores`'s caller, `save_combined_plot`,
    `save_wordcloud_figure`, ...) is unrelated to naming communities as graph
    data and is not needed here.
  - Communities are included once they reach `SUBSTANTIVE_COMMUNITY_MIN_SIZE`
    (`community_quality_metrics.py` -- "the cutoff the website already uses
    to decide which communities are worth naming"), not the original's
    `_modularity_meta`/`range(50)` community-id cap, which is an artifact of
    that script's core-corpus community ids happening to fit under 50 and
    would silently drop legitimate neighbor communities with higher ids.

Scoped to the same 7-resolution plateau as `neighbor_community_quality_metrics.py`
(RESOLUTIONS = [0.003..0.009]), reusing that module's `community_size_at_res=<r>`
column already computed there rather than recomputing sizes.

Output is a PER-VERTEX attribute (`top_keywords_at_res=<r>`, same string
broadcast to every vertex in a community) rather than a graph-level one --
Gephi's GraphML importer has no data model for graph-level attributes (see
[[expand-citation-network-neighbors-workstream]] memory), so a graph-level
"community -> keywords" map would be silently dropped on import.

Names are computed over GENUINE author keywords (Scopus, with PubMed as
fallback -- 54.4% of neighbours), so they are the same kind of object as the
core corpus's own community names. Coverage is partial but sufficient: every
community at or above `SUBSTANTIVE_COMMUNITY_MIN_SIZE` still has at least 3
keyworded members (median 53% of a community's members carry keywords).

Outputs (data/graph_level_data/neighbor_communities/):
  neighbor_citation_network_with_author_keyword_names.graphml
      neighbor_citation_network_with_scopus_keywords.graphml plus
      `top_keywords_at_res=<r>` vertex columns for each of the 7 resolutions.
  community_author_keywords_per_resolution.parquet
      one row per (resolution, community_id) with its size and top keywords,
      for quick human review without opening the graphml.
"""

import json
import logging
from pathlib import Path
import re
import sys
from typing import Final

from hamilton import driver
from hamilton.function_modifiers import dataloader, datasaver, group, parameterize, source, value
from hamilton.io import utils
import hamilton.log_setup
from hamilton_sdk import adapters
import igraph as ig
import numpy as np
import pandas as pd
import scipy.sparse
from sklearn.feature_extraction.text import TfidfVectorizer

from motor_learning_network.community_quality_metrics import SUBSTANTIVE_COMMUNITY_MIN_SIZE
from motor_learning_network.constants import (
    DEFAULT_UI_PROJECT_ID,
    DEFAULT_UI_USERNAME,
    FIGURES_PATH,
    GRAPH_LEVEL_DATA_PATH,
    RAW_DATA_PATH,
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

SYNONYMS_THRESHOLD: Final = 0.99
NORM: Final = "l2"
IDF_BIAS: Final = 0.0
TOP_N_KEYWORDS: Final[int] = 3
KEYWORD_DIVIDING_CHARACTER: Final[str] = "|"
MIN_COMMUNITY_SIZE: Final[int] = SUBSTANTIVE_COMMUNITY_MIN_SIZE

_res_node_names: Final[list[str]] = [f"res_{str(r).replace('.', '_')}" for r in RESOLUTIONS]

OUTPUT_DIR: Final[Path] = GRAPH_LEVEL_DATA_PATH / "neighbor_communities"
# Reads the graph carrying REAL author keywords (Scopus, PubMed fallback),
# not the bare community-metrics graph. Until 2026-09-21 the `keywords`
# attribute on the neighbour graphs silently held OpenAlex's algorithmic Topic
# labels, so the community names this module produced were TF-IDF over topics
# -- a different kind of label from the core corpus's author-supplied
# keywords, and not comparable with it. See fetch_neighbor_scopus_keywords.py.
INPUT_GRAPHML: Final[Path] = OUTPUT_DIR / "neighbor_citation_network_with_scopus_keywords.graphml"
SYNONYM_DICT_PATH: Final[Path] = (
    RAW_DATA_PATH / f"keyword_synonyms_{SYNONYMS_THRESHOLD}_with_transitivity.json"
)
# Distinct from the superseded topic-based artifacts
# (neighbor_citation_network_with_community_keywords.graphml /
# community_keywords_per_resolution.parquet), which are kept so the two
# namings can be compared rather than silently replaced.
OUTPUT_GRAPHML: Final[Path] = (
    OUTPUT_DIR / "neighbor_citation_network_with_author_keyword_names.graphml"
)
COMMUNITY_KEYWORDS_PARQUET: Final[Path] = (
    OUTPUT_DIR / "community_author_keywords_per_resolution.parquet"
)

#####################
##  Aux Functions  ##
#####################


def _normalize_keyword(keyword: str) -> str:
    return keyword.lower()


def _correct_tfidf(X: scipy.sparse.csr_matrix, vectorizer: TfidfVectorizer) -> scipy.sparse.csr_matrix:
    """Strip sklearn's `+1` IDF smoothing constant -- see
    find_keywords_per_cluster_noverlap.py's `_correct_tfidf` (replicated here
    rather than imported to avoid pulling that module's plotting dependencies
    into this DAG)."""
    X_array = X.toarray()
    wrong_idf = vectorizer.idf_
    corrected_idf = wrong_idf - 1.0 + IDF_BIAS
    tf = np.divide(X_array, wrong_idf)
    return scipy.sparse.csr_matrix(np.multiply(tf, corrected_idf))


def _build_synonym_map(synonym_dict: dict) -> dict[str, str]:
    canonical_map: dict[str, str] = {}
    for key, values in synonym_dict.items():
        canonical_name = _normalize_keyword(key)
        for variant in [key] + values:
            norm_variant = _normalize_keyword(variant)
            if norm_variant not in canonical_map:
                canonical_map[norm_variant] = canonical_name
    return canonical_map


def _filtered_keywords_df(
    graph: ig.Graph,
    resolution: float,
    keyword_dividing_character: str,
    min_community_size: int,
) -> pd.DataFrame:
    """One row per vertex in a community at/above `min_community_size`, with
    its keyword list split on `keyword_dividing_character` and then further
    on stray `&`/`,` separators (matches the original script's splitting)."""
    community_col = f"cpm_communities_at_res={resolution}"
    size_col = f"community_size_at_res={resolution}"
    df = pd.DataFrame(
        {
            "keywords": graph.vs["keywords"],
            "community_id": graph.vs[community_col],
            "community_size": graph.vs[size_col],
        }
    )
    df = df[df["community_size"] >= min_community_size].reset_index(drop=True)
    df["keywords"] = df["keywords"].fillna("").str.split(keyword_dividing_character)
    df["keywords"] = df["keywords"].apply(
        lambda kws: [
            part.strip()
            for k in kws
            for part in re.split(r"\s*[&,]\s*", k)
            if part and part.strip()
        ]
    )
    return df


def _canonical_corpus(filtered_keywords_df: pd.DataFrame, synonym_map: dict) -> dict[int, str]:
    """{community_id: "kw1\\tkw2\\t..."} -- every raw keyword rewritten to its
    canonical synonym, then joined per community into one tab-separated
    "document" for TF-IDF."""
    corpus: dict[int, list[str]] = {}
    for keywords, community_id in zip(
        filtered_keywords_df["keywords"], filtered_keywords_df["community_id"]
    ):
        bucket = corpus.setdefault(community_id, [])
        for raw_term in keywords:
            norm_term = _normalize_keyword(raw_term)
            canonical_term = synonym_map.get(norm_term, norm_term)
            bucket.append(canonical_term)
    return {community_id: "\t".join(terms) for community_id, terms in sorted(corpus.items())}


def _top_keywords_per_community(corpus: dict[int, str], top_n: int) -> dict[int, str]:
    """{community_id: "Keyword A; Keyword B; Keyword C"} -- the top-N keywords
    by corrected TF-IDF score, empty-document communities dropped."""
    community_ids = [community_id for community_id, doc in corpus.items() if doc]
    documents = [corpus[community_id] for community_id in community_ids]
    if not documents:
        return {}

    vectorizer = TfidfVectorizer(
        tokenizer=lambda x: x.split("\t"), token_pattern=None, lowercase=False, norm=NORM
    )
    X = vectorizer.fit_transform(documents)
    X = _correct_tfidf(X, vectorizer)
    feature_names = vectorizer.get_feature_names_out()

    result: dict[int, str] = {}
    for i, community_id in enumerate(community_ids):
        scores = pd.Series(X[i].toarray().flatten(), index=feature_names)
        scores = scores[scores > 0].sort_values(ascending=False)
        top_terms = [keyword.title() for keyword in scores.head(top_n).index]
        result[community_id] = "; ".join(top_terms)
    return result


def _attach_top_keywords(
    graph: ig.Graph, resolution: float, top_keywords_by_community: dict[int, str]
) -> ig.Graph:
    """Missing communities get "" rather than None: igraph's GraphML writer
    stringifies a Python None in a string attribute as the literal text
    "None" instead of omitting it (unlike float NaN, which round-trips as a
    real missing value), so None would show up as visible text in Gephi."""
    community_col = f"cpm_communities_at_res={resolution}"
    attr_col = f"top_keywords_at_res={resolution}"
    graph.vs[attr_col] = [
        top_keywords_by_community.get(community_id, "") for community_id in graph.vs[community_col]
    ]
    return graph


def _community_keywords_df(
    graph: ig.Graph, top_keywords_per_resolution: dict[float, dict[int, str]]
) -> pd.DataFrame:
    rows = []
    for resolution, top_keywords_by_community in top_keywords_per_resolution.items():
        community_col = f"cpm_communities_at_res={resolution}"
        size_col = f"community_size_at_res={resolution}"
        sizes = dict(zip(graph.vs[community_col], graph.vs[size_col]))
        for community_id, keywords in sorted(top_keywords_by_community.items()):
            rows.append(
                {
                    "resolution": resolution,
                    "community_id": community_id,
                    "community_size": sizes.get(community_id),
                    "top_keywords": keywords,
                }
            )
    return pd.DataFrame(rows)


##################
##     Main     ##
##################


def _main() -> int:
    inputs = dict(
        input_graphml_path=INPUT_GRAPHML,
        synonym_dict_path=SYNONYM_DICT_PATH,
        keyword_dividing_character=KEYWORD_DIVIDING_CHARACTER,
        min_community_size=MIN_COMMUNITY_SIZE,
        top_n_keywords=TOP_N_KEYWORDS,
        output_graphml_path=OUTPUT_GRAPHML,
        community_keywords_parquet_path=COMMUNITY_KEYWORDS_PARQUET,
    )
    outputs = ["save_neighbor_graph_with_keywords", "save_community_keywords_parquet"]

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
def neighbor_graph(input_graphml_path: Path) -> tuple[ig.Graph, dict]:
    graph = ig.Graph.Read_GraphML(str(input_graphml_path))
    return graph, utils.get_file_metadata(input_graphml_path)


@dataloader()
def synonym_dict(synonym_dict_path: Path) -> tuple[dict, dict]:
    """Same hard-coded Purkinje Cell alias patch as
    find_keywords_per_cluster_noverlap.py's `synonym_dict` node."""
    with open(synonym_dict_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    data["Purkinje Cell"].extend(["Purkinje Cell ( PC )"])
    return data, utils.get_file_metadata(synonym_dict_path)


def synonym_map(synonym_dict: dict) -> dict[str, str]:
    mapping = _build_synonym_map(synonym_dict)
    logger.info(f"Built synonym map with {len(mapping)} variant entries.")
    return mapping


@parameterize(
    **{
        f"filtered_keywords_df_{name}": {"resolution": value(r)}
        for name, r in zip(_res_node_names, RESOLUTIONS)
    }
)
def filtered_keywords_df(
    neighbor_graph: ig.Graph,
    resolution: float,
    keyword_dividing_character: str,
    min_community_size: int,
) -> pd.DataFrame:
    df = _filtered_keywords_df(neighbor_graph, resolution, keyword_dividing_character, min_community_size)
    logger.info(
        f"[res={resolution}] {len(df)} vertices in communities >= {min_community_size}, "
        f"{df['community_id'].nunique()} communities."
    )
    return df


@parameterize(
    **{
        f"canonical_corpus_{name}": {"filtered_keywords_df": source(f"filtered_keywords_df_{name}")}
        for name in _res_node_names
    }
)
def canonical_corpus(filtered_keywords_df: pd.DataFrame, synonym_map: dict) -> dict[int, str]:
    return _canonical_corpus(filtered_keywords_df, synonym_map)


@parameterize(
    **{
        f"top_keywords_by_community_{name}": {"canonical_corpus": source(f"canonical_corpus_{name}")}
        for name in _res_node_names
    }
)
def top_keywords_by_community(canonical_corpus: dict[int, str], top_n_keywords: int) -> dict[int, str]:
    return _top_keywords_per_community(canonical_corpus, top_n_keywords)


@parameterize(
    top_keywords_per_resolution={
        "results": group(
            *[source(f"top_keywords_by_community_{name}") for name in _res_node_names]
        )
    }
)
def top_keywords_per_resolution(results: list[dict[int, str]]) -> dict[float, dict[int, str]]:
    return dict(zip(RESOLUTIONS, results))


def neighbor_graph_with_keywords(
    neighbor_graph: ig.Graph, top_keywords_per_resolution: dict[float, dict[int, str]]
) -> ig.Graph:
    for resolution, top_keywords_by_community in top_keywords_per_resolution.items():
        neighbor_graph = _attach_top_keywords(neighbor_graph, resolution, top_keywords_by_community)
    return neighbor_graph


def community_keywords_df(
    neighbor_graph: ig.Graph, top_keywords_per_resolution: dict[float, dict[int, str]]
) -> pd.DataFrame:
    return _community_keywords_df(neighbor_graph, top_keywords_per_resolution)


@datasaver()
def save_neighbor_graph_with_keywords(
    neighbor_graph_with_keywords: ig.Graph, output_graphml_path: Path
) -> dict:
    neighbor_graph_with_keywords.write(output_graphml_path)
    return utils.get_file_metadata(output_graphml_path)


@datasaver()
def save_community_keywords_parquet(
    community_keywords_df: pd.DataFrame, community_keywords_parquet_path: Path
) -> dict:
    community_keywords_df.to_parquet(community_keywords_parquet_path)
    return utils.get_file_metadata(community_keywords_parquet_path)


if __name__ == "__main__":
    sys.exit(_main())
