import igraph as ig
import re
import sys
import json
import logging
from pathlib import Path
from typing import Final

import numpy as np
import pandas as pd
import scipy.sparse
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sklearn.feature_extraction.text import TfidfVectorizer

from hamilton.function_modifiers import (
    dataloader,
    datasaver,
    value,
    source,
    parameterize,
)
from hamilton.io import utils
from hamilton_sdk import adapters
from hamilton import driver
import hamilton.log_setup

from motor_learning_network.constants import (
    GRAPH_LEVEL_DATA_PATH,
    KEYWORDS_LEVEL_DATA_PATH,
    RAW_DATA_PATH,
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
if EXECUTE:
    logger.info("Executing the DAG!")

# ── Resolution sweep ──────────────────────────────────────────────────────────
YEAR = 1960
TD_IDF_SAVING_PATH = KEYWORDS_LEVEL_DATA_PATH / f"until_{YEAR}_test"
TD_IDF_SAVING_PATH.mkdir(parents=True,exist_ok=True)
RESOLUTIONS: list[float] = [0.005]
# RESOLUTIONS: list[float] = [round(0.001 + i * 0.001, 3) for i in range(1, 9)]
# e.g. [0.002, 0.003, ..., 0.009]

# ── Per-resolution output directory helper ────────────────────────────────────
NORM: Final = "l2"
IDF_BIAS: Final = 0.0
SYNONYMS_THRESHOLD: Final = 0.99

def _out_dir(resolution: float) -> Path:
    d = (
        TD_IDF_SAVING_PATH / "td-df-per-cluster-as-document"
        / f"res-{resolution}-threshold-{SYNONYMS_THRESHOLD}-norm-{NORM}-fixidf-{IDF_BIAS}"
    )
    d.mkdir(parents=True, exist_ok=True)
    return d

def _modularity_meta(resolution: float) -> dict:
    return {
        float(i): {"label": f"Community {i} at resolution {resolution}", "color": "#AAAAAA"}
        for i in range(50)
    }

# Parameterize node names per resolution
_res_node_names = [f"res_{str(r).replace('.', '_')}" for r in RESOLUTIONS]


##################
##     Main     ##
##################

def _main() -> int:
    ########################
    ##  UI configuration  ##
    ########################
    UI_CONFIG = adapters.HamiltonTracker(
        project_id=DEFAULT_UI_PROJECT_ID,
        username=DEFAULT_UI_USERNAME,
        dag_name=CURRENT_FILE_NAME,
        tags={"environment": "DEV", "team": TEAM_NAME, "version": "0.1"},
    )

    ########################
    ## Inputs and Outputs ##
    ########################
    inputs = dict(
        resolutions=RESOLUTIONS,
        norm=NORM,
        idf_bias=IDF_BIAS,
        synonyms_threshold=SYNONYMS_THRESHOLD,
        citation_network_path=GRAPH_LEVEL_DATA_PATH/"citation_network_until_1960.graphml",
        synonym_dict_path=RAW_DATA_PATH/f"keyword_synonyms_{SYNONYMS_THRESHOLD}_with_transitivity.json",
        top_n_histogram=40,
        keyword_dividing_character="|"
    )

    outputs = [f"save_combined_plot_{name}" for name in _res_node_names]

    import __main__

    dr = (
        driver.Builder()
        .with_modules(__main__)
        # .with_config()
        # .with_cache()
        .with_adapters(UI_CONFIG)
        .build()
    )

    #######################
    ##   Sanity checks   ##
    #######################
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

    ###################
    ##   Execution   ##
    ###################
    if EXECUTE:
        dr.execute(outputs, inputs=inputs)
    return 0

#####################
##  Aux Functions  ##
#####################

def _normalize_keyword(keyword: str) -> str:
    return keyword.lower()

def _split_keywords(keywords_value):
    """Split a keyword string on commas that are not inside parentheses/brackets."""
    if keywords_value is None or (isinstance(keywords_value, float) and pd.isna(keywords_value)):
        return []
    s = str(keywords_value).strip()
    if not s:
        return []

    parts, current = [], []
    paren_depth = bracket_depth = 0
    for char in s:
        if char == "(":
            paren_depth += 1
            current.append(char)
        elif char == ")":
            paren_depth -= 1
            current.append(char)
        elif char == "[":
            bracket_depth += 1
            current.append(char)
        elif char == "]":
            bracket_depth -= 1
            current.append(char)
        elif char == "," and paren_depth == 0 and bracket_depth == 0:
            part = "".join(current).strip()
            if part:
                parts.append(part)
            current = []
        else:
            current.append(char)
    if current:
        part = "".join(current).strip()
        if part:
            parts.append(part)
    return parts


def _correct_tfidf(
    X: scipy.sparse.csr_matrix, vectorizer: TfidfVectorizer
) -> scipy.sparse.csr_matrix:
    X_array = X.toarray()
    wrong_idf = vectorizer.idf_
    corrected_idf = wrong_idf - 1.0 + IDF_BIAS
    tf = np.divide(X_array, wrong_idf)
    return scipy.sparse.csr_matrix(np.multiply(tf, corrected_idf))


def _sanitize_filename(name: str) -> str:
    name = str(name).replace("\n", " ")
    name = re.sub(r'[\\/*?:"<>|]', "", name)
    return re.sub(r"\s+", "_", name).strip()


def _aggregate_top_scores(
    X: scipy.sparse.csr_matrix,
    vectorizer: TfidfVectorizer,
    cluster_ids: list,
    modularity_meta: dict,
    top_n: int = 3,
) -> list[dict]:
    feature_names = vectorizer.get_feature_names_out()
    combined = []
    for i, cluster_id in enumerate(cluster_ids):
        meta = modularity_meta.get(cluster_id, {})
        display_label = meta.get("label", f"Cluster {cluster_id}")
        plot_color = meta.get("color", "#1f77b4")
        cluster_vector = X[i].toarray().flatten()
        scores = pd.Series(cluster_vector, index=feature_names)
        scores = scores[scores > 0].sort_values(ascending=False)
        for rank, (keyword, score) in enumerate(scores.head(top_n).items()):
            combined.append(
                {
                    "cluster_id": cluster_id,
                    "cluster_label": display_label.replace("\n", " "),
                    "cluster_color": plot_color,
                    "keyword": keyword.title(),
                    "score": score,
                    "rank": rank + 1,
                }
            )
    return combined

#########################
##    DAG Definition   ##
#########################

# ── 1. Load raw data ──────────────────────────────────────────────────────────

@dataloader()
def citation_network(citation_network_path: Path) -> tuple[ig.Graph, dict]:
    citation_network = ig.Graph.Read(citation_network_path)
    metadata = utils.get_file_metadata(citation_network_path)
    return citation_network, metadata

@dataloader()
def synonym_dict(synonym_dict_path: Path) -> tuple[dict, dict]:
    """Load the synonym dictionary JSON and patch in the Purkinje Cell alias."""
    with open(synonym_dict_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # Hard-coded patch from original script
    data["Purkinje Cell"].extend(["Purkinje Cell ( PC )"])
    metadata = utils.get_file_metadata(synonym_dict_path)
    return data, metadata


# ── 2. Build canonical synonym map (shared across resolutions) ────────────────

def synonym_map(synonym_dict: dict) -> dict[str, str]:
    """
    Build a flat map from every variant (normalised) → canonical form (normalised).

    The dictionary key is chosen as the canonical name for its group.
    """
    canonical_map: dict[str, str] = {}
    for key, values in synonym_dict.items():
        canonical_name = _normalize_keyword(key)
        for variant in [key] + values:
            norm_variant = _normalize_keyword(variant)
            if norm_variant not in canonical_map:
                canonical_map[norm_variant] = canonical_name
    logger.info(f"Built synonym map with {len(canonical_map)} variant entries.")
    return canonical_map


# ── 3. Per-resolution nodes ───────────────────────────────────────────────────
#
# For each resolution we produce four nodes:
#   filtered_df_<res>          – DataFrame filtered to that resolution's communities
#   canonical_corpus_<res>     – {cluster_id: tab-joined canonical keyword string}
#   tfidf_matrix_<res>         – (X, vectorizer, cluster_ids, qa_log)
#   save_combined_plot_<res>   – side-effecting datasaver; returns metadata dict
#
# The @parameterize decorator fans out one function definition into N nodes.

@parameterize(**{f"filtered_df_{name}": {"resolution": value(res)} for name, res in zip(_res_node_names, RESOLUTIONS)})
def filtered_df(
    citation_network: ig.Graph,
    resolution: float,
    keyword_dividing_character: str,
) -> pd.DataFrame:
    """Filter the dataframe to rows that belong to communities defined for this resolution."""
    community_col = f"cpm_communities_at_res={resolution}"
    
    # df["keywords"] = df["keywords"].fillna("").str.split(keyword_dividing_character)
    # # df["keywords"] = df["keywords"].apply(lambda kws: [k.strip() for k in kws if k and k.strip()]).tolist()
    # df["keywords"] = df["keywords"].apply(lambda kws: [
    #     part.strip()
    #     for k in kws
    #     for part in re.split(r'\s*[&,]\s*', k)
    #     if part and part.strip()
    # ]).tolist()

    df = pd.DataFrame({attr: citation_network.vs[attr] for attr in ["keywords",community_col]})
    df["keywords"] = df["keywords"].fillna("").str.split(keyword_dividing_character)
    orig = df["keywords"].tolist()
    df["keywords"] = (
        df["keywords"]
        .apply(lambda kws: [
            part.strip()
            for k in kws
            for part in re.split(r'\s*[&,]\s*', k)
            if part and part.strip()
        ]).tolist()
    )

    # compare and collect changed rows
    changes = []
    for i, (o, p) in enumerate(zip(orig, df["keywords"].astype(str).tolist())):
        if o != [''] and o != p:
            changes.append((i, o, p))

    # write a simple text report
    out_path = Path(TD_IDF_SAVING_PATH/"keyword_changes.txt")
    with out_path.open("w", encoding="utf-8") as f:
        f.write("row_index\toriginal\tprocessed\n")
        for idx, o, p in changes:
            f.write(f"{idx}\t{o}\t{p}\n")

    print(f"Saved {len(changes)} changes to {out_path}")
    modularity_meta = _modularity_meta(resolution)
    df = df.copy()
    df = df.drop(df[df["keywords"].isin(["Unknown keywords"])].index)
    df = df[df[community_col].isin(modularity_meta)]
    df = df.dropna().reset_index(drop=True)
    logger.info(
        f"[res={resolution}] Filtered dataframe: {len(df)} rows, "
        f"{df[community_col].nunique()} unique communities."
    )
    return df


@parameterize(
    **{
        f"canonical_corpus_{name}": {
            "filtered_df": source(f"filtered_df_{name}"),
            "resolution": value(res),
        }
        for name, res in zip(_res_node_names, RESOLUTIONS)
    }
)
def canonical_corpus(
    filtered_df: pd.DataFrame,
    synonym_map: dict,
    resolution: float,
) -> tuple[dict[int, str], dict[str, str]]:
    """
    Rewrite every paper's keyword list using canonical synonyms, then aggregate
    per cluster into a single tab-separated document string.

    Returns:
        corpus     – {{cluster_id: "kw1\\tkw2\\t..."}} sorted by cluster_id
        qa_log     – {{raw_term: canonical_term}} for every unique raw keyword seen
    """
    modularity_meta = _modularity_meta(resolution)
    community_col = f"cpm_communities_at_res={resolution}"
    corpus: dict[int, list[str]] = {}
    qa_log: dict[str, str] = {}
    all_canonical: set[str] = set()

    for keywords, cluster_id in zip(
        filtered_df["keywords"], filtered_df[community_col]
    ):
        if cluster_id not in corpus:
            corpus[cluster_id] = set()

        terms = tuple(keywords)
        for raw_term in terms:
            norm_term = _normalize_keyword(raw_term)
            canonical_term = synonym_map.get(norm_term, norm_term)
            corpus[cluster_id].add(canonical_term)
            all_canonical.add(canonical_term)

            if raw_term not in qa_log:
                qa_log[raw_term] = canonical_term
            elif qa_log[raw_term] != canonical_term:
                logger.warning(
                    f"Inconsistent canonical mapping for '{raw_term}': "
                    f"'{qa_log[raw_term]}' vs '{canonical_term}'"
                )

    # Stringify each cluster document
    corpus_str: dict[int, str] = {
        k: "\t".join(v) for k, v in sorted(corpus.items())
    }

    # Save QA artefacts to the output directory
    out_dir = _out_dir(resolution)

    qa_log_path = out_dir / "qa_canonical_keyword_mapping.json"
    with open(qa_log_path, "w", encoding="utf-8") as f:
        json.dump(dict(sorted(qa_log.items())), f, indent=4, ensure_ascii=False)
    logger.info(
        f"[res={resolution}] QA log saved ({len(qa_log)} unique raw keywords) → {qa_log_path}"
    )

    all_kw_path = out_dir / "all_canonical_keywords_processed.txt"
    with open(all_kw_path, "w", encoding="utf-8") as f:
        sorted_kw = sorted(all_canonical)
        f.write(f"Total unique CANONICAL keywords processed: {len(sorted_kw)}\n")
        f.write("=" * 80 + "\n\n")
        for kw in sorted_kw:
            f.write(f"{kw.title()}\n")
    logger.info(f"[res={resolution}] All-keywords QA report → {all_kw_path}")

    return corpus_str, qa_log


@parameterize(
    **{
        f"tfidf_matrix_{name}": {
            "canonical_corpus": source(f"canonical_corpus_{name}"),
            "resolution": value(res),
        }
        for name, res in zip(_res_node_names, RESOLUTIONS)
    }
)
def tfidf_matrix(
    canonical_corpus: tuple[dict[int, str], dict[str, str]],
    resolution: float,
) -> tuple[scipy.sparse.csr_matrix, TfidfVectorizer, list]:
    """
    Fit a TF-IDF vectorizer on the per-cluster canonical corpus and apply the
    IDF bias correction from the original script.

    Returns:
        X            – corrected sparse TF-IDF matrix (n_clusters × n_features)
        vectorizer   – fitted TfidfVectorizer
        cluster_ids  – ordered list of cluster IDs corresponding to X rows
    """
    corpus_str, _ = canonical_corpus
    cluster_ids = list(corpus_str.keys())
    documents = list(corpus_str.values())

    vectorizer = TfidfVectorizer(
        tokenizer=lambda x: x.split("\t"),
        token_pattern=None,
        lowercase=False,
        norm=NORM,
    )
    X = vectorizer.fit_transform(documents)
    X = _correct_tfidf(X, vectorizer)

    logger.info(
        f"[res={resolution}] TF-IDF matrix shape: {X.shape}  "
        f"(clusters × features)"
    )
    return X, vectorizer, cluster_ids


@datasaver()
@parameterize(
    **{
        f"save_combined_plot_{name}": {
            "tfidf_matrix": source(f"tfidf_matrix_{name}"),
            "resolution": value(res),
        }
        for name, res in zip(_res_node_names, RESOLUTIONS)
    }
)
def save_combined_plot(
    tfidf_matrix: tuple[scipy.sparse.csr_matrix, TfidfVectorizer, list],
    resolution: float,
    top_n_histogram: int,
) -> dict:
    """
    For each cluster: save a TF-IDF CSV and a horizontal-bar histogram.
    Then save a combined 'top-3 per cluster' overview plot.

    Returns file-metadata dict (datasaver contract).
    """
    X, vectorizer, cluster_ids = tfidf_matrix
    modularity_meta = _modularity_meta(resolution)
    out_dir = _out_dir(resolution)
    feature_names = vectorizer.get_feature_names_out()

    # ── Per-cluster CSV + histogram ───────────────────────────────────────────
    for i, cluster_id in enumerate(cluster_ids):
        meta = modularity_meta.get(cluster_id, {})
        display_label = meta.get("label", f"Cluster {cluster_id}")
        plot_color = meta.get("color", "#1f77b4")
        file_label = _sanitize_filename(meta.get("label", str(cluster_id)))

        cluster_vector = X[i].toarray().flatten()
        scores = pd.Series(cluster_vector, index=feature_names)
        scores = scores[scores > 0].sort_values(ascending=False)

        # CSV
        df_out = pd.DataFrame(
            {"canonical_keyword": scores.index, "tfidf_score": scores.values}
        )
        df_out["canonical_keyword"] = df_out["canonical_keyword"].str.title()
        csv_path = out_dir / f"cluster_{cluster_id}_{file_label}_tfidf_scores.csv"
        df_out.to_csv(csv_path, index=False)
        logger.info(f"[res={resolution}] Saved CSV → {csv_path.name}")

        # Histogram
        top = df_out.head(top_n_histogram)
        if top.empty:
            logger.warning(
                f"[res={resolution}] No TF-IDF scores for {display_label}, skipping plot."
            )
            continue
        labels = top["canonical_keyword"].tolist()[::-1]
        values = top["tfidf_score"].tolist()[::-1]

        plt.figure(figsize=(10, max(4, len(labels) * 0.35)))
        plt.barh(labels, values, color=plot_color)
        plt.xlabel("TF-IDF Score")
        plt.title(
            f"{display_label} - Top {top_n_histogram} Cluster-Distinguishing Keywords",
            loc="center",
        )
        plt.tight_layout()
        png_path = out_dir / f"cluster_{cluster_id}_{file_label}_tfidf_histogram.png"
        plt.savefig(png_path, dpi=150)
        plt.close()
        logger.info(f"[res={resolution}] Saved histogram → {png_path.name}")

    # ── Combined top-3 overview ───────────────────────────────────────────────
    combined_scores = _aggregate_top_scores(
        X, vectorizer, cluster_ids, modularity_meta, top_n=3
    )

    if combined_scores:
        df_combined = pd.DataFrame(combined_scores)
        df_combined["sort_label"] = (
            df_combined["cluster_label"].str.split(":").str[-1].str.strip()
        )
        df_combined = df_combined.sort_values(
            by=["sort_label", "score"], ascending=[False, True]
        ).drop(columns=["sort_label"])
        df_combined["plot_label"] = df_combined.apply(
            lambda row: f"{row['keyword']} ({row['cluster_label']})", axis=1
        )

        labels = df_combined["plot_label"].tolist()
        values = df_combined["score"].tolist()
        colors = df_combined["cluster_color"].tolist()

        legend_handles = [
            plt.Rectangle((0, 0), 1, 1, fc=modularity_meta[cid]["color"])
            for cid in sorted(modularity_meta)
            if cid in df_combined["cluster_id"].unique()
        ]
        legend_labels = [
            modularity_meta[cid]["label"].replace("\n", " ")
            for cid in sorted(modularity_meta)
            if cid in df_combined["cluster_id"].unique()
        ]

        plt.figure(figsize=(12, max(6, len(labels) * 0.4)))
        plt.barh(labels, values, color=colors)
        plt.xlabel("TF-IDF Score of cluster as a single document")
        plt.title("Top 3 Canonical Keywords by Cluster", loc="center")
        plt.legend(
            legend_handles,
            legend_labels,
            title="Cluster",
            loc="lower right",
            framealpha=0.8,
        )
        plt.tight_layout()
        combined_png = out_dir / "combined_top_3_tfidf_histogram.png"
        plt.savefig(combined_png, dpi=150)
        plt.close()
        logger.info(f"[res={resolution}] Saved combined top-3 plot → {combined_png.name}")

    metadata = utils.get_file_metadata(out_dir)
    return metadata


if __name__ == "__main__":
    sys.exit(_main())