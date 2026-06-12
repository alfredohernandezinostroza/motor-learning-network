import igraph as ig
import re
import sys
import json
import logging
from pathlib import Path
from typing import Final
from collections import defaultdict

import numpy as np
import pandas as pd
import scipy.sparse
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sklearn.feature_extraction.text import TfidfVectorizer
from fa2 import ForceAtlas2
from wordcloud import WordCloud

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
YEAR = 1990
TD_IDF_SAVING_PATH = KEYWORDS_LEVEL_DATA_PATH / f"until_{YEAR}_test_6"
TD_IDF_SAVING_PATH.mkdir(parents=True, exist_ok=True)
TOP_N_CLUSTERS = 5
RESOLUTIONS: list[float] = [0.001, 0.002]
# RESOLUTIONS: list[float] = [round(0.001 + i * 0.001, 3) for i in range(1, 9)]

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
        citation_network_path=GRAPH_LEVEL_DATA_PATH / f"citation_network_until_{YEAR}_with_layout.graphml",
        synonym_dict_path=RAW_DATA_PATH / f"keyword_synonyms_{SYNONYMS_THRESHOLD}_with_transitivity.json",
        top_n_histogram=40,
        keyword_dividing_character="|",
        min_cluster_size=3,
        # wordcloud inputs
        forceatlas2_iterations=500,
        top_n_clusters=TOP_N_CLUSTERS,
        top_n_words=20,
        wordcloud_width=400,
        wordcloud_height=400,
    )

    tfidf_outputs = [f"save_combined_plot_{name}" for name in _res_node_names]
    wordcloud_outputs = [f"save_wordcloud_figure_{name}" for name in _res_node_names]
    outputs = tfidf_outputs + wordcloud_outputs

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


def _cluster_centroids_and_radii(
    graph: ig.Graph,
    cluster_attr: str,
    scale: float = 0.45,
) -> dict[float, dict]:
    """
    For each cluster compute centroid (x, y) and a display half-width radius
    based on the 75th-percentile distance of nodes from the centroid.

    Returns { cluster_id -> {"cx": float, "cy": float, "radius": float} }
    """
    xs = np.array(graph.vs["x"], dtype=float)
    ys = np.array(graph.vs["y"], dtype=float)
    memberships = np.array(graph.vs[cluster_attr], dtype=float)

    result = {}
    for cid in np.unique(memberships):
        mask = memberships == cid
        cx, cy = float(xs[mask].mean()), float(ys[mask].mean())
        dists = np.sqrt((xs[mask] - cx) ** 2 + (ys[mask] - cy) ** 2)
        radius = float(np.percentile(dists, 75)) * scale
        result[cid] = {"cx": cx, "cy": cy, "radius": radius}
    return result


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


# ── 2. Layout (shared across all resolutions) ─────────────────────────────────

def citation_network_with_layout(
    citation_network: ig.Graph,
    forceatlas2_iterations: int,
) -> ig.Graph:
    """
    Ensure the graph has 'x' and 'y' vertex attributes for spatial positioning.

    If both attributes are already present on every vertex, the existing layout
    is reused and ForceAtlas2 is NOT run again — this avoids redundant computation
    when the graphml was saved with layout coordinates.

    If either attribute is missing or contains any null/None values, ForceAtlas2
    is run from scratch on the undirected graph and the resulting coordinates are
    stored as 'x' and 'y' vertex attributes.
    """
    attr_names = citation_network.vs.attribute_names()
    has_x = "x" in attr_names
    has_y = "y" in attr_names

    if has_x and has_y:
        x_vals = citation_network.vs["x"]
        y_vals = citation_network.vs["y"]
        any_null = any(v is None for v in x_vals) or any(v is None for v in y_vals)
        if not any_null:
            logger.info(
                f"Graph already has 'x' and 'y' vertex attributes "
                f"({citation_network.vcount()} vertices). "
                f"Skipping ForceAtlas2 layout computation."
            )
            return citation_network
        else:
            logger.warning(
                f"Graph has 'x' and 'y' attributes but {sum(v is None for v in x_vals + y_vals)} "
                f"null values were found. Re-computing ForceAtlas2 layout."
            )
    else:
        missing = [a for a in ("x", "y") if a not in attr_names]
        logger.info(
            f"Graph is missing vertex attribute(s) {missing}. "
            f"Running ForceAtlas2 ({forceatlas2_iterations} iterations) "
            f"on {citation_network.vcount()} vertices …"
        )

    forceatlas2 = ForceAtlas2(verbose=True)
    layout = forceatlas2.forceatlas2_igraph_layout(
        citation_network.as_undirected(), iterations=forceatlas2_iterations
    )
    citation_network.vs["x"] = [coord[0] for coord in layout]
    citation_network.vs["y"] = [coord[1] for coord in layout]
    logger.info(
        f"ForceAtlas2 layout computed and stored on graph "
        f"({citation_network.vcount()} vertices)."
    )
    return citation_network


# ── 3. Build canonical synonym map (shared across resolutions) ────────────────

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


# ── 4. Per-resolution nodes ───────────────────────────────────────────────────

@parameterize(
    **{
        f"filtered_df_{name}": {"resolution": value(res)}
        for name, res in zip(_res_node_names, RESOLUTIONS)
    }
)
def filtered_df(
    citation_network: ig.Graph,
    resolution: float,
    keyword_dividing_character: str,
    min_cluster_size: int,
) -> pd.DataFrame:
    """
    Extract vertex attributes from the graph into a DataFrame, split and clean
    the keyword strings, then filter to rows whose community id is defined in
    _modularity_meta for this resolution.
    """
    community_col = f"cpm_communities_at_res={resolution}"

    df = pd.DataFrame({attr: citation_network.vs[attr] for attr in ["keywords", community_col]})
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

    # Write a keyword-change QA report
    changes = [
        (i, o, p)
        for i, (o, p) in enumerate(zip(orig, df["keywords"].astype(str).tolist()))
        if o != [''] and o != p
    ]
    out_path = TD_IDF_SAVING_PATH / "keyword_changes.txt"
    with out_path.open("w", encoding="utf-8") as f:
        f.write("row_index\toriginal\tprocessed\n")
        for idx, o, p in changes:
            f.write(f"{idx}\t{o}\t{p}\n")
    logger.info(f"[res={resolution}] Saved {len(changes)} keyword changes → {out_path}")

    modularity_meta = _modularity_meta(resolution)
    df = df.drop(df[df["keywords"].isin(["Unknown keywords"])].index)
    df = df[df[community_col].isin(modularity_meta)]
    df = df.dropna().reset_index(drop=True)
    logger.info(
        f"[res={resolution}] Filtered dataframe: {len(df)} rows, "
        f"{df[community_col].nunique()} unique communities."
    )
    #drop clusters with lesst than min_cluster_size nodes
    cluster_sizes = df[community_col].value_counts()
    valid_clusters = cluster_sizes[cluster_sizes >= min_cluster_size].index
    df = df[df[community_col].isin(valid_clusters)].reset_index(drop=True)
    logger.info(
        f"[res={resolution}] Dropped {(~df[community_col].isin(valid_clusters)).sum()} "  # already filtered, so log before
        f"clusters with <= {min_cluster_size-1} nodes. "
        f"{df[community_col].nunique()} clusters remaining."
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
    corpus: dict[int, set[str]] = {}
    qa_log: dict[str, str] = {}
    all_canonical: set[str] = set()

    for keywords, cluster_id in zip(filtered_df["keywords"], filtered_df[community_col]):
        if cluster_id not in corpus:
            corpus[cluster_id] = set()
        for raw_term in tuple(keywords):
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

    corpus_str: dict[int, str] = {
        k: "\t".join(v) for k, v in sorted(corpus.items())
    }

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
    IDF bias correction.

    Returns:
        X           – corrected sparse TF-IDF matrix (n_clusters × n_features)
        vectorizer  – fitted TfidfVectorizer
        cluster_ids – ordered list of cluster IDs corresponding to X rows
    """
    corpus_str, _ = canonical_corpus
    # cluster_ids = list(corpus_str.keys())
    # documents = list(corpus_str.values())

    cluster_ids = [x for x in corpus_str.keys() if corpus_str[x]]
    documents = [x for x in corpus_str.values() if x]

    vectorizer = TfidfVectorizer(
        tokenizer=lambda x: x.split("\t"),
        token_pattern=None,
        lowercase=False,
        norm=NORM,
    )
    X = vectorizer.fit_transform(documents)
    X = _correct_tfidf(X, vectorizer)

    logger.info(
        f"[res={resolution}] TF-IDF matrix shape: {X.shape} (clusters × features)"
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

    for i, cluster_id in enumerate(cluster_ids):
        meta = modularity_meta.get(cluster_id, {})
        display_label = meta.get("label", f"Cluster {cluster_id}")
        plot_color = meta.get("color", "#1f77b4")
        file_label = _sanitize_filename(meta.get("label", str(cluster_id)))

        cluster_vector = X[i].toarray().flatten()
        scores = pd.Series(cluster_vector, index=feature_names)
        scores = scores[scores > 0].sort_values(ascending=False)

        df_out = pd.DataFrame({"canonical_keyword": scores.index, "tfidf_score": scores.values})
        df_out["canonical_keyword"] = df_out["canonical_keyword"].str.title()
        csv_path = out_dir / f"cluster_{cluster_id}_{file_label}_tfidf_scores.csv"
        df_out.to_csv(csv_path, index=False)
        logger.info(f"[res={resolution}] Saved CSV → {csv_path.name}")

        top = df_out.head(top_n_histogram)
        if top.empty:
            logger.warning(f"[res={resolution}] No TF-IDF scores for {display_label}, skipping plot.")
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

    combined_scores = _aggregate_top_scores(X, vectorizer, cluster_ids, modularity_meta, top_n=3)

    if combined_scores:
        df_combined = pd.DataFrame(combined_scores)
        df_combined["sort_label"] = df_combined["cluster_label"].str.split(":").str[-1].str.strip()
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
        plt.legend(legend_handles, legend_labels, title="Cluster", loc="lower right", framealpha=0.8)
        plt.tight_layout()
        combined_png = out_dir / "combined_top_3_tfidf_histogram.png"
        plt.savefig(combined_png, dpi=150)
        plt.close()
        logger.info(f"[res={resolution}] Saved combined top-3 plot → {combined_png.name}")

    metadata = utils.get_file_metadata(combined_png)
    return metadata


# ── 5. Wordcloud nodes ────────────────────────────────────────────────────────
#
# citation_network_with_layout  – shared; computes/reuses (x, y) vertex coords
# save_wordcloud_figure_<res>   – @datasaver per resolution; reads tfidf_matrix
#                                 for word frequencies and the laid-out graph
#                                 for centroid positions


@parameterize(
    **{
        f"save_wordcloud_figure_{name}": {
            "tfidf_matrix": source(f"tfidf_matrix_{name}"),
            "resolution": value(res),
        }
        for name, res in zip(_res_node_names, RESOLUTIONS)
    }
)
@datasaver()
def save_wordcloud_figure(
    tfidf_matrix: tuple[scipy.sparse.csr_matrix, TfidfVectorizer, list],
    citation_network_with_layout: ig.Graph,
    resolution: float,
    top_n_clusters: int,
    top_n_words: int,
    wordcloud_width: int,
    wordcloud_height: int,
) -> dict:
    """
    Render one figure per resolution:
      - faint scatter of all graph nodes as spatial background
      - one wordcloud per top-N cluster (by node count), centred on its centroid
      - saved as a native vector SVG inside _out_dir(resolution)
    """
    X, vectorizer, cluster_ids = tfidf_matrix
    feature_names = vectorizer.get_feature_names_out()
    cluster_attr = f"cpm_communities_at_res={resolution}"
    out_dir = _out_dir(resolution)

    # ── Build per-cluster keyword frequency dicts from the tfidf matrix ───────
    cluster_freqs: dict[float, dict[str, float]] = {}
    for i, cid in enumerate(cluster_ids):
        vec = X[i].toarray().flatten()
        scores = pd.Series(vec, index=feature_names)
        scores = scores[scores > 0].sort_values(ascending=False).head(top_n_words)
        if not scores.empty:
            cluster_freqs[float(cid)] = {kw.title(): float(sc) for kw, sc in scores.items()}

    if not cluster_freqs:
        logger.warning(f"[res={resolution}] No non-empty cluster frequency dicts. Skipping wordcloud.")
        return utils.get_file_metadata(out_dir)

    # ── Select top N clusters by node count ───────────────────────────────────
    if cluster_attr not in citation_network_with_layout.vs.attribute_names():
        logger.warning(f"[res={resolution}] Vertex attribute '{cluster_attr}' not found in graph.")
        return utils.get_file_metadata(out_dir)

    memberships = np.array(citation_network_with_layout.vs[cluster_attr], dtype=float)
    unique_ids, counts = np.unique(memberships, return_counts=True)
    top_indices = np.argsort(counts)[::-1][:top_n_clusters]
    top_cluster_ids = set(unique_ids[top_indices].tolist())

    drawable = sorted(top_cluster_ids & set(cluster_freqs.keys()))
    if not drawable:
        logger.warning(f"[res={resolution}] No overlap between top clusters and TF-IDF scores.")
        return utils.get_file_metadata(out_dir)

    logger.info(f"[res={resolution}] Drawing vector wordclouds for {len(drawable)} clusters: {drawable}")

    # ── Compute centroids and radii for drawable clusters ─────────────────────
    layout_data = _cluster_centroids_and_radii(citation_network_with_layout, cluster_attr)

    # ── Render ────────────────────────────────────────────────────────────────
    all_x = np.array(citation_network_with_layout.vs["x"], dtype=float)
    all_y = np.array(citation_network_with_layout.vs["y"], dtype=float)

    fig, ax = plt.subplots(figsize=(18, 18))
    ax.set_facecolor("white")
    fig.patch.set_facecolor("white")

    # Background: all nodes as a faint scatter
    ax.scatter(all_x, all_y, s=2, c="#cccccc", alpha=0.4, zorder=1, linewidths=0)
    
    # Calculate mapping from Matplotlib data coordinates to font points for accurate sizing
    # Figure is 18x18 inches; 1 inch = 72 points
    fig_width_pts = 18 * 72 
    # Force Matplotlib to calculate data limits based on the scatter plot
    xlim = ax.get_xlim()
    data_range_x = xlim[1] - xlim[0]
    pts_per_data_unit = fig_width_pts / data_range_x if data_range_x > 0 else 1

    scale = 3 

    for cid in drawable:
        info = layout_data[cid]
        cx, cy, r = info["cx"], info["cy"], info["radius"]
        freqs = cluster_freqs[cid]

        # Initialize Wordcloud with relative_scaling=1.0 for proportional sizing
        wc = WordCloud(
            width=wordcloud_width,
            height=wordcloud_height,
            background_color=None,
            mode="RGBA",
            prefer_horizontal=0.9,
            max_words=top_n_words,
            colormap="tab10",
            relative_scaling=1.0, # <-- This enforces strictly linear mapping to TF-IDF score
        ).generate_from_frequencies(freqs)

        # Matplotlib data space boundaries for this wordcloud extent
        data_w = 2 * scale * r
        data_h = 2 * scale * r
        
        # Scaling factor to translate PIL canvas pixels to Matplotlib font points
        pt_scale_factor = (data_w * pts_per_data_unit) / wordcloud_width

        # Bypass rasterization and extract vector layout
        for item in wc.layout_:
            # Unpack the layout tuple
            (word, count), f_size, (y_px, x_px), orientation, color = item
            
            # Convert 'rgb(r, g, b)' string from PIL into Matplotlib Hex
            if isinstance(color, str) and color.startswith("rgb("):
                r_val, g_val, b_val = [int(c.strip()) for c in color.strip("rgb()").split(",")]
                color = f"#{r_val:02x}{g_val:02x}{b_val:02x}"

            # 1. Map Wordcloud pixel origin (top-left) to Matplotlib data origin
            x_data = (cx - scale * r) + (x_px / wordcloud_width) * data_w
            y_data = (cy + scale * r) - (y_px / wordcloud_height) * data_h
            
            # 2. Scale font size geometrically
            mpl_fontsize = f_size * pt_scale_factor
            
            # 3. Handle rotation
            rot = 90 if orientation is not None else 0
            
            # 4. Render as native Matplotlib vector text
            ax.text(
                x_data, y_data, word,
                fontsize=mpl_fontsize,
                color=color,               
                rotation=rot,
                ha='left',
                va='top',
                zorder=2
            )
        # Mark the centroid
        ax.scatter(cx, cy, s=50, c="black", zorder=3, linewidths=0)
        logger.info(
            f"[res={resolution}] Rendered vector wordcloud for cluster {cid} "
            f"at centroid ({cx:.1f}, {cy:.1f}), radius={r:.1f}"
        )

    ax.axis("off")
    ax.set_title(f"Cluster wordclouds  |  resolution={resolution}", fontsize=14, pad=12)
    plt.tight_layout()

    # Save as an SVG file
    svg_path = out_dir / f"cluster_wordclouds_at_{resolution}.svg"
    fig.savefig(svg_path, format="svg", bbox_inches="tight")
    plt.close(fig)
    logger.info(f"[res={resolution}] Saved vector wordcloud figure → {svg_path}")

    return utils.get_file_metadata(svg_path)
# def save_wordcloud_figure(
#     tfidf_matrix: tuple[scipy.sparse.csr_matrix, TfidfVectorizer, list],
#     citation_network_with_layout: ig.Graph,
#     resolution: float,
#     top_n_clusters: int,
#     top_n_words: int,
#     wordcloud_width: int,
#     wordcloud_height: int,
# ) -> dict:
#     """
#     Render one figure per resolution:
#       - faint scatter of all graph nodes as spatial background
#       - one wordcloud per top-N cluster (by node count), centred on its centroid
#         with size proportional to tfidf_score
#       - saved as a PNG inside _out_dir(resolution)

#     The wordcloud box half-width is derived from the 75th-percentile distance
#     of each cluster's nodes from their centroid, so clouds scale naturally with
#     cluster spread.

#     Returns file-metadata dict (datasaver contract).
#     """
#     X, vectorizer, cluster_ids = tfidf_matrix
#     feature_names = vectorizer.get_feature_names_out()
#     cluster_attr = f"cpm_communities_at_res={resolution}"
#     out_dir = _out_dir(resolution)

#     # ── Build per-cluster keyword frequency dicts from the tfidf matrix ───────
#     cluster_freqs: dict[float, dict[str, float]] = {}
#     for i, cid in enumerate(cluster_ids):
#         vec = X[i].toarray().flatten()
#         scores = pd.Series(vec, index=feature_names)
#         scores = scores[scores > 0].sort_values(ascending=False).head(top_n_words)
#         if not scores.empty:
#             cluster_freqs[float(cid)] = {kw.title(): float(sc) for kw, sc in scores.items()}

#     if not cluster_freqs:
#         logger.warning(f"[res={resolution}] No non-empty cluster frequency dicts. Skipping wordcloud.")
#         metadata = utils.get_file_metadata(out_dir)
#         return metadata

#     # ── Select top N clusters by node count ───────────────────────────────────
#     if cluster_attr not in citation_network_with_layout.vs.attribute_names():
#         logger.warning(
#             f"[res={resolution}] Vertex attribute '{cluster_attr}' not found in graph. "
#             f"Skipping wordcloud figure."
#         )
#         metadata = utils.get_file_metadata(out_dir)
#         return metadata

#     memberships = np.array(citation_network_with_layout.vs[cluster_attr], dtype=float)
#     unique_ids, counts = np.unique(memberships, return_counts=True)
#     top_indices = np.argsort(counts)[::-1][:top_n_clusters]
#     top_cluster_ids = set(unique_ids[top_indices].tolist())

#     # Keep only clusters that have both layout data and tfidf scores
#     drawable = sorted(top_cluster_ids & set(cluster_freqs.keys()))
#     if not drawable:
#         logger.warning(
#             f"[res={resolution}] No overlap between top-{top_n_clusters} clusters by size "
#             f"and clusters with TF-IDF scores. Skipping wordcloud figure."
#         )
#         metadata = utils.get_file_metadata(out_dir)
#         return metadata

#     logger.info(
#         f"[res={resolution}] Drawing wordclouds for {len(drawable)} clusters: {drawable}"
#     )

#     # ── Compute centroids and radii for drawable clusters ─────────────────────
#     layout_data = _cluster_centroids_and_radii(
#         citation_network_with_layout, cluster_attr
#     )

#     # ── Render ────────────────────────────────────────────────────────────────
#     all_x = np.array(citation_network_with_layout.vs["x"], dtype=float)
#     all_y = np.array(citation_network_with_layout.vs["y"], dtype=float)

#     fig, ax = plt.subplots(figsize=(18, 18))
#     ax.set_facecolor("white")
#     fig.patch.set_facecolor("white")

#     # Background: all nodes as a faint scatter
#     ax.scatter(all_x, all_y, s=2, c="#cccccc", alpha=0.4, zorder=1, linewidths=0)

#     for cid in drawable:
#         info = layout_data[cid]
#         cx, cy, r = info["cx"], info["cy"], info["radius"]
#         freqs = cluster_freqs[cid]

#         wc = WordCloud(
#             width=wordcloud_width,
#             height=wordcloud_height,
#             background_color=None,
#             mode="RGBA",
#             prefer_horizontal=0.9,
#             max_words=top_n_words,
#             colormap="tab10",
#         ).generate_from_frequencies(freqs)

#         img = wc.to_array()

#         # Place the wordcloud centred on (cx, cy); extent uses data coordinates
#         sacale = 3
#         ax.imshow(
#             img,
#             extent=(cx - sacale*r, cx + sacale*r, cy - sacale*r, cy + sacale*r),
#             origin="upper",
#             aspect="auto",
#             zorder=2,
#             interpolation="bilinear",
#         )
#         # Mark the centroid
#         ax.scatter(cx, cy, s=50, c="black", zorder=3, linewidths=0)
#         logger.info(
#             f"[res={resolution}] Rendered wordcloud for cluster {cid} "
#             f"at centroid ({cx:.1f}, {cy:.1f}), radius={r:.1f}"
#         )

#     ax.axis("off")
#     ax.set_title(f"Cluster wordclouds  |  resolution={resolution}", fontsize=14, pad=12)
#     plt.tight_layout()

#     png_path = out_dir / f"cluster_wordclouds_at_{resolution}.resolution.png"
#     fig.savefig(png_path, dpi=300, bbox_inches="tight")
#     plt.close(fig)
#     logger.info(f"[res={resolution}] Saved wordcloud figure → {png_path}")

#     metadata = utils.get_file_metadata(png_path)
#     return metadata


if __name__ == "__main__":
    sys.exit(_main())