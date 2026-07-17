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
from scipy.spatial import ConvexHull, Voronoi
from scipy.ndimage import binary_dilation
from shapely.geometry import Polygon, MultiPolygon, box as shapely_box
from shapely.ops import unary_union
from PIL import Image, ImageDraw, ImageFont
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
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
# YEAR = 1960
# RESOLUTIONS: list[float] = [0.005]
# YEAR = 1980
# RESOLUTIONS: list[float] = [0.001, 0.012]
# YEAR = 1990
# RESOLUTIONS: list[float] = [0.001, 0.002]
# YEAR = 2000
# RESOLUTIONS: list[float] = [0.003, 0.014]
# YEAR = 2005
# RESOLUTIONS: list[float] = [0.003, 0.007]
# YEAR = 2010
# RESOLUTIONS: list[float] = [0.0006, 0.002]
# YEAR = 2015
# RESOLUTIONS: list[float] = [0.0009, 0.003]
YEAR = 2020
RESOLUTIONS: list[float] = [0.001, 0.002]
# YEAR = 2026
# RESOLUTIONS: list[float] = [0.0004, 0.001]
TD_IDF_SAVING_PATH = KEYWORDS_LEVEL_DATA_PATH / f"until_{YEAR}_wordcloud_test_noverlap_true_size"
TD_IDF_SAVING_PATH.mkdir(parents=True, exist_ok=True)
TOP_N_CLUSTERS = 15
# RESOLUTIONS: list[float] = [round(0.001 + i * 0.001, 3) for i in range(1, 9)]

# ── Per-resolution output directory helper ────────────────────────────────────
NORM: Final = "l2"
IDF_BIAS: Final = 0.0
SYNONYMS_THRESHOLD: Final = 0.99

# ── Analytic word-cloud sizing ────────────────────────────────────────────────
# Font size is a pure function of TF-IDF (not chosen by WordCloud's fit loop):
#     size_pts(word) = BASE_MAX_FONT_PTS * tfidf(word) / global_max_tfidf
# → trustworthy sizes within AND across clusters.  See _pack_cluster_analytic.
BASE_MAX_FONT_PTS: Final = 80.0      # point size of the globally highest-TF-IDF word
MIN_READABLE_PTS: Final = 3.0        # words whose true size is below this are dropped
MARGIN_PX: Final = 2                 # glyph padding (px) → guarantees gaps between words
PREFER_HORIZONTAL: Final = 0.9       # fraction of words attempted horizontal first
ALLOW_CLUSTER_RESCALE: Final = False # if a pocket is too small for its top word, apply
                                     # ONE uniform shrink to that cluster (keeps within-
                                     # cluster proportions exact; trades away strict
                                     # cross-cluster comparability for that cluster only)
_FONT_PATH: Final = fm.findfont(fm.FontProperties(family="DejaVu Sans"))

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
        min_cluster_size=10, # >= to this number will be included
        # wordcloud inputs
        forceatlas2_iterations=500,
        top_n_clusters=TOP_N_CLUSTERS,
        top_n_words=20,
    )

    # tfidf_outputs = [f"save_combined_plot_{name}" for name in _res_node_names]
    wordcloud_outputs = [f"save_wordcloud_figure_{name}" for name in _res_node_names]
    # outputs = tfidf_outputs + wordcloud_outputs
    outputs = wordcloud_outputs

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


def _voronoi_finite_polygons(centroids: np.ndarray, bounding_box: tuple) -> list[Polygon]:
    """
    Compute finite Voronoi cells for each centroid, clipped to bounding_box.

    Parameters
    ----------
    centroids : (N, 2) array of centroid coordinates
    bounding_box : (min_x, min_y, max_x, max_y) in data coordinates —
                   cells are clipped to this rectangle so every cell is finite.

    Returns
    -------
    List of N shapely Polygons, one per centroid, in the same order.
    If N == 1 the single cell is the entire bounding box.
    """
    min_x, min_y, max_x, max_y = bounding_box
    bbox_poly = shapely_box(min_x, min_y, max_x, max_y)

    if len(centroids) == 1:
        return [bbox_poly]

    # scipy.spatial.Voronoi needs at least 3 non-collinear points to produce
    # finite regions for all vertices.  We add four far-away dummy points
    # outside the bounding box so every real centroid ends up with a finite cell.
    pad = max(max_x - min_x, max_y - min_y) * 10
    dummy = np.array([
        [min_x - pad, min_y - pad],
        [max_x + pad, min_y - pad],
        [max_x + pad, max_y + pad],
        [min_x - pad, max_y + pad],
    ])
    all_points = np.vstack([centroids, dummy])
    vor = Voronoi(all_points)

    cells: list[Polygon] = []
    for idx in range(len(centroids)):
        region_idx = vor.point_region[idx]
        region = vor.regions[region_idx]
        if -1 in region or len(region) == 0:
            # Unbounded region — use the whole bbox as fallback
            cells.append(bbox_poly)
            continue
        verts = vor.vertices[region]
        cell_poly = Polygon(verts)
        # Clip to bounding box so no cell extends outside the figure
        clipped = cell_poly.intersection(bbox_poly)
        if clipped.is_empty:
            cells.append(bbox_poly)
        else:
            cells.append(clipped)

    return cells


def _convex_hull_polygon(points: np.ndarray) -> Polygon:
    """
    Return the shapely Polygon for the convex hull of `points` (N×2).
    Falls back gracefully for degenerate cases (1 or 2 points).
    """
    if len(points) >= 3:
        try:
            hull = ConvexHull(points)
            return Polygon(points[hull.vertices])
        except Exception:
            pass
    # Degenerate: build a small square around the mean
    cx, cy = points.mean(axis=0)
    r = max(float(np.ptp(points, axis=0).max()), 1.0)
    return shapely_box(cx - r, cy - r, cx + r, cy + r)


# ── Analytic word packing (replaces WordCloud's size+placement) ───────────────
def _spiral_offsets(max_r: float, step: float) -> list[tuple[float, float]]:
    """Archimedean-ish spiral of (dx, dy) offsets, centre first, expanding out.

    Placing the largest words first at small radius produces a compact, centred
    cloud (the same visual intent as WordCloud's spiral search)."""
    step = max(1.0, float(step))
    pts: list[tuple[float, float]] = [(0.0, 0.0)]
    r = step
    while r <= max_r:
        n = max(8, int(2 * np.pi * r / step))
        for k in range(n):
            a = 2 * np.pi * k / n
            pts.append((r * np.cos(a), r * np.sin(a)))
        r += step
    return pts


def _render_word_stamp(
    word: str, font_path: str, size_px: float, rotate: bool, margin_px: int
) -> "np.ndarray | None":
    """Render `word` to a boolean ink stamp at `size_px` pixels.

    The stamp is the glyph ink dilated by `margin_px`, so OR-ing it into the
    occupancy grid both reserves the glyph footprint and enforces a small gap to
    its neighbours. Returns None if the word is too small to render."""
    size_px = int(round(size_px))
    if size_px < 3:
        return None
    font = ImageFont.truetype(font_path, size_px)
    probe = Image.new("L", (8, 8), 0)
    left, top, right, bottom = ImageDraw.Draw(probe).textbbox((0, 0), word, font=font)
    w, h = right - left, bottom - top
    if w <= 0 or h <= 0:
        return None
    img = Image.new("L", (w + 2 * margin_px, h + 2 * margin_px), 0)
    ImageDraw.Draw(img).text((margin_px - left, margin_px - top), word, fill=255, font=font)
    arr = np.array(img) > 0
    if margin_px > 0:
        arr = binary_dilation(arr, iterations=margin_px)
    if rotate:
        arr = np.rot90(arr)
    return arr


def _find_spot(
    occ: np.ndarray, stamp: np.ndarray, cx: float, cy: float,
    spiral: list[tuple[float, float]],
) -> "tuple[int, int] | None":
    """First spiral position where `stamp` collides with nothing in `occ`.

    `occ` is True where a pixel is blocked — that already includes the region
    boundary (everything outside hull∩Voronoi) AND previously-placed words, so a
    single `&` test enforces both containment and non-overlap."""
    H, W = occ.shape
    sh, sw = stamp.shape
    for ox, oy in spiral:
        x = int(round(cx + ox - sw / 2.0))
        y = int(round(cy + oy - sh / 2.0))
        if x < 0 or y < 0 or x + sw > W or y + sh > H:
            continue
        if not np.any(occ[y:y + sh, x:x + sw] & stamp):
            return x, y
    return None


def _pack_cluster_analytic(
    freqs: dict[str, float],
    global_max_tfidf: float,
    pt_scale_factor: float,
    wc_width_px: int,
    wc_height_px: int,
    region_allowed: np.ndarray,
    seed: int,
) -> tuple[list[dict], float, int]:
    """Place a cluster's words at TF-IDF-derived sizes with zero overlap.

    Returns (placed, cluster_scale, dropped):
        placed         list of {word, x_px, y_px, size_pts, rotate}
        cluster_scale  uniform shrink applied (1.0 unless ALLOW_CLUSTER_RESCALE
                       kicked in)
        dropped        number of words that did not fit at their true size
    """
    rng = np.random.default_rng(seed)
    occ = ~region_allowed.astype(bool)          # True = blocked
    H, W = occ.shape
    ys, xs = np.where(region_allowed)
    if xs.size == 0:
        return [], 1.0, len(freqs)
    cx0, cy0 = float(xs.mean()), float(ys.mean())
    spiral = _spiral_offsets(float(np.hypot(W, H)), step=max(2.0, min(H, W) * 0.02))

    items = sorted(freqs.items(), key=lambda kv: kv[1], reverse=True)

    def sizes(score: float, s: float) -> tuple[float, float]:
        size_pts = BASE_MAX_FONT_PTS * (score / global_max_tfidf) * s
        return size_pts, size_pts / pt_scale_factor   # (points, mask pixels)

    # ── Optional uniform per-cluster rescale so the top word fits ─────────────
    cluster_scale = 1.0
    if ALLOW_CLUSTER_RESCALE and items:
        top_word, top_score = items[0]

        def top_fits(s: float) -> bool:
            _, spx = sizes(top_score, s)
            st = _render_word_stamp(top_word, _FONT_PATH, spx, False, MARGIN_PX)
            return st is not None and _find_spot(occ, st, cx0, cy0, spiral) is not None

        if not top_fits(1.0):
            lo, hi = 0.05, 1.0
            for _ in range(18):                  # binary search the largest fitting scale
                mid = 0.5 * (lo + hi)
                if top_fits(mid):
                    lo = mid
                else:
                    hi = mid
            cluster_scale = lo

    # ── Place words, biggest first ────────────────────────────────────────────
    placed: list[dict] = []
    dropped = 0
    for word, score in items:
        size_pts, size_px = sizes(score, cluster_scale)
        if size_pts < MIN_READABLE_PTS or size_px < 3:
            dropped += 1
            continue

        horizontal_first = rng.random() < PREFER_HORIZONTAL
        orientations = [False, True] if horizontal_first else [True, False]

        spot = None
        chosen_stamp = None
        chosen_rot = False
        for rot in orientations:
            stamp = _render_word_stamp(word, _FONT_PATH, size_px, rot, MARGIN_PX)
            if stamp is None or stamp.shape[0] > H or stamp.shape[1] > W:
                continue
            pos = _find_spot(occ, stamp, cx0, cy0, spiral)
            if pos is not None:
                spot, chosen_stamp, chosen_rot = pos, stamp, rot
                break

        if spot is None:
            dropped += 1
            continue

        x, y = spot
        sh, sw = chosen_stamp.shape
        occ[y:y + sh, x:x + sw] |= chosen_stamp
        placed.append({
            "word": word,
            "x_px": x + MARGIN_PX,          # back out the padding → ink top-left
            "y_px": y + MARGIN_PX,
            "size_pts": size_pts,
            "rotate": chosen_rot,
        })

    return placed, cluster_scale, dropped


def _build_voronoi_hull_mask(
    cluster_points: np.ndarray,
    voronoi_cell: Polygon,
    mask_width: int,
    mask_height: int,
) -> tuple[np.ndarray, dict]:
    """
    Build a WordCloud-compatible mask for one cluster.

    The allowed region is the intersection of:
      - the cluster's convex hull (tight boundary around its own nodes)
      - its Voronoi cell  (guarantees no overlap with neighbouring clusters)

    Parameters
    ----------
    cluster_points : (N, 2) array of node (x, y) coordinates for this cluster
    voronoi_cell   : shapely Polygon — the Voronoi cell in data coordinates
    mask_width     : pixel width of the output mask
    mask_height    : pixel height of the output mask

    Returns
    -------
    mask : uint8 ndarray (mask_height × mask_width)
           WordCloud convention: 0 = word CAN go here, 255 = blocked.
           (Yes, it is inverted relative to what you might expect.)
    transform : dict with keys
        "data_min_x", "data_min_y", "data_max_x", "data_max_y",
        "px_per_data_x", "px_per_data_y"
        — everything needed to unproject pixel→data coordinates later.
    """
    hull_poly = _convex_hull_polygon(cluster_points)
    region = hull_poly.intersection(voronoi_cell)

    if region.is_empty:
        logger.warning("Convex-hull ∩ Voronoi cell is empty — falling back to hull alone.")
        region = hull_poly

    # If the result is a MultiPolygon take the largest piece
    if isinstance(region, MultiPolygon):
        region = max(region.geoms, key=lambda p: p.area)

    # Data bounding box of the region (used for the affine transform)
    bounds = region.bounds  # (min_x, min_y, max_x, max_y)
    data_min_x, data_min_y, data_max_x, data_max_y = bounds
    data_span_x = data_max_x - data_min_x or 1.0
    data_span_y = data_max_y - data_min_y or 1.0

    px_per_data_x = mask_width / data_span_x
    px_per_data_y = mask_height / data_span_y

    transform = {
        "data_min_x": data_min_x,
        "data_min_y": data_min_y,
        "data_max_x": data_max_x,
        "data_max_y": data_max_y,
        "px_per_data_x": px_per_data_x,
        "px_per_data_y": px_per_data_y,
        "mask_width": mask_width,
        "mask_height": mask_height,
    }

    # Project region polygon vertices into pixel space
    # PIL convention: pixel (0,0) is top-left; y_px increases downward
    def data_to_px(x, y):
        px = (x - data_min_x) * px_per_data_x
        py = (data_max_y - y) * px_per_data_y  # flip y
        return px, py

    # Rasterise using PIL
    img = Image.new("L", (mask_width, mask_height), color=255)  # start all blocked
    draw = ImageDraw.Draw(img)

    exterior_px = [data_to_px(x, y) for x, y in region.exterior.coords]
    draw.polygon(exterior_px, fill=0)  # allowed area = 0

    # Punch out any interior holes (rare but correct)
    for interior in region.interiors:
        hole_px = [data_to_px(x, y) for x, y in interior.coords]
        draw.polygon(hole_px, fill=255)

    mask = np.array(img)  # shape: (mask_height, mask_width), dtype uint8
    logger.debug(
        f"Mask built: {mask_width}×{mask_height} px, "
        f"allowed={int((mask == 0).sum())}, "
        f"blocked={int((mask == 255).sum())}"
    )
    return mask, transform


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
    corpus: dict[int, list[str]] = {}
    qa_log: dict[str, str] = {}
    all_canonical: set[str] = set()

    for keywords, cluster_id in zip(filtered_df["keywords"], filtered_df[community_col]):
        if cluster_id not in corpus:
            corpus[cluster_id] = []
        for raw_term in tuple(keywords):
            norm_term = _normalize_keyword(raw_term)
            canonical_term = synonym_map.get(norm_term, norm_term)
            corpus[cluster_id].append(canonical_term)
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
) -> dict:
    """
    Render one figure per resolution using a convex-hull + Voronoi mask approach
    with ANALYTIC font sizing:
      - faint scatter of all graph nodes as spatial background
      - for each of the top-N clusters (by node count):
          1. compute the convex hull of the cluster's node positions
          2. intersect it with the cluster's Voronoi cell (computed over all
             drawable centroids) -- this GUARANTEES zero overlap between clouds
          3. rasterise the intersection into an allowed-region mask
          4. size each word ANALYTICALLY from its TF-IDF score
                 size_pts = BASE_MAX_FONT_PTS * tfidf / global_max_tfidf
             (one global scale -> sizes are trustworthy within AND across
             clusters; no WordCloud fit-loop, no floor clamping)
          5. pack words biggest-first into the region with a glyph-level
             collision grid (_pack_cluster_analytic)
          6. unproject each placed word from pixel space to data coordinates
          7. draw with ax.text() for a clean vector SVG output
      - saved as an SVG inside _out_dir(resolution)

    Because size is fixed globally, a word only appears if it fits its cluster's
    pocket at its true size. Words are placed biggest-first, so a full pocket
    drops the SMALLEST (least important) words -- never resizes them. Dropped
    counts are logged. See ALLOW_CLUSTER_RESCALE for the small-pocket escape
    hatch.

    Returns file-metadata dict (datasaver contract).
    """
    X, vectorizer, cluster_ids = tfidf_matrix
    feature_names = vectorizer.get_feature_names_out()
    cluster_attr = f"cpm_communities_at_res={resolution}"
    out_dir = _out_dir(resolution)

    # -- Per-cluster keyword frequency dicts from the tfidf matrix -------------
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

    # -- Select top N clusters by node count ----------------------------------
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

    logger.info(f"[res={resolution}] Drawing analytic wordclouds for {len(drawable)} clusters: {drawable}")

    # -- Spatial data ---------------------------------------------------------
    all_x = np.array(citation_network_with_layout.vs["x"], dtype=float)
    all_y = np.array(citation_network_with_layout.vs["y"], dtype=float)

    # Global bounding box used to clip Voronoi cells
    pad = max(all_x.ptp(), all_y.ptp()) * 0.05
    bbox = (all_x.min() - pad, all_y.min() - pad,
            all_x.max() + pad, all_y.max() + pad)

    # Centroids of drawable clusters (in the order of `drawable`)
    centroids = np.array([
        [float(all_x[memberships == cid].mean()),
         float(all_y[memberships == cid].mean())]
        for cid in drawable
    ])

    # -- Voronoi cells for drawable centroids ---------------------------------
    voronoi_cells = _voronoi_finite_polygons(centroids, bbox)
    # voronoi_cells[i] corresponds to drawable[i]

    # -- GLOBAL font scale -- the whole point of this version -----------------
    # size is BASE_MAX_FONT_PTS at the single highest TF-IDF across all clusters,
    # and scales linearly down from there for every word everywhere.
    global_max_tfidf = float(max(
        sc for cid in drawable for sc in cluster_freqs[cid].values()
    ))

    # -- Figure setup ---------------------------------------------------------
    fig, ax = plt.subplots(figsize=(18, 18))
    ax.set_facecolor("white")
    fig.patch.set_facecolor("white")
    ax.scatter(all_x, all_y, s=2, c="#cccccc", alpha=0.4, zorder=1, linewidths=0)
    ax.set_xlim(all_x.min() - pad, all_x.max() + pad)
    ax.set_ylim(all_y.min() - pad, all_y.max() + pad)
    fig.canvas.draw()  # needed so get_xlim/ylim are accurate

    fig_width_pts = 18 * 72   # 1 inch = 72 pt
    data_range_x = ax.get_xlim()[1] - ax.get_xlim()[0]
    pts_per_data_unit = fig_width_pts / data_range_x if data_range_x > 0 else 1.0

    modularity_meta = _modularity_meta(resolution)

    for idx, cid in enumerate(drawable):
        freqs = cluster_freqs[cid]

        # Cluster colour
        meta = modularity_meta.get(cid, {})
        cluster_color = meta.get("color", "#AAAAAA")
        if cluster_color == "#AAAAAA":
            cluster_color = plt.get_cmap("tab10")(int(cid) % 10)

        # Node positions for this cluster
        mask_members = memberships == cid
        cluster_pts = np.column_stack([all_x[mask_members], all_y[mask_members]])
        voronoi_cell = voronoi_cells[idx]

        # Mask resolution proportional to the region's data-space area (generous,
        # so the analytic packing is fine-grained).
        region_area = voronoi_cell.intersection(
            _convex_hull_polygon(cluster_pts)
        ).area
        full_area = (bbox[2] - bbox[0]) * (bbox[3] - bbox[1])
        area_fraction = min(region_area / full_area, 1.0) if full_area > 0 else 1.0
        target_px = max(600, int(1800 * np.sqrt(area_fraction)))
        wc_width_px = wc_height_px = target_px

        wc_mask, transform = _build_voronoi_hull_mask(
            cluster_pts, voronoi_cell, wc_width_px, wc_height_px
        )
        region_allowed = (wc_mask == 0)
        allowed_fraction = region_allowed.mean()
        if allowed_fraction < 0.01:
            logger.warning(
                f"[res={resolution}] Cluster {cid}: mask has only "
                f"{allowed_fraction:.1%} allowed pixels -- skipping."
            )
            continue

        # points -> mask-pixel conversion for THIS cluster's mask
        data_span_x = (transform["data_max_x"] - transform["data_min_x"]) or 1.0
        data_span_y = (transform["data_max_y"] - transform["data_min_y"]) or 1.0
        pt_scale_factor = (data_span_x * pts_per_data_unit) / wc_width_px

        placed, cluster_scale, dropped = _pack_cluster_analytic(
            freqs=freqs,
            global_max_tfidf=global_max_tfidf,
            pt_scale_factor=pt_scale_factor,
            wc_width_px=wc_width_px,
            wc_height_px=wc_height_px,
            region_allowed=region_allowed,
            seed=int(cid),
        )

        # -- Unproject pixel positions -> data coords and draw ----------------
        dmin_x = transform["data_min_x"]
        dmax_y = transform["data_max_y"]
        for p in placed:
            x_data = dmin_x + (p["x_px"] / wc_width_px) * data_span_x
            y_data = dmax_y - (p["y_px"] / wc_height_px) * data_span_y
            ax.text(
                x_data, y_data, p["word"],
                fontsize=p["size_pts"],            # <-- TF-IDF-derived, global scale
                color=cluster_color,
                rotation=(90 if p["rotate"] else 0),
                ha="left", va="top", zorder=2,
            )

        scale_note = "" if cluster_scale == 1.0 else f", uniform rescale={cluster_scale:.2f}"
        logger.info(
            f"[res={resolution}] Cluster {cid}: placed {len(placed)} words, "
            f"dropped {dropped} (did not fit at true size), "
            f"mask allowed {allowed_fraction:.1%} of pixels{scale_note}."
        )

    ax.axis("off")
    ax.set_title(f"Cluster wordclouds (analytic size)  |  resolution={resolution}", fontsize=14, pad=12)
    plt.tight_layout()

    svg_path = out_dir / f"cluster_wordclouds_at_{resolution}.svg"
    fig.savefig(svg_path, format="svg", bbox_inches="tight")
    plt.close(fig)
    logger.info(f"[res={resolution}] Saved analytic wordcloud figure -> {svg_path}")

    return utils.get_file_metadata(svg_path)


if __name__ == "__main__":
    sys.exit(_main())