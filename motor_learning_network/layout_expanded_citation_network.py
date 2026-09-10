"""Lay out the neighbor papers added by build_expanded_citation_network.py /
resolve_neighbor_reference_edges.py, without disturbing the core's existing
ForceAtlas2 layout.

Naive ForceAtlas2 on the whole expanded graph would ignore the structure we
already have: the core's layout already encodes its community structure
spatially, and every neighbor is anchored to it by >= MIN_IN_GRAPH_CITERS
core edges (see get_neighbor_metadata.py). Two stages instead:

  1. Deterministic seed (fast, no simulation). Every neighbor gets an angle
     (direction from the core's centroid to the centroid of its own core
     anchors) and a radius just outside the core's *local* spatial extent in
     that direction. "Local" matters: the core isn't a disk around one
     centroid, it's an irregular, elongated, multi-lobed shape (thin spokes
     radiating out at various angles, a couple of separate island clusters)
     -- a single global boundary distance badly misrepresents that, and in
     several directions "just past the global boundary" is still deep
     inside the real core mass. The boundary radius is computed per angular
     bin instead. Radius also scales with hub-ness: closer in for
     high-core-degree neighbors, farther out for niche ones.
  2. Short force-directed relaxation restricted to the neighbor subgraph
     ONLY. Core vertices and any edge touching one are excluded entirely,
     not just frozen -- the first version pinned the core and kept
     neighbor-core edges as attractive forces, and even a handful of
     iterations dragged most neighbors right back into the core interior
     (their core anchors are scattered throughout the interior, not on a
     clean edge), undoing the seed almost completely. Restricting the
     relaxation to neighbor-neighbor edges only lets thematically-linked
     neighbors (from resolve_neighbor_reference_edges.py) cluster near each
     other without anything pulling them back inward. This also cuts the
     relaxation's O(n^2) repulsion cost roughly in proportion to
     (31,774 / 54,756)^2, since core vertices aren't in that computation at all.

Output: data/graph_level_data/citation_network_expanded_with_layout.graphml
  Same graph as citation_network_expanded_with_reference_edges.graphml;
  core vertices' x/y are byte-identical (untouched), neighbor vertices get
  new x/y from this module.
"""

import logging
import math
from pathlib import Path
import sys
from typing import Final

from hamilton import driver
from hamilton.function_modifiers import dataloader, datasaver
from hamilton.io import utils
import hamilton.log_setup
from hamilton_sdk import adapters
import igraph as ig
import networkx as nx
import numpy as np

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

INPUT_GRAPHML: Final[Path] = (
    GRAPH_LEVEL_DATA_PATH / "citation_network_expanded_with_reference_edges.graphml"
)
OUTPUT_GRAPHML: Final[Path] = (
    GRAPH_LEVEL_DATA_PATH / "citation_network_expanded_with_layout.graphml"
)

# Core-vertex distances from the centroid are binned by angle (10 degrees
# each) so the boundary radius follows the core's actual (non-circular)
# shape; within each bin, this percentile (not max) is used so a handful of
# outlier core nodes don't blow up that bin's radius.
NUM_ANGLE_BINS: Final[int] = 36
CORE_BOUNDARY_PERCENTILE: Final[int] = 95
# A handful of bins can still legitimately contain a thin, far-reaching
# spoke of core papers (779-1,162 of them, not noise) rather than a small
# sample -- measured up to ~5.4x the median bin radius. Left uncapped, a
# neighbor whose anchor angle happens to point into one of those bins would
# be seeded 5x farther out than everywhere else, blowing up the whole
# plot's scale and squashing every other point into invisibility. Cap each
# bin against the overall median so one spoke direction can't dominate.
CORE_BOUNDARY_RADIUS_CAP_MULTIPLE: Final[float] = 2.0
# Seed radius = local_boundary_radius * (RADIUS_NEAR + RADIUS_SPREAD * (1 - hub_score)):
# a pure hub (max core-degree) lands at RADIUS_NEAR x the local boundary, a
# pure niche neighbor (min core-degree) at (RADIUS_NEAR + RADIUS_SPREAD) x.
RADIUS_NEAR: Final[float] = 1.05
RADIUS_SPREAD: Final[float] = 0.6
# Quick pass: the seed already encodes the meaningful structure; a short
# relaxation just lets neighbor-neighbor edges cluster related neighbors
# without drifting far from it.
RELAXATION_ITERATIONS: Final[int] = 10
RELAXATION_SEED: Final[int] = 42

#####################
##  Aux Functions  ##
#####################


def _core_centroid(graph: ig.Graph) -> tuple[float, float]:
    xs = [v["x"] for v in graph.vs if v["is_original_node"] is True]
    ys = [v["y"] for v in graph.vs if v["is_original_node"] is True]
    return (float(np.mean(xs)), float(np.mean(ys)))


def _angle_bin(angle: float, num_bins: int) -> int:
    bin_width = 2 * math.pi / num_bins
    return int(((angle + math.pi) // bin_width) % num_bins)


def _core_boundary_radii_by_angle(
    graph: ig.Graph,
    core_centroid: tuple[float, float],
    num_bins: int = NUM_ANGLE_BINS,
    percentile: int = CORE_BOUNDARY_PERCENTILE,
) -> list[float]:
    """Per-angle-bin boundary radius around core_centroid, following the
    core's actual (non-circular) shape instead of one global distance."""
    cx, cy = core_centroid
    bins: list[list[float]] = [[] for _ in range(num_bins)]
    for v in graph.vs:
        if v["is_original_node"] is not True:
            continue
        dx, dy = v["x"] - cx, v["y"] - cy
        bins[_angle_bin(math.atan2(dy, dx), num_bins)].append(math.hypot(dx, dy))

    radii = [float(np.percentile(b, percentile)) if b else None for b in bins]
    if all(r is None for r in radii):
        raise ValueError("No core vertices found to compute boundary radii.")

    median_radius = float(np.median([r for r in radii if r is not None]))
    cap = CORE_BOUNDARY_RADIUS_CAP_MULTIPLE * median_radius
    radii = [min(r, cap) if r is not None else None for r in radii]

    # Fill any empty bins (angular gaps with no core vertices) from the
    # nearest non-empty bin, wrapping around the circle.
    filled = list(radii)
    for i, r in enumerate(radii):
        if r is not None:
            continue
        for offset in range(1, num_bins):
            for candidate in (i - offset, i + offset):
                candidate %= num_bins
                if radii[candidate] is not None:
                    filled[i] = radii[candidate]
                    break
            if filled[i] is not None:
                break
    return filled


def _boundary_radius_at_angle(angle: float, boundary_radii: list[float]) -> float:
    return boundary_radii[_angle_bin(angle, len(boundary_radii))]


def _core_anchor_stats(graph: ig.Graph) -> dict[str, dict]:
    """For every non-core vertex: the centroid of its core-vertex neighbors'
    (x, y) (in either edge direction) and how many such core anchors it has.
    Every neighbor is guaranteed >= MIN_IN_GRAPH_CITERS core anchors by
    construction (get_neighbor_metadata.py), so this is never empty for a
    real neighbor vertex."""
    core_indices = {v.index for v in graph.vs if v["is_original_node"] is True}
    stats: dict[str, dict] = {}
    for v in graph.vs:
        if v["is_original_node"] is True:
            continue
        core_neighbor_indices = set(graph.neighbors(v.index, mode="all")) & core_indices
        if not core_neighbor_indices:
            continue
        xs = [graph.vs[i]["x"] for i in core_neighbor_indices]
        ys = [graph.vs[i]["y"] for i in core_neighbor_indices]
        stats[v["name"]] = {
            "centroid": (float(np.mean(xs)), float(np.mean(ys))),
            "core_degree": len(core_neighbor_indices),
        }
    return stats


def _anchor_angle(
    anchor_centroid: tuple[float, float], core_centroid: tuple[float, float], doi: str
) -> float:
    cx, cy = core_centroid
    ax, ay = anchor_centroid
    dx, dy = ax - cx, ay - cy
    if dx == 0.0 and dy == 0.0:
        # Degenerate: anchors happen to average out to the core centroid
        # exactly. Fall back to a deterministic angle from the DOI so the
        # seed is still stable across reruns, rather than 0 for every such case.
        return 2 * math.pi * (hash(doi) % 1000) / 1000 - math.pi
    return math.atan2(dy, dx)


def _seed_position(
    angle: float,
    core_centroid: tuple[float, float],
    core_degree: int,
    boundary_radius: float,
    min_core_degree: int,
    max_core_degree: int,
) -> tuple[float, float]:
    cx, cy = core_centroid
    if max_core_degree > min_core_degree:
        hub_score = (math.log(core_degree) - math.log(min_core_degree)) / (
            math.log(max_core_degree) - math.log(min_core_degree)
        )
        hub_score = min(max(hub_score, 0.0), 1.0)
    else:
        hub_score = 0.0

    radius = boundary_radius * (RADIUS_NEAR + RADIUS_SPREAD * (1 - hub_score))
    return (cx + radius * math.cos(angle), cy + radius * math.sin(angle))


def _seed_positions(graph: ig.Graph) -> dict[str, tuple[float, float]]:
    core_centroid = _core_centroid(graph)
    boundary_radii = _core_boundary_radii_by_angle(graph, core_centroid)
    anchor_stats = _core_anchor_stats(graph)
    core_degrees = [stats["core_degree"] for stats in anchor_stats.values()]
    min_core_degree, max_core_degree = min(core_degrees), max(core_degrees)

    positions = {}
    for doi, stats in anchor_stats.items():
        angle = _anchor_angle(stats["centroid"], core_centroid, doi)
        boundary_radius = _boundary_radius_at_angle(angle, boundary_radii)
        positions[doi] = _seed_position(
            angle,
            core_centroid,
            stats["core_degree"],
            boundary_radius,
            min_core_degree,
            max_core_degree,
        )
    return positions


def _rescale_to_match(
    reference: dict[str, tuple[float, float]], target: dict[str, tuple[float, float]]
) -> dict[str, tuple[float, float]]:
    """Rescale + recenter `target` so its centroid and average spread from
    that centroid match `reference`'s. `networkx.spring_layout` silently
    rescales its output to fit a unit circle around the origin whenever
    `fixed` isn't set (it only preserves absolute coordinates when there
    are frozen nodes to anchor the scale to) -- since the relaxation graph
    has no core nodes at all, that rescale would otherwise collapse the
    seed step's real-world coordinates into a tiny region overlapping the
    core, invisible at that scale. This undoes the rescale while keeping
    the *relative* rearrangement the relaxation produced."""
    ref_keys = list(reference.keys())
    ref_center = (
        float(np.mean([reference[k][0] for k in ref_keys])),
        float(np.mean([reference[k][1] for k in ref_keys])),
    )
    ref_spread = float(
        np.mean(
            [
                math.hypot(reference[k][0] - ref_center[0], reference[k][1] - ref_center[1])
                for k in ref_keys
            ]
        )
    )

    tgt_keys = list(target.keys())
    tgt_center = (
        float(np.mean([target[k][0] for k in tgt_keys])),
        float(np.mean([target[k][1] for k in tgt_keys])),
    )
    tgt_spread = float(
        np.mean(
            [
                math.hypot(target[k][0] - tgt_center[0], target[k][1] - tgt_center[1])
                for k in tgt_keys
            ]
        )
    )

    scale = ref_spread / tgt_spread if tgt_spread > 0 else 1.0
    return {
        k: (
            ref_center[0] + (target[k][0] - tgt_center[0]) * scale,
            ref_center[1] + (target[k][1] - tgt_center[1]) * scale,
        )
        for k in tgt_keys
    }


def _relaxed_neighbor_positions(
    graph: ig.Graph,
    seed_positions: dict[str, tuple[float, float]],
    iterations: int = RELAXATION_ITERATIONS,
    seed: int = RELAXATION_SEED,
) -> dict[str, tuple[float, float]]:
    """Force-directed relaxation restricted to the neighbor subgraph only --
    core vertices and any edge touching one are excluded entirely, not just
    frozen. The seed step already captured the core-anchor relationship (via
    the anchor-derived angle/radius); keeping neighbor-core edges as
    attractive forces here would just drag neighbors back toward those
    (interior, not boundary) core points, undoing the seed."""
    is_core = {v["name"]: v["is_original_node"] is True for v in graph.vs}

    relaxation_graph = nx.Graph()
    relaxation_graph.add_nodes_from(seed_positions.keys())
    for e in graph.es:
        source_name = graph.vs[e.source]["name"]
        target_name = graph.vs[e.target]["name"]
        if is_core[source_name] or is_core[target_name]:
            continue
        relaxation_graph.add_edge(source_name, target_name)

    new_positions = nx.spring_layout(
        relaxation_graph, pos=seed_positions, iterations=iterations, seed=seed
    )
    neighbor_positions = {doi: tuple(new_positions[doi]) for doi in seed_positions}
    return _rescale_to_match(seed_positions, neighbor_positions)


def _apply_neighbor_positions(
    graph: ig.Graph, positions: dict[str, tuple[float, float]]
) -> ig.Graph:
    for v in graph.vs:
        if v["name"] in positions:
            x, y = positions[v["name"]]
            v["x"], v["y"] = x, y
    return graph


##################
##     Main     ##
##################


def _main() -> int:
    inputs = dict(
        input_graphml_path=INPUT_GRAPHML,
        output_graphml_path=OUTPUT_GRAPHML,
    )
    outputs = ["save_laid_out_citation_network"]

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
def base_graph(input_graphml_path: Path) -> tuple[ig.Graph, dict]:
    graph = ig.Graph.Read_GraphML(str(input_graphml_path))
    return graph, utils.get_file_metadata(input_graphml_path)


def seed_positions(base_graph: ig.Graph) -> dict[str, tuple[float, float]]:
    positions = _seed_positions(base_graph)
    logger.info(f"Seeded {len(positions)} neighbor positions from their core anchors.")
    return positions


def relaxed_neighbor_positions(
    base_graph: ig.Graph, seed_positions: dict[str, tuple[float, float]]
) -> dict[str, tuple[float, float]]:
    return _relaxed_neighbor_positions(base_graph, seed_positions)


def laid_out_citation_network(
    base_graph: ig.Graph, relaxed_neighbor_positions: dict[str, tuple[float, float]]
) -> ig.Graph:
    return _apply_neighbor_positions(base_graph, relaxed_neighbor_positions)


@datasaver()
def save_laid_out_citation_network(
    laid_out_citation_network: ig.Graph, output_graphml_path: Path
) -> dict:
    laid_out_citation_network.write(output_graphml_path)
    return utils.get_file_metadata(output_graphml_path)


if __name__ == "__main__":
    sys.exit(_main())
