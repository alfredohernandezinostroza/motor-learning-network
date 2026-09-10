"""Lay out the neighbor papers added by build_expanded_citation_network.py /
resolve_neighbor_reference_edges.py, without disturbing the core's existing
ForceAtlas2 layout.

Naive ForceAtlas2 on the whole expanded graph would ignore the structure we
already have: the core's layout already encodes its community structure
spatially, and every neighbor is anchored to it by >= MIN_IN_GRAPH_CITERS
core edges (see get_neighbor_metadata.py). Two stages instead:

  1. Deterministic seed (fast, no simulation). Every neighbor gets an angle
     (direction from the core's centroid to the centroid of its own core
     anchors) and a radius (just outside the core's spatial extent, closer
     in for "hub" neighbors with many core anchors, farther out for niche
     ones) -- this alone guarantees a neighbor lands near the part of the
     corpus it's actually connected to, not at a random angle.
  2. Short force-directed relaxation, core pinned. `networkx.spring_layout`
     with `fixed=<core node names>`, seeded from stage 1, using only edges
     that touch at least one neighbor (core-core edges are dropped -- both
     endpoints are frozen, so they contribute nothing to the relaxation).
     Real neighbor-neighbor/neighbor-core edges (from
     resolve_neighbor_reference_edges.py) now pull thematically-linked
     neighbors together, not just toward their core anchors.

Output: data/graph_level_data/citation_network_expanded_with_layout.graphml
  Same graph as citation_network_expanded_with_reference_edges.graphml;
  core vertices' x/y are byte-identical (frozen), neighbor vertices get new
  x/y from this module.
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

# Percentile (not max) of core-vertex distances from the core centroid, so a
# handful of outlier core nodes don't blow up the boundary radius.
CORE_BOUNDARY_PERCENTILE: Final[int] = 95
# Seed radius = core_boundary_radius * (RADIUS_NEAR + RADIUS_SPREAD * (1 - hub_score)):
# a pure hub (max core-degree) lands at RADIUS_NEAR x the boundary, a pure
# niche neighbor (min core-degree) at (RADIUS_NEAR + RADIUS_SPREAD) x.
RADIUS_NEAR: Final[float] = 1.05
RADIUS_SPREAD: Final[float] = 0.6
# Quick pass: the seed already encodes the meaningful structure; a short
# relaxation just declutters local overlap without drifting far from it.
# Measured on the real graph (54,756 vertices): networkx's spring_layout
# computes repulsion densely (O(n^2)) internally, ~93s/iteration at this
# scale -- 5 iterations (~8 min) is the "quick pass"; raise deliberately,
# not by default, since cost scales linearly with iteration count.
RELAXATION_ITERATIONS: Final[int] = 5
RELAXATION_SEED: Final[int] = 42

#####################
##  Aux Functions  ##
#####################


def _core_centroid(graph: ig.Graph) -> tuple[float, float]:
    xs = [v["x"] for v in graph.vs if v["is_original_node"] is True]
    ys = [v["y"] for v in graph.vs if v["is_original_node"] is True]
    return (float(np.mean(xs)), float(np.mean(ys)))


def _core_boundary_radius(
    graph: ig.Graph, core_centroid: tuple[float, float], percentile: int = CORE_BOUNDARY_PERCENTILE
) -> float:
    cx, cy = core_centroid
    distances = [
        math.hypot(v["x"] - cx, v["y"] - cy) for v in graph.vs if v["is_original_node"] is True
    ]
    return float(np.percentile(distances, percentile))


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


def _seed_position(
    anchor_centroid: tuple[float, float],
    core_centroid: tuple[float, float],
    core_degree: int,
    core_boundary_radius: float,
    min_core_degree: int,
    max_core_degree: int,
    doi: str,
) -> tuple[float, float]:
    cx, cy = core_centroid
    ax, ay = anchor_centroid
    dx, dy = ax - cx, ay - cy
    if dx == 0.0 and dy == 0.0:
        # Degenerate: anchors happen to average out to the core centroid
        # exactly. Fall back to a deterministic angle from the DOI so the
        # seed is still stable across reruns, rather than 0 for every such case.
        angle = 2 * math.pi * (hash(doi) % 1000) / 1000
    else:
        angle = math.atan2(dy, dx)

    if max_core_degree > min_core_degree:
        hub_score = (math.log(core_degree) - math.log(min_core_degree)) / (
            math.log(max_core_degree) - math.log(min_core_degree)
        )
        hub_score = min(max(hub_score, 0.0), 1.0)
    else:
        hub_score = 0.0

    radius = core_boundary_radius * (RADIUS_NEAR + RADIUS_SPREAD * (1 - hub_score))
    return (cx + radius * math.cos(angle), cy + radius * math.sin(angle))


def _seed_positions(graph: ig.Graph) -> dict[str, tuple[float, float]]:
    core_centroid = _core_centroid(graph)
    core_boundary_radius = _core_boundary_radius(graph, core_centroid)
    anchor_stats = _core_anchor_stats(graph)
    core_degrees = [stats["core_degree"] for stats in anchor_stats.values()]
    min_core_degree, max_core_degree = min(core_degrees), max(core_degrees)

    return {
        doi: _seed_position(
            stats["centroid"],
            core_centroid,
            stats["core_degree"],
            core_boundary_radius,
            min_core_degree,
            max_core_degree,
            doi,
        )
        for doi, stats in anchor_stats.items()
    }


def _relaxed_neighbor_positions(
    graph: ig.Graph,
    seed_positions: dict[str, tuple[float, float]],
    iterations: int = RELAXATION_ITERATIONS,
    seed: int = RELAXATION_SEED,
) -> dict[str, tuple[float, float]]:
    """Force-directed relaxation with the core frozen in place. Core-core
    edges are dropped from the relaxation graph -- both endpoints are fixed,
    so they can't move and contribute no force to anything that can."""
    is_core = {v["name"]: v["is_original_node"] is True for v in graph.vs}
    core_positions = {
        name: (v["x"], v["y"]) for v in graph.vs for name in [v["name"]] if is_core[name]
    }

    relaxation_graph = nx.Graph()
    relaxation_graph.add_nodes_from(is_core.keys())
    for e in graph.es:
        source_name = graph.vs[e.source]["name"]
        target_name = graph.vs[e.target]["name"]
        if is_core[source_name] and is_core[target_name]:
            continue
        relaxation_graph.add_edge(source_name, target_name)

    initial_positions = {**core_positions, **seed_positions}
    fixed_nodes = list(core_positions.keys())

    new_positions = nx.spring_layout(
        relaxation_graph,
        pos=initial_positions,
        fixed=fixed_nodes,
        iterations=iterations,
        seed=seed,
    )
    return {doi: tuple(new_positions[doi]) for doi in seed_positions}


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
