import math

import igraph as ig
import pytest

from motor_learning_network.layout_expanded_citation_network import (
    _apply_neighbor_positions,
    _core_anchor_stats,
    _core_boundary_radius,
    _core_centroid,
    _relaxed_neighbor_positions,
    _seed_position,
    _seed_positions,
)


@pytest.fixture
def tiny_expanded_graph() -> ig.Graph:
    """Two core papers (a square-ish layout) each cited by a shared neighbor
    n1 (a hub, 2 core anchors) and a lone neighbor n2 (niche, 1 core anchor),
    plus n1 -> n2 so the neighbors are connected to each other too."""
    g = ig.Graph(directed=True)
    g.add_vertices(4)
    g.vs["name"] = ["core-a", "core-b", "n1", "n2"]
    g.vs["is_original_node"] = [True, True, False, False]
    g.vs["x"] = [0.0, 10.0, None, None]
    g.vs["y"] = [0.0, 0.0, None, None]
    g.add_edges([(0, 2), (1, 2), (0, 3), (2, 3)])  # core-a->n1, core-b->n1, core-a->n2, n1->n2
    return g


# ── _core_centroid / _core_boundary_radius ────────────────────────────────────
def test_core_centroid_averages_only_core_vertices(tiny_expanded_graph):
    assert _core_centroid(tiny_expanded_graph) == (5.0, 0.0)


def test_core_boundary_radius_is_distance_from_centroid(tiny_expanded_graph):
    centroid = _core_centroid(tiny_expanded_graph)
    radius = _core_boundary_radius(tiny_expanded_graph, centroid, percentile=95)
    assert radius == pytest.approx(5.0)


# ── _core_anchor_stats ─────────────────────────────────────────────────────────
def test_core_anchor_stats_centroid_and_degree(tiny_expanded_graph):
    stats = _core_anchor_stats(tiny_expanded_graph)
    assert stats["n1"]["core_degree"] == 2
    assert stats["n1"]["centroid"] == (5.0, 0.0)  # midpoint of core-a and core-b
    assert stats["n2"]["core_degree"] == 1
    assert stats["n2"]["centroid"] == (0.0, 0.0)  # only core-a


def test_core_anchor_stats_excludes_core_vertices(tiny_expanded_graph):
    stats = _core_anchor_stats(tiny_expanded_graph)
    assert "core-a" not in stats
    assert "core-b" not in stats


# ── _seed_position ─────────────────────────────────────────────────────────────
def test_seed_position_places_hub_closer_than_niche():
    core_centroid = (0.0, 0.0)
    hub_pos = _seed_position(
        anchor_centroid=(10.0, 0.0),
        core_centroid=core_centroid,
        core_degree=100,
        core_boundary_radius=10.0,
        min_core_degree=5,
        max_core_degree=100,
        doi="hub",
    )
    niche_pos = _seed_position(
        anchor_centroid=(10.0, 0.0),
        core_centroid=core_centroid,
        core_degree=5,
        core_boundary_radius=10.0,
        min_core_degree=5,
        max_core_degree=100,
        doi="niche",
    )
    hub_radius = math.hypot(*hub_pos)
    niche_radius = math.hypot(*niche_pos)
    assert hub_radius < niche_radius


def test_seed_position_angle_points_toward_anchor():
    pos = _seed_position(
        anchor_centroid=(0.0, 10.0),  # straight "up" from the core centroid
        core_centroid=(0.0, 0.0),
        core_degree=5,
        core_boundary_radius=10.0,
        min_core_degree=5,
        max_core_degree=5,
        doi="x",
    )
    x, y = pos
    assert x == pytest.approx(0.0, abs=1e-9)
    assert y > 0


def test_seed_position_degenerate_case_is_deterministic():
    kwargs = dict(
        anchor_centroid=(0.0, 0.0),
        core_centroid=(0.0, 0.0),
        core_degree=5,
        core_boundary_radius=10.0,
        min_core_degree=5,
        max_core_degree=5,
        doi="same-doi-both-times",
    )
    assert _seed_position(**kwargs) == _seed_position(**kwargs)


# ── _seed_positions ────────────────────────────────────────────────────────────
def test_seed_positions_covers_every_neighbor(tiny_expanded_graph):
    positions = _seed_positions(tiny_expanded_graph)
    assert set(positions.keys()) == {"n1", "n2"}


# ── _relaxed_neighbor_positions ────────────────────────────────────────────────
def test_relaxed_neighbor_positions_keeps_core_fixed(tiny_expanded_graph):
    seeds = _seed_positions(tiny_expanded_graph)
    relaxed = _relaxed_neighbor_positions(tiny_expanded_graph, seeds, iterations=5)

    # only neighbors are returned -- core positions are never touched
    assert set(relaxed.keys()) == {"n1", "n2"}


def test_relaxed_neighbor_positions_is_deterministic(tiny_expanded_graph):
    seeds = _seed_positions(tiny_expanded_graph)
    first = _relaxed_neighbor_positions(tiny_expanded_graph, seeds, iterations=5, seed=42)
    second = _relaxed_neighbor_positions(tiny_expanded_graph, seeds, iterations=5, seed=42)
    assert first == second


# ── _apply_neighbor_positions ──────────────────────────────────────────────────
def test_apply_neighbor_positions_updates_only_named_vertices(tiny_expanded_graph):
    g = _apply_neighbor_positions(tiny_expanded_graph, {"n1": (1.0, 2.0)})
    x_by_name = dict(zip(g.vs["name"], g.vs["x"]))
    y_by_name = dict(zip(g.vs["name"], g.vs["y"]))
    assert (x_by_name["n1"], y_by_name["n1"]) == (1.0, 2.0)
    # core untouched
    assert (x_by_name["core-a"], y_by_name["core-a"]) == (0.0, 0.0)
    # n2 untouched (not in the positions dict passed in)
    assert x_by_name["n2"] is None
