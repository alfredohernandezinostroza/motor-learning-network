import math

import igraph as ig
import pytest

from motor_learning_network.layout_expanded_citation_network import (
    _anchor_angle,
    _apply_neighbor_positions,
    _boundary_radius_at_angle,
    _core_anchor_stats,
    _core_boundary_radii_by_angle,
    _core_centroid,
    _relaxed_neighbor_positions,
    _rescale_to_match,
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


# ── _core_centroid ─────────────────────────────────────────────────────────────
def test_core_centroid_averages_only_core_vertices(tiny_expanded_graph):
    assert _core_centroid(tiny_expanded_graph) == (5.0, 0.0)


# ── _core_boundary_radii_by_angle / _boundary_radius_at_angle ─────────────────
def test_core_boundary_radii_by_angle_matches_distance_for_single_direction():
    g = ig.Graph(directed=True)
    g.add_vertices(2)
    g.vs["name"] = ["a", "b"]
    g.vs["is_original_node"] = [True, True]
    g.vs["x"] = [10.0, 0.0]
    g.vs["y"] = [0.0, 0.0]

    radii = _core_boundary_radii_by_angle(g, core_centroid=(5.0, 0.0), num_bins=4, percentile=95)
    # both core vertices sit on the x-axis, 5 units from the centroid, in
    # opposite bins -- every bin's radius should resolve to ~5 (empty bins
    # fill from the nearest non-empty one)
    assert all(r == pytest.approx(5.0) for r in radii)


def test_core_boundary_radii_by_angle_caps_a_lone_far_spoke():
    # 35 core points clustered around distance 100 (spread across bins so
    # each bin's own percentile is ~100), plus one lone spoke of points at
    # distance 10,000 concentrated in a single bin -- that bin's raw 95th
    # percentile would be ~10,000 (a ~100x outlier); capped, it should land
    # at CORE_BOUNDARY_RADIUS_CAP_MULTIPLE (2.0) x the ~100 median.
    g = ig.Graph(directed=True)
    n_typical = 72
    n_spoke = 10
    g.add_vertices(n_typical + n_spoke)
    g.vs["name"] = [f"c{i}" for i in range(n_typical + n_spoke)]
    g.vs["is_original_node"] = [True] * (n_typical + n_spoke)

    xs, ys = [], []
    for i in range(n_typical):
        angle = 2 * math.pi * i / n_typical
        xs.append(100 * math.cos(angle))
        ys.append(100 * math.sin(angle))
    for _ in range(n_spoke):
        xs.append(10000.0)  # all on the positive x-axis -> one bin
        ys.append(0.0)
    g.vs["x"] = xs
    g.vs["y"] = ys

    radii = _core_boundary_radii_by_angle(g, core_centroid=(0.0, 0.0), num_bins=36, percentile=95)
    assert max(radii) < 1000  # nowhere near the raw ~10,000 outlier
    assert max(radii) == pytest.approx(2.0 * 100, rel=0.2)


def test_boundary_radius_at_angle_picks_the_right_bin():
    # 4 bins over [-pi, pi): bin 0 = [-180,-90)deg, bin1 = [-90,0)deg,
    # bin2 = [0,90)deg, bin3 = [90,180)deg.
    radii = [1.0, 2.0, 3.0, 4.0]
    assert _boundary_radius_at_angle(0.0, radii) == 3.0
    assert _boundary_radius_at_angle(-math.pi / 2, radii) == 2.0


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


# ── _anchor_angle ──────────────────────────────────────────────────────────────
def test_anchor_angle_points_toward_anchor():
    angle = _anchor_angle(anchor_centroid=(0.0, 10.0), core_centroid=(0.0, 0.0), doi="x")
    assert angle == pytest.approx(math.pi / 2)


def test_anchor_angle_degenerate_case_is_deterministic():
    kwargs = dict(anchor_centroid=(0.0, 0.0), core_centroid=(0.0, 0.0), doi="same-doi-both-times")
    assert _anchor_angle(**kwargs) == _anchor_angle(**kwargs)


# ── _seed_position ─────────────────────────────────────────────────────────────
def test_seed_position_places_hub_closer_than_niche():
    hub_pos = _seed_position(
        angle=0.0,
        core_centroid=(0.0, 0.0),
        core_degree=100,
        boundary_radius=10.0,
        min_core_degree=5,
        max_core_degree=100,
    )
    niche_pos = _seed_position(
        angle=0.0,
        core_centroid=(0.0, 0.0),
        core_degree=5,
        boundary_radius=10.0,
        min_core_degree=5,
        max_core_degree=100,
    )
    assert math.hypot(*hub_pos) < math.hypot(*niche_pos)


def test_seed_position_uses_given_boundary_radius():
    # min_core_degree == max_core_degree -> hub_score forced to 0 -> the
    # spread's full (RADIUS_NEAR + RADIUS_SPREAD) multiplier applies.
    x, y = _seed_position(
        angle=0.0,
        core_centroid=(0.0, 0.0),
        core_degree=5,
        boundary_radius=10.0,
        min_core_degree=5,
        max_core_degree=5,
    )
    assert math.hypot(x, y) == pytest.approx(10.0 * (1.05 + 0.6))


# ── _seed_positions ────────────────────────────────────────────────────────────
def test_seed_positions_covers_every_neighbor(tiny_expanded_graph):
    positions = _seed_positions(tiny_expanded_graph)
    assert set(positions.keys()) == {"n1", "n2"}


def test_seed_positions_places_neighbors_outside_core_extent(tiny_expanded_graph):
    positions = _seed_positions(tiny_expanded_graph)
    core_centroid = _core_centroid(tiny_expanded_graph)
    core_max_extent = 5.0  # both core points are 5 units from the centroid
    for x, y in positions.values():
        distance = math.hypot(x - core_centroid[0], y - core_centroid[1])
        assert distance > core_max_extent


# ── _rescale_to_match ──────────────────────────────────────────────────────────
def test_rescale_to_match_preserves_reference_center_and_spread():
    reference = {"a": (100.0, 0.0), "b": (-100.0, 0.0)}  # center (0,0), spread 100
    target = {"a": (1.0, 0.0), "b": (-1.0, 0.0)}  # center (0,0), spread 1 -- collapsed

    rescaled = _rescale_to_match(reference, target)

    center_x = sum(p[0] for p in rescaled.values()) / len(rescaled)
    spread = sum(math.hypot(p[0] - center_x, p[1]) for p in rescaled.values()) / len(rescaled)
    assert center_x == pytest.approx(0.0, abs=1e-9)
    assert spread == pytest.approx(100.0)


def test_rescale_to_match_handles_zero_spread_target():
    reference = {"a": (10.0, 0.0), "b": (-10.0, 0.0)}
    target = {"a": (5.0, 5.0), "b": (5.0, 5.0)}  # both at the same point -> zero spread
    rescaled = _rescale_to_match(reference, target)  # must not raise (division by zero)
    assert set(rescaled.keys()) == {"a", "b"}


# ── _relaxed_neighbor_positions ────────────────────────────────────────────────
def test_relaxed_neighbor_positions_covers_every_neighbor(tiny_expanded_graph):
    seeds = _seed_positions(tiny_expanded_graph)
    relaxed = _relaxed_neighbor_positions(tiny_expanded_graph, seeds, iterations=5)
    assert set(relaxed.keys()) == {"n1", "n2"}


def test_relaxed_neighbor_positions_is_deterministic(tiny_expanded_graph):
    seeds = _seed_positions(tiny_expanded_graph)
    first = _relaxed_neighbor_positions(tiny_expanded_graph, seeds, iterations=5, seed=42)
    second = _relaxed_neighbor_positions(tiny_expanded_graph, seeds, iterations=5, seed=42)
    assert first == second


def test_relaxed_neighbor_positions_does_not_collapse_scale(tiny_expanded_graph):
    # Regression test: networkx.spring_layout rescales its output to a unit
    # circle whenever `fixed` isn't set, which silently collapsed the seed's
    # real-world coordinates (thousands of units) down to ~1 unit -- making
    # every neighbor land essentially on top of the core. The relaxed
    # positions' spread from centroid must stay the same order of magnitude
    # as the seed's, not shrink by orders of magnitude.
    seeds = _seed_positions(tiny_expanded_graph)
    relaxed = _relaxed_neighbor_positions(tiny_expanded_graph, seeds, iterations=5)

    def _spread(positions):
        cx = sum(p[0] for p in positions.values()) / len(positions)
        cy = sum(p[1] for p in positions.values()) / len(positions)
        return sum(math.hypot(p[0] - cx, p[1] - cy) for p in positions.values()) / len(positions)

    seed_spread = _spread(seeds)
    relaxed_spread = _spread(relaxed)
    assert relaxed_spread == pytest.approx(seed_spread, rel=0.5)


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
