import math

import numpy as np
import pytest
import igraph as ig

from motor_learning_network.topic_community_analysis import (
    UnionFind,
    _normalized_entropy,
    _find_disconnected_pairs,
    _compute_topic_community_metrics,
)


# ── normalized_entropy ────────────────────────────────────────────────────────
def test_entropy_single_bucket_is_zero():
    norm, eff = _normalized_entropy([10])
    assert norm == 0.0
    assert eff == 1.0


def test_entropy_even_split_is_maximal():
    # k evenly filled buckets -> normalized entropy 1, effective number k.
    norm, eff = _normalized_entropy([5, 5, 5, 5])
    assert norm == pytest.approx(1.0)
    assert eff == pytest.approx(4.0)


def test_entropy_skewed_between_zero_and_one():
    norm, eff = _normalized_entropy([90, 10])
    assert 0.0 < norm < 1.0
    assert 1.0 < eff < 2.0


# ── UnionFind ─────────────────────────────────────────────────────────────────
def test_unionfind_merges_transitively():
    uf = UnionFind()
    uf.union(0, 1)
    uf.union(1, 2)
    assert uf.find(0) == uf.find(2)
    # A separate pair stays in its own component.
    uf.union(4, 5)
    assert uf.find(0) != uf.find(4)
    assert uf.find(4) == uf.find(5)


# ── find_disconnected_pairs ───────────────────────────────────────────────────
def test_disconnected_pair_flagged_when_expected_high_observed_zero():
    # Two substantial communities (0 and 1), high internal degree, zero edges
    # between them -> flagged. e_int large so expected = deg_a*deg_b/(2*e_int)
    # clears PAIR_MIN_EXPECTED=3.0.
    counts = {0: 40, 1: 40}
    deg = {0: 60, 1: 60}
    pair_edges = {}          # observed zero between (0, 1)
    e_int = 100
    flagged = _find_disconnected_pairs(counts, deg, pair_edges, e_int, names={})
    assert len(flagged) == 1
    p = flagged[0]
    assert (p["community_a"], p["community_b"]) == (0, 1)
    assert p["observed_edges"] == 0
    assert p["expected_edges"] == pytest.approx(60 * 60 / (2 * 100), abs=0.1)


def test_disconnected_pair_not_flagged_when_well_connected():
    # Same setup but observed edges above PAIR_MAX_RATIO * expected -> not a gap.
    counts = {0: 40, 1: 40}
    deg = {0: 60, 1: 60}
    expected = 60 * 60 / (2 * 100)          # = 18
    pair_edges = {(0, 1): int(expected)}    # observed >> 0.20 * expected
    flagged = _find_disconnected_pairs(counts, deg, pair_edges, e_int=100, names={})
    assert flagged == []


def test_disconnected_pair_ignores_small_communities():
    # Communities below PAIR_MIN_PAPERS (15) are never paired.
    counts = {0: 5, 1: 5}
    deg = {0: 60, 1: 60}
    flagged = _find_disconnected_pairs(counts, deg, {}, e_int=100, names={})
    assert flagged == []


# ── compute_topic_community_metrics (node-level, on a synthetic igraph) ────────
@pytest.fixture
def two_community_topic_graph():
    """One topic (0) split into two citation communities (0 and 1) that are each
    internally connected (a triangle) but do NOT cite each other.

        community 0: 0-1-2 (triangle)      community 1: 3-4-5 (triangle)
    All six papers share topic 0. No edge crosses the two communities.
    """
    g = ig.Graph()
    g.add_vertices(6)
    g.vs["topic"] = [0, 0, 0, 0, 0, 0]
    g.vs["cpm_communities_at_res=0.005"] = [0, 0, 0, 1, 1, 1]
    g.add_edges([(0, 1), (1, 2), (0, 2),      # community 0 triangle
                 (3, 4), (4, 5), (3, 5)])     # community 1 triangle
    return g


def test_metrics_two_community_topic(two_community_topic_graph):
    g = two_community_topic_graph
    topic = np.array(g.vs["topic"])
    community = np.array(g.vs["cpm_communities_at_res=0.005"])
    metrics, disconnected = _compute_topic_community_metrics(g, topic, community)

    m = metrics["0"]
    assert m["n_papers"] == 6
    assert m["n_communities"] == 2
    # Two equal communities -> even split -> entropy ~1, effective ~2.
    assert m["community_entropy"] == pytest.approx(1.0, abs=1e-3)
    assert m["effective_n_communities"] == pytest.approx(2.0, abs=1e-2)
    assert m["dominant_share"] == pytest.approx(0.5)
    # Every edge is internal to the topic (no boundary edges).
    assert m["internal_edges"] == 6
    assert m["boundary_edges"] == 0
    assert m["internal_edge_ratio"] == pytest.approx(1.0)
    # The two triangles are disconnected from each other -> LCC is 3 of 6.
    assert m["lcc_fraction"] == pytest.approx(0.5)
    # No edge bridges the communities -> zero cross-community, integration 0.
    assert m["cross_community_edges"] == 0
    assert m["cross_community_edge_ratio"] == 0.0
    assert m["community_integration"] == 0.0


def test_metrics_single_community_integration_is_none():
    # One topic entirely inside one community -> integration undefined (None).
    g = ig.Graph()
    g.add_vertices(4)
    g.vs["topic"] = [0, 0, 0, 0]
    g.vs["cpm_communities_at_res=0.005"] = [7, 7, 7, 7]
    g.add_edges([(0, 1), (1, 2), (2, 3)])
    topic = np.array(g.vs["topic"])
    community = np.array(g.vs["cpm_communities_at_res=0.005"])
    metrics, _ = _compute_topic_community_metrics(g, topic, community)
    m = metrics["0"]
    assert m["n_communities"] == 1
    assert m["community_integration"] is None
    # A single 4-node path is one connected component.
    assert m["lcc_fraction"] == pytest.approx(1.0)
