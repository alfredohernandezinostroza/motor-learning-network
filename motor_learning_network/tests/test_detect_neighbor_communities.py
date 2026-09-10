import cdlib
import igraph as ig
import pandas as pd
import pytest

from motor_learning_network.detect_neighbor_communities import (
    HIGH_RESOLUTIONS,
    LOW_RESOLUTIONS,
    MID_RESOLUTIONS,
    RESOLUTIONS,
    _attach_communities,
    _communities_per_resolution_df,
    _leiden_cpm_communities,
    _neighbor_subgraph,
)


# ── resolution bands ───────────────────────────────────────────────────────────
def test_resolution_bands_match_the_three_graphml_files():
    assert LOW_RESOLUTIONS == [0.001, 0.002, 0.003, 0.004, 0.005, 0.006, 0.007, 0.008, 0.009]
    assert MID_RESOLUTIONS == [round(0.01 * i, 3) for i in range(1, 20)]
    assert MID_RESOLUTIONS[0] == 0.01
    assert MID_RESOLUTIONS[-1] == 0.19
    assert HIGH_RESOLUTIONS == [0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    assert len(RESOLUTIONS) == 36
    assert RESOLUTIONS == LOW_RESOLUTIONS + MID_RESOLUTIONS + HIGH_RESOLUTIONS


# ── _neighbor_subgraph ─────────────────────────────────────────────────────────
@pytest.fixture
def tiny_expanded_graph() -> ig.Graph:
    """core-a, core-b (core) and n1, n2, n3 (neighbors). Edges: core-a->n1
    (core->neighbor, must be dropped), n1->n2, n2->n3 (neighbor->neighbor,
    must be kept), n3->core-b (neighbor->core, must be dropped)."""
    g = ig.Graph(directed=True)
    g.add_vertices(5)
    g.vs["name"] = ["core-a", "core-b", "n1", "n2", "n3"]
    g.vs["is_original_node"] = [True, True, False, False, False]
    g.add_edges([(0, 2), (2, 3), (3, 4), (4, 1)])
    return g


def test_neighbor_subgraph_keeps_only_neighbor_vertices(tiny_expanded_graph):
    sub = _neighbor_subgraph(tiny_expanded_graph)
    assert set(sub.vs["name"]) == {"n1", "n2", "n3"}


def test_neighbor_subgraph_keeps_only_neighbor_neighbor_edges(tiny_expanded_graph):
    sub = _neighbor_subgraph(tiny_expanded_graph)
    name = sub.vs["name"]
    edges = {(name[e.source], name[e.target]) for e in sub.es}
    assert edges == {("n1", "n2"), ("n2", "n3")}


# ── _leiden_cpm_communities ────────────────────────────────────────────────────
def test_leiden_cpm_communities_returns_node_clustering_covering_all_vertices():
    g = ig.Graph(directed=True)
    g.add_vertices(6)
    g.vs["name"] = [f"n{i}" for i in range(6)]
    g.add_edges([(0, 1), (1, 2), (2, 0), (3, 4), (4, 5), (5, 3)])  # two triangles

    result = _leiden_cpm_communities(g, resolution=0.1, n_iterations=10, seed=0)

    assert isinstance(result, cdlib.NodeClustering)
    covered = {doi for community in result.communities for doi in community}
    assert covered == set(g.vs["name"])


def test_leiden_cpm_communities_is_deterministic_given_a_seed():
    g = ig.Graph(directed=True)
    g.add_vertices(6)
    g.vs["name"] = [f"n{i}" for i in range(6)]
    g.add_edges([(0, 1), (1, 2), (2, 0), (3, 4), (4, 5), (5, 3)])

    first = _leiden_cpm_communities(g, resolution=0.1, n_iterations=10, seed=0)
    second = _leiden_cpm_communities(g, resolution=0.1, n_iterations=10, seed=0)
    assert sorted(map(sorted, first.communities)) == sorted(map(sorted, second.communities))


# ── _communities_per_resolution_df ─────────────────────────────────────────────
def test_communities_per_resolution_df_shapes_one_column_per_resolution():
    g = ig.Graph(directed=True)
    g.add_vertices(4)
    g.vs["name"] = ["a", "b", "c", "d"]
    g.add_edges([(0, 1), (2, 3)])

    low = _leiden_cpm_communities(g, resolution=0.01, n_iterations=10, seed=0)
    high = _leiden_cpm_communities(g, resolution=0.5, n_iterations=10, seed=0)

    df = _communities_per_resolution_df([low, high])

    assert set(df.columns) == {"cpm_communities_at_res=0.01", "cpm_communities_at_res=0.5"}
    assert set(df.index) == {"a", "b", "c", "d"}


# ── _attach_communities ─────────────────────────────────────────────────────────
def test_attach_communities_sets_vertex_attributes_in_vertex_order():
    g = ig.Graph(directed=True)
    g.add_vertices(3)
    g.vs["name"] = ["b", "a", "c"]  # deliberately not sorted
    communities_df = pd.DataFrame(
        {"cpm_communities_at_res=0.1": [10, 20, 30]}, index=["a", "b", "c"]
    )

    g = _attach_communities(g, communities_df)

    values_by_name = dict(zip(g.vs["name"], g.vs["cpm_communities_at_res=0.1"]))
    assert values_by_name == {"a": 10, "b": 20, "c": 30}
