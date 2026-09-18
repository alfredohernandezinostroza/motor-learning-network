import igraph as ig
import pandas as pd
import pytest

from motor_learning_network.build_expanded_citation_network import (
    _add_neighbor_edges,
    _add_neighbor_vertices,
    _neighbor_edges_from_references,
)


@pytest.fixture
def tiny_base_graph() -> ig.Graph:
    """Two original papers, a->b, with the same shape of attributes as the
    real graph (metadata columns + a layout column that new vertices should
    NOT receive)."""
    g = ig.Graph(directed=True)
    g.add_vertices(2)
    g.vs["name"] = ["a", "b"]
    g.vs["title"] = ["Paper A", "Paper B"]
    g.vs["x"] = [1.0, 2.0]
    g.add_edges([(0, 1)])
    return g


@pytest.fixture
def neighbor_metadata_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "doi": ["external-1", "external-2"],
            "title": ["External Paper 1", "External Paper 2"],
            "authors": ["Doe, Jane", "Smith, John"],
            "abstract": ["", ""],
            "openalex_topics": ["", ""],
            "journal": ["Some Journal", "Another Journal"],
            "source_database": ["OpenAlex", "OpenAlex"],
            "year": [2020, 2021],
        }
    )


# ── _add_neighbor_vertices ────────────────────────────────────────────────────
def test_add_neighbor_vertices_appends_expected_count(tiny_base_graph, neighbor_metadata_df):
    graph = _add_neighbor_vertices(tiny_base_graph, neighbor_metadata_df)
    assert graph.vcount() == 4
    assert set(graph.vs["name"]) == {"a", "b", "external-1", "external-2"}


def test_add_neighbor_vertices_marks_is_original_node(tiny_base_graph, neighbor_metadata_df):
    graph = _add_neighbor_vertices(tiny_base_graph, neighbor_metadata_df)
    is_original_by_name = dict(zip(graph.vs["name"], graph.vs["is_original_node"]))
    assert is_original_by_name == {"a": True, "b": True, "external-1": False, "external-2": False}


def test_add_neighbor_vertices_carries_metadata_but_not_layout(
    tiny_base_graph, neighbor_metadata_df
):
    graph = _add_neighbor_vertices(tiny_base_graph, neighbor_metadata_df)
    title_by_name = dict(zip(graph.vs["name"], graph.vs["title"]))
    assert title_by_name["external-1"] == "External Paper 1"
    assert title_by_name["external-2"] == "External Paper 2"
    # original vertices' existing attributes are untouched
    assert title_by_name["a"] == "Paper A"
    # layout column was never set for new vertices (igraph fills with None)
    x_by_name = dict(zip(graph.vs["name"], graph.vs["x"]))
    assert x_by_name["external-1"] is None
    assert x_by_name["a"] == 1.0


# ── _neighbor_edges_from_references ───────────────────────────────────────────
def test_neighbor_edges_from_references_only_from_in_graph_citers():
    references_df = pd.DataFrame(
        {
            "citing_doi": ["a", "not-in-graph"],
            "cited_dois": [("external-1",), ("external-1",)],
        }
    )
    edges = _neighbor_edges_from_references(
        references_df, graph_dois={"a", "b"}, kept_external_dois={"external-1"}
    )
    assert edges == [("a", "external-1")]


def test_neighbor_edges_from_references_only_to_kept_external_dois():
    references_df = pd.DataFrame(
        {
            "citing_doi": ["a"],
            "cited_dois": [("external-1", "external-not-kept", "b")],
        }
    )
    edges = _neighbor_edges_from_references(
        references_df, graph_dois={"a", "b"}, kept_external_dois={"external-1"}
    )
    # external-not-kept is dropped (below threshold), b is dropped (already an in-graph edge)
    assert edges == [("a", "external-1")]


def test_neighbor_edges_from_references_normalizes_case():
    references_df = pd.DataFrame(
        {
            "citing_doi": ["a"],
            "cited_dois": [("EXTERNAL-1",)],
        }
    )
    edges = _neighbor_edges_from_references(
        references_df, graph_dois={"a"}, kept_external_dois={"external-1"}
    )
    assert edges == [("a", "external-1")]


def test_neighbor_edges_from_references_ignores_none_reference_lists():
    references_df = pd.DataFrame({"citing_doi": ["a"], "cited_dois": [None]})
    edges = _neighbor_edges_from_references(
        references_df, graph_dois={"a"}, kept_external_dois={"external-1"}
    )
    assert edges == []


# ── _add_neighbor_edges ────────────────────────────────────────────────────────
def test_add_neighbor_edges_adds_only_new_edges_to_neighbor_vertices(
    tiny_base_graph, neighbor_metadata_df
):
    graph = _add_neighbor_vertices(tiny_base_graph, neighbor_metadata_df)
    original_edge_count = graph.ecount()

    references_df = pd.DataFrame(
        {
            "citing_doi": ["a", "b"],
            "cited_dois": [("external-1", "b"), ("external-2", "not-kept")],
        }
    )
    graph = _add_neighbor_edges(
        graph,
        references_df,
        graph_dois={"a", "b"},
        kept_external_dois={"external-1", "external-2"},
    )

    assert graph.ecount() == original_edge_count + 2
    name = graph.vs["name"]
    new_edges = {(name[e.source], name[e.target]) for e in graph.es[original_edge_count:]}
    assert new_edges == {("a", "external-1"), ("b", "external-2")}


def test_add_neighbor_edges_no_op_when_no_matching_references(
    tiny_base_graph, neighbor_metadata_df
):
    graph = _add_neighbor_vertices(tiny_base_graph, neighbor_metadata_df)
    original_edge_count = graph.ecount()

    references_df = pd.DataFrame({"citing_doi": ["a"], "cited_dois": [("not-kept",)]})
    graph = _add_neighbor_edges(
        graph,
        references_df,
        graph_dois={"a", "b"},
        kept_external_dois={"external-1", "external-2"},
    )

    assert graph.ecount() == original_edge_count
