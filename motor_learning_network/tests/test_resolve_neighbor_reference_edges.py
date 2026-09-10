import igraph as ig
import pandas as pd

from motor_learning_network.resolve_neighbor_reference_edges import (
    _add_reference_edges,
    _build_openalex_id_index,
    _resolve_reference_edges,
)


# ── _build_openalex_id_index ──────────────────────────────────────────────────
def test_build_openalex_id_index_combines_neighbors_and_core():
    neighbor_metadata_df = pd.DataFrame({"doi": ["n1", "n2"], "openalex_id": ["W1", "W2"]})
    core_openalex_ids_df = pd.DataFrame({"doi": ["c1"], "openalex_id": ["W3"]})

    index = _build_openalex_id_index(neighbor_metadata_df, core_openalex_ids_df)

    assert index == {"W1": "n1", "W2": "n2", "W3": "c1"}


def test_build_openalex_id_index_drops_missing_openalex_id():
    neighbor_metadata_df = pd.DataFrame({"doi": ["n1"], "openalex_id": [None]})
    core_openalex_ids_df = pd.DataFrame({"doi": [], "openalex_id": []})

    index = _build_openalex_id_index(neighbor_metadata_df, core_openalex_ids_df)

    assert index == {}


# ── _resolve_reference_edges ──────────────────────────────────────────────────
def test_resolve_reference_edges_maps_to_neighbor_and_core():
    neighbor_metadata_df = pd.DataFrame(
        {
            "doi": ["n1", "n2"],
            "referenced_openalex_ids": [("W2", "Wc1", "Wunknown"), ()],
        }
    )
    openalex_id_to_doi = {"W2": "n2", "Wc1": "c1"}

    edges = _resolve_reference_edges(neighbor_metadata_df, openalex_id_to_doi)

    assert set(edges) == {("n1", "n2"), ("n1", "c1")}


def test_resolve_reference_edges_drops_self_loops():
    neighbor_metadata_df = pd.DataFrame({"doi": ["n1"], "referenced_openalex_ids": [("Wself",)]})
    openalex_id_to_doi = {"Wself": "n1"}

    edges = _resolve_reference_edges(neighbor_metadata_df, openalex_id_to_doi)

    assert edges == []


def test_resolve_reference_edges_ignores_unresolved_ids():
    neighbor_metadata_df = pd.DataFrame(
        {"doi": ["n1"], "referenced_openalex_ids": [("Wunresolved",)]}
    )
    edges = _resolve_reference_edges(neighbor_metadata_df, openalex_id_to_doi={})
    assert edges == []


# ── _add_reference_edges ──────────────────────────────────────────────────────
def test_add_reference_edges_adds_only_resolvable_edges():
    g = ig.Graph(directed=True)
    g.add_vertices(3)
    g.vs["name"] = ["n1", "n2", "c1"]
    original_edge_count = g.ecount()

    g = _add_reference_edges(g, [("n1", "n2"), ("n1", "c1"), ("n1", "not-in-graph")])

    assert g.ecount() == original_edge_count + 2
    name = g.vs["name"]
    new_edges = {(name[e.source], name[e.target]) for e in g.es[original_edge_count:]}
    assert new_edges == {("n1", "n2"), ("n1", "c1")}


def test_add_reference_edges_no_op_on_empty_list():
    g = ig.Graph(directed=True)
    g.add_vertices(1)
    g.vs["name"] = ["n1"]

    g = _add_reference_edges(g, [])

    assert g.ecount() == 0
