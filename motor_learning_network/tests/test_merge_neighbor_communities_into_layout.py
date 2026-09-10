import igraph as ig
import pandas as pd
import pytest

from motor_learning_network.merge_neighbor_communities_into_layout import (
    _merge_neighbor_graph_attributes,
    _merge_neighbor_vertex_attributes,
    _neighbor_graph_level_attributes,
    _neighbor_vertex_metrics_df,
)


@pytest.fixture
def tiny_base_graph() -> ig.Graph:
    """Core + neighbor vertices as they'd sit on the laid-out expanded
    graph: core already has its own (unrelated) community assignment at the
    same resolution value, which must survive untouched."""
    g = ig.Graph(directed=True)
    g.add_vertices(3)
    g.vs["name"] = ["core-a", "n1", "n2"]
    g.vs["is_original_node"] = [True, False, False]
    g.vs["x"] = [0.0, 1.0, 2.0]
    g.vs["cpm_communities_at_res=0.003"] = [7, None, None]  # core's own (unrelated) partition
    return g


@pytest.fixture
def tiny_neighbor_graph() -> ig.Graph:
    g = ig.Graph(directed=True)
    g.add_vertices(2)
    g.vs["name"] = ["n1", "n2"]
    g.vs["cpm_communities_at_res=0.003"] = [0, 0]
    g.vs["community_size_at_res=0.003"] = [2, 2]
    g.vs["conductance_at_res=0.003"] = [0.1, 0.1]
    g.vs["conductance_out_at_res=0.003"] = [0.05, 0.05]
    g.vs["conductance_in_at_res=0.003"] = [0.05, 0.05]
    g.vs["internal_edge_density_at_res=0.003"] = [0.5, 0.5]
    g.vs["internal_edge_surprise_at_res=0.003"] = [1.2, 1.2]
    g.vs["internal_directed_edge_count_at_res=0.003"] = [1, 1]
    g.vs["boundary_edge_count_at_res=0.003"] = [0, 0]
    g["modularity_at_res=0.003"] = 0.42
    g["number_of_communities_at_res=0.003"] = 1
    g["some_unrelated_graph_attribute"] = "not resolution-tagged, must not transfer"
    return g


# ── _neighbor_vertex_metrics_df ────────────────────────────────────────────────
def test_neighbor_vertex_metrics_df_renames_and_indexes_by_doi(tiny_neighbor_graph):
    df = _neighbor_vertex_metrics_df(tiny_neighbor_graph, resolutions=[0.003])
    assert set(df.index) == {"n1", "n2"}
    assert "neighbor_cpm_communities_at_res=0.003" in df.columns
    assert "neighbor_conductance_at_res=0.003" in df.columns
    assert df.loc["n1", "neighbor_cpm_communities_at_res=0.003"] == 0
    assert df.loc["n1", "neighbor_conductance_at_res=0.003"] == 0.1


# ── _merge_neighbor_vertex_attributes ──────────────────────────────────────────
def test_merge_neighbor_vertex_attributes_leaves_core_untouched_and_nan(
    tiny_base_graph, tiny_neighbor_graph
):
    metrics_df = _neighbor_vertex_metrics_df(tiny_neighbor_graph, resolutions=[0.003])
    merged = _merge_neighbor_vertex_attributes(tiny_base_graph, metrics_df)

    # core's own (unrelated) partition column is untouched
    community_by_name = dict(zip(merged.vs["name"], merged.vs["cpm_communities_at_res=0.003"]))
    assert community_by_name["core-a"] == 7

    # core gets nan on the new neighbor_* column; neighbors get their own value
    neighbor_col_by_name = dict(
        zip(merged.vs["name"], merged.vs["neighbor_cpm_communities_at_res=0.003"])
    )
    assert pd.isna(neighbor_col_by_name["core-a"])
    assert neighbor_col_by_name["n1"] == 0
    assert neighbor_col_by_name["n2"] == 0


def test_merge_neighbor_vertex_attributes_transfers_all_metric_columns(
    tiny_base_graph, tiny_neighbor_graph
):
    metrics_df = _neighbor_vertex_metrics_df(tiny_neighbor_graph, resolutions=[0.003])
    merged = _merge_neighbor_vertex_attributes(tiny_base_graph, metrics_df)

    for metric in [
        "community_size",
        "conductance",
        "conductance_out",
        "conductance_in",
        "internal_edge_density",
        "internal_edge_surprise",
        "internal_directed_edge_count",
        "boundary_edge_count",
    ]:
        assert f"neighbor_{metric}_at_res=0.003" in merged.vs.attributes()


# ── _neighbor_graph_level_attributes / _merge_neighbor_graph_attributes ───────
def test_neighbor_graph_level_attributes_only_transfers_resolution_tagged(tiny_neighbor_graph):
    attrs = _neighbor_graph_level_attributes(tiny_neighbor_graph, resolutions=[0.003])
    assert attrs == {
        "neighbor_modularity_at_res=0.003": 0.42,
        "neighbor_number_of_communities_at_res=0.003": 1,
    }
    assert "neighbor_some_unrelated_graph_attribute" not in attrs


def test_merge_neighbor_graph_attributes_sets_them_on_base_graph(
    tiny_base_graph, tiny_neighbor_graph
):
    attrs = _neighbor_graph_level_attributes(tiny_neighbor_graph, resolutions=[0.003])
    merged = _merge_neighbor_graph_attributes(tiny_base_graph, attrs)
    assert merged["neighbor_modularity_at_res=0.003"] == 0.42
    assert merged["neighbor_number_of_communities_at_res=0.003"] == 1
