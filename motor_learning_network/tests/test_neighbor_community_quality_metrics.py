import igraph as ig
import numpy as np

from motor_learning_network.community_quality_metrics import _resolution_plateau_flags
from motor_learning_network.neighbor_community_quality_metrics import (
    citation_network_with_community_metrics,
    community_membership_for_resolution,
    community_quality_metrics_for_resolution,
    parallel_edge_count,
    reciprocal_edge_pair_count,
    self_loop_count,
    undirected_networkx_graph,
)


def _tiny_neighbor_graph() -> ig.Graph:
    """Two triangles (communities 0 and 1 at res=0.1) joined by one bridge,
    with community columns already attached the way detect_neighbor_communities.py
    would leave them."""
    g = ig.Graph(directed=True)
    g.add_vertices(6)
    g.vs["name"] = [f"n{i}" for i in range(6)]
    g.add_edges([(0, 1), (1, 2), (2, 0), (3, 4), (4, 5), (5, 3), (2, 3)])
    g.vs["cpm_communities_at_res=0.1"] = [0, 0, 0, 1, 1, 1]
    g.vs["cpm_communities_at_res=0.2"] = [0, 0, 0, 1, 1, 1]
    return g


def test_this_module_reuses_community_quality_metrics_end_to_end():
    """Wires the reused helpers together on a tiny graph -- the internals
    (conductance, surprise, significance, ...) are already covered by
    test_community_quality_metrics.py; this just checks the reuse/wiring in
    this module actually produces a coherent result."""
    graph = _tiny_neighbor_graph()
    undirected = undirected_networkx_graph(graph)

    membership_01 = community_membership_for_resolution(graph, 0.1)
    membership_02 = community_membership_for_resolution(graph, 0.2)
    assert list(membership_01) == [0, 0, 0, 1, 1, 1]

    # this module's `resolution_plateau_flags` node always zips against the
    # full 36-value RESOLUTIONS, so exercise the underlying (reused) private
    # helper directly here with a resolutions list matching this tiny fixture.
    plateau = _resolution_plateau_flags([membership_01, membership_02], [0.1, 0.2], threshold=0.9)
    assert plateau[0.1]["is_on_resolution_plateau"] is True  # identical partitions

    bundle = community_quality_metrics_for_resolution(
        citation_network=graph,
        undirected_networkx_graph=undirected,
        resolution=0.1,
        community_membership=membership_01,
        resolution_plateau_flags=plateau,
        n_iterations=2,
        stability_seeds=[1],
    )
    assert bundle["per_partition"]["number_of_communities"] == 2
    assert len(bundle["per_community"]) == 2
    for community in bundle["per_community"]:
        assert community["community_size"] == 3
        assert community["internal_directed_edge_count"] == 3  # each triangle

    scored_graph = citation_network_with_community_metrics(
        citation_network=graph,
        community_quality_metrics_all_resolutions=[bundle],
        community_memberships_by_resolution=[membership_01],
        reciprocal_edge_pair_count=reciprocal_edge_pair_count(graph),
        parallel_edge_count=parallel_edge_count(graph),
        self_loop_count=self_loop_count(graph),
    )
    assert "community_size_at_res=0.1" in scored_graph.vs.attributes()
    assert scored_graph.vs["community_size_at_res=0.1"] == [3, 3, 3, 3, 3, 3]
    assert np.isclose(scored_graph["modularity_at_res=0.1"], bundle["per_partition"]["modularity"])
