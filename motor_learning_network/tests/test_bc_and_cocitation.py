from motor_learning_network.build_bcp_and_cocitation import bibliographic_coupling_network, cocitation_network
import igraph as ig
import pytest
import numpy as np

@pytest.fixture
def citation_network():
    return ig.Graph.Adjacency( [[0, 0, 1, 1],
                                [1, 0, 0, 1],
                                [0, 1, 1, 1],
                                [0, 0, 1, 1]], mode='directed', loops=False)

                                       
def test_bibliographic_coupling(citation_network: ig.Graph):
    A = np.array(citation_network.get_adjacency().data)
    bc_weights = citation_network.bibcoupling()
    bc_network = ig.Graph.Weighted_Adjacency(bc_weights, mode="undirected", loops=False)
    B = np.array(bc_network.get_adjacency().data)
    expected_B = A @ A.T #definition of bibliographic coupling adjacency matrix
    np.fill_diagonal(expected_B, 0)
    print(f"A = {A}")
    print(f"B = {B}")
    print(f"expected_B = {expected_B}")
    assert np.array_equal(B, expected_B)

def test_cocitation(citation_network: ig.Graph):
    A = np.array(citation_network.get_adjacency().data)
    cocitation_weights = citation_network.cocitation()
    cocitation_network = ig.Graph.Weighted_Adjacency(cocitation_weights, mode="undirected", loops=False)
    C = np.array(cocitation_network.get_adjacency().data)
    expected_C = A.T @ A #definition of cocitation adjacency matrix
    np.fill_diagonal(expected_C, 0)
    print(f"A = {A}")
    print(f"C = {C}")
    print(f"expected_C = {expected_C}")
    assert np.array_equal(C, expected_C)

