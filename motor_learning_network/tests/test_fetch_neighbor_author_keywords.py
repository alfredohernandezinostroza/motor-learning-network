import igraph as ig
import pandas as pd
import pytest

from motor_learning_network.fetch_neighbor_author_keywords import (
    _author_keywords_df,
    _extract_author_keywords,
    _pmid_batches,
    _pmids_to_query,
    _pubmed_query_for_pmids,
    _split_openalex_topics_from_keywords,
)


# ── _pmids_to_query ─────────────────────────────────────────────────────────
def test_pmids_to_query_dedups_and_drops_missing():
    df = pd.DataFrame({"pubmed_id": ["123", "456", "123", None]})
    assert _pmids_to_query(df) == ["123", "456"]


# ── _pmid_batches ────────────────────────────────────────────────────────────
def test_pmid_batches_splits_into_fixed_size_chunks():
    batches = _pmid_batches(["1", "2", "3", "4", "5"], batch_size=2)
    assert batches == [["1", "2"], ["3", "4"], ["5"]]


# ── _pubmed_query_for_pmids ─────────────────────────────────────────────────
def test_pubmed_query_for_pmids_builds_or_clause():
    assert _pubmed_query_for_pmids(["123", "456"]) == "123[pmid] OR 456[pmid]"


# ── _extract_author_keywords ─────────────────────────────────────────────────
class _FakeArticle:
    def __init__(self, pubmed_id, keywords):
        self.pubmed_id = pubmed_id
        self.keywords = keywords


def test_extract_author_keywords_joins_with_pipe():
    articles = [_FakeArticle("123", ["Motor Learning", "Cerebellum"])]
    assert _extract_author_keywords(articles) == {"123": "Motor Learning|Cerebellum"}


def test_extract_author_keywords_skips_articles_without_keywords():
    articles = [_FakeArticle("123", []), _FakeArticle("456", None), _FakeArticle("789", ["X"])]
    assert _extract_author_keywords(articles) == {"789": "X"}


def test_extract_author_keywords_skips_articles_without_pmid():
    articles = [_FakeArticle(None, ["X"])]
    assert _extract_author_keywords(articles) == {}


# ── _author_keywords_df ────────────────────────────────────────────────────
def test_author_keywords_df_maps_by_pmid_and_flags_missing():
    neighbor_metadata_df = pd.DataFrame(
        {"doi": ["d1", "d2", "d3"], "pubmed_id": ["123", "456", None]}
    )
    author_keywords_by_pmid = {"123": "Motor Learning|Cerebellum"}
    df = _author_keywords_df(neighbor_metadata_df, author_keywords_by_pmid).set_index("doi")

    assert df.loc["d1", "author_keywords"] == "Motor Learning|Cerebellum"
    assert bool(df.loc["d1", "has_author_keywords"]) is True
    assert df.loc["d2", "author_keywords"] == ""
    assert bool(df.loc["d2", "has_author_keywords"]) is False
    assert df.loc["d3", "author_keywords"] == ""
    assert bool(df.loc["d3", "has_author_keywords"]) is False


# ── _split_openalex_topics_from_keywords ────────────────────────────────────
@pytest.fixture
def tiny_combined_graph() -> ig.Graph:
    """Core + neighbor vertices, as they'd sit on citation_network_expanded_with_layout.graphml:
    `keywords` currently holds real author keywords for core, OpenAlex topics for neighbors."""
    g = ig.Graph(directed=True)
    g.add_vertices(3)
    g.vs["name"] = ["core-a", "n1", "n2"]
    g.vs["is_original_node"] = [True, False, False]
    g.vs["keywords"] = ["Real Author Keyword", "OpenAlex Topic For N1", "OpenAlex Topic For N2"]
    return g


def test_split_leaves_core_keywords_untouched(tiny_combined_graph):
    graph = _split_openalex_topics_from_keywords(tiny_combined_graph, author_keywords_by_doi={})
    keywords_by_name = dict(zip(graph.vs["name"], graph.vs["keywords"]))
    assert keywords_by_name["core-a"] == "Real Author Keyword"
    assert graph.vs.find(name="core-a")["openalex_topics"] == ""


def test_split_moves_neighbor_keywords_to_openalex_topics(tiny_combined_graph):
    graph = _split_openalex_topics_from_keywords(tiny_combined_graph, author_keywords_by_doi={})
    topics_by_name = dict(zip(graph.vs["name"], graph.vs["openalex_topics"]))
    assert topics_by_name["n1"] == "OpenAlex Topic For N1"
    assert topics_by_name["n2"] == "OpenAlex Topic For N2"


def test_split_sets_neighbor_keywords_to_fetched_author_keywords(tiny_combined_graph):
    author_keywords_by_doi = {"n1": "Motor Learning; Cerebellum"}
    graph = _split_openalex_topics_from_keywords(tiny_combined_graph, author_keywords_by_doi)
    keywords_by_name = dict(zip(graph.vs["name"], graph.vs["keywords"]))
    assert keywords_by_name["n1"] == "Motor Learning; Cerebellum"
    assert keywords_by_name["n2"] == ""  # no fetched author keywords for n2


def test_split_on_neighbor_only_graph_treats_every_vertex_as_neighbor():
    """The Track B graph (neighbor_citation_network_with_community_keywords.graphml)
    has no `is_original_node` attribute at all -- every vertex is a neighbor."""
    g = ig.Graph(directed=True)
    g.add_vertices(2)
    g.vs["name"] = ["n1", "n2"]
    g.vs["keywords"] = ["Topic 1", "Topic 2"]

    graph = _split_openalex_topics_from_keywords(g, author_keywords_by_doi={"n1": "Real Keyword"})

    assert graph.vs["openalex_topics"] == ["Topic 1", "Topic 2"]
    assert graph.vs["keywords"] == ["Real Keyword", ""]
