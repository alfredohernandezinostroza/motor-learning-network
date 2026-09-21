import igraph as ig
import pandas as pd
import pytest

from motor_learning_network.fetch_neighbor_scopus_keywords import (
    SOURCE_CORE_CORPUS,
    SOURCE_NONE,
    SOURCE_PUBMED,
    SOURCE_SCOPUS,
    _apply_keywords_to_graph,
    _batch_query,
    _combined_keywords_df,
    _doi_batches,
    _keywords_from_entry,
    _load_checkpoint,
    _proxies,
    _save_checkpoint,
    _verify_proxy_exit_ip,
)


# ── _batch_query ────────────────────────────────────────────────────────────
def test_batch_query_builds_or_clause():
    assert _batch_query(["10.1/a", "10.2/b"]) == 'DOI("10.1/a") OR DOI("10.2/b")'


def test_batch_query_quotes_sici_dois_with_parentheses():
    """9.2% of neighbour DOIs are Wiley SICI-style; unquoted, Scopus would read
    their parentheses as boolean grouping and return the wrong papers."""
    doi = "10.1002/(sici)1096-9861(19971020)387:2<167::aid-cne1>3.0.co;2-z"
    assert _batch_query([doi]) == f'DOI("{doi}")'


# ── _doi_batches ────────────────────────────────────────────────────────────
def test_doi_batches_splits_into_fixed_size_chunks():
    assert _doi_batches(["1", "2", "3", "4", "5"], batch_size=2) == [
        ["1", "2"], ["3", "4"], ["5"]
    ]


# ── _keywords_from_entry ────────────────────────────────────────────────────
def test_keywords_from_entry_parses_search_api_string_form():
    assert _keywords_from_entry({"authkeywords": "Cerebellum | Motor learning"}) == (
        "Cerebellum|Motor learning"
    )


def test_keywords_from_entry_parses_abstract_retrieval_list_form():
    entry = {"authkeywords": {"author-keyword": [{"$": "Cerebellum"}, {"$": "Gait"}]}}
    assert _keywords_from_entry(entry) == "Cerebellum|Gait"


def test_keywords_from_entry_parses_single_keyword_dict_form():
    """AbstractRetrieval collapses a one-element list into a bare dict."""
    entry = {"authkeywords": {"author-keyword": {"$": "Cerebellum"}}}
    assert _keywords_from_entry(entry) == "Cerebellum"


def test_keywords_from_entry_drops_empty_segments():
    assert _keywords_from_entry({"authkeywords": "A |  | B"}) == "A|B"


def test_keywords_from_entry_returns_empty_string_when_absent():
    assert _keywords_from_entry({}) == ""
    assert _keywords_from_entry({"authkeywords": None}) == ""


# ── checkpointing ───────────────────────────────────────────────────────────
def test_checkpoint_roundtrip_preserves_confirmed_empties(tmp_path):
    """Empties must persist, or a resume re-queries every keyword-less paper
    forever and the fetch never converges."""
    path = tmp_path / "checkpoint.parquet"
    _save_checkpoint(path, found={"10.1/a": "Gait"}, queried={"10.1/a", "10.2/b"})
    loaded = _load_checkpoint(path)
    assert loaded == {"10.1/a": "Gait", "10.2/b": ""}


def test_load_checkpoint_returns_empty_dict_when_absent(tmp_path):
    assert _load_checkpoint(tmp_path / "nope.parquet") == {}


# ── _proxies / _verify_proxy_exit_ip ────────────────────────────────────────
def test_proxies_returns_none_without_a_url():
    assert _proxies("") is None


def test_proxies_maps_both_schemes():
    assert _proxies("socks5h://127.0.0.1:11080") == {
        "http": "socks5h://127.0.0.1:11080",
        "https": "socks5h://127.0.0.1:11080",
    }


def test_verify_proxy_exit_ip_fails_without_a_url():
    ok, detail = _verify_proxy_exit_ip("")
    assert ok is False
    assert "not set" in detail


def test_verify_proxy_exit_ip_rejects_a_non_institutional_exit(monkeypatch):
    """The guard's whole purpose: an unproxied run still returns HTTP 200 with
    authkeywords silently absent, so a wrong exit IP must fail loudly."""
    class _Response:
        text = "194.210.215.155\n"

    monkeypatch.setattr(
        "motor_learning_network.fetch_neighbor_scopus_keywords.requests.get",
        lambda *a, **k: _Response(),
    )
    ok, detail = _verify_proxy_exit_ip("socks5h://127.0.0.1:11080")
    assert ok is False
    assert "194.210.215.155" in detail


def test_verify_proxy_exit_ip_accepts_either_jhu_range(monkeypatch):
    """JHU exits from 162.129.x as well as the 128.220.x that vpn.jh.edu
    itself resolves to; a single-prefix check would reject a good tunnel."""
    for ip in ("128.220.37.20", "162.129.250.12"):
        class _Response:
            text = ip

        monkeypatch.setattr(
            "motor_learning_network.fetch_neighbor_scopus_keywords.requests.get",
            lambda *a, **k: _Response(),
        )
        ok, detail = _verify_proxy_exit_ip("socks5h://127.0.0.1:11080")
        assert ok is True
        assert detail == ip


# ── _combined_keywords_df ───────────────────────────────────────────────────
@pytest.fixture
def metadata_df() -> pd.DataFrame:
    return pd.DataFrame({"doi": ["10.1/scopus", "10.2/pubmed", "10.3/both", "10.4/neither"]})


@pytest.fixture
def pubmed_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "doi": ["10.2/pubmed", "10.3/both"],
            "author_keywords": ["MEMORY/physiology", "HIPPOCAMPUS"],
        }
    )


def test_combined_prefers_scopus_over_pubmed(metadata_df, pubmed_df):
    df = _combined_keywords_df(
        metadata_df, {"10.3/both": "Hippocampus|Memory"}, pubmed_df
    ).set_index("doi")
    assert df.loc["10.3/both", "author_keywords"] == "Hippocampus|Memory"
    assert df.loc["10.3/both", "keywords_source"] == SOURCE_SCOPUS


def test_combined_falls_back_to_pubmed(metadata_df, pubmed_df):
    df = _combined_keywords_df(metadata_df, {}, pubmed_df).set_index("doi")
    assert df.loc["10.2/pubmed", "author_keywords"] == "MEMORY/physiology"
    assert df.loc["10.2/pubmed", "keywords_source"] == SOURCE_PUBMED


def test_combined_marks_papers_with_no_keywords_at_all(metadata_df, pubmed_df):
    df = _combined_keywords_df(metadata_df, {}, pubmed_df).set_index("doi")
    assert df.loc["10.4/neither", "author_keywords"] == ""
    assert df.loc["10.4/neither", "keywords_source"] == SOURCE_NONE


def test_combined_keeps_both_raw_columns_for_audit(metadata_df, pubmed_df):
    df = _combined_keywords_df(
        metadata_df, {"10.3/both": "Hippocampus|Memory"}, pubmed_df
    ).set_index("doi")
    assert df.loc["10.3/both", "scopus_keywords"] == "Hippocampus|Memory"
    assert df.loc["10.3/both", "pubmed_keywords"] == "HIPPOCAMPUS"


def test_combined_lowercases_dois_on_both_sides():
    """Scopus echoes prism:doi in its own casing; a mismatch here would make a
    paper with keywords look keyword-less."""
    metadata = pd.DataFrame({"doi": ["10.1/MiXeD"]})
    pubmed = pd.DataFrame({"doi": ["10.1/mixed"], "author_keywords": ["X"]})
    df = _combined_keywords_df(metadata, {"10.1/mixed": "Gait"}, pubmed).set_index("doi")
    assert df.loc["10.1/mixed", "author_keywords"] == "Gait"


def test_combined_has_one_row_per_neighbor(metadata_df, pubmed_df):
    df = _combined_keywords_df(metadata_df, {}, pubmed_df)
    assert len(df) == len(metadata_df)


# ── _apply_keywords_to_graph ────────────────────────────────────────────────
@pytest.fixture
def combined_graph() -> ig.Graph:
    graph = ig.Graph(directed=True)
    graph.add_vertices(3)
    graph.vs["name"] = ["core-a", "n1", "n2"]
    graph.vs["is_original_node"] = [True, False, False]
    graph.vs["keywords"] = ["Core Keyword", "Old PubMed KW", ""]
    return graph


def test_apply_overwrites_neighbor_keywords(combined_graph):
    graph = _apply_keywords_to_graph(
        combined_graph, {"n1": "Cerebellum|Gait"}, {"n1": SOURCE_SCOPUS}
    )
    by_name = dict(zip(graph.vs["name"], graph.vs["keywords"]))
    assert by_name["n1"] == "Cerebellum|Gait"


def test_apply_leaves_core_keywords_untouched(combined_graph):
    graph = _apply_keywords_to_graph(combined_graph, {}, {})
    by_name = dict(zip(graph.vs["name"], graph.vs["keywords"]))
    assert by_name["core-a"] == "Core Keyword"
    assert graph.vs.find(name="core-a")["keywords_source"] == SOURCE_CORE_CORPUS


def test_apply_uses_empty_string_not_none_for_missing(combined_graph):
    """igraph's GraphML writer renders a Python None in a string attribute as
    the literal text "None", which would show up as visible labels in Gephi."""
    graph = _apply_keywords_to_graph(combined_graph, {}, {})
    assert graph.vs.find(name="n2")["keywords"] == ""
    assert graph.vs.find(name="n2")["keywords_source"] == SOURCE_NONE
    assert None not in graph.vs["keywords"]
    assert None not in graph.vs["keywords_source"]


def test_apply_on_neighbor_only_graph_treats_every_vertex_as_neighbor():
    """The neighbour-only graph carries no `is_original_node` attribute."""
    graph = ig.Graph(directed=True)
    graph.add_vertices(2)
    graph.vs["name"] = ["n1", "n2"]
    graph.vs["keywords"] = ["old1", "old2"]

    graph = _apply_keywords_to_graph(graph, {"n1": "Gait"}, {"n1": SOURCE_SCOPUS})

    assert graph.vs["keywords"] == ["Gait", ""]
    assert graph.vs["keywords_source"] == [SOURCE_SCOPUS, SOURCE_NONE]


# ── _fetch_scopus_keywords: tunnel requirement is conditional ───────────────
def test_fetch_skips_tunnel_check_when_fully_checkpointed(tmp_path, monkeypatch):
    """A rerun with nothing left to fetch is pure local file work. Requiring a
    live tunnel there would hold every later rerun hostage to a DSID cookie
    that expires after a few hours and has nothing to do with the work."""
    from motor_learning_network import fetch_neighbor_scopus_keywords as module

    def _explode(*args, **kwargs):
        raise AssertionError("must not touch the network when nothing is pending")

    monkeypatch.setattr(module, "_verify_proxy_exit_ip", _explode)
    monkeypatch.setattr(module.requests, "get", _explode)

    path = tmp_path / "checkpoint.parquet"
    _save_checkpoint(path, found={"10.1/a": "Gait"}, queried={"10.1/a", "10.2/b"})

    result = module._fetch_scopus_keywords(["10.1/a", "10.2/b"], path, proxy_url="")
    assert result == {"10.1/a": "Gait"}


def test_fetch_refuses_to_run_unproxied_when_work_remains(tmp_path, monkeypatch):
    """The inverse: with DOIs still pending, a dead tunnel must fail loudly
    rather than bank a run of silent false negatives."""
    from motor_learning_network import fetch_neighbor_scopus_keywords as module

    monkeypatch.setattr(
        module, "_verify_proxy_exit_ip", lambda *a, **k: (False, "proxy unreachable")
    )
    with pytest.raises(RuntimeError, match="tunnel not usable"):
        module._fetch_scopus_keywords(["10.9/new"], tmp_path / "none.parquet", proxy_url="")
