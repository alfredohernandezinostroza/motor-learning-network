import igraph as ig
import pandas as pd

from motor_learning_network.neighbor_community_keywords import (
    _attach_top_keywords,
    _build_synonym_map,
    _canonical_corpus,
    _community_keywords_df,
    _filtered_keywords_df,
    _top_keywords_per_community,
)


# ── _build_synonym_map ─────────────────────────────────────────────────────────
def test_build_synonym_map_maps_variants_to_lowercased_key():
    synonym_dict = {"Motor Learning": ["Skill Acquisition", "MOTOR LEARNING"]}
    mapping = _build_synonym_map(synonym_dict)
    assert mapping["skill acquisition"] == "motor learning"
    assert mapping["motor learning"] == "motor learning"


# ── _filtered_keywords_df ──────────────────────────────────────────────────────
def _tiny_neighbor_graph() -> ig.Graph:
    g = ig.Graph(directed=True)
    g.add_vertices(4)
    g.vs["name"] = ["n0", "n1", "n2", "n3"]
    g.vs["keywords"] = [
        "Motor Learning|Cerebellum",
        "Motor Learning|Basal Ganglia, Striatum",
        "Reaching Movements",
        "",
    ]
    g.vs["cpm_communities_at_res=0.005"] = [0, 0, 1, 1]
    g.vs["community_size_at_res=0.005"] = [2, 2, 2, 2]
    return g


def test_filtered_keywords_df_splits_on_dividing_character_and_stray_commas():
    graph = _tiny_neighbor_graph()
    df = _filtered_keywords_df(graph, 0.005, "|", min_community_size=1)
    assert df.loc[df["community_id"] == 0, "keywords"].tolist() == [
        ["Motor Learning", "Cerebellum"],
        ["Motor Learning", "Basal Ganglia", "Striatum"],
    ]


def test_filtered_keywords_df_drops_communities_below_min_size():
    graph = _tiny_neighbor_graph()
    graph.vs["community_size_at_res=0.005"] = [2, 2, 1, 1]
    df = _filtered_keywords_df(graph, 0.005, "|", min_community_size=2)
    assert set(df["community_id"]) == {0}


# ── _canonical_corpus ──────────────────────────────────────────────────────────
def test_canonical_corpus_rewrites_synonyms_and_joins_per_community():
    filtered_keywords_df = pd.DataFrame(
        {
            "keywords": [["Motor Learning", "Cerebellum"], ["Skill Acquisition"]],
            "community_id": [0, 0],
        }
    )
    synonym_map = {"skill acquisition": "motor learning"}
    corpus = _canonical_corpus(filtered_keywords_df, synonym_map)
    assert corpus == {0: "motor learning\tcerebellum\tmotor learning"}


def test_canonical_corpus_keeps_communities_separate():
    filtered_keywords_df = pd.DataFrame(
        {"keywords": [["Cerebellum"], ["Basal Ganglia"]], "community_id": [0, 1]}
    )
    corpus = _canonical_corpus(filtered_keywords_df, synonym_map={})
    assert corpus == {0: "cerebellum", 1: "basal ganglia"}


# ── _correct_tfidf + _top_keywords_per_community ───────────────────────────────
def test_top_keywords_per_community_ranks_distinguishing_terms_first():
    # "shared" appears in every community's document so should be down-weighted
    # relative to each community's own distinguishing term.
    corpus = {
        0: "shared\tshared\tcerebellum\tcerebellum\tcerebellum",
        1: "shared\tshared\tbasal ganglia\tbasal ganglia\tbasal ganglia",
    }
    result = _top_keywords_per_community(corpus, top_n=1)
    assert result == {0: "Cerebellum", 1: "Basal Ganglia"}


def test_top_keywords_per_community_drops_empty_documents():
    corpus = {0: "cerebellum", 1: ""}
    result = _top_keywords_per_community(corpus, top_n=3)
    assert set(result.keys()) == {0}


def test_top_keywords_per_community_empty_corpus_returns_empty_dict():
    assert _top_keywords_per_community({}, top_n=3) == {}


# ── _attach_top_keywords ────────────────────────────────────────────────────────
def test_attach_top_keywords_broadcasts_to_every_member_and_blanks_missing():
    """Missing communities get "" rather than None -- igraph's GraphML writer
    stringifies a bare None in a string attribute as the literal text "None"
    instead of omitting it."""
    graph = _tiny_neighbor_graph()
    top_keywords_by_community = {0: "Cerebellum; Motor Learning"}
    graph = _attach_top_keywords(graph, 0.005, top_keywords_by_community)
    assert graph.vs["top_keywords_at_res=0.005"] == [
        "Cerebellum; Motor Learning",
        "Cerebellum; Motor Learning",
        "",
        "",
    ]


# ── _community_keywords_df ─────────────────────────────────────────────────────
def test_community_keywords_df_one_row_per_resolution_and_community():
    graph = _tiny_neighbor_graph()
    top_keywords_per_resolution = {0.005: {0: "Cerebellum; Motor Learning", 1: "Reaching Movements"}}
    df = _community_keywords_df(graph, top_keywords_per_resolution)
    assert len(df) == 2
    row0 = df[df["community_id"] == 0].iloc[0]
    assert row0["resolution"] == 0.005
    assert row0["community_size"] == 2
    assert row0["top_keywords"] == "Cerebellum; Motor Learning"
