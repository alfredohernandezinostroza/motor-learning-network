import pandas as pd

from motor_learning_network.get_neighbor_metadata import (
    _chunked,
    _external_citation_counts,
    _openalex_work_to_row,
    _reconstruct_abstract_from_inverted_index,
)


# ── _external_citation_counts ─────────────────────────────────────────────────
def test_external_citation_counts_only_counts_in_graph_citers():
    references_df = pd.DataFrame(
        {
            "citing_doi": ["a", "b", "c"],
            "cited_dois": [
                ("external-1", "b"),  # a is in-graph; cites external-1 and in-graph b
                ("external-1",),  # b is in-graph; also cites external-1
                ("external-1",),  # c is NOT in-graph; must not count
            ],
        }
    )
    graph_dois = {"a", "b"}

    counts = _external_citation_counts(references_df, graph_dois)

    assert counts == {"external-1": 2}


def test_external_citation_counts_ignores_none_and_empty_reference_lists():
    references_df = pd.DataFrame(
        {
            "citing_doi": ["a", "b"],
            "cited_dois": [None, ()],
        }
    )
    graph_dois = {"a", "b"}

    counts = _external_citation_counts(references_df, graph_dois)

    assert counts == {}


def test_external_citation_counts_lowercases_and_dedupes_within_a_row():
    references_df = pd.DataFrame(
        {
            "citing_doi": ["a"],
            "cited_dois": [("EXTERNAL-1", "external-1")],  # same DOI, different case
        }
    )
    graph_dois = {"a"}

    counts = _external_citation_counts(references_df, graph_dois)

    assert counts == {"external-1": 1}


# ── _reconstruct_abstract_from_inverted_index ─────────────────────────────────
def test_reconstruct_abstract_from_inverted_index_orders_words_by_position():
    inverted_index = {"citations": [1], "Motor": [0], "matter.": [2]}
    assert _reconstruct_abstract_from_inverted_index(inverted_index) == "Motor citations matter."


def test_reconstruct_abstract_from_inverted_index_handles_empty():
    assert _reconstruct_abstract_from_inverted_index(None) == ""
    assert _reconstruct_abstract_from_inverted_index({}) == ""


def test_reconstruct_abstract_from_inverted_index_handles_repeated_word():
    inverted_index = {"a": [0, 2], "b": [1]}
    assert _reconstruct_abstract_from_inverted_index(inverted_index) == "a b a"


# ── _openalex_work_to_row ─────────────────────────────────────────────────────
def test_openalex_work_to_row_maps_expected_fields():
    work = {
        "doi": "https://doi.org/10.1234/Example",
        "title": "A Study of Motor Learning",
        "authorships": [
            {"author": {"display_name": "Jane Doe"}},
            {"author": {"display_name": "John Smith"}},
        ],
        "abstract_inverted_index": {"Motor": [0], "learning.": [1]},
        "topics": [{"display_name": "Motor control"}, {"display_name": "Cerebellum"}],
        "primary_location": {"source": {"display_name": "Journal of Motor Behavior"}},
        "ids": {"pmid": "https://pubmed.ncbi.nlm.nih.gov/12345678"},
        "publication_year": 2019,
        "id": "https://openalex.org/W2100837269",
        "referenced_works": [
            "https://openalex.org/W1980521345",
            "https://openalex.org/W2016739232",
        ],
    }

    row = _openalex_work_to_row(work)

    assert row["doi"] == "10.1234/example"
    assert row["title"] == "A Study of Motor Learning"
    assert row["authors"] == "Jane Doe|John Smith"
    assert row["abstract"] == "Motor learning."
    assert row["openalex_topics"] == "Motor control|Cerebellum"
    assert row["journal"] == "Journal of Motor Behavior"
    assert row["source_database"] == "OpenAlex"
    assert row["pubmed_id"] == "12345678"
    assert row["year"] == 2019
    assert row["openalex_id"] == "W2100837269"
    assert row["referenced_openalex_ids"] == ("W1980521345", "W2016739232")


def test_openalex_work_to_row_handles_missing_optional_fields():
    row = _openalex_work_to_row({"doi": "10.1/x"})

    assert row["doi"] == "10.1/x"
    assert row["title"] == ""
    assert row["authors"] == ""
    assert row["abstract"] == ""
    assert row["openalex_topics"] == ""
    assert row["journal"] == ""
    assert row["pubmed_id"] is None
    assert row["year"] is None
    assert row["openalex_id"] is None
    assert row["referenced_openalex_ids"] == ()


def test_openalex_work_to_row_falls_back_to_host_venue():
    work = {"doi": "10.1/x", "host_venue": {"display_name": "Old-Style Venue"}}
    row = _openalex_work_to_row(work)
    assert row["journal"] == "Old-Style Venue"


# ── _chunked ───────────────────────────────────────────────────────────────────
def test_chunked_splits_into_expected_sizes():
    items = list(range(11))
    chunks = list(_chunked(items, 5))
    assert chunks == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9], [10]]


def test_chunked_empty_input():
    assert list(_chunked([], 5)) == []
