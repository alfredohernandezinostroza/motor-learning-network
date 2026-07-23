import pandas as pd
import pytest
import igraph as ig

from motor_learning_network.preprocess_text_for_embeddings import (
    _decode_entities,
    _strip_copyright,
    _normalize_headings,
    _normalize_whitespace,
    _clean_field,
    _build_records,
    _TITLE_PLACEHOLDERS,
    _ABSTRACT_PLACEHOLDERS,
    ST_AVAILABLE,
    ST_MISSING,
    ST_PLACEHOLDER,
)


# ── _decode_entities ──────────────────────────────────────────────────────────
def test_decode_entities_single_and_double():
    assert _decode_entities("Bax &amp; Willis") == "Bax & Willis"
    # Double-encoded (&amp;amp;) is decoded on the second pass.
    assert _decode_entities("A &amp;amp; B") == "A & B"


# ── _strip_copyright ──────────────────────────────────────────────────────────
def test_strip_copyright_literal_sign():
    text, truncated = _strip_copyright("Real abstract text. © 2020 Elsevier Ltd.")
    assert truncated is True
    assert text.strip() == "Real abstract text."


def test_strip_copyright_escaped_form_before_decoding():
    # The escaped &copy; must be caught before HTML decoding.
    text, truncated = _strip_copyright("Body of the abstract &copy; 2019 The Authors")
    assert truncated is True
    assert text.strip() == "Body of the abstract"


def test_strip_copyright_absent():
    text, truncated = _strip_copyright("No copyright here")
    assert truncated is False
    assert text == "No copyright here"


# ── _normalize_headings ───────────────────────────────────────────────────────
def test_normalize_headings_removes_standalone_labels():
    out, changed = _normalize_headings("Background: We studied X. Methods: We did Y.")
    assert changed is True
    assert "Background:" not in out and "Methods:" not in out
    assert "We studied X." in out and "We did Y." in out


def test_normalize_headings_leaves_inline_words():
    # "methods" mid-sentence (no heading separator) must be preserved.
    out, changed = _normalize_headings("Our methods were validated against results.")
    assert changed is False
    assert out == "Our methods were validated against results."


# ── _normalize_whitespace ─────────────────────────────────────────────────────
def test_normalize_whitespace_collapses_and_strips():
    assert _normalize_whitespace("  a\n\t  b   c  ") == "a b c"


# ── _clean_field (integration of the pipeline) ────────────────────────────────
def test_clean_field_missing_and_placeholder():
    assert _clean_field(None, _ABSTRACT_PLACEHOLDERS)[1] == ST_MISSING
    assert _clean_field("   ", _ABSTRACT_PLACEHOLDERS)[1] == ST_MISSING
    # Whole-field placeholder (case/bracket-insensitive) -> placeholder status.
    assert _clean_field("[N/A]", _ABSTRACT_PLACEHOLDERS)[1] == ST_PLACEHOLDER
    assert _clean_field("No abstract available.", _ABSTRACT_PLACEHOLDERS)[1] == ST_PLACEHOLDER


def test_clean_field_placeholder_phrase_inside_real_text_is_kept():
    # The placeholder phrase appears inside a real sentence -> NOT a placeholder.
    text, status, _ = _clean_field(
        "The drug was not available to the control group during the trial.",
        _ABSTRACT_PLACEHOLDERS,
    )
    assert status == ST_AVAILABLE
    assert "control group" in text


def test_clean_field_full_pipeline():
    raw = "Background: We &amp; colleagues ran a study.  © 2021 Wiley."
    text, status, flags = _clean_field(raw, _ABSTRACT_PLACEHOLDERS)
    assert status == ST_AVAILABLE
    assert text == "We & colleagues ran a study."
    assert flags["copyright_truncated"] is True
    assert flags["heading_normalized"] is True


# ── _build_records (node-level, on a synthetic igraph) ────────────────────────
@pytest.fixture
def small_graph():
    g = ig.Graph()
    g.add_vertices(4)
    g.vs["name"] = ["p_both", "p_title_only", "p_abstract_only", "p_neither"]
    g.vs["title"] = ["A Motor Study", "Title Present", None, None]
    g.vs["abstract"] = ["We examined reaching.", "No abstract available", "Only the abstract survives.", "   "]
    return g


def test_build_records_availability_and_skip(small_graph):
    df = pd.DataFrame(_build_records(small_graph))
    by_id = {r["node_id"]: r for r in df.to_dict("records")}

    # Both present -> combined Title/Abstract embedding_text.
    both = by_id["p_both"]
    assert both["title_status"] == ST_AVAILABLE and both["abstract_status"] == ST_AVAILABLE
    assert both["embedding_text"] == "Title: A Motor Study\nAbstract: We examined reaching."
    assert both["skip_reason"] is None

    # Title present, abstract is a placeholder -> title-only, still kept.
    tonly = by_id["p_title_only"]
    assert tonly["abstract_status"] == ST_PLACEHOLDER
    assert tonly["embedding_text"] == "Title: Title Present"

    # Abstract only -> abstract-only embedding_text.
    aonly = by_id["p_abstract_only"]
    assert aonly["title_status"] == ST_MISSING
    assert aonly["embedding_text"] == "Abstract: Only the abstract survives."

    # Neither -> skipped, no embedding_text.
    neither = by_id["p_neither"]
    assert neither["embedding_text"] is None
    assert neither["skip_reason"] == "missing_both_title_and_abstract"
