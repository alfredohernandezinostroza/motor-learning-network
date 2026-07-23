"""Build a citation-independent text dataset for embedding generation.

For every node in a citation-network GraphML this extracts ONLY the node
identifier, title, and abstract, cleans the text (HTML-entity decoding,
copyright/publisher-boilerplate removal, structured-abstract heading
normalization, whitespace normalization), and builds a combined
``embedding_text`` field of the form::

    Title: {title}\nAbstract: {abstract}

It deliberately includes NO citation-derived information (no edges, references,
authors, venues, years, communities, citation counts): the embeddings that
consume this reflect paper *content* only, so the text view stays independent of
the citation-structure view it is later compared against.

A paper is kept if *either* the title or the abstract is available; it is skipped
only when both are unavailable. Title and abstract availability are tracked
separately (available / missing / placeholder).

Ported from the companion Mariana-Embedding-Space-Analysis project
(``scripts/preprocess_graphml_text_for_embeddings.py``) into this repo's
Hamilton-DAG idiom. The pure text-cleaning functions are kept verbatim (they are
the tested core); node text is read with igraph (matching the rest of this repo)
rather than the original's hand-rolled ElementTree parser, which makes it robust
to the ``title``/``abstract`` attribute names this repo's GraphML uses.

Outputs (data/processed/):
  paper_text_for_embeddings_all.parquet    full audit table (one row per node)
  paper_text_for_embeddings_ready.parquet  embedding-ready rows (node_id, embedding_text)
"""

import sys
import html
import re
import logging
from pathlib import Path
from typing import Final, Optional

import pandas as pd
import igraph as ig

from hamilton.function_modifiers import dataloader, datasaver
from hamilton.io import utils
from hamilton import driver
import hamilton.log_setup

from motor_learning_network.constants import (
    GRAPH_LEVEL_DATA_PATH,
    PROCESSED_DATA_PATH,
    FIGURES_PATH,
)

###################
##   Constants   ##
###################
CURRENT_FILE_NAME = Path(__file__).stem
hamilton.log_setup.setup_logging(logging.INFO)
logger = logging.getLogger(__name__)

EXECUTE = True

# Node attributes carrying the text (this repo's GraphML attr.names). The node
# identifier follows the repo convention (``name``, as used in detect_missing_links).
TITLE_ATTR: Final[str] = "title"
ABSTRACT_ATTR: Final[str] = "abstract"
ID_ATTR: Final[str] = "name"

# Status labels for per-field availability.
ST_AVAILABLE: Final[str] = "available"
ST_MISSING: Final[str] = "missing"
ST_PLACEHOLDER: Final[str] = "placeholder"

# A field is treated as unavailable ("placeholder") if, after trimming and
# stripping surrounding brackets/punctuation, it equals one of these (case-
# insensitive). Matched against the WHOLE field only -- never scanned inside real
# text -- so sentences containing e.g. "was not available to the ..." are safe.
_ABSTRACT_PLACEHOLDERS: Final[set[str]] = {
    "no abstract available", "abstract not available", "abstract unavailable",
    "not available", "n/a", "na", "none", "null",
}
_TITLE_PLACEHOLDERS: Final[set[str]] = {
    "no title available", "title not available", "title unavailable", "untitled",
    "not available", "n/a", "na", "none", "null",
}

# Copyright sign in literal or HTML-escaped form -- marks the start of publisher
# boilerplate so everything from there on is dropped (matched BEFORE decoding).
_COPYRIGHT_RE = re.compile(r"(?:©|&copy;|&#0*169;|&#x0*A9;)", re.IGNORECASE)

# Structured-abstract section headings, normalized away only when they appear as
# a standalone label (heading word immediately followed by a separator).
_HEADING_WORDS: Final[list[str]] = [
    "Background", "Backgrounds", "Objective", "Objectives", "Aim", "Aims",
    "Method", "Methods", "Methodology", "Materials and Methods",
    "Material and Methods", "Result", "Results", "Discussion", "Discussions",
    "Conclusion", "Conclusions", "Significance", "Introduction", "Purpose",
    "Design", "Setting", "Settings", "Participants", "Intervention",
    "Interventions", "Findings", "Finding", "Implication", "Implications",
    "Outcome", "Outcomes", "Measurements", "Main Outcome Measures", "Context",
    "Rationale",
]
# Longest-first so multi-word headings match before their single-word prefixes.
_HEADING_ALT = "|".join(re.escape(w) for w in sorted(_HEADING_WORDS, key=len, reverse=True))
_HEADING_RE = re.compile(
    r"(?:(?<=^)|(?<=[.;!?])\s+|(?<=\n)|(?<=\s))"   # boundary before heading
    r"[\[(]?\s*"                                    # optional opening bracket
    r"(?:" + _HEADING_ALT + r")"                    # the heading word(s)
    r"\s*[\])]?\s*"                                 # optional closing bracket
    r"(?::|–|—|-)\s+",                              # required separator
    re.IGNORECASE,
)
_WHITESPACE_RE = re.compile(r"\s+")


#####################
##  Aux Functions  ##
#####################
def _decode_entities(text: str) -> str:
    """Decode HTML entities (``&amp;`` -> ``&``); run twice for double-encoding."""
    if not text:
        return text
    out = html.unescape(text)
    if "&" in out:
        out = html.unescape(out)
    return out


def _strip_copyright(text: str) -> tuple[str, bool]:
    """Remove the copyright symbol and everything after it. Returns (text, truncated)."""
    if not text:
        return text, False
    m = _COPYRIGHT_RE.search(text)
    if m is None:
        return text, False
    return text[: m.start()], True


def _normalize_headings(text: str) -> tuple[str, bool]:
    """Strip standalone structured-abstract heading labels, keeping the prose after."""
    if not text:
        return text, False
    new_text, n = _HEADING_RE.subn(" ", text)
    return new_text, n > 0


def _normalize_whitespace(text: str) -> str:
    """Collapse whitespace runs to a single space and strip ends."""
    if not text:
        return text
    return _WHITESPACE_RE.sub(" ", text).strip()


def _normalized_for_placeholder_check(text: str) -> str:
    """Lowercased, bracket/quote/period-stripped form used only for placeholder matching."""
    s = text.strip()
    s = re.sub(r"^[\[\(\{]+|[\]\)\}]+$", "", s).strip()
    s = s.strip("\"'").strip()
    s = s.rstrip(".").strip()
    return s.lower()


def _clean_field(raw: Optional[str], placeholders: set[str]) -> tuple[str, str, dict]:
    """Clean one title/abstract field. Returns (clean_text, status, flags)."""
    flags = {"copyright_truncated": False, "heading_normalized": False}
    if raw is None or not raw.strip():
        return "", ST_MISSING, flags

    # Remove copyright boilerplate FIRST (catches escaped forms before decoding).
    text, truncated = _strip_copyright(raw)
    flags["copyright_truncated"] = truncated
    text = _decode_entities(text)
    # A copyright sign may only surface after decoding (rare) -- handle again.
    text2, truncated2 = _strip_copyright(text)
    if truncated2:
        text, flags["copyright_truncated"] = text2, True

    text, heading_changed = _normalize_headings(text)
    flags["heading_normalized"] = heading_changed
    text = _normalize_whitespace(text)

    if not text:
        return "", ST_MISSING, flags
    if _normalized_for_placeholder_check(text) in placeholders:
        return "", ST_PLACEHOLDER, flags
    return text, ST_AVAILABLE, flags


def _node_text(v: ig.Vertex) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Extract (node_id, title_raw, abstract_raw) from a vertex, guarding absent attrs."""
    attrs = v.attributes()
    node_id = attrs.get(ID_ATTR)
    node_id = str(node_id) if node_id not in (None, "") else str(v.index)
    return node_id, attrs.get(TITLE_ATTR), attrs.get(ABSTRACT_ATTR)


def _build_records(graph: ig.Graph) -> list[dict]:
    """Clean every node's title/abstract and assemble the audit records."""
    records = []
    for v in graph.vs:
        node_id, title_raw, abstract_raw = _node_text(v)
        title_clean, title_status, t_flags = _clean_field(title_raw, _TITLE_PLACEHOLDERS)
        abstract_clean, abstract_status, a_flags = _clean_field(abstract_raw, _ABSTRACT_PLACEHOLDERS)

        title_ok = title_status == ST_AVAILABLE
        abstract_ok = abstract_status == ST_AVAILABLE
        if title_ok and abstract_ok:
            embedding_text, skip_reason = f"Title: {title_clean}\nAbstract: {abstract_clean}", None
        elif title_ok:
            embedding_text, skip_reason = f"Title: {title_clean}", None
        elif abstract_ok:
            embedding_text, skip_reason = f"Abstract: {abstract_clean}", None
        else:
            embedding_text, skip_reason = None, "missing_both_title_and_abstract"

        records.append({
            "node_id": node_id,
            "title_clean": title_clean or None,
            "abstract_clean": abstract_clean or None,
            "embedding_text": embedding_text,
            "title_status": title_status,
            "abstract_status": abstract_status,
            "skip_reason": skip_reason,
            "copyright_truncated": t_flags["copyright_truncated"] or a_flags["copyright_truncated"],
            "heading_normalized": t_flags["heading_normalized"] or a_flags["heading_normalized"],
        })
    return records


##################
##     Main     ##
##################
def _main() -> int:
    inputs = dict(
        citation_network_path=GRAPH_LEVEL_DATA_PATH / "citation_network_with_topics_new.graphml",
        text_records_all_path=PROCESSED_DATA_PATH / "paper_text_for_embeddings_all.parquet",
        embedding_ready_path=PROCESSED_DATA_PATH / "paper_text_for_embeddings_ready.parquet",
    )
    outputs = ["save_text_records_all", "save_embedding_ready_records"]
    import __main__
    dr = (
        driver.Builder()
        .with_modules(__main__)
        .build()
    )
    dr.validate_execution(outputs, inputs=inputs)
    dr.display_all_functions(
        FIGURES_PATH / f"{CURRENT_FILE_NAME}_all_functions.png",
        keep_dot=True, deduplicate_inputs=True,
    )
    dr.visualize_execution(
        outputs, inputs=inputs,
        output_file_path=FIGURES_PATH / f"{CURRENT_FILE_NAME}.png",
        keep_dot=False, deduplicate_inputs=True,
    )
    if EXECUTE:
        dr.execute(outputs, inputs=inputs)
    return 0


#########################
##    DAG Definition   ##
#########################
@dataloader()
def citation_network(citation_network_path: Path) -> tuple[ig.Graph, dict]:
    graph = ig.Graph.Read_GraphML(str(citation_network_path))
    metadata = utils.get_file_metadata(citation_network_path)
    return graph, metadata


def text_records(citation_network: ig.Graph) -> pd.DataFrame:
    """Full audit table: one row per node with cleaned text + availability status."""
    df = pd.DataFrame(_build_records(citation_network))
    n_ready = int(df["embedding_text"].notna().sum())
    n_skipped = int((df["skip_reason"] == "missing_both_title_and_abstract").sum())
    logger.info(
        "text_records: %d nodes, %d embedding-ready, %d skipped (no title & no abstract)",
        len(df), n_ready, n_skipped,
    )
    return df


def embedding_ready_records(text_records: pd.DataFrame) -> pd.DataFrame:
    """Rows with usable text, reduced to what the embedding step needs."""
    ready = text_records[text_records["embedding_text"].notna()].copy()
    return ready[["node_id", "embedding_text", "title_status", "abstract_status"]].reset_index(drop=True)


@datasaver()
def save_text_records_all(text_records: pd.DataFrame, text_records_all_path: Path) -> dict:
    text_records.to_parquet(text_records_all_path, index=False)
    return utils.get_file_metadata(text_records_all_path)


@datasaver()
def save_embedding_ready_records(embedding_ready_records: pd.DataFrame, embedding_ready_path: Path) -> dict:
    embedding_ready_records.to_parquet(embedding_ready_path, index=False)
    return utils.get_file_metadata(embedding_ready_path)


if __name__ == "__main__":
    sys.exit(_main())
