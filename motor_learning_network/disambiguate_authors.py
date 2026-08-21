"""Author-name disambiguation for the unified motor learning corpus.

Phase 1 of the discovery-hypergraph workstream (docs/DISCOVERY_HYPERGRAPH_PLAN.md).
Every downstream random walk over the research hypergraph steps through author
nodes, so a lumped "Zhang, Y" supernode or a split prolific researcher corrupts
the whole prediction stage. This module turns the 153,397 raw author mentions in
``clean_unified_database.parquet`` into stable canonical author identities.

The key observation is that this is *not* from-scratch disambiguation. The raw
source tables already carry externally-disambiguated identifiers:

  - Scopus ``Author full names`` embeds Elsevier's own author codes inline, as
    ``"Ruiz-Olaya, Andres Felipe (55909479000); Lopez-Delis, Alberto (...)"``,
    at 100% coverage of the Scopus rows;
  - Web of Science ``OI`` carries ORCIDs as ``"Lee, Won Taek/0000-0001-..."``
    and ``RI`` carries ResearcherIDs.

Joining those back to the deduplicated corpus by DOI reaches 32,406 of 36,338
papers (89%). So the problem is *cross-source reconciliation with hard anchors*:
the anchors supply must-link and cannot-link constraints, a pairwise model
scores the remaining unanchored mentions, and constrained clustering resolves
the rest.

Method
------
1. Explode the corpus into author mentions and normalize names (accent folding,
   punctuation stripping, "Last, First" vs "First Last" handling).
2. Parse external identifiers out of the Scopus and WoS tables, join by
   (normalized DOI, normalized name).
3. Block on (normalized last name, first initial) -- 78,011 blocks, max size
   154, yielding 572,629 candidate pairs, small enough to score exhaustively
   with no approximate nearest-neighbour step.
4. Score each candidate pair with a logistic model over shared coauthors,
   affiliation overlap, journal overlap, year gap and given-name compatibility.
   The model is *supervised by the anchors themselves*: pairs sharing an
   external identifier are positives, pairs holding conflicting identifiers are
   negatives.
5. Cluster with union-find under must-link (shared identifier) and cannot-link
   (conflicting identifier) constraints, merging candidate edges in descending
   probability order and refusing any merge that would collide two distinct
   external identities.

Anchor identities are split into a training and a held-out evaluation half
before any of this, so the report measures recovery of groupings the model was
never shown.

Outputs (data/processed/author_disambiguation/):
  disambiguated_authors.parquet         mention-level: paper_index, raw_name,
    canonical_author_id, external identifiers, block
  author_clusters.parquet               cluster-level: canonical_author_id,
    mention_count, paper_count, first/last year, most frequent surface form
  author_disambiguation_report.parquet  held-out pairwise + B-cubed precision,
    recall and F1, plus the corpus-level diagnostics named in
    _disambiguation_diagnostics
"""

import re
import sys
import logging
import unicodedata
from pathlib import Path
from typing import Final
from collections import defaultdict, Counter

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.special import expit

from hamilton import driver
from hamilton.io import utils
from hamilton.function_modifiers import dataloader, datasaver
from hamilton_sdk import adapters
import hamilton.log_setup

from motor_learning_network.constants import (
    PROCESSED_DATA_PATH,
    FIGURES_PATH,
    DEFAULT_UI_PROJECT_ID,
    DEFAULT_UI_USERNAME,
    TEAM_NAME,
)

###################
##   Constants   ##
###################
CURRENT_FILE_NAME = Path(__file__).stem
hamilton.log_setup.setup_logging(logging.INFO)
logger = logging.getLogger(__name__)

EXECUTE = True
USE_TRACKER = False

OUTPUT_DIR: Final[Path] = PROCESSED_DATA_PATH / "author_disambiguation"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MENTIONS_PARQUET: Final[Path] = OUTPUT_DIR / "disambiguated_authors.parquet"
CLUSTERS_PARQUET: Final[Path] = OUTPUT_DIR / "author_clusters.parquet"
REPORT_PARQUET: Final[Path] = OUTPUT_DIR / "author_disambiguation_report.parquet"

# Surface forms that are placeholders rather than people. Discovered empirically:
# "Anon, J" is the single largest block in the corpus at 154 mentions, which would
# otherwise fuse 154 unrelated papers into one extremely well-connected fake author
# and give the random walker a superhighway between unrelated literatures.
PLACEHOLDER_NAME_PATTERNS: Final[tuple[str, ...]] = (
    r"^anon", r"^anonymous", r"^\[?no author", r"^et al", r"^unknown",
)

# Mentions whose names normalize to nothing survive as singleton identities rather
# than collapsing into one shared empty block. 116 mentions are non-Latin-script
# names (e.g. Korean) that accent folding reduces to the empty string; they are
# real people and must not be merged with each other just for being unparseable.
UNPARSEABLE_BLOCK_PREFIX: Final[str] = "__unparseable__"

# Probability above which a candidate pair is merged, subject to the cannot-link
# constraints. Deliberately high: splitting one researcher into two identities
# costs the walker some edges, but lumping two researchers costs it a false
# bridge between unrelated topics, which is the more damaging error here.
MATCH_PROBABILITY_THRESHOLD: Final[float] = 0.90

# Fraction of externally-anchored identities held out of model training entirely,
# used only to score the finished clustering.
HELD_OUT_IDENTITY_FRACTION: Final[float] = 0.20
RANDOM_SEED: Final[int] = 0

# L2 penalty for the logistic model. The feature count is tiny (6) against ~10^5
# labelled pairs, so this only stabilizes separable features rather than doing
# real model selection.
LOGISTIC_L2_PENALTY: Final[float] = 1.0

FEATURE_NAMES: Final[tuple[str, ...]] = (
    "shared_coauthor_count",
    "shared_affiliation_token_fraction",
    "same_journal",
    "publication_year_gap",
    "given_name_compatibility",
    "log_block_size",
)


#####################
##  Aux Functions  ##
#####################
def _strip_accents(text: str) -> str:
    return "".join(
        character
        for character in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(character)
    )


def _normalize_name(raw_name: str) -> str:
    """Fold a surface form to a comparable key: accents stripped, lowercased,
    punctuation reduced to spaces, commas preserved so "Last, First" order can
    still be recovered downstream."""
    folded = _strip_accents(str(raw_name)).lower()
    folded = re.sub(r"[^a-z,\s\-]", " ", folded)
    folded = re.sub(r"\s+", " ", folded)
    return folded.strip().strip("-").strip()


def _split_last_and_given(raw_name: str) -> tuple[str, str]:
    """Split a surface form into (last name, given names).

    Handles both "Last, First M." and "First M. Last". The comma form is
    unambiguous and is what every source in this corpus uses; the fallback
    treats the final whitespace token as the last name, which is wrong for
    unpunctuated compound surnames but affects a negligible share of mentions.
    """
    normalized = _normalize_name(raw_name)
    if not normalized:
        return "", ""
    if "," in normalized:
        last, _, given = normalized.partition(",")
        return last.strip(), given.strip()
    tokens = normalized.split()
    if len(tokens) == 1:
        return tokens[0], ""
    return tokens[-1], " ".join(tokens[:-1])


def _is_placeholder_name(normalized_name: str) -> bool:
    return any(re.match(pattern, normalized_name) for pattern in PLACEHOLDER_NAME_PATTERNS)


def _normalize_doi(doi_series: pd.Series) -> pd.Series:
    return (
        doi_series.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"^https?://(dx\.)?doi\.org/", "", regex=True)
    )


def _parse_name_with_parenthesized_identifier(field: str) -> list[tuple[str, str]]:
    """Parse Scopus ``Author full names``: ``"Name A (123); Name B (456)"``."""
    parsed: list[tuple[str, str]] = []
    for chunk in str(field).split(";"):
        match = re.match(r"^(.*?)\s*\((\d+)\)$", chunk.strip())
        if match:
            parsed.append((match.group(1).strip(), match.group(2)))
    return parsed


def _parse_name_with_slashed_identifier(field: str) -> list[tuple[str, str]]:
    """Parse Web of Science ``OI``/``RI``: ``"Name A/0000-0001-...; Name B/..."``."""
    parsed: list[tuple[str, str]] = []
    text = str(field)
    if text.strip().lower() in {"nan", ""}:
        return parsed
    for chunk in text.split(";"):
        if "/" not in chunk:
            continue
        name, _, identifier = chunk.strip().rpartition("/")
        name, identifier = name.strip(), identifier.strip()
        if name and identifier and identifier.lower() != "nan":
            parsed.append((name, identifier))
    return parsed


def _affiliation_tokens(affiliation_text: str) -> frozenset[str]:
    """Content tokens of a paper's affiliation string, for Jaccard overlap.

    Institution words only: the generic scaffolding ("university", "department",
    "of") appears in nearly every affiliation and would make every pair in the
    corpus look weakly similar."""
    if affiliation_text is None:
        return frozenset()
    text = _strip_accents(str(affiliation_text)).lower()
    tokens = re.findall(r"[a-z]{3,}", text)
    return frozenset(tokens) - _AFFILIATION_STOPWORDS


_AFFILIATION_STOPWORDS: Final[frozenset[str]] = frozenset({
    "university", "universite", "universidad", "universita", "univ",
    "department", "departement", "departamento", "dept", "school", "faculty",
    "institute", "institut", "instituto", "center", "centre", "centro",
    "hospital", "clinic", "college", "laboratory", "laboratoire", "lab",
    "research", "science", "sciences", "medical", "medicine", "health",
    "and", "the", "for", "of", "des", "der", "und", "van", "del", "los",
    "usa", "uk", "germany", "france", "japan", "china", "canada", "australia",
})


def _given_name_compatibility(given_a: str, given_b: str) -> int:
    """Score how compatible two given-name strings are.

    2 = both spelled out and identical, 1 = compatible (one abbreviates the
    other, or initials agree), 0 = outright contradictory. Kept ordinal rather
    than one-hot because the logistic model reads it monotonically: more
    agreement should never lower the match probability.
    """
    tokens_a = [token for token in given_a.replace("-", " ").split() if token]
    tokens_b = [token for token in given_b.replace("-", " ").split() if token]
    if not tokens_a or not tokens_b:
        return 1  # absent given name is uninformative, not contradictory

    def is_initials_run(tokens: list[str]) -> bool:
        """A lone short token standing for several initials, as in "Ivry, RB".

        Web of Science and older Scopus records write given names this way. Read
        naively as a spelled-out name, "rb" looks *contradictory* against
        "richard b" and splits one researcher into two identities -- which is
        exactly what happened to Ivry, Wolpert, Shea, Magill and Haith before
        this case was handled.
        """
        return len(tokens) == 1 and 1 < len(tokens[0]) <= 3

    def initials(tokens: list[str]) -> str:
        return "".join(token[0] for token in tokens)

    is_spelled_out_a = len(tokens_a[0]) > 1 and not is_initials_run(tokens_a)
    is_spelled_out_b = len(tokens_b[0]) > 1 and not is_initials_run(tokens_b)

    if is_spelled_out_a and is_spelled_out_b:
        if tokens_a[0] != tokens_b[0]:
            return 0
        return 2 if len(tokens_a) == len(tokens_b) else 1

    form_a = tokens_a[0] if is_initials_run(tokens_a) else initials(tokens_a)
    form_b = tokens_b[0] if is_initials_run(tokens_b) else initials(tokens_b)
    if is_spelled_out_a or is_spelled_out_b:
        # An initials run against a spelled-out name must agree exactly. Prefix
        # matching here would fuse genuinely short given names -- "Wang, Bo"
        # would become compatible with "Wang, Baoling".
        return 1 if form_a == form_b else 0
    # Both sides are initials: one may simply list fewer of them ("S." vs "SP").
    return 1 if (form_a.startswith(form_b) or form_b.startswith(form_a)) else 0


def _union_find_parent(parent: dict[int, int], node: int) -> int:
    root = node
    while parent[root] != root:
        root = parent[root]
    while parent[node] != root:  # path compression
        parent[node], node = root, parent[node]
    return root


def _fit_logistic_regression(
    feature_matrix: np.ndarray, labels: np.ndarray, l2_penalty: float
) -> np.ndarray:
    """Fit L2-penalized logistic regression by direct likelihood maximization.

    Hand-rolled rather than sklearn because sklearn is not in this project's
    default pixi environment, and six features over ~10^5 rows does not justify
    adding the dependency. scipy's BFGS on the exact gradient is more than
    adequate and keeps the module importable anywhere the repo runs.
    """
    design = np.hstack([np.ones((feature_matrix.shape[0], 1)), feature_matrix])
    n_parameters = design.shape[1]

    def negative_log_likelihood(weights: np.ndarray) -> tuple[float, np.ndarray]:
        logits = design @ weights
        # log(1 + exp(x)) computed stably for large |x|
        log_terms = np.logaddexp(0.0, logits)
        loss = float(np.sum(log_terms - labels * logits))
        penalty_weights = weights.copy()
        penalty_weights[0] = 0.0  # never penalize the intercept
        loss += 0.5 * l2_penalty * float(penalty_weights @ penalty_weights)
        probabilities = expit(logits)
        gradient = design.T @ (probabilities - labels) + l2_penalty * penalty_weights
        return loss, gradient

    result = minimize(
        negative_log_likelihood,
        x0=np.zeros(n_parameters),
        jac=True,
        method="L-BFGS-B",
    )
    return result.x


def _predict_logistic(feature_matrix: np.ndarray, weights: np.ndarray) -> np.ndarray:
    design = np.hstack([np.ones((feature_matrix.shape[0], 1)), feature_matrix])
    # expit rather than 1/(1+exp(-x)): the naive form overflows for |x| >~ 700,
    # which a large year gap against a big weight can genuinely reach.
    return expit(design @ weights)


#########################
##    DAG Definition   ##
#########################
@dataloader()
def loaded_clean_database(clean_database_path: Path) -> tuple[pd.DataFrame, dict]:
    """Load the deduplicated corpus that defines which papers exist."""
    database = pd.read_parquet(clean_database_path)
    return database, utils.get_file_metadata(clean_database_path)


@dataloader()
def loaded_scopus_database(scopus_database_path: Path) -> tuple[pd.DataFrame, dict]:
    """Load the raw Scopus export, for its inline author codes and affiliations."""
    database = pd.read_parquet(
        scopus_database_path,
        columns=["Author full names", "Author(s) ID", "Affiliations", "DOI"],
    )
    return database, utils.get_file_metadata(scopus_database_path)


@dataloader()
def loaded_wos_database(wos_database_path: Path) -> tuple[pd.DataFrame, dict]:
    """Load the raw Web of Science export, for ORCIDs and ResearcherIDs."""
    database = pd.read_parquet(wos_database_path, columns=["OI", "RI", "DI"])
    return database, utils.get_file_metadata(wos_database_path)


def raw_author_mentions(loaded_clean_database: pd.DataFrame) -> pd.DataFrame:
    """Explode the corpus into one row per (paper, author position).

    A "mention" is the atomic unit of disambiguation: one name as printed on one
    paper. Clustering mentions is what produces canonical authors.
    """
    records: list[tuple] = []
    normalized_dois = _normalize_doi(loaded_clean_database["doi"])
    for paper_index, doi, year, journal, authors in zip(
        loaded_clean_database["index"],
        normalized_dois,
        loaded_clean_database["year"],
        loaded_clean_database["journal"],
        loaded_clean_database["authors"],
    ):
        if authors is None:
            continue
        for position, raw_name in enumerate(authors):
            if raw_name is None:
                continue
            raw_name = str(raw_name).strip()
            if raw_name:
                records.append((paper_index, doi, year, journal, position, raw_name))
    mentions = pd.DataFrame(
        records,
        columns=["paper_index", "doi", "year", "journal", "author_position", "raw_name"],
    )
    logger.info("Exploded %d author mentions from %d papers",
                len(mentions), loaded_clean_database.shape[0])
    return mentions


def normalized_author_mentions(raw_author_mentions: pd.DataFrame) -> pd.DataFrame:
    """Add normalized name, last/given split, and the blocking key.

    Placeholder names are dropped outright; names that normalize to nothing are
    given a per-mention unique block so they stay separate identities.
    """
    mentions = raw_author_mentions.copy()
    split = [_split_last_and_given(name) for name in mentions["raw_name"]]
    mentions["last_name"] = [last for last, _ in split]
    mentions["given_names"] = [given for _, given in split]
    mentions["normalized_name"] = (
        mentions["last_name"] + ", " + mentions["given_names"]
    ).str.strip().str.strip(",").str.strip()

    is_placeholder = mentions["last_name"].map(_is_placeholder_name)
    dropped = int(is_placeholder.sum())
    mentions = mentions.loc[~is_placeholder].copy()
    logger.info("Dropped %d placeholder-name mentions (Anon / et al / unknown)", dropped)

    first_initial = mentions["given_names"].str[:1].fillna("")
    block = mentions["last_name"] + "|" + first_initial
    unparseable = mentions["last_name"].eq("")
    block = block.where(
        ~unparseable,
        UNPARSEABLE_BLOCK_PREFIX + mentions.index.astype(str),
    )
    mentions["block"] = block
    logger.info("Formed %d blocks; %d mentions had unparseable names",
                mentions["block"].nunique(), int(unparseable.sum()))
    return mentions.reset_index(drop=True)


def scopus_author_anchors(loaded_scopus_database: pd.DataFrame) -> pd.DataFrame:
    """Extract (doi, normalized name, Scopus author code) triples."""
    records: list[tuple] = []
    normalized_dois = _normalize_doi(loaded_scopus_database["DOI"])
    for doi, full_names in zip(normalized_dois, loaded_scopus_database["Author full names"]):
        if full_names is None:
            continue
        for name, identifier in _parse_name_with_parenthesized_identifier(full_names):
            records.append((doi, _normalize_name(name), "scopus_author_id", identifier))
    anchors = pd.DataFrame(records, columns=["doi", "normalized_name", "identifier_kind", "identifier"])
    logger.info("Parsed %d Scopus author anchors", len(anchors))
    return anchors


def wos_orcid_anchors(loaded_wos_database: pd.DataFrame) -> pd.DataFrame:
    """Extract (doi, normalized name, ORCID) triples from the WoS ``OI`` field."""
    records: list[tuple] = []
    normalized_dois = _normalize_doi(loaded_wos_database["DI"])
    for doi, orcid_field in zip(normalized_dois, loaded_wos_database["OI"]):
        for name, identifier in _parse_name_with_slashed_identifier(orcid_field):
            records.append((doi, _normalize_name(name), "orcid", identifier.lower()))
    anchors = pd.DataFrame(records, columns=["doi", "normalized_name", "identifier_kind", "identifier"])
    logger.info("Parsed %d WoS ORCID anchors", len(anchors))
    return anchors


def wos_researcher_id_anchors(loaded_wos_database: pd.DataFrame) -> pd.DataFrame:
    """Extract (doi, normalized name, ResearcherID) triples from ``RI``."""
    records: list[tuple] = []
    normalized_dois = _normalize_doi(loaded_wos_database["DI"])
    for doi, researcher_field in zip(normalized_dois, loaded_wos_database["RI"]):
        for name, identifier in _parse_name_with_slashed_identifier(researcher_field):
            records.append((doi, _normalize_name(name), "researcher_id", identifier.upper()))
    anchors = pd.DataFrame(records, columns=["doi", "normalized_name", "identifier_kind", "identifier"])
    logger.info("Parsed %d WoS ResearcherID anchors", len(anchors))
    return anchors


def external_author_anchors(
    scopus_author_anchors: pd.DataFrame,
    wos_orcid_anchors: pd.DataFrame,
    wos_researcher_id_anchors: pd.DataFrame,
) -> pd.DataFrame:
    """Stack the anchor sources into one table keyed by (doi, normalized name).

    Namespaced as ``kind:value`` so a Scopus code and an ORCID can never collide
    numerically, and so a single column carries the identity for clustering.
    """
    anchors = pd.concat(
        [scopus_author_anchors, wos_orcid_anchors, wos_researcher_id_anchors],
        ignore_index=True,
    )
    anchors = anchors[anchors["normalized_name"].str.len() > 0]
    anchors["external_identity"] = anchors["identifier_kind"] + ":" + anchors["identifier"]
    anchors = anchors.drop_duplicates(["doi", "normalized_name", "external_identity"])
    logger.info("Combined %d external anchors over %d distinct identities",
                len(anchors), anchors["external_identity"].nunique())
    return anchors


def anchored_author_mentions(
    normalized_author_mentions: pd.DataFrame, external_author_anchors: pd.DataFrame
) -> pd.DataFrame:
    """Attach an external identity to each mention where one is available.

    Joined on (doi, normalized name) rather than author position: the sources
    order authors consistently, but a name-keyed join degrades gracefully when
    one source drops an author while a positional join would silently shift
    every identifier by one.
    """
    best_anchor = external_author_anchors.drop_duplicates(["doi", "normalized_name"])
    mentions = normalized_author_mentions.merge(
        best_anchor[["doi", "normalized_name", "external_identity"]],
        on=["doi", "normalized_name"],
        how="left",
    )
    mentions["mention_id"] = np.arange(len(mentions))
    anchored_count = int(mentions["external_identity"].notna().sum())
    logger.info(
        "Anchored %d/%d mentions (%.1f%%) to an external identity",
        anchored_count, len(mentions), 100.0 * anchored_count / max(len(mentions), 1),
    )
    return mentions


def anchor_evaluation_split(anchored_author_mentions: pd.DataFrame) -> dict:
    """Split external identities into a training and a held-out half.

    The split is on *identity*, not on mention: holding out random mentions
    would leak, because a held-out mention's identity would still be visible
    through its siblings during model training.
    """
    identities = np.sort(anchored_author_mentions["external_identity"].dropna().unique())
    generator = np.random.default_rng(RANDOM_SEED)
    is_held_out = generator.random(len(identities)) < HELD_OUT_IDENTITY_FRACTION
    held_out = set(identities[is_held_out])
    logger.info("Held out %d of %d external identities for evaluation",
                len(held_out), len(identities))
    return {
        "held_out_identities": held_out,
        "training_identities": set(identities) - held_out,
    }


def mention_context_index(
    anchored_author_mentions: pd.DataFrame, loaded_scopus_database: pd.DataFrame
) -> dict:
    """Per-paper coauthor sets and affiliation token sets, keyed by paper.

    Precomputed once here rather than inside the pair loop; the pair loop runs
    ~5.7x10^5 times and would otherwise rebuild these sets constantly.
    """
    coauthors_by_paper: dict[int, frozenset[str]] = {
        paper_index: frozenset(group)
        for paper_index, group in anchored_author_mentions.groupby("paper_index")["normalized_name"]
    }
    normalized_dois = _normalize_doi(loaded_scopus_database["DOI"])
    affiliation_tokens_by_doi: dict[str, frozenset[str]] = {
        doi: _affiliation_tokens(affiliations)
        for doi, affiliations in zip(normalized_dois, loaded_scopus_database["Affiliations"])
    }
    return {
        "coauthors_by_paper": coauthors_by_paper,
        "affiliation_tokens_by_doi": affiliation_tokens_by_doi,
    }


def candidate_mention_pairs(anchored_author_mentions: pd.DataFrame) -> pd.DataFrame:
    """All within-block mention pairs -- the candidates for merging.

    Blocking on (last name, first initial) is what makes this tractable: the
    corpus has 78,011 blocks with a maximum size of 154, so exhaustive
    within-block pairing yields ~5.7x10^5 pairs against the ~1.2x10^10 pairs an
    unblocked comparison would need.
    """
    pairs: list[tuple[int, int]] = []
    for _, group in anchored_author_mentions.groupby("block", sort=False):
        mention_ids = group["mention_id"].to_numpy()
        if len(mention_ids) < 2:
            continue
        left, right = np.triu_indices(len(mention_ids), k=1)
        pairs.extend(zip(mention_ids[left], mention_ids[right]))
    pair_frame = pd.DataFrame(pairs, columns=["left_mention_id", "right_mention_id"])
    logger.info("Generated %d within-block candidate pairs", len(pair_frame))
    return pair_frame


def pairwise_disambiguation_features(
    candidate_mention_pairs: pd.DataFrame,
    anchored_author_mentions: pd.DataFrame,
    mention_context_index: dict,
) -> pd.DataFrame:
    """Compute the feature matrix for every candidate pair.

    Features, in the order given by FEATURE_NAMES:
      shared_coauthor_count -- the single strongest signal; two mentions sharing
        a coauthor are nearly always the same person.
      shared_affiliation_token_fraction -- Jaccard over institution tokens.
      same_journal -- weak but cheap topical/venue continuity.
      publication_year_gap -- careers are finite; a 40-year gap is evidence
        against identity even when the names match exactly.
      given_name_compatibility -- see _given_name_compatibility.
      log_block_size -- how crowded the surname/initial block is.

    A note on the sign of log_block_size, because it is counterintuitive: it was
    added expecting a *penalty* (a coincidence inside a 143-mention "Wang, Y"
    block should count for less than one inside a small block), but the fitted
    weight comes out positive, so in practice it raises the match probability.
    That is a real property of this corpus rather than a bug: the largest blocks
    here belong to prolific motor learning researchers -- 104 of the 106
    mentions in the "wulf|g" block are one person -- so within-block pairs in
    big blocks genuinely are more often the same individual. The feature is kept
    with its honest name, and the effect should be re-checked on any corpus
    where common surnames rather than prolific authors drive block size.
    """
    mentions = anchored_author_mentions.set_index("mention_id")
    coauthors_by_paper = mention_context_index["coauthors_by_paper"]
    affiliation_tokens_by_doi = mention_context_index["affiliation_tokens_by_doi"]

    paper_index = mentions["paper_index"].to_dict()
    doi_by_mention = mentions["doi"].to_dict()
    year_by_mention = mentions["year"].to_dict()
    journal_by_mention = mentions["journal"].to_dict()
    given_by_mention = mentions["given_names"].to_dict()
    name_by_mention = mentions["normalized_name"].to_dict()
    block_size = mentions["block"].value_counts().to_dict()
    block_by_mention = mentions["block"].to_dict()

    empty: frozenset[str] = frozenset()
    rows = np.zeros((len(candidate_mention_pairs), len(FEATURE_NAMES)), dtype=float)

    for row_number, (left, right) in enumerate(
        zip(candidate_mention_pairs["left_mention_id"], candidate_mention_pairs["right_mention_id"])
    ):
        left_paper, right_paper = paper_index[left], paper_index[right]
        left_coauthors = coauthors_by_paper.get(left_paper, empty) - {name_by_mention[left]}
        right_coauthors = coauthors_by_paper.get(right_paper, empty) - {name_by_mention[right]}
        rows[row_number, 0] = len(left_coauthors & right_coauthors)

        left_affiliation = affiliation_tokens_by_doi.get(doi_by_mention[left], empty)
        right_affiliation = affiliation_tokens_by_doi.get(doi_by_mention[right], empty)
        union_size = len(left_affiliation | right_affiliation)
        rows[row_number, 1] = (
            len(left_affiliation & right_affiliation) / union_size if union_size else 0.0
        )

        left_journal, right_journal = journal_by_mention[left], journal_by_mention[right]
        rows[row_number, 2] = float(
            left_journal is not None and left_journal == right_journal
        )

        left_year, right_year = year_by_mention[left], year_by_mention[right]
        rows[row_number, 3] = (
            abs(float(left_year) - float(right_year))
            if pd.notna(left_year) and pd.notna(right_year)
            else 0.0
        )

        rows[row_number, 4] = _given_name_compatibility(
            given_by_mention[left], given_by_mention[right]
        )
        rows[row_number, 5] = np.log1p(block_size.get(block_by_mention[left], 1))

    features = pd.DataFrame(rows, columns=list(FEATURE_NAMES))
    features["left_mention_id"] = candidate_mention_pairs["left_mention_id"].to_numpy()
    features["right_mention_id"] = candidate_mention_pairs["right_mention_id"].to_numpy()
    return features


def pair_supervision_labels(
    candidate_mention_pairs: pd.DataFrame,
    anchored_author_mentions: pd.DataFrame,
    anchor_evaluation_split: dict,
) -> pd.DataFrame:
    """Label candidate pairs from the *training* anchors only.

    A pair is a positive when both mentions carry the same training identity and
    a negative when they carry two different ones. Pairs touching a held-out
    identity, or with any unanchored side, are left unlabelled -- they are what
    the model is for.

    Note the label distribution is not the population distribution: anchored
    mentions skew toward Scopus-indexed papers. That biases the intercept, not
    the feature weights, and the threshold is set on held-out performance
    anyway, so it is recorded rather than corrected.
    """
    identity_by_mention = (
        anchored_author_mentions.set_index("mention_id")["external_identity"].to_dict()
    )
    training_identities = anchor_evaluation_split["training_identities"]

    labels = np.full(len(candidate_mention_pairs), -1, dtype=int)
    for row_number, (left, right) in enumerate(
        zip(candidate_mention_pairs["left_mention_id"], candidate_mention_pairs["right_mention_id"])
    ):
        left_identity = identity_by_mention.get(left)
        right_identity = identity_by_mention.get(right)
        if left_identity is None or right_identity is None:
            continue
        if left_identity not in training_identities or right_identity not in training_identities:
            continue
        labels[row_number] = int(left_identity == right_identity)

    labelled = pd.DataFrame({
        "left_mention_id": candidate_mention_pairs["left_mention_id"].to_numpy(),
        "right_mention_id": candidate_mention_pairs["right_mention_id"].to_numpy(),
        "label": labels,
    })
    logger.info(
        "Supervision: %d positive, %d negative, %d unlabelled candidate pairs",
        int((labels == 1).sum()), int((labels == 0).sum()), int((labels == -1).sum()),
    )
    return labelled


def pairwise_match_model(
    pairwise_disambiguation_features: pd.DataFrame, pair_supervision_labels: pd.DataFrame
) -> dict:
    """Fit the logistic match model on the anchor-supervised pairs."""
    is_labelled = pair_supervision_labels["label"].to_numpy() >= 0
    if is_labelled.sum() < 100:
        raise ValueError(
            f"Only {int(is_labelled.sum())} labelled pairs available; cannot fit a "
            "match model. Check that the DOI join to Scopus/WoS actually landed."
        )
    feature_matrix = pairwise_disambiguation_features[list(FEATURE_NAMES)].to_numpy()[is_labelled]
    labels = pair_supervision_labels["label"].to_numpy()[is_labelled].astype(float)

    feature_means = feature_matrix.mean(axis=0)
    feature_scales = feature_matrix.std(axis=0)
    feature_scales[feature_scales == 0] = 1.0
    standardized = (feature_matrix - feature_means) / feature_scales

    weights = _fit_logistic_regression(standardized, labels, LOGISTIC_L2_PENALTY)
    in_sample = _predict_logistic(standardized, weights)
    logger.info(
        "Fitted match model on %d pairs (%.1f%% positive); in-sample accuracy %.4f",
        len(labels), 100.0 * labels.mean(), float(((in_sample > 0.5) == labels).mean()),
    )
    for name, weight in zip(FEATURE_NAMES, weights[1:]):
        logger.info("  weight %-38s % .4f", name, weight)
    return {
        "weights": weights,
        "feature_means": feature_means,
        "feature_scales": feature_scales,
        "training_pair_count": int(len(labels)),
        "training_positive_fraction": float(labels.mean()),
    }


def pairwise_match_probabilities(
    pairwise_disambiguation_features: pd.DataFrame, pairwise_match_model: dict
) -> pd.DataFrame:
    """Score every candidate pair with the fitted model."""
    feature_matrix = pairwise_disambiguation_features[list(FEATURE_NAMES)].to_numpy()
    standardized = (
        feature_matrix - pairwise_match_model["feature_means"]
    ) / pairwise_match_model["feature_scales"]
    probabilities = _predict_logistic(standardized, pairwise_match_model["weights"])
    return pd.DataFrame({
        "left_mention_id": pairwise_disambiguation_features["left_mention_id"].to_numpy(),
        "right_mention_id": pairwise_disambiguation_features["right_mention_id"].to_numpy(),
        "match_probability": probabilities,
    })


def _cluster_mentions_under_constraints(
    mention_ids: np.ndarray,
    visible_identity_by_mention: dict[int, str],
    match_probabilities: pd.DataFrame,
    label: str,
) -> pd.DataFrame:
    """Union-find clustering under must-link / cannot-link identity constraints.

    Three passes:

    1. **Must-link.** Mentions sharing a *visible* external identity are merged
       unconditionally -- Scopus and ORCID are more reliable than the model.
    2. **Cannot-link.** Each component tracks the external identities it holds;
       a merge that would place two distinct identities together is refused.
    3. **Model edges**, in descending probability order, so that when a merge
       has to be refused it is the weakest evidence that loses.

    Descending order matters: union-find is order-dependent once constraints can
    veto a merge, so the confident edges must be applied first.

    ``visible_identity_by_mention`` is the seam that makes honest evaluation
    possible -- pass every anchor for the production clustering, or withhold the
    evaluation half to force the model to rediscover those groupings unaided.
    """
    parent = {int(mention_id): int(mention_id) for mention_id in mention_ids}
    identities_in_component: dict[int, set[str]] = defaultdict(set)
    for mention_id, identity in visible_identity_by_mention.items():
        identities_in_component[int(mention_id)].add(identity)

    def try_merge(left: int, right: int) -> bool:
        left_root, right_root = _union_find_parent(parent, left), _union_find_parent(parent, right)
        if left_root == right_root:
            return True
        combined = identities_in_component[left_root] | identities_in_component[right_root]
        if len(combined) > 1:
            return False  # cannot-link: two distinct external identities
        parent[right_root] = left_root
        identities_in_component[left_root] = combined
        identities_in_component.pop(right_root, None)
        return True

    mentions_by_identity: dict[str, list[int]] = defaultdict(list)
    for mention_id, identity in visible_identity_by_mention.items():
        mentions_by_identity[identity].append(int(mention_id))
    must_link_merges = 0
    for grouped_mentions in mentions_by_identity.values():
        anchor = grouped_mentions[0]
        for other in grouped_mentions[1:]:
            if _union_find_parent(parent, anchor) != _union_find_parent(parent, other):
                try_merge(anchor, other)
                must_link_merges += 1

    accepted = match_probabilities[
        match_probabilities["match_probability"] >= MATCH_PROBABILITY_THRESHOLD
    ].sort_values("match_probability", ascending=False)
    model_merges = refused_merges = 0
    for left, right in zip(accepted["left_mention_id"], accepted["right_mention_id"]):
        if try_merge(int(left), int(right)):
            model_merges += 1
        else:
            refused_merges += 1

    canonical = {
        int(mention_id): _union_find_parent(parent, int(mention_id)) for mention_id in mention_ids
    }
    clusters = pd.DataFrame({
        "mention_id": list(canonical.keys()),
        "canonical_author_id": list(canonical.values()),
    })
    logger.info(
        "[%s] clustered %d mentions into %d canonical authors "
        "(%d must-link merges, %d model merges, %d merges refused by cannot-link)",
        label, len(clusters), clusters["canonical_author_id"].nunique(),
        must_link_merges, model_merges, refused_merges,
    )
    return clusters


def constrained_author_clusters(
    anchored_author_mentions: pd.DataFrame, pairwise_match_probabilities: pd.DataFrame
) -> pd.DataFrame:
    """Production clustering: every available anchor is used.

    This is the artifact downstream stages consume, so it should exploit all the
    evidence there is. Its quality is *estimated* by the separate held-out
    clustering below, which deliberately handicaps itself.
    """
    visible = {
        int(mention_id): identity
        for mention_id, identity in zip(
            anchored_author_mentions["mention_id"], anchored_author_mentions["external_identity"]
        )
        if isinstance(identity, str)
    }
    return _cluster_mentions_under_constraints(
        anchored_author_mentions["mention_id"].to_numpy(),
        visible,
        pairwise_match_probabilities,
        label="production",
    )


def held_out_author_clusters(
    anchored_author_mentions: pd.DataFrame,
    pairwise_match_probabilities: pd.DataFrame,
    anchor_evaluation_split: dict,
) -> pd.DataFrame:
    """Evaluation clustering: the held-out identities are hidden from the run.

    Without this the report is meaningless. If the held-out anchors are allowed
    into the must-link pass, the evaluation simply re-reads the identifiers it
    is supposed to be predicting and scores a perfect 1.000 by construction --
    which is exactly what the first run of this module produced before the split
    was enforced here. Hiding them forces the pipeline to recover those
    groupings from features alone, which is the question actually being asked.
    """
    training_identities = anchor_evaluation_split["training_identities"]
    visible = {
        int(mention_id): identity
        for mention_id, identity in zip(
            anchored_author_mentions["mention_id"], anchored_author_mentions["external_identity"]
        )
        if isinstance(identity, str) and identity in training_identities
    }
    return _cluster_mentions_under_constraints(
        anchored_author_mentions["mention_id"].to_numpy(),
        visible,
        pairwise_match_probabilities,
        label="held-out evaluation",
    )


def disambiguated_author_table(
    anchored_author_mentions: pd.DataFrame, constrained_author_clusters: pd.DataFrame
) -> pd.DataFrame:
    """Mention-level output: every author mention with its canonical identity."""
    return anchored_author_mentions.merge(constrained_author_clusters, on="mention_id", how="left")


def author_clusters(disambiguated_author_table: pd.DataFrame) -> pd.DataFrame:
    """Cluster-level summary: one row per canonical author."""
    def most_common_surface_form(names: pd.Series) -> str:
        return Counter(names).most_common(1)[0][0]

    clusters = disambiguated_author_table.groupby("canonical_author_id").agg(
        mention_count=("mention_id", "size"),
        paper_count=("paper_index", "nunique"),
        first_year=("year", "min"),
        last_year=("year", "max"),
        display_name=("raw_name", most_common_surface_form),
        distinct_surface_forms=("raw_name", "nunique"),
        external_identity_count=("external_identity", "nunique"),
    ).reset_index()
    return clusters.sort_values("paper_count", ascending=False).reset_index(drop=True)


def _pairwise_and_bcubed_scores(
    truth_by_mention: dict[int, str], predicted_by_mention: dict[int, int]
) -> dict[str, float]:
    """Pairwise and B-cubed precision/recall/F1 over commonly-keyed mentions.

    Both are reported because they fail differently: pairwise scores are
    dominated by large clusters (one 100-mention author contributes 4,950 pairs
    against a 2-mention author's 1), while B-cubed weights every mention
    equally. A lumping error that pairwise barely notices shows up in B-cubed.
    """
    shared = [m for m in truth_by_mention if m in predicted_by_mention]
    truth_groups: dict[str, set[int]] = defaultdict(set)
    predicted_groups: dict[int, set[int]] = defaultdict(set)
    for mention in shared:
        truth_groups[truth_by_mention[mention]].add(mention)
        predicted_groups[predicted_by_mention[mention]].add(mention)

    def pair_count(size: int) -> int:
        return size * (size - 1) // 2

    truth_pairs = sum(pair_count(len(g)) for g in truth_groups.values())
    predicted_pairs = sum(
        pair_count(len([m for m in g if m in truth_by_mention]))
        for g in predicted_groups.values()
    )
    correct_pairs = 0
    for group in predicted_groups.values():
        labels = Counter(truth_by_mention[m] for m in group if m in truth_by_mention)
        correct_pairs += sum(pair_count(count) for count in labels.values())

    pairwise_precision = correct_pairs / predicted_pairs if predicted_pairs else 1.0
    pairwise_recall = correct_pairs / truth_pairs if truth_pairs else 1.0

    bcubed_precision_total = bcubed_recall_total = 0.0
    for mention in shared:
        predicted_group = predicted_groups[predicted_by_mention[mention]]
        truth_group = truth_groups[truth_by_mention[mention]]
        overlap = len(predicted_group & truth_group)
        bcubed_precision_total += overlap / len(predicted_group)
        bcubed_recall_total += overlap / len(truth_group)
    mention_count = max(len(shared), 1)
    bcubed_precision = bcubed_precision_total / mention_count
    bcubed_recall = bcubed_recall_total / mention_count

    def harmonic_mean(precision: float, recall: float) -> float:
        return 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0

    return {
        "evaluated_mention_count": float(len(shared)),
        "pairwise_precision": pairwise_precision,
        "pairwise_recall": pairwise_recall,
        "pairwise_f1": harmonic_mean(pairwise_precision, pairwise_recall),
        "bcubed_precision": bcubed_precision,
        "bcubed_recall": bcubed_recall,
        "bcubed_f1": harmonic_mean(bcubed_precision, bcubed_recall),
    }


def author_disambiguation_report(
    disambiguated_author_table: pd.DataFrame,
    held_out_author_clusters: pd.DataFrame,
    anchor_evaluation_split: dict,
    pairwise_match_model: dict,
    author_clusters: pd.DataFrame,
) -> pd.DataFrame:
    """Held-out validation metrics plus corpus-level diagnostics.

    Scored against ``held_out_author_clusters`` -- the run that could not see
    the evaluation identifiers -- never against the production clustering, which
    was given them and would therefore score a vacuous 1.000.

    The held-out figures answer the question that matters: given mentions whose
    true grouping the pipeline never saw, does it reconstruct that grouping? The
    diagnostics answer the question the metrics hide: is anything pathological,
    like a single canonical author absorbing hundreds of papers?
    """
    held_out = anchor_evaluation_split["held_out_identities"]
    evaluation_rows = disambiguated_author_table[
        disambiguated_author_table["external_identity"].isin(held_out)
    ]
    held_out_cluster_by_mention = dict(
        zip(held_out_author_clusters["mention_id"], held_out_author_clusters["canonical_author_id"])
    )
    truth_by_mention = dict(
        zip(evaluation_rows["mention_id"], evaluation_rows["external_identity"])
    )
    predicted_by_mention = {
        mention_id: held_out_cluster_by_mention[mention_id]
        for mention_id in truth_by_mention
        if mention_id in held_out_cluster_by_mention
    }
    metrics = _pairwise_and_bcubed_scores(truth_by_mention, predicted_by_mention)

    metrics.update({
        "held_out_identity_count": float(len(held_out)),
        "training_pair_count": float(pairwise_match_model["training_pair_count"]),
        "training_positive_fraction": pairwise_match_model["training_positive_fraction"],
        "total_mentions": float(len(disambiguated_author_table)),
        "total_canonical_authors": float(len(author_clusters)),
        "anchored_mention_fraction": float(
            disambiguated_author_table["external_identity"].notna().mean()
        ),
        "largest_cluster_paper_count": float(author_clusters["paper_count"].max()),
        "single_mention_cluster_fraction": float(
            (author_clusters["mention_count"] == 1).mean()
        ),
        "mean_papers_per_author": float(author_clusters["paper_count"].mean()),
    })
    for name, weight in zip(FEATURE_NAMES, pairwise_match_model["weights"][1:]):
        metrics[f"model_weight_{name}"] = float(weight)

    report = pd.DataFrame(
        sorted(metrics.items()), columns=["metric", "value"]
    )
    logger.info("Disambiguation report:\n%s", report.to_string(index=False))
    return report


@datasaver()
def save_disambiguated_authors(disambiguated_author_table: pd.DataFrame) -> dict:
    disambiguated_author_table.to_parquet(MENTIONS_PARQUET, index=False)
    return utils.get_file_metadata(MENTIONS_PARQUET)


@datasaver()
def save_author_clusters(author_clusters: pd.DataFrame) -> dict:
    author_clusters.to_parquet(CLUSTERS_PARQUET, index=False)
    return utils.get_file_metadata(CLUSTERS_PARQUET)


@datasaver()
def save_author_disambiguation_report(author_disambiguation_report: pd.DataFrame) -> dict:
    author_disambiguation_report.to_parquet(REPORT_PARQUET, index=False)
    return utils.get_file_metadata(REPORT_PARQUET)


##################
##     Main     ##
##################
def _main() -> int:
    inputs = dict(
        clean_database_path=PROCESSED_DATA_PATH / "clean_unified_database.parquet",
        scopus_database_path=PROCESSED_DATA_PATH / "scopus_database.parquet",
        wos_database_path=PROCESSED_DATA_PATH / "wos_database.parquet",
    )
    outputs = ["save_disambiguated_authors", "save_author_clusters", "save_author_disambiguation_report"]
    import __main__

    builder = driver.Builder().with_modules(__main__)
    if USE_TRACKER:
        builder = builder.with_adapters(
            adapters.HamiltonTracker(
                project_id=DEFAULT_UI_PROJECT_ID,
                username=DEFAULT_UI_USERNAME,
                dag_name=CURRENT_FILE_NAME,
                tags={"environment": "DEV", "team": TEAM_NAME, "version": "0.1"},
            )
        )
    dr = builder.build()

    dr.validate_execution(outputs, inputs=inputs)
    dr.display_all_functions(
        FIGURES_PATH / f"{CURRENT_FILE_NAME}_all_functions.png", keep_dot=True, deduplicate_inputs=True
    )
    dr.visualize_execution(
        outputs, inputs=inputs,
        output_file_path=FIGURES_PATH / f"{CURRENT_FILE_NAME}.png",
        keep_dot=False, deduplicate_inputs=True,
    )
    if EXECUTE:
        dr.execute(outputs, inputs=inputs)
    return 0


if __name__ == "__main__":
    sys.exit(_main())
