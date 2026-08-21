"""Unit tests for the author-disambiguation helpers.

Covers the pure functions where a silent bug would quietly corrupt every
downstream random walk: name normalization, identifier parsing, the
given-name compatibility score, constrained union-find, and the evaluation
metrics themselves (a wrong metric is worse than no metric).
"""

import numpy as np
import pandas as pd
import pytest

from motor_learning_network.disambiguate_authors import (
    _normalize_name,
    _split_last_and_given,
    _is_placeholder_name,
    _normalize_doi,
    _parse_name_with_parenthesized_identifier,
    _parse_name_with_slashed_identifier,
    _affiliation_tokens,
    _given_name_compatibility,
    _union_find_parent,
    _fit_logistic_regression,
    _predict_logistic,
    _pairwise_and_bcubed_scores,
)


class TestNameNormalization:
    def test_strips_accents_and_lowercases(self):
        assert _normalize_name("Ruiz-Olaya, Andrès Felipe") == "ruiz-olaya, andres felipe"

    def test_strips_digits_and_punctuation_but_keeps_comma(self):
        assert _normalize_name("Yavari, Fatemeh B.2") == "yavari, fatemeh b"

    def test_collapses_whitespace(self):
        assert _normalize_name("  Smith,   John   ") == "smith, john"

    def test_non_latin_script_normalizes_to_empty(self):
        # Korean names strip to nothing -- the pipeline must give these their own
        # block rather than fusing every such author into one identity.
        assert _normalize_name("윤영진") == ""

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("Wulf, Gabriele", ("wulf", "gabriele")),
            ("Cohen, L.", ("cohen", "l")),
            ("Gabriele Wulf", ("wulf", "gabriele")),
            ("Madonna", ("madonna", "")),
            ("", ("", "")),
        ],
    )
    def test_split_last_and_given(self, raw, expected):
        assert _split_last_and_given(raw) == expected


class TestPlaceholderDetection:
    @pytest.mark.parametrize("name", ["anon", "anonymous", "et al", "unknown"])
    def test_flags_placeholders(self, name):
        assert _is_placeholder_name(name)

    @pytest.mark.parametrize("name", ["anderson", "wulf", "etchells"])
    def test_keeps_real_names(self, name):
        # "anderson" must survive even though it starts with "an", and
        # "etchells" despite starting with "et".
        assert not _is_placeholder_name(name)


class TestDoiNormalization:
    def test_strips_url_prefix_and_lowercases(self):
        series = pd.Series(["https://doi.org/10.1162/JOCN_a_00675", " 10.1016/X "])
        assert list(_normalize_doi(series)) == ["10.1162/jocn_a_00675", "10.1016/x"]

    def test_strips_dx_prefix(self):
        assert _normalize_doi(pd.Series(["http://dx.doi.org/10.1/A"]))[0] == "10.1/a"


class TestIdentifierParsing:
    def test_parses_scopus_inline_codes(self):
        field = "Ruiz-Olaya, Andrès Felipe (55909479000); López-Delis, Alberto (55976515500)"
        assert _parse_name_with_parenthesized_identifier(field) == [
            ("Ruiz-Olaya, Andrès Felipe", "55909479000"),
            ("López-Delis, Alberto", "55976515500"),
        ]

    def test_scopus_parser_skips_entries_without_codes(self):
        assert _parse_name_with_parenthesized_identifier("Smith, John") == []

    def test_parses_wos_orcids(self):
        field = "Lee, Won Taek/0000-0001-7348-9562; Rhyu, Im Joo/0000-0002-5558-6278"
        assert _parse_name_with_slashed_identifier(field) == [
            ("Lee, Won Taek", "0000-0001-7348-9562"),
            ("Rhyu, Im Joo", "0000-0002-5558-6278"),
        ]

    def test_wos_parser_handles_nan_placeholder(self):
        # WoS writes the string "nan" rather than a null for missing fields.
        assert _parse_name_with_slashed_identifier("nan") == []
        assert _parse_name_with_slashed_identifier(float("nan")) == []

    def test_wos_parser_splits_on_last_slash(self):
        # A slash inside the name must not break the identifier split.
        assert _parse_name_with_slashed_identifier("Ng, A/B/0000-0001") == [("Ng, A/B", "0000-0001")]


class TestAffiliationTokens:
    def test_drops_generic_scaffolding(self):
        tokens = _affiliation_tokens("Department of Kinesiology, University of Nevada, Las Vegas, USA")
        assert "kinesiology" in tokens and "nevada" in tokens
        assert "university" not in tokens and "department" not in tokens and "usa" not in tokens

    def test_handles_none(self):
        assert _affiliation_tokens(None) == frozenset()


class TestGivenNameCompatibility:
    def test_identical_full_names_score_highest(self):
        assert _given_name_compatibility("gabriele", "gabriele") == 2

    def test_conflicting_full_names_score_zero(self):
        assert _given_name_compatibility("gabriele", "gerhard") == 0

    def test_initial_compatible_with_full_name(self):
        assert _given_name_compatibility("g", "gabriele") == 1

    def test_conflicting_initials_score_zero(self):
        assert _given_name_compatibility("g", "m") == 0

    def test_absent_given_name_is_uninformative_not_contradictory(self):
        assert _given_name_compatibility("", "gabriele") == 1

    def test_full_name_with_extra_middle_name_is_compatible(self):
        assert _given_name_compatibility("gabriele", "gabriele marie") == 1

    @pytest.mark.parametrize(
        "initials_run,spelled_out",
        [
            ("rb", "richard b"),   # Ivry
            ("dm", "daniel m"),    # Wolpert
            ("ch", "charles h"),   # Shea
            ("ra", "richard a"),   # Magill
            ("am", "adrian m"),    # Haith
            ("jw", "john w"),      # Krakauer
        ],
    )
    def test_concatenated_initials_match_the_spelled_out_name(self, initials_run, spelled_out):
        """WoS writes "Ivry, RB" where Scopus writes "Ivry, Richard B."

        Read as a spelled-out name, "rb" would look contradictory and split one
        researcher into two canonical identities.
        """
        assert _given_name_compatibility(initials_run, spelled_out) == 1
        assert _given_name_compatibility(spelled_out, initials_run) == 1

    def test_initials_run_must_agree_exactly_with_spelled_out_name(self):
        # "bo" is a real given name, not the initials B.O. -- prefix matching
        # here would wrongly fuse it with every "Ba..." name sharing a surname.
        assert _given_name_compatibility("bo", "baoling") == 0
        assert _given_name_compatibility("rb", "robert") == 0

    def test_two_initials_runs_may_list_different_depths(self):
        # "Swinnen, S." and "Swinnen, SP" are compatible; neither is spelled out.
        assert _given_name_compatibility("s", "sp") == 1
        assert _given_name_compatibility("sp", "s") == 1

    def test_conflicting_initials_runs_stay_incompatible(self):
        assert _given_name_compatibility("rb", "jw") == 0


class TestUnionFind:
    def test_finds_root_and_compresses_path(self):
        parent = {0: 0, 1: 0, 2: 1, 3: 2}
        assert _union_find_parent(parent, 3) == 0
        assert parent[3] == 0  # compressed

    def test_singleton_is_its_own_root(self):
        parent = {5: 5}
        assert _union_find_parent(parent, 5) == 5


class TestLogisticRegression:
    def test_recovers_a_separable_signal(self):
        generator = np.random.default_rng(0)
        features = generator.normal(size=(400, 2))
        labels = (features[:, 0] + 0.5 * features[:, 1] > 0).astype(float)
        weights = _fit_logistic_regression(features, labels, l2_penalty=1.0)
        predictions = _predict_logistic(features, weights)
        assert ((predictions > 0.5) == labels).mean() > 0.95
        assert weights[1] > 0  # first feature drives the label positively

    def test_probabilities_stay_in_range(self):
        features = np.array([[1e6], [-1e6]])
        weights = np.array([0.0, 1.0])
        probabilities = _predict_logistic(features, weights)
        assert np.all((probabilities >= 0.0) & (probabilities <= 1.0))


class TestClusteringMetrics:
    def test_perfect_clustering_scores_one(self):
        truth = {0: "a", 1: "a", 2: "b", 3: "b"}
        predicted = {0: 10, 1: 10, 2: 20, 3: 20}
        scores = _pairwise_and_bcubed_scores(truth, predicted)
        assert scores["pairwise_f1"] == pytest.approx(1.0)
        assert scores["bcubed_f1"] == pytest.approx(1.0)

    def test_total_lumping_has_perfect_recall_and_poor_precision(self):
        truth = {0: "a", 1: "a", 2: "b", 3: "b"}
        predicted = {0: 1, 1: 1, 2: 1, 3: 1}  # everything in one cluster
        scores = _pairwise_and_bcubed_scores(truth, predicted)
        assert scores["pairwise_recall"] == pytest.approx(1.0)
        assert scores["pairwise_precision"] == pytest.approx(2 / 6)
        assert scores["bcubed_precision"] == pytest.approx(0.5)

    def test_total_splitting_has_perfect_precision_and_zero_recall(self):
        truth = {0: "a", 1: "a", 2: "b", 3: "b"}
        predicted = {0: 1, 1: 2, 2: 3, 3: 4}  # every mention its own cluster
        scores = _pairwise_and_bcubed_scores(truth, predicted)
        assert scores["pairwise_recall"] == pytest.approx(0.0)
        assert scores["bcubed_precision"] == pytest.approx(1.0)
        assert scores["bcubed_recall"] == pytest.approx(0.5)

    def test_pairwise_and_bcubed_weight_lumping_differently(self):
        """The two metrics are reported together because they disagree.

        One 10-mention author lumped with one 2-mention author, all in a single
        predicted cluster. Pairwise weights by pair count, B-cubed weights every
        mention equally, so they land on different numbers -- and *which* is
        harsher depends on the cluster-size distribution rather than being fixed.
        Exact values are pinned here so a regression in either formula shows up.
        """
        truth = {i: "big" for i in range(10)} | {10: "small", 11: "small"}
        predicted = {i: 1 for i in range(12)}
        scores = _pairwise_and_bcubed_scores(truth, predicted)

        # pairwise: correct pairs C(10,2)+C(2,2)=46 over predicted pairs C(12,2)=66
        assert scores["pairwise_precision"] == pytest.approx(46 / 66)
        # b-cubed: 10 mentions see 10/12 of their cluster as correct, 2 see 2/12
        assert scores["bcubed_precision"] == pytest.approx((10 * (10 / 12) + 2 * (2 / 12)) / 12)
        # both have perfect recall -- nothing that belonged together was separated
        assert scores["pairwise_recall"] == pytest.approx(1.0)
        assert scores["bcubed_recall"] == pytest.approx(1.0)
        assert scores["pairwise_precision"] != pytest.approx(scores["bcubed_precision"])
