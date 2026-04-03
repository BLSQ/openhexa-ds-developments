import dataclasses

import pytest

from pyramid_matcher.matchers import BaseMatcher, FuzzyMatcher, MatchResult

import tests.test_config as test_config


# ================================
# Fixtures
# ================================


@pytest.fixture
def fuzzy_matcher() -> FuzzyMatcher:
    """Fixture that returns a default FuzzyMatcher instance (threshold=80, scorer=WRatio).

    Returns
    -------
    FuzzyMatcher
        An instance of the FuzzyMatcher class.
    """
    return FuzzyMatcher()


# ================================
# Initialization
# ================================


def test_fuzzy_matcher_default_initialization():
    """Test that FuzzyMatcher initializes with default threshold and WRatio scorer."""
    matcher = FuzzyMatcher()
    assert matcher.threshold == 80
    assert matcher.scorer == matcher.fuzz.WRatio


def test_fuzzy_matcher_custom_initialization():
    """Test that FuzzyMatcher respects custom threshold and scorer_name at construction."""
    matcher = FuzzyMatcher(threshold=90, scorer_name="ratio")
    assert matcher.threshold == 90
    assert matcher.scorer == matcher.fuzz.ratio


def test_fuzzy_matcher_is_base_matcher():
    """Test that FuzzyMatcher is a subclass of BaseMatcher."""
    assert isinstance(FuzzyMatcher(), BaseMatcher)


# ================================
# set_scorer
# ================================


@pytest.mark.parametrize(
    "scorer_name,expected_fn_name",
    [
        ("ratio", "ratio"),
        ("partial_ratio", "partial_ratio"),
        ("token_sort_ratio", "token_sort_ratio"),
        ("token_set_ratio", "token_set_ratio"),
        ("wratio", "WRatio"),
    ],
)
def test_set_scorer_all_valid(
    fuzzy_matcher: FuzzyMatcher, scorer_name: str, expected_fn_name: str
):
    """Test that all supported scorer names set the correct scorer function."""
    fuzzy_matcher.set_scorer(scorer_name)
    assert fuzzy_matcher.scorer.__name__ == expected_fn_name


def test_set_scorer_case_insensitive(fuzzy_matcher: FuzzyMatcher):
    """Test that scorer names are treated case-insensitively."""
    fuzzy_matcher.set_scorer("RATIO")
    assert fuzzy_matcher.scorer == fuzzy_matcher.fuzz.ratio


def test_set_scorer_invalid_raises(fuzzy_matcher: FuzzyMatcher):
    """Test that an unknown scorer name raises a ValueError."""
    with pytest.raises(ValueError, match="Unknown scorer"):
        fuzzy_matcher.set_scorer("cosine")


# ================================
# set_threshold
# ================================


def test_set_threshold(fuzzy_matcher: FuzzyMatcher):
    """Test that set_threshold updates the threshold attribute."""
    fuzzy_matcher.set_threshold(95)
    assert fuzzy_matcher.threshold == 95


# ================================
# __str__
# ================================


def test_fuzzy_matcher_str_default(fuzzy_matcher: FuzzyMatcher):
    """Test that the string representation of FuzzyMatcher contains expected substrings."""
    text = str(fuzzy_matcher)
    assert "FuzzyMatcher" in text
    assert "WRatio" in text


@pytest.mark.parametrize(
    "scorer_name,expected_in_str",
    [
        ("ratio", "ratio"),
        ("partial_ratio", "partial_ratio"),
        ("token_sort_ratio", "token_sort_ratio"),
        ("token_set_ratio", "token_set_ratio"),
        ("wratio", "WRatio"),
    ],
)
def test_str_all_scorers(
    fuzzy_matcher: FuzzyMatcher, scorer_name: str, expected_in_str: str
):
    """Test that __str__ reflects the active scorer name for every valid scorer."""
    fuzzy_matcher.set_scorer(scorer_name)
    assert expected_in_str in str(fuzzy_matcher)


# ================================
# get_similarity — basic
# ================================


def test_fuzzy_get_similarity_exact_match(fuzzy_matcher: FuzzyMatcher):
    """Test that get_similarity returns the correct MatchResult for an exact match."""
    result = fuzzy_matcher.get_similarity("TSHUAPA", test_config.candidates_dict)
    assert result is not None
    assert result.query == "TSHUAPA"
    assert result.matched == "TSHUAPA"
    assert result.score == 100
    assert result.attributes == ["ym2K6YcSNl9"]


def test_fuzzy_get_similarity_returns_match_result_type(fuzzy_matcher: FuzzyMatcher):
    """Test that get_similarity returns a MatchResult instance."""
    result = fuzzy_matcher.get_similarity("TSHUAPA", test_config.candidates_dict)
    assert isinstance(result, MatchResult)


def test_fuzzy_get_similarity_result_field_types(fuzzy_matcher: FuzzyMatcher):
    """Test that MatchResult fields have the expected types."""
    result = fuzzy_matcher.get_similarity("TSHUAPA", test_config.candidates_dict)
    assert result is not None
    assert isinstance(result.query, str)
    assert isinstance(result.matched, str)
    assert isinstance(result.attributes, list)
    assert isinstance(result.score, (int, float))


def test_fuzzy_get_similarity_no_match(fuzzy_matcher: FuzzyMatcher):
    """Test that get_similarity returns None when the best score is below threshold."""
    result = fuzzy_matcher.get_similarity("NOWHERE", test_config.candidates_dict)
    assert result is None


def test_fuzzy_get_similarity_empty_candidates(fuzzy_matcher: FuzzyMatcher):
    """Test that get_similarity returns None when the candidates dict is empty."""
    result = fuzzy_matcher.get_similarity("TSHUAPA", {})
    assert result is None


# ================================
# get_similarity — threshold
# ================================


def test_get_similarity_threshold_above_returns_match(fuzzy_matcher: FuzzyMatcher):
    """Test that a fuzzy match above the threshold is returned."""
    fuzzy_matcher.set_threshold(90)
    result = fuzzy_matcher.get_similarity("TSHUAPAS", test_config.candidates_dict)
    assert result is not None
    assert result.matched == "TSHUAPA"
    assert result.attributes == ["ym2K6YcSNl9"]
    assert result.score > 93.3


def test_get_similarity_threshold_below_returns_none(fuzzy_matcher: FuzzyMatcher):
    """Test that a fuzzy match below the threshold returns None."""
    fuzzy_matcher.set_threshold(95)
    result = fuzzy_matcher.get_similarity("TSHUAPAS", test_config.candidates_dict)
    assert result is None


def test_get_similarity_threshold_exact_match_at_100(fuzzy_matcher: FuzzyMatcher):
    """Test that a score exactly equal to the threshold passes (>= comparison)."""
    fuzzy_matcher.set_threshold(100)
    result = fuzzy_matcher.get_similarity("TSHUAPA", test_config.candidates_dict)
    assert result is not None
    assert result.score == 100


def test_get_similarity_threshold_100_rejects_near_match(fuzzy_matcher: FuzzyMatcher):
    """Test that a near-match (score < 100) is rejected when threshold==100."""
    fuzzy_matcher.set_threshold(100)
    result = fuzzy_matcher.get_similarity("TSHUAPAS", test_config.candidates_dict)
    assert result is None


# ================================
# get_similarity — candidates structure
# ================================


def test_get_similarity_multiple_attributes(fuzzy_matcher: FuzzyMatcher):
    """Test that multiple attributes per candidate are all returned in MatchResult."""
    result = fuzzy_matcher.get_similarity(
        "TSHUAPA", test_config.candidates_multiple_attributes
    )
    assert result is not None
    assert result.attributes == ["id1", "id2", "id3"]


def test_get_similarity_selects_best_match(fuzzy_matcher: FuzzyMatcher):
    """Test that the highest-scoring candidate is selected when multiple candidates are close."""
    result = fuzzy_matcher.get_similarity("TSHUAPA", test_config.candidates_best_match)
    assert result is not None
    assert result.matched == "TSHUAPA"
    assert result.score == 100


def test_get_similarity_single_candidate_match(fuzzy_matcher: FuzzyMatcher):
    """Test that a single candidate is matched correctly."""
    candidates = {"TSHUAPA": ["ym2K6YcSNl9"]}
    result = fuzzy_matcher.get_similarity("TSHUAPA", candidates)
    assert result is not None
    assert result.matched == "TSHUAPA"


# ================================
# MatchResult immutability
# ================================


def test_match_result_is_frozen(fuzzy_matcher: FuzzyMatcher):
    """Test that MatchResult is immutable (frozen dataclass)."""
    result = fuzzy_matcher.get_similarity("TSHUAPA", test_config.candidates_dict)
    with pytest.raises((dataclasses.FrozenInstanceError, TypeError, AttributeError)):
        result.score = 50  # type: ignore[misc]
