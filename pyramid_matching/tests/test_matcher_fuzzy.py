import pytest

from pyramid_matcher.matchers import FuzzyMatcher


@pytest.fixture
def fuzzy_matcher() -> FuzzyMatcher:
    """Fixture that returns an instance of FuzzyMatcher.

    Returns
    -------
    FuzzyMatcher
        An instance of the FuzzyMatcher class.
    """
    return FuzzyMatcher()


def test_fuzzy_matcher_str(fuzzy_matcher: FuzzyMatcher):
    """Test that the string representation of FuzzyMatcher contains expected substrings."""
    text = str(fuzzy_matcher)
    assert "FuzzyMatcher" in text
    assert "WRatio" in text


def test_fuzzy_matcher_ratio_change(fuzzy_matcher: FuzzyMatcher):
    """Test string representation of FuzzyMatcher contains 'ratio' when scorer==fuzz.ratio."""
    fuzzy_matcher.set_scorer("ratio")
    text = str(fuzzy_matcher)
    assert "FuzzyMatcher" in text
    assert "ratio" in text


def test_fuzzy_get_similarity_match(fuzzy_matcher: FuzzyMatcher):
    """Test that get_similarity returns the correct match and score for a valid query."""
    candidates = {
        "TSHUAPA": ["ym2K6YcSNl9"],
        "HAUT LOMAMI": ["fEKDiQIuqeE"],
        "KWILU": ["BmKjwqc6BEw"],
        "HAUT KATANGA": ["F9w3VW1cQmb"],
        "EQUATEUR": ["XjeRGfqHMrl"],
        "MANIEMA": ["uyuwe6bqphf"],
    }
    result = fuzzy_matcher.get_similarity("TSHUAPA", candidates)
    assert result.query == "TSHUAPA", "Expected query to be 'TSHUAPA'"
    assert result.matched == "TSHUAPA", "Expected query to be 'TSHUAPA'"
    assert result.score == 100, "Expected score to be 100"
    assert result.attributes == ["ym2K6YcSNl9"], "Expected  list of attributes, result[2] == 'ym2K6YcSNl9'"


def test_matcher_match_with_fuzzy_threshold(fuzzy_matcher: FuzzyMatcher):
    """Test fuzzy matcher similarity function with a set of candidates."""
    candidates = {
        "TSHUAPA": ["ym2K6YcSNl9"],
        "HAUT LOMAMI": ["fEKDiQIuqeE"],
        "KWILU": ["BmKjwqc6BEw"],
        "HAUT KATANGA": ["F9w3VW1cQmb"],
        "EQUATEUR": ["XjeRGfqHMrl"],
        "MANIEMA": ["uyuwe6bqphf"],
    }
    fuzzy_matcher.set_threshold(90)
    result = fuzzy_matcher.get_similarity("TSHUAPAS", candidates)
    assert result.matched == "TSHUAPA"
    assert result.attributes == ["ym2K6YcSNl9"], "Expected  list of attributes, result[3] == 'ym2K6YcSNl9'"
    assert result.score > 93.3, "Expected score > 93.33"
    fuzzy_matcher.set_threshold(95)
    result = fuzzy_matcher.get_similarity("TSHUAPAS", candidates)
    assert result is None, "Expected no match above threshold 95"


def test_fuzzy_get_similarity_no_match(fuzzy_matcher: FuzzyMatcher):
    """Test that get_similarity returns None when there is no match above the threshold."""
    candidates = {
        "TSHUAPA": ["ym2K6YcSNl9"],
        "HAUT LOMAMI": ["fEKDiQIuqeE"],
        "KWILU": ["BmKjwqc6BEw"],
        "HAUT KATANGA": ["F9w3VW1cQmb"],
        "EQUATEUR": ["XjeRGfqHMrl"],
        "MANIEMA": ["uyuwe6bqphf"],
    }
    fuzzy_matcher.set_threshold(80)
    result = fuzzy_matcher.get_similarity("NOWEHERE", candidates)
    assert result is None, "Expected query to be None"
