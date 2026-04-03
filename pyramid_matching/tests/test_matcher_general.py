import dataclasses

import pytest

from pyramid_matcher.matchers import BaseMatcher, CandidateAttributes, MatchResult


# ================================
# Helpers for BaseMatcher tests
# ================================


class _IncompleteSubclass(BaseMatcher):
    """Subclass that does NOT implement get_similarity."""

    pass


class _ConcreteSubclass(BaseMatcher):
    """Minimal concrete subclass that implements get_similarity."""

    def get_similarity(
        self, query: str, candidates: dict[str, CandidateAttributes]
    ) -> MatchResult | None:
        return None


# ================================
# MatchResult
# ================================


@pytest.fixture
def match_result() -> MatchResult:
    return MatchResult(
        query="TSHUAPA", matched="TSHUAPA", attributes=["id1"], score=100.0
    )


def test_match_result_construction(match_result: MatchResult):
    """Test that MatchResult stores all fields correctly."""
    assert match_result.query == "TSHUAPA"
    assert match_result.matched == "TSHUAPA"
    assert match_result.attributes == ["id1"]
    assert match_result.score == 100.0


def test_match_result_attributes_is_list(match_result: MatchResult):
    """Test that the attributes field is a list."""
    assert isinstance(match_result.attributes, list)


def test_match_result_frozen_existing_field(match_result: MatchResult):
    """Test that setting an existing field raises on a frozen dataclass."""
    with pytest.raises((dataclasses.FrozenInstanceError, TypeError, AttributeError)):
        match_result.score = 50.0  # type: ignore[misc]


def test_match_result_frozen_new_field(match_result: MatchResult):
    """Test that adding a new attribute raises on a frozen dataclass."""
    with pytest.raises((dataclasses.FrozenInstanceError, TypeError, AttributeError)):
        match_result.new_field = "value"  # type: ignore[attr-defined]


def test_match_result_equality():
    """Test that two MatchResults with identical values are equal."""
    r1 = MatchResult(query="A", matched="B", attributes=["id1"], score=90.0)
    r2 = MatchResult(query="A", matched="B", attributes=["id1"], score=90.0)
    assert r1 == r2


def test_match_result_inequality_by_score():
    """Test that MatchResults differing in score are not equal."""
    r1 = MatchResult(query="A", matched="B", attributes=["id1"], score=90.0)
    r2 = MatchResult(query="A", matched="B", attributes=["id1"], score=80.0)
    assert r1 != r2


def test_match_result_inequality_by_query():
    """Test that MatchResults differing in query are not equal."""
    r1 = MatchResult(query="A", matched="B", attributes=["id1"], score=90.0)
    r2 = MatchResult(query="X", matched="B", attributes=["id1"], score=90.0)
    assert r1 != r2


def test_match_result_inequality_by_attributes():
    """Test that MatchResults differing in attributes are not equal."""
    r1 = MatchResult(query="A", matched="B", attributes=["id1"], score=90.0)
    r2 = MatchResult(query="A", matched="B", attributes=["id2"], score=90.0)
    assert r1 != r2


# ================================
# BaseMatcher
# ================================


def test_base_matcher_cannot_be_instantiated():
    """Test that BaseMatcher (abstract) cannot be instantiated directly."""
    with pytest.raises(TypeError):
        BaseMatcher()  # type: ignore[abstract]


def test_base_matcher_concrete_subclass_can_be_instantiated():
    """Test that a complete subclass implementing get_similarity can be instantiated."""
    matcher = _ConcreteSubclass()
    assert isinstance(matcher, BaseMatcher)


def test_base_matcher_get_similarity_is_abstract():
    """Test that get_similarity is declared abstract on BaseMatcher."""
    assert "get_similarity" in BaseMatcher.__abstractmethods__


def test_base_matcher_concrete_subclass_get_similarity_returns_none():
    """Test that the concrete subclass get_similarity can return None."""
    matcher = _ConcreteSubclass()
    result = matcher.get_similarity("TSHUAPA", {"TSHUAPA": ["id1"]})
    assert result is None
