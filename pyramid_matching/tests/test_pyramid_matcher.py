import logging
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from pyramid_matcher.matchers import BaseMatcher, FuzzyMatcher
from pyramid_matcher.pyramid_matcher import PyramidMatcher
import pyramid_matcher.pyramid_matcher as pyma

import tests.test_config as test_config


# ================================
# Fixtures
# ================================


@pytest.fixture
def pyramid_matcher() -> PyramidMatcher:
    """Fixture that returns a default PyramidMatcher instance.

    Returns
    -------
    PyramidMatcher
        An instance of the PyramidMatcher class with default arguments.
    """
    return PyramidMatcher()


# ================================
# Initialization
# ================================
# What we test:
# 1. By default, PyramidMatcher creates a FuzzyMatcher with threshold 80 and WRatio scorer. →
#    the matcher attribute is a FuzzyMatcher; its threshold and scorer are checked separately.
#    → test_default_init_uses_fuzzy_matcher, test_default_fuzzy_matcher_threshold,
#    test_default_fuzzy_matcher_scorer
#
# 2. A custom matcher passed at construction is stored directly. → the matcher attribute is
#    the exact object passed in.
#    → test_custom_matcher_is_used
#
# 3. Logger behaviour depends on whether current_run is present in globals: if absent, a
#    standard Python logger at INFO level with a StreamHandler is created; if present, logger
#    is None so that the OpenHEXA run logger is used instead. → monkeypatch injects / removes
#    current_run from the module dict before instantiating.
#    → test_default_logger_without_current_run, test_default_logger_has_stream_handler,
#    test_default_logger_with_current_run
#
# 4. A custom logger passed at construction is stored directly. →
#    → test_custom_logger_is_used
#
# 5. The default column prefixes are "candidate_" and "reference_". →
#    → test_default_prefixes


def test_default_init_uses_fuzzy_matcher(pyramid_matcher: PyramidMatcher):
    """Test that PyramidMatcher defaults to a FuzzyMatcher when no matcher is provided."""
    assert isinstance(pyramid_matcher.matcher, FuzzyMatcher)


def test_default_fuzzy_matcher_threshold(pyramid_matcher: PyramidMatcher):
    """Test that the default FuzzyMatcher is initialised with threshold 80."""
    assert pyramid_matcher.matcher.threshold == 80


def test_default_fuzzy_matcher_scorer(pyramid_matcher: PyramidMatcher):
    """Test that the default FuzzyMatcher is initialised with the WRatio scorer."""
    assert pyramid_matcher.matcher.scorer == pyramid_matcher.matcher.fuzz.WRatio


def test_custom_matcher_is_used():
    """Test that a custom matcher provided at construction is stored on the instance."""

    class _DummyMatcher(BaseMatcher):
        def get_similarity(self, query, candidates):
            return None

    custom_matcher = _DummyMatcher()
    pm = PyramidMatcher(matcher=custom_matcher)
    assert pm.matcher is custom_matcher


def test_default_logger_without_current_run(monkeypatch):
    """Test that when current_run is None (e.g. Jupyter), a stdlib logger at INFO level is created."""
    monkeypatch.setattr(pyma, "current_run", None)

    matcher = PyramidMatcher()

    assert matcher.logger is not None
    assert matcher.logger.level == logging.INFO


def test_default_logger_with_current_run(monkeypatch):
    """Test that when current_run is available (pipeline context), the logger is None."""
    fake_run = MagicMock()
    monkeypatch.setattr(pyma, "current_run", fake_run)

    matcher = PyramidMatcher()

    assert matcher.logger is None


def test_default_logger_has_stream_handler(monkeypatch):
    """Test that the default logger has a StreamHandler attached."""
    monkeypatch.setattr(pyma, "current_run", None)

    matcher = PyramidMatcher()
    assert matcher.logger is not None
    assert any(isinstance(h, logging.StreamHandler) for h in matcher.logger.handlers)


def test_custom_logger_is_used():
    """Test that a custom logger provided at construction is stored on the instance."""
    custom_logger = logging.getLogger("test_logger")
    pm = PyramidMatcher(logger=custom_logger)
    assert pm.logger is custom_logger


def test_default_prefixes(pyramid_matcher: PyramidMatcher):
    """Test that the default candidate and reference prefixes are set correctly."""
    assert pyramid_matcher.prefix_candidate_data == "candidate_"
    assert pyramid_matcher.prefix_reference_data == "reference_"


# ================================
# _set_reference_pyramid and _set_candidate_pyramid
# ================================
# What we test:
# 1. A Polars DataFrame is accepted and stored as-is; a pandas DataFrame is automatically
#    converted to Polars. → the stored attribute is always a pl.DataFrame.
#    → test_set_reference_pyramid_accepts_polars, test_set_reference_pyramid_converts_pandas,
#    test_set_candidate_pyramid_accepts_polars, test_set_candidate_pyramid_converts_pandas
#
# 2. Duplicate rows in the input are removed before storing. → a pyramid with one repeated
#    row ends up with the original unique row count.
#    → test_set_reference_pyramid_deduplicates_rows, test_set_candidate_pyramid_deduplicates_rows
#
# 3. An input that fails _is_valid (e.g. a bare string, which Polars converts to a
#    character-column DataFrame with no level_ columns) raises a ValueError. →
#    → test_set_reference_pyramid_raises_on_unconvertible_input,
#    test_set_candidate_pyramid_raises_on_unconvertible_input


def test_set_reference_pyramid_accepts_polars(pyramid_matcher: PyramidMatcher):
    """Test that a Polars DataFrame is accepted and stored as-is."""
    pyramid_matcher._set_reference_pyramid(test_config.reference_pyramid)
    assert isinstance(pyramid_matcher.reference_pyramid, pl.DataFrame)


def test_set_reference_pyramid_converts_pandas(pyramid_matcher: PyramidMatcher):
    """Test that a pandas DataFrame is converted to Polars and stored."""
    pyramid_matcher._set_reference_pyramid(test_config.reference_pyramid_pandas)
    assert isinstance(pyramid_matcher.reference_pyramid, pl.DataFrame)


def test_set_reference_pyramid_deduplicates_rows(pyramid_matcher: PyramidMatcher):
    """Test that duplicate rows are removed when storing the reference pyramid."""
    pyramid_matcher._set_reference_pyramid(
        test_config.reference_pyramid_with_duplicates
    )
    assert pyramid_matcher.reference_pyramid.shape[0] == 10


def test_set_reference_pyramid_raises_on_unconvertible_input(
    pyramid_matcher: PyramidMatcher,
):
    """Test that a ValueError is raised when the input cannot be converted to a DataFrame."""
    with pytest.raises(ValueError, match="Invalid reference pyramid format"):
        pyramid_matcher._set_reference_pyramid("not_a_dataframe")  # type: ignore[arg-type]


def test_set_candidate_pyramid_accepts_polars(pyramid_matcher: PyramidMatcher):
    """Test that a Polars DataFrame is accepted and stored as-is."""
    pyramid_matcher._set_candidate_pyramid(test_config.candidate_pyramid)
    assert isinstance(pyramid_matcher.candidate_pyramid, pl.DataFrame)


def test_set_candidate_pyramid_converts_pandas(pyramid_matcher: PyramidMatcher):
    """Test that a pandas DataFrame is converted to Polars and stored."""
    pyramid_matcher._set_candidate_pyramid(test_config.candidate_pyramid_pandas)
    assert isinstance(pyramid_matcher.candidate_pyramid, pl.DataFrame)


def test_set_candidate_pyramid_deduplicates_rows(pyramid_matcher: PyramidMatcher):
    """Test that duplicate rows are removed when storing the candidate pyramid."""
    pyramid_matcher._set_candidate_pyramid(
        test_config.candidate_pyramid_with_duplicates
    )
    assert pyramid_matcher.candidate_pyramid.shape[0] == 10


def test_set_candidate_pyramid_raises_on_unconvertible_input(
    pyramid_matcher: PyramidMatcher,
):
    """Test that a ValueError is raised when the input cannot be converted to a DataFrame."""
    with pytest.raises(ValueError, match="Invalid candidate pyramid format"):
        pyramid_matcher._set_candidate_pyramid("not_a_dataframe")  # type: ignore[arg-type]


# ================================
# _is_valid
# ================================
# What we test:
# 1. A DataFrame with at least one column whose name starts with "level_" is valid. →
#    → test_is_valid_returns_true_with_level_columns
#
# 2. A DataFrame whose columns do not start with "level_" returns False, even if it has
#    other columns. → test_is_valid_returns_false_without_level_columns
#
# 3. An empty DataFrame with no columns at all also returns False. →
#    → test_is_valid_returns_false_for_empty_dataframe


def test_is_valid_returns_true_with_level_columns(pyramid_matcher: PyramidMatcher):
    """Test that _is_valid returns True when the DataFrame has at least one level_ column."""
    assert pyramid_matcher._is_valid(test_config.reference_pyramid) is True


def test_is_valid_returns_false_without_level_columns(pyramid_matcher: PyramidMatcher):
    """Test that _is_valid returns False when no column starts with 'level_'."""
    assert (
        pyramid_matcher._is_valid(test_config.reference_pyramid_no_level_cols) is False
    )


def test_is_valid_returns_false_for_empty_dataframe(pyramid_matcher: PyramidMatcher):
    """Test that _is_valid returns False for a DataFrame with no columns at all."""
    assert pyramid_matcher._is_valid(pl.DataFrame()) is False


# ================================
# _get_levels_to_match
# ================================
# What we test:
# 1. When both pyramids share all levels (detected via column suffix), they are all returned
#    sorted. → the standard 5-level setup is used.
#    → test_get_levels_to_match_returns_common_levels
#
# 2. When the two pyramids share no common levels, an empty list is returned. → the candidate
#    has only a non-numeric level name (level_a_*) absent from the reference.
#    → test_get_levels_to_match_returns_empty_when_no_common_levels
#
# 3. When the pyramids share only a subset of levels, only those shared levels are returned. →
#    the candidate has only levels 1–3.
#    → test_get_levels_to_match_partial_intersection
#
# 4. The matching_col_suffix parameter controls which column suffix is scanned for detection. →
#    using "_id" instead of "_name" still finds all 5 levels.
#    → test_get_levels_to_match_with_custom_suffix


@pytest.fixture
def pyramid_matcher_loaded() -> PyramidMatcher:
    """Fixture that returns a PyramidMatcher with both pyramids already loaded.

    Returns
    -------
    PyramidMatcher
        An instance with reference_pyramid and candidate_pyramid set.
    """
    pm = PyramidMatcher()
    pm._set_reference_pyramid(test_config.reference_pyramid)
    pm._set_candidate_pyramid(test_config.candidate_pyramid)
    return pm


def test_get_levels_to_match_returns_common_levels(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """Test that _get_levels_to_match returns all levels present in both pyramids, sorted."""
    levels = pyramid_matcher_loaded._get_levels_to_match("_name")
    assert levels == ["level_1", "level_2", "level_3", "level_4", "level_5"]


def test_get_levels_to_match_returns_empty_when_no_common_levels(
    pyramid_matcher: PyramidMatcher,
):
    """Test that _get_levels_to_match returns an empty list when no levels are shared."""
    pyramid_matcher._set_reference_pyramid(test_config.reference_pyramid)
    pyramid_matcher._set_candidate_pyramid(test_config.candidate_pyramid_no_overlap)
    levels = pyramid_matcher._get_levels_to_match("_name")
    assert levels == []


def test_get_levels_to_match_partial_intersection(pyramid_matcher: PyramidMatcher):
    """Test that _get_levels_to_match returns only the levels shared by both pyramids."""
    pyramid_matcher._set_reference_pyramid(test_config.reference_pyramid)
    pyramid_matcher._set_candidate_pyramid(test_config.candidate_pyramid_partial)
    levels = pyramid_matcher._get_levels_to_match("_name")
    assert levels == ["level_1", "level_2", "level_3"]


def test_get_levels_to_match_with_custom_suffix(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """Test that _get_levels_to_match respects a custom matching_col_suffix."""
    levels = pyramid_matcher_loaded._get_levels_to_match("_id")
    assert levels == ["level_1", "level_2", "level_3", "level_4", "level_5"]


# ================================
# _check_levels
# ================================
# What we test:
# 1. All levels present in both pyramids pass without raising. →
#    → test_check_levels_valid_does_not_raise
#
# 2. An empty list of levels passes without raising. →
#    → test_check_levels_empty_list_does_not_raise
#
# 3. A custom suffix is respected when checking column presence. →
#    → test_check_levels_custom_suffix_does_not_raise
#
# 4. A level whose column is missing from the reference pyramid raises ValueError. → a
#    non-existent level_6 is passed.
#    → test_check_levels_raises_if_level_missing_in_reference
#
# 5. A level whose column is missing from the candidate pyramid raises ValueError. → the
#    candidate has non-overlapping level names (level_a_*).
#    → test_check_levels_raises_if_level_missing_in_candidate


def test_check_levels_valid_does_not_raise(pyramid_matcher_loaded: PyramidMatcher):
    """Test that _check_levels does not raise when all levels are present in both pyramids."""
    pyramid_matcher_loaded._check_levels(
        ["level_1", "level_2", "level_3", "level_4", "level_5"], "_name"
    )


def test_check_levels_empty_list_does_not_raise(pyramid_matcher_loaded: PyramidMatcher):
    """Test that _check_levels does not raise when the levels list is empty."""
    pyramid_matcher_loaded._check_levels([], "_name")


def test_check_levels_custom_suffix_does_not_raise(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """Test that _check_levels respects a custom suffix when checking column presence."""
    pyramid_matcher_loaded._check_levels(
        ["level_1", "level_2", "level_3", "level_4", "level_5"], "_id"
    )


def test_check_levels_raises_if_level_missing_in_reference(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """Test that a ValueError is raised when a level column is absent from the reference pyramid."""
    with pytest.raises(ValueError, match="not present in data dataframe"):
        pyramid_matcher_loaded._check_levels(
            ["level_1", "level_2", "level_3", "level_4", "level_6"], "_name"
        )


def test_check_levels_raises_if_level_missing_in_candidate(
    pyramid_matcher: PyramidMatcher,
):
    """Test that a ValueError is raised when a level column is absent from the candidate pyramid."""
    pyramid_matcher._set_reference_pyramid(test_config.reference_pyramid)
    pyramid_matcher._set_candidate_pyramid(test_config.candidate_pyramid_no_overlap)
    with pytest.raises(ValueError, match="not present in pyramid dataframe"):
        pyramid_matcher._check_levels(["level_1"], "_name")


# ================================
# _get_attributes
# ================================
# What we test:
# 1. The result is a dict keyed by every level passed in. →
#    → test_get_attributes_returns_dict_keyed_by_level
#
# 2. Each level entry contains exactly a "reference" key and a "candidate" key. →
#    → test_get_attributes_each_level_has_reference_and_candidate
#
# 3. The matching column (level_N_name) itself is excluded from both attribute lists. →
#    → test_get_attributes_excludes_matching_column
#
# 4. Reference-specific extra columns (id, region, code, chief_town) are collected per level. →
#    → test_get_attributes_reference_extra_columns
#
# 5. Candidate-specific extra columns (id, source, abbreviation, population_group) are
#    collected per level. → test_get_attributes_candidate_extra_columns
#
# 6. A level that has no domain-specific extra columns beyond its id returns only the id. →
#    level_4 has no extra reference or candidate attributes.
#    → test_get_attributes_level_with_no_domain_attrs
#
# 7. A level with multiple extra attributes returns all of them. → level_5 has id + 2 extras
#    on both the reference and candidate sides.
#    → test_get_attributes_level_with_multiple_attrs
#
# 8. Reference-only columns are absent from the candidate list and vice versa. →
#    → test_get_attributes_no_cross_contamination


def test_get_attributes_returns_dict_keyed_by_level(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """Test that _get_attributes returns a dict with one key per level."""
    levels = ["level_1", "level_2", "level_3", "level_4", "level_5"]
    result = pyramid_matcher_loaded._get_attributes(levels, "_name")
    assert list(result.keys()) == levels


def test_get_attributes_each_level_has_reference_and_candidate(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """Test that each level entry contains both 'reference' and 'candidate' keys."""
    result = pyramid_matcher_loaded._get_attributes(["level_1"], "_name")
    assert "reference" in result["level_1"]
    assert "candidate" in result["level_1"]


def test_get_attributes_excludes_matching_column(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """Test that the matching column itself is not included in the attributes."""
    result = pyramid_matcher_loaded._get_attributes(["level_1", "level_5"], "_name")
    assert "level_1_name" not in result["level_1"]["reference"]
    assert "level_1_name" not in result["level_1"]["candidate"]
    assert "level_5_name" not in result["level_5"]["reference"]
    assert "level_5_name" not in result["level_5"]["candidate"]


def test_get_attributes_reference_extra_columns(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """Test that reference-specific attribute columns are included for each level."""
    result = pyramid_matcher_loaded._get_attributes(
        ["level_1", "level_2", "level_3"], "_name"
    )
    assert "level_1_region" in result["level_1"]["reference"]
    assert "level_2_code" in result["level_2"]["reference"]
    assert "level_3_chief_town" in result["level_3"]["reference"]


def test_get_attributes_candidate_extra_columns(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """Test that candidate-specific attribute columns are included for each level."""
    result = pyramid_matcher_loaded._get_attributes(
        ["level_1", "level_2", "level_3"], "_name"
    )
    assert "level_1_source" in result["level_1"]["candidate"]
    assert "level_2_abbreviation" in result["level_2"]["candidate"]
    assert "level_3_population_group" in result["level_3"]["candidate"]


def test_get_attributes_level_with_no_domain_attrs(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """Test that level_4 returns only the id column since it has no extra domain attributes."""
    result = pyramid_matcher_loaded._get_attributes(["level_4"], "_name")
    assert result["level_4"]["reference"] == ["level_4_id"]
    assert result["level_4"]["candidate"] == ["level_4_id"]


def test_get_attributes_level_with_multiple_attrs(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """Test that level_5 returns all three attribute columns (id + two domain attrs)."""
    result = pyramid_matcher_loaded._get_attributes(["level_5"], "_name")
    assert result["level_5"]["reference"] == [
        "level_5_id",
        "level_5_type",
        "level_5_settlement",
    ]
    assert result["level_5"]["candidate"] == [
        "level_5_id",
        "level_5_urban_rural",
        "level_5_facility_count",
    ]


def test_get_attributes_no_cross_contamination(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """Test that reference-only columns are absent from candidate results and vice versa."""
    result = pyramid_matcher_loaded._get_attributes(["level_1", "level_5"], "_name")
    assert "level_1_region" not in result["level_1"]["candidate"]
    assert "level_1_source" not in result["level_1"]["reference"]
    assert "level_5_type" not in result["level_5"]["candidate"]
    assert "level_5_settlement" not in result["level_5"]["candidate"]
    assert "level_5_urban_rural" not in result["level_5"]["reference"]
    assert "level_5_facility_count" not in result["level_5"]["reference"]


# =========================
# run_matching
# =========================


def _stub_run_matching(pm: PyramidMatcher, levels_return: list | None = None):
    """Mock the functions that have already been tested in order to isolate the logic of run_matching for testing.

    Parameters
    ----------
    pm : PyramidMatcher
        The instance whose methods will be patched.
    levels_return : list | None
        Value that the ``_get_levels_to_match`` stub will return. Defaults to [].

    Returns
    -------
    tuple[ExitStack, dict]
        ExitStack to use as a context manager, and a dict of named Mock objects.
    """
    levels = levels_return if levels_return is not None else []

    def _set_ref_side(x):
        pm.reference_pyramid = test_config.reference_pyramid

    def _set_can_side(x):
        pm.candidate_pyramid = test_config.candidate_pyramid

    def _get_attrs_side(lvls, suffix):
        return {lvl: {"reference": [], "candidate": []} for lvl in lvls}

    stack = ExitStack()

    stack.enter_context(
        patch.object(pm, "_set_reference_pyramid", side_effect=_set_ref_side)
    )
    stack.enter_context(
        patch.object(pm, "_set_candidate_pyramid", side_effect=_set_can_side)
    )
    stack.enter_context(
        patch.object(
            pm,
            "_match_level",
            return_value=(pl.DataFrame(), pl.DataFrame(), pl.DataFrame()),
        )
    )
    stack.enter_context(
        patch.object(
            pm, "_add_repeated_matches", side_effect=lambda data, *a, **kw: data
        )
    )
    stack.enter_context(
        patch.object(
            pm, "_reorder_match_columns", return_value=(pl.DataFrame(), pl.DataFrame())
        )
    )

    mocks = {
        "mock_get_levels_to_match": stack.enter_context(
            patch.object(pm, "_get_levels_to_match", return_value=levels)
        ),
        "mock_check_levels": stack.enter_context(patch.object(pm, "_check_levels")),
        "mock_get_attributes": stack.enter_context(
            patch.object(pm, "_get_attributes", side_effect=_get_attrs_side)
        ),
    }
    return stack, mocks


# --- Input validation ---
# 1. Passing None as either pyramid argument raises ValueError before any other logic runs. →
#    both reference and candidate are checked independently.
#    → test_run_matching_raises_when_reference_is_none,
#    test_run_matching_raises_when_candidate_is_none


def test_run_matching_raises_when_reference_is_none():
    """run_matching raises ValueError when reference_pyramid is None."""
    pm = PyramidMatcher()
    with pytest.raises(
        ValueError, match="Both reference_pyramid and candidate_pyramid"
    ):
        pm.run_matching(None, test_config.candidate_pyramid)


def test_run_matching_raises_when_candidate_is_none():
    """run_matching raises ValueError when candidate_pyramid is None."""
    pm = PyramidMatcher()
    with pytest.raises(
        ValueError, match="Both reference_pyramid and candidate_pyramid"
    ):
        pm.run_matching(test_config.reference_pyramid, None)


# --- Level auto-detection ---
# 2. When levels_to_match is None, run_matching delegates level detection to
#    _get_levels_to_match, forwarding the matching_col_suffix; _check_levels is skipped. →
#    all other methods are stubbed so only the delegation behaviour is verified.
#    → test_run_matching_calls_get_levels_to_match_when_none_provided,
#    test_run_matching_get_levels_to_match_uses_matching_suffix,
#    test_run_matching_does_not_call_check_levels_when_levels_auto_detected


def test_run_matching_calls_get_levels_to_match_when_none_provided():
    """run_matching calls _get_levels_to_match when levels_to_match is None."""
    pm = PyramidMatcher()
    stack, mocks = _stub_run_matching(pm)
    with stack:
        pm.run_matching(test_config.reference_pyramid, test_config.candidate_pyramid)
    mocks["mock_get_levels_to_match"].assert_called_once()


def test_run_matching_get_levels_to_match_uses_matching_suffix():
    """run_matching forwards matching_col_suffix to _get_levels_to_match."""
    pm = PyramidMatcher()
    stack, mocks = _stub_run_matching(pm)
    with stack:
        pm.run_matching(
            test_config.reference_pyramid,
            test_config.candidate_pyramid,
            matching_col_suffix="_id",
        )
    mocks["mock_get_levels_to_match"].assert_called_once_with("_id")


def test_run_matching_does_not_call_check_levels_when_levels_auto_detected():
    """_check_levels is not called when levels_to_match is None (auto-detection path)."""
    pm = PyramidMatcher()
    stack, mocks = _stub_run_matching(pm)
    with stack:
        pm.run_matching(test_config.reference_pyramid, test_config.candidate_pyramid)
    mocks["mock_check_levels"].assert_not_called()


# --- Level validation ---
# 3. When levels_to_match is explicit, _check_levels is called with the original unsorted list
#    and _get_levels_to_match is not called; the levels are sorted in-place before being used
#    further. → a side_effect captures the list by value at call time, before the sort
#    mutates it, so the assertion sees the original order.
#    → test_run_matching_calls_check_levels_when_levels_provided,
#    test_run_matching_does_not_call_get_levels_when_levels_provided,
#    test_run_matching_sorts_explicitly_provided_levels


def test_run_matching_calls_check_levels_when_levels_provided():
    """run_matching calls _check_levels with the unsorted list, before sorting."""
    pm = PyramidMatcher()
    stack, mocks = _stub_run_matching(pm)
    # Capture the list by value at call time; assert_called_once_with would inspect
    # the same list object *after* in-place sort and give a false positive.
    captured = []
    mocks["mock_check_levels"].side_effect = lambda levels, _: captured.append(
        list(levels)
    )
    with stack:
        pm.run_matching(
            test_config.reference_pyramid,
            test_config.candidate_pyramid,
            levels_to_match=["level_2", "level_1"],
        )
    assert captured == [["level_2", "level_1"]]


def test_run_matching_does_not_call_get_levels_when_levels_provided():
    """_get_levels_to_match is not called when levels_to_match is explicitly given."""
    pm = PyramidMatcher()
    stack, mocks = _stub_run_matching(pm)
    with stack:
        pm.run_matching(
            test_config.reference_pyramid,
            test_config.candidate_pyramid,
            levels_to_match=["level_1"],
        )
    mocks["mock_get_levels_to_match"].assert_not_called()


def test_run_matching_sorts_explicitly_provided_levels():
    """run_matching sorts levels_to_match before passing them to _get_attributes."""
    pm = PyramidMatcher()
    stack, mocks = _stub_run_matching(pm)
    captured = []
    original_get_attrs = mocks["mock_get_attributes"].side_effect
    mocks["mock_get_attributes"].side_effect = lambda levels, suffix: (
        captured.append(list(levels)) or original_get_attrs(levels, suffix)
    )
    with stack:
        pm.run_matching(
            test_config.reference_pyramid,
            test_config.candidate_pyramid,
            levels_to_match=["level_3", "level_1", "level_2"],
        )
    assert captured == [["level_1", "level_2", "level_3"]]


# --- Matching loop ---
# What we test:
# 5. For each level in levels_to_match, _match_level is called once and receives a
#    levels_already_matched list that grows by one after each iteration; after the full loop,
#    _reorder_match_columns is called exactly once.
#    → test_run_matching_calls_match_level_once_per_level_and_reorder_match_columns_once
#
# 6. For each level in levels_to_match, _add_repeated_matches is called once with an
#    upper_levels list that starts empty and gains one level after each iteration.
#    → test_run_matching_calls_add_repeated_matches_once_per_level_with_growing_upper_levels
#
# --- Output assembly ---
# 7. When _match_level returns non-empty unmatched data for a level, an "unmatched_level"
#    column containing the level name is added to that data before it is accumulated.
#    → test_run_matching_adds_unmatched_level_column_to_unmatched_data
#
# 8. When all entries are matched (no unmatched data returned), reference_not_matched and
#    candidate_not_matched are empty DataFrames; the return tuple is ordered as
#    (matched, simplified, reference_not_matched, candidate_not_matched).
#    → test_run_matching_returns_empty_unmatched_dfs_and_correct_tuple_order_when_all_matched


def test_run_matching_calls_match_level_once_per_level_and_reorder_match_columns_once():
    """_match_level is called once per level with a growing levels_already_matched; _reorder_match_columns is called exactly once after the loop."""
    pm = PyramidMatcher()
    levels = ["level_1", "level_2"]
    stack, mocks = _stub_run_matching(pm, levels_return=levels)

    captured_target_levels = []
    captured_levels_already_matched = []

    def ml_side_effect(
        already_matched,
        target_level,
        levels_already_matched,
        attributes_level,
        matching_col_suffix,
    ):
        captured_target_levels.append(target_level)
        captured_levels_already_matched.append(list(levels_already_matched))
        return pl.DataFrame(), pl.DataFrame(), pl.DataFrame()

    with stack:
        with (
            patch.object(pm, "_match_level", side_effect=ml_side_effect),
            patch.object(
                pm,
                "_reorder_match_columns",
                return_value=(pl.DataFrame(), pl.DataFrame()),
            ) as mock_rmc,
        ):
            pm.run_matching(
                test_config.reference_pyramid, test_config.candidate_pyramid
            )

    assert captured_target_levels == levels
    assert captured_levels_already_matched == [[], ["level_1"]]
    mock_rmc.assert_called_once()


def test_run_matching_calls_add_repeated_matches_once_per_level_with_growing_upper_levels():
    """_add_repeated_matches is called once per level, with an upper_levels list that starts empty and grows by one each iteration."""
    pm = PyramidMatcher()
    levels = ["level_1", "level_2"]
    stack, mocks = _stub_run_matching(pm, levels_return=levels)

    captured_levels = []
    captured_upper_levels = []

    def arm_side_effect(data, level, upper_levels, suffix):
        captured_levels.append(level)
        captured_upper_levels.append(list(upper_levels))
        return data

    with stack:
        with patch.object(pm, "_add_repeated_matches", side_effect=arm_side_effect):
            pm.run_matching(
                test_config.reference_pyramid, test_config.candidate_pyramid
            )

    assert captured_levels == levels
    assert captured_upper_levels == [[], ["level_1"]]


def test_run_matching_adds_unmatched_level_column_to_unmatched_data():
    """When _match_level returns non-empty unmatched DataFrames, an 'unmatched_level' column with the level name is added to each."""
    pm = PyramidMatcher()
    stack, mocks = _stub_run_matching(pm, levels_return=["level_1"])

    unmatched_ref = pl.DataFrame({"level_1_name": ["KWILU"]})
    unmatched_can = pl.DataFrame({"level_1_name": ["KWILU ALT"]})

    with stack:
        with patch.object(
            pm,
            "_match_level",
            return_value=(pl.DataFrame(), unmatched_can, unmatched_ref),
        ):
            _, _, reference_not_matched, candidate_not_matched = pm.run_matching(
                test_config.reference_pyramid, test_config.candidate_pyramid
            )

    assert "unmatched_level" in reference_not_matched.columns
    assert reference_not_matched["unmatched_level"][0] == "level_1"
    assert "unmatched_level" in candidate_not_matched.columns
    assert candidate_not_matched["unmatched_level"][0] == "level_1"


def test_run_matching_returns_empty_unmatched_dfs_and_correct_tuple_order_when_all_matched():
    """When no unmatched data is returned, reference_not_matched and candidate_not_matched are empty; the return tuple is (matched, simplified, reference_not_matched, candidate_not_matched)."""
    pm = PyramidMatcher()
    stack, mocks = _stub_run_matching(pm, levels_return=["level_1"])

    expected_matched = pl.DataFrame(
        {"candidate_level_1_name": ["TSHUAPA"], "reference_level_1_name": ["TSHUAPA"]}
    )
    expected_simplified = pl.DataFrame({"candidate_level_1_name": ["TSHUAPA"]})

    with stack:
        with patch.object(
            pm,
            "_reorder_match_columns",
            return_value=(expected_matched, expected_simplified),
        ):
            result = pm.run_matching(
                test_config.reference_pyramid, test_config.candidate_pyramid
            )

    assert_frame_equal(result[0], expected_matched)
    assert_frame_equal(result[1], expected_simplified)
    assert result[2].is_empty()
    assert result[3].is_empty()


# ================================
# _select_group
# ================================
# What we test:
# 1. With no levels matched yet (empty list), the full reference and candidate pyramids are
#    returned (as clones). → the result equals the stored pyramids row-for-row.
#    → test_select_group_empty_levels_returns_full_pyramids
#
# 2. The reference pyramid is filtered using the reference-prefixed value from the row. → a
#    row with different candidate and reference values shows that each pyramid uses its own
#    prefix.
#    → test_select_group_filters_reference_by_reference_value
#
# 3. The candidate pyramid is filtered using the candidate-prefixed value from the row. →
#    same row as above; the candidate result corresponds to the candidate-prefix value.
#    → test_select_group_filters_candidate_by_candidate_value
#
# 4. Two levels in the row each narrow the pyramids further, producing a smaller sub-group. →
#    → test_select_group_two_levels_narrows_both_pyramids


def test_select_group_empty_levels_returns_full_pyramids(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """With no levels matched yet, the full cloned pyramids are returned."""
    ref, candidate = pyramid_matcher_loaded._select_group([], {}, "_name")
    assert_frame_equal(ref, test_config.reference_pyramid, check_row_order=False)
    assert_frame_equal(candidate, test_config.candidate_pyramid, check_row_order=False)


def test_select_group_filters_reference_by_reference_value(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """The reference pyramid is filtered using the reference-prefixed value from the row."""
    row = {
        "candidate_level_1_name": "HAUT LOMAMI",
        "reference_level_1_name": "TSHUAPA",
    }
    ref, _ = pyramid_matcher_loaded._select_group(["level_1"], row, "_name")
    assert_frame_equal(
        ref, test_config.select_group_ref_level1_tshuapa, check_row_order=False
    )


def test_select_group_filters_candidate_by_candidate_value(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """The candidate pyramid is filtered using the candidate-prefixed value from the row."""
    row = {
        "candidate_level_1_name": "HAUT LOMAMI",
        "reference_level_1_name": "TSHUAPA",
    }
    _, candidate = pyramid_matcher_loaded._select_group(["level_1"], row, "_name")
    assert_frame_equal(
        candidate,
        test_config.select_group_can_level1_haut_lomami,
        check_row_order=False,
    )


def test_select_group_two_levels_narrows_both_pyramids(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """Filtering by two levels correctly narrows both pyramids to the matching subset."""
    row = {
        "candidate_level_1_name": "TSHUAPA",
        "reference_level_1_name": "TSHUAPA",
        "candidate_level_2_name": "BOENDE",
        "reference_level_2_name": "BOENDE",
    }
    ref, candidate = pyramid_matcher_loaded._select_group(
        ["level_1", "level_2"], row, "_name"
    )
    assert_frame_equal(
        ref,
        test_config.select_group_ref_level1_tshuapa_level2_boende,
        check_row_order=False,
    )
    assert_frame_equal(
        candidate,
        test_config.select_group_can_level1_tshuapa_level2_boende,
        check_row_order=False,
    )


# ================================
# _define_schema_match
# ================================
# What we test:
# 1. With empty attribute lists, the schema has exactly the three fixed columns:
#    candidate_name, reference_name, and score. →
#    → test_define_schema_match_no_attributes_returns_three_columns
#
# 2. Reference attributes are inserted after reference_name and before the score, with the
#    reference_ prefix. →
#    → test_define_schema_match_reference_attrs_are_prefixed
#
# 3. Candidate attributes are appended after the score, with the candidate_ prefix. →
#    → test_define_schema_match_candidate_attrs_are_prefixed
#
# 4. The full ordering is: candidate_name, reference_name, reference_attrs, score,
#    candidate_attrs. →
#    → test_define_schema_match_full_column_order


def test_define_schema_match_no_attributes_returns_three_columns(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """With empty attribute lists the schema has exactly the three fixed columns."""
    schema = pyramid_matcher_loaded._define_schema_match(
        {"reference": [], "candidate": []}, "level_1", "_name"
    )
    assert schema == [
        "candidate_level_1_name",
        "reference_level_1_name",
        "score_level_1",
    ]


def test_define_schema_match_reference_attrs_are_prefixed(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """Reference attributes are included with the reference prefix."""
    schema = pyramid_matcher_loaded._define_schema_match(
        {"reference": ["level_1_id", "level_1_region"], "candidate": []},
        "level_1",
        "_name",
    )
    assert schema == [
        "candidate_level_1_name",
        "reference_level_1_name",
        "reference_level_1_id",
        "reference_level_1_region",
        "score_level_1",
    ]


def test_define_schema_match_candidate_attrs_are_prefixed(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """Candidate attributes are included with the candidate prefix."""
    schema = pyramid_matcher_loaded._define_schema_match(
        {"reference": [], "candidate": ["level_1_id", "level_1_source"]},
        "level_1",
        "_name",
    )
    assert schema == [
        "candidate_level_1_name",
        "reference_level_1_name",
        "score_level_1",
        "candidate_level_1_id",
        "candidate_level_1_source",
    ]


def test_define_schema_match_full_column_order(pyramid_matcher_loaded: PyramidMatcher):
    """Full ordering: candidate_name, reference_name, reference_attrs, score, candidate_attrs."""
    schema = pyramid_matcher_loaded._define_schema_match(
        {
            "reference": ["level_1_id", "level_1_region"],
            "candidate": ["level_1_id", "level_1_source"],
        },
        "level_1",
        "_name",
    )
    assert schema == [
        "candidate_level_1_name",
        "reference_level_1_name",
        "reference_level_1_id",
        "reference_level_1_region",
        "score_level_1",
        "candidate_level_1_id",
        "candidate_level_1_source",
    ]


# ================================
# _add_already_matched_levels
# ================================
# What we test:
# 1. An empty row dict leaves the DataFrame unchanged. →
#    → test_add_already_matched_levels_empty_row_returns_df_unchanged
#
# 2. Each key-value pair in the row is broadcast as a constant column to all rows. → one
#    matched level adds two new columns (candidate and reference name).
#    → test_add_already_matched_levels_adds_one_level_columns
#
# 3. Multiple key-value pairs (two previously matched levels) are all added as constant
#    columns. →
#    → test_add_already_matched_levels_adds_multiple_levels_columns


def test_add_already_matched_levels_empty_row_returns_df_unchanged(
    pyramid_matcher: PyramidMatcher,
):
    """An empty row dict leaves the DataFrame unchanged."""
    result = pyramid_matcher._add_already_matched_levels(
        {}, test_config.add_already_matched_input_df
    )
    assert_frame_equal(result, test_config.add_already_matched_input_df)


def test_add_already_matched_levels_adds_one_level_columns(
    pyramid_matcher: PyramidMatcher,
):
    """Each key-value in the row is added as a literal column broadcast to all rows."""
    result = pyramid_matcher._add_already_matched_levels(
        test_config.add_already_matched_row_one_level,
        test_config.add_already_matched_input_df,
    )
    assert_frame_equal(result, test_config.add_already_matched_expected_one_level)


def test_add_already_matched_levels_adds_multiple_levels_columns(
    pyramid_matcher: PyramidMatcher,
):
    """All key-value pairs from a multi-level row are added as literal columns."""
    result = pyramid_matcher._add_already_matched_levels(
        test_config.add_already_matched_row_two_levels,
        test_config.add_already_matched_input_df_level3,
    )
    assert_frame_equal(result, test_config.add_already_matched_expected_two_levels)


# ================================
# _match_level_group
# ================================
# What we test:
# 1. When every candidate finds a match, df_matches contains one row per matched candidate
#    with all the expected columns. → a match_map returns a result for every candidate name.
#    → test_match_level_group_df_matches_correct_when_all_matched
#
# 2. Rows whose names appear in the matched result are excluded from the unmatched DataFrames.
#    → only TSHUAPA is matched; HAUT LOMAMI rows appear in both unmatched outputs.
#    Unmatched data is filtered from the passed-in groups (reference_group / candidate_group).
#    → test_match_level_group_unmatched_excludes_matched_names
#
# 3. When no candidates match, df_matches is empty but retains the expected column schema. →
#    → test_match_level_group_empty_df_matches_has_correct_schema
#
# 4. When no candidates match, both unmatched DataFrames equal the passed-in groups. →
#    unmatched is filtered from the passed-in groups, so only the group rows are returned.
#    → test_match_level_group_unmatched_equals_input_groups_when_no_match


def test_match_level_group_df_matches_correct_when_all_matched(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """df_matches contains all matched rows when every candidate finds a match."""
    match_map = {
        "TSHUAPA": MagicMock(
            query="TSHUAPA", matched="TSHUAPA", attributes=["ref_id_1"], score=100.0
        ),
        "HAUT LOMAMI": MagicMock(
            query="HAUT LOMAMI",
            matched="HAUT LOMAMI",
            attributes=["ref_id_2"],
            score=100.0,
        ),
    }
    with patch.object(
        pyramid_matcher_loaded.matcher,
        "get_similarity",
        side_effect=lambda query, _: match_map.get(query),
    ):
        df_matches, _, _ = pyramid_matcher_loaded._match_level_group(
            reference_group=test_config.reference_pyramid_one_level,
            candidate_group=test_config.candidate_pyramid_one_level,
            level="level_1",
            attributes_level=test_config.match_level_group_attributes_level1,
            schema_match=test_config.match_level_group_schema_level1,
            matching_col_suffix="_name",
        )
    assert_frame_equal(
        df_matches,
        test_config.match_level_group_df_matches_all_level1,
        check_row_order=False,
    )


def test_match_level_group_unmatched_excludes_matched_names(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """Unmatched DataFrames exclude rows whose names appear in the matched result."""
    with patch.object(
        pyramid_matcher_loaded.matcher,
        "get_similarity",
        side_effect=lambda query, _: (
            test_config.match_results_level1.get("TSHUAPA")
            if query == "TSHUAPA"
            else None
        ),
    ):
        _, df_unmatched_can, df_unmatched_ref = (
            pyramid_matcher_loaded._match_level_group(
                reference_group=test_config.reference_pyramid_one_level,
                candidate_group=test_config.candidate_pyramid_one_level,
                level="level_1",
                attributes_level=test_config.match_level_group_attributes_level1,
                schema_match=test_config.match_level_group_schema_level1,
                matching_col_suffix="_name",
            )
        )
    print(df_unmatched_ref)
    print(test_config.match_level_group_unmatched_ref_when_tshuapa_matched)
    assert_frame_equal(
        df_unmatched_ref,
        test_config.match_level_group_unmatched_ref_when_tshuapa_matched,
        check_row_order=False,
    )
    print(df_unmatched_can)
    print(test_config.match_level_group_unmatched_can_when_tshuapa_matched)
    assert_frame_equal(
        df_unmatched_can,
        test_config.match_level_group_unmatched_can_when_tshuapa_matched,
        check_row_order=False,
    )


def test_match_level_group_empty_df_matches_has_correct_schema(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """When no candidates match, df_matches is empty but has the expected schema."""
    with patch.object(
        pyramid_matcher_loaded.matcher, "get_similarity", return_value=None
    ):
        df_matches, _, _ = pyramid_matcher_loaded._match_level_group(
            reference_group=test_config.reference_pyramid_one_level,
            candidate_group=test_config.candidate_pyramid_one_level,
            level="level_1",
            attributes_level=test_config.match_level_group_attributes_level1,
            schema_match=test_config.match_level_group_schema_level1,
            matching_col_suffix="_name",
        )
    assert df_matches.is_empty()
    assert df_matches.columns == test_config.match_level_group_schema_level1


def test_match_level_group_unmatched_equals_input_groups_when_no_match(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """When no candidates match, both unmatched DataFrames equal the passed-in groups."""
    with patch.object(
        pyramid_matcher_loaded.matcher, "get_similarity", return_value=None
    ):
        _, df_unmatched_can, df_unmatched_ref = (
            pyramid_matcher_loaded._match_level_group(
                reference_group=test_config.reference_pyramid_one_level,
                candidate_group=test_config.candidate_pyramid_one_level,
                level="level_1",
                attributes_level=test_config.match_level_group_attributes_level1,
                schema_match=test_config.match_level_group_schema_level1,
                matching_col_suffix="_name",
            )
        )
    assert_frame_equal(
        df_unmatched_ref, test_config.reference_pyramid_one_level, check_row_order=False
    )
    assert_frame_equal(
        df_unmatched_can, test_config.candidate_pyramid_one_level, check_row_order=False
    )


# ================================
# _match_level
# ================================
# What we test:
# 1. When no level has been matched yet, we compare the full reference pyramid against the full
#    candidate pyramid. → already_matched is empty, so _match_level_group is called exactly once
#    with the full pyramids and its output is returned directly.
#    → test_match_level_delegates_to_match_level_group_when_already_matched_is_empty
#
# 2. When previous levels have already been matched, for each matched pair we select the
#    relevant sub-groups and run the matching only within those sub-groups. → already_matched
#    has N rows, so _select_group and _match_level_group are each called N times.
#    → test_match_level_calls_select_group_and_match_level_group_once_per_row_in_already_matched
#
# 3. When a sub-group produces matches, the information from the previously matched levels is
#    added to those matches before collecting them. → _add_already_matched_levels is called
#    once for each group that returns at least one match.
#    → test_match_level_adds_already_matched_levels_when_match_is_found
#
# 4. When a sub-group finds no match, we skip it without any further processing. →
#    _add_already_matched_levels is never called.
#    → test_match_level_does_not_call_add_already_matched_levels_when_no_match
#
# 5. When no sub-group finds any match at all, the output match DataFrame is empty. →
#    df_match is empty.
#    → test_match_level_returns_empty_df_match_when_no_group_produces_matches
#
# 6. Matches coming from different sub-groups are all combined into a single output DataFrame.
#    → df_match has as many rows as sub-groups (one match per group).
#    → test_match_level_concatenates_matches_from_all_row_groups


def test_match_level_delegates_to_match_level_group_when_already_matched_is_empty(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """When already_matched is empty, _match_level_group is called once with the full pyramids and its output is returned directly."""
    expected = (
        test_config.match_level_group_df_matches_all_level1,
        test_config.match_level_group_unmatched_ref_when_tshuapa_matched,
        test_config.match_level_group_unmatched_can_when_tshuapa_matched,
    )
    with patch.object(
        pyramid_matcher_loaded, "_match_level_group", return_value=expected
    ) as mock_mlg:
        result = pyramid_matcher_loaded._match_level(
            already_matched=pl.DataFrame(),
            target_level="level_1",
            levels_already_matched=[],
            attributes_level=test_config.match_level_group_attributes_level1,
            matching_col_suffix="_name",
        )
    mock_mlg.assert_called_once()
    call_kwargs = mock_mlg.call_args.kwargs
    assert_frame_equal(
        call_kwargs["reference_group"],
        test_config.reference_pyramid,
        check_row_order=False,
    )
    assert_frame_equal(
        call_kwargs["candidate_group"],
        test_config.candidate_pyramid,
        check_row_order=False,
    )
    assert_frame_equal(result[0], expected[0])
    assert_frame_equal(result[1], expected[1])
    assert_frame_equal(result[2], expected[2])


def test_match_level_calls_select_group_and_match_level_group_once_per_row_in_already_matched(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """When already_matched has N rows, both _select_group and _match_level_group are called N times."""
    with (
        patch.object(
            pyramid_matcher_loaded,
            "_select_group",
            return_value=(test_config.reference_pyramid, test_config.candidate_pyramid),
        ) as mock_sg,
        patch.object(
            pyramid_matcher_loaded,
            "_match_level_group",
            return_value=(pl.DataFrame(), pl.DataFrame(), pl.DataFrame()),
        ) as mock_mlg,
    ):
        pyramid_matcher_loaded._match_level(
            already_matched=test_config.match_level_group_df_matches_all_level1,
            target_level="level_2",
            levels_already_matched=["level_1"],
            attributes_level=test_config.match_level_group_attributes_level1,
            matching_col_suffix="_name",
        )
    n_rows = len(test_config.match_level_group_df_matches_all_level1)
    assert mock_sg.call_count == n_rows
    assert mock_mlg.call_count == n_rows


def test_match_level_adds_already_matched_levels_when_match_is_found(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """_add_already_matched_levels is called for each group that produces at least one match."""
    with (
        patch.object(
            pyramid_matcher_loaded,
            "_select_group",
            return_value=(test_config.reference_pyramid, test_config.candidate_pyramid),
        ),
        patch.object(
            pyramid_matcher_loaded,
            "_match_level_group",
            return_value=(
                test_config.match_level_single_match_level2,
                pl.DataFrame(),
                pl.DataFrame(),
            ),
        ),
        patch.object(
            pyramid_matcher_loaded,
            "_add_already_matched_levels",
            side_effect=lambda row, df: df,
        ) as mock_add,
    ):
        pyramid_matcher_loaded._match_level(
            already_matched=test_config.match_level_group_df_matches_all_level1,
            target_level="level_2",
            levels_already_matched=["level_1"],
            attributes_level=test_config.match_level_group_attributes_level1,
            matching_col_suffix="_name",
        )
    assert mock_add.call_count == len(
        test_config.match_level_group_df_matches_all_level1
    )


def test_match_level_does_not_call_add_already_matched_levels_when_no_match(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """_add_already_matched_levels is not called when _match_level_group returns no matches."""
    with (
        patch.object(
            pyramid_matcher_loaded,
            "_select_group",
            return_value=(test_config.reference_pyramid, test_config.candidate_pyramid),
        ),
        patch.object(
            pyramid_matcher_loaded,
            "_match_level_group",
            return_value=(pl.DataFrame(), pl.DataFrame(), pl.DataFrame()),
        ),
        patch.object(pyramid_matcher_loaded, "_add_already_matched_levels") as mock_add,
    ):
        pyramid_matcher_loaded._match_level(
            already_matched=test_config.match_level_group_df_matches_all_level1,
            target_level="level_2",
            levels_already_matched=["level_1"],
            attributes_level=test_config.match_level_group_attributes_level1,
            matching_col_suffix="_name",
        )
    mock_add.assert_not_called()


def test_match_level_returns_empty_df_match_when_no_group_produces_matches(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """When no row group produces any matches, the returned match DataFrame is empty."""
    with (
        patch.object(
            pyramid_matcher_loaded,
            "_select_group",
            return_value=(test_config.reference_pyramid, test_config.candidate_pyramid),
        ),
        patch.object(
            pyramid_matcher_loaded,
            "_match_level_group",
            return_value=(pl.DataFrame(), pl.DataFrame(), pl.DataFrame()),
        ),
    ):
        df_match, _, _ = pyramid_matcher_loaded._match_level(
            already_matched=test_config.match_level_group_df_matches_all_level1,
            target_level="level_2",
            levels_already_matched=["level_1"],
            attributes_level=test_config.match_level_group_attributes_level1,
            matching_col_suffix="_name",
        )
    assert df_match.is_empty()


def test_match_level_concatenates_matches_from_all_row_groups(
    pyramid_matcher_loaded: PyramidMatcher,
):
    """Matches returned from each row group are concatenated into a single output DataFrame."""
    with (
        patch.object(
            pyramid_matcher_loaded,
            "_select_group",
            return_value=(test_config.reference_pyramid, test_config.candidate_pyramid),
        ),
        patch.object(
            pyramid_matcher_loaded,
            "_match_level_group",
            return_value=(
                test_config.match_level_single_match_level2,
                pl.DataFrame(),
                pl.DataFrame(),
            ),
        ),
        patch.object(
            pyramid_matcher_loaded,
            "_add_already_matched_levels",
            side_effect=lambda row, df: df,
        ),
    ):
        df_match, _, _ = pyramid_matcher_loaded._match_level(
            already_matched=test_config.match_level_group_df_matches_all_level1,
            target_level="level_2",
            levels_already_matched=["level_1"],
            attributes_level=test_config.match_level_group_attributes_level1,
            matching_col_suffix="_name",
        )
    assert len(df_match) == len(test_config.match_level_group_df_matches_all_level1)


# ================================
# _add_repeated_matches
# ================================
# What we test:
# 1. When every reference maps to exactly one candidate, no match is repeated: all rows should
#    carry False in the new `repeated_matches_{level}` column, and no intermediate `count`
#    column should remain in the output.
#    → test_add_repeated_matches_adds_false_column_when_no_reference_is_repeated
#
# 2. When a reference is matched to more than one candidate, those rows should be flagged True,
#    while rows whose reference appears only once remain False.
#    → test_add_repeated_matches_marks_true_only_for_references_with_multiple_candidates
#
# 3. When upper levels are provided, a reference that appears under two different upper-level
#    parents is treated as two separate groups and is therefore not flagged as repeated. Only
#    rows that share both the upper-level parent and the current-level reference get flagged.
#    → test_add_repeated_matches_considers_upper_levels_when_grouping


def test_add_repeated_matches_adds_false_column_when_no_reference_is_repeated(
    pyramid_matcher: PyramidMatcher,
):
    """When no reference maps to more than one candidate, all rows are False and no count column is left."""
    result = pyramid_matcher._add_repeated_matches(
        test_config.add_repeated_matches_no_repeats, "level_1", [], "_name"
    )
    assert "count" not in result.columns
    assert_frame_equal(
        result,
        test_config.add_repeated_matches_no_repeats_expected,
        check_row_order=False,
    )


def test_add_repeated_matches_marks_true_only_for_references_with_multiple_candidates(
    pyramid_matcher: PyramidMatcher,
):
    """Rows whose reference maps to more than one candidate are True; rows with a single candidate are False."""
    result = pyramid_matcher._add_repeated_matches(
        test_config.add_repeated_matches_with_repeats, "level_1", [], "_name"
    )
    assert_frame_equal(
        result,
        test_config.add_repeated_matches_with_repeats_expected,
        check_row_order=False,
    )


def test_add_repeated_matches_considers_upper_levels_when_grouping(
    pyramid_matcher: PyramidMatcher,
):
    """The same level_2 reference under different level_1 parents is not flagged; only rows sharing both parent and reference are."""
    result = pyramid_matcher._add_repeated_matches(
        test_config.add_repeated_matches_upper_levels_input,
        "level_2",
        ["level_1"],
        "_name",
    )
    assert_frame_equal(
        result,
        test_config.add_repeated_matches_upper_levels_expected,
        check_row_order=False,
    )


# ================================
# _reorder_match_columns
# ================================
# What we test:
# 1. For a single level with no attributes, the full output places candidate match then reference
#    match then score then repeated_matches; the simple output drops score and repeated_matches
#    entirely.
#    → test_reorder_match_columns_correct_order_and_simple_output_for_one_level_no_attrs
#
# 2. When a level has attributes, the full output places candidate match, candidate attrs,
#    reference match, reference attrs, score, repeated_matches; the simple output keeps
#    both match columns and their attributes but drops score and repeated_matches.
#    → test_reorder_match_columns_attrs_placed_correctly_in_both_outputs
#
# 3. Columns that are not part of any level ordering (extra columns) appear at the front of
#    the full output but are absent from the simple output.
#    → test_reorder_match_columns_extra_cols_prepended_to_full_and_absent_from_simple
#
# 4. With multiple levels, the ordering is applied level by level in sequence for both outputs.
#    → test_reorder_match_columns_two_levels_ordered_sequentially


def test_reorder_match_columns_correct_order_and_simple_output_for_one_level_no_attrs(
    pyramid_matcher: PyramidMatcher,
):
    """For one level with no attributes, both full and simple outputs have the correct column order."""
    full, simple = pyramid_matcher._reorder_match_columns(
        test_config.reorder_match_columns_one_level_no_attrs_input,
        ["level_1"],
        test_config.reorder_match_columns_one_level_no_attrs_attributes,
        "_name",
    )
    assert_frame_equal(
        full,
        test_config.reorder_match_columns_one_level_no_attrs_full,
        check_row_order=False,
    )
    assert_frame_equal(
        simple,
        test_config.reorder_match_columns_one_level_no_attrs_simple,
        check_row_order=False,
    )


def test_reorder_match_columns_attrs_placed_correctly_in_both_outputs(
    pyramid_matcher: PyramidMatcher,
):
    """Candidate attrs follow the candidate match column, reference attrs follow the reference match column; simple output keeps attrs but drops score and repeated_matches."""
    full, simple = pyramid_matcher._reorder_match_columns(
        test_config.reorder_match_columns_one_level_with_attrs_input,
        ["level_1"],
        test_config.reorder_match_columns_one_level_with_attrs_attributes,
        "_name",
    )
    assert_frame_equal(
        full,
        test_config.reorder_match_columns_one_level_with_attrs_full,
        check_row_order=False,
    )
    assert_frame_equal(
        simple,
        test_config.reorder_match_columns_one_level_with_attrs_simple,
        check_row_order=False,
    )


def test_reorder_match_columns_extra_cols_prepended_to_full_and_absent_from_simple(
    pyramid_matcher: PyramidMatcher,
):
    """Columns not belonging to any level ordering appear at the front of the full output and are absent from the simple output."""
    full, simple = pyramid_matcher._reorder_match_columns(
        test_config.reorder_match_columns_with_extra_col_input,
        ["level_1"],
        test_config.reorder_match_columns_one_level_no_attrs_attributes,
        "_name",
    )
    assert_frame_equal(
        full,
        test_config.reorder_match_columns_with_extra_col_full,
        check_row_order=False,
    )
    assert_frame_equal(
        simple,
        test_config.reorder_match_columns_with_extra_col_simple,
        check_row_order=False,
    )


def test_reorder_match_columns_two_levels_ordered_sequentially(
    pyramid_matcher: PyramidMatcher,
):
    """With two levels, the full and simple outputs apply the per-level ordering one level at a time."""
    full, simple = pyramid_matcher._reorder_match_columns(
        test_config.reorder_match_columns_two_levels_input,
        ["level_1", "level_2"],
        test_config.reorder_match_columns_two_levels_attributes,
        "_name",
    )
    assert_frame_equal(
        full,
        test_config.reorder_match_columns_two_levels_full,
        check_row_order=False,
    )
    assert_frame_equal(
        simple,
        test_config.reorder_match_columns_two_levels_simple,
        check_row_order=False,
    )
