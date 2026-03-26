import logging
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import polars as pl
import pytest

from d2d_development.exceptions import ExtractorError
from d2d_development.utils import log_message, save_to_parquet


class CustomError(Exception):
    """Custom exception for testing invalid logging levels."""

    pass


def test_log_message_info():
    """Test that log_message logs info messages correctly."""
    logger = Mock(spec=logging.Logger)
    with patch("d2d_development.utils.current_run") as mock_run:
        log_message(logger, "msg", level="info")
        logger.info.assert_called_once_with("msg")
        mock_run.log_info.assert_called_once_with("msg")


def test_log_message_warning():
    """Test that log_message logs warning messages correctly."""
    logger = Mock(spec=logging.Logger)
    with patch("d2d_development.utils.current_run") as mock_run:
        log_message(logger, "warn", level="warning")
        logger.warning.assert_called_once_with("warn")
        mock_run.log_warning.assert_called_once_with("warn")


def test_log_message_error():
    """Test that log_message logs error messages correctly, including error details."""
    logger = Mock(spec=logging.Logger)
    with patch("d2d_development.utils.current_run") as mock_run:
        log_message(logger, "err", error_details="details", level="error")
        logger.error.assert_called_once_with("err Details: details")
        mock_run.log_error.assert_called_once_with("err")


def test_log_message_no_current_run():
    """Test that log_message works even if current_run is not available."""
    logger = Mock(spec=logging.Logger)
    with patch("d2d_development.utils.current_run", None):
        log_message(logger, "msg", level="info")
        logger.info.assert_called_once_with("msg")


def test_log_message_log_current_run_false():
    """Test that log_message does not log to current_run when log_current_run is False."""
    logger = Mock(spec=logging.Logger)
    with patch("d2d_development.utils.current_run") as mock_run:
        log_message(logger, "msg", level="info", log_current_run=False)
        logger.info.assert_called_once_with("msg")
        mock_run.log_info.assert_not_called()


def test_log_message_invalid_level():
    """Test that log_message raises the specified exception for invalid logging levels."""
    logger = Mock(spec=logging.Logger)
    with pytest.raises(CustomError):
        log_message(logger, "bad", level="bad", exception_class=CustomError)


def test_save_polars_dataframe(tmp_path: Path):
    """Test saving a Polars DataFrame to Parquet."""
    df = pl.DataFrame({"a": [1, 2], "b": [3, 4]})
    file = tmp_path / "test.parquet"
    save_to_parquet(df, file)
    assert file.exists()


def test_save_pandas_dataframe(tmp_path: Path):
    """Test saving a Pandas DataFrame to Parquet."""
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    file = tmp_path / "test_pd.parquet"
    save_to_parquet(df, file)
    assert file.exists()


def test_invalid_type_raises(tmp_path: Path):
    """Test that passing an invalid type raises an ExtractorError."""
    file = tmp_path / "fail.parquet"
    with pytest.raises(ExtractorError):
        save_to_parquet([1, 2, 3], file)


def test_overwrite_file(tmp_path: Path):
    """Test that saving to an existing file overwrites it."""
    df1 = pd.DataFrame({"a": [1]})
    df2 = pd.DataFrame({"a": [2]})
    file = tmp_path / "overwrite.parquet"
    save_to_parquet(df1, file)
    save_to_parquet(df2, file)
    result = pd.read_parquet(file)
    assert result["a"].iloc[0] == 2


def test_write_exception_cleanup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Test that if writing to Parquet raises an exception, no temp files are left behind."""
    df = pd.DataFrame({"a": [1]})
    file = tmp_path / "fail.parquet"
    # Patch to_parquet to raise
    monkeypatch.setattr(
        df, "to_parquet", lambda *a, **k: (_ for _ in ()).throw(Exception("fail"))
    )
    with pytest.raises(ExtractorError):
        save_to_parquet(df, file)
    # Check no temp files left
    assert not any(tmp_path.glob("*.parquet*"))
