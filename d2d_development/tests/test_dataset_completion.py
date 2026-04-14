from unittest.mock import MagicMock

from d2d_development.dataset_completion import DatasetCompletionSync


def test_log_and_append_error_updates_summary_and_logs():
    """Test that _log_and_append_error correctly updates the import summary and logs the error."""
    # Arrange
    dummy_logger = MagicMock()
    sync = DatasetCompletionSync(
        source_dhis2=None,  # You can use None or a mock if not used in this test
        target_dhis2=None,
        logger=dummy_logger,
    )
    sync.log_function = MagicMock()
    sync._reset_summary()
    sync._log_and_append_error(error_type="fetch_errors", ds="DS1", pe="202601", ou="OU1", error_msg="Test error")

    # Assert
    assert sync.import_summary["errors"]["fetch_errors"] == [
        {"ds": "DS1", "pe": "202601", "ou": "OU1", "error": "Test error"}
    ]
    sync.log_function.assert_called_once()
    log_args = sync.log_function.call_args[1]
    assert "[fetch_errors]" in log_args["message"]
    assert "DS1" in log_args["message"]
    assert "Test error" in log_args["message"]


def test_log_and_append_error_different_types():
    """Test that _log_and_append_error can handle different error types."""
    sync = DatasetCompletionSync(None, None)
    sync.log_function = MagicMock()
    sync._reset_summary()
    sync._log_and_append_error("push_errors", "DS2", "202602", "OU2", "Push error")
    assert sync.import_summary["errors"]["push_errors"] == [
        {"ds": "DS2", "pe": "202602", "ou": "OU2", "error": "Push error"}
    ]


def test_log_and_append_error_log_level_and_flag():
    """Test that _log_and_append_error correctly handles log level and log_current_run flag."""
    sync = DatasetCompletionSync(None, None)
    sync.log_function = MagicMock()
    sync._reset_summary()
    sync._log_and_append_error(
        "fetch_errors", "DS4", "202604", "OU4", "Another error", level="warning", log_current_run=True
    )
    log_args = sync.log_function.call_args[1]
    assert log_args["level"] == "warning"
    assert log_args["log_current_run"] is True


def test_log_and_append_error_multiple_errors():
    """Test that multiple calls to _log_and_append_error append errors correctly."""
    sync = DatasetCompletionSync(None, None)
    sync.log_function = MagicMock()
    sync._reset_summary()
    sync._log_and_append_error("fetch_errors", "DS5", "202605", "OU5", "First error")
    sync._log_and_append_error("fetch_errors", "DS5", "202605", "OU6", "Second error")
    assert len(sync.import_summary["errors"]["fetch_errors"]) == 2
