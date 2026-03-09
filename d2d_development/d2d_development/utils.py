import logging

from openhexa.sdk import current_run


def log_message(
    logger: logging.Logger,
    message: str,
    error_details: str = "",
    log_current_run: bool = True,
    level: str = "info",
    exception_class: type[Exception] = Exception,
) -> None:
    """Log a message to both the current run and the configured logger.

    Parameters
    ----------
    logger : logging.Logger
        The logger to use for logging the message.
    message : str
        The message to log.
    error_details : str, optional
        Additional details to include in error logs, by default "".
    log_current_run : bool, optional
        Whether to log the message to the current run, by default True.
    level : str, optional
        The logging level ('info', 'warning', 'error'), by default 'info'.
    exception_class : Exception, optional
        The exception class to raise for invalid logging levels, by default None.
    """
    if level == "info":
        logger.info(message)
    elif level == "warning":
        logger.warning(message)
    elif level == "error":
        logger.error(f"{message} Details: {error_details}")
    else:
        raise exception_class(f"Invalid logging level: {level}")

    # Log to current_run only if it exists
    if log_current_run and "current_run" in globals() and current_run is not None:
        if level == "info":
            current_run.log_info(message)
        elif level == "warning":
            current_run.log_warning(message)
        elif level == "error":
            current_run.log_error(message)
