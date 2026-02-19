"""dp - Self-hosted data platform."""

from __future__ import annotations

import logging

__version__ = "0.1.0"


def setup_logging(level: str = "INFO") -> None:
    """Configure structured logging for the dp platform.

    Sets up a consistent log format with timestamps, logger names, and levels.
    Call this once at application startup (CLI or server).
    """
    log_level = getattr(logging, level.upper(), logging.INFO)
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    root_logger = logging.getLogger("dp")
    root_logger.setLevel(log_level)
    # Avoid duplicate handlers on repeated calls
    if not root_logger.handlers:
        root_logger.addHandler(handler)
