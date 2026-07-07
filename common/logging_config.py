from __future__ import annotations

import logging
import os

_CONFIGURED = False


def get_logger(name: str) -> logging.Logger:
    """Return a module-level logger with a consistent structured format.

    Call sites do `from common.logging_config import get_logger` and
    `logger = get_logger(__name__)`.
    """
    global _CONFIGURED
    if not _CONFIGURED:
        logging.basicConfig(
            level=os.getenv("LOG_LEVEL", "INFO"),
            format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        )
        _CONFIGURED = True
    return logging.getLogger(name)
