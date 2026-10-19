"""Logging setup with a low-noise default for the trading loop."""
from __future__ import annotations

import logging
import os
from typing import Optional

_FORMAT = "%(asctime)s %(levelname)-8s %(name)s: %(message)s"


def configure_logging(level: Optional[str] = None) -> None:
    """Configures root logging once; ``HFT_LOG_LEVEL`` overrides the default."""
    resolved = (level or os.environ.get("HFT_LOG_LEVEL", "INFO")).upper()
    logging.basicConfig(
        level=getattr(logging, resolved, logging.INFO),
        format=_FORMAT,
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
