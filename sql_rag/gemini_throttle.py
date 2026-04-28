"""
Throttled, retrying wrapper for Google Gemini calls.

Used by bank statement extraction to:
- Cap call rate (≥1s between consecutive Gemini calls process-wide)
- Retry 429 / quota-exhausted responses with exponential backoff
- Surface non-retryable failures as typed exceptions instead of bare `Exception`

This module is shared between Opera SE (`statement_reconcile.py`) and
Opera 3 (`statement_reconcile_opera3.py`) so both data sources behave identically.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Iterable

logger = logging.getLogger(__name__)


class RateLimitExhaustedError(Exception):
    """Raised when Gemini returns 429 / RESOURCE_EXHAUSTED on every retry."""

    def __init__(self, filename: str | None = None, last_error: str | None = None):
        self.filename = filename
        self.last_error = last_error
        msg = f"Gemini rate limit exhausted after retries"
        if filename:
            msg += f" for {filename}"
        if last_error:
            msg += f": {last_error}"
        super().__init__(msg)


class ExtractionFailedError(Exception):
    """Raised when Gemini extraction fails for a non-rate-limit reason."""

    def __init__(self, filename: str | None = None, reason: str | None = None):
        self.filename = filename
        self.reason = reason
        msg = "Gemini extraction failed"
        if filename:
            msg += f" for {filename}"
        if reason:
            msg += f": {reason}"
        super().__init__(msg)
