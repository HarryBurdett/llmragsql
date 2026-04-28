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


_RATE_LIMIT_TOKENS = (
    "429",
    "resource exhausted",
    "resource_exhausted",
    "quota",
    "rate limit",
)


def is_rate_limit_error(exc: BaseException) -> bool:
    """Detect a Gemini 429 / quota-exceeded response from any exception type.

    Inspects the exception message for known rate-limit tokens. Case-insensitive.
    """
    msg = str(exc).lower()
    if not msg:
        return False
    return any(tok in msg for tok in _RATE_LIMIT_TOKENS)


# --- Throttle state (process-wide) ---

_lock = threading.Lock()
_last_call_time: float = 0.0
_MIN_INTERVAL_SECONDS = 1.0
_BACKOFF_SCHEDULE = (5.0, 15.0, 45.0)  # seconds; len == max retries


def _reset_throttle_state_for_testing() -> None:
    """Reset module-level throttle state. Test-only helper."""
    global _last_call_time
    with _lock:
        _last_call_time = 0.0


def call_gemini_with_throttle(
    model: Any,
    parts: Iterable[Any],
    *,
    filename: str | None = None,
) -> Any:
    """Call `model.generate_content(parts)` with throttling and 429 retries.

    Args:
        model: a configured `genai.GenerativeModel` instance.
        parts: the list passed to `generate_content` (file_part + prompt, etc).
        filename: optional filename for inclusion in error messages and logs.

    Returns:
        The successful response from `generate_content`.

    Raises:
        RateLimitExhaustedError: all retries hit 429.
        ExtractionFailedError: a non-rate-limit exception occurred (no retry).
    """
    global _last_call_time

    parts_list = list(parts)
    last_error: BaseException | None = None

    # Initial attempt + len(_BACKOFF_SCHEDULE) retries
    for attempt in range(len(_BACKOFF_SCHEDULE) + 1):
        # --- Throttle: ensure >= _MIN_INTERVAL_SECONDS since last call ---
        with _lock:
            now = time.monotonic()
            elapsed = now - _last_call_time
            wait = _MIN_INTERVAL_SECONDS - elapsed
            prev_last = _last_call_time

        if wait > 0 and prev_last > 0.0:
            time.sleep(wait)

        # --- Attempt the call ---
        try:
            response = model.generate_content(parts_list)
            with _lock:
                _last_call_time = time.monotonic()
            return response
        except Exception as exc:
            with _lock:
                _last_call_time = time.monotonic()
            last_error = exc

            if not is_rate_limit_error(exc):
                # Not retryable — surface immediately
                raise ExtractionFailedError(filename=filename, reason=str(exc)) from exc

            # Rate limit — back off and retry if we have retries left
            if attempt < len(_BACKOFF_SCHEDULE):
                backoff = _BACKOFF_SCHEDULE[attempt]
                logger.warning(
                    "Gemini 429 retry %d/%d after %.0fs%s",
                    attempt + 1,
                    len(_BACKOFF_SCHEDULE),
                    backoff,
                    f" for {filename}" if filename else "",
                )
                time.sleep(backoff)
                continue

            # All retries exhausted
            logger.warning(
                "Gemini rate limit exhausted after %d retries%s",
                len(_BACKOFF_SCHEDULE),
                f" for {filename}" if filename else "",
            )
            raise RateLimitExhaustedError(
                filename=filename, last_error=str(last_error)
            ) from last_error

    # Unreachable, but satisfies type checkers
    raise RateLimitExhaustedError(filename=filename, last_error=str(last_error))
