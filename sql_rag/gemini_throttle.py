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

import google.generativeai as genai

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

# --- Multi-key rotation state (module-level) ---

_keys: list[str] = []
_exhausted_until: dict[int, float] = {}
_EXHAUSTION_DURATION_SECONDS = 1800.0  # 30 minutes


def configure_keys(keys: list[str | None]) -> None:
    """Configure the list of Gemini API keys to use for rotation.

    Empty strings and None entries are silently dropped. If the cleaned key
    list is identical to the currently-configured list, this call is a no-op
    — exhaustion state and the active-key cursor are preserved. This matters
    because reconcilers are instantiated per HTTP request, so the same keys
    are re-passed many times during a scan; without idempotence, every
    request would wipe the 30-minute cooldown timer.

    Pass an empty list to disable rotation entirely (the helper then falls
    back to whatever key the caller's `model` already has configured globally
    — i.e. today's behaviour).
    """
    global _keys, _exhausted_until
    cleaned = [k.strip() for k in keys if k and isinstance(k, str) and k.strip()]
    with _lock:
        if cleaned == _keys:
            # Same key list — preserve exhaustion state. Don't log the
            # "configured with N key(s)" line on every reconciler init.
            return
        _keys = cleaned
        _exhausted_until = {}
    logger.info("Gemini throttle configured with %d key(s)", len(cleaned))


def _get_active_keys_for_testing() -> list[str]:
    """Test-only accessor for the configured key list."""
    with _lock:
        return list(_keys)


def _select_active_key_idx() -> int | None:
    """Return the index of the lowest-indexed key not currently exhausted.

    Returns None if no keys are configured, or if every configured key
    is currently inside its exhaustion cooldown window. When an exhausted
    key rolls off the cooldown, an INFO log is emitted once (the
    `_exhausted_until` entry is then cleared so the log doesn't repeat).
    """
    with _lock:
        if not _keys:
            return None
        now = time.monotonic()
        for idx in range(len(_keys)):
            until = _exhausted_until.get(idx, 0.0)
            if until <= now:
                # If this key was previously exhausted, log its recovery once
                # and clear the timestamp so subsequent calls don't re-log.
                if idx in _exhausted_until:
                    logger.info(
                        "Gemini key %d/%d eligible again after %.0f-minute cooldown",
                        idx + 1,
                        len(_keys),
                        _EXHAUSTION_DURATION_SECONDS / 60.0,
                    )
                    del _exhausted_until[idx]
                return idx
        return None


def _mark_key_exhausted(idx: int) -> None:
    """Flag a key as exhausted for the next _EXHAUSTION_DURATION_SECONDS."""
    with _lock:
        _exhausted_until[idx] = time.monotonic() + _EXHAUSTION_DURATION_SECONDS


def _reset_throttle_state_for_testing() -> None:
    """Reset module-level throttle state. Test-only helper."""
    global _last_call_time, _keys, _exhausted_until
    with _lock:
        _last_call_time = 0.0
        _keys = []
        _exhausted_until = {}


def _attempt_with_backoff(
    model: Any,
    parts_list: list[Any],
    *,
    filename: str | None = None,
) -> Any:
    """Inner attempt loop: initial call + retries on 429.

    Raises RateLimitExhaustedError if all retries hit 429.
    Raises ExtractionFailedError on the first non-rate-limit failure (no retry).
    """
    global _last_call_time

    last_error: BaseException | None = None

    for attempt in range(len(_BACKOFF_SCHEDULE) + 1):
        # Throttle: ensure >= _MIN_INTERVAL_SECONDS since last call
        with _lock:
            now = time.monotonic()
            elapsed = now - _last_call_time
            wait = _MIN_INTERVAL_SECONDS - elapsed
            prev_last = _last_call_time

        if wait > 0 and prev_last > 0.0:
            time.sleep(wait)

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
                raise ExtractionFailedError(filename=filename, reason=str(exc)) from exc

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

            logger.warning(
                "Gemini rate limit exhausted after %d retries%s",
                len(_BACKOFF_SCHEDULE),
                f" for {filename}" if filename else "",
            )
            raise RateLimitExhaustedError(
                filename=filename, last_error=str(last_error)
            ) from last_error

    raise RateLimitExhaustedError(filename=filename, last_error=str(last_error))


def call_gemini_with_throttle(
    model: Any,
    parts: Iterable[Any],
    *,
    filename: str | None = None,
) -> Any:
    """Call `model.generate_content(parts)` with throttling, 429 retries, and
    automatic key rotation when configured via `configure_keys()`.

    If no keys have been configured (`configure_keys` not called or called
    with `[]`), behaviour is identical to a single-key throttle: the call
    uses whatever key `genai.configure()` has set globally.

    If keys are configured:
        - Outer loop tries each non-exhausted key in numbered order.
        - For each key, the inner attempt-with-backoff runs (initial + 3 retries).
        - On RateLimitExhaustedError, the key is marked exhausted (30-min cooldown)
          and rotation continues to the next eligible key.
        - On ExtractionFailedError, the error propagates immediately (other keys
          would not help).
        - When all configured keys are exhausted, RateLimitExhaustedError is raised.

    Args:
        model: a configured `genai.GenerativeModel` instance.
        parts: the list passed to `generate_content`.
        filename: optional filename for inclusion in error messages and logs.

    Returns:
        The successful response from `generate_content`.

    Raises:
        RateLimitExhaustedError: every available key was rate-limited.
        ExtractionFailedError: a non-rate-limit exception occurred.
    """
    parts_list = list(parts)

    # Single-key path: no rotation, preserves existing behaviour
    with _lock:
        keys_snapshot = list(_keys)

    if not keys_snapshot:
        return _attempt_with_backoff(model, parts_list, filename=filename)

    # Multi-key path: rotate through eligible keys
    last_error: BaseException | None = None

    while True:
        active_idx = _select_active_key_idx()
        if active_idx is None:
            logger.warning(
                "All %d Gemini keys rate-limited; raising RateLimitExhaustedError%s",
                len(keys_snapshot),
                f" for {filename}" if filename else "",
            )
            raise RateLimitExhaustedError(
                filename=filename,
                last_error=f"all {len(keys_snapshot)} keys rate-limited",
            ) from last_error

        active_key = keys_snapshot[active_idx]

        # Configure SDK with the active key under the throttle lock to keep
        # the swap atomic relative to other in-flight calls.
        with _lock:
            genai.configure(api_key=active_key)

        try:
            return _attempt_with_backoff(model, parts_list, filename=filename)
        except RateLimitExhaustedError as exc:
            last_error = exc
            _mark_key_exhausted(active_idx)
            next_idx = _select_active_key_idx()
            if next_idx is None:
                logger.warning(
                    "All %d Gemini keys rate-limited; raising RateLimitExhaustedError%s",
                    len(keys_snapshot),
                    f" for {filename}" if filename else "",
                )
                raise RateLimitExhaustedError(
                    filename=filename,
                    last_error=f"all {len(keys_snapshot)} keys rate-limited",
                ) from exc
            logger.warning(
                "Gemini key %d/%d rate-limit exhausted; rotating to key %d/%d%s",
                active_idx + 1,
                len(keys_snapshot),
                next_idx + 1,
                len(keys_snapshot),
                f" for {filename}" if filename else "",
            )
            continue
        except ExtractionFailedError:
            # Non-rate-limit errors are not key-specific — re-raise.
            raise
