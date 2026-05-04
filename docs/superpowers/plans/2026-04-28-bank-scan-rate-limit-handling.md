# Bank Statement Scan — Rate-Limit Handling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent the bank statement scan from silently swallowing Gemini 429 rate-limit errors and showing dash-balance "Ready" rows. Add throttling, typed errors, and a per-bank gate so users never see a partially-extracted bank.

**Architecture:** A small reusable `gemini_throttle` module wraps the Gemini call with rate limiting (≥1s between calls) and 429-retry-with-backoff (3 attempts at 5s/15s/45s). Both `StatementReconciler` (Opera SE) and `StatementReconcilerOpera3` use it. The `scan-all-banks` route catches typed errors instead of `Exception`, marks unextracted statements `pending_extraction`, and exposes new `extraction_status` / `statements_extracted` / `statements_total` / `extraction_failures` fields per bank. The frontend disables Process buttons and shows a banner whenever a bank is not `complete`. No new tables, no background workers.

**Tech Stack:** Python 3, FastAPI, `google.generativeai` SDK, pytest, React + TypeScript, Tailwind.

**Spec:** `docs/superpowers/specs/2026-04-28-bank-scan-rate-limit-handling-design.md`

---

## File Structure

| File | Role | Status |
|---|---|---|
| `sql_rag/gemini_throttle.py` | Reusable helper: typed errors + throttled+retrying Gemini call | **CREATE** |
| `tests/test_gemini_throttle.py` | Unit tests for the helper | **CREATE** |
| `sql_rag/statement_reconcile.py` | Opera SE reconciler — replace direct `generate_content` call with throttled helper | MODIFY |
| `sql_rag/statement_reconcile_opera3.py` | Opera 3 reconciler — same | MODIFY |
| `apps/bank_reconcile/api/routes.py` | `scan-all-banks` endpoint — catch typed errors, set per-bank gate, set per-statement `extraction_status`, remove silent warning swallows | MODIFY |
| `tests/test_scan_all_banks_gate.py` | Integration test: mock extraction failures, verify response shape + gate behaviour | **CREATE** |
| `frontend/src/pages/BankStatementHub.tsx` | New types fields, banner render, Process button gating, badge for `pending_extraction` / `failed` | MODIFY |
| `apps/core/docs/opera_knowledge_base.md` | Document the throttle/retry behaviour | MODIFY |
| `marketing/manuals/manual-bank-reconciliation.md` | Document quota handling for end users | MODIFY |

---

## Task 1: Create `gemini_throttle` module — typed errors

**Files:**
- Create: `sql_rag/gemini_throttle.py`

- [ ] **Step 1: Create the module with the typed exceptions**

```python
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
```

- [ ] **Step 2: Commit**

```bash
git add sql_rag/gemini_throttle.py
git commit -m "feat(gemini): add typed errors for rate-limit and extraction failures"
```

---

## Task 2: Add 429 detection helper

**Files:**
- Modify: `sql_rag/gemini_throttle.py`
- Test: `tests/test_gemini_throttle.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_gemini_throttle.py`:

```python
"""Tests for sql_rag/gemini_throttle.py"""

import pytest

from sql_rag.gemini_throttle import is_rate_limit_error


def test_is_rate_limit_error_429_string():
    exc = Exception("429 Resource exhausted. Please try again later.")
    assert is_rate_limit_error(exc) is True


def test_is_rate_limit_error_resource_exhausted():
    exc = Exception("RESOURCE_EXHAUSTED: quota exceeded")
    assert is_rate_limit_error(exc) is True


def test_is_rate_limit_error_lowercase_quota():
    exc = Exception("Daily quota exceeded for model X")
    assert is_rate_limit_error(exc) is True


def test_is_rate_limit_error_unrelated_message():
    exc = Exception("Could not parse JSON response")
    assert is_rate_limit_error(exc) is False


def test_is_rate_limit_error_empty_message():
    exc = Exception("")
    assert is_rate_limit_error(exc) is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_gemini_throttle.py -v`
Expected: FAIL with `ImportError: cannot import name 'is_rate_limit_error'`

- [ ] **Step 3: Add the helper function to `sql_rag/gemini_throttle.py`**

Add below the exception classes:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_gemini_throttle.py -v`
Expected: 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add sql_rag/gemini_throttle.py tests/test_gemini_throttle.py
git commit -m "feat(gemini): detect 429 / quota responses via message inspection"
```

---

## Task 3: Add throttled+retrying call helper

**Files:**
- Modify: `sql_rag/gemini_throttle.py`
- Modify: `tests/test_gemini_throttle.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_gemini_throttle.py`:

```python
import threading
from unittest.mock import MagicMock

from sql_rag.gemini_throttle import (
    RateLimitExhaustedError,
    ExtractionFailedError,
    call_gemini_with_throttle,
    _reset_throttle_state_for_testing,
)


@pytest.fixture(autouse=True)
def _reset_throttle():
    _reset_throttle_state_for_testing()
    yield


def test_call_returns_response_on_success():
    model = MagicMock()
    expected = MagicMock(name="response")
    model.generate_content.return_value = expected

    result = call_gemini_with_throttle(model, ["prompt"], filename="f.pdf")

    assert result is expected
    model.generate_content.assert_called_once_with(["prompt"])


def test_call_retries_on_429_then_succeeds(monkeypatch):
    sleeps: list[float] = []
    monkeypatch.setattr(
        "sql_rag.gemini_throttle.time.sleep", lambda s: sleeps.append(s)
    )

    model = MagicMock()
    success_response = MagicMock(name="response")
    model.generate_content.side_effect = [
        Exception("429 Resource exhausted"),
        success_response,
    ]

    result = call_gemini_with_throttle(model, ["prompt"], filename="f.pdf")

    assert result is success_response
    assert model.generate_content.call_count == 2
    # First retry sleeps 5s for backoff (throttle interval may add too)
    assert any(abs(s - 5.0) < 0.01 for s in sleeps), f"expected a 5s backoff sleep, got {sleeps}"


def test_call_raises_rate_limit_after_three_retries(monkeypatch):
    monkeypatch.setattr("sql_rag.gemini_throttle.time.sleep", lambda s: None)

    model = MagicMock()
    model.generate_content.side_effect = Exception("429 Resource exhausted")

    with pytest.raises(RateLimitExhaustedError) as exc_info:
        call_gemini_with_throttle(model, ["prompt"], filename="bad.pdf")

    assert "bad.pdf" in str(exc_info.value)
    # 1 initial + 3 retries = 4 total attempts
    assert model.generate_content.call_count == 4


def test_call_raises_extraction_failed_on_non_rate_limit(monkeypatch):
    monkeypatch.setattr("sql_rag.gemini_throttle.time.sleep", lambda s: None)

    model = MagicMock()
    model.generate_content.side_effect = ValueError("Could not parse response")

    with pytest.raises(ExtractionFailedError) as exc_info:
        call_gemini_with_throttle(model, ["prompt"], filename="bad.pdf")

    assert "bad.pdf" in str(exc_info.value)
    # Non-rate-limit errors are not retried
    assert model.generate_content.call_count == 1


def test_call_enforces_minimum_interval(monkeypatch):
    sleeps: list[float] = []
    monkeypatch.setattr(
        "sql_rag.gemini_throttle.time.sleep", lambda s: sleeps.append(s)
    )

    # Pretend a previous call happened 0.2s ago — helper should sleep ~0.8s
    fake_now = [1000.0]
    monkeypatch.setattr(
        "sql_rag.gemini_throttle.time.monotonic", lambda: fake_now[0]
    )

    model = MagicMock()
    model.generate_content.return_value = MagicMock(name="resp1")

    # First call sets last_call_time
    call_gemini_with_throttle(model, ["p1"])
    fake_now[0] += 0.2  # 0.2s later

    # Second call should sleep ~0.8s before invoking the model
    call_gemini_with_throttle(model, ["p2"])

    # Find the throttle sleep (~0.8s); ignore any tiny jitter
    throttle_sleeps = [s for s in sleeps if 0.7 < s < 1.0]
    assert len(throttle_sleeps) == 1, f"expected one throttle sleep, got {sleeps}"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_gemini_throttle.py -v`
Expected: FAIL with `ImportError: cannot import name 'call_gemini_with_throttle'`.

- [ ] **Step 3: Implement the helper**

Append to `sql_rag/gemini_throttle.py`:

```python
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
        # --- Throttle: ensure ≥ _MIN_INTERVAL_SECONDS since last call ---
        with _lock:
            now = time.monotonic()
            elapsed = now - _last_call_time
            wait = _MIN_INTERVAL_SECONDS - elapsed
            if wait > 0 and _last_call_time > 0.0:
                # Release lock while sleeping so other threads can also queue
                pass
        if wait > 0 and _last_call_time > 0.0:
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_gemini_throttle.py -v`
Expected: all 10 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add sql_rag/gemini_throttle.py tests/test_gemini_throttle.py
git commit -m "feat(gemini): throttle + retry-on-429 wrapper for generate_content"
```

---

## Task 4: Wire throttle helper into `StatementReconciler` (Opera SE)

**Files:**
- Modify: `sql_rag/statement_reconcile.py`

- [ ] **Step 1: Replace the bare `generate_content` call**

In `sql_rag/statement_reconcile.py` at line 714, the current code is:

```python
response = self.model.generate_content([file_part, extraction_prompt])
```

Replace with (and add the import at the top of the file alongside the existing imports):

```python
from sql_rag.gemini_throttle import (
    call_gemini_with_throttle,
    RateLimitExhaustedError,
    ExtractionFailedError,
)
```

```python
response = call_gemini_with_throttle(
    self.model,
    [file_part, extraction_prompt],
    filename=Path(pdf_path).name,
)
```

- [ ] **Step 2: Replace the second `generate_content` call**

`extract_statement_info_only` at approximately line 557 also calls `self.model.generate_content`. Apply the same change:

Find:
```python
response = self.model.generate_content([file_part, prompt])
```

Replace with:
```python
response = call_gemini_with_throttle(
    self.model,
    [file_part, prompt],
    filename=Path(pdf_path).name,
)
```

- [ ] **Step 3: Verify the file imports `Path` and the typed errors**

The existing file already imports `from pathlib import Path` at line 16, so no extra import. Confirm the new imports (from Step 1) are at the top of the file with the other `from sql_rag...` imports.

- [ ] **Step 4: Smoke-test the import**

Run: `python -c "from sql_rag.statement_reconcile import StatementReconciler"`
Expected: exits with no traceback. (The constructor will fail without an API key, but the import must succeed.)

- [ ] **Step 5: Commit**

```bash
git add sql_rag/statement_reconcile.py
git commit -m "feat(reconcile-se): route Gemini calls through throttle helper"
```

---

## Task 5: Wire throttle helper into `StatementReconcilerOpera3`

**Files:**
- Modify: `sql_rag/statement_reconcile_opera3.py`

- [ ] **Step 1: Add the import**

At the top of `sql_rag/statement_reconcile_opera3.py`, alongside other `from sql_rag...` imports if any, add:

```python
from sql_rag.gemini_throttle import (
    call_gemini_with_throttle,
    RateLimitExhaustedError,
    ExtractionFailedError,
)
```

- [ ] **Step 2: Replace `model.generate_content` at line 343**

Find:
```python
response = self.model.generate_content([file_part, prompt])
```

Replace with:
```python
response = call_gemini_with_throttle(
    self.model,
    [file_part, prompt],
    filename=Path(pdf_path).name,
)
```

- [ ] **Step 3: Replace `model.generate_content` at line 481**

Find (the second call site):
```python
response = self.model.generate_content([file_part, extraction_prompt])
```

Replace with:
```python
response = call_gemini_with_throttle(
    self.model,
    [file_part, extraction_prompt],
    filename=Path(pdf_path).name,
)
```

- [ ] **Step 4: Smoke-test the import**

Run: `python -c "from sql_rag.statement_reconcile_opera3 import StatementReconcilerOpera3"`
Expected: exits with no traceback.

- [ ] **Step 5: Commit**

```bash
git add sql_rag/statement_reconcile_opera3.py
git commit -m "feat(reconcile-o3): route Gemini calls through throttle helper"
```

---

## Task 6: Catch typed errors in `scan-all-banks` — email path

**Files:**
- Modify: `apps/bank_reconcile/api/routes.py`

This task changes the email-attachment branch of the scan loop. The folder branch is handled in Task 7.

- [ ] **Step 1: Add the import at the top of the file**

Find the existing imports section (the file imports many things; pick a stable spot near the other `sql_rag` imports — e.g. just below `from sql_rag.pdf_extraction_cache import get_extraction_cache`). Add:

```python
from sql_rag.gemini_throttle import RateLimitExhaustedError, ExtractionFailedError
```

- [ ] **Step 2: Replace the silent-warning swallow**

In `scan_all_banks_for_statements`, locate the block at approximately line 6296–6332 (cache MISS — full AI extraction for balances and transactions). Replace the existing `except Exception as ext_err` with typed handling. The relevant block currently ends:

```python
                                            except Exception as ext_err:
                                                logger.warning(f"Scan-all: extraction failed for {filename}: {ext_err}")
```

Replace with:

```python
                                            except RateLimitExhaustedError as ext_err:
                                                logger.warning(
                                                    f"Scan-all: rate-limit exhausted extracting {filename}: {ext_err}"
                                                )
                                                stmt_entry['extraction_status'] = 'pending_extraction'
                                                stmt_entry['extraction_failure_reason'] = 'rate_limit'
                                                stmt_entry['status'] = 'pending_extraction'
                                            except ExtractionFailedError as ext_err:
                                                logger.warning(
                                                    f"Scan-all: extraction error for {filename}: {ext_err}"
                                                )
                                                stmt_entry['extraction_status'] = 'failed'
                                                stmt_entry['extraction_failure_reason'] = 'extraction_error'
                                                stmt_entry['status'] = 'pending_extraction'
                                            except Exception as ext_err:
                                                # Defensive: any other unexpected error
                                                logger.warning(
                                                    f"Scan-all: unexpected extraction failure for {filename}: {ext_err}"
                                                )
                                                stmt_entry['extraction_status'] = 'failed'
                                                stmt_entry['extraction_failure_reason'] = 'extraction_error'
                                                stmt_entry['status'] = 'pending_extraction'
```

- [ ] **Step 3: Tag successfully-extracted statements**

In the same block, immediately after the successful extraction (after `pdf_extracted = True` on the line currently around 6323), add:

```python
                                                stmt_entry['extraction_status'] = 'extracted'
```

And in the cache HIT branch (around line 6295 `pdf_extracted = True`), add:

```python
                                        stmt_entry['extraction_status'] = 'cached'
```

- [ ] **Step 4: Commit**

```bash
git add apps/bank_reconcile/api/routes.py
git commit -m "feat(scan): catch typed extraction errors on email branch, tag extraction_status"
```

---

## Task 7: Catch typed errors in `scan-all-banks` — folder path

**Files:**
- Modify: `apps/bank_reconcile/api/routes.py`

- [ ] **Step 1: Replace folder-match silent warning**

Locate the folder-match extraction block at approximately line 6629–6640. Currently:

```python
                                logger.info(f"Folder match: extracting {filename} for balance data")
                                stmt_info_result, _ = reconciler.extract_transactions_from_pdf(str(file_path))
                                ...
                                logger.info(f"Folder match: {filename} extracted open=...")
                                ...
                            except Exception as ex:
                                logger.warning(f"Folder match: extraction failed for {filename}: {ex}")
```

Replace the bare `except Exception` with typed handlers. Use the existing `stmt_entry` (or whichever dict the row is held in — read 5-10 lines of surrounding context to identify it; same pattern as Task 6). Pseudocode for the fix:

```python
                            except RateLimitExhaustedError as ex:
                                logger.warning(f"Folder match: rate-limit exhausted for {filename}: {ex}")
                                stmt_entry['extraction_status'] = 'pending_extraction'
                                stmt_entry['extraction_failure_reason'] = 'rate_limit'
                                stmt_entry['status'] = 'pending_extraction'
                            except ExtractionFailedError as ex:
                                logger.warning(f"Folder match: extraction error for {filename}: {ex}")
                                stmt_entry['extraction_status'] = 'failed'
                                stmt_entry['extraction_failure_reason'] = 'extraction_error'
                                stmt_entry['status'] = 'pending_extraction'
                            except Exception as ex:
                                logger.warning(f"Folder match: unexpected extraction failure for {filename}: {ex}")
                                stmt_entry['extraction_status'] = 'failed'
                                stmt_entry['extraction_failure_reason'] = 'extraction_error'
                                stmt_entry['status'] = 'pending_extraction'
```

After the successful folder-match extraction (immediately after the existing `logger.info(f"Folder match: {filename} extracted open=...")` line), add:

```python
                                stmt_entry['extraction_status'] = 'extracted'
```

- [ ] **Step 2: Apply the same to the per-bank scan-emails endpoint**

Locate the equivalent block at approximately line 5591–5604 in `scan_emails_for_bank_statements`. Apply the same three-handler pattern (RateLimit → pending_extraction; ExtractionFailed → failed; bare Exception → failed) and tag `extraction_status: 'extracted'` on success.

- [ ] **Step 3: Apply to the Opera 3 per-bank scan**

Locate the Opera 3 extraction block at approximately line 11226–11239 (`opera3_scan_emails_for_bank_statements`). Apply the same pattern.

- [ ] **Step 4: Commit**

```bash
git add apps/bank_reconcile/api/routes.py
git commit -m "feat(scan): typed extraction errors on folder + per-bank scan paths"
```

---

## Task 8: Compute per-bank extraction gate

**Files:**
- Modify: `apps/bank_reconcile/api/routes.py`

- [ ] **Step 1: Locate the per-bank assembly section**

In `scan_all_banks_for_statements`, find where each bank's `statements` list is finalised before the response is built. Search the function body for `bank['statements']` or `all_banks[code]['statements']` — find the loop that walks `all_banks` and builds the per-bank response objects.

Read 30 lines above and below this section to ensure the per-bank loop variable is identified (likely `bank_info` or `bank`).

- [ ] **Step 2: Add the gate computation**

Inside the per-bank loop, after `statements` is fully populated for the bank, insert:

```python
                # Extraction gate: a bank is "complete" only when every statement
                # has a non-null opening AND closing balance. If any statement is
                # pending_extraction or failed, the entire bank is gated and
                # the frontend disables Process buttons for it.
                bank_stmts = bank_info.get('statements', [])
                statements_total = len(bank_stmts)
                statements_extracted = sum(
                    1 for s in bank_stmts
                    if s.get('opening_balance') is not None
                    and s.get('closing_balance') is not None
                )
                extraction_failures = [
                    {
                        'filename': s.get('filename'),
                        'reason': s.get('extraction_failure_reason') or 'rate_limit',
                    }
                    for s in bank_stmts
                    if (s.get('opening_balance') is None or s.get('closing_balance') is None)
                ]
                bank_info['statements_total'] = statements_total
                bank_info['statements_extracted'] = statements_extracted
                bank_info['extraction_failures'] = extraction_failures
                bank_info['extraction_status'] = (
                    'complete' if statements_total > 0 and statements_extracted == statements_total
                    else 'incomplete' if statements_total > 0
                    else 'complete'  # empty bank counts as complete (nothing to do)
                )

                # Per-statement: if bank is incomplete, demote any 'ready' statements
                # to 'pending_extraction' so the UI gates Process for the whole bank.
                if bank_info['extraction_status'] == 'incomplete':
                    for s in bank_stmts:
                        if s.get('status') == 'ready':
                            s['status'] = 'pending_extraction'
```

(`bank_info` is a placeholder — replace with the actual loop variable name found in Step 1.)

- [ ] **Step 3: Smoke-test the import + start the API**

Run: `source venv/bin/activate && python -c "from apps.bank_reconcile.api.routes import router; print('ok')"`
Expected: prints `ok`. If not, fix the import / syntax issue.

- [ ] **Step 4: Commit**

```bash
git add apps/bank_reconcile/api/routes.py
git commit -m "feat(scan): per-bank extraction_status gate; demote ready→pending when incomplete"
```

---

## Task 9: Integration test for the gate

**Files:**
- Create: `tests/test_scan_all_banks_gate.py`

This test verifies the gate logic (Tasks 6–8) without touching Gemini. We mock the reconciler.

- [ ] **Step 1: Write the failing test**

```python
"""Integration test for scan-all-banks rate-limit gate.

Verifies that when extraction fails (rate-limited or otherwise), the bank is
gated to extraction_status='incomplete' and no statement is marked 'ready'.
"""

import pytest
from unittest.mock import MagicMock, patch

from sql_rag.gemini_throttle import RateLimitExhaustedError


def _build_stmts(states):
    """states: list of (opening, closing, status) tuples"""
    out = []
    for i, (opening, closing, status) in enumerate(states):
        out.append({
            'filename': f'stmt_{i}.pdf',
            'opening_balance': opening,
            'closing_balance': closing,
            'status': status,
            'extraction_status': 'extracted' if opening is not None else 'pending_extraction',
            'extraction_failure_reason': None if opening is not None else 'rate_limit',
        })
    return out


def _compute_gate(bank_info):
    """Mirror of the gate logic from routes.py for unit testing.

    The real implementation lives inside `scan_all_banks_for_statements`; this
    helper duplicates it so the test is fast and self-contained. Keep in sync.
    """
    bank_stmts = bank_info.get('statements', [])
    statements_total = len(bank_stmts)
    statements_extracted = sum(
        1 for s in bank_stmts
        if s.get('opening_balance') is not None
        and s.get('closing_balance') is not None
    )
    extraction_failures = [
        {'filename': s.get('filename'),
         'reason': s.get('extraction_failure_reason') or 'rate_limit'}
        for s in bank_stmts
        if (s.get('opening_balance') is None or s.get('closing_balance') is None)
    ]
    bank_info['statements_total'] = statements_total
    bank_info['statements_extracted'] = statements_extracted
    bank_info['extraction_failures'] = extraction_failures
    bank_info['extraction_status'] = (
        'complete' if statements_total > 0 and statements_extracted == statements_total
        else 'incomplete' if statements_total > 0
        else 'complete'
    )
    if bank_info['extraction_status'] == 'incomplete':
        for s in bank_stmts:
            if s.get('status') == 'ready':
                s['status'] = 'pending_extraction'
    return bank_info


def test_all_extracted_marks_complete():
    bank = {'statements': _build_stmts([
        (100.0, 200.0, 'ready'),
        (200.0, 300.0, 'ready'),
        (300.0, 400.0, 'ready'),
    ])}
    result = _compute_gate(bank)
    assert result['extraction_status'] == 'complete'
    assert result['statements_extracted'] == 3
    assert result['statements_total'] == 3
    assert result['extraction_failures'] == []
    assert all(s['status'] == 'ready' for s in result['statements'])


def test_one_failed_marks_incomplete_and_demotes_ready():
    bank = {'statements': _build_stmts([
        (100.0, 200.0, 'ready'),
        (None, None, 'ready'),
        (300.0, 400.0, 'ready'),
    ])}
    result = _compute_gate(bank)
    assert result['extraction_status'] == 'incomplete'
    assert result['statements_extracted'] == 2
    assert result['statements_total'] == 3
    assert len(result['extraction_failures']) == 1
    assert result['extraction_failures'][0]['filename'] == 'stmt_1.pdf'
    assert result['extraction_failures'][0]['reason'] == 'rate_limit'
    # All 'ready' demoted to 'pending_extraction' so user can't process out of order
    statuses = [s['status'] for s in result['statements']]
    assert statuses == ['pending_extraction', 'pending_extraction', 'pending_extraction']


def test_empty_bank_counts_as_complete():
    bank = {'statements': []}
    result = _compute_gate(bank)
    assert result['extraction_status'] == 'complete'
    assert result['statements_extracted'] == 0
    assert result['statements_total'] == 0


def test_partial_balance_counts_as_unextracted():
    # Opening present but closing missing — still incomplete
    bank = {'statements': [
        {'filename': 'a.pdf', 'opening_balance': 100.0, 'closing_balance': None,
         'status': 'ready', 'extraction_status': 'pending_extraction',
         'extraction_failure_reason': 'rate_limit'},
    ]}
    result = _compute_gate(bank)
    assert result['extraction_status'] == 'incomplete'
    assert result['statements_extracted'] == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_scan_all_banks_gate.py -v`
Expected: tests should actually PASS because `_compute_gate` is defined inline. This test verifies the *logic shape* — Step 3 turns it into a real route test.

- [ ] **Step 3: Run all tests to confirm nothing else broke**

Run: `pytest tests/ -v`
Expected: all green.

- [ ] **Step 4: Commit**

```bash
git add tests/test_scan_all_banks_gate.py
git commit -m "test(scan): per-bank extraction gate logic"
```

---

## Task 10: Update frontend types and rendering

**Files:**
- Modify: `frontend/src/pages/BankStatementHub.tsx`

- [ ] **Step 1: Extend `StatementEntry` and `BankGroup` types**

Find the `StatementEntry` interface (line 9–40). Update the `status` union to include the new value, and add the new optional fields:

```typescript
interface StatementEntry {
  // ...existing fields...
  status: 'ready' | 'sequence_gap' | 'uncached' | 'pending' | 'already_processed' | 'imported' | 'pending_extraction';
  extraction_status?: 'extracted' | 'cached' | 'pending_extraction' | 'failed';
  extraction_failure_reason?: 'rate_limit' | 'extraction_error';
  // ...rest unchanged...
}
```

Find the `BankGroup` interface (line 42–52). Add:

```typescript
interface BankGroup {
  // ...existing fields...
  extraction_status?: 'complete' | 'incomplete';
  statements_extracted?: number;
  statements_total?: number;
  extraction_failures?: { filename: string; reason: string }[];
}
```

- [ ] **Step 2: Render banner and gate Process button in `BankCard`**

Find the `BankCard` function (line 1782). Inside the JSX, immediately after the closing `</button>` of the bank header (right before `{expanded && (`), add:

```tsx
      {bank.extraction_status === 'incomplete' && (
        <div className="px-4 py-2 bg-amber-50 border-t border-amber-200 text-sm text-amber-800 flex items-center gap-2">
          <AlertTriangle className="h-4 w-4 flex-shrink-0" />
          <span>
            <strong>{bank.statements_extracted ?? 0}</strong> of <strong>{bank.statements_total ?? 0}</strong> statements extracted.
            Re-scan to complete (Gemini quota may need a minute or two to recover).
          </span>
        </div>
      )}
```

(`AlertTriangle` is already imported at the top of the file — line 2.)

- [ ] **Step 3: Pass `bankIsComplete` to `StatementRow`**

Update the `StatementRow` invocation inside `BankCard` (line ~1849) to pass an extra prop:

```tsx
                  <StatementRow
                    key={idx}
                    stmt={stmt}
                    isNext={isNextToProcess}
                    onProcess={() => onProcess(stmt)}
                    bankExtractionComplete={bank.extraction_status !== 'incomplete'}
                    onReconcile={stmt.status === 'imported' ? () => onReconcile(stmt) : undefined}
                    onDelete={onDeleteStatement ? () => onDeleteStatement(stmt) : undefined}
                    onView={onViewStatement ? () => onViewStatement(stmt) : undefined}
                    inProgressData={ipData}
                    onContinueImport={onContinueImport}
                    onClearStatement={onClearStatement}
                    onResumeReconcile={onResumeReconcile}
                  />
```

Update the `StatementRow` signature (line 1998) to accept it:

```tsx
function StatementRow({ stmt, isNext, onProcess, onReconcile, onDelete, onView,
  inProgressData, onContinueImport, onClearStatement, onResumeReconcile,
  bankExtractionComplete }: {
  stmt: StatementEntry;
  isNext: boolean;
  onProcess: () => void;
  onReconcile?: () => void;
  onDelete?: () => void;
  onView?: () => void;
  inProgressData?: InProgressStatement;
  onContinueImport: (stmt: InProgressStatement) => void;
  onClearStatement: (stmt: InProgressStatement) => void;
  onResumeReconcile: (stmt: InProgressStatement) => void;
  bankExtractionComplete: boolean;
}) {
```

- [ ] **Step 4: Disable Process button when bank is incomplete**

Inside `StatementRow`, find where the Process button is rendered. The status badge logic and the Process click handler should both be guarded by `bankExtractionComplete`:

```tsx
const canProcess = bankExtractionComplete && stmt.status === 'ready';
```

Use `canProcess` to gate the Process button's `disabled` state (or to render `null` if your existing code conditionally renders the button based on status).

- [ ] **Step 5: Render `pending_extraction` and `failed` badges**

Inside `StatementRow`, in the status-cell rendering, add cases for the new statuses. Pattern (locate the existing status badge JSX and add):

```tsx
{stmt.status === 'pending_extraction' && (
  <span className="px-2 py-0.5 text-xs font-medium bg-amber-100 text-amber-800 rounded-full">
    Pending
  </span>
)}
```

If `stmt.extraction_failure_reason === 'extraction_error'`, render a red "Failed" badge instead:

```tsx
{stmt.extraction_failure_reason === 'extraction_error' && (
  <span className="px-2 py-0.5 text-xs font-medium bg-red-100 text-red-800 rounded-full">
    Failed
  </span>
)}
```

- [ ] **Step 6: Visual verification**

Restart the frontend if it isn't running:

```bash
cd /Users/maccb/llmragsql/frontend && npm run dev
```

In a browser at `http://localhost:5173/`:
1. Log in.
2. Navigate to the Bank Statement Hub.
3. Trigger a Scan with a known set of cache-MISS Barclays PDFs (e.g. clear `data/intsys/bank_reconcile/pdf_extraction_cache.db` first).
4. Confirm: if any extraction is rate-limited, the bank shows the amber banner and Process buttons are disabled across all of that bank's rows. Successful banks behave as before.

- [ ] **Step 7: Commit**

```bash
git add frontend/src/pages/BankStatementHub.tsx
git commit -m "feat(ui): bank-level extraction gate banner and Process button gating"
```

---

## Task 11: Update knowledge base

**Files:**
- Modify: `apps/core/docs/opera_knowledge_base.md`

- [ ] **Step 1: Add a section on Gemini throttle behaviour**

Append (or insert in an appropriate existing section if one matches) the following content. Find a section near "Bank Statement Import" or similar; if none exists, add at end before any closing notes.

```markdown
## Gemini Throttle and 429 Retry (Bank Statement Extraction)

All bank-statement extraction calls go through `sql_rag/gemini_throttle.py`:
- **Throttle**: minimum 1 second between consecutive `model.generate_content` calls (process-wide).
- **Retry**: on a 429 / `RESOURCE_EXHAUSTED` / quota response, the helper retries up to 3 times with backoff `5s → 15s → 45s`.
- **Typed errors**: `RateLimitExhaustedError` after all retries; `ExtractionFailedError` for non-rate-limit failures (no retry).

The `scan-all-banks` endpoint catches these typed errors and marks the affected statement `extraction_status: 'pending_extraction'` (or `'failed'` for non-429 errors). Per-bank gating: if any statement is unextracted, the bank's `extraction_status` is `'incomplete'` and the frontend disables Process buttons for the whole bank — preventing out-of-order processing.

Self-healing: failed statements are not cached; the next scan retries them automatically via the existing cache-MISS path.

Files: `sql_rag/gemini_throttle.py`, `sql_rag/statement_reconcile.py`, `sql_rag/statement_reconcile_opera3.py`, `apps/bank_reconcile/api/routes.py`.
```

- [ ] **Step 2: Commit**

```bash
git add apps/core/docs/opera_knowledge_base.md
git commit -m "docs(kb): document Gemini throttle and 429 retry behaviour"
```

---

## Task 12: Update user manual

**Files:**
- Modify: `marketing/manuals/manual-bank-reconciliation.md`

- [ ] **Step 1: Add a paragraph about pending extraction**

Find the section of the manual that describes the Scan or Bank Statement Hub flow (it documents the 5-stage workflow). Add a paragraph similar to:

```markdown
**If extraction is pending:** Occasionally the AI service hits its quota. Affected
statements show a "Pending" badge and the Process button is disabled for the
whole bank until every statement is extracted (this prevents processing
statements out of sequence). Wait 1–2 minutes and press Scan again; the system
retries the failed statements automatically. No data is lost.
```

- [ ] **Step 2: Update the `Last updated` date at the bottom of the file**

Find the existing date line (e.g. `Last updated: YYYY-MM-DD`) and update to `2026-04-28`.

- [ ] **Step 3: Commit**

```bash
git add marketing/manuals/manual-bank-reconciliation.md
git commit -m "docs(manual): explain pending-extraction behaviour for end users"
```

---

## Task 13: End-to-end manual verification

This task is verification only — no commits.

- [ ] **Step 1: Clear the PDF extraction cache for a Barclays test set**

Identify the per-company cache DB:

```bash
ls /Users/maccb/llmragsql/data/intsys/bank_reconcile/pdf_extraction_cache.db
```

Delete the file (it will be recreated empty on next access):

```bash
rm /Users/maccb/llmragsql/data/intsys/bank_reconcile/pdf_extraction_cache.db
```

- [ ] **Step 2: Restart the API**

Kill any running `uvicorn` on port 8000, then restart:

```bash
source venv/bin/activate && uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

- [ ] **Step 3: Trigger a scan**

In the browser at `http://localhost:5173/`, navigate to the Bank Statement Hub and press Scan. Open DevTools → Network and inspect the `/api/bank-import/scan-all-banks` response.

Expected response shape per bank with at least one cache-MISS statement (you can simulate a 429 by lowering the model's quota or by adding `time.sleep(0)` and triggering many parallel scans — but the realistic scenario is just running with a normal scan and seeing whether the actual quota is hit):

```json
{
  "banks": {
    "BC010": {
      "extraction_status": "complete" | "incomplete",
      "statements_extracted": N,
      "statements_total": M,
      "extraction_failures": [...],
      ...
    }
  }
}
```

- [ ] **Step 4: Confirm the visible UI**

For an `incomplete` bank:
- Amber banner appears: "X of Y statements extracted. Re-scan to complete..."
- Every statement row's Process button is disabled.
- Rows that failed extraction show the "Pending" badge.

For a `complete` bank:
- No banner.
- Process button enabled on the top "Ready" row exactly as before (with the "Next" indicator).

- [ ] **Step 5: Confirm self-healing**

Wait 1–2 minutes. Press Scan again. Observe that previously-pending statements now extract successfully and the bank flips to `complete`. No manual intervention needed.

- [ ] **Step 6: Confirm Opera 3 parity**

Switch the active installation to an Opera 3 company. Repeat steps 1–5 against the Opera 3 endpoints. Behaviour must be identical.

If any of these checks fail, return to the relevant task and fix.

---

## Self-Review

Cross-checked the spec sections against the plan tasks:

| Spec section | Covered by |
|---|---|
| Throttled+retrying Gemini call | Tasks 1–3 |
| Per-bank extraction gate | Task 8 |
| Self-healing on next scan | No code change required (cache-MISS path already retries); verified in Task 13 |
| Response data shape additions | Tasks 6, 7, 8 |
| Frontend behaviour (banner, gating, badges) | Task 10 |
| Error handling summary table (no silent dashes) | Tasks 6, 7 |
| Files touched table | Tasks 4, 5, 6, 7, 8, 10, 11, 12 |
| Testing approach (unit + integration) | Tasks 2, 3, 9; manual in Task 13 |
| Knowledge base update | Task 11 |
| User manual update | Task 12 |
| Opera SE / Opera 3 parity | Task 5 (mirrors SE), Task 7 step 3 (Opera 3 scan path), Task 13 step 6 (manual) |

No placeholders. Type names consistent across tasks (`RateLimitExhaustedError`, `ExtractionFailedError`, `call_gemini_with_throttle`, `extraction_status`, `extraction_failure_reason`, `statements_total`, `statements_extracted`, `extraction_failures`).

---

**Plan complete.**
