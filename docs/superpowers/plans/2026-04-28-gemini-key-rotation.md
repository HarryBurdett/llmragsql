# Gemini API Key Rotation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Add automatic rotation across multiple Gemini API keys when one is rate-limit-exhausted. `config.ini` accepts numbered keys (`api_key`, `api_key_2`, `api_key_3`, …); the throttle helper rotates through them on 429-exhaustion. After all keys are exhausted, the existing `RateLimitExhaustedError` is raised so the per-bank gate / banner / button-gating shipped earlier today keeps working unchanged.

**Architecture:** All changes localised to `sql_rag/gemini_throttle.py` (rotation logic), `sql_rag/statement_reconcile.py` and `sql_rag/statement_reconcile_opera3.py` (load keys from config). No new modules, no new locks, no new DB tables. Backwards compatible — single-key configs behave byte-identically to today.

**Tech Stack:** Python 3.10+, `google.generativeai` SDK, pytest, configparser.

**Spec:** `docs/superpowers/specs/2026-04-28-gemini-key-rotation-design.md`

---

## File Structure

| File | Role | Status |
|---|---|---|
| `sql_rag/gemini_throttle.py` | Add module-level key list + exhaustion map; add `configure_keys()`; refactor `call_gemini_with_throttle` into outer-per-key + inner-per-attempt loop | MODIFY |
| `tests/test_gemini_throttle.py` | Add rotation tests (single-key behaviour unchanged + multi-key scenarios) | MODIFY |
| `sql_rag/statement_reconcile.py` | Load `api_key_2`, `api_key_3`, … from config; call `configure_keys()` | MODIFY |
| `sql_rag/statement_reconcile_opera3.py` | Same | MODIFY |
| `apps/core/docs/opera_knowledge_base.md` | Document rotation behaviour | MODIFY |

---

## Task 1: Add module state and `configure_keys()` helper

**Files:**
- Modify: `sql_rag/gemini_throttle.py`
- Modify: `tests/test_gemini_throttle.py`

This task adds plumbing only — `call_gemini_with_throttle` is not yet aware of the key list. Behaviour does not change.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_gemini_throttle.py`:

```python
from sql_rag.gemini_throttle import configure_keys, _get_active_keys_for_testing


def test_configure_keys_stores_provided_list():
    configure_keys(["k1", "k2", "k3"])
    assert _get_active_keys_for_testing() == ["k1", "k2", "k3"]


def test_configure_keys_strips_empty_and_none():
    configure_keys(["k1", "", None, "k2", "  "])
    assert _get_active_keys_for_testing() == ["k1", "k2"]


def test_configure_keys_with_empty_list_resets():
    configure_keys(["k1"])
    configure_keys([])
    assert _get_active_keys_for_testing() == []


def test_reset_clears_keys():
    configure_keys(["k1", "k2"])
    _reset_throttle_state_for_testing()
    assert _get_active_keys_for_testing() == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `source venv/bin/activate && pytest tests/test_gemini_throttle.py -v`
Expected: FAIL with `ImportError: cannot import name 'configure_keys'` or `'_get_active_keys_for_testing'`.

- [ ] **Step 3: Add module state and helper**

Insert into `sql_rag/gemini_throttle.py` near the existing `_lock`, `_last_call_time` declarations (around line 70-76):

```python
# --- Multi-key rotation state (module-level) ---

_keys: list[str] = []
_exhausted_until: dict[int, float] = {}
_EXHAUSTION_DURATION_SECONDS = 1800.0  # 30 minutes


def configure_keys(keys: list[str | None]) -> None:
    """Configure the list of Gemini API keys to use for rotation.

    Empty strings and None entries are silently dropped. Each call replaces
    the previous list. Pass an empty list to disable rotation entirely
    (the helper then falls back to whatever key the caller's `model` already
    has configured globally — i.e. today's behaviour).
    """
    global _keys, _exhausted_until
    cleaned = [k.strip() for k in keys if k and isinstance(k, str) and k.strip()]
    with _lock:
        _keys = cleaned
        _exhausted_until = {}
    logger.info("Gemini throttle configured with %d key(s)", len(cleaned))


def _get_active_keys_for_testing() -> list[str]:
    """Test-only accessor for the configured key list."""
    with _lock:
        return list(_keys)
```

Update `_reset_throttle_state_for_testing()` to also clear keys:

```python
def _reset_throttle_state_for_testing() -> None:
    """Reset module-level throttle state. Test-only helper."""
    global _last_call_time, _keys, _exhausted_until
    with _lock:
        _last_call_time = 0.0
        _keys = []
        _exhausted_until = {}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `source venv/bin/activate && pytest tests/test_gemini_throttle.py -v`
Expected: all tests pass (existing 10 + new 4 = 14 passed).

- [ ] **Step 5: Commit**

```bash
git add sql_rag/gemini_throttle.py tests/test_gemini_throttle.py
git commit -m "feat(gemini): add module state and configure_keys() helper"
```

---

## Task 2: Add `_select_active_key_idx()` and `_mark_key_exhausted()` helpers

**Files:**
- Modify: `sql_rag/gemini_throttle.py`
- Modify: `tests/test_gemini_throttle.py`

Helpers used by the rotation loop. No call_gemini_with_throttle changes yet.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_gemini_throttle.py`:

```python
from sql_rag.gemini_throttle import _select_active_key_idx, _mark_key_exhausted


def test_select_active_returns_none_when_no_keys():
    configure_keys([])
    assert _select_active_key_idx() is None


def test_select_active_returns_first_when_all_eligible():
    configure_keys(["k1", "k2", "k3"])
    assert _select_active_key_idx() == 0


def test_select_active_skips_exhausted(monkeypatch):
    monkeypatch.setattr("sql_rag.gemini_throttle.time.monotonic", lambda: 1000.0)
    configure_keys(["k1", "k2", "k3"])
    _mark_key_exhausted(0)
    assert _select_active_key_idx() == 1


def test_select_active_returns_none_when_all_exhausted(monkeypatch):
    monkeypatch.setattr("sql_rag.gemini_throttle.time.monotonic", lambda: 1000.0)
    configure_keys(["k1", "k2"])
    _mark_key_exhausted(0)
    _mark_key_exhausted(1)
    assert _select_active_key_idx() is None


def test_exhausted_key_recovers_after_cooldown(monkeypatch):
    fake_now = [1000.0]
    monkeypatch.setattr("sql_rag.gemini_throttle.time.monotonic", lambda: fake_now[0])
    configure_keys(["k1", "k2"])
    _mark_key_exhausted(0)
    assert _select_active_key_idx() == 1

    # Advance past 30-minute cooldown
    fake_now[0] += 1800.0 + 1.0
    assert _select_active_key_idx() == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `source venv/bin/activate && pytest tests/test_gemini_throttle.py -v`
Expected: FAIL with ImportError on the new names.

- [ ] **Step 3: Add the helpers**

Append to `sql_rag/gemini_throttle.py` after `configure_keys()`:

```python
def _select_active_key_idx() -> int | None:
    """Return the index of the lowest-indexed key not currently exhausted.

    Returns None if no keys are configured, or if every configured key
    is currently inside its exhaustion cooldown window.

    Caller is expected to hold (or acquire) `_lock` if mutating state
    after this call. The function itself acquires the lock to read state.
    """
    with _lock:
        if not _keys:
            return None
        now = time.monotonic()
        for idx in range(len(_keys)):
            until = _exhausted_until.get(idx, 0.0)
            if until <= now:
                return idx
        return None


def _mark_key_exhausted(idx: int) -> None:
    """Flag a key as exhausted for the next _EXHAUSTION_DURATION_SECONDS."""
    with _lock:
        _exhausted_until[idx] = time.monotonic() + _EXHAUSTION_DURATION_SECONDS
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `source venv/bin/activate && pytest tests/test_gemini_throttle.py -v`
Expected: 19 passed.

- [ ] **Step 5: Commit**

```bash
git add sql_rag/gemini_throttle.py tests/test_gemini_throttle.py
git commit -m "feat(gemini): add active-key selection and exhaustion-marking helpers"
```

---

## Task 3: Refactor `call_gemini_with_throttle` to rotate keys

**Files:**
- Modify: `sql_rag/gemini_throttle.py`
- Modify: `tests/test_gemini_throttle.py`

This is the central change. The existing single-key behaviour must remain byte-identical when `configure_keys()` has not been called (or has been called with `[]`).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_gemini_throttle.py`:

```python
import google.generativeai as genai


def test_no_keys_configured_uses_existing_model_key(monkeypatch):
    """Backwards-compat: when configure_keys() never called, no genai.configure swap."""
    monkeypatch.setattr("sql_rag.gemini_throttle.time.sleep", lambda s: None)
    configure_calls: list[str] = []
    monkeypatch.setattr(
        "sql_rag.gemini_throttle.genai.configure",
        lambda **kw: configure_calls.append(kw.get("api_key", "")),
    )
    model = MagicMock()
    model.generate_content.return_value = MagicMock(name="resp")

    result = call_gemini_with_throttle(model, ["p"])

    assert result is not None
    # No keys configured → no genai.configure swap should occur
    assert configure_calls == []


def test_rotates_to_second_key_when_first_exhausted(monkeypatch):
    monkeypatch.setattr("sql_rag.gemini_throttle.time.sleep", lambda s: None)
    configure_keys(["k1", "k2"])
    configure_calls: list[str] = []
    monkeypatch.setattr(
        "sql_rag.gemini_throttle.genai.configure",
        lambda **kw: configure_calls.append(kw.get("api_key", "")),
    )

    model = MagicMock()
    success_response = MagicMock(name="resp")
    # k1: 4 attempts (initial + 3 retries) all 429
    # k2: 1 attempt succeeds
    model.generate_content.side_effect = [
        Exception("429 Resource exhausted"),
        Exception("429 Resource exhausted"),
        Exception("429 Resource exhausted"),
        Exception("429 Resource exhausted"),
        success_response,
    ]

    result = call_gemini_with_throttle(model, ["p"], filename="test.pdf")

    assert result is success_response
    assert model.generate_content.call_count == 5
    # genai.configure called twice — once for k1, once when rotating to k2
    assert configure_calls == ["k1", "k2"]


def test_raises_when_all_keys_exhausted(monkeypatch):
    monkeypatch.setattr("sql_rag.gemini_throttle.time.sleep", lambda s: None)
    configure_keys(["k1", "k2"])
    monkeypatch.setattr("sql_rag.gemini_throttle.genai.configure", lambda **kw: None)

    model = MagicMock()
    model.generate_content.side_effect = Exception("429 Resource exhausted")

    with pytest.raises(RateLimitExhaustedError) as exc_info:
        call_gemini_with_throttle(model, ["p"], filename="bad.pdf")

    # 4 attempts on k1 + 4 attempts on k2 = 8 total
    assert model.generate_content.call_count == 8
    assert "bad.pdf" in str(exc_info.value)


def test_non_rate_limit_error_does_not_rotate(monkeypatch):
    monkeypatch.setattr("sql_rag.gemini_throttle.time.sleep", lambda s: None)
    configure_keys(["k1", "k2"])
    monkeypatch.setattr("sql_rag.gemini_throttle.genai.configure", lambda **kw: None)

    model = MagicMock()
    model.generate_content.side_effect = ValueError("Could not parse response")

    with pytest.raises(ExtractionFailedError):
        call_gemini_with_throttle(model, ["p"], filename="bad.pdf")

    # Only one attempt on k1 — no retry, no rotation
    assert model.generate_content.call_count == 1


def test_first_key_success_no_rotation(monkeypatch):
    monkeypatch.setattr("sql_rag.gemini_throttle.time.sleep", lambda s: None)
    configure_keys(["k1", "k2", "k3"])
    configure_calls: list[str] = []
    monkeypatch.setattr(
        "sql_rag.gemini_throttle.genai.configure",
        lambda **kw: configure_calls.append(kw.get("api_key", "")),
    )

    model = MagicMock()
    model.generate_content.return_value = MagicMock(name="resp")

    result = call_gemini_with_throttle(model, ["p"])

    assert result is not None
    assert model.generate_content.call_count == 1
    assert configure_calls == ["k1"]
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `source venv/bin/activate && pytest tests/test_gemini_throttle.py -v`
Expected: the new tests fail because `call_gemini_with_throttle` does not yet handle the keys list.

- [ ] **Step 3: Refactor `call_gemini_with_throttle`**

The current single-loop implementation is replaced by an outer per-key loop containing the existing inner attempt-with-backoff logic. The inner logic is extracted into a private `_attempt_with_backoff()` helper.

Replace the existing `call_gemini_with_throttle` function in `sql_rag/gemini_throttle.py` with the following two functions:

```python
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
    rotation_count = 0

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
            rotation_count += 1
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
```

Note that the import for `genai` is needed at the top of the file. Add (alongside the other imports near the top):

```python
import google.generativeai as genai
```

- [ ] **Step 4: Run all tests**

Run: `source venv/bin/activate && pytest tests/test_gemini_throttle.py -v`
Expected: 24 passed (10 original + 4 from Task 1 + 5 from Task 2 + 5 from this task).

If any of the original 10 tests fail, the refactor has broken backwards compatibility. Investigate before proceeding.

Run the full suite to check no regressions:

```bash
source venv/bin/activate && pytest tests/ -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add sql_rag/gemini_throttle.py tests/test_gemini_throttle.py
git commit -m "feat(gemini): rotate to next key on 429-exhaustion of active key"
```

---

## Task 4: Load multiple keys in `StatementReconciler` (Opera SE)

**Files:**
- Modify: `sql_rag/statement_reconcile.py`

- [ ] **Step 1: Add the `configure_keys` import**

The file already imports `from sql_rag.gemini_throttle import (call_gemini_with_throttle, RateLimitExhaustedError, ExtractionFailedError)`. Extend that import to also include `configure_keys`:

```python
from sql_rag.gemini_throttle import (
    call_gemini_with_throttle,
    RateLimitExhaustedError,
    ExtractionFailedError,
    configure_keys,
)
```

- [ ] **Step 2: Load numbered keys from config and call `configure_keys`**

In `StatementReconciler.__init__`, find the existing API-key loading block. The current code loads a single key:

```python
        # Get API key: parameter > config > environment
        api_key = gemini_api_key
        if not api_key and config.has_section('gemini'):
            api_key = config.get('gemini', 'api_key', fallback='')
        if not api_key:
            api_key = os.environ.get('GEMINI_API_KEY', '')
```

Immediately after this block (and before the `if not api_key:` validation), add the numbered-key loading:

```python
        # Collect numbered keys (api_key_2, api_key_3, ...) for rotation.
        # The bare api_key above is always Key #1 in the rotation list.
        rotation_keys: list[str] = [api_key] if api_key else []
        if config.has_section('gemini'):
            n = 2
            while True:
                k = config.get('gemini', f'api_key_{n}', fallback='').strip()
                if not k:
                    break
                rotation_keys.append(k)
                n += 1

        # Configure throttle helper with the full key list. Rotation activates
        # automatically when more than one key is provided. With a single key,
        # behaviour is identical to before this change.
        configure_keys(rotation_keys)
        if len(rotation_keys) > 1:
            logger.info(
                "Gemini key rotation enabled with %d keys", len(rotation_keys)
            )
```

- [ ] **Step 3: Smoke-test the import**

Run: `source venv/bin/activate && python -c "from sql_rag.statement_reconcile import StatementReconciler; print('ok')"`
Expected: prints `ok`.

- [ ] **Step 4: Re-run all tests**

```bash
source venv/bin/activate && pytest tests/ -v
```

Expected: all tests still pass.

- [ ] **Step 5: Commit**

```bash
git add sql_rag/statement_reconcile.py
git commit -m "feat(reconcile-se): load numbered Gemini keys for rotation"
```

---

## Task 5: Same for `StatementReconcilerOpera3`

**Files:**
- Modify: `sql_rag/statement_reconcile_opera3.py`

- [ ] **Step 1: Add `configure_keys` to the import**

Find the existing import:

```python
from sql_rag.gemini_throttle import (
    call_gemini_with_throttle,
    RateLimitExhaustedError,
    ExtractionFailedError,
)
```

Extend:

```python
from sql_rag.gemini_throttle import (
    call_gemini_with_throttle,
    RateLimitExhaustedError,
    ExtractionFailedError,
    configure_keys,
)
```

- [ ] **Step 2: Replicate the numbered-key loading**

In `StatementReconcilerOpera3.__init__`, find the API-key loading block (mirror of the SE version) and add the same numbered-key loading + `configure_keys()` call as in Task 4 Step 2. Use the same code block verbatim — the file structure matches.

- [ ] **Step 3: Smoke-test the import**

Run: `source venv/bin/activate && python -c "from sql_rag.statement_reconcile_opera3 import StatementReconcilerOpera3; print('ok')"`
Expected: prints `ok`.

- [ ] **Step 4: Re-run all tests**

```bash
source venv/bin/activate && pytest tests/ -v
```

Expected: all tests still pass.

- [ ] **Step 5: Commit**

```bash
git add sql_rag/statement_reconcile_opera3.py
git commit -m "feat(reconcile-o3): load numbered Gemini keys for rotation"
```

---

## Task 6: Update knowledge base

**Files:**
- Modify: `apps/core/docs/opera_knowledge_base.md`

- [ ] **Step 1: Append a sub-section under the existing "Gemini Throttle and 429 Retry" section**

Below the existing throttle/retry section (added in commit `2909f3d` earlier today), insert the following block:

```markdown
### Gemini Multi-Key Rotation

`config.ini` `[gemini]` section accepts numbered keys for automatic rotation when one key is rate-limited:

```ini
[gemini]
api_key = AIzaSy...key1
api_key_2 = AIzaSy...key2
api_key_3 = AIzaSy...key3
model = gemini-2.0-flash
```

The bare `api_key` is Key #1. Numbered keys are loaded in order until a missing entry is encountered. Empty values are silently skipped.

**Behaviour:** When the active key returns 429 across all 3 inner retries, it is flagged exhausted (30-minute cooldown) and the next eligible key is configured via `genai.configure()` and tried. Rotation continues until a key succeeds or every key is exhausted. When all keys are exhausted, the same `RateLimitExhaustedError` is raised as in the single-key case — so the per-bank gate / banner / button-gating reuse identical paths.

**Recovery:** After 30 minutes, an exhausted key is retried automatically. No operator intervention is required.

**Backwards compatibility:** With a single key configured (the existing pattern), behaviour is byte-identical to before rotation was added — no `genai.configure` swaps occur on every call.

**Logs to watch for in the API output:**

- `Gemini throttle configured with N key(s)` — at process start
- `Gemini key rotation enabled with N keys` — at reconciler init when N > 1
- `Gemini key X/N rate-limit exhausted; rotating to key Y/N for {filename}` — every rotation
- `All N Gemini keys rate-limited; raising RateLimitExhaustedError for {filename}` — full-stop case

Files: `sql_rag/gemini_throttle.py`, `sql_rag/statement_reconcile.py`, `sql_rag/statement_reconcile_opera3.py`.
```

- [ ] **Step 2: Commit**

```bash
git add apps/core/docs/opera_knowledge_base.md
git commit -m "docs(kb): document Gemini multi-key rotation"
```

---

## Task 7: End-to-end manual verification

This task is verification only — no commits.

- [ ] **Step 1: Add a second key to `config.ini`**

Edit your local `config.ini` `[gemini]` section. To verify rotation logging works without affecting normal operation, add a placeholder Key #2 that you know will fail (e.g. an obviously bad string), then a real Key #3:

```ini
[gemini]
api_key = <your existing real key>
api_key_2 = INVALID_KEY_FOR_ROTATION_TEST
api_key_3 = <a second real key, ideally with quota available>
model = gemini-2.0-flash
```

Or: if you have two real keys with quota available, configure them both and skip the rotation-log testing — the rotation will only fire when Key 1 actually exhausts.

- [ ] **Step 2: Restart the API**

```bash
lsof -ti:8000 | xargs kill 2>/dev/null
source venv/bin/activate && uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

In the startup logs, look for:

```
Gemini throttle configured with N key(s)
Gemini key rotation enabled with N keys
```

- [ ] **Step 3: Trigger a scan that requires a cache MISS**

Either delete the local PDF cache or pick a previously-uncached statement. Trigger a Bank Statement Hub scan in the browser.

- [ ] **Step 4: Observe rotation in the API log**

If Key 1 exhausts mid-scan, you should see:

```
Gemini key 1/3 rate-limit exhausted; rotating to key 2/3 for <filename>
```

If Key 2 is also bad/exhausted, the next rotation should log to key 3/3, and so on. If all three exhaust, the final log should be:

```
All 3 Gemini keys rate-limited; raising RateLimitExhaustedError for <filename>
```

The frontend should render the same amber banner + disabled Process buttons exactly as today.

- [ ] **Step 5: Confirm cooldown recovery**

Mark a key exhausted (by triggering a rotation), wait 30 minutes, trigger another scan. The previously-exhausted key should be tried again. If its quota window has reset, the call succeeds and the key returns to active rotation; if not, another rotation fires. Either way: no manual intervention.

If any of these steps fail, return to the relevant earlier task and fix.

---

## Self-Review

| Spec section | Covered by |
|---|---|
| Goal: multi-key rotation transparently to callers | Task 3 (helper) + Tasks 4–5 (callers load keys) |
| Config syntax: numbered keys, single-key backwards compat | Tasks 4 + 5 (config loading); Task 3 (single-key path) |
| Behaviour: outer per-key + inner per-attempt | Task 3 |
| 30-minute cooldown recovery | Task 2 (`_select_active_key_idx`) + Task 3 (recovery test) |
| Strict serial concurrency | Task 3 (uses existing `_lock`) |
| Logging | Task 3 (rotation/all-exhausted logs); Tasks 4–5 (init logs) |
| Backwards compatibility (single-key path) | Task 1 (configure_keys with `[]` resets); Task 3 (single-key fast path) |
| Opera SE / Opera 3 parity | Task 4 (SE) + Task 5 (Opera 3) |
| Docs (knowledge base) | Task 6 |
| Manual verification | Task 7 |

No placeholders. Type names consistent across tasks (`configure_keys`, `_select_active_key_idx`, `_mark_key_exhausted`, `_EXHAUSTION_DURATION_SECONDS`, `_attempt_with_backoff`).

---

**Plan complete.**
