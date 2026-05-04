# Gemini API Key Rotation — Design Spec

**Date**: 2026-04-28
**Status**: Approved (defaults accepted, ready for implementation plan)

## Problem

The bank statement scan calls Gemini (`gemini-2.0-flash`) for every cache-MISS PDF. Free-tier keys are capped at 1,500 requests/day. On a busy day with multiple banks and re-scans, the cap is hit and every subsequent scan fails with 429 — even after the throttle/retry work shipped earlier today, because per-call retries don't reset a daily quota window.

Today's commercial workaround is to either pay for billing on the Gemini key (immediate fix, costs money) or wait until the daily window resets. Neither is good for a commercial product running production scans for end users.

## Goal

Support **multiple Gemini API keys** in configuration. The system uses Key 1 until it returns rate-limit errors, then automatically rotates to Key 2 (and 3, 4, …). Effective daily quota multiplies by the number of configured keys. Rotation is transparent to callers — `StatementReconciler` and `StatementReconcilerOpera3` continue to call `call_gemini_with_throttle()` exactly as today.

## Out of Scope

- Per-company key isolation (the keys belong to the operator, not the company).
- Persisting key-exhaustion state across API restarts (in-memory is sufficient — a restart re-tries every key fresh, which is acceptable since the worst case is one wasted call to a still-exhausted key before rotating).
- Switching AI providers (Claude, OpenAI). Different feature.
- A dashboard/UI for key health. Operators inspect the API log when needed.
- Parallel use of multiple keys for throughput. Strict serial — preserves the existing 1-call-in-flight invariant of the throttle.

## Configuration

`config.ini` `[gemini]` section accepts numbered keys. The bare `api_key` form remains valid for backwards compatibility:

```ini
[gemini]
# Single key (existing form — still works)
api_key = AIzaSy...key1
model = gemini-2.0-flash
```

```ini
[gemini]
# Multiple keys (new form — rotation enabled)
api_key = AIzaSy...key1
api_key_2 = AIzaSy...key2
api_key_3 = AIzaSy...key3
model = gemini-2.0-flash
```

Numbered keys (`api_key_2`, `api_key_3`, …) are loaded in order. Empty values are skipped silently. The bare `api_key` is always Key #1. There is no upper limit on the number of keys.

If only one key is configured, behaviour is identical to today (no rotation, no key-manager state changes).

## Behaviour

### At Process Start

Both `StatementReconciler.__init__` and `StatementReconcilerOpera3.__init__` already read `[gemini].api_key` from config. Add: also read all numbered variants (`api_key_2`, …) and call a new helper `gemini_throttle.configure_keys(keys: list[str])` with the full list. The first key is also passed to `genai.configure(api_key=...)` as today (preserving the initial state).

### During a Call

`call_gemini_with_throttle()` becomes a two-level loop:

- **Outer loop — per key** (in numbered order, skipping any currently flagged exhausted):
  1. `genai.configure(api_key=current_key)` (under the existing throttle lock).
  2. Run the existing inner attempt-with-backoff (initial + 3 retries at 5s / 15s / 45s).
  3. If inner loop raises `RateLimitExhaustedError`: mark this key exhausted (with a timestamp), log the rotation, continue to the next key.
  4. If inner loop raises `ExtractionFailedError` (non-rate-limit): rotation does not help, re-raise immediately.
  5. If inner loop succeeds: return the response.

- **All keys exhausted**: raise `RateLimitExhaustedError(filename=..., last_error="all N keys rate-limited")`. This is the same exception type as today, so existing route-level error handling (per-bank gate, banner, button gating) keeps working unchanged.

### Exhaustion Recovery

A key marked exhausted at time `T` is considered eligible again at `T + 30 minutes`. The throttle's outer loop checks `time.monotonic() >= exhausted_until[key_idx]` when iterating. This handles transient per-minute hiccups automatically — operator never has to clear state. After a full daily-quota burn, the key stays "ineligible" for 30 minutes at a time and is retried; the first attempt after recovery will fail again (one wasted call) but successive attempts succeed once the quota window resets.

### Logging

Every rotation logs at WARNING level:

```
Gemini key 1/3 rate-limit exhausted; rotating to key 2/3 for {filename}
```

Every recovery (when an exhausted key is tried again after the 30-minute window) logs at INFO level:

```
Gemini key 2/3 eligible again after 30-minute cooldown
```

When all keys are exhausted, the existing "Gemini rate limit exhausted after retries" warning is replaced with:

```
All 3 Gemini keys rate-limited; raising RateLimitExhaustedError for {filename}
```

These logs are operator-facing only — they don't surface to the end user beyond the existing per-bank banner.

## Concurrency Model

Strict serial. The existing module-level `_lock` in `gemini_throttle.py` already serialises all Gemini calls process-wide. Rotation reuses the same lock — no new lock primitives. This preserves the spec invariant from the previous round: at most one Gemini call in flight at a time.

The "active key index" and "exhausted-until" map are module-level and protected by the same lock when read or written.

## Data Shapes

No changes to public API responses. No new fields in the scan-all-banks response. No new fields on bank or statement entries. The rotation is purely an internal operational improvement — the user-visible behaviour matches what we shipped today, just rarer.

## Files Touched

| File | Change |
|---|---|
| `sql_rag/gemini_throttle.py` | Add module-level key list + exhaustion map. Add `configure_keys(keys)` helper. Refactor `call_gemini_with_throttle` into outer per-key loop + inner per-attempt loop. |
| `sql_rag/statement_reconcile.py` | In `__init__`, read `api_key_2`, `api_key_3`, … from config; call `configure_keys()` with the full list. Initial `genai.configure` continues to use Key #1 for backwards compatibility. |
| `sql_rag/statement_reconcile_opera3.py` | Mirror. |
| `tests/test_gemini_throttle.py` | New tests: rotates on first key 429-exhaustion; surfaces ExtractionFailedError without rotating; raises after all keys exhausted; recovers after 30 minutes; respects single-key config (no behaviour change). |
| `apps/core/docs/opera_knowledge_base.md` | Document multi-key config and rotation behaviour. |
| `config.ini` | NOT modified in repo. Operator updates their own copy. |

## Testing

Unit tests in `tests/test_gemini_throttle.py`. All tests use `monkeypatch.setattr` on `time.sleep` and `time.monotonic` to keep them fast. None hit real Gemini.

- **Single-key behaviour unchanged**: configure 1 key, exhaust it on 3 retries → `RateLimitExhaustedError` (same as today).
- **Two-key rotation on success**: configure 2 keys; Key 1 returns 4× 429 then Key 2 returns success. Result: success, `model.generate_content` called 5 times, log shows rotation.
- **Two-key rotation when both exhausted**: configure 2 keys; both exhaust → `RateLimitExhaustedError` with message naming both keys.
- **Non-rate-limit error doesn't rotate**: configure 2 keys; Key 1 raises `ValueError` once → `ExtractionFailedError`. Key 2 never tried.
- **Recovery after cooldown**: configure 1 key; mark it exhausted; advance fake clock by 30 min; subsequent call attempts the key again.
- **Empty / missing numbered keys are skipped**: `configure_keys(["key1", "", None, "key2"])` results in 2 active keys.

Manual verification (Task 13 of the implementation plan):

- Add a fake "key2" to config (e.g. duplicate the working one or paste a known-bad key for the rotation log to fire), restart API, trigger a scan that hits 429, observe the API log for the rotation message.

## Success Criteria

1. With a single key configured, behaviour is **byte-identical** to the throttle work shipped earlier today (same retries, same backoff, same exception, same logs).
2. With two or more keys configured, a 429-exhausted key triggers automatic rotation to the next configured key without operator intervention.
3. After 30 minutes, an exhausted key is retried; if its quota window has reset (free-tier daily), the call succeeds and the key is restored to active rotation.
4. Existing route-level handling (per-bank gate, banner, Process button gating) continues to work unchanged — same `RateLimitExhaustedError` is raised when all keys are exhausted.
5. Both Opera SE and Opera 3 reconcilers benefit transparently — no per-data-source code branching.
