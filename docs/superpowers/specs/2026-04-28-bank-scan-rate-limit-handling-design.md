# Bank Statement Scan — Rate-Limit Handling — Design Spec

**Date**: 2026-04-28
**Status**: Approved (brainstorming complete, ready for implementation plan)

## Problem

When the bank statement scan (`/api/bank-import/scan-all-banks` and the Opera 3 equivalent) extracts opening and closing balances from a PDF for the first time, it calls Gemini. If Gemini returns `429 Resource exhausted`, the existing code logs a `WARNING` and continues — the statement is returned to the UI marked **Ready** with empty balance fields.

Real-world example (Barclays current account, 28 Apr 2026): three statements scanned in one request. Statement #2 was a cache HIT and showed correct balances. Statements #1 and #3 were cache MISSes; both hit the Gemini 429 quota and were silently returned with dashes for opening/closing balance, yet still shown as **Ready** to process.

This is critical because the bank statement reconciliation workflow depends on processing statements in sequence by opening balance. A statement with no opening balance cannot be ordered, and a "Ready" badge with no balances will trick the user into processing the wrong statement first.

The earlier hypothesis — that Barclays has a layout the AI can't read — is **wrong**. All three statements have identical layouts. The cached one extracts cleanly when re-tried. The fix is not a per-bank PDF hints library; the fix is to handle Gemini quota correctly and to gate the UI on full extraction success.

## Goal

When a bank statement PDF cannot be extracted because of a Gemini 429 rate limit, the system must:

1. Distinguish that from a successful extraction with empty balances.
2. Avoid burning the rest of the quota on the same scan.
3. Show the user that statements are *pending re-extraction*, not *ready to process*.
4. Auto-recover on the next scan without user intervention.

**Hard constraint**: every statement scanned must reach a verified opening + closing balance before the user is offered a "Process" button. Partial extraction (some statements with balances, others with dashes) makes the sequence undeterminable and is treated as a scan failure for that bank, not a user-visible mixed result.

## Out of Scope

- Per-bank PDF layout templates / hints library (proven not to be the cause).
- Switching AI providers.
- Paid-tier quota upgrade.
- Background worker process or new queue tables (the existing cache-MISS path on the next scan already provides retry; we don't need new infrastructure).

## Architecture

Three pieces, all in-process:

### 1. Throttled + retrying Gemini call

New helper `extract_with_throttle()` in `sql_rag/statement_reconcile.py`, mirrored in `sql_rag/statement_reconcile_opera3.py`.

**Throttle**:
- Process-level lock (`threading.Lock` for sync paths) ensures at most one Gemini call in flight at a time across the scan.
- Minimum interval 1 second between consecutive calls. Tracked via a module-level `last_call_time`. Sleep before the call if needed.

**Retry on 429**:
- Catch the Google API exception. Detect rate-limit by inspecting the message for any of: `429`, `Resource exhausted`, `quota`, `RESOURCE_EXHAUSTED`.
- Backoff schedule: 5s → 15s → 45s. Maximum 3 retries within the same request. Worst-case ~65s per PDF.
- Each retry logs `"429 retry N/3 after Xs for {filename}"`.

**Outcomes**:
- Success → return `(StatementInfo, transactions)` exactly as `extract_transactions_from_pdf` does today.
- All 3 retries failed with 429 → raise `RateLimitExhaustedError(filename)`.
- Non-429 extraction failure → raise `ExtractionFailedError(filename, reason)`.

The existing `extract_transactions_from_pdf` is updated to call this helper for the actual Gemini round-trip. Callers of `extract_transactions_from_pdf` in `apps/bank_reconcile/api/routes.py` catch the typed errors instead of swallowing a generic `Exception`.

### 2. Per-bank extraction gate

In the `scan-all-banks` endpoint, after the extraction loop completes, for each bank compute:

- `statements_total` — count of statements assigned to that bank.
- `statements_extracted` — count with non-null `opening_balance` AND `closing_balance`.
- `extraction_status` — `"complete"` if all extracted, else `"incomplete"`.
- `extraction_failures` — list of `{filename, reason}` for statements that failed.

If `extraction_status === "incomplete"`:
- Do NOT mark any of the bank's statements as `Ready`.
- Each unextracted statement gets `extraction_status: "pending_extraction"` (or `"failed"` for non-rate-limit errors).
- Each successfully-extracted statement keeps its existing status but is **not** offered for processing in the UI (gate is per-bank).

### 3. Self-healing on next scan

Failed statements are not added to the cache (`pdf_extraction_cache.db`). The existing cache-MISS path on the next scan picks them up automatically, runs `extract_with_throttle()` again, and — assuming quota has recovered — caches the result. No queue, no worker. The user simply presses Scan again.

## Data Shapes

### Response additions to `scan-all-banks`

Existing fields remain unchanged. Per bank:

```json
{
  "banks": {
    "BC010": {
      "...existing fields...": "...",
      "extraction_status": "complete | incomplete",
      "statements_extracted": 1,
      "statements_total": 3,
      "extraction_failures": [
        {"filename": "Statement 24-APR-26 ...pdf", "reason": "rate_limit"},
        {"filename": "Statement 17-APR-26 ...pdf", "reason": "rate_limit"}
      ]
    }
  }
}
```

Per statement:

```json
{
  "filename": "...",
  "extraction_status": "extracted | cached | pending_extraction | failed",
  "opening_balance": null,
  "closing_balance": null,
  "status": "pending_extraction"
}
```

`status` (the existing field) becomes `pending_extraction` for any unextracted statement in an incomplete bank. The UI uses this to disable the Process button.

## Frontend Behaviour

- **Bank header banner** (when `extraction_status === "incomplete"`):
  Amber banner under the bank summary:
  > "{statements_extracted} of {statements_total} statements extracted. Re-scan to complete."

- **Process button** disabled for every statement of an incomplete bank, regardless of which individual statements have balances.

- **Per-statement badge**: rows with `extraction_status: pending_extraction` show an amber "Pending" badge instead of "Ready". Rows with `failed` show red "Failed".

- **Scan button**: when at least one bank is `incomplete` after a scan, the Scan button copy switches to "Re-scan to complete extraction" until everything is `complete`.

- When `extraction_status === "complete"` for a bank, behaviour is identical to today: Process buttons enabled, "Next" sequence badge shown on the top Ready statement.

## Error Handling Summary

| Scenario | What happens | What user sees |
|---|---|---|
| Cache HIT | Return cached info, no Gemini call | Balances shown, Ready (subject to bank gate) |
| Cache MISS, success on first try | Cached, return result | Balances shown, Ready (subject to bank gate) |
| Cache MISS, 429 then success on retry | Cached, return result | Balances shown, Ready (subject to bank gate) |
| Cache MISS, 3× 429 | Statement `pending_extraction`, bank `incomplete` | "Pending" badge; banner on bank; Process disabled |
| Cache MISS, non-429 extraction error | Statement `failed`, bank `incomplete` | "Failed" badge; banner on bank; Process disabled |

No path silently returns a `Ready` statement with null balances. The previous warning-and-continue at `apps/bank_reconcile/api/routes.py:6332` (and the equivalent folder-match path) is removed.

## Files Touched

| File | Change |
|---|---|
| `sql_rag/statement_reconcile.py` | Add `extract_with_throttle()` and typed errors. Wrap the Gemini call inside `extract_transactions_from_pdf` with the helper. |
| `sql_rag/statement_reconcile_opera3.py` | Mirror. |
| `apps/bank_reconcile/api/routes.py` | In `scan-all-banks` (line 5823+) and Opera 3 mirror at `/api/opera3/bank-import/scan-emails`: catch `RateLimitExhaustedError` / `ExtractionFailedError`, set per-statement `extraction_status` and per-bank gate fields. Remove silent `WARNING` swallows. |
| `frontend/src/pages/BankStatementHub.tsx` | Render per-bank banner, gate Process buttons, show per-statement Pending/Failed badges, change Scan button copy. |
| `marketing/manuals/manual-bank-reconciliation.md` | Add sentence about quota handling and re-scan behaviour. Update "Last updated" date. |
| `apps/core/docs/opera_knowledge_base.md` | Note that scan throttles Gemini at 1s minimum and retries 429s up to 3 times. |

## Testing

- **Unit**: stub `model.generate_content` to raise an exception with `"429 Resource exhausted"`; assert `extract_with_throttle` retries 3 times at 5/15/45s and finally raises `RateLimitExhaustedError`.
- **Unit**: two back-to-back calls measured ≥1s apart (throttle).
- **Unit**: gate computation — bank with 3 statements, 1 cache HIT and 2 `pending_extraction` → bank `extraction_status: incomplete`, no statements marked Ready.
- **Integration** (gated by `RUN_GEMINI_TESTS=1`): clear cache for the 3 Barclays Apr 2026 statements in `~/Downloads/bank-statements/BC010-barclays-bank-current-a-c/`, run scan; assert response is either all 3 extracted, or all 3 marked `pending_extraction` with the bank gated. Never mixed.
- **Manual**: reproduce the 28 Apr screenshot scenario (cache cleared, scan triggered) and confirm UI shows the banner and disabled buttons; press Scan again and confirm the bank flips to `complete`.

## Success Criteria

1. The scenario in the 28 Apr 2026 screenshot — bank with 1 of 3 statements showing balances and 2 with dashes, all marked Ready — is no longer reachable through any code path.
2. A bank with any unextracted statement is gated: no Process button can be pressed for it.
3. 429s are retried 3× with exponential backoff before being reported as `pending_extraction`.
4. The user is told (via banner) when re-scanning will help, and confirms by pressing Scan.
5. Behaviour identical for Opera SE and Opera 3.
6. No new background workers, queues, or DB tables introduced.
