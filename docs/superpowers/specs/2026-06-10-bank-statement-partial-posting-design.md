# Bank Statement Partial Posting — Design

**Status:** Approved (verbally, 2026-06-10)
**Owner:** Harry Burdett
**Scope:** Two passes. This doc covers both.

## Problem

When a user imports a bank statement and not every transaction can post (period blocked, already posted from a previous run, etc.), today's UI reports the outcome in alarming red — even when nothing has actually failed. Two repeating scenarios:

1. **First import:** 6 purchase transactions, 3 in a blocked period. The system posts the 3 that can go, skips the 3 that can't, and shows a red error containing the skip reasons as concatenated semicolon text.
2. **Re-import after the period reopens:** the duplicate-check correctly catches the 3 already-posted rows and shows them as skipped — again with a red "Import Failed" header, when actually the outcome is correct and benign.

Both are mis-framed as errors. Worse, the operator has no persistent way to see which transactions are still outstanding after closing the browser.

## Principle

Partial posting is allowed and correct — there's no harm in posting what can be posted. But the system must:

1. **Distinguish three outcomes per row:** Posted / Held / Failed.
2. **Use the right tone for each:** green for posted, amber/neutral for held, red only for actual failures.
3. **Persist per-row state across sessions** so the operator can leave, return, and immediately see what's still outstanding.
4. **Make held items easy to retry** (one click) without re-running the whole import or re-extracting the PDF.

## Three states (drives both UI + persistence)

| State | When | Examples |
|---|---|---|
| **Posted** | Row wrote to Opera this run | Sales receipt posted to atran/stran/ntran |
| **Held** | Skipped for a legitimate reason — not a failure | Period blocked; already-posted (duplicate detected); operator ignored; unmatched |
| **Failed** | Genuine error — operator must act | DB error; validation error; schema mismatch |

## Result banner decision table

| Posted | Held | Failed | Banner |
|---|---|---|---|
| >0 | 0 | 0 | 🟢 "Imported N transactions" |
| 0 | >0 | 0 | 🟢 "All N transactions already on file — nothing to import" |
| >0 | >0 | 0 | 🟢 "Imported N, held M — see below" |
| 0 | 0 | 0 | 🟢 "Nothing to import" |
| any | any | >0 | 🔴 "Import failed — M transaction(s) couldn't post" |

Red **only** when something actually broke.

## Pass 1 — Messaging fix (this commit)

### Backend response shape

```jsonc
{
  "success": <bool — true when no failures>,
  "summary": "all_posted" | "all_already_posted" | "partial" | "nothing_to_import" | "failed",
  "counts": {
    "posted": N,
    "held": M,
    "failed": F,
    "total": T
  },
  "outcomes": [
    {
      "row": 1,
      "status": "posted" | "held" | "failed",
      "sub_status": "already_posted" | "period_blocked" | "unmatched" | "user_ignored" | "validation_error" | "db_error" | "...",
      "reason": "human-readable short text",
      "amount": -13.82,
      "date": "2026-05-31",
      "description": "...",
      "opera_entry_ref": "P100008306",  // when posted or already-posted
      "action": "purchase_payment"
    },
    ...
  ],
  // legacy fields preserved for backwards compat:
  "imported_count": N, "imported_transactions": [...], "errors": [...],
  ...
}
```

### Frontend banner

`BankStatementReconcile.tsx` reads `summary` + `counts` and renders the appropriate banner. The per-row table (already exists) gets a `status` column that uses the icon scheme.

### Opera 3 parity

Same change to `apps/bank_reconcile/api/routes.py:/api/opera3/bank-import/import-from-pdf` (line 13103) — Opera 3 endpoint MUST produce the same response shape. Frontend doesn't need to branch.

### Endpoints updated in Pass 1

- `POST /api/bank-import/import-from-pdf`
- `POST /api/bank-import/import-with-overrides`
- `POST /api/bank-import/import-from-email`
- `POST /api/opera3/bank-import/import-from-pdf`

All four populate `outcomes`, `counts`, `summary`. `success` reflects "no failures", not "any imported".

### What does NOT change in Pass 1

- No new tables. No schema migration. No retry-held endpoint. No Hub badge.
- The persistent per-row state is computed at response time from data the endpoint already has internally (`imported`, `already_posted`, `errors`, `skipped_*`). It just isn't structured for the UI yet — that's the fix.

## Pass 2 — Persistence + Retry (separate commit)

### Schema changes

```sql
ALTER TABLE bank_statement_imports
  ADD COLUMN posted_count INTEGER DEFAULT 0,
  ADD COLUMN held_count INTEGER DEFAULT 0,
  ADD COLUMN failed_count INTEGER DEFAULT 0;

CREATE TABLE bank_statement_import_rows (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  statement_import_id INTEGER NOT NULL,
  row_number INTEGER NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('posted','held','failed')),
  sub_status TEXT,
  reason TEXT,
  amount REAL,
  txn_date TEXT,
  description TEXT,
  action TEXT,
  opera_entry_ref TEXT,
  posted_at TEXT,
  FOREIGN KEY (statement_import_id) REFERENCES bank_statement_imports(id),
  UNIQUE (statement_import_id, row_number)
);
```

### Behaviour additions

- On every import (or retry), the endpoint upserts one row per transaction into `bank_statement_import_rows`. `bank_statement_imports.{posted,held,failed}_count` are recalculated.
- Bank Statement Hub list query joins to these counts; statement gets a status badge based on:
  - failed > 0 → 🔴 Failed
  - held > 0, posted ≥ 0 → 🟡 Partial
  - posted > 0, held = 0, failed = 0 → 🟢 Complete
  - all zero → ⚪ Not started
- New `POST /api/bank-import/retry-held/{statement_import_id}` endpoint reads the held rows for that statement and re-attempts only those — uses the same posting machinery, updates outcomes, recalculates counts.
- Bank Statement Reconcile page gets a "Retry held items" button when there are any held rows on the statement.
- Opera 3 parity: mirror endpoint + same DB table.

## Testing

- Unit tests for `_build_import_outcomes()` helper (Pass 1): given a synthetic `imported / already_posted / errors / skipped` arrays, produces correct outcomes + counts + summary.
- Integration test: import a statement where every row is a duplicate → expect `summary='all_already_posted'`, `success=True`, `counts.failed=0`.
- Pass 2 only: test that `retry-held` only re-attempts held rows, leaves posted rows untouched.

## KB updates (mandatory per CLAUDE.md)

Both passes update:

- `apps/core/docs/opera_knowledge_base.md` (local) — new "Bank Import Outcome States" section
- `~/opera-knowledge-ref/packages/opera-knowledge/business-rules/bank-import-outcomes.md` (central, new file)
- Same commit, central pushed.
