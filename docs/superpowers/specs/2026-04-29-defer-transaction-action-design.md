# Defer Transaction Action — Design Spec

**Date**: 2026-04-29
**Status**: Approved (defaults accepted, ready for implementation plan)

## Problem

In the Bank Statement Reconciliation workflow, Stage 3 (Import) requires every unmatched statement row to have an action assigned (sales receipt, supplier payment, nominal, etc.) or to be marked Ignore. There is no way to say "this transaction is real and will be posted to Opera manually outside this workflow — let me proceed with the rest". The user is forced either to assign details and post the row themselves, which they don't want to do, or to use Ignore — which adds the row to a permanent ignore-list table and excludes it from all future reconciliations.

Real workflow: a colleague enters certain transactions into Opera manually outside the bank-import routine. The user running this routine wants to reconcile everything else for the session and let the next reconciliation pass auto-match the deferred row once the manual posting has happened.

The earlier hypothesis — that Ignore could be repurposed — is wrong: Ignore is permanent and the wrong verb. The user wants a session-only deferral.

## Goal

Allow the user to mark a per-row action `defer` ("Awaiting manual entry") at Stage 3. A deferred row:

1. Counts as "decided" so the Import button enables.
2. Is **not** posted to Opera (no `atran`, `aentry`, `ntran`, `anoml` writes).
3. Is **not** added to the permanent ignore list — no `/ignore-transaction` API call.
4. Reappears as unmatched on the next scan if Opera still has no record; auto-matches once the manual posting has been entered.

**The existing Stage 4 (Reconcile) flow is not changed.** A deferred row reaches Stage 4 simply as an unmatched row — same as any other unmatched line — and the existing partial-reconciliation dialog already handles "you have unmatched lines, the bank reconciliation will not be marked complete, continue?". No new dialog text, no new badge logic at Stage 4, no new empty-state banner.

## Out of Scope

- Changes to Opera's reconciliation flag logic (Opera's bank statement remains unreconciled for that period until every transaction is reconciled — that's correct accounting behaviour).
- A separate "deferred items" dashboard or notification system (could be a later feature).
- Auto-creating a placeholder Opera entry on defer (the user explicitly does not want this).
- Persisting deferred state across sessions in the bank-import draft DB. Defer is session-only; the next scan reconstructs state from Opera.

## Architecture

### Data model

The existing `MatchedTransaction` (or equivalent line-state) already carries:

```python
action: Optional[str]  # 'sales_receipt' | 'purchase_payment' | 'skip' | 'manual'
matched_account: Optional[str]
skip_reason: Optional[str]
```

Add one new value to the `action` enum: `'defer'`. No new fields. This keeps Defer semantically distinct from `'skip'` (today's "non-posting noise") and from the permanent Ignore endpoint.

### Flow

```
Stage 2 (Review & Match)
  ├── auto-matched
  ├── suggested
  └── unmatched ─────┐
                     │
Stage 3 (Import)     ▼
  Per row, user picks one of:
    • Assign details + post to Opera   → action = 'sales_receipt' / 'purchase_payment' / etc.
    • Ignore (already in Opera)        → POST /ignore-transaction (permanent)
    • DEFER (NEW — awaiting manual)    → action = 'defer'
  Import button enable rule:
    every row has action != null
  On click:
    skip rows where action == 'defer' (no Opera writes)
    post the rest as today

Stage 4 (Reconcile)
  Unchanged. Deferred rows are simply unmatched at this stage —
  identical handling to any other unmatched line. The existing
  Partial Reconciliation dialog covers the "not all reconciled"
  case as today.
```

### Backend changes

| File | Change |
|---|---|
| `sql_rag/bank_import.py` (Opera SE) | Import loop already switches on `action`. Add `elif action == 'defer'` branch that logs the row at INFO level and continues — no Opera writes. |
| `sql_rag/bank_import_opera3.py` (Opera 3) | Mirror. |
| `apps/bank_reconcile/api/routes.py` | Inside the existing import endpoint loop, when `action == 'defer'` is encountered, call the audit helper (next file). No new HTTP endpoint. |
| `sql_rag/deferred_transactions_db.py` (new) | Small SQLite wrapper at `data/<company>/bank_reconcile/deferred_transactions.db`. Schema: `bank_code TEXT, statement_date TEXT, amount REAL, description TEXT, deferred_at TEXT, deferred_by TEXT`. (`amount` in pounds — this is audit only, no arithmetic, so float is fine.) Auto-creates schema on first use; single `record(...)` insert helper. Failure to write logs a WARNING but does not block the workflow. |
| Per-row return shape from Import endpoint | Add `deferred_count` alongside existing `imported_count`, `skipped_count`. |

### Frontend changes

| File | Change |
|---|---|
| `frontend/src/pages/BankStatementReconcile.tsx` | Add **Defer** button to the per-row action menu at Stage 3. Render amber "Awaiting manual entry" badge for `action === 'defer'` rows so the user can see what they marked while still on Stage 3. Update the import-button enable rule to count deferred rows as decided. **No changes to Stage 4 rendering, partial-reconciliation dialog text, or any other UI outside Stage 3.** |

### Persistence between sessions

Deferred status is **session-only** in the UI. When the user closes and reopens the statement scan:

- If the manual posting has happened in Opera since: the row auto-matches as a normal Opera entry.
- If not: the row reappears as unmatched and the user can defer again or take a different action.

No persisted "deferred state" needed in the bank-import draft DB. The audit log entry is for traceability only — not consulted at scan time.

## UX

### Stage 3 per-row buttons

| Button | Label | Effect |
|---|---|---|
| Assign | (existing) | Opens dropdowns; posts to Opera on Import. |
| Ignore | (existing) | Hits `/ignore-transaction` — permanent. |
| **Defer** | "Awaiting manual entry" | Sets `action='defer'`; amber badge; no Opera writes; not added to ignore list. |

Visually:
- Defer pill: amber background with a clock icon. Distinct from grey/strike-through Ignore (permanently excluded) and green/check Assigned (will post).
- Hover/title: "This row will not be imported. It will reappear on the next scan unless it's been entered into Opera manually."

### Stage 4 (Reconcile)

No changes. Deferred rows are simply unmatched at Stage 4. The existing partial-reconciliation dialog already handles "matched entries will be posted but the reconciliation will not be marked complete" — same wording, same flow, no new conditional logic.

## Error Handling Summary

| Scenario | Behaviour |
|---|---|
| User defers a row, manual posting never happens | Row reappears as unmatched on next scan. Idempotent — no failure. |
| User defers a row, manual posting happens before next scan | Row auto-matches on next scan as a normal Opera entry. |
| User defers every row | Stage 4 sees no matched rows; existing UI handles "nothing to reconcile" without changes. |
| Audit table SQLite write fails | Log WARNING, continue with import. Defer must not block the workflow because of an audit-only write. |
| User deferred row in one session and opens a different statement | Defer state is per-statement-scan, scoped by the current import draft. No leakage. |

## Files Touched

| File | Change |
|---|---|
| `sql_rag/bank_import.py` | Add `'defer'` action branch in the import loop. |
| `sql_rag/bank_import_opera3.py` | Mirror. |
| `apps/bank_reconcile/api/routes.py` | Audit-log helper for deferred rows; surface `deferred_count` in import response. |
| (already covered above) | |
| `frontend/src/pages/BankStatementReconcile.tsx` | Defer button, badge, counter, dialog text update. |
| `apps/core/docs/opera_knowledge_base.md` | Document `'defer'` action and behaviour. |
| `marketing/manuals/manual-bank-reconciliation.md` | Add a paragraph explaining when to use Defer vs Ignore vs Assign. Update Last-updated date. |

## Testing

### Backend unit tests

- `tests/test_bank_import_defer.py` (new):
  - `bank_import.py`: given a list of lines including `action='defer'`, verify those rows are skipped (no atran/aentry/ntran writes) while other actions process normally; verify return shape includes `deferred_count`.
  - Same coverage in `bank_import_opera3.py` mirror.
- `tests/test_deferred_transactions_db.py` (new):
  - Schema init creates table on first call.
  - Insert helper writes the expected fields and survives a re-init.

### Frontend manual verification

- Defer button appears for each unmatched row at Stage 3.
- Click Defer: row gets amber badge, Import button still disabled until other rows are decided.
- All decided (mix of assign + defer): Import button enables.
- Click Import: matched rows post to Opera; deferred rows do not (verify via the Opera atran query the page already exposes for sanity-checking).
- At Stage 4: deferred rows simply appear as unmatched (no special UI); the existing partial-reconciliation dialog fires unchanged.

### End-to-end

- Defer one row in a statement.
- Manually insert the corresponding entry into Opera atran (simulating colleague's manual posting).
- Re-scan the same statement (or next month's statement that lists the same transaction).
- Confirm the previously-deferred row auto-matches as a normal Opera entry.

## Success Criteria

1. The user can complete a Stage 3 → Stage 4 workflow with at least one row deferred, **without** assigning posting details for that row, **without** marking it permanently Ignored, and **without** any data being written to Opera for that row.
2. The deferred row reappears as unmatched on the next scan if Opera still has no record.
3. The deferred row auto-matches on the next scan if Opera now has a matching entry.
4. Stage 4 (Reconcile) UI and the partial-reconciliation dialog are **unchanged** — deferred rows participate in the same "unmatched/un-reconciled" code path as any other unmatched row.
5. Audit entries for every defer action exist in `deferred_transactions.db` for traceability.
6. Both Opera SE and Opera 3 paths behave identically.
7. No changes to Opera databases (SQL Server schema, FoxPro DBF schema) — strict adherence to the project rule.
