# Sequential Statement Gating — Design Spec

**Date**: 2026-04-30
**Status**: Approved (defaults accepted, ready for implementation plan)

## Problem

The bank reconciliation operator hits a transaction they cannot decide on (need to refer to a colleague). With the Defer feature shipped today they can mark the row "Awaiting manual entry" and complete what they can in the current statement — but the next statement is then **blocked from being processed at all** because of the existing opening-balance chain rule: the next statement's opening balance must equal Opera's reconciled balance, which only advances when the prior statement actually reconciles.

Real-world impact (Cloudsis, 30 Apr 2026): a single deferred row in the current statement holds up:
- Posting receipts and payments from the next statement (so customer statements can't be sent on time).
- Posting the next statement's purchase ledger entries (so supplier statement reconciliations are blocked).
- Any other downstream accounting that depends on having Opera up-to-date.

The operator wants to keep updating Opera as much as possible, even when reconciliation has to wait.

## Goal

Allow the operator to **process the next statement (Stages 1–3: Match / Assign / Import)** as soon as **every row in the current statement has been decided** (imported / ignored / deferred). Reconciliation of the next statement (Stage 4) remains gated by the existing opening-balance check — which naturally cascades from one statement to the next without needing a new gate.

The operator gets:

- Posting integrity preserved — same atran/aentry/ntran writes as today.
- Reconciliation integrity preserved — Stage 4 still requires opening balance to match Opera's `nk_recbal`.
- Customer / supplier ledgers continue to update — driven by per-row imports, not by reconciliation.
- A clean per-statement state — operator can't have multiple half-decided statements in flight.

## Out of Scope

- Auto-routing or notification of deferred rows to a third party (parked for a future feature; the operator chases manually for now).
- Cross-statement "pending deferred items" dashboard.
- Any change to the existing Reconcile-stage logic (opening balance check is unchanged).
- Any change to the Defer behaviour shipped today.
- Bypassing the chain integrity at reconcile time (Stage 4 still strictly sequential).

## Statement State Model

Per bank, each statement carries one of these states:

| State | Meaning | What user can do next |
|---|---|---|
| `pending_extraction` | Gemini quota / extraction failure (existing) | wait, re-scan |
| `ready` | extracted, in sequence, opening balance matches Opera's reconciled balance | open and process |
| `in_progress` | opened by user; some rows still undecided | finish deciding all rows |
| `imported` (NEW) | every row decided (imported / ignored / deferred), at least one row deferred so reconciliation can't yet complete | the **next statement becomes openable**; this statement waits for deferred-row resolution |
| `reconciled` | Stage 4 complete — Opera's `nk_recbal` updated | done |

**Key change**: today the next statement is gated until the prior is `reconciled`. After this work, it's gated until the prior is **`imported` or `reconciled`**.

Reconciliation of statement N+1 still requires N+1's opening balance to match Opera's `nk_recbal`. Since `nk_recbal` only advances on actual Reconcile completion, statement N+1 can't reconcile until statement N reconciles. The chain stays intact without an extra check.

## Data Model

**No new columns** — the new `imported` state is derived from existing data:

| State | Derivation |
|---|---|
| `reconciled` | `bank_statement_imports.is_reconciled = 1` |
| `imported` | a non-reconciled import record exists for this statement AND `deferred_transactions.db` has ≥1 row matching this bank+statement period |
| `in_progress` | a draft exists in `email_data.bank_import_drafts` but no import record yet |
| `ready` | extraction complete, no draft, no import — and prior statement is `imported` or `reconciled` |
| `pending_extraction` | extraction failed for this PDF (existing logic) |

A statement that has been imported with **zero deferred rows** transitions straight to `reconciled` if Stage 4 also runs successfully (the existing flow already does this when opening balance matches). The new `imported` state only applies when at least one row is deferred — i.e. reconciliation is actively waiting.

## API Changes

`/api/bank-import/scan-all-banks` (and its Opera 3 mirror at `/api/opera3/bank-import/scan-emails`):

- For each statement entry in the response, populate two new fields:
  - `state`: one of the values above.
  - `deferred_count`: number of rows in `deferred_transactions.db` for this bank + statement period.
- Update the "next statement is openable" computation: `prior.is_reconciled` → `prior.state in ('imported', 'reconciled')`.

No other endpoints change.

## UX Changes (Bank Statement Hub)

`frontend/src/pages/BankStatementHub.tsx`:

- New amber pill **"Imported · N deferred"** rendered when a statement's `state === 'imported'`. The pill appears alongside existing badges. Clicking it opens the statement at Stage 3 with deferred rows visible.
- Per-bank summary line above each bank's statements table: *"X statements imported with deferred items, Y transactions awaiting decision"*. Hidden when both are zero.
- "Process" button enable rule: enabled when the *previous* statement (by sequence) is in state `imported` or `reconciled`. Existing logic was `is_reconciled` only.
- Existing Reconcile / Process bottom button (the green one) — unchanged behaviour. The existing partial-reconciliation dialog already handles "imports succeed, reconcile is parked" because deferred rows trip the opening-balance check.

No new dialogs, no new pages.

## Error Handling

| Scenario | Behaviour |
|---|---|
| Operator tries to open next statement while current is `in_progress` | Process button disabled with tooltip "Decide every row in the current statement (import / ignore / defer) before processing the next one". |
| Operator clears all deferred rows from current statement (resolves them) | State auto-recomputes on next scan-all-banks load: was `imported`, becomes `reconciled` if Stage 4 also runs. Next statement's reconcile becomes possible. |
| Multiple statements end up `imported` (operator left deferred rows in N, then N+1, etc.) | Each shows the amber "Imported · N deferred" pill. Per-bank summary aggregates. None can reconcile until the chain unblocks from the bottom (resolve N's deferred rows → N reconciles → N+1's reconcile unblocks). |
| Deferred row resolved with the wrong amount | Statement N's opening = Opera reconciled balance (unchanged), but closing won't match — Reconcile fails with the existing "closing balance mismatch" error. Operator amends and retries. No new exposure. |
| Concurrent scans by multiple users | Existing per-bank import lock applies. Only one user posts at a time. Reading state is unaffected. |

## Files Touched

| File | Change |
|---|---|
| `apps/bank_reconcile/api/routes.py` | In `scan_all_banks_for_statements` (Opera SE) and `opera3_scan_emails_for_bank_statements` (Opera 3): compute `state` and `deferred_count` per statement entry; relax the next-statement gate from `is_reconciled` to `state in ('imported', 'reconciled')`. |
| `frontend/src/pages/BankStatementHub.tsx` | Render new amber pill; render per-bank summary line; update Process button enable rule to read the `state` field instead of `is_reconciled`. |
| `apps/core/docs/opera_knowledge_base.md` | Document the `imported` state, the relaxed next-statement gate, and the per-bank summary line. |
| `marketing/manuals/manual-bank-reconciliation.md` | Add a short paragraph: "You can move to the next statement as soon as every row in the current one has been decided. Reconciliation of the next statement waits until the current one fully reconciles, but Opera's customer/supplier ledgers update with each import." Update Last-updated date. |

No new SQLite tables, no new endpoints, no Opera schema changes.

## Testing

**Unit tests** for the state-derivation function:

- Statement with `is_reconciled = 1` → `reconciled`.
- Statement with import record, `is_reconciled = 0`, ≥1 deferred row → `imported`.
- Statement with import record, `is_reconciled = 0`, zero deferred rows → also `reconciled` (Stage 4 ran cleanly).
- Statement with draft but no import record → `in_progress`.
- Statement with extraction failure → `pending_extraction`.
- Statement with no draft, no import, no failure, prior is `imported` → `ready`.
- Statement with no draft, no import, no failure, prior is `in_progress` → `pending` (blocked on prior).

**Integration test** of the scan endpoint:

- Set up: bank with 3 statements. Statement 1 reconciled, statement 2 imported with deferred row, statement 3 not yet processed.
- Assert: response has statement 1 = `reconciled`, statement 2 = `imported` with deferred_count > 0, statement 3 = `ready` (because prior is `imported` — the new gate works).

**Manual end-to-end** on Cloudsis live data:

1. Pick the statement currently blocked by the colleague's pending answer.
2. Defer the stuck row, ensure all other rows are imported / ignored / deferred. Confirm the statement transitions to `imported` state with the amber pill.
3. Confirm the *next* statement is now `ready` and its Process button is enabled.
4. Process the next statement: extract, match, import everything. Confirm Opera's customer balances and supplier balances are updated despite the prior statement's reconciliation still being parked.
5. Attempt to reconcile the next statement → blocked by opening-balance mismatch (the existing chain check). Confirm the error message is clear.
6. Later, when the colleague answers, return to the original statement, import the deferred row. Confirm Stage 4 of the original statement now runs cleanly. Confirm the next statement's Reconcile becomes possible.

## Success Criteria

1. After deferring a row in the current statement, the next statement is immediately openable for processing — without waiting for the current to reconcile.
2. The next statement can complete Stages 1–3 (Match / Assign / Import) and post to Opera, updating customer/supplier ledgers.
3. The next statement's Reconcile is correctly gated by the opening-balance-equals-`nk_recbal` check — no out-of-order reconciliation possible.
4. The operator sees a clear amber "Imported · N deferred" pill on every statement that's waiting on deferred-row resolution.
5. The per-bank summary line gives a single-glance count of outstanding work.
6. No new SQLite tables, no new endpoints, no Opera schema changes — the change is a state derivation + UI gate relaxation.
7. Behaviour identical for Opera SE and Opera 3 paths.
