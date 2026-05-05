# Bank-Rec Local-Status Self-Heal — Design Spec

**Date**: 2026-05-05
**Status**: Approved (brainstorming complete, ready for implementation plan)

## Problem

When a bank statement is imported via the app and reconciled in two parts —
matched entries posted with `ae_tmpstat` markers (partial rec) and the
balance finished by the operator in Opera Cashbook > Reconcile — the local
store and Opera fall out of sync.

Concrete walk-through (intsys, BC010, real example today):

1. Operator imports `Statement 01-MAY-26 AC 90764205 02063157.pdf`. App
   writes `bank_statement_imports.is_reconciled = 0`, posts 20 matched
   entries with `ae_tmpstat`, defers 10, leaves 6 ignored/matched-to-existing.
2. Operator hits **Continue** on the Partial Reconciliation prompt.
3. Operator finishes the rec in Opera. Opera writes `aentry.ae_reclnum > 0`
   on the matched entries, clears `ae_tmpstat`, advances `nbank.nk_recbal`
   to the statement closing balance, advances `nk_lststdt` to the statement
   period_end, advances `nk_lststno` to the next statement number.
4. Opera has no knowledge of our local `bank_statement_imports` row.
5. Operator returns to the app and clicks Scan. The scan endpoint filters
   "already processed" by checking `is_reconciled` — still 0 — so the
   statement reappears as **Awaiting Reconcile**, even though Opera says
   it's done.

Confirmed today against intsys / BC010:

```
Local row (bank_statement_imports id=71):
  closing_balance = £115,064.71
  period_end       = 2026-05-01
  is_reconciled    = 0   ← STALE

Opera (nbank.BC010, Barclays Bank Current A/C):
  nk_recbal  = £115,064.71   ← matches closing exactly
  nk_lststdt = 2026-05-01     ← matches period_end exactly
  nk_lststno = 86940          (last statement number reconciled)
  nk_reccfwd = £0.00          (cleared = full rec, not partial)
```

Opera is unambiguous: this statement was fully reconciled. The local flag
is the only thing wrong.

## Goal

A read-only self-heal that, on each scan-emails call, asks Opera "for any
bank_statement_imports row I still think is in-progress, has Opera in fact
moved past it?" and updates the local flag accordingly. No Opera writes.
No user action.

## Out of Scope

- Modifying any Opera data — read-only.
- Touching the deferred-row tracking. Self-heal only flips the rec status;
  if the rec is complete in Opera, deferreds must already be in Opera as
  posted entries and will auto-match on the next scan via existing logic.
- Surfacing "Opera said it's done but our app didn't know" as its own UI
  category. The fix makes the symptom invisible — the row drops out of the
  scan list cleanly, mirroring the manual's existing "already-processed
  statements are hidden".
- A periodic background job. Scan-time check is sufficient — that's the only
  place stale state manifests.

## The Rule

A `bank_statement_imports` row with `is_reconciled = 0` self-heals to
`is_reconciled = 1` if **all** of the following hold against the row's
bank account in Opera:

1. **Balance match.** `nbank.nk_recbal / 100.0` equals
   `bank_statement_imports.closing_balance` within £0.01.
2. **Date match.** `nbank.nk_lststdt` (cast to date) is **on or after**
   `bank_statement_imports.period_end` (cast to date).
3. **Statement-number match (when available).** When the row's
   `statement_number` is populated:
   `nbank.nk_lststno >= bank_statement_imports.statement_number`. If the
   row has no `statement_number` stored (legacy rows from before this
   change), this check is skipped — checks 1 and 2 alone are sufficient
   to disambiguate, and skipping it for legacy rows is the only way the
   heal applies retroactively.

All three are checked AND-ed together (or 1 + 2 for legacy rows).

The use of `>=` rather than equality on checks 2 and 3 is intentional: if
subsequent statements have been reconciled too, those checks still pass —
correctly — for the older statement. (Opera's sequential rec gating
guarantees a later statement can't be reconciled without earlier ones.)

## Architecture

**Single source of truth**: a new module `sql_rag/bank_rec_heal.py` exposing
one function:

```python
def heal_bank_statement_imports(
    bank_code: str,
    company_db_path: Path,
    opera_data_source: OperaDataSource,
) -> HealResult:
    """For every bank_statement_imports row on this bank with
    is_reconciled=0, check Opera and flip to 1 where the three-fact
    rule is satisfied. Read-only against Opera. Returns a HealResult
    with the count of rows healed and a list of audit lines."""
```

The module owns the rule's truth. The scan-emails endpoint imports it and
calls it once per bank before filtering. Tests pin the rule.

`OperaDataSource` is the existing abstraction over SE / Opera 3 — same
shape used by the duplicate-check and matcher modules. Both
`OperaSEDataSource` and `Opera3DataSource` need a `read_nbank(bank_code)`
method returning a `NbankSnapshot` with the four fields the rule needs:
`recbal_pounds`, `lststdt`, `lststno`, `bank_code`. SE reads with
`WITH (NOLOCK)`; Opera 3 reads the DBF directly (no lock needed).

## Files Touched

| File | Change |
|---|---|
| `sql_rag/bank_rec_heal.py` | **CREATE** — the heal module: rule, single function, audit-line builder |
| `sql_rag/duplicate_check_se.py` | MODIFY `OperaSEDataSource` — add `read_nbank(bank_code) -> NbankSnapshot` (WITH NOLOCK) |
| `sql_rag/duplicate_check_o3.py` | MODIFY `Opera3DataSource` — add `read_nbank(bank_code) -> NbankSnapshot` |
| `sql_rag/email_storage.py` (or wherever the schema lives) | MODIFY — add migration: SQLite `ALTER TABLE bank_statement_imports ADD COLUMN statement_number INTEGER` guarded by a `PRAGMA table_info` check (SQLite has no native `ADD COLUMN IF NOT EXISTS`). The migration runs once per company DB at startup; subsequent runs detect the column is present and no-op. |
| `apps/bank_reconcile/api/routes.py` | MODIFY scan-emails endpoint (SE) — call `heal_bank_statement_imports` per bank before filtering |
| `apps/bank_reconcile/api/routes.py` | MODIFY `complete_reconciliation` endpoint (SE) — after a successful Opera write, `UPDATE bank_statement_imports SET statement_number = ? WHERE id = ?` |
| `apps/bank_reconcile/api/routes.py` | MODIFY scan-emails endpoint (Opera 3) — same heal call |
| `apps/bank_reconcile/api/routes.py` | MODIFY `opera3 complete_reconciliation` endpoint — same statement_number population |
| `tests/test_bank_rec_heal.py` | **CREATE** — unit tests on `heal_bank_statement_imports`: truth table for the three checks, legacy-row fallback, idempotency |
| `tests/test_bank_rec_heal_regression.py` | **CREATE** — regression test for the intsys/BC010/import 71 scenario as the canonical fixture |
| `apps/core/docs/opera_knowledge_base.md` | MODIFY — add "Bank Rec Self-Heal Rule" section |
| `~/opera-knowledge-ref/packages/opera-knowledge/business-rules/bank-rec-self-heal.md` | **CREATE** — central KB doc (per CLAUDE.md mandatory rule) |
| `marketing/manuals/manual-bank-reconciliation.md` | MODIFY — add a sentence to Stage 5 noting that statements completed in Opera drop off the scan list automatically on next scan |

## Data Flow

### On rec completion (writes the statement_number)

1. Frontend calls `/api/bank-reconciliation/complete?partial=true&...` with
   the `import_id` and a `statement_number` param.
2. Existing code calls `OperaSQLImport.complete_reconciliation(...)` which
   writes Opera (`ae_reclnum` or `ae_tmpstat`, advances `nk_lststno`, etc.).
3. **NEW:** on success — specifically inside the existing
   `if result.success and import_id and email_storage:` block in the route
   (where `bank_statement_imports.is_reconciled` and `reconciled_count` are
   already updated via `email_storage`) — also persist the
   `statement_number`:
   ```sql
   UPDATE bank_statement_imports
      SET statement_number = ?
    WHERE id = ?
   ```
   This applies for both partial and full completions — we need the number
   stored in either case so the heal can use it later. Same write
   transaction as the existing flag updates, no extra round-trip.
4. Same flow on the Opera 3 mirror endpoint.

### On scan (heal then filter)

1. Frontend calls `/api/bank-reconciliation/scan-emails?bank_code=...`.
2. Existing code runs the IMAP/folder scan and assembles the candidate list.
3. **NEW:** **after** the import-record list for the bank is loaded but
   **before** the "already-processed" filter is applied, call
   `heal_bank_statement_imports(bank_code, company_db, opera_data_source)`:
   1. Read `nbank` for this bank (SE: `WITH (NOLOCK)`; Opera 3: DBF read).
   2. Read every `bank_statement_imports` row for this bank with
      `is_reconciled = 0`.
   3. For each row, evaluate the three-fact rule.
   4. For rows where the rule is satisfied, run:
      ```sql
      UPDATE bank_statement_imports
         SET is_reconciled = 1,
             reconciled_date = COALESCE(reconciled_date, ?),
             reconciled_count = CASE
                                  WHEN ? IS NULL THEN reconciled_count
                                  ELSE ?
                                END
       WHERE id = ?
      ```
      `reconciled_date` source: `nbank.nk_recldte` if non-null, else `now()`.
      Use `COALESCE` so we never overwrite a previously-set date — a
      partial-rec completion may already have stamped one.

      `reconciled_count`: when the row has a stored `statement_number`,
      compute it via `SELECT COUNT(*) FROM aentry WITH (NOLOCK) WHERE
      ae_acnt=? AND ae_frstat=? AND ae_reclnum>0` (or DBF equivalent on
      Opera 3) and pass that. For legacy rows with no stored statement
      number, pass NULL so the existing `reconciled_count` value is
      preserved (which may already be non-zero from the partial rec).
   5. Append an audit line per healed row at INFO level via the standard
      logger to `api_debug.log`:
      ```
      bank_rec_heal: bank=BC010 import_id=71 healed
        nk_recbal=£115064.71 ≈ closing=£115064.71
        nk_lststdt=2026-05-01 >= period_end=2026-05-01
        nk_lststno=86940 >= statement_number=86940 (or "skipped — legacy row")
      ```
   6. Return the count + audit lines for the route to include in the
      response's diagnostic block (never user-facing UI text — purely
      audit / debugging).
4. Existing filter step now sees the healed rows as `is_reconciled=1` and
   excludes them from the scan list — they drop out cleanly.

If the heal commits the local UPDATE successfully but the surrounding
response assembly later fails, no rollback is needed: Opera is unchanged,
local SQLite reflects truth, and idempotency means the next scan
re-evaluates each row and finds it already in the correct state.

### Locking discipline

- Every Opera SE read in the heal uses `WITH (NOLOCK)` — required by
  `business-rules/locking-protocol.md`.
- The local SQLite `UPDATE` on `bank_statement_imports` runs in a
  short-lived transaction — open, update, commit. No locks held across
  Opera calls.
- For Opera 3, DBF reads do not require locks; the central KB
  `platform/opera3-write-agent.md` covers the read/write split.

## Error Handling

- **Opera unreachable on heal:** caught, logged at WARNING, heal skipped
  for this scan. Scan continues with the original (unhealed) list — same
  behaviour as today. The next scan tries again.
- **`nbank` row missing for the bank:** treated as "Opera doesn't know
  about this bank" — no rows healed, audit line records the miss. Should
  not happen in normal Opera state; logged as a data-integrity signal.
- **`bank_statement_imports.statement_number` populated for one row but
  not another:** treated independently — each row evaluated on its own
  facts. Legacy rows fall back to the 2-check rule, new rows use 3.
- **Concurrent heal + import:** import path takes a per-bank import lock
  (existing `_bank_lock_key`); heal does not — heal only reads from Opera
  and writes to local SQLite. No conflict.
- **Corrupt or NULL Opera fields** (e.g. `nk_recbal IS NULL`): treated as
  "rule not satisfied" — no heal performed. Logged at DEBUG.
- **Public API surface unchanged.** `is_reconciled` semantics are the
  same; we just become more accurate about when it's 1.

## Per-Company Isolation

Every call to `heal_bank_statement_imports` resolves the local SQLite path
via `get_current_db_path('email_data.db')` (existing helper) and the Opera
data source via the request-scoped company context. No cross-company
bleed; intsys's heal queries intsys's Opera DB and intsys's local SQLite,
cloudsis queries cloudsis's, etc. — same multi-tenant rules already
enforced everywhere else.

## Opera SE / Opera 3 Parity (mandatory per CLAUDE.md)

The rule is identical on both platforms; only the read mechanism differs:

| | Opera SE | Opera 3 |
|---|---|---|
| `nbank` read | `SELECT ... FROM nbank WITH (NOLOCK) WHERE ...` | DBF read via Opera3DataSource |
| `aentry` count for `reconciled_count` | `SELECT COUNT(*) FROM aentry WITH (NOLOCK) WHERE ae_acnt=? AND ae_frstat=? AND ae_reclnum>0` | DBF scan via Opera3DataSource |
| Local UPDATE | identical SQLite UPDATE | identical SQLite UPDATE |
| Heal called from | SE scan-emails route | Opera 3 scan-emails route |
| Tests | shared `OperaDataSource` interface; both implementations exercised in test suite | same |

Both endpoints get the change in the same commit. Same applies to the
`statement_number` population on `complete_reconciliation`.

## Testing

| Test | Pins |
|---|---|
| `test_heal_three_facts_match_marks_done` | All three checks pass → row flips to `is_reconciled=1`, audit line emitted with all three proof strings. |
| `test_heal_balance_mismatch_no_change` | `nk_recbal` differs from closing by >£0.01 → row stays at 0, no audit. |
| `test_heal_balance_match_within_one_pence` | `nk_recbal` differs by exactly £0.01 → still considered match (boundary). |
| `test_heal_balance_match_outside_one_pence` | Differs by £0.011 → no match (boundary). |
| `test_heal_date_strictly_before_period_end` | `nk_lststdt < period_end` → row stays at 0. |
| `test_heal_date_equals_period_end` | `nk_lststdt == period_end` → satisfied (≥, not >). |
| `test_heal_date_after_period_end` | `nk_lststdt > period_end` (later statement reconciled too) → satisfied. |
| `test_heal_statement_number_match` | `nk_lststno == stored statement_number` → satisfied. |
| `test_heal_statement_number_advanced` | `nk_lststno > stored` (next rec already done) → satisfied. |
| `test_heal_statement_number_behind` | `nk_lststno < stored` → not satisfied. |
| `test_heal_legacy_row_no_stored_number_uses_two_checks` | `statement_number IS NULL` → skip check 3, use checks 1+2 only. Heal works for legacy rows like import 71. |
| `test_heal_idempotent` | Run twice → second run finds nothing to heal, no spurious updates, no error. |
| `test_heal_opera_unreachable_skips_silently` | Mock Opera connection failure → heal returns 0 healed + warning log line, scan continues normally. |
| `test_heal_nbank_missing_logs_and_skips` | Bank not in `nbank` → no heal, structured warning. |
| `test_real_world_partial_rec_completed_in_opera_regression` | Build the exact scenario captured today (closing £115,064.71, period_end 2026-05-01, Opera `nk_recbal=£115,064.71`, `nk_lststdt=2026-05-01`, `nk_lststno=86940`, statement_number=NULL on the legacy row) → heal flips `is_reconciled` to 1 with the captured proof string and preserves any existing `reconciled_count`. Pin this canonical scenario so the rule cannot silently regress. (Test data anonymised — uses a generic bank code, not real tenant identifiers.) |
| `test_completion_populates_statement_number_se` | After a successful SE `complete_reconciliation`, `bank_statement_imports.statement_number` equals the value passed in. |
| `test_completion_populates_statement_number_o3` | Same on Opera 3. |
| `test_se_o3_parity` | Same logical input on SE and Opera 3 produces same heal decision. |
| `test_heal_reads_use_nolock` | Source-inspect the SE read query — must contain `WITH (NOLOCK)`. |

All tests use existing harness conventions; mocks for the SQL connector and
for the Opera 3 DBF reader follow the patterns already used in
`test_already_posted_fallback.py` etc.

## Verification

After implementation, on intsys / BC010:

1. Confirm import 71's row has `is_reconciled=0`, `statement_number IS NULL`
   (legacy row).
2. Run a fresh statement scan for BC010.
3. Heal runs:
   - Check 1: `nk_recbal=£115,064.71 ≈ closing=£115,064.71` ✓
   - Check 2: `nk_lststdt=2026-05-01 ≥ period_end=2026-05-01` ✓
   - Check 3: skipped (legacy row, no stored number)
   - Outcome: row flips to `is_reconciled=1`. Audit line in `api_debug.log`.
4. Scan response no longer includes the May statement.
5. Re-run scan — heal returns 0 healed (idempotent).
6. Verify Opera is untouched: query `nbank.BC010` and `aentry` for BC010
   — every field unchanged from before the heal. (Read-only mandate.)

## KB / Manual

**Local KB** (`apps/core/docs/opera_knowledge_base.md`):

> ## Bank Rec Self-Heal Rule
>
> The local `bank_statement_imports.is_reconciled` flag is the app's view of
> whether a statement has been reconciled. When the operator runs a partial
> rec via the app and finishes it in Opera Cashbook > Reconcile, Opera
> updates `nbank.nk_recbal`, `nk_lststdt`, `nk_lststno` and `aentry.ae_reclnum`,
> but does not touch our local store. The scan-emails endpoint runs a
> read-only self-heal that detects this and updates the local flag.
>
> A row heals to `is_reconciled=1` when ALL of:
> 1. `nbank.nk_recbal/100.0 ≈ closing_balance` within £0.01.
> 2. `nbank.nk_lststdt ≥ period_end`.
> 3. `nbank.nk_lststno ≥ stored statement_number` (skipped for legacy rows
>    where `statement_number IS NULL`).
>
> The heal is read-only against Opera. SE reads use `WITH (NOLOCK)`. Both
> Opera SE and Opera 3 implement the same rule — see
> `business-rules/bank-rec-self-heal.md` in the central KB.

**Central KB**
(`~/opera-knowledge-ref/packages/opera-knowledge/business-rules/bank-rec-self-heal.md`):
canonical reference; cross-references `bank-rec-completion.md` and
`bank-rec-open-items.md`.

**Manual** (`marketing/manuals/manual-bank-reconciliation.md`): add to
Stage 5:

> If the rec was completed in Opera (after a partial rec via this app), the
> statement automatically drops off the scan list on the next scan. The app
> detects Opera's reconciled state and updates its own status accordingly,
> read-only.

## Success Criteria

1. The intsys/BC010/import 71 scenario re-scans cleanly: statement no
   longer appears as Awaiting Reconcile; `is_reconciled=1`.
2. The truth-table tests fail loudly if any of the three checks regresses.
3. No Opera data modified by the implementation. (Verifiable by snapshot
   comparison before/after.)
4. Function signatures, response shapes unchanged — fully
   backwards-compatible.
5. SE and Opera 3 produce identical heal decisions for the same logical
   input.
6. Heal is idempotent — running twice produces the same outcome.
7. `bank_statement_imports.statement_number` is populated for every new
   completion going forward.
8. Audit log captures every heal flip with the proof string.
