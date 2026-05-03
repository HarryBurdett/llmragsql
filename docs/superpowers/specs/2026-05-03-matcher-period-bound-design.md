# Matcher Period-Bound Validation — Design Spec

**Status:** Draft for review
**Date:** 2026-05-03
**Author:** Claude (per Charlie Burdett's mandate to fix accumulated issues)

## Goal

The bank-statement-to-cashbook matcher must **never pair an aentry with a statement line if the aentry's date falls outside the statement's own period**. The complete-reconciliation handler must **refuse to set partial-reconciliation markers (`ae_tmpstat`) on entries outside the statement period**. Eliminate the bug that today applied tmpstat reservations to a February entry against a March statement.

## Why

Today, in Cloudsis BB005:
- The user opened the **March 5202** statement (period 2026-03-01 → 2026-03-31).
- The matcher (`match_statement_to_cashbook`) used a **45-day date tolerance** and paired four entries:
  - `P100000731` (2026-02-28) — **February**, doesn't belong on a March statement.
  - `P100000742`, `P100000749`, `R500000366` — all **April**, also don't belong.
- The user clicked Reconcile.
- The complete-reconciliation handler detected a £30k balance mismatch, switched to "partial reconcile mode", and set `ae_tmpstat` on all four — including the Feb entry.
- Result: dangling reservation markers on the wrong entries, blocking them from being correctly reconciled in the future.

Two bugs combined:
1. **Matcher tolerance:** 45 days allowed a Feb entry to "match" a March statement. The tolerance should be bounded by the statement's own period (with a small grace window for end-of-period postings, not 45 days).
2. **Complete-reconcile validation:** the handler accepted entries from outside the statement period without complaint and applied tmpstat anyway.

## Constraints (must hold)

- **Period-bound matching:** the matcher's candidate pool is restricted to aentries whose `ae_lstdate` is within `[period_start - GRACE, period_end + GRACE]`, where `GRACE` is small (e.g. 7 days, configurable).
- **Period-bound reconciliation:** the complete-reconciliation handler validates that every entry being reconciled has `ae_lstdate` within the statement period (same grace), and refuses with a clear error otherwise.
- **No silent fallback:** if the user tries to reconcile entries outside the period, they see an explicit message — not a partial reconcile that quietly applies tmpstat to wrong rows.
- **Backwards compatible:** historical reconciles with wider tolerances must still load correctly.

## Architecture

```
                    ┌─────────────────────────────────┐
                    │ Statement S                     │
                    │   period_start = 2026-03-01     │
                    │   period_end   = 2026-03-31     │
                    └─────────────┬───────────────────┘
                                  │
     ┌────────────────────────────┼────────────────────────────┐
     │                            │                            │
     ▼                            ▼                            ▼
┌──────────┐            ┌────────────────────┐           ┌──────────┐
│ Matcher  │            │ User picks entries │           │ Complete │
│          │            │ in UI to reconcile │           │ handler  │
└────┬─────┘            └─────────┬──────────┘           └────┬─────┘
     │ candidate pool             │                           │ validation
     │ filtered to                │                           │ rejects entries
     │ aentries with              │                           │ outside period
     │ ae_lstdate in              ▼                           │
     │ [Pstart-G, Pend+G]   matched_entries                   │
     │                                                        │
     └────────────── period ─────────────────────────────────►│
                                                              ▼
                                                    Set tmpstat (or commit)
                                                    only on in-period rows
```

## Components

### 1. Matcher — `opera_sql_import.py::match_statement_to_cashbook`

Already partially fixed today (auto-matched + already-reconciled buckets). Add a **strict period filter on the candidate pool**:

```python
# Existing: get unreconciled aentries on this bank
df = self.sql.execute_query(f"""
    SELECT ae_entry, ... FROM aentry WITH (NOLOCK)
    WHERE ae_acnt = '{bank_account}'
      AND ae_reclnum = 0
      -- NEW: enforce period bounds
      AND ae_lstdate BETWEEN '{period_start - grace}' AND '{period_end + grace}'
    ORDER BY ae_lstdate, ae_entry
""")
```

`period_start` and `period_end` come from the statement (added to the function signature). `grace` defaults to 7 days, configurable per call. Same restriction on the already-reconciled second pass.

**Fallback:** if `period_start`/`period_end` are not provided, the matcher falls back to the date-tolerance behaviour but **logs a warning** that period-bounded matching wasn't used. Eventually all callers will pass period info.

### 2. Complete-reconciliation handler — `apps/bank_reconcile/api/routes.py`

The endpoint that fires on "Mark Reconciled" / final-reconcile click. Currently:
- Receives `entries[]` from the frontend (each with `entry_number`, `statement_line`).
- Loads each from Opera, applies reconcile or tmpstat.

New validation, runs **before** any reconcile or tmpstat write:

```python
out_of_period = []
for e in entries:
    aentry = load_aentry(bank_code, e['entry_number'])
    if aentry.ae_lstdate < period_start - grace or aentry.ae_lstdate > period_end + grace:
        out_of_period.append({
            'entry': aentry.ae_entry,
            'date': aentry.ae_lstdate.isoformat(),
            'period': f'{period_start}..{period_end}',
        })

if out_of_period:
    return {
        "success": False,
        "error": "Entries fall outside the statement period",
        "out_of_period": out_of_period,
    }
```

The frontend surfaces this clearly (modal listing the out-of-period entries) and cancels the reconcile attempt. No tmpstat is applied. No partial reconcile state is created.

### 3. Tmpstat-clear utility (companion piece)

Today's session left dangling tmpstat markers on 4 entries. Add a utility endpoint at `/api/reconcile/bank/{bank_code}/clear-orphan-tmpstat` that:

- Lists aentries on this bank with `ae_tmpstat > 0 AND ae_reclnum = 0` (orphan reservations).
- Returns them to the user with date, value, ref.
- On user confirm, runs `UPDATE aentry SET ae_tmpstat = 0` on those entries (with ROWLOCK).
- Logs the cleanup.

This is the safe equivalent of "cancel the in-progress reconcile in Opera" but available in our app — useful because today's case had the user with dangling markers and no clear way to clear them.

### 4. Frontend changes

`BankStatementReconcile.tsx`:
- The error response from complete-reconciliation surfaces as a modal listing out-of-period entries with their dates.
- A new menu item under "Utilities" → "Clear Orphan Tmpstat" calls the new endpoint.

### 5. Test suite

`tests/test_matcher_period_bound.py`:
- Statement period 2026-03-01..2026-03-31, candidate pool includes Feb, March, April aentries → only March (+ grace) match.
- Grace boundary: aentry on 2026-04-05 with grace=7 → matches; on 2026-04-09 → doesn't.
- No period info supplied → falls back to old tolerance, warning logged.

`tests/test_complete_reconciliation_period_validation.py`:
- Reconcile attempt with all entries in-period → succeeds.
- Reconcile attempt with one out-of-period entry → returns error response, no tmpstat written.
- Reconcile attempt with grace boundary entries → succeeds.

`tests/test_clear_orphan_tmpstat.py`:
- aentry with tmpstat > 0 and reclnum = 0 → listed.
- aentry with tmpstat > 0 and reclnum > 0 → not listed (it's a real reconcile).
- Clearing succeeds; ae_tmpstat → 0; ae_reclnum unchanged.

## Data flow

```
1. User opens March statement; matcher fetches candidates restricted to March + 7 days
2. Matcher returns auto_matched/suggested/etc., all in-period
3. User ticks entries, clicks Reconcile
4. Frontend POSTs entries to complete-reconciliation
5. Endpoint validates each entry's date against the period
6. If any out-of-period → error returned, modal shown, no Opera write
7. Else → reconcile proceeds normally
```

## Error handling

- `period_start`/`period_end` malformed → ValueError.
- Out-of-period entries detected → 400 response with structured `out_of_period` list. No partial action.
- Database query failure → propagated to caller; reconcile aborted.

## Testing strategy

- Unit tests as above.
- Integration test against a fixture database: simulate the Cloudsis case (March statement, Feb + April candidates, expect March-only match + clean error on attempt to include out-of-period).
- Smoke test against real Cloudsis: run the matcher with the March statement period; assert only March entries match.

## Migration plan

1. Write tests (TDD).
2. Modify matcher signature to accept period bounds; refactor to use them.
3. Add validation to complete-reconciliation handler.
4. Add tmpstat-clear endpoint + UI utility.
5. Verify against today's Cloudsis case.

## Done criteria

- [ ] All matcher and validation tests pass.
- [ ] Live test on Cloudsis: March statement loads with only March aentries as candidates.
- [ ] Live test: attempt to reconcile a Feb entry against March → clean error, no tmpstat written.
- [ ] Tmpstat-clear endpoint works: clears the 4 orphan markers from today's session safely.
- [ ] Documentation: KB updated with the period-bound matching rule.

## Out of scope

- Reworking Opera's reconcile UI itself. The user can still reconcile via Opera directly; we just stop our app from creating bad state.

## Risks / failure modes

- **Grace window choice:** 7 days is a guess. If real statements have longer late-postings (e.g. month-end batch 10 days late), 7 might be too short. **Mitigation:** make `grace` configurable per bank in nbank or settings; default 7 days.
- **Existing tmpstat markers:** the cleanup utility writes to Opera, which contradicts the "read-only audit" default. **Mitigation:** explicit user confirm in UI; logs every write; never runs automatically.
- **Backwards compat:** old call sites that don't pass period info may regress. **Mitigation:** fallback path with warning; gradual migration.
