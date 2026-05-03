# Duplicate-Check Consolidation — Design Spec

**Status:** Draft for review
**Date:** 2026-05-03
**Author:** Claude (per Charlie Burdett's mandate to fix accumulated issues)

## Goal

Replace the **six scattered duplicate-check implementations** across analyse-time and post-time with a **single, type-aware, sign-aware function** that both flows call. Eliminate the entire class of "analyse says duplicate, post says no" (and vice-versa) bugs. Apply equally to Opera SE and Opera 3.

## Why

Today's session traced duplicate-check bugs through six different code sites:

| # | Location | What it checked |
|---|---|---|
| 1 | `bank_import.py::_is_already_posted` Check 0/0b/1 | atran/aentry by amount + ref + comment |
| 2 | `bank_import.py::_is_already_posted` Check 2/3 | stran/ptran by account + amount + type filter |
| 3 | `opera_sql_import.py::check_duplicate_before_posting` Check 1 | atran/aentry post-time |
| 4 | `opera_sql_import.py::check_duplicate_before_posting` Check 2/3 | stran/ptran post-time |
| 5 | `bank_duplicates.py::find_duplicates` "exact" match | atran/aentry/stran/ptran by amount |
| 6 | Opera 3 mirrors of (1)–(4) in `bank_import_opera3.py` and `opera3_foxpro_import.py` | same logic on FoxPro |

Across these, **the same conceptual question is asked six different ways**, with subtly different logic each time:
- Some used `ABS(ABS(value) - amount)` (sign-blind).
- Some used signed comparison.
- Some had type filters (`st_trtype = 'R'`); some dropped them.
- Some treated stran/ptran credit notes as "duplicate" when they should be "allocation target".
- Typos snuck into individual sites (`pt_ref`, `st_ref`).

Today we fixed each one. The class will reappear unless consolidated.

## Constraints (must hold)

- **Single source of truth:** one function, both flows call it. No parallel implementations.
- **Type-aware:** the duplicate check matches Opera's posting type (at_type, st_trtype, pt_trtype) corresponding to the action being checked.
- **Sign-aware:** signed amount comparison everywhere. A +£X receipt is never a duplicate of a -£X refund.
- **Type-correct ledger semantics:** stran/ptran rows of the *wrong* type (e.g. an unallocated credit note when posting a refund) are **allocation targets, not duplicates**. The function distinguishes.
- **Symmetric across SE and Opera 3:** the same logical contract; storage-specific implementations (SQL Server vs DBF).
- **Fully tested:** every action × direction × ledger-state combination has a test.

## Architecture

```
┌──────────────────────────────────────────────────────┐
│ apps/bank_reconcile/api/routes.py                    │
│   import-from-pdf endpoint                           │
│   refresh-matches endpoint                           │
└──────┬───────────────────────────────────────────────┘
       │
       │ both call
       ▼
┌──────────────────────────────────────────────────────┐
│ sql_rag/duplicate_check.py (new module)              │
│                                                      │
│   def check_for_duplicate(                           │
│       data_source,           # DataSource protocol   │
│       bank_code,                                     │
│       transaction_date,                              │
│       signed_amount_pounds,  # signed bank-line £    │
│       action,                # sales_receipt etc.    │
│       account_code,          # customer/supplier     │
│       description,                                   │
│       reference,                                     │
│       *,                                             │
│       date_tolerance_days=14,                        │
│       exclude_entries=None,                          │
│   ) -> DuplicateResult:                              │
│       """                                            │
│       Returns DuplicateResult with:                  │
│         - is_duplicate: bool                         │
│         - matched_table: 'aentry'|'stran'|'ptran'    │
│         - matched_entry: str                         │
│         - reason: str (human-readable)               │
│       """                                            │
└─────────────┬────────────────────────────────────────┘
              │ uses
              ▼
┌──────────────────────────────────────────────────────┐
│ Action → Type Mapping (constant)                     │
│   sales_receipt   → at_type=4, st_trtype='R'         │
│   sales_refund    → at_type=3, st_trtype='F'         │
│   purchase_pmt    → at_type=5, pt_trtype='P'         │
│   purchase_refund → at_type=6, pt_trtype='F'         │
│   nominal_pmt     → at_type=1, n/a                   │
│   nominal_recpt   → at_type=2, n/a                   │
│   bank_transfer   → at_type=8, n/a                   │
└──────────────────────────────────────────────────────┘
```

Two implementations of the `data_source` protocol: `OperaSEDataSource` (uses SQLConnector) and `Opera3DataSource` (uses FoxPro reader). Same protocol, different storage. The duplicate-check logic itself is identical.

## Components

### 1. `sql_rag/duplicate_check.py` (new)

Pure module containing:

- **`DataSource` protocol** with methods:
  - `find_aentry_by_signed_value(bank_code, date_from, date_to, signed_pence, exclude_ids) -> list[Row]`
  - `find_stran_by_signed_value(account, date_from, date_to, signed_pounds, st_trtype) -> list[Row]`
  - `find_ptran_by_signed_value(account, date_from, date_to, signed_pounds, pt_trtype) -> list[Row]`
- **`ACTION_TYPE_MAP` constant** mapping action → expected at_type, st_trtype, pt_trtype.
- **`check_for_duplicate(...)`** — the single function. Logic:
  1. Compute date window: `[transaction_date - tolerance, transaction_date + tolerance]`.
  2. Compute signed pence from signed pounds.
  3. **Cashbook check (authoritative):** call `find_aentry_by_signed_value` filtered by the action's `at_type`. If found and not excluded → DUPLICATE in cashbook.
  4. **Reference + amount fallback:** if `reference` is set and ≥6 chars, look for any aentry on the bank with matching signed value AND `ae_entref LIKE %reference%`. Captures cases where description differs but ref matches.
  5. **Ledger advisory check (NOT a duplicate signal):** for refund actions, look for matching stran/ptran row of the *expected refund type* (`'F'`). If found, return as `DuplicateResult(is_duplicate=False, advisory='allocation_target', target_entry=...)`. The caller can use this to auto-allocate but must still post.
  6. Otherwise: NOT a duplicate.

### 2. `sql_rag/duplicate_check_se.py` (new)

`OperaSEDataSource` implementation. Uses `SQLConnector`. All queries use signed comparisons (`ABS(ae_value - signed_pence) < 1`). All queries use the type filter passed in. No ABS-on-ABS. No hard-coded type strings.

### 3. `sql_rag/duplicate_check_opera3.py` (new)

`Opera3DataSource` implementation. Uses the FoxPro reader. Same protocol contract, FoxPro-specific traversal.

### 4. Refactor old call sites

Replace direct logic in:
- `sql_rag/bank_import.py::_is_already_posted` → calls `check_for_duplicate`.
- `sql_rag/opera_sql_import.py::check_duplicate_before_posting` → calls `check_for_duplicate`.
- `sql_rag/bank_duplicates.py::find_duplicates` "exact" match path → calls `check_for_duplicate` for the cashbook portion.
- Opera 3 mirrors: `bank_import_opera3.py`, `opera3_foxpro_import.py`.

The old functions become thin wrappers maintaining their existing return contracts (so we don't have to refactor every caller). Internally they delegate to the new function.

### 5. Test suite

`tests/test_duplicate_check.py`:

For each combination:
- 7 actions (sales_receipt, sales_refund, purchase_payment, purchase_refund, nominal_payment, nominal_receipt, bank_transfer)
- 2 directions (positive amount, negative amount)
- 4 ledger states:
  - No matching cashbook entry, no matching ledger row → NOT duplicate
  - Matching cashbook of correct type → DUPLICATE
  - Matching cashbook of WRONG sign → NOT duplicate (the fix from today)
  - No cashbook, but matching credit note in ledger of correct refund type → NOT duplicate, advisory='allocation_target'

That's roughly 7 × 2 × 4 = 56 explicit test cases. Plus regression tests for each historical bug:
- `pt_ref` typo (now `pt_trref`)
- `st_ref` typo (now `st_trref`)
- ABS-on-ABS sign blindness
- `bank_duplicates "exact" match` sign blindness
- The Cloudsis P051 case (refund vs receipt direction)

Tests use fixture data, not live Opera. The DataSource protocol is mocked.

Plus integration tests: against a real test database (or a SQLite shim that mirrors Opera's schema), end-to-end through `import-from-pdf`.

## Data flow (single transaction)

```
1. Import-from-pdf calls check_for_duplicate(action='sales_refund', signed=-198, ...)
2. check_for_duplicate computes window, signs
3. Looks up aentry: at_type=3, signed_pence=-19800 in window
   → if exists not excluded → DUPLICATE
4. Falls through to reference check if applicable
5. Falls through to advisory check on stran type='F'
   → if exists → return is_duplicate=False, advisory='allocation_target'
6. Else: not duplicate
```

## Error handling

- Action not in `ACTION_TYPE_MAP`: raises `ValueError` with the action name. No silent fall-through.
- DataSource query failure: propagated to caller; not swallowed.
- Schema typo at any column: caught by the schema validator at PR time (depends on the validator spec).

## Testing strategy

- 56 explicit unit tests for the action × direction × state matrix.
- Regression tests for every historical bug fixed today.
- Integration test against a fixture database mimicking Opera's schema (no live Opera dependency).
- Existing live-data smoke tests retained (run against Cloudsis if available).

## Migration plan

1. Write tests for the new module (TDD).
2. Implement `check_for_duplicate` + the SE DataSource.
3. Refactor `_is_already_posted`, `check_duplicate_before_posting`, `find_duplicates "exact"` — old functions delegate.
4. Run full test suite; existing tests must still pass.
5. Implement Opera 3 DataSource.
6. Refactor Opera 3 mirrors.
7. Live smoke test against Cloudsis Monzo data (the case from today).
8. Deprecate (and eventually remove) the old call-site logic.

## Done criteria

- [ ] `sql_rag/duplicate_check.py` module exists, fully tested.
- [ ] All 56 matrix test cases pass.
- [ ] All historical-bug regression tests pass.
- [ ] Old call sites delegate to the new function (no duplicated logic).
- [ ] Opera 3 mirrors do the same via Opera3DataSource.
- [ ] Documentation in `apps/core/docs/opera_knowledge_base.md` and central KB explains the function and its action-type mapping.

## Out of scope (separate spec)

- **Refund auto-allocation:** the advisory result (`allocation_target`) opens the door to auto-allocating new refunds against existing credit notes. That's a workflow change, not part of this consolidation.

## Risks / failure modes

- **Refactor blast radius:** the old call sites have evolved over time and may have site-specific quirks. **Mitigation:** keep old function signatures as wrappers; only the *internals* change. Existing callers continue to work.
- **Performance:** running a single function for every duplicate check might add overhead. **Mitigation:** the function is pure Python + SQL; should match or beat the current scattered logic.
- **Edge cases not caught by 56 tests:** the matrix may miss combinations. **Mitigation:** when a real bug surfaces, add a regression test before fixing — same TDD discipline as everywhere else in the project.
