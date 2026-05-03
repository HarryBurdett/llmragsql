# Period-Reconciled Function — Design Spec

**Status:** Draft for review
**Date:** 2026-05-03
**Author:** Claude (per Charlie Burdett's mandate to fix accumulated issues)

## Goal

Replace the **four scattered "is this period reconciled?" heuristics** across the bank-reconcile flow with a **single function** that gives the same answer everywhere. Eliminate the bug class where one path says "reconciled, hide it" while another says "imported, show it" — and the user sees inconsistent UI.

## Why

Today's session fixed the same fundamental bug **in four places**:

| # | Location | What its check did |
|---|---|---|
| 1 | `scan-all-banks` auto-promote (line 7631) | `closing == nk_recbal` → mark reconciled |
| 2 | `imported-for-reconciliation` auto-mark (line 1992) | `closing == nk_recbal` → mark reconciled |
| 3 | Step 5 chain filter, "closing matches reconciled opening of later" | balance-equality heuristic |
| 4 | Step 5 chain filter, "closing equals rec_bal" | balance-equality heuristic |

Each had **the same wrong assumption: balance match = reconciled**. Each got patched to add a period-aware aentry check. But the four sites still have **independent code copies** — the next bug will need patching four times again.

After today's fixes, two-stage logic emerged as the right rule:
1. **Historical match:** statement closing matches an `aentry.ae_recbal` from a closed reconcile batch AND closing < `nk_recbal` → done.
2. **Period-aware:** else if closing equals `nk_recbal`, count unreconciled aentries in the period; zero means done.

This logic is subtle and easy to get wrong, which is exactly why it must live in one function.

## Constraints (must hold)

- **Single source of truth:** one function. Every consumer calls it.
- **Conservative default:** if the function can't verify (missing period info, SQL error), it returns `STATUS_UNKNOWN` and consumers MUST treat unknown as "show, don't auto-promote".
- **Pure read:** the function never writes to Opera. It reads `aentry`, `nbank`. Returns a status enum.
- **Testable without live Opera:** uses a DataSource protocol so unit tests run against fixtures.
- **Extensible:** easy to add new checks (e.g. supplier-statement reconciliation reuse) without touching consumers.

## Architecture

```
┌──────────────────────────────────────────────────┐
│ Consumers:                                       │
│   scan-all-banks auto-promote loop               │
│   imported-for-reconciliation auto-mark loop     │
│   Step 5 chain filter (unchained pass)           │
│   Step 5 chain filter (closing-equals-rec_bal)   │
│   future: per-statement status query             │
└──────┬───────────────────────────────────────────┘
       │ all call
       ▼
┌──────────────────────────────────────────────────┐
│ sql_rag/period_reconciliation.py (new)           │
│                                                  │
│   class PeriodReconciliationStatus(Enum):        │
│       FULLY_RECONCILED                           │
│       PARTIALLY_RECONCILED                       │
│       NOT_RECONCILED                             │
│       UNKNOWN                                    │
│                                                  │
│   @dataclass                                     │
│   class PeriodReconciliationResult:              │
│       status: PeriodReconciliationStatus         │
│       unreconciled_count: int | None             │
│       matched_historical_boundary: bool          │
│       reason: str  # human-readable              │
│                                                  │
│   def check_period_reconciled(                   │
│       data_source: DataSource,                   │
│       bank_code: str,                            │
│       period_start: date,                        │
│       period_end: date,                          │
│       statement_closing: float,                  │
│       current_rec_bal: float,                    │
│   ) -> PeriodReconciliationResult: ...           │
└──────────────────────────────────────────────────┘
```

## Components

### 1. `sql_rag/period_reconciliation.py` (new)

The function. Logic:

```
1. If period_start or period_end is None → UNKNOWN, "no period info"
2. If statement_closing is None → UNKNOWN, "no closing balance"
3. Query historical batch boundaries on this bank:
       SELECT DISTINCT ae_recbal
       FROM aentry WITH (NOLOCK)
       WHERE ae_acnt = ? AND ae_reclnum > 0 AND ae_recbal IS NOT NULL
   Build set of historical_recbals (in pence, integer-rounded).
4. statement_closing_pence = round(statement_closing * 100)
5. current_rec_bal_pence = round(current_rec_bal * 100)
6. If statement_closing_pence < current_rec_bal_pence
   AND statement_closing_pence in historical_recbals:
       → FULLY_RECONCILED, matched_historical_boundary=True
7. If abs(statement_closing_pence - current_rec_bal_pence) <= 1:  # at the current frontier
   Query unreconciled count:
       SELECT COUNT(*) FROM aentry WITH (NOLOCK)
       WHERE ae_acnt = ?
         AND ae_lstdate BETWEEN ? AND ?
         AND (ae_reclnum IS NULL OR ae_reclnum = 0)
   If count == 0 → FULLY_RECONCILED
   If count > 0 → PARTIALLY_RECONCILED
8. statement_closing > current_rec_bal → NOT_RECONCILED, "future statement"
9. statement_closing < current_rec_bal AND not in historical → NOT_RECONCILED,
   "balance doesn't match any boundary — investigate"
```

Each return value carries a `reason` string for logging.

### 2. `DataSource` protocol

```python
class DataSource(Protocol):
    def query_historical_recbals(self, bank_code: str) -> set[int]: ...
    def query_unreconciled_in_period(
        self, bank_code: str, period_start: date, period_end: date
    ) -> int: ...
```

Two implementations: `OperaSEDataSource` and `Opera3DataSource`.

### 3. Refactor consumers

**Each consumer becomes:**

```python
result = check_period_reconciled(
    data_source=ds,
    bank_code=code,
    period_start=stmt['period_start'],
    period_end=stmt['period_end'],
    statement_closing=stmt['closing_balance'],
    current_rec_bal=bank['reconciled_balance'],
)

if result.status == PeriodReconciliationStatus.FULLY_RECONCILED:
    # auto-promote / hide
    logger.info(f"... {result.reason}")
elif result.status == PeriodReconciliationStatus.PARTIALLY_RECONCILED:
    # keep visible, label "partial"
elif result.status == PeriodReconciliationStatus.UNKNOWN:
    # conservative — keep visible
elif result.status == PeriodReconciliationStatus.NOT_RECONCILED:
    # keep visible
```

No more inline SQL, no more scattered heuristics.

### 4. Test suite

`tests/test_period_reconciliation.py`:

- **Fixture-based DataSource** with controllable historical_recbals and unreconciled counts.
- Test matrix:
  - Closing matches a historical boundary AND < rec_bal → FULLY_RECONCILED
  - Closing matches rec_bal AND zero unreconciled in period → FULLY_RECONCILED
  - Closing matches rec_bal AND nonzero unreconciled → PARTIALLY_RECONCILED
  - Closing > rec_bal (future statement) → NOT_RECONCILED
  - Closing < rec_bal but no historical match → NOT_RECONCILED ("orphan")
  - period_start = None → UNKNOWN
  - DataSource raises → UNKNOWN, error logged
- **Regression tests:**
  - The Cloudsis April Monzo case (closing == rec_bal, 9 unreconciled → PARTIALLY_RECONCILED).
  - The Cloudsis March Monzo case (closing < rec_bal, matches batch 207 → FULLY_RECONCILED, even though 2 orphan contras exist).

## Data flow (single statement)

```
1. scan-all-banks loops over folder PDFs for BB005
2. For statement S, calls check_period_reconciled(BB005, S.period_start, S.period_end, S.closing, BB005.rec_bal)
3. Function queries Opera (1 query for historical recbals, 1 for unreconciled count if needed)
4. Returns FULLY_RECONCILED → consumer hides S
   PARTIALLY_RECONCILED → consumer keeps S visible with status='imported'
   NOT_RECONCILED → consumer shows S as 'ready' or 'pending'
   UNKNOWN → consumer keeps visible (conservative)
```

## Error handling

- DataSource query fails → return UNKNOWN with reason; consumer treats as "show".
- Bank balance None → UNKNOWN.
- Period bounds inverted (start > end) → ValueError. Caller bug.

## Performance

The function does at most 2 queries per statement check. Historical_recbals can be cached per scan-all-banks invocation (stable for the duration of one scan). Add a simple `lru_cache` keyed by (bank_code, scan_id) on the DataSource side.

## Testing strategy

- Unit tests with fixture DataSource: full matrix above.
- Regression tests against the historical bugs.
- Integration test (optional, gated on test database) against the actual Opera schema.

## Migration plan

1. Write tests (TDD).
2. Implement `period_reconciliation.py` + SE DataSource.
3. Replace consumer #1 (scan-all-banks auto-promote): inline logic → single function call.
4. Run tests + manual scan; verify identical behaviour.
5. Replace consumer #2 (imported-for-reconciliation).
6. Replace consumer #3 (Step 5 chain — historical-boundary path).
7. Replace consumer #4 (Step 5 chain — closing-equals-rec_bal path).
8. Implement Opera 3 DataSource if any of these have Opera 3 mirrors.
9. Verify on Cloudsis Monzo data (today's case).

## Done criteria

- [ ] `sql_rag/period_reconciliation.py` exists and is fully tested.
- [ ] All 4 consumers call it; no inline period-reconciled SQL remains in `routes.py`.
- [ ] All matrix test cases pass.
- [ ] Cloudsis Monzo regression: April shows `imported`, March hidden, on first scan after fix.
- [ ] Documentation in central KB (`business-rules/period-reconciliation.md`) explains the rule.

## Out of scope

- Updating Opera's `nk_recbal` based on a period-reconciled determination. The function is **read-only**; writing is a separate concern.

## Risks / failure modes

- **Caching subtlety:** historical_recbals can change between scans (a new reconcile commits). Cache must be scoped per scan, not global. **Mitigation:** explicit scan-scoped cache; documented.
- **Same blast radius as duplicate-check consolidation:** changing the four consumers may surface ancillary bugs. **Mitigation:** TDD; one consumer at a time; verify against live data after each.
