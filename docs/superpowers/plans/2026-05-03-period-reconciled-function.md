# Period-Reconciled Function Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the four scattered "is this statement period reconciled?" heuristics with a single tested function that gives the same answer everywhere.

**Architecture:** A new pure-logic module `sql_rag/period_reconciliation.py` exposes `check_period_reconciled(...)` returning a `PeriodReconciliationResult` enum. The function uses a `DataSource` protocol (Opera SE + Opera 3 implementations) so it can be tested with fixtures. Four call sites in `apps/bank_reconcile/api/routes.py` — auto-promote in scan-all-banks, auto-mark in imported-for-reconciliation, two paths in Step 5 chain filter — all delegate to this one function.

**Tech Stack:** Python 3.9, pyodbc (Opera SE), pytest, no new external deps.

**Source spec:** `docs/superpowers/specs/2026-05-03-period-reconciled-function-design.md`

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `sql_rag/period_reconciliation.py` | **create** | The function, the enum, the result dataclass, the DataSource protocol |
| `sql_rag/period_reconciliation_se.py` | **create** | OperaSEDataSource implementation (uses SQLConnector) |
| `sql_rag/period_reconciliation_o3.py` | **create** | Opera3DataSource implementation (uses FoxPro reader) |
| `tests/test_period_reconciliation.py` | **create** | Unit tests with fixture DataSource |
| `tests/test_period_reconciliation_regression.py` | **create** | Regression tests for the historical bugs (Cloudsis April + March cases) |
| `apps/bank_reconcile/api/routes.py` | **modify** | Replace 4 call sites with delegation to the new function |
| `apps/core/docs/opera_knowledge_base.md` | **modify** | Document the function and the rule |
| `~/opera-knowledge-ref/packages/opera-knowledge/business-rules/period-reconciliation.md` | **create** | Central KB documentation |

---

## Task 1: Define result types and protocol

**Files:**
- Create: `sql_rag/period_reconciliation.py` (skeleton — types only, no logic yet)
- Test: `tests/test_period_reconciliation.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_period_reconciliation.py
"""Unit tests for sql_rag.period_reconciliation."""
from sql_rag.period_reconciliation import (
    PeriodReconciliationStatus,
    PeriodReconciliationResult,
    DataSource,
)


def test_status_enum_has_required_values():
    """The status enum must have exactly the four documented values."""
    expected = {'FULLY_RECONCILED', 'PARTIALLY_RECONCILED',
                'NOT_RECONCILED', 'UNKNOWN'}
    actual = {s.name for s in PeriodReconciliationStatus}
    assert actual == expected


def test_result_dataclass_fields():
    """Result carries status, count, boundary flag, and reason."""
    r = PeriodReconciliationResult(
        status=PeriodReconciliationStatus.FULLY_RECONCILED,
        unreconciled_count=0,
        matched_historical_boundary=True,
        reason="closing matches a historical batch boundary",
    )
    assert r.status is PeriodReconciliationStatus.FULLY_RECONCILED
    assert r.unreconciled_count == 0
    assert r.matched_historical_boundary is True
    assert "boundary" in r.reason


def test_datasource_protocol_methods_exist():
    """DataSource protocol declares the expected methods."""
    methods = {m for m in dir(DataSource) if not m.startswith('_')}
    assert 'query_historical_recbals' in methods
    assert 'query_unreconciled_in_period' in methods
```

- [ ] **Step 2: Run test to verify it fails**

Run: `source venv/bin/activate && python -m pytest tests/test_period_reconciliation.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'sql_rag.period_reconciliation'`

- [ ] **Step 3: Write minimal implementation**

```python
# sql_rag/period_reconciliation.py
"""Single source of truth for 'is this statement period fully reconciled?'.

Replaces four scattered heuristics across scan-all-banks and
imported-for-reconciliation. See
docs/superpowers/specs/2026-05-03-period-reconciled-function-design.md.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Optional, Protocol


class PeriodReconciliationStatus(Enum):
    FULLY_RECONCILED = "fully_reconciled"
    PARTIALLY_RECONCILED = "partially_reconciled"
    NOT_RECONCILED = "not_reconciled"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class PeriodReconciliationResult:
    status: PeriodReconciliationStatus
    unreconciled_count: Optional[int]
    matched_historical_boundary: bool
    reason: str


class DataSource(Protocol):
    """Protocol for the data lookups the function needs.

    Implemented by OperaSEDataSource (SQL Server / pyodbc) and
    Opera3DataSource (FoxPro DBF). Tests use a fixture implementation.
    """

    def query_historical_recbals(self, bank_code: str) -> set[int]:
        """Return the set of historical reconcile-batch boundary balances
        for this bank, in pence (integer-rounded).

        A "boundary" is any aentry.ae_recbal where ae_reclnum > 0. Each
        such balance was once the reconciled balance of a committed
        batch.
        """
        ...

    def query_unreconciled_in_period(
        self, bank_code: str, period_start: date, period_end: date
    ) -> int:
        """Return count of aentry rows on this bank whose ae_lstdate is in
        [period_start, period_end] AND ae_reclnum is null or zero.
        """
        ...
```

- [ ] **Step 4: Run test to verify it passes**

Run: `source venv/bin/activate && python -m pytest tests/test_period_reconciliation.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add sql_rag/period_reconciliation.py tests/test_period_reconciliation.py
git commit -m "feat(period-recon): scaffold types, protocol, status enum

First commit toward consolidating the four scattered period-reconciliation
heuristics into one tested function. This commit lands the public types
only — no logic yet. Tests pin the enum values, dataclass shape, and
DataSource protocol surface so future refactors don't drift.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Implement happy-path "fully reconciled via historical match"

**Files:**
- Modify: `sql_rag/period_reconciliation.py`
- Modify: `tests/test_period_reconciliation.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_period_reconciliation.py`:

```python
from datetime import date

class _FakeDataSource:
    """In-memory DataSource for unit tests."""
    def __init__(
        self,
        historical_recbals: set[int] | None = None,
        unreconciled_count: int = 0,
        raise_on_recbals: Exception | None = None,
        raise_on_unrec: Exception | None = None,
    ):
        self._recbals = historical_recbals or set()
        self._unrec = unreconciled_count
        self._raise_on_recbals = raise_on_recbals
        self._raise_on_unrec = raise_on_unrec

    def query_historical_recbals(self, bank_code):
        if self._raise_on_recbals:
            raise self._raise_on_recbals
        return self._recbals

    def query_unreconciled_in_period(self, bank_code, ps, pe):
        if self._raise_on_unrec:
            raise self._raise_on_unrec
        return self._unrec


def test_fully_reconciled_when_closing_matches_historical_boundary_and_below_recbal():
    """The Cloudsis March case: closing £50,377.38 matches a historical
    batch boundary (T100000030 in batch 207) AND closing < current rec_bal
    £82,557.56 → FULLY_RECONCILED, matched_historical_boundary=True.
    """
    from sql_rag.period_reconciliation import (
        check_period_reconciled,
        PeriodReconciliationStatus,
    )
    ds = _FakeDataSource(
        historical_recbals={5037738},  # £50,377.38 in pence
    )
    result = check_period_reconciled(
        data_source=ds,
        bank_code="BB005",
        period_start=date(2026, 3, 1),
        period_end=date(2026, 3, 31),
        statement_closing=50377.38,
        current_rec_bal=82557.56,
    )
    assert result.status is PeriodReconciliationStatus.FULLY_RECONCILED
    assert result.matched_historical_boundary is True
    assert "boundary" in result.reason.lower()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `source venv/bin/activate && python -m pytest tests/test_period_reconciliation.py::test_fully_reconciled_when_closing_matches_historical_boundary_and_below_recbal -v`
Expected: FAIL with `ImportError: cannot import name 'check_period_reconciled'`

- [ ] **Step 3: Write minimal implementation**

Append to `sql_rag/period_reconciliation.py`:

```python
def _to_pence(amount: Optional[float]) -> Optional[int]:
    """Convert pounds to pence, integer-rounded. None passes through."""
    if amount is None:
        return None
    return int(round(amount * 100))


def check_period_reconciled(
    *,
    data_source: DataSource,
    bank_code: str,
    period_start: Optional[date],
    period_end: Optional[date],
    statement_closing: Optional[float],
    current_rec_bal: Optional[float],
) -> PeriodReconciliationResult:
    """Single source of truth for 'is this statement period fully reconciled?'.

    Two-stage rule:
      1. Historical match: if statement_closing matches a known reconcile-
         batch boundary on this bank AND closing < current_rec_bal, the
         period is from a prior closed cycle.
      2. Period-aware: if closing equals current_rec_bal, count
         unreconciled aentries in the period; zero means done.

    Conservative default: returns UNKNOWN if inputs are missing or a
    DataSource query fails. Callers MUST treat UNKNOWN as "show, don't
    auto-promote" to match the project's no-quick-fixes mandate.

    See docs/superpowers/specs/2026-05-03-period-reconciled-function-design.md
    for the full design.
    """
    # Stage 0: input validation
    if statement_closing is None:
        return PeriodReconciliationResult(
            status=PeriodReconciliationStatus.UNKNOWN,
            unreconciled_count=None,
            matched_historical_boundary=False,
            reason="no statement closing balance — cannot determine state",
        )

    # Stage 1: historical match
    closing_pence = _to_pence(statement_closing)
    rec_bal_pence = _to_pence(current_rec_bal)
    historical: set[int]
    try:
        historical = data_source.query_historical_recbals(bank_code)
    except Exception as e:
        return PeriodReconciliationResult(
            status=PeriodReconciliationStatus.UNKNOWN,
            unreconciled_count=None,
            matched_historical_boundary=False,
            reason=f"could not query historical recbals: {e}",
        )

    if (
        rec_bal_pence is not None
        and closing_pence < rec_bal_pence
        and closing_pence in historical
    ):
        return PeriodReconciliationResult(
            status=PeriodReconciliationStatus.FULLY_RECONCILED,
            unreconciled_count=None,
            matched_historical_boundary=True,
            reason=(
                f"closing £{statement_closing:,.2f} matches a historical "
                f"batch boundary AND is below current rec_bal "
                f"£{current_rec_bal:,.2f} (prior closed cycle)"
            ),
        )

    # Stage 2 (placeholder for now — Task 3 implements)
    # Stage 3 (placeholder for now — Task 4 implements)
    return PeriodReconciliationResult(
        status=PeriodReconciliationStatus.UNKNOWN,
        unreconciled_count=None,
        matched_historical_boundary=False,
        reason="not implemented — task 3+ pending",
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `source venv/bin/activate && python -m pytest tests/test_period_reconciliation.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add sql_rag/period_reconciliation.py tests/test_period_reconciliation.py
git commit -m "feat(period-recon): stage 1 — historical-boundary match

Implements the first of three branches: when a statement's closing
balance matches a previously-committed batch boundary on this bank
AND that closing is below the current nk_recbal, the period is
fully reconciled (it's a prior closed cycle). The Cloudsis March
case is the canonical test (closing £50,377.38 = T100000030 recbal
in batch 207, while current rec_bal is £82,557.56 in batch 209).

Stages 2 (period-aware) and 3 (not-reconciled / future) follow.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Implement "partially / fully reconciled at current rec_bal"

**Files:**
- Modify: `sql_rag/period_reconciliation.py`
- Modify: `tests/test_period_reconciliation.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_period_reconciliation.py`:

```python
def test_partially_reconciled_when_closing_equals_recbal_with_unreconciled_aentries():
    """The Cloudsis April case today: closing £82,557.56 == rec_bal,
    9 aentry rows in period are still unreconciled → PARTIALLY_RECONCILED.
    """
    from sql_rag.period_reconciliation import (
        check_period_reconciled,
        PeriodReconciliationStatus,
    )
    ds = _FakeDataSource(
        historical_recbals={5037738, 8255756},  # March + April closings
        unreconciled_count=9,
    )
    result = check_period_reconciled(
        data_source=ds,
        bank_code="BB005",
        period_start=date(2026, 4, 1),
        period_end=date(2026, 4, 28),
        statement_closing=82557.56,
        current_rec_bal=82557.56,
    )
    assert result.status is PeriodReconciliationStatus.PARTIALLY_RECONCILED
    assert result.unreconciled_count == 9
    assert "9" in result.reason


def test_fully_reconciled_when_closing_equals_recbal_and_zero_unreconciled():
    """A fresh fully-reconciled state: closing = rec_bal, every aentry
    in the period is reconciled.
    """
    from sql_rag.period_reconciliation import (
        check_period_reconciled,
        PeriodReconciliationStatus,
    )
    ds = _FakeDataSource(
        historical_recbals={8255756},
        unreconciled_count=0,
    )
    result = check_period_reconciled(
        data_source=ds,
        bank_code="BB005",
        period_start=date(2026, 4, 1),
        period_end=date(2026, 4, 30),
        statement_closing=82557.56,
        current_rec_bal=82557.56,
    )
    assert result.status is PeriodReconciliationStatus.FULLY_RECONCILED
    assert result.unreconciled_count == 0


def test_unknown_when_period_missing_at_recbal_boundary():
    """If closing == rec_bal but period info is missing, conservative
    UNKNOWN — caller must keep statement visible.
    """
    from sql_rag.period_reconciliation import (
        check_period_reconciled,
        PeriodReconciliationStatus,
    )
    ds = _FakeDataSource(historical_recbals={8255756})
    result = check_period_reconciled(
        data_source=ds,
        bank_code="BB005",
        period_start=None,
        period_end=None,
        statement_closing=82557.56,
        current_rec_bal=82557.56,
    )
    assert result.status is PeriodReconciliationStatus.UNKNOWN
    assert "period" in result.reason.lower()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `source venv/bin/activate && python -m pytest tests/test_period_reconciliation.py -v`
Expected: 3 new tests FAIL with assertion errors (function returns "not implemented" UNKNOWN)

- [ ] **Step 3: Write minimal implementation**

In `sql_rag/period_reconciliation.py`, replace the placeholder section (after the historical-match block, before the final `return UNKNOWN`) with:

```python
    # Stage 2: closing equals current rec_bal — query period
    if rec_bal_pence is not None and abs(closing_pence - rec_bal_pence) <= 1:
        if period_start is None or period_end is None:
            return PeriodReconciliationResult(
                status=PeriodReconciliationStatus.UNKNOWN,
                unreconciled_count=None,
                matched_historical_boundary=False,
                reason="closing matches rec_bal but period bounds missing",
            )
        try:
            unrec = data_source.query_unreconciled_in_period(
                bank_code, period_start, period_end
            )
        except Exception as e:
            return PeriodReconciliationResult(
                status=PeriodReconciliationStatus.UNKNOWN,
                unreconciled_count=None,
                matched_historical_boundary=False,
                reason=f"could not query unreconciled count: {e}",
            )
        if unrec == 0:
            return PeriodReconciliationResult(
                status=PeriodReconciliationStatus.FULLY_RECONCILED,
                unreconciled_count=0,
                matched_historical_boundary=False,
                reason=(
                    f"closing £{statement_closing:,.2f} equals rec_bal AND "
                    f"every aentry in period {period_start}..{period_end} "
                    f"is reconciled"
                ),
            )
        return PeriodReconciliationResult(
            status=PeriodReconciliationStatus.PARTIALLY_RECONCILED,
            unreconciled_count=unrec,
            matched_historical_boundary=False,
            reason=(
                f"closing £{statement_closing:,.2f} equals rec_bal but "
                f"{unrec} aentry rows in period are still unreconciled"
            ),
        )
```

Then remove the trailing `return PeriodReconciliationResult(... not implemented ...)` placeholder — replaced in Task 4.

- [ ] **Step 4: Run tests to verify they pass**

Run: `source venv/bin/activate && python -m pytest tests/test_period_reconciliation.py -v`
Expected: PASS (7 tests). Note: the test relying on the trailing UNKNOWN return for "not implemented" will fail — that's expected, replaced in Task 4.

If the existing `test_unknown_when_period_missing_at_recbal_boundary` test is not yet in place because the function falls through without hitting the rec_bal branch, ensure the rec_bal branch fires by setting `current_rec_bal` equal to `statement_closing` in that test. (It already does in the test as written.)

- [ ] **Step 5: Commit**

```bash
git add sql_rag/period_reconciliation.py tests/test_period_reconciliation.py
git commit -m "feat(period-recon): stage 2 — period-aware check at current rec_bal

When a statement's closing equals nk_recbal, ambiguous: could be
in-progress or just-completed. Resolve by querying unreconciled
aentry count in the statement period. Zero → FULLY_RECONCILED;
non-zero → PARTIALLY_RECONCILED with the count surfaced.

Cloudsis April Monzo regression: closing £82,557.56 matches rec_bal,
9 unreconciled rows in 2026-04-01..2026-04-28 → PARTIALLY_RECONCILED.
The scan-all-banks consumer (next task) keeps the statement visible.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Implement "not reconciled" branches and finalise function

**Files:**
- Modify: `sql_rag/period_reconciliation.py`
- Modify: `tests/test_period_reconciliation.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_period_reconciliation.py`:

```python
def test_not_reconciled_when_closing_above_recbal():
    """A future statement: closing > current rec_bal."""
    from sql_rag.period_reconciliation import (
        check_period_reconciled,
        PeriodReconciliationStatus,
    )
    ds = _FakeDataSource(historical_recbals={5037738})
    result = check_period_reconciled(
        data_source=ds,
        bank_code="BB005",
        period_start=date(2026, 5, 1),
        period_end=date(2026, 5, 31),
        statement_closing=85000.00,
        current_rec_bal=82557.56,
    )
    assert result.status is PeriodReconciliationStatus.NOT_RECONCILED
    assert "future" in result.reason.lower() or "above" in result.reason.lower()


def test_not_reconciled_when_closing_below_recbal_no_historical_match():
    """An orphan: closing < rec_bal but doesn't match any historical
    boundary. Likely a malformed statement or genuine gap in history.
    """
    from sql_rag.period_reconciliation import (
        check_period_reconciled,
        PeriodReconciliationStatus,
    )
    ds = _FakeDataSource(historical_recbals={1000000, 2000000})  # neither match
    result = check_period_reconciled(
        data_source=ds,
        bank_code="BB005",
        period_start=date(2026, 4, 1),
        period_end=date(2026, 4, 30),
        statement_closing=12345.67,
        current_rec_bal=82557.56,
    )
    assert result.status is PeriodReconciliationStatus.NOT_RECONCILED
    assert "boundary" in result.reason.lower() or "investigate" in result.reason.lower()


def test_unknown_when_recbals_query_fails():
    """If we can't reach Opera, never auto-promote."""
    from sql_rag.period_reconciliation import (
        check_period_reconciled,
        PeriodReconciliationStatus,
    )
    ds = _FakeDataSource(raise_on_recbals=RuntimeError("Opera unreachable"))
    result = check_period_reconciled(
        data_source=ds,
        bank_code="BB005",
        period_start=date(2026, 4, 1),
        period_end=date(2026, 4, 30),
        statement_closing=82557.56,
        current_rec_bal=82557.56,
    )
    assert result.status is PeriodReconciliationStatus.UNKNOWN
    assert "Opera unreachable" in result.reason


def test_unknown_when_unreconciled_query_fails_at_recbal_boundary():
    """If we hit Stage 2 but the unreconciled query fails, UNKNOWN."""
    from sql_rag.period_reconciliation import (
        check_period_reconciled,
        PeriodReconciliationStatus,
    )
    ds = _FakeDataSource(
        historical_recbals={8255756},
        raise_on_unrec=RuntimeError("query timeout"),
    )
    result = check_period_reconciled(
        data_source=ds,
        bank_code="BB005",
        period_start=date(2026, 4, 1),
        period_end=date(2026, 4, 30),
        statement_closing=82557.56,
        current_rec_bal=82557.56,
    )
    assert result.status is PeriodReconciliationStatus.UNKNOWN
    assert "timeout" in result.reason
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `source venv/bin/activate && python -m pytest tests/test_period_reconciliation.py -v`
Expected: 4 new tests FAIL — they hit the trailing UNKNOWN "not implemented" placeholder.

- [ ] **Step 3: Write minimal implementation**

In `sql_rag/period_reconciliation.py`, replace the trailing placeholder return with:

```python
    # Stage 3: closing != rec_bal and not a historical match
    if rec_bal_pence is not None and closing_pence > rec_bal_pence:
        return PeriodReconciliationResult(
            status=PeriodReconciliationStatus.NOT_RECONCILED,
            unreconciled_count=None,
            matched_historical_boundary=False,
            reason=(
                f"closing £{statement_closing:,.2f} is above current rec_bal "
                f"£{current_rec_bal:,.2f} — future statement, awaiting reconcile"
            ),
        )

    # closing < rec_bal but not a historical boundary: orphan / data gap
    return PeriodReconciliationResult(
        status=PeriodReconciliationStatus.NOT_RECONCILED,
        unreconciled_count=None,
        matched_historical_boundary=False,
        reason=(
            f"closing £{statement_closing:,.2f} is below rec_bal but doesn't "
            f"match any historical boundary — investigate (orphan or gap)"
        ),
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `source venv/bin/activate && python -m pytest tests/test_period_reconciliation.py -v`
Expected: PASS (11 tests)

- [ ] **Step 5: Commit**

```bash
git add sql_rag/period_reconciliation.py tests/test_period_reconciliation.py
git commit -m "feat(period-recon): stage 3 — not-reconciled and error paths

Final branches:
  - closing > rec_bal: future statement, NOT_RECONCILED
  - closing < rec_bal, no historical match: orphan, NOT_RECONCILED
  - DataSource query failure: UNKNOWN (caller must show statement)
  - period bounds missing at rec_bal boundary: UNKNOWN

The function is now total — every input combination produces a
deterministic result. 11 unit tests, full coverage of the documented
matrix.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Implement Opera SE DataSource

**Files:**
- Create: `sql_rag/period_reconciliation_se.py`
- Modify: `tests/test_period_reconciliation.py` (smoke test only — no live DB)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_period_reconciliation.py`:

```python
def test_se_datasource_construction_and_protocol():
    """OperaSEDataSource exists, takes a SQLConnector, satisfies the protocol."""
    from sql_rag.period_reconciliation_se import OperaSEDataSource

    # We don't need a real connector for this test — just check the
    # class exists, takes the connector, and exposes the protocol methods.
    class _StubConnector:
        def execute_query(self, q):
            raise NotImplementedError
    ds = OperaSEDataSource(_StubConnector())
    assert hasattr(ds, 'query_historical_recbals')
    assert hasattr(ds, 'query_unreconciled_in_period')
```

- [ ] **Step 2: Run test to verify it fails**

Run: `source venv/bin/activate && python -m pytest tests/test_period_reconciliation.py::test_se_datasource_construction_and_protocol -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'sql_rag.period_reconciliation_se'`

- [ ] **Step 3: Write minimal implementation**

```python
# sql_rag/period_reconciliation_se.py
"""Opera SE DataSource for the period-reconciliation function.

Wraps SQLConnector queries against aentry to satisfy the DataSource
protocol used by sql_rag.period_reconciliation.check_period_reconciled.
"""
from __future__ import annotations

from datetime import date
from typing import Any


class OperaSEDataSource:
    """DataSource for Opera SQL SE.

    Parameters
    ----------
    sql_connector : SQLConnector-like
        Anything with an `execute_query(sql) -> DataFrame-like` method.
        We only call execute_query; pyodbc / SQLAlchemy are concerns of
        the caller.
    """

    def __init__(self, sql_connector: Any) -> None:
        self._sql = sql_connector

    def query_historical_recbals(self, bank_code: str) -> set[int]:
        """Return the set of historical reconcile-batch boundary balances
        on this bank, in pence (integer-rounded).
        """
        df = self._sql.execute_query(f"""
            SELECT DISTINCT ae_recbal
            FROM aentry WITH (NOLOCK)
            WHERE ae_acnt = '{bank_code}'
              AND ae_reclnum > 0
              AND ae_recbal IS NOT NULL
        """)
        if df is None or df.empty:
            return set()
        return {
            int(round(float(v)))
            for v in df['ae_recbal']
            if v is not None
        }

    def query_unreconciled_in_period(
        self, bank_code: str, period_start: date, period_end: date
    ) -> int:
        """Count aentry rows in the period for this bank with no reclnum."""
        df = self._sql.execute_query(f"""
            SELECT COUNT(*) AS n
            FROM aentry WITH (NOLOCK)
            WHERE ae_acnt = '{bank_code}'
              AND ae_lstdate BETWEEN '{period_start.isoformat()}' AND '{period_end.isoformat()}'
              AND (ae_reclnum IS NULL OR ae_reclnum = 0)
        """)
        if df is None or df.empty:
            return 0
        return int(df.iloc[0]['n'])
```

- [ ] **Step 4: Run test to verify it passes**

Run: `source venv/bin/activate && python -m pytest tests/test_period_reconciliation.py -v`
Expected: PASS (12 tests)

- [ ] **Step 5: Commit**

```bash
git add sql_rag/period_reconciliation_se.py tests/test_period_reconciliation.py
git commit -m "feat(period-recon): Opera SE DataSource implementation

OperaSEDataSource wraps SQLConnector queries against aentry to satisfy
the protocol used by check_period_reconciled. SQL uses signed-pence
recbals (integer-rounded) so the comparison is exact, no float drift.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Implement Opera 3 DataSource

**Files:**
- Create: `sql_rag/period_reconciliation_o3.py`
- Modify: `tests/test_period_reconciliation.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_period_reconciliation.py`:

```python
def test_o3_datasource_construction_and_protocol():
    """Opera3DataSource exists, takes a FoxPro reader, satisfies the protocol."""
    from sql_rag.period_reconciliation_o3 import Opera3DataSource

    class _StubReader:
        def read_table(self, name):
            return []
    ds = Opera3DataSource(_StubReader())
    assert hasattr(ds, 'query_historical_recbals')
    assert hasattr(ds, 'query_unreconciled_in_period')


def test_o3_datasource_filters_aentry_by_bank_and_reclnum():
    """O3 reader returns all aentry rows; the DataSource must filter them.
    Uses an in-memory list to avoid touching real DBF files.
    """
    from sql_rag.period_reconciliation_o3 import Opera3DataSource
    from datetime import date

    aentry_rows = [
        {'ae_acnt': 'BB005', 'ae_reclnum': 207.0, 'ae_recbal': 5037738.0,
         'ae_lstdate': date(2026, 3, 27)},
        {'ae_acnt': 'BB005', 'ae_reclnum': 0.0, 'ae_recbal': None,
         'ae_lstdate': date(2026, 4, 16)},
        {'ae_acnt': 'BB010', 'ae_reclnum': 100.0, 'ae_recbal': 12345.0,
         'ae_lstdate': date(2026, 3, 1)},  # different bank, must be excluded
    ]

    class _StubReader:
        def read_table(self, name):
            assert name == 'aentry'
            return aentry_rows

    ds = Opera3DataSource(_StubReader())
    rec = ds.query_historical_recbals('BB005')
    assert rec == {5037738}

    unrec = ds.query_unreconciled_in_period(
        'BB005', date(2026, 4, 1), date(2026, 4, 28)
    )
    assert unrec == 1  # the 04-16 row with reclnum=0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `source venv/bin/activate && python -m pytest tests/test_period_reconciliation.py -v`
Expected: 2 new tests FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Write minimal implementation**

```python
# sql_rag/period_reconciliation_o3.py
"""Opera 3 (FoxPro DBF) DataSource for the period-reconciliation function.

Wraps a FoxPro reader (anything with read_table(name) -> list[dict]) to
satisfy the DataSource protocol.
"""
from __future__ import annotations

from datetime import date
from typing import Any, Iterable


def _row_get(row: Any, *keys: str) -> Any:
    """Look up the first non-None value among case-insensitive variants
    of the given keys. Opera 3 DBF readers return either dict-like rows
    with uppercase or lowercase keys, depending on driver.
    """
    for k in keys:
        for variant in (k, k.upper(), k.lower()):
            if isinstance(row, dict) and variant in row:
                v = row[variant]
                if v is not None:
                    return v
            elif hasattr(row, variant):
                v = getattr(row, variant)
                if v is not None:
                    return v
    return None


class Opera3DataSource:
    """DataSource for Opera 3 (FoxPro DBF).

    Parameters
    ----------
    reader : object with `read_table(name) -> Iterable[row]`
        The Opera 3 FoxPro reader. Rows may be dicts or namedtuple-like.
    """

    def __init__(self, reader: Any) -> None:
        self._reader = reader

    def _aentry_rows(self) -> Iterable:
        return self._reader.read_table('aentry')

    def query_historical_recbals(self, bank_code: str) -> set[int]:
        out: set[int] = set()
        for row in self._aentry_rows():
            acnt = _row_get(row, 'ae_acnt')
            if not acnt or str(acnt).strip() != bank_code:
                continue
            reclnum = _row_get(row, 'ae_reclnum') or 0
            if float(reclnum) <= 0:
                continue
            recbal = _row_get(row, 'ae_recbal')
            if recbal is None:
                continue
            out.add(int(round(float(recbal))))
        return out

    def query_unreconciled_in_period(
        self, bank_code: str, period_start: date, period_end: date
    ) -> int:
        n = 0
        for row in self._aentry_rows():
            acnt = _row_get(row, 'ae_acnt')
            if not acnt or str(acnt).strip() != bank_code:
                continue
            reclnum = _row_get(row, 'ae_reclnum') or 0
            if float(reclnum) > 0:
                continue
            lstdate = _row_get(row, 'ae_lstdate')
            if lstdate is None:
                continue
            # Normalise to date
            if hasattr(lstdate, 'date'):
                lstdate = lstdate.date()
            if period_start <= lstdate <= period_end:
                n += 1
        return n
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `source venv/bin/activate && python -m pytest tests/test_period_reconciliation.py -v`
Expected: PASS (14 tests)

- [ ] **Step 5: Commit**

```bash
git add sql_rag/period_reconciliation_o3.py tests/test_period_reconciliation.py
git commit -m "feat(period-recon): Opera 3 DataSource implementation

Opera3DataSource wraps a FoxPro DBF reader. Same protocol as the SE
implementation, full parity per project rule. Handles dict-vs-attr
row access and case-insensitive field names so it works with any
of the readers in sql_rag/.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: Replace consumer #1 — scan-all-banks auto-promote

**Files:**
- Modify: `apps/bank_reconcile/api/routes.py` (the auto-promote loop, currently lines ~7631–7720)
- Test: existing tests must still pass; `tests/test_period_reconciliation.py` already covers the function logic.

- [ ] **Step 1: Write the failing test**

Add a callsite-level integration test that exercises the auto-promote loop without spinning up the FastAPI app. Append to `tests/test_period_reconciliation.py`:

```python
def test_scan_all_banks_auto_promote_uses_function():
    """The scan-all-banks auto-promote loop must delegate to
    check_period_reconciled — not maintain its own inline SQL.
    Read the source file and assert the inline SQL pattern is gone.
    """
    from pathlib import Path
    routes_path = Path(__file__).resolve().parent.parent / "apps" / "bank_reconcile" / "api" / "routes.py"
    src = routes_path.read_text(encoding='utf-8')

    # The auto-promote section must call the new function
    assert "check_period_reconciled" in src, (
        "scan-all-banks auto-promote should call check_period_reconciled"
    )

    # No more inline 'COUNT(*)' against aentry inside the auto-promote section
    # (heuristic check — refine if false positive). The marker comment in the
    # code identifies the section.
    auto_promote_start = src.find("Auto-promote imported statements")
    auto_promote_end = src.find("# Remove reconciled statements from bank lists", auto_promote_start)
    assert auto_promote_start != -1 and auto_promote_end != -1, (
        "Auto-promote section markers must remain so future maintainers "
        "can find it"
    )
    section = src[auto_promote_start:auto_promote_end]
    assert "COUNT(*)" not in section, (
        "Auto-promote section should no longer issue inline COUNT(*) — "
        "delegate to check_period_reconciled"
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `source venv/bin/activate && python -m pytest tests/test_period_reconciliation.py::test_scan_all_banks_auto_promote_uses_function -v`
Expected: FAIL — current code has inline `COUNT(*)` in the auto-promote section.

- [ ] **Step 3: Write minimal implementation**

In `apps/bank_reconcile/api/routes.py`, find the auto-promote section (search for `# Auto-promote imported statements` — currently around line 7631). Replace the entire body of that block with a call to `check_period_reconciled`.

```python
        # Auto-promote imported statements where the period is fully
        # reconciled per check_period_reconciled — single source of truth
        # for the "is this period done?" question. See
        # sql_rag/period_reconciliation.py.
        try:
            from sql_rag.period_reconciliation import (
                check_period_reconciled,
                PeriodReconciliationStatus,
            )
            from sql_rag.period_reconciliation_se import OperaSEDataSource
            from datetime import date as _date_type, datetime as _dt

            ds = OperaSEDataSource(sql_connector) if sql_connector else None
            for code, bank in all_banks.items():
                rec_bal = bank.get('reconciled_balance')
                if rec_bal is None or ds is None:
                    continue
                for stmt in list(bank['statements']):
                    if not stmt.get('is_imported'):
                        continue
                    period_start = stmt.get('period_start')
                    period_end = stmt.get('period_end')
                    # Normalise period strings → date
                    def _to_date(v):
                        if v is None:
                            return None
                        if isinstance(v, _date_type) and not isinstance(v, _dt):
                            return v
                        if isinstance(v, _dt):
                            return v.date()
                        if isinstance(v, str):
                            try:
                                return _dt.fromisoformat(v.replace('Z', '+00:00')).date()
                            except ValueError:
                                try:
                                    return _date_type.fromisoformat(v[:10])
                                except ValueError:
                                    return None
                        return None
                    ps = _to_date(period_start)
                    pe = _to_date(period_end)
                    result = check_period_reconciled(
                        data_source=ds,
                        bank_code=code,
                        period_start=ps,
                        period_end=pe,
                        statement_closing=stmt.get('closing_balance'),
                        current_rec_bal=rec_bal,
                    )
                    if result.status is PeriodReconciliationStatus.FULLY_RECONCILED:
                        fn = stmt.get('filename', '')
                        logger.info(
                            f"Scan cleanup: auto-marking '{fn}' as reconciled — "
                            f"{result.reason}"
                        )
                        final_rec_filenames.add(fn)
                        try:
                            email_storage.mark_statement_reconciled(
                                filename=fn,
                                reconciled_count=0,
                                bank_code=code,
                            )
                        except Exception:
                            pass
                    elif result.status is PeriodReconciliationStatus.PARTIALLY_RECONCILED:
                        logger.info(
                            f"Scan cleanup: NOT auto-marking '{stmt.get('filename','?')}' — "
                            f"{result.reason}"
                        )
                    elif result.status is PeriodReconciliationStatus.UNKNOWN:
                        logger.info(
                            f"Scan cleanup: NOT auto-marking '{stmt.get('filename','?')}' — "
                            f"{result.reason}"
                        )
                    # NOT_RECONCILED: keep visible silently — normal case
        except Exception as promo_err:
            logger.warning(f"Auto-promote scan cleanup failed: {promo_err}")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `source venv/bin/activate && python -m pytest tests/test_period_reconciliation.py -v`
Expected: PASS (15 tests). Run the rest of the suite too:

Run: `source venv/bin/activate && python -m pytest tests/ -x -q`
Expected: PASS — no regressions.

- [ ] **Step 5: Commit**

```bash
git add apps/bank_reconcile/api/routes.py tests/test_period_reconciliation.py
git commit -m "refactor(scan): scan-all-banks auto-promote uses check_period_reconciled

Replace ~50 lines of inline period-reconciliation logic with a delegate
to check_period_reconciled. Same behaviour, single source of truth.
The auto-promote section is now ~30 lines of glue (date normalisation,
status routing, logging) — no SQL, no two-stage logic to maintain in
multiple places.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: Replace consumer #2 — imported-for-reconciliation auto-mark

**Files:**
- Modify: `apps/bank_reconcile/api/routes.py` (the auto-mark loop in `/api/statement-files/imported-for-reconciliation`, currently around lines ~1992–2070)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_period_reconciliation.py`:

```python
def test_imported_for_reconciliation_uses_function():
    """The /api/statement-files/imported-for-reconciliation auto-mark
    loop must also delegate to check_period_reconciled.
    """
    from pathlib import Path
    routes_path = Path(__file__).resolve().parent.parent / "apps" / "bank_reconcile" / "api" / "routes.py"
    src = routes_path.read_text(encoding='utf-8')

    # Locate the section by its existing comment marker
    start = src.find("# Add Opera reconciled balance info and auto-mark reconciled statements")
    if start == -1:
        # Section may have been renamed during refactor — fall back to
        # endpoint marker
        start = src.find('imported-for-reconciliation')
    assert start != -1, "imported-for-reconciliation section must be findable"

    # The endpoint should call check_period_reconciled — appears at least once
    # globally in the file (already true after Task 7), but we also want
    # this specific section to use it. Heuristic: count usages.
    n = src.count("check_period_reconciled(")
    assert n >= 2, (
        f"Expected ≥2 call sites of check_period_reconciled; found {n}. "
        "imported-for-reconciliation must also delegate."
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `source venv/bin/activate && python -m pytest tests/test_period_reconciliation.py::test_imported_for_reconciliation_uses_function -v`
Expected: FAIL — only 1 callsite after Task 7.

- [ ] **Step 3: Write minimal implementation**

Find the auto-mark loop in `apps/bank_reconcile/api/routes.py` near the `/api/statement-files/imported-for-reconciliation` endpoint (currently around lines 1992–2070, with a comment "Add Opera reconciled balance info and auto-mark reconciled statements"). Replace with:

```python
                    # Add Opera reconciled balance info and delegate the
                    # "is this period reconciled?" decision to
                    # check_period_reconciled — single source of truth.
                    from sql_rag.period_reconciliation import (
                        check_period_reconciled,
                        PeriodReconciliationStatus,
                    )
                    from sql_rag.period_reconciliation_se import OperaSEDataSource
                    from datetime import date as _date_type, datetime as _dt

                    ds = OperaSEDataSource(sql_connector)

                    def _to_date(v):
                        if v is None:
                            return None
                        if isinstance(v, _date_type) and not isinstance(v, _dt):
                            return v
                        if isinstance(v, _dt):
                            return v.date()
                        if isinstance(v, str):
                            try:
                                return _dt.fromisoformat(v.replace('Z', '+00:00')).date()
                            except ValueError:
                                try:
                                    return _date_type.fromisoformat(v[:10])
                                except ValueError:
                                    return None
                        return None

                    for stmt in statements:
                        bc = stmt.get('bank_code', '').strip()
                        rec_bal = rec_balances.get(bc)
                        if rec_bal is None:
                            continue
                        stmt['opera_reconciled_balance'] = rec_bal
                        if stmt.get('is_reconciled'):
                            continue

                        result = check_period_reconciled(
                            data_source=ds,
                            bank_code=bc,
                            period_start=_to_date(stmt.get('period_start')),
                            period_end=_to_date(stmt.get('period_end')),
                            statement_closing=stmt.get('closing_balance'),
                            current_rec_bal=rec_bal,
                        )
                        if result.status is PeriodReconciliationStatus.FULLY_RECONCILED:
                            try:
                                email_storage.mark_statement_reconciled(
                                    filename=stmt['filename'],
                                    bank_code=bc,
                                )
                                stmt['is_reconciled'] = 1
                                logger.info(
                                    f"Auto-marked statement '{stmt['filename']}' as reconciled — "
                                    f"{result.reason}"
                                )
                            except Exception as mark_err:
                                logger.warning(f"Failed to auto-mark statement reconciled: {mark_err}")
                        else:
                            logger.info(
                                f"imported-for-reconciliation: NOT auto-marking "
                                f"'{stmt.get('filename','?')}' — {result.reason}"
                            )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `source venv/bin/activate && python -m pytest tests/test_period_reconciliation.py -v`
Expected: PASS (16 tests).

Run: `source venv/bin/activate && python -m pytest tests/ -x -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add apps/bank_reconcile/api/routes.py tests/test_period_reconciliation.py
git commit -m "refactor(scan): imported-for-reconciliation auto-mark uses check_period_reconciled

Second consumer migrated. Same delegation pattern as scan-all-banks
auto-promote (Task 7). Two of four call sites now consolidated.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 9: Replace consumers #3 + #4 — Step 5 chain filter

**Files:**
- Modify: `apps/bank_reconcile/api/routes.py` (the Step 5 chain filter in scan-all-banks, currently around lines ~7548–7619)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_period_reconciliation.py`:

```python
def test_step_5_chain_filter_uses_function():
    """The Step 5 chain filter must also delegate the
    'is this period reconciled?' question to the function.
    """
    from pathlib import Path
    routes_path = Path(__file__).resolve().parent.parent / "apps" / "bank_reconcile" / "api" / "routes.py"
    src = routes_path.read_text(encoding='utf-8')

    # After this task, ≥3 call sites total
    n = src.count("check_period_reconciled(")
    assert n >= 3, (
        f"Expected ≥3 call sites of check_period_reconciled; found {n}. "
        "Step 5 chain filter must also delegate."
    )

    # The chain section's inline historical-recbals query must be gone
    chain_start = src.find("# Pre-compute historical batch boundary balances for this bank")
    if chain_start == -1:
        chain_start = src.find("Step 5 chain")
    assert chain_start != -1, "Step 5 chain section must be findable"

    chain_end = src.find("bank['statements'] = stmts", chain_start)
    if chain_end == -1:
        chain_end = chain_start + 5000
    section = src[chain_start:chain_end]
    assert "SELECT DISTINCT ae_recbal" not in section, (
        "Step 5 chain section should no longer issue its own historical-"
        "recbals query — delegate to check_period_reconciled"
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `source venv/bin/activate && python -m pytest tests/test_period_reconciliation.py::test_step_5_chain_filter_uses_function -v`
Expected: FAIL — chain filter still has inline `SELECT DISTINCT ae_recbal`.

- [ ] **Step 3: Write minimal implementation**

In `apps/bank_reconcile/api/routes.py`, locate the Step 5 chain filter (search for `Step 5 chain` log line; the body is roughly lines 7548–7619 currently). Replace the inline `hist_recbals` precomputation AND the unchained classification block with a single delegation per statement:

```python
            # Filter out statements that are already fully reconciled per
            # check_period_reconciled — the single source of truth that
            # combines historical-boundary match + period-aware aentry
            # check. See sql_rag/period_reconciliation.py.
            from sql_rag.period_reconciliation import (
                check_period_reconciled,
                PeriodReconciliationStatus,
            )
            from sql_rag.period_reconciliation_se import OperaSEDataSource
            from datetime import date as _date_type, datetime as _dt

            ds = OperaSEDataSource(sql_connector) if sql_connector else None

            def _to_date(v):
                if v is None:
                    return None
                if isinstance(v, _date_type) and not isinstance(v, _dt):
                    return v
                if isinstance(v, _dt):
                    return v.date()
                if isinstance(v, str):
                    try:
                        return _dt.fromisoformat(v.replace('Z', '+00:00')).date()
                    except ValueError:
                        try:
                            return _date_type.fromisoformat(v[:10])
                        except ValueError:
                            return None
                return None

            chained = []
            unchained = []
            for s in stmts:
                if ds is None or rec_bal is None:
                    unchained.append(s)
                    continue
                result = check_period_reconciled(
                    data_source=ds,
                    bank_code=code,
                    period_start=_to_date(s.get('period_start')),
                    period_end=_to_date(s.get('period_end')),
                    statement_closing=s.get('closing_balance'),
                    current_rec_bal=rec_bal,
                )
                if result.status is PeriodReconciliationStatus.FULLY_RECONCILED:
                    chained.append(s)  # done — will be filtered out below
                    logger.info(
                        f"Step 5 chain: filtering {s.get('filename','?')} — "
                        f"{result.reason}"
                    )
                else:
                    unchained.append(s)

            # `chained` is the set we hide; `unchained` we keep
            stmts = unchained
            logger.info(
                f"Step 5 chain: {code} chained={len(chained)} unchained={len(unchained)}"
            )
```

This replaces both the historical-boundary loop and the closing-equals-rec_bal special case with one delegation per statement.

- [ ] **Step 4: Run tests to verify they pass**

Run: `source venv/bin/activate && python -m pytest tests/test_period_reconciliation.py -v`
Expected: PASS (17 tests).

Run: `source venv/bin/activate && python -m pytest tests/ -x -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add apps/bank_reconcile/api/routes.py tests/test_period_reconciliation.py
git commit -m "refactor(scan): Step 5 chain filter uses check_period_reconciled

Third and fourth consumers migrated in one pass — both the
historical-boundary classification and the closing-equals-rec_bal
special case in Step 5 chain are now a single delegation per
statement. The four scattered heuristics that have been giving us
divergent answers all day are now one function call. End of bug
class.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 10: Live verification against Cloudsis

**Files:**
- Manual run; no file changes.

- [ ] **Step 1: Restart the API**

```bash
pkill -f "uvicorn api.main" 2>/dev/null; sleep 2
source venv/bin/activate && nohup uvicorn api.main:app --reload --host 0.0.0.0 --port 8000 > /tmp/api.log 2>&1 & disown
sleep 4
curl -s http://localhost:8000/api/health
```

Expected output: `{"status":"healthy","service":"sql-rag-api"}`

- [ ] **Step 2: Reset April 2944 import to known-good state**

```bash
sqlite3 /Users/maccb/llmragsql/data/cloudsis/core/email_data.db "
UPDATE bank_statement_imports SET is_reconciled = 0, reconciled_date = NULL, target_system = 'opera_se'
WHERE filename LIKE '%2026-04-01-2026-04-28_2944%';
SELECT id, filename, target_system, is_reconciled FROM bank_statement_imports WHERE filename LIKE '%2944%';
"
```

Expected: id, filename, `opera_se`, 0.

- [ ] **Step 3: Hit scan-all-banks via curl**

```bash
curl -s -X POST -H 'Content-Type: application/json' -d '{"username":"admin","password":"Harry"}' http://localhost:8000/api/auth/force-clear-session > /dev/null
TOKEN=$(curl -s -X POST -c /tmp/c.txt -H 'Content-Type: application/json' -d '{"username":"admin","password":"Harry"}' http://localhost:8000/api/auth/login | python3 -c "import sys,json; print(json.load(sys.stdin).get('token',''))")
curl -s -b /tmp/c.txt -H "Authorization: Bearer $TOKEN" "http://localhost:8000/api/bank-import/scan-all-banks?days_back=30&validate_balances=true" | python3 -c "
import sys, json
d = json.load(sys.stdin)
banks = d.get('banks', {})
bb005 = banks.get('BB005', {})
stmts = bb005.get('statements', [])
print(f'BB005 statements: {len(stmts)}')
for s in stmts:
    print(f'  status={s.get(\"status\"):<22}  open=£{s.get(\"opening_balance\")}  close=£{s.get(\"closing_balance\")}  file={s.get(\"filename\",\"?\")[:65]}')
"
```

Expected: exactly one BB005 statement returned, the April 2944, with `status=imported`. March is filtered (closing matches a prior batch boundary).

- [ ] **Step 4: Confirm is_reconciled stays at 0 across multiple frontend calls**

```bash
# Hit the second auto-mark endpoint too
curl -s -b /tmp/c.txt -H "Authorization: Bearer $TOKEN" "http://localhost:8000/api/statement-files/imported-for-reconciliation" > /dev/null
curl -s -b /tmp/c.txt -H "Authorization: Bearer $TOKEN" "http://localhost:8000/api/statement-files/imported-for-reconciliation?include_reconciled=true" > /dev/null
sqlite3 /Users/maccb/llmragsql/data/cloudsis/core/email_data.db "SELECT is_reconciled FROM bank_statement_imports WHERE filename LIKE '%2944%'"
```

Expected: `0`.

- [ ] **Step 5: Confirm log shows the function being used**

```bash
grep -E "check_period_reconciled|matches a historical batch boundary|aentry rows in period are still unreconciled" /tmp/api.log | tail -10
```

Expected: log entries showing the function-derived reasons (e.g. "matches a historical batch boundary AND is below current rec_bal" for March, "X aentry rows in period are still unreconciled" for April).

- [ ] **Step 6: Mark task done — no commit (manual verification only)**

---

## Task 11: Knowledge base updates (mandatory per CLAUDE.md)

**Files:**
- Modify: `apps/core/docs/opera_knowledge_base.md`
- Create: `~/opera-knowledge-ref/packages/opera-knowledge/business-rules/period-reconciliation.md`

- [ ] **Step 1: Append a section to the local KB**

Append to `apps/core/docs/opera_knowledge_base.md`:

```markdown

## Period-Reconciled Decision Function

**Single source of truth** for "is this statement period fully reconciled in Opera?". Lives at `sql_rag/period_reconciliation.py`. Used by:

- `scan-all-banks` auto-promote
- `imported-for-reconciliation` auto-mark
- `Step 5 chain filter` in scan-all-banks

Two-stage rule:
1. **Historical match:** statement closing equals an `aentry.ae_recbal` from a closed reconcile batch AND closing < current `nk_recbal` → `FULLY_RECONCILED`. The period was finalised in a prior cycle.
2. **Period-aware:** if closing equals current `nk_recbal`, count aentries in the period with `ae_reclnum = 0`. Zero → `FULLY_RECONCILED`. Non-zero → `PARTIALLY_RECONCILED`.

Conservative default: returns `UNKNOWN` if inputs are missing or a query fails. Callers MUST treat `UNKNOWN` as "show the statement, don't auto-promote".

**Critical for Monzo-style banks** whose statement filenames change on every download — there's no permanent filename-based reconciled marker, so we derive reconciliation state from Opera's ae_recbal history.

**Do not duplicate this logic.** If you find inline period-reconciliation SQL anywhere, replace it with a `check_period_reconciled` call.
```

- [ ] **Step 2: Pull and write the central KB file**

```bash
cd ~/opera-knowledge-ref && git pull --rebase origin main
cat > ~/opera-knowledge-ref/packages/opera-knowledge/business-rules/period-reconciliation.md <<'EOF'
# Period-Reconciliation Decision

The single canonical answer to **"is this statement period fully reconciled in Opera?"**. All consumers MUST call the function `check_period_reconciled` in `sql_rag/period_reconciliation.py`. No inline SQL or balance-equality heuristics.

## The Rule

A two-stage check:

### Stage 1 — Historical-boundary match

If `statement_closing == aentry.ae_recbal` for any aentry on this bank with `ae_reclnum > 0`, AND `statement_closing < nk_recbal`, the statement is from a prior closed reconcile cycle. Status: `FULLY_RECONCILED`.

This handles banks like Monzo whose statement filenames change on every download — no permanent filename marker, so we derive reconciliation from Opera's batch-boundary history.

### Stage 2 — Period-aware count

If `statement_closing == nk_recbal`, ambiguous (could be the in-progress reconcile or just-finished). Query:

```sql
SELECT COUNT(*) FROM aentry WITH (NOLOCK)
WHERE ae_acnt = ?
  AND ae_lstdate BETWEEN ? AND ?
  AND (ae_reclnum IS NULL OR ae_reclnum = 0)
```

Zero → `FULLY_RECONCILED`. Non-zero → `PARTIALLY_RECONCILED`.

### Stage 3 — Other

- `closing > nk_recbal`: future statement, `NOT_RECONCILED`.
- `closing < nk_recbal` but no historical match: orphan / data gap, `NOT_RECONCILED`.

### UNKNOWN paths

Conservative defaults:
- Period bounds missing → `UNKNOWN`.
- DataSource query failure → `UNKNOWN`.

**Caller contract:** treat `UNKNOWN` as "show the statement, don't auto-promote". Never silently advance state on `UNKNOWN`.

## Why this combination

Balance-only matches mislead: partial reconciles bump `nk_recbal` to the closing without committing every entry's `ae_reclnum`.

Period-only checks mislead in the other direction: orphan contra pairs that net to zero never appear on a statement and stay `ae_reclnum = 0` forever, making fully-reconciled periods look incomplete.

The two-stage rule is the smallest combination that handles both classes correctly.

## Consumers

- `scan-all-banks` auto-promote
- `imported-for-reconciliation` auto-mark
- `Step 5 chain filter` in scan-all-banks

If a new consumer needs this answer, call the function. Do not reimplement.
EOF
cd ~/opera-knowledge-ref && git add packages/opera-knowledge/business-rules/period-reconciliation.md
git commit -m "Document period-reconciliation decision function

The single canonical answer to 'is this statement period fully
reconciled?'. All consumers must call sql_rag/period_reconciliation.py
::check_period_reconciled — no inline SQL, no balance-equality
heuristics.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
git push origin main
```

- [ ] **Step 3: Commit local KB**

```bash
git -C /Users/maccb/llmragsql add apps/core/docs/opera_knowledge_base.md
git -C /Users/maccb/llmragsql commit -m "$(cat <<'EOF'
docs(kb): document the period-reconciliation function

Mirrors the central KB entry at
opera-knowledge-ref/packages/opera-knowledge/business-rules/
period-reconciliation.md.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
git -C /Users/maccb/llmragsql push
```

- [ ] **Step 4: Verify both KBs reach their remotes**

```bash
echo "=== Local KB head ==="
git -C /Users/maccb/llmragsql log --oneline -1 -- apps/core/docs/opera_knowledge_base.md
echo "=== Central KB head ==="
git -C ~/opera-knowledge-ref log --oneline -1 -- packages/opera-knowledge/business-rules/period-reconciliation.md
echo "=== Central remote check ==="
git -C ~/opera-knowledge-ref status -sb | head -3
```

Expected: both show fresh commits; central status shows "up to date with origin/main".

---

## Done Criteria (final review at end of plan)

- [ ] `sql_rag/period_reconciliation.py` exists, fully tested (11+ unit tests).
- [ ] `sql_rag/period_reconciliation_se.py` exists, takes a SQLConnector.
- [ ] `sql_rag/period_reconciliation_o3.py` exists, takes a FoxPro reader.
- [ ] All 4 call sites in `apps/bank_reconcile/api/routes.py` delegate to `check_period_reconciled` — no inline period-reconciliation SQL remains.
- [ ] Three call-site grep tests verify the consolidation didn't regress.
- [ ] Existing test suite passes (`pytest tests/ -x -q` clean).
- [ ] Live test on Cloudsis: April 2944 shows as `imported`, March 5202 hidden, `is_reconciled` stays 0 across multiple endpoint hits.
- [ ] Local KB updated and committed.
- [ ] Central KB file at `business-rules/period-reconciliation.md` exists, committed and pushed.
