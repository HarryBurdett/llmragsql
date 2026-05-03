"""Single source of truth for 'is this statement period fully reconciled?'.

Replaces four scattered heuristics across scan-all-banks and
imported-for-reconciliation. See
docs/superpowers/specs/2026-05-03-period-reconciled-function-design.md.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Optional, Protocol, runtime_checkable

__all__ = [
    "PeriodReconciliationStatus",
    "PeriodReconciliationResult",
    "DataSource",
]


class PeriodReconciliationStatus(Enum):
    """Status returned by check_period_reconciled.

    Caller contract:
      - FULLY_RECONCILED: hide / auto-promote — the period is finished.
      - PARTIALLY_RECONCILED: show with a 'partial' label — operator
        still has work to do.
      - NOT_RECONCILED: show as ready — fresh statement to process.
      - UNKNOWN: SHOW. Never silently auto-promote on UNKNOWN. The
        function returns UNKNOWN when inputs are missing or a query
        failed; consumers must default to visible.
    """
    FULLY_RECONCILED = "fully_reconciled"
    PARTIALLY_RECONCILED = "partially_reconciled"
    NOT_RECONCILED = "not_reconciled"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class PeriodReconciliationResult:
    """Result of check_period_reconciled.

    Invariants (test-pinned):
      - unreconciled_count is None when the count was not queried
        (Stage 1 path or any UNKNOWN/NOT_RECONCILED path).
      - unreconciled_count is 0 only when explicitly queried and zero —
        i.e. status is FULLY_RECONCILED via Stage 2.
      - matched_historical_boundary is True only when status is
        FULLY_RECONCILED via Stage 1.
      - reason is always a non-empty human-readable string.

    Frozen: results are values, never mutated. Hashable for caching.
    """
    status: PeriodReconciliationStatus
    unreconciled_count: Optional[int]
    matched_historical_boundary: bool
    reason: str


@runtime_checkable
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

    # Stages 3 placeholder (task 4 implements)
    return PeriodReconciliationResult(
        status=PeriodReconciliationStatus.UNKNOWN,
        unreconciled_count=None,
        matched_historical_boundary=False,
        reason="not implemented — task 4 pending (closing != rec_bal, no historical match)",
    )
