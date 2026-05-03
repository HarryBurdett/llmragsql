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
