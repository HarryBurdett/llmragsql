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
