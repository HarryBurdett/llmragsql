"""Unit tests for sql_rag.period_reconciliation."""
from __future__ import annotations

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


def test_status_enum_value_strings_are_stable():
    """The .value strings are the natural form for logs and JSON
    payloads. Lock them so a future rename doesn't silently break
    downstream consumers.
    """
    from sql_rag.period_reconciliation import PeriodReconciliationStatus
    assert PeriodReconciliationStatus.FULLY_RECONCILED.value == "fully_reconciled"
    assert PeriodReconciliationStatus.PARTIALLY_RECONCILED.value == "partially_reconciled"
    assert PeriodReconciliationStatus.NOT_RECONCILED.value == "not_reconciled"
    assert PeriodReconciliationStatus.UNKNOWN.value == "unknown"


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


def test_datasource_protocol_signatures_pinned():
    """Pin the DataSource protocol method NAMES, signatures, AND
    runtime-checkable behaviour. Catches signature drift between SE and
    Opera 3 implementations.
    """
    import inspect
    from sql_rag.period_reconciliation import DataSource

    # query_historical_recbals(self, bank_code) -> set[int]
    sig = inspect.signature(DataSource.query_historical_recbals)
    params = list(sig.parameters)
    assert params == ['self', 'bank_code'], (
        f"query_historical_recbals signature drifted: {params}"
    )

    # query_unreconciled_in_period(self, bank_code, period_start, period_end) -> int
    sig = inspect.signature(DataSource.query_unreconciled_in_period)
    params = list(sig.parameters)
    assert params == ['self', 'bank_code', 'period_start', 'period_end'], (
        f"query_unreconciled_in_period signature drifted: {params}"
    )

    # Runtime-checkable: a class with the right method names satisfies
    # isinstance; one missing a method does not.
    class _Good:
        def query_historical_recbals(self, bank_code): return set()
        def query_unreconciled_in_period(self, bank_code, period_start, period_end): return 0
    class _Bad:
        def query_historical_recbals(self, bank_code): return set()
        # missing query_unreconciled_in_period
    assert isinstance(_Good(), DataSource)
    assert not isinstance(_Bad(), DataSource)
