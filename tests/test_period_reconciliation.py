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
