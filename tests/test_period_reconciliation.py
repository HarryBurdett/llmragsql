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
