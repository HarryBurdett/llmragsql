"""Behavioural tests for the F9 VAT-reconciliation helpers.

Audit cross-cutting F9: reconcile_vat at 661 lines was the last
huge handler. The shared shape (vat-codes-with-rates, repeated
zvtran/nvat aggregations, per-account NL movement) is now in
apps.balance_check.logic.vat_reconcile.
"""
from __future__ import annotations

from datetime import date as _date

import pytest

from apps.balance_check.logic.vat_reconcile import (
    NlMovementResult,
    VatAggregate,
    VatCodesResult,
    _coerce_rate_date,
    _pick_applicable_rate,
    fetch_nl_vat_movements,
    fetch_nvat_aggregate,
    fetch_vat_codes_with_rates,
    fetch_zvtran_aggregate,
)


class _FakeConn:
    def __init__(self, canned):
        self.canned = list(canned)
        self.queries = []

    def execute_query(self, sql):
        self.queries.append(sql)
        return self.canned.pop(0) if self.canned else []


# ====================================================================
# _pick_applicable_rate / _coerce_rate_date
# ====================================================================


def test_pick_rate_both_dates_picks_most_recent_effective():
    """Both rate1 and rate2 effective; the later effective date wins."""
    assert _pick_applicable_rate(20.0, 17.5, _date(2026, 1, 1), _date(2026, 4, 1), _date(2026, 5, 1)) == 17.5


def test_pick_rate_both_dates_only_rate1_effective():
    assert _pick_applicable_rate(20.0, 17.5, _date(2026, 1, 1), _date(2026, 6, 1), _date(2026, 4, 1)) == 20.0


def test_pick_rate_both_dates_only_rate2_effective():
    assert _pick_applicable_rate(20.0, 17.5, _date(2026, 8, 1), _date(2026, 1, 1), _date(2026, 4, 1)) == 17.5


def test_pick_rate_only_rate2_date_present():
    assert _pick_applicable_rate(20.0, 17.5, None, _date(2026, 1, 1), _date(2026, 4, 1)) == 17.5


def test_pick_rate_no_dates_falls_back_to_rate1():
    assert _pick_applicable_rate(20.0, 17.5, None, None, _date(2026, 4, 1)) == 20.0


def test_coerce_rate_date_handles_pandas_timestamp():
    """Pandas Timestamps have a .date() method."""
    class _T:
        def date(self): return _date(2026, 4, 1)
    assert _coerce_rate_date(_T()) == _date(2026, 4, 1)


def test_coerce_rate_date_handles_date_directly():
    d = _date(2026, 4, 1)
    assert _coerce_rate_date(d) == d


def test_coerce_rate_date_handles_none():
    assert _coerce_rate_date(None) is None


def test_coerce_rate_date_handles_nan():
    """float('nan') != float('nan'), so the NaN check catches it."""
    assert _coerce_rate_date(float('nan')) is None


# ====================================================================
# fetch_vat_codes_with_rates
# ====================================================================


def test_vat_codes_classifies_output_vs_input():
    rows = [
        {'tx_code': 'S1', 'tx_desc': 'Standard Output', 'tx_rate1': 20.0,
         'tx_rate1dy': None, 'tx_rate2': 0, 'tx_rate2dy': None,
         'tx_trantyp': 'S', 'tx_nominal': '2200'},
        {'tx_code': 'P1', 'tx_desc': 'Standard Input', 'tx_rate1': 20.0,
         'tx_rate1dy': None, 'tx_rate2': 0, 'tx_rate2dy': None,
         'tx_trantyp': 'P', 'tx_nominal': '2210'},
    ]
    conn = _FakeConn([rows])
    out = fetch_vat_codes_with_rates(conn, _date(2026, 4, 1))
    assert len(out.vat_codes) == 2
    assert out.output_nominal_accounts == {'2200'}
    assert out.input_nominal_accounts == {'2210'}


def test_vat_codes_strips_whitespace():
    rows = [{
        'tx_code': 'S1   ', 'tx_desc': 'Output  ', 'tx_rate1': 20.0,
        'tx_rate1dy': None, 'tx_rate2': 0, 'tx_rate2dy': None,
        'tx_trantyp': 'S  ', 'tx_nominal': '2200  ',
    }]
    conn = _FakeConn([rows])
    out = fetch_vat_codes_with_rates(conn, _date(2026, 4, 1))
    assert out.vat_codes[0]['code'] == 'S1'
    assert out.vat_codes[0]['nominal_account'] == '2200'


def test_vat_codes_handles_empty_table():
    conn = _FakeConn([[]])
    out = fetch_vat_codes_with_rates(conn, _date(2026, 4, 1))
    assert out.vat_codes == []
    assert out.output_nominal_accounts == set()
    assert out.input_nominal_accounts == set()


def test_vat_codes_filters_to_home_country_only():
    """SQL filter tx_ctrytyp = 'H' is in the query."""
    conn = _FakeConn([[]])
    fetch_vat_codes_with_rates(conn, _date(2026, 4, 1))
    assert "tx_ctrytyp = 'H'" in conn.queries[0]


def test_vat_codes_picks_rate2_when_more_recent():
    rows = [{
        'tx_code': 'S1', 'tx_desc': 'Standard',
        'tx_rate1': 20.0, 'tx_rate1dy': _date(2024, 1, 1),
        'tx_rate2': 17.5, 'tx_rate2dy': _date(2026, 1, 1),
        'tx_trantyp': 'S', 'tx_nominal': '2200',
    }]
    conn = _FakeConn([rows])
    out = fetch_vat_codes_with_rates(conn, _date(2026, 4, 1))
    assert out.vat_codes[0]['rate'] == 17.5


# ====================================================================
# fetch_zvtran_aggregate
# ====================================================================


def test_zvtran_aggregate_output_query_filters_by_vattype_and_done():
    conn = _FakeConn([[]])
    fetch_zvtran_aggregate(
        conn, vattype='S',
        quarter_start='2026-04-01', quarter_end='2026-06-30',
    )
    sql = conn.queries[0]
    assert "va_vattype = 'S'" in sql
    assert "va_done = 0" in sql
    assert "va_taxdate >= '2026-04-01'" in sql
    assert "va_taxdate <= '2026-06-30'" in sql


def test_zvtran_aggregate_sums_vat_and_lists_codes():
    rows = [
        {'vat_code': 'S1', 'transaction_count': 10, 'vat_amount': 1000.0, 'net_amount': 5000.0},
        {'vat_code': 'S2', 'transaction_count': 3, 'vat_amount': 60.0, 'net_amount': 300.0},
    ]
    conn = _FakeConn([rows])
    out = fetch_zvtran_aggregate(
        conn, vattype='S',
        quarter_start='2026-04-01', quarter_end='2026-06-30',
    )
    assert out.total_vat == 1060.0
    assert len(out.by_code) == 2
    assert out.by_code[0]['vat_code'] == 'S1'
    assert out.by_code[0]['vat_amount'] == 1000.0
    assert out.by_code[0]['net_amount'] == 5000.0


def test_zvtran_aggregate_omits_net_when_requested():
    rows = [{'vat_code': 'S1', 'transaction_count': 1, 'vat_amount': 100.0, 'net_amount': 500.0}]
    conn = _FakeConn([rows])
    out = fetch_zvtran_aggregate(
        conn, vattype='S',
        quarter_start='2026-01-01', quarter_end='2026-03-31',
        include_net=False,
    )
    assert 'net_amount' not in out.by_code[0]


def test_zvtran_aggregate_handles_empty_result():
    conn = _FakeConn([[]])
    out = fetch_zvtran_aggregate(
        conn, vattype='S',
        quarter_start='2026-04-01', quarter_end='2026-06-30',
    )
    assert out.total_vat == 0.0
    assert out.by_code == []


# ====================================================================
# fetch_nvat_aggregate
# ====================================================================


def test_nvat_aggregate_query_uses_nv_vattype_and_nv_date():
    conn = _FakeConn([[]])
    fetch_nvat_aggregate(
        conn, vattype='P',
        period_start='2026-04-01', period_end='2026-06-30',
    )
    sql = conn.queries[0]
    assert "nv_vattype = 'P'" in sql
    assert "nv_date >= '2026-04-01'" in sql
    assert "FROM nvat WITH (NOLOCK)" in sql


def test_nvat_aggregate_no_net_amount_field():
    """nvat.nv_trvalue isn't selected — only vat_amount."""
    rows = [{'vat_code': 'P1', 'transaction_count': 5, 'vat_amount': 250.0}]
    conn = _FakeConn([rows])
    out = fetch_nvat_aggregate(
        conn, vattype='P',
        period_start='2026-04-01', period_end='2026-06-30',
    )
    assert out.total_vat == 250.0
    assert 'net_amount' not in out.by_code[0]


# ====================================================================
# fetch_nl_vat_movements
# ====================================================================


def test_nl_movements_output_account_credits_feed_total():
    """Output account: credits feed output_total; debits ignored."""
    canned = [
        [{'debits': 0, 'credits': 1000.0, 'net': -1000.0, 'transaction_count': 5}],
        [{'description': 'Output VAT'}],
    ]
    conn = _FakeConn(canned)
    out = fetch_nl_vat_movements(
        conn,
        output_nominal_accounts={'2200'},
        input_nominal_accounts=set(),
        period_start='2026-04-01', period_end='2026-06-30',
    )
    assert out.output_total == 1000.0
    assert out.input_total == 0.0
    assert len(out.accounts) == 1
    assert out.accounts[0]['type'] == 'Output'


def test_nl_movements_input_account_debits_feed_total():
    """Input account: debits feed input_total; credits ignored."""
    canned = [
        [{'debits': 200.0, 'credits': 0, 'net': 200.0, 'transaction_count': 3}],
        [{'description': 'Input VAT'}],
    ]
    conn = _FakeConn(canned)
    out = fetch_nl_vat_movements(
        conn,
        output_nominal_accounts=set(),
        input_nominal_accounts={'2210'},
        period_start='2026-04-01', period_end='2026-06-30',
    )
    assert out.input_total == 200.0
    assert out.output_total == 0.0
    assert len(out.accounts) == 1
    assert out.accounts[0]['type'] == 'Input'


def test_nl_movements_skips_account_with_zero_transactions():
    canned = [
        [{'debits': 0, 'credits': 0, 'net': 0, 'transaction_count': 0}],
    ]
    conn = _FakeConn(canned)
    out = fetch_nl_vat_movements(
        conn,
        output_nominal_accounts={'2200'},
        input_nominal_accounts=set(),
        period_start='2026-04-01', period_end='2026-06-30',
    )
    assert out.accounts == []
    assert out.output_total == 0.0


def test_nl_movements_handles_account_in_both_sets_as_mixed():
    """If an account appears in both output and input sets, type='Output'
    (first branch of the conditional) — preserves original behaviour."""
    canned = [
        [{'debits': 100.0, 'credits': 200.0, 'net': -100.0, 'transaction_count': 2}],
        [{'description': 'Mixed VAT'}],
    ]
    conn = _FakeConn(canned)
    out = fetch_nl_vat_movements(
        conn,
        output_nominal_accounts={'2200'},
        input_nominal_accounts={'2200'},
        period_start='2026-04-01', period_end='2026-06-30',
    )
    assert len(out.accounts) == 1
    assert out.accounts[0]['type'] == 'Output'  # output check first
    # Both totals fire (credit on output, debit on input)
    assert out.output_total == 200.0
    assert out.input_total == 100.0


def test_nl_movements_handles_empty_account_sets():
    out = fetch_nl_vat_movements(
        _FakeConn([]),
        output_nominal_accounts=set(),
        input_nominal_accounts=set(),
        period_start='2026-04-01', period_end='2026-06-30',
    )
    assert out.accounts == []
    assert out.output_total == 0.0
    assert out.input_total == 0.0
