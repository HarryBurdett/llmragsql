"""Tests for the F9 scan-chain-ordering helpers extracted from
scan_all_banks_for_statements (Step 5)."""
from __future__ import annotations

from datetime import date as _date

import pytest

from apps.bank_reconcile.logic.scan_chain_ordering import (
    _to_date,
    sort_statements_by_chain,
    filter_fully_reconciled_statements,
)


# ====================================================================
# _to_date
# ====================================================================


def test_to_date_passes_through_date():
    d = _date(2026, 4, 1)
    assert _to_date(d) == d


def test_to_date_handles_none():
    assert _to_date(None) is None


def test_to_date_parses_iso_string():
    assert _to_date('2026-04-01') == _date(2026, 4, 1)


def test_to_date_parses_iso_with_z_suffix():
    assert _to_date('2026-04-01T12:00:00Z') == _date(2026, 4, 1)


def test_to_date_parses_short_iso_prefix():
    """First 10 chars of any ISO-ish string also work."""
    assert _to_date('2026-04-01 some trailing junk') == _date(2026, 4, 1)


def test_to_date_returns_none_on_unparseable():
    assert _to_date('not a date') is None


# ====================================================================
# sort_statements_by_chain
# ====================================================================


def _stmt(filename, opening, closing):
    return {'filename': filename, 'opening_balance': opening, 'closing_balance': closing}


def test_chain_walks_from_reconciled_balance():
    """Starting at rec=£100, chain picks £100→£200, £200→£300, £300→£400."""
    stmts = [
        _stmt('c.pdf', 300.0, 400.0),
        _stmt('a.pdf', 100.0, 200.0),
        _stmt('b.pdf', 200.0, 300.0),
    ]
    out = sort_statements_by_chain(stmts, reconciled_balance=100.0)
    assert [s['filename'] for s in out] == ['a.pdf', 'b.pdf', 'c.pdf']


def test_chain_unmatched_remainder_sorted_by_opening():
    """No statement has opening=100, so all are 'remainder' — sorted by opening."""
    stmts = [
        _stmt('a.pdf', 500.0, 600.0),
        _stmt('b.pdf', 200.0, 300.0),
        _stmt('c.pdf', 800.0, 900.0),
    ]
    out = sort_statements_by_chain(stmts, reconciled_balance=100.0)
    assert [s['filename'] for s in out] == ['b.pdf', 'a.pdf', 'c.pdf']


def test_chain_partial_match_then_remainder():
    """Chain picks a matching statement then exits with remainder sorted."""
    stmts = [
        _stmt('a.pdf', 100.0, 200.0),  # chains
        _stmt('b.pdf', 500.0, 600.0),  # no chain
        _stmt('c.pdf', 700.0, 800.0),  # no chain
    ]
    out = sort_statements_by_chain(stmts, reconciled_balance=100.0)
    assert out[0]['filename'] == 'a.pdf'
    # Remaining sorted by opening: b (500) then c (700)
    assert [s['filename'] for s in out[1:]] == ['b.pdf', 'c.pdf']


def test_chain_no_reconciled_falls_back_to_simple_sort():
    """No reconciled_balance: sort by (has_opening, opening, sort_key)."""
    stmts = [
        {'filename': 'a.pdf', 'opening_balance': 500.0, 'sort_key': (1,)},
        {'filename': 'b.pdf', 'opening_balance': None, 'sort_key': (1,)},
        {'filename': 'c.pdf', 'opening_balance': 200.0, 'sort_key': (1,)},
    ]
    out = sort_statements_by_chain(stmts, reconciled_balance=None)
    # 'has_opening' first (200, 500), then None last
    assert [s['filename'] for s in out] == ['c.pdf', 'a.pdf', 'b.pdf']


def test_chain_single_statement_returns_simple_sort():
    """One statement → fall back to simple sort path."""
    stmts = [_stmt('only.pdf', 100.0, 200.0)]
    out = sort_statements_by_chain(stmts, reconciled_balance=None)
    assert out == stmts


def test_chain_handles_penny_tolerance():
    """Opening within £0.01 of current_bal counts as a match."""
    stmts = [_stmt('a.pdf', 100.005, 200.0)]
    out = sort_statements_by_chain(stmts, reconciled_balance=100.0)
    assert out[0]['filename'] == 'a.pdf'


def test_chain_handles_missing_closing():
    """If closing_balance is None, current_bal stays put."""
    stmts = [
        _stmt('a.pdf', 100.0, None),
        _stmt('b.pdf', 100.0, 200.0),
    ]
    out = sort_statements_by_chain(stmts, reconciled_balance=100.0)
    # Both have opening=100 — chain picks one (first in input order),
    # then the next iteration finds the other (still matches 100).
    assert len(out) == 2


def test_chain_empty_list():
    out = sort_statements_by_chain([], reconciled_balance=100.0)
    assert out == []


# ====================================================================
# filter_fully_reconciled_statements
# ====================================================================


def test_filter_no_reconciled_balance_passes_through():
    stmts = [_stmt('a.pdf', 100.0, 200.0)]
    out = filter_fully_reconciled_statements(stmts, None, 'BC010', None)
    assert out is stmts  # unchanged


def test_filter_no_statements_returns_empty():
    out = filter_fully_reconciled_statements([], None, 'BC010', 100.0)
    assert out == []


def test_filter_no_connector_passes_through():
    stmts = [_stmt('a.pdf', 100.0, 200.0)]
    out = filter_fully_reconciled_statements(stmts, None, 'BC010', 100.0)
    assert out is stmts
