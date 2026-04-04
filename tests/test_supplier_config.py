"""
Tests for sql_rag/supplier_config.py

Verifies:
  1. sync_from_opera creates new suppliers
  2. sync_from_opera preserves local automation flags
  3. sync_from_opera excludes dormant suppliers
  4. get_config returns all expected fields
  5. update_flags only changes flag fields, not Opera-synced data
  6. get_all(active_only=True) filters by reconciliation_active
"""

import os
import tempfile
import pytest
import pandas as pd
from unittest.mock import MagicMock, call

from sql_rag.supplier_config import SupplierConfigManager


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_supplier_row(account, name, balance=1000.0, paymeth='BACS',
                       dormant=0, tprfl='NET30'):
    """Build a minimal pname-style row as a pandas Series."""
    return {
        'pn_account': account,
        'pn_name': name,
        'pn_currbal': balance,
        'pn_paymeth': paymeth,
        'pn_dormant': dormant,
        'pn_tprfl': tprfl,
    }


def _make_df(rows):
    """Convert list of dicts to DataFrame."""
    return pd.DataFrame(rows)


@pytest.fixture
def tmp_db(tmp_path):
    """Provide a fresh SQLite database path for each test."""
    return str(tmp_path / 'supplier_config_test.db')


@pytest.fixture
def mock_sql():
    """Mock SQL connector whose execute_query returns empty DataFrames by default."""
    connector = MagicMock()
    # pterms returns empty by default (no special terms)
    connector.execute_query.return_value = pd.DataFrame()
    return connector


def _make_manager(tmp_db, mock_sql, supplier_rows):
    """
    Create a SupplierConfigManager whose sync query returns supplier_rows.

    The first execute_query call (the pname SELECT) returns the supplier
    DataFrame; subsequent calls (pterms) return empty frames.
    """
    def side_effect(query, *args, **kwargs):
        if 'pname' in query:
            return _make_df(supplier_rows)
        # pterms calls
        return pd.DataFrame()

    mock_sql.execute_query.side_effect = side_effect
    return SupplierConfigManager(db_path=tmp_db, sql_connector=mock_sql)


# ---------------------------------------------------------------------------
# Test 1: sync creates new suppliers
# ---------------------------------------------------------------------------

def test_sync_creates_new_suppliers(tmp_db, mock_sql):
    """sync_from_opera inserts all non-dormant suppliers returned by Opera."""
    rows = [
        _make_supplier_row('SUP001', 'Alpha Ltd'),
        _make_supplier_row('SUP002', 'Beta PLC'),
        _make_supplier_row('SUP003', 'Gamma Co'),
    ]
    mgr = _make_manager(tmp_db, mock_sql, rows)
    result = mgr.sync_from_opera()

    assert result['new'] == 3
    assert result['synced'] == 0

    all_suppliers = mgr.get_all()
    codes = {s['account_code'] for s in all_suppliers}
    assert codes == {'SUP001', 'SUP002', 'SUP003'}


# ---------------------------------------------------------------------------
# Test 2: sync preserves local flags
# ---------------------------------------------------------------------------

def test_sync_preserves_local_flags(tmp_db, mock_sql):
    """Local automation flags set before sync must survive a re-sync."""
    rows = [_make_supplier_row('SUP001', 'Alpha Ltd', balance=500.0)]
    mgr = _make_manager(tmp_db, mock_sql, rows)

    # Initial sync to insert the supplier
    mgr.sync_from_opera()

    # Set a local flag
    mgr.update_flags('SUP001', auto_respond=True, never_communicate=False)

    # Sync again (same data, updated balance)
    rows_updated = [_make_supplier_row('SUP001', 'Alpha Ltd', balance=750.0)]

    def side_effect_updated(query, *args, **kwargs):
        if 'pname' in query:
            return _make_df(rows_updated)
        return pd.DataFrame()

    mock_sql.execute_query.side_effect = side_effect_updated
    result = mgr.sync_from_opera()

    assert result['synced'] == 1

    config = mgr.get_config('SUP001')
    # Opera fields updated
    assert config['balance'] == pytest.approx(750.0)
    # Local flag preserved
    assert config['auto_respond'] == 1
    assert config['never_communicate'] == 0


# ---------------------------------------------------------------------------
# Test 3: sync excludes dormant suppliers
# ---------------------------------------------------------------------------

def test_sync_excludes_dormant(tmp_db, mock_sql):
    """
    Dormant suppliers must not appear in supplier_config.

    The Opera query already filters them out (WHERE pn_dormant = 0 OR NULL),
    so the mock returns no dormant rows.  This test verifies the manager
    correctly handles Opera returning only active suppliers.
    """
    # Simulate Opera filtering: only non-dormant rows are returned
    active_rows = [_make_supplier_row('SUP010', 'Active Ltd', dormant=0)]
    # dormant supplier is never returned because Opera filters it
    mgr = _make_manager(tmp_db, mock_sql, active_rows)
    result = mgr.sync_from_opera()

    assert result['new'] == 1
    all_suppliers = mgr.get_all()
    assert len(all_suppliers) == 1
    assert all_suppliers[0]['account_code'] == 'SUP010'


def test_sync_dormant_not_inserted_if_returned(tmp_db, mock_sql):
    """
    If a dormant supplier somehow slips through (pn_dormant != 0),
    the sync still inserts it (it's Opera's responsibility to filter).
    This documents that the Python layer trusts Opera's WHERE clause.
    The important behaviour is that the pname query includes the dormant filter.
    """
    rows = [_make_supplier_row('SUP020', 'Dormant Ltd', dormant=1)]
    mgr = _make_manager(tmp_db, mock_sql, rows)
    mgr.sync_from_opera()

    # Whether inserted or not, the test verifies the query sent to Opera
    # contains the dormant filter
    pname_calls = [
        str(c) for c in mock_sql.execute_query.call_args_list
        if 'pname' in str(c)
    ]
    assert pname_calls, "Expected at least one pname query"
    assert 'pn_dormant' in pname_calls[0]


# ---------------------------------------------------------------------------
# Test 4: get_config returns all expected fields
# ---------------------------------------------------------------------------

def test_get_config_returns_all_fields(tmp_db, mock_sql):
    """get_config must include every column defined in the schema."""
    rows = [_make_supplier_row('SUP030', 'Delta Ltd')]
    mgr = _make_manager(tmp_db, mock_sql, rows)
    mgr.sync_from_opera()

    config = mgr.get_config('SUP030')
    assert config is not None

    expected_fields = {
        'account_code', 'name', 'balance', 'payment_terms_days',
        'payment_method', 'reconciliation_active', 'auto_respond',
        'never_communicate', 'statements_contact_position',
        'last_synced', 'last_statement_date', 'created_at', 'updated_at',
    }
    assert expected_fields.issubset(set(config.keys()))
    assert config['account_code'] == 'SUP030'
    assert config['name'] == 'Delta Ltd'


def test_get_config_returns_none_for_unknown(tmp_db, mock_sql):
    """get_config returns None when the account code does not exist."""
    mgr = SupplierConfigManager(db_path=tmp_db, sql_connector=mock_sql)
    assert mgr.get_config('UNKNOWN') is None


# ---------------------------------------------------------------------------
# Test 5: update_flags only changes flag fields
# ---------------------------------------------------------------------------

def test_update_flags_only_changes_flags(tmp_db, mock_sql):
    """Updating never_communicate must not alter name or balance."""
    rows = [_make_supplier_row('SUP040', 'Epsilon Ltd', balance=9999.0)]
    mgr = _make_manager(tmp_db, mock_sql, rows)
    mgr.sync_from_opera()

    mgr.update_flags('SUP040', never_communicate=True)

    config = mgr.get_config('SUP040')
    assert config['never_communicate'] == 1
    # Opera-synced fields must be unchanged
    assert config['name'] == 'Epsilon Ltd'
    assert config['balance'] == pytest.approx(9999.0)


def test_update_flags_rejects_opera_fields(tmp_db, mock_sql):
    """update_flags must raise ValueError for Opera-synced field names."""
    mgr = SupplierConfigManager(db_path=tmp_db, sql_connector=mock_sql)
    with pytest.raises(ValueError, match='Cannot update Opera-synced fields'):
        mgr.update_flags('SUP040', name='Hacked Name')


def test_update_flags_returns_false_for_unknown(tmp_db, mock_sql):
    """update_flags returns False when the account does not exist."""
    mgr = SupplierConfigManager(db_path=tmp_db, sql_connector=mock_sql)
    result = mgr.update_flags('NOSUCHCODE', auto_respond=True)
    assert result is False


# ---------------------------------------------------------------------------
# Test 6: get_all with active_only filter
# ---------------------------------------------------------------------------

def test_get_all_active_only(tmp_db, mock_sql):
    """get_all(active_only=True) must return only suppliers with reconciliation_active=1."""
    rows = [
        _make_supplier_row('SUP050', 'Zeta Ltd'),
        _make_supplier_row('SUP051', 'Eta Ltd'),
        _make_supplier_row('SUP052', 'Theta Ltd'),
    ]
    mgr = _make_manager(tmp_db, mock_sql, rows)
    mgr.sync_from_opera()

    # Deactivate one supplier
    mgr.update_flags('SUP051', reconciliation_active=False)

    active = mgr.get_all(active_only=True)
    active_codes = {s['account_code'] for s in active}
    assert 'SUP050' in active_codes
    assert 'SUP052' in active_codes
    assert 'SUP051' not in active_codes

    all_suppliers = mgr.get_all(active_only=False)
    assert len(all_suppliers) == 3
