"""Pins:
  - SE / Opera 3 parity for the heal rule.
  - Both complete_reconciliation routes persist statement_number.
  - Both scan-emails routes call heal_bank_statement_imports.

Spec: docs/superpowers/specs/2026-05-05-bank-rec-self-heal-design.md
CLAUDE.md: Opera SE / Opera 3 FULL PARITY (mandatory).
"""
import inspect
from datetime import date
from unittest.mock import MagicMock

import pandas as pd


# ---------------------------------------------------------------------------
# SE / Opera 3 parity
# ---------------------------------------------------------------------------


class _FakeReader:
    def __init__(self, tables):
        self._tables = tables
    def read_table(self, name):
        return self._tables.get(name, [])


def _se_data_source(recbal=115064.71, lststdt=date(2026, 5, 1), lststno=86940):
    from sql_rag.duplicate_check_se import OperaSEDataSource
    fake_sql = MagicMock()
    fake_sql.execute_query.return_value = pd.DataFrame([{
        'recbal_pounds': recbal, 'lststdt': lststdt, 'lststno': lststno,
    }])
    return OperaSEDataSource(fake_sql)


def _o3_data_source(recbal_pounds=115064.71, lststdt=date(2026, 5, 1), lststno=86940):
    from sql_rag.duplicate_check_o3 import Opera3DataSource
    reader = _FakeReader({
        'nbank': [{
            'nk_acnt': 'BANK01',
            'nk_recbal': int(round(recbal_pounds * 100)),
            'nk_lststdt': lststdt,
            'nk_lststno': lststno,
        }],
        'aentry': [],
    })
    return Opera3DataSource(reader)


def test_parity_three_facts_match():
    from sql_rag.bank_rec_heal import is_row_healable

    se_snap = _se_data_source().read_nbank('BANK01')
    o3_snap = _o3_data_source().read_nbank('BANK01')

    row = {'closing_balance': 115064.71, 'period_end': date(2026, 5, 1),
           'statement_number': 86940}

    assert is_row_healable(row, se_snap)[0] == is_row_healable(row, o3_snap)[0] == True


def test_parity_balance_mismatch():
    from sql_rag.bank_rec_heal import is_row_healable

    se_snap = _se_data_source(recbal=100000.00).read_nbank('BANK01')
    o3_snap = _o3_data_source(recbal_pounds=100000.00).read_nbank('BANK01')

    row = {'closing_balance': 115064.71, 'period_end': date(2026, 5, 1),
           'statement_number': 86940}

    assert is_row_healable(row, se_snap)[0] == is_row_healable(row, o3_snap)[0] == False


def test_parity_legacy_row():
    from sql_rag.bank_rec_heal import is_row_healable

    se_snap = _se_data_source().read_nbank('BANK01')
    o3_snap = _o3_data_source().read_nbank('BANK01')

    row = {'closing_balance': 115064.71, 'period_end': date(2026, 5, 1),
           'statement_number': None}

    assert is_row_healable(row, se_snap)[0] == is_row_healable(row, o3_snap)[0] == True


# ---------------------------------------------------------------------------
# Route source-inspection: completion writes statement_number
# ---------------------------------------------------------------------------


def test_se_completion_writes_statement_number():
    from apps.bank_reconcile.api import routes
    src = inspect.getsource(routes.complete_reconciliation)
    assert 'statement_number' in src
    # Both partial and full UPDATE blocks must persist statement_number.
    assert src.count('statement_number = ?') >= 2


def test_opera3_completion_writes_statement_number():
    from apps.bank_reconcile.api import routes
    src = inspect.getsource(routes.opera3_complete_reconciliation)
    assert 'statement_number' in src
    assert src.count('statement_number = ?') >= 2


# ---------------------------------------------------------------------------
# Route source-inspection: scan-emails calls heal
# ---------------------------------------------------------------------------


def test_se_scan_emails_calls_heal():
    from apps.bank_reconcile.api import routes
    src = inspect.getsource(routes.scan_emails_for_bank_statements)
    assert 'heal_bank_statement_imports' in src


def test_opera3_scan_emails_calls_heal():
    from apps.bank_reconcile.api import routes
    src = inspect.getsource(routes.opera3_scan_emails_for_bank_statements)
    assert 'heal_bank_statement_imports' in src
