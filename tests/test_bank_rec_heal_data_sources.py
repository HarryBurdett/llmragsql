"""Tests for OperaSEDataSource.read_nbank() / count_reconciled_aentry()
and Opera3DataSource.read_nbank() / count_reconciled_aentry().

Spec: docs/superpowers/specs/2026-05-05-bank-rec-self-heal-design.md
"""
from datetime import date
from unittest.mock import MagicMock

import pandas as pd


# ---------------------------------------------------------------------------
# OperaSEDataSource
# ---------------------------------------------------------------------------


def test_se_read_nbank_returns_snapshot():
    from sql_rag.duplicate_check_se import OperaSEDataSource

    fake_df = pd.DataFrame([{
        'recbal_pounds': 115064.71,
        'lststdt': date(2026, 5, 1),
        'lststno': 86940,
    }])
    fake_sql = MagicMock()
    fake_sql.execute_query.return_value = fake_df

    ds = OperaSEDataSource(fake_sql)
    snap = ds.read_nbank('BC010')
    assert snap is not None
    assert snap.bank_code == 'BC010'
    assert snap.recbal_pounds == 115064.71
    assert snap.lststdt == date(2026, 5, 1)
    assert snap.lststno == 86940


def test_se_read_nbank_query_uses_nolock_and_pence_division():
    from sql_rag.duplicate_check_se import OperaSEDataSource

    fake_sql = MagicMock()
    fake_sql.execute_query.return_value = pd.DataFrame()
    OperaSEDataSource(fake_sql).read_nbank('BC010')

    sql = fake_sql.execute_query.call_args[0][0]
    assert 'WITH (NOLOCK)' in sql
    assert 'nk_recbal / 100.0' in sql
    assert 'nk_lststdt' in sql
    assert 'nk_lststno' in sql


def test_se_read_nbank_returns_none_when_bank_missing():
    from sql_rag.duplicate_check_se import OperaSEDataSource

    fake_sql = MagicMock()
    fake_sql.execute_query.return_value = pd.DataFrame()
    assert OperaSEDataSource(fake_sql).read_nbank('UNKNOWN') is None


def test_se_count_reconciled_aentry_returns_int():
    from sql_rag.duplicate_check_se import OperaSEDataSource

    fake_sql = MagicMock()
    fake_sql.execute_query.return_value = pd.DataFrame([{'cnt': 17}])
    assert OperaSEDataSource(fake_sql).count_reconciled_aentry('BC010', 86940) == 17


def test_se_count_reconciled_aentry_query_correct():
    from sql_rag.duplicate_check_se import OperaSEDataSource

    fake_sql = MagicMock()
    fake_sql.execute_query.return_value = pd.DataFrame([{'cnt': 0}])
    OperaSEDataSource(fake_sql).count_reconciled_aentry('BC010', 86940)
    sql = fake_sql.execute_query.call_args[0][0]
    assert 'WITH (NOLOCK)' in sql
    assert 'ae_acnt' in sql
    assert 'ae_frstat' in sql
    assert 'ae_reclnum > 0' in sql


# ---------------------------------------------------------------------------
# Opera3DataSource
# ---------------------------------------------------------------------------


class _FakeReader:
    def __init__(self, tables):
        self._tables = tables

    def read_table(self, name):
        return self._tables.get(name, [])


def test_o3_read_nbank_returns_snapshot_with_pence_to_pounds():
    from sql_rag.duplicate_check_o3 import Opera3DataSource

    reader = _FakeReader({
        'nbank': [
            {'nk_acnt': 'BC010', 'nk_recbal': 11506471,
             'nk_lststdt': date(2026, 5, 1), 'nk_lststno': 86940},
            {'nk_acnt': 'OTHER', 'nk_recbal': 100, 'nk_lststdt': None,
             'nk_lststno': 1},
        ]
    })
    snap = Opera3DataSource(reader).read_nbank('BC010')
    assert snap is not None
    assert snap.recbal_pounds == 115064.71
    assert snap.lststdt == date(2026, 5, 1)
    assert snap.lststno == 86940


def test_o3_read_nbank_returns_none_when_bank_missing():
    from sql_rag.duplicate_check_o3 import Opera3DataSource

    reader = _FakeReader({'nbank': [{'nk_acnt': 'OTHER'}]})
    assert Opera3DataSource(reader).read_nbank('BC010') is None


def test_o3_read_nbank_handles_padded_acnt():
    from sql_rag.duplicate_check_o3 import Opera3DataSource

    reader = _FakeReader({
        'nbank': [
            {'nk_acnt': 'BC010    ', 'nk_recbal': 11506471,
             'nk_lststdt': date(2026, 5, 1), 'nk_lststno': 86940},
        ]
    })
    snap = Opera3DataSource(reader).read_nbank('BC010')
    assert snap is not None
    assert snap.recbal_pounds == 115064.71


def test_o3_count_reconciled_aentry_counts_matching_dbf_rows():
    from sql_rag.duplicate_check_o3 import Opera3DataSource

    reader = _FakeReader({
        'aentry': [
            {'ae_acnt': 'BC010', 'ae_frstat': 86940, 'ae_reclnum': 2682},
            {'ae_acnt': 'BC010', 'ae_frstat': 86940, 'ae_reclnum': 2682},
            {'ae_acnt': 'BC010', 'ae_frstat': 86939, 'ae_reclnum': 2681},
            {'ae_acnt': 'BC010', 'ae_frstat': 86940, 'ae_reclnum': 0},
            {'ae_acnt': 'OTHER', 'ae_frstat': 86940, 'ae_reclnum': 2682},
        ]
    })
    assert Opera3DataSource(reader).count_reconciled_aentry('BC010', 86940) == 2


# ---------------------------------------------------------------------------
# count_reconciled_aentry_in_period — date-range fallback for legacy rows
# ---------------------------------------------------------------------------


def test_se_count_reconciled_aentry_in_period_query_correct():
    from sql_rag.duplicate_check_se import OperaSEDataSource

    fake_sql = MagicMock()
    fake_sql.execute_query.return_value = pd.DataFrame([{'cnt': 0}])
    OperaSEDataSource(fake_sql).count_reconciled_aentry_in_period(
        'BC010', date(2026, 4, 25), date(2026, 5, 1)
    )
    sql = fake_sql.execute_query.call_args[0][0]
    assert 'WITH (NOLOCK)' in sql
    assert 'ae_recdate BETWEEN' in sql
    assert "'2026-04-25'" in sql
    assert "'2026-05-01'" in sql
    assert 'ae_reclnum > 0' in sql


def test_o3_count_reconciled_aentry_in_period_counts_by_recdate():
    from sql_rag.duplicate_check_o3 import Opera3DataSource

    reader = _FakeReader({
        'aentry': [
            {'ae_acnt': 'BC010', 'ae_recdate': date(2026, 4, 28),
             'ae_reclnum': 2682},
            {'ae_acnt': 'BC010', 'ae_recdate': date(2026, 4, 30),
             'ae_reclnum': 2682},
            {'ae_acnt': 'BC010', 'ae_recdate': date(2026, 5, 1),
             'ae_reclnum': 2682},
            {'ae_acnt': 'BC010', 'ae_recdate': date(2026, 4, 24),  # before
             'ae_reclnum': 2681},
            {'ae_acnt': 'BC010', 'ae_recdate': date(2026, 5, 2),  # after
             'ae_reclnum': 2682},
            {'ae_acnt': 'BC010', 'ae_recdate': date(2026, 4, 28),
             'ae_reclnum': 0},  # unreconciled
            {'ae_acnt': 'OTHER', 'ae_recdate': date(2026, 4, 28),
             'ae_reclnum': 2682},  # different bank
        ]
    })
    n = Opera3DataSource(reader).count_reconciled_aentry_in_period(
        'BC010', date(2026, 4, 25), date(2026, 5, 1)
    )
    assert n == 3
