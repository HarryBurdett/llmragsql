"""Tests for the bank-rec local-status self-heal module.

Spec: docs/superpowers/specs/2026-05-05-bank-rec-self-heal-design.md
"""
import sqlite3
import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


def test_nbank_snapshot_holds_required_fields():
    """NbankSnapshot is the data carrier between data sources and the rule
    evaluator. Frozen, with the four fields the rule needs."""
    from sql_rag.bank_rec_heal import NbankSnapshot

    snap = NbankSnapshot(
        bank_code='BC010',
        recbal_pounds=115064.71,
        lststdt=date(2026, 5, 1),
        lststno=86940,
    )
    assert snap.bank_code == 'BC010'
    assert snap.recbal_pounds == 115064.71
    assert snap.lststdt == date(2026, 5, 1)
    assert snap.lststno == 86940


def test_heal_result_holds_count_and_audit_lines():
    """HealResult carries what scan-emails needs to render diagnostics."""
    from sql_rag.bank_rec_heal import HealResult

    r = HealResult(healed_count=2, audit_lines=['line one', 'line two'])
    assert r.healed_count == 2
    assert r.audit_lines == ['line one', 'line two']


# ---------------------------------------------------------------------------
# Helpers for the rule-evaluator tests
# ---------------------------------------------------------------------------


def _snap(recbal=115064.71, lststdt=date(2026, 5, 1), lststno=86940, bank='BC010'):
    from sql_rag.bank_rec_heal import NbankSnapshot
    return NbankSnapshot(
        bank_code=bank,
        recbal_pounds=recbal,
        lststdt=lststdt,
        lststno=lststno,
    )


def _row(closing=115064.71, period_end=date(2026, 5, 1), statement_number=None):
    return {
        'closing_balance': closing,
        'period_end': period_end,
        'statement_number': statement_number,
    }


# ---------------------------------------------------------------------------
# is_row_healable() — three-fact truth table
# ---------------------------------------------------------------------------


def test_three_facts_match_returns_true():
    from sql_rag.bank_rec_heal import is_row_healable
    healable, _ = is_row_healable(_row(statement_number=86940), _snap())
    assert healable is True


def test_balance_mismatch_returns_false():
    from sql_rag.bank_rec_heal import is_row_healable
    healable, _ = is_row_healable(
        _row(closing=115000.00, statement_number=86940), _snap(recbal=115064.71)
    )
    assert healable is False


def test_balance_match_within_one_pence_returns_true():
    from sql_rag.bank_rec_heal import is_row_healable
    healable, _ = is_row_healable(
        _row(closing=115064.70, statement_number=86940), _snap(recbal=115064.71)
    )
    assert healable is True


def test_balance_match_outside_one_pence_returns_false():
    from sql_rag.bank_rec_heal import is_row_healable
    healable, _ = is_row_healable(
        _row(closing=115064.699, statement_number=86940), _snap(recbal=115064.71)
    )
    assert healable is False


def test_date_strictly_before_period_end_returns_false():
    from sql_rag.bank_rec_heal import is_row_healable
    healable, _ = is_row_healable(
        _row(period_end=date(2026, 5, 1), statement_number=86940),
        _snap(lststdt=date(2026, 4, 30)),
    )
    assert healable is False


def test_date_equals_period_end_returns_true():
    from sql_rag.bank_rec_heal import is_row_healable
    healable, _ = is_row_healable(
        _row(period_end=date(2026, 5, 1), statement_number=86940),
        _snap(lststdt=date(2026, 5, 1)),
    )
    assert healable is True


def test_date_after_period_end_returns_true():
    from sql_rag.bank_rec_heal import is_row_healable
    healable, _ = is_row_healable(
        _row(period_end=date(2026, 5, 1), statement_number=86940),
        _snap(lststdt=date(2026, 6, 1)),
    )
    assert healable is True


def test_statement_number_match_returns_true():
    from sql_rag.bank_rec_heal import is_row_healable
    healable, _ = is_row_healable(_row(statement_number=86940), _snap(lststno=86940))
    assert healable is True


def test_statement_number_advanced_returns_true():
    from sql_rag.bank_rec_heal import is_row_healable
    healable, _ = is_row_healable(_row(statement_number=86940), _snap(lststno=86941))
    assert healable is True


def test_statement_number_behind_returns_false():
    from sql_rag.bank_rec_heal import is_row_healable
    healable, _ = is_row_healable(_row(statement_number=86940), _snap(lststno=86939))
    assert healable is False


def test_legacy_row_no_stored_number_uses_two_checks():
    from sql_rag.bank_rec_heal import is_row_healable
    healable, audit = is_row_healable(_row(statement_number=None), _snap(lststno=999))
    assert healable is True
    assert ('skipped' in audit.lower()) or ('legacy' in audit.lower())


def test_null_nk_recbal_returns_false():
    from sql_rag.bank_rec_heal import is_row_healable
    healable, _ = is_row_healable(_row(statement_number=86940), _snap(recbal=None))
    assert healable is False


def test_null_nk_lststdt_returns_false():
    from sql_rag.bank_rec_heal import is_row_healable
    healable, _ = is_row_healable(_row(statement_number=86940), _snap(lststdt=None))
    assert healable is False


# ---------------------------------------------------------------------------
# heal_bank_statement_imports() — orchestrator behaviour
# ---------------------------------------------------------------------------


def _make_email_db(tmp_path, rows):
    """Build a real on-disk email_data.db with bank_statement_imports rows."""
    from api.email.storage import EmailStorage

    db_path = tmp_path / 'email_data.db'
    EmailStorage(str(db_path))   # creates schema + statement_number column

    with sqlite3.connect(str(db_path)) as conn:
        for r in rows:
            conn.execute("""
                INSERT INTO bank_statement_imports
                    (bank_code, filename, opening_balance, closing_balance,
                     statement_date, period_end, is_reconciled,
                     reconciled_count, statement_number)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                r.get('bank_code', 'BC010'),
                r.get('filename', 'test.pdf'),
                r.get('opening_balance', 0),
                r.get('closing_balance'),
                r.get('statement_date', '2026-05-01'),
                r.get('period_end'),
                r.get('is_reconciled', 0),
                r.get('reconciled_count', 0),
                r.get('statement_number'),
            ))
        conn.commit()
    return db_path


def _make_data_source(snapshot, reconciled_count=20):
    ds = MagicMock()
    ds.read_nbank.return_value = snapshot
    ds.count_reconciled_aentry.return_value = reconciled_count
    return ds


def test_heal_three_facts_match_marks_done(tmp_path):
    from sql_rag.bank_rec_heal import (
        NbankSnapshot, heal_bank_statement_imports
    )

    db = _make_email_db(tmp_path, [{
        'closing_balance': 115064.71,
        'period_end': '2026-05-01',
        'is_reconciled': 0,
        'reconciled_count': 0,
        'statement_number': 86940,
    }])
    ds = _make_data_source(NbankSnapshot(
        bank_code='BC010',
        recbal_pounds=115064.71,
        lststdt=date(2026, 5, 1),
        lststno=86940,
    ), reconciled_count=20)

    result = heal_bank_statement_imports('BC010', db, ds)

    assert result.healed_count == 1
    assert len(result.audit_lines) == 1
    assert 'healed' in result.audit_lines[0]

    with sqlite3.connect(str(db)) as conn:
        row = conn.execute(
            "SELECT is_reconciled, reconciled_count FROM bank_statement_imports"
        ).fetchone()
        assert row[0] == 1
        assert row[1] == 20


def test_heal_legacy_row_with_no_statement_number_uses_transactions_imported(tmp_path):
    """Legacy row (statement_number=NULL): heal still fires (2-check rule)
    AND populates reconciled_count from the local transactions_imported
    field. Counting by ae_frstat is not possible (no stored statement
    number); using ae_recdate against the period gives wrong answers
    because Opera's ae_recdate is when the user clicked Reconcile in
    Opera, not the statement period. transactions_imported is the
    correct measure: it's the count of entries we posted (with
    ae_tmpstat), all of which got reconciled by the time the heal
    fired."""
    from sql_rag.bank_rec_heal import (
        NbankSnapshot, heal_bank_statement_imports
    )

    db = _make_email_db(tmp_path, [{
        'closing_balance': 115064.71,
        'period_end': '2026-05-01',
        'is_reconciled': 0,
        'reconciled_count': 5,
        'statement_number': None,
    }])
    # Set transactions_imported to 20 (canonical real-world scenario)
    with sqlite3.connect(str(db)) as conn:
        conn.execute("UPDATE bank_statement_imports SET transactions_imported=20")
        conn.commit()

    ds = _make_data_source(NbankSnapshot(
        bank_code='BC010',
        recbal_pounds=115064.71,
        lststdt=date(2026, 5, 1),
        lststno=86940,
    ))

    result = heal_bank_statement_imports('BC010', db, ds)

    assert result.healed_count == 1
    # Neither Opera count method should be called for legacy rows.
    ds.count_reconciled_aentry.assert_not_called()

    with sqlite3.connect(str(db)) as conn:
        row = conn.execute(
            "SELECT is_reconciled, reconciled_count FROM bank_statement_imports"
        ).fetchone()
        assert row[0] == 1
        assert row[1] == 20  # populated from transactions_imported


def test_heal_legacy_row_with_zero_transactions_imported_preserves_count(tmp_path):
    """If transactions_imported is 0 (no entries posted via this app),
    we have no signal — leave reconciled_count untouched."""
    from sql_rag.bank_rec_heal import (
        NbankSnapshot, heal_bank_statement_imports
    )

    db = _make_email_db(tmp_path, [{
        'closing_balance': 115064.71,
        'period_end': '2026-05-01',
        'is_reconciled': 0,
        'reconciled_count': 5,
        'statement_number': None,
    }])
    # transactions_imported stays 0 (default)

    ds = _make_data_source(NbankSnapshot(
        bank_code='BC010',
        recbal_pounds=115064.71,
        lststdt=date(2026, 5, 1),
        lststno=86940,
    ))

    result = heal_bank_statement_imports('BC010', db, ds)

    assert result.healed_count == 1
    ds.count_reconciled_aentry.assert_not_called()

    with sqlite3.connect(str(db)) as conn:
        row = conn.execute(
            "SELECT is_reconciled, reconciled_count FROM bank_statement_imports"
        ).fetchone()
        assert row[0] == 1
        assert row[1] == 5  # preserved


def test_heal_balance_mismatch_no_change(tmp_path):
    from sql_rag.bank_rec_heal import (
        NbankSnapshot, heal_bank_statement_imports
    )

    db = _make_email_db(tmp_path, [{
        'closing_balance': 100000.00,
        'period_end': '2026-05-01',
        'is_reconciled': 0,
        'statement_number': 86940,
    }])
    ds = _make_data_source(NbankSnapshot(
        bank_code='BC010',
        recbal_pounds=115064.71,
        lststdt=date(2026, 5, 1),
        lststno=86940,
    ))

    result = heal_bank_statement_imports('BC010', db, ds)
    assert result.healed_count == 0

    with sqlite3.connect(str(db)) as conn:
        row = conn.execute(
            "SELECT is_reconciled FROM bank_statement_imports"
        ).fetchone()
        assert row[0] == 0


def test_heal_idempotent(tmp_path):
    from sql_rag.bank_rec_heal import (
        NbankSnapshot, heal_bank_statement_imports
    )

    db = _make_email_db(tmp_path, [{
        'closing_balance': 115064.71,
        'period_end': '2026-05-01',
        'is_reconciled': 0,
        'statement_number': 86940,
    }])
    ds = _make_data_source(NbankSnapshot(
        bank_code='BC010',
        recbal_pounds=115064.71,
        lststdt=date(2026, 5, 1),
        lststno=86940,
    ))

    first = heal_bank_statement_imports('BC010', db, ds)
    second = heal_bank_statement_imports('BC010', db, ds)
    assert first.healed_count == 1
    assert second.healed_count == 0


def test_heal_only_touches_target_bank(tmp_path):
    from sql_rag.bank_rec_heal import (
        NbankSnapshot, heal_bank_statement_imports
    )

    db = _make_email_db(tmp_path, [
        {'bank_code': 'BC010', 'closing_balance': 115064.71,
         'period_end': '2026-05-01', 'statement_number': 86940},
        {'bank_code': 'OTHER', 'closing_balance': 115064.71,
         'period_end': '2026-05-01', 'statement_number': 86940},
    ])
    ds = _make_data_source(NbankSnapshot(
        bank_code='BC010',
        recbal_pounds=115064.71,
        lststdt=date(2026, 5, 1),
        lststno=86940,
    ))

    heal_bank_statement_imports('BC010', db, ds)

    with sqlite3.connect(str(db)) as conn:
        rows = conn.execute(
            "SELECT bank_code, is_reconciled FROM bank_statement_imports ORDER BY bank_code"
        ).fetchall()
        assert {r[0]: r[1] for r in rows} == {'BC010': 1, 'OTHER': 0}


def test_heal_opera_unreachable_skips_silently(tmp_path):
    from sql_rag.bank_rec_heal import heal_bank_statement_imports

    db = _make_email_db(tmp_path, [{
        'closing_balance': 115064.71,
        'period_end': '2026-05-01',
        'statement_number': 86940,
    }])
    ds = MagicMock()
    ds.read_nbank.side_effect = ConnectionError('Opera unreachable')

    result = heal_bank_statement_imports('BC010', db, ds)
    assert result.healed_count == 0

    with sqlite3.connect(str(db)) as conn:
        row = conn.execute(
            "SELECT is_reconciled FROM bank_statement_imports"
        ).fetchone()
        assert row[0] == 0


def test_heal_nbank_missing_returns_zero(tmp_path):
    from sql_rag.bank_rec_heal import heal_bank_statement_imports

    db = _make_email_db(tmp_path, [{
        'closing_balance': 115064.71,
        'period_end': '2026-05-01',
        'statement_number': 86940,
    }])
    ds = MagicMock()
    ds.read_nbank.return_value = None

    result = heal_bank_statement_imports('BC010', db, ds)
    assert result.healed_count == 0


# ---------------------------------------------------------------------------
# Schema migration
# ---------------------------------------------------------------------------


def test_statement_number_column_exists_on_init():
    """A fresh EmailStorage init must have statement_number INTEGER on
    bank_statement_imports."""
    from api.email.storage import EmailStorage

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / 'email_data.db'
        EmailStorage(str(db_path))

        with sqlite3.connect(str(db_path)) as conn:
            cols = {
                row[1]: row[2]
                for row in conn.execute(
                    "PRAGMA table_info(bank_statement_imports)"
                ).fetchall()
            }
            assert 'statement_number' in cols
            assert cols['statement_number'].upper() == 'INTEGER'


def test_migration_idempotent():
    """Two consecutive inits don't error."""
    from api.email.storage import EmailStorage

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / 'email_data.db'
        EmailStorage(str(db_path))
        EmailStorage(str(db_path))


def test_existing_legacy_data_preserved_through_migration():
    """If the table existed without statement_number, legacy rows survive
    the migration with NULL statement_number."""
    from api.email.storage import EmailStorage

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / 'email_data.db'
        EmailStorage(str(db_path))

        with sqlite3.connect(str(db_path)) as conn:
            conn.execute("""
                INSERT INTO bank_statement_imports
                    (bank_code, filename, opening_balance, closing_balance,
                     statement_date, period_end, is_reconciled)
                VALUES (?, ?, ?, ?, ?, ?, 0)
            """, ('BC010', 'legacy.pdf', 100.0, 200.0, '2026-05-01', '2026-05-01'))
            conn.commit()

        EmailStorage(str(db_path))   # second init — no-op migration

        with sqlite3.connect(str(db_path)) as conn:
            row = conn.execute(
                "SELECT bank_code, statement_number FROM bank_statement_imports"
            ).fetchone()
            assert row[0] == 'BC010'
            assert row[1] is None
