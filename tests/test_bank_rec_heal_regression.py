"""Real-world scenario regression — pins the canonical case the
self-heal was built to fix.

Captured 2026-05-05. Local row stays at is_reconciled=0 even though
Opera has fully reconciled the statement:

  Local row (legacy — no statement_number stored):
    closing_balance  = £115,064.71
    period_end       = 2026-05-01
    is_reconciled    = 0
    reconciled_count = 20  (from the partial-rec completion)

  Opera nbank state:
    nk_recbal  = £115,064.71
    nk_lststdt = 2026-05-01
    nk_lststno = 86940
    nk_reccfwd = £0.00

After heal:
  is_reconciled    → 1
  reconciled_count → 20  (preserved — legacy rows not overwritten)

Spec: docs/superpowers/specs/2026-05-05-bank-rec-self-heal-design.md
"""
import sqlite3
import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock


def test_real_world_partial_rec_completed_in_opera_regression():
    from api.email.storage import EmailStorage
    from sql_rag.bank_rec_heal import (
        NbankSnapshot, heal_bank_statement_imports
    )

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / 'email_data.db'
        EmailStorage(str(db_path))

        with sqlite3.connect(str(db_path)) as conn:
            # Note: reconciled_count starts at 0 — the user did the rec
            # in Opera, our app's complete_reconciliation never ran, so
            # nothing populated reconciled_count. transactions_imported
            # = 20 (we did post 20 entries with ae_tmpstat). The heal's
            # legacy fallback picks up transactions_imported and writes
            # it into reconciled_count.
            conn.execute("""
                INSERT INTO bank_statement_imports
                    (id, bank_code, filename, opening_balance, closing_balance,
                     statement_date, period_end, is_reconciled,
                     transactions_imported, reconciled_count, statement_number)
                VALUES
                    (71, 'BANK01', 'Statement 01-MAY-26.pdf',
                     116726.07, 115064.71,
                     '2026-05-01', '2026-05-01',
                     0, 20, 0, NULL)
            """)
            conn.commit()

        ds = MagicMock()
        ds.read_nbank.return_value = NbankSnapshot(
            bank_code='BANK01',
            recbal_pounds=115064.71,
            lststdt=date(2026, 5, 1),
            lststno=86940,
        )

        result = heal_bank_statement_imports(
            bank_code='BANK01',
            company_db_path=db_path,
            opera_data_source=ds,
        )

        assert result.healed_count == 1
        assert len(result.audit_lines) == 1
        audit = result.audit_lines[0]
        assert 'check 1 ok' in audit
        assert '115064.71' in audit
        assert 'check 2 ok' in audit
        assert '2026-05-01' in audit
        assert ('check 3 skipped' in audit) or ('legacy' in audit.lower())

        ds.count_reconciled_aentry.assert_not_called()

        with sqlite3.connect(str(db_path)) as conn:
            row = conn.execute(
                "SELECT is_reconciled, reconciled_count, statement_number, bank_code "
                "FROM bank_statement_imports WHERE id = 71"
            ).fetchone()
            assert row[0] == 1
            # Legacy fallback: reconciled_count populated from
            # transactions_imported (was 0, now 20).
            assert row[1] == 20
            assert row[2] is None
            assert row[3] == 'BANK01'


def test_regression_idempotency():
    from api.email.storage import EmailStorage
    from sql_rag.bank_rec_heal import (
        NbankSnapshot, heal_bank_statement_imports
    )

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / 'email_data.db'
        EmailStorage(str(db_path))

        with sqlite3.connect(str(db_path)) as conn:
            conn.execute("""
                INSERT INTO bank_statement_imports
                    (id, bank_code, filename, closing_balance, period_end,
                     is_reconciled, reconciled_count, statement_number)
                VALUES
                    (71, 'BANK01', 'x.pdf', 115064.71, '2026-05-01',
                     0, 20, NULL)
            """)
            conn.commit()

        ds = MagicMock()
        ds.read_nbank.return_value = NbankSnapshot(
            bank_code='BANK01',
            recbal_pounds=115064.71,
            lststdt=date(2026, 5, 1),
            lststno=86940,
        )

        first = heal_bank_statement_imports('BANK01', db_path, ds)
        second = heal_bank_statement_imports('BANK01', db_path, ds)
        assert first.healed_count == 1
        assert second.healed_count == 0
