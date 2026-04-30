"""Tests for sql_rag/deferred_transactions_db.py

Covers schema initialisation, single-record write, repeated writes, and the
count_for_bank helper used to verify audit traceability in higher-level tests.
"""

import os
import tempfile

import pytest

from sql_rag.deferred_transactions_db import DeferredTransactionsDB, derive_statement_state


@pytest.fixture
def tmpdb(tmp_path):
    db_path = tmp_path / "deferred.db"
    return DeferredTransactionsDB(str(db_path))


def test_schema_is_created_on_first_use(tmpdb):
    # Schema is created on construction — count_for_bank should work even with no rows.
    assert tmpdb.count_for_bank("BC010") == 0


def test_record_inserts_a_row(tmpdb):
    tmpdb.record(
        bank_code="BC010",
        statement_date="2026-04-17",
        amount=123.45,
        description="Test deferred payment",
        deferred_by="admin",
    )
    assert tmpdb.count_for_bank("BC010") == 1


def test_record_supports_multiple_rows_for_same_bank(tmpdb):
    tmpdb.record(bank_code="BC010", statement_date="2026-04-17", amount=10.0,
                 description="A", deferred_by="admin")
    tmpdb.record(bank_code="BC010", statement_date="2026-04-17", amount=20.0,
                 description="B", deferred_by="admin")
    tmpdb.record(bank_code="BC020", statement_date="2026-04-17", amount=30.0,
                 description="C", deferred_by="admin")
    assert tmpdb.count_for_bank("BC010") == 2
    assert tmpdb.count_for_bank("BC020") == 1


def test_record_handles_negative_amounts(tmpdb):
    tmpdb.record(
        bank_code="BC010",
        statement_date="2026-04-17",
        amount=-456.78,  # Payment out
        description="Outgoing payment deferred",
        deferred_by="admin",
    )
    assert tmpdb.count_for_bank("BC010") == 1


def test_record_handles_missing_optional_fields(tmpdb):
    # description and deferred_by may be empty strings
    tmpdb.record(
        bank_code="BC010",
        statement_date="2026-04-17",
        amount=5.0,
        description="",
        deferred_by="",
    )
    assert tmpdb.count_for_bank("BC010") == 1


def test_reopening_db_preserves_rows(tmp_path):
    path = str(tmp_path / "deferred.db")
    db1 = DeferredTransactionsDB(path)
    db1.record(bank_code="BC010", statement_date="2026-04-17", amount=1.0,
               description="X", deferred_by="admin")

    db2 = DeferredTransactionsDB(path)  # Re-open
    assert db2.count_for_bank("BC010") == 1


def test_count_for_statement_returns_zero_with_no_rows(tmpdb):
    assert tmpdb.count_for_statement(
        bank_code="BC010",
        period_start="2026-04-01",
        period_end="2026-04-30",
    ) == 0


def test_count_for_statement_includes_only_in_period(tmpdb):
    tmpdb.record(bank_code="BC010", statement_date="2026-04-15",
                 amount=10.0, description="A", deferred_by="admin")
    tmpdb.record(bank_code="BC010", statement_date="2026-04-25",
                 amount=20.0, description="B", deferred_by="admin")
    tmpdb.record(bank_code="BC010", statement_date="2026-05-10",
                 amount=30.0, description="C — outside period", deferred_by="admin")
    tmpdb.record(bank_code="BC020", statement_date="2026-04-15",
                 amount=40.0, description="D — wrong bank", deferred_by="admin")

    count = tmpdb.count_for_statement(
        bank_code="BC010",
        period_start="2026-04-01",
        period_end="2026-04-30",
    )
    assert count == 2  # only A and B


def test_count_for_statement_period_inclusive_at_boundaries(tmpdb):
    tmpdb.record(bank_code="BC010", statement_date="2026-04-01",
                 amount=10.0, description="start of period", deferred_by="admin")
    tmpdb.record(bank_code="BC010", statement_date="2026-04-30",
                 amount=20.0, description="end of period", deferred_by="admin")

    count = tmpdb.count_for_statement(
        bank_code="BC010",
        period_start="2026-04-01",
        period_end="2026-04-30",
    )
    assert count == 2  # both boundary dates included


def test_count_for_statement_handles_missing_period_args(tmpdb):
    tmpdb.record(bank_code="BC010", statement_date="2026-04-15",
                 amount=10.0, description="A", deferred_by="admin")

    # If either period bound is None or empty, fall back to count_for_bank semantics.
    assert tmpdb.count_for_statement(bank_code="BC010", period_start=None, period_end=None) == 1
    assert tmpdb.count_for_statement(bank_code="BC010", period_start="", period_end="") == 1
    assert tmpdb.count_for_statement(bank_code="BC010", period_start="2026-04-01", period_end=None) == 1


def test_derive_state_reconciled():
    assert derive_statement_state(
        is_reconciled=True,
        has_import_record=True,
        has_draft=False,
        deferred_count=0,
        extraction_status='cached',
    ) == 'reconciled'


def test_derive_state_reconciled_overrides_other_flags():
    """If is_reconciled is True, that beats everything else."""
    assert derive_statement_state(
        is_reconciled=True,
        has_import_record=True,
        has_draft=True,
        deferred_count=5,
        extraction_status='extracted',
    ) == 'reconciled'


def test_derive_state_imported_when_deferred_present():
    assert derive_statement_state(
        is_reconciled=False,
        has_import_record=True,
        has_draft=False,
        deferred_count=2,
        extraction_status='extracted',
    ) == 'imported'


def test_derive_state_in_progress_when_draft_only():
    assert derive_statement_state(
        is_reconciled=False,
        has_import_record=False,
        has_draft=True,
        deferred_count=0,
        extraction_status='extracted',
    ) == 'in_progress'


def test_derive_state_pending_extraction():
    assert derive_statement_state(
        is_reconciled=False,
        has_import_record=False,
        has_draft=False,
        deferred_count=0,
        extraction_status='pending_extraction',
    ) == 'pending_extraction'


def test_derive_state_failed_extraction_falls_back_to_pending():
    assert derive_statement_state(
        is_reconciled=False,
        has_import_record=False,
        has_draft=False,
        deferred_count=0,
        extraction_status='failed',
    ) == 'pending_extraction'


def test_derive_state_ready_default():
    """No draft, no import, no deferred, extraction is good — ready to be processed."""
    assert derive_statement_state(
        is_reconciled=False,
        has_import_record=False,
        has_draft=False,
        deferred_count=0,
        extraction_status='extracted',
    ) == 'ready'


def test_derive_state_imported_with_no_deferred_falls_to_reconciled_intent():
    """An import record without deferred rows — Stage 4 should already have run.
    Treat as 'reconciled' for the purpose of state derivation."""
    assert derive_statement_state(
        is_reconciled=False,
        has_import_record=True,
        has_draft=False,
        deferred_count=0,
        extraction_status='extracted',
    ) == 'reconciled'
