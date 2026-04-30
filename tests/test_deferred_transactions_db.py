"""Tests for sql_rag/deferred_transactions_db.py

Covers schema initialisation, single-record write, repeated writes, and the
count_for_bank helper used to verify audit traceability in higher-level tests.
"""

import os
import tempfile

import pytest

from sql_rag.deferred_transactions_db import DeferredTransactionsDB


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
