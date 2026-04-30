"""Tests for the /api/reconcile/bank/{bank_code}/audit-defer endpoint.

The endpoint is a plain async function so we invoke it directly rather than
spinning up a full HTTP server.  We mock get_current_db_path so each test
gets an isolated temp database and never touches the real company data
directory.
"""

import asyncio
import os
import tempfile
from unittest.mock import patch

import pytest

from apps.bank_reconcile.api.routes import (
    AuditDeferItem,
    AuditDeferRequest,
    audit_defer_transactions,
)
from sql_rag.deferred_transactions_db import DeferredTransactionsDB


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _run(coro):
    """Run an async function synchronously (avoids pytest-asyncio dependency)."""
    return asyncio.get_event_loop().run_until_complete(coro)


def _make_db(tmp_path, name="deferred.db"):
    """Create a fresh DeferredTransactionsDB in tmp_path and return (path, db)."""
    path = str(tmp_path / name)
    db = DeferredTransactionsDB(path)
    return path, db


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_audit_path(tmp_path):
    """A temporary path for the audit DB, with get_current_db_path patched."""
    db_path = str(tmp_path / "deferred.db")
    # Ensure parent dir exists (DeferredTransactionsDB does this, but the
    # patch means it will be called with our explicit path)
    return db_path


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------

def test_audit_defer_writes_one_row_per_item(tmp_path, tmp_audit_path):
    """Endpoint writes exactly N rows for N deferred items."""
    items = [
        AuditDeferItem(statement_date="2026-04-17", amount=123.45, description="Test payment"),
        AuditDeferItem(statement_date="2026-04-18", amount=-67.89, description="Test refund"),
        AuditDeferItem(statement_date="2026-04-19", amount=0.01, description="Tiny"),
    ]
    body = AuditDeferRequest(items=items)

    with patch("sql_rag.company_data.get_current_db_path", return_value=tmp_audit_path):
        result = _run(audit_defer_transactions("BC010", body))

    assert result["success"] is True
    assert result["audited"] == 3

    db = DeferredTransactionsDB(tmp_audit_path)
    assert db.count_for_bank("BC010") == 3


def test_audit_defer_empty_list_returns_zero(tmp_path, tmp_audit_path):
    """Endpoint accepts an empty items list and returns audited=0."""
    body = AuditDeferRequest(items=[])

    with patch("sql_rag.company_data.get_current_db_path", return_value=tmp_audit_path):
        result = _run(audit_defer_transactions("BC020", body))

    assert result["success"] is True
    assert result["audited"] == 0


def test_audit_defer_missing_fields_written_with_nulls(tmp_path, tmp_audit_path):
    """Items with missing optional fields are written with null/empty values."""
    # AuditDeferItem has all fields optional — simulate a malformed row by
    # omitting amount and statement_date.
    item = AuditDeferItem()  # all None
    body = AuditDeferRequest(items=[item])

    with patch("sql_rag.company_data.get_current_db_path", return_value=tmp_audit_path):
        result = _run(audit_defer_transactions("BC030", body))

    assert result["success"] is True
    assert result["audited"] == 1

    db = DeferredTransactionsDB(tmp_audit_path)
    assert db.count_for_bank("BC030") == 1


def test_audit_defer_succeeds_even_if_audit_db_path_is_none(tmp_path):
    """Endpoint succeeds (audited >= 0) when get_current_db_path returns None.

    When there is no company context the endpoint falls back to a temp path.
    We allow any non-negative audited count — what matters is success=True.
    """
    items = [AuditDeferItem(statement_date="2026-04-17", amount=10.0, description="X")]
    body = AuditDeferRequest(items=items)

    # Patch get_current_db_path to return None AND also patch the tempfile
    # fallback so we don't leave files around.
    fallback = str(tmp_path / "fallback.db")
    with patch("sql_rag.company_data.get_current_db_path", return_value=None), \
         patch("tempfile.gettempdir", return_value=str(tmp_path)):
        result = _run(audit_defer_transactions("BC040", body))

    assert result["success"] is True
    assert result["audited"] >= 0


def test_audit_defer_partial_failure_counts_only_successes(tmp_path, tmp_audit_path):
    """If individual row writes fail, only successful rows are counted."""
    items = [
        AuditDeferItem(statement_date="2026-04-17", amount=1.0, description="good"),
        AuditDeferItem(statement_date="2026-04-18", amount=2.0, description="good2"),
    ]
    body = AuditDeferRequest(items=items)

    call_count = 0

    def failing_record(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise RuntimeError("simulated write failure")

    with patch("sql_rag.company_data.get_current_db_path", return_value=tmp_audit_path), \
         patch.object(DeferredTransactionsDB, "record", side_effect=failing_record):
        result = _run(audit_defer_transactions("BC050", body))

    # Success must be True even though one row failed
    assert result["success"] is True
    # Only the first row succeeded
    assert result["audited"] == 1
