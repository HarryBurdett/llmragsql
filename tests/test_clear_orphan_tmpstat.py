"""Tests for the orphan-tmpstat clear endpoint."""
from __future__ import annotations

from pathlib import Path


def test_clear_orphan_tmpstat_endpoint_exists_and_filters_correctly_in_source():
    """The new endpoint must:
      1. Be registered at the expected path.
      2. Filter aentries by ae_tmpstat > 0 AND ae_reclnum = 0.
      3. Use ROWLOCK on the UPDATE.
      4. Run a SELECT first to return the list to the user.
    """
    routes = Path(__file__).resolve().parent.parent / "apps" / "bank_reconcile" / "api" / "routes.py"
    src = routes.read_text(encoding='utf-8')

    # Endpoint registered
    assert "/api/reconcile/bank/{bank_code}/clear-orphan-tmpstat" in src, \
        "clear-orphan-tmpstat endpoint not registered"

    # Find the function
    start = src.find('@router.post("/api/reconcile/bank/{bank_code}/clear-orphan-tmpstat")')
    if start == -1:
        start = src.find('"/api/reconcile/bank/{bank_code}/clear-orphan-tmpstat"')
    assert start != -1
    end = src.find("\n@router.", start + 10)
    if end == -1:
        end = start + 6000
    body = src[start:end]

    assert "ae_tmpstat > 0" in body or "ae_tmpstat &gt; 0" in body, \
        "must filter ae_tmpstat > 0"
    assert "ae_reclnum = 0" in body or "ae_reclnum IS NULL OR ae_reclnum = 0" in body, \
        "must require ae_reclnum = 0 (orphan, not real reconcile)"
    assert "WITH (ROWLOCK)" in body or "ROWLOCK" in body, \
        "UPDATE must use ROWLOCK per project rules"
    assert "SELECT" in body and "UPDATE" in body, \
        "endpoint must SELECT (list) before UPDATE (clear)"


def test_clear_endpoint_writes_log_entry_per_clear():
    """The endpoint must log when it modifies Opera state."""
    routes = Path(__file__).resolve().parent.parent / "apps" / "bank_reconcile" / "api" / "routes.py"
    src = routes.read_text(encoding='utf-8')
    start = src.find('"/api/reconcile/bank/{bank_code}/clear-orphan-tmpstat"')
    body = src[start:start + 6000] if start != -1 else ""
    assert "logger.info" in body or "logger.warning" in body, \
        "clear endpoint must log when modifying Opera"
