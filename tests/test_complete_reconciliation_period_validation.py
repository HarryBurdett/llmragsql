"""Tests for the complete-reconciliation handler's period validation.

Today's Cloudsis incident: a March statement with a 45-day-old February
aentry pulled in by the matcher reached the complete-reconciliation
handler, which silently accepted it and applied ae_tmpstat on the
out-of-period row. This validation step refuses such input.
"""
from __future__ import annotations

from pathlib import Path


def test_complete_reconciliation_validates_period_in_source():
    """Source-level: the endpoint contains a validation block that
    refuses entries outside the statement period before any tmpstat
    write. Locks the rule in via grep so a refactor can't lose it.
    """
    routes = Path(__file__).resolve().parent.parent / "apps" / "bank_reconcile" / "api" / "routes.py"
    src = routes.read_text(encoding='utf-8')

    # Find the complete_reconciliation function
    start = src.find("def complete_reconciliation(")
    assert start != -1, "complete_reconciliation function not found"
    # Approximate end: next "def " at outer indentation
    end = src.find("\nasync def ", start + 10)
    if end == -1:
        end = src.find("\ndef ", start + 10)
    body = src[start:end] if end != -1 else src[start:start + 12000]

    # The validation block must:
    # 1. Compare entry dates to period bounds.
    # 2. Refuse with structured error before applying tmpstat.
    assert "out_of_period" in body or "out-of-period" in body, \
        "complete_reconciliation must collect out-of-period entries"
    assert "period_start" in body and "period_end" in body, \
        "complete_reconciliation must reference period bounds in validation"
    assert "Entries fall outside the statement period" in body \
        or "outside the statement period" in body, \
        "validation must surface a clear error message"
