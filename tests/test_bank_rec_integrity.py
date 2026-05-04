"""Tests for the bank-rec startup integrity check.

The check inspects every nbank row for any sign that a non-canonical
(Path A or Path B style) reconciliation has corrupted the bank's
sequence counters. The post-fix invariant is:

    nk_lstrecl == nk_reclnum     (both point at the next batch number)

If they diverge, a non-canonical write path has touched the bank and
the next reconciliation would inherit the corruption.

The check is read-only (NOLOCK), runs at API startup and after every
`_ensure_company_context()` switch, and emits one WARNING per offending
bank. It NEVER mutates Opera.
"""

import logging
from unittest.mock import MagicMock

import pytest

from sql_rag.bank_rec_integrity import (
    BankRecIntegrityIssue,
    check_bank_rec_integrity,
    log_bank_rec_integrity,
)


def _make_sql_connector(rows):
    """Stub a sql_connector whose execute_query returns a fake DataFrame."""
    fake_df = MagicMock()
    fake_df.iterrows.return_value = enumerate(rows)
    fake_df.__len__ = lambda self: len(rows)
    fake_df.empty = (len(rows) == 0)

    sc = MagicMock()
    sc.execute_query.return_value = fake_df
    return sc


# ---------------------------------------------------------------------------
# Pure logic
# ---------------------------------------------------------------------------

def test_clean_state_returns_no_issues():
    """nk_lstrecl == nk_reclnum on every bank → no issues."""
    rows = [
        {"nk_acnt": "BB005", "nk_desc": "Monzo", "nk_lstrecl": 209, "nk_reclnum": 209,
         "nk_recbal": 5059248},
        {"nk_acnt": "BB025", "nk_desc": "Tide", "nk_lstrecl": 0, "nk_reclnum": 0,
         "nk_recbal": 0},
    ]
    sc = _make_sql_connector(rows)
    issues = check_bank_rec_integrity(sc)
    assert issues == []


def test_lstrecl_ahead_of_reclnum_flagged():
    """The historical Cloudsis pattern: nk_lstrecl > 0, nk_reclnum = 0."""
    rows = [
        {"nk_acnt": "BB005", "nk_desc": "Monzo",
         "nk_lstrecl": 212, "nk_reclnum": 0, "nk_recbal": 5059248},
    ]
    sc = _make_sql_connector(rows)
    issues = check_bank_rec_integrity(sc)
    assert len(issues) == 1
    issue = issues[0]
    assert isinstance(issue, BankRecIntegrityIssue)
    assert issue.bank_acnt == "BB005"
    assert issue.kind == "reclnum_lstrecl_mismatch"
    assert "212" in issue.detail
    assert "0" in issue.detail


def test_reclnum_ahead_of_lstrecl_flagged():
    """Reverse mismatch is also a sign of corruption."""
    rows = [
        {"nk_acnt": "BC010", "nk_desc": "Barclays",
         "nk_lstrecl": 5, "nk_reclnum": 9, "nk_recbal": 0},
    ]
    sc = _make_sql_connector(rows)
    issues = check_bank_rec_integrity(sc)
    assert len(issues) == 1
    assert issues[0].bank_acnt == "BC010"


def test_null_values_treated_as_zero():
    """SQL NULL on either field = 0 = no mismatch (untouched bank)."""
    rows = [
        {"nk_acnt": "BB005", "nk_desc": "Monzo",
         "nk_lstrecl": None, "nk_reclnum": None, "nk_recbal": 0},
        {"nk_acnt": "BB025", "nk_desc": "Tide",
         "nk_lstrecl": 0, "nk_reclnum": None, "nk_recbal": 0},
        {"nk_acnt": "BB030", "nk_desc": "Tide Saver",
         "nk_lstrecl": None, "nk_reclnum": 0, "nk_recbal": 0},
    ]
    sc = _make_sql_connector(rows)
    issues = check_bank_rec_integrity(sc)
    assert issues == []


def test_empty_bank_list_returns_no_issues():
    """A company with no banks is fine."""
    sc = _make_sql_connector([])
    assert check_bank_rec_integrity(sc) == []


def test_multiple_banks_only_flagged_ones_returned():
    """Mixed clean and dirty banks — only dirty ones in the result."""
    rows = [
        {"nk_acnt": "BB005", "nk_desc": "Monzo",
         "nk_lstrecl": 209, "nk_reclnum": 0, "nk_recbal": 5059248},  # dirty
        {"nk_acnt": "BB025", "nk_desc": "Tide",
         "nk_lstrecl": 12, "nk_reclnum": 12, "nk_recbal": 96946},     # clean
        {"nk_acnt": "BB030", "nk_desc": "Tide Saver",
         "nk_lstrecl": 5, "nk_reclnum": 5, "nk_recbal": 8936481},     # clean
        {"nk_acnt": "BC010", "nk_desc": "Barclays",
         "nk_lstrecl": 100, "nk_reclnum": 99, "nk_recbal": 1000000},  # dirty
    ]
    sc = _make_sql_connector(rows)
    issues = check_bank_rec_integrity(sc)
    assert len(issues) == 2
    flagged = sorted(i.bank_acnt for i in issues)
    assert flagged == ["BB005", "BC010"]


# ---------------------------------------------------------------------------
# Logging integration
# ---------------------------------------------------------------------------

def test_log_bank_rec_integrity_emits_warning_per_issue(caplog):
    rows = [
        {"nk_acnt": "BB005", "nk_desc": "Monzo",
         "nk_lstrecl": 212, "nk_reclnum": 0, "nk_recbal": 5059248},
        {"nk_acnt": "BC010", "nk_desc": "Barclays",
         "nk_lstrecl": 100, "nk_reclnum": 99, "nk_recbal": 1000000},
    ]
    sc = _make_sql_connector(rows)
    with caplog.at_level(logging.WARNING, logger="sql_rag.bank_rec_integrity"):
        log_bank_rec_integrity(sc, company_id="cloudsis")
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 2
    assert any("BB005" in r.message for r in warnings)
    assert any("BC010" in r.message for r in warnings)
    # Each warning mentions the company_id so multi-company logs are searchable
    assert all("cloudsis" in r.message for r in warnings)


def test_log_bank_rec_integrity_silent_when_clean(caplog):
    rows = [
        {"nk_acnt": "BB005", "nk_desc": "Monzo",
         "nk_lstrecl": 209, "nk_reclnum": 209, "nk_recbal": 5059248},
    ]
    sc = _make_sql_connector(rows)
    with caplog.at_level(logging.WARNING, logger="sql_rag.bank_rec_integrity"):
        log_bank_rec_integrity(sc, company_id="cloudsis")
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings == []


def test_log_bank_rec_integrity_handles_sql_failure_silently(caplog):
    """A read-only check should never block startup if the DB is down."""
    sc = MagicMock()
    sc.execute_query.side_effect = RuntimeError("connection refused")
    with caplog.at_level(logging.WARNING, logger="sql_rag.bank_rec_integrity"):
        log_bank_rec_integrity(sc, company_id="cloudsis")
    # Should log a warning that the check couldn't run, but not raise
    assert any("could not run" in r.message.lower() or "failed" in r.message.lower()
               for r in caplog.records)
