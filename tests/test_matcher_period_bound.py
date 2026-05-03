"""Tests for the matcher's period-bound candidate restriction.

Critical regression coverage: today's bug paired a 2026-02-28 aentry
with a 2026-03-01..2026-03-31 statement using a 45-day date tolerance.
The fix is to bound the candidate pool by the statement period itself
(plus a small grace window), rejecting out-of-period candidates
deterministically.
"""
from __future__ import annotations

import inspect
from datetime import date

import pytest


def test_match_statement_to_cashbook_signature_has_period_bounds():
    """Signature pins the new keyword arguments — explicit, not magic."""
    from sql_rag.opera_sql_import import OperaSQLImport
    sig = inspect.signature(OperaSQLImport.match_statement_to_cashbook)
    assert 'period_start' in sig.parameters
    assert 'period_end' in sig.parameters
    assert 'period_grace_days' in sig.parameters


def test_match_query_restricts_aentry_pool_by_period_bounds():
    """The SQL emitted for the unreconciled-aentry candidate pool must
    include `ae_lstdate BETWEEN '<period_start - grace>' AND '<period_end + grace>'`.
    """
    captured: list[str] = []

    class _Spy:
        def execute_query(self, q):
            captured.append(q)
            class _DF:
                empty = True
                def to_dict(self, *a, **k): return []
                def iterrows(self): return iter([])
            return _DF()

    from sql_rag.opera_sql_import import OperaSQLImport
    op = OperaSQLImport(_Spy())
    op.match_statement_to_cashbook(
        bank_account="BB005",
        statement_transactions=[],
        period_start=date(2026, 3, 1),
        period_end=date(2026, 3, 31),
        period_grace_days=7,
    )
    # At least one query targets aentry on this bank with the
    # period-bounded ae_lstdate filter.
    aentry_queries = [q for q in captured
                      if "FROM aentry" in q and "ae_acnt = 'BB005'" in q]
    assert aentry_queries, f"no aentry candidate query emitted: {captured}"
    bounded = [q for q in aentry_queries if "ae_lstdate BETWEEN" in q]
    assert bounded, (
        "aentry candidate query must include "
        "`ae_lstdate BETWEEN <period_start-grace> AND <period_end+grace>`"
    )
    # Verify exact bounds
    q = bounded[0]
    assert "'2026-02-22'" in q  # period_start - 7 days
    assert "'2026-04-07'" in q  # period_end + 7 days


def test_match_falls_back_with_warning_when_period_bounds_missing():
    """Backwards-compat: if period bounds aren't passed, fall back to
    the old date-tolerance behaviour but log a warning.
    """
    captured_logs: list[str] = []

    import logging
    handler = logging.StreamHandler()
    class _Capture(logging.Handler):
        def emit(self, record):
            captured_logs.append(record.getMessage())
    test_handler = _Capture()
    logger = logging.getLogger("sql_rag.opera_sql_import")
    logger.addHandler(test_handler)

    class _Stub:
        def execute_query(self, q):
            class _DF:
                empty = True
                def to_dict(self, *a, **k): return []
                def iterrows(self): return iter([])
            return _DF()

    from sql_rag.opera_sql_import import OperaSQLImport
    try:
        op = OperaSQLImport(_Stub())
        op.match_statement_to_cashbook(
            bank_account="BB005",
            statement_transactions=[],
            period_start=None,
            period_end=None,
        )
    finally:
        logger.removeHandler(test_handler)

    assert any("period bounds not provided" in m.lower() for m in captured_logs), \
        f"expected fallback warning; got logs: {captured_logs}"
