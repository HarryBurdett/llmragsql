"""Single source of truth: 'is this aentry row an open item for bank-rec?'

An Opera atran/aentry row is a candidate for matching against a new bank
statement iff:

  ae_reclnum = 0    AND    ae_remove = 0

Reconciled entries (ae_reclnum > 0) belong to past statements and never
re-match — once reconciled, an entry is deemed correct accounting and final.

Correction-pair-matched entries (ae_remove = True) are settled via Opera's
matching facility (the operator linked a mistaken posting with its reversing
entry); both sides cancel out and don't appear in bank reconciliation.

Both filters MUST be applied at every candidate-fetch site. Anywhere we
touch aentry to find rec candidates, import from here.

See:
  - apps/core/docs/opera_knowledge_base.md  ('Bank Rec Open-Items Rule')
  - ~/opera-knowledge-ref/.../business-rules/bank-rec-open-items.md
  - tests/test_bank_rec_candidate_filter.py  (contract test)
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping


OPEN_FOR_REC_SQL = "ae_reclnum = 0 AND ae_remove = 0"
"""SQL WHERE-clause fragment. Append to a query that already has aentry
in scope (either via FROM or via JOIN). Prefix with the table alias if
needed: e.g. ``f"a.{OPEN_FOR_REC_SQL.replace('ae_', 'a.ae_')}"``."""


def _coerce_reclnum(v: Any) -> int:
    """Coerce a possibly-Decimal/None reclnum to int. None → 0."""
    if v is None:
        return 0
    if isinstance(v, Decimal):
        return int(v)
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def _coerce_remove(v: Any) -> bool:
    """Coerce a possibly-string/None ae_remove to bool. None / 'F' / 0 / False → False."""
    if v is None or v is False or v == 0:
        return False
    if isinstance(v, str):
        return v.strip().upper() in ('T', 'TRUE', '1', 'Y', 'YES')
    return bool(v)


def is_open_for_rec(aentry_row: Mapping[str, Any]) -> bool:
    """Python equivalent of OPEN_FOR_REC_SQL for in-memory filters.

    Args:
        aentry_row: A dict-like with at least 'ae_reclnum' and 'ae_remove'
            keys (either or both may be missing — defaults are 0/False).

    Returns:
        True iff the row is an open item eligible for rec matching.
    """
    reclnum = _coerce_reclnum(aentry_row.get('ae_reclnum'))
    if reclnum != 0:
        return False
    if _coerce_remove(aentry_row.get('ae_remove')):
        return False
    return True
