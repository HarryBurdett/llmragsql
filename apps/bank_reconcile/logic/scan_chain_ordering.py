"""Helpers for ordering and filtering scan-all-banks statement lists.

Audit cross-cutting F9: scan_all_banks_for_statements has three
self-contained loops in its Step 5 ("sort and finalize each bank's
statements") section that are good extraction candidates:

  1. fill_missing_balances_from_cache(statements)
       Look up the PDF extraction cache for any statement whose
       opening_balance is None and patch in the cached values.

  2. sort_statements_by_chain(statements, reconciled_balance)
       Walk the chain from `reconciled_balance` forwards, picking the
       statement whose opening matches at each step. Falls back to
       sorting by opening balance if no chain match.

  3. filter_fully_reconciled_statements(...)
       Use sql_rag.period_reconciliation.check_period_reconciled to
       drop statements whose period is fully reconciled in Opera.

Each helper is pure — it operates on plain dicts and either returns
a new list or mutates in place explicitly.
"""
from __future__ import annotations

import hashlib
import logging
from datetime import date as _date_type, datetime as _dt
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ====================================================================
# fill_missing_balances_from_cache
# ====================================================================


def fill_missing_balances_from_cache(statements: List[Dict[str, Any]]) -> None:
    """For each statement with no opening_balance and a file_path,
    look up the PDF extraction cache and patch in any cached
    opening_balance, closing_balance, period_start, period_end.

    Mutates `statements` in place. Logs an info line per filled
    statement.
    """
    try:
        from sql_rag.pdf_extraction_cache import get_extraction_cache
    except Exception:
        return
    cache = get_extraction_cache()

    for stmt in statements:
        if stmt.get('opening_balance') is not None:
            continue
        file_path = stmt.get('file_path')
        if not file_path:
            continue
        try:
            fp = Path(file_path)
            if not fp.exists():
                continue
            content = fp.read_bytes()
            ph = hashlib.sha256(content).hexdigest()
            cached = cache.get(ph)
            if not cached:
                continue
            ci, _ = cached
            if ci.get('opening_balance') is not None:
                stmt['opening_balance'] = float(ci['opening_balance']) if ci['opening_balance'] else None
            if ci.get('closing_balance') is not None:
                stmt['closing_balance'] = float(ci['closing_balance']) if ci['closing_balance'] else None
            if ci.get('period_start'):
                stmt['period_start'] = ci['period_start']
            if ci.get('period_end'):
                stmt['period_end'] = ci['period_end']
            logger.info(
                f"Step 5: filled missing data from cache for {stmt.get('filename')} — "
                f"open={stmt.get('opening_balance')}"
            )
        except Exception as e:
            logger.debug(f"Step 5: cache lookup failed for {stmt.get('filename')}: {e}")


# ====================================================================
# sort_statements_by_chain
# ====================================================================


def sort_statements_by_chain(
    statements: List[Dict[str, Any]],
    reconciled_balance: Optional[float],
) -> List[Dict[str, Any]]:
    """Order statements by walking the balance chain forwards.

    Starting from `reconciled_balance`, find the statement whose
    opening_balance matches (within £0.01) and pick it; advance to
    its closing balance; repeat. If no exact match is found, sort
    the remaining by opening balance and append.

    Falls back to a simple opening-balance sort (with sort_key
    tiebreaker) if there's only one statement or no reconciled
    balance.
    """
    if reconciled_balance is None or len(statements) <= 1:
        return sorted(
            statements,
            key=lambda s: (
                0 if s.get('opening_balance') is not None else 1,
                s.get('opening_balance') or 0,
                s.get('sort_key', (9999,)),
            ),
        )

    ordered: List[Dict[str, Any]] = []
    remaining = list(statements)
    current_bal = reconciled_balance
    while remaining:
        best_idx = None
        for i, s in enumerate(remaining):
            opening = s.get('opening_balance')
            if opening is not None and abs(opening - current_bal) <= 0.01:
                best_idx = i
                break
        if best_idx is not None:
            picked = remaining.pop(best_idx)
            ordered.append(picked)
            closing = picked.get('closing_balance')
            current_bal = closing if closing is not None else current_bal
        else:
            remaining.sort(key=lambda s: (s.get('opening_balance') or float('inf')))
            ordered.extend(remaining)
            break
    return ordered


# ====================================================================
# filter_fully_reconciled_statements
# ====================================================================


def _to_date(v):
    """Best-effort conversion of statement period_start/period_end
    values (str | datetime | date | None) to a date or None."""
    if v is None:
        return None
    if isinstance(v, _date_type) and not isinstance(v, _dt):
        return v
    if isinstance(v, _dt):
        return v.date()
    if isinstance(v, str):
        try:
            return _dt.fromisoformat(v.replace('Z', '+00:00')).date()
        except ValueError:
            try:
                return _date_type.fromisoformat(v[:10])
            except ValueError:
                return None
    return None


def filter_fully_reconciled_statements(
    statements: List[Dict[str, Any]],
    sql_connector,
    bank_code: str,
    reconciled_balance: Optional[float],
) -> List[Dict[str, Any]]:
    """Drop statements whose period is fully reconciled in Opera.

    Uses sql_rag.period_reconciliation.check_period_reconciled as
    the single source of truth (matches what the Opera Cashbook >
    Reconcile UI considers "done").

    Returns the filtered list; logs which statements were dropped
    and why.
    """
    if reconciled_balance is None or not statements:
        return statements

    try:
        from sql_rag.period_reconciliation import (
            check_period_reconciled, PeriodReconciliationStatus,
        )
        from sql_rag.period_reconciliation_se import OperaSEDataSource
    except Exception:
        return statements

    ds = OperaSEDataSource(sql_connector) if sql_connector else None
    if ds is None:
        return statements

    chained: List[Dict[str, Any]] = []
    unchained: List[Dict[str, Any]] = []
    for s in statements:
        result = check_period_reconciled(
            data_source=ds,
            bank_code=bank_code,
            period_start=_to_date(s.get('period_start')),
            period_end=_to_date(s.get('period_end')),
            statement_closing=s.get('closing_balance'),
            current_rec_bal=reconciled_balance,
        )
        if result.status is PeriodReconciliationStatus.FULLY_RECONCILED:
            chained.append(s)
            logger.info(
                f"Step 5 chain: filtering {s.get('filename', '?')} — "
                f"{result.reason}"
            )
        else:
            unchained.append(s)

    logger.info(
        f"Step 5 chain: {bank_code} chained={len(chained)} unchained={len(unchained)}"
    )
    return unchained
