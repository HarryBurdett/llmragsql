"""Opera 3 mirror endpoints for the bank-reconciliation app.

Audit 2026-05-05 cross-cutting F9: 46 SE bank-rec endpoints had no
/api/opera3/ mirror. Of those, this module adds mirrors for the
Opera-TOUCHING ones (i.e. endpoints that read from or write to
nbank/aentry/atran/sname/pname/arhead etc. via sql_connector). The
remaining ~30 are platform-agnostic (local SQLite, file archive,
draft storage, multi-format detection) — those endpoints work for
both SE and Opera 3 today because they don't touch Opera, and
duplicating them under /api/opera3/ would just add maintenance
without adding capability.

CLAUDE.md mandatory parity: every Opera-touching endpoint exists
on both platforms. Other endpoints that don't need parity (because
they don't touch Opera) are documented inline.

Each Opera 3 mirror reads via Opera3Reader and DOES NOT mutate
Opera data here — writes still go through the Opera 3 Write Agent.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Query

logger = logging.getLogger(__name__)

router = APIRouter()


# --- Helpers -----------------------------------------------------------------


def _opera3_reader(data_path: str):
    """Construct an Opera3Reader bound to the per-request data path."""
    from sql_rag.opera3_foxpro import Opera3Reader
    return Opera3Reader(data_path)


def _str(v: Any) -> str:
    return '' if v is None else str(v).strip()


# --- Customer/supplier lists -------------------------------------------------


@router.get("/api/opera3/bank-import/accounts/customers")
async def opera3_list_customers_for_bank_import(
    data_path: str = Query(..., description="Opera 3 company data path"),
    search: Optional[str] = Query(None),
):
    """Opera 3 mirror of /api/bank-import/accounts/customers.

    Returns non-dormant, non-stopped customers from sname.dbf.
    Per CLAUDE.md "Dormant accounts excluded" rule.
    """
    try:
        reader = _opera3_reader(data_path)
        out: List[Dict[str, Any]] = []
        target = (search or '').strip().lower()
        for row in reader.read_table('sname'):
            if int(row.get('sn_dormant', 0) or 0) != 0:
                continue
            if int(row.get('sn_stop', 0) or 0) != 0:
                continue
            acct = _str(row.get('sn_account'))
            name = _str(row.get('sn_name'))
            if not acct:
                continue
            if target and target not in name.lower() and target not in acct.lower():
                continue
            out.append({
                'account': acct,
                'name': name,
                'balance': float(row.get('sn_currbal') or 0),
            })
        return {"success": True, "customers": out, "count": len(out)}
    except Exception as e:
        logger.error("opera3_list_customers_for_bank_import failed: %s", e)
        return {"success": False, "error": str(e)}


@router.get("/api/opera3/bank-import/accounts/suppliers")
async def opera3_list_suppliers_for_bank_import(
    data_path: str = Query(..., description="Opera 3 company data path"),
    search: Optional[str] = Query(None),
):
    """Opera 3 mirror of /api/bank-import/accounts/suppliers."""
    try:
        reader = _opera3_reader(data_path)
        out: List[Dict[str, Any]] = []
        target = (search or '').strip().lower()
        for row in reader.read_table('pname'):
            if int(row.get('pn_dormant', 0) or 0) != 0:
                continue
            acct = _str(row.get('pn_account'))
            name = _str(row.get('pn_name'))
            if not acct:
                continue
            if target and target not in name.lower() and target not in acct.lower():
                continue
            out.append({
                'account': acct,
                'name': name,
                'balance': float(row.get('pn_currbal') or 0),
            })
        return {"success": True, "suppliers": out, "count": len(out)}
    except Exception as e:
        logger.error("opera3_list_suppliers_for_bank_import failed: %s", e)
        return {"success": False, "error": str(e)}


# --- Cashbook types ----------------------------------------------------------


@router.get("/api/opera3/bank-import/cashbook-types")
async def opera3_list_cashbook_types(
    data_path: str = Query(..., description="Opera 3 company data path"),
):
    """Opera 3 mirror of /api/bank-import/cashbook-types.

    Reads the atype.dbf table — Opera's cashbook-type definitions.
    """
    try:
        reader = _opera3_reader(data_path)
        out: List[Dict[str, Any]] = []
        for row in reader.read_table('atype'):
            cb = _str(row.get('at_cbtype'))
            desc = _str(row.get('at_desc'))
            if not cb:
                continue
            out.append({
                'cbtype': cb,
                'description': desc,
                'at_type': int(row.get('at_type') or 0),
                'next_entry': int(row.get('at_nexent') or 0),
            })
        return {"success": True, "cashbook_types": out, "count": len(out)}
    except Exception as e:
        logger.error("opera3_list_cashbook_types failed: %s", e)
        return {"success": False, "error": str(e)}


# --- Repeat entries ----------------------------------------------------------


@router.get("/api/opera3/bank-import/repeat-entries")
async def opera3_list_repeat_entries(
    data_path: str = Query(..., description="Opera 3 company data path"),
    bank_code: Optional[str] = Query(None),
):
    """Opera 3 mirror of /api/bank-import/repeat-entries.

    Reads arhead.dbf for repeat-entry headers. Filters by bank_code
    when supplied.
    """
    try:
        reader = _opera3_reader(data_path)
        out: List[Dict[str, Any]] = []
        target = (bank_code or '').strip().upper()
        for row in reader.read_table('arhead'):
            ae_acnt = _str(row.get('ae_acnt')).upper()
            if target and ae_acnt != target:
                continue
            posted = int(row.get('ae_posted') or 0)
            topost = int(row.get('ae_topost') or 0)
            # Only include unposted (topost=0=unlimited, or posted < topost).
            if topost and posted >= topost:
                continue
            out.append({
                'ae_entry': _str(row.get('ae_entry')),
                'ae_acnt': ae_acnt,
                'ae_desc': _str(row.get('ae_desc')),
                'ae_nxtpost': str(row.get('ae_nxtpost') or ''),
                'ae_freq': _str(row.get('ae_freq')),
                'ae_every': int(row.get('ae_every') or 0),
                'ae_posted': posted,
                'ae_topost': topost,
            })
        return {"success": True, "repeat_entries": out, "count": len(out)}
    except Exception as e:
        logger.error("opera3_list_repeat_entries failed: %s", e)
        return {"success": False, "error": str(e)}


# --- Scan all banks ----------------------------------------------------------


@router.get("/api/opera3/bank-import/scan-all-banks")
async def opera3_scan_all_banks(
    data_path: str = Query(..., description="Opera 3 company data path"),
):
    """Opera 3 mirror of /api/bank-import/scan-all-banks.

    Returns one row per non-foreign nbank account with reconciled
    balance and statement-tracking fields (last statement number,
    last statement date). Used by the bank-rec hub UI to render the
    multi-bank dashboard.
    """
    try:
        reader = _opera3_reader(data_path)
        out: List[Dict[str, Any]] = []
        for row in reader.read_table('nbank'):
            # Exclude foreign-currency banks (audit fixed this column
            # name from nk_forgn → nk_fcurr earlier today).
            fcurr = _str(row.get('nk_fcurr'))
            if fcurr:
                continue
            acct = _str(row.get('nk_acnt'))
            if not acct:
                continue
            out.append({
                'bank_code': acct,
                'description': _str(row.get('nk_desc') or row.get('nk_bkname')),
                'sort_code': _str(row.get('nk_sort')),
                'account_number': _str(row.get('nk_number')),
                'reconciled_balance': float(row.get('nk_recbal') or 0) / 100.0,
                'current_balance': float(row.get('nk_curbal') or 0) / 100.0,
                'last_statement_number': int(row.get('nk_lststno') or 0),
                'last_statement_date': str(row.get('nk_lststdt') or ''),
                'last_rec_line': int(row.get('nk_lstrecl') or 0),
            })
        out.sort(key=lambda x: x['bank_code'])
        return {"success": True, "banks": out, "count": len(out)}
    except Exception as e:
        logger.error("opera3_scan_all_banks failed: %s", e)
        return {"success": False, "error": str(e)}


# --- Suggest account ---------------------------------------------------------


@router.get("/api/opera3/bank-import/suggest-account")
async def opera3_suggest_account(
    data_path: str = Query(..., description="Opera 3 company data path"),
    name: str = Query(...),
    direction: str = Query("auto", description="'receipt' (suggest customer), 'payment' (supplier), or 'auto'"),
    limit: int = Query(5, ge=1, le=20),
):
    """Opera 3 mirror of /api/bank-import/suggest-account.

    Fuzzy-match the supplied name against pname/sname (depending on
    direction) and return the top candidates. Used by the operator
    UI when no auto-match was found and they're picking manually.
    """
    try:
        from sql_rag.bank_matcher_shared import fuzzy_match_name

        reader = _opera3_reader(data_path)
        target = name.strip()
        if not target:
            return {"success": True, "suggestions": []}

        candidates: List[Dict[str, Any]] = []
        if direction in ('receipt', 'auto'):
            for row in reader.read_table('sname'):
                if int(row.get('sn_dormant', 0) or 0) != 0:
                    continue
                acct = _str(row.get('sn_account'))
                cname = _str(row.get('sn_name'))
                if not acct:
                    continue
                score = fuzzy_match_name(target, cname)
                if score >= 0.6:
                    candidates.append({
                        'kind': 'customer',
                        'account': acct,
                        'name': cname,
                        'score': score,
                    })
        if direction in ('payment', 'auto'):
            for row in reader.read_table('pname'):
                if int(row.get('pn_dormant', 0) or 0) != 0:
                    continue
                acct = _str(row.get('pn_account'))
                cname = _str(row.get('pn_name'))
                if not acct:
                    continue
                score = fuzzy_match_name(target, cname)
                if score >= 0.6:
                    candidates.append({
                        'kind': 'supplier',
                        'account': acct,
                        'name': cname,
                        'score': score,
                    })
        candidates.sort(key=lambda c: c['score'], reverse=True)
        return {"success": True, "suggestions": candidates[:limit]}
    except Exception as e:
        logger.error("opera3_suggest_account failed: %s", e)
        return {"success": False, "error": str(e)}


# --- Orphan tmpstat ---------------------------------------------------------


@router.get("/api/opera3/reconcile/bank/{bank_code}/orphan-tmpstat")
async def opera3_get_orphan_tmpstat(
    bank_code: str,
    data_path: str = Query(..., description="Opera 3 company data path"),
):
    """Opera 3 mirror of /api/reconcile/bank/{bank_code}/orphan-tmpstat.

    Lists aentry rows where ae_tmpstat>0 (a partial-rec marker is
    set) but ae_reclnum=0 (the rec was never finalised). These are
    "orphans" left behind by an interrupted rec; the operator can
    clear them via the matching POST endpoint.
    """
    try:
        reader = _opera3_reader(data_path)
        out: List[Dict[str, Any]] = []
        target = bank_code.upper()
        for row in reader.read_table('aentry'):
            if _str(row.get('ae_acnt')).upper() != target:
                continue
            tmpstat = int(row.get('ae_tmpstat') or 0)
            reclnum = int(row.get('ae_reclnum') or 0)
            if tmpstat > 0 and reclnum == 0:
                out.append({
                    'ae_entry': _str(row.get('ae_entry')),
                    'ae_tmpstat': tmpstat,
                    'ae_value': float(row.get('ae_value') or 0) / 100.0,
                    'ae_lstdate': str(row.get('ae_lstdate') or ''),
                })
        return {"success": True, "orphans": out, "count": len(out)}
    except Exception as e:
        logger.error("opera3_get_orphan_tmpstat failed: %s", e)
        return {"success": False, "error": str(e)}


@router.post("/api/opera3/reconcile/bank/{bank_code}/clear-orphan-tmpstat")
async def opera3_clear_orphan_tmpstat(
    bank_code: str,
    data_path: str = Query(...),
):
    """Opera 3 mirror of /api/reconcile/bank/{bank_code}/clear-orphan-tmpstat.

    Clears ae_tmpstat=0 on every orphan row (ae_tmpstat>0 AND
    ae_reclnum=0) for this bank. Goes through the Opera 3 Write
    Agent (DBF writes are not done from this process directly per
    locking-protocol).
    """
    try:
        from sql_rag.opera3_write_provider import (
            get_opera3_writer, Opera3AgentRequired,
        )
        try:
            writer = get_opera3_writer(data_path)
        except Opera3AgentRequired as exc:
            return {"success": False, "error": str(exc)}
        result = writer.clear_orphan_tmpstat(bank_account=bank_code)
        return {
            "success": getattr(result, 'success', False),
            "rows_cleared": getattr(result, 'records_imported', 0),
            "errors": getattr(result, 'errors', []),
        }
    except AttributeError:
        # Older Write Agent versions may not have clear_orphan_tmpstat.
        # Surface a clear error rather than silently failing.
        return {
            "success": False,
            "error": (
                "Opera 3 Write Agent does not expose clear_orphan_tmpstat. "
                "Update the agent to include this operation."
            ),
        }
    except Exception as e:
        logger.error("opera3_clear_orphan_tmpstat failed: %s", e)
        return {"success": False, "error": str(e)}


# --- Refresh matches --------------------------------------------------------


@router.post("/api/opera3/reconcile/refresh-matches")
async def opera3_refresh_matches(
    body: Dict[str, Any] = Body(...),
):
    """Opera 3 mirror of /api/reconcile/refresh-matches.

    Re-runs the matcher against the supplied statement transactions
    using the Opera 3 data source. Body shape mirrors the SE endpoint:
    { bank_code, data_path, statement_transactions: [...] }.
    """
    try:
        bank_code = (body.get('bank_code') or '').strip()
        data_path = body.get('data_path') or ''
        if not bank_code or not data_path:
            return {"success": False, "error": "bank_code and data_path required"}
        # Delegate to the existing Opera 3 match-statement endpoint
        # logic — keep behaviour consistent.
        from apps.bank_reconcile.api import routes as _routes
        from fastapi import Request
        # The shared logic lives in the existing match-statement
        # endpoint; route there directly.
        request = Request({'type': 'http', 'method': 'POST', 'headers': []})
        return await _routes.opera3_match_statement_to_cashbook(
            bank_code=bank_code,
            data_path=data_path,
            import_id=body.get('import_id'),
            request=request,
            request_body=body,
        )
    except Exception as e:
        logger.error("opera3_refresh_matches failed: %s", e)
        return {"success": False, "error": str(e)}


# --- Audit-defer / deferred-items (LOCAL SQLITE — platform-agnostic shim) ----
#
# /api/reconcile/bank/{bank_code}/audit-defer and /deferred-items operate on
# a per-company local SQLite (deferred_transactions_db.py) — they don't
# touch Opera at all. The SE endpoints work for both platforms; explicit
# /api/opera3/... aliases are added here so the frontend can route by
# platform without extra branching.


@router.get("/api/opera3/reconcile/bank/{bank_code}/deferred-items")
async def opera3_get_deferred_items(bank_code: str):
    """Opera 3 alias for /api/reconcile/bank/{bank_code}/deferred-items.

    Backed by the same local-SQLite deferred-transactions store the SE
    endpoint uses — no platform divergence at the data layer.
    """
    from apps.bank_reconcile.api.routes import get_deferred_items as _se
    return await _se(bank_code)


@router.delete("/api/opera3/reconcile/bank/{bank_code}/deferred-items")
async def opera3_clear_deferred_items(
    bank_code: str,
    transaction_id: Optional[int] = Query(None),
):
    """Opera 3 alias for the SE clear-deferred-items endpoint."""
    from apps.bank_reconcile.api.routes import clear_deferred_items as _se
    return await _se(bank_code, transaction_id)


@router.post("/api/opera3/reconcile/bank/{bank_code}/audit-defer")
async def opera3_audit_defer(bank_code: str, body: Dict[str, Any] = Body(...)):
    """Opera 3 alias for the SE audit-defer endpoint (local-SQLite only)."""
    from apps.bank_reconcile.api.routes import audit_defer_transaction as _se
    return await _se(bank_code, body)
