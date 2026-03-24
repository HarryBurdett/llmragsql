"""
Bank Reconcile App — API Routes

All bank statement import, reconciliation, cashbook, and archive endpoints.
Covers both Opera SE and Opera 3.
"""

from fastapi import APIRouter, Query, Body, Request, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse, StreamingResponse
from typing import Optional, List, Dict, Any
import logging
import os
import json
from datetime import datetime, date, timedelta
from pathlib import Path

from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter()

# Cashbook recurring entry type/frequency descriptions
TYPE_DESCRIPTIONS = {
    1: "Nominal Payment",
    2: "Nominal Receipt",
    3: "Sales Refund",
    4: "Sales Receipt",
    5: "Purchase Payment",
    6: "Purchase Refund",
}

FREQ_DESCRIPTIONS = {
    "D": "Daily",
    "W": "Weekly",
    "M": "Monthly",
    "Q": "Quarterly",
    "Y": "Yearly",
}


class ReconcileEntriesRequest(BaseModel):
    """Request body for marking entries as reconciled."""
    entries: List[dict]
    statement_number: int
    statement_date: Optional[str] = None
    reconciliation_date: Optional[str] = None


# Per-request globals — populated by _sync_from_main(), called from
# _ensure_company_context() in api/main.py after swapping company resources.
# Single-threaded async (one uvicorn worker) makes this safe.
sql_connector = None
email_storage = None
email_sync_manager = None
config = None
current_company = None
customer_linker = None
friendly_db_error = None
_bank_lock_key = None
_load_company_settings = None
_save_company_settings = None
_compute_similarity_key = None
_get_bank_subfolder_name = None
_get_opera3_provider = None
is_bank_statement_attachment = None
detect_bank_from_email = None
extract_statement_number_from_filename = None

_SYNC_NAMES = (
    'sql_connector', 'email_storage', 'email_sync_manager',
    'config', 'current_company', 'customer_linker',
    'friendly_db_error', '_bank_lock_key',
    '_load_company_settings', '_save_company_settings',
    '_compute_similarity_key', '_get_bank_subfolder_name',
    '_get_opera3_provider', 'is_bank_statement_attachment',
    'detect_bank_from_email', 'extract_statement_number_from_filename',
)

def _sync_from_main():
    """Propagate per-request globals from api.main into this module."""
    import api.main as m
    g = globals()
    for name in _SYNC_NAMES:
        g[name] = getattr(m, name, None)


@router.get("/api/reconcile/banks")
async def get_bank_accounts():
    """
    Get list of bank accounts for reconciliation.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Get bank accounts from nbank
        banks_sql = """
            SELECT nk_acnt AS account_code, RTRIM(nk_desc) AS description,
                   nk_sort AS sort_code, nk_number AS account_number
            FROM nbank WITH (NOLOCK)
            ORDER BY nk_acnt
        """
        banks = sql_connector.execute_query(banks_sql)
        if hasattr(banks, 'to_dict'):
            banks = banks.to_dict('records')

        return {
            "success": True,
            "banks": [
                {
                    "account_code": b['account_code'].strip() if b['account_code'] else '',
                    "description": b['description'].strip() if b['description'] else '',
                    "sort_code": b['sort_code'].strip() if b['sort_code'] else '',
                    "account_number": b['account_number'].strip() if b['account_number'] else ''
                }
                for b in banks or []
            ]
        }
    except Exception as e:
        logger.error(f"Failed to get bank accounts: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/reconcile/bank/{bank_code}")
async def reconcile_bank(bank_code: str):
    """
    Reconcile a specific bank account (aentry) to its Nominal Ledger control account.
    Uses anoml transfer file to identify pending postings.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        reconciliation = {
            "success": True,
            "reconciliation_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "bank_code": bank_code,
            "bank_account": {},
            "cashbook": {},
            "nominal_ledger": {},
            "variance": {},
            "status": "UNRECONCILED",
            "details": []
        }

        # Get bank account details
        bank_sql = f"""
            SELECT nk_acnt, RTRIM(nk_desc) AS description, nk_sort, nk_number
            FROM nbank
            WHERE nk_acnt = '{bank_code}'
        """
        bank_result = sql_connector.execute_query(bank_sql)
        if hasattr(bank_result, 'to_dict'):
            bank_result = bank_result.to_dict('records')

        if not bank_result:
            return {"success": False, "error": f"Bank account {bank_code} not found"}

        bank_info = bank_result[0]
        reconciliation["bank_account"] = {
            "code": bank_info['nk_acnt'].strip(),
            "description": bank_info['description'] or '',
            "sort_code": bank_info['nk_sort'].strip() if bank_info['nk_sort'] else '',
            "account_number": bank_info['nk_number'].strip() if bank_info['nk_number'] else ''
        }

        # ========== NOMINAL LEDGER SETUP (get current year first) ==========
        current_year_sql = "SELECT MAX(nt_year) AS current_year FROM ntran"
        cy_result = sql_connector.execute_query(current_year_sql)
        if hasattr(cy_result, 'to_dict'):
            cy_result = cy_result.to_dict('records')
        current_year = int(cy_result[0]['current_year']) if cy_result and cy_result[0]['current_year'] else datetime.now().year

        # ========== CASHBOOK (aentry/atran) ==========
        # Get CURRENT YEAR cashbook movements from atran (amounts stored in PENCE)
        # atran does NOT include B/F entries - only actual transactions
        cb_current_year_sql = f"""
            SELECT
                COUNT(DISTINCT at_entry) AS entry_count,
                COUNT(*) AS transaction_count,
                SUM(CASE WHEN at_value > 0 THEN at_value ELSE 0 END) AS receipts_pence,
                SUM(CASE WHEN at_value < 0 THEN ABS(at_value) ELSE 0 END) AS payments_pence,
                SUM(at_value) AS net_pence
            FROM atran
            WHERE at_acnt = '{bank_code}'
              AND YEAR(at_pstdate) = {current_year}
        """
        cb_cy_result = sql_connector.execute_query(cb_current_year_sql)
        if hasattr(cb_cy_result, 'to_dict'):
            cb_cy_result = cb_cy_result.to_dict('records')

        cb_cy_count = int(cb_cy_result[0]['entry_count'] or 0) if cb_cy_result else 0
        cb_cy_txn_count = int(cb_cy_result[0]['transaction_count'] or 0) if cb_cy_result else 0
        cb_cy_receipts_pence = float(cb_cy_result[0]['receipts_pence'] or 0) if cb_cy_result else 0
        cb_cy_payments_pence = float(cb_cy_result[0]['payments_pence'] or 0) if cb_cy_result else 0
        cb_cy_net_pence = float(cb_cy_result[0]['net_pence'] or 0) if cb_cy_result else 0

        # Convert to pounds
        cb_cy_receipts_pounds = cb_cy_receipts_pence / 100
        cb_cy_payments_pounds = cb_cy_payments_pence / 100
        cb_cy_movements = cb_cy_net_pence / 100  # Current year movements only

        # Also get ALL TIME totals for reference
        cb_all_sql = f"""
            SELECT
                COUNT(DISTINCT at_entry) AS entry_count,
                COUNT(*) AS transaction_count,
                SUM(at_value) AS net_pence
            FROM atran
            WHERE at_acnt = '{bank_code}'
        """
        cb_all_result = sql_connector.execute_query(cb_all_sql)
        if hasattr(cb_all_result, 'to_dict'):
            cb_all_result = cb_all_result.to_dict('records')
        cb_all_count = int(cb_all_result[0]['entry_count'] or 0) if cb_all_result else 0
        cb_all_net_pence = float(cb_all_result[0]['net_pence'] or 0) if cb_all_result else 0
        cb_all_total = cb_all_net_pence / 100

        # ========== BANK MASTER (nbank.nk_curbal) ==========
        # Get the running balance from nbank - stored in PENCE
        # This represents the CURRENT closing balance
        nbank_bal_sql = f"""
            SELECT nk_curbal FROM nbank WHERE nk_acnt = '{bank_code}'
        """
        nbank_result = sql_connector.execute_query(nbank_bal_sql)
        if hasattr(nbank_result, 'to_dict'):
            nbank_result = nbank_result.to_dict('records')
        nbank_curbal_pence = float(nbank_result[0]['nk_curbal'] or 0) if nbank_result else 0
        nbank_curbal_pounds = nbank_curbal_pence / 100

        # ========== NOMINAL LEDGER ==========
        # Get the nominal ledger balance for this bank account
        current_year_sql = "SELECT MAX(nt_year) AS current_year FROM ntran"
        cy_result = sql_connector.execute_query(current_year_sql)
        if hasattr(cy_result, 'to_dict'):
            cy_result = cy_result.to_dict('records')
        current_year = int(cy_result[0]['current_year']) if cy_result and cy_result[0]['current_year'] else datetime.now().year

        # Get account details from nacnt
        nacnt_sql = f"""
            SELECT na_acnt, RTRIM(na_desc) AS description, na_ytddr, na_ytdcr, na_prydr, na_prycr
            FROM nacnt
            WHERE na_acnt = '{bank_code}'
        """
        nacnt_result = sql_connector.execute_query(nacnt_sql)
        if hasattr(nacnt_result, 'to_dict'):
            nacnt_result = nacnt_result.to_dict('records')

        nl_total = 0
        nl_details = {}
        if nacnt_result:
            acc = nacnt_result[0]
            pry_dr = float(acc['na_prydr'] or 0)
            pry_cr = float(acc['na_prycr'] or 0)
            bf_balance = pry_dr - pry_cr

            # Get current year transactions
            ntran_sql = f"""
                SELECT
                    SUM(CASE WHEN nt_value > 0 THEN nt_value ELSE 0 END) AS debits,
                    SUM(CASE WHEN nt_value < 0 THEN ABS(nt_value) ELSE 0 END) AS credits,
                    SUM(nt_value) AS net
                FROM ntran
                WHERE nt_acnt = '{bank_code}' AND nt_year = {current_year}
            """
            ntran_result = sql_connector.execute_query(ntran_sql)
            if hasattr(ntran_result, 'to_dict'):
                ntran_result = ntran_result.to_dict('records')

            current_year_dr = float(ntran_result[0]['debits'] or 0) if ntran_result else 0
            current_year_cr = float(ntran_result[0]['credits'] or 0) if ntran_result else 0
            current_year_net = float(ntran_result[0]['net'] or 0) if ntran_result else 0

            # Bank is a debit balance account (same logic as debtors control)
            # Use current year net for reconciliation (consistent with creditors/debtors)
            current_year_balance = current_year_net if current_year_net > 0 else abs(current_year_net)
            closing_balance = current_year_balance
            nl_total = current_year_balance

            nl_details = {
                "source": "ntran (Nominal Ledger)",
                "account": bank_code,
                "description": acc['description'] or '',
                "current_year": current_year,
                "brought_forward": round(bf_balance, 2),
                "current_year_debits": round(current_year_dr, 2),
                "current_year_credits": round(current_year_cr, 2),
                "current_year_net": round(current_year_net, 2),
                "closing_balance": round(closing_balance, 2),
                "total_balance": round(nl_total, 2)
            }
        else:
            nl_details = {
                "source": "ntran (Nominal Ledger)",
                "account": bank_code,
                "description": "Account not found in nacnt",
                "total_balance": 0
            }

        # Calculate expected closing balance:
        # atran current year movements + nacnt prior year B/F = expected closing
        cb_expected_closing = cb_cy_movements + bf_balance if nacnt_result else cb_cy_movements

        # For bank reconciliation, we compare:
        # 1. cb_expected_closing - atran movements + B/F
        # 2. nbank_curbal_pounds - bank master current balance
        # 3. nl_total - ntran current year net (includes B/F entry)
        # All three should match when fully reconciled

        # ========== TRANSFER FILE (anoml) ==========
        # Check for transactions in the transfer file for this bank
        # ax_nacnt contains the nominal account (which for banks is the bank code itself)
        anoml_pending_sql = f"""
            SELECT
                ax_nacnt AS nominal_account,
                ax_source AS source,
                ax_date AS date,
                ax_value AS value,
                ax_tref AS reference,
                ax_comment AS comment,
                ax_done AS status
            FROM anoml
            WHERE ax_nacnt = '{bank_code}' AND (ax_done <> 'Y' OR ax_done IS NULL)
            ORDER BY ax_date DESC
        """
        try:
            anoml_pending = sql_connector.execute_query(anoml_pending_sql)
            if hasattr(anoml_pending, 'to_dict'):
                anoml_pending = anoml_pending.to_dict('records')
        except Exception:
            anoml_pending = []

        # Count posted vs pending in transfer file for this bank
        anoml_summary_sql = f"""
            SELECT
                CASE WHEN ax_done = 'Y' THEN 'Posted' ELSE 'Pending' END AS status,
                COUNT(*) AS count,
                SUM(ax_value) AS total
            FROM anoml
            WHERE ax_nacnt = '{bank_code}'
            GROUP BY CASE WHEN ax_done = 'Y' THEN 'Posted' ELSE 'Pending' END
        """
        try:
            anoml_summary = sql_connector.execute_query(anoml_summary_sql)
            if hasattr(anoml_summary, 'to_dict'):
                anoml_summary = anoml_summary.to_dict('records')
        except Exception:
            anoml_summary = []

        posted_count = 0
        posted_total = 0
        pending_count = 0
        pending_total = 0
        for row in anoml_summary or []:
            if row['status'] == 'Posted':
                posted_count = int(row['count'] or 0)
                posted_total = float(row['total'] or 0)
            else:
                pending_count = int(row['count'] or 0)
                pending_total = float(row['total'] or 0)

        # Build pending transactions list
        pending_transactions = []
        for row in anoml_pending or []:
            tr_date = row['date']
            if hasattr(tr_date, 'strftime'):
                tr_date = tr_date.strftime('%Y-%m-%d')
            value = float(row['value'] or 0)
            source_desc = {'P': 'Purchase', 'S': 'Sales', 'A': 'Cashbook', 'J': 'Journal'}.get(
                row['source'].strip() if row['source'] else '', row['source'] or ''
            )
            pending_transactions.append({
                "nominal_account": row['nominal_account'].strip() if row['nominal_account'] else '',
                "source": row['source'].strip() if row['source'] else '',
                "source_desc": source_desc,
                "date": str(tr_date) if tr_date else '',
                "value": round(value, 2),
                "reference": row['reference'].strip() if row['reference'] else '',
                "comment": row['comment'].strip() if row['comment'] else ''
            })

        reconciliation["cashbook"] = {
            "source": "atran (Cashbook Transactions)",
            "current_year": current_year,
            "current_year_entries": cb_cy_count,
            "current_year_transactions": cb_cy_txn_count,
            "current_year_receipts": round(cb_cy_receipts_pounds, 2),
            "current_year_payments": round(cb_cy_payments_pounds, 2),
            "current_year_movements": round(cb_cy_movements, 2),
            "prior_year_bf": round(bf_balance, 2) if nacnt_result else 0,
            "expected_closing": round(cb_expected_closing, 2),
            "all_time_entries": cb_all_count,
            "all_time_net": round(cb_all_total, 2),
            "transfer_file": {
                "source": "anoml (Cashbook to Nominal Transfer File)",
                "posted_to_nl": {
                    "count": posted_count,
                    "total": round(posted_total, 2)
                },
                "pending_transfer": {
                    "count": pending_count,
                    "total": round(pending_total, 2),
                    "transactions": pending_transactions
                }
            }
        }

        # Bank master balance
        reconciliation["bank_master"] = {
            "source": "nbank.nk_curbal (Bank Master Balance)",
            "balance_pence": round(nbank_curbal_pence, 0),
            "balance_pounds": round(nbank_curbal_pounds, 2)
        }

        # Nominal ledger details already calculated above
        reconciliation["nominal_ledger"] = nl_details

        # ========== VARIANCE CALCULATION ==========
        # Primary comparison: Cashbook expected closing vs nbank.nk_curbal
        # (atran movements + B/F should equal bank master balance)
        variance_cb_nbank = cb_expected_closing - nbank_curbal_pounds
        variance_cb_nbank_abs = abs(variance_cb_nbank)

        # Secondary comparison: Bank Master vs Nominal Ledger current year net
        # (nbank.nk_curbal should equal ntran current year total)
        variance_nbank_nl = nbank_curbal_pounds - nl_total
        variance_nbank_nl_abs = abs(variance_nbank_nl)

        # Tertiary comparison: Cashbook expected vs Nominal Ledger
        variance_cb_nl = cb_expected_closing - nl_total
        variance_cb_nl_abs = abs(variance_cb_nl)

        # All three should match when fully reconciled
        all_reconciled = variance_cb_nbank_abs < 1.00 and variance_nbank_nl_abs < 1.00

        reconciliation["variance"] = {
            "cashbook_vs_bank_master": {
                "description": "atran movements + B/F vs nbank.nk_curbal",
                "cashbook_expected": round(cb_expected_closing, 2),
                "bank_master": round(nbank_curbal_pounds, 2),
                "amount": round(variance_cb_nbank, 2),
                "absolute": round(variance_cb_nbank_abs, 2),
                "reconciled": variance_cb_nbank_abs < 1.00
            },
            "bank_master_vs_nominal": {
                "description": "nbank.nk_curbal vs ntran current year",
                "bank_master": round(nbank_curbal_pounds, 2),
                "nominal_ledger": round(nl_total, 2),
                "amount": round(variance_nbank_nl, 2),
                "absolute": round(variance_nbank_nl_abs, 2),
                "reconciled": variance_nbank_nl_abs < 1.00
            },
            "cashbook_vs_nominal": {
                "description": "atran expected vs ntran",
                "cashbook_expected": round(cb_expected_closing, 2),
                "nominal_ledger": round(nl_total, 2),
                "amount": round(variance_cb_nl, 2),
                "absolute": round(variance_cb_nl_abs, 2),
                "reconciled": variance_cb_nl_abs < 1.00
            },
            "summary": {
                "current_year": current_year,
                "cashbook_movements": round(cb_cy_movements, 2),
                "prior_year_bf": round(bf_balance, 2) if nacnt_result else 0,
                "cashbook_expected_closing": round(cb_expected_closing, 2),
                "bank_master_balance": round(nbank_curbal_pounds, 2),
                "nominal_ledger_balance": round(nl_total, 2),
                "transfer_file_pending": round(pending_total, 2),
                "all_reconciled": all_reconciled,
                "has_pending_transfers": pending_count > 0
            }
        }

        # Determine status based on all three sources matching
        if all_reconciled:
            reconciliation["status"] = "RECONCILED"
            if pending_count > 0:
                reconciliation["message"] = f"Bank {bank_code} reconciles across all sources. {pending_count} entries (£{abs(pending_total):,.2f}) in transfer file pending."
            else:
                reconciliation["message"] = f"Bank {bank_code} fully reconciles: Cashbook = Bank Master = Nominal Ledger"
        else:
            reconciliation["status"] = "UNRECONCILED"
            # Build detailed message showing where mismatches occur
            issues = []
            if variance_cb_nl_abs >= 1.00:
                if variance_cb_nl > 0:
                    issues.append(f"Cashbook £{variance_cb_nl_abs:,.2f} MORE than NL")
                else:
                    issues.append(f"Cashbook £{variance_cb_nl_abs:,.2f} LESS than NL")
            if variance_cb_nbank_abs >= 1.00:
                if variance_cb_nbank > 0:
                    issues.append(f"Cashbook £{variance_cb_nbank_abs:,.2f} MORE than Bank Master")
                else:
                    issues.append(f"Cashbook £{variance_cb_nbank_abs:,.2f} LESS than Bank Master")
            if variance_nbank_nl_abs >= 1.00:
                if variance_nbank_nl > 0:
                    issues.append(f"Bank Master £{variance_nbank_nl_abs:,.2f} MORE than NL")
                else:
                    issues.append(f"Bank Master £{variance_nbank_nl_abs:,.2f} LESS than NL")
            reconciliation["message"] = "; ".join(issues) if issues else "Variance detected"

        return reconciliation

    except Exception as e:
        logger.error(f"Bank reconciliation failed for {bank_code}: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/reconcile/bank/{bank_code}/status")
async def get_bank_reconciliation_status(bank_code: str):
    """
    Get current bank reconciliation status including balances and unreconciled counts.
    Also checks if there's a reconciliation in progress in Opera.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        from sql_rag.opera_sql_import import OperaSQLImport
        from sql_rag.statement_reconcile import StatementReconciler
        opera = OperaSQLImport(sql_connector)

        status = opera.get_reconciliation_status(bank_code)

        if 'error' in status:
            return {"success": False, "error": status['error']}

        # Check if there's a reconciliation in progress in Opera
        reconciler = StatementReconciler(sql_connector, config=config)
        rec_in_progress = reconciler.check_reconciliation_in_progress(bank_code)

        return {
            "success": True,
            "reconciliation_in_progress": rec_in_progress.get('in_progress', False),
            "reconciliation_in_progress_message": rec_in_progress.get('message') if rec_in_progress.get('in_progress') else None,
            "partial_entries": rec_in_progress.get('partial_entries', 0) if rec_in_progress.get('in_progress') else 0,
            **status
        }
    except Exception as e:
        logger.error(f"Failed to get reconciliation status for {bank_code}: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/reconcile/bank/{bank_code}/unreconciled")
async def get_unreconciled_entries(bank_code: str, include_incomplete: bool = Query(False, description="Include incomplete (not posted to NL) entries")):
    """
    Get list of unreconciled cashbook entries for a bank account.

    By default, excludes incomplete batches (ae_complet = 0) which are "hidden"
    transactions not yet posted to the Nominal Ledger.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        from sql_rag.opera_sql_import import OperaSQLImport
        opera = OperaSQLImport(sql_connector)

        entries = opera.get_unreconciled_entries(bank_code, include_incomplete=include_incomplete)

        return {
            "success": True,
            "bank_code": bank_code,
            "count": len(entries),
            "entries": entries
        }
    except Exception as e:
        logger.error(f"Failed to get unreconciled entries for {bank_code}: {e}")
        return {"success": False, "error": str(e)}





@router.post("/api/reconcile/bank/{bank_code}/complete-batch/{entry_number}")
async def complete_batch(bank_code: str, entry_number: str):
    """
    Complete an incomplete cashbook batch, making it available for reconciliation.

    Reads unposted anoml (transfer file) records, creates ntran entries,
    updates nacnt/nhist/nbank balances, marks anoml as posted, and sets ae_complet = 1.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    # Acquire bank-level lock
    from sql_rag.import_lock import acquire_import_lock, release_import_lock
    if not acquire_import_lock(_bank_lock_key(bank_code), locked_by="api", endpoint="complete-batch"):
        return {"success": False, "error": f"Bank account {bank_code} is currently being modified by another user. Please wait and try again."}

    try:
        from sql_rag.opera_sql_import import OperaSQLImport
        opera = OperaSQLImport(sql_connector)

        result = opera.complete_batch_posting(bank_code, entry_number)

        release_import_lock(_bank_lock_key(bank_code))
        if result.success:
            return {
                "success": True,
                "entry_number": entry_number,
                "message": f"Batch {entry_number} completed and posted to nominal",
                "details": result.warnings
            }
        else:
            return {
                "success": False,
                "error": "; ".join(result.errors)
            }

    except Exception as e:
        logger.error(f"Failed to complete batch {entry_number}: {e}")
        try:
            release_import_lock(_bank_lock_key(bank_code))
        except Exception:
            pass
        return {"success": False, "error": str(e)}





@router.post("/api/reconcile/bank/{bank_code}/mark-reconciled")
async def mark_entries_reconciled(bank_code: str, request: ReconcileEntriesRequest):
    """
    Mark cashbook entries as reconciled.

    This replicates Opera's Bank Reconciliation routine:
    - Updates aentry records with reconciliation batch number, statement line, etc.
    - Updates nbank master with new reconciled balance

    Request body:
    {
        "entries": [
            {"entry_number": "P100008036", "statement_line": 10},
            {"entry_number": "PR00000534", "statement_line": 20}
        ],
        "statement_number": 86918,
        "statement_date": "2026-02-08",
        "reconciliation_date": "2026-02-08"
    }
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    # Acquire bank-level lock
    from sql_rag.import_lock import acquire_import_lock, release_import_lock
    if not acquire_import_lock(_bank_lock_key(bank_code), locked_by="api", endpoint="mark-reconciled"):
        return {"success": False, "error": f"Bank account {bank_code} is currently being modified by another user. Please wait and try again."}

    try:
        from sql_rag.opera_sql_import import OperaSQLImport
        from datetime import datetime

        opera = OperaSQLImport(sql_connector)

        # Parse dates if provided
        stmt_date = None
        rec_date = None
        if request.statement_date:
            stmt_date = datetime.strptime(request.statement_date, '%Y-%m-%d').date()
        if request.reconciliation_date:
            rec_date = datetime.strptime(request.reconciliation_date, '%Y-%m-%d').date()

        result = opera.mark_entries_reconciled(
            bank_account=bank_code,
            entries=request.entries,
            statement_number=request.statement_number,
            statement_date=stmt_date,
            reconciliation_date=rec_date
        )

        release_import_lock(_bank_lock_key(bank_code))
        if result.success:
            return {
                "success": True,
                "message": f"Reconciled {result.records_imported} entries",
                "records_reconciled": result.records_imported,
                "details": result.warnings
            }
        else:
            return {
                "success": False,
                "errors": result.errors
            }
    except Exception as e:
        logger.error(f"Failed to mark entries reconciled for {bank_code}: {e}")
        try:
            release_import_lock(_bank_lock_key(bank_code))
        except Exception:
            pass
        return {"success": False, "error": str(e)}





@router.post("/api/reconcile/bank/{bank_code}/unreconcile")
async def unreconcile_entries(bank_code: str, entry_numbers: List[str]):
    """
    Unreconcile previously reconciled entries (reverse reconciliation).

    Request body: ["P100008036", "PR00000534"]
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    # Acquire bank-level lock
    from sql_rag.import_lock import acquire_import_lock, release_import_lock
    if not acquire_import_lock(_bank_lock_key(bank_code), locked_by="api", endpoint="unreconcile"):
        return {"success": False, "error": f"Bank account {bank_code} is currently being modified by another user. Please wait and try again."}

    try:
        from datetime import datetime

        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        entry_list = "', '".join(entry_numbers)

        # Reset reconciliation fields
        update_sql = f"""
            UPDATE aentry WITH (ROWLOCK)
            SET ae_reclnum = 0,
                ae_recdate = NULL,
                ae_statln = 0,
                ae_frstat = 0,
                ae_tostat = 0,
                ae_tmpstat = 0,
                datemodified = '{now_str}'
            WHERE ae_acnt = '{bank_code}'
              AND ae_entry IN ('{entry_list}')
              AND ae_reclnum > 0
        """

        with sql_connector.engine.connect() as conn:
            trans = conn.begin()
            try:
                from sqlalchemy import text
                result = conn.execute(text(update_sql))
                rows_affected = result.rowcount

                # Recalculate nbank reconciled balance
                recalc_sql = f"""
                    SELECT COALESCE(SUM(ae_value), 0) as reconciled_total
                    FROM aentry WITH (NOLOCK)
                    WHERE ae_acnt = '{bank_code}'
                      AND ae_reclnum > 0
                """
                recalc_result = conn.execute(text(recalc_sql))
                new_rec_total = float(recalc_result.fetchone()[0] or 0)

                # Update nbank
                nbank_update = f"""
                    UPDATE nbank WITH (ROWLOCK)
                    SET nk_recbal = {int(new_rec_total)},
                        datemodified = '{now_str}'
                    WHERE nk_acnt = '{bank_code}'
                """
                conn.execute(text(nbank_update))

                trans.commit()

                release_import_lock(_bank_lock_key(bank_code))
                return {
                    "success": True,
                    "message": f"Unreconciled {rows_affected} entries",
                    "entries_unreconciled": rows_affected,
                    "new_reconciled_balance": new_rec_total / 100.0
                }
            except Exception as e:
                trans.rollback()
                raise

    except Exception as e:
        logger.error(f"Failed to unreconcile entries for {bank_code}: {e}")
        try:
            release_import_lock(_bank_lock_key(bank_code))
        except Exception:
            pass
        return {"success": False, "error": str(e)}





@router.post("/api/reconcile/bank/{bank_code}/ignore-transaction")
async def ignore_bank_transaction(
    bank_code: str,
    transaction_date: str = Query(..., description="Transaction date (YYYY-MM-DD)"),
    amount: float = Query(..., description="Transaction amount in pounds"),
    description: str = Query(None, description="Transaction description"),
    reference: str = Query(None, description="Transaction reference"),
    reason: str = Query(None, description="Reason for ignoring")
):
    """
    Mark a bank transaction as ignored for reconciliation.

    This is used for transactions that appear on bank statements but have
    already been entered in Opera manually (e.g., GoCardless receipts).
    """
    try:
        record_id = email_storage.ignore_bank_transaction(
            bank_account=bank_code,
            transaction_date=transaction_date,
            amount=amount,
            description=description,
            reference=reference,
            reason=reason,
            ignored_by="API"
        )
        return {
            "success": True,
            "message": f"Transaction ignored: £{amount:.2f} on {transaction_date}",
            "record_id": record_id
        }
    except Exception as e:
        logger.error(f"Failed to ignore transaction: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/reconcile/bank/{bank_code}/ignored-transactions")
async def get_ignored_transactions(
    bank_code: str,
    limit: int = Query(100, description="Maximum records to return")
):
    """
    Get list of ignored transactions for a bank account.
    """
    try:
        transactions = email_storage.get_ignored_transactions(
            bank_account=bank_code,
            limit=limit
        )
        return {
            "success": True,
            "transactions": transactions,
            "count": len(transactions)
        }
    except Exception as e:
        logger.error(f"Failed to get ignored transactions: {e}")
        return {"success": False, "error": str(e)}





@router.delete("/api/reconcile/bank/ignored-transaction/{record_id}")
async def unignore_transaction(record_id: int):
    """
    Remove a transaction from the ignored list.
    """
    try:
        deleted = email_storage.unignore_transaction(record_id)
        if deleted:
            return {"success": True, "message": "Transaction removed from ignored list"}
        else:
            return {"success": False, "error": "Record not found"}
    except Exception as e:
        logger.error(f"Failed to unignore transaction: {e}")
        return {"success": False, "error": str(e)}





@router.delete("/api/reconcile/bank/{bank_code}/unignore-transaction")
async def unignore_transaction_by_match(
    bank_code: str,
    transaction_date: str = Query(...),
    amount: float = Query(...)
):
    """
    Remove a transaction from the ignored list by matching bank, date and amount.
    Used when user re-checks the include checkbox on an unmatched item.
    """
    try:
        deleted = email_storage.unignore_transaction_by_match(bank_code, transaction_date, amount)
        if deleted:
            return {"success": True, "message": "Transaction removed from ignored list"}
        else:
            return {"success": False, "error": "No matching ignored transaction found"}
    except Exception as e:
        logger.error(f"Failed to unignore transaction: {e}")
        return {"success": False, "error": str(e)}





@router.post("/api/reconcile/process-statement")
async def process_bank_statement(
    file_path: str,
    bank_code: str = Query(..., description="Opera bank account code (selected by user)")
):
    """
    Process a bank statement PDF/image and extract transactions for matching.

    Workflow:
    1. User selects bank account from dropdown
    2. User provides statement file path
    3. System validates statement matches selected bank account
    4. System extracts and matches transactions

    Args:
        file_path: Path to the statement file (PDF or image)
        bank_code: Opera bank account code (user-selected)

    Returns:
        Statement info, validation result, extracted transactions, and matches
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        from sql_rag.statement_reconcile import StatementReconciler, StatementInfo
        from pathlib import Path

        if not Path(file_path).exists():
            return {"success": False, "error": f"File not found: {file_path}"}

        # Pass config to use configured Gemini API key and model
        reconciler = StatementReconciler(sql_connector, config=config)
        logger.info(f"process-statement-SE: reconciler created for {Path(file_path).name}")

        # Check if there's a reconciliation in progress in Opera
        try:
            rec_in_progress = reconciler.check_reconciliation_in_progress(bank_code)
        except Exception as rec_err:
            logger.error(f"process-statement-SE: check_reconciliation_in_progress failed: {rec_err}")
            rec_in_progress = {'in_progress': False}
        if rec_in_progress['in_progress']:
            return {
                "success": False,
                "error": rec_in_progress['message'],
                "bank_code": bank_code,
                "reconciliation_in_progress": True,
                "partial_entries": rec_in_progress.get('partial_entries', 0)
            }

        # Extract transactions from statement
        logger.info(f"process-statement-SE: BEFORE extraction for {Path(file_path).name}")
        import sys as _sys
        _sys.stderr.write(f"EXTRACT: calling extract_transactions_from_pdf for {file_path}\n")
        _sys.stderr.flush()
        statement_info, transactions = reconciler.extract_transactions_from_pdf(file_path)
        _sys.stderr.write(f"EXTRACT: got {len(transactions)} transactions, info_open={statement_info.opening_balance}\n")
        _sys.stderr.flush()
        logger.info(f"process-statement-SE: AFTER extraction — got {len(transactions)} transactions")

        # Validate that statement matches the selected bank account
        bank_validation = reconciler.validate_statement_bank(bank_code, statement_info)
        if not bank_validation['valid']:
            return {
                "success": False,
                "error": bank_validation['error'],
                "bank_code": bank_code,
                "bank_validation": bank_validation,
                "statement_info": {
                    "bank_name": statement_info.bank_name,
                    "account_number": statement_info.account_number,
                    "sort_code": statement_info.sort_code
                }
            }

        # Check for statement period overlap (prevent double-posting)
        if email_storage and statement_info:
            period_start_str = statement_info.period_start.isoformat() if statement_info.period_start else None
            period_end_str = statement_info.period_end.isoformat() if statement_info.period_end else None
            # Fall back to first/last transaction dates
            if not period_start_str and transactions:
                dates = [t.date for t in transactions if t.date]
                if dates:
                    period_start_str = min(dates).isoformat() if hasattr(min(dates), 'isoformat') else str(min(dates))
                    period_end_str = max(dates).isoformat() if hasattr(max(dates), 'isoformat') else str(max(dates))

            if period_start_str and period_end_str:
                overlap = email_storage.check_period_overlap(
                    bank_code=bank_code,
                    period_start=period_start_str,
                    period_end=period_end_str
                )
                if overlap:
                    # Verify the overlapping import's entries still exist in Opera
                    # (database may have been restored, making the tracking record stale)
                    overlap_id = overlap.get('import_id')
                    if overlap_id and sql_connector:
                        try:
                            # Get entry numbers recorded for the previous import
                            recorded_entries = email_storage.get_import_entry_numbers(overlap_id)
                            if recorded_entries:
                                # Check if any of those entries exist in Opera
                                entry_list = "', '".join(recorded_entries[:5])  # Check first 5
                                with sql_connector.engine.connect() as oconn:
                                    result = oconn.execute(text(f"""
                                        SELECT COUNT(*) FROM aentry WITH (NOLOCK)
                                        WHERE ae_acnt = '{bank_code}'
                                          AND ae_entry IN ('{entry_list}')
                                    """))
                                    found = result.scalar() or 0
                                if found == 0:
                                    # Entries don't exist in Opera — database was restored
                                    logger.info(f"Auto-removing stale import record {overlap_id} (entries not found in Opera — database restored)")
                                    email_storage.delete_import_record(overlap_id)
                                    overlap = None
                            else:
                                # No transactions recorded — stale record
                                logger.info(f"Auto-removing stale import record {overlap_id} (no transactions recorded)")
                                email_storage.delete_import_record(overlap_id)
                                overlap = None
                        except Exception as e:
                            logger.warning(f"Could not verify overlap record {overlap_id}: {e}")
                if overlap:
                    return {
                        "success": False,
                        "overlap_warning": True,
                        "error": f"This statement period ({period_start_str} to {period_end_str}) overlaps with a previously imported statement: '{overlap['filename']}' ({overlap['period_start']} to {overlap['period_end']}). Clear the previous import first or choose a different statement.",
                        "overlap_details": overlap,
                        "bank_code": bank_code,
                        "statement_info": {
                            "bank_name": statement_info.bank_name,
                            "account_number": statement_info.account_number,
                            "opening_balance": statement_info.opening_balance,
                            "closing_balance": statement_info.closing_balance,
                        }
                    }

        # Validate statement sequence (opening balance must match reconciled balance)
        sequence_validation = reconciler.validate_statement_sequence(bank_code, statement_info)
        rec_bal_validated = sequence_validation.get('reconciled_balance')

        # If the extracted opening balance doesn't match the reconciled balance,
        # the AI likely misread the PDF (e.g. picked up a savings account balance).
        # Use the reconciled balance as the authoritative opening balance.
        if sequence_validation['status'] != 'process' and rec_bal_validated is not None:
            logger.warning(f"Opening balance mismatch: extracted £{statement_info.opening_balance}, "
                           f"Opera reconciled £{rec_bal_validated:.2f} — using reconciled balance")
            statement_info = StatementInfo(
                bank_name=statement_info.bank_name,
                account_number=statement_info.account_number,
                sort_code=statement_info.sort_code,
                statement_date=statement_info.statement_date,
                period_start=statement_info.period_start,
                period_end=statement_info.period_end,
                opening_balance=rec_bal_validated,
                closing_balance=statement_info.closing_balance
            )

        # Get unreconciled Opera entries for the date range
        # Use wider date range to catch entries that might have been posted with slightly different dates
        # Add 14 days buffer on each side to catch entries posted with different dates
        # (GC payouts, bank transfers can take several days to settle)
        from datetime import timedelta
        date_from = statement_info.period_start - timedelta(days=14) if statement_info.period_start else None
        date_to = statement_info.period_end + timedelta(days=14) if statement_info.period_end else None

        opera_entries = reconciler.get_unreconciled_entries(
            bank_code,
            date_from=date_from,
            date_to=date_to
        )

        logger.info(f"Statement processing: {len(transactions)} stmt txns, {len(opera_entries)} unreconciled Opera entries")
        logger.info(f"Date range: {date_from} to {date_to}")
        if opera_entries:
            logger.info(f"Sample Opera entry: {opera_entries[0]}")
        if transactions:
            logger.info(f"Sample statement txn: date={transactions[0].date}, amount={transactions[0].amount}, desc={transactions[0].description[:50] if transactions[0].description else 'N/A'}")

        # Match transactions - use 7 day tolerance to catch entries imported with different dates
        matches, unmatched_stmt, unmatched_opera = reconciler.match_transactions(
            transactions, opera_entries, date_tolerance_days=7
        )

        logger.info(f"Matching result: {len(matches)} matches, {len(unmatched_stmt)} unmatched stmt, {len(unmatched_opera)} unmatched opera")

        # Get Opera bank reconciliation status for display
        opera_status = reconciler.get_bank_reconciliation_status(bank_code)

        # Format response
        return {
            "success": True,
            "bank_code": bank_code,
            "bank_validation": bank_validation,  # Validation that statement matches selected bank
            "statement_info": {
                "bank_name": statement_info.bank_name,
                "account_number": statement_info.account_number,
                "sort_code": statement_info.sort_code,
                "statement_date": statement_info.statement_date.isoformat() if statement_info.statement_date else None,
                "period_start": statement_info.period_start.isoformat() if statement_info.period_start else None,
                "period_end": statement_info.period_end.isoformat() if statement_info.period_end else None,
                "opening_balance": statement_info.opening_balance,
                "closing_balance": statement_info.closing_balance
            },
            # Opera reconciliation status - reliable data from Opera
            "opera_status": {
                "reconciled_balance": opera_status.get('reconciled_balance'),
                "current_balance": opera_status.get('current_balance'),
                "last_statement_number": opera_status.get('last_statement_number'),
                "last_reconciliation_date": opera_status.get('last_reconciliation_date')
            },
            "extracted_transactions": len(transactions),
            "opera_unreconciled": len(opera_entries),
            "matches": [
                {
                    "statement_txn": {
                        "date": m.statement_txn.date.isoformat(),
                        "description": m.statement_txn.description,
                        "amount": m.statement_txn.amount,
                        "balance": m.statement_txn.balance,
                        "type": m.statement_txn.transaction_type
                    },
                    "opera_entry": {
                        "ae_entry": m.opera_entry['ae_entry'],
                        "ae_date": m.opera_entry['ae_date'].isoformat() if hasattr(m.opera_entry['ae_date'], 'isoformat') else str(m.opera_entry['ae_date']),
                        "ae_ref": m.opera_entry['ae_ref'],
                        "value_pounds": m.opera_entry['value_pounds'],
                        "ae_detail": m.opera_entry.get('ae_detail', '')
                    },
                    "match_score": m.match_score,
                    "match_reasons": m.match_reasons
                }
                for m in matches
            ],
            "unmatched_statement": [
                {
                    "date": t.date.isoformat(),
                    "description": t.description,
                    "amount": t.amount,
                    "balance": t.balance,
                    "type": t.transaction_type
                }
                for t in unmatched_stmt
                if not email_storage.is_transaction_ignored(bank_code, t.date.isoformat(), t.amount)
            ],
            "unmatched_opera": [
                {
                    "ae_entry": e['ae_entry'],
                    "ae_date": e['ae_date'].isoformat() if hasattr(e['ae_date'], 'isoformat') else str(e['ae_date']),
                    "ae_ref": e['ae_ref'],
                    "value_pounds": e['value_pounds'],
                    "ae_detail": e.get('ae_detail', '')
                }
                for e in unmatched_opera
            ]
        }

    except Exception as e:
        logger.error(f"Failed to process statement: {e}", exc_info=True)
        return {"success": True, "error": str(e), "extracted_transactions": 0,
                "matches": [], "unmatched_statement": [], "unmatched_opera": [],
                "statement_info": {}, "opera_status": {}, "bank_code": bank_code,
                "bank_validation": {}, "_exception": str(e)}





@router.post("/api/reconcile/process-statement-unified")
async def process_statement_unified(
    file_path: str,
    bank_code: str = Query(..., description="Opera bank account code (selected by user)")
):
    """
    Unified statement processing: identifies transactions to IMPORT and RECONCILE.

    Workflow:
    1. User selects bank account from dropdown
    2. User provides statement file path
    3. System extracts transactions from PDF
    4. System VALIDATES statement matches selected bank (sort code/account number)
    5. System matches against existing Opera entries
    6. System identifies new transactions for import and existing ones to reconcile

    Args:
        file_path: Path to the statement PDF
        bank_code: Opera bank account code (user-selected from dropdown)

    Returns:
        bank_code: The selected bank code
        bank_validation: Validation result (confirms statement matches selected account)
        to_import: Transactions not in Opera (need importing)
        to_reconcile: Matches with unreconciled Opera entries
        already_reconciled: Matches with already reconciled entries (verification)
        balance_check: Closing balance verification
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        from sql_rag.statement_reconcile import StatementReconciler
        from pathlib import Path

        if not Path(file_path).exists():
            return {"success": False, "error": f"File not found: {file_path}"}

        # Pass config to use configured Gemini API key and model
        reconciler = StatementReconciler(sql_connector, config=config)

        # Use the unified processing method - bank_code can be None for auto-detection
        result = reconciler.process_statement_unified(bank_code, file_path)

        # Format the response
        def format_stmt_txn(txn):
            return {
                "date": txn.date.isoformat() if hasattr(txn.date, 'isoformat') else str(txn.date),
                "description": txn.description,
                "amount": txn.amount,
                "balance": txn.balance,
                "type": txn.transaction_type,
                "reference": txn.reference
            }

        def format_match(m):
            return {
                "statement_txn": format_stmt_txn(m['statement_txn']),
                "opera_entry": {
                    "ae_entry": m['opera_entry']['ae_entry'],
                    "ae_date": m['opera_entry']['ae_date'].isoformat() if hasattr(m['opera_entry']['ae_date'], 'isoformat') else str(m['opera_entry']['ae_date']),
                    "ae_ref": m['opera_entry']['ae_ref'],
                    "value_pounds": m['opera_entry']['value_pounds'],
                    "ae_detail": m['opera_entry'].get('ae_detail', ''),
                    "is_reconciled": m['opera_entry'].get('is_reconciled', False)
                },
                "match_score": m['match_score'],
                "match_reasons": m['match_reasons']
            }

        # Handle error case (e.g., bank not found)
        if not result.get('success', False):
            return result

        stmt_info = result['statement_info']

        return {
            "success": True,
            "bank_code": result.get('bank_code'),
            "bank_validation": result.get('bank_validation'),  # Validation that statement matches selected bank
            "statement_info": {
                "bank_name": stmt_info.bank_name,
                "account_number": stmt_info.account_number,
                "sort_code": stmt_info.sort_code,
                "statement_date": stmt_info.statement_date.isoformat() if stmt_info.statement_date else None,
                "period_start": stmt_info.period_start.isoformat() if stmt_info.period_start else None,
                "period_end": stmt_info.period_end.isoformat() if stmt_info.period_end else None,
                "opening_balance": stmt_info.opening_balance,
                "closing_balance": stmt_info.closing_balance
            },
            "summary": result['summary'],
            "to_import": [format_stmt_txn(txn) for txn in result['to_import']],
            "to_reconcile": [format_match(m) for m in result['to_reconcile']],
            "already_reconciled": [format_match(m) for m in result['already_reconciled']],
            "balance_check": result['balance_check']
        }

    except Exception as e:
        logger.error(f"Failed to process unified statement: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}





@router.post("/api/reconcile/bank/{bank_code}/import-from-statement")
async def import_from_statement(
    bank_code: str,
    transactions: List[Dict],
    statement_date: str
):
    """
    Import transactions from a bank statement using the existing bank import matching logic.

    This uses the same matching infrastructure as CSV imports but with PDF-extracted data.
    Transactions are matched against customers/suppliers and categorized automatically.

    Args:
        bank_code: The bank account code
        transactions: List of transactions to import (date, description, amount, type)
        statement_date: Statement date for reference

    Returns:
        Preview of how transactions would be categorized (same format as CSV preview)
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        from datetime import datetime
        from sql_rag.bank_import import BankStatementImporter, BankTransaction

        # Create bank transactions from the PDF-extracted data
        bank_txns = []
        for i, txn in enumerate(transactions):
            amount = float(txn['amount'])
            txn_date = datetime.strptime(txn['date'][:10], '%Y-%m-%d')

            bank_txn = BankTransaction(
                row_number=i + 1,
                date=txn_date.date(),
                name=txn.get('description', '')[:100],
                reference=txn.get('reference') or txn.get('description', '')[:30],
                amount=amount,
                abs_amount=abs(amount),
                is_debit=amount < 0,
                transaction_type=txn.get('type', 'Other')
            )
            bank_txns.append(bank_txn)

        # Use the existing bank importer for matching
        importer = BankStatementImporter(
            sql_connector=sql_connector,
            bank_code=bank_code,
            default_vat_code='0'
        )

        # Match each transaction
        matched_receipts = []
        matched_payments = []
        unmatched = []

        for txn in bank_txns:
            importer._match_transaction(txn)

            txn_data = {
                "row": txn.row_number,
                "date": str(txn.date),
                "name": txn.name,
                "reference": txn.reference,
                "amount": txn.amount,
                "action": txn.action,
                "match_type": txn.match_type,
                "matched_account": txn.matched_account,
                "matched_name": txn.matched_name,
                "match_score": txn.match_score,
                "skip_reason": txn.skip_reason
            }

            if txn.action == 'sales_receipt':
                matched_receipts.append(txn_data)
            elif txn.action == 'purchase_payment':
                matched_payments.append(txn_data)
            else:
                unmatched.append(txn_data)

        return {
            "success": True,
            "total_transactions": len(bank_txns),
            "matched_receipts": matched_receipts,
            "matched_payments": matched_payments,
            "unmatched": unmatched,
            "summary": {
                "receipts": len(matched_receipts),
                "payments": len(matched_payments),
                "unmatched": len(unmatched)
            }
        }

    except Exception as e:
        logger.error(f"Failed to process import from statement for {bank_code}: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}





@router.post("/api/reconcile/bank/{bank_code}/confirm-matches")
async def confirm_statement_matches(
    bank_code: str,
    matches: List[Dict],
    statement_balance: float,
    statement_date: str
):
    """
    Confirm matched transactions and mark them as reconciled in Opera.

    Args:
        bank_code: The bank account code
        matches: List of confirmed matches (each with 'ae_entry' key)
        statement_balance: Closing balance from the statement
        statement_date: Statement date (YYYY-MM-DD)

    Returns:
        Reconciliation result
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    # Acquire bank-level lock
    from sql_rag.import_lock import acquire_import_lock, release_import_lock
    if not acquire_import_lock(_bank_lock_key(bank_code), locked_by="api", endpoint="confirm-matches"):
        return {"success": False, "error": f"Bank account {bank_code} is currently being modified by another user. Please wait and try again."}

    try:
        from datetime import datetime

        stmt_date = datetime.strptime(statement_date, '%Y-%m-%d')

        # Get the entry IDs to reconcile
        entry_ids = [m.get('ae_entry') or m.get('opera_entry', {}).get('ae_entry') for m in matches]
        entry_ids = [e for e in entry_ids if e]  # Filter out None values

        if not entry_ids:
            return {"success": False, "error": "No valid entry IDs provided"}

        # Get next batch number
        batch_query = f"""
            SELECT ISNULL(MAX(ae_reclnum), 0) + 1 as next_batch
            FROM aentry WITH (NOLOCK)
            WHERE ae_bank = '{bank_code}'
        """
        batch_result = sql_connector.execute_query(batch_query)
        next_batch = int(batch_result.iloc[0]['next_batch']) if batch_result is not None else 1

        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        reconciled_count = 0

        with sql_connector.engine.connect() as conn:
            trans = conn.begin()
            try:
                from sqlalchemy import text

                for i, entry_id in enumerate(entry_ids):
                    line_number = (i + 1) * 10

                    update_query = f"""
                        UPDATE aentry WITH (ROWLOCK)
                        SET ae_reclnum = {next_batch},
                            ae_statln = {line_number},
                            ae_recdate = '{stmt_date.strftime('%Y-%m-%d')}',
                            ae_recbal = {int(statement_balance * 100)},
                            datemodified = '{now_str}'
                        WHERE ae_entry = '{entry_id}'
                          AND ae_bank = '{bank_code}'
                          AND ae_reclnum = 0
                    """
                    result = conn.execute(text(update_query))
                    reconciled_count += result.rowcount

                # Update nbank
                nbank_update = f"""
                    UPDATE nbank WITH (ROWLOCK)
                    SET nk_recbal = {int(statement_balance * 100)},
                        nk_lstrecl = {next_batch},
                        nk_lststno = ISNULL(nk_lststno, 0) + 1,
                        nk_lststdt = '{stmt_date.strftime('%Y-%m-%d')}',
                        datemodified = '{now_str}'
                    WHERE nk_code = '{bank_code}'
                """
                conn.execute(text(nbank_update))

                trans.commit()

                release_import_lock(_bank_lock_key(bank_code))
                return {
                    "success": True,
                    "message": f"Reconciled {reconciled_count} entries",
                    "reconciled_count": reconciled_count,
                    "batch_number": next_batch,
                    "statement_balance": statement_balance
                }
            except Exception as e:
                trans.rollback()
                raise

    except Exception as e:
        logger.error(f"Failed to confirm matches for {bank_code}: {e}")
        try:
            release_import_lock(_bank_lock_key(bank_code))
        except Exception:
            pass
        return {"success": False, "error": str(e)}





@router.get("/api/reconcile/bank/{bank_code}/scan-emails")
async def scan_emails_for_statements(bank_code: str, email_address: Optional[str] = None):
    """
    Scan email inbox for bank statement attachments.

    Args:
        bank_code: The bank account code
        email_address: Optional email address to scan (defaults to configured inbox)

    Returns:
        List of emails with bank statement attachments
    """
    # TODO: Implement email scanning using existing email infrastructure
    # For now, return a placeholder
    return {
        "success": True,
        "message": "Email scanning not yet implemented - use file upload",
        "statements_found": []
    }





@router.post("/api/archive/file")
async def archive_import_file(
    file_path: str,
    import_type: str,
    transactions_extracted: Optional[int] = None,
    transactions_matched: Optional[int] = None,
    transactions_reconciled: Optional[int] = None
):
    """
    Archive a processed import file.

    Args:
        file_path: Path to the file to archive
        import_type: Type of import ('bank-statement', 'gocardless', 'invoice')
        transactions_extracted: Number of transactions extracted from file
        transactions_matched: Number of transactions matched
        transactions_reconciled: Number of transactions reconciled

    Returns:
        Archive result with new file path
    """
    try:
        from sql_rag.file_archive import archive_file

        metadata = {
            "transactions_extracted": transactions_extracted,
            "transactions_matched": transactions_matched,
            "transactions_reconciled": transactions_reconciled,
        }

        result = archive_file(file_path, import_type, metadata)
        return result

    except Exception as e:
        logger.error(f"Failed to archive file {file_path}: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/archive/history")
async def get_archive_history(import_type: Optional[str] = None, limit: int = 50):
    """
    Get archive history.

    Args:
        import_type: Filter by type ('bank-statement', 'gocardless', 'invoice'), or None for all
        limit: Maximum entries to return

    Returns:
        List of archived files with metadata
    """
    try:
        from sql_rag.file_archive import get_archive_history as get_history

        history = get_history(import_type, limit)
        return {"success": True, "history": history, "count": len(history)}

    except Exception as e:
        logger.error(f"Failed to get archive history: {e}")
        return {"success": False, "error": str(e)}





@router.post("/api/archive/restore")
async def restore_archived_file(archive_path: str):
    """
    Restore an archived file back to its original location.

    Args:
        archive_path: Current path of the archived file

    Returns:
        Restore result with restored file path
    """
    try:
        from sql_rag.file_archive import restore_file

        result = restore_file(archive_path)
        if not result["success"]:
            raise HTTPException(status_code=400, detail=result["error"])
        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to restore file {archive_path}: {e}")
        raise HTTPException(status_code=500, detail=str(e))





@router.get("/api/archive/pending")
async def get_pending_files(import_type: str):
    """
    Get list of files pending in source directories (not yet archived).

    Args:
        import_type: Type of import to check ('bank-statement', 'gocardless', 'invoice')

    Returns:
        List of pending files
    """
    try:
        from sql_rag.file_archive import get_pending_files as get_pending

        files = get_pending(import_type)
        return {"success": True, "files": files, "count": len(files)}

    except Exception as e:
        logger.error(f"Failed to get pending files: {e}")
        return {"success": False, "error": str(e)}





@router.post("/api/statement-files/mark-reconciled")
async def mark_statement_reconciled(
    filename: str = Query(..., description="Statement filename"),
    bank_code: str = Query(None, description="Bank code"),
    reconciled_count: int = Query(0, description="Number of entries reconciled")
):
    """
    Mark a statement file as reconciled after bank reconciliation is complete.
    """
    try:
        success = email_storage.mark_statement_reconciled(
            filename=filename,
            reconciled_count=reconciled_count,
            bank_code=bank_code
        )
        return {
            "success": success,
            "message": f"Statement '{filename}' marked as reconciled" if success else "No matching import record found"
        }
    except Exception as e:
        logger.error(f"Failed to mark statement as reconciled: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/statement-files/imported-for-reconciliation")
async def get_imported_statements_for_reconciliation(
    bank_code: Optional[str] = Query(None, description="Filter by bank code"),
    limit: int = Query(200, description="Maximum records to return"),
    include_reconciled: bool = Query(False, description="Include completed/reconciled statements (for Manage tab)")
):
    """
    Get imported statements.

    By default returns only statements not yet reconciled (for Load Statements tab).
    With include_reconciled=True, returns all statements including completed ones
    (for Manage tab).
    """
    try:
        statements = email_storage.get_imported_statements_for_reconciliation(
            bank_code=bank_code,
            limit=limit,
            include_reconciled=include_reconciled
        )

        # Cross-check against Opera to remove statements already reconciled
        if statements and sql_connector:
            try:
                # Get reconciled balances for all relevant banks
                bank_codes = list({s['bank_code'] for s in statements if s.get('bank_code')})
                if bank_codes:
                    placeholders = ','.join([f"'{bc}'" for bc in bank_codes])
                    rec_df = sql_connector.execute_query(f"""
                        SELECT RTRIM(nk_acnt) as bank_code, nk_recbal / 100.0 as reconciled_balance
                        FROM nbank WITH (NOLOCK)
                        WHERE nk_acnt IN ({placeholders})
                    """)
                    rec_balances = {}
                    if rec_df is not None:
                        for _, row in rec_df.iterrows():
                            rec_balances[row['bank_code'].strip()] = float(row['reconciled_balance'])

                    # Add Opera reconciled balance info and auto-mark reconciled statements
                    for stmt in statements:
                        bc = stmt.get('bank_code', '').strip()
                        rec_bal = rec_balances.get(bc)
                        if rec_bal is not None:
                            stmt['opera_reconciled_balance'] = rec_bal
                            # If closing balance matches Opera reconciled balance, statement is complete
                            closing = stmt.get('closing_balance')
                            if closing is not None and abs(float(closing) - rec_bal) < 0.02:
                                if not stmt.get('is_reconciled'):
                                    # Auto-mark as reconciled in tracking DB
                                    try:
                                        email_storage.mark_statement_reconciled(
                                            filename=stmt['filename'],
                                            bank_code=bc
                                        )
                                        stmt['is_reconciled'] = 1
                                        logger.info(f"Auto-marked statement '{stmt['filename']}' as reconciled (closing {closing} matches Opera reconciled balance {rec_bal})")
                                    except Exception as mark_err:
                                        logger.warning(f"Failed to auto-mark statement reconciled: {mark_err}")

                    # Filter out auto-reconciled statements if not including reconciled
                    if not include_reconciled:
                        statements = [s for s in statements if not s.get('is_reconciled')]
            except Exception as e:
                logger.warning(f"Could not cross-check Opera reconciliation status: {e}")

        return {
            "success": True,
            "statements": statements,
            "count": len(statements)
        }
    except Exception as e:
        logger.error(f"Failed to get imported statements: {e}")
        return {"success": False, "error": str(e), "statements": []}





@router.post("/api/bank-import/detect-format")
async def detect_file_format(filepath: str = Query(..., description="Path to bank statement file")):
    """
    Detect the format of a bank statement file.

    Supports: CSV, OFX, QIF, MT940
    """
    import os
    if not filepath or not filepath.strip():
        return {"success": False, "error": "File path is required"}

    if not os.path.exists(filepath):
        return {"success": False, "error": f"File not found: {filepath}"}

    try:
        from sql_rag.bank_import import BankStatementImport

        detected = BankStatementImport.detect_file_format(filepath)
        return {
            "success": True,
            "filepath": filepath,
            "format": detected,
            "supported_formats": ["CSV", "OFX", "QIF", "MT940"]
        }
    except Exception as e:
        logger.error(f"Format detection error: {e}")
        return {"success": False, "error": str(e)}





@router.post("/api/bank-import/detect-bank")
async def detect_bank_from_file(
    filepath: str = Query(..., description="Path to bank statement file")
):
    """
    Detect which Opera bank account a bank statement file belongs to.

    Reads the bank details (sort code, account number) from the file
    and matches against Opera's nbank table.

    Supports multiple CSV formats:
    - Header row format: "Account Number:,20-96-89,90764205"
    - Data column format: Account field with "sort_code account_number"

    Returns the detected bank code and account details.
    """
    import os
    import csv
    import re

    if not filepath or not filepath.strip():
        return {
            "success": False,
            "error": "File path is required"
        }

    if not os.path.exists(filepath):
        return {
            "success": False,
            "error": f"File not found: {filepath}"
        }

    try:
        from sql_rag.bank_import import BankStatementImport

        sort_code = None
        account_number = None

        # Read file and try multiple detection methods
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()[:30]  # Read first 30 lines for detection

        # Method 1: Scan ALL lines for sort code (XX-XX-XX) and account number (8 digits) patterns
        # This works regardless of CSV format - just find the patterns
        for line in lines:
            # Look for sort code pattern: XX-XX-XX (6 digits with dashes)
            sort_match = re.search(r'(\d{2}-\d{2}-\d{2})', line)
            # Look for 8-digit account number (not part of a longer number)
            acct_match = re.search(r'(?<!\d)(\d{8})(?!\d)', line)

            if sort_match and acct_match:
                sort_code = sort_match.group(1)
                account_number = acct_match.group(1)
                break

        # Method 2: Try to find in data rows with 'Account' column (format: "20-96-89 90764205")
        if not (sort_code and account_number):
            try:
                with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                    # Find the header row (contains 'Date' and 'Account' columns)
                    for i, line in enumerate(f):
                        line_lower = line.lower()
                        if 'date' in line_lower and 'account' in line_lower:
                            f.seek(0)
                            # Skip to header row
                            for _ in range(i):
                                next(f)
                            reader = csv.DictReader(f)
                            first_row = next(reader, None)
                            if first_row:
                                # Try 'Account' field (case-insensitive lookup)
                                account_field = None
                                for key in first_row.keys():
                                    if key.lower() == 'account':
                                        account_field = first_row[key].strip()
                                        break
                                if account_field:
                                    # Format: "20-96-89 90764205"
                                    parts = account_field.split(' ', 1)
                                    if len(parts) == 2:
                                        sort_code = parts[0].strip()
                                        account_number = parts[1].strip()
                            break
            except Exception as e:
                logger.warning(f"Method 2 bank detection error: {e}")

        # If we found bank details, look up in Opera
        detected_code = None
        if sort_code and account_number:
            detected_code = BankStatementImport.find_bank_account_by_details(sort_code, account_number)

        if detected_code:
            # Get full bank details
            bank_accounts = BankStatementImport.get_available_bank_accounts()
            bank_info = next((b for b in bank_accounts if b['code'] == detected_code), None)

            return {
                "success": True,
                "detected": True,
                "bank_code": detected_code,
                "bank_description": bank_info['description'] if bank_info else detected_code,
                "sort_code": bank_info.get('sort_code', '') if bank_info else sort_code,
                "account_number": bank_info.get('account_number', '') if bank_info else account_number,
                "message": f"Detected bank account: {detected_code}"
            }
        else:
            # Could not detect - return all available banks for manual selection
            bank_accounts = BankStatementImport.get_available_bank_accounts()
            return {
                "success": True,
                "detected": False,
                "bank_code": None,
                "message": f"Could not detect bank account from file.{' Found: ' + sort_code + ' ' + account_number if sort_code else ''} Please select manually.",
                "available_banks": bank_accounts
            }

    except Exception as e:
        logger.error(f"Bank detection error: {e}")
        return {
            "success": False,
            "error": str(e)
        }





@router.get("/api/bank-import/raw-preview")
async def raw_preview_bank_file(
    filepath: str = Query(..., description="Path to bank statement file"),
    lines: int = Query(50, description="Number of lines to preview")
):
    """
    Preview raw contents of a bank statement file.
    Returns first N lines of the file for inspection before processing.
    """
    import os
    if not filepath or not filepath.strip():
        return {
            "success": False,
            "error": "File path is required"
        }

    if not os.path.exists(filepath):
        return {
            "success": False,
            "error": f"File not found: {filepath}"
        }

    try:
        # Try to read with different encodings
        file_lines = []
        for encoding in ['utf-8', 'latin-1', 'cp1252']:
            try:
                with open(filepath, 'r', encoding=encoding) as f:
                    for i, line in enumerate(f):
                        if i >= lines:
                            break
                        file_lines.append(line.rstrip('\n\r'))
                break
            except UnicodeDecodeError:
                continue

        if not file_lines:
            return {
                "success": False,
                "error": "Could not read file with any supported encoding"
            }

        return {
            "success": True,
            "lines": file_lines,
            "total_lines_shown": len(file_lines)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }





@router.post("/api/bank-import/preview-multiformat")
async def preview_bank_import_multiformat(
    filepath: str = Query(..., description="Path to bank statement file"),
    bank_code: str = Query(..., description="Opera bank account code"),
    format_override: Optional[str] = Query(None, description="Force specific format: CSV, OFX, QIF, MT940")
):
    """
    Preview bank statement import with auto-format detection.

    Supports CSV, OFX, QIF, and MT940 formats.
    Returns transactions categorized for import with duplicate detection.
    """
    import os
    if not filepath or not filepath.strip():
        return {
            "success": False,
            "error": "File path is required",
            "transactions": []
        }

    if not os.path.exists(filepath):
        return {
            "success": False,
            "error": f"File not found: {filepath}",
            "transactions": []
        }

    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.bank_import import BankStatementImport

        importer = BankStatementImport(
            bank_code=bank_code,
            use_enhanced_matching=True,
            use_fingerprinting=True
        )

        # Validate bank account matches the CSV file
        is_valid, validation_message, detected_bank = importer.validate_bank_account_from_csv(filepath)
        if not is_valid:
            return {
                "success": False,
                "error": validation_message,
                "bank_mismatch": True,
                "detected_bank": detected_bank,
                "selected_bank": bank_code,
                "transactions": []
            }

        # Parse with auto-detection
        transactions, detected_format = importer.parse_file(filepath, format_override)

        # Process transactions (matching, duplicate detection)
        importer.process_transactions(transactions)

        # Validate period accounting for each transaction using ledger-specific rules
        from sql_rag.opera_config import (
            get_period_posting_decision,
            get_current_period_info,
            is_open_period_accounting_enabled,
            validate_posting_period,
            get_ledger_type_for_transaction
        )

        period_info = get_current_period_info(sql_connector)
        open_period_enabled = is_open_period_accounting_enabled(sql_connector)
        period_violations = []

        for txn in transactions:
            # Store original date for reference
            txn.original_date = txn.date

            # Determine the appropriate ledger type based on transaction action
            # Only validate matched transactions that have an action
            if txn.action and txn.action not in ('skip', None):
                ledger_type = get_ledger_type_for_transaction(txn.action)

                # Use ledger-specific validation
                period_result = validate_posting_period(sql_connector, txn.date, ledger_type)

                if not period_result.is_valid:
                    txn.period_valid = False
                    txn.period_error = period_result.error_message
                    period_violations.append({
                        "row": txn.row_number,
                        "date": txn.date.isoformat(),
                        "name": txn.name,
                        "action": txn.action,
                        "ledger_type": ledger_type,
                        "error": period_result.error_message,
                        "transaction_year": period_result.year,
                        "transaction_period": period_result.period,
                        "current_year": period_info.get('np_year'),
                        "current_period": period_info.get('np_perno')
                    })
                else:
                    txn.period_valid = True
                    txn.period_error = None
            else:
                # Unmatched/skipped transactions - still check basic year validation
                decision = get_period_posting_decision(sql_connector, txn.date)
                if not decision.can_post:
                    txn.period_valid = False
                    txn.period_error = decision.error_message
                else:
                    txn.period_valid = True
                    txn.period_error = None

        # Categorize for frontend
        matched_receipts = []
        matched_payments = []
        matched_refunds = []
        repeat_entries = []
        unmatched = []
        already_posted = []
        skipped = []

        # Load GoCardless FX imports to auto-detect FX transactions in bank statement
        gc_fx_refs = {}
        if email_storage:
            try:
                gc_fx_refs = email_storage.get_gocardless_fx_imports('opera_se')
                if gc_fx_refs:
                    logger.info(f"Loaded {len(gc_fx_refs)} GoCardless FX references for auto-detection (multiformat)")
            except Exception as e:
                logger.warning(f"Failed to load GoCardless FX imports: {e}")

        for txn in transactions:
            txn_data = {
                "row": txn.row_number,
                "date": txn.date.isoformat(),
                "amount": txn.amount,
                "name": txn.name,
                "reference": txn.reference,
                "memo": txn.memo,
                "fit_id": txn.fit_id,
                "account": txn.matched_account,
                "account_name": txn.matched_name,
                "match_score": round(txn.match_score * 100) if txn.match_score else 0,
                "match_source": txn.match_source,
                "action": txn.action,
                "reason": txn.skip_reason,
                "fingerprint": txn.fingerprint,
                "is_duplicate": txn.is_duplicate,
                "duplicate_candidates": [
                    {
                        "table": c.table,
                        "record_id": c.record_id,
                        "match_type": c.match_type,
                        "confidence": round(c.confidence * 100)
                    }
                    for c in (txn.duplicate_candidates or [])
                ],
                "refund_credit_note": getattr(txn, 'refund_credit_note', None),
                "refund_credit_amount": getattr(txn, 'refund_credit_amount', None),
                # Repeat entry fields
                "repeat_entry_ref": getattr(txn, 'repeat_entry_ref', None),
                "repeat_entry_desc": getattr(txn, 'repeat_entry_desc', None),
                "repeat_entry_next_date": getattr(txn, 'repeat_entry_next_date', None).isoformat() if getattr(txn, 'repeat_entry_next_date', None) else None,
                "repeat_entry_posted": getattr(txn, 'repeat_entry_posted', None),
                "repeat_entry_total": getattr(txn, 'repeat_entry_total', None),
                "repeat_entry_freq": getattr(txn, 'repeat_entry_freq', None),
                "repeat_entry_every": getattr(txn, 'repeat_entry_every', None),
                # Period validation fields
                "period_valid": getattr(txn, 'period_valid', True),
                "period_error": getattr(txn, 'period_error', None),
                "original_date": getattr(txn, 'original_date', txn.date).isoformat() if getattr(txn, 'original_date', None) else txn.date.isoformat(),
            }

            if txn.action == 'sales_receipt':
                matched_receipts.append(txn_data)
            elif txn.action == 'purchase_payment':
                matched_payments.append(txn_data)
            elif txn.action in ('sales_refund', 'purchase_refund'):
                matched_refunds.append(txn_data)
            elif txn.action == 'repeat_entry':
                # Calculate all outstanding posting dates and their period status
                freq = getattr(txn, 'repeat_entry_freq', None)
                every = getattr(txn, 'repeat_entry_every', 1) or 1
                posted = getattr(txn, 'repeat_entry_posted', 0) or 0
                total = getattr(txn, 'repeat_entry_total', 0) or 0
                next_date = getattr(txn, 'repeat_entry_next_date', None)

                outstanding_postings = []
                if freq and next_date and (total == 0 or posted < total):
                    from dateutil.relativedelta import relativedelta
                    calc_date = next_date
                    calc_posted = posted
                    # Calculate up to remaining postings (or max 12 for unlimited)
                    max_calc = (total - posted) if total > 0 else 12
                    for _ in range(max_calc):
                        if total > 0 and calc_posted >= total:
                            break
                        period_result = validate_posting_period(sql_connector, calc_date, 'NL')
                        outstanding_postings.append({
                            "date": calc_date.isoformat() if hasattr(calc_date, 'isoformat') else str(calc_date),
                            "period_valid": period_result.is_valid,
                            "period_error": period_result.error_message if not period_result.is_valid else None,
                            "period": period_result.period,
                            "year": period_result.year,
                        })
                        # Advance to next date based on frequency
                        if freq == 'D':
                            calc_date = calc_date + timedelta(days=every)
                        elif freq == 'W':
                            calc_date = calc_date + timedelta(weeks=every)
                        elif freq == 'M':
                            calc_date = calc_date + relativedelta(months=every)
                        elif freq == 'Q':
                            calc_date = calc_date + relativedelta(months=3 * every)
                        elif freq == 'Y':
                            calc_date = calc_date + relativedelta(years=every)
                        else:
                            break
                        calc_posted += 1

                txn_data["outstanding_postings"] = outstanding_postings
                txn_data["outstanding_count"] = len(outstanding_postings)
                txn_data["outstanding_blocked"] = sum(1 for p in outstanding_postings if not p["period_valid"])
                txn_data["outstanding_open"] = sum(1 for p in outstanding_postings if p["period_valid"])
                repeat_entries.append(txn_data)
            elif txn.is_duplicate or (txn.skip_reason and 'Already' in txn.skip_reason):
                already_posted.append(txn_data)
            else:
                # Check if this is a GoCardless FX transaction
                gc_fx_match = None
                if gc_fx_refs:
                    txn_text = f"{txn.name or ''} {txn.memo or ''}".upper()
                    for ref_key, ref_data in gc_fx_refs.items():
                        if ref_key.upper() in txn_text:
                            gc_fx_match = (ref_key, ref_data)
                            break
                if gc_fx_match:
                    ref_key, ref_data = gc_fx_match
                    txn_data['action'] = 'gc_fx_ignore'
                    txn_data['reason'] = f"GoCardless FX payout ({ref_data['currency']})"
                    txn_data['gc_fx_currency'] = ref_data['currency']
                    txn_data['gc_fx_original_amount'] = ref_data['gross_amount']
                    txn_data['gc_fx_gbp_amount'] = ref_data['fx_amount']
                    txn_data['gc_fx_reference'] = ref_key
                    skipped.append(txn_data)
                    logger.info(f"Auto-detected GoCardless FX transaction (multiformat): {ref_key} ({ref_data['currency']} {ref_data['gross_amount']} -> GBP {ref_data['fx_amount']})")
                else:
                    # All non-matched, non-duplicate transactions go to unmatched
                    # so the user can assign them manually via dropdown
                    unmatched.append(txn_data)

        return {
            "success": True,
            "filename": filepath,
            "detected_format": detected_format,
            "total_transactions": len(transactions),
            "matched_receipts": matched_receipts,
            "matched_payments": matched_payments,
            "matched_refunds": matched_refunds,
            "repeat_entries": repeat_entries,
            "unmatched": unmatched,
            "already_posted": already_posted,
            "skipped": skipped,
            "summary": {
                "to_import": len(matched_receipts) + len(matched_payments) + len(matched_refunds),
                "refund_count": len(matched_refunds),
                "repeat_entry_count": len(repeat_entries),
                "unmatched_count": len(unmatched),
                "already_posted_count": len(already_posted),
                "skipped_count": len(skipped)
            },
            # Period validation info
            "period_info": {
                "current_year": period_info.get('np_year'),
                "current_period": period_info.get('np_perno'),
                "open_period_accounting": open_period_enabled
            },
            "period_violations": period_violations,
            "has_period_violations": len(period_violations) > 0
        }

    except Exception as e:
        logger.error(f"Multi-format preview error: {e}")
        return {"success": False, "error": str(e), "transactions": []}





@router.post("/api/bank-import/correction")
async def record_correction(
    bank_name: str = Query(..., description="Name from bank statement"),
    wrong_account: str = Query(..., description="The incorrectly matched account"),
    correct_account: str = Query(..., description="The correct account"),
    ledger_type: str = Query(..., description="'S' for supplier, 'C' for customer"),
    account_name: Optional[str] = Query(None, description="Name of the correct account")
):
    """
    Record a user correction for alias learning.

    This teaches the system to:
    1. Map the bank name to the correct account
    2. Avoid matching to the wrong account in future
    """
    try:
        from sql_rag.bank_aliases import BankAliasManager

        # Try enhanced manager first
        try:
            from sql_rag.bank_aliases import EnhancedAliasManager
            manager = EnhancedAliasManager()
        except ImportError:
            manager = BankAliasManager()

        if hasattr(manager, 'record_correction'):
            success = manager.record_correction(
                bank_name=bank_name,
                wrong_account=wrong_account,
                correct_account=correct_account,
                ledger_type=ledger_type.upper(),
                account_name=account_name
            )
        else:
            # Fallback: just save the alias
            success = manager.save_alias(
                bank_name=bank_name,
                ledger_type=ledger_type.upper(),
                account_code=correct_account,
                match_score=1.0,
                account_name=account_name
            )

        return {
            "success": success,
            "message": f"Correction recorded: '{bank_name}' -> {correct_account}" if success else "Failed to record correction"
        }

    except Exception as e:
        logger.error(f"Error recording correction: {e}")
        return {"success": False, "error": str(e)}





@router.post("/api/bank-import/check-duplicates")
async def check_duplicates(
    transactions: List[Dict[str, Any]],
    bank_code: str = Query(..., description="Opera bank account code")
):
    """
    Check multiple transactions for duplicates.

    Input: List of transactions with 'name', 'amount', 'date', optional 'account'
    Returns: Dict mapping transaction index to duplicate candidates
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.bank_duplicates import EnhancedDuplicateDetector
        from datetime import datetime

        detector = EnhancedDuplicateDetector(sql_connector)

        # Parse dates if needed
        for txn in transactions:
            if isinstance(txn.get('date'), str):
                try:
                    txn['date'] = datetime.strptime(txn['date'], '%Y-%m-%d').date()
                except ValueError:
                    txn['date'] = datetime.strptime(txn['date'], '%d/%m/%Y').date()

        results = detector.check_batch(transactions, bank_code)

        # Format for JSON response
        formatted_results = {}
        for idx, candidates in results.items():
            formatted_results[str(idx)] = [
                {
                    "table": c.table,
                    "record_id": c.record_id,
                    "match_type": c.match_type,
                    "confidence": round(c.confidence * 100),
                    "details": c.details
                }
                for c in candidates
            ]

        return {
            "success": True,
            "duplicates_found": len(results),
            "results": formatted_results
        }

    except ImportError:
        return {"success": False, "error": "Duplicate detection module not available"}
    except Exception as e:
        logger.error(f"Error checking duplicates: {e}")
        return {"success": False, "error": str(e)}





@router.post("/api/bank-import/duplicate-override")
async def override_duplicate(
    transaction_hash: str = Query(..., description="Hash of the transaction"),
    reason: str = Query(..., description="Reason for override")
):
    """
    Record a duplicate override decision.

    When a user decides to import a transaction despite it being flagged
    as a potential duplicate, record the decision.
    """
    try:
        from sql_rag.bank_aliases import BankAliasManager
        import sqlite3

        manager = BankAliasManager()
        conn = manager._get_conn()

        # Create table if needed
        conn.execute("""
            CREATE TABLE IF NOT EXISTS duplicate_overrides (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_hash TEXT NOT NULL UNIQUE,
                override_reason TEXT,
                user_code TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.execute("""
            INSERT OR REPLACE INTO duplicate_overrides (transaction_hash, override_reason)
            VALUES (?, ?)
        """, (transaction_hash, reason))
        conn.commit()

        return {
            "success": True,
            "message": "Duplicate override recorded"
        }

    except Exception as e:
        logger.error(f"Error recording duplicate override: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/bank-import/cashbook-types")
async def get_cashbook_types(category: str = Query(None, description="Filter by category: R (Receipt), P (Payment), T (Transfer)")):
    """Get available cashbook types from Opera atype table."""
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        query = """
            SELECT ay_cbtype, ay_desc, ay_type, ay_batched
            FROM atype WITH (NOLOCK)
        """
        if category:
            query += f" WHERE RTRIM(ay_type) = '{category}'"
        query += " ORDER BY ay_type, ay_cbtype"

        df = sql_connector.execute_query(query)
        if df is None or len(df) == 0:
            return {"success": True, "types": []}

        types = [
            {
                "code": row['ay_cbtype'].strip(),
                "description": row['ay_desc'].strip() if row['ay_desc'] else '',
                "category": row['ay_type'].strip() if row['ay_type'] else '',
                "batched": bool(row.get('ay_batched', 0))
            }
            for _, row in df.iterrows()
        ]
        return {"success": True, "types": types}
    except Exception as e:
        logger.error(f"Error fetching cashbook types: {e}")
        return {"success": False, "error": str(e), "types": []}





@router.get("/api/bank-import/config")
async def get_match_config():
    """
    Get matching configuration settings.
    """
    try:
        from sql_rag.bank_aliases import BankAliasManager

        manager = BankAliasManager()
        conn = manager._get_conn()

        cursor = conn.execute("""
            SELECT * FROM match_config ORDER BY id DESC LIMIT 1
        """)
        row = cursor.fetchone()

        if row:
            config = dict(row)
        else:
            config = {
                "min_match_score": 0.6,
                "learn_threshold": 0.8,
                "ambiguity_threshold": 0.15,
                "use_phonetic": True,
                "use_levenshtein": True,
                "use_ngram": True
            }

        return {
            "success": True,
            "config": config
        }

    except Exception as e:
        logger.error(f"Error getting match config: {e}")
        return {
            "success": True,
            "config": {
                "min_match_score": 0.6,
                "learn_threshold": 0.8,
                "ambiguity_threshold": 0.15
            }
        }





@router.put("/api/bank-import/config")
async def update_match_config(
    min_match_score: float = Query(0.6, ge=0.0, le=1.0),
    learn_threshold: float = Query(0.8, ge=0.0, le=1.0),
    ambiguity_threshold: float = Query(0.15, ge=0.0, le=1.0),
    use_phonetic: bool = Query(True),
    use_levenshtein: bool = Query(True),
    use_ngram: bool = Query(True)
):
    """
    Update matching configuration settings.
    """
    try:
        from sql_rag.bank_aliases import BankAliasManager
        from datetime import datetime

        manager = BankAliasManager()
        conn = manager._get_conn()

        conn.execute("""
            INSERT INTO match_config (
                min_match_score, learn_threshold, ambiguity_threshold,
                use_phonetic, use_levenshtein, use_ngram, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            min_match_score, learn_threshold, ambiguity_threshold,
            1 if use_phonetic else 0,
            1 if use_levenshtein else 0,
            1 if use_ngram else 0,
            datetime.now().isoformat()
        ))
        conn.commit()

        return {
            "success": True,
            "message": "Configuration updated"
        }

    except Exception as e:
        logger.error(f"Error updating match config: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/bank-import/list-csv")
async def list_csv_files(directory: str):
    """
    List CSV files in a directory with their dates and sizes.
    Used by the frontend to populate a file picker dropdown.
    """
    import glob
    from datetime import datetime

    try:
        if not os.path.isdir(directory):
            return {"success": False, "files": [], "error": f"Directory not found: {directory}"}

        csv_files = []
        for pattern in ['*.csv', '*.CSV']:
            for filepath in glob.glob(os.path.join(directory, pattern)):
                stat = os.stat(filepath)
                csv_files.append({
                    "filename": os.path.basename(filepath),
                    "size_bytes": stat.st_size,
                    "size_display": f"{stat.st_size / 1024:.1f} KB" if stat.st_size < 1048576 else f"{stat.st_size / 1048576:.1f} MB",
                    "modified": datetime.fromtimestamp(stat.st_mtime).strftime("%d/%m/%Y %H:%M"),
                    "modified_timestamp": stat.st_mtime,
                })

        # Deduplicate (*.csv and *.CSV could match same file on case-insensitive filesystem)
        seen = set()
        unique_files = []
        for f in csv_files:
            if f["filename"] not in seen:
                seen.add(f["filename"])
                unique_files.append(f)

        # Sort by date descending (newest first)
        unique_files.sort(key=lambda f: f["modified_timestamp"], reverse=True)

        # For each CSV, detect which bank account it belongs to (from first row)
        from sql_rag.bank_import import BankStatementImport
        for f in unique_files:
            full_path = os.path.join(directory, f["filename"])
            try:
                detected_bank = BankStatementImport.find_bank_account_by_details_from_csv(full_path)
                f["detected_bank"] = detected_bank
            except Exception:
                f["detected_bank"] = None

        return {"success": True, "files": unique_files, "directory": directory}

    except Exception as e:
        logger.error(f"Error listing CSV files: {e}")
        return {"success": False, "files": [], "error": str(e)}





@router.get("/api/bank-import/list-pdf")
async def list_pdf_files(
    directory: str,
    bank_code: str = Query(..., description="Bank account code to check import history")
):
    """
    List PDF files in a directory with their dates and sizes.
    Also checks if each PDF has already been imported for the given bank account.
    """
    import glob
    from datetime import datetime

    try:
        if not os.path.isdir(directory):
            return {"success": False, "files": [], "error": f"Directory not found: {directory}"}

        pdf_files = []
        for pattern in ['*.pdf', '*.PDF']:
            for filepath in glob.glob(os.path.join(directory, pattern)):
                stat = os.stat(filepath)
                filename = os.path.basename(filepath)

                # Check if this PDF has been imported before
                already_processed = False
                statement_date = None
                import_sequence = None

                if sql_connector:
                    try:
                        # Check import history for this filename and bank
                        history_df = sql_connector.execute_query(f"""
                            SELECT TOP 1
                                import_date,
                                ISNULL(statement_date, '') as statement_date
                            FROM bank_import_history WITH (NOLOCK)
                            WHERE filename = '{filename.replace("'", "''")}'
                              AND bank_code = '{bank_code}'
                            ORDER BY import_date DESC
                        """)
                        if not history_df.empty:
                            already_processed = True
                            stmt_date = history_df.iloc[0].get('statement_date')
                            if stmt_date and str(stmt_date).strip():
                                statement_date = str(stmt_date)
                    except Exception as e:
                        logger.debug(f"Could not check import history for {filename}: {e}")

                pdf_files.append({
                    "filename": filename,
                    "size_bytes": stat.st_size,
                    "size_display": f"{stat.st_size / 1024:.1f} KB" if stat.st_size < 1048576 else f"{stat.st_size / 1048576:.1f} MB",
                    "modified": datetime.fromtimestamp(stat.st_mtime).strftime("%d/%m/%Y %H:%M"),
                    "modified_timestamp": stat.st_mtime,
                    "already_processed": already_processed,
                    "statement_date": statement_date,
                    "import_sequence": import_sequence,
                })

        # Deduplicate
        seen = set()
        unique_files = []
        for f in pdf_files:
            if f["filename"].lower() not in seen:
                seen.add(f["filename"].lower())
                unique_files.append(f)

        # Sort: unprocessed first by date (oldest first for sequential import), then processed
        def sort_key(f):
            if f["already_processed"]:
                return (1, -f["modified_timestamp"])  # Processed files at end, newest first
            else:
                return (0, f["modified_timestamp"])   # Unprocessed files first, oldest first
        unique_files.sort(key=sort_key)

        # Annotate PDF files with draft (in-progress) status
        if email_storage:
            try:
                draft_keys = email_storage.get_draft_statement_keys(bank_code)
                draft_lookup = {}
                for dk in draft_keys:
                    if dk.get('source') == 'pdf' and dk.get('filename'):
                        draft_lookup[dk['filename']] = dk['updated_at']
                for f in unique_files:
                    if f['filename'] in draft_lookup:
                        f['has_draft'] = True
                        f['draft_updated_at'] = draft_lookup[f['filename']]
                    else:
                        f['has_draft'] = False
            except Exception as e:
                logger.debug(f"Could not annotate drafts for list-pdf: {e}")

        return {"success": True, "files": unique_files, "directory": directory}

    except Exception as e:
        logger.error(f"Error listing PDF files: {e}")
        return {"success": False, "files": [], "error": str(e)}





@router.post("/api/bank-import/draft")
async def save_bank_import_draft(request: Request):
    """Save work-in-progress state for a bank statement import."""
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        body = await request.json()
        bank_code = body.get("bank_code")
        source = body.get("source")
        filename = body.get("filename")
        if not bank_code or not source or not filename:
            return {"success": False, "error": "bank_code, source, and filename are required"}

        import json as _json
        draft_id = email_storage.save_import_draft(
            bank_code=bank_code,
            source=source,
            filename=filename,
            preview_data=_json.dumps(body.get("preview_data", {})),
            user_edits=_json.dumps(body.get("user_edits", {})),
            email_id=body.get("email_id"),
            attachment_id=body.get("attachment_id"),
            pdf_hash=body.get("pdf_hash"),
            target_system=body.get("target_system", "opera_se"),
        )
        return {"success": True, "draft_id": draft_id}

    except Exception as e:
        logger.error(f"Error saving bank import draft: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/bank-import/draft")
async def load_bank_import_draft(
    bank_code: str = Query(...),
    source: str = Query(...),
    email_id: Optional[int] = Query(None),
    attachment_id: Optional[str] = Query(None),
    pdf_hash: Optional[str] = Query(None),
    filename: Optional[str] = Query(None),
):
    """Load a saved work-in-progress draft for a bank statement import."""
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        import json as _json
        draft = email_storage.load_import_draft(
            bank_code=bank_code,
            source=source,
            email_id=email_id,
            attachment_id=attachment_id,
            pdf_hash=pdf_hash,
            filename=filename,
        )
        if draft:
            return {
                "success": True,
                "has_draft": True,
                "draft": {
                    "id": draft["id"],
                    "preview_data": _json.loads(draft["preview_data"]),
                    "user_edits": _json.loads(draft["user_edits"]),
                    "updated_at": draft["updated_at"],
                },
            }
        return {"success": True, "has_draft": False}

    except Exception as e:
        logger.error(f"Error loading bank import draft: {e}")
        return {"success": False, "error": str(e)}





@router.delete("/api/bank-import/draft")
async def delete_bank_import_draft(
    bank_code: str = Query(...),
    source: str = Query(...),
    email_id: Optional[int] = Query(None),
    attachment_id: Optional[str] = Query(None),
    pdf_hash: Optional[str] = Query(None),
    filename: Optional[str] = Query(None),
):
    """Delete a saved work-in-progress draft after import completion or manual clear."""
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        email_storage.delete_import_draft(
            bank_code=bank_code,
            source=source,
            email_id=email_id,
            attachment_id=attachment_id,
            pdf_hash=pdf_hash,
            filename=filename,
        )
        return {"success": True}

    except Exception as e:
        logger.error(f"Error deleting bank import draft: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/bank-import/pdf-content")
async def get_pdf_content(
    filename: str = Query(..., description="Full path to PDF file"),
):
    """
    Return PDF file content as base64 for viewing in the browser.
    """
    import base64

    try:
        resolved = filename
        # If just a filename (no path separator), search bank statement folders
        if not os.path.exists(resolved) and '/' not in filename and '\\' not in filename:
            try:
                settings = _load_company_settings()
                base_folder = settings.get("bank_statements_base_folder", "")
                if base_folder:
                    base = Path(base_folder)
                    for search_root in [base, base / 'archive']:
                        if search_root.exists():
                            for subfolder in search_root.iterdir():
                                if subfolder.is_dir():
                                    candidate = subfolder / filename
                                    if candidate.exists():
                                        resolved = str(candidate)
                                        break
                        if os.path.exists(resolved):
                            break
            except Exception:
                pass

        if not os.path.exists(resolved):
            return {"success": False, "error": f"PDF file not found: {filename}"}

        with open(resolved, 'rb') as f:
            pdf_bytes = f.read()

        pdf_base64 = base64.b64encode(pdf_bytes).decode('utf-8')

        return {
            "success": True,
            "pdf_data": pdf_base64,
            "filename": os.path.basename(resolved)
        }
    except Exception as e:
        logger.error(f"Error reading PDF file: {e}")
        return {"success": False, "error": str(e)}





@router.post("/api/bank-import/preview-from-pdf")
async def preview_bank_import_from_pdf(
    file_path: str = Query(..., description="Full path to PDF file"),
    bank_code: str = Query(..., description="Opera bank account code"),
):
    """
    Preview bank statement from PDF file.
    Uses AI extraction to parse the PDF and match transactions.
    Same response format as preview-from-email.
    """
    logger.info(f"preview-from-pdf: Called with file_path={file_path}, bank_code={bank_code}")

    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    if not os.path.exists(file_path):
        logger.warning(f"preview-from-pdf: File not found: {file_path}")
        return {"success": False, "error": f"File not found: {file_path}"}

    try:
        from sql_rag.bank_import import BankStatementImport, BankTransaction
        from sql_rag.statement_reconcile import StatementReconciler
        from sql_rag.opera_config import (
            get_period_posting_decision,
            get_current_period_info,
            is_open_period_accounting_enabled,
            validate_posting_period,
            get_ledger_type_for_transaction
        )

        filename = os.path.basename(file_path)
        logger.info(f"preview-from-pdf: Processing {filename}")

        # Use AI extraction for PDF - use the correct method that matches email endpoint
        reconciler = StatementReconciler(sql_connector, config=config)
        statement_info, stmt_transactions = reconciler.extract_transactions_from_pdf(file_path)

        logger.info(f"preview-from-pdf: Extracted {len(stmt_transactions)} transactions from PDF")

        if not statement_info:
            return {
                "success": False,
                "filename": filename,
                "error": "Failed to extract statement information from PDF"
            }

        # Convert StatementInfo to dict format for backward compatibility
        statement_info_dict = {
            "bank_name": statement_info.bank_name,
            "account_number": statement_info.account_number,
            "sort_code": statement_info.sort_code,
            "statement_date": statement_info.statement_date.isoformat() if statement_info.statement_date else None,
            "period_start": statement_info.period_start.isoformat() if statement_info.period_start else None,
            "period_end": statement_info.period_end.isoformat() if statement_info.period_end else None,
            "opening_balance": statement_info.opening_balance,
            "closing_balance": statement_info.closing_balance
        }

        # Get bank account details for validation (sort code, account number, reconciled balance)
        bank_df = sql_connector.execute_query(f"""
            SELECT
                RTRIM(nk_acnt) as code,
                RTRIM(nk_desc) as description,
                RTRIM(ISNULL(nk_sort, '')) as sort_code,
                RTRIM(ISNULL(nk_number, '')) as account_number,
                nk_recbal / 100.0 as reconciled_balance
            FROM nbank WITH (NOLOCK)
            WHERE nk_acnt = '{bank_code}'
        """)

        if bank_df.empty:
            return {"success": False, "error": f"Bank account {bank_code} not found"}

        opera_bank = bank_df.iloc[0]
        opera_sort = opera_bank['sort_code'].replace('-', '').replace(' ', '')
        opera_acct = opera_bank['account_number'].replace('-', '').replace(' ', '')
        reconciled_balance = float(opera_bank['reconciled_balance']) if opera_bank['reconciled_balance'] is not None else None

        # Validate bank account match
        stmt_sort = (statement_info_dict.get('sort_code') or '').replace('-', '').replace(' ', '')
        stmt_acct = (statement_info_dict.get('account_number') or '').replace('-', '').replace(' ', '')

        if stmt_sort and stmt_acct and opera_sort and opera_acct:
            if stmt_sort != opera_sort or stmt_acct != opera_acct:
                return {
                    "success": False,
                    "bank_mismatch": True,
                    "detected_bank": f"{stmt_sort} / {stmt_acct}",
                    "selected_bank": f"{opera_sort} / {opera_acct} ({bank_code})",
                    "error": "Bank account mismatch"
                }

        # Check statement sequence — correct opening balance if AI got it wrong
        opening_balance = statement_info_dict.get('opening_balance')
        if opening_balance is not None and reconciled_balance is not None:
            tolerance = 0.02
            if abs(opening_balance - reconciled_balance) > tolerance:
                # AI likely extracted wrong opening balance — use reconciled balance
                logger.warning(f"preview-from-pdf: Opening balance mismatch: extracted £{opening_balance:,.2f} vs reconciled £{reconciled_balance:,.2f} — using reconciled")
                statement_info_dict['opening_balance'] = reconciled_balance
                opening_balance = reconciled_balance
        elif opening_balance is None and reconciled_balance is not None:
            statement_info_dict['opening_balance'] = reconciled_balance
            opening_balance = reconciled_balance

        # Validate closing balance using transaction balance chain from opening.
        # Walks from opening, finding each transaction where current + amount = balance.
        # Excludes phantom transactions from other accounts (e.g. Monzo savings).
        if opening_balance is not None and stmt_transactions:
            try:
                current_bal = opening_balance
                chain_used = set()
                for _ in range(len(stmt_transactions)):
                    found = False
                    for i, st in enumerate(stmt_transactions):
                        if i in chain_used:
                            continue
                        expected = round(current_bal + st.amount, 2)
                        if st.balance is not None and abs(expected - st.balance) < 0.02:
                            current_bal = st.balance
                            chain_used.add(i)
                            found = True
                            break
                    if not found:
                        break
                if chain_used:
                    statement_info_dict['closing_balance'] = current_bal
                    excluded = len(stmt_transactions) - len(chain_used)
                    if excluded > 0:
                        logger.info(f"preview-from-pdf: Balance chain excluded {excluded} phantom transaction(s), closing=£{current_bal:,.2f}")
            except Exception as chain_err:
                logger.warning(f"preview-from-pdf: Balance chain failed: {chain_err}")

        # Update extraction cache with corrected balances so the scan listing is correct
        try:
            from sql_rag.pdf_extraction_cache import get_extraction_cache
            _cache = get_extraction_cache()
            _pdf_bytes = open(file_path, 'rb').read()
            _pdf_hash = _cache.hash_pdf(_pdf_bytes)
            _cached = _cache.get(_pdf_hash)
            if _cached:
                _info, _txns = _cached
                changed = False
                if statement_info_dict.get('opening_balance') is not None and _info.get('opening_balance') != statement_info_dict['opening_balance']:
                    _info['opening_balance'] = statement_info_dict['opening_balance']
                    changed = True
                if statement_info_dict.get('closing_balance') is not None and _info.get('closing_balance') != statement_info_dict['closing_balance']:
                    _info['closing_balance'] = statement_info_dict['closing_balance']
                    changed = True
                if changed:
                    _cache.put(_pdf_hash, _info, _txns)
                    logger.info(f"preview-from-pdf: Updated extraction cache with corrected balances")
        except Exception:
            pass

        # Convert StatementTransaction to BankTransaction objects
        logger.info(f"preview-from-pdf: Converting {len(stmt_transactions)} StatementTransactions to BankTransactions")
        transactions = []
        for i, st in enumerate(stmt_transactions, start=1):
            # StatementTransaction.amount: positive = money in, negative = money out
            bt = BankTransaction(
                row_number=i,
                date=st.date,
                amount=st.amount,
                subcategory=st.transaction_type or '',
                memo=st.description or '',
                name=st.description or '',
                reference=st.reference or '',
                fit_id=''
            )
            transactions.append(bt)

        # Now match transactions using BankStatementImport
        importer = BankStatementImport(bank_code=bank_code, sql_connector=sql_connector)

        # Process transactions (matching, duplicate detection) — same as OFX/CSV flow
        importer.process_transactions(transactions)

        # Get period info for validation
        period_info = None
        try:
            period_result = get_current_period_info(sql_connector)
            if period_result:
                period_info = {
                    'current_year': period_result.get('current_year'),
                    'current_period': period_result.get('current_period'),
                    'open_period_accounting': is_open_period_accounting_enabled(sql_connector)
                }
        except Exception as e:
            logger.warning(f"Could not get period info: {e}")

        # Load pattern learner for suggestions on unmatched items
        try:
            from sql_rag.bank_patterns import BankPatternLearner
            pattern_learner = BankPatternLearner(company_code=sql_connector.company_code if hasattr(sql_connector, 'company_code') else 'default')
        except Exception as e:
            logger.warning(f"Could not initialize pattern learner: {e}")
            pattern_learner = None

        # Load GoCardless FX imports for auto-detection
        gc_fx_refs = {}
        if email_storage:
            try:
                gc_fx_refs = email_storage.get_gocardless_fx_imports('opera_se')
                if gc_fx_refs:
                    logger.info(f"Loaded {len(gc_fx_refs)} GoCardless FX references for auto-detection (PDF)")
            except Exception as e:
                logger.warning(f"Failed to load GoCardless FX imports: {e}")

        # Build result lists from importer.result (same structure as OFX/CSV preview)
        matched_receipts = []
        matched_payments = []
        matched_refunds = []
        repeat_entries = []
        unmatched = []
        already_posted = []
        skipped = []

        for txn in transactions:
            txn_dict = {
                'row': txn.row_number,
                'date': txn.date,
                'amount': txn.amount,
                'name': txn.name,
                'reference': txn.reference,
                'memo': txn.memo,
            }

            if txn.is_duplicate:
                txn_dict['action'] = 'skip'
                txn_dict['reason'] = txn.skip_reason or 'Already posted'
                already_posted.append(txn_dict)
            elif txn.action == 'skip' and not txn.matched_account:
                # No match found — treat as unmatched for manual assignment
                # (includes "No supplier/customer match", ambiguous matches, etc.)
                sim_key = _compute_similarity_key(txn.name, txn.memo or "")
                txn_dict['action'] = 'manual'
                txn_dict['reason'] = txn.skip_reason or 'No match found'
                txn_dict['similarity_key'] = sim_key
                unmatched.append(txn_dict)
            elif txn.action in ('sales_receipt', 'purchase_payment', 'sales_refund', 'purchase_refund',
                                'nominal_payment', 'nominal_receipt', 'bank_transfer', 'repeat_entry'):
                txn_dict['account'] = txn.matched_account or ''
                txn_dict['account_name'] = txn.matched_name or ''
                txn_dict['match_score'] = txn.match_score or 0
                txn_dict['match_source'] = txn.match_source or ''
                txn_dict['action'] = txn.action
                txn_dict['transaction_type'] = txn.action
                if hasattr(txn, 'bank_transfer_details'):
                    txn_dict['bank_transfer_details'] = txn.bank_transfer_details

                if txn.action in ('sales_refund', 'purchase_refund'):
                    matched_refunds.append(txn_dict)
                elif txn.action == 'repeat_entry':
                    txn_dict['repeat_entry_ref'] = getattr(txn, 'repeat_entry_ref', '')
                    txn_dict['repeat_entry_desc'] = getattr(txn, 'repeat_entry_desc', '')
                    repeat_entries.append(txn_dict)
                elif txn.amount > 0:
                    matched_receipts.append(txn_dict)
                else:
                    matched_payments.append(txn_dict)
            else:
                # Unmatched — try pattern suggestion
                suggestion = None
                if pattern_learner:
                    try:
                        suggestion = pattern_learner.find_pattern(txn.memo or txn.name)
                    except Exception:
                        pass

                sim_key = _compute_similarity_key(txn.name, txn.memo or "")
                txn_dict['action'] = 'manual'
                txn_dict['reason'] = 'No match found'
                txn_dict['similarity_key'] = sim_key

                if suggestion:
                    txn_dict['suggested_type'] = suggestion.transaction_type
                    txn_dict['suggested_account'] = suggestion.account_code
                    txn_dict['suggested_account_name'] = suggestion.account_name
                    txn_dict['suggested_ledger_type'] = suggestion.ledger_type
                    txn_dict['suggested_vat_code'] = suggestion.vat_code
                    txn_dict['suggested_nominal_code'] = suggestion.nominal_code
                    txn_dict['suggestion_confidence'] = suggestion.confidence
                    txn_dict['suggestion_source'] = suggestion.match_type

                # Check GoCardless FX
                gc_fx_match = None
                if gc_fx_refs:
                    txn_text = f"{txn.name or ''} {txn.memo or ''}".upper()
                    for ref_key, ref_data in gc_fx_refs.items():
                        if ref_key.upper() in txn_text:
                            gc_fx_match = (ref_key, ref_data)
                            break
                if gc_fx_match:
                    ref_key, ref_data = gc_fx_match
                    txn_dict['action'] = 'gc_fx_ignore'
                    txn_dict['reason'] = f"GoCardless FX payout ({ref_data['currency']})"
                    txn_dict['gc_fx_currency'] = ref_data['currency']
                    txn_dict['gc_fx_original_amount'] = ref_data['gross_amount']
                    txn_dict['gc_fx_gbp_amount'] = ref_data['fx_amount']
                    txn_dict['gc_fx_reference'] = ref_key
                    skipped.append(txn_dict)
                else:
                    unmatched.append(txn_dict)

        # Compute similarity counts for unmatched items
        from collections import Counter
        sim_key_counts = Counter(item.get('similarity_key', '') for item in unmatched)
        for item in unmatched:
            key = item.get('similarity_key', '')
            item['similar_count'] = sim_key_counts.get(key, 1)

        # Build response
        logger.info(f"preview-from-pdf: Returning response - total={len(transactions)}, receipts={len(matched_receipts)}, payments={len(matched_payments)}, unmatched={len(unmatched)}, skipped={len(skipped)}")
        return {
            "success": True,
            "filename": filename,
            "detected_format": "PDF",
            "total_transactions": len(transactions),
            "matched_receipts": matched_receipts,
            "matched_payments": matched_payments,
            "matched_refunds": matched_refunds,
            "repeat_entries": repeat_entries,
            "unmatched": unmatched,
            "already_posted": already_posted,
            "skipped": skipped,
            "summary": {
                "to_import": len(matched_receipts) + len(matched_payments) + len(matched_refunds),
                "refund_count": len(matched_refunds),
                "repeat_entry_count": len(repeat_entries),
                "unmatched_count": len(unmatched),
                "already_posted_count": len(already_posted),
                "skipped_count": len(skipped)
            },
            "errors": [],
            "period_info": period_info,
            "statement_bank_info": {
                "bank_name": statement_info_dict.get('bank_name'),
                "account_number": statement_info_dict.get('account_number'),
                "sort_code": statement_info_dict.get('sort_code'),
                "statement_date": statement_info_dict.get('statement_date'),
                "opening_balance": statement_info_dict.get('opening_balance'),
                "closing_balance": statement_info_dict.get('closing_balance'),
                "matched_opera_bank": bank_code,
                "matched_opera_name": opera_bank['description']
            },
            # Raw statement data for reconcile screen — same data regardless of email or file source
            "statement_transactions": [
                {
                    "line_number": i,
                    "date": st.date.isoformat() if hasattr(st.date, 'isoformat') else str(st.date),
                    "description": st.description or '',
                    "amount": st.amount,
                    "balance": st.balance,
                    "transaction_type": st.transaction_type or '',
                    "reference": st.reference or ''
                }
                for i, st in enumerate(stmt_transactions, start=1)
            ] if stmt_transactions else [],
            "statement_info": statement_info_dict
        }

    except Exception as e:
        logger.error(f"Error previewing PDF: {e}", exc_info=True)
        return {"success": False, "error": str(e)}





@router.post("/api/bank-import/import-from-pdf")
async def import_bank_statement_from_pdf(
    request: Request,
    file_path: str = Query(..., description="Full path to PDF file"),
    bank_code: str = Query(..., description="Opera bank account code"),
    auto_allocate: bool = Query(False, description="Auto-allocate to oldest invoices"),
    auto_reconcile: bool = Query(False, description="Auto-reconcile imported entries against bank statement"),
    resume_import_id: Optional[int] = Query(None, description="Import ID to resume from (skips already-posted lines)")
):
    """
    Import bank statement from PDF file.
    Uses the same import logic as import-from-email.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    if not os.path.exists(file_path):
        return {"success": False, "error": f"File not found: {file_path}"}

    try:
        from sql_rag.bank_import import BankStatementImport
        from sql_rag.statement_reconcile import StatementReconciler

        filename = os.path.basename(file_path)

        # Get request body for overrides
        body = await request.json() if request.headers.get('content-type') == 'application/json' else {}
        overrides = body.get('overrides', [])
        selected_rows = body.get('selected_rows', [])
        date_overrides = body.get('date_overrides', [])
        rejected_refund_rows = body.get('rejected_refund_rows', [])

        # Use AI extraction for PDF - use the correct method that matches email endpoint
        reconciler = StatementReconciler(sql_connector, config=config)
        statement_info, stmt_transactions = reconciler.extract_transactions_from_pdf(file_path)

        if not statement_info:
            return {"success": False, "error": "Failed to extract statement information from PDF"}

        # Convert StatementInfo to dict format for backward compatibility
        statement_info_dict = {
            "bank_name": statement_info.bank_name,
            "account_number": statement_info.account_number,
            "sort_code": statement_info.sort_code,
            "statement_date": statement_info.statement_date.isoformat() if statement_info.statement_date else None,
            "period_start": statement_info.period_start.isoformat() if statement_info.period_start else None,
            "period_end": statement_info.period_end.isoformat() if statement_info.period_end else None,
            "opening_balance": statement_info.opening_balance,
            "closing_balance": statement_info.closing_balance
        }

        # Check for statement period overlap (prevent double-posting)
        skip_overlap_check = body.get('skip_overlap_check', False)
        if not skip_overlap_check and email_storage:
            period_start_str = statement_info_dict.get('period_start')
            period_end_str = statement_info_dict.get('period_end')
            # Fall back to first/last transaction dates if period not explicitly available
            if not period_start_str and stmt_transactions:
                dates = [st.date for st in stmt_transactions if st.date]
                if dates:
                    period_start_str = min(dates).isoformat() if hasattr(min(dates), 'isoformat') else str(min(dates))
                    period_end_str = max(dates).isoformat() if hasattr(max(dates), 'isoformat') else str(max(dates))

            if period_start_str and period_end_str:
                overlap = email_storage.check_period_overlap(
                    bank_code=bank_code,
                    period_start=period_start_str,
                    period_end=period_end_str,
                    exclude_import_id=resume_import_id
                )
                if overlap:
                    return {
                        "success": False,
                        "overlap_warning": True,
                        "error": f"Statement period overlaps with a previously imported statement",
                        "overlap_details": {
                            "existing_import_id": overlap['import_id'],
                            "existing_filename": overlap['filename'],
                            "existing_period": f"{overlap['period_start']} to {overlap['period_end']}",
                            "existing_import_date": overlap['import_date'],
                            "new_period": f"{period_start_str} to {period_end_str}"
                        }
                    }

        # Import using BankStatementImport
        importer = BankStatementImport(bank_code=bank_code, sql_connector=sql_connector)

        # Convert StatementTransaction to BankTransaction format
        from sql_rag.bank_import import BankTransaction
        transactions = []
        for i, st in enumerate(stmt_transactions, start=1):
            bt = BankTransaction(
                row_number=i,
                date=st.date,
                amount=st.amount,
                subcategory=st.transaction_type or '',
                memo=st.description or '',
                name=st.description or '',
                reference=st.reference or '',
                fit_id=''
            )
            transactions.append(bt)

        # Process transactions to match accounts
        importer.process_transactions(transactions)

        # Apply date overrides
        date_override_map = {d['row']: d['date'] for d in date_overrides}
        for txn in transactions:
            if txn.row_number in date_override_map:
                new_date_str = date_override_map[txn.row_number]
                txn.original_date = txn.date
                txn.date = datetime.strptime(new_date_str, '%Y-%m-%d').date()

        # Apply manual overrides
        override_map = {o['row']: o for o in overrides}
        for txn in transactions:
            if txn.row_number in override_map:
                override = override_map[txn.row_number]
                if override.get('account'):
                    txn.manual_account = override.get('account')
                    txn.manual_ledger_type = override.get('ledger_type')
                # Apply cashbook type override
                if override.get('cbtype'):
                    txn.cbtype = override.get('cbtype')
                transaction_type = override.get('transaction_type')
                if transaction_type and transaction_type in ('sales_receipt', 'purchase_payment', 'sales_refund', 'purchase_refund', 'nominal_payment', 'nominal_receipt', 'bank_transfer'):
                    txn.action = transaction_type
                    # Store bank transfer details on the transaction
                    if transaction_type == 'bank_transfer':
                        txn.bank_transfer_details = override.get('bank_transfer_details', {})
                elif override.get('ledger_type') == 'C':
                    txn.action = 'sales_receipt'
                elif override.get('ledger_type') == 'S':
                    txn.action = 'purchase_payment'
                elif override.get('ledger_type') == 'N':
                    txn.action = 'nominal_payment' if txn.amount < 0 else 'nominal_receipt'
                # Apply project/department/VAT codes for nominal entries
                if override.get('project_code'):
                    txn.project_code = override['project_code']
                if override.get('department_code'):
                    txn.department_code = override['department_code']
                if override.get('vat_code'):
                    txn.vat_code = override['vat_code']

        # Convert selected_rows to set
        selected_rows_set = set(selected_rows) if selected_rows else None
        rejected_refund_set = set(rejected_refund_rows) if rejected_refund_rows else set()

        # Acquire bank-level import lock to prevent concurrent imports
        from sql_rag.import_lock import acquire_import_lock, release_import_lock
        if not acquire_import_lock(_bank_lock_key(bank_code), locked_by="api", endpoint="import-from-pdf"):
            return {"success": False, "error": f"Bank account {bank_code} is currently being imported by another user. Please wait for the current import to complete."}

        # Load already-posted lines for partial recovery (skip on resume)
        already_posted = {}
        if resume_import_id and email_storage:
            try:
                already_posted = email_storage.get_posted_lines(resume_import_id)
                if already_posted:
                    logger.info(f"Resume import: {len(already_posted)} lines already posted for import_id={resume_import_id}")
            except Exception as e:
                logger.warning(f"Could not load posted lines for resume: {e}")

        # Import transactions one by one (same pattern as email import)
        imported = []
        errors = []
        skipped_not_selected = 0
        skipped_incomplete = 0
        skipped_duplicates = 0
        skipped_already_posted = 0

        for txn in transactions:
            # Skip rows not in selected_rows (if specified)
            if selected_rows_set is not None and txn.row_number not in selected_rows_set:
                skipped_not_selected += 1
                continue

            # Skip already-posted lines (partial recovery resume)
            if txn.row_number in already_posted:
                skipped_already_posted += 1
                imported.append({
                    "row": txn.row_number,
                    "date": txn.date.isoformat(),
                    "amount": txn.amount,
                    "account": txn.manual_account or txn.matched_account or '',
                    "account_name": txn.matched_name or '',
                    "action": txn.action,
                    "entry_number": already_posted[txn.row_number],
                    "name": txn.name or '',
                    "already_posted": True
                })
                continue

            # Skip rejected refunds
            if txn.row_number in rejected_refund_set:
                continue

            # Handle bank transfers separately (paired entries in two banks)
            if txn.action == 'bank_transfer' and not txn.is_duplicate:
                bt_details = getattr(txn, 'bank_transfer_details', {}) or {}
                dest_bank = bt_details.get('dest_bank') or txn.manual_account
                if not dest_bank:
                    errors.append({"row": txn.row_number, "error": "Bank transfer missing destination bank"})
                    continue

                amount = abs(txn.amount)
                # Direction: negative = paying out (current bank is source), positive = receiving in
                if txn.amount < 0:
                    source, dest = bank_code, dest_bank
                else:
                    source, dest = dest_bank, bank_code

                try:
                    from sql_rag.opera_sql_import import OperaSQLImport
                    opera_import = OperaSQLImport(sql_connector)
                    bt_result = opera_import.import_bank_transfer(
                        source_bank=source,
                        dest_bank=dest,
                        amount_pounds=amount,
                        reference=(bt_details.get('reference') or txn.reference or '')[:20],
                        post_date=txn.date,
                        comment=(bt_details.get('comment') or txn.memo or '')[:50],
                        input_by='SQLRAG'
                    )
                    if bt_result.get('success'):
                        imported.append({
                            "row": txn.row_number,
                            "date": txn.date.isoformat() if txn.date else None,
                            "amount": txn.amount,
                            "account": dest_bank if txn.amount < 0 else source,
                            "account_name": f"Transfer {'to' if txn.amount < 0 else 'from'} {dest_bank if txn.amount < 0 else source}",
                            "action": 'bank_transfer',
                            "entry_number": bt_result.get('source_entry') or bt_result.get('dest_entry'),
                            "name": txn.name or '',
                            "memo": txn.memo or '',
                            "reference": txn.reference or '',
                            "allocated": False,
                            "allocation_result": None
                        })
                    else:
                        errors.append({"row": txn.row_number, "error": bt_result.get('error', 'Bank transfer failed')})
                except Exception as e:
                    errors.append({"row": txn.row_number, "error": f"Bank transfer error: {str(e)}"})
                continue

            if txn.action in ('sales_receipt', 'purchase_payment', 'sales_refund', 'purchase_refund', 'nominal_payment', 'nominal_receipt') and not txn.is_duplicate:
                account = txn.manual_account or txn.matched_account
                if not account:
                    skipped_incomplete += 1
                    errors.append({"row": txn.row_number, "error": "Missing account"})
                    continue

                # Just-in-time duplicate check - catches entries that appeared since statement was processed
                try:
                    from sql_rag.opera_sql_import import OperaSQLImport as _OI
                    _oi = _OI(sql_connector)
                    acct_type = 'customer' if txn.action in ('sales_receipt', 'sales_refund') else 'supplier' if txn.action in ('purchase_payment', 'purchase_refund') else 'nominal'
                    dup_check = _oi.check_duplicate_before_posting(
                        bank_account=bank_code,
                        transaction_date=txn.date,
                        amount_pounds=abs(txn.amount),
                        account_code=account,
                        account_type=acct_type
                    )
                    if dup_check['is_duplicate']:
                        skipped_duplicates += 1
                        errors.append({"row": txn.row_number, "error": f"Skipped - {dup_check['details']}"})
                        logger.warning(f"Row {txn.row_number}: Pre-posting duplicate detected - {dup_check['details']}")
                        continue
                except Exception as dup_err:
                    logger.warning(f"Row {txn.row_number}: Pre-posting duplicate check failed: {dup_err}")

                try:
                    result = importer.import_transaction(txn, validate_only=False)
                    if result.success:
                        import_record = {
                            "row": txn.row_number,
                            "date": txn.date.isoformat() if txn.date else None,
                            "amount": txn.amount,
                            "account": txn.manual_account or txn.matched_account,
                            "account_name": txn.matched_name or '',
                            "action": txn.action,
                            "batch_ref": getattr(result, 'batch_ref', None) or getattr(result, 'batch_number', None),
                            "entry_number": getattr(result, 'entry_number', None),
                            "name": txn.name or '',
                            "memo": txn.memo or '',
                            "reference": txn.reference or '',
                            "allocated": False,
                            "allocation_result": None
                        }

                        # Auto-allocate if enabled
                        if auto_allocate and txn.action in ('sales_receipt', 'purchase_payment'):
                            from sql_rag.opera_sql_import import OperaSQLImport
                            opera_import = OperaSQLImport(sql_connector)
                            account_code = txn.manual_account or txn.matched_account
                            txn_ref = getattr(result, 'transaction_ref', None) or txn.reference or txn.name[:20]

                            if txn.action == 'sales_receipt':
                                alloc_result = opera_import.auto_allocate_receipt(
                                    customer_account=account_code,
                                    receipt_ref=txn_ref,
                                    receipt_amount=abs(txn.amount),
                                    allocation_date=txn.date,
                                    bank_account=bank_code,
                                    description=txn.memo or txn.name
                                )
                                import_record["allocated"] = alloc_result.get("success", False)
                                import_record["allocation_result"] = alloc_result
                            elif txn.action == 'purchase_payment':
                                alloc_result = opera_import.auto_allocate_payment(
                                    supplier_account=account_code,
                                    payment_ref=txn_ref,
                                    payment_amount=abs(txn.amount),
                                    allocation_date=txn.date,
                                    bank_account=bank_code,
                                    description=txn.memo or txn.name
                                )
                                import_record["allocated"] = alloc_result.get("success", False)
                                import_record["allocation_result"] = alloc_result

                        imported.append(import_record)
                    else:
                        error_msg = '; '.join(result.errors) if result.errors else 'Import failed'
                        errors.append({"row": txn.row_number, "error": error_msg})
                except Exception as e:
                    errors.append({"row": txn.row_number, "error": str(e)})

        # Calculate totals
        receipts_imported = sum(1 for t in imported if t['action'] == 'sales_receipt')
        payments_imported = sum(1 for t in imported if t['action'] == 'purchase_payment')
        refunds_imported = sum(1 for t in imported if t['action'] in ('sales_refund', 'purchase_refund'))
        transfers_imported = sum(1 for t in imported if t['action'] == 'bank_transfer')
        total_receipts = sum(t['amount'] for t in imported if t['action'] == 'sales_receipt')
        total_payments = sum(abs(t['amount']) for t in imported if t['action'] == 'purchase_payment')

        # Build result
        result = {
            "success": len(imported) > 0,
            "imported_count": len(imported),
            "imported_transactions_count": len(imported),
            "receipts_imported": receipts_imported,
            "payments_imported": payments_imported,
            "refunds_imported": refunds_imported,
            "transfers_imported": transfers_imported,
            "total_receipts": total_receipts,
            "total_payments": total_payments,
            "skipped_not_selected": skipped_not_selected,
            "skipped_incomplete": skipped_incomplete,
            "skipped_duplicates": skipped_duplicates,
            "imported_transactions": imported,
            "errors": errors,
            "auto_allocate_enabled": auto_allocate,
            "statement_info": statement_info_dict
        }

        # Record in import history
        if len(imported) > 0:
            try:
                total_receipts = result.get('receipts_imported', 0)
                total_payments = result.get('payments_imported', 0)
                transactions_imported = total_receipts + total_payments

                # Get current user
                current_user = getattr(request.state, 'user', None)
                imported_by = current_user.get('username', 'Unknown') if current_user else 'Unknown'

                statement_date = statement_info_dict.get('statement_date')
                if statement_date:
                    # Format for SQL
                    try:
                        from datetime import datetime
                        if isinstance(statement_date, str):
                            # Try parsing various formats
                            for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y']:
                                try:
                                    dt = datetime.strptime(statement_date.split('T')[0].split(' ')[0], fmt)
                                    statement_date = dt.strftime('%Y-%m-%d')
                                    break
                                except:
                                    pass
                    except:
                        pass

                sql_connector.execute_query(f"""
                    INSERT INTO bank_import_history
                    (filename, source, bank_code, total_receipts, total_payments,
                     transactions_imported, target_system, imported_by, statement_date)
                    VALUES (
                        '{filename.replace("'", "''")}',
                        'pdf',
                        '{bank_code}',
                        {total_receipts},
                        {total_payments},
                        {transactions_imported},
                        'Opera SQL',
                        '{imported_by}',
                        {f"'{statement_date}'" if statement_date else 'NULL'}
                    )
                """)

                # Also record in email_storage for statement status tracking
                # This allows BankStatementReconcile to show the import status
                if email_storage:
                    import_record_id = email_storage.record_bank_statement_import(
                        bank_code=bank_code,
                        filename=filename,
                        transactions_imported=transactions_imported,
                        source='file',
                        target_system='opera_se',
                        total_receipts=total_receipts,
                        total_payments=total_payments,
                        imported_by=imported_by,
                        opening_balance=statement_info_dict.get('opening_balance'),
                        closing_balance=statement_info_dict.get('closing_balance'),
                        statement_date=statement_info_dict.get('statement_date'),
                        account_number=statement_info_dict.get('account_number'),
                        sort_code=statement_info_dict.get('sort_code'),
                        period_start=statement_info_dict.get('period_start'),
                        period_end=statement_info_dict.get('period_end'),
                        file_path=file_path
                    )
                    result['import_id'] = import_record_id

                    # Persist statement transactions for reconciliation lifecycle
                    if stmt_transactions and import_record_id:
                        try:
                            raw_txns = [
                                {
                                    "line_number": i,
                                    "date": st.date.isoformat() if hasattr(st.date, 'isoformat') else str(st.date),
                                    "description": st.description or '',
                                    "amount": st.amount,
                                    "balance": st.balance,
                                    "transaction_type": st.transaction_type or '',
                                    "reference": st.reference or ''
                                }
                                for i, st in enumerate(stmt_transactions, start=1)
                            ]
                            email_storage.save_statement_transactions(
                                import_id=import_record_id,
                                transactions=raw_txns,
                                statement_info=statement_info_dict
                            )
                            logger.info(f"Saved {len(raw_txns)} statement transactions for import_id={import_record_id}")

                            # Mark successfully imported transactions as posted (for partial recovery)
                            for imp_txn in imported:
                                entry_num = imp_txn.get('entry_number')
                                row_num = imp_txn.get('row')
                                if entry_num and row_num:
                                    email_storage.mark_transaction_posted(import_record_id, row_num, str(entry_num))
                        except Exception as txn_err:
                            logger.warning(f"Could not save statement transactions: {txn_err}")
            except Exception as e:
                logger.warning(f"Could not record import history: {e}")

            # Learn patterns from successful imports
            try:
                from sql_rag.bank_patterns import BankPatternLearner
                pattern_learner = BankPatternLearner(company_code=sql_connector.company_code if hasattr(sql_connector, 'company_code') else 'default')

                # Learn from overrides (user's explicit choices)
                for override in overrides:
                    if override.get('account') and override.get('ledger_type'):
                        # Find the transaction memo
                        txn = next((t for t in transactions if t.row_number == override.get('row')), None)
                        if txn:
                            pattern_learner.learn_pattern(
                                description=txn.memo or txn.name,
                                transaction_type=override.get('transaction_type', 'PI' if txn.amount < 0 else 'SI'),
                                account_code=override['account'],
                                account_name=override.get('account_name'),
                                ledger_type=override['ledger_type'],
                                vat_code=override.get('vat_code'),
                                nominal_code=override.get('nominal_code'),
                                net_amount=override.get('net_amount')
                            )
                logger.info(f"Learned patterns from {len(overrides)} overrides")
            except Exception as e:
                logger.warning(f"Could not learn patterns: {e}")

            # Auto-reconciliation: mark imported entries as reconciled
            if auto_reconcile:
                try:
                    from sql_rag.opera_sql_import import OperaSQLImport

                    imported = result.get('imported_transactions', [])
                    errors = result.get('errors', [])

                    if len(imported) > 0 and len(errors) == 0:
                        # Collect entries with valid entry_numbers
                        entries_to_reconcile = []
                        statement_line = 10

                        for txn in imported:
                            entry_num = txn.get('entry_number')
                            if entry_num:
                                entries_to_reconcile.append({
                                    'entry_number': entry_num,
                                    'statement_line': statement_line
                                })
                                statement_line += 10

                        if len(entries_to_reconcile) == len(imported):
                            # All entries have entry_numbers - proceed
                            latest_date = None
                            for txn in imported:
                                if txn.get('date'):
                                    txn_date = datetime.strptime(txn['date'], '%Y-%m-%d').date() if isinstance(txn['date'], str) else txn['date']
                                    if latest_date is None or txn_date > latest_date:
                                        latest_date = txn_date

                            if latest_date is None:
                                latest_date = datetime.now().date()

                            statement_number = int(latest_date.strftime('%y%m%d'))

                            opera_import = OperaSQLImport(sql_connector)
                            recon_result = opera_import.mark_entries_reconciled(
                                bank_account=bank_code,
                                entries=entries_to_reconcile,
                                statement_number=statement_number,
                                statement_date=latest_date,
                                reconciliation_date=datetime.now().date()
                            )

                            result['reconciliation_result'] = {
                                "success": recon_result.success,
                                "entries_reconciled": recon_result.records_imported if recon_result.success else 0,
                                "statement_number": statement_number,
                                "statement_date": latest_date.isoformat(),
                                "messages": recon_result.warnings if recon_result.success else recon_result.errors
                            }
                            result['auto_reconcile_enabled'] = True

                            if recon_result.success:
                                logger.info(f"PDF auto-reconciliation complete: {len(entries_to_reconcile)} entries")
                            else:
                                logger.warning(f"PDF auto-reconciliation failed: {recon_result.errors}")
                        else:
                            missing_count = len(imported) - len(entries_to_reconcile)
                            result['reconciliation_result'] = {
                                "success": False,
                                "entries_reconciled": 0,
                                "messages": [f"Cannot auto-reconcile: {missing_count} entries missing entry_number"]
                            }
                    else:
                        result['reconciliation_result'] = {
                            "success": False,
                            "entries_reconciled": 0,
                            "messages": ["Cannot auto-reconcile: import had errors or no transactions"]
                        }

                except Exception as recon_err:
                    logger.error(f"PDF auto-reconciliation error: {recon_err}")
                    result['reconciliation_result'] = {
                        "success": False,
                        "entries_reconciled": 0,
                        "messages": [f"Auto-reconciliation error: {str(recon_err)}"]
                    }

        release_import_lock(_bank_lock_key(bank_code))
        return result

    except Exception as e:
        release_import_lock(_bank_lock_key(bank_code))
        logger.error(f"Error importing PDF: {e}", exc_info=True)
        return {"success": False, "error": str(e)}





@router.post("/api/bank-import/validate-csv")
async def validate_csv_bank_match(
    filepath: str = Query(..., description="Path to CSV file"),
    bank_code: str = Query(..., description="Selected bank account code")
):
    """
    Validate that a CSV file matches the selected bank account.

    Returns whether the bank account in the CSV (sort code + account number)
    matches the selected Opera bank account.
    """
    import os
    if not filepath or not os.path.exists(filepath):
        return {"success": False, "error": "File not found", "valid": False}

    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.bank_import import BankStatementImport

        importer = BankStatementImport(bank_code=bank_code, sql_connector=sql_connector)
        is_valid, message, detected_bank = importer.validate_bank_account_from_csv(filepath)

        return {
            "success": True,
            "valid": is_valid,
            "message": message,
            "detected_bank": detected_bank,
            "selected_bank": bank_code
        }

    except Exception as e:
        logger.error(f"Error validating CSV: {e}")
        return {"success": False, "error": str(e), "valid": False}





@router.get("/api/bank-import/accounts/customers")
async def get_customers_for_dropdown():
    """
    Get customer accounts for dropdown selection in UI.

    Returns simplified list for account selection dropdowns.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        df = sql_connector.execute_query("""
            SELECT
                RTRIM(sn_account) as code,
                RTRIM(sn_name) as name,
                RTRIM(ISNULL(sn_key1, '')) as search_key
            FROM sname WITH (NOLOCK)
            ORDER BY sn_account
        """)

        accounts = [
            {
                "code": row['code'],
                "name": row['name'],
                "search_key": row.get('search_key', ''),
                "display": f"{row['code']} - {row['name']}"
            }
            for _, row in df.iterrows()
        ]

        return {
            "success": True,
            "count": len(accounts),
            "accounts": accounts
        }

    except Exception as e:
        logger.error(f"Error getting customers: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/bank-import/accounts/suppliers")
async def get_suppliers_for_dropdown():
    """
    Get supplier accounts for dropdown selection in UI.

    Returns simplified list for account selection dropdowns.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        df = sql_connector.execute_query("""
            SELECT
                RTRIM(pn_account) as code,
                RTRIM(pn_name) as name,
                RTRIM(ISNULL(pn_payee, '')) as payee
            FROM pname WITH (NOLOCK)
            ORDER BY pn_account
        """)

        accounts = [
            {
                "code": row['code'],
                "name": row['name'],
                "payee": row.get('payee', ''),
                "display": f"{row['code']} - {row['name']}"
            }
            for _, row in df.iterrows()
        ]

        return {
            "success": True,
            "count": len(accounts),
            "accounts": accounts
        }

    except Exception as e:
        logger.error(f"Error getting suppliers: {e}")
        return {"success": False, "error": str(e)}





@router.post("/api/bank-import/import-with-overrides")
async def import_with_manual_overrides(
    filepath: str = Query(..., description="Path to bank statement file"),
    bank_code: str = Query(..., description="Opera bank account code"),
    auto_allocate: bool = Query(False, description="Auto-allocate receipts/payments to invoices where possible"),
    auto_reconcile: bool = Query(False, description="Auto-reconcile imported entries against bank statement"),
    request_body: Dict[str, Any] = Body(None)
):
    """
    Import bank statement with manual account overrides, date overrides, and rejected rows.

    Request body format:
    {
        "overrides": [{"row": 1, "account": "A001", "ledger_type": "C", "transaction_type": "sales_refund"}, ...],
        "date_overrides": [{"row": 1, "date": "2025-01-15"}, ...],  // Date changes for period violations
        "selected_rows": [1, 2, 3, 5]  // Row numbers to import (only these rows will be imported)
    }

    Also accepts legacy format (just array of overrides) for backwards compatibility.
    If selected_rows is not provided, all matched transactions are imported.
    Import will be blocked if any selected transactions have period violations.

    Auto-allocation (when auto_allocate=True):
    - Receipts: allocated to customer invoices if invoice ref found in description OR clears account (2+ invoices)
    - Payments: allocated to supplier invoices using same rules
    - Single invoice with no reference: NOT auto-allocated (dangerous assumption)
    - Zero tolerance on amounts - must match exactly

    Auto-reconciliation (when auto_reconcile=True):
    - Marks all imported entries as reconciled in Opera
    - Statement number generated from latest transaction date (YYMMDD format)
    - Only reconciles if ALL entries imported successfully (all-or-nothing)
    - Updates nbank.nk_recbal with new reconciled balance
    """
    import os
    from datetime import datetime
    if not filepath or not os.path.exists(filepath):
        return {"success": False, "error": "File not found"}

    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    # Acquire bank-level lock
    from sql_rag.import_lock import acquire_import_lock, release_import_lock
    if not acquire_import_lock(_bank_lock_key(bank_code), locked_by="api", endpoint="import-with-overrides"):
        return {"success": False, "error": f"Bank account {bank_code} is currently being imported by another user. Please wait for the current import to complete."}

    try:
        from sql_rag.bank_import import BankStatementImport
        from sql_rag.opera_config import get_period_posting_decision

        # Handle both new format (object with overrides and selected_rows) and legacy format (just array)
        if request_body is None:
            overrides = []
            date_overrides = []
            selected_rows = None  # None means import all matched
        elif isinstance(request_body, list):
            # Legacy format: just an array of overrides
            overrides = request_body
            date_overrides = []
            selected_rows = None
        else:
            # New format: object with overrides, date_overrides, and selected_rows
            overrides = request_body.get('overrides', [])
            date_overrides = request_body.get('date_overrides', [])
            selected_rows_list = request_body.get('selected_rows')
            selected_rows = set(selected_rows_list) if selected_rows_list is not None else None

        importer = BankStatementImport(
            bank_code=bank_code,
            use_enhanced_matching=True,
            use_fingerprinting=True
        )

        # Parse and process
        transactions, detected_format = importer.parse_file(filepath)
        importer.process_transactions(transactions)

        # Apply date overrides first (to fix period violations)
        date_override_map = {d['row']: d['date'] for d in date_overrides}
        for txn in transactions:
            if txn.row_number in date_override_map:
                new_date_str = date_override_map[txn.row_number]
                txn.original_date = txn.date  # Preserve original
                txn.date = datetime.strptime(new_date_str, '%Y-%m-%d').date()

        # Apply manual overrides (supports unmatched, skipped, and refund modifications)
        override_map = {o['row']: o for o in overrides}
        for txn in transactions:
            if txn.row_number in override_map:
                override = override_map[txn.row_number]
                # Only apply account override if provided
                if override.get('account'):
                    txn.manual_account = override.get('account')
                    txn.manual_ledger_type = override.get('ledger_type')

                # Apply cashbook type override
                if override.get('cbtype'):
                    txn.cbtype = override.get('cbtype')

                # Use explicit transaction_type if provided, otherwise infer from ledger type
                transaction_type = override.get('transaction_type')
                if transaction_type and transaction_type in ('sales_receipt', 'purchase_payment', 'sales_refund', 'purchase_refund', 'nominal_payment', 'nominal_receipt', 'bank_transfer'):
                    txn.action = transaction_type
                    # Store bank transfer details on the transaction
                    if transaction_type == 'bank_transfer':
                        txn.bank_transfer_details = override.get('bank_transfer_details', {})
                elif override.get('ledger_type') == 'C':
                    txn.action = 'sales_receipt'
                elif override.get('ledger_type') == 'S':
                    txn.action = 'purchase_payment'
                elif override.get('ledger_type') == 'N':
                    txn.action = 'nominal_payment' if txn.amount < 0 else 'nominal_receipt'
                # Apply project/department/VAT codes for nominal entries
                if override.get('project_code'):
                    txn.project_code = override['project_code']
                if override.get('department_code'):
                    txn.department_code = override['department_code']
                if override.get('vat_code'):
                    txn.vat_code = override['vat_code']

        # Validate periods for all selected transactions before importing
        # Use ledger-specific validation (SL for receipts/refunds to customers, PL for payments/refunds from suppliers)
        from sql_rag.opera_config import (
            validate_posting_period,
            get_ledger_type_for_transaction,
            get_current_period_info
        )

        period_info = get_current_period_info(sql_connector)
        period_violations = []

        for txn in transactions:
            # Only check transactions that will be imported
            if selected_rows is not None and txn.row_number not in selected_rows:
                continue
            if txn.action not in ('sales_receipt', 'purchase_payment', 'sales_refund', 'purchase_refund', 'nominal_payment', 'nominal_receipt', 'bank_transfer'):
                continue
            if txn.is_duplicate:
                continue

            # Get the appropriate ledger type for this transaction
            ledger_type = get_ledger_type_for_transaction(txn.action)

            # Use ledger-specific period validation
            period_result = validate_posting_period(sql_connector, txn.date, ledger_type)

            if not period_result.is_valid:
                ledger_names = {'SL': 'Sales Ledger', 'PL': 'Purchase Ledger', 'NL': 'Nominal Ledger'}
                period_violations.append({
                    "row": txn.row_number,
                    "date": txn.date.isoformat(),
                    "name": txn.name,
                    "amount": txn.amount,
                    "action": txn.action,
                    "ledger_type": ledger_type,
                    "ledger_name": ledger_names.get(ledger_type, ledger_type),
                    "error": period_result.error_message,
                    "year": period_result.year,
                    "period": period_result.period
                })

        # Block import if any period violations exist
        if period_violations:
            return {
                "success": False,
                "error": "Cannot import - some transactions are in blocked periods for their respective ledgers",
                "period_violations": period_violations,
                "period_info": {
                    "current_year": period_info.get('np_year'),
                    "current_period": period_info.get('np_perno')
                },
                "message": "The following transactions cannot be posted because their dates fall within "
                          "closed or blocked periods for the Sales or Purchase Ledger. Please adjust the "
                          "dates or open the periods in Opera before importing."
            }

        # Block import if there are unprocessed repeat entries in OPEN periods
        # Period-blocked repeat entries are silently skipped (they can't be posted anyway)
        # User must run Opera's Repeat Entries routine for open-period entries first
        unprocessed_repeat_entries = []
        for txn in transactions:
            if txn.action == 'repeat_entry' and getattr(txn, 'period_valid', True):
                unprocessed_repeat_entries.append({
                    "row": txn.row_number,
                    "name": txn.name,
                    "amount": txn.amount,
                    "date": txn.date.isoformat(),
                    "entry_ref": getattr(txn, 'repeat_entry_ref', None),
                    "entry_desc": getattr(txn, 'repeat_entry_desc', None)
                })

        if unprocessed_repeat_entries:
            return {
                "success": False,
                "error": "Cannot import - there are unprocessed repeat entries",
                "repeat_entries": unprocessed_repeat_entries,
                "message": "Please run Opera's Repeat Entries routine first to post these transactions, "
                          "then re-preview the bank statement. The repeat entry transactions will then "
                          "be detected as already posted (duplicates) and excluded from import."
            }

        # Import transactions (all 4 action types), only importing selected rows
        imported = []
        errors = []
        skipped_not_selected = 0
        skipped_incomplete = 0

        for txn in transactions:
            # Skip rows not in selected_rows (if selected_rows is specified)
            if selected_rows is not None and txn.row_number not in selected_rows:
                skipped_not_selected += 1
                continue

            # Handle bank transfers separately (paired entries in two banks)
            if txn.action == 'bank_transfer' and not txn.is_duplicate:
                bt_details = getattr(txn, 'bank_transfer_details', {}) or {}
                dest_bank = bt_details.get('dest_bank') or txn.manual_account
                if not dest_bank:
                    errors.append({"row": txn.row_number, "error": "Bank transfer missing destination bank"})
                    continue

                amount = abs(txn.amount)
                if txn.amount < 0:
                    source, dest = bank_code, dest_bank
                else:
                    source, dest = dest_bank, bank_code

                try:
                    from sql_rag.opera_sql_import import OperaSQLImport
                    opera_import = OperaSQLImport(sql_connector)
                    bt_result = opera_import.import_bank_transfer(
                        source_bank=source,
                        dest_bank=dest,
                        amount_pounds=amount,
                        reference=(bt_details.get('reference') or txn.reference or '')[:20],
                        post_date=txn.date,
                        comment=(bt_details.get('comment') or txn.memo or '')[:50],
                        input_by='SQLRAG'
                    )
                    if bt_result.get('success'):
                        imported.append({
                            "row": txn.row_number,
                            "account": dest_bank if txn.amount < 0 else source,
                            "account_name": f"Transfer {'to' if txn.amount < 0 else 'from'} {dest_bank if txn.amount < 0 else source}",
                            "amount": txn.amount,
                            "action": 'bank_transfer',
                            "entry_number": bt_result.get('source_entry') or bt_result.get('dest_entry'),
                            "name": txn.name or '',
                            "memo": txn.memo or '',
                            "reference": txn.reference or '',
                            "allocated": False,
                            "allocation_result": None
                        })
                    else:
                        errors.append({"row": txn.row_number, "error": bt_result.get('error', 'Bank transfer failed')})
                except Exception as e:
                    errors.append({"row": txn.row_number, "error": f"Bank transfer error: {str(e)}"})
                continue

            if txn.action in ('sales_receipt', 'purchase_payment', 'sales_refund', 'purchase_refund', 'nominal_payment', 'nominal_receipt') and not txn.is_duplicate:
                # Validate mandatory data before import
                account = txn.manual_account or txn.matched_account
                if not account:
                    skipped_incomplete += 1
                    errors.append({
                        "row": txn.row_number,
                        "error": "Missing account - cannot import without customer/supplier assigned"
                    })
                    continue

                if not txn.action or txn.action not in ('sales_receipt', 'purchase_payment', 'sales_refund', 'purchase_refund', 'nominal_payment', 'nominal_receipt'):
                    skipped_incomplete += 1
                    errors.append({
                        "row": txn.row_number,
                        "error": "Missing transaction type - cannot import without valid type assigned"
                    })
                    continue

                try:
                    result = importer.import_transaction(txn)
                    if result.success:
                        import_record = {
                            "row": txn.row_number,
                            "account": txn.manual_account or txn.matched_account,
                            "account_name": txn.matched_name or '',
                            "amount": txn.amount,
                            "action": txn.action,
                            "batch_ref": getattr(result, 'batch_ref', None) or getattr(result, 'batch_number', None),
                            "entry_number": getattr(result, 'entry_number', None),  # For auto-reconciliation
                            "date": txn.date.isoformat() if txn.date else None,
                            "name": txn.name or '',
                            "memo": txn.memo or '',
                            "reference": txn.reference or '',
                            "allocated": False,
                            "allocation_result": None
                        }

                        # Auto-allocate if enabled
                        if auto_allocate and txn.action in ('sales_receipt', 'purchase_payment'):
                            from sql_rag.opera_sql_import import OperaSQLImport
                            opera_import = OperaSQLImport(sql_connector)
                            account_code = txn.manual_account or txn.matched_account
                            # Get the reference from the import result
                            txn_ref = getattr(result, 'transaction_ref', None) or txn.reference or txn.name[:20]

                            if txn.action == 'sales_receipt':
                                alloc_result = opera_import.auto_allocate_receipt(
                                    customer_account=account_code,
                                    receipt_ref=txn_ref,
                                    receipt_amount=abs(txn.amount),
                                    allocation_date=txn.date,
                                    bank_account=bank_code,
                                    description=txn.memo or txn.name
                                )
                                import_record["allocated"] = alloc_result.get("success", False)
                                import_record["allocation_result"] = alloc_result
                            elif txn.action == 'purchase_payment':
                                alloc_result = opera_import.auto_allocate_payment(
                                    supplier_account=account_code,
                                    payment_ref=txn_ref,
                                    payment_amount=abs(txn.amount),
                                    allocation_date=txn.date,
                                    bank_account=bank_code,
                                    description=txn.memo or txn.name
                                )
                                import_record["allocated"] = alloc_result.get("success", False)
                                import_record["allocation_result"] = alloc_result

                        imported.append(import_record)

                        # Learn from manual assignment
                        if txn.manual_account and importer.alias_manager:
                            inferred_ledger = 'C' if txn.action in ('sales_receipt', 'sales_refund') else 'S'
                            importer.alias_manager.save_alias(
                                bank_name=txn.name,
                                ledger_type=txn.manual_ledger_type or inferred_ledger,
                                account_code=txn.manual_account,
                                match_score=1.0,
                                created_by='MANUAL_IMPORT'
                            )
                    else:
                        error_msg = '; '.join(result.errors) if result.errors else 'Import failed'
                        errors.append({"row": txn.row_number, "error": error_msg})
                except Exception as e:
                    errors.append({"row": txn.row_number, "error": str(e)})

        # Calculate totals by action type
        receipts_imported = sum(1 for t in imported if t['action'] == 'sales_receipt')
        payments_imported = sum(1 for t in imported if t['action'] == 'purchase_payment')
        refunds_imported = sum(1 for t in imported if t['action'] in ('sales_refund', 'purchase_refund'))
        transfers_imported = sum(1 for t in imported if t['action'] == 'bank_transfer')

        # Calculate amounts
        total_receipts = sum(t['amount'] for t in imported if t['action'] == 'sales_receipt')
        total_payments = sum(abs(t['amount']) for t in imported if t['action'] == 'purchase_payment')

        # Calculate allocation stats
        allocations_attempted = sum(1 for t in imported if t.get('allocation_result'))
        allocations_successful = sum(1 for t in imported if t.get('allocated', False))

        # Record import in history (for file imports)
        if len(imported) > 0 and email_storage:
            try:
                email_storage.record_bank_statement_import(
                    bank_code=bank_code,
                    filename=os.path.basename(filepath),
                    transactions_imported=len(imported),
                    source='file',
                    target_system='opera_se',
                    total_receipts=total_receipts,
                    total_payments=total_payments,
                    imported_by='BANK_IMPORT',
                    file_path=filepath
                )
            except Exception as history_err:
                logger.warning(f"Failed to record import history: {history_err}")

        # Auto-reconciliation: mark imported entries as reconciled
        reconciliation_result = None
        if auto_reconcile and len(imported) > 0 and len(errors) == 0:
            try:
                from sql_rag.opera_sql_import import OperaSQLImport

                # Collect entries with valid entry_numbers
                entries_to_reconcile = []
                statement_line = 10  # Start at 10, increment by 10

                for txn in imported:
                    entry_num = txn.get('entry_number')
                    if entry_num:
                        entries_to_reconcile.append({
                            'entry_number': entry_num,
                            'statement_line': statement_line
                        })
                        statement_line += 10

                if len(entries_to_reconcile) == len(imported):
                    # All entries have entry_numbers - proceed with reconciliation

                    # Generate statement number from latest transaction date (YYMMDD format)
                    latest_date = None
                    for txn in imported:
                        if txn.get('date'):
                            txn_date = datetime.strptime(txn['date'], '%Y-%m-%d').date() if isinstance(txn['date'], str) else txn['date']
                            if latest_date is None or txn_date > latest_date:
                                latest_date = txn_date

                    if latest_date is None:
                        latest_date = datetime.now().date()

                    # Statement number as YYMMDD (e.g., 260209 for 2026-02-09)
                    statement_number = int(latest_date.strftime('%y%m%d'))

                    opera_import = OperaSQLImport(sql_connector)
                    recon_result = opera_import.mark_entries_reconciled(
                        bank_account=bank_code,
                        entries=entries_to_reconcile,
                        statement_number=statement_number,
                        statement_date=latest_date,
                        reconciliation_date=datetime.now().date()
                    )

                    reconciliation_result = {
                        "success": recon_result.success,
                        "entries_reconciled": recon_result.records_imported if recon_result.success else 0,
                        "statement_number": statement_number,
                        "statement_date": latest_date.isoformat(),
                        "messages": recon_result.warnings if recon_result.success else recon_result.errors
                    }

                    if recon_result.success:
                        logger.info(f"Auto-reconciliation complete: {len(entries_to_reconcile)} entries, statement {statement_number}")
                    else:
                        logger.warning(f"Auto-reconciliation failed: {recon_result.errors}")
                else:
                    # Some entries missing entry_numbers - cannot reconcile
                    missing_count = len(imported) - len(entries_to_reconcile)
                    reconciliation_result = {
                        "success": False,
                        "entries_reconciled": 0,
                        "messages": [f"Cannot auto-reconcile: {missing_count} entries missing entry_number"]
                    }
                    logger.warning(f"Auto-reconciliation skipped: {missing_count} entries missing entry_number")

            except Exception as recon_err:
                logger.error(f"Auto-reconciliation error: {recon_err}")
                reconciliation_result = {
                    "success": False,
                    "entries_reconciled": 0,
                    "messages": [f"Auto-reconciliation error: {str(recon_err)}"]
                }

        return {
            "success": len(imported) > 0,  # Success if any transactions were imported
            "imported_count": len(imported),
            "imported_transactions_count": len(imported),  # Frontend expects this name
            "receipts_imported": receipts_imported,
            "payments_imported": payments_imported,
            "refunds_imported": refunds_imported,
            "transfers_imported": transfers_imported,
            "total_receipts": total_receipts,
            "total_payments": total_payments,
            "skipped_not_selected": skipped_not_selected,
            "skipped_incomplete": skipped_incomplete,
            "imported_transactions": imported,
            "errors": errors,
            "auto_allocate_enabled": auto_allocate,
            "allocations_attempted": allocations_attempted,
            "allocations_successful": allocations_successful,
            "auto_reconcile_enabled": auto_reconcile,
            "reconciliation_result": reconciliation_result
        }

    except Exception as e:
        logger.error(f"Import with overrides error: {e}")
        return {"success": False, "error": str(e)}
    finally:
        try:
            release_import_lock(_bank_lock_key(bank_code))
        except Exception:
            pass





@router.post("/api/bank-import/update-repeat-entry-date")
async def update_repeat_entry_date(
    entry_ref: str = Query(..., description="Repeat entry reference (ae_entry)"),
    bank_code: str = Query(..., description="Bank account code"),
    new_date: str = Query(..., description="New next posting date (YYYY-MM-DD)"),
    statement_name: Optional[str] = Query(None, description="Bank statement name/reference for learning")
):
    """
    Update the next posting date (ae_nxtpost) for a repeat entry.

    This allows syncing the repeat entry schedule with the actual bank transaction date.
    After updating, the user should run Opera's Repeat Entries routine to post the transaction,
    then re-preview the bank statement.

    If statement_name is provided, saves an alias for future automatic matching.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from datetime import datetime

        # Validate date format
        try:
            parsed_date = datetime.strptime(new_date, '%Y-%m-%d').date()
        except ValueError:
            return {"success": False, "error": f"Invalid date format: {new_date}. Expected YYYY-MM-DD"}

        # Verify the repeat entry exists
        verify_query = f"""
            SELECT ae_entry, ae_desc, ae_nxtpost, ae_acnt
            FROM arhead WITH (NOLOCK)
            WHERE RTRIM(ae_entry) = '{entry_ref}'
              AND RTRIM(ae_acnt) = '{bank_code}'
        """
        df = sql_connector.execute_query(verify_query)

        if df is None or len(df) == 0:
            return {
                "success": False,
                "error": f"Repeat entry '{entry_ref}' not found for bank '{bank_code}'"
            }

        old_date = df.iloc[0]['ae_nxtpost']
        description = str(df.iloc[0]['ae_desc']).strip()

        # Get current timestamp for audit fields
        now = datetime.now()
        amend_date = now.strftime('%Y-%m-%d')
        amend_time = now.strftime('%H:%M:%S')

        # Update the next posting date AND audit fields in the header record
        update_query = f"""
            UPDATE arhead WITH (ROWLOCK)
            SET ae_nxtpost = '{new_date}',
                sq_amdate = '{amend_date}',
                sq_amtime = '{amend_time}',
                sq_amuser = 'BANKIMP'
            WHERE RTRIM(ae_entry) = '{entry_ref}'
              AND RTRIM(ae_acnt) = '{bank_code}'
        """
        rows_affected = sql_connector.execute_non_query(update_query)

        if rows_affected == 0:
            return {
                "success": False,
                "error": f"No rows updated - entry may have been modified"
            }

        logger.info(f"Updated repeat entry {entry_ref} ae_nxtpost from {old_date} to {new_date} ({rows_affected} row(s))")

        # Save alias for future matching if statement_name provided
        alias_saved = False
        if statement_name:
            try:
                from sql_rag.bank_aliases import BankAliasManager
                alias_manager = BankAliasManager()
                alias_saved = alias_manager.save_repeat_entry_alias(
                    bank_name=statement_name,
                    bank_code=bank_code,
                    entry_ref=entry_ref,
                    entry_desc=description
                )
                if alias_saved:
                    logger.info(f"Saved repeat entry alias: '{statement_name}' -> {entry_ref}")
            except Exception as alias_err:
                logger.warning(f"Could not save repeat entry alias: {alias_err}")

        return {
            "success": True,
            "message": f"Updated '{description}' next posting date to {new_date}",
            "entry_ref": entry_ref,
            "old_date": str(old_date) if old_date else None,
            "new_date": new_date,
            "alias_saved": alias_saved
        }

    except Exception as e:
        logger.error(f"Error updating repeat entry date: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/bank-import/repeat-entries")
async def list_repeat_entries(
    bank_code: str = Query(..., description="Bank account code")
):
    """
    List all active repeat entries for a bank account.
    Useful for debugging repeat entry matching.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        query = f"""
            SELECT
                h.ae_entry,
                h.ae_desc,
                h.ae_nxtpost,
                h.ae_freq,
                h.ae_every,
                h.ae_posted,
                h.ae_topost,
                h.ae_type,
                l.at_value,
                l.at_account,
                l.at_cbtype,
                l.at_comment,
                CASE WHEN h.ae_topost = 0 OR h.ae_posted < h.ae_topost THEN 'Active' ELSE 'Completed' END as status
            FROM arhead h WITH (NOLOCK)
            JOIN arline l WITH (NOLOCK) ON h.ae_entry = l.at_entry AND h.ae_acnt = l.at_acnt
            WHERE RTRIM(h.ae_acnt) = '{bank_code}'
            ORDER BY h.ae_nxtpost DESC
        """
        df = sql_connector.execute_query(query)

        if df is None or len(df) == 0:
            return {
                "success": True,
                "bank_code": bank_code,
                "repeat_entries": [],
                "message": f"No repeat entries found for bank {bank_code}"
            }

        entries = []
        for _, row in df.iterrows():
            amount_pence = row.get('at_value', 0)
            amount_pounds = abs(amount_pence) / 100 if amount_pence else 0
            entries.append({
                "entry_ref": str(row.get('ae_entry', '')).strip(),
                "description": str(row.get('ae_desc', '')).strip() or str(row.get('at_comment', '')).strip(),
                "next_post_date": str(row.get('ae_nxtpost', ''))[:10] if row.get('ae_nxtpost') else None,
                "frequency": row.get('ae_freq', ''),
                "every": row.get('ae_every', 1),
                "posted_count": row.get('ae_posted', 0),
                "total_posts": row.get('ae_topost', 0),
                "status": row.get('status', ''),
                "amount_pence": amount_pence,
                "amount_pounds": amount_pounds,
                "account": str(row.get('at_account', '')).strip(),
                "cb_type": str(row.get('at_cbtype', '')).strip()
            })

        return {
            "success": True,
            "bank_code": bank_code,
            "repeat_entries": entries,
            "count": len(entries)
        }

    except Exception as e:
        logger.error(f"Error listing repeat entries: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/bank-import/folder-settings")
async def get_bank_import_folder_settings():
    """Get bank statement folder settings (per-company). Used by both email and folder import."""
    try:
        settings = _load_company_settings()
        base = settings.get("bank_statements_base_folder", "")
        archive = settings.get("bank_statements_archive_folder", "")
        return {
            "success": True,
            "base_folder": base,
            "archive_folder": archive,
            "folder_enabled": bool(base),  # Enabled when base folder is configured
        }
    except Exception as e:
        logger.error(f"Error reading bank import folder settings: {e}")
        return {"success": True, "base_folder": "", "archive_folder": "", "folder_enabled": False}





@router.post("/api/bank-import/folder-settings")
async def save_bank_import_folder_settings(request: Request):
    """Save bank statement folder settings (per-company). Used by both email and folder import."""
    try:
        body = await request.json()
        settings = _load_company_settings()
        settings["bank_statements_base_folder"] = body.get("base_folder", "")
        settings["bank_statements_archive_folder"] = body.get("archive_folder", "")
        if _save_company_settings(settings):
            return {"success": True, "message": "Folder settings saved"}
        return {"success": False, "error": "Failed to save settings"}
    except Exception as e:
        logger.error(f"Error saving bank import folder settings: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/bank-import/scan-folder")
async def scan_folder_for_bank_statements(
    bank_code: str = Query(..., description="Opera bank account code"),
    validate_balances: bool = Query(True, description="Validate statement balances against Opera"),
):
    """
    Scan the configured input folder for bank statement PDFs.

    Returns statements in sequential import order, same format as scan-emails.
    PDFs are validated against Opera's reconciled balance using the extraction cache.
    """
    try:
        from datetime import datetime
        from pathlib import Path

        # Check folder is configured (per-company settings)
        settings = _load_company_settings()
        base_folder = settings.get("bank_statements_base_folder", "")

        if not base_folder:
            return {"success": False, "error": "Statement folders not configured. Set the base folder in Bank Rec Settings."}

        base_path = Path(base_folder)
        if not base_path.exists():
            return {"success": False, "error": f"Base folder does not exist: {base_folder}"}

        # Get bank details from Opera (need description for subfolder name)
        reconciled_balance = None
        opera_sort_code = None
        opera_account_number = None
        bank_description = ""

        if sql_connector:
            from sqlalchemy import text
            with sql_connector.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT RTRIM(nk_sort) as sort_code, RTRIM(nk_number) as account_number,
                           nk_recbal, RTRIM(nk_desc) as bank_desc
                    FROM nbank WHERE RTRIM(nk_code) = :bank_code
                """), {"bank_code": bank_code.strip()})
                row = result.fetchone()
                if row:
                    opera_sort_code = (row[0] or '').replace('-', '').replace(' ', '').strip()
                    opera_account_number = (row[1] or '').replace('-', '').replace(' ', '').strip()
                    reconciled_balance = row[2] / 100.0 if row[2] is not None else None
                    bank_description = row[3] if len(row) > 3 else ""

        # Resolve bank-specific subfolder
        archive_folder = settings.get("bank_statements_archive_folder", "")
        subfolder_name = _get_bank_subfolder_name(bank_code, bank_description)
        folder_path = base_path / subfolder_name
        folder_path.mkdir(parents=True, exist_ok=True)

        # Also check highest reconciled closing balance (effective reconciled balance)
        effective_rec_balance = reconciled_balance
        if email_storage and reconciled_balance is not None:
            try:
                highest_closing = email_storage.get_highest_reconciled_closing_balance(bank_code)
                if highest_closing is not None and highest_closing > reconciled_balance:
                    effective_rec_balance = highest_closing
            except Exception:
                pass

        # Get already-processed filenames
        reconciled_filenames = set()
        imported_filenames = set()
        if email_storage:
            try:
                reconciled_keys = email_storage.get_reconciled_statement_keys(bank_code)
                reconciled_filenames = {k[0] for k in reconciled_keys}
            except Exception:
                pass
            try:
                imported = email_storage.list_bank_imports(bank_code=bank_code, limit=500)
                imported_filenames = {imp.get('filename', '') for imp in imported if imp.get('filename')}
            except Exception:
                pass

        # Scan folder for PDFs
        from sql_rag.statement_reconcile import PDFExtractionCache
        scan_cache = PDFExtractionCache()

        statements = []
        total_pdfs = 0

        for file_path in sorted(folder_path.iterdir()):
            if not file_path.is_file() or file_path.suffix.lower() != '.pdf':
                continue

            total_pdfs += 1
            filename = file_path.name

            # Skip already reconciled
            if filename in reconciled_filenames:
                continue

            is_imported = filename in imported_filenames
            stmt_entry = {
                'filename': filename,
                'full_path': str(file_path),
                'source': 'folder',
                'already_processed': False,
                'is_imported': is_imported,
                'status': 'imported' if is_imported else 'pending',
                'file_size': file_path.stat().st_size,
                'file_modified': datetime.fromtimestamp(file_path.stat().st_mtime).isoformat(),
            }

            # Try cache lookup for balance validation and bank matching
            if validate_balances:
                try:
                    with open(str(file_path), 'rb') as f:
                        content_bytes = f.read()
                    pdf_hash = scan_cache.hash_pdf(content_bytes)
                    cached = scan_cache.get(pdf_hash)

                    if cached:
                        info_data, _ = cached
                        stmt_entry['opening_balance'] = float(info_data.get('opening_balance')) if info_data.get('opening_balance') is not None else None
                        stmt_entry['closing_balance'] = float(info_data.get('closing_balance')) if info_data.get('closing_balance') is not None else None
                        stmt_entry['period_start'] = info_data.get('period_start')
                        stmt_entry['period_end'] = info_data.get('period_end')
                        stmt_entry['bank_name'] = info_data.get('bank_name')
                        stmt_entry['account_number'] = info_data.get('account_number')
                        stmt_entry['sort_code'] = info_data.get('sort_code')

                        # Verify this PDF matches the selected bank
                        stmt_sort = (info_data.get('sort_code') or '').replace('-', '').replace(' ', '').strip()
                        stmt_acct = (info_data.get('account_number') or '').replace('-', '').replace(' ', '').strip()

                        if opera_sort_code and opera_account_number:
                            if stmt_sort and stmt_acct:
                                if stmt_sort != opera_sort_code or stmt_acct != opera_account_number:
                                    continue  # Skip — different bank

                        # Check if already past reconciled balance
                        if effective_rec_balance is not None and stmt_entry.get('closing_balance') is not None:
                            if stmt_entry['closing_balance'] <= effective_rec_balance and stmt_entry.get('opening_balance') is not None:
                                # Check if this statement's closing matches a known reconciled closing
                                if filename in reconciled_filenames:
                                    continue

                        stmt_entry['status'] = 'ready' if not is_imported else 'imported'
                except Exception as e:
                    logger.warning(f"Could not validate PDF {filename}: {e}")

            statements.append(stmt_entry)

        # Sort by opening balance chain (same as scan-emails)
        statements_with_balance = [s for s in statements if s.get('opening_balance') is not None]
        statements_without_balance = [s for s in statements if s.get('opening_balance') is None]

        if statements_with_balance and effective_rec_balance is not None:
            # Build chain: find statement whose opening balance matches reconciled balance
            ordered = []
            remaining = list(statements_with_balance)
            current_balance = effective_rec_balance

            for _ in range(len(remaining)):
                found = False
                for i, stmt in enumerate(remaining):
                    ob = stmt.get('opening_balance')
                    if ob is not None and abs(ob - current_balance) < 0.02:
                        stmt['import_sequence'] = len(ordered) + 1
                        ordered.append(stmt)
                        current_balance = stmt.get('closing_balance', current_balance)
                        remaining.pop(i)
                        found = True
                        break
                if not found:
                    break

            # Append any unchained statements at the end
            for stmt in remaining:
                stmt['import_sequence'] = len(ordered) + 1
                ordered.append(stmt)

            statements = ordered + statements_without_balance
        else:
            # Sort by filename as fallback
            statements.sort(key=lambda s: s['filename'])

        # Number all statements
        for i, stmt in enumerate(statements):
            if 'import_sequence' not in stmt:
                stmt['import_sequence'] = i + 1

        return {
            "success": True,
            "statements_found": statements,
            "total_found": len(statements),
            "total_pdfs_scanned": total_pdfs,
            "input_folder": str(folder_path),
            "bank_code": bank_code,
            "reconciled_balance": reconciled_balance,
            "message": f"Found {len(statements)} statement(s) in {folder_path}" if statements else f"No unprocessed statements found in {folder_path}",
        }

    except Exception as e:
        logger.error(f"Error scanning folder for bank statements: {e}")
        return {"success": False, "error": str(e)}





@router.post("/api/bank-import/fetch-emails-to-folder")
async def fetch_email_statements_to_folder(
    bank_code: str = Query(..., description="Opera bank account code"),
    days_back: int = Query(30, description="Number of days to search back"),
):
    """
    Download bank statement PDF attachments from email inbox into the bank's
    statement subfolder.  Returns the number of new files saved so the frontend
    can refresh the folder scan list.

    Flow:
    1. Scan inbox for emails with PDF attachments (same logic as scan-emails)
    2. For each PDF that is NOT already on disk in the bank subfolder, download
       and save it.
    3. Return count of newly saved files.
    """
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        from datetime import datetime, timedelta
        from pathlib import Path

        # Load company settings for base folder
        settings = _load_company_settings()
        base_folder = settings.get("bank_statements_base_folder", "")
        if not base_folder:
            return {"success": False, "error": "Statement folders not configured. Set the base folder in Bank Rec Settings."}

        base_path = Path(base_folder)
        if not base_path.exists():
            return {"success": False, "error": f"Base folder does not exist: {base_folder}"}

        # Get bank description from Opera for subfolder name
        bank_description = ""
        if sql_connector:
            try:
                from sqlalchemy import text as sa_text
                with sql_connector.engine.connect() as conn:
                    result = conn.execute(sa_text("""
                        SELECT RTRIM(nk_desc) as bank_desc
                        FROM nbank WHERE RTRIM(nk_code) = :bank_code
                    """), {"bank_code": bank_code.strip()})
                    row = result.fetchone()
                    if row:
                        bank_description = row[0] or ""
            except Exception:
                pass

        # Ensure bank subfolder exists
        subfolder_name = _get_bank_subfolder_name(bank_code, bank_description)
        folder_path = base_path / subfolder_name
        folder_path.mkdir(parents=True, exist_ok=True)

        # Get existing filenames in the folder (to avoid re-downloading)
        existing_files = {f.name.lower() for f in folder_path.iterdir() if f.is_file()}

        # Scan emails
        from_date = datetime.utcnow() - timedelta(days=days_back)
        result = email_storage.get_emails(from_date=from_date, page=1, page_size=500)

        saved_count = 0
        skipped_count = 0
        saved_files = []

        for email in result.get('emails', []):
            email_id = email.get('id')
            if not email.get('has_attachments'):
                continue

            email_detail = email_storage.get_email_by_id(email_id)
            if not email_detail:
                continue

            attachments = email_detail.get('attachments', [])
            email_from = email.get('from_address', '')
            email_subject = email.get('subject', '')

            for att in attachments:
                filename = att.get('filename', '')
                content_type = att.get('content_type', '')
                attachment_id = att.get('attachment_id', '')

                if not is_bank_statement_attachment(filename, content_type, email_from, email_subject):
                    continue

                # Skip if file already exists in folder
                if filename.lower() in existing_files:
                    skipped_count += 1
                    continue

                # Download the attachment
                try:
                    provider_id = email_detail.get('provider_id')
                    message_id = email_detail.get('message_id')
                    folder_id = email_detail.get('folder_id', 'INBOX')

                    if not (provider_id and message_id and email_sync_manager and provider_id in email_sync_manager.providers):
                        continue

                    provider = email_sync_manager.providers[provider_id]

                    # Resolve folder_id if it's numeric
                    if isinstance(folder_id, int):
                        with email_storage._get_connection() as conn:
                            cursor = conn.cursor()
                            cursor.execute("SELECT folder_id FROM email_folders WHERE id = ?", (folder_id,))
                            row = cursor.fetchone()
                            if row:
                                folder_id = row['folder_id']

                    dl_result = await provider.download_attachment(message_id, attachment_id, folder_id)

                    # Try archive folder if not found in original folder
                    if not dl_result and folder_id not in ('Archive/Bank Statements', 'Archive/BankStatements'):
                        for archive_folder_name in ['Archive/Bank Statements', 'Archive/BankStatements']:
                            dl_result = await provider.download_attachment(message_id, attachment_id, archive_folder_name)
                            if dl_result:
                                break

                    if dl_result:
                        content_bytes, _, _ = dl_result
                        dest_path = folder_path / filename

                        # Handle duplicate filename
                        if dest_path.exists():
                            stem = Path(filename).stem
                            suffix = Path(filename).suffix
                            counter = 1
                            while dest_path.exists():
                                dest_path = folder_path / f"{stem}_{counter}{suffix}"
                                counter += 1

                        with open(dest_path, 'wb') as f:
                            f.write(content_bytes)

                        saved_count += 1
                        saved_files.append(dest_path.name)
                        existing_files.add(dest_path.name.lower())
                        logger.info(f"Saved email attachment to folder: {dest_path}")

                except Exception as dl_err:
                    logger.warning(f"Failed to download attachment {filename}: {dl_err}")
                    continue

        return {
            "success": True,
            "saved_count": saved_count,
            "skipped_count": skipped_count,
            "saved_files": saved_files,
            "folder": str(folder_path),
            "message": f"Downloaded {saved_count} new statement(s) from email" if saved_count > 0 else "No new statements found in email",
        }

    except Exception as e:
        logger.error(f"Error fetching email statements to folder: {e}")
        return {"success": False, "error": str(e)}





@router.post("/api/bank-import/archive-statement")
async def archive_folder_statement(request: Request):
    """
    Move a successfully imported statement PDF from input folder to archive folder.
    Called after successful import+reconciliation.
    """
    try:
        import shutil
        from pathlib import Path

        body = await request.json()
        file_path = body.get("file_path", "")
        if not file_path:
            return {"success": False, "error": "file_path is required"}

        source_path = Path(file_path)
        if not source_path.exists():
            return {"success": False, "error": f"File not found: {file_path}"}

        # Get archive folder from per-company settings
        settings = _load_company_settings()
        archive_folder = settings.get("bank_statements_archive_folder", "")
        bank_code = body.get("bank_code", "")
        bank_description = body.get("bank_description", "")

        if not archive_folder:
            # Default: create 'archive' subfolder in base folder
            archive_dir = source_path.parent.parent / 'archive'
        else:
            archive_dir = Path(archive_folder)

        # Use bank-specific subfolder if bank info provided
        if bank_code:
            subfolder = _get_bank_subfolder_name(bank_code, bank_description)
            archive_dir = archive_dir / subfolder

        # Create year-month subfolder
        now = datetime.now()
        archive_dir = archive_dir / now.strftime('%Y-%m')
        archive_dir.mkdir(parents=True, exist_ok=True)

        dest = archive_dir / source_path.name
        # Handle duplicate filenames
        if dest.exists():
            stem = dest.stem
            suffix = dest.suffix
            counter = 1
            while dest.exists():
                dest = archive_dir / f"{stem}_{counter}{suffix}"
                counter += 1

        shutil.move(str(source_path), str(dest))
        logger.info(f"Archived statement {source_path.name} to {dest}")

        return {
            "success": True,
            "message": f"Statement archived to {dest}",
            "archive_path": str(dest),
        }

    except Exception as e:
        logger.error(f"Error archiving statement: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/opera3/bank-import/folder-settings")
async def opera3_get_bank_import_folder_settings():
    """Get bank statement folder import settings (Opera 3)."""
    return await get_bank_import_folder_settings()





@router.post("/api/opera3/bank-import/folder-settings")
async def opera3_save_bank_import_folder_settings(request: Request):
    """Save bank statement folder import settings (Opera 3)."""
    return await save_bank_import_folder_settings(request)





@router.get("/api/opera3/bank-import/scan-folder")
async def opera3_scan_folder_for_bank_statements(
    bank_code: str = Query(..., description="Opera bank account code"),
    validate_balances: bool = Query(True),
):
    """Scan folder for bank statement PDFs (Opera 3)."""
    return await scan_folder_for_bank_statements(bank_code=bank_code, validate_balances=validate_balances)





@router.post("/api/opera3/bank-import/archive-statement")
async def opera3_archive_folder_statement(request: Request):
    """Archive a statement PDF after import (Opera 3)."""
    return await archive_folder_statement(request)





@router.post("/api/opera3/bank-import/fetch-emails-to-folder")
async def opera3_fetch_email_statements_to_folder(
    bank_code: str = Query(..., description="Opera bank account code"),
    days_back: int = Query(30, description="Number of days to search back"),
):
    """Fetch email attachments to folder (Opera 3) — delegates to shared implementation."""
    return await fetch_email_statements_to_folder(bank_code=bank_code, days_back=days_back)





@router.get("/api/bank-import/scan-emails")
async def scan_emails_for_bank_statements(
    bank_code: str = Query(..., description="Opera bank account code"),
    days_back: int = Query(30, description="Number of days to search back"),
    include_processed: bool = Query(False, description="Include already-processed emails"),
    validate_balances: bool = Query(True, description="Validate statement balances against Opera (slower but filters invalid)")
):
    """
    Scan inbox for emails with bank statement attachments.

    Returns list of candidate emails with:
    - email_id, subject, from_address, received_at
    - attachments: [{attachment_id, filename, size_bytes}]
    - detected_bank (if identifiable from sender/filename)
    - already_processed flag

    If validate_balances=True (default), PDFs are parsed to check opening balance
    against Opera's reconciled balance, filtering out already-processed statements.
    """
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        from datetime import datetime, timedelta
        import tempfile
        import os

        # Get bank details from Opera (sort code, account number, reconciled balance)
        reconciled_balance = None
        opera_sort_code = None
        opera_account_number = None
        bank_exists = False

        if sql_connector:
            try:
                bank_query = """
                    SELECT nk_recbal / 100.0 as reconciled_balance,
                           RTRIM(nk_sort) as sort_code,
                           RTRIM(nk_number) as account_number
                    FROM nbank WITH (NOLOCK)
                    WHERE nk_acnt = :bank_code
                """
                result = sql_connector.execute_query(bank_query, {'bank_code': bank_code})
                # Result is a DataFrame
                if result is not None and not result.empty:
                    reconciled_balance = float(result.iloc[0]['reconciled_balance']) if result.iloc[0]['reconciled_balance'] is not None else None
                    opera_sort_code = result.iloc[0]['sort_code']
                    opera_account_number = result.iloc[0]['account_number']
                    bank_exists = True
                    logger.info(f"Opera bank {bank_code}: sort={opera_sort_code}, acct={opera_account_number}, reconciled=£{reconciled_balance:,.2f}" if reconciled_balance else f"Opera bank {bank_code}: sort={opera_sort_code}, acct={opera_account_number}")
                else:
                    logger.warning(f"Bank {bank_code} not found in nbank table")
            except Exception as e:
                logger.warning(f"Could not get bank details: {e}")

        # If bank doesn't exist in Opera, return error
        if not bank_exists:
            return {
                "error": f"Bank account '{bank_code}' not found in Opera. Please select a valid bank account.",
                "statements": [],
                "already_processed_count": 0
            }

        # Calculate date range
        from_date = datetime.utcnow() - timedelta(days=days_back)

        # Get emails with attachments in date range
        result = email_storage.get_emails(
            from_date=from_date,
            page=1,
            page_size=500  # Reasonable limit for scanning
        )

        statements_found = []
        already_processed_count = 0
        total_emails_scanned = 0
        total_pdfs_found = 0
        skipped_reasons = []  # Track why statements were filtered out

        # Load reconciled statement keys — skip fully reconciled statements
        try:
            reconciled_keys = email_storage.get_reconciled_statement_keys()
            reconciled_filenames = email_storage.get_reconciled_filenames()
        except Exception:
            reconciled_keys = set()
            reconciled_filenames = set()

        # Build effective reconciled balance: max of Opera nk_recbal and the
        # highest closing balance from all reconciled statements for this bank.
        try:
            _rec_closing = email_storage.get_reconciled_closing_balances()
            _tracked_max = _rec_closing.get(bank_code, 0)
            effective_reconciled_balance = max(reconciled_balance or 0, _tracked_max)
        except Exception:
            effective_reconciled_balance = reconciled_balance

        # Load reconciled opening balances for chain-based completion detection
        try:
            _rec_openings = email_storage.get_reconciled_opening_balances()
            bank_rec_openings = _rec_openings.get(bank_code, set())
        except Exception:
            bank_rec_openings = set()

        for email in result.get('emails', []):
            email_id = email.get('id')
            if not email.get('has_attachments'):
                continue

            total_emails_scanned += 1

            # Get attachments for this email
            email_detail = email_storage.get_email_by_id(email_id)
            if not email_detail:
                continue

            attachments = email_detail.get('attachments', [])
            if not attachments:
                continue

            # Filter to potential bank statement attachments
            statement_attachments = []
            email_from = email.get('from_address', '')
            email_subject = email.get('subject', '')

            for att in attachments:
                filename = att.get('filename', '')
                content_type = att.get('content_type', '')
                attachment_id = att.get('attachment_id', '')

                if is_bank_statement_attachment(filename, content_type, email_from, email_subject):
                    total_pdfs_found += 1

                    # Skip fully reconciled statements — archive from load list
                    if (email_id, attachment_id) in reconciled_keys or filename in reconciled_filenames:
                        already_processed_count += 1
                        skipped_reasons.append(f"Statement {filename}: already reconciled")
                        continue

                    statement_attachments.append({
                        'attachment_id': attachment_id,
                        'filename': filename,
                        'size_bytes': att.get('size_bytes', 0),
                        'content_type': content_type,
                        'already_processed': False
                    })

            if statement_attachments:
                # Detect bank from sender or first attachment filename
                detected_bank = detect_bank_from_email(
                    email.get('from_address', ''),
                    statement_attachments[0]['filename']
                )

                # Extract statement date from filename/subject for ordering
                email_subject = email.get('subject', '')
                first_filename = statement_attachments[0]['filename']
                sort_key, statement_date = extract_statement_number_from_filename(first_filename, email_subject)

                # Add sort key and statement date to each attachment
                for att in statement_attachments:
                    att_sort_key, att_stmt_date = extract_statement_number_from_filename(att['filename'], email_subject)
                    att['sort_key'] = att_sort_key
                    att['statement_date'] = att_stmt_date

                # Validate statements using cache-only lookup (no Gemini calls during scan)
                # Balance is the control — not the tracking DB
                is_valid_statement = True
                statement_opening_balance = None
                validation_status = None

                if validate_balances:
                    from sql_rag.pdf_extraction_cache import PDFExtractionCache, get_extraction_cache
                    from sql_rag.statement_reconcile import StatementInfo
                    scan_cache = get_extraction_cache()

                    for att in statement_attachments:
                        if att['filename'].lower().endswith('.pdf'):
                            try:
                                # Get provider and download attachment
                                provider_id = email_detail.get('provider_id')
                                message_id = email_detail.get('message_id')
                                folder_id = email_detail.get('folder_id', 'INBOX')

                                if provider_id and message_id and provider_id in email_sync_manager.providers:
                                    provider = email_sync_manager.providers[provider_id]

                                    # Get actual folder_id string
                                    if isinstance(folder_id, int):
                                        with email_storage._get_connection() as conn:
                                            cursor = conn.cursor()
                                            cursor.execute("SELECT folder_id FROM email_folders WHERE id = ?", (folder_id,))
                                            row = cursor.fetchone()
                                            if row:
                                                folder_id = row['folder_id']

                                    # Download PDF to check cache (no Gemini call)
                                    result = await provider.download_attachment(message_id, att['attachment_id'], folder_id)

                                    # If download failed, email may have been moved to archive
                                    if not result and folder_id not in ('Archive/Bank Statements', 'Archive/BankStatements'):
                                        for archive_folder in ['Archive/Bank Statements', 'Archive/BankStatements']:
                                            result = await provider.download_attachment(message_id, att['attachment_id'], archive_folder)
                                            if result:
                                                logger.info(f"Found {att.get('filename', '')} in {archive_folder} (moved from {folder_id})")
                                                break

                                    if result:
                                        content_bytes, _, _ = result
                                        pdf_hash = scan_cache.hash_pdf(content_bytes)
                                        cached = scan_cache.get(pdf_hash)

                                        if cached:
                                            # Cache hit — use cached extraction for validation
                                            info_data, _ = cached
                                            logger.info(f"Scan cache HIT for {att['filename']} — validating from cache")

                                            # Parse cached statement info
                                            opening_bal_raw = info_data.get('opening_balance')
                                            closing_bal_raw = info_data.get('closing_balance')

                                            att['period_start'] = info_data.get('period_start')
                                            att['period_end'] = info_data.get('period_end')
                                            att['bank_name'] = info_data.get('bank_name')
                                            att['account_number'] = info_data.get('account_number')
                                            att['sort_code'] = info_data.get('sort_code')
                                            att['closing_balance'] = float(closing_bal_raw) if closing_bal_raw is not None else None

                                            # Check: Statement sort code/account number must match Opera bank
                                            stmt_sort = (info_data.get('sort_code') or '').replace('-', '').replace(' ', '').strip()
                                            stmt_acct = (info_data.get('account_number') or '').replace('-', '').replace(' ', '').strip()
                                            opera_sort = (opera_sort_code or '').replace('-', '').replace(' ', '').strip()
                                            opera_acct = (opera_account_number or '').replace('-', '').replace(' ', '').strip()

                                            account_matches = False
                                            if stmt_sort and stmt_acct and opera_sort and opera_acct:
                                                account_matches = (stmt_sort == opera_sort and stmt_acct == opera_acct)
                                                if not account_matches:
                                                    logger.info(f"Statement account mismatch: statement={stmt_sort}/{stmt_acct}, opera={opera_sort}/{opera_acct}")
                                                    is_valid_statement = False
                                                    validation_status = 'wrong_account'
                                                    skipped_reasons.append(f"Statement {att['filename']}: wrong bank account ({stmt_sort}/{stmt_acct} vs Opera {opera_sort}/{opera_acct})")
                                            elif stmt_acct and opera_acct:
                                                account_matches = (stmt_acct == opera_acct)
                                                if not account_matches:
                                                    logger.info(f"Statement account number mismatch: statement={stmt_acct}, opera={opera_acct}")
                                                    is_valid_statement = False
                                                    validation_status = 'wrong_account'
                                                    skipped_reasons.append(f"Statement {att['filename']}: wrong account number ({stmt_acct} vs Opera {opera_acct})")
                                            else:
                                                account_matches = True

                                            # Store opening balance if available
                                            if opening_bal_raw is not None:
                                                statement_opening_balance = float(opening_bal_raw)
                                                att['opening_balance'] = statement_opening_balance

                                                # Chain check: if this statement's closing matches a
                                                # reconciled statement's opening, the chain moved past it
                                                stmt_closing = float(closing_bal_raw) if closing_bal_raw is not None else None
                                                chain_complete = stmt_closing is not None and round(stmt_closing, 2) in bank_rec_openings

                                                # Balance validation — use effective reconciled balance
                                                # (max of Opera nk_recbal and tracked reconciled closing balances)
                                                eff_bal = effective_reconciled_balance if effective_reconciled_balance is not None else reconciled_balance
                                                if account_matches and (chain_complete or (eff_bal is not None and statement_opening_balance < eff_bal - 0.01)):
                                                    if chain_complete:
                                                        is_valid_statement = False
                                                        validation_status = 'already_processed'
                                                        logger.info(f"Statement filtered out (chain): closing £{stmt_closing:,.2f} matches reconciled opening")
                                                        skipped_reasons.append(f"Statement {att['filename']}: already processed (closing matches reconciled statement's opening)")
                                                    else:
                                                        is_valid_statement = False
                                                        validation_status = 'already_processed'
                                                        logger.info(f"Statement filtered out: opening £{statement_opening_balance:,.2f} < reconciled £{eff_bal:,.2f}")
                                                        skipped_reasons.append(f"Statement {att['filename']}: already processed (opening £{statement_opening_balance:,.2f} < reconciled £{eff_bal:,.2f})")

                                                        try:
                                                            email_storage.record_bank_statement_import(
                                                                bank_code=bank_code,
                                                                filename=att['filename'],
                                                                transactions_imported=0,
                                                                source='email',
                                                                target_system='already_processed',
                                                                email_id=email_id,
                                                                attachment_id=att['attachment_id'],
                                                                total_receipts=0,
                                                                total_payments=0,
                                                                imported_by='AUTO_SKIP_SCAN'
                                                            )
                                                            already_processed_count += 1
                                                        except:
                                                            pass
                                        else:
                                            # Cache miss — skip Gemini during scan, will extract when user selects
                                            logger.info(f"Scan cache MISS for {att['filename']} — balances will be extracted when selected")
                            except Exception as e:
                                logger.warning(f"Could not validate PDF statement info: {e}")
                                pass

                # Only add valid statements
                if is_valid_statement:
                    # Get period dates and bank info from first attachment (if extracted from PDF)
                    first_att = statement_attachments[0] if statement_attachments else {}
                    period_start = first_att.get('period_start')
                    period_end = first_att.get('period_end')
                    bank_name = first_att.get('bank_name')
                    account_number = first_att.get('account_number')
                    closing_balance = first_att.get('closing_balance')

                    statements_found.append({
                        'email_id': email_id,
                        'message_id': email.get('message_id'),
                        'subject': email.get('subject'),
                        'from_address': email.get('from_address'),
                        'from_name': email.get('from_name'),
                        'received_at': email.get('received_at'),
                        'attachments': statement_attachments,
                        'detected_bank': detected_bank,
                        'already_processed': all(a['already_processed'] for a in statement_attachments),
                        'sort_key': sort_key,
                        'statement_date': statement_date,
                        'opening_balance': statement_opening_balance,
                        # Add extracted statement info from PDF
                        'period_start': period_start,
                        'period_end': period_end,
                        'closing_balance': closing_balance,
                        'bank_name': bank_name,
                        'account_number': account_number
                    })

        # Deduplicate: if the same filename appears in multiple emails, keep only the newest
        # Archive the duplicate (older) email to keep inbox clean
        seen_filenames = {}  # filename -> index in statements_found
        deduped_statements = []
        duplicates_archived = 0
        duplicate_emails_to_archive = []  # (email_id, message_id, filename) tuples

        for stmt in statements_found:
            filenames = [a['filename'] for a in stmt.get('attachments', [])]
            is_duplicate = False
            for fn in filenames:
                fn_lower = fn.lower().strip()
                if fn_lower in seen_filenames:
                    # Keep the newer email (later received_at)
                    existing = deduped_statements[seen_filenames[fn_lower]]
                    existing_date = existing.get('received_at', '')
                    new_date = stmt.get('received_at', '')
                    if new_date > existing_date:
                        # Replace older with newer - archive the older one
                        duplicate_emails_to_archive.append((existing['email_id'], existing.get('message_id'), fn))
                        deduped_statements[seen_filenames[fn_lower]] = stmt
                    else:
                        # Current is older - archive it
                        duplicate_emails_to_archive.append((stmt['email_id'], stmt.get('message_id'), fn))
                    is_duplicate = True
                    logger.info(f"Duplicate statement filtered: {fn} (email_id={stmt['email_id']}, keeping email_id={existing['email_id']})")
                    break
            if not is_duplicate:
                for fn in filenames:
                    seen_filenames[fn.lower().strip()] = len(deduped_statements)
                deduped_statements.append(stmt)

        if len(deduped_statements) < len(statements_found):
            logger.info(f"Deduplicated {len(statements_found) - len(deduped_statements)} duplicate statement(s)")

        # Archive duplicate emails to keep inbox clean
        for dup_email_id, dup_message_id, dup_filename in duplicate_emails_to_archive:
            if dup_message_id and email_sync_manager:
                try:
                    # Find the provider for this email
                    email_detail = email_storage.get_email_by_id(dup_email_id)
                    if email_detail:
                        provider_id = email_detail.get('provider_id')
                        if provider_id and provider_id in email_sync_manager.providers:
                            provider = email_sync_manager.providers[provider_id]
                            moved = await provider.move_email(dup_message_id, 'INBOX', 'Archive/Bank Statements')
                            if moved:
                                duplicates_archived += 1
                                logger.info(f"Archived duplicate statement email: {dup_filename} (email_id={dup_email_id})")
                            else:
                                logger.warning(f"Failed to archive duplicate email_id={dup_email_id}")
                except Exception as archive_err:
                    logger.warning(f"Could not archive duplicate email: {archive_err}")

        statements_found = deduped_statements

        # Sort statements in sequential import order by chaining
        # opening/closing balances starting from reconciled balance
        if reconciled_balance is not None and len(statements_found) > 1:
            ordered = []
            remaining = list(statements_found)
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
                    remaining.sort(key=lambda s: (0 if s.get('opening_balance') is not None else 1, s.get('opening_balance') or 0, s.get('sort_key', (9999,))))
                    ordered.extend(remaining)
                    break
            statements_found = ordered
        else:
            statements_found.sort(key=lambda s: (0 if s.get('opening_balance') is not None else 1, s.get('opening_balance') or 0, s.get('sort_key', (9999,))))

        # Add sequence numbers and detect missing statements
        expected_opening = reconciled_balance if reconciled_balance else None
        missing_statements = []

        for i, stmt in enumerate(statements_found, start=1):
            stmt['import_sequence'] = i
            # Remove sort_key from response (internal use only)
            del stmt['sort_key']
            for att in stmt['attachments']:
                if 'sort_key' in att:
                    del att['sort_key']

            # Check for gaps in the sequence
            opening = stmt.get('opening_balance')
            closing = None
            for att in stmt['attachments']:
                if att.get('closing_balance'):
                    closing = att['closing_balance']
                    break

            if expected_opening is not None and opening is not None:
                # Allow small tolerance for rounding
                if abs(opening - expected_opening) > 0.02:
                    stmt['has_gap'] = True
                    stmt['expected_opening'] = expected_opening
                    missing_statements.append({
                        'position': i,
                        'expected_opening': expected_opening,
                        'actual_opening': opening,
                        'gap_amount': opening - expected_opening
                    })
                else:
                    stmt['has_gap'] = False

            # Next statement should open with this one's closing
            if closing is not None:
                expected_opening = closing

        # Build response message
        message = None
        if len(statements_found) == 0:
            # No relevant statements found - give detailed feedback
            balance_info = f" Expected next statement opening balance: £{reconciled_balance:,.2f}." if reconciled_balance is not None else ""
            pdfs_info = f" Found {total_pdfs_found} PDF attachment(s) in {total_emails_scanned} email(s) with attachments." if total_pdfs_found > 0 else f" No PDF attachments found in {total_emails_scanned} email(s) scanned."
            processed_info = f" {already_processed_count} statement(s) already processed." if already_processed_count > 0 else ""
            dup_info = f" {duplicates_archived} duplicate(s) archived." if duplicates_archived > 0 else ""
            message = f"Scan complete — no new statements found for import.{balance_info}{pdfs_info}{processed_info}{dup_info}"
        elif len(statements_found) == 1:
            dup_info = f" {duplicates_archived} duplicate(s) archived." if duplicates_archived > 0 else ""
            message = f"Found 1 statement ready for import.{dup_info}"
        else:
            dup_info = f" {duplicates_archived} duplicate(s) archived." if duplicates_archived > 0 else ""
            message = f"Found {len(statements_found)} statement(s). Import in sequence order (1, 2, 3...) to maintain balance chain.{dup_info}"
        if missing_statements:
            gaps_msg = f" WARNING: {len(missing_statements)} missing statement(s) detected in sequence."
            message = (message or "") + gaps_msg

        # Annotate statements with draft (in-progress) status
        if email_storage:
            try:
                draft_keys = email_storage.get_draft_statement_keys(bank_code)
                draft_lookup = {}
                for dk in draft_keys:
                    key = (str(dk.get('email_id') or ''), str(dk.get('attachment_id') or ''))
                    draft_lookup[key] = dk['updated_at']
                for stmt in statements_found:
                    for att in stmt.get('attachments', []):
                        key = (str(stmt.get('email_id', '')), str(att.get('attachment_id', '')))
                        if key in draft_lookup:
                            att['has_draft'] = True
                            att['draft_updated_at'] = draft_lookup[key]
                        else:
                            att['has_draft'] = False
            except Exception as e:
                logger.debug(f"Could not annotate drafts for scan-emails: {e}")

        return {
            "success": True,
            "statements_found": statements_found,
            "total_found": len(statements_found),
            "already_processed_count": already_processed_count,
            "duplicates_archived": duplicates_archived,
            "total_emails_scanned": total_emails_scanned,
            "total_pdfs_found": total_pdfs_found,
            "days_searched": days_back,
            "bank_code": bank_code,
            "reconciled_balance": reconciled_balance,
            "missing_statements": missing_statements if missing_statements else None,
            "has_missing_statements": len(missing_statements) > 0,
            "skipped_reasons": skipped_reasons if skipped_reasons else None,
            "message": message
        }

    except Exception as e:
        logger.error(f"Error scanning emails for bank statements: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/bank-import/scan-all-banks")
async def scan_all_banks_for_statements(
    days_back: int = Query(30, description="Number of days to search back"),
    include_processed: bool = Query(False, description="Include already-processed emails"),
    validate_balances: bool = Query(True, description="Validate statement balances against Opera")
):
    """
    Scan inbox for bank statement attachments across ALL Opera bank accounts.

    Groups results by bank, validates each statement against its bank's reconciled balance,
    and returns a dashboard-ready response. Also scans PDF files from local folders.

    Returns:
        banks: dict keyed by bank_code with statements list per bank
        unidentified: statements that couldn't be matched to a bank
        total_statements: count across all banks
    """
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        from datetime import datetime, timedelta
        from pathlib import Path

        import time as _time
        _t0 = _time.time()
        _timings = {}

        # --- Step 0: Sync mailbox to get latest emails (skip if synced recently) ---
        sync_result = None
        if email_sync_manager:
            try:
                # Ensure sync manager uses the current company's storage
                if email_sync_manager.storage is not email_storage:
                    email_sync_manager.storage = email_storage
                    logger.info("Updated email_sync_manager.storage to match current company")

                # Check if we synced recently — skip if within last 5 minutes
                sync_status = email_sync_manager.get_sync_status()
                recent_sync = False
                for prov in sync_status.get('providers', []):
                    last_sync = prov.get('last_sync')
                    if last_sync:
                        try:
                            last_dt = datetime.fromisoformat(last_sync.replace('Z', '+00:00')) if isinstance(last_sync, str) else last_sync
                            if (datetime.now(last_dt.tzinfo) if last_dt.tzinfo else datetime.utcnow()) - last_dt.replace(tzinfo=None) < timedelta(minutes=5):
                                recent_sync = True
                        except Exception:
                            pass

                if recent_sync:
                    sync_result = {'skipped': True, 'reason': 'synced_recently'}
                    logger.info("Mailbox synced within last 5 minutes — skipping sync")
                else:
                    sync_result = await email_sync_manager.sync_all_providers()
                    logger.info(f"Mailbox sync completed before scan: {sync_result}")
            except Exception as sync_err:
                logger.warning(f"Mailbox sync failed (continuing with cached emails): {sync_err}")

        _timings['sync'] = round(_time.time() - _t0, 1)
        _t1 = _time.time()

        # --- Step 1: Load ALL bank accounts from nbank ---
        all_banks = {}  # bank_code -> bank info
        bank_lookup = {}  # (norm_sort, norm_acct) -> bank_code

        if sql_connector:
            try:
                banks_df = sql_connector.execute_query("""
                    SELECT RTRIM(nk_acnt) as code,
                           RTRIM(nk_desc) as description,
                           RTRIM(nk_sort) as sort_code,
                           RTRIM(nk_number) as account_number,
                           nk_recbal / 100.0 as reconciled_balance,
                           nk_curbal / 100.0 as current_balance,
                           CASE WHEN nk_petty = 1 THEN 'Petty Cash' ELSE 'Bank Account' END as type
                    FROM nbank WITH (NOLOCK)
                    ORDER BY nk_acnt
                """)
                if banks_df is not None and not banks_df.empty:
                    for _, row in banks_df.iterrows():
                        code = row['code'].strip() if row['code'] else ''
                        if not code:
                            continue
                        sort_code = row['sort_code'].strip() if row['sort_code'] else ''
                        account_number = row['account_number'].strip() if row['account_number'] else ''
                        rec_bal = float(row['reconciled_balance']) if row['reconciled_balance'] is not None else None

                        all_banks[code] = {
                            'bank_code': code,
                            'description': row['description'] or code,
                            'sort_code': sort_code,
                            'account_number': account_number,
                            'reconciled_balance': rec_bal,
                            'current_balance': float(row['current_balance']) if row['current_balance'] is not None else None,
                            'type': row['type'],
                            'statements': [],
                            'statement_count': 0
                        }

                        # Build lookup by normalized sort+acct
                        norm_sort = sort_code.replace('-', '').replace(' ', '').strip()
                        norm_acct = account_number.replace('-', '').replace(' ', '').strip()
                        if norm_sort and norm_acct:
                            bank_lookup[(norm_sort, norm_acct)] = code

                    logger.info(f"Scan-all-banks: loaded {len(all_banks)} bank accounts, {len(bank_lookup)} with sort/acct for matching")
            except Exception as e:
                logger.warning(f"Could not load bank accounts: {e}")
                return {"success": False, "error": friendly_db_error(e)}

        if not all_banks:
            return {"success": False, "error": "No bank accounts found in Opera"}

        # --- Step 2: Single email fetch (only emails with attachments) ---
        from_date = datetime.utcnow() - timedelta(days=days_back)
        emails_with_atts = email_storage.get_emails_with_attachments(from_date=from_date, page_size=500)

        # --- Load all statement tracking data in a single consolidated SQLite query ---
        try:
            logger.info(f"Scan: loading tracking data from {email_storage.db_path}")
            _tracking = email_storage.get_all_statement_tracking_data()
            logger.info(f"Scan: got {len(_tracking.get('managed_keys',set()))} managed_keys, {len(_tracking.get('managed_filenames',set()))} managed_filenames")
            reconciled_keys = _tracking['reconciled_keys']
            reconciled_filenames = _tracking['reconciled_filenames']
            imported_nr_keys = _tracking['imported_nr_keys']
            imported_nr_filenames = _tracking['imported_nr_filenames']
            reconciled_closing_balances = _tracking['reconciled_closing_balances']
            reconciled_opening_balances = _tracking['reconciled_opening_balances']
            managed_keys = _tracking['managed_keys']
            managed_filenames = _tracking['managed_filenames']
            cached_stmt_info = _tracking['cached_stmt_info']
            imported_hashes = _tracking['imported_hashes']
            imported_identities = _tracking['imported_identities']
            logger.info(f"Loaded all statement tracking data in single query: "
                        f"{len(reconciled_keys)} reconciled, {len(imported_nr_keys)} imported-nr, "
                        f"{len(managed_keys)} managed, {len(cached_stmt_info)} cached info, "
                        f"{len(imported_hashes)} hashes, {len(imported_identities)} identities")
        except Exception:
            reconciled_keys = set()
            reconciled_filenames = set()
            imported_nr_keys = set()
            imported_nr_filenames = set()
            reconciled_closing_balances = {}
            reconciled_opening_balances = {}
            managed_keys = set()
            managed_filenames = set()
            cached_stmt_info = {}
            imported_hashes = {}
            imported_identities = set()

        unidentified = []
        non_current = {
            'already_processed': [],
            'old_statements': [],
            'not_classified': [],
            'advanced': []
        }
        total_emails_scanned = 0
        total_pdfs_found = 0

        from sql_rag.pdf_extraction_cache import get_extraction_cache
        scan_cache = get_extraction_cache()

        # Build detected_bank_name → bank_code lookup from previous imports
        # This allows Tide/fintech matching even with generic filenames like "attachment.pdf"
        detected_name_to_bank = {}  # {'tide': 'BC020', 'monzo': 'BC030', ...}
        for fn, info in cached_stmt_info.items():
            stmt_sort = (info.get('sort_code') or '').replace('-', '').replace(' ', '').strip()
            stmt_acct = (info.get('account_number') or '').replace('-', '').replace(' ', '').strip()
            if stmt_sort and stmt_acct:
                bcode = bank_lookup.get((stmt_sort, stmt_acct))
                if bcode and info.get('bank_code'):
                    detected_name_to_bank[info['bank_code'].lower().strip()] = bcode
        if detected_name_to_bank:
            logger.info(f"Scan-all-banks: built detected name→bank mapping: {detected_name_to_bank}")

        seen_hashes = {}  # Track hashes within this scan: {hash: first_filename}
        seen_identities = {}  # Track statement identities: {(sort,acct,opening,closing): first_filename}
        duplicates_archived = 0

        # Per-bank dedup: track filenames and statement periods to filter duplicates
        # Key: (bank_code, filename_lower) -> stmt_entry with newest received_at
        seen_bank_filenames = {}  # {(bank_code, filename_lower): (received_at, stmt_entry)}
        # Key: (bank_code, period_key) -> stmt_entry with newest received_at
        # period_key extracted from filename patterns like Monzo_bank_statement_YYYY-MM-DD-YYYY-MM-DD_XXXX.pdf
        seen_bank_periods = {}  # {(bank_code, period_key): (received_at, stmt_entry)}
        # Key: (bank_code, start_date) -> (end_date, stmt_entry) — for overlapping period dedup
        # e.g. partial Feb (02-01 to 02-19) superseded by full Feb (02-01 to 02-28)
        seen_bank_starts = {}  # {(bank_code, start_date): (end_date, stmt_entry)}

        def _extract_statement_period(fn: str) -> str:
            """Extract statement period from filename for dedup. Returns period key or empty string."""
            import re as _re_period
            # Monzo: Monzo_bank_statement_2026-01-01-2026-01-31_4539.pdf
            m = _re_period.search(r'(\d{4}-\d{2}-\d{2})[_-](\d{4}-\d{2}-\d{2})', fn)
            if m:
                return f"{m.group(1)}_{m.group(2)}"
            # Barclays: Statement DD-MMM-YY AC XXXXXXXX XXXXXXXX.pdf
            m = _re_period.search(r'statement\s+(\d{1,2}-\w{3}-\d{2})\s+ac\s+(\d{8})', fn.lower())
            if m:
                return f"{m.group(1)}_{m.group(2)}"
            return ''

        def _extract_period_dates(fn: str):
            """Extract (start_date, end_date) from filename. Returns (str, str) or (None, None)."""
            import re as _re_dates
            m = _re_dates.search(r'(\d{4}-\d{2}-\d{2})[_-](\d{4}-\d{2}-\d{2})', fn)
            if m:
                return m.group(1), m.group(2)
            return None, None

        logger.info(f"Scan: email_storage.db_path = {getattr(email_storage, 'db_path', 'UNKNOWN')}")
        _timings['load_banks_and_lookups'] = round(_time.time() - _t1, 1)

        # --- Step 2b: Resolve base folder and ensure bank subfolders exist ---
        settings = _load_company_settings()
        base_path = Path(settings.get("bank_statements_base_folder", ""))
        emails_saved_to_folders = 0

        if base_path and base_path.exists():
            for bank_code_iter, bank_info in all_banks.items():
                try:
                    subfolder = _get_bank_subfolder_name(bank_code_iter, bank_info.get('description', ''))
                    bank_folder_path = base_path / subfolder
                    bank_folder_path.mkdir(parents=True, exist_ok=True)
                except Exception:
                    pass

        _folder_hashes = {}  # folder_path -> set of SHA256 hashes (for content dedup across filenames)

        _t2 = _time.time()

        # --- Step 3: Scan emails — save PDFs to bank subfolders (or build email entries if no folder configured) ---
        for email in emails_with_atts:
            email_id = email.get('id')
            if not email.get('has_attachments'):
                continue

            total_emails_scanned += 1
            attachments = email.get('attachments', [])
            if not attachments:
                continue

            email_from = email.get('from_address', '')
            email_subject = email.get('subject', '')

            for att in attachments:
                filename = att.get('filename', '')
                content_type = att.get('content_type', '')
                attachment_id = att.get('attachment_id', '')
                content_bytes = None  # Track downloaded bytes for save-to-folder

                if not is_bank_statement_attachment(filename, content_type, email_from, email_subject):
                    continue

                total_pdfs_found += 1

                # Skip managed (archived/deleted/retained) — these are explicitly dismissed
                # Check exact filename AND base filename (without counter suffix like _37, _38)
                # because saved PDFs may have been renamed to avoid conflicts
                import re as _mf_re
                _base_fn = _mf_re.sub(r'_\d+(\.\w+)$', r'\1', filename)  # "file_37.pdf" -> "file.pdf"
                _base = filename.rsplit('.', 1)[0]
                _is_managed = (
                    (email_id, attachment_id) in managed_keys
                    or filename in managed_filenames
                    or any(mf.startswith(_base) for mf in managed_filenames)
                )
                if _is_managed:
                    logger.info(f"Scan: skipping managed email {filename}")
                    continue

                # Skip fully reconciled statements — archive from load list
                if (email_id, attachment_id) in reconciled_keys or filename in reconciled_filenames:
                    continue

                is_imported_not_reconciled = (email_id, attachment_id) in imported_nr_keys
                detected_bank_name = detect_bank_from_email(email_from, filename, email_subject)
                sort_key, statement_date = extract_statement_number_from_filename(filename, email_subject)

                stmt_entry = {
                    'email_id': email_id,
                    'attachment_id': attachment_id,
                    'filename': filename,
                    'source': 'email',
                    'subject': email_subject,
                    'from_address': email_from,
                    'received_at': email.get('received_at'),
                    'detected_bank_name': detected_bank_name,
                    'already_processed': False,
                    'is_imported': is_imported_not_reconciled,
                    'status': 'imported' if is_imported_not_reconciled else 'pending',
                    'sort_key': sort_key,
                    'statement_date': statement_date
                }

                # Try cache-based validation — balance is the control, not tracking DB
                matched_bank_code = None

                # FAST PATH: Use cached statement info from previous imports (no IMAP download needed)
                cached_info = cached_stmt_info.get(filename)
                if cached_info and validate_balances:
                    logger.info(f"Scan fast-path: using cached info for {filename}")
                    opening = cached_info.get('opening_balance')
                    closing = cached_info.get('closing_balance')
                    stmt_entry['opening_balance'] = opening
                    stmt_entry['closing_balance'] = closing
                    stmt_entry['period_start'] = cached_info.get('period_start')
                    stmt_entry['period_end'] = cached_info.get('period_end')
                    stmt_entry['sort_code'] = cached_info.get('sort_code')
                    stmt_entry['account_number'] = cached_info.get('account_number')
                    stmt_entry['bank_name'] = cached_info.get('bank_code')

                    # Match to Opera bank
                    stmt_sort = (cached_info.get('sort_code') or '').replace('-', '').replace(' ', '').strip()
                    stmt_acct = (cached_info.get('account_number') or '').replace('-', '').replace(' ', '').strip()
                    if stmt_sort and stmt_acct:
                        lookup_key = (stmt_sort, stmt_acct)
                        matched_bank_code = bank_lookup.get(lookup_key)

                    # Fallback: detected bank name from import history (Tide/fintech with generic filenames)
                    if not matched_bank_code and detected_bank_name:
                        matched_bank_code = detected_name_to_bank.get(detected_bank_name.lower())
                        if matched_bank_code:
                            logger.info(f"Cached fast-path: matched '{filename}' to {matched_bank_code} via detected bank name '{detected_bank_name}'")

                    # Also try description matching for cached statements without sort/acct
                    if not matched_bank_code and detected_bank_name:
                        detected_lower = detected_bank_name.lower()
                        for bcode, binfo in all_banks.items():
                            desc_lower = (binfo.get('description') or '').lower()
                            if detected_lower in desc_lower or desc_lower in detected_lower:
                                matched_bank_code = bcode
                                break

                    # Don't pre-filter by balance — let the chain sort handle ordering
                    if matched_bank_code:
                        stmt_entry['status'] = 'ready'

                elif validate_balances and filename.lower().endswith('.pdf'):
                    # No cached import data — try metadata matching first, then extract if needed
                    import tempfile
                    import os as _os
                    stmt_entry['status'] = 'ready'

                    # Try metadata-based bank matching BEFORE downloading PDF
                    if detected_bank_name:
                        detected_lower = detected_bank_name.lower()
                        for bcode, binfo in all_banks.items():
                            desc_lower = (binfo.get('description') or '').lower()
                            if detected_lower in desc_lower or desc_lower in detected_lower:
                                matched_bank_code = bcode
                                break
                    if not matched_bank_code and detected_bank_name:
                        matched_bank_code = detected_name_to_bank.get(detected_bank_name.lower())
                    if not matched_bank_code:
                        import re as _re
                        acct_matches = _re.findall(r'\b(\d{8})\b', filename)
                        for acct_num in acct_matches:
                            for bcode, binfo in all_banks.items():
                                opera_acct = (binfo.get('account_number') or '').replace('-', '').replace(' ', '').strip()
                                if opera_acct and acct_num == opera_acct:
                                    matched_bank_code = bcode
                                    break
                            if matched_bank_code:
                                break
                    if not matched_bank_code:
                        match_sources = [(email_from or '').lower(), filename.lower(), (email_subject or '').lower()]
                        for bcode, binfo in all_banks.items():
                            desc = (binfo.get('description') or '').lower()
                            desc_words = [w for w in desc.split() if len(w) >= 4 and w not in ('bank', 'account', 'current', 'the', 'and', 'for', 'with')]
                            for word in desc_words:
                                for source in match_sources:
                                    if word in source:
                                        matched_bank_code = bcode
                                        break
                                if matched_bank_code:
                                    break
                            if matched_bank_code:
                                break

                    # Download PDF and check extraction cache (NO Gemini calls during scan)
                    pdf_extracted = False
                    if not matched_bank_code:
                        logger.info(f"Scan-all: skipping {filename} — no Opera bank match from metadata")
                    else:
                      try:
                        logger.info(f"Scan-all: attempting PDF download for {filename} (email_id={email_id}, bank={matched_bank_code})")
                        email_detail = email_storage.get_email_by_id(email_id)
                        if email_detail and email_sync_manager:
                            provider_id = email_detail.get('provider_id')
                            message_id_imap = email_detail.get('message_id')
                            folder_id = email_detail.get('folder_id', 'INBOX')

                            if provider_id and message_id_imap and provider_id in email_sync_manager.providers:
                                provider = email_sync_manager.providers[provider_id]

                                if isinstance(folder_id, int):
                                    with email_storage._get_connection() as conn:
                                        cursor = conn.cursor()
                                        cursor.execute("SELECT folder_id FROM email_folders WHERE id = ?", (folder_id,))
                                        row = cursor.fetchone()
                                        if row:
                                            folder_id = row['folder_id']

                                dl_result = await provider.download_attachment(message_id_imap, attachment_id, folder_id)
                                if not dl_result and folder_id not in ('Archive/Bank Statements', 'Archive/BankStatements'):
                                    for archive_folder in ['Archive/Bank Statements', 'Archive/BankStatements']:
                                        dl_result = await provider.download_attachment(message_id_imap, attachment_id, archive_folder)
                                        if dl_result:
                                            break

                                if dl_result:
                                    content_bytes, _, _ = dl_result
                                    pdf_hash = scan_cache.hash_pdf(content_bytes)
                                    cached = scan_cache.get(pdf_hash)

                                    if cached:
                                        info_data, _ = cached
                                        logger.info(f"Scan-all: cache HIT for {filename}")
                                        opening = float(info_data.get('opening_balance')) if info_data.get('opening_balance') is not None else None
                                        closing = float(info_data.get('closing_balance')) if info_data.get('closing_balance') is not None else None
                                        stmt_entry['opening_balance'] = opening
                                        stmt_entry['closing_balance'] = closing
                                        stmt_entry['period_start'] = info_data.get('period_start')
                                        stmt_entry['period_end'] = info_data.get('period_end')
                                        stmt_entry['bank_name'] = info_data.get('bank_name')
                                        stmt_entry['account_number'] = info_data.get('account_number')
                                        stmt_entry['sort_code'] = info_data.get('sort_code')

                                        stmt_sort = (info_data.get('sort_code') or '').replace('-', '').replace(' ', '').strip()
                                        stmt_acct = (info_data.get('account_number') or '').replace('-', '').replace(' ', '').strip()
                                        if stmt_sort and stmt_acct:
                                            matched_bank_code = bank_lookup.get((stmt_sort, stmt_acct)) or matched_bank_code

                                        if matched_bank_code:
                                            # Chain check: if closing balance matches a reconciled opening,
                                            # this statement has already been processed
                                            bank_rec_opens = reconciled_opening_balances.get(matched_bank_code, set())
                                            chain_complete = closing is not None and round(closing, 2) in bank_rec_opens
                                            if chain_complete:
                                                stmt_entry['category'] = 'already_processed'
                                                stmt_entry['status'] = 'already_processed'
                                                logger.info(f"Scan-all: filtered {filename} — chain complete (closing £{closing:,.2f} matches reconciled opening)")
                                            else:
                                                stmt_entry['status'] = 'ready'

                                        pdf_extracted = True
                                    else:
                                        # Cache miss — lightweight AI extraction for balances only
                                        logger.info(f"Scan-all: cache MISS for {filename} — extracting statement info")
                                        try:
                                            import tempfile
                                            with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
                                                tmp.write(content_bytes)
                                                tmp_path = tmp.name
                                            try:
                                                from sql_rag.statement_reconcile import StatementReconciler
                                                reconciler = StatementReconciler(sql_connector, config=config)
                                                info_data = reconciler.extract_statement_info_only(tmp_path)
                                                if info_data:
                                                    opening = float(info_data.get('opening_balance')) if info_data.get('opening_balance') is not None else None
                                                    closing = float(info_data.get('closing_balance')) if info_data.get('closing_balance') is not None else None
                                                    stmt_entry['opening_balance'] = opening
                                                    stmt_entry['closing_balance'] = closing
                                                    stmt_entry['period_start'] = info_data.get('period_start')
                                                    stmt_entry['period_end'] = info_data.get('period_end')
                                                    stmt_entry['bank_name'] = info_data.get('bank_name')
                                                    stmt_entry['account_number'] = info_data.get('account_number')
                                                    stmt_entry['sort_code'] = info_data.get('sort_code')

                                                    stmt_sort = (info_data.get('sort_code') or '').replace('-', '').replace(' ', '').strip()
                                                    stmt_acct = (info_data.get('account_number') or '').replace('-', '').replace(' ', '').strip()
                                                    if stmt_sort and stmt_acct:
                                                        matched_bank_code = bank_lookup.get((stmt_sort, stmt_acct)) or matched_bank_code

                                                    pdf_extracted = True
                                                    logger.info(f"Scan-all: extracted {filename} — open={opening} close={closing}")
                                            finally:
                                                import os as _os2
                                                try:
                                                    _os2.unlink(tmp_path)
                                                except Exception:
                                                    pass
                                        except Exception as ext_err:
                                            logger.warning(f"Scan-all: extraction failed for {filename}: {ext_err}")
                      except Exception as dl_err:
                        logger.warning(f"Scan-all: could not download {filename}: {dl_err}")

                    logger.info(f"Scan-all: {filename} bank={matched_bank_code or 'unknown'}, extracted={pdf_extracted}")

                # --- Save email PDF to bank subfolder (unified flow) ---
                logger.info(f"Save section: {filename} matched_bank={matched_bank_code} base_path={base_path} content_bytes={'YES' if content_bytes else 'NO'}")
                # If folders are configured, save the PDF to the matched bank's subfolder
                # so the folder scan (Step 4) handles everything through one code path.
                if matched_bank_code and base_path and base_path.exists():
                    bank_info_ref = all_banks[matched_bank_code]
                    subfolder = _get_bank_subfolder_name(matched_bank_code, bank_info_ref.get('description', ''))
                    bank_folder = base_path / subfolder
                    bank_folder.mkdir(parents=True, exist_ok=True)

                    dest_file = bank_folder / filename
                    saved_or_exists = False

                    # Build hash index of ALL existing PDFs in this bank folder (for content dedup)
                    # Cached per-folder so we only scan once per bank per scan cycle
                    folder_key = str(bank_folder)
                    if folder_key not in _folder_hashes:
                        import hashlib as _hl
                        _folder_hashes[folder_key] = set()
                        for existing_file in bank_folder.iterdir():
                            if existing_file.is_file() and existing_file.suffix.lower() == '.pdf':
                                try:
                                    _folder_hashes[folder_key].add(_hl.sha256(existing_file.read_bytes()).hexdigest())
                                except Exception:
                                    pass

                    logger.info(f"Save check: {filename} content={'YES' if content_bytes else 'NO'} dest_exists={dest_file.exists()} folder={bank_folder}")
                    if content_bytes is not None:
                        import hashlib as _hl
                        new_hash = _hl.sha256(content_bytes).hexdigest()
                        if new_hash in _folder_hashes.get(folder_key, set()):
                            # Content already exists in this folder (possibly under a different name)
                            saved_or_exists = True
                        elif dest_file.exists():
                            # Same filename but different content — rename with counter
                            stem = Path(filename).stem
                            suffix = Path(filename).suffix
                            counter = 1
                            while (bank_folder / f"{stem}_{counter}{suffix}").exists():
                                counter += 1
                            renamed = f"{stem}_{counter}{suffix}"
                            with open(bank_folder / renamed, 'wb') as f:
                                f.write(content_bytes)
                            _folder_hashes[folder_key].add(new_hash)
                            emails_saved_to_folders += 1
                            saved_or_exists = True
                            logger.info(f"Saved email PDF as renamed '{renamed}' to {bank_folder} (original '{filename}' had different content)")
                        else:
                            # New file — save to folder
                            with open(dest_file, 'wb') as f:
                                f.write(content_bytes)
                            _folder_hashes.setdefault(folder_key, set()).add(new_hash)
                            emails_saved_to_folders += 1
                            saved_or_exists = True
                            logger.info(f"Saved email PDF '{filename}' to {bank_folder}")
                    elif dest_file.exists():
                        saved_or_exists = True  # No bytes to compare, file with same name exists
                    else:
                        # File not on disk yet — download from IMAP and save to folder
                        logger.info(f"Email PDF '{filename}' not on disk at {dest_file} — downloading")
                        if content_bytes is None:
                            try:
                                # Use direct IMAP connection for reliable download
                                email_detail_dl = email_storage.get_email_by_id(email_id)
                                if email_detail_dl:
                                    provider_id_dl = email_detail_dl.get('provider_id')
                                    message_id_dl = email_detail_dl.get('message_id')
                                    if provider_id_dl and message_id_dl and email_sync_manager and provider_id_dl in email_sync_manager.providers:
                                        provider_dl = email_sync_manager.providers[provider_id_dl]
                                        # Ensure fresh IMAP connection
                                        if not provider_dl.is_authenticated:
                                            await provider_dl.authenticate()
                                        # Resolve folder_id from DB integer to IMAP name
                                        folder_id_dl = email_detail_dl.get('folder_id', 'INBOX')
                                        if isinstance(folder_id_dl, int):
                                            with email_storage._get_connection() as _fconn:
                                                _frow = _fconn.cursor().execute("SELECT folder_id FROM email_folders WHERE id = ?", (folder_id_dl,)).fetchone()
                                                if _frow:
                                                    folder_id_dl = _frow['folder_id']
                                        # Download attachment
                                        dl_result = await provider_dl.download_attachment(message_id_dl, attachment_id, folder_id_dl)
                                        if not dl_result:
                                            # Try archive folders
                                            for af in ['Archive/Bank Statements', 'Archive/BankStatements', 'INBOX']:
                                                if af == folder_id_dl:
                                                    continue
                                                dl_result = await provider_dl.download_attachment(message_id_dl, attachment_id, af)
                                                if dl_result:
                                                    break
                                        if dl_result:
                                            content_bytes = dl_result[0]
                                            logger.info(f"Downloaded {filename}: {len(content_bytes)} bytes")
                                        else:
                                            logger.warning(f"IMAP download returned None for {filename} (email_id={email_id})")
                            except Exception as dl_err:
                                logger.warning(f"Could not download email PDF '{filename}': {dl_err}")

                        if content_bytes is not None:
                            import hashlib as _hl
                            new_hash = _hl.sha256(content_bytes).hexdigest()
                            if new_hash in _folder_hashes.get(folder_key, set()):
                                saved_or_exists = True  # Already in folder under different name
                            else:
                                # Check if dest_file appeared (race) or needs rename
                                save_path = dest_file
                                if save_path.exists():
                                    stem = Path(filename).stem
                                    suffix = Path(filename).suffix
                                    counter = 1
                                    while (bank_folder / f"{stem}_{counter}{suffix}").exists():
                                        counter += 1
                                    save_path = bank_folder / f"{stem}_{counter}{suffix}"
                                with open(save_path, 'wb') as f:
                                    f.write(content_bytes)
                                _folder_hashes.setdefault(folder_key, set()).add(new_hash)
                                emails_saved_to_folders += 1
                                saved_or_exists = True
                                logger.info(f"Saved email PDF '{save_path.name}' to {bank_folder}")

                    if saved_or_exists:
                        # Verify the file actually exists in the folder before skipping
                        if dest_file.exists() or any(
                            f.name == filename for f in bank_folder.iterdir() if f.is_file()
                        ):
                            continue  # Folder scan (Step 4) will pick this up
                        else:
                            logger.warning(f"Email PDF '{filename}' reported saved but not found in {bank_folder} — keeping as email entry")
                    # else: download failed or file not in folder — fall through to legacy email flow

                # --- Legacy fallback: build email stmt_entry when no folder configured or download failed ---
                cat = stmt_entry.get('category')
                if matched_bank_code and cat in ('already_processed', 'old_statement'):
                    stmt_entry['matched_bank_code'] = matched_bank_code
                    stmt_entry['matched_bank_description'] = all_banks[matched_bank_code]['description']
                    stmt_entry['matched_sort_code'] = all_banks[matched_bank_code]['sort_code']
                    stmt_entry['matched_account_number'] = all_banks[matched_bank_code]['account_number']
                    nc_key = 'old_statements' if cat == 'old_statement' else 'already_processed'
                    non_current[nc_key].append(stmt_entry)

                    if email_storage and filename:
                        try:
                            email_storage.mark_statement_reconciled(
                                filename=filename,
                                bank_code=matched_bank_code,
                                reconciled_count=0
                            )
                        except Exception:
                            pass
                elif matched_bank_code and cat == 'advanced':
                    stmt_entry['matched_bank_code'] = matched_bank_code
                    stmt_entry['matched_bank_description'] = all_banks[matched_bank_code]['description']
                    stmt_entry['matched_sort_code'] = all_banks[matched_bank_code]['sort_code']
                    stmt_entry['matched_account_number'] = all_banks[matched_bank_code]['account_number']
                    non_current['advanced'].append(stmt_entry)
                elif matched_bank_code:
                    stmt_entry['matched_bank_code'] = matched_bank_code
                    stmt_entry['matched_bank_description'] = all_banks[matched_bank_code]['description']
                    stmt_entry['matched_sort_code'] = all_banks[matched_bank_code]['sort_code']
                    stmt_entry['matched_account_number'] = all_banks[matched_bank_code]['account_number']

                    if stmt_entry.get('status') in ('ready', 'imported'):
                        all_banks[matched_bank_code]['statements'].append(stmt_entry)
                    else:
                        logger.info(f"Skipping {filename} for {matched_bank_code}: status={stmt_entry.get('status')}")
                else:
                    logger.info(f"Skipping {filename}: no matching Opera bank in current company")

        _timings['scan_emails'] = round(_time.time() - _t2, 1)
        _t3 = _time.time()

        # --- Step 4: Scan local PDF folders (includes email PDFs saved in Step 3) ---
        # base_path and subfolders were set up in Step 2b

        # Scan all subdirectories under the base folder
        scan_folders = []
        if base_path and base_path.exists():
            for child in sorted(base_path.iterdir()):
                if child.is_dir() and child.name != 'archive':
                    scan_folders.append(child)

        for folder in scan_folders:
            folder_name = folder.name
            if not folder.exists():
                continue

            for file_path in folder.iterdir():
                if not file_path.is_file() or file_path.suffix.lower() != '.pdf':
                    continue

                filename = file_path.name
                status_info = email_storage.get_statement_status(filename)

                file_is_imported_nr = filename in imported_nr_filenames
                # Skip managed (archived/deleted/retained) — check exact AND base filename
                _folder_base = filename.rsplit('.', 1)[0]
                # Strip counter suffix: "file_37.pdf" base is "file_37", but we need "file"
                import re as _fm_re
                _folder_base_clean = _fm_re.sub(r'_\d+$', '', _folder_base)
                _folder_is_managed = (
                    filename in managed_filenames
                    or any(mf.startswith(_folder_base_clean) for mf in managed_filenames)
                )
                if _folder_is_managed:
                    continue
                # Skip fully reconciled statements
                if filename in reconciled_filenames:
                    continue

                total_pdfs_found += 1
                sort_key, statement_date = extract_statement_number_from_filename(filename, '')

                stmt_entry = {
                    'filename': filename,
                    'source': 'pdf',
                    'full_path': str(file_path),
                    'folder': folder_name,
                    'already_processed': False,
                    'is_imported': file_is_imported_nr,
                    'status': 'imported' if file_is_imported_nr else 'pending',
                    'sort_key': sort_key,
                    'statement_date': statement_date
                }

                # Try cache lookup for bank matching
                matched_bank_code = None
                if validate_balances:
                    try:
                        with open(str(file_path), 'rb') as f:
                            content_bytes = f.read()
                        pdf_hash = scan_cache.hash_pdf(content_bytes)
                        cached = scan_cache.get(pdf_hash)

                        if cached:
                            info_data, _ = cached
                            stmt_entry['opening_balance'] = float(info_data.get('opening_balance')) if info_data.get('opening_balance') is not None else None
                            stmt_entry['closing_balance'] = float(info_data.get('closing_balance')) if info_data.get('closing_balance') is not None else None
                            stmt_entry['period_start'] = info_data.get('period_start')
                            stmt_entry['period_end'] = info_data.get('period_end')
                            stmt_entry['bank_name'] = info_data.get('bank_name')
                            stmt_entry['account_number'] = info_data.get('account_number')
                            stmt_entry['sort_code'] = info_data.get('sort_code')

                            stmt_sort = (info_data.get('sort_code') or '').replace('-', '').replace(' ', '').strip()
                            stmt_acct = (info_data.get('account_number') or '').replace('-', '').replace(' ', '').strip()
                            matched_bank_code = bank_lookup.get((stmt_sort, stmt_acct))

                            if matched_bank_code:
                                # Chain check: if closing balance matches a reconciled opening,
                                # this statement has already been processed
                                closing = stmt_entry.get('closing_balance')
                                bank_rec_opens = reconciled_opening_balances.get(matched_bank_code, set())
                                chain_complete = closing is not None and round(closing, 2) in bank_rec_opens
                                if chain_complete:
                                    stmt_entry['category'] = 'already_processed'
                                    stmt_entry['status'] = 'already_processed'
                                    logger.info(f"Folder scan: filtered {filename} — chain complete (closing £{closing:,.2f} matches reconciled opening)")
                                else:
                                    stmt_entry['status'] = 'ready'
                    except Exception as e:
                        logger.warning(f"Could not read/validate PDF file {filename}: {e}")

                # Fallback: match by folder name if cache didn't match
                # Folder names follow the pattern BC060-natwest-bank-... where BC060 is the bank code
                if not matched_bank_code:
                    folder_prefix = folder_name.split('-')[0].upper() if '-' in folder_name else folder_name.upper()
                    if folder_prefix in all_banks:
                        matched_bank_code = folder_prefix
                        bank_info_ref = all_banks[matched_bank_code]
                        stmt_entry['status'] = 'ready'
                        stmt_entry['matched_bank_code'] = matched_bank_code
                        stmt_entry['matched_bank_description'] = bank_info_ref['description']
                        stmt_entry['matched_sort_code'] = bank_info_ref['sort_code']
                        stmt_entry['matched_account_number'] = bank_info_ref['account_number']
                        stmt_entry['sort_code'] = bank_info_ref['sort_code']
                        stmt_entry['account_number'] = bank_info_ref['account_number']
                        logger.info(f"Matched {filename} to {matched_bank_code} via folder name '{folder_name}'")

                # Assign to matched bank, non-current, or not_classified
                cat = stmt_entry.get('category')
                if matched_bank_code and cat in ('already_processed', 'old_statement'):
                    stmt_entry['matched_bank_code'] = matched_bank_code
                    stmt_entry['matched_bank_description'] = all_banks[matched_bank_code]['description']
                    stmt_entry['matched_sort_code'] = all_banks[matched_bank_code]['sort_code']
                    stmt_entry['matched_account_number'] = all_banks[matched_bank_code]['account_number']
                    nc_key = 'old_statements' if cat == 'old_statement' else 'already_processed'
                    non_current[nc_key].append(stmt_entry)

                    # Auto-mark as reconciled in tracking DB (same as email path)
                    if email_storage and filename:
                        try:
                            email_storage.mark_statement_reconciled(
                                filename=filename,
                                bank_code=matched_bank_code,
                                reconciled_count=0
                            )
                            logger.info(f"Auto-marked file '{filename}' as reconciled (Opera reconciled past it)")
                        except Exception as mark_err:
                            logger.warning(f"Could not auto-mark file '{filename}' as reconciled: {mark_err}")
                elif matched_bank_code and cat == 'advanced':
                    stmt_entry['matched_bank_code'] = matched_bank_code
                    stmt_entry['matched_bank_description'] = all_banks[matched_bank_code]['description']
                    stmt_entry['matched_sort_code'] = all_banks[matched_bank_code]['sort_code']
                    stmt_entry['matched_account_number'] = all_banks[matched_bank_code]['account_number']
                    non_current['advanced'].append(stmt_entry)
                elif matched_bank_code:
                    # Ensure matched bank details are populated
                    if 'matched_bank_code' not in stmt_entry:
                        stmt_entry['matched_bank_code'] = matched_bank_code
                        stmt_entry['matched_bank_description'] = all_banks[matched_bank_code]['description']
                        stmt_entry['matched_sort_code'] = all_banks[matched_bank_code]['sort_code']
                        stmt_entry['matched_account_number'] = all_banks[matched_bank_code]['account_number']

                    if stmt_entry.get('status') in ('ready', 'imported'):
                        # --- Dedup: skip if same filename or period already seen for this bank ---
                        fn_lower = filename.lower().strip()
                        fn_key = (matched_bank_code, fn_lower)
                        file_mtime = str(file_path.stat().st_mtime) if file_path.exists() else ''
                        is_dup = False

                        if fn_key in seen_bank_filenames:
                            prev_date, prev_entry = seen_bank_filenames[fn_key]
                            if file_mtime > prev_date:
                                try:
                                    all_banks[matched_bank_code]['statements'].remove(prev_entry)
                                except ValueError:
                                    pass
                                seen_bank_filenames[fn_key] = (file_mtime, stmt_entry)
                            else:
                                is_dup = True

                        if not is_dup:
                            period_key = _extract_statement_period(filename)
                            if period_key:
                                bp_key = (matched_bank_code, period_key)
                                if bp_key in seen_bank_periods:
                                    prev_date, prev_entry = seen_bank_periods[bp_key]
                                    if file_mtime > prev_date:
                                        try:
                                            all_banks[matched_bank_code]['statements'].remove(prev_entry)
                                        except ValueError:
                                            pass
                                        seen_bank_periods[bp_key] = (file_mtime, stmt_entry)
                                    else:
                                        is_dup = True

                        if not is_dup:
                            start_date, end_date = _extract_period_dates(filename)
                            if start_date and end_date:
                                bs_key = (matched_bank_code, start_date)
                                if bs_key in seen_bank_starts:
                                    prev_end, prev_entry = seen_bank_starts[bs_key]
                                    if end_date > prev_end:
                                        try:
                                            all_banks[matched_bank_code]['statements'].remove(prev_entry)
                                        except ValueError:
                                            pass
                                        seen_bank_starts[bs_key] = (end_date, stmt_entry)
                                    elif end_date < prev_end:
                                        is_dup = True

                        if not is_dup:
                            all_banks[matched_bank_code]['statements'].append(stmt_entry)
                            seen_bank_filenames[fn_key] = (file_mtime, stmt_entry)
                            period_key = _extract_statement_period(filename)
                            if period_key:
                                seen_bank_periods[(matched_bank_code, period_key)] = (file_mtime, stmt_entry)
                            start_date, end_date = _extract_period_dates(filename)
                            if start_date and end_date:
                                seen_bank_starts[(matched_bank_code, start_date)] = (end_date, stmt_entry)
                    else:
                        logger.info(f"Skipping {filename} for {matched_bank_code}: status={stmt_entry.get('status')}")
                else:
                    # No matched bank — skip silently
                    logger.info(f"Skipping local PDF {filename}: no matching Opera bank in current company")

        # --- Step 4b: Correct balances using reconciled balance and chain validation ---
        for code, bank in all_banks.items():
            rec_bal = bank.get('reconciled_balance')
            if rec_bal is None:
                continue
            for s in bank.get('statements', []):
                # Correct opening to rec_bal
                ob = s.get('opening_balance')
                if ob is None or abs(ob - rec_bal) > 0.02:
                    s['opening_balance'] = rec_bal

                # For the next statement (opening ≈ rec_bal), run chain validation
                # on the full extraction to get the correct closing balance
                full_path = s.get('full_path')
                if full_path and s.get('opening_balance') == rec_bal:
                    try:
                        from sql_rag.pdf_extraction_cache import get_extraction_cache
                        _ec = get_extraction_cache()
                        _pb = open(full_path, 'rb').read()
                        _ph = _ec.hash_pdf(_pb)
                        _cached = _ec.get(_ph)
                        if _cached:
                            _info, _txns = _cached
                            if _txns and not _info.get('_info_only'):
                                # Have full transactions — run chain from rec_bal
                                from sql_rag.statement_reconcile import StatementReconciler
                                def _sf(v):
                                    if v is None: return None
                                    if isinstance(v, (int,float)): return float(v)
                                    return float(str(v).replace(',','').replace('£','').strip()) if str(v).strip() else None
                                cur = rec_bal
                                used = set()
                                for _ in range(len(_txns)):
                                    fnd = False
                                    for j, t in enumerate(_txns):
                                        if j in used: continue
                                        mi = _sf(t.get('money_in')) or 0
                                        mo = _sf(t.get('money_out')) or 0
                                        bal = _sf(t.get('balance'))
                                        if bal is not None and abs(round(cur + mi - mo, 2) - bal) < 0.02:
                                            cur = bal
                                            used.add(j)
                                            fnd = True
                                            break
                                    if not fnd: break
                                if used:
                                    s['closing_balance'] = cur
                                    logger.info(f"Scan: chain-corrected closing for {s.get('filename','')}: £{cur:,.2f}")
                    except Exception:
                        pass

        # --- Step 5: Sort and finalize each bank's statements ---
        banks_with_statements = {}
        total_statements = 0

        for code, bank in all_banks.items():
            stmts = bank['statements']
            logger.info(f"Step 5: bank {code} has {len(stmts)} statements before filtering")
            if not stmts:
                continue

            # Sort statements in sequential import order by chaining
            # opening/closing balances starting from reconciled balance
            rec_bal = bank.get('reconciled_balance')
            if rec_bal is not None and len(stmts) > 1:
                ordered = []
                remaining = list(stmts)
                current_bal = rec_bal
                while remaining:
                    # Find statement whose opening balance matches current_bal
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
                        # No exact match — append remaining sorted by opening balance
                        remaining.sort(key=lambda s: (s.get('opening_balance') or float('inf')))
                        ordered.extend(remaining)
                        break
                stmts = ordered
            else:
                stmts.sort(key=lambda s: (0 if s.get('opening_balance') is not None else 1, s.get('opening_balance') or 0, s.get('sort_key', (9999,))))

            # Filter out statements that Opera has already reconciled past.
            # Daisy-chain from rec_bal: the next statement's opening must match
            # rec_bal. Any statement that comes before it in the chain is done.
            # We chain forward from rec_bal, collecting only reachable statements.
            if rec_bal is not None and len(stmts) > 0:
                # Build lookup: opening_balance -> list of statements
                by_opening: dict[float, list] = {}
                no_balance = []
                for s in stmts:
                    ob = s.get('opening_balance')
                    if ob is not None:
                        key = round(ob, 2)
                        by_opening.setdefault(key, []).append(s)
                    else:
                        no_balance.append(s)

                # Sort helper
                import re as _sort_re
                def _period_sort_key(s):
                    ps = s.get('period_start', '')
                    if ps:
                        return ps
                    fn = s.get('filename', '')
                    m = _sort_re.search(r'(\d{4}-\d{2}-\d{2})', fn)
                    if m:
                        return m.group(1)
                    return '9999'

                # Walk the chain from rec_bal forward
                chained = []
                current_bal = round(rec_bal, 2)
                visited = set()
                while True:
                    candidates = by_opening.get(current_bal, [])
                    picked = None
                    for c in candidates:
                        cid = id(c)
                        if cid not in visited:
                            picked = c
                            visited.add(cid)
                            break
                    # If no match in by_opening, check no_balance for the next
                    # statement chronologically (opening balance wasn't extracted)
                    if picked is None and no_balance:
                        no_balance.sort(key=_period_sort_key)
                        picked = no_balance.pop(0)
                        visited.add(id(picked))
                    if picked is None:
                        break
                    chained.append(picked)
                    cb = picked.get('closing_balance')
                    if cb is None:
                        break
                    current_bal = round(cb, 2)

                # Collect unchained statements with balances that don't connect
                # Filter out any whose closing balance matches rec_bal (already done)
                unchained = []
                for bal_list in by_opening.values():
                    for s in bal_list:
                        if id(s) not in visited:
                            cb = s.get('closing_balance')
                            # Skip if closing equals rec_bal — this was the last reconciled statement
                            if cb is not None and abs(cb - rec_bal) < 0.01:
                                continue
                            unchained.append(s)
                unchained.sort(key=_period_sort_key)
                no_balance.sort(key=_period_sort_key)
                stmts = chained + unchained + no_balance
                logger.info(f"Step 5 chain: {code} chained={len(chained)} unchained={len(unchained)} no_balance={len(no_balance)} total={len(stmts)}")

            bank['statements'] = stmts
            logger.info(f"Step 5 final: {code} = {len(stmts)} statements")

            # Clean up internal sort keys and add sequence numbers
            for i, s in enumerate(stmts, start=1):
                s.pop('sort_key', None)
                s['import_sequence'] = i

            bank['statement_count'] = len(stmts)
            total_statements += len(stmts)
            banks_with_statements[code] = bank

        # Annotate statements with draft (in-progress) status per bank
        if email_storage:
            try:
                for code, bank in banks_with_statements.items():
                    draft_keys = email_storage.get_draft_statement_keys(code)
                    if not draft_keys:
                        continue
                    # Build lookup by filename (unified) and legacy (source, email_id, attachment_id)
                    draft_by_filename = {}
                    draft_by_key = {}
                    for dk in draft_keys:
                        fn = dk.get('filename', '')
                        if fn:
                            draft_by_filename[fn] = dk['updated_at']
                        if dk.get('source') == 'email':
                            key = ('email', str(dk.get('email_id') or ''), str(dk.get('attachment_id') or ''))
                        else:
                            key = ('pdf', fn, '')
                        draft_by_key[key] = dk['updated_at']
                    for stmt in bank.get('statements', []):
                        fn = stmt.get('filename', '')
                        src = stmt.get('source', 'pdf')
                        # Try filename match first (works for both old email and new pdf sources)
                        if fn and fn in draft_by_filename:
                            stmt['has_draft'] = True
                            stmt['draft_updated_at'] = draft_by_filename[fn]
                        else:
                            # Fallback: exact key match
                            if src == 'email':
                                key = ('email', str(stmt.get('email_id', '')), str(stmt.get('attachment_id', '')))
                            else:
                                key = ('pdf', fn, '')
                            if key in draft_by_key:
                                stmt['has_draft'] = True
                                stmt['draft_updated_at'] = draft_by_key[key]
                            else:
                                stmt['has_draft'] = False
            except Exception as e:
                logger.debug(f"Could not annotate drafts for scan-all-banks: {e}")

        # --- Final cleanup: remove any reconciled statements that slipped through ---
        # Reload reconciled filenames (may have been updated by auto-mark during scan)
        try:
            final_rec_filenames = email_storage.get_reconciled_filenames()
        except Exception:
            final_rec_filenames = reconciled_filenames

        # Auto-promote imported statements where Opera's reconciled balance
        # exactly matches the statement's closing balance.
        try:
            for code, bank in all_banks.items():
                rec_bal = bank.get('reconciled_balance')
                if rec_bal is None:
                    continue
                for stmt in list(bank['statements']):
                    closing = stmt.get('closing_balance')
                    if (closing is not None
                            and stmt.get('is_imported')
                            and abs(rec_bal - closing) < 0.01):
                        fn = stmt.get('filename', '')
                        logger.info(f"Scan cleanup: auto-marking '{fn}' as reconciled "
                                    f"(Opera reconciled £{rec_bal:.2f} matches closing £{closing:.2f})")
                        final_rec_filenames.add(fn)
                        try:
                            email_storage.mark_statement_reconciled(
                                filename=fn,
                                reconciled_count=0,
                                bank_code=code
                            )
                        except Exception:
                            pass
        except Exception as promo_err:
            logger.warning(f"Auto-promote scan cleanup failed: {promo_err}")

        # Remove reconciled statements from bank lists
        for code, bank in all_banks.items():
            before = len(bank['statements'])
            bank['statements'] = [s for s in bank['statements'] if s.get('filename') not in final_rec_filenames]
            after = len(bank['statements'])
            if before != after:
                logger.info(f"Final cleanup: {code} reduced from {before} to {after} statements")

        # Remove reconciled statements from non_current lists
        for nc_key in non_current:
            non_current[nc_key] = [s for s in non_current[nc_key] if s.get('filename') not in final_rec_filenames]

        # Sort non_current lists: group by bank, then by statement date descending (newest first)
        def _nc_sort_key(s):
            """Return a date string for sorting. Higher = newer."""
            # Use statement_date (ISO format sorts correctly)
            sd = s.get('statement_date') or ''
            if sd:
                return sd
            # Fallback: sort_key tuple (year, month, day, seq) from filename date extraction
            sk = s.get('sort_key')
            if sk and isinstance(sk, (list, tuple)) and len(sk) >= 3:
                return f"{sk[0]:04d}-{sk[1]:02d}-{sk[2]:02d}"
            return ''

        for nc_key in non_current:
            # Sort by bank (ascending), then by date (descending = newest first)
            # Two-pass: first sort by date descending, then stable-sort by bank ascending
            non_current[nc_key].sort(key=_nc_sort_key, reverse=True)
            non_current[nc_key].sort(key=lambda s: s.get('matched_bank_code') or '')
            if non_current[nc_key]:
                logger.info(f"Sorted non_current[{nc_key}] ({len(non_current[nc_key])} items):")
                for s in non_current[nc_key]:
                    logger.info(f"  bank={s.get('matched_bank_code')}  date={s.get('statement_date')}  sk={s.get('sort_key')}  ob={s.get('opening_balance')}  file={s.get('filename','')[:40]}")

        # Build message
        bank_count = len(banks_with_statements)
        if total_statements == 0:
            message = f"No new statements found across {len(all_banks)} bank accounts ({total_emails_scanned} emails scanned, {total_pdfs_found} PDFs checked)"
        else:
            saved_msg = f", {emails_saved_to_folders} saved from email" if emails_saved_to_folders > 0 else ""
            message = f"Found {total_statements} statement(s) across {bank_count} bank(s){saved_msg}"

        return {
            "success": True,
            "banks": banks_with_statements,
            "unidentified": [],
            "non_current": non_current,
            "non_current_count": sum(len(v) for v in non_current.values()),
            "total_statements": total_statements,
            "total_banks_with_statements": bank_count,
            "total_banks_loaded": len(all_banks),
            "total_emails_scanned": total_emails_scanned,
            "total_pdfs_found": total_pdfs_found,
            "emails_saved_to_folders": emails_saved_to_folders,
            "duplicates_archived": duplicates_archived,
            "days_searched": days_back,
            "mailbox_synced": sync_result is not None and not (isinstance(sync_result, dict) and sync_result.get('skipped')),
            "mailbox_sync_skipped": isinstance(sync_result, dict) and sync_result.get('skipped', False),
            "timings": {**_timings, "total": round(_time.time() - _t0, 1)},
            "message": message
        }

    except Exception as e:
        logger.error(f"Error scanning all banks for statements: {e}")
        return {"success": False, "error": str(e)}





@router.post("/api/bank-import/archive-statement")
async def archive_bank_statement(
    source: str = Query(..., description="Statement source: 'email' or 'pdf'"),
    email_id: int = Query(None, description="Email ID (for email source)"),
    filename: str = Query(None, description="Statement filename"),
    full_path: str = Query(None, description="Full file path (for pdf source)"),
    bank_code: str = Query(None, description="Opera bank code")
):
    """
    Archive a bank statement after reconciliation.

    For email: moves email to Archive/Bank Statements folder.
    For PDF: moves file to archive subfolder.
    Records the action in bank_statement_imports tracking.
    """
    try:
        import shutil
        from datetime import datetime
        from pathlib import Path

        archived = False
        archive_detail = None

        if source == 'email' and email_id:
            # Look up email to get message_id and provider
            email_detail = email_storage.get_email_by_id(email_id)
            if email_detail:
                provider_id = email_detail.get('provider_id')
                message_id = email_detail.get('message_id')
                folder_id = email_detail.get('folder_id', 'INBOX')

                if isinstance(folder_id, int):
                    with email_storage._get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute("SELECT folder_id FROM email_folders WHERE id = ?", (folder_id,))
                        row = cursor.fetchone()
                        if row:
                            folder_id = row['folder_id']

                if provider_id and message_id and provider_id in email_sync_manager.providers:
                    provider = email_sync_manager.providers[provider_id]
                    moved = await provider.move_email(message_id, folder_id, 'Archive/Bank Statements')
                    if moved:
                        archived = True
                        archive_detail = f"Email moved to Archive/Bank Statements"
                        logger.info(f"Archived reconciled statement email: {filename} (email_id={email_id})")
                    else:
                        logger.warning(f"Failed to archive email_id={email_id} — may already be archived")
                        archive_detail = "Email move failed — may already be archived"
            else:
                archive_detail = f"Email {email_id} not found"

        elif source == 'pdf' and full_path:
            file_path = Path(full_path)
            if file_path.exists():
                # Create archive subfolder: {parent}/archive/YYYY-MM/
                now = datetime.now()
                archive_dir = file_path.parent / 'archive' / now.strftime('%Y-%m')
                archive_dir.mkdir(parents=True, exist_ok=True)

                dest = archive_dir / file_path.name
                # Handle duplicate filenames
                if dest.exists():
                    stem = dest.stem
                    suffix = dest.suffix
                    counter = 1
                    while dest.exists():
                        dest = archive_dir / f"{stem}_{counter}{suffix}"
                        counter += 1

                shutil.move(str(file_path), str(dest))
                archived = True
                archive_detail = f"File moved to {dest}"
                logger.info(f"Archived reconciled statement file: {filename} → {dest}")
            else:
                archive_detail = f"File not found: {full_path}"

        # Record in tracking database
        if archived and email_storage and filename:
            try:
                email_storage.record_bank_statement_import(
                    bank_code=bank_code or 'UNKNOWN',
                    filename=filename,
                    transactions_imported=0,
                    source=source,
                    target_system='archived',
                    email_id=email_id if source == 'email' else None,
                    attachment_id=None,
                    imported_by='AUTO_ARCHIVE_RECONCILE'
                )
            except Exception as e:
                logger.warning(f"Could not record archive action: {e}")

        return {
            "success": archived,
            "archived": archived,
            "detail": archive_detail,
            "filename": filename,
            "source": source
        }

    except Exception as e:
        logger.error(f"Error archiving statement: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/bank-import/archived-statements")
async def get_archived_statements():
    """Get list of archived/deleted bank statement records."""
    try:
        results = email_storage.get_archived_statements()
        return {"success": True, "statements": results, "count": len(results)}
    except Exception as e:
        logger.error(f"Error fetching archived statements: {e}")
        return {"success": False, "error": str(e), "statements": [], "count": 0}





@router.post("/api/bank-import/restore-statement")
async def restore_archived_statement(request: Request):
    """
    Restore an archived bank statement back to its bank subfolder.

    Body: { "record_id": int }
    """
    try:
        import shutil
        from pathlib import Path

        body = await request.json()
        record_id = body.get("record_id")
        if not record_id:
            return {"success": False, "error": "record_id is required"}

        # Look up the tracking record
        record = email_storage.get_bank_statement_import_by_id(record_id)
        if not record:
            return {"success": False, "error": f"Record {record_id} not found"}

        filename = record.get("filename", "")
        bank_code = record.get("bank_code", "")

        if not filename:
            return {"success": False, "error": "Record has no filename"}

        # Find the file in archive folders
        settings = _load_company_settings()
        base_folder = settings.get("bank_statements_base_folder", "")
        archive_folder = settings.get("bank_statements_archive_folder", "")

        if not base_folder:
            return {"success": False, "error": "No bank_statements_base_folder configured"}

        bp = Path(base_folder)
        if not archive_folder:
            archive_folder = str(bp / "archive")

        # Search archive subfolders for the file
        archive_path = Path(archive_folder)
        found_file = None
        if archive_path.exists():
            for sub in archive_path.iterdir():
                if sub.is_dir():
                    candidate = sub / filename
                    if candidate.exists():
                        found_file = candidate
                        break
            # Also check archive root
            if not found_file:
                candidate = archive_path / filename
                if candidate.exists():
                    found_file = candidate

        if not found_file:
            # Try base_folder/archive as fallback
            fallback_archive = bp / "archive"
            if fallback_archive.exists() and fallback_archive != archive_path:
                for sub in fallback_archive.iterdir():
                    if sub.is_dir():
                        candidate = sub / filename
                        if candidate.exists():
                            found_file = candidate
                            break
                if not found_file:
                    candidate = fallback_archive / filename
                    if candidate.exists():
                        found_file = candidate

        if not found_file:
            # File not found on disk — just delete tracking record
            email_storage.delete_bank_statement_import_record(record_id)
            return {
                "success": True,
                "message": f"Tracking record removed (file not found in archive)",
                "file_restored": False
            }

        # Find the bank subfolder to restore to
        dest_folder = None
        if bank_code and bp.exists():
            for child in bp.iterdir():
                if child.is_dir() and child.name.startswith(bank_code):
                    dest_folder = child
                    break

        if not dest_folder:
            # Fallback: restore to base folder
            dest_folder = bp

        dest_folder.mkdir(parents=True, exist_ok=True)
        dest_path = dest_folder / filename

        # Handle name collision
        if dest_path.exists():
            stem, suffix = dest_path.stem, dest_path.suffix
            counter = 1
            while dest_path.exists():
                dest_path = dest_folder / f"{stem}_{counter}{suffix}"
                counter += 1

        shutil.move(str(found_file), str(dest_path))

        # Delete the tracking record
        email_storage.delete_bank_statement_import_record(record_id)

        logger.info(f"Restored archived statement: {filename} → {dest_path}")
        return {
            "success": True,
            "message": f"Restored {filename} to {dest_folder.name}/",
            "file_restored": True,
            "restored_path": str(dest_path)
        }

    except Exception as e:
        logger.error(f"Error restoring archived statement: {e}")
        return {"success": False, "error": str(e)}





@router.post("/api/bank-import/manage-statements")
async def manage_bank_statements(request: Request):
    """
    Bulk manage non-current bank statements: archive, delete, or retain.

    Body: {
        "action": "archive" | "delete" | "retain",
        "statements": [
            {
                "source": "email" | "pdf",
                "email_id": 123,
                "attachment_id": "0",
                "filename": "stmt.pdf",
                "full_path": "/path/to.pdf",
                "matched_bank_code": "BC010",
                "category": "already_processed"
            }
        ]
    }
    """
    try:
        import shutil
        from datetime import datetime
        from pathlib import Path

        body = await request.json()
        action = body.get('action')
        statements = body.get('statements', [])

        if action not in ('archive', 'delete', 'retain'):
            return {"success": False, "error": f"Invalid action: {action}. Must be 'archive', 'delete', or 'retain'."}

        if not statements:
            return {"success": False, "error": "No statements provided"}

        results = []
        success_count = 0
        fail_count = 0

        for stmt in statements:
            source = stmt.get('source', 'email')
            email_id = stmt.get('email_id')
            filename = stmt.get('filename', '')
            full_path = stmt.get('full_path')
            bank_code = stmt.get('matched_bank_code') or 'UNKNOWN'
            attachment_id = stmt.get('attachment_id')
            result_entry = {'filename': filename, 'source': source, 'action': action, 'success': False}

            try:
                if action == 'archive':
                    if source == 'email' and email_id:
                        email_detail = email_storage.get_email_by_id(email_id)
                        if not email_detail:
                            result_entry['error'] = f"Email {email_id} not found in database"
                        else:
                            provider_id = email_detail.get('provider_id')
                            message_id = email_detail.get('message_id')
                            folder_id = email_detail.get('folder_id', 'INBOX')

                            if isinstance(folder_id, int):
                                with email_storage._get_connection() as conn:
                                    cursor = conn.cursor()
                                    cursor.execute("SELECT folder_id FROM email_folders WHERE id = ?", (folder_id,))
                                    row = cursor.fetchone()
                                    if row:
                                        folder_id = row['folder_id']

                            if not provider_id or not message_id:
                                result_entry['error'] = f"Email missing provider or message ID"
                            elif provider_id not in email_sync_manager.providers:
                                result_entry['error'] = f"Email provider not connected — check email settings"
                            else:
                                provider = email_sync_manager.providers[provider_id]
                                moved = await provider.move_email(message_id, folder_id, 'Archive/Bank Statements')
                                result_entry['success'] = moved
                                if moved:
                                    logger.info(f"Manage: archived email statement {filename}")
                                else:
                                    result_entry['error'] = "Failed to move email to archive folder"
                    elif source == 'pdf' and full_path:
                        fp = Path(full_path)
                        if fp.exists():
                            now = datetime.now()
                            archive_dir = fp.parent / 'archive' / now.strftime('%Y-%m')
                            archive_dir.mkdir(parents=True, exist_ok=True)
                            dest = archive_dir / fp.name
                            if dest.exists():
                                stem, suffix = dest.stem, dest.suffix
                                counter = 1
                                while dest.exists():
                                    dest = archive_dir / f"{stem}_{counter}{suffix}"
                                    counter += 1
                            shutil.move(str(fp), str(dest))
                            result_entry['success'] = True
                            logger.info(f"Manage: archived file {filename} → {dest}")

                elif action == 'delete':
                    # Move ALL copies (including counter-suffixed) to archive subfolder
                    if filename and bank_code and bank_code != 'UNKNOWN':
                        try:
                            settings = _load_company_settings()
                            base_folder = settings.get("bank_statements_base_folder", "")
                            archive_folder = settings.get("bank_statements_archive_folder", "")
                            if base_folder:
                                bp = Path(base_folder)
                                if not archive_folder:
                                    archive_folder = str(bp / 'archive')
                                archive_dir = Path(archive_folder) / datetime.now().strftime('%Y-%m')
                                archive_dir.mkdir(parents=True, exist_ok=True)

                                # Find ALL copies in the bank subfolder (base name + suffixed variants)
                                _del_base = filename.rsplit('.', 1)[0]
                                import re as _del_re
                                _del_base_clean = _del_re.sub(r'_\d+$', '', _del_base)

                                if bp.exists():
                                    for child in bp.iterdir():
                                        if child.is_dir() and child.name.upper().startswith(bank_code):
                                            for pdf_file in child.iterdir():
                                                if not pdf_file.is_file() or pdf_file.suffix.lower() != '.pdf':
                                                    continue
                                                pdf_base = pdf_file.stem
                                                pdf_base_clean = _del_re.sub(r'_\d+$', '', pdf_base)
                                                if pdf_base_clean == _del_base_clean:
                                                    dest = archive_dir / pdf_file.name
                                                    if dest.exists():
                                                        stem = pdf_file.stem
                                                        suffix = pdf_file.suffix
                                                        counter = 1
                                                        while dest.exists():
                                                            dest = archive_dir / f"{stem}_{counter}{suffix}"
                                                            counter += 1
                                                    shutil.move(str(pdf_file), str(dest))
                                                    logger.info(f"Manage: archived {pdf_file.name} → {dest}")
                                            break
                        except Exception as local_err:
                            logger.warning(f"Could not archive local PDF copies: {local_err}")

                    if source == 'email' and email_id:
                        if not email_storage:
                            result_entry['error'] = "Email storage not initialised"
                        else:
                            email_detail = email_storage.get_email_by_id(email_id)
                            if not email_detail:
                                # Email not in local DB — treat as already deleted, just record it
                                result_entry['success'] = True
                                logger.info(f"Manage: email {email_id} not in DB (already deleted?), marking as deleted for {filename}")
                            else:
                                provider_id = email_detail.get('provider_id')
                                message_id = email_detail.get('message_id')
                                folder_id = email_detail.get('folder_id', 'INBOX')

                                if isinstance(folder_id, int):
                                    with email_storage._get_connection() as conn:
                                        cursor = conn.cursor()
                                        cursor.execute("SELECT folder_id FROM email_folders WHERE id = ?", (folder_id,))
                                        row = cursor.fetchone()
                                        if row:
                                            folder_id = row['folder_id']

                                if not email_sync_manager:
                                    result_entry['error'] = "Email sync not configured"
                                elif not provider_id or not message_id:
                                    result_entry['error'] = f"Email missing provider or message ID"
                                elif provider_id not in email_sync_manager.providers:
                                    result_entry['error'] = f"Email provider '{provider_id}' not connected — check email settings"
                                else:
                                    provider = email_sync_manager.providers[provider_id]
                                    # Move email to archive folder (not Trash)
                                    moved = False
                                    for archive_imap_folder in ['Archive/Bank Statements', 'Archive/BankStatements', 'Archive']:
                                        try:
                                            moved = await provider.move_email(message_id, folder_id, archive_imap_folder)
                                            if moved:
                                                logger.info(f"Manage: moved email to {archive_imap_folder}")
                                                break
                                        except Exception:
                                            continue
                                    if not moved:
                                        # Fallback: try Trash if archive folders don't exist
                                        try:
                                            moved = await provider.move_email(message_id, folder_id, 'Deleted Items')
                                        except Exception as move_err:
                                            result_entry['error'] = f"Could not archive email: {move_err}"
                                    result_entry['success'] = moved or True  # Local archive is primary, email move is best-effort
                                    if moved:
                                        logger.info(f"Manage: deleted email statement {filename}")
                                    elif 'error' not in result_entry:
                                        result_entry['error'] = "Failed to move email to Trash folder"
                    elif source == 'pdf' and full_path:
                        fp = Path(full_path)
                        if fp.exists():
                            # Archive the specific file (copies already handled above)
                            try:
                                settings_del = _load_company_settings()
                                archive_f = settings_del.get("bank_statements_archive_folder", "")
                                if not archive_f:
                                    archive_f = str(Path(settings_del.get("bank_statements_base_folder", ".")) / 'archive')
                                arch_dir = Path(archive_f) / datetime.now().strftime('%Y-%m')
                                arch_dir.mkdir(parents=True, exist_ok=True)
                                dest = arch_dir / fp.name
                                if dest.exists():
                                    stem, suffix = fp.stem, fp.suffix
                                    c = 1
                                    while dest.exists():
                                        dest = arch_dir / f"{stem}_{c}{suffix}"
                                        c += 1
                                shutil.move(str(fp), str(dest))
                                logger.info(f"Manage: archived file {filename} → {dest}")
                            except Exception:
                                fp.unlink()  # Fallback: delete if archive fails
                                logger.info(f"Manage: deleted file {filename} (archive failed)")
                            result_entry['success'] = True
                        else:
                            result_entry['success'] = True
                            logger.info(f"Manage: file already gone {filename}")
                    else:
                        result_entry['error'] = f"Cannot delete: source={source}, email_id={email_id}, path={full_path}"

                elif action == 'retain':
                    # Just record — no email/file movement
                    result_entry['success'] = True

                # Record action in tracking database — ALWAYS record for delete/retain
                # so the scan excludes the statement even if the server-side move failed.
                # For archive, only record on success (we need the file to actually move).
                should_record = (result_entry['success'] or action in ('delete', 'retain'))
                if should_record and email_storage:
                    try:
                        # Map action verb to past tense — delete now archives, so both map to 'archived'
                        target_system_map = {'archive': 'archived', 'delete': 'archived', 'retain': 'retained'}
                        # Map source to DB-compatible value ('pdf' -> 'file')
                        db_source = 'file' if source == 'pdf' else source
                        email_storage.record_bank_statement_import(
                            bank_code=bank_code,
                            filename=filename,
                            transactions_imported=0,
                            source=db_source,
                            target_system=target_system_map[action],
                            email_id=email_id if source == 'email' else None,
                            attachment_id=attachment_id,
                            imported_by=f'MANAGE_{action.upper()}'
                        )
                        # For delete/retain: marking locally is the primary action
                        # The IMAP move is best-effort, so treat local record as success
                        if action in ('delete', 'retain') and not result_entry['success']:
                            imap_warning = result_entry.get('error', '')
                            result_entry['success'] = True
                            if imap_warning:
                                result_entry['warning'] = f"Excluded from scan. Note: {imap_warning}"
                                del result_entry['error']
                            logger.info(f"Manage: recorded {action} locally for {filename} (server-side move skipped)")
                    except Exception as rec_err:
                        logger.warning(f"Could not record manage action: {rec_err}")
                        if not result_entry['success']:
                            result_entry['error'] = f"Failed to record {action}: {rec_err}"

            except Exception as stmt_err:
                result_entry['error'] = str(stmt_err)
                logger.warning(f"Error managing statement {filename}: {stmt_err}")

            if result_entry['success']:
                success_count += 1
            else:
                fail_count += 1
            results.append(result_entry)

        # Build error summary from individual failures
        error_msgs = [r.get('error', '') for r in results if not r['success'] and r.get('error')]
        error_summary = '; '.join(error_msgs) if error_msgs else None

        resp = {
            "success": success_count > 0,
            "action": action,
            "total": len(statements),
            "success_count": success_count,
            "fail_count": fail_count,
            "results": results,
            "message": f"{action.title()}d {success_count} of {len(statements)} statement(s)"
        }
        if not resp["success"] and error_summary:
            resp["error"] = error_summary
        return resp

    except Exception as e:
        logger.error(f"Error managing statements: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/bank-import/raw-preview-email")
async def raw_preview_email_attachment(
    email_id: int = Query(..., description="Email ID"),
    attachment_id: str = Query(..., description="Attachment ID"),
    lines: int = Query(50, description="Number of lines to preview")
):
    """
    Preview raw contents of an email attachment.
    Returns first N lines of the file for inspection before processing.
    """
    if not email_storage or not email_sync_manager:
        raise HTTPException(status_code=503, detail="Email services not initialized")

    try:
        # Get email details
        email = email_storage.get_email_by_id(email_id)
        if not email:
            return {"success": False, "error": f"Email {email_id} not found"}

        # Find the attachment
        attachments = email.get('attachments', [])
        attachment_meta = next(
            (a for a in attachments if a.get('attachment_id') == attachment_id),
            None
        )
        if not attachment_meta:
            return {"success": False, "error": f"Attachment {attachment_id} not found"}

        filename = attachment_meta.get('filename', 'unknown')

        # Get provider for this email
        provider_id = email.get('provider_id')
        provider = email_sync_manager.providers.get(provider_id)
        if not provider:
            return {"success": False, "error": f"Provider {provider_id} not available"}

        # Download attachment content
        content = provider.get_attachment_content(
            email.get('message_id', str(email_id)),
            attachment_id
        )
        if not content:
            return {"success": False, "error": "Could not download attachment"}

        # Check if it's a PDF (binary) - return base64 encoded for display
        if filename.lower().endswith('.pdf'):
            import base64
            pdf_base64 = base64.b64encode(content).decode('utf-8')
            return {
                "success": True,
                "is_pdf": True,
                "pdf_data": pdf_base64,
                "filename": filename,
                "lines": [f"PDF file: {filename}"],
                "total_lines_shown": 1
            }

        # Try to decode as text
        file_lines = []
        for encoding in ['utf-8', 'latin-1', 'cp1252']:
            try:
                text_content = content.decode(encoding)
                for i, line in enumerate(text_content.splitlines()):
                    if i >= lines:
                        break
                    file_lines.append(line.rstrip('\n\r'))
                break
            except (UnicodeDecodeError, AttributeError):
                continue

        if not file_lines:
            return {
                "success": False,
                "error": "Could not decode attachment with any supported encoding"
            }

        return {
            "success": True,
            "lines": file_lines,
            "total_lines_shown": len(file_lines),
            "filename": filename
        }

    except Exception as e:
        logger.error(f"Error reading email attachment: {e}")
        return {"success": False, "error": str(e)}





@router.post("/api/bank-import/preview-from-email")
async def preview_bank_import_from_email(
    email_id: int = Query(..., description="Email ID"),
    attachment_id: str = Query(..., description="Attachment ID"),
    bank_code: str = Query(..., description="Opera bank account code"),
    format_override: Optional[str] = Query(None, description="Force specific format: CSV, OFX, QIF, MT940"),
    extraction_method: str = Query("auto", description="Extraction method: auto (AI for PDFs), ai (force AI), parse (force text parsing)")
):
    """
    Preview bank statement from email attachment.
    Same response format as preview-multiformat.

    Extraction methods:
    - auto: Use AI extraction for PDFs, text parsing for CSV/OFX/QIF/MT940
    - ai: Force AI extraction for any file type
    - parse: Force text parsing (will fail for binary PDFs)
    """
    if not email_storage or not email_sync_manager:
        raise HTTPException(status_code=503, detail="Email services not initialized")

    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.bank_import import BankStatementImport
        from sql_rag.opera_config import (
            get_period_posting_decision,
            get_current_period_info,
            is_open_period_accounting_enabled,
            validate_posting_period,
            get_ledger_type_for_transaction
        )

        # Get email details
        email = email_storage.get_email_by_id(email_id)
        if not email:
            return {"success": False, "error": f"Email {email_id} not found"}

        # Find the attachment
        attachments = email.get('attachments', [])
        attachment_meta = next(
            (a for a in attachments if a.get('attachment_id') == attachment_id),
            None
        )
        if not attachment_meta:
            return {"success": False, "error": f"Attachment {attachment_id} not found"}

        filename = attachment_meta.get('filename', 'statement')

        # Get provider for this email
        provider_id = email.get('provider_id')
        provider = email_sync_manager.providers.get(provider_id)
        if not provider:
            return {"success": False, "error": f"Provider {provider_id} not available"}

        message_id = email.get('message_id')

        # Get folder_id
        folder_id_db = email.get('folder_id')
        folder_id = 'INBOX'
        if folder_id_db:
            with email_storage._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT folder_id FROM email_folders WHERE id = ?", (folder_id_db,))
                row = cursor.fetchone()
                if row:
                    folder_id = row['folder_id']

        # Download attachment content
        result = await provider.download_attachment(message_id, attachment_id, folder_id)

        # If download failed, email may have been moved to archive
        if not result and folder_id not in ('Archive/Bank Statements', 'Archive/BankStatements'):
            for archive_folder in ['Archive/Bank Statements', 'Archive/BankStatements']:
                result = await provider.download_attachment(message_id, attachment_id, archive_folder)
                if result:
                    logger.info(f"Found attachment in {archive_folder} (moved from {folder_id})")
                    break

        if not result:
            return {"success": False, "error": "Failed to download attachment"}

        content_bytes, _, _ = result

        # Determine extraction method
        use_ai_extraction = (
            extraction_method == 'ai' or
            (extraction_method == 'auto' and filename.lower().endswith('.pdf'))
        )

        # Initialize variables
        detected_bank_info = None
        detected_bank_code = None
        stmt_transactions = None  # Raw statement transactions from PDF extraction
        statement_info = None  # Statement header info from PDF extraction

        # Handle AI extraction (for PDFs or when forced)
        if use_ai_extraction:
            import tempfile
            import os
            from sql_rag.statement_reconcile import StatementReconciler
            from sql_rag.bank_import import BankTransaction

            # Save to temp file for AI extraction
            file_ext = '.' + filename.split('.')[-1] if '.' in filename else '.pdf'
            with tempfile.NamedTemporaryFile(suffix=file_ext, delete=False) as tmp_file:
                tmp_file.write(content_bytes)
                tmp_path = tmp_file.name

            try:
                # Use StatementReconciler for AI extraction
                reconciler = StatementReconciler(sql_connector, config=config)
                statement_info, stmt_transactions = reconciler.extract_transactions_from_pdf(tmp_path)

                # Convert StatementTransaction to BankTransaction format
                transactions = []
                for i, st in enumerate(stmt_transactions, start=1):
                    # StatementTransaction.amount: positive = money in, negative = money out
                    txn = BankTransaction(
                        row_number=i,
                        date=st.date,
                        amount=st.amount,
                        subcategory=st.transaction_type or '',
                        memo=st.description or '',
                        name=st.description or '',
                        reference=st.reference or '',
                        fit_id=''
                    )
                    transactions.append(txn)

                detected_format = "PDF (AI Extraction)"

                # Validate/detect bank from statement
                detected_bank_info = {
                    "bank_name": statement_info.bank_name,
                    "account_number": statement_info.account_number,
                    "sort_code": statement_info.sort_code,
                    "statement_date": statement_info.statement_date.isoformat() if statement_info.statement_date else None,
                    "opening_balance": statement_info.opening_balance,
                    "closing_balance": statement_info.closing_balance
                }

                # Always try to match detected bank to Opera bank accounts
                banks_df = sql_connector.execute_query("""
                    SELECT nk_acnt, nk_sort, nk_number, RTRIM(nk_desc) AS nk_desc
                    FROM nbank WITH (NOLOCK)
                """)
                if banks_df is not None and len(banks_df) > 0:
                    for _, bank in banks_df.iterrows():
                        # Normalize sort codes for comparison
                        stmt_sort = (statement_info.sort_code or '').replace('-', '').replace(' ', '')
                        bank_sort = (bank['nk_sort'] or '').replace('-', '').replace(' ', '')
                        stmt_acnt = (statement_info.account_number or '').replace(' ', '')
                        bank_acnt = (bank['nk_number'] or '').replace(' ', '')

                        if stmt_sort == bank_sort and stmt_acnt == bank_acnt:
                            detected_bank_code = bank['nk_acnt'].strip()
                            detected_bank_info['matched_opera_bank'] = detected_bank_code
                            detected_bank_info['matched_opera_name'] = bank['nk_desc']
                            break

                # Warn if selected bank doesn't match detected bank
                if detected_bank_code and detected_bank_code.strip() != bank_code.strip():
                    detected_bank_info['bank_mismatch'] = True
                    detected_bank_info['selected_bank'] = bank_code

                # Validate statement sequence (opening balance must match reconciled balance)
                effective_bank_code = detected_bank_code or bank_code
                if statement_info.opening_balance is not None:
                    sequence_validation = reconciler.validate_statement_sequence(effective_bank_code, statement_info)

                    if sequence_validation['status'] == 'skip':
                        # Already processed - mark as processed so it won't appear in future scans
                        try:
                            email_storage.record_bank_statement_import(
                                bank_code=effective_bank_code,
                                filename=filename,
                                transactions_imported=0,
                                source='email',
                                target_system='already_processed',
                                email_id=email_id,
                                attachment_id=attachment_id,
                                total_receipts=0,
                                total_payments=0,
                                imported_by='AUTO_SKIP'
                            )
                            logger.info(f"Auto-marked statement as processed: email_id={email_id}, attachment_id={attachment_id}")
                        except Exception as track_err:
                            logger.warning(f"Failed to auto-mark statement as processed: {track_err}")

                        return {
                            "success": False,
                            "status": "skipped",
                            "reason": "already_processed",
                            "bank_code": effective_bank_code,
                            "filename": filename,
                            "statement_info": {
                                "bank_name": statement_info.bank_name,
                                "account_number": statement_info.account_number,
                                "opening_balance": statement_info.opening_balance,
                                "closing_balance": statement_info.closing_balance,
                                "period_start": str(statement_info.period_start) if statement_info.period_start else None,
                                "period_end": str(statement_info.period_end) if statement_info.period_end else None
                            },
                            "reconciled_balance": sequence_validation['reconciled_balance'],
                            "errors": [
                                f"STATEMENT ALREADY PROCESSED",
                                f"Statement Opening Balance: £{statement_info.opening_balance:,.2f}",
                                f"Opera Reconciled Balance: £{sequence_validation['reconciled_balance']:,.2f}",
                                f"Statement Closing Balance: £{statement_info.closing_balance:,.2f}" if statement_info.closing_balance else None,
                                f"",
                                f"The statement opening balance is LESS than Opera's reconciled balance,",
                                f"which means this statement period has already been processed.",
                                f"",
                                f"This statement has been marked as processed and will not appear again."
                            ],
                            "message": "Statement marked as processed - will not appear in future scans."
                        }

                    if sequence_validation['status'] == 'pending':
                        # Future statement - missing one in between
                        return {
                            "success": False,
                            "status": "pending",
                            "reason": "missing_statement",
                            "bank_code": effective_bank_code,
                            "filename": filename,
                            "statement_info": {
                                "bank_name": statement_info.bank_name,
                                "account_number": statement_info.account_number,
                                "opening_balance": statement_info.opening_balance,
                                "closing_balance": statement_info.closing_balance
                            },
                            "reconciled_balance": sequence_validation['reconciled_balance'],
                            "errors": [
                                f"MISSING STATEMENT - Cannot import yet",
                                f"Statement Opening Balance: £{statement_info.opening_balance:,.2f}",
                                f"Opera Reconciled Balance: £{sequence_validation['reconciled_balance']:,.2f}",
                                f"",
                                f"The statement opening balance is GREATER than Opera's reconciled balance.",
                                f"You may be missing a statement in between.",
                                f"Import the earlier statement first."
                            ]
                        }

                # Create importer for matching
                importer = BankStatementImport(
                    bank_code=detected_bank_code or bank_code,
                    use_enhanced_matching=True,
                    use_fingerprinting=True
                )

            finally:
                os.unlink(tmp_path)
        else:
            # Decode bytes to string for text-based formats
            try:
                content = content_bytes.decode('utf-8')
            except UnicodeDecodeError:
                content = content_bytes.decode('latin-1')

            # Create importer and parse content
            importer = BankStatementImport(
                bank_code=bank_code,
                use_enhanced_matching=True,
                use_fingerprinting=True
            )

            # Validate bank from content (for CSV files)
            if filename.lower().endswith('.csv'):
                is_valid, validation_message, detected_bank = importer.validate_bank_from_content(content)
                if not is_valid:
                    return {
                        "success": False,
                        "error": validation_message,
                        "bank_mismatch": True,
                        "detected_bank": detected_bank,
                        "selected_bank": bank_code,
                        "transactions": []
                    }

            # Parse content
            transactions, detected_format = importer.parse_content(content, filename, format_override)

        # Process transactions (matching, duplicate detection)
        importer.process_transactions(transactions)

        # Validate period accounting for each transaction
        period_info = get_current_period_info(sql_connector)
        open_period_enabled = is_open_period_accounting_enabled(sql_connector)
        period_violations = []

        for txn in transactions:
            txn.original_date = txn.date

            if txn.action and txn.action not in ('skip', None):
                ledger_type = get_ledger_type_for_transaction(txn.action)
                period_result = validate_posting_period(sql_connector, txn.date, ledger_type)

                if not period_result.is_valid:
                    txn.period_valid = False
                    txn.period_error = period_result.error_message
                    period_violations.append({
                        "row": txn.row_number,
                        "date": txn.date.isoformat(),
                        "name": txn.name,
                        "action": txn.action,
                        "ledger_type": ledger_type,
                        "error": period_result.error_message,
                        "transaction_year": period_result.year,
                        "transaction_period": period_result.period,
                        "current_year": period_info.get('np_year'),
                        "current_period": period_info.get('np_perno')
                    })
                else:
                    txn.period_valid = True
                    txn.period_error = None
            else:
                decision = get_period_posting_decision(sql_connector, txn.date)
                if not decision.can_post:
                    txn.period_valid = False
                    txn.period_error = decision.error_message
                else:
                    txn.period_valid = True
                    txn.period_error = None

        # Load pattern learner for suggestions on unmatched transactions
        try:
            from sql_rag.bank_patterns import BankPatternLearner
            pattern_learner = BankPatternLearner(company_code=sql_connector.company_code if hasattr(sql_connector, 'company_code') else 'default')
        except Exception as e:
            logger.warning(f"Could not initialize pattern learner: {e}")
            pattern_learner = None

        # Categorize for frontend (same as preview-multiformat)
        matched_receipts = []
        matched_payments = []
        matched_refunds = []
        repeat_entries = []
        unmatched = []
        already_posted = []
        skipped = []

        # Load GoCardless FX imports to auto-detect FX transactions in bank statement
        gc_fx_refs = {}
        if email_storage:
            try:
                gc_fx_refs = email_storage.get_gocardless_fx_imports('opera_se')
                if gc_fx_refs:
                    logger.info(f"Loaded {len(gc_fx_refs)} GoCardless FX references for auto-detection")
            except Exception as e:
                logger.warning(f"Failed to load GoCardless FX imports: {e}")

        for txn in transactions:
            # Look up pattern suggestion for unmatched transactions
            suggestion = None
            if pattern_learner and not txn.matched_account:
                try:
                    suggestion = pattern_learner.find_pattern(txn.memo or txn.name)
                except Exception as e:
                    logger.warning(f"Pattern lookup failed: {e}")

            txn_data = {
                "row": txn.row_number,
                "date": txn.date.isoformat(),
                "amount": txn.amount,
                "name": txn.name,
                "reference": txn.reference,
                "memo": txn.memo,
                "fit_id": txn.fit_id,
                "account": txn.matched_account,
                "account_name": txn.matched_name,
                "match_score": round(txn.match_score * 100) if txn.match_score else 0,
                "match_source": txn.match_source,
                "action": txn.action,
                "reason": txn.skip_reason,
                "fingerprint": txn.fingerprint,
                "is_duplicate": txn.is_duplicate,
                "duplicate_candidates": [
                    {
                        "table": c.table,
                        "record_id": c.record_id,
                        "match_type": c.match_type,
                        "confidence": round(c.confidence * 100)
                    }
                    for c in (txn.duplicate_candidates or [])
                ],
                "refund_credit_note": getattr(txn, 'refund_credit_note', None),
                "refund_credit_amount": getattr(txn, 'refund_credit_amount', None),
                "repeat_entry_ref": getattr(txn, 'repeat_entry_ref', None),
                "repeat_entry_desc": getattr(txn, 'repeat_entry_desc', None),
                "repeat_entry_next_date": getattr(txn, 'repeat_entry_next_date', None).isoformat() if getattr(txn, 'repeat_entry_next_date', None) else None,
                "repeat_entry_posted": getattr(txn, 'repeat_entry_posted', None),
                "repeat_entry_total": getattr(txn, 'repeat_entry_total', None),
                "repeat_entry_freq": getattr(txn, 'repeat_entry_freq', None),
                "repeat_entry_every": getattr(txn, 'repeat_entry_every', None),
                "period_valid": getattr(txn, 'period_valid', True),
                "period_error": getattr(txn, 'period_error', None),
                "original_date": getattr(txn, 'original_date', txn.date).isoformat() if getattr(txn, 'original_date', None) else txn.date.isoformat(),
                "bank_transfer_details": getattr(txn, 'bank_transfer_details', None),
            }

            # Add suggestion fields if pattern found (for unmatched/skipped transactions)
            if suggestion:
                txn_data['suggested_type'] = suggestion.transaction_type
                txn_data['suggested_account'] = suggestion.account_code
                txn_data['suggested_account_name'] = suggestion.account_name
                txn_data['suggested_ledger_type'] = suggestion.ledger_type
                txn_data['suggested_vat_code'] = suggestion.vat_code
                txn_data['suggested_nominal_code'] = suggestion.nominal_code
                txn_data['suggestion_confidence'] = suggestion.confidence
                txn_data['suggestion_source'] = suggestion.match_type

            if txn.action == 'sales_receipt':
                matched_receipts.append(txn_data)
            elif txn.action == 'purchase_payment':
                matched_payments.append(txn_data)
            elif txn.action == 'bank_transfer':
                matched_receipts.append(txn_data) if txn.amount > 0 else matched_payments.append(txn_data)
            elif txn.action in ('sales_refund', 'purchase_refund'):
                matched_refunds.append(txn_data)
            elif txn.action == 'repeat_entry':
                # Calculate all outstanding posting dates and their period status
                freq = getattr(txn, 'repeat_entry_freq', None)
                every = getattr(txn, 'repeat_entry_every', 1) or 1
                posted = getattr(txn, 'repeat_entry_posted', 0) or 0
                total = getattr(txn, 'repeat_entry_total', 0) or 0
                next_date = getattr(txn, 'repeat_entry_next_date', None)

                outstanding_postings = []
                if freq and next_date and (total == 0 or posted < total):
                    from dateutil.relativedelta import relativedelta
                    calc_date = next_date
                    calc_posted = posted
                    max_calc = (total - posted) if total > 0 else 12
                    for _ in range(max_calc):
                        if total > 0 and calc_posted >= total:
                            break
                        period_result = validate_posting_period(sql_connector, calc_date, 'NL')
                        outstanding_postings.append({
                            "date": calc_date.isoformat() if hasattr(calc_date, 'isoformat') else str(calc_date),
                            "period_valid": period_result.is_valid,
                            "period_error": period_result.error_message if not period_result.is_valid else None,
                            "period": period_result.period,
                            "year": period_result.year,
                        })
                        if freq == 'D':
                            calc_date = calc_date + timedelta(days=every)
                        elif freq == 'W':
                            calc_date = calc_date + timedelta(weeks=every)
                        elif freq == 'M':
                            calc_date = calc_date + relativedelta(months=every)
                        elif freq == 'Q':
                            calc_date = calc_date + relativedelta(months=3 * every)
                        elif freq == 'Y':
                            calc_date = calc_date + relativedelta(years=every)
                        else:
                            break
                        calc_posted += 1

                txn_data["outstanding_postings"] = outstanding_postings
                txn_data["outstanding_count"] = len(outstanding_postings)
                txn_data["outstanding_blocked"] = sum(1 for p in outstanding_postings if not p["period_valid"])
                txn_data["outstanding_open"] = sum(1 for p in outstanding_postings if p["period_valid"])
                repeat_entries.append(txn_data)
            elif txn.is_duplicate or (txn.skip_reason and 'Already' in txn.skip_reason):
                already_posted.append(txn_data)
            else:
                # Check if this is a GoCardless FX transaction
                gc_fx_match = None
                if gc_fx_refs:
                    txn_text = f"{txn.name or ''} {txn.memo or ''}".upper()
                    for ref_key, ref_data in gc_fx_refs.items():
                        if ref_key.upper() in txn_text:
                            gc_fx_match = (ref_key, ref_data)
                            break
                if gc_fx_match:
                    ref_key, ref_data = gc_fx_match
                    txn_data['action'] = 'gc_fx_ignore'
                    txn_data['reason'] = f"GoCardless FX payout ({ref_data['currency']})"
                    txn_data['gc_fx_currency'] = ref_data['currency']
                    txn_data['gc_fx_original_amount'] = ref_data['gross_amount']
                    txn_data['gc_fx_gbp_amount'] = ref_data['fx_amount']
                    txn_data['gc_fx_reference'] = ref_key
                    skipped.append(txn_data)
                    logger.info(f"Auto-detected GoCardless FX transaction: {ref_key} ({ref_data['currency']} {ref_data['gross_amount']} -> GBP {ref_data['fx_amount']})")
                else:
                    # All non-matched, non-duplicate transactions go to unmatched
                    # so the user can assign them manually via dropdown
                    unmatched.append(txn_data)

        # Include detected bank info if available (from AI extraction)
        statement_bank_info = detected_bank_info if use_ai_extraction else None

        # Build raw statement transactions list for reconcile screen
        # These are the original PDF-extracted lines with balances, descriptions etc.
        raw_statement_lines = []
        if use_ai_extraction and stmt_transactions:
            for i, st in enumerate(stmt_transactions, start=1):
                raw_statement_lines.append({
                    "line_number": i,
                    "date": st.date.isoformat() if hasattr(st.date, 'isoformat') else str(st.date),
                    "description": st.description or '',
                    "amount": st.amount,
                    "balance": st.balance,
                    "transaction_type": st.transaction_type or '',
                    "reference": st.reference or ''
                })

        # Check for bank mismatch (for frontend warning)
        has_bank_mismatch = (
            statement_bank_info and
            statement_bank_info.get('bank_mismatch', False)
        )

        return {
            "success": True,
            "filename": filename,
            "source": "email",
            "email_id": email_id,
            "attachment_id": attachment_id,
            "detected_format": detected_format,
            "statement_bank_info": statement_bank_info,
            "bank_mismatch": has_bank_mismatch,
            "detected_bank": statement_bank_info.get('matched_opera_bank') if has_bank_mismatch else None,
            "selected_bank": bank_code if has_bank_mismatch else None,
            "bank_code_used": detected_bank_code if use_ai_extraction and detected_bank_code else bank_code,
            "total_transactions": len(transactions),
            "matched_receipts": matched_receipts,
            "matched_payments": matched_payments,
            "matched_refunds": matched_refunds,
            "repeat_entries": repeat_entries,
            "unmatched": unmatched,
            "already_posted": already_posted,
            "skipped": skipped,
            "summary": {
                "to_import": len(matched_receipts) + len(matched_payments) + len(matched_refunds),
                "refund_count": len(matched_refunds),
                "repeat_entry_count": len(repeat_entries),
                "unmatched_count": len(unmatched),
                "already_posted_count": len(already_posted),
                "skipped_count": len(skipped)
            },
            "errors": [],
            "period_info": {
                "current_year": period_info.get('np_year'),
                "current_period": period_info.get('np_perno'),
                "open_period_accounting": open_period_enabled
            },
            "period_violations": period_violations,
            "has_period_violations": len(period_violations) > 0,
            # Raw statement data for reconcile screen — same data regardless of email or file source
            "statement_transactions": raw_statement_lines,
            "statement_info": {
                "bank_name": statement_info.bank_name if use_ai_extraction and statement_info else None,
                "account_number": statement_info.account_number if use_ai_extraction and statement_info else None,
                "sort_code": statement_info.sort_code if use_ai_extraction and statement_info else None,
                "statement_date": statement_info.statement_date.isoformat() if use_ai_extraction and statement_info and statement_info.statement_date else None,
                "period_start": statement_info.period_start.isoformat() if use_ai_extraction and statement_info and statement_info.period_start else None,
                "period_end": statement_info.period_end.isoformat() if use_ai_extraction and statement_info and statement_info.period_end else None,
                "opening_balance": statement_info.opening_balance if use_ai_extraction and statement_info else None,
                "closing_balance": statement_info.closing_balance if use_ai_extraction and statement_info else None
            } if use_ai_extraction and statement_info else None
        }

    except Exception as e:
        logger.error(f"Error previewing bank import from email: {e}")
        return {"success": False, "error": str(e)}





@router.post("/api/bank-import/import-from-email")
async def import_bank_statement_from_email(
    email_id: int = Query(..., description="Email ID"),
    attachment_id: str = Query(..., description="Attachment ID"),
    bank_code: str = Query(..., description="Opera bank account code"),
    auto_allocate: bool = Query(False, description="Auto-allocate receipts/payments to invoices where possible"),
    auto_reconcile: bool = Query(False, description="Auto-reconcile imported entries against bank statement"),
    resume_import_id: Optional[int] = Query(None, description="Import ID to resume from (skips already-posted lines)"),
    request_body: Optional[Dict[str, Any]] = Body(None)
):
    """
    Import bank statement from email attachment.
    Same request body format as import-with-overrides.

    Request body format:
    {
        "overrides": [{"row": 1, "account": "A001", "ledger_type": "C", "transaction_type": "sales_refund"}, ...],
        "date_overrides": [{"row": 1, "date": "2025-01-15"}, ...],
        "selected_rows": [1, 2, 3, 5]
    }

    Auto-allocation (when auto_allocate=True):
    - Receipts: allocated to customer invoices if invoice ref found in description OR clears account (2+ invoices)
    - Payments: allocated to supplier invoices using same rules
    - Single invoice with no reference: NOT auto-allocated (dangerous assumption)
    - Zero tolerance on amounts - must match exactly

    Auto-reconciliation (when auto_reconcile=True):
    - Marks all imported entries as reconciled in Opera
    - Statement number generated from latest transaction date (YYMMDD format)
    - Only reconciles if ALL entries imported successfully (all-or-nothing)
    - Updates nbank.nk_recbal with new reconciled balance
    """
    if not email_storage or not email_sync_manager:
        raise HTTPException(status_code=503, detail="Email services not initialized")

    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from datetime import datetime
        from sql_rag.bank_import import BankStatementImport
        from sql_rag.opera_config import validate_posting_period, get_ledger_type_for_transaction, get_current_period_info

        # Get email details
        email = email_storage.get_email_by_id(email_id)
        if not email:
            return {"success": False, "error": f"Email {email_id} not found"}

        # Find the attachment
        attachments = email.get('attachments', [])
        attachment_meta = next(
            (a for a in attachments if a.get('attachment_id') == attachment_id),
            None
        )
        if not attachment_meta:
            return {"success": False, "error": f"Attachment {attachment_id} not found"}

        filename = attachment_meta.get('filename', 'statement')

        # Get provider for this email
        provider_id = email.get('provider_id')
        provider = email_sync_manager.providers.get(provider_id)
        if not provider:
            return {"success": False, "error": f"Provider {provider_id} not available"}

        message_id = email.get('message_id')

        # Get folder_id
        folder_id_db = email.get('folder_id')
        folder_id = 'INBOX'
        if folder_id_db:
            with email_storage._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT folder_id FROM email_folders WHERE id = ?", (folder_id_db,))
                row = cursor.fetchone()
                if row:
                    folder_id = row['folder_id']

        # Download attachment content
        result = await provider.download_attachment(message_id, attachment_id, folder_id)

        # If download failed, email may have been moved to archive
        if not result and folder_id not in ('Archive/Bank Statements', 'Archive/BankStatements'):
            for archive_folder in ['Archive/Bank Statements', 'Archive/BankStatements']:
                result = await provider.download_attachment(message_id, attachment_id, archive_folder)
                if result:
                    logger.info(f"Found attachment in {archive_folder} (moved from {folder_id})")
                    break

        if not result:
            return {"success": False, "error": "Failed to download attachment"}

        content_bytes, _, _ = result

        # Parse request body first
        logger.info(f"import-from-email: request_body type={type(request_body)}, content={request_body}")
        if request_body is None:
            overrides = []
            date_overrides = []
            selected_rows = None
            logger.info("import-from-email: No request body - will import all valid transactions")
        elif isinstance(request_body, list):
            overrides = request_body
            date_overrides = []
            selected_rows = None
            logger.info(f"import-from-email: Request body is list of {len(request_body)} overrides")
        else:
            overrides = request_body.get('overrides', [])
            date_overrides = request_body.get('date_overrides', [])
            selected_rows_list = request_body.get('selected_rows')
            selected_rows = set(selected_rows_list) if selected_rows_list is not None else None
            logger.info(f"import-from-email: Parsed body - overrides={len(overrides)}, "
                       f"date_overrides={len(date_overrides)}, selected_rows={selected_rows}")

        # Create importer
        importer = BankStatementImport(
            bank_code=bank_code,
            use_enhanced_matching=True,
            use_fingerprinting=True
        )

        # Handle PDF files with AI extraction (same as preview endpoint)
        if filename.lower().endswith('.pdf'):
            import tempfile
            import os
            from sql_rag.statement_reconcile import StatementReconciler
            from sql_rag.bank_import import BankTransaction

            # Save to temp file for AI extraction
            with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp_file:
                tmp_file.write(content_bytes)
                tmp_path = tmp_file.name

            try:
                # Use StatementReconciler for AI extraction
                reconciler = StatementReconciler(sql_connector, config=config)
                statement_info, stmt_transactions = reconciler.extract_transactions_from_pdf(tmp_path)

                # Validate bank account match before importing
                if statement_info and statement_info.sort_code and statement_info.account_number:
                    stmt_sort = (statement_info.sort_code or '').replace('-', '').replace(' ', '').strip()
                    stmt_acct = (statement_info.account_number or '').replace('-', '').replace(' ', '').strip()
                    # Get Opera bank details
                    bank_df = sql_connector.execute_query(
                        "SELECT RTRIM(nk_sort) as sort_code, RTRIM(nk_number) as account_number FROM nbank WITH (NOLOCK) WHERE nk_acnt = :bank_code",
                        {'bank_code': bank_code}
                    )
                    if bank_df is not None and not bank_df.empty:
                        opera_sort = (bank_df.iloc[0]['sort_code'] or '').replace('-', '').replace(' ', '').strip()
                        opera_acct = (bank_df.iloc[0]['account_number'] or '').replace('-', '').replace(' ', '').strip()
                        if stmt_sort and opera_sort and stmt_sort != opera_sort:
                            return {"success": False, "error": f"Bank account mismatch: statement sort code {statement_info.sort_code} does not match Opera bank {bank_code} sort code. Please select the correct bank."}
                        if stmt_acct and opera_acct and stmt_acct != opera_acct:
                            return {"success": False, "error": f"Bank account mismatch: statement account {statement_info.account_number} does not match Opera bank {bank_code} account. Please select the correct bank."}

                # Check for statement period overlap (prevent double-posting)
                skip_overlap_check = request_body.get('skip_overlap_check', False) if request_body else False
                if not skip_overlap_check and email_storage and statement_info:
                    period_start_str = statement_info.period_start.isoformat() if hasattr(statement_info, 'period_start') and statement_info.period_start else None
                    period_end_str = statement_info.period_end.isoformat() if hasattr(statement_info, 'period_end') and statement_info.period_end else None
                    # Fall back to first/last transaction dates
                    if not period_start_str and stmt_transactions:
                        dates = [st.date for st in stmt_transactions if st.date]
                        if dates:
                            period_start_str = min(dates).isoformat() if hasattr(min(dates), 'isoformat') else str(min(dates))
                            period_end_str = max(dates).isoformat() if hasattr(max(dates), 'isoformat') else str(max(dates))

                    if period_start_str and period_end_str:
                        overlap = email_storage.check_period_overlap(
                            bank_code=bank_code,
                            period_start=period_start_str,
                            period_end=period_end_str,
                            exclude_import_id=resume_import_id
                        )
                        if overlap:
                            return {
                                "success": False,
                                "overlap_warning": True,
                                "error": f"Statement period overlaps with a previously imported statement",
                                "overlap_details": {
                                    "existing_import_id": overlap['import_id'],
                                    "existing_filename": overlap['filename'],
                                    "existing_period": f"{overlap['period_start']} to {overlap['period_end']}",
                                    "existing_import_date": overlap['import_date'],
                                    "new_period": f"{period_start_str} to {period_end_str}"
                                }
                            }

                # Convert StatementTransaction to BankTransaction format
                transactions = []
                for i, st in enumerate(stmt_transactions, start=1):
                    txn = BankTransaction(
                        row_number=i,
                        date=st.date,
                        amount=st.amount,
                        subcategory=st.transaction_type or '',
                        memo=st.description or '',
                        name=st.description or '',
                        reference=st.reference or '',
                        fit_id=''
                    )
                    transactions.append(txn)

                detected_format = "PDF (AI Extraction)"
            finally:
                # Clean up temp file
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
        else:
            # Decode bytes to string for CSV/OFX/QIF/MT940
            try:
                content = content_bytes.decode('utf-8')
            except UnicodeDecodeError:
                content = content_bytes.decode('latin-1')

            transactions, detected_format = importer.parse_content(content, filename)
        importer.process_transactions(transactions)

        # Apply date overrides
        date_override_map = {d['row']: d['date'] for d in date_overrides}
        for txn in transactions:
            if txn.row_number in date_override_map:
                new_date_str = date_override_map[txn.row_number]
                txn.original_date = txn.date
                txn.date = datetime.strptime(new_date_str, '%Y-%m-%d').date()

        # Apply manual overrides
        override_map = {o['row']: o for o in overrides}
        for txn in transactions:
            if txn.row_number in override_map:
                override = override_map[txn.row_number]
                if override.get('account'):
                    txn.manual_account = override.get('account')
                    txn.manual_ledger_type = override.get('ledger_type')

                # Apply cashbook type override
                if override.get('cbtype'):
                    txn.cbtype = override.get('cbtype')

                transaction_type = override.get('transaction_type')
                if transaction_type and transaction_type in ('sales_receipt', 'purchase_payment', 'sales_refund', 'purchase_refund', 'nominal_payment', 'nominal_receipt', 'bank_transfer'):
                    txn.action = transaction_type
                    # Store bank transfer details on the transaction
                    if transaction_type == 'bank_transfer':
                        txn.bank_transfer_details = override.get('bank_transfer_details', {})
                elif override.get('ledger_type') == 'C':
                    txn.action = 'sales_receipt'
                elif override.get('ledger_type') == 'S':
                    txn.action = 'purchase_payment'
                elif override.get('ledger_type') == 'N':
                    txn.action = 'nominal_payment' if txn.amount < 0 else 'nominal_receipt'
                # Apply project/department/VAT codes for nominal entries
                if override.get('project_code'):
                    txn.project_code = override['project_code']
                if override.get('department_code'):
                    txn.department_code = override['department_code']
                if override.get('vat_code'):
                    txn.vat_code = override['vat_code']

        # Validate periods
        period_info = get_current_period_info(sql_connector)
        period_violations = []

        for txn in transactions:
            if selected_rows is not None and txn.row_number not in selected_rows:
                continue
            if txn.action not in ('sales_receipt', 'purchase_payment', 'sales_refund', 'purchase_refund', 'nominal_payment', 'nominal_receipt', 'bank_transfer'):
                continue
            if txn.is_duplicate:
                continue

            ledger_type = get_ledger_type_for_transaction(txn.action)
            period_result = validate_posting_period(sql_connector, txn.date, ledger_type)

            if not period_result.is_valid:
                ledger_names = {'SL': 'Sales Ledger', 'PL': 'Purchase Ledger', 'NL': 'Nominal Ledger'}
                period_violations.append({
                    "row": txn.row_number,
                    "date": txn.date.isoformat(),
                    "name": txn.name,
                    "amount": txn.amount,
                    "action": txn.action,
                    "ledger_type": ledger_type,
                    "ledger_name": ledger_names.get(ledger_type, ledger_type),
                    "error": period_result.error_message,
                    "year": period_result.year,
                    "period": period_result.period
                })

        if period_violations:
            return {
                "success": False,
                "error": "Cannot import - some transactions are in blocked periods",
                "period_violations": period_violations,
                "period_info": {
                    "current_year": period_info.get('np_year'),
                    "current_period": period_info.get('np_perno')
                }
            }

        # Check for unprocessed repeat entries in OPEN periods only
        # Period-blocked repeat entries are silently skipped (they can't be posted anyway)
        unprocessed_repeat_entries = []
        for txn in transactions:
            if txn.action == 'repeat_entry' and getattr(txn, 'period_valid', True):
                unprocessed_repeat_entries.append({
                    "row": txn.row_number,
                    "name": txn.name,
                    "amount": txn.amount,
                    "date": txn.date.isoformat(),
                    "entry_ref": getattr(txn, 'repeat_entry_ref', None),
                    "entry_desc": getattr(txn, 'repeat_entry_desc', None)
                })

        if unprocessed_repeat_entries:
            return {
                "success": False,
                "error": "Cannot import - there are unprocessed repeat entries",
                "repeat_entries": unprocessed_repeat_entries,
                "message": "Please run Opera's Repeat Entries routine first"
            }

        # Acquire bank-level import lock to prevent concurrent imports
        from sql_rag.import_lock import acquire_import_lock, release_import_lock
        if not acquire_import_lock(_bank_lock_key(bank_code), locked_by="api", endpoint="import-from-email"):
            return {"success": False, "error": f"Bank account {bank_code} is currently being imported by another user. Please wait for the current import to complete."}

        # Load already-posted lines for partial recovery (skip on resume)
        already_posted_email = {}
        if resume_import_id and email_storage:
            try:
                already_posted_email = email_storage.get_posted_lines(resume_import_id)
                if already_posted_email:
                    logger.info(f"Resume email import: {len(already_posted_email)} lines already posted for import_id={resume_import_id}")
            except Exception as e:
                logger.warning(f"Could not load posted lines for email resume: {e}")

        # Import transactions
        imported = []
        errors = []
        skipped_not_selected = 0
        skipped_incomplete = 0
        skipped_duplicates = 0
        skipped_already_posted = 0

        for txn in transactions:
            if selected_rows is not None and txn.row_number not in selected_rows:
                skipped_not_selected += 1
                continue

            # Skip already-posted lines (partial recovery resume)
            if txn.row_number in already_posted_email:
                skipped_already_posted += 1
                imported.append({
                    "row": txn.row_number,
                    "date": txn.date.isoformat(),
                    "amount": txn.amount,
                    "account": txn.manual_account or txn.matched_account or '',
                    "account_name": txn.matched_name or '',
                    "action": txn.action,
                    "entry_number": already_posted_email[txn.row_number],
                    "name": txn.name or '',
                    "already_posted": True
                })
                continue

            # Log transaction details for selected rows
            logger.info(f"Processing row {txn.row_number}: action={txn.action}, "
                       f"account={txn.manual_account or txn.matched_account}, "
                       f"is_duplicate={txn.is_duplicate}, amount={txn.amount}")

            # Handle bank transfers separately (paired entries in two banks)
            if txn.action == 'bank_transfer' and not txn.is_duplicate:
                bt_details = getattr(txn, 'bank_transfer_details', {}) or {}
                dest_bank = bt_details.get('dest_bank') or txn.manual_account
                if not dest_bank:
                    errors.append({"row": txn.row_number, "error": "Bank transfer missing destination bank"})
                    continue

                amount = abs(txn.amount)
                # Direction: negative = paying out (current bank is source), positive = receiving in
                if txn.amount < 0:
                    source, dest = bank_code, dest_bank
                else:
                    source, dest = dest_bank, bank_code

                try:
                    from sql_rag.opera_sql_import import OperaSQLImport
                    opera_import = OperaSQLImport(sql_connector)
                    bt_result = opera_import.import_bank_transfer(
                        source_bank=source,
                        dest_bank=dest,
                        amount_pounds=amount,
                        reference=(bt_details.get('reference') or txn.reference or '')[:20],
                        post_date=txn.date,
                        comment=(bt_details.get('comment') or txn.memo or '')[:50],
                        input_by='SQLRAG'
                    )
                    if bt_result.get('success'):
                        imported.append({
                            "row": txn.row_number,
                            "date": txn.date.isoformat() if txn.date else None,
                            "amount": txn.amount,
                            "account": dest_bank if txn.amount < 0 else source,
                            "account_name": f"Transfer {'to' if txn.amount < 0 else 'from'} {dest_bank if txn.amount < 0 else source}",
                            "action": 'bank_transfer',
                            "entry_number": bt_result.get('source_entry') or bt_result.get('dest_entry'),
                            "name": txn.name or '',
                            "memo": txn.memo or '',
                            "reference": txn.reference or '',
                            "allocated": False,
                            "allocation_result": None
                        })
                    else:
                        errors.append({"row": txn.row_number, "error": bt_result.get('error', 'Bank transfer failed')})
                except Exception as e:
                    errors.append({"row": txn.row_number, "error": f"Bank transfer error: {str(e)}"})
                continue

            if txn.action in ('sales_receipt', 'purchase_payment', 'sales_refund', 'purchase_refund', 'nominal_payment', 'nominal_receipt') and not txn.is_duplicate:
                account = txn.manual_account or txn.matched_account
                if not account:
                    skipped_incomplete += 1
                    errors.append({"row": txn.row_number, "error": "Missing account"})
                    continue

                if not txn.action or txn.action not in ('sales_receipt', 'purchase_payment', 'sales_refund', 'purchase_refund', 'nominal_payment', 'nominal_receipt'):
                    skipped_incomplete += 1
                    errors.append({"row": txn.row_number, "error": "Missing transaction type"})
                    continue

                # Just-in-time duplicate check - catches entries that appeared since statement was processed
                try:
                    from sql_rag.opera_sql_import import OperaSQLImport as _OI
                    _oi = _OI(sql_connector)
                    acct_type = 'customer' if txn.action in ('sales_receipt', 'sales_refund') else 'supplier' if txn.action in ('purchase_payment', 'purchase_refund') else 'nominal'
                    dup_check = _oi.check_duplicate_before_posting(
                        bank_account=bank_code,
                        transaction_date=txn.date,
                        amount_pounds=abs(txn.amount),
                        account_code=account,
                        account_type=acct_type
                    )
                    if dup_check['is_duplicate']:
                        skipped_duplicates += 1
                        errors.append({"row": txn.row_number, "error": f"Skipped - {dup_check['details']}"})
                        logger.warning(f"Row {txn.row_number}: Pre-posting duplicate detected - {dup_check['details']}")
                        continue
                except Exception as dup_err:
                    logger.warning(f"Row {txn.row_number}: Pre-posting duplicate check failed: {dup_err}")

                try:
                    result = importer.import_transaction(txn, validate_only=False)
                    if result.success:
                        import_record = {
                            "row": txn.row_number,
                            "date": txn.date.isoformat(),
                            "amount": txn.amount,
                            "account": txn.manual_account or txn.matched_account,
                            "account_name": txn.matched_name or '',
                            "action": txn.action,
                            "batch_ref": getattr(result, 'batch_ref', None) or getattr(result, 'batch_number', None),
                            "entry_number": getattr(result, 'entry_number', None),  # For reconciliation
                            "name": txn.name or '',
                            "memo": txn.memo or '',
                            "reference": txn.reference or '',
                            "allocated": False,
                            "allocation_result": None
                        }

                        # Auto-allocate if enabled
                        if auto_allocate and txn.action in ('sales_receipt', 'purchase_payment'):
                            from sql_rag.opera_sql_import import OperaSQLImport
                            opera_import = OperaSQLImport(sql_connector)
                            account_code = txn.manual_account or txn.matched_account
                            # Get the reference from the import result
                            txn_ref = getattr(result, 'transaction_ref', None) or txn.reference or txn.name[:20]

                            if txn.action == 'sales_receipt':
                                alloc_result = opera_import.auto_allocate_receipt(
                                    customer_account=account_code,
                                    receipt_ref=txn_ref,
                                    receipt_amount=abs(txn.amount),
                                    allocation_date=txn.date,
                                    bank_account=bank_code,
                                    description=txn.memo or txn.name
                                )
                                import_record["allocated"] = alloc_result.get("success", False)
                                import_record["allocation_result"] = alloc_result
                            elif txn.action == 'purchase_payment':
                                alloc_result = opera_import.auto_allocate_payment(
                                    supplier_account=account_code,
                                    payment_ref=txn_ref,
                                    payment_amount=abs(txn.amount),
                                    allocation_date=txn.date,
                                    bank_account=bank_code,
                                    description=txn.memo or txn.name
                                )
                                import_record["allocated"] = alloc_result.get("success", False)
                                import_record["allocation_result"] = alloc_result

                        imported.append(import_record)

                        # Save alias for manual overrides
                        if txn.manual_account and importer.alias_manager:
                            inferred_ledger = 'C' if txn.action in ('sales_receipt', 'sales_refund') else 'S'
                            importer.alias_manager.save_alias(
                                bank_name=txn.name,
                                ledger_type=txn.manual_ledger_type or inferred_ledger,
                                account_code=txn.manual_account,
                                match_score=1.0,
                                created_by='MANUAL_IMPORT'
                            )
                    else:
                        error_msg = '; '.join(result.errors) if result.errors else 'Import failed'
                        errors.append({"row": txn.row_number, "error": error_msg})
                except Exception as e:
                    errors.append({"row": txn.row_number, "error": str(e)})

        # Calculate totals by action type
        receipts_imported = sum(1 for t in imported if t['action'] == 'sales_receipt')
        payments_imported = sum(1 for t in imported if t['action'] == 'purchase_payment')
        refunds_imported = sum(1 for t in imported if t['action'] in ('sales_refund', 'purchase_refund'))
        transfers_imported = sum(1 for t in imported if t['action'] == 'bank_transfer')

        # Calculate amounts
        total_receipts = sum(t['amount'] for t in imported if t['action'] == 'sales_receipt')
        total_payments = sum(abs(t['amount']) for t in imported if t['action'] == 'purchase_payment')

        # Calculate allocation stats
        allocations_attempted = sum(1 for t in imported if t.get('allocation_result'))
        allocations_successful = sum(1 for t in imported if t.get('allocated', False))

        # Record successful import in tracking table
        if len(imported) > 0:
            # Build statement metadata from extraction if available
            stmt_opening = None
            stmt_closing = None
            stmt_date_str = None
            stmt_acct_num = None
            stmt_sort_code = None
            stmt_period_start = None
            stmt_period_end = None
            if filename.lower().endswith('.pdf') and statement_info:
                stmt_opening = statement_info.opening_balance
                stmt_closing = statement_info.closing_balance
                stmt_date_str = statement_info.statement_date.isoformat() if statement_info.statement_date else None
                stmt_acct_num = statement_info.account_number
                stmt_sort_code = statement_info.sort_code
                stmt_period_start = statement_info.period_start.isoformat() if hasattr(statement_info, 'period_start') and statement_info.period_start else None
                stmt_period_end = statement_info.period_end.isoformat() if hasattr(statement_info, 'period_end') and statement_info.period_end else None

            import_record_id = email_storage.record_bank_statement_import(
                bank_code=bank_code,
                filename=filename,
                transactions_imported=len(imported),
                source='email',
                target_system='opera_se',
                email_id=email_id,
                attachment_id=attachment_id,
                total_receipts=total_receipts,
                total_payments=total_payments,
                imported_by='BANK_IMPORT',
                opening_balance=stmt_opening,
                closing_balance=stmt_closing,
                statement_date=stmt_date_str,
                account_number=stmt_acct_num,
                sort_code=stmt_sort_code,
                period_start=stmt_period_start,
                period_end=stmt_period_end
            )
            result_response = {
                "success": True,
                "import_id": import_record_id,
            }

            # Persist statement transactions for reconciliation lifecycle
            if filename.lower().endswith('.pdf') and stmt_transactions and import_record_id:
                try:
                    raw_txns = [
                        {
                            "line_number": i,
                            "date": st.date.isoformat() if hasattr(st.date, 'isoformat') else str(st.date),
                            "description": st.description or '',
                            "amount": st.amount,
                            "balance": st.balance,
                            "transaction_type": st.transaction_type or '',
                            "reference": st.reference or ''
                        }
                        for i, st in enumerate(stmt_transactions, start=1)
                    ]
                    email_storage.save_statement_transactions(
                        import_id=import_record_id,
                        transactions=raw_txns,
                        statement_info={
                            'opening_balance': stmt_opening,
                            'closing_balance': stmt_closing,
                            'statement_date': stmt_date_str,
                            'account_number': stmt_acct_num,
                            'sort_code': stmt_sort_code,
                        }
                    )
                    logger.info(f"Saved {len(raw_txns)} statement transactions for email import_id={import_record_id}")

                    # Mark successfully imported transactions as posted (for partial recovery)
                    for imp_txn in imported:
                        entry_num = imp_txn.get('entry_number')
                        row_num = imp_txn.get('row')
                        if entry_num and row_num:
                            email_storage.mark_transaction_posted(import_record_id, row_num, str(entry_num))
                except Exception as txn_err:
                    logger.warning(f"Could not save statement transactions: {txn_err}")

            # Learn patterns from successful imports
            try:
                from sql_rag.bank_patterns import BankPatternLearner
                pattern_learner = BankPatternLearner(company_code=sql_connector.company_code if hasattr(sql_connector, 'company_code') else 'default')

                # Learn from overrides (user's explicit choices)
                for override in overrides:
                    if override.get('account') and override.get('ledger_type'):
                        # Find the transaction memo
                        txn = next((t for t in transactions if t.row_number == override.get('row')), None)
                        if txn:
                            pattern_learner.learn_pattern(
                                description=txn.memo or txn.name,
                                transaction_type=override.get('transaction_type', 'PI' if txn.amount < 0 else 'SI'),
                                account_code=override['account'],
                                account_name=override.get('account_name'),
                                ledger_type=override['ledger_type'],
                                vat_code=override.get('vat_code'),
                                nominal_code=override.get('nominal_code'),
                                net_amount=override.get('net_amount')
                            )
                logger.info(f"Learned patterns from {len(overrides)} overrides")
            except Exception as e:
                logger.warning(f"Could not learn patterns: {e}")

        # Archive the email after successful import (move to archive folder)
        archive_status = "not_attempted"
        archive_folder = "Archive/Bank Statements"
        if len(imported) > 0 and email_sync_manager:
            try:
                if provider_id and message_id and provider_id in email_sync_manager.providers:
                    provider = email_sync_manager.providers[provider_id]
                    # Move email to archive folder
                    move_success = await provider.move_email(
                        message_id=message_id,
                        source_folder=folder_id,
                        dest_folder=archive_folder
                    )
                    archive_status = "archived" if move_success else "move_failed"
                    if move_success:
                        logger.info(f"Archived bank statement email {email_id} to {archive_folder}")
                    else:
                        logger.warning(f"Failed to archive bank statement email {email_id}")
                else:
                    archive_status = "provider_not_available"
            except Exception as archive_err:
                logger.warning(f"Failed to archive bank statement email: {archive_err}")
                archive_status = f"error: {str(archive_err)}"

        # Log import summary
        logger.info(f"Bank import complete: {len(imported)} imported, {len(errors)} errors, "
                    f"{skipped_not_selected} not selected, {skipped_incomplete} incomplete")
        if selected_rows is not None:
            logger.info(f"Selected rows: {selected_rows}")

        # Auto-reconciliation: mark imported entries as reconciled
        reconciliation_result = None
        if auto_reconcile and len(imported) > 0 and len(errors) == 0:
            try:
                from sql_rag.opera_sql_import import OperaSQLImport

                # Collect entries with valid entry_numbers
                entries_to_reconcile = []
                statement_line = 10

                for txn in imported:
                    entry_num = txn.get('entry_number')
                    if entry_num:
                        entries_to_reconcile.append({
                            'entry_number': entry_num,
                            'statement_line': statement_line
                        })
                        statement_line += 10

                if len(entries_to_reconcile) == len(imported):
                    # All entries have entry_numbers - proceed
                    latest_date = None
                    for txn in imported:
                        if txn.get('date'):
                            txn_date = datetime.strptime(txn['date'], '%Y-%m-%d').date() if isinstance(txn['date'], str) else txn['date']
                            if latest_date is None or txn_date > latest_date:
                                latest_date = txn_date

                    if latest_date is None:
                        latest_date = datetime.now().date()

                    statement_number = int(latest_date.strftime('%y%m%d'))

                    opera_import = OperaSQLImport(sql_connector)
                    recon_result = opera_import.mark_entries_reconciled(
                        bank_account=bank_code,
                        entries=entries_to_reconcile,
                        statement_number=statement_number,
                        statement_date=latest_date,
                        reconciliation_date=datetime.now().date()
                    )

                    reconciliation_result = {
                        "success": recon_result.success,
                        "entries_reconciled": recon_result.records_imported if recon_result.success else 0,
                        "statement_number": statement_number,
                        "statement_date": latest_date.isoformat(),
                        "messages": recon_result.warnings if recon_result.success else recon_result.errors
                    }

                    if recon_result.success:
                        logger.info(f"Email auto-reconciliation complete: {len(entries_to_reconcile)} entries")
                    else:
                        logger.warning(f"Email auto-reconciliation failed: {recon_result.errors}")
                else:
                    missing_count = len(imported) - len(entries_to_reconcile)
                    reconciliation_result = {
                        "success": False,
                        "entries_reconciled": 0,
                        "messages": [f"Cannot auto-reconcile: {missing_count} entries missing entry_number"]
                    }

            except Exception as recon_err:
                logger.error(f"Email auto-reconciliation error: {recon_err}")
                reconciliation_result = {
                    "success": False,
                    "entries_reconciled": 0,
                    "messages": [f"Auto-reconciliation error: {str(recon_err)}"]
                }

        release_import_lock(_bank_lock_key(bank_code))
        return {
            "success": len(imported) > 0,  # Success if any transactions were imported
            "source": "email",
            "email_id": email_id,
            "attachment_id": attachment_id,
            "filename": filename,
            "import_id": import_record_id if len(imported) > 0 else None,
            "imported_count": len(imported),
            "imported_transactions_count": len(imported),  # Frontend expects this name
            "receipts_imported": receipts_imported,
            "payments_imported": payments_imported,
            "refunds_imported": refunds_imported,
            "transfers_imported": transfers_imported,
            "total_receipts": total_receipts,
            "total_payments": total_payments,
            "total_amount": abs(total_receipts) + abs(total_payments),
            "skipped_not_selected": skipped_not_selected,
            "skipped_incomplete": skipped_incomplete,
            "skipped_duplicates": skipped_duplicates,
            "skipped_rejected": skipped_not_selected + skipped_incomplete + skipped_duplicates,
            "imported_transactions": imported,
            "errors": errors,
            "archive_status": archive_status,
            "auto_allocate_enabled": auto_allocate,
            "allocations_attempted": allocations_attempted,
            "allocations_successful": allocations_successful,
            "auto_reconcile_enabled": auto_reconcile,
            "reconciliation_result": reconciliation_result
        }

    except Exception as e:
        release_import_lock(_bank_lock_key(bank_code))
        logger.error(f"Error importing bank statement from email: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/bank-import/import-history")
async def get_bank_statement_import_history(
    bank_code: Optional[str] = Query(None, description="Filter by bank code"),
    limit: int = Query(50, description="Maximum records to return"),
    from_date: Optional[str] = Query(None, description="Filter from date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="Filter to date (YYYY-MM-DD)")
):
    """
    Get history of bank statement imports (both file and email).

    Returns list of import records with email details (for email imports).
    """
    if not email_storage:
        return {"success": False, "error": "Email storage not configured"}

    try:
        history = email_storage.get_bank_statement_import_history(
            bank_code=bank_code,
            target_system='opera_se',
            from_date=from_date,
            to_date=to_date,
            limit=limit
        )
        return {
            "success": True,
            "imports": history,
            "count": len(history)
        }
    except Exception as e:
        logger.error(f"Error getting import history: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/bank-import/statement-review/{import_id}")
async def get_statement_review(import_id: int):
    """
    Get statement transactions with Opera reconciliation status for review.

    Returns all transactions from a bank statement import in statement order,
    enriched with whether each posted entry has been reconciled in Opera.
    """
    if not email_storage:
        return {"success": False, "error": "Email storage not configured"}

    try:
        # Get transactions from SQLite
        transactions = email_storage.get_statement_transactions(import_id)
        if not transactions:
            return {"success": True, "import_id": import_id, "transactions": [], "summary": {"total": 0, "reconciled": 0, "unreconciled": 0, "not_imported": 0}}

        # Get import metadata for filename, bank_code, balances
        import_meta = None
        try:
            with email_storage._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT filename, bank_code, opening_balance, closing_balance FROM bank_statement_imports WHERE id = ?",
                    (import_id,)
                )
                row = cursor.fetchone()
                if row:
                    import_meta = dict(row)
        except Exception as e:
            logger.warning(f"Could not fetch import metadata for {import_id}: {e}")

        # Collect posted entry numbers for batch Opera lookup
        posted_entries = [t['posted_entry_number'] for t in transactions if t.get('posted_entry_number')]

        # Batch query Opera for reconciliation status
        reconciled_entries = set()
        if posted_entries and sql_connector:
            try:
                from sqlalchemy import text as sa_text
                param_dict = {f'p{i}': v for i, v in enumerate(posted_entries)}
                placeholders = ','.join([f':p{i}' for i in range(len(posted_entries))])
                query = f"SELECT ae_entry FROM aentry WHERE ae_entry IN ({placeholders}) AND ISNULL(ae_reclnum, 0) > 0"
                with sql_connector.engine.connect() as conn:
                    result = conn.execute(sa_text(query), param_dict)
                    reconciled_entries = {row[0].strip() if isinstance(row[0], str) else str(row[0]) for row in result}
            except Exception as e:
                logger.warning(f"Could not query Opera reconciliation status: {e}")

        # Build response transactions
        result_txns = []
        reconciled_count = 0
        unreconciled_count = 0
        not_imported_count = 0

        for t in transactions:
            entry_num = t.get('posted_entry_number')
            if entry_num:
                entry_stripped = entry_num.strip() if isinstance(entry_num, str) else str(entry_num)
                is_reconciled = entry_stripped in reconciled_entries
                if is_reconciled:
                    reconciled_count += 1
                else:
                    unreconciled_count += 1
            else:
                is_reconciled = None
                not_imported_count += 1

            result_txns.append({
                "line_number": t.get('line_number'),
                "date": t.get('date'),
                "description": t.get('description'),
                "amount": t.get('amount'),
                "balance": t.get('balance'),
                "posted_entry_number": entry_num,
                "is_reconciled": is_reconciled,
            })

        return {
            "success": True,
            "import_id": import_id,
            "filename": import_meta.get('filename') if import_meta else None,
            "bank_code": import_meta.get('bank_code') if import_meta else None,
            "opening_balance": import_meta.get('opening_balance') if import_meta else None,
            "closing_balance": import_meta.get('closing_balance') if import_meta else None,
            "transactions": result_txns,
            "summary": {
                "total": len(result_txns),
                "reconciled": reconciled_count,
                "unreconciled": unreconciled_count,
                "not_imported": not_imported_count,
            }
        }
    except Exception as e:
        logger.error(f"Error getting statement review for import {import_id}: {e}")
        return {"success": False, "error": str(e)}





@router.delete("/api/bank-import/import-history/{record_id}")
async def delete_bank_statement_import_record(record_id: int):
    """
    Delete a single bank statement import history record to allow re-importing.

    This removes the tracking record so the statement can be imported again.
    Does not affect Opera data - only the import tracking.

    Use case: After restoring Opera data, may need to re-import old statements.
    """
    if not email_storage:
        return {"success": False, "error": "Email storage not configured"}

    try:
        deleted = email_storage.delete_bank_statement_import_record(record_id)
        if deleted:
            return {
                "success": True,
                "message": "Import record deleted - statement can now be re-imported"
            }
        else:
            return {
                "success": False,
                "error": f"Record {record_id} not found"
            }
    except Exception as e:
        logger.error(f"Error deleting import record: {e}")
        return {"success": False, "error": str(e)}





@router.delete("/api/bank-import/import-history")
async def clear_bank_statement_import_history(
    bank_code: Optional[str] = Query(None, description="Filter by bank code"),
    from_date: Optional[str] = Query(None, description="Clear from date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="Clear to date (YYYY-MM-DD)")
):
    """
    Clear bank statement import history within optional filters.

    If no filters specified, clears ALL history.
    Returns number of records deleted.
    """
    if not email_storage:
        return {"success": False, "error": "Email storage not configured"}

    try:
        deleted_count = email_storage.clear_bank_statement_import_history(
            bank_code=bank_code,
            from_date=from_date,
            to_date=to_date
        )
        return {
            "success": True,
            "deleted_count": deleted_count,
            "message": f"Cleared {deleted_count} import history records"
        }
    except Exception as e:
        logger.error(f"Error clearing import history: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/bank-import/email-import-history")
async def get_bank_statement_email_import_history_legacy(
    bank_code: Optional[str] = Query(None, description="Filter by bank code"),
    limit: int = Query(50, description="Maximum records to return")
):
    """
    Legacy endpoint - use /api/bank-import/import-history instead.
    Get history of bank statements imported from email.
    """
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        history = email_storage.get_bank_statement_import_history(bank_code=bank_code, limit=limit)
        return {
            "success": True,
            "history": history,
            "count": len(history)
        }
    except Exception as e:
        logger.error(f"Error getting email import history: {e}")
        return {"success": False, "error": str(e)}





@router.post("/api/bank-reconciliation/validate-statement")
async def validate_statement_for_reconciliation(
    bank_code: str = Query(..., description="Bank account code"),
    opening_balance: float = Query(..., description="Statement opening balance (pounds)"),
    closing_balance: float = Query(..., description="Statement closing balance (pounds)"),
    statement_number: Optional[int] = Query(None, description="Statement number (if on statement)"),
    statement_date: str = Query(..., description="Statement date (YYYY-MM-DD)")
):
    """
    Validate that a statement is ready for reconciliation.

    Checks:
    - Opening balance matches Opera's expected (nk_recbal)
    - Statement is next in sequence

    Returns validation result with expected values or error message.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from datetime import datetime
        from sql_rag.opera_sql_import import OperaSQLImport

        # Parse statement date
        stmt_date = datetime.strptime(statement_date, '%Y-%m-%d').date()

        opera_import = OperaSQLImport(sql_connector)
        result = opera_import.validate_statement_for_reconciliation(
            bank_account=bank_code,
            opening_balance=opening_balance,
            closing_balance=closing_balance,
            statement_number=statement_number,
            statement_date=stmt_date
        )

        return result

    except Exception as e:
        logger.error(f"Error validating statement: {e}")
        return {"valid": False, "error_message": str(e)}





@router.post("/api/bank-reconciliation/match-statement")
async def match_statement_to_cashbook(
    bank_code: str = Query(..., description="Bank account code"),
    date_tolerance_days: int = Query(3, description="Days tolerance for date matching"),
    import_id: Optional[int] = Query(None, description="Import record ID - if provided, loads transactions from DB and persists match results"),
    request_body: Dict[str, Any] = Body(None)
):
    """
    Match statement lines to unreconciled cashbook entries.

    If import_id is provided, transactions are loaded from the DB (bank_statement_transactions table)
    and match results are persisted back. If not, transactions must be in the request body.

    Request body format:
    {
        "statement_transactions": [
            {
                "line_number": 1,
                "date": "2026-02-09",
                "amount": 500.00,
                "reference": "BACS-12345",
                "description": "Payment from Customer Ltd"
            },
            ...
        ]
    }

    Returns:
    - auto_matched: Confident matches (imported entries) - green
    - suggested_matched: Probable matches (existing entries) - amber, user reviews
    - unmatched_statement: Statement lines with no match - red
    - unmatched_cashbook: Cashbook entries not on statement
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    # Load transactions from DB if import_id provided, otherwise from request body
    statement_transactions = None
    logger.info(f"match-statement called: bank_code={bank_code}, import_id={import_id}, has_body={request_body is not None}")
    if import_id and email_storage:
        db_txns = email_storage.get_statement_transactions(import_id)
        if db_txns:
            statement_transactions = [
                {
                    "line_number": t['line_number'],
                    "date": t['date'],
                    "amount": t['amount'],
                    "reference": t.get('reference', ''),
                    "description": t.get('description', ''),
                    "balance": t.get('balance'),
                    "transaction_type": t.get('transaction_type', '')
                }
                for t in db_txns
            ]
            logger.info(f"Loaded {len(statement_transactions)} statement transactions from DB for import_id={import_id}")
            if statement_transactions:
                sample = statement_transactions[0]
                logger.info(f"  DB sample: line={sample['line_number']}, amount={sample['amount']}, date={sample['date']}, ref='{sample.get('reference','')}'")
        else:
            logger.warning(f"No transactions found in DB for import_id={import_id}")

    if not statement_transactions:
        if not request_body or 'statement_transactions' not in request_body:
            logger.warning(f"No statement_transactions available: import_id={import_id}, request_body keys={list(request_body.keys()) if request_body else 'None'}")
            return {"success": False, "error": "Request body must include statement_transactions (or provide import_id)"}
        statement_transactions = request_body['statement_transactions']
        logger.info(f"Using {len(statement_transactions)} statement transactions from request body")

    try:
        from sql_rag.opera_sql_import import OperaSQLImport

        opera_import = OperaSQLImport(sql_connector)
        result = opera_import.match_statement_to_cashbook(
            bank_account=bank_code,
            statement_transactions=statement_transactions,
            date_tolerance_days=date_tolerance_days
        )

        # Filter out ignored transactions from unmatched list
        if email_storage and result.get('success') and result.get('unmatched_statement'):
            filtered_unmatched = []
            ignored_count = 0
            for txn in result['unmatched_statement']:
                txn_date = txn.get('statement_date', '')
                txn_amount = txn.get('statement_amount', 0)
                if email_storage.is_transaction_ignored(bank_code, txn_date, txn_amount):
                    ignored_count += 1
                else:
                    filtered_unmatched.append(txn)
            if ignored_count > 0:
                logger.info(f"Filtered {ignored_count} ignored transaction(s) from unmatched list")
            result['unmatched_statement'] = filtered_unmatched

        # Persist match results to DB if import_id provided
        if import_id and email_storage and result.get('success'):
            try:
                matches_to_save = []
                for m in result.get('auto_matched', []):
                    matches_to_save.append({
                        'line_number': m.get('statement_line'),
                        'matched_entry': m.get('entry_number'),
                        'match_confidence': m.get('confidence', 1.0),
                        'match_type': 'auto'
                    })
                for m in result.get('suggested_matched', []):
                    matches_to_save.append({
                        'line_number': m.get('statement_line'),
                        'matched_entry': m.get('entry_number'),
                        'match_confidence': m.get('confidence', 0.5),
                        'match_type': 'suggested'
                    })
                if matches_to_save:
                    email_storage.update_transaction_matches_bulk(import_id, matches_to_save)
                    logger.info(f"Persisted {len(matches_to_save)} match results for import_id={import_id}")
            except Exception as match_err:
                logger.warning(f"Could not persist match results: {match_err}")

        # Include import_id in response
        if import_id:
            result['import_id'] = import_id

        return result

    except Exception as e:
        logger.error(f"Error matching statement: {e}")
        return {"success": False, "error": str(e)}





@router.post("/api/bank-reconciliation/complete")
async def complete_reconciliation(
    bank_code: str = Query(..., description="Bank account code"),
    statement_number: int = Query(..., description="Statement number"),
    statement_date: str = Query(..., description="Statement date (YYYY-MM-DD)"),
    closing_balance: float = Query(..., description="Statement closing balance (pounds)"),
    partial: bool = Query(False, description="Partial reconciliation - skip closing balance validation"),
    import_id: Optional[int] = Query(None, description="Import record ID for persisting reconciliation status"),
    request_body: Dict[str, Any] = Body(None)
):
    """
    Complete bank reconciliation - mark all matched entries as reconciled.

    Request body format:
    {
        "matched_entries": [
            {"entry_number": "R200001234", "statement_line": 1},
            {"entry_number": "P100008036", "statement_line": 2},
            ...
        ],
        "statement_transactions": [
            {"line_number": 1, ...},  # Original statement lines for gap calculation
            ...
        ]
    }

    If import_id is provided, statement_transactions can be loaded from DB.
    On success, marks bank_statement_transactions as reconciled and updates bank_statement_imports.

    Validates closing balance (opening + entries = closing).
    Only succeeds if balance validates.
    Updates Opera tables: aentry and nbank.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    if not request_body:
        return {"success": False, "error": "Request body required"}

    matched_entries = request_body.get('matched_entries', [])
    statement_transactions = request_body.get('statement_transactions', [])

    # Load statement transactions from DB if import_id provided and not in request body
    if not statement_transactions and import_id and email_storage:
        db_txns = email_storage.get_statement_transactions(import_id)
        if db_txns:
            statement_transactions = [
                {
                    "line_number": t['line_number'],
                    "date": t['date'],
                    "amount": t['amount'],
                    "reference": t.get('reference', ''),
                    "description": t.get('description', ''),
                    "balance": t.get('balance'),
                    "transaction_type": t.get('transaction_type', '')
                }
                for t in db_txns
            ]

    if not matched_entries:
        return {"success": False, "error": "No matched entries provided"}

    # Acquire bank-level lock
    from sql_rag.import_lock import acquire_import_lock, release_import_lock
    if not acquire_import_lock(_bank_lock_key(bank_code), locked_by="api", endpoint="bank-reconciliation-complete"):
        return {"success": False, "error": f"Bank account {bank_code} is currently being modified by another user. Please wait and try again."}

    try:
        from datetime import datetime
        from sql_rag.opera_sql_import import OperaSQLImport

        logger.info(f"complete_reconciliation: bank={bank_code}, stmt_no={statement_number}, "
                     f"date={statement_date}, closing={closing_balance}, partial={partial}, "
                     f"matched_count={len(matched_entries)}, stmt_txn_count={len(statement_transactions)}")

        stmt_date = datetime.strptime(statement_date, '%Y-%m-%d').date()

        opera_import = OperaSQLImport(sql_connector)
        result = opera_import.complete_reconciliation(
            bank_account=bank_code,
            statement_number=statement_number,
            statement_date=stmt_date,
            closing_balance=closing_balance,
            matched_entries=matched_entries,
            statement_transactions=statement_transactions,
            partial=partial
        )
        if not result.success:
            logger.warning(f"complete_reconciliation failed: {result.errors}")

        # On success, mark transactions as reconciled in DB
        if result.success and import_id and email_storage:
            try:
                # Update match status for reconciled entries
                matches_to_save = []
                for entry in matched_entries:
                    matches_to_save.append({
                        'line_number': entry.get('statement_line'),
                        'matched_entry': entry.get('entry_number'),
                        'match_confidence': 1.0,
                        'match_type': 'manual'
                    })
                if matches_to_save:
                    email_storage.update_transaction_matches_bulk(import_id, matches_to_save)

                # Mark all transactions as reconciled
                email_storage.mark_transactions_reconciled(import_id)

                # Check if reconciliation is actually complete by comparing
                # Opera's new reconciled balance to the statement closing balance.
                # This catches partial reconciliations that complete the statement.
                new_rec_bal = getattr(result, 'new_reconciled_balance', None)
                statement_actually_complete = (
                    not partial
                    or (new_rec_bal is not None and abs(new_rec_bal - closing_balance) < 0.01)
                )

                if partial and not statement_actually_complete:
                    # Genuinely partial: update reconciled_count but keep is_reconciled=0
                    with email_storage._get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute("""
                            UPDATE bank_statement_imports
                            SET reconciled_count = ?,
                                reconciled_date = ?
                            WHERE id = ?
                        """, (result.records_imported, datetime.now().isoformat(), import_id))
                    logger.info(f"Partial reconciliation: import_id={import_id}, {result.records_imported} entries reconciled (statement stays in-progress)")
                else:
                    # Statement is complete — either explicit full reconcile or
                    # partial that brought Opera reconciled balance to statement closing balance
                    if partial and statement_actually_complete:
                        logger.info(f"Partial reconciliation promoted to full: Opera reconciled balance £{new_rec_bal:.2f} matches statement closing £{closing_balance:.2f}")
                    email_storage.mark_statement_reconciled(
                        filename='',
                        reconciled_count=result.records_imported,
                        bank_code=bank_code
                    )
                    with email_storage._get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute("""
                            UPDATE bank_statement_imports
                            SET is_reconciled = 1,
                                reconciled_date = ?,
                                reconciled_count = ?
                            WHERE id = ?
                        """, (datetime.now().isoformat(), result.records_imported, import_id))
                    logger.info(f"Full reconciliation: import_id={import_id}, {result.records_imported} entries reconciled")

                    # Auto-archive the source statement after full reconciliation
                    try:
                        import_record = email_storage.get_bank_statement_import_by_id(import_id)
                        if import_record:
                            archived = False
                            archive_detail = None
                            stmt_source = import_record.get('source', 'email')
                            stmt_filename = import_record.get('filename', '')

                            if stmt_source == 'email' and import_record.get('email_id'):
                                email_detail = email_storage.get_email_by_id(import_record['email_id'])
                                if email_detail:
                                    provider_id = email_detail.get('provider_id')
                                    message_id = email_detail.get('message_id')
                                    folder_id = email_detail.get('folder_id', 'INBOX')
                                    if isinstance(folder_id, int):
                                        with email_storage._get_connection() as fconn:
                                            fcursor = fconn.cursor()
                                            fcursor.execute("SELECT folder_id FROM email_folders WHERE id = ?", (folder_id,))
                                            frow = fcursor.fetchone()
                                            if frow:
                                                folder_id = frow['folder_id']
                                    if provider_id and message_id and email_sync_manager and provider_id in email_sync_manager.providers:
                                        provider = email_sync_manager.providers[provider_id]
                                        moved = await provider.move_email(message_id, folder_id, 'Archive/Bank Statements')
                                        if moved:
                                            archived = True
                                            archive_detail = "Email moved to Archive/Bank Statements"
                                        else:
                                            archive_detail = "Email move failed — may already be archived"
                                    else:
                                        archive_detail = "Email provider not available for archive"
                                else:
                                    archive_detail = f"Email {import_record['email_id']} not found"

                            elif stmt_source == 'file' and import_record.get('file_path'):
                                import shutil
                                from pathlib import Path
                                file_path = Path(import_record['file_path'])
                                if file_path.exists():
                                    settings = _load_company_settings()
                                    archive_folder = settings.get("bank_statements_archive_folder", "")
                                    if archive_folder:
                                        archive_dir = Path(archive_folder)
                                    else:
                                        archive_dir = file_path.parent / 'archive'
                                    archive_dir = archive_dir / datetime.now().strftime('%Y-%m')
                                    archive_dir.mkdir(parents=True, exist_ok=True)
                                    dest = archive_dir / file_path.name
                                    if dest.exists():
                                        stem, suffix = dest.stem, dest.suffix
                                        counter = 1
                                        while dest.exists():
                                            dest = archive_dir / f"{stem}_{counter}{suffix}"
                                            counter += 1
                                    shutil.move(str(file_path), str(dest))
                                    archived = True
                                    archive_detail = f"File moved to {dest}"
                                else:
                                    archive_detail = f"File not found: {import_record['file_path']}"

                            if archived:
                                logger.info(f"Auto-archived reconciled statement: {stmt_filename} ({archive_detail})")
                            elif archive_detail:
                                logger.warning(f"Could not auto-archive statement: {stmt_filename} ({archive_detail})")
                    except Exception as archive_err:
                        logger.warning(f"Auto-archive after reconciliation failed (non-blocking): {archive_err}")

            except Exception as db_err:
                logger.warning(f"Could not update reconciliation status in DB: {db_err}")

        # If a partial reconciliation was promoted to full, reflect that in the response
        effective_partial = partial
        if result.success:
            new_rec_bal_check = getattr(result, 'new_reconciled_balance', None)
            if partial and new_rec_bal_check is not None and abs(new_rec_bal_check - closing_balance) < 0.01:
                effective_partial = False

        release_import_lock(_bank_lock_key(bank_code))
        return {
            "success": result.success,
            "entries_reconciled": result.records_imported if result.success else 0,
            "messages": result.warnings if result.success else result.errors,
            "partial": effective_partial,
            "statement_number": statement_number,
            "statement_date": statement_date,
            "closing_balance": closing_balance,
            "new_reconciled_balance": result.new_reconciled_balance if hasattr(result, 'new_reconciled_balance') else None
        }

    except Exception as e:
        logger.error(f"Error completing reconciliation: {e}")
        try:
            release_import_lock(_bank_lock_key(bank_code))
        except Exception:
            pass
        return {"success": False, "error": str(e)}





@router.get("/api/bank-reconciliation/statement-transactions/{import_id}")
async def get_statement_transactions(import_id: int):
    """
    Retrieve stored statement transactions for an import.

    Returns the PDF-extracted statement lines with their current match/reconcile status.
    Used by the reconcile page to load transactions from DB instead of sessionStorage.
    """
    try:
        if not email_storage:
            return {"success": False, "error": "Email storage not initialized"}

        transactions = email_storage.get_statement_transactions(import_id)

        # Also get the import record for statement metadata
        with email_storage._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, bank_code, filename, opening_balance, closing_balance,
                       statement_date, account_number, sort_code, source,
                       transactions_imported, total_receipts, total_payments,
                       COALESCE(is_reconciled, 0) as is_reconciled
                FROM bank_statement_imports WHERE id = ?
            """, (import_id,))
            row = cursor.fetchone()
            import_record = dict(row) if row else None

        if not import_record:
            return {"success": False, "error": f"Import record {import_id} not found"}

        # Resolve full path for the statement PDF file
        full_path = None
        filename = import_record.get('filename', '')
        bank_code = import_record.get('bank_code', '')
        if filename and bank_code:
            try:
                settings = _load_company_settings()
                base_path = settings.get("bank_statements_base_folder", "")
                if base_path:
                    base = Path(base_path)
                    # Search bank subfolders for the file
                    for subfolder in base.iterdir():
                        if subfolder.is_dir() and subfolder.name.startswith(bank_code):
                            candidate = subfolder / filename
                            if candidate.exists():
                                full_path = str(candidate)
                                break
                    # Also check archive folders
                    if not full_path:
                        archive_base = base / 'archive'
                        if archive_base.exists():
                            for subfolder in archive_base.iterdir():
                                if subfolder.is_dir() and subfolder.name.startswith(bank_code):
                                    candidate = subfolder / filename
                                    if candidate.exists():
                                        full_path = str(candidate)
                                        break
            except Exception as e:
                logger.debug(f"Could not resolve full path for {filename}: {e}")

        return {
            "success": True,
            "import_id": import_id,
            "import_record": import_record,
            "transactions": transactions,
            "count": len(transactions),
            "statement_info": {
                "opening_balance": import_record.get('opening_balance'),
                "closing_balance": import_record.get('closing_balance'),
                "statement_date": import_record.get('statement_date'),
                "account_number": import_record.get('account_number'),
                "sort_code": import_record.get('sort_code'),
                "full_path": full_path,
            }
        }

    except Exception as e:
        logger.error(f"Error getting statement transactions: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/bank-reconciliation/status")
async def get_reconciliation_status(
    bank_code: str = Query(..., description="Bank account code")
):
    """
    Get current reconciliation status for a bank account.

    Returns:
    - reconciled_balance: Total reconciled (expected opening for next statement)
    - current_balance: Current total of all entries
    - unreconciled_count: Number of unreconciled entries
    - unreconciled_total: Total amount unreconciled
    - last_statement_number: Last reconciled statement number
    - last_statement_date: Date of last reconciled statement
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.opera_sql_import import OperaSQLImport

        opera_import = OperaSQLImport(sql_connector)
        status = opera_import.get_reconciliation_status(bank_code)

        return {
            "success": True,
            **status
        }

    except Exception as e:
        logger.error(f"Error getting reconciliation status: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/bank-reconciliation/unreconciled-entries")
async def get_unreconciled_entries(
    bank_code: str = Query(..., description="Bank account code"),
    include_incomplete: bool = Query(False, description="Include incomplete (not posted to NL) entries")
):
    """
    Get list of unreconciled cashbook entries for a bank account.

    Returns entries that have not yet been reconciled (ae_reclnum = 0).
    By default only includes complete entries (ae_complet = 1).
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.opera_sql_import import OperaSQLImport

        opera_import = OperaSQLImport(sql_connector)
        entries = opera_import.get_unreconciled_entries(
            bank_account=bank_code,
            include_incomplete=include_incomplete
        )

        return {
            "success": True,
            "bank_code": bank_code,
            "entries": entries,
            "count": len(entries)
        }

    except Exception as e:
        logger.error(f"Error getting unreconciled entries: {e}")
        return {"success": False, "error": str(e)}





@router.post("/api/cashbook/auto-match-statement-lines")
async def auto_match_statement_lines(request: Request):
    """
    Auto-match bank statement lines to customers (receipts) or suppliers (payments).

    For receipts (positive amounts):
    - Match by invoice reference in description
    - Match by exact outstanding amount
    - Fuzzy match by customer name in description

    For payments (negative amounts):
    - Match by supplier name in description
    - Match by outstanding invoice amount

    Request body:
    - lines: List of unmatched statement lines with:
        - statement_line: Line number
        - statement_date: Date string
        - statement_amount: Amount (positive=receipt, negative=payment)
        - statement_reference: Reference
        - statement_description: Description

    Returns lines with matched_account and matched_name if found.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        body = await request.json()
        lines = body.get('lines', [])

        if not lines:
            return {"success": True, "lines": []}

        import re
        from difflib import SequenceMatcher

        # Load customers
        customers_df = sql_connector.execute_query("""
            SELECT sn_account, sn_name FROM sname WITH (NOLOCK)
            WHERE (sn_stop = 0 OR sn_stop IS NULL)
        """)
        customers = {}
        if customers_df is not None and not customers_df.empty:
            customers = {
                str(row['sn_account']).strip(): str(row['sn_name']).strip()
                for _, row in customers_df.iterrows()
            }

        # Load suppliers
        suppliers_df = sql_connector.execute_query("""
            SELECT pn_account, pn_name FROM pname WITH (NOLOCK)
            WHERE (pn_stop = 0 OR pn_stop IS NULL)
        """)
        suppliers = {}
        if suppliers_df is not None and not suppliers_df.empty:
            suppliers = {
                str(row['pn_account']).strip(): str(row['pn_name']).strip()
                for _, row in suppliers_df.iterrows()
            }

        def extract_invoice_refs(text: str) -> list:
            """Extract invoice references from text."""
            if not text:
                return []
            refs = []
            patterns = [
                (r'INV\s*(\d+)', 'INV'),
                (r'Invoice\s*#?\s*(\d+)', 'INV'),
                (r'SI-?(\d+)', 'SI'),
                (r'(?:^|\s)(\d{5,6})(?:\s|$|,)', ''),
            ]
            for pattern, prefix in patterns:
                for match in re.finditer(pattern, text, re.IGNORECASE):
                    ref = f"{prefix}{match.group(1)}" if prefix else match.group(1)
                    if ref not in refs:
                        refs.append(ref)
            return refs

        # Pre-load outstanding invoices in bulk (single query each)
        outstanding_sales = {}  # amount -> (account, name)
        try:
            sales_df = sql_connector.execute_query("""
                SELECT st.st_account, sn.sn_name, st.st_trbal, st.st_trref
                FROM stran st WITH (NOLOCK)
                JOIN sname sn WITH (NOLOCK) ON st.st_account = sn.sn_account
                WHERE st.st_trbal > 0.01
            """)
            if sales_df is not None and not sales_df.empty:
                for _, row in sales_df.iterrows():
                    bal = round(float(row['st_trbal']), 2)
                    acct = str(row['st_account']).strip()
                    name = str(row['sn_name']).strip()
                    ref = str(row.get('st_trref', '')).strip()
                    if bal not in outstanding_sales:
                        outstanding_sales[bal] = (acct, name, ref)
        except Exception:
            pass

        outstanding_purchases = {}  # amount -> (account, name)
        try:
            purch_df = sql_connector.execute_query("""
                SELECT pt.pt_account, pn.pn_name, pt.pt_trbal, pt.pt_trref
                FROM ptran pt WITH (NOLOCK)
                JOIN pname pn WITH (NOLOCK) ON pt.pt_account = pn.pn_acnt
                WHERE pt.pt_trbal > 0.01
            """)
            if purch_df is not None and not purch_df.empty:
                for _, row in purch_df.iterrows():
                    bal = round(float(row['pt_trbal']), 2)
                    acct = str(row['pt_account']).strip()
                    name = str(row['pn_name']).strip()
                    if bal not in outstanding_purchases:
                        outstanding_purchases[bal] = (acct, name)
        except Exception:
            pass

        # Pre-load recent stran refs for invoice matching (single query)
        stran_by_ref = {}  # ref_suffix -> (account, name)
        try:
            ref_df = sql_connector.execute_query("""
                SELECT st.st_account, sn.sn_name, st.st_trref
                FROM stran st WITH (NOLOCK)
                JOIN sname sn WITH (NOLOCK) ON st.st_account = sn.sn_account
                WHERE st.st_trref IS NOT NULL AND st.st_trref <> ''
            """)
            if ref_df is not None and not ref_df.empty:
                for _, row in ref_df.iterrows():
                    ref = str(row['st_trref']).strip().upper()
                    acct = str(row['st_account']).strip()
                    name = str(row['sn_name']).strip()
                    if ref and ref not in stran_by_ref:
                        stran_by_ref[ref] = (acct, name)
        except Exception:
            pass

        # Pre-build uppercase name lookups for fast substring matching
        customer_names_upper = {code: name.upper() for code, name in customers.items()}
        supplier_names_upper = {code: name.upper() for code, name in suppliers.items()}

        def find_customer_by_invoice(refs: list) -> tuple:
            """Find customer by invoice reference using pre-loaded data."""
            for ref in refs:
                ref_upper = ref.upper()
                # Check exact match
                if ref_upper in stran_by_ref:
                    return stran_by_ref[ref_upper][0], stran_by_ref[ref_upper][1], 'invoice_ref'
                # Check suffix match (last 6 chars)
                suffix = ref_upper[-6:] if len(ref_upper) >= 6 else ref_upper
                for stored_ref, (acct, name) in stran_by_ref.items():
                    if suffix in stored_ref or ref_upper in stored_ref:
                        return acct, name, 'invoice_ref'
            return None, None, None

        def find_customer_by_amount(amount: float) -> tuple:
            """Find customer with outstanding invoice matching amount."""
            key = round(amount, 2)
            # Check exact match first
            if key in outstanding_sales:
                return outstanding_sales[key][0], outstanding_sales[key][1], 'amount_match'
            # Check within tolerance
            for bal, (acct, name, _ref) in outstanding_sales.items():
                if abs(bal - key) < 0.02:
                    return acct, name, 'amount_match'
            return None, None, None

        def find_supplier_by_amount(amount: float) -> tuple:
            """Find supplier with outstanding invoice matching amount."""
            key = round(amount, 2)
            if key in outstanding_purchases:
                return outstanding_purchases[key][0], outstanding_purchases[key][1], 'amount_match'
            for bal, (acct, name) in outstanding_purchases.items():
                if abs(bal - key) < 0.02:
                    return acct, name, 'amount_match'
            return None, None, None

        def fuzzy_match_name(text: str, names_dict: dict, names_upper: dict, threshold: float = 0.7) -> tuple:
            """Fuzzy match text against names dictionary."""
            if not text:
                return None, None, None
            text_upper = text.upper()
            best_match = None
            best_score = 0
            for code, name_upper in names_upper.items():
                # Direct substring match (fast path)
                if name_upper in text_upper or text_upper in name_upper:
                    return code, names_dict[code], 'name_match'
                # Fuzzy match
                score = SequenceMatcher(None, text_upper, name_upper).ratio()
                if score > best_score and score >= threshold:
                    best_score = score
                    best_match = (code, names_dict[code])
            if best_match:
                return best_match[0], best_match[1], 'fuzzy_match'
            return None, None, None

        # Process each line
        matched_lines = []
        for line in lines:
            amount = float(line.get('statement_amount', 0))
            description = str(line.get('statement_description', '') or '')
            reference = str(line.get('statement_reference', '') or '')
            search_text = f"{description} {reference}"

            matched_account = None
            matched_name = None
            match_method = None
            account_type = None

            if amount > 0:
                # Receipt - look for customer
                account_type = 'customer'

                # 1. Try invoice reference
                refs = extract_invoice_refs(search_text)
                if refs:
                    matched_account, matched_name, match_method = find_customer_by_invoice(refs)

                # 2. Try amount match
                if not matched_account:
                    matched_account, matched_name, match_method = find_customer_by_amount(amount)

                # 3. Try fuzzy name match
                if not matched_account:
                    matched_account, matched_name, match_method = fuzzy_match_name(search_text, customers, customer_names_upper)

            else:
                # Payment - look for supplier
                account_type = 'supplier'
                abs_amount = abs(amount)

                # 1. Try amount match
                matched_account, matched_name, match_method = find_supplier_by_amount(abs_amount)

                # 2. Try fuzzy name match
                if not matched_account:
                    matched_account, matched_name, match_method = fuzzy_match_name(search_text, suppliers, supplier_names_upper)

            # Add match info to line
            matched_line = dict(line)
            matched_line['matched_account'] = matched_account
            matched_line['matched_name'] = matched_name
            matched_line['match_method'] = match_method
            matched_line['suggested_type'] = account_type
            matched_lines.append(matched_line)

        # Count matches
        matched_count = sum(1 for l in matched_lines if l.get('matched_account'))

        return {
            "success": True,
            "lines": matched_lines,
            "matched_count": matched_count,
            "total_count": len(matched_lines)
        }

    except Exception as e:
        logger.error(f"Error auto-matching statement lines: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}





@router.get("/api/bank-import/suggest-account")
async def suggest_account_for_transaction(
    name: str = Query(..., description="Transaction name/description to match"),
    transaction_type: str = Query(..., description="Transaction type: sales_receipt, purchase_payment, sales_refund, purchase_refund"),
    limit: int = Query(5, description="Max suggestions to return")
):
    """
    Suggest a customer or supplier account based on transaction name and type.

    Now that the user has selected the transaction type, we know whether to search
    customers (for sales_receipt/sales_refund) or suppliers (for purchase_payment/purchase_refund).

    Returns top matches with confidence scores.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from difflib import SequenceMatcher

        # Determine which ledger to search based on transaction type
        is_customer = transaction_type in ('sales_receipt', 'sales_refund')

        if is_customer:
            # Search customers
            query = """
                SELECT sn_account as code, RTRIM(sn_name) as name
                FROM sname WITH (NOLOCK)
                WHERE sn_stop = 'N' OR sn_stop IS NULL
                ORDER BY sn_name
            """
        else:
            # Search suppliers
            query = """
                SELECT pn_account as code, RTRIM(pn_name) as name
                FROM pname WITH (NOLOCK)
                WHERE pn_stop = 'N' OR pn_stop IS NULL
                ORDER BY pn_name
            """

        df = sql_connector.execute_query(query)
        if df is None or df.empty:
            return {"success": True, "suggestions": [], "ledger_type": 'C' if is_customer else 'S'}

        # Build name dictionary
        accounts = {row['code']: row['name'] for _, row in df.iterrows()}

        # Search for matches
        name_upper = name.upper().strip()
        matches = []

        for code, acc_name in accounts.items():
            if not acc_name:
                continue
            acc_name_upper = acc_name.upper()

            # Direct substring match (high confidence)
            if acc_name_upper in name_upper or name_upper in acc_name_upper:
                matches.append({
                    'code': code,
                    'name': acc_name,
                    'score': 95,
                    'match_type': 'substring'
                })
                continue

            # Word-based matching - check if significant words match
            name_words = set(w for w in name_upper.split() if len(w) > 2)
            acc_words = set(w for w in acc_name_upper.split() if len(w) > 2)
            common_words = name_words & acc_words
            if common_words and len(common_words) >= min(2, len(acc_words)):
                word_score = len(common_words) / max(len(name_words), len(acc_words)) * 100
                if word_score >= 40:
                    matches.append({
                        'code': code,
                        'name': acc_name,
                        'score': int(min(90, word_score + 30)),
                        'match_type': 'word_match'
                    })
                    continue

            # Fuzzy match (lower confidence)
            score = SequenceMatcher(None, name_upper, acc_name_upper).ratio() * 100
            if score >= 60:
                matches.append({
                    'code': code,
                    'name': acc_name,
                    'score': int(score),
                    'match_type': 'fuzzy'
                })

        # Sort by score descending and limit
        matches.sort(key=lambda x: x['score'], reverse=True)
        suggestions = matches[:limit]

        logger.info(f"suggest-account: Searching for '{name}' in {'customers' if is_customer else 'suppliers'}, found {len(matches)} matches, returning {len(suggestions)}")
        if suggestions:
            logger.info(f"suggest-account: Top suggestion: {suggestions[0]['name']} (code={suggestions[0]['code']}, score={suggestions[0]['score']})")

        return {
            "success": True,
            "suggestions": suggestions,
            "ledger_type": 'C' if is_customer else 'S',
            "searched_count": len(accounts),
            "search_term": name
        }

    except Exception as e:
        logger.error(f"Error suggesting account: {e}")
        return {"success": False, "error": str(e), "suggestions": []}





@router.post("/api/cashbook/create-entry")
async def create_cashbook_entry(request: Request):
    """
    Create a new cashbook entry for an unmatched statement line.

    Request body:
    - bank_account: Bank account code (e.g., BC010)
    - transaction_date: Date of transaction (YYYY-MM-DD)
    - amount: Positive amount (sign determined by transaction_type)
    - reference: Transaction reference
    - description: Transaction description
    - transaction_type: One of 'sales_receipt', 'purchase_payment', 'other_receipt', 'other_payment'
    - account_code: Customer/Supplier/Nominal code
    - account_type: 'customer', 'supplier', or 'nominal'
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        body = await request.json()

        bank_account = body.get('bank_account', '')
        transaction_date = body.get('transaction_date')
        amount = float(body.get('amount', 0))
        reference = body.get('reference', '')
        description = body.get('description', '')
        transaction_type = body.get('transaction_type', 'other_payment')
        account_code = body.get('account_code', '')
        account_type = body.get('account_type', 'nominal')
        import_id = body.get('import_id')  # For partial recovery tracking
        line_number = body.get('line_number')  # Statement line number
        project_code = body.get('project_code', '')
        department_code = body.get('department_code', '')
        vat_code = body.get('vat_code', '')

        if not transaction_date:
            return {"success": False, "error": "Transaction date is required"}

        if amount <= 0:
            return {"success": False, "error": "Amount must be positive"}

        from sql_rag.opera_sql_import import OperaSQLImport
        from datetime import date

        opera_import = OperaSQLImport(sql_connector)

        # Parse date
        if isinstance(transaction_date, str):
            txn_date = date.fromisoformat(transaction_date[:10])
        else:
            txn_date = transaction_date

        # Pre-flight duplicate check - catch entries that appeared since statement was processed
        dup_check = opera_import.check_duplicate_before_posting(
            bank_account=bank_account,
            transaction_date=txn_date,
            amount_pounds=amount,
            account_code=account_code,
            account_type=account_type
        )
        if dup_check['is_duplicate']:
            return {
                "success": False,
                "error": f"Transaction already exists in Opera: {dup_check['details']}",
                "duplicate": True,
                "duplicate_details": dup_check
            }

        # Use the appropriate import method based on account type
        if account_type == 'customer':
            # Customer receipt
            result = opera_import.import_sales_receipt(
                bank_account=bank_account,
                customer_account=account_code,
                amount_pounds=amount,
                reference=reference,
                post_date=txn_date,
                input_by='RECONCILE',
                payment_method='Bank Import'
            )
            entry_type = 'Sales Receipt'
        elif account_type == 'supplier':
            # Supplier payment
            result = opera_import.import_purchase_payment(
                bank_account=bank_account,
                supplier_account=account_code,
                amount_pounds=amount,
                reference=reference,
                post_date=txn_date,
                input_by='RECONCILE'
            )
            entry_type = 'Purchase Payment'
        else:
            # Nominal-only transaction (bank charges, interest, etc.)
            # Determine if receipt or payment based on transaction_type
            is_receipt = transaction_type in ['other_receipt', 'nominal_receipt']

            result = opera_import.import_nominal_entry(
                bank_account=bank_account,
                nominal_account=account_code,
                amount_pounds=amount,
                reference=reference,
                post_date=txn_date,
                description=description,
                input_by='RECONCILE',
                is_receipt=is_receipt,
                project_code=project_code,
                department_code=department_code,
                vat_code=vat_code
            )
            entry_type = 'Nominal Receipt' if is_receipt else 'Nominal Payment'

        if result.success:
            entry_number = None
            if hasattr(result, 'entry_numbers') and result.entry_numbers:
                entry_number = result.entry_numbers[0]
            elif hasattr(result, 'entry_number'):
                entry_number = result.entry_number

            # Mark transaction as posted for partial recovery tracking
            if import_id and line_number and entry_number and email_storage:
                try:
                    email_storage.mark_transaction_posted(import_id, line_number, str(entry_number))
                except Exception as mark_err:
                    logger.warning(f"Could not mark transaction as posted: {mark_err}")

            return {
                "success": True,
                "entry_number": entry_number,
                "message": f"Created {entry_type} for £{amount:.2f}"
            }
        else:
            error_msg = "; ".join(result.errors) if hasattr(result, 'errors') and result.errors else str(result.message if hasattr(result, 'message') else 'Unknown error')
            return {
                "success": False,
                "error": error_msg
            }

    except Exception as e:
        logger.error(f"Error creating cashbook entry: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}





@router.get("/api/cashbook/bank-accounts")
async def get_bank_accounts():
    """
    Get list of valid bank accounts for transfers.
    Returns non-foreign-currency bank accounts from nbank.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.opera_sql_import import OperaSQLImport

        opera_import = OperaSQLImport(sql_connector)
        accounts = opera_import.get_bank_accounts_for_transfer()

        return {
            "success": True,
            "accounts": accounts,
            "count": len(accounts)
        }

    except Exception as e:
        logger.error(f"Error getting bank accounts: {e}")
        return {"success": False, "error": str(e), "accounts": []}





@router.post("/api/cashbook/create-bank-transfer")
async def create_bank_transfer(
    source_bank: str = Query(..., description="Source bank account code"),
    dest_bank: str = Query(..., description="Destination bank account code"),
    amount: float = Query(..., description="Transfer amount (positive)"),
    reference: str = Query(..., description="Reference (max 20 chars)"),
    date: str = Query(..., description="Transfer date YYYY-MM-DD"),
    comment: str = Query("", description="Optional comment")
):
    """
    Create a bank transfer between two Opera bank accounts.
    Used from bank reconciliation for unmatched transfer lines.

    Creates paired entries in both source and destination bank accounts:
    - Source bank: negative entry (money going out)
    - Dest bank: positive entry (money coming in)
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.opera_sql_import import OperaSQLImport
        from datetime import date as date_type

        # Validate inputs
        if source_bank == dest_bank:
            return {"success": False, "error": "Source and destination bank must be different"}

        if amount <= 0:
            return {"success": False, "error": "Transfer amount must be positive"}

        # Parse date
        try:
            transfer_date = date_type.fromisoformat(date[:10])
        except ValueError:
            return {"success": False, "error": f"Invalid date format: {date}. Use YYYY-MM-DD"}

        opera_import = OperaSQLImport(sql_connector)

        result = opera_import.import_bank_transfer(
            source_bank=source_bank,
            dest_bank=dest_bank,
            amount_pounds=amount,
            reference=reference[:20] if reference else "",
            post_date=transfer_date,
            comment=comment[:50] if comment else "",
            input_by="RECONCILE"
        )

        if result.success:
            # Parse entry numbers from the combined "source/dest" format
            entries = result.entry_number.split('/') if result.entry_number else ['', '']
            return {
                "success": True,
                "source_entry": entries[0] if len(entries) > 0 else '',
                "dest_entry": entries[1] if len(entries) > 1 else '',
                "source_bank": source_bank,
                "dest_bank": dest_bank,
                "amount": amount,
                "message": f"Bank transfer created: {result.entry_number}",
                "warnings": result.warnings or []
            }
        else:
            return {
                "success": False,
                "error": result.errors[0] if result.errors else 'Unknown error'
            }

    except Exception as e:
        logger.error(f"Error creating bank transfer: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}





@router.get("/api/opera3/reconcile/bank/{bank_code}/status")
async def opera3_bank_reconciliation_status(
    bank_code: str,
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """
    Get bank reconciliation status for Opera 3.
    Returns reconciliation_in_progress flag and balance information.
    """
    try:
        from sql_rag.opera3_data_provider import Opera3DataProvider

        provider = Opera3DataProvider(data_path)
        result = provider.get_bank_reconciliation_status(bank_code)

        return result

    except Exception as e:
        logger.error(f"Opera 3 bank reconciliation status failed: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/opera3/recurring-entries/check/{bank_code}")
async def opera3_check_recurring_entries(
    bank_code: str,
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """Check for due recurring entries for an Opera 3 bank account."""
    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        from sql_rag.opera3_config import get_opera3_config, get_period_posting_decision as o3_get_period_posting_decision
        from datetime import date as date_type

        mode = _load_company_settings().get("recurring_entries_mode", "process")
        reader = Opera3Reader(data_path)
        o3_config = get_opera3_config(data_path)

        arhead = reader.read_table("arhead")
        arline = reader.read_table("arline")

        if not arhead or not arline:
            return {"success": True, "mode": mode, "entries": [], "total_due": 0, "postable_count": 0, "blocked_count": 0}

        today = date_type.today()
        bank_upper = bank_code.strip().upper()

        # Filter active + due entries for this bank
        due_headers = []
        for h in arhead:
            acnt = str(h.get("AE_ACNT", h.get("ae_acnt", ""))).strip().upper()
            if acnt != bank_upper:
                continue
            to_post = int(h.get("AE_TOPOST", h.get("ae_topost", 0)) or 0)
            posted = int(h.get("AE_POSTED", h.get("ae_posted", 0)) or 0)
            if to_post != 0 and posted >= to_post:
                continue
            nxt = h.get("AE_NXTPOST", h.get("ae_nxtpost"))
            if not nxt:
                continue
            if isinstance(nxt, str):
                try:
                    nxt = date_type.fromisoformat(nxt[:10])
                except (ValueError, TypeError):
                    continue
            elif hasattr(nxt, 'date'):
                nxt = nxt.date()
            if nxt > today:
                continue
            due_headers.append((h, nxt))

        if not due_headers:
            return {"success": True, "mode": mode, "entries": [], "total_due": 0, "postable_count": 0, "blocked_count": 0}

        # Build line lookup
        line_lookup = {}
        for l in arline:
            key = (str(l.get("AT_ENTRY", l.get("at_entry", ""))).strip().upper(),
                   str(l.get("AT_ACNT", l.get("at_acnt", ""))).strip().upper())
            line_lookup.setdefault(key, []).append(l)

        # Look up account descriptions
        account_descs = {}
        try:
            nacnt_data = reader.read_table("nacnt")
            if nacnt_data:
                for n in nacnt_data:
                    code = str(n.get("NA_ACNT", n.get("na_acnt", ""))).strip()
                    desc = str(n.get("NA_DESC", n.get("na_desc", ""))).strip()
                    if code:
                        account_descs[code] = desc
            pname_data = reader.read_table("pname")
            if pname_data:
                for p in pname_data:
                    code = str(p.get("PN_ACNT", p.get("pn_acnt", ""))).strip()
                    name = str(p.get("PN_NAME", p.get("pn_name", ""))).strip()
                    if code:
                        account_descs[code] = name
            sname_data = reader.read_table("sname")
            if sname_data:
                for s in sname_data:
                    code = str(s.get("SN_ACNT", s.get("sn_acnt", ""))).strip()
                    name = str(s.get("SN_NAME", s.get("sn_name", ""))).strip()
                    if code:
                        account_descs[code] = name
        except Exception:
            pass

        entries = []
        postable_count = 0
        blocked_count = 0

        # Helper: generate all outstanding posting dates from ae_nxtpost to today
        def _outstanding_dates_o3(nxt_post_date, freq_code, every, posted, total):
            from dateutil.relativedelta import relativedelta
            dates = []
            if not nxt_post_date:
                return dates
            current = nxt_post_date
            max_remaining = (total - posted) if total > 0 else 24  # cap for safety
            while current <= today and len(dates) < max_remaining:
                dates.append(current)
                fu = freq_code.upper().strip()
                if fu == 'D':
                    current = current + timedelta(days=every)
                elif fu == 'W':
                    current = current + timedelta(weeks=every)
                elif fu == 'M':
                    current = current + relativedelta(months=every)
                elif fu == 'Q':
                    current = current + relativedelta(months=3 * every)
                elif fu == 'Y':
                    current = current + relativedelta(years=every)
                else:
                    current = current + relativedelta(months=every)
            return dates

        for h, nxt_post_date in due_headers:
            entry_ref = str(h.get("AE_ENTRY", h.get("ae_entry", ""))).strip()
            ae_type = int(h.get("AE_TYPE", h.get("ae_type", 0)) or 0)
            freq = str(h.get("AE_FREQ", h.get("ae_freq", ""))).strip()
            ae_every = int(h.get("AE_EVERY", h.get("ae_every", 1)) or 1)
            ae_posted = int(h.get("AE_POSTED", h.get("ae_posted", 0)) or 0)
            ae_topost = int(h.get("AE_TOPOST", h.get("ae_topost", 0)) or 0)

            key = (entry_ref.upper(), bank_upper)
            lines = line_lookup.get(key, [])
            if not lines:
                continue
            first_line = lines[0]
            line_count = len(lines)

            # Sum amount across ALL lines
            total_amount_pence = sum(abs(int(l.get("AT_VALUE", l.get("at_value", 0)) or 0)) for l in lines)
            total_amount_pounds = round(total_amount_pence / 100.0, 2)
            account = str(first_line.get("AT_ACCOUNT", first_line.get("at_account", ""))).strip()
            vat_code = str(first_line.get("AT_VATCDE", first_line.get("at_vatcde", ""))).strip()
            vat_val = int(first_line.get("AT_VATVAL", first_line.get("at_vatval", 0)) or 0)

            # Build per-line detail
            line_details = []
            for l in lines:
                l_acct = str(l.get("AT_ACCOUNT", l.get("at_account", ""))).strip()
                l_vat_code = str(l.get("AT_VATCDE", l.get("at_vatcde", ""))).strip()
                l_vat_val = int(l.get("AT_VATVAL", l.get("at_vatval", 0)) or 0)
                line_details.append({
                    "account": l_acct,
                    "account_desc": account_descs.get(l_acct, ""),
                    "amount_pence": int(l.get("AT_VALUE", l.get("at_value", 0)) or 0),
                    "amount_pounds": round(abs(int(l.get("AT_VALUE", l.get("at_value", 0)) or 0)) / 100.0, 2),
                    "vat_code": l_vat_code,
                    "vat_amount_pence": l_vat_val,
                    "project": str(l.get("AT_PROJECT", l.get("at_project", ""))).strip(),
                    "department": str(l.get("AT_JOB", l.get("at_job", ""))).strip(),
                    "comment": str(l.get("AT_COMMENT", l.get("at_comment", ""))).strip(),
                })

            description = str(h.get("AE_DESC", h.get("ae_desc", ""))).strip() or str(first_line.get("AT_ENTREF", first_line.get("at_entref", ""))).strip()

            # Generate all outstanding posting dates
            outstanding = _outstanding_dates_o3(nxt_post_date, freq, ae_every, ae_posted, ae_topost)
            if not outstanding:
                outstanding = [nxt_post_date]

            for post_dt in outstanding:
                can_post = True
                blocked_reason = None

                # Check unsupported types
                if ae_type not in (1, 2, 3, 4, 5, 6):
                    can_post = False
                    blocked_reason = f"Type {ae_type} ({TYPE_DESCRIPTIONS.get(ae_type, 'Unknown')}) — process in Opera"
                else:
                    try:
                        decision = o3_get_period_posting_decision(o3_config, post_dt)
                        if not decision.can_post:
                            can_post = False
                            blocked_reason = decision.error_message or "Period is blocked"
                    except Exception as pe:
                        can_post = False
                        blocked_reason = f"Period validation error: {pe}"

                if can_post:
                    postable_count += 1
                else:
                    blocked_count += 1

                # Use composite key: entry_ref:YYYY-MM-DD for multi-date entries
                post_date_str = post_dt.isoformat() if post_dt else None
                composite_ref = f"{entry_ref}:{post_date_str}" if len(outstanding) > 1 else entry_ref

                entries.append({
                    "entry_ref": composite_ref,
                    "base_entry_ref": entry_ref,
                    "type": ae_type,
                    "type_desc": TYPE_DESCRIPTIONS.get(ae_type, f"Type {ae_type}"),
                    "description": description,
                    "account": account,
                    "account_desc": account_descs.get(account, ""),
                    "cbtype": str(first_line.get("AT_CBTYPE", first_line.get("at_cbtype", ""))).strip(),
                    "amount_pence": total_amount_pence,
                    "amount_pounds": total_amount_pounds,
                    "next_post_date": post_date_str,
                    "posted_count": ae_posted,
                    "total_posts": ae_topost,
                    "frequency": FREQ_DESCRIPTIONS.get(freq, freq),
                    "project": str(first_line.get("AT_PROJECT", first_line.get("at_project", ""))).strip(),
                    "department": str(first_line.get("AT_JOB", first_line.get("at_job", ""))).strip(),
                    "can_post": can_post,
                    "blocked_reason": blocked_reason,
                    "comment": str(first_line.get("AT_COMMENT", first_line.get("at_comment", ""))).strip(),
                    "vat_code": vat_code,
                    "vat_amount_pence": vat_val,
                    "line_count": line_count,
                    "lines": line_details,
                })

        return {
            "success": True,
            "mode": mode,
            "entries": entries,
            "total_due": len(entries),
            "postable_count": postable_count,
            "blocked_count": blocked_count
        }

    except Exception as e:
        logger.error(f"Error checking Opera 3 recurring entries: {e}")
        return {"success": False, "error": str(e)}





@router.post("/api/opera3/recurring-entries/post")
async def opera3_post_recurring_entries(request: Request):
    """
    Post selected recurring entries to Opera 3 FoxPro.

    Request body:
    {
        "bank_code": "BC010",
        "data_path": "/path/to/opera3/data",
        "entries": [
            {"entry_ref": "REC0000053", "override_date": "2026-02-20"}
        ]
    }
    """
    try:
        from sql_rag.opera3_write_provider import get_opera3_writer, Opera3AgentRequired
        from datetime import date as date_type

        body = await request.json()
        bank_code = body.get("bank_code", "").strip()
        data_path = body.get("data_path", "").strip()
        entries = body.get("entries", [])

        if not bank_code:
            return {"success": False, "error": "bank_code is required"}
        if not data_path:
            return {"success": False, "error": "data_path is required"}
        if not entries:
            return {"success": False, "error": "No entries to post"}

        try:
            importer = get_opera3_writer(data_path)
        except Opera3AgentRequired as e:
            return {"success": False, "error": str(e)}
        results = []
        posted_count = 0
        failed_count = 0

        for entry in entries:
            raw_ref = entry.get("entry_ref", "").strip()
            override_str = entry.get("override_date")

            # Parse composite key (entry_ref:YYYY-MM-DD) from multi-date entries
            if ':' in raw_ref:
                entry_ref, date_part = raw_ref.rsplit(':', 1)
                override_str = override_str or date_part
            else:
                entry_ref = raw_ref

            override_date = None
            if override_str:
                try:
                    override_date = date_type.fromisoformat(override_str)
                except (ValueError, TypeError):
                    results.append({"entry_ref": raw_ref, "success": False, "error": f"Invalid date: {override_str}"})
                    failed_count += 1
                    continue

            result = importer.post_recurring_entry(
                bank_account=bank_code,
                entry_ref=entry_ref,
                override_date=override_date
            )

            if result.success:
                posted_count += 1
                results.append({
                    "entry_ref": entry_ref,
                    "success": True,
                    "message": f"Posted {result.entry_number or 'entry'}",
                    "entry_number": result.entry_number,
                    "warnings": result.warnings
                })
            else:
                failed_count += 1
                results.append({
                    "entry_ref": entry_ref,
                    "success": False,
                    "error": "; ".join(result.errors) if result.errors else "Unknown error"
                })

        return {
            "success": posted_count > 0 or failed_count == 0,
            "results": results,
            "posted_count": posted_count,
            "failed_count": failed_count
        }

    except Exception as e:
        logger.error(f"Error posting Opera 3 recurring entries: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/opera3/bank-import/scan-emails")
async def opera3_scan_emails_for_bank_statements(
    bank_code: str = Query(..., description="Opera bank account code"),
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    days_back: int = Query(30, description="Number of days to search back"),
    include_processed: bool = Query(False, description="Include already-processed emails"),
    validate_balances: bool = Query(True, description="Validate statement balances against Opera (slower but filters invalid)")
):
    """
    Scan inbox for emails with bank statement attachments (Opera 3 version).

    Returns list of candidate emails with:
    - email_id, subject, from_address, received_at
    - attachments: [{attachment_id, filename, size_bytes}]
    - detected_bank (if identifiable from sender/filename)
    - already_processed flag

    If validate_balances=True (default), PDFs are parsed to check opening balance
    against Opera 3's reconciled balance, filtering out already-processed statements.
    Invalid statements are automatically archived.
    """
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        from datetime import datetime, timedelta
        import tempfile
        import os

        # Get reconciled balance from Opera 3 (FoxPro)
        reconciled_balance = None
        if validate_balances:
            try:
                from sql_rag.opera3_foxpro import Opera3Reader
                reader = Opera3Reader(data_path)
                nbank_records = reader.read_table("nbank")
                for record in nbank_records:
                    nb_acnt = str(record.get('NB_ACNT', record.get('nb_acnt', ''))).strip().upper()
                    if nb_acnt == bank_code.upper():
                        # nk_recbal is in pence, convert to pounds
                        nk_recbal = float(record.get('NK_RECBAL', record.get('nk_recbal', 0)) or 0)
                        reconciled_balance = nk_recbal / 100.0
                        logger.info(f"Opera 3 reconciled balance for {bank_code}: £{reconciled_balance:,.2f}")
                        break
            except Exception as e:
                logger.warning(f"Could not get Opera 3 reconciled balance: {e}")

        # Calculate date range
        from_date = datetime.utcnow() - timedelta(days=days_back)

        # Get emails with attachments in date range
        result = email_storage.get_emails(
            from_date=from_date,
            page=1,
            page_size=500
        )

        statements_found = []
        already_processed_count = 0
        total_emails_scanned = 0
        total_pdfs_found = 0
        skipped_reasons = []  # Track why statements were filtered out

        # Load reconciled statement keys — skip fully reconciled statements
        try:
            reconciled_keys = email_storage.get_reconciled_statement_keys()
            reconciled_filenames = email_storage.get_reconciled_filenames()
        except Exception:
            reconciled_keys = set()
            reconciled_filenames = set()

        # Build effective reconciled balance for Opera 3 scan
        try:
            _rec_closing = email_storage.get_reconciled_closing_balances()
            _tracked_max = _rec_closing.get(bank_code, 0)
            effective_reconciled_balance = max(reconciled_balance or 0, _tracked_max)
        except Exception:
            effective_reconciled_balance = reconciled_balance

        # Load reconciled opening balances for chain-based completion detection
        try:
            _rec_openings = email_storage.get_reconciled_opening_balances()
            bank_rec_openings = _rec_openings.get(bank_code, set())
        except Exception:
            bank_rec_openings = set()

        for email in result.get('emails', []):
            email_id = email.get('id')
            if not email.get('has_attachments'):
                continue

            total_emails_scanned += 1

            # Get attachments for this email
            email_detail = email_storage.get_email_by_id(email_id)
            if not email_detail:
                continue

            attachments = email_detail.get('attachments', [])
            if not attachments:
                continue

            # Filter to potential bank statement attachments
            statement_attachments = []
            email_from = email.get('from_address', '')
            email_subject = email.get('subject', '')

            for att in attachments:
                filename = att.get('filename', '')
                content_type = att.get('content_type', '')
                attachment_id = att.get('attachment_id', '')

                if is_bank_statement_attachment(filename, content_type, email_from, email_subject):
                    total_pdfs_found += 1

                    # Skip fully reconciled statements — archive from load list
                    if (email_id, attachment_id) in reconciled_keys or filename in reconciled_filenames:
                        already_processed_count += 1
                        skipped_reasons.append(f"Statement {filename}: already reconciled")
                        continue

                    statement_attachments.append({
                        'attachment_id': attachment_id,
                        'filename': filename,
                        'size_bytes': att.get('size_bytes', 0),
                        'content_type': content_type,
                        'already_processed': False
                    })

            if statement_attachments:
                # Detect bank from sender or first attachment filename
                detected_bank = detect_bank_from_email(
                    email.get('from_address', ''),
                    statement_attachments[0]['filename']
                )

                # Extract statement date from filename/subject for ordering
                email_subject = email.get('subject', '')
                first_filename = statement_attachments[0]['filename']
                sort_key, statement_date = extract_statement_number_from_filename(first_filename, email_subject)

                # Add sort key and statement date to each attachment
                for att in statement_attachments:
                    att_sort_key, att_stmt_date = extract_statement_number_from_filename(att['filename'], email_subject)
                    att['sort_key'] = att_sort_key
                    att['statement_date'] = att_stmt_date

                # Validate balance for PDF statements if enabled
                # Balance is the control — not the tracking DB
                is_valid_statement = True
                statement_opening_balance = None
                validation_status = None

                if validate_balances and reconciled_balance is not None:
                    # Cache-only validation — no Gemini calls during scan
                    from sql_rag.pdf_extraction_cache import PDFExtractionCache, get_extraction_cache
                    scan_cache = get_extraction_cache()

                    for att in statement_attachments:
                        if att['filename'].lower().endswith('.pdf'):
                            try:
                                # Get provider and download attachment
                                provider_id = email_detail.get('provider_id')
                                message_id = email_detail.get('message_id')
                                folder_id = email_detail.get('folder_id', 'INBOX')

                                if provider_id and message_id and email_sync_manager and provider_id in email_sync_manager.providers:
                                    provider = email_sync_manager.providers[provider_id]

                                    # Get actual folder_id string
                                    if isinstance(folder_id, int):
                                        with email_storage._get_connection() as conn:
                                            cursor = conn.cursor()
                                            cursor.execute("SELECT folder_id FROM email_folders WHERE id = ?", (folder_id,))
                                            row = cursor.fetchone()
                                            if row:
                                                folder_id = row['folder_id']

                                    # Download PDF to check cache (no Gemini call)
                                    download_result = await provider.download_attachment(message_id, att['attachment_id'], folder_id)
                                    if download_result:
                                        content_bytes, _, _ = download_result
                                        pdf_hash = scan_cache.hash_pdf(content_bytes)
                                        cached = scan_cache.get(pdf_hash)

                                        if cached:
                                            # Cache hit — use cached extraction for validation
                                            info_data, _ = cached
                                            logger.info(f"Opera 3 scan cache HIT for {att['filename']} — validating from cache")

                                            opening_bal_raw = info_data.get('opening_balance')
                                            closing_bal_raw = info_data.get('closing_balance')

                                            if opening_bal_raw is not None:
                                                statement_opening_balance = float(opening_bal_raw)
                                                att['opening_balance'] = statement_opening_balance
                                                att['closing_balance'] = float(closing_bal_raw) if closing_bal_raw is not None else None

                                                # Chain check + balance validation
                                                stmt_closing = float(closing_bal_raw) if closing_bal_raw is not None else None
                                                chain_complete = stmt_closing is not None and round(stmt_closing, 2) in bank_rec_openings
                                                eff_bal = effective_reconciled_balance if effective_reconciled_balance is not None else reconciled_balance
                                                if chain_complete or (eff_bal is not None and statement_opening_balance < eff_bal - 0.01):
                                                    is_valid_statement = False
                                                    validation_status = 'already_processed'
                                                    if chain_complete:
                                                        logger.info(f"Opera 3 statement filtered out (chain): closing £{stmt_closing:,.2f} matches reconciled opening")
                                                    else:
                                                        logger.info(f"Opera 3 statement filtered out: opening £{statement_opening_balance:,.2f} < reconciled £{eff_bal:,.2f}")
                                                    skipped_reasons.append(f"Statement {att['filename']}: already processed (opening £{statement_opening_balance:,.2f} < reconciled £{eff_bal:,.2f})")

                                                    try:
                                                        email_storage.record_bank_statement_import(
                                                            bank_code=bank_code,
                                                            filename=att['filename'],
                                                            transactions_imported=0,
                                                            source='email',
                                                            target_system='opera3_already_processed',
                                                            email_id=email_id,
                                                            attachment_id=att['attachment_id'],
                                                            total_receipts=0,
                                                            total_payments=0,
                                                            imported_by='OPERA3_AUTO_SKIP_SCAN'
                                                        )
                                                        already_processed_count += 1

                                                        # Auto-archive the email for invalid statements
                                                        try:
                                                            archive_folder = "Archive/BankStatements/Invalid"
                                                            if email_sync_manager and provider_id in email_sync_manager.providers:
                                                                archive_provider = email_sync_manager.providers[provider_id]
                                                                move_success = await archive_provider.move_email(
                                                                    message_id,
                                                                    folder_id,
                                                                    archive_folder
                                                                )
                                                                if move_success:
                                                                    logger.info(f"Opera 3: Auto-archived invalid statement email {email_id} to {archive_folder}")
                                                                else:
                                                                    logger.warning(f"Opera 3: Failed to auto-archive invalid statement email {email_id}")
                                                        except Exception as archive_err:
                                                            logger.warning(f"Opera 3: Could not auto-archive invalid statement email: {archive_err}")
                                                    except:
                                                        pass
                                        else:
                                            # Cache miss — skip Gemini during scan, will extract when user selects
                                            logger.info(f"Opera 3 scan cache MISS for {att['filename']} — skipping extraction during scan")
                            except Exception as e:
                                logger.warning(f"Opera 3: Could not validate statement balance: {e}")
                                pass

                # Only add valid statements
                if is_valid_statement:
                    statements_found.append({
                        'email_id': email_id,
                        'message_id': email.get('message_id'),
                        'subject': email.get('subject'),
                        'from_address': email.get('from_address'),
                        'from_name': email.get('from_name'),
                        'received_at': email.get('received_at'),
                        'attachments': statement_attachments,
                        'detected_bank': detected_bank,
                        'already_processed': all(a['already_processed'] for a in statement_attachments),
                        'sort_key': sort_key,
                        'statement_date': statement_date,
                        'opening_balance': statement_opening_balance
                    })

        # Deduplicate: if the same filename appears in multiple emails, keep only the newest
        # Archive the duplicate (older) email to keep inbox clean
        seen_filenames = {}  # filename -> index in statements_found
        deduped_statements = []
        duplicates_archived = 0
        duplicate_emails_to_archive = []

        for stmt in statements_found:
            filenames = [a['filename'] for a in stmt.get('attachments', [])]
            is_duplicate = False
            for fn in filenames:
                fn_lower = fn.lower().strip()
                if fn_lower in seen_filenames:
                    existing = deduped_statements[seen_filenames[fn_lower]]
                    existing_date = existing.get('received_at', '')
                    new_date = stmt.get('received_at', '')
                    if new_date > existing_date:
                        duplicate_emails_to_archive.append((existing['email_id'], existing.get('message_id'), fn))
                        deduped_statements[seen_filenames[fn_lower]] = stmt
                    else:
                        duplicate_emails_to_archive.append((stmt['email_id'], stmt.get('message_id'), fn))
                    is_duplicate = True
                    logger.info(f"Opera 3: Duplicate statement filtered: {fn} (email_id={stmt['email_id']})")
                    break
            if not is_duplicate:
                for fn in filenames:
                    seen_filenames[fn.lower().strip()] = len(deduped_statements)
                deduped_statements.append(stmt)

        if len(deduped_statements) < len(statements_found):
            logger.info(f"Opera 3: Deduplicated {len(statements_found) - len(deduped_statements)} duplicate statement(s)")

        # Archive duplicate emails
        for dup_email_id, dup_message_id, dup_filename in duplicate_emails_to_archive:
            if dup_message_id and email_sync_manager:
                try:
                    email_detail = email_storage.get_email_by_id(dup_email_id)
                    if email_detail:
                        provider_id = email_detail.get('provider_id')
                        if provider_id and provider_id in email_sync_manager.providers:
                            provider = email_sync_manager.providers[provider_id]
                            moved = await provider.move_email(dup_message_id, 'INBOX', 'Archive/Bank Statements')
                            if moved:
                                duplicates_archived += 1
                                logger.info(f"Opera 3: Archived duplicate: {dup_filename} (email_id={dup_email_id})")
                            else:
                                logger.warning(f"Opera 3: Failed to archive duplicate email_id={dup_email_id}")
                except Exception as archive_err:
                    logger.warning(f"Opera 3: Could not archive duplicate email: {archive_err}")

        statements_found = deduped_statements

        # Sort statements in sequential import order by chaining
        # opening/closing balances starting from reconciled balance
        if reconciled_balance is not None and len(statements_found) > 1:
            ordered = []
            remaining = list(statements_found)
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
                    remaining.sort(key=lambda s: (0 if s.get('opening_balance') is not None else 1, s.get('opening_balance') or 0, s.get('sort_key', (9999,))))
                    ordered.extend(remaining)
                    break
            statements_found = ordered
        else:
            statements_found.sort(key=lambda s: (0 if s.get('opening_balance') is not None else 1, s.get('opening_balance') or 0, s.get('sort_key', (9999,))))

        # Add sequence numbers and detect missing statements
        expected_opening = reconciled_balance if reconciled_balance else None
        missing_statements = []

        for i, stmt in enumerate(statements_found, start=1):
            stmt['import_sequence'] = i
            del stmt['sort_key']
            for att in stmt['attachments']:
                if 'sort_key' in att:
                    del att['sort_key']

            # Check for gaps in the sequence
            opening = stmt.get('opening_balance')
            closing = None
            for att in stmt['attachments']:
                if att.get('closing_balance'):
                    closing = att['closing_balance']
                    break

            if expected_opening is not None and opening is not None:
                if abs(opening - expected_opening) > 0.02:
                    stmt['has_gap'] = True
                    stmt['expected_opening'] = expected_opening
                    missing_statements.append({
                        'position': i,
                        'expected_opening': expected_opening,
                        'actual_opening': opening,
                        'gap_amount': opening - expected_opening
                    })
                else:
                    stmt['has_gap'] = False

            if closing is not None:
                expected_opening = closing

        # Build response message
        message = None
        if len(statements_found) == 0:
            balance_info = f" Expected next statement opening balance: £{reconciled_balance:,.2f}." if reconciled_balance is not None else ""
            pdfs_info = f" Found {total_pdfs_found} PDF attachment(s) in {total_emails_scanned} email(s) with attachments." if total_pdfs_found > 0 else f" No PDF attachments found in {total_emails_scanned} email(s) scanned."
            processed_info = f" {already_processed_count} statement(s) already processed." if already_processed_count > 0 else ""
            dup_info = f" {duplicates_archived} duplicate(s) archived." if duplicates_archived > 0 else ""
            message = f"Scan complete — no new statements found for import.{balance_info}{pdfs_info}{processed_info}{dup_info}"
        elif len(statements_found) == 1:
            dup_info = f" {duplicates_archived} duplicate(s) archived." if duplicates_archived > 0 else ""
            message = f"Found 1 statement ready for import.{dup_info}"
        else:
            dup_info = f" {duplicates_archived} duplicate(s) archived." if duplicates_archived > 0 else ""
            message = f"Found {len(statements_found)} statement(s). Import in sequence order (1, 2, 3...) to maintain balance chain.{dup_info}"
        if missing_statements:
            gaps_msg = f" WARNING: {len(missing_statements)} missing statement(s) detected in sequence."
            message = (message or "") + gaps_msg

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "statements_found": statements_found,
            "total_found": len(statements_found),
            "already_processed_count": already_processed_count,
            "duplicates_archived": duplicates_archived,
            "total_emails_scanned": total_emails_scanned,
            "total_pdfs_found": total_pdfs_found,
            "days_searched": days_back,
            "bank_code": bank_code,
            "reconciled_balance": reconciled_balance,
            "missing_statements": missing_statements if missing_statements else None,
            "has_missing_statements": len(missing_statements) > 0,
            "skipped_reasons": skipped_reasons if skipped_reasons else None,
            "message": message
        }

    except Exception as e:
        logger.error(f"Opera 3: Error scanning emails for bank statements: {e}")
        return {"success": False, "error": str(e)}





@router.post("/api/opera3/bank-import/preview-from-email")
async def opera3_preview_bank_import_from_email(
    email_id: int = Query(..., description="Email ID"),
    attachment_id: str = Query(..., description="Attachment ID"),
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    bank_code: str = Query(..., description="Opera bank account code")
):
    """
    Preview bank statement from email attachment for Opera 3 (FoxPro).
    Uses AI extraction for PDFs and matches against Opera 3 customer/supplier data.
    """
    if not email_storage or not email_sync_manager:
        raise HTTPException(status_code=503, detail="Email services not initialized")

    if not data_path or not data_path.strip():
        return {"success": False, "error": "Opera 3 data path is required"}

    import os
    if not os.path.isdir(data_path):
        return {"success": False, "error": f"Opera 3 data path not found: {data_path}"}

    try:
        import tempfile
        from sql_rag.opera3_foxpro import Opera3Reader
        from sql_rag.statement_reconcile_opera3 import StatementReconcilerOpera3
        from sql_rag.bank_import_opera3 import BankStatementMatcherOpera3

        # Get email details
        email = email_storage.get_email_by_id(email_id)
        if not email:
            return {"success": False, "error": f"Email {email_id} not found"}

        # Find the attachment
        attachments = email.get('attachments', [])
        attachment_meta = next(
            (a for a in attachments if a.get('attachment_id') == attachment_id),
            None
        )
        if not attachment_meta:
            return {"success": False, "error": f"Attachment {attachment_id} not found"}

        filename = attachment_meta.get('filename', 'statement')

        # Get provider for this email
        provider_id = email.get('provider_id')
        provider = email_sync_manager.providers.get(provider_id)
        if not provider:
            return {"success": False, "error": f"Provider {provider_id} not available"}

        message_id = email.get('message_id')

        # Get folder_id
        folder_id_db = email.get('folder_id')
        folder_id = 'INBOX'
        if folder_id_db:
            with email_storage._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT folder_id FROM email_folders WHERE id = ?", (folder_id_db,))
                row = cursor.fetchone()
                if row:
                    folder_id = row['folder_id']

        # Download attachment content
        result = await provider.download_attachment(message_id, attachment_id, folder_id)
        if not result:
            return {"success": False, "error": "Failed to download attachment"}

        content_bytes, _, _ = result

        # Initialize Opera 3 reader
        reader = Opera3Reader(data_path)

        # Get reconciled balance from Opera 3
        reconciled_balance = None
        try:
            nbank_records = reader.read_table("nbank")
            for record in nbank_records:
                nb_acnt = str(record.get('NB_ACNT', record.get('nb_acnt', ''))).strip().upper()
                if nb_acnt == bank_code.upper():
                    nk_recbal = float(record.get('NK_RECBAL', record.get('nk_recbal', 0)) or 0)
                    reconciled_balance = nk_recbal / 100.0
                    break
        except Exception as e:
            logger.warning(f"Could not get Opera 3 reconciled balance: {e}")

        # Save to temp file for AI extraction
        file_ext = '.' + filename.split('.')[-1] if '.' in filename else '.pdf'
        with tempfile.NamedTemporaryFile(suffix=file_ext, delete=False) as tmp_file:
            tmp_file.write(content_bytes)
            tmp_path = tmp_file.name

        try:
            # Use StatementReconcilerOpera3 for AI extraction
            reconciler = StatementReconcilerOpera3(reader, config=config)
            statement_info, stmt_transactions = reconciler.extract_transactions_from_pdf(tmp_path)

            # Validate statement sequence
            if statement_info and statement_info.opening_balance is not None and reconciled_balance is not None:
                if statement_info.opening_balance < reconciled_balance - 0.01:
                    # Already processed - mark as processed and archive
                    try:
                        email_storage.record_bank_statement_import(
                            bank_code=bank_code,
                            filename=filename,
                            transactions_imported=0,
                            source='email',
                            target_system='opera3_already_processed',
                            email_id=email_id,
                            attachment_id=attachment_id,
                            total_receipts=0,
                            total_payments=0,
                            imported_by='OPERA3_AUTO_SKIP'
                        )

                        # Auto-archive the email
                        try:
                            archive_folder = "Archive/BankStatements/Invalid"
                            move_success = await provider.move_email(message_id, folder_id, archive_folder)
                            if move_success:
                                logger.info(f"Opera 3: Auto-archived invalid statement email {email_id}")
                        except Exception as archive_err:
                            logger.warning(f"Opera 3: Could not archive email: {archive_err}")

                    except Exception as track_err:
                        logger.warning(f"Failed to auto-mark statement as processed: {track_err}")

                    return {
                        "success": False,
                        "source": "opera3",
                        "status": "skipped",
                        "reason": "already_processed",
                        "bank_code": bank_code,
                        "filename": filename,
                        "statement_info": {
                            "bank_name": statement_info.bank_name if statement_info else None,
                            "account_number": statement_info.account_number if statement_info else None,
                            "opening_balance": statement_info.opening_balance if statement_info else None,
                            "closing_balance": statement_info.closing_balance if statement_info else None,
                        },
                        "reconciled_balance": reconciled_balance,
                        "errors": [
                            "STATEMENT ALREADY PROCESSED",
                            f"Statement Opening Balance: £{statement_info.opening_balance:,.2f}",
                            f"Opera 3 Reconciled Balance: £{reconciled_balance:,.2f}",
                            "This statement has a lower opening balance than Opera's reconciled balance, indicating it has already been processed.",
                            "The email has been automatically archived."
                        ]
                    }

            # Use matcher to categorize transactions
            matcher = BankStatementMatcherOpera3(data_path)

            # Process transactions through matcher
            from sql_rag.bank_import_opera3 import BankTransaction
            transactions = []
            for i, st in enumerate(stmt_transactions, start=1):
                txn = BankTransaction(
                    row_number=i,
                    date=st.date,
                    amount=st.amount,
                    subcategory=st.transaction_type or '',
                    memo=st.description or '',
                    name=st.description or '',
                    reference=st.reference or ''
                )
                transactions.append(txn)

            # Process through matcher
            matcher.process_transactions(transactions, check_duplicates=True, bank_code=bank_code)

            # Load pattern learner for suggestions on unmatched items
            try:
                from sql_rag.bank_patterns import BankPatternLearner
                # For Opera 3, use data_path folder name as company code
                company_code = os.path.basename(data_path.rstrip('/\\')) or 'opera3_default'
                pattern_learner = BankPatternLearner(company_code=company_code)
            except Exception as e:
                logger.warning(f"Could not initialize pattern learner for Opera 3: {e}")
                pattern_learner = None

            # Categorize results
            matched_receipts = []
            matched_payments = []
            repeat_entries = []
            already_posted = []
            skipped = []
            unmatched = []

            for txn in transactions:
                txn_data = {
                    "row": txn.row_number,
                    "date": txn.date.isoformat(),
                    "amount": txn.amount,
                    "name": txn.name,
                    "reference": txn.reference,
                    "account": txn.matched_account,
                    "account_name": txn.matched_name,
                    "match_score": txn.match_score if txn.match_score else 0,
                    "reason": txn.skip_reason,
                    "repeat_entry_ref": getattr(txn, 'repeat_entry_ref', None),
                    "repeat_entry_desc": getattr(txn, 'repeat_entry_desc', None),
                }

                if txn.action == 'sales_receipt':
                    matched_receipts.append(txn_data)
                elif txn.action == 'purchase_payment':
                    matched_payments.append(txn_data)
                elif txn.action == 'repeat_entry':
                    repeat_entries.append(txn_data)
                elif txn.skip_reason and 'Already' in txn.skip_reason:
                    already_posted.append(txn_data)
                elif not txn.matched_account:
                    # Unmatched - try to get suggestions from pattern learner
                    if pattern_learner:
                        try:
                            suggestion = pattern_learner.find_pattern(txn.memo or txn.name)
                            if suggestion:
                                txn_data['suggested_type'] = suggestion.transaction_type
                                txn_data['suggested_account'] = suggestion.account_code
                                txn_data['suggested_account_name'] = suggestion.account_name
                                txn_data['suggested_ledger_type'] = suggestion.ledger_type
                                txn_data['suggested_vat_code'] = suggestion.vat_code
                                txn_data['suggested_nominal_code'] = suggestion.nominal_code
                                txn_data['suggestion_confidence'] = suggestion.confidence
                                txn_data['suggestion_source'] = suggestion.match_type
                        except Exception as e:
                            logger.warning(f"Opera 3 pattern lookup failed: {e}")
                    unmatched.append(txn_data)
                else:
                    skipped.append(txn_data)

            return {
                "success": True,
                "source": "opera3",
                "data_path": data_path,
                "filename": filename,
                "total_transactions": len(transactions),
                "matched_receipts": matched_receipts,
                "matched_payments": matched_payments,
                "repeat_entries": repeat_entries,
                "already_posted": already_posted,
                "unmatched": unmatched,
                "skipped": skipped,
                "statement_bank_info": {
                    "bank_name": statement_info.bank_name if statement_info else None,
                    "account_number": statement_info.account_number if statement_info else None,
                    "sort_code": statement_info.sort_code if statement_info else None,
                    "opening_balance": statement_info.opening_balance if statement_info else None,
                    "closing_balance": statement_info.closing_balance if statement_info else None,
                },
                "reconciled_balance": reconciled_balance,
                "errors": []
            }

        finally:
            os.unlink(tmp_path)

    except Exception as e:
        logger.error(f"Opera 3 preview-from-email error: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}





@router.post("/api/opera3/bank-import/preview-from-pdf")
async def opera3_preview_bank_import_from_pdf(
    file_path: str = Query(..., description="Path to PDF file"),
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    bank_code: str = Query(..., description="Opera bank account code")
):
    """
    Preview bank statement from PDF file for Opera 3 (FoxPro).
    Uses AI extraction for PDFs and matches against Opera 3 customer/supplier data.
    """
    if not data_path or not data_path.strip():
        return {"success": False, "error": "Opera 3 data path is required"}

    import os
    if not os.path.isdir(data_path):
        return {"success": False, "error": f"Opera 3 data path not found: {data_path}"}

    if not file_path or not os.path.exists(file_path):
        return {"success": False, "error": f"PDF file not found: {file_path}"}

    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        from sql_rag.statement_reconcile_opera3 import StatementReconcilerOpera3
        from sql_rag.bank_import_opera3 import BankStatementMatcherOpera3

        filename = os.path.basename(file_path)

        # Initialize Opera 3 reader
        reader = Opera3Reader(data_path)

        # Get bank details from Opera 3 (reconciled balance, sort code, account number)
        reconciled_balance = None
        opera_sort = ''
        opera_acct = ''
        try:
            nbank_records = reader.read_table("nbank")
            for record in nbank_records:
                nb_acnt = str(record.get('NB_ACNT', record.get('nb_acnt', record.get('nk_acnt', '')))).strip().upper()
                if nb_acnt == bank_code.upper():
                    nk_recbal = float(record.get('NK_RECBAL', record.get('nk_recbal', 0)) or 0)
                    reconciled_balance = nk_recbal / 100.0
                    opera_sort = str(record.get('NK_SORT', record.get('nk_sort', ''))).strip()
                    opera_acct = str(record.get('NK_NUMBER', record.get('nk_number', ''))).strip()
                    break
        except Exception as e:
            logger.warning(f"Could not get Opera 3 bank details: {e}")

        # Use StatementReconcilerOpera3 for AI extraction
        reconciler = StatementReconcilerOpera3(reader, config=config)
        statement_info, stmt_transactions = reconciler.extract_transactions_from_pdf(file_path)

        # Validate bank account match (sort code / account number)
        if statement_info and opera_sort and opera_acct:
            stmt_sort = (statement_info.sort_code or '').replace('-', '').replace(' ', '')
            stmt_acct = (statement_info.account_number or '').replace('-', '').replace(' ', '')
            opera_sort_norm = opera_sort.replace('-', '').replace(' ', '')
            opera_acct_norm = opera_acct.replace('-', '').replace(' ', '')
            if stmt_sort and stmt_acct and opera_sort_norm and opera_acct_norm:
                if stmt_sort != opera_sort_norm or stmt_acct != opera_acct_norm:
                    return {
                        "success": False,
                        "source": "opera3",
                        "bank_mismatch": True,
                        "detected_bank": f"{stmt_sort} / {stmt_acct}",
                        "selected_bank": f"{opera_sort_norm} / {opera_acct_norm} ({bank_code})",
                        "error": "Bank account mismatch"
                    }

        # Check statement sequence — correct opening balance if AI got it wrong
        # (parity with SE preview-from-pdf: don't reject, just correct)
        opening_balance = statement_info.opening_balance if statement_info else None
        if opening_balance is not None and reconciled_balance is not None:
            tolerance = 0.02
            if abs(opening_balance - reconciled_balance) > tolerance:
                # AI likely extracted wrong opening balance — use reconciled balance
                logger.warning(f"Opera 3 preview-from-pdf: Opening balance mismatch: extracted £{opening_balance:,.2f} vs reconciled £{reconciled_balance:,.2f} — using reconciled")
                if statement_info:
                    statement_info = type(statement_info)(
                        bank_name=statement_info.bank_name,
                        account_number=statement_info.account_number,
                        sort_code=statement_info.sort_code,
                        statement_date=statement_info.statement_date,
                        period_start=statement_info.period_start,
                        period_end=statement_info.period_end,
                        opening_balance=reconciled_balance,
                        closing_balance=statement_info.closing_balance
                    )
                opening_balance = reconciled_balance
        elif opening_balance is None and reconciled_balance is not None:
            if statement_info:
                statement_info = type(statement_info)(
                    bank_name=statement_info.bank_name,
                    account_number=statement_info.account_number,
                    sort_code=statement_info.sort_code,
                    statement_date=statement_info.statement_date,
                    period_start=statement_info.period_start,
                    period_end=statement_info.period_end,
                    opening_balance=reconciled_balance,
                    closing_balance=statement_info.closing_balance
                )
            opening_balance = reconciled_balance

        # Validate closing balance using transaction balance chain from opening.
        # Walks from opening, finding each transaction where current + amount = balance.
        # Excludes phantom transactions from other accounts (e.g. Monzo savings).
        if opening_balance is not None and stmt_transactions:
            try:
                current_bal = opening_balance
                chain_used = set()
                for _ in range(len(stmt_transactions)):
                    found = False
                    for i, st in enumerate(stmt_transactions):
                        if i in chain_used:
                            continue
                        expected = round(current_bal + st.amount, 2)
                        if st.balance is not None and abs(expected - st.balance) < 0.02:
                            current_bal = st.balance
                            chain_used.add(i)
                            found = True
                            break
                    if not found:
                        break
                if chain_used and statement_info:
                    statement_info = type(statement_info)(
                        bank_name=statement_info.bank_name,
                        account_number=statement_info.account_number,
                        sort_code=statement_info.sort_code,
                        statement_date=statement_info.statement_date,
                        period_start=statement_info.period_start,
                        period_end=statement_info.period_end,
                        opening_balance=statement_info.opening_balance,
                        closing_balance=current_bal
                    )
                    excluded = len(stmt_transactions) - len(chain_used)
                    if excluded > 0:
                        logger.info(f"Opera 3 preview-from-pdf: Balance chain excluded {excluded} phantom transaction(s), closing=£{current_bal:,.2f}")
            except Exception as chain_err:
                logger.warning(f"Opera 3 preview-from-pdf: Balance chain failed: {chain_err}")

        # Use matcher to categorize transactions
        matcher = BankStatementMatcherOpera3(data_path)

        # Process transactions through matcher
        from sql_rag.bank_import_opera3 import BankTransaction
        transactions = []
        for i, st in enumerate(stmt_transactions, start=1):
            txn = BankTransaction(
                row_number=i,
                date=st.date,
                amount=st.amount,
                subcategory=st.transaction_type or '',
                memo=st.description or '',
                name=st.description or '',
                reference=st.reference or ''
            )
            transactions.append(txn)

        # Process through matcher
        matcher.process_transactions(transactions, check_duplicates=True, bank_code=bank_code)

        # Load pattern learner for suggestions on unmatched items
        try:
            from sql_rag.bank_patterns import BankPatternLearner
            company_code = os.path.basename(data_path.rstrip('/\\')) or 'opera3_default'
            pattern_learner = BankPatternLearner(company_code=company_code)
        except Exception as e:
            logger.warning(f"Could not initialize pattern learner for Opera 3 PDF: {e}")
            pattern_learner = None

        # Categorize results
        matched_receipts = []
        matched_payments = []
        repeat_entries = []
        already_posted = []
        unmatched = []
        skipped = []

        for txn in transactions:
            txn_data = {
                "row": txn.row_number,
                "date": txn.date.isoformat(),
                "amount": txn.amount,
                "name": txn.name,
                "reference": txn.reference,
                "account": txn.matched_account,
                "account_name": txn.matched_name,
                "match_score": txn.match_score if txn.match_score else 0,
                "reason": txn.skip_reason,
                "repeat_entry_ref": getattr(txn, 'repeat_entry_ref', None),
                "repeat_entry_desc": getattr(txn, 'repeat_entry_desc', None),
            }

            if txn.action == 'sales_receipt':
                matched_receipts.append(txn_data)
            elif txn.action == 'purchase_payment':
                matched_payments.append(txn_data)
            elif txn.action == 'repeat_entry':
                repeat_entries.append(txn_data)
            elif txn.skip_reason and 'Already' in txn.skip_reason:
                already_posted.append(txn_data)
            elif not txn.matched_account:
                # Unmatched - add similarity key for "apply to all similar"
                txn_data['similarity_key'] = _compute_similarity_key(txn.name, txn.memo or "")
                # Try to get suggestions from pattern learner
                if pattern_learner:
                    try:
                        suggestion = pattern_learner.find_pattern(txn.memo or txn.name)
                        if suggestion:
                            txn_data['suggested_type'] = suggestion.transaction_type
                            txn_data['suggested_account'] = suggestion.account_code
                            txn_data['suggested_account_name'] = suggestion.account_name
                            txn_data['suggested_ledger_type'] = suggestion.ledger_type
                            txn_data['suggested_vat_code'] = suggestion.vat_code
                            txn_data['suggested_nominal_code'] = suggestion.nominal_code
                            txn_data['suggestion_confidence'] = suggestion.confidence
                            txn_data['suggestion_source'] = suggestion.match_type
                    except Exception as e:
                        logger.warning(f"Opera 3 PDF pattern lookup failed: {e}")
                unmatched.append(txn_data)
            else:
                skipped.append(txn_data)

        # Compute similarity counts for unmatched items
        from collections import Counter
        sim_key_counts = Counter(item.get('similarity_key', '') for item in unmatched)
        for item in unmatched:
            key = item.get('similarity_key', '')
            item['similar_count'] = sim_key_counts.get(key, 1)

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "filename": filename,
            "total_transactions": len(transactions),
            "matched_receipts": matched_receipts,
            "matched_payments": matched_payments,
            "repeat_entries": repeat_entries,
            "already_posted": already_posted,
            "unmatched": unmatched,
            "skipped": skipped,
            "statement_bank_info": {
                "bank_name": statement_info.bank_name if statement_info else None,
                "account_number": statement_info.account_number if statement_info else None,
                "sort_code": statement_info.sort_code if statement_info else None,
                "opening_balance": statement_info.opening_balance if statement_info else None,
                "closing_balance": statement_info.closing_balance if statement_info else None,
            },
            "reconciled_balance": reconciled_balance,
            "errors": []
        }

    except Exception as e:
        logger.error(f"Opera 3 preview-from-pdf error: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}





@router.post("/api/opera3/bank-import/import-from-pdf")
async def opera3_import_bank_statement_from_pdf(
    request: Request,
    file_path: str = Query(..., description="Path to PDF file"),
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    bank_code: str = Query(..., description="Opera bank account code"),
    auto_allocate: bool = Query(False, description="Auto-allocate to oldest invoices"),
    auto_reconcile: bool = Query(False, description="Auto-reconcile imported entries against bank statement"),
    resume_import_id: Optional[int] = Query(None, description="Import ID to resume from (skips already-posted lines)")
):
    """
    Import bank statement from PDF file for Opera 3 (FoxPro).
    Uses AI extraction for PDFs and imports directly to Opera 3 DBF files.
    """
    import os
    from datetime import datetime

    if not data_path or not data_path.strip():
        return {"success": False, "error": "Opera 3 data path is required"}

    if not os.path.isdir(data_path):
        return {"success": False, "error": f"Opera 3 data path not found: {data_path}"}

    if not file_path or not os.path.exists(file_path):
        return {"success": False, "error": f"PDF file not found: {file_path}"}

    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        from sql_rag.statement_reconcile_opera3 import StatementReconcilerOpera3
        from sql_rag.bank_import_opera3 import BankStatementMatcherOpera3, BankTransaction

        filename = os.path.basename(file_path)

        # Get request body for overrides
        body = await request.json() if request.headers.get('content-type') == 'application/json' else {}
        overrides = body.get('overrides', [])
        selected_rows = body.get('selected_rows', [])
        date_overrides = body.get('date_overrides', [])
        rejected_refund_rows = body.get('rejected_refund_rows', [])
        auto_allocate_disabled_rows = body.get('auto_allocate_disabled_rows', [])

        # Initialize Opera 3 reader
        reader = Opera3Reader(data_path)

        # Use StatementReconcilerOpera3 for AI extraction
        reconciler = StatementReconcilerOpera3(reader, config=config)
        statement_info, stmt_transactions = reconciler.extract_transactions_from_pdf(file_path)

        if not statement_info:
            return {"success": False, "error": "Failed to extract statement information from PDF"}

        # Check for statement period overlap (prevent double-posting)
        skip_overlap_check = body.get('skip_overlap_check', False)
        if not skip_overlap_check and email_storage:
            period_start_str = statement_info.period_start.isoformat() if hasattr(statement_info, 'period_start') and statement_info.period_start else None
            period_end_str = statement_info.period_end.isoformat() if hasattr(statement_info, 'period_end') and statement_info.period_end else None
            # Fall back to first/last transaction dates
            if not period_start_str and stmt_transactions:
                dates = [st.date for st in stmt_transactions if st.date]
                if dates:
                    period_start_str = min(dates).isoformat() if hasattr(min(dates), 'isoformat') else str(min(dates))
                    period_end_str = max(dates).isoformat() if hasattr(max(dates), 'isoformat') else str(max(dates))

            if period_start_str and period_end_str:
                overlap = email_storage.check_period_overlap(
                    bank_code=bank_code,
                    period_start=period_start_str,
                    period_end=period_end_str,
                    exclude_import_id=resume_import_id
                )
                if overlap:
                    return {
                        "success": False,
                        "overlap_warning": True,
                        "error": f"Statement period overlaps with a previously imported statement",
                        "overlap_details": {
                            "existing_import_id": overlap['import_id'],
                            "existing_filename": overlap['filename'],
                            "existing_period": f"{overlap['period_start']} to {overlap['period_end']}",
                            "existing_import_date": overlap['import_date'],
                            "new_period": f"{period_start_str} to {period_end_str}"
                        }
                    }

        # Use matcher for transaction processing
        matcher = BankStatementMatcherOpera3(data_path)

        # Convert to BankTransaction objects
        transactions = []
        for i, st in enumerate(stmt_transactions, start=1):
            txn = BankTransaction(
                row_number=i,
                date=st.date,
                amount=st.amount,
                subcategory=st.transaction_type or '',
                memo=st.description or '',
                name=st.description or '',
                reference=st.reference or ''
            )
            transactions.append(txn)

        # Apply date overrides
        date_override_map = {d['row']: d['date'] for d in date_overrides}
        for txn in transactions:
            if txn.row_number in date_override_map:
                override_date = date_override_map[txn.row_number]
                if isinstance(override_date, str):
                    txn.date = datetime.strptime(override_date, '%Y-%m-%d').date()
                else:
                    txn.date = override_date

        # Apply overrides from user selections
        override_map = {o['row']: o for o in overrides}
        for txn in transactions:
            if txn.row_number in override_map:
                override = override_map[txn.row_number]
                if override.get('account'):
                    txn.matched_account = override['account']
                    txn.matched_name = override.get('account_name', '')
                    txn.match_score = 1.0
                if override.get('ledger_type'):
                    txn.match_type = 'customer' if override['ledger_type'] == 'C' else 'supplier'
                if override.get('transaction_type'):
                    txn.action = override['transaction_type']
                    # Store bank transfer details on the transaction
                    if override['transaction_type'] == 'bank_transfer':
                        txn.bank_transfer_details = override.get('bank_transfer_details', {})
                # Apply project/department/VAT codes for nominal entries
                if override.get('project_code'):
                    txn.project_code = override['project_code']
                if override.get('department_code'):
                    txn.department_code = override['department_code']
                if override.get('vat_code'):
                    txn.vat_code = override['vat_code']

        # Process transactions through matcher
        matcher.process_transactions(transactions, check_duplicates=True, bank_code=bank_code)

        # Filter to selected rows if provided
        if selected_rows:
            selected_set = set(selected_rows)
            transactions = [t for t in transactions if t.row_number in selected_set]

        # Load already-posted lines for partial recovery (skip on resume)
        already_posted_o3 = {}
        if resume_import_id and email_storage:
            try:
                already_posted_o3 = email_storage.get_posted_lines(resume_import_id)
                if already_posted_o3:
                    logger.info(f"Resume Opera 3 import: {len(already_posted_o3)} lines already posted for import_id={resume_import_id}")
                    transactions = [t for t in transactions if t.row_number not in already_posted_o3]
            except Exception as e:
                logger.warning(f"Could not load posted lines for Opera 3 resume: {e}")

        # Import using the matcher's import method
        result = matcher.import_approved(
            result=matcher._create_preview_result(transactions, filename),
            bank_code=bank_code,
            validate_only=False,
            auto_allocate=auto_allocate
        )

        # Build response - include already-posted lines from previous partial import
        imported = []
        errors = []

        for row_num, entry_num in already_posted_o3.items():
            imported.append({
                'row': row_num,
                'date': '',
                'amount': 0,
                'name': '',
                'action': '',
                'account': '',
                'entry_number': entry_num,
                'already_posted': True
            })

        for txn in result.transactions:
            if txn.action in ('sales_receipt', 'purchase_payment', 'sales_refund', 'purchase_refund', 'nominal_payment', 'nominal_receipt', 'bank_transfer'):
                if hasattr(txn, 'entry_number') and txn.entry_number:
                    imported.append({
                        'row': txn.row_number,
                        'date': txn.date.isoformat(),
                        'amount': txn.amount,
                        'name': txn.name,
                        'action': txn.action,
                        'account': txn.matched_account,
                        'entry_number': txn.entry_number
                    })
                elif txn.skip_reason:
                    errors.append({'row': txn.row_number, 'error': txn.skip_reason})

        # Calculate totals
        receipts_imported = sum(1 for t in imported if t['action'] == 'sales_receipt')
        payments_imported = sum(1 for t in imported if t['action'] == 'purchase_payment')
        transfers_imported = sum(1 for t in imported if t['action'] == 'bank_transfer')
        total_receipts = sum(t['amount'] for t in imported if t['action'] == 'sales_receipt')
        total_payments = sum(abs(t['amount']) for t in imported if t['action'] == 'purchase_payment')

        # Auto-reconciliation for Opera 3
        reconciliation_result = None
        if auto_reconcile and len(imported) > 0 and len(errors) == 0:
            try:
                from sql_rag.opera3_write_provider import get_opera3_writer

                # Collect entries with valid entry_numbers
                entries_to_reconcile = []
                statement_line = 10

                for txn in imported:
                    entry_num = txn.get('entry_number')
                    if entry_num:
                        entries_to_reconcile.append({
                            'entry_number': entry_num,
                            'statement_line': statement_line
                        })
                        statement_line += 10

                if len(entries_to_reconcile) == len(imported):
                    # All entries have entry_numbers - proceed
                    latest_date = None
                    for txn in imported:
                        if txn.get('date'):
                            txn_date = datetime.strptime(txn['date'], '%Y-%m-%d').date() if isinstance(txn['date'], str) else txn['date']
                            if latest_date is None or txn_date > latest_date:
                                latest_date = txn_date

                    if latest_date is None:
                        latest_date = datetime.now().date()

                    statement_number = int(latest_date.strftime('%y%m%d'))

                    # Use Opera 3 writer (agent or direct)
                    foxpro_import = get_opera3_writer(data_path)
                    recon_result = foxpro_import.mark_entries_reconciled(
                        bank_account=bank_code,
                        entries=entries_to_reconcile,
                        statement_number=statement_number,
                        statement_date=latest_date,
                        reconciliation_date=datetime.now().date()
                    )

                    reconciliation_result = {
                        "success": recon_result.success if hasattr(recon_result, 'success') else True,
                        "entries_reconciled": len(entries_to_reconcile),
                        "statement_number": statement_number,
                        "statement_date": latest_date.isoformat(),
                        "messages": []
                    }

                    logger.info(f"Opera 3 PDF auto-reconciliation complete: {len(entries_to_reconcile)} entries")
                else:
                    missing_count = len(imported) - len(entries_to_reconcile)
                    reconciliation_result = {
                        "success": False,
                        "entries_reconciled": 0,
                        "messages": [f"Cannot auto-reconcile: {missing_count} entries missing entry_number"]
                    }

            except ImportError:
                logger.warning("Opera 3 FoxPro import not available for reconciliation")
                reconciliation_result = {
                    "success": False,
                    "entries_reconciled": 0,
                    "messages": ["Opera 3 reconciliation module not available"]
                }
            except Exception as recon_err:
                logger.error(f"Opera 3 PDF auto-reconciliation error: {recon_err}")
                reconciliation_result = {
                    "success": False,
                    "entries_reconciled": 0,
                    "messages": [f"Auto-reconciliation error: {str(recon_err)}"]
                }

        # Record in import history and persist statement transactions
        if len(imported) > 0 and email_storage:
            try:
                current_user = getattr(request.state, 'user', None)
                imported_by = current_user.get('username', 'Unknown') if current_user else 'Unknown'

                # Extract statement metadata
                stmt_opening = statement_info.get('opening_balance') if isinstance(statement_info, dict) else getattr(statement_info, 'opening_balance', None)
                stmt_closing = statement_info.get('closing_balance') if isinstance(statement_info, dict) else getattr(statement_info, 'closing_balance', None)
                stmt_date_str = None
                stmt_acct_num = None
                stmt_sort_code = None
                if isinstance(statement_info, dict):
                    stmt_date_str = statement_info.get('statement_date')
                    stmt_acct_num = statement_info.get('account_number')
                    stmt_sort_code = statement_info.get('sort_code')
                else:
                    stmt_date_str = getattr(statement_info, 'statement_date', None)
                    if stmt_date_str and hasattr(stmt_date_str, 'isoformat'):
                        stmt_date_str = stmt_date_str.isoformat()
                    stmt_acct_num = getattr(statement_info, 'account_number', None)
                    stmt_sort_code = getattr(statement_info, 'sort_code', None)

                import_record_id = email_storage.record_bank_statement_import(
                    bank_code=bank_code,
                    filename=filename,
                    transactions_imported=len(imported),
                    source='file',
                    target_system='opera3',
                    total_receipts=total_receipts,
                    total_payments=total_payments,
                    imported_by=imported_by,
                    opening_balance=stmt_opening,
                    closing_balance=stmt_closing,
                    statement_date=stmt_date_str,
                    account_number=stmt_acct_num,
                    sort_code=stmt_sort_code,
                    file_path=file_path
                )

                # Persist statement transactions for reconciliation lifecycle
                if stmt_transactions and import_record_id:
                    try:
                        raw_txns = [
                            {
                                "line_number": i,
                                "date": st.date.isoformat() if hasattr(st.date, 'isoformat') else str(st.date),
                                "description": st.description or '',
                                "amount": st.amount,
                                "balance": st.balance,
                                "transaction_type": st.transaction_type or '',
                                "reference": st.reference or ''
                            }
                            for i, st in enumerate(stmt_transactions, start=1)
                        ]
                        stmt_info_dict = {
                            'opening_balance': stmt_opening,
                            'closing_balance': stmt_closing,
                            'statement_date': stmt_date_str,
                            'account_number': stmt_acct_num,
                            'sort_code': stmt_sort_code,
                        }
                        email_storage.save_statement_transactions(
                            import_id=import_record_id,
                            transactions=raw_txns,
                            statement_info=stmt_info_dict
                        )
                        logger.info(f"Saved {len(raw_txns)} statement transactions for Opera 3 import_id={import_record_id}")

                        # Mark successfully imported transactions as posted (for partial recovery)
                        for imp_txn in imported:
                            entry_num = imp_txn.get('entry_number')
                            row_num = imp_txn.get('row')
                            if entry_num and row_num:
                                email_storage.mark_transaction_posted(import_record_id, row_num, str(entry_num))
                    except Exception as txn_err:
                        logger.warning(f"Could not save Opera 3 statement transactions: {txn_err}")
            except Exception as e:
                logger.warning(f"Could not record Opera 3 import history: {e}")

        # Learn patterns from successful imports (parity with SE version)
        if overrides:
            try:
                from sql_rag.bank_patterns import BankPatternLearner
                company_code = os.path.basename(data_path.rstrip('/\\')) or 'opera3_default'
                pattern_learner = BankPatternLearner(company_code=company_code)

                # Learn from overrides (user's explicit choices)
                for override in overrides:
                    if override.get('account') and override.get('ledger_type'):
                        # Find the transaction memo
                        txn = next((t for t in transactions if t.row_number == override.get('row')), None)
                        if txn:
                            pattern_learner.learn_pattern(
                                description=txn.memo or txn.name,
                                transaction_type=override.get('transaction_type', 'PI' if txn.amount < 0 else 'SI'),
                                account_code=override['account'],
                                account_name=override.get('account_name'),
                                ledger_type=override['ledger_type'],
                                vat_code=override.get('vat_code'),
                                nominal_code=override.get('nominal_code'),
                                net_amount=override.get('net_amount')
                            )
                logger.info(f"Learned patterns from {len(overrides)} overrides (Opera 3 PDF)")
            except Exception as e:
                logger.warning(f"Could not learn patterns (Opera 3 PDF): {e}")

        return {
            "success": len(imported) > 0,
            "source": "opera3",
            "data_path": data_path,
            "filename": filename,
            "imported_count": len(imported),
            "imported_transactions_count": len(imported),
            "receipts_imported": receipts_imported,
            "payments_imported": payments_imported,
            "transfers_imported": transfers_imported,
            "total_receipts": total_receipts,
            "total_payments": total_payments,
            "imported_transactions": imported,
            "errors": errors,
            "auto_allocate_enabled": auto_allocate,
            "auto_reconcile_enabled": auto_reconcile,
            "reconciliation_result": reconciliation_result
        }

    except Exception as e:
        logger.error(f"Opera 3 import-from-pdf error: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}





@router.post("/api/opera3/bank-import/preview")
async def opera3_preview_bank_import(
    filepath: str = Query(..., description="Path to CSV file"),
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """
    Preview what would be imported from a bank statement CSV using Opera 3 data.
    Matches transactions against Opera 3 customer/supplier master files.
    """
    # Backend validation - check for empty paths
    import os

    if not filepath or not filepath.strip():
        return {
            "success": False,
            "filename": "",
            "total_transactions": 0,
            "matched_receipts": [],
            "matched_payments": [],
            "repeat_entries": [],
            "already_posted": [],
            "skipped": [],
            "errors": ["CSV file path is required. Please enter the path to your bank statement CSV file."]
        }

    if not data_path or not data_path.strip():
        return {
            "success": False,
            "filename": filepath,
            "total_transactions": 0,
            "matched_receipts": [],
            "matched_payments": [],
            "repeat_entries": [],
            "already_posted": [],
            "skipped": [],
            "errors": ["Opera 3 data path is required. Please enter the path to your Opera 3 company data folder."]
        }

    if not os.path.exists(filepath):
        return {
            "success": False,
            "filename": filepath,
            "total_transactions": 0,
            "matched_receipts": [],
            "matched_payments": [],
            "repeat_entries": [],
            "already_posted": [],
            "skipped": [],
            "errors": [f"CSV file not found: {filepath}. Please check the file path."]
        }

    if not os.path.isdir(data_path):
        return {
            "success": False,
            "filename": filepath,
            "total_transactions": 0,
            "matched_receipts": [],
            "matched_payments": [],
            "repeat_entries": [],
            "already_posted": [],
            "skipped": [],
            "errors": [f"Opera 3 data path not found: {data_path}. Please check the folder path."]
        }

    try:
        from sql_rag.bank_import_opera3 import BankStatementMatcherOpera3

        matcher = BankStatementMatcherOpera3(data_path)
        result = matcher.preview_file(filepath)

        # Load pattern learner for suggestions on unmatched items
        try:
            from sql_rag.bank_patterns import BankPatternLearner
            # For Opera 3, use data_path folder name as company code
            company_code = os.path.basename(data_path.rstrip('/\\')) or 'opera3_default'
            pattern_learner = BankPatternLearner(company_code=company_code)
        except Exception as e:
            logger.warning(f"Could not initialize pattern learner for Opera 3 CSV: {e}")
            pattern_learner = None

        # Categorize transactions for frontend display
        matched_receipts = []
        matched_payments = []
        repeat_entries = []
        already_posted = []
        unmatched = []
        skipped = []

        for txn in result.transactions:
            txn_data = {
                "row": txn.row_number,
                "date": txn.date.isoformat(),
                "amount": txn.amount,
                "name": txn.name,
                "reference": txn.reference,
                "account": txn.matched_account,
                "match_score": txn.match_score if txn.match_score else 0,
                "reason": txn.skip_reason,
                # Repeat entry fields
                "repeat_entry_ref": getattr(txn, 'repeat_entry_ref', None),
                "repeat_entry_desc": getattr(txn, 'repeat_entry_desc', None),
                "repeat_entry_next_date": getattr(txn, 'repeat_entry_next_date', None).isoformat() if getattr(txn, 'repeat_entry_next_date', None) else None,
                "repeat_entry_posted": getattr(txn, 'repeat_entry_posted', None),
                "repeat_entry_total": getattr(txn, 'repeat_entry_total', None),
            }

            if txn.action == 'sales_receipt':
                matched_receipts.append(txn_data)
            elif txn.action == 'purchase_payment':
                matched_payments.append(txn_data)
            elif txn.action == 'repeat_entry':
                repeat_entries.append(txn_data)
            elif txn.skip_reason and 'Already' in txn.skip_reason:
                already_posted.append(txn_data)
            elif not txn.matched_account:
                # Unmatched - try to get suggestions from pattern learner
                if pattern_learner:
                    try:
                        suggestion = pattern_learner.find_pattern(txn.memo or txn.name)
                        if suggestion:
                            txn_data['suggested_type'] = suggestion.transaction_type
                            txn_data['suggested_account'] = suggestion.account_code
                            txn_data['suggested_account_name'] = suggestion.account_name
                            txn_data['suggested_ledger_type'] = suggestion.ledger_type
                            txn_data['suggested_vat_code'] = suggestion.vat_code
                            txn_data['suggested_nominal_code'] = suggestion.nominal_code
                            txn_data['suggestion_confidence'] = suggestion.confidence
                            txn_data['suggestion_source'] = suggestion.match_type
                    except Exception as e:
                        logger.warning(f"Opera 3 CSV pattern lookup failed: {e}")
                unmatched.append(txn_data)
            else:
                skipped.append(txn_data)

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "filename": result.filename,
            "total_transactions": result.total_transactions,
            "matched_receipts": matched_receipts,
            "matched_payments": matched_payments,
            "repeat_entries": repeat_entries,
            "already_posted": already_posted,
            "unmatched": unmatched,
            "skipped": skipped,
            "errors": result.errors,
            "summary": {
                "repeat_entry_count": len(repeat_entries)
            }
        }

    except FileNotFoundError as e:
        return {"success": False, "error": f"File not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 bank import preview error: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/opera3/bank-import/import-history")
async def opera3_get_bank_statement_import_history(
    bank_code: Optional[str] = Query(None, description="Filter by bank code"),
    limit: int = Query(50, description="Maximum records to return"),
    from_date: Optional[str] = Query(None, description="Filter from date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="Filter to date (YYYY-MM-DD)")
):
    """
    Get history of bank statement imports into Opera 3 (both file and email).
    """
    if not email_storage:
        return {"success": False, "error": "Email storage not configured"}

    try:
        history = email_storage.get_bank_statement_import_history(
            bank_code=bank_code,
            target_system='opera3',
            from_date=from_date,
            to_date=to_date,
            limit=limit
        )
        return {
            "success": True,
            "imports": history,
            "count": len(history)
        }
    except Exception as e:
        logger.error(f"Error getting Opera 3 import history: {e}")
        return {"success": False, "error": str(e)}





@router.delete("/api/opera3/bank-import/import-history/{record_id}")
async def opera3_delete_bank_statement_import_record(record_id: int):
    """
    Delete a single Opera 3 bank statement import history record to allow re-importing.

    This removes the tracking record so the statement can be imported again.
    Does not affect Opera data - only the import tracking.
    """
    if not email_storage:
        return {"success": False, "error": "Email storage not configured"}

    try:
        deleted = email_storage.delete_bank_statement_import_record(record_id)
        if deleted:
            return {
                "success": True,
                "message": "Import record deleted - statement can now be re-imported"
            }
        else:
            return {
                "success": False,
                "error": f"Record {record_id} not found"
            }
    except Exception as e:
        logger.error(f"Error deleting import record: {e}")
        return {"success": False, "error": str(e)}





@router.delete("/api/opera3/bank-import/import-history")
async def opera3_clear_bank_statement_import_history(
    bank_code: Optional[str] = Query(None, description="Filter by bank code"),
    from_date: Optional[str] = Query(None, description="Clear from date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="Clear to date (YYYY-MM-DD)")
):
    """
    Clear Opera 3 bank statement import history within optional filters.
    """
    if not email_storage:
        return {"success": False, "error": "Email storage not configured"}

    try:
        # Get records for opera3 only (need to filter first)
        history = email_storage.get_bank_statement_import_history(
            bank_code=bank_code,
            target_system='opera3',
            from_date=from_date,
            to_date=to_date,
            limit=1000
        )

        deleted_count = 0
        for record in history:
            if email_storage.delete_bank_statement_import_record(record['id']):
                deleted_count += 1

        return {
            "success": True,
            "deleted_count": deleted_count,
            "message": f"Cleared {deleted_count} Opera 3 import history records"
        }
    except Exception as e:
        logger.error(f"Error clearing Opera 3 import history: {e}")
        return {"success": False, "error": str(e)}





@router.post("/api/opera3/cashbook/auto-match-statement-lines")
async def opera3_auto_match_statement_lines(request: Request):
    """
    Auto-match bank statement lines to Opera 3 customers/suppliers.
    Mirrors the SQL SE /api/cashbook/auto-match-statement-lines endpoint.
    """
    try:
        body = await request.json()
        lines = body.get('lines', [])
        data_path = body.get('data_path', '')

        if not lines:
            return {"success": True, "lines": []}

        if not data_path:
            return {"success": False, "error": "data_path is required for Opera 3"}

        import re
        from difflib import SequenceMatcher

        provider = _get_opera3_provider(data_path)

        # Load customers
        customers_raw = provider.get_customers(active_only=True)
        customers = {c['account']: c['name'] for c in customers_raw if c.get('account')}

        # Load suppliers
        suppliers_raw = provider.get_suppliers(active_only=True)
        suppliers = {s['account']: s['name'] for s in suppliers_raw if s.get('account')}

        # Load outstanding sales invoices
        outstanding_sales = {}
        try:
            stran_records = provider._read_table_safe("stran")
            sname_lookup = {c['account']: c['name'] for c in customers_raw}
            for r in stran_records:
                bal = provider._get_num(r, 'ST_TRBAL')
                if bal > 0.01:
                    acct = provider._get_str(r, 'ST_ACCOUNT')
                    name = sname_lookup.get(acct, '')
                    ref = provider._get_str(r, 'ST_TRREF')
                    bal_rounded = round(bal, 2)
                    if bal_rounded not in outstanding_sales:
                        outstanding_sales[bal_rounded] = (acct, name, ref)
        except Exception:
            pass

        # Load outstanding purchase invoices
        outstanding_purchases = {}
        try:
            ptran_records = provider._read_table_safe("ptran")
            pname_lookup = {s['account']: s['name'] for s in suppliers_raw}
            for r in ptran_records:
                bal = provider._get_num(r, 'PT_TRBAL')
                if bal > 0.01:
                    acct = provider._get_str(r, 'PT_ACCOUNT')
                    name = pname_lookup.get(acct, '')
                    bal_rounded = round(bal, 2)
                    if bal_rounded not in outstanding_purchases:
                        outstanding_purchases[bal_rounded] = (acct, name)
        except Exception:
            pass

        # Load stran refs for invoice matching
        stran_by_ref = {}
        try:
            for r in stran_records:
                ref = provider._get_str(r, 'ST_TRREF').upper()
                if ref:
                    acct = provider._get_str(r, 'ST_ACCOUNT')
                    name = sname_lookup.get(acct, '')
                    if ref not in stran_by_ref:
                        stran_by_ref[ref] = (acct, name)
        except Exception:
            pass

        def extract_invoice_refs(text: str) -> list:
            if not text:
                return []
            refs = []
            patterns = [
                (r'INV\s*(\d+)', 'INV'),
                (r'Invoice\s*#?\s*(\d+)', 'INV'),
                (r'SI-?(\d+)', 'SI'),
                (r'(?:^|\s)(\d{5,6})(?:\s|$|,)', ''),
            ]
            for pattern, prefix in patterns:
                for match in re.finditer(pattern, text, re.IGNORECASE):
                    ref = f"{prefix}{match.group(1)}" if prefix else match.group(1)
                    if ref not in refs:
                        refs.append(ref)
            return refs

        customer_names_upper = {code: name.upper() for code, name in customers.items()}
        supplier_names_upper = {code: name.upper() for code, name in suppliers.items()}

        def fuzzy_match_name(text: str, names_dict: dict, names_upper: dict, threshold: float = 0.7) -> tuple:
            if not text:
                return None, None, None
            text_upper = text.upper()
            best_match = None
            best_score = 0
            for code, name_upper in names_upper.items():
                if name_upper in text_upper or text_upper in name_upper:
                    return code, names_dict[code], 'name_match'
                score = SequenceMatcher(None, text_upper, name_upper).ratio()
                if score > best_score and score >= threshold:
                    best_score = score
                    best_match = (code, names_dict[code])
            if best_match:
                return best_match[0], best_match[1], 'fuzzy_match'
            return None, None, None

        # Process each line
        matched_lines = []
        for line in lines:
            amount = float(line.get('statement_amount', 0))
            description = str(line.get('statement_description', '') or '')
            reference = str(line.get('statement_reference', '') or '')
            search_text = f"{description} {reference}"

            matched_account = None
            matched_name = None
            match_method = None
            account_type = None

            if amount > 0:
                account_type = 'customer'
                # 1. Try invoice reference
                refs = extract_invoice_refs(search_text)
                if refs:
                    for ref in refs:
                        ref_upper = ref.upper()
                        if ref_upper in stran_by_ref:
                            matched_account, matched_name = stran_by_ref[ref_upper]
                            match_method = 'invoice_ref'
                            break
                        suffix = ref_upper[-6:] if len(ref_upper) >= 6 else ref_upper
                        for stored_ref, (acct, name) in stran_by_ref.items():
                            if suffix in stored_ref or ref_upper in stored_ref:
                                matched_account, matched_name = acct, name
                                match_method = 'invoice_ref'
                                break
                        if matched_account:
                            break

                # 2. Try amount match
                if not matched_account:
                    key = round(amount, 2)
                    if key in outstanding_sales:
                        matched_account, matched_name = outstanding_sales[key][0], outstanding_sales[key][1]
                        match_method = 'amount_match'
                    else:
                        for bal, (acct, name, _ref) in outstanding_sales.items():
                            if abs(bal - key) < 0.02:
                                matched_account, matched_name = acct, name
                                match_method = 'amount_match'
                                break

                # 3. Try fuzzy name match
                if not matched_account:
                    matched_account, matched_name, match_method = fuzzy_match_name(search_text, customers, customer_names_upper)

            else:
                account_type = 'supplier'
                abs_amount = abs(amount)
                # 1. Try amount match
                key = round(abs_amount, 2)
                if key in outstanding_purchases:
                    matched_account, matched_name = outstanding_purchases[key][0], outstanding_purchases[key][1]
                    match_method = 'amount_match'
                else:
                    for bal, (acct, name) in outstanding_purchases.items():
                        if abs(bal - key) < 0.02:
                            matched_account, matched_name = acct, name
                            match_method = 'amount_match'
                            break

                # 2. Try fuzzy name match
                if not matched_account:
                    matched_account, matched_name, match_method = fuzzy_match_name(search_text, suppliers, supplier_names_upper)

            matched_line = dict(line)
            matched_line['matched_account'] = matched_account
            matched_line['matched_name'] = matched_name
            matched_line['match_method'] = match_method
            matched_line['suggested_type'] = account_type
            matched_lines.append(matched_line)

        matched_count = sum(1 for l in matched_lines if l.get('matched_account'))
        return {
            "success": True,
            "lines": matched_lines,
            "matched_count": matched_count,
            "total_count": len(matched_lines)
        }

    except Exception as e:
        logger.error(f"Error auto-matching Opera 3 statement lines: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}





@router.post("/api/opera3/cashbook/create-entry")
async def opera3_create_cashbook_entry(request: Request):
    """
    Create a new cashbook entry in Opera 3 for an unmatched statement line.
    Mirrors the SQL SE /api/cashbook/create-entry endpoint.
    """
    try:
        body = await request.json()

        data_path = body.get('data_path', '')
        bank_account = body.get('bank_account', '')
        transaction_date = body.get('transaction_date')
        amount = float(body.get('amount', 0))
        reference = body.get('reference', '')
        description = body.get('description', '')
        transaction_type = body.get('transaction_type', 'other_payment')
        account_code = body.get('account_code', '')
        account_type = body.get('account_type', 'nominal')

        if not data_path:
            return {"success": False, "error": "data_path is required for Opera 3"}

        if not transaction_date:
            return {"success": False, "error": "Transaction date is required"}

        if amount <= 0:
            return {"success": False, "error": "Amount must be positive"}

        from sql_rag.opera3_write_provider import get_opera3_writer, Opera3AgentRequired
        from datetime import date

        try:
            foxpro_import = get_opera3_writer(data_path)
        except Opera3AgentRequired as e:
            return {"success": False, "error": str(e)}

        # Parse date
        if isinstance(transaction_date, str):
            txn_date = date.fromisoformat(transaction_date[:10])
        else:
            txn_date = transaction_date

        if account_type == 'customer':
            result = foxpro_import.import_sales_receipt(
                bank_account=bank_account,
                customer_account=account_code,
                amount_pounds=amount,
                reference=reference,
                post_date=txn_date,
                input_by='RECONCILE',
                payment_method='Bank Import'
            )
            entry_type = 'Sales Receipt'
        elif account_type == 'supplier':
            result = foxpro_import.import_purchase_payment(
                bank_account=bank_account,
                supplier_account=account_code,
                amount_pounds=amount,
                reference=reference,
                post_date=txn_date,
                input_by='RECONCILE'
            )
            entry_type = 'Purchase Payment'
        else:
            is_receipt = transaction_type in ['other_receipt', 'nominal_receipt']
            result = foxpro_import.import_nominal_entry(
                bank_account=bank_account,
                nominal_account=account_code,
                amount_pounds=amount,
                reference=reference,
                post_date=txn_date,
                description=description,
                input_by='RECONCILE',
                is_receipt=is_receipt
            )
            entry_type = 'Nominal Receipt' if is_receipt else 'Nominal Payment'

        if result.success:
            entry_number = None
            if hasattr(result, 'entry_numbers') and result.entry_numbers:
                entry_number = result.entry_numbers[0]
            elif hasattr(result, 'entry_number'):
                entry_number = result.entry_number

            return {
                "success": True,
                "entry_number": entry_number,
                "message": f"Created {entry_type} for £{amount:.2f}"
            }
        else:
            error_msg = "; ".join(result.errors) if hasattr(result, 'errors') and result.errors else str(result.message if hasattr(result, 'message') else 'Unknown error')
            return {"success": False, "error": error_msg}

    except Exception as e:
        logger.error(f"Error creating Opera 3 cashbook entry: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}





@router.post("/api/opera3/reconcile/process-statement")
async def opera3_process_statement(
    file_path: str = Query(..., description="Path to the statement PDF"),
    bank_code: str = Query(..., description="Opera bank account code"),
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """
    Process a bank statement PDF and extract transactions for matching (Opera 3).

    Workflow:
    1. User selects bank account and Opera 3 data path
    2. User provides statement file path
    3. System validates statement matches selected bank account
    4. System extracts and matches transactions
    """
    try:
        from sql_rag.statement_reconcile_opera3 import StatementReconcilerOpera3
        from sql_rag.opera3_foxpro import Opera3Reader
        from pathlib import Path

        if not Path(file_path).exists():
            return {"success": False, "error": f"File not found: {file_path}"}

        if not Path(data_path).exists():
            return {"success": False, "error": f"Opera 3 data path not found: {data_path}"}

        # Create Opera 3 reader and reconciler
        reader = Opera3Reader(data_path)
        reconciler = StatementReconcilerOpera3(reader, config=config)

        # Check if there's a reconciliation in progress in Opera 3
        try:
            rec_in_progress = reconciler.check_reconciliation_in_progress(bank_code)
        except Exception as rec_err:
            logger.error(f"process-statement-Opera3: check_reconciliation_in_progress failed: {rec_err}")
            rec_in_progress = {'in_progress': False}
        if rec_in_progress['in_progress']:
            return {
                "success": False,
                "error": rec_in_progress['message'],
                "bank_code": bank_code,
                "reconciliation_in_progress": True,
                "partial_entries": rec_in_progress.get('partial_entries', 0)
            }

        # Extract transactions from statement
        statement_info, transactions = reconciler.extract_transactions_from_pdf(file_path)

        # Validate that statement matches the selected bank account
        bank_validation = reconciler.validate_statement_bank(bank_code, statement_info)
        if not bank_validation['valid']:
            return {
                "success": False,
                "error": bank_validation['error'],
                "bank_code": bank_code,
                "bank_validation": bank_validation,
                "statement_info": {
                    "bank_name": statement_info.bank_name,
                    "account_number": statement_info.account_number,
                    "sort_code": statement_info.sort_code
                }
            }

        # Check for statement period overlap (prevent double-posting)
        if email_storage and statement_info:
            period_start_str = statement_info.period_start.isoformat() if statement_info.period_start else None
            period_end_str = statement_info.period_end.isoformat() if statement_info.period_end else None
            if not period_start_str and transactions:
                dates = [t.date for t in transactions if t.date]
                if dates:
                    period_start_str = min(dates).isoformat() if hasattr(min(dates), 'isoformat') else str(min(dates))
                    period_end_str = max(dates).isoformat() if hasattr(max(dates), 'isoformat') else str(max(dates))
            if period_start_str and period_end_str:
                overlap = email_storage.check_period_overlap(
                    bank_code=bank_code,
                    period_start=period_start_str,
                    period_end=period_end_str
                )
                if overlap:
                    # Verify entries still exist in Opera (handles database restores)
                    overlap_id = overlap.get('import_id')
                    if overlap_id and sql_connector:
                        try:
                            recorded_entries = email_storage.get_import_entry_numbers(overlap_id)
                            if recorded_entries:
                                entry_list = "', '".join(recorded_entries[:5])
                                with sql_connector.engine.connect() as oconn:
                                    found = oconn.execute(text(f"""
                                        SELECT COUNT(*) FROM aentry WITH (NOLOCK)
                                        WHERE ae_acnt = '{bank_code}' AND ae_entry IN ('{entry_list}')
                                    """)).scalar() or 0
                                if found == 0:
                                    logger.info(f"Auto-removing stale import record {overlap_id} (database restored)")
                                    email_storage.delete_import_record(overlap_id)
                                    overlap = None
                            else:
                                email_storage.delete_import_record(overlap_id)
                                overlap = None
                        except Exception as e:
                            logger.warning(f"Could not verify overlap record {overlap_id}: {e}")
                if overlap:
                    return {
                        "success": False,
                        "overlap_warning": True,
                        "error": f"This statement period ({period_start_str} to {period_end_str}) overlaps with a previously imported statement: '{overlap['filename']}' ({overlap['period_start']} to {overlap['period_end']}). Clear the previous import first or choose a different statement.",
                        "overlap_details": overlap,
                        "bank_code": bank_code,
                        "statement_info": {
                            "bank_name": statement_info.bank_name,
                            "account_number": statement_info.account_number,
                            "opening_balance": statement_info.opening_balance,
                            "closing_balance": statement_info.closing_balance,
                        }
                    }

        # Validate statement sequence — correct opening balance if AI got it wrong
        # (parity with SE process-statement: don't block, just correct opening to rec_bal)
        rec_bal = None
        try:
            nbank_records = reader.read_table("nbank")
            for record in nbank_records:
                nb_acnt = str(record.get('NB_ACNT', record.get('nb_acnt', record.get('nk_acnt', '')))).strip().upper()
                if nb_acnt == bank_code.upper():
                    nk_recbal = float(record.get('NK_RECBAL', record.get('nk_recbal', 0)) or 0)
                    rec_bal = nk_recbal / 100.0
                    break
        except Exception as e:
            logger.warning(f"Could not get Opera 3 reconciled balance for sequence check: {e}")

        if rec_bal is not None and statement_info.opening_balance is not None:
            tolerance = 0.02
            if abs(statement_info.opening_balance - rec_bal) > tolerance:
                logger.warning(f"Opera 3 process-statement: Opening balance mismatch: extracted £{statement_info.opening_balance:,.2f}, "
                               f"Opera 3 reconciled £{rec_bal:.2f} — using reconciled balance")
                from sql_rag.statement_reconcile import StatementInfo
                statement_info = StatementInfo(
                    bank_name=statement_info.bank_name,
                    account_number=statement_info.account_number,
                    sort_code=statement_info.sort_code,
                    statement_date=statement_info.statement_date,
                    period_start=statement_info.period_start,
                    period_end=statement_info.period_end,
                    opening_balance=rec_bal,
                    closing_balance=statement_info.closing_balance
                )

        # Get unreconciled Opera entries for the date range (with 7 day buffer)
        from datetime import timedelta
        date_from = statement_info.period_start - timedelta(days=14) if statement_info.period_start else None
        date_to = statement_info.period_end + timedelta(days=14) if statement_info.period_end else None

        opera_entries = reconciler.get_unreconciled_entries(
            bank_code,
            date_from=date_from,
            date_to=date_to
        )

        # Match transactions - use 7 day tolerance to catch entries imported with different dates
        matches, unmatched_stmt, unmatched_opera = reconciler.match_transactions(
            transactions, opera_entries, date_tolerance_days=7
        )

        # Format response
        return {
            "success": True,
            "bank_code": bank_code,
            "bank_validation": bank_validation,
            "statement_info": {
                "bank_name": statement_info.bank_name,
                "account_number": statement_info.account_number,
                "sort_code": statement_info.sort_code,
                "statement_date": statement_info.statement_date.isoformat() if statement_info.statement_date else None,
                "period_start": statement_info.period_start.isoformat() if statement_info.period_start else None,
                "period_end": statement_info.period_end.isoformat() if statement_info.period_end else None,
                "opening_balance": statement_info.opening_balance,
                "closing_balance": statement_info.closing_balance
            },
            "extracted_transactions": len(transactions),
            "opera_unreconciled": len(opera_entries),
            "matches": [
                {
                    "statement_txn": {
                        "date": m.statement_txn.date.isoformat(),
                        "description": m.statement_txn.description,
                        "amount": m.statement_txn.amount,
                        "balance": m.statement_txn.balance,
                        "type": m.statement_txn.transaction_type
                    },
                    "opera_entry": {
                        "ae_entry": m.opera_entry['ae_entry'],
                        "ae_date": m.opera_entry['ae_date'].isoformat() if hasattr(m.opera_entry['ae_date'], 'isoformat') else str(m.opera_entry['ae_date']),
                        "ae_ref": m.opera_entry['ae_ref'],
                        "value_pounds": m.opera_entry['value_pounds'],
                        "ae_detail": m.opera_entry.get('ae_detail', '')
                    },
                    "match_score": m.match_score,
                    "match_reasons": m.match_reasons
                }
                for m in matches
            ],
            "unmatched_statement": [
                {
                    "date": t.date.isoformat(),
                    "description": t.description,
                    "amount": t.amount,
                    "balance": t.balance,
                    "type": t.transaction_type
                }
                for t in unmatched_stmt
                if not email_storage.is_transaction_ignored(bank_code, t.date.isoformat(), t.amount)
            ],
            "unmatched_opera": [
                {
                    "ae_entry": e['ae_entry'],
                    "ae_date": e['ae_date'].isoformat() if hasattr(e['ae_date'], 'isoformat') else str(e['ae_date']),
                    "ae_ref": e['ae_ref'],
                    "value_pounds": e['value_pounds'],
                    "ae_detail": e.get('ae_detail', '')
                }
                for e in unmatched_opera
            ]
        }

    except Exception as e:
        logger.error(f"Opera 3 statement processing failed: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}





@router.post("/api/opera3/reconcile/process-statement-unified")
async def opera3_process_statement_unified(
    file_path: str = Query(..., description="Path to the statement PDF"),
    bank_code: str = Query(..., description="Opera bank account code"),
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """
    Unified statement processing for Opera 3: identifies transactions to IMPORT and RECONCILE.
    """
    try:
        from sql_rag.statement_reconcile_opera3 import StatementReconcilerOpera3
        from sql_rag.opera3_foxpro import Opera3Reader
        from pathlib import Path

        if not Path(file_path).exists():
            return {"success": False, "error": f"File not found: {file_path}"}

        if not Path(data_path).exists():
            return {"success": False, "error": f"Opera 3 data path not found: {data_path}"}

        # Create Opera 3 reader and reconciler
        reader = Opera3Reader(data_path)
        reconciler = StatementReconcilerOpera3(reader, config=config)

        # Use the unified processing method
        result = reconciler.process_statement_unified(bank_code, file_path)

        # Handle error case
        if not result.get('success', False):
            return result

        stmt_info = result['statement_info']

        # Format the response
        def format_stmt_txn(txn):
            return {
                "date": txn.date.isoformat() if hasattr(txn.date, 'isoformat') else str(txn.date),
                "description": txn.description,
                "amount": txn.amount,
                "balance": txn.balance,
                "type": txn.transaction_type,
                "reference": txn.reference
            }

        def format_match(m):
            return {
                "statement_txn": format_stmt_txn(m['statement_txn']),
                "opera_entry": {
                    "ae_entry": m['opera_entry']['ae_entry'],
                    "ae_date": m['opera_entry']['ae_date'].isoformat() if hasattr(m['opera_entry']['ae_date'], 'isoformat') else str(m['opera_entry']['ae_date']),
                    "ae_ref": m['opera_entry']['ae_ref'],
                    "value_pounds": m['opera_entry']['value_pounds'],
                    "ae_detail": m['opera_entry'].get('ae_detail', ''),
                    "is_reconciled": m['opera_entry'].get('is_reconciled', False)
                },
                "match_score": m['match_score'],
                "match_reasons": m['match_reasons']
            }

        return {
            "success": True,
            "bank_code": result.get('bank_code'),
            "bank_validation": result.get('bank_validation'),
            "statement_info": {
                "bank_name": stmt_info.bank_name,
                "account_number": stmt_info.account_number,
                "sort_code": stmt_info.sort_code,
                "statement_date": stmt_info.statement_date.isoformat() if stmt_info.statement_date else None,
                "period_start": stmt_info.period_start.isoformat() if stmt_info.period_start else None,
                "period_end": stmt_info.period_end.isoformat() if stmt_info.period_end else None,
                "opening_balance": stmt_info.opening_balance,
                "closing_balance": stmt_info.closing_balance
            },
            "summary": result['summary'],
            "to_import": [format_stmt_txn(txn) for txn in result['to_import']],
            "to_reconcile": [format_match(m) for m in result['to_reconcile']],
            "already_reconciled": [format_match(m) for m in result['already_reconciled']],
            "balance_check": result['balance_check']
        }

    except Exception as e:
        logger.error(f"Opera 3 unified statement processing failed: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}





@router.get("/api/opera3/reconcile/banks")
async def opera3_get_reconcile_banks(
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """
    Get list of bank accounts for reconciliation from Opera 3.
    Mirrors /api/reconcile/banks.
    """
    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        from pathlib import Path

        if not Path(data_path).exists():
            return {"success": False, "error": f"Opera 3 data path not found: {data_path}"}

        reader = Opera3Reader(data_path)
        nbank_records = reader.read_table('nbank')

        banks = []
        for row in nbank_records:
            banks.append({
                "account_code": (row.get('nk_acnt') or '').strip(),
                "description": (row.get('nk_desc') or '').strip(),
                "sort_code": (row.get('nk_sort') or '').strip(),
                "account_number": (row.get('nk_number') or '').strip()
            })

        banks.sort(key=lambda x: x['account_code'])

        return {
            "success": True,
            "banks": banks
        }
    except Exception as e:
        logger.error(f"Opera 3 get reconcile banks failed: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/opera3/reconcile/bank/{bank_code}")
async def opera3_reconcile_bank(
    bank_code: str,
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """
    Reconcile a specific bank account from Opera 3 FoxPro data.
    Mirrors /api/reconcile/bank/{bank_code} — compares cashbook (aentry/atran)
    to nominal ledger (ntran) and bank master (nbank).
    """
    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        from pathlib import Path

        if not Path(data_path).exists():
            return {"success": False, "error": f"Opera 3 data path not found: {data_path}"}

        reader = Opera3Reader(data_path)

        # Read required tables
        nbank_records = reader.read_table('nbank')
        bank_info = None
        for row in nbank_records:
            if (row.get('nk_acnt') or '').strip().upper() == bank_code.upper():
                bank_info = row
                break

        if not bank_info:
            return {"success": False, "error": f"Bank account {bank_code} not found"}

        nbank_curbal_pence = float(bank_info.get('nk_curbal', 0) or 0)
        nbank_curbal_pounds = nbank_curbal_pence / 100.0
        nbank_recbal_pence = float(bank_info.get('nk_recbal', 0) or 0)

        # Read atran for cashbook movements
        atran_records = reader.read_table('atran')
        cb_net_pence = 0.0
        cb_receipts_pence = 0.0
        cb_payments_pence = 0.0
        cb_txn_count = 0
        for row in atran_records:
            if (row.get('at_acnt') or '').strip().upper() == bank_code.upper():
                val = float(row.get('at_value', 0) or 0)
                cb_net_pence += val
                if val > 0:
                    cb_receipts_pence += val
                else:
                    cb_payments_pence += abs(val)
                cb_txn_count += 1

        cb_net_pounds = cb_net_pence / 100.0

        # Read nacnt for nominal balance
        nacnt_records = reader.read_table('nacnt')
        nacnt_info = None
        for row in nacnt_records:
            if (row.get('na_acnt') or '').strip().upper() == bank_code.upper():
                nacnt_info = row
                break

        bf_balance = 0.0
        nl_ytd_net = 0.0
        if nacnt_info:
            pry_dr = float(nacnt_info.get('na_prydr', 0) or 0)
            pry_cr = float(nacnt_info.get('na_prycr', 0) or 0)
            bf_balance = pry_dr - pry_cr
            ytd_dr = float(nacnt_info.get('na_ytddr', 0) or 0)
            ytd_cr = float(nacnt_info.get('na_ytdcr', 0) or 0)
            nl_ytd_net = ytd_dr - ytd_cr

        nl_total = nl_ytd_net if nl_ytd_net != 0 else abs(nl_ytd_net)

        # Variance calculation
        variance_nbank_nl = nbank_curbal_pounds - nl_total
        variance_nbank_nl_abs = abs(variance_nbank_nl)
        all_reconciled = variance_nbank_nl_abs < 1.00

        reconciliation = {
            "success": True,
            "source": "opera3",
            "reconciliation_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "bank_code": bank_code,
            "bank_account": {
                "code": bank_code,
                "description": (bank_info.get('nk_desc') or '').strip(),
                "sort_code": (bank_info.get('nk_sort') or '').strip(),
                "account_number": (bank_info.get('nk_number') or '').strip()
            },
            "cashbook": {
                "source": "atran (Cashbook Transactions)",
                "transaction_count": cb_txn_count,
                "receipts": round(cb_receipts_pence / 100.0, 2),
                "payments": round(cb_payments_pence / 100.0, 2),
                "net": round(cb_net_pounds, 2)
            },
            "bank_master": {
                "source": "nbank.nk_curbal (Bank Master Balance)",
                "balance_pence": round(nbank_curbal_pence, 0),
                "balance_pounds": round(nbank_curbal_pounds, 2)
            },
            "nominal_ledger": {
                "source": "nacnt (Nominal Account Balances)",
                "account": bank_code,
                "description": (nacnt_info.get('na_desc') or '').strip() if nacnt_info else '',
                "brought_forward": round(bf_balance, 2),
                "ytd_net": round(nl_ytd_net, 2),
                "total_balance": round(nl_total, 2)
            },
            "variance": {
                "bank_master_vs_nominal": {
                    "description": "nbank.nk_curbal vs nacnt YTD",
                    "bank_master": round(nbank_curbal_pounds, 2),
                    "nominal_ledger": round(nl_total, 2),
                    "amount": round(variance_nbank_nl, 2),
                    "absolute": round(variance_nbank_nl_abs, 2),
                    "reconciled": variance_nbank_nl_abs < 1.00
                },
                "summary": {
                    "bank_master_balance": round(nbank_curbal_pounds, 2),
                    "nominal_ledger_balance": round(nl_total, 2),
                    "all_reconciled": all_reconciled
                }
            },
            "status": "RECONCILED" if all_reconciled else "UNRECONCILED",
            "message": f"Bank {bank_code} reconciles." if all_reconciled else f"Bank {bank_code} has variance: Bank Master vs NL = £{variance_nbank_nl_abs:,.2f}"
        }

        return reconciliation

    except Exception as e:
        logger.error(f"Opera 3 bank reconciliation failed for {bank_code}: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/opera3/reconcile/bank/{bank_code}/unreconciled")
async def opera3_get_unreconciled_entries(
    bank_code: str,
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    include_incomplete: bool = Query(False, description="Include incomplete entries")
):
    """
    Get unreconciled cashbook entries for an Opera 3 bank account.
    Mirrors /api/reconcile/bank/{bank_code}/unreconciled.
    """
    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        from pathlib import Path

        if not Path(data_path).exists():
            return {"success": False, "error": f"Opera 3 data path not found: {data_path}"}

        reader = Opera3Reader(data_path)
        aentry_records = reader.read_table('aentry')

        entries = []
        for row in aentry_records:
            if (row.get('ae_acnt') or '').strip().upper() != bank_code.upper():
                continue

            rec_num = int(row.get('ae_reclnum', 0) or 0)
            if rec_num > 0:
                continue  # Already reconciled

            complet = int(row.get('ae_complet', 0) or 0)
            if not include_incomplete and complet == 0:
                continue

            ae_value = float(row.get('ae_value', 0) or 0)
            ae_date = row.get('ae_date')
            if ae_date and hasattr(ae_date, 'isoformat'):
                ae_date = ae_date.isoformat()
            else:
                ae_date = str(ae_date) if ae_date else ''

            entries.append({
                "entry_number": (row.get('ae_entry') or '').strip(),
                "date": ae_date,
                "reference": (row.get('ae_ref') or '').strip(),
                "detail": (row.get('ae_detail') or '').strip(),
                "value_pence": round(ae_value, 0),
                "value_pounds": round(ae_value / 100.0, 2),
                "complete": complet == 1
            })

        return {
            "success": True,
            "bank_code": bank_code,
            "count": len(entries),
            "entries": entries
        }
    except Exception as e:
        logger.error(f"Opera 3 get unreconciled entries failed for {bank_code}: {e}")
        return {"success": False, "error": str(e)}





@router.post("/api/opera3/reconcile/bank/{bank_code}/mark-reconciled")
async def opera3_mark_entries_reconciled(
    bank_code: str,
    request: ReconcileEntriesRequest,
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """
    Mark cashbook entries as reconciled in Opera 3 FoxPro.
    Mirrors /api/reconcile/bank/{bank_code}/mark-reconciled.
    """
    from sql_rag.import_lock import acquire_import_lock, release_import_lock
    if not acquire_import_lock(_bank_lock_key(bank_code), locked_by="api", endpoint="opera3-mark-reconciled"):
        return {"success": False, "error": f"Bank account {bank_code} is currently being modified by another user. Please wait and try again."}

    try:
        from sql_rag.opera3_write_provider import get_opera3_writer, Opera3AgentRequired
        from datetime import datetime as dt

        try:
            foxpro_import = get_opera3_writer(data_path)
        except Opera3AgentRequired as e:
            release_import_lock(_bank_lock_key(bank_code))
            return {"success": False, "error": str(e)}

        stmt_date = None
        rec_date = None
        if request.statement_date:
            stmt_date = dt.strptime(request.statement_date, '%Y-%m-%d').date()
        if request.reconciliation_date:
            rec_date = dt.strptime(request.reconciliation_date, '%Y-%m-%d').date()

        result = foxpro_import.mark_entries_reconciled(
            bank_account=bank_code,
            entries=request.entries,
            statement_number=request.statement_number,
            statement_date=stmt_date,
            reconciliation_date=rec_date
        )

        release_import_lock(_bank_lock_key(bank_code))
        if result.success:
            return {
                "success": True,
                "message": f"Reconciled {result.records_imported} entries",
                "records_reconciled": result.records_imported,
                "details": result.warnings
            }
        else:
            return {
                "success": False,
                "errors": result.errors
            }
    except Exception as e:
        logger.error(f"Opera 3 mark entries reconciled failed for {bank_code}: {e}")
        try:
            release_import_lock(_bank_lock_key(bank_code))
        except Exception:
            pass
        return {"success": False, "error": str(e)}





@router.post("/api/opera3/reconcile/bank/{bank_code}/unreconcile")
async def opera3_unreconcile_entries(
    bank_code: str,
    entry_numbers: List[str],
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """
    Unreconcile previously reconciled entries in Opera 3 FoxPro.
    Mirrors /api/reconcile/bank/{bank_code}/unreconcile.
    """
    from sql_rag.import_lock import acquire_import_lock, release_import_lock
    if not acquire_import_lock(_bank_lock_key(bank_code), locked_by="api", endpoint="opera3-unreconcile"):
        return {"success": False, "error": f"Bank account {bank_code} is currently being modified by another user. Please wait and try again."}

    try:
        from sql_rag.opera3_write_provider import get_opera3_writer, Opera3AgentRequired
        from pathlib import Path

        if not Path(data_path).exists():
            release_import_lock(_bank_lock_key(bank_code))
            return {"success": False, "error": f"Opera 3 data path not found: {data_path}"}

        try:
            foxpro_import = get_opera3_writer(data_path)
        except Opera3AgentRequired as e:
            release_import_lock(_bank_lock_key(bank_code))
            return {"success": False, "error": str(e)}

        from sql_rag.opera3_foxpro import Opera3Reader
        reader = Opera3Reader(data_path)

        # Read aentry and reset reconciliation fields for matching entries
        import dbf
        aentry_path = Path(data_path) / 'aentry.dbf'
        if not aentry_path.exists():
            aentry_path = Path(data_path) / 'AENTRY.DBF'

        rows_affected = 0
        entry_set = set(e.strip().upper() for e in entry_numbers)

        table = dbf.Table(str(aentry_path))
        table.open(dbf.READ_WRITE)
        try:
            for record in table:
                ae_acnt = (str(record.ae_acnt) if hasattr(record, 'ae_acnt') else '').strip().upper()
                ae_entry = (str(record.ae_entry) if hasattr(record, 'ae_entry') else '').strip().upper()

                if ae_acnt == bank_code.upper() and ae_entry in entry_set:
                    rec_num = int(record.ae_reclnum if hasattr(record, 'ae_reclnum') else 0) or 0
                    if rec_num > 0:
                        with record as r:
                            r.ae_reclnum = 0
                            r.ae_statln = 0
                            r.ae_frstat = 0
                            r.ae_tostat = 0
                            r.ae_tmpstat = 0
                        rows_affected += 1
        finally:
            table.close()

        # Recalculate reconciled balance and update nbank
        aentry_records = reader.read_table('aentry')
        new_rec_total_pence = 0.0
        for row in aentry_records:
            if (row.get('ae_acnt') or '').strip().upper() == bank_code.upper():
                if int(row.get('ae_reclnum', 0) or 0) > 0:
                    new_rec_total_pence += float(row.get('ae_value', 0) or 0)

        nbank_path = Path(data_path) / 'nbank.dbf'
        if not nbank_path.exists():
            nbank_path = Path(data_path) / 'NBANK.DBF'

        nbank_table = dbf.Table(str(nbank_path))
        nbank_table.open(dbf.READ_WRITE)
        try:
            for record in nbank_table:
                nk_acnt = (str(record.nk_acnt) if hasattr(record, 'nk_acnt') else '').strip().upper()
                if nk_acnt == bank_code.upper():
                    with record as r:
                        r.nk_recbal = int(new_rec_total_pence)
                    break
        finally:
            nbank_table.close()

        release_import_lock(_bank_lock_key(bank_code))
        return {
            "success": True,
            "message": f"Unreconciled {rows_affected} entries",
            "entries_unreconciled": rows_affected,
            "new_reconciled_balance": new_rec_total_pence / 100.0
        }

    except Exception as e:
        logger.error(f"Opera 3 unreconcile entries failed for {bank_code}: {e}")
        try:
            release_import_lock(_bank_lock_key(bank_code))
        except Exception:
            pass
        return {"success": False, "error": str(e)}





@router.post("/api/opera3/reconcile/bank/{bank_code}/ignore-transaction")
async def opera3_ignore_bank_transaction(
    bank_code: str,
    transaction_date: str = Query(..., description="Transaction date (YYYY-MM-DD)"),
    amount: float = Query(..., description="Transaction amount in pounds"),
    description: str = Query(None, description="Transaction description"),
    reference: str = Query(None, description="Transaction reference"),
    reason: str = Query(None, description="Reason for ignoring")
):
    """
    Mark a bank transaction as ignored for Opera 3 reconciliation.
    Mirrors /api/reconcile/bank/{bank_code}/ignore-transaction.
    Uses same shared email_storage (SQLite) since ignored transactions are not Opera-version-specific.
    """
    try:
        record_id = email_storage.ignore_bank_transaction(
            bank_account=bank_code,
            transaction_date=transaction_date,
            amount=amount,
            description=description,
            reference=reference,
            reason=reason,
            ignored_by="API-Opera3"
        )
        return {
            "success": True,
            "message": f"Transaction ignored: £{amount:.2f} on {transaction_date}",
            "record_id": record_id
        }
    except Exception as e:
        logger.error(f"Opera 3 failed to ignore transaction: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/opera3/reconcile/bank/{bank_code}/ignored-transactions")
async def opera3_get_ignored_transactions(
    bank_code: str,
    limit: int = Query(100, description="Maximum records to return")
):
    """
    Get list of ignored transactions for an Opera 3 bank account.
    Mirrors /api/reconcile/bank/{bank_code}/ignored-transactions.
    Uses shared email_storage.
    """
    try:
        transactions = email_storage.get_ignored_transactions(
            bank_account=bank_code,
            limit=limit
        )
        return {
            "success": True,
            "transactions": transactions,
            "count": len(transactions)
        }
    except Exception as e:
        logger.error(f"Opera 3 failed to get ignored transactions: {e}")
        return {"success": False, "error": str(e)}





@router.delete("/api/opera3/reconcile/bank/ignored-transaction/{record_id}")
async def opera3_unignore_transaction(record_id: int):
    """
    Remove a transaction from the ignored list (Opera 3).
    Mirrors /api/reconcile/bank/ignored-transaction/{record_id}.
    Uses shared email_storage.
    """
    try:
        deleted = email_storage.unignore_transaction(record_id)
        if deleted:
            return {"success": True, "message": "Transaction removed from ignored list"}
        else:
            return {"success": False, "error": "Record not found"}
    except Exception as e:
        logger.error(f"Opera 3 failed to unignore transaction: {e}")
        return {"success": False, "error": str(e)}





@router.delete("/api/opera3/reconcile/bank/{bank_code}/unignore-transaction")
async def opera3_unignore_transaction_by_match(
    bank_code: str,
    transaction_date: str = Query(...),
    amount: float = Query(...)
):
    """
    Remove a transaction from the ignored list by matching bank, date, and amount (Opera 3).
    Mirrors /api/reconcile/bank/{bank_code}/unignore-transaction.
    Uses shared email_storage.
    """
    try:
        deleted = email_storage.unignore_transaction_by_match(bank_code, transaction_date, amount)
        if deleted:
            return {"success": True, "message": "Transaction removed from ignored list"}
        else:
            return {"success": False, "error": "No matching ignored transaction found"}
    except Exception as e:
        logger.error(f"Opera 3 failed to unignore transaction: {e}")
        return {"success": False, "error": str(e)}





@router.post("/api/opera3/reconcile/bank/{bank_code}/import-from-statement")
async def opera3_import_from_statement(
    bank_code: str,
    transactions: List[Dict],
    statement_date: str,
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """
    Import transactions from a bank statement using Opera 3 matching logic.
    Mirrors /api/reconcile/bank/{bank_code}/import-from-statement.
    """
    try:
        from sql_rag.bank_import_opera3 import BankStatementMatcherOpera3, BankTransaction
        from pathlib import Path

        if not Path(data_path).exists():
            return {"success": False, "error": f"Opera 3 data path not found: {data_path}"}

        # Create bank transactions from PDF-extracted data
        bank_txns = []
        for i, txn in enumerate(transactions):
            amount = float(txn['amount'])
            txn_date = datetime.strptime(txn['date'][:10], '%Y-%m-%d')
            description = txn.get('description', '')[:100]

            bank_txn = BankTransaction(
                row_number=i + 1,
                date=txn_date.date(),
                amount=amount,
                subcategory=txn.get('type', 'Other'),
                memo=description,
                name=description,
                reference=txn.get('reference') or description[:30]
            )
            bank_txns.append(bank_txn)

        matcher = BankStatementMatcherOpera3(data_path=data_path)

        matched_receipts = []
        matched_payments = []
        unmatched = []

        for txn in bank_txns:
            matcher._match_transaction(txn, bank_code=bank_code)

            txn_data = {
                "row": txn.row_number,
                "date": str(txn.date),
                "name": txn.name,
                "reference": txn.reference,
                "amount": txn.amount,
                "action": txn.action,
                "match_type": txn.match_type,
                "matched_account": txn.matched_account,
                "matched_name": txn.matched_name,
                "match_score": txn.match_score,
                "skip_reason": txn.skip_reason
            }

            if txn.action == 'sales_receipt':
                matched_receipts.append(txn_data)
            elif txn.action == 'purchase_payment':
                matched_payments.append(txn_data)
            else:
                unmatched.append(txn_data)

        return {
            "success": True,
            "total_transactions": len(bank_txns),
            "matched_receipts": matched_receipts,
            "matched_payments": matched_payments,
            "unmatched": unmatched,
            "summary": {
                "receipts": len(matched_receipts),
                "payments": len(matched_payments),
                "unmatched": len(unmatched)
            }
        }

    except Exception as e:
        logger.error(f"Opera 3 import from statement failed for {bank_code}: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}





@router.post("/api/opera3/reconcile/bank/{bank_code}/confirm-matches")
async def opera3_confirm_statement_matches(
    bank_code: str,
    matches: List[Dict],
    statement_balance: float,
    statement_date: str,
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """
    Confirm matched transactions and mark them as reconciled in Opera 3.
    Mirrors /api/reconcile/bank/{bank_code}/confirm-matches.
    """
    from sql_rag.import_lock import acquire_import_lock, release_import_lock
    if not acquire_import_lock(_bank_lock_key(bank_code), locked_by="api", endpoint="opera3-confirm-matches"):
        return {"success": False, "error": f"Bank account {bank_code} is currently being modified by another user. Please wait and try again."}

    try:
        from sql_rag.opera3_write_provider import get_opera3_writer, Opera3AgentRequired

        try:
            foxpro_import = get_opera3_writer(data_path)
        except Opera3AgentRequired as e:
            release_import_lock(_bank_lock_key(bank_code))
            return {"success": False, "error": str(e)}

        stmt_date = datetime.strptime(statement_date, '%Y-%m-%d')

        # Get the entry IDs to reconcile
        entry_ids = [m.get('ae_entry') or m.get('opera_entry', {}).get('ae_entry') for m in matches]
        entry_ids = [e for e in entry_ids if e]

        if not entry_ids:
            release_import_lock(_bank_lock_key(bank_code))
            return {"success": False, "error": "No valid entry IDs provided"}

        # Build entries list with statement line numbers
        entries = []
        for i, entry_id in enumerate(entry_ids):
            entries.append({
                "entry_number": entry_id,
                "statement_line": (i + 1) * 10
            })

        # Get next statement number from nbank
        from sql_rag.opera3_foxpro import Opera3Reader
        reader = Opera3Reader(data_path)
        nbank_records = reader.read_table('nbank')
        next_batch = 1
        for row in nbank_records:
            if (row.get('nk_acnt') or '').strip().upper() == bank_code.upper():
                next_batch = int(row.get('nk_lstrecl', 0) or 0) + 1
                break

        result = foxpro_import.mark_entries_reconciled(
            bank_account=bank_code,
            entries=entries,
            statement_number=next_batch,
            statement_date=stmt_date.date(),
            reconciliation_date=stmt_date.date()
        )

        release_import_lock(_bank_lock_key(bank_code))
        if result.success:
            return {
                "success": True,
                "message": f"Reconciled {result.records_imported} entries",
                "reconciled_count": result.records_imported,
                "batch_number": next_batch,
                "statement_balance": statement_balance
            }
        else:
            return {
                "success": False,
                "errors": result.errors
            }

    except Exception as e:
        logger.error(f"Opera 3 confirm matches failed for {bank_code}: {e}")
        try:
            release_import_lock(_bank_lock_key(bank_code))
        except Exception:
            pass
        return {"success": False, "error": str(e)}





@router.get("/api/opera3/reconcile/bank/{bank_code}/scan-emails")
async def opera3_scan_emails_for_statements(bank_code: str, email_address: Optional[str] = None):
    """
    Scan email inbox for bank statement attachments (Opera 3).
    Mirrors /api/reconcile/bank/{bank_code}/scan-emails.
    """
    return {
        "success": True,
        "message": "Email scanning not yet implemented - use file upload",
        "statements_found": []
    }





@router.post("/api/opera3/reconcile/bank/{bank_code}/complete-batch/{entry_number}")
async def opera3_complete_batch(
    bank_code: str,
    entry_number: str,
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """
    Complete an incomplete cashbook batch in Opera 3, making it available for reconciliation.
    Mirrors /api/reconcile/bank/{bank_code}/complete-batch/{entry_number}.
    In Opera 3, batches are typically already complete (ae_complet=1) on write.
    """
    from sql_rag.import_lock import acquire_import_lock, release_import_lock
    if not acquire_import_lock(_bank_lock_key(bank_code), locked_by="api", endpoint="opera3-complete-batch"):
        return {"success": False, "error": f"Bank account {bank_code} is currently being modified by another user. Please wait and try again."}

    try:
        from pathlib import Path

        if not Path(data_path).exists():
            release_import_lock(_bank_lock_key(bank_code))
            return {"success": False, "error": f"Opera 3 data path not found: {data_path}"}

        # In Opera 3, ae_complet is set to 1 on creation, so this is a no-op in most cases.
        # But we set it just in case.
        import dbf
        aentry_path = Path(data_path) / 'aentry.dbf'
        if not aentry_path.exists():
            aentry_path = Path(data_path) / 'AENTRY.DBF'

        table = dbf.Table(str(aentry_path))
        table.open(dbf.READ_WRITE)
        found = False
        try:
            for record in table:
                ae_acnt = (str(record.ae_acnt) if hasattr(record, 'ae_acnt') else '').strip().upper()
                ae_entry_val = (str(record.ae_entry) if hasattr(record, 'ae_entry') else '').strip().upper()
                if ae_acnt == bank_code.upper() and ae_entry_val == entry_number.upper():
                    found = True
                    complet = int(record.ae_complet if hasattr(record, 'ae_complet') else 0) or 0
                    if complet == 0:
                        with record as r:
                            r.ae_complet = 1
                    break
        finally:
            table.close()

        release_import_lock(_bank_lock_key(bank_code))
        if found:
            return {
                "success": True,
                "entry_number": entry_number,
                "message": f"Batch {entry_number} completed"
            }
        else:
            return {
                "success": False,
                "error": f"Entry {entry_number} not found in bank {bank_code}"
            }

    except Exception as e:
        logger.error(f"Opera 3 complete batch failed: {e}")
        try:
            release_import_lock(_bank_lock_key(bank_code))
        except Exception:
            pass
        return {"success": False, "error": str(e)}





@router.post("/api/opera3/bank-reconciliation/validate-statement")
async def opera3_validate_statement_for_reconciliation(
    bank_code: str = Query(..., description="Bank account code"),
    opening_balance: float = Query(..., description="Statement opening balance (pounds)"),
    closing_balance: float = Query(..., description="Statement closing balance (pounds)"),
    statement_date: str = Query(..., description="Statement date (YYYY-MM-DD)"),
    statement_number: Optional[int] = Query(None, description="Statement number"),
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """
    Validate a statement is ready for reconciliation in Opera 3.
    Mirrors /api/bank-reconciliation/validate-statement.
    """
    try:
        from sql_rag.opera3_data_provider import Opera3DataProvider
        from pathlib import Path

        if not Path(data_path).exists():
            return {"valid": False, "error_message": f"Opera 3 data path not found: {data_path}"}

        provider = Opera3DataProvider(data_path)
        seq_result = provider.validate_statement_sequence(bank_code, opening_balance)

        if seq_result.get('status') == 'error':
            return {"valid": False, "error_message": seq_result.get('error', 'Unknown error')}

        rec_bal = seq_result.get('reconciled_balance', 0)
        tolerance = 0.02
        balance_ok = abs(opening_balance - rec_bal) <= tolerance

        return {
            "valid": balance_ok,
            "reconciled_balance": rec_bal,
            "opening_balance": opening_balance,
            "closing_balance": closing_balance,
            "balance_difference": round(opening_balance - rec_bal, 2),
            "error_message": None if balance_ok else f"Opening balance £{opening_balance:,.2f} does not match Opera reconciled balance £{rec_bal:,.2f}",
            "source": "opera3"
        }

    except Exception as e:
        logger.error(f"Opera 3 validate statement failed: {e}")
        return {"valid": False, "error_message": str(e)}





@router.post("/api/opera3/bank-reconciliation/match-statement")
async def opera3_match_statement_to_cashbook(
    bank_code: str = Query(..., description="Bank account code"),
    date_tolerance_days: int = Query(3, description="Days tolerance for date matching"),
    import_id: Optional[int] = Query(None, description="Import record ID"),
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    request_body: Dict[str, Any] = Body(None)
):
    """
    Match statement lines to unreconciled Opera 3 cashbook entries.
    Mirrors /api/bank-reconciliation/match-statement.
    """
    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        from pathlib import Path

        if not Path(data_path).exists():
            return {"success": False, "error": f"Opera 3 data path not found: {data_path}"}

        # Load transactions from DB if import_id provided
        statement_transactions = None
        if import_id and email_storage:
            db_txns = email_storage.get_statement_transactions(import_id)
            if db_txns:
                statement_transactions = [
                    {
                        "line_number": t['line_number'],
                        "date": t['date'],
                        "amount": t['amount'],
                        "reference": t.get('reference', ''),
                        "description": t.get('description', ''),
                        "balance": t.get('balance'),
                        "transaction_type": t.get('transaction_type', '')
                    }
                    for t in db_txns
                ]

        if not statement_transactions:
            if not request_body or 'statement_transactions' not in request_body:
                return {"success": False, "error": "Request body must include statement_transactions (or provide import_id)"}
            statement_transactions = request_body['statement_transactions']

        # Read Opera 3 unreconciled entries
        reader = Opera3Reader(data_path)
        aentry_records = reader.read_table('aentry')

        unreconciled = []
        for row in aentry_records:
            if (row.get('ae_acnt') or '').strip().upper() != bank_code.upper():
                continue
            if int(row.get('ae_reclnum', 0) or 0) > 0:
                continue
            if int(row.get('ae_complet', 0) or 0) == 0:
                continue

            ae_value = float(row.get('ae_value', 0) or 0)
            ae_date = row.get('ae_date')

            unreconciled.append({
                "entry_number": (row.get('ae_entry') or '').strip(),
                "date": ae_date.isoformat() if hasattr(ae_date, 'isoformat') else str(ae_date or ''),
                "value_pence": ae_value,
                "value_pounds": ae_value / 100.0,
                "reference": (row.get('ae_ref') or '').strip(),
                "detail": (row.get('ae_detail') or '').strip(),
                "comment": (row.get('ae_comment') or '').strip()
            })

        # Simple matching: match by amount and date proximity
        auto_matched = []
        suggested_matched = []
        unmatched_statement = []
        unmatched_cashbook = list(unreconciled)

        for txn in statement_transactions:
            stmt_amount = float(txn.get('amount', 0))
            stmt_amount_pence = round(stmt_amount * 100)
            stmt_date_str = txn.get('date', '')

            best_match = None
            best_score = 0

            for entry in unmatched_cashbook:
                entry_pence = round(entry['value_pence'])

                # Amount must match exactly
                if entry_pence != stmt_amount_pence:
                    continue

                # Date proximity scoring
                try:
                    from datetime import date as date_type
                    if isinstance(stmt_date_str, str) and stmt_date_str:
                        stmt_dt = date_type.fromisoformat(stmt_date_str[:10])
                    else:
                        stmt_dt = None
                    entry_dt_str = entry['date']
                    if isinstance(entry_dt_str, str) and entry_dt_str:
                        entry_dt = date_type.fromisoformat(entry_dt_str[:10])
                    else:
                        entry_dt = None
                    if stmt_dt and entry_dt:
                        day_diff = abs((stmt_dt - entry_dt).days)
                        if day_diff <= date_tolerance_days:
                            score = 1.0 - (day_diff / (date_tolerance_days + 1))
                        elif day_diff <= 14:
                            score = 0.5
                        else:
                            score = 0.3
                    else:
                        score = 0.6
                except Exception:
                    score = 0.5

                if score > best_score:
                    best_score = score
                    best_match = entry

            if best_match:
                match_entry = {
                    "statement_line": txn.get('line_number'),
                    "statement_date": txn.get('date'),
                    "statement_amount": stmt_amount,
                    "statement_reference": txn.get('reference', ''),
                    "statement_description": txn.get('description', ''),
                    "entry_number": best_match['entry_number'],
                    "entry_date": best_match['date'],
                    "entry_amount": best_match['value_pounds'],
                    "entry_reference": best_match['reference'],
                    "confidence": best_score
                }

                if best_score >= 0.8:
                    auto_matched.append(match_entry)
                else:
                    suggested_matched.append(match_entry)

                unmatched_cashbook.remove(best_match)
            else:
                unmatched_statement.append({
                    "statement_line": txn.get('line_number'),
                    "statement_date": txn.get('date'),
                    "statement_amount": stmt_amount,
                    "statement_reference": txn.get('reference', ''),
                    "statement_description": txn.get('description', '')
                })

        # Filter ignored from unmatched
        if email_storage:
            filtered_unmatched = []
            for txn in unmatched_statement:
                if not email_storage.is_transaction_ignored(bank_code, txn.get('statement_date', ''), txn.get('statement_amount', 0)):
                    filtered_unmatched.append(txn)
            unmatched_statement = filtered_unmatched

        result = {
            "success": True,
            "source": "opera3",
            "auto_matched": auto_matched,
            "suggested_matched": suggested_matched,
            "unmatched_statement": unmatched_statement,
            "unmatched_cashbook": [
                {
                    "entry_number": e['entry_number'],
                    "entry_date": e['date'],
                    "entry_amount": e['value_pounds'],
                    "entry_reference": e['reference']
                }
                for e in unmatched_cashbook
            ],
            "summary": {
                "auto_matched": len(auto_matched),
                "suggested_matched": len(suggested_matched),
                "unmatched_statement": len(unmatched_statement),
                "unmatched_cashbook": len(unmatched_cashbook)
            }
        }

        if import_id:
            result['import_id'] = import_id

        return result

    except Exception as e:
        logger.error(f"Opera 3 match statement failed: {e}")
        return {"success": False, "error": str(e)}





@router.post("/api/opera3/bank-reconciliation/complete")
async def opera3_complete_reconciliation(
    bank_code: str = Query(..., description="Bank account code"),
    statement_number: int = Query(..., description="Statement number"),
    statement_date: str = Query(..., description="Statement date (YYYY-MM-DD)"),
    closing_balance: float = Query(..., description="Statement closing balance (pounds)"),
    partial: bool = Query(False, description="Partial reconciliation"),
    import_id: Optional[int] = Query(None, description="Import record ID"),
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    request_body: Dict[str, Any] = Body(None)
):
    """
    Complete bank reconciliation for Opera 3 - mark matched entries as reconciled.
    Mirrors /api/bank-reconciliation/complete.
    """
    if not request_body:
        return {"success": False, "error": "Request body required"}

    matched_entries = request_body.get('matched_entries', [])
    if not matched_entries:
        return {"success": False, "error": "No matched entries provided"}

    from sql_rag.import_lock import acquire_import_lock, release_import_lock
    if not acquire_import_lock(_bank_lock_key(bank_code), locked_by="api", endpoint="opera3-bank-reconciliation-complete"):
        return {"success": False, "error": f"Bank account {bank_code} is currently being modified by another user. Please wait and try again."}

    try:
        from sql_rag.opera3_write_provider import get_opera3_writer, Opera3AgentRequired

        try:
            foxpro_import = get_opera3_writer(data_path)
        except Opera3AgentRequired as e:
            release_import_lock(_bank_lock_key(bank_code))
            return {"success": False, "error": str(e)}

        stmt_date = datetime.strptime(statement_date, '%Y-%m-%d').date()

        # Build entries list
        entries = []
        for entry in matched_entries:
            entries.append({
                "entry_number": entry.get('entry_number', ''),
                "statement_line": entry.get('statement_line', 0)
            })

        result = foxpro_import.mark_entries_reconciled(
            bank_account=bank_code,
            entries=entries,
            statement_number=statement_number,
            statement_date=stmt_date,
            reconciliation_date=stmt_date,
            partial=partial
        )

        # Update DB tracking if import_id provided
        if result.success and import_id and email_storage:
            try:
                matches_to_save = []
                for entry in matched_entries:
                    matches_to_save.append({
                        'line_number': entry.get('statement_line'),
                        'matched_entry': entry.get('entry_number'),
                        'match_confidence': 1.0,
                        'match_type': 'manual'
                    })
                if matches_to_save:
                    email_storage.update_transaction_matches_bulk(import_id, matches_to_save)

                email_storage.mark_transactions_reconciled(import_id)

                if not partial:
                    email_storage.mark_statement_reconciled(
                        filename='',
                        reconciled_count=result.records_imported,
                        bank_code=bank_code
                    )
                    with email_storage._get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute("""
                            UPDATE bank_statement_imports
                            SET is_reconciled = 1,
                                reconciled_date = ?,
                                reconciled_count = ?
                            WHERE id = ?
                        """, (datetime.now().isoformat(), result.records_imported, import_id))
            except Exception as db_err:
                logger.warning(f"Could not update reconciliation status in DB: {db_err}")

        release_import_lock(_bank_lock_key(bank_code))
        return {
            "success": result.success,
            "entries_reconciled": result.records_imported if result.success else 0,
            "messages": result.warnings if result.success else result.errors,
            "partial": partial,
            "statement_number": statement_number,
            "statement_date": statement_date,
            "closing_balance": closing_balance
        }

    except Exception as e:
        logger.error(f"Opera 3 complete reconciliation failed: {e}")
        try:
            release_import_lock(_bank_lock_key(bank_code))
        except Exception:
            pass
        return {"success": False, "error": str(e)}





@router.get("/api/opera3/bank-reconciliation/statement-transactions/{import_id}")
async def opera3_get_statement_transactions(import_id: int):
    """
    Retrieve stored statement transactions for an Opera 3 import.
    Mirrors /api/bank-reconciliation/statement-transactions/{import_id}.
    Uses shared email_storage (same SQLite DB).
    """
    try:
        if not email_storage:
            return {"success": False, "error": "Email storage not initialized"}

        transactions = email_storage.get_statement_transactions(import_id)

        with email_storage._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, bank_code, filename, opening_balance, closing_balance,
                       statement_date, account_number, sort_code, source,
                       transactions_imported, total_receipts, total_payments,
                       COALESCE(is_reconciled, 0) as is_reconciled
                FROM bank_statement_imports WHERE id = ?
            """, (import_id,))
            row = cursor.fetchone()
            import_record = dict(row) if row else None

        if not import_record:
            return {"success": False, "error": f"Import record {import_id} not found"}

        return {
            "success": True,
            "source": "opera3",
            "import_id": import_id,
            "import_record": import_record,
            "transactions": transactions,
            "count": len(transactions),
            "statement_info": {
                "opening_balance": import_record.get('opening_balance'),
                "closing_balance": import_record.get('closing_balance'),
                "statement_date": import_record.get('statement_date'),
                "account_number": import_record.get('account_number'),
                "sort_code": import_record.get('sort_code'),
            }
        }

    except Exception as e:
        logger.error(f"Opera 3 get statement transactions failed: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/opera3/bank-reconciliation/status")
async def opera3_get_bank_reconciliation_status(
    bank_code: str = Query(..., description="Bank account code"),
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """
    Get current reconciliation status for an Opera 3 bank account.
    Mirrors /api/bank-reconciliation/status.
    """
    try:
        from sql_rag.opera3_data_provider import Opera3DataProvider

        provider = Opera3DataProvider(data_path)
        status = provider.get_bank_reconciliation_status(bank_code)

        if not status.get('success', False):
            return status

        return {
            "success": True,
            "source": "opera3",
            **status
        }

    except Exception as e:
        logger.error(f"Opera 3 bank reconciliation status failed: {e}")
        return {"success": False, "error": str(e)}





@router.get("/api/opera3/bank-reconciliation/unreconciled-entries")
async def opera3_get_bank_reconciliation_unreconciled(
    bank_code: str = Query(..., description="Bank account code"),
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    include_incomplete: bool = Query(False, description="Include incomplete entries")
):
    """
    Get unreconciled cashbook entries for an Opera 3 bank account.
    Mirrors /api/bank-reconciliation/unreconciled-entries.
    Delegates to /api/opera3/reconcile/bank/{bank_code}/unreconciled.
    """
    return await opera3_get_unreconciled_entries(bank_code, data_path, include_incomplete)





@router.get("/api/opera3/cashbook/bank-accounts")
async def opera3_get_cashbook_bank_accounts(
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """
    Get list of bank accounts for transfers from Opera 3.
    Mirrors /api/cashbook/bank-accounts.
    """
    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        from pathlib import Path

        if not Path(data_path).exists():
            return {"success": False, "error": f"Opera 3 data path not found: {data_path}", "accounts": []}

        reader = Opera3Reader(data_path)
        nbank_records = reader.read_table('nbank')

        accounts = []
        for row in nbank_records:
            # Exclude foreign currency banks (nk_forgn) and petty cash if needed
            is_foreign = (row.get('nk_forgn', 0) or 0) == 1
            if is_foreign:
                continue

            accounts.append({
                "code": (row.get('nk_acnt') or '').strip(),
                "description": (row.get('nk_desc') or '').strip(),
                "sort_code": (row.get('nk_sort') or '').strip(),
                "account_number": (row.get('nk_number') or '').strip(),
                "balance": float(row.get('nk_curbal', 0) or 0) / 100.0
            })

        accounts.sort(key=lambda x: x['code'])

        return {
            "success": True,
            "accounts": accounts,
            "count": len(accounts)
        }

    except Exception as e:
        logger.error(f"Opera 3 get cashbook bank accounts failed: {e}")
        return {"success": False, "error": str(e), "accounts": []}





@router.post("/api/opera3/cashbook/create-bank-transfer")
async def opera3_create_bank_transfer(
    source_bank: str = Query(..., description="Source bank account code"),
    dest_bank: str = Query(..., description="Destination bank account code"),
    amount: float = Query(..., description="Transfer amount (positive)"),
    reference: str = Query(..., description="Reference (max 20 chars)"),
    date: str = Query(..., description="Transfer date YYYY-MM-DD"),
    comment: str = Query("", description="Optional comment"),
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """
    Create a bank transfer between two Opera 3 bank accounts.
    Mirrors /api/cashbook/create-bank-transfer.
    """
    try:
        from sql_rag.opera3_write_provider import get_opera3_writer, Opera3AgentRequired
        from datetime import date as date_type

        if source_bank == dest_bank:
            return {"success": False, "error": "Source and destination bank must be different"}

        if amount <= 0:
            return {"success": False, "error": "Transfer amount must be positive"}

        try:
            transfer_date = date_type.fromisoformat(date[:10])
        except ValueError:
            return {"success": False, "error": f"Invalid date format: {date}. Use YYYY-MM-DD"}

        try:
            foxpro_import = get_opera3_writer(data_path)
        except Opera3AgentRequired as e:
            return {"success": False, "error": str(e)}

        result = foxpro_import.import_bank_transfer(
            source_bank=source_bank,
            dest_bank=dest_bank,
            amount_pounds=amount,
            reference=reference[:20] if reference else "",
            post_date=transfer_date,
            comment=comment[:50] if comment else "",
            input_by="RECONCILE"
        )

        # import_bank_transfer returns a dict in Opera 3
        if isinstance(result, dict):
            if result.get('success', False):
                return {
                    "success": True,
                    "source_entry": result.get('source_entry', ''),
                    "dest_entry": result.get('dest_entry', ''),
                    "source_bank": source_bank,
                    "dest_bank": dest_bank,
                    "amount": amount,
                    "message": f"Bank transfer created: {result.get('source_entry', '')} / {result.get('dest_entry', '')}",
                    "warnings": result.get('warnings', [])
                }
            else:
                return {"success": False, "error": result.get('error', 'Unknown error')}
        else:
            # Handle ImportResult-style return
            if result.success:
                entry_num = getattr(result, 'entry_number', '') or ''
                entries = entry_num.split('/') if entry_num else ['', '']
                return {
                    "success": True,
                    "source_entry": entries[0] if len(entries) > 0 else '',
                    "dest_entry": entries[1] if len(entries) > 1 else '',
                    "source_bank": source_bank,
                    "dest_bank": dest_bank,
                    "amount": amount,
                    "message": f"Bank transfer created: {entry_num}",
                    "warnings": result.warnings or []
                }
            else:
                return {"success": False, "error": result.errors[0] if result.errors else 'Unknown error'}

    except Exception as e:
        logger.error(f"Opera 3 create bank transfer failed: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}



