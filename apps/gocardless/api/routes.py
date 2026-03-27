"""
GoCardless API routes.

Extracted from api/main.py - provides ALL endpoints for GoCardless import,
partner portal, mandates, subscriptions, payment requests, settings,
scan-emails, import-from-email, api-payouts, due-invoices, etc.

Covers both Opera SQL SE (/api/gocardless/*) and Opera 3 (/api/opera3/gocardless/*).
"""

import os
import json
import logging
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, UploadFile, File, Body, Request
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

router = APIRouter()


# ============================================================
# Per-request globals — populated by _sync_from_main()
# ============================================================
# Called from _ensure_company_context() in api/main.py after swapping
# company resources. Single-threaded async (one uvicorn worker) is safe.
sql_connector = None
email_storage = None
email_sync_manager = None
config = None
current_company = None
customer_linker = None
friendly_db_error = None
_bank_lock_key = None
_get_active_company_id = None
_o3_get_str = None
_o3_get_num = None
_o3_get_int = None
_complete_mandate_setup = None
_complete_mandate_setup_opera3 = None

_SYNC_NAMES = (
    'sql_connector', 'email_storage', 'email_sync_manager',
    'config', 'current_company', 'customer_linker',
    'friendly_db_error', '_bank_lock_key', '_get_active_company_id',
    '_o3_get_str', '_o3_get_num', '_o3_get_int',
    '_complete_mandate_setup', '_complete_mandate_setup_opera3',
)

def _sync_from_main():
    """Propagate per-request globals from api.main into this module."""
    import api.main as m
    g = globals()
    for name in _SYNC_NAMES:
        g[name] = getattr(m, name, None)


# GoCardless settings helpers also need these imports from company_data
from fastapi.responses import HTMLResponse
from sql_rag.company_data import get_company_db_path, get_current_db_path
from sql_rag.gocardless_payments import get_payments_db

# Subscription frequency mapping (Opera repeat doc frequency code → GoCardless interval)
FREQUENCY_MAP = {
    'W': ('weekly', 1),
    'M': ('monthly', 1),
    'Q': ('monthly', 3),
    'A': ('yearly', 1),
}


# GoCardless Settings Storage (per-company, with root fallback)
_GOCARDLESS_SETTINGS_FILENAME = "gocardless_settings.json"
# Fallback path: project root (same as api/main.py's Path(__file__).parent.parent)
# From apps/gocardless/api/routes.py, project root is 4 levels up
_GOCARDLESS_ROOT_FALLBACK = Path(__file__).resolve().parent.parent.parent.parent / _GOCARDLESS_SETTINGS_FILENAME


# ============================================================
# GoCardless Import Endpoints
# ============================================================

from sql_rag.gocardless_parser import parse_gocardless_email, parse_gocardless_table, GoCardlessBatch


@router.post("/api/gocardless/ocr")
async def ocr_gocardless_image(file: UploadFile = File(...)):
    """
    Extract text from a GoCardless screenshot using OCR.
    Accepts file upload via multipart form.
    """
    try:
        import pytesseract
        from PIL import Image
        import io

        # Read uploaded file into memory
        contents = await file.read()
        img = Image.open(io.BytesIO(contents))

        # Extract text using OCR
        text = pytesseract.image_to_string(img)

        if not text.strip():
            return {"success": False, "error": "No text could be extracted from image"}

        return {
            "success": True,
            "text": text,
            "filename": file.filename
        }

    except ImportError:
        return {"success": False, "error": "OCR not available - pytesseract not installed"}
    except Exception as e:
        logger.error(f"OCR error: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/gocardless/ocr-path")
async def ocr_gocardless_image_path(file_path: str = Body(..., embed=True)):
    """
    Extract text from a GoCardless screenshot using OCR (test mode - file path).
    """
    try:
        import pytesseract
        from PIL import Image
        import os

        if not os.path.exists(file_path):
            return {"success": False, "error": f"File not found: {file_path}"}

        img = Image.open(file_path)
        text = pytesseract.image_to_string(img)

        if not text.strip():
            return {"success": False, "error": "No text could be extracted from image"}

        return {
            "success": True,
            "text": text,
            "file_path": file_path
        }

    except ImportError:
        return {"success": False, "error": "OCR not available - pytesseract not installed"}
    except Exception as e:
        logger.error(f"OCR error: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/gocardless/test-data")
async def get_gocardless_test_data():
    """
    Returns test data extracted from gocardless.png screenshot for testing.
    """
    return {
        "success": True,
        "payment_count": 18,
        "gross_amount": 29869.80,
        "gocardless_fees": -118.31,
        "vat_on_fees": -19.73,
        "net_amount": 29751.49,
        "bank_reference": "INTSYSUKLTD-KN3CMJ",
        "payments": [
            {"customer_name": "Deep Blue Restaurantes Ltd", "description": "Intsys INV26362,26363", "amount": 7380.00, "invoice_refs": ["INV26362", "INV26363"]},
            {"customer_name": "Medimpex UK Ltd", "description": "Intsys INV26365", "amount": 1530.00, "invoice_refs": ["INV26365"]},
            {"customer_name": "The Prospect Trust", "description": "Intsys INV", "amount": 3000.00, "invoice_refs": []},
            {"customer_name": "SMCP UK Limited", "description": "Intsys INV26374,26375", "amount": 1320.00, "invoice_refs": ["INV26374", "INV26375"]},
            {"customer_name": "Vectair Systems Limited", "description": "Intsys INV26378", "amount": 8398.80, "invoice_refs": ["INV26378"]},
            {"customer_name": "Jackson Lifts", "description": "Intsys Opera 3 Support", "amount": 123.00, "invoice_refs": []},
            {"customer_name": "Vectair Systems Limited", "description": "Opera SE Toolkit", "amount": 109.20, "invoice_refs": []},
            {"customer_name": "A WARNE & CO LTD", "description": "Intsys Data Connector", "amount": 168.00, "invoice_refs": []},
            {"customer_name": "Physique Management Ltd", "description": "Intsys Pegasus Support", "amount": 551.40, "invoice_refs": []},
            {"customer_name": "Ormiston Wire Ltd", "description": "Intsys Opera 3 Support", "amount": 90.00, "invoice_refs": []},
            {"customer_name": "Totality GCS Ltd", "description": "Intsys Pegasus Support", "amount": 240.00, "invoice_refs": []},
            {"customer_name": "Red Band Chemical Co Ltd T/A Lindsay & Gilmour", "description": "Intsys Pegasus Upgrade Plan", "amount": 74.40, "invoice_refs": []},
            {"customer_name": "P Flannery Plant Hire (Oval) Ltd", "description": "Intsys Pegasus Upgrade Plan", "amount": 78.00, "invoice_refs": []},
            {"customer_name": "Harro Foods Limited", "description": "Intsys Opera 3 Sales Website", "amount": 5607.00, "invoice_refs": []},
            {"customer_name": "Physique Management Ltd", "description": "Intsys Data Connector", "amount": 168.00, "invoice_refs": []},
            {"customer_name": "Nisbets Limited", "description": "Intsys Opera 3 Licence Subs", "amount": 540.00, "invoice_refs": []},
            {"customer_name": "Vectair Systems Limited", "description": "Intsys Pegasus WEBLINK", "amount": 192.00, "invoice_refs": []},
            {"customer_name": "ST Astier Limited", "description": "Intsys CIS Support", "amount": 300.00, "invoice_refs": []}
        ]
    }


@router.post("/api/gocardless/parse")
async def parse_gocardless_content(
    content: str = Body(..., description="GoCardless email content or table text")
):
    """
    Parse GoCardless email content to extract customer payments.
    Returns parsed payments ready for customer matching.
    """
    try:
        # Try parsing as full email first, then as table only
        batch = parse_gocardless_email(content)
        if not batch.payments:
            batch = parse_gocardless_table(content)

        if not batch.payments:
            return {
                "success": False,
                "error": "Could not parse any payments from the content. Please paste the GoCardless email or payment table."
            }

        return {
            "success": True,
            "payment_count": batch.payment_count,
            "gross_amount": batch.gross_amount,
            "gocardless_fees": batch.gocardless_fees,
            "vat_on_fees": batch.vat_on_fees,
            "net_amount": batch.net_amount,
            "bank_reference": batch.bank_reference,
            "payments": [
                {
                    "customer_name": p.customer_name,
                    "description": p.description,
                    "amount": p.amount,
                    "invoice_refs": p.invoice_refs
                }
                for p in batch.payments
            ]
        }

    except Exception as e:
        logger.error(f"Error parsing GoCardless content: {e}")
        return {"success": False, "error": str(e)}


def _match_gocardless_payments_helper(payments: List[Dict[str, Any]], connector) -> Dict[str, Any]:
    """
    Helper function to match GoCardless payments to Opera customers.

    Matching uses mandate lookup only — if a payment's mandate_id or
    gocardless_customer_id is linked in the mandates table, use that
    Opera account. Otherwise leave blank for manual assignment.

    Args:
        payments: List of payment dicts with customer_name, description, amount,
                  and optionally mandate_id, customer_id
        connector: SQL connector for database queries

    Returns:
        Dict with success, payments (matched), unmatched_count
    """
    from sql_rag.gocardless_payments import get_payments_db

    # Build mandate lookups from the company-specific mandates table
    payments_db = get_payments_db()
    logger.info(f"GC match helper: using mandates DB at {payments_db.db_path}")
    all_mandates = payments_db.list_mandates()
    logger.info(f"GC match helper: found {len(all_mandates)} total mandates")

    # Lookup by mandate_id and by gocardless_customer_id
    mandate_by_id = {}
    mandate_by_customer = {}
    for m in all_mandates:
        if m.get('opera_account') and m['opera_account'] != '__UNLINKED__':
            mid = m.get('mandate_id', '').strip()
            if mid:
                mandate_by_id[mid] = m
            cid = m.get('gocardless_customer_id', '').strip() if m.get('gocardless_customer_id') else ''
            if cid:
                mandate_by_customer[cid] = m

    logger.info(f"GC match helper: {len(mandate_by_id)} mandates by ID, {len(mandate_by_customer)} by customer_id")

    # Build name-based lookup from mandates (normalised for matching)
    import re as _re
    def _normalize_company_name(name: str) -> str:
        """Normalise company name for matching: lowercase, strip common suffixes."""
        n = name.lower().strip()
        # Remove content in parentheses for matching
        n = _re.sub(r'\s*\([^)]*\)', '', n).strip()
        # Normalise "&" and "and"
        n = n.replace(' and ', ' & ')
        # Remove spaces between single letters (I C -> IC, P J -> PJ)
        n = _re.sub(r'\b([a-z])\s+([a-z])\b', r'\1\2', n)
        # Remove common company suffixes
        for suffix in [' limited', ' ltd', ' ltd.', ' plc', ' inc', ' llp', ' lp',
                       ' company', ' co', ' group', ' uk', ' holdings']:
            if n.endswith(suffix):
                n = n[:-len(suffix)].strip()
        # Remove trailing punctuation
        n = n.rstrip('.,')
        return n

    mandate_by_name = {}  # normalised_name -> mandate
    for m in all_mandates:
        if m.get('opera_account') and m['opera_account'] != '__UNLINKED__':
            # Index by opera_name (normalised)
            name = m.get('opera_name', '').strip()
            if name:
                mandate_by_name[_normalize_company_name(name)] = m
            # Also index by gocardless_name if available
            gc_name = m.get('gocardless_name', '').strip() if m.get('gocardless_name') else ''
            if gc_name:
                mandate_by_name[_normalize_company_name(gc_name)] = m

    logger.info(f"GC match helper: {len(mandate_by_name)} mandates by name")

    # Also build a customer name lookup from Opera for displaying matched names
    customers = {}
    try:
        customers_df = connector.execute_query("""
            SELECT sn_account, sn_name FROM sname WITH (NOLOCK)
            WHERE sn_stop = 0 OR sn_stop IS NULL
        """)
        if customers_df is not None:
            customers = {
                row['sn_account'].strip(): row['sn_name'].strip()
                for _, row in customers_df.iterrows()
            }
    except Exception:
        pass

    matched_payments = []
    unmatched_count = 0
    backfill_updates = []  # Track mandates that need gocardless_customer_id backfilled

    for payment in payments:
        customer_name = payment.get('customer_name', '')
        amount = payment.get('amount', 0)
        description = payment.get('description', '')
        mandate_id = payment.get('mandate_id', '')
        customer_id = payment.get('customer_id', '')
        metadata = payment.get('metadata', {})

        best_match = None
        best_name = None
        match_method = None
        metadata_invoice_refs = []

        # Priority 0: Metadata from GoCardless (set when payment was created via this app)
        # Contains opera_account and invoice refs — gives an exact match
        meta_account = (metadata.get('opera_account') or '').strip()
        meta_invoices = (metadata.get('invoices') or '').strip()
        if meta_account and meta_account in customers:
            best_match = meta_account
            best_name = customers.get(meta_account, '')
            match_method = f"metadata:opera_account={meta_account}"
            if meta_invoices:
                metadata_invoice_refs = [r.strip() for r in meta_invoices.split(',') if r.strip()]

        # Priority 1: Mandate lookup by mandate_id
        if not best_match and mandate_id and mandate_id in mandate_by_id:
            m = mandate_by_id[mandate_id]
            best_match = m['opera_account']
            best_name = customers.get(best_match, m.get('opera_name', ''))
            match_method = f"mandate:{mandate_id}"

        # Priority 2: Mandate lookup by gocardless_customer_id
        if not best_match and customer_id and customer_id in mandate_by_customer:
            m = mandate_by_customer[customer_id]
            best_match = m['opera_account']
            best_name = customers.get(best_match, m.get('opera_name', ''))
            match_method = f"customer:{customer_id}"

        # Priority 3: Name-based matching against mandate opera_name/gocardless_name
        if not best_match and customer_name and customer_name.lower() not in ('unknown', '', 'not provided'):
            norm_name = _normalize_company_name(customer_name)
            # Try exact normalised match first
            if norm_name in mandate_by_name:
                m = mandate_by_name[norm_name]
                best_match = m['opera_account']
                best_name = customers.get(best_match, m.get('opera_name', ''))
                match_method = f"name_exact:{norm_name}"
            else:
                # Try contains match (either direction)
                for stored_name, m in mandate_by_name.items():
                    if norm_name in stored_name or stored_name in norm_name:
                        best_match = m['opera_account']
                        best_name = customers.get(best_match, m.get('opera_name', ''))
                        match_method = f"name_contains:{stored_name}"
                        break

        # Priority 4: Name-based matching against Opera customer names (sname)
        # For customers not in mandates table at all
        if not best_match and customer_name and customer_name.lower() not in ('unknown', '', 'not provided'):
            norm_name = _normalize_company_name(customer_name)
            for acct, opera_name in customers.items():
                norm_opera = _normalize_company_name(opera_name)
                if norm_name == norm_opera:
                    best_match = acct
                    best_name = opera_name
                    match_method = f"opera_exact:{norm_name}"
                    break
                if norm_name in norm_opera or norm_opera in norm_name:
                    best_match = acct
                    best_name = opera_name
                    match_method = f"opera_contains:{norm_opera}"
                    break

        # Backfill gocardless_customer_id if we matched by name and have a customer_id
        if best_match and customer_id and match_method and 'name' in match_method:
            backfill_updates.append((best_match, customer_id, mandate_id))

        logger.info(f"GC match: '{customer_name}' mandate={mandate_id} -> {best_match} ({best_name}) via {match_method}")

        # Use invoice refs from metadata if available (exact match from payment request)
        invoice_refs = payment.get('invoice_refs', [])
        if not invoice_refs and metadata_invoice_refs:
            invoice_refs = metadata_invoice_refs

        matched_payment = {
            "customer_name": customer_name,
            "description": description,
            "amount": amount,
            "invoice_refs": invoice_refs,
            "matched_account": best_match,
            "matched_name": best_name,
            "match_score": 1.0 if best_match else 0,
            "match_method": match_method,
            "match_status": "matched" if best_match else "unmatched",
            "possible_duplicate": False,
            "duplicate_warning": None,
            "gc_payment_id": payment.get('gc_payment_id', '')
        }
        matched_payments.append(matched_payment)

        if not best_match:
            unmatched_count += 1

    # Backfill gocardless_customer_id on mandates matched by name
    if backfill_updates:
        try:
            import sqlite3
            conn = sqlite3.connect(str(payments_db.db_path))
            for opera_account, gc_customer_id, gc_mandate_id in backfill_updates:
                conn.execute("""
                    UPDATE gocardless_mandates
                    SET gocardless_customer_id = ?,
                        updated_at = datetime('now')
                    WHERE opera_account = ?
                      AND (gocardless_customer_id IS NULL OR gocardless_customer_id = '')
                """, (gc_customer_id, opera_account))
            conn.commit()
            conn.close()
            logger.info(f"GC match: backfilled {len(backfill_updates)} mandate customer IDs")
        except Exception as e:
            logger.warning(f"GC match: backfill failed: {e}")

    return {
        "success": True,
        "payments": matched_payments,
        "unmatched_count": unmatched_count,
        "total_count": len(payments)
    }


@router.post("/api/gocardless/match-customers")
async def match_gocardless_customers(
    payments: List[Dict[str, Any]] = Body(..., description="List of payments from parse endpoint")
):
    """
    Match GoCardless payment customer names to Opera customer accounts.

    Matching priority:
    1. Invoice reference lookup - extract INV number from description and find in stran
    2. Amount + invoice pattern - match amount against outstanding invoices
    3. Fuzzy name matching - fall back to customer name comparison
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        # Use the helper function for mandate-based matching
        result = _match_gocardless_payments_helper(payments, sql_connector)
        if not result.get("success"):
            return result

        matched_payments = result["payments"]
        unmatched_count = result.get("unmatched_count", 0)

        # Check for potential duplicates in Opera atran (cashbook) last 90 days
        # Look for receipts with same value - GoCardless batches go through cashbook
        try:
            # Get default cashbook type from settings for filtering
            gc_settings = _load_gocardless_settings()
            default_cbtype = gc_settings.get('default_batch_type', '')

            # Query atran for receipts (at_type=1 is receipt, at_value is positive for receipts)
            # Also join to aentry to get the reference - check full cashbook history
            duplicate_check_df = sql_connector.execute_query(f"""
                SELECT at_value, at_pstdate as at_date, at_cbtype, ae_entref as ae_ref, ae_pstdate as ae_date
                FROM atran WITH (NOLOCK)
                JOIN aentry WITH (NOLOCK) ON ae_acnt = at_acnt AND ae_cntr = at_cntr
                    AND ae_cbtype = at_cbtype AND ae_entry = at_entry
                WHERE at_type = 1  -- Receipts
                  {f"AND at_cbtype = '{default_cbtype}'" if default_cbtype else ""}
                ORDER BY at_pstdate DESC
            """)

            if duplicate_check_df is not None and len(duplicate_check_df) > 0:
                for payment in matched_payments:
                    amount = payment['amount']
                    # Convert to pence for comparison (atran stores in pence)
                    amount_pence = int(round(amount * 100))

                    # Check for transactions with same value
                    for _, row in duplicate_check_df.iterrows():
                        existing_pence = abs(int(row['at_value'] or 0))
                        # Allow small tolerance (1 penny)
                        if abs(existing_pence - amount_pence) <= 1:
                            payment['possible_duplicate'] = True
                            tx_date = row['at_date']
                            date_str = tx_date.strftime('%d/%m/%Y') if hasattr(tx_date, 'strftime') else str(tx_date)[:10]
                            ref = row['ae_ref'].strip() if row.get('ae_ref') else 'N/A'
                            cbtype = row['at_cbtype'].strip() if row.get('at_cbtype') else ''
                            payment['duplicate_warning'] = f"Cashbook entry found: £{existing_pence/100:.2f} on {date_str} (type: {cbtype}, ref: {ref})"
                            break
        except Exception as dup_err:
            logger.warning(f"Could not check for duplicates: {dup_err}")

        duplicate_count = len([p for p in matched_payments if p.get('possible_duplicate')])

        return {
            "success": True,
            "total_payments": len(matched_payments),
            "matched_count": len([p for p in matched_payments if p['match_status'] == 'matched']),
            "review_count": len([p for p in matched_payments if p['match_status'] == 'review']),
            "unmatched_count": unmatched_count,
            "duplicate_count": duplicate_count,
            "payments": matched_payments
        }

    except Exception as e:
        logger.error(f"Error matching GoCardless customers: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/gocardless/validate-date")
async def validate_gocardless_date(
    post_date: str = Query(..., description="Posting date to validate (YYYY-MM-DD)")
):
    """
    Validate that a posting date is allowed in Opera.
    Checks period status based on Opera's Open Period Accounting settings.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from datetime import datetime
        from sql_rag.opera_config import validate_posting_period, get_current_period_info

        # Parse date
        try:
            parsed_date = datetime.strptime(post_date, '%Y-%m-%d').date()
        except ValueError:
            return {"success": False, "valid": False, "error": f"Invalid date format: {post_date}. Use YYYY-MM-DD"}

        # Get current period info
        current_info = get_current_period_info(sql_connector)

        # Validate for Sales Ledger (GoCardless receipts affect SL)
        result = validate_posting_period(sql_connector, parsed_date, 'SL')

        return {
            "success": True,
            "valid": result.is_valid,
            "error": result.error_message if not result.is_valid else None,
            "year": result.year,
            "period": result.period,
            "current_year": current_info.get('np_year'),
            "current_period": current_info.get('np_perno'),
            "open_period_accounting": result.open_period_accounting
        }

    except Exception as e:
        logger.error(f"Error validating posting date: {e}")
        return {"success": False, "valid": False, "error": str(e)}


@router.post("/api/gocardless/import")
async def import_gocardless_batch(
    bank_code: str = Query(..., description="Opera bank account code"),
    post_date: str = Query(..., description="Posting date (YYYY-MM-DD)"),
    reference: str = Query("GoCardless", description="Batch reference"),
    complete_batch: bool = Query(False, description="Complete batch immediately or leave for review"),
    cbtype: str = Query(None, description="Cashbook type code for batched receipt"),
    gocardless_fees: float = Query(0.0, description="GoCardless fees amount in pounds (gross including VAT)"),
    vat_on_fees: float = Query(0.0, description="VAT element of fees in pounds"),
    fees_nominal_account: str = Query(None, description="Nominal account for posting net fees"),
    fees_vat_code: str = Query("2", description="VAT code for fees - looked up in ztax for rate and nominal"),
    fees_payment_type: str = Query(None, description="Cashbook type code for fees entry (e.g., 'NP')"),
    currency: str = Query(None, description="Currency code from GoCardless (e.g., 'GBP'). Rejected if not home currency."),
    payout_id: str = Query(None, description="GoCardless payout ID for history tracking"),
    source: str = Query("api", description="Import source: 'api' or 'email'"),
    dest_bank_account: str = Query(None, description="Payout destination bank account number (from GoCardless)"),
    dest_bank_sort_code: str = Query(None, description="Payout destination bank sort code (from GoCardless)"),
    payments: List[Dict[str, Any]] = Body(..., description="List of payments with customer_account, amount, and auto_allocate flag")
):
    """
    Import GoCardless batch into Opera as a batch receipt.

    Creates:
    - One aentry header (batch total)
    - Multiple atran lines (one per customer)
    - Multiple stran records

    If GC control bank is configured (gocardless_bank_code in settings), receipts+fees
    post to the control bank and net payout auto-transfers to bank_code (the destination).

    If complete_batch=False, leaves the batch for review in Opera (ae_complet=0).
    If complete_batch=True, also creates ntran/anoml records.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.opera_sql_import import OperaSQLImport
        from datetime import datetime

        # Validate payments
        if not payments:
            return {"success": False, "error": "No payments provided"}

        # Validate each payment has required fields
        validated_payments = []
        for idx, p in enumerate(payments):
            if not p.get('customer_account'):
                return {"success": False, "error": f"Payment {idx+1}: Missing customer_account"}
            if not p.get('amount'):
                return {"success": False, "error": f"Payment {idx+1}: Missing amount"}

            validated_payments.append({
                "customer_account": p['customer_account'],
                "customer_name": p.get('customer_name', ''),
                "opera_customer_name": p.get('opera_customer_name', ''),
                "amount": float(p['amount']),
                "description": p.get('description', '')[:35],
                "auto_allocate": p.get('auto_allocate', True),
                "gc_payment_id": p.get('gc_payment_id', '')
            })

        # Parse date
        try:
            parsed_date = datetime.strptime(post_date, '%Y-%m-%d').date()
        except ValueError:
            return {"success": False, "error": f"Invalid date format: {post_date}. Use YYYY-MM-DD"}

        # Validate posting period
        from sql_rag.opera_config import validate_posting_period
        period_result = validate_posting_period(sql_connector, parsed_date, 'SL')  # Sales Ledger
        if not period_result.is_valid:
            return {"success": False, "error": f"Cannot post to this date: {period_result.error_message}"}

        # Validate fees configuration - MUST have fees_nominal_account if fees > 0
        if gocardless_fees > 0 and not fees_nominal_account:
            return {
                "success": False,
                "error": f"GoCardless fees of £{gocardless_fees:.2f} cannot be posted: Fees Nominal Account not configured. "
                         "Please configure the Fees Nominal Account in GoCardless Settings before importing."
            }

        # Resolve GC control bank and destination bank
        settings = _load_gocardless_settings()
        gc_bank = settings.get("gocardless_bank_code") or os.environ.get("GOCARDLESS_BANK_CODE", "")
        transfer_cbtype = settings.get("gocardless_transfer_cbtype", "")

        # If payout has bank details, resolve destination bank from nbank by sort/account
        resolved_dest_bank = bank_code
        if dest_bank_sort_code or dest_bank_account:
            try:
                # Normalise: strip spaces/dashes
                norm_sort = (dest_bank_sort_code or "").replace(" ", "").replace("-", "").strip()
                norm_acct = (dest_bank_account or "").replace(" ", "").strip()
                # Look up matching bank in nbank
                bank_lookup = sql_connector.execute_query(f"""
                    SELECT nk_acnt, RTRIM(nk_desc) as nk_desc,
                           RTRIM(nk_sort) as nk_sort, RTRIM(nk_number) as nk_number
                    FROM nbank WITH (NOLOCK)
                """)
                if bank_lookup is not None and len(bank_lookup) > 0:
                    for _, row in bank_lookup.iterrows():
                        db_sort = (row.get('nk_sort', '') or '').replace(" ", "").replace("-", "").strip()
                        db_acct = (row.get('nk_number', '') or '').replace(" ", "").strip()
                        # Match by sort code AND account number (or account ending)
                        sort_match = norm_sort and db_sort and norm_sort == db_sort
                        acct_match = norm_acct and db_acct and (db_acct.endswith(norm_acct) or norm_acct.endswith(db_acct) or db_acct == norm_acct)
                        if sort_match and acct_match:
                            resolved_dest_bank = row['nk_acnt'].strip()
                            logger.info(f"GC import: resolved destination bank {resolved_dest_bank} from sort={norm_sort} acct={norm_acct}")
                            break
                        elif sort_match and not norm_acct:
                            # Sort code match only (account number not available from GC)
                            resolved_dest_bank = row['nk_acnt'].strip()
                            logger.info(f"GC import: resolved destination bank {resolved_dest_bank} from sort={norm_sort} (no account number)")
                            break
            except Exception as e:
                logger.warning(f"GC import: bank lookup by sort/account failed: {e}")

        # If GC control bank is set and different from destination, post to control and transfer
        destination_bank = None
        if gc_bank and gc_bank.strip() and resolved_dest_bank.strip() != gc_bank.strip():
            destination_bank = resolved_dest_bank
        posting_bank = gc_bank.strip() if gc_bank and gc_bank.strip() else resolved_dest_bank
        logger.info(f"GC import: posting_bank={posting_bank}, destination_bank={destination_bank}")

        # Validate all bank accounts exist in Opera before posting
        banks_to_check = [posting_bank]
        if destination_bank:
            banks_to_check.append(destination_bank)
        for check_bank in banks_to_check:
            bank_check = sql_connector.execute_query(f"""
                SELECT nk_acnt FROM nbank WITH (NOLOCK)
                WHERE RTRIM(nk_acnt) = '{check_bank.strip()}'
            """)
            if bank_check is None or len(bank_check) == 0:
                label = "GC Control bank" if check_bank == posting_bank else "Destination bank"
                return {
                    "success": False,
                    "error": f"{label} '{check_bank}' does not exist in this company's bank accounts. "
                             "Please update GoCardless Settings with valid bank codes for this company."
                }

        # Acquire bank-level import lock
        from sql_rag.import_lock import acquire_import_lock, release_import_lock
        if not acquire_import_lock(_bank_lock_key(posting_bank), locked_by="api", endpoint="gocardless-import"):
            return {"success": False, "error": f"Bank account {posting_bank} is currently being imported by another user. Please wait for the current import to complete."}

        # Import the batch
        importer = OperaSQLImport(sql_connector)
        result = importer.import_gocardless_batch(
                bank_account=posting_bank,
                payments=validated_payments,
                post_date=parsed_date,
                reference=reference,
                gocardless_fees=gocardless_fees,
                vat_on_fees=vat_on_fees,
                fees_nominal_account=fees_nominal_account,
                fees_vat_code=fees_vat_code,
                fees_payment_type=fees_payment_type,
                complete_batch=complete_batch,
                cbtype=cbtype,
                input_by="GOCARDLS",
                currency=currency,
            auto_allocate=True,
            destination_bank=destination_bank,
            transfer_cbtype=transfer_cbtype or None
        )

        if result.success:
            # Record to import history
            try:
                import json
                gross_amount = sum(p['amount'] for p in validated_payments)
                net_amount = gross_amount - gocardless_fees
                payments_json = json.dumps([{
                    "customer_account": p['customer_account'],
                    "gc_customer_name": p.get('customer_name', ''),
                    "opera_customer_name": p.get('opera_customer_name', ''),
                    "amount": p['amount'],
                    "description": p.get('description', '')
                } for p in validated_payments])

                email_storage.record_gocardless_import(
                    target_system='opera_se',
                    payout_id=payout_id,
                    source=source,
                    bank_reference=reference,
                    gross_amount=gross_amount,
                    net_amount=net_amount,
                    gocardless_fees=gocardless_fees,
                    vat_on_fees=vat_on_fees,
                    payment_count=len(validated_payments),
                    payments_json=payments_json,
                    batch_ref=result.batch_ref if hasattr(result, 'batch_ref') else None,
                    imported_by="GOCARDLS",
                    post_date=post_date
                )
                logger.info(f"Recorded GoCardless import to history: ref={reference}, payout_id={payout_id}")
            except Exception as hist_err:
                logger.warning(f"Failed to record import to history: {hist_err}")

            release_import_lock(_bank_lock_key(posting_bank))
            return {
                "success": True,
                "message": f"Successfully imported {len(payments)} payments",
                "payments_imported": result.records_imported,
                "complete": complete_batch,
                "details": [w for w in result.warnings if w]
            }
        else:
            release_import_lock(_bank_lock_key(posting_bank))
            return {
                "success": False,
                "error": "; ".join(result.errors),
                "payments_processed": result.records_processed
            }

    except AttributeError as e:
        try:
            from sql_rag.import_lock import release_import_lock as _rel
            _rel(_bank_lock_key(posting_bank))
        except Exception:
            pass
        logger.error(f"Error importing GoCardless batch: {e}")
        if "import_gocardless_batch" in str(e):
            return {"success": False, "error": "GoCardless batch import not available. Please restart the API server."}
        return {"success": False, "error": f"Configuration error: {e}"}
    except ConnectionError as e:
        try:
            from sql_rag.import_lock import release_import_lock as _rel
            _rel(_bank_lock_key(posting_bank))
        except Exception:
            pass
        logger.error(f"Database connection error: {e}")
        return {"success": False, "error": "Cannot connect to Opera database. Please check the connection."}
    except Exception as e:
        try:
            from sql_rag.import_lock import release_import_lock as _rel
            _rel(_bank_lock_key(posting_bank))
        except Exception:
            pass
        logger.error(f"Error importing GoCardless batch: {e}")
        error_msg = str(e)
        # Make common errors more readable
        if "Invalid object name" in error_msg:
            return {"success": False, "error": "Database table not found. Please check Opera database connection."}
        if "Login failed" in error_msg:
            return {"success": False, "error": "Database login failed. Please check credentials."}
        if "Cannot insert" in error_msg or "duplicate" in error_msg.lower():
            return {"success": False, "error": "Failed to create records in Opera. A duplicate entry may exist."}
        if "foreign key" in error_msg.lower():
            return {"success": False, "error": "Invalid customer or bank account code. Please verify the accounts exist in Opera."}
        return {"success": False, "error": f"Import failed: {error_msg}"}


@router.get("/api/gocardless/batch-types")
async def get_gocardless_batch_types():
    """
    Get available batched receipt types from Opera for GoCardless import.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        df = sql_connector.execute_query("""
            SELECT ay_cbtype, ay_desc, ay_batched
            FROM atype
            WHERE ay_type = 'R' AND ay_batched = 1
            ORDER BY ay_desc
        """)

        if df is None or len(df) == 0:
            return {
                "success": True,
                "batch_types": [],
                "warning": "No batched receipt types found. You may need to create a GoCardless type in Opera."
            }

        types = [
            {
                "code": row['ay_cbtype'].strip(),
                "description": row['ay_desc'].strip(),
                "is_gocardless": 'gocardless' in row['ay_desc'].lower()
            }
            for _, row in df.iterrows()
        ]

        return {
            "success": True,
            "batch_types": types,
            "recommended": next((t for t in types if t['is_gocardless']), types[0] if types else None)
        }

    except Exception as e:
        logger.error(f"Error getting batch types: {e}")
        return {"success": False, "error": str(e)}


def _load_gocardless_settings() -> dict:
    """Load GoCardless settings from per-company file, falling back to root.

    Uses the current_company global (set alongside sql_connector on company switch)
    rather than company_data._current_company_id to ensure settings always match
    the active database connection even when multiple tabs switch companies.
    """
    defaults = {
        "default_batch_type": "",
        "default_bank_code": "",
        "fees_nominal_account": "",
        "fees_vat_code": "1",
        "fees_payment_type": "",
        "company_reference": "",  # e.g., "INTSYSUKLTD" - filters emails by bank reference
        "exclude_description_patterns": [],  # Optional: patterns to exclude from payments (e.g., ["Cloudsis"])
        "auto_allocate": False,  # Automatically allocate receipts to matching invoices
        "gocardless_bank_code": os.environ.get("GOCARDLESS_BANK_CODE", ""),  # GC Control bank for clearing
        "gocardless_transfer_cbtype": "",  # Cashbook type for GC→bank transfer (e.g. "T1")
        "subscription_tag": "SUB",  # Analysis code used to tag subscription repeat docs
        "subscription_frequencies": ["W", "M", "A"]  # Frequency codes to include (W=Weekly, M=Monthly, Q=Quarterly, A=Annual)
    }

    # Use per-request company (from session) with fallback to process global
    company_id = _get_active_company_id()
    if company_id:
        company_path = get_company_db_path(company_id, _GOCARDLESS_SETTINGS_FILENAME)
        if company_path and company_path.exists():
            try:
                with open(company_path) as f:
                    return json.load(f)
            except Exception:
                pass

    # Fall back to root-level file (pre-migration or no company set)
    if _GOCARDLESS_ROOT_FALLBACK.exists():
        try:
            with open(_GOCARDLESS_ROOT_FALLBACK) as f:
                return json.load(f)
        except Exception:
            pass

    return defaults


def _load_gocardless_partner_settings() -> dict:
    """
    Load GoCardless settings with partner credentials.

    Partner portal endpoints have no session context (unauthenticated), so
    get_current_db_path() returns None and falls back to the root settings
    file — which may not have partner credentials.

    This function first tries the normal path, then scans all company
    settings to find one with partner_client_id configured.
    """
    # Try normal path first (works when user is logged in)
    settings = _load_gocardless_settings()
    if settings.get('partner_client_id'):
        return settings

    # No partner credentials found — scan all company settings
    data_dir = Path(__file__).parent.parent / "data"
    if data_dir.exists():
        for company_dir in sorted(data_dir.iterdir()):
            settings_file = company_dir / _GOCARDLESS_SETTINGS_FILENAME
            if settings_file.exists():
                try:
                    with open(settings_file) as f:
                        company_settings = json.load(f)
                    if company_settings.get('partner_client_id'):
                        return company_settings
                except Exception:
                    continue

    return settings


def _save_gocardless_partner_settings(settings: dict) -> bool:
    """
    Save GoCardless partner settings — finds the correct file even without session.

    Scans company settings to find the one that already has partner credentials,
    or falls back to root settings file.
    """
    # If we have a company context, use it
    company_path = get_current_db_path(_GOCARDLESS_SETTINGS_FILENAME)
    if company_path and company_path.exists():
        try:
            with open(company_path, 'w') as f:
                json.dump(settings, f, indent=2)
            return True
        except Exception:
            pass

    # No session — find which company file has partner credentials
    data_dir = Path(__file__).parent.parent / "data"
    if data_dir.exists():
        for company_dir in sorted(data_dir.iterdir()):
            settings_file = company_dir / _GOCARDLESS_SETTINGS_FILENAME
            if settings_file.exists():
                try:
                    with open(settings_file) as f:
                        existing = json.load(f)
                    if existing.get('partner_client_id'):
                        with open(settings_file, 'w') as f:
                            json.dump(settings, f, indent=2)
                        return True
                except Exception:
                    continue

    # Fall back to root
    return _save_gocardless_settings(settings)


def _save_gocardless_settings(settings: dict) -> bool:
    """Save GoCardless settings to per-company file."""
    company_id = _get_active_company_id()
    if company_id:
        company_path = get_company_db_path(company_id, _GOCARDLESS_SETTINGS_FILENAME)
    else:
        company_path = None
    save_path = company_path if company_path else _GOCARDLESS_ROOT_FALLBACK

    try:
        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, 'w') as f:
            json.dump(settings, f, indent=2)
        return True
    except Exception as e:
        logger.error(f"Failed to save GoCardless settings: {e}")
        return False


@router.get("/api/gocardless/setup-status")
async def get_gocardless_setup_status():
    """Check if GoCardless is configured. Used by launcher and GC pages to detect if signup is needed."""
    settings = _load_gocardless_settings()
    api_token = settings.get("api_access_token", "")
    configured = bool(api_token and len(api_token) > 10)

    pending_signup = None
    if not configured:
        try:
            from sql_rag.gocardless_payments import get_payments_db
            payments_db = get_payments_db()
            pending_signup = payments_db.get_latest_partner_signup()
            if pending_signup and pending_signup.get('status') in ('completed', 'failed'):
                pending_signup = None  # Only show active signups
        except Exception:
            pass

    return {
        "success": True,
        "configured": configured,
        "pending_signup": pending_signup
    }


@router.post("/api/gocardless/partner/initiate-signup")
async def initiate_gocardless_partner_signup(request: Request):
    """
    Initiate GoCardless partner signup via OAuth Connect flow.
    Generates an authorisation URL and stores the signup record.
    """
    body = await request.json()
    company_name = body.get('company_name', '')
    company_email = body.get('company_email', '')

    if not company_email:
        return {"success": False, "error": "Company email is required"}

    try:
        from sql_rag.gocardless_payments import get_payments_db
        from sql_rag.gocardless_api import create_partner_client_from_settings
        import secrets

        settings = _load_gocardless_partner_settings()
        partner_client = create_partner_client_from_settings(settings)

        authorisation_url = None
        state_token = secrets.token_urlsafe(32)

        if partner_client:
            # Build the redirect URI — use configured URI, or auto-detect from request
            redirect_uri = settings.get('partner_redirect_uri', '')
            if not redirect_uri:
                # Auto-detect from the incoming request so it works from any network address
                base = str(request.base_url).rstrip('/')
                redirect_uri = f"{base}/api/gocardless/partner/callback"
            authorisation_url = partner_client.get_authorisation_url(
                redirect_uri=redirect_uri,
                prefill_email=company_email,
                prefill_company_name=company_name,
                state=state_token,
            )

        payments_db = get_payments_db()
        signup = payments_db.create_partner_signup(
            company_name=company_name,
            company_email=company_email,
            authorisation_url=authorisation_url
        )

        # Store the state token for CSRF validation on callback
        payments_db.update_partner_signup(signup['id'], status_detail=state_token)

        if authorisation_url:
            return {
                "success": True,
                "signup_id": signup['id'],
                "authorisation_url": authorisation_url,
                "message": "Redirecting to GoCardless to complete registration.",
            }
        else:
            # Partner credentials not configured — fall back to manual setup
            return {
                "success": True,
                "signup_id": signup['id'],
                "authorisation_url": None,
                "message": "Partner credentials not configured. Please register at GoCardless and enter your API key in Settings.",
                "next_step": "manual",
            }
    except Exception as e:
        logger.error(f"Failed to initiate partner signup: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/gocardless/partner/callback")
async def gocardless_partner_callback(request: Request, code: str = None, state: str = None, error: str = None):
    """
    OAuth callback from GoCardless after merchant completes signup.
    Exchanges the authorisation code for an access token and saves it.
    GoCardless redirects the merchant's browser here, so we return HTML (not JSON).
    The signup portal polls /signup-status, so it will auto-detect completion.
    """
    from fastapi.responses import HTMLResponse, RedirectResponse

    def _callback_html(title: str, message: str, success: bool) -> HTMLResponse:
        """Return a friendly HTML page instead of raw JSON for the browser redirect."""
        color = "#10b981" if success else "#ef4444"
        icon = "&#10003;" if success else "&#10007;"
        return HTMLResponse(f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>{title}</title>
<style>body{{font-family:Inter,system-ui,sans-serif;display:flex;justify-content:center;align-items:center;min-height:100vh;margin:0;background:#f8fafc}}
.card{{text-align:center;max-width:420px;padding:3rem;background:white;border-radius:1rem;box-shadow:0 4px 24px rgba(0,0,0,0.08)}}
.icon{{font-size:3rem;color:{color};margin-bottom:1rem}}.title{{font-size:1.25rem;font-weight:700;margin-bottom:0.5rem}}
.msg{{color:#64748b;font-size:0.95rem;line-height:1.5}}.hint{{margin-top:1.5rem;color:#94a3b8;font-size:0.85rem}}</style></head>
<body><div class="card"><div class="icon">{icon}</div><div class="title">{title}</div>
<div class="msg">{message}</div><div class="hint">You can close this tab and return to the signup page.</div></div></body></html>""")

    if error:
        logger.warning(f"GoCardless partner callback error: {error}")
        return _callback_html("Signup Error", f"GoCardless returned an error: {error}", False)

    if not code:
        return _callback_html("Missing Code", "No authorisation code received from GoCardless.", False)

    try:
        from sql_rag.gocardless_payments import get_payments_db
        from sql_rag.gocardless_api import create_partner_client_from_settings

        settings = _load_gocardless_partner_settings()
        partner_client = create_partner_client_from_settings(settings)

        if not partner_client:
            return _callback_html("Not Configured", "Partner credentials not configured.", False)

        # Validate state token (CSRF protection)
        payments_db = get_payments_db()
        signup = payments_db.get_latest_partner_signup()
        if signup and state and signup.get('status_detail') != state:
            logger.warning("GoCardless partner callback: state token mismatch")
            return _callback_html("Invalid Request", "Invalid state token — please try signing up again.", False)

        # Exchange code for access token — redirect_uri MUST match what was sent in initiate-signup
        redirect_uri = settings.get('partner_redirect_uri', '')
        if not redirect_uri:
            base = str(request.base_url).rstrip('/')
            redirect_uri = f"{base}/api/gocardless/partner/callback"
        token_response = partner_client.exchange_authorisation_code(
            code=code,
            redirect_uri=redirect_uri,
        )

        access_token = token_response.get('access_token')
        organisation_id = token_response.get('organisation_id', '')

        if not access_token:
            return {"success": False, "error": "No access token received from GoCardless"}

        # Verify the token works by fetching creditor info
        org_info = {}
        try:
            org_info = partner_client.get_organisation_info(access_token)
        except Exception as e:
            logger.warning(f"Could not fetch org info after token exchange: {e}")

        creditor_name = org_info.get('name', '')

        # Store the merchant's token in their signup record — NOT in our settings.
        # api_access_token in settings is OUR token for our own DD collection.
        # Each merchant gets their own token stored against their signup record.
        if signup:
            payments_db.update_partner_signup(
                signup['id'],
                status='completed',
                completed_at=datetime.now().isoformat(),
                access_token_obtained=1,
                merchant_access_token=access_token,
                merchant_organisation_id=organisation_id,
                merchant_creditor_name=creditor_name,
                partner_referral_id=organisation_id,
                status_detail='OAuth token obtained successfully',
            )

        org_display = f" ({creditor_name})" if creditor_name else ""
        return _callback_html(
            "Account Connected",
            f"Your GoCardless account{org_display} has been connected successfully. "
            "The signup page will update automatically.",
            True
        )
    except Exception as e:
        logger.error(f"GoCardless partner callback failed: {e}")
        return _callback_html("Connection Failed", f"Something went wrong: {e}", False)


@router.get("/api/gocardless/partner/signup-status")
async def get_gocardless_partner_signup_status():
    """Poll the latest partner signup status."""
    try:
        from sql_rag.gocardless_payments import get_payments_db
        payments_db = get_payments_db()
        signup = payments_db.get_latest_partner_signup()

        if not signup:
            return {"success": True, "signup": None}

        # Strip merchant_access_token from response — never expose tokens to frontend
        safe_signup = {k: v for k, v in signup.items() if k != 'merchant_access_token'}

        return {"success": True, "signup": safe_signup}
    except Exception as e:
        logger.error(f"Failed to get partner signup status: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/gocardless/partner/admin-auth")
async def gocardless_partner_admin_auth(request: Request):
    """Validate admin password for the Crakd.ai signup app config panel."""
    body = await request.json()
    password = body.get('password', '')
    settings = _load_gocardless_partner_settings()
    stored_password = settings.get('partner_admin_password', '')
    if not stored_password:
        # First time — no password set yet, allow access to set one
        return {"success": True, "first_time": True}
    if password == stored_password:
        return {"success": True}
    return {"success": False, "error": "Incorrect password"}


@router.put("/api/gocardless/partner/admin-password")
async def update_gocardless_partner_admin_password(request: Request):
    """Set or change the admin password for the Crakd.ai signup app."""
    body = await request.json()
    new_password = body.get('password', '').strip()
    if not new_password or len(new_password) < 4:
        return {"success": False, "error": "Password must be at least 4 characters"}

    settings = _load_gocardless_partner_settings()
    settings['partner_admin_password'] = new_password
    if _save_gocardless_partner_settings(settings):
        return {"success": True}
    return {"success": False, "error": "Failed to save"}


@router.put("/api/gocardless/partner/merchant-app-url")
async def set_merchant_app_url(request: Request):
    """Save the deployment URL for a merchant."""
    body = await request.json()
    signup_id = body.get('signup_id')
    app_url = body.get('app_url', '').strip().rstrip('/')

    if not signup_id:
        return {"success": False, "error": "No signup ID provided"}

    try:
        from sql_rag.gocardless_payments import get_payments_db
        payments_db = get_payments_db()
        payments_db.update_partner_signup(signup_id, merchant_app_url=app_url)
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.post("/api/gocardless/partner/activate-merchant")
async def activate_gocardless_merchant(request: Request):
    """
    Activate a merchant — deploy their GoCardless access token to their app.
    Pushes the token to the merchant's deployment via their app URL,
    or saves locally if the app URL points to this server.
    """
    body = await request.json()
    signup_id = body.get('signup_id')

    if not signup_id:
        return {"success": False, "error": "No signup ID provided"}

    try:
        from sql_rag.gocardless_payments import get_payments_db
        payments_db = get_payments_db()
        signup = payments_db.get_merchant_signup(signup_id)

        if not signup:
            return {"success": False, "error": "Signup record not found"}

        token = signup.get('merchant_access_token')
        if not token:
            return {"success": False, "error": "No access token for this merchant — signup may not be complete"}

        app_url = signup.get('merchant_app_url', '').strip().rstrip('/')
        if not app_url:
            return {"success": False, "error": "No app URL configured for this merchant"}

        company_name = signup.get('merchant_creditor_name') or signup.get('company_name', '')

        # Check if the app URL is this server (local deployment)
        import urllib.parse
        local_hosts = ['localhost', '127.0.0.1', '0.0.0.0']
        parsed = urllib.parse.urlparse(app_url)
        is_local = parsed.hostname in local_hosts

        if is_local:
            # Deploy locally — write directly to settings
            settings = _load_gocardless_partner_settings()
            settings['api_access_token'] = token
            if not _save_gocardless_partner_settings(settings):
                return {"success": False, "error": "Failed to save local settings"}
        else:
            # Deploy remotely — push token to the merchant's app API
            try:
                import requests as req
                resp = req.put(
                    f"{app_url}/api/gocardless/deploy-token",
                    json={"access_token": token, "company_name": company_name},
                    timeout=15,
                )
                if resp.status_code != 200:
                    return {"success": False, "error": f"Remote app returned {resp.status_code}: {resp.text[:200]}"}
                data = resp.json()
                if not data.get('success'):
                    return {"success": False, "error": data.get('error', 'Remote app rejected the token')}
            except Exception as e:
                return {"success": False, "error": f"Cannot reach merchant app at {app_url}: {e}"}

        # Mark as activated
        payments_db.update_partner_signup(signup_id, status='activated')
        logger.info(f"Activated GoCardless merchant: {company_name} (signup {signup_id}) -> {app_url}")

        return {
            "success": True,
            "company_name": company_name,
            "app_url": app_url,
            "message": f"Token deployed for {company_name}",
        }
    except Exception as e:
        logger.error(f"Failed to activate merchant: {e}")
        return {"success": False, "error": str(e)}


@router.put("/api/gocardless/deploy-token")
async def deploy_gocardless_token(request: Request):
    """
    Receive a GoCardless access token from the Crakd.ai partner portal.
    Called remotely when a merchant is activated.
    """
    body = await request.json()
    token = body.get('access_token', '')
    company_name = body.get('company_name', '')

    if not token:
        return {"success": False, "error": "No token provided"}

    settings = _load_gocardless_settings()
    settings['api_access_token'] = token
    if _save_gocardless_settings(settings):
        logger.info(f"GoCardless token deployed remotely for {company_name}")
        return {"success": True, "message": f"Token deployed for {company_name}"}
    return {"success": False, "error": "Failed to save settings"}


@router.get("/api/gocardless/partner/config")
async def get_gocardless_partner_config(request: Request):
    """Check if Partner credentials are configured."""
    settings = _load_gocardless_partner_settings()
    has_partner = bool(settings.get('partner_client_id') and settings.get('partner_client_secret'))
    redirect_uri = settings.get('partner_redirect_uri', '')
    if not redirect_uri:
        base = str(request.base_url).rstrip('/')
        redirect_uri = f"{base}/api/gocardless/partner/callback"
    return {
        "success": True,
        "partner_configured": has_partner,
        "partner_sandbox": settings.get('api_sandbox', False),
        "redirect_uri": redirect_uri,
    }


@router.get("/api/gocardless/partner/merchants")
async def list_gocardless_partner_merchants(status: str = None):
    """List all merchants onboarded via the Partner signup flow."""
    try:
        from sql_rag.gocardless_payments import get_payments_db
        payments_db = get_payments_db()
        signups = payments_db.get_all_merchant_signups(status=status)

        # Strip access tokens — never expose to frontend
        safe_signups = []
        for s in signups:
            safe = {k: v for k, v in s.items() if k != 'merchant_access_token'}
            safe['has_token'] = bool(s.get('merchant_access_token'))
            safe_signups.append(safe)

        return {"success": True, "merchants": safe_signups}
    except Exception as e:
        logger.error(f"Failed to list partner merchants: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/gocardless/settings")
async def get_gocardless_settings():
    """Get GoCardless import settings.

    Note: API access token is masked for security. Returns api_key_configured flag instead.
    """
    settings = _load_gocardless_settings()

    # Mask the API key for security - don't send it to the frontend
    api_token = settings.get("api_access_token", "")
    settings["api_key_configured"] = bool(api_token and len(api_token) > 10)
    if api_token:
        # Show last 4 characters only for identification
        settings["api_key_hint"] = f"...{api_token[-4:]}" if len(api_token) > 4 else "****"
    else:
        settings["api_key_hint"] = ""
    # Remove the actual token from response
    settings.pop("api_access_token", None)

    # Mask partner client secret
    if settings.get("partner_client_secret"):
        settings["partner_client_secret"] = "••••••••"

    return {"success": True, "settings": settings}


@router.post("/api/gocardless/settings")
async def save_gocardless_settings(request: Request):
    """Save GoCardless import settings using merge approach.

    Only updates fields that are explicitly provided in the request body.
    Unspecified fields are preserved from existing settings.
    If api_access_token is None or empty, the existing token is preserved.
    """
    body = await request.json()

    # Detect if this is a partner portal request (has partner credentials but no session)
    is_partner_request = any(k in body for k in ['partner_client_id', 'partner_client_secret', 'partner_redirect_uri'])

    # Load existing settings as base — use partner-aware loader if needed
    if is_partner_request:
        existing_settings = _load_gocardless_partner_settings()
    else:
        existing_settings = _load_gocardless_settings()

    # Merge: only update fields present in the request body
    settings = dict(existing_settings)
    for key in ["default_batch_type", "default_bank_code", "fees_nominal_account",
                 "fees_vat_code", "fees_payment_type", "company_reference",
                 "archive_folder", "api_sandbox", "data_source",
                 "exclude_description_patterns", "gocardless_bank_code",
                 "gocardless_transfer_cbtype", "subscription_tag",
                 "subscription_frequencies", "partner_client_id",
                 "partner_redirect_uri", "request_statement_reference"]:
        if key in body and body[key] is not None:
            settings[key] = body[key]

    # API token: only update if a non-empty value is provided
    api_access_token = body.get("api_access_token")
    if api_access_token and str(api_access_token).strip():
        settings["api_access_token"] = str(api_access_token).strip()

    # Partner client secret: only update if a non-empty, non-masked value is provided
    partner_secret = body.get("partner_client_secret")
    if partner_secret and str(partner_secret).strip() and partner_secret != '••••••••':
        settings["partner_client_secret"] = str(partner_secret).strip()

    if is_partner_request:
        save_ok = _save_gocardless_partner_settings(settings)
    else:
        save_ok = _save_gocardless_settings(settings)

    if save_ok:
        return {"success": True, "message": "Settings saved"}
    return {"success": False, "error": "Failed to save settings"}


@router.post("/api/gocardless/update-subscription-tags")
async def update_subscription_tags(request: Request):
    """Preview or apply subscription tag updates to Opera repeat documents.

    Request body:
        mode: 'preview' or 'apply'
        overwrite: bool (default false) - if true, overwrite docs with different analysis codes
    """
    try:
        body = await request.json()
        mode = body.get("mode", "preview")
        overwrite = body.get("overwrite", False)

        gc_settings = _load_gocardless_settings()
        sub_tag = gc_settings.get("subscription_tag", "SUB")
        frequencies = gc_settings.get("subscription_frequencies", ["W", "M", "A"])

        if not sub_tag:
            return {"success": False, "error": "Subscription tag is not configured"}
        if not frequencies:
            return {"success": False, "error": "No frequency filters selected"}

        if not sql_connector:
            return {"success": False, "error": "Database not connected"}

        # Build frequency filter
        freq_placeholders = ', '.join([f':f{i}' for i in range(len(frequencies))])
        freq_params = {f'f{i}': f for i, f in enumerate(frequencies)}

        # Query matching repeat documents
        query = f"""
            SELECT ih_doc, ih_account, ih_name, ih_ignore, ih_analsys
            FROM ihead
            WHERE ih_docstat = 'U'
              AND (ih_econtr IS NULL OR ih_econtr >= GETDATE())
              AND RTRIM(ih_ignore) IN ({freq_placeholders})
            ORDER BY ih_account, ih_doc
        """

        result = sql_connector.execute_query(query, params=freq_params)

        if result is None or result.empty:
            if mode == 'preview':
                return {
                    "success": True,
                    "total_matching": 0,
                    "already_tagged": 0,
                    "will_tag": 0,
                    "has_different": 0,
                    "documents": []
                }
            return {"success": True, "updated": 0}

        documents = []
        already_tagged = 0
        will_tag = 0
        has_different = 0

        for _, row in result.iterrows():
            doc_ref = (row['ih_doc'] or '').strip()
            account = (row['ih_account'] or '').strip()
            name = (row['ih_name'] or '').strip()
            freq_code = (row['ih_ignore'] or '').strip()
            current_analsys = (row['ih_analsys'] or '').strip()

            freq_labels = {'W': 'Weekly', 'F': 'Fortnightly', 'M': 'Monthly', 'B': 'Bi-monthly', 'Q': 'Quarterly', 'H': 'Half-yearly', 'A': 'Annual'}

            if current_analsys == sub_tag:
                already_tagged += 1
                status = 'already_tagged'
            elif not current_analsys:
                will_tag += 1
                status = 'will_tag'
            else:
                has_different += 1
                status = 'has_different'

            documents.append({
                'doc_ref': doc_ref,
                'account': account,
                'name': name,
                'frequency': freq_labels.get(freq_code, freq_code),
                'frequency_code': freq_code,
                'current_analsys': current_analsys,
                'status': status
            })

        total_matching = len(documents)

        if mode == 'preview':
            return {
                "success": True,
                "tag": sub_tag,
                "total_matching": total_matching,
                "already_tagged": already_tagged,
                "will_tag": will_tag,
                "has_different": has_different,
                "documents": documents
            }

        # Apply mode
        if overwrite:
            # Update all matching docs (blank/null, already tagged, and different values)
            update_query = f"""
                UPDATE ihead WITH (ROWLOCK)
                SET ih_analsys = :tag, datemodified = GETDATE()
                WHERE ih_docstat = 'U'
                  AND (ih_econtr IS NULL OR ih_econtr >= GETDATE())
                  AND RTRIM(ih_ignore) IN ({freq_placeholders})
                  AND (RTRIM(ih_analsys) != :tag OR ih_analsys IS NULL OR RTRIM(ih_analsys) = '')
            """
        else:
            # Only update where ih_analsys is blank/null or already equals tag
            update_query = f"""
                UPDATE ihead WITH (ROWLOCK)
                SET ih_analsys = :tag, datemodified = GETDATE()
                WHERE ih_docstat = 'U'
                  AND (ih_econtr IS NULL OR ih_econtr >= GETDATE())
                  AND RTRIM(ih_ignore) IN ({freq_placeholders})
                  AND (ih_analsys IS NULL OR RTRIM(ih_analsys) = '')
            """

        update_params = {**freq_params, 'tag': sub_tag}

        from sqlalchemy import text
        with sql_connector.engine.connect() as conn:
            result = conn.execute(text(update_query), update_params)
            updated_count = result.rowcount
            conn.commit()

        return {
            "success": True,
            "updated": updated_count,
            "tag": sub_tag,
            "overwrite": overwrite
        }

    except Exception as e:
        logger.error(f"Error updating subscription tags: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/gocardless/nominal-accounts")
async def get_nominal_accounts():
    """Get nominal accounts for dropdown selection from nacnt table."""
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        df = sql_connector.execute_query("""
            SELECT na_acnt, na_desc,
                   ISNULL(na_allwprj, 0) as na_allwprj,
                   ISNULL(na_allwjob, 0) as na_allwjob,
                   RTRIM(ISNULL(na_project, '')) as na_project,
                   RTRIM(ISNULL(na_job, '')) as na_job
            FROM nacnt WITH (NOLOCK)
            WHERE na_acnt NOT LIKE 'Z%'
            ORDER BY na_acnt
        """)

        if df is None or len(df) == 0:
            return {"success": True, "accounts": []}

        accounts = [
            {
                "code": row['na_acnt'].strip(),
                "description": row['na_desc'].strip() if row['na_desc'] else '',
                "allow_project": int(row.get('na_allwprj', 0) or 0),
                "allow_department": int(row.get('na_allwjob', 0) or 0),
                "default_project": (row.get('na_project', '') or '').strip(),
                "default_department": (row.get('na_job', '') or '').strip(),
            }
            for _, row in df.iterrows()
        ]
        return {"success": True, "accounts": accounts}
    except Exception as e:
        logger.error(f"Error fetching nominal accounts: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/gocardless/vat-codes")
async def get_vat_codes(
    as_of_date: str = Query(None, description="Date to determine applicable rate (YYYY-MM-DD). Defaults to today.")
):
    """
    Get VAT codes for dropdown selection from ztax table (Purchase type for fees).

    VAT rates can change over time. This endpoint returns the applicable rate based on:
    - as_of_date parameter if provided
    - Today's date otherwise

    The ztax table stores two rate/date pairs:
    - tx_rate1 / tx_rate1dy: First rate and its effective date
    - tx_rate2 / tx_rate2dy: Second rate and its effective date

    Logic: Use the rate where the effective date is most recent but <= as_of_date
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from datetime import datetime, date
        import pandas as pd

        # Determine the reference date
        if as_of_date:
            try:
                ref_date = datetime.strptime(as_of_date, '%Y-%m-%d').date()
            except ValueError:
                ref_date = date.today()
        else:
            ref_date = date.today()

        # Query ztax table with both rates and dates
        # tx_trantyp 'P' for Purchase (fees are expenses), tx_ctrytyp 'H' for Home country
        df = sql_connector.execute_query("""
            SELECT tx_code, tx_desc, tx_rate1, tx_rate1dy, tx_rate2, tx_rate2dy
            FROM ztax WITH (NOLOCK)
            WHERE tx_trantyp = 'P' AND tx_ctrytyp = 'H'
            ORDER BY tx_code
        """)

        if df is None or len(df) == 0:
            return {"success": True, "codes": []}

        codes = []
        for _, row in df.iterrows():
            code = str(row['tx_code']).strip()
            description = row['tx_desc'].strip() if row['tx_desc'] else ''

            rate1 = float(row['tx_rate1']) if pd.notna(row['tx_rate1']) else 0
            rate2 = float(row['tx_rate2']) if pd.notna(row['tx_rate2']) else 0

            # Parse dates (handle NaT/None)
            date1 = None
            date2 = None
            if pd.notna(row['tx_rate1dy']):
                date1 = row['tx_rate1dy'].date() if hasattr(row['tx_rate1dy'], 'date') else row['tx_rate1dy']
            if pd.notna(row['tx_rate2dy']):
                date2 = row['tx_rate2dy'].date() if hasattr(row['tx_rate2dy'], 'date') else row['tx_rate2dy']

            # Determine applicable rate based on dates
            # Use the rate with the most recent effective date that's <= ref_date
            applicable_rate = rate1  # Default to rate1

            if date1 and date2:
                # Both dates exist - find the most recent one <= ref_date
                if date2 <= ref_date and date1 <= ref_date:
                    # Both are applicable, use the more recent one
                    applicable_rate = rate2 if date2 > date1 else rate1
                elif date2 <= ref_date:
                    applicable_rate = rate2
                elif date1 <= ref_date:
                    applicable_rate = rate1
            elif date2 and date2 <= ref_date:
                applicable_rate = rate2
            elif date1 and date1 <= ref_date:
                applicable_rate = rate1
            elif not date1 and not date2:
                # No dates, default to rate1
                applicable_rate = rate1

            codes.append({
                "code": code,
                "description": description,
                "rate": applicable_rate,
                # Include rate history for reference
                "rate1": rate1,
                "rate1_date": date1.isoformat() if date1 else None,
                "rate2": rate2,
                "rate2_date": date2.isoformat() if date2 else None
            })

        return {"success": True, "codes": codes, "as_of_date": ref_date.isoformat()}
    except Exception as e:
        logger.error(f"Error fetching VAT codes: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/gocardless/import-config")
async def get_gocardless_import_config(
    as_of_date: str = Query(None, description="Date to determine applicable VAT rate (YYYY-MM-DD). Defaults to today.")
):
    """
    Consolidated endpoint returning batch_types, nominal_accounts, and vat_codes
    in a single response to reduce frontend round-trips.
    """
    batch_result = await get_gocardless_batch_types()
    nominal_result = await get_nominal_accounts()
    vat_result = await get_vat_codes(as_of_date=as_of_date)

    return {
        "success": True,
        "batch_types": batch_result.get("batch_types", []),
        "batch_types_recommended": batch_result.get("recommended"),
        "nominal_accounts": nominal_result.get("accounts", []),
        "vat_codes": vat_result.get("codes", []),
        "vat_as_of_date": vat_result.get("as_of_date"),
    }


@router.get("/api/gocardless/payment-types")
async def get_nominal_payment_types():
    """Get payment types from atype (for nominal payments like fees)."""
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        # Get Payment types (ay_type = 'P') excluding batched items
        df = sql_connector.execute_query("""
            SELECT ay_cbtype, ay_desc
            FROM atype WITH (NOLOCK)
            WHERE ay_type = 'P' AND (ay_batched = 0 OR ay_batched IS NULL)
            ORDER BY ay_cbtype
        """)

        if df is None or len(df) == 0:
            return {"success": True, "types": []}

        types = [
            {"code": row['ay_cbtype'].strip(), "description": row['ay_desc'].strip()}
            for _, row in df.iterrows()
        ]
        return {"success": True, "types": types}
    except Exception as e:
        logger.error(f"Error fetching payment types: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/gocardless/test-api")
async def test_gocardless_api():
    """Test GoCardless API connection using saved credentials."""
    settings = _load_gocardless_settings()
    access_token = settings.get("api_access_token")

    if not access_token:
        return {"success": False, "error": "No API access token configured"}

    try:
        from sql_rag.gocardless_api import create_client_from_settings
        client = create_client_from_settings(settings)
        if not client:
            return {"success": False, "error": "Could not create GoCardless client"}
        result = client.test_connection()
        return result
    except Exception as e:
        logger.error(f"GoCardless API test failed: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/gocardless/api-payouts")
async def get_gocardless_api_payouts(
    status: str = Query("paid", description="Payout status filter"),
    limit: int = Query(20, description="Number of payouts to fetch"),
    days_back: Optional[int] = Query(None, description="Fetch payouts from last N days (default from settings)")
):
    """
    Fetch payouts directly from GoCardless API.

    Returns payouts with full payment details, ready for matching and import.
    Uses payout_lookback_days from settings if days_back not specified.
    """
    settings = _load_gocardless_settings()
    access_token = settings.get("api_access_token")

    # Use setting if not overridden by query param
    if days_back is None:
        days_back = int(settings.get("payout_lookback_days", 30))

    if not access_token:
        return {"success": False, "error": "No API access token configured. Go to Settings to add your GoCardless API credentials."}

    try:
        from sql_rag.gocardless_api import GoCardlessClient
        from datetime import datetime, timedelta

        sandbox = settings.get("api_sandbox", False)
        client = GoCardlessClient(access_token=access_token, sandbox=sandbox)

        # Calculate date range
        created_at_gte = (datetime.now() - timedelta(days=days_back)).date()

        # Fetch payouts
        payouts, _ = client.get_payouts(
            status=status,
            limit=limit,
            created_at_gte=created_at_gte
        )

        # Get home currency for foreign currency detection
        home_currency_code = 'GBP'
        if sql_connector:
            try:
                from sql_rag.opera_sql_import import OperaSQLImport
                importer = OperaSQLImport(sql_connector)
                home_currency_result = importer.get_home_currency()
                if isinstance(home_currency_result, dict):
                    home_currency_code = home_currency_result.get('code', 'GBP')
                elif home_currency_result:
                    home_currency_code = str(home_currency_result)
            except Exception:
                pass

        # Fetch full details for each payout (with payments)
        batches = []
        # Diagnostic counters to understand why payouts are filtered
        filter_stats = {
            "total_from_api": len(payouts),
            "filtered_duplicate_in_opera": 0,
            "filtered_already_in_history": 0,
            "filtered_period_closed": 0,
            "filtered_all_payments_excluded": 0,
            "filtered_error": 0,
            "error_details": [],
            "included": 0
        }

        # === EARLY FILTERING ===
        # Check import history BEFORE expensive get_payout_with_payments() calls
        # This avoids fetching full payment details for already-imported payouts
        company_ref = settings.get("company_reference", "").strip()
        payouts_to_fetch = []
        for payout in payouts:
            is_foreign_currency = payout.currency.upper() != home_currency_code.upper()

            # Check import history (by payout_id and bank_reference)
            try:
                if email_storage.is_gocardless_payout_imported(payout.id):
                    filter_stats["filtered_already_in_history"] += 1
                    continue
                if payout.reference and email_storage.is_gocardless_reference_imported(payout.reference):
                    filter_stats["filtered_already_in_history"] += 1
                    continue
            except Exception:
                pass

            # Check company reference prefix (cheap string check)
            if company_ref and payout.reference:
                payout_company = payout.reference.split('-')[0].upper()
                if company_ref.upper() not in payout_company and payout_company not in company_ref.upper():
                    filter_stats["filtered_all_payments_excluded"] += 1
                    continue

            payouts_to_fetch.append(payout)

        # === PARALLEL PAYOUT FETCHING ===
        # Fetch full payment details for remaining payouts in parallel
        from concurrent.futures import ThreadPoolExecutor, as_completed
        full_payouts_map = {}
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(client.get_payout_with_payments, p.id): p for p in payouts_to_fetch}
            for future in as_completed(futures):
                p = futures[future]
                try:
                    full_payouts_map[p.id] = future.result()
                except Exception as e:
                    error_msg = str(e)
                    if "archived" not in error_msg.lower():
                        filter_stats["error_details"].append(f"{p.id}: {error_msg[:200]}")
                    filter_stats["filtered_error"] += 1
                    logger.warning(f"Error fetching payout details {p.id}: {e}")

        # Process payouts in original order (preserves date ordering)
        for payout in payouts_to_fetch:
            full_payout = full_payouts_map.get(payout.id)
            if not full_payout:
                continue

            try:
                is_foreign_currency = full_payout.currency.upper() != home_currency_code.upper()

                # Check for duplicate in Opera cashbook
                # is_definite_duplicate = reference + value match in Opera (skip these)
                # is_value_mismatch = reference found but value differs (show for review)
                # possible_duplicate = amount-only match (show warning but include)
                possible_duplicate = False
                is_definite_duplicate = False
                is_value_mismatch = False
                bank_tx_warning = None
                if sql_connector:
                    try:
                        gross_pence = int(round(full_payout.gross_amount * 100))
                        net_pence = int(round(full_payout.amount * 100))

                        # Build bank account filter — check both GC control bank and destination bank
                        gc_bank = settings.get("gocardless_bank_code", "").strip()
                        dest_bank = settings.get("default_bank_code", "").strip()
                        bank_codes = [b for b in [gc_bank, dest_bank] if b]
                        if bank_codes:
                            bank_filter = "AND ae_acnt IN (" + ",".join(f"'{b}'" for b in bank_codes) + ")"
                        else:
                            bank_filter = ""  # No filter — check all banks

                        # For foreign currency, we can only reliably check by reference
                        # since the amount in Opera will be in GBP (different from EUR/USD gross)
                        if is_foreign_currency:
                            # Check by exact payout reference only (no amount comparison)
                            if full_payout.reference:
                                # Use the last part of reference (after the company prefix)
                                ref_suffix = full_payout.reference.split('-')[-1] if '-' in full_payout.reference else full_payout.reference[-8:]
                                ref_df = sql_connector.execute_query(f"""
                                    SELECT TOP 1 ae_entref, ae_value, at_pstdate as at_date
                                    FROM aentry WITH (NOLOCK)
                                    JOIN atran WITH (NOLOCK) ON ae_acnt = at_acnt AND ae_cntr = at_cntr
                                        AND ae_cbtype = at_cbtype AND ae_entry = at_entry
                                    WHERE at_type IN (1, 4, 6)
                                      AND ae_value > 0
                                      AND RTRIM(ae_entref) LIKE '%{ref_suffix}%'
                                      {bank_filter}
                                    ORDER BY at_pstdate DESC
                                """)
                                if ref_df is not None and len(ref_df) > 0:
                                    row = ref_df.iloc[0]
                                    is_definite_duplicate = True  # Reference match = definite duplicate
                                    possible_duplicate = True
                                    tx_date = row['at_date']
                                    date_str = tx_date.strftime('%d/%m/%Y') if hasattr(tx_date, 'strftime') else str(tx_date)[:10]
                                    bank_tx_warning = f"Already posted - ref '{ref_suffix}' found: £{int(row['ae_value'])/100:.2f} on {date_str} (note: foreign currency, GBP equivalent)"
                        else:
                            # For GBP payouts, check by reference then by amount
                            # When reference matches, compare Opera value against full GC gross
                            # to detect value mismatches (e.g. incorrect Cloudsis-filtered imports)
                            if full_payout.reference:
                                # Use the last part of reference for matching
                                ref_suffix = full_payout.reference.split('-')[-1] if '-' in full_payout.reference else full_payout.reference[-8:]
                                ref_df = sql_connector.execute_query(f"""
                                    SELECT TOP 1 ae_entref, ae_value, at_pstdate as at_date
                                    FROM aentry WITH (NOLOCK)
                                    JOIN atran WITH (NOLOCK) ON ae_acnt = at_acnt AND ae_cntr = at_cntr
                                        AND ae_cbtype = at_cbtype AND ae_entry = at_entry
                                    WHERE at_type IN (1, 4, 6)
                                      AND ae_value > 0
                                      AND RTRIM(ae_entref) LIKE '%{ref_suffix}%'
                                      {bank_filter}
                                    ORDER BY at_pstdate DESC
                                """)
                                if ref_df is not None and len(ref_df) > 0:
                                    row = ref_df.iloc[0]
                                    opera_value_pence = int(row['ae_value'])
                                    tx_date = row['at_date']
                                    date_str = tx_date.strftime('%d/%m/%Y') if hasattr(tx_date, 'strftime') else str(tx_date)[:10]

                                    # Compare Opera value against full GC gross
                                    if abs(opera_value_pence - gross_pence) <= 100:
                                        # Values match - correctly imported, skip
                                        is_definite_duplicate = True
                                        possible_duplicate = True
                                        bank_tx_warning = f"Already posted - ref '{ref_suffix}': £{opera_value_pence/100:.2f} on {date_str}"
                                    else:
                                        # Reference exists but value differs - include for review
                                        # Don't set possible_duplicate so import button stays enabled
                                        is_value_mismatch = True
                                        bank_tx_warning = f"Value mismatch - ref '{ref_suffix}' in Opera: £{opera_value_pence/100:.2f} on {date_str}, GC gross: £{gross_pence/100:.2f}"

                            # Check by gross amount + date proximity if not found by reference
                            # Only flag if amount matches AND date is within 14 days (avoids false positives)
                            if not possible_duplicate and gross_pence > 0 and full_payout.arrival_date:
                                payout_date_str = full_payout.arrival_date.strftime('%Y-%m-%d')
                                gross_df = sql_connector.execute_query(f"""
                                    SELECT TOP 1 ae_value, at_pstdate as at_date, ae_entref
                                    FROM aentry WITH (NOLOCK)
                                    JOIN atran WITH (NOLOCK) ON ae_acnt = at_acnt AND ae_cntr = at_cntr
                                        AND ae_cbtype = at_cbtype AND ae_entry = at_entry
                                    WHERE at_type IN (1, 4, 6)
                                      AND ae_value > 0
                                      AND ABS(ae_value - {gross_pence}) <= 1
                                      AND ABS(DATEDIFF(day, at_pstdate, '{payout_date_str}')) <= 14
                                      {bank_filter}
                                    ORDER BY at_pstdate DESC
                                """)
                                if gross_df is not None and len(gross_df) > 0:
                                    row = gross_df.iloc[0]
                                    possible_duplicate = True
                                    tx_date = row['at_date']
                                    date_str = tx_date.strftime('%d/%m/%Y') if hasattr(tx_date, 'strftime') else str(tx_date)[:10]
                                    ref = row['ae_entref'].strip() if row.get('ae_entref') else 'N/A'
                                    bank_tx_warning = f"Already posted - gross amount: £{int(row['ae_value'])/100:.2f} on {date_str} (ref: {ref})"
                    except Exception as dup_err:
                        logger.warning(f"Could not check duplicate for payout {payout.id}: {dup_err}")

                # Skip payouts with confirmed reference match (definite duplicate)
                # Amount-only matches (possible_duplicate) are NOT skipped - shown with warning
                if is_definite_duplicate:
                    filter_stats["filtered_duplicate_in_opera"] += 1
                    logger.debug(f"Skipping payout {payout.id} - already posted in Opera (reference match): {bank_tx_warning}")
                    continue

                # Validate posting period
                period_valid = True
                period_error = None
                if full_payout.arrival_date and sql_connector:
                    try:
                        from sql_rag.opera_config import validate_posting_period
                        period_result = validate_posting_period(sql_connector, full_payout.arrival_date, 'SL')
                        period_valid = period_result.is_valid
                        if not period_valid:
                            period_error = period_result.error_message
                    except Exception:
                        pass

                # Skip payouts where the posting period is closed
                if not period_valid:
                    filter_stats["filtered_period_closed"] += 1
                    logger.debug(f"Skipping payout {payout.id} - period closed: {period_error}")
                    continue

                # Build payment list from all payments (no per-payment filtering)
                all_payments = []
                for p in full_payout.payments:
                    description = p.description or p.reference or ""
                    all_payments.append({
                        "customer_name": p.customer_name or "Not provided",
                        "description": description,
                        "amount": p.amount,
                        "invoice_refs": [],
                        "customer_id": p.customer_id or "",
                        "mandate_id": p.mandate_id or "",
                        "gc_payment_id": p.id or "",
                        "metadata": p.metadata or {}
                    })

                # Use the full customer matching helper for proper name/invoice/amount matching
                if all_payments and sql_connector:
                    match_result = _match_gocardless_payments_helper(all_payments, sql_connector)
                    if match_result.get("success"):
                        matched_payments = match_result["payments"]
                    else:
                        matched_payments = all_payments
                else:
                    matched_payments = all_payments

                # Skip payout if no payments at all
                if not matched_payments:
                    filter_stats["filtered_all_payments_excluded"] += 1
                    logger.debug(f"Skipping payout {full_payout.reference} - no payments")
                    continue

                # Use actual payout amounts — no recalculation needed since nothing is filtered
                payout_gross = full_payout.gross_amount
                payout_fees = full_payout.deducted_fees
                payout_vat = full_payout.fees_vat or 0
                payout_net = payout_gross - payout_fees

                # Determine import status
                if is_foreign_currency:
                    import_status = "needs_manual_posting"
                    import_status_message = f"Foreign currency ({full_payout.currency}) - cannot auto-import, needs manual posting in Opera"
                elif not period_valid:
                    import_status = "period_closed"
                    import_status_message = period_error
                elif is_value_mismatch:
                    import_status = "value_mismatch"
                    import_status_message = bank_tx_warning
                elif possible_duplicate:
                    import_status = "review_duplicate"
                    import_status_message = bank_tx_warning
                else:
                    import_status = "ready"
                    import_status_message = None

                batch_data = {
                    "payout_id": full_payout.id,
                    "source": "api",
                    "possible_duplicate": possible_duplicate,
                    "is_value_mismatch": is_value_mismatch,
                    "bank_tx_warning": bank_tx_warning,
                    "period_valid": period_valid,
                    "period_error": period_error,
                    "is_foreign_currency": is_foreign_currency,
                    "home_currency": home_currency_code,
                    "import_status": import_status,
                    "import_status_message": import_status_message,
                    "batch": {
                        "gross_amount": payout_gross,
                        "gocardless_fees": payout_fees,
                        "vat_on_fees": payout_vat,
                        "net_amount": payout_net,
                        "bank_reference": full_payout.reference,
                        "currency": full_payout.currency,
                        "payment_date": full_payout.arrival_date.isoformat() if full_payout.arrival_date else None,
                        "payment_count": len(matched_payments),
                        "payments": matched_payments,
                        "fx_amount": full_payout.fx_amount,
                        "fx_currency": full_payout.fx_currency,
                        "exchange_rate": full_payout.exchange_rate,
                        "dest_bank_account": full_payout.bank_account_number,
                        "dest_bank_sort_code": full_payout.bank_sort_code
                    }
                }
                batches.append(batch_data)
                filter_stats["included"] += 1

            except Exception as e:
                filter_stats["filtered_error"] += 1
                filter_stats["error_details"].append(f"{payout.id}: {str(e)[:200]}")
                logger.warning(f"Error processing payout {payout.id}: {e}")

        return {
            "success": True,
            "source": "api",
            "environment": "sandbox" if sandbox else "live",
            "total_payouts": len(batches),
            "filter_stats": filter_stats,
            "batches": batches
        }

    except Exception as e:
        logger.error(f"Error fetching GoCardless API payouts: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/gocardless/import-history")
async def get_gocardless_import_history(
    limit: int = Query(50, description="Maximum records to return"),
    from_date: str = Query(None, description="Filter from date (YYYY-MM-DD)"),
    to_date: str = Query(None, description="Filter to date (YYYY-MM-DD)")
):
    """
    Get history of GoCardless imports.

    Returns list of previously imported batches with details.
    Enriches payments with Opera customer names looked up from sname table.
    """
    try:
        history = email_storage.get_gocardless_import_history(
            limit=limit,
            target_system='opera_se',
            from_date=from_date,
            to_date=to_date
        )

        # Enrich payments_json with Opera customer names and GC customer names
        if history:
            import json as _json
            # Collect all unique customer accounts across all history records
            all_accounts = set()
            for record in history:
                if record.get('payments_json'):
                    try:
                        payments = _json.loads(record['payments_json'])
                        for p in payments:
                            if p.get('customer_account'):
                                all_accounts.add(p['customer_account'])
                    except Exception:
                        pass

            # Look up Opera customer names from sname table
            opera_names = {}
            if all_accounts and sql_connector:
                try:
                    placeholders = ','.join([f"'{a}'" for a in all_accounts])
                    name_df = sql_connector.execute_query(
                        f"SELECT sn_account, sn_name FROM sname WITH (NOLOCK) WHERE sn_account IN ({placeholders})"
                    )
                    if name_df is not None and len(name_df) > 0:
                        for _, row in name_df.iterrows():
                            opera_names[row['sn_account'].strip()] = row['sn_name'].strip()
                except Exception as e:
                    logger.debug(f"Failed to look up Opera customer names for history: {e}")

            # Look up GoCardless customer names from mandates table (opera_account -> opera_name)
            gc_names = {}
            if all_accounts:
                try:
                    import sqlite3 as _sqlite3
                    from sql_rag.company_data import get_current_db_path
                    gc_db_path = get_current_db_path("gocardless_payments.db")
                    if gc_db_path and gc_db_path.exists():
                        gc_conn = _sqlite3.connect(str(gc_db_path))
                        gc_cursor = gc_conn.cursor()
                        placeholders = ','.join([f"'{a}'" for a in all_accounts])
                        gc_cursor.execute(
                            f"SELECT opera_account, opera_name FROM gocardless_mandates WHERE opera_account IN ({placeholders})"
                        )
                        for row in gc_cursor.fetchall():
                            acct = row[0].strip() if row[0] else ''
                            name = row[1].strip() if row[1] else ''
                            if acct and name:
                                gc_names[acct] = name
                        gc_conn.close()
                except Exception as e:
                    logger.debug(f"Failed to look up GC customer names for history: {e}")

            # Enrich each record's payments_json
            for record in history:
                if record.get('payments_json'):
                    try:
                        payments = _json.loads(record['payments_json'])
                        enriched = False
                        for p in payments:
                            acct = p.get('customer_account', '')
                            # Add opera_customer_name if missing
                            if not p.get('opera_customer_name') and acct in opera_names:
                                p['opera_customer_name'] = opera_names[acct]
                                enriched = True
                            # Add gc_customer_name if missing — from mandates table
                            if not p.get('gc_customer_name') and acct in gc_names:
                                p['gc_customer_name'] = gc_names[acct]
                                enriched = True
                            # Rename old customer_name to gc_customer_name for consistency
                            if 'customer_name' in p and 'gc_customer_name' not in p:
                                p['gc_customer_name'] = p.pop('customer_name')
                                enriched = True
                        if enriched:
                            record['payments_json'] = _json.dumps(payments)
                    except Exception:
                        pass

        return {
            "success": True,
            "total": len(history),
            "imports": history
        }
    except Exception as e:
        logger.error(f"Error fetching GoCardless import history: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/gocardless/receipt-search")
async def search_gocardless_receipts(
    customer: str = Query(None, description="Customer name or account code to search"),
    from_date: str = Query(None, description="Filter from date (YYYY-MM-DD)"),
    to_date: str = Query(None, description="Filter to date (YYYY-MM-DD)"),
    limit: int = Query(200, description="Maximum results")
):
    """
    Search GoCardless receipts by customer name/account and date range.

    Flattens payments_json from import history into individual receipt rows,
    each with the parent batch info (payout date, batch ref, bank reference).
    """
    try:
        import json as _json

        # Fetch all history in the date range (generous limit to search within)
        history = email_storage.get_gocardless_import_history(
            limit=1000,
            target_system='opera_se',
            from_date=from_date,
            to_date=to_date
        )

        # Flatten payments_json into individual receipt rows
        receipts = []
        search_lower = customer.lower().strip() if customer else None

        # Collect all unique accounts for Opera name lookup
        all_accounts = set()
        for record in history:
            if record.get('payments_json'):
                try:
                    payments = _json.loads(record['payments_json'])
                    for p in payments:
                        if p.get('customer_account'):
                            all_accounts.add(p['customer_account'])
                except Exception:
                    pass

        # Look up Opera customer names
        opera_names = {}
        if all_accounts and sql_connector:
            try:
                placeholders = ','.join([f"'{a}'" for a in all_accounts])
                name_df = sql_connector.execute_query(
                    f"SELECT sn_account, sn_name FROM sname WITH (NOLOCK) WHERE sn_account IN ({placeholders})"
                )
                if name_df is not None and len(name_df) > 0:
                    for _, row in name_df.iterrows():
                        opera_names[row['sn_account'].strip()] = row['sn_name'].strip()
            except Exception as e:
                logger.debug(f"Failed to look up Opera customer names for receipt search: {e}")

        for record in history:
            if not record.get('payments_json'):
                continue
            try:
                payments = _json.loads(record['payments_json'])
            except Exception:
                continue

            for p in payments:
                acct = p.get('customer_account', '')
                gc_name = p.get('gc_customer_name') or p.get('customer_name') or ''
                opera_name = p.get('opera_customer_name') or opera_names.get(acct, '')
                amount = p.get('amount', 0)

                # Apply customer search filter
                if search_lower:
                    searchable = f"{acct} {gc_name} {opera_name}".lower()
                    if search_lower not in searchable:
                        continue

                receipts.append({
                    'import_id': record.get('id'),
                    'receipt_date': record.get('post_date') or record.get('import_date'),
                    'payout_id': record.get('payout_id'),
                    'bank_reference': record.get('bank_reference'),
                    'batch_ref': record.get('batch_ref'),
                    'customer_account': acct,
                    'customer_name': opera_name or gc_name,
                    'gc_customer_name': gc_name,
                    'amount': amount,
                    'currency': p.get('currency', 'GBP'),
                    'payment_id': p.get('payment_id', ''),
                    'invoice_ref': p.get('invoice_ref') or p.get('reference') or '',
                })

        # Sort by date descending, then customer name
        receipts.sort(key=lambda r: (r['receipt_date'] or '', r['customer_name']), reverse=True)
        receipts = receipts[:limit]

        # Summary totals
        total_amount = sum(r['amount'] for r in receipts)

        return {
            "success": True,
            "total": len(receipts),
            "total_amount": round(total_amount, 2),
            "receipts": receipts
        }
    except Exception as e:
        logger.error(f"Error searching GoCardless receipts: {e}")
        return {"success": False, "error": friendly_db_error(str(e))}


@router.post("/api/gocardless/revalidate-batches")
async def revalidate_gocardless_batches(
    batches: List[Dict[str, Any]] = Body(..., description="Batches to revalidate")
):
    """
    Revalidate existing GoCardless batches against Opera.

    Use this after changing Opera parameters (opening periods, etc.) to refresh
    validation status without re-fetching from GoCardless API.

    Revalidates:
    - Posting period (checks if date is in open period)
    - Duplicate detection (checks if already posted to cashbook)
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.opera_config import validate_posting_period

        # Get current period info
        period_df = sql_connector.execute_query("""
            SELECT np_year, np_perno FROM nparm WITH (NOLOCK)
        """)
        current_period = None
        if period_df is not None and len(period_df) > 0:
            current_period = {
                "year": int(period_df.iloc[0]['np_year']),
                "period": int(period_df.iloc[0]['np_perno'])
            }

        # Get home currency
        home_currency_code = 'GBP'
        try:
            from sql_rag.opera_sql_import import OperaSQLImport
            importer = OperaSQLImport(sql_connector)
            home_currency_result = importer.get_home_currency()
            if isinstance(home_currency_result, dict):
                home_currency_code = home_currency_result.get('code', 'GBP')
            elif home_currency_result:
                home_currency_code = str(home_currency_result)
        except Exception:
            pass

        revalidated_batches = []

        for batch in batches:
            batch_data = batch.get('batch', {})
            gross_amount = batch_data.get('gross_amount', 0)
            net_amount = batch_data.get('net_amount', 0)
            bank_reference = batch_data.get('bank_reference', '')
            payment_date_str = batch_data.get('payment_date')
            currency = batch_data.get('currency', 'GBP')

            # Parse payment date
            payment_date = None
            if payment_date_str:
                try:
                    payment_date = datetime.strptime(payment_date_str[:10], '%Y-%m-%d').date()
                except:
                    pass

            # Check foreign currency
            is_foreign_currency = currency.upper() != home_currency_code.upper()

            # Revalidate posting period
            period_valid = True
            period_error = None
            if payment_date:
                try:
                    period_result = validate_posting_period(sql_connector, payment_date, 'SL')
                    period_valid = period_result.is_valid
                    if not period_valid:
                        period_error = period_result.error_message
                except Exception as e:
                    logger.warning(f"Period validation failed: {e}")

            # Revalidate duplicate detection
            possible_duplicate = False
            bank_tx_warning = None

            try:
                gross_pence = int(round(gross_amount * 100))
                net_pence = int(round(net_amount * 100))

                if is_foreign_currency:
                    # Foreign currency: only check by reference
                    if bank_reference:
                        ref_suffix = bank_reference.split('-')[-1] if '-' in bank_reference else bank_reference[-8:]
                        ref_df = sql_connector.execute_query(f"""
                            SELECT TOP 1 ae_entref, at_value, at_pstdate as at_date
                            FROM aentry WITH (NOLOCK)
                            JOIN atran WITH (NOLOCK) ON ae_acnt = at_acnt AND ae_cntr = at_cntr
                                AND ae_cbtype = at_cbtype AND ae_entry = at_entry
                            WHERE at_type IN (1, 4, 6)
                              AND at_value > 0
                              AND RTRIM(ae_entref) LIKE '%{ref_suffix}%'
                            ORDER BY at_pstdate DESC
                        """)
                        if ref_df is not None and len(ref_df) > 0:
                            row = ref_df.iloc[0]
                            possible_duplicate = True
                            tx_date = row['at_date']
                            date_str = tx_date.strftime('%d/%m/%Y') if hasattr(tx_date, 'strftime') else str(tx_date)[:10]
                            bank_tx_warning = f"Already posted - ref '{ref_suffix}' found: £{int(row['at_value'])/100:.2f} on {date_str} (note: foreign currency, GBP equivalent)"
                else:
                    # GBP: check by reference + amount OR amount alone
                    if bank_reference:
                        ref_suffix = bank_reference.split('-')[-1] if '-' in bank_reference else bank_reference[-8:]
                        ref_df = sql_connector.execute_query(f"""
                            SELECT TOP 1 ae_entref, at_value, at_pstdate as at_date
                            FROM aentry WITH (NOLOCK)
                            JOIN atran WITH (NOLOCK) ON ae_acnt = at_acnt AND ae_cntr = at_cntr
                                AND ae_cbtype = at_cbtype AND ae_entry = at_entry
                            WHERE at_type IN (1, 4, 6)
                              AND RTRIM(ae_entref) LIKE '%{ref_suffix}%'
                              AND ABS(at_value - {gross_pence}) <= 100
                            ORDER BY at_pstdate DESC
                        """)
                        if ref_df is not None and len(ref_df) > 0:
                            row = ref_df.iloc[0]
                            possible_duplicate = True
                            tx_date = row['at_date']
                            date_str = tx_date.strftime('%d/%m/%Y') if hasattr(tx_date, 'strftime') else str(tx_date)[:10]
                            bank_tx_warning = f"Already posted - ref '{ref_suffix}': £{int(row['at_value'])/100:.2f} on {date_str}"

                    # Check by gross amount if not found by reference
                    # Only flag if amount matches AND date is within 14 days
                    if not possible_duplicate and gross_pence > 0 and payment_date:
                        payout_date_str = payment_date.strftime('%Y-%m-%d')
                        gross_df = sql_connector.execute_query(f"""
                            SELECT TOP 1 at_value, at_pstdate as at_date, ae_entref
                            FROM atran WITH (NOLOCK)
                            JOIN aentry WITH (NOLOCK) ON ae_acnt = at_acnt AND ae_cntr = at_cntr
                                AND ae_cbtype = at_cbtype AND ae_entry = at_entry
                            WHERE at_type IN (1, 4, 6)
                              AND at_value > 0
                              AND ABS(at_value - {gross_pence}) <= 1
                              AND ABS(DATEDIFF(day, at_pstdate, '{payout_date_str}')) <= 14
                            ORDER BY at_pstdate DESC
                        """)
                        if gross_df is not None and len(gross_df) > 0:
                            row = gross_df.iloc[0]
                            possible_duplicate = True
                            tx_date = row['at_date']
                            date_str = tx_date.strftime('%d/%m/%Y') if hasattr(tx_date, 'strftime') else str(tx_date)[:10]
                            ref = row['ae_entref'].strip() if row.get('ae_entref') else 'N/A'
                            bank_tx_warning = f"Already posted - gross amount: £{int(row['at_value'])/100:.2f} on {date_str} (ref: {ref})"
            except Exception as dup_err:
                logger.warning(f"Duplicate check failed: {dup_err}")

            # Build revalidated batch (preserve original data, update validation fields)
            revalidated_batch = {
                **batch,
                "period_valid": period_valid,
                "period_error": period_error,
                "possible_duplicate": possible_duplicate,
                "bank_tx_warning": bank_tx_warning,
                "is_foreign_currency": is_foreign_currency,
                "home_currency": home_currency_code
            }
            revalidated_batches.append(revalidated_batch)

        return {
            "success": True,
            "batches": revalidated_batches,
            "current_period": current_period,
            "message": f"Revalidated {len(revalidated_batches)} batch(es) against Opera"
        }

    except Exception as e:
        logger.error(f"Error revalidating batches: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/gocardless/bank-accounts")
async def get_bank_accounts():
    """Get bank accounts for dropdown selection from nbank table."""
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        df = sql_connector.execute_query("""
            SELECT nk_acnt, nk_desc
            FROM nbank WITH (NOLOCK)
            ORDER BY nk_acnt
        """)

        if df is None or len(df) == 0:
            return {"success": True, "accounts": []}

        accounts = [
            {"code": row['nk_acnt'].strip(), "description": row['nk_desc'].strip() if row['nk_desc'] else ''}
            for _, row in df.iterrows()
        ]
        return {"success": True, "accounts": accounts}
    except Exception as e:
        logger.error(f"Error fetching bank accounts: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/gocardless/scan-emails")
async def scan_gocardless_emails(
    from_date: Optional[str] = Query(None, description="Start date filter (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="End date filter (YYYY-MM-DD)"),
    include_processed: bool = Query(False, description="Include previously processed emails"),
    company_reference: Optional[str] = Query(None, description="Override company reference filter (e.g., INTSYSUKLTD)")
):
    """
    Scan mailbox for GoCardless payment notification emails.

    Searches for emails from GoCardless and parses them to extract payment batches.
    Filters by company reference (from settings or parameter) to ensure only
    transactions for the correct company are returned.

    Returns a list of batches ready for review and import.
    """
    if not email_storage:
        return {"success": False, "error": "Email storage not configured"}

    try:
        from datetime import datetime
        from sql_rag.gocardless_parser import parse_gocardless_email

        # Auto-sync email inbox before scanning
        if email_sync_manager:
            try:
                # Ensure sync manager uses the current company's storage
                if email_sync_manager.storage is not email_storage:
                    email_sync_manager.storage = email_storage
                import asyncio
                # Timeout sync after 30 seconds to avoid blocking if email server is slow
                await asyncio.wait_for(email_sync_manager.sync_all_providers(), timeout=30.0)
                logger.info("Auto-synced email inbox before GoCardless scan")
            except asyncio.TimeoutError:
                logger.warning("Email sync timed out after 30s (continuing with cached emails)")
            except Exception as sync_err:
                logger.warning(f"Email sync failed (continuing with cached emails): {sync_err}")

        # Load settings to get company reference
        settings = _load_gocardless_settings()
        company_ref = company_reference or settings.get('company_reference', '')

        # Parse date filters
        date_from = datetime.strptime(from_date, '%Y-%m-%d') if from_date else None
        date_to = datetime.strptime(to_date, '%Y-%m-%d') if to_date else None

        # Get list of already-imported email IDs and bank references (unless include_processed is True)
        imported_email_ids = set()
        imported_references = set()
        if not include_processed:
            imported_email_ids = set(email_storage.get_imported_gocardless_email_ids())
            imported_references = email_storage.get_imported_gocardless_references()

        # Search for GoCardless emails
        # Search by sender domain or subject containing "gocardless"
        result = email_storage.get_emails(
            search="gocardless",
            from_date=date_from,
            to_date=date_to,
            page_size=100  # Get up to 100 emails
        )

        emails = result.get('emails', []) or result.get('items', [])

        if not emails:
            return {
                "success": True,
                "message": "No GoCardless emails found",
                "batches": [],
                "total_emails": 0,
                "company_reference": company_ref
            }

        # Import period validation
        from sql_rag.opera_config import validate_posting_period

        # Parse each email to extract payment batches
        batches = []
        processed_count = 0
        error_count = 0
        skipped_wrong_company = 0
        skipped_already_imported = 0
        skipped_duplicates = 0

        for email in emails:
            try:
                email_id = email.get('id')

                # Skip already-imported emails (unless include_processed is True)
                if email_id in imported_email_ids:
                    skipped_already_imported += 1
                    continue

                # Get email content (prefer text, fall back to HTML)
                content = email.get('body_text') or email.get('body_html') or ''

                if not content:
                    continue

                # Check if this email looks like a payment notification
                # GoCardless payment emails typically have "payout", "payment", or "paid" in subject
                subject = email.get('subject', '').lower()
                if not any(keyword in subject for keyword in ['payout', 'payment', 'collected', 'paid']):
                    continue

                # Parse the email content
                batch = parse_gocardless_email(content)

                # Filter by company reference if configured
                # The bank reference in GoCardless emails contains the company identifier (e.g., "INTSYSUKLTD")
                if company_ref:
                    batch_ref = (batch.bank_reference or '').upper()
                    if company_ref.upper() not in batch_ref and batch_ref not in company_ref.upper():
                        # Also check the email body for the reference
                        if company_ref.upper() not in content.upper():
                            skipped_wrong_company += 1
                            continue

                # Skip if this bank reference has already been imported (via email or API)
                if batch.bank_reference and batch.bank_reference in imported_references:
                    skipped_already_imported += 1
                    continue

                # Check for foreign currency (include in results but flag as not importable)
                is_foreign_currency = False
                home_currency_code = 'GBP'  # Default
                if sql_connector:
                    from sql_rag.opera_sql_import import OperaSQLImport
                    importer = OperaSQLImport(sql_connector)
                    home_currency = importer.get_home_currency()
                    home_currency_code = home_currency['code']
                    if batch.currency and batch.currency.upper() != home_currency_code.upper():
                        is_foreign_currency = True
                        logger.debug(f"Foreign currency batch found: {batch.currency} (home is {home_currency_code})")
                else:
                    # Fallback to GBP if no database connection
                    if batch.currency and batch.currency != 'GBP':
                        is_foreign_currency = True
                        logger.debug(f"Foreign currency batch found: {batch.currency}")

                # Only include if we found payments
                if batch.payments:
                    # Format payment date if available
                    payment_date_str = None
                    if batch.payment_date:
                        payment_date_str = batch.payment_date.strftime('%Y-%m-%d')

                    # Check for duplicate batch in cashbook using NET amount, GROSS amount, and reference
                    possible_duplicate = False
                    duplicate_warning = None
                    bank_tx_warning = None  # Additional check for gross amount in bank transactions
                    ref_warning = None  # Check by GoCardless reference
                    try:
                        net_pence = int(round(batch.net_amount * 100))
                        gross_pence = int(round(batch.gross_amount * 100))
                        gc_settings = _load_gocardless_settings()
                        default_cbtype = gc_settings.get('default_batch_type', '')
                        bank_ref = (batch.bank_reference or '').strip()

                        # Check 1: By GoCardless reference (most reliable for future imports)
                        if bank_ref:
                            ref_df = sql_connector.execute_query(f"""
                                SELECT TOP 1 ae_entref, at_value, at_pstdate as at_date
                                FROM aentry WITH (NOLOCK)
                                JOIN atran WITH (NOLOCK) ON ae_acnt = at_acnt AND ae_cntr = at_cntr
                                    AND ae_cbtype = at_cbtype AND ae_entry = at_entry
                                WHERE at_type = 4
                                  AND RTRIM(ae_entref) = '{bank_ref[:20]}'
                                ORDER BY at_pstdate DESC
                            """)
                            if ref_df is not None and len(ref_df) > 0:
                                row = ref_df.iloc[0]
                                possible_duplicate = True
                                tx_date = row['at_date']
                                date_str = tx_date.strftime('%d/%m/%Y') if hasattr(tx_date, 'strftime') else str(tx_date)[:10]
                                ref_warning = f"Already imported: ref '{bank_ref}' on {date_str}"

                        # Check 2: NET amount in cashbook (catches direct GoCardless imports where net was posted)
                        if not ref_warning:  # Only check by amount if reference didn't match
                            dup_df = sql_connector.execute_query(f"""
                                SELECT TOP 1 at_value, at_pstdate as at_date, at_cbtype, ae_entref
                                FROM atran WITH (NOLOCK)
                                JOIN aentry WITH (NOLOCK) ON ae_acnt = at_acnt AND ae_cntr = at_cntr
                                    AND ae_cbtype = at_cbtype AND ae_entry = at_entry
                                WHERE at_type = 4
                                  AND ABS(at_value - {net_pence}) <= 1
                                  {f"AND at_cbtype = '{default_cbtype}'" if default_cbtype else ""}
                                ORDER BY at_pstdate DESC
                            """)
                            if dup_df is not None and len(dup_df) > 0:
                                row = dup_df.iloc[0]
                                possible_duplicate = True
                                tx_date = row['at_date']
                                date_str = tx_date.strftime('%d/%m/%Y') if hasattr(tx_date, 'strftime') else str(tx_date)[:10]
                                ref = row['ae_entref'].strip() if row.get('ae_entref') else 'N/A'
                                duplicate_warning = f"Cashbook entry found: £{int(row['at_value'])/100:.2f} on {date_str} (ref: {ref})"

                        # Check 3: GROSS amount in cashbook (catches bank statement imports or manual entries)
                        # Check all positive receipt types (1=Sales Receipt, 4=Nominal Payment, 6=Nominal Receipt)
                        # Manual GoCardless entries often use type 4 with "GC" reference
                        # Only flag if date is within 14 days of payout date
                        if batch.payment_date:
                            payout_date_str = batch.payment_date.strftime('%Y-%m-%d')
                            gross_df = sql_connector.execute_query(f"""
                                SELECT TOP 1 at_value, at_pstdate as at_date, at_cbtype, ae_entref, at_refer
                                FROM atran WITH (NOLOCK)
                                JOIN aentry WITH (NOLOCK) ON ae_acnt = at_acnt AND ae_cntr = at_cntr
                                    AND ae_cbtype = at_cbtype AND ae_entry = at_entry
                                WHERE at_type IN (1, 4, 6)
                                  AND at_value > 0
                                  AND ABS(at_value - {gross_pence}) <= 1
                                  AND ABS(DATEDIFF(day, at_pstdate, '{payout_date_str}')) <= 14
                                ORDER BY at_pstdate DESC
                            """)
                            if gross_df is not None and len(gross_df) > 0:
                                row = gross_df.iloc[0]
                                tx_date = row['at_date']
                                date_str = tx_date.strftime('%d/%m/%Y') if hasattr(tx_date, 'strftime') else str(tx_date)[:10]
                                existing_ref = row['ae_entref'].strip() if row.get('ae_entref') else (row.get('at_refer', '').strip() or 'N/A')
                                # Only flag as definite duplicate if the existing ref matches our GC ref
                                # Otherwise just show as warning (amount-only match is not conclusive)
                                if bank_ref and existing_ref.upper().startswith(bank_ref[:10].upper()):
                                    bank_tx_warning = f"Already posted - gross amount: £{int(row['at_value'])/100:.2f} on {date_str} (ref: {existing_ref})"
                                    if not possible_duplicate:
                                        possible_duplicate = True
                                else:
                                    # Warning only - different ref suggests different transaction with same amount
                                    bank_tx_warning = f"Similar amount found: £{int(row['at_value'])/100:.2f} on {date_str} (ref: {existing_ref}) - verify before importing"

                        # Check 3b: Find batched entries where total matches gross and verify individual payments
                        # This catches manual posting as a batch without correct GC reference
                        # Only run this expensive check if reference and gross amount checks didn't find anything
                        if not ref_warning and not bank_tx_warning and batch.payments and len(batch.payments) > 1:
                            # Find entries where total of positive values equals gross amount
                            batch_entry_df = sql_connector.execute_query(f"""
                                SELECT at_acnt, at_cntr, at_cbtype, at_entry,
                                       SUM(at_value) as entry_total,
                                       MIN(at_pstdate) as entry_date,
                                       COUNT(*) as line_count
                                FROM atran WITH (NOLOCK)
                                WHERE at_type IN (1, 4, 6)
                                  AND at_value > 0
                                GROUP BY at_acnt, at_cntr, at_cbtype, at_entry
                                HAVING ABS(SUM(at_value) - {gross_pence}) <= 10
                                   AND COUNT(*) >= {len(batch.payments)}
                                ORDER BY MIN(at_pstdate) DESC
                            """)
                            if batch_entry_df is not None and len(batch_entry_df) > 0:
                                # Check each matching entry to see if individual payments match
                                for _, entry_row in batch_entry_df.iterrows():
                                    entry_key = f"at_acnt = '{entry_row['at_acnt'].strip()}' AND at_cntr = '{entry_row['at_cntr'].strip()}' AND at_cbtype = '{entry_row['at_cbtype'].strip()}' AND at_entry = '{entry_row['at_entry'].strip()}'"
                                    entry_lines_df = sql_connector.execute_query(f"""
                                        SELECT at_value, at_name FROM atran WITH (NOLOCK)
                                        WHERE {entry_key} AND at_type IN (1, 4, 6) AND at_value > 0
                                    """)
                                    if entry_lines_df is not None and len(entry_lines_df) > 0:
                                        # Get all amounts from this entry
                                        entry_amounts = sorted([int(row['at_value']) for _, row in entry_lines_df.iterrows()])
                                        # Get all amounts from GoCardless batch
                                        gc_amounts = sorted([int(round(p.amount * 100)) for p in batch.payments])
                                        # Check if amounts match (allow 1 penny tolerance per amount)
                                        if len(entry_amounts) == len(gc_amounts):
                                            amounts_match = all(abs(a - b) <= 1 for a, b in zip(entry_amounts, gc_amounts))
                                            if amounts_match:
                                                tx_date = entry_row['entry_date']
                                                date_str = tx_date.strftime('%d/%m/%Y') if hasattr(tx_date, 'strftime') else str(tx_date)[:10]
                                                bank_tx_warning = f"Already posted - batch: {len(entry_amounts)} payments totaling £{int(entry_row['entry_total'])/100:.2f} on {date_str}"
                                                if not possible_duplicate:
                                                    possible_duplicate = True
                                                break

                        # Check 3c: Individual payment amounts with GC reference (catches manual posting of individual customer receipts)
                        if not bank_tx_warning and batch.payments:
                            for payment in batch.payments[:5]:  # Check first 5 payments
                                payment_pence = int(round(payment.amount * 100))
                                payment_df = sql_connector.execute_query(f"""
                                    SELECT TOP 1 at_value, at_pstdate as at_date, at_name, at_refer
                                    FROM atran WITH (NOLOCK)
                                    WHERE at_type IN (1, 4)
                                      AND at_value > 0
                                      AND ABS(at_value - {payment_pence}) <= 1
                                      AND (at_refer LIKE '%GC%' OR at_refer LIKE '%GoCardless%')
                                    ORDER BY at_pstdate DESC
                                """)
                                if payment_df is not None and len(payment_df) > 0:
                                    row = payment_df.iloc[0]
                                    tx_date = row['at_date']
                                    date_str = tx_date.strftime('%d/%m/%Y') if hasattr(tx_date, 'strftime') else str(tx_date)[:10]
                                    name = row['at_name'].strip()[:20] if row.get('at_name') else ''
                                    bank_tx_warning = f"Already posted - payment: £{int(row['at_value'])/100:.2f} ({name}) on {date_str} with GC ref"
                                    if not possible_duplicate:
                                        possible_duplicate = True
                                    break  # Found one match, that's enough

                        # Check 4: FEES amount in cashbook (catches manual posting of fees as separate payment)
                        # Fees are posted as negative payments, so check for matching payment amount
                        fees_pence = int(round(abs(batch.gocardless_fees) * 100))
                        if fees_pence > 0:
                            fees_df = sql_connector.execute_query(f"""
                                SELECT TOP 1 at_value, at_pstdate as at_date, at_cbtype, ae_entref
                                FROM atran WITH (NOLOCK)
                                JOIN aentry WITH (NOLOCK) ON ae_acnt = at_acnt AND ae_cntr = at_cntr
                                    AND ae_cbtype = at_cbtype AND ae_entry = at_entry
                                WHERE at_type IN (2, 4)
                                  AND ABS(ABS(at_value) - {fees_pence}) <= 1
                                ORDER BY at_pstdate DESC
                            """)
                            if fees_df is not None and len(fees_df) > 0:
                                row = fees_df.iloc[0]
                                tx_date = row['at_date']
                                date_str = tx_date.strftime('%d/%m/%Y') if hasattr(tx_date, 'strftime') else str(tx_date)[:10]
                                ref = row['ae_entref'].strip() if row.get('ae_entref') else 'N/A'
                                if not bank_tx_warning:
                                    bank_tx_warning = f"Already posted - fees: £{abs(int(row['at_value']))/100:.2f} on {date_str} (ref: {ref})"
                                else:
                                    bank_tx_warning += f" | Fees also posted: £{abs(int(row['at_value']))/100:.2f} on {date_str}"
                                if not possible_duplicate:
                                    possible_duplicate = True
                    except Exception as dup_err:
                        logger.warning(f"Could not check batch duplicate: {dup_err}")

                    # Validate posting period for the payment date
                    period_valid = True
                    period_error = None
                    if batch.payment_date:
                        try:
                            period_result = validate_posting_period(sql_connector, batch.payment_date.date(), 'SL')
                            period_valid = period_result.is_valid
                            if not period_valid:
                                period_error = period_result.error_message
                        except Exception as period_err:
                            logger.warning(f"Could not validate period: {period_err}")

                    batch_data = {
                        "email_id": email.get('id'),
                        "email_subject": email.get('subject'),
                        "email_date": email.get('received_at'),
                        "email_from": email.get('from_address'),
                        "possible_duplicate": possible_duplicate,
                        "duplicate_warning": duplicate_warning,
                        "bank_tx_warning": bank_tx_warning,  # Gross amount found in bank transactions
                        "ref_warning": ref_warning,  # Reference already exists in cashbook
                        "period_valid": period_valid,
                        "period_error": period_error,
                        "is_foreign_currency": is_foreign_currency,
                        "home_currency": home_currency_code,
                        "batch": {
                            "gross_amount": batch.gross_amount,
                            "gocardless_fees": batch.gocardless_fees,
                            "vat_on_fees": batch.vat_on_fees,
                            "net_amount": batch.net_amount,
                            "bank_reference": batch.bank_reference,
                            "currency": batch.currency,
                            "payment_date": payment_date_str,
                            "payment_count": len(batch.payments),
                            "payments": [
                                {
                                    "customer_name": p.customer_name,
                                    "description": p.description,
                                    "amount": p.amount,
                                    "invoice_refs": p.invoice_refs
                                }
                                for p in batch.payments
                            ]
                        }
                    }
                    # Always include batch but track duplicate count for stats
                    batches.append(batch_data)
                    if possible_duplicate:
                        skipped_duplicates += 1
                    processed_count += 1

            except Exception as e:
                logger.warning(f"Error parsing email {email.get('id')}: {e}")
                error_count += 1
                continue

        # Get current period info for client-side validation
        from sql_rag.opera_config import get_current_period_info
        current_period = get_current_period_info(sql_connector)

        return {
            "success": True,
            "total_emails": len(emails),
            "parsed_count": processed_count,
            "error_count": error_count,
            "skipped_wrong_company": skipped_wrong_company,
            "skipped_already_imported": skipped_already_imported,
            "skipped_duplicates": skipped_duplicates,
            "company_reference": company_ref,
            "current_period": {
                "year": current_period.get('np_year'),
                "period": current_period.get('np_perno')
            },
            "batches": batches
        }

    except Exception as e:
        logger.error(f"Error scanning GoCardless emails: {e}")
        return {"success": False, "error": str(e)}


@router.delete("/api/gocardless/import-history")
async def clear_gocardless_import_history(
    from_date: Optional[str] = Query(None, description="Clear from date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="Clear to date (YYYY-MM-DD)")
):
    """
    Clear GoCardless import history within a date range.

    If no dates specified, clears ALL history (use with caution).
    This only removes the tracking records - does not affect Opera data.
    """
    if not email_storage:
        return {"success": False, "error": "Email storage not configured"}

    try:
        deleted_count = email_storage.clear_gocardless_import_history(
            from_date=from_date,
            to_date=to_date
        )
        return {
            "success": True,
            "deleted_count": deleted_count,
            "message": f"Cleared {deleted_count} import history records"
        }
    except Exception as e:
        logger.error(f"Error clearing GoCardless import history: {e}")
        return {"success": False, "error": str(e)}


@router.delete("/api/gocardless/import-history/{record_id}")
async def delete_gocardless_import_record(record_id: int):
    """
    Delete a single import history record to allow re-importing.

    This removes the tracking record so the payout can be fetched and imported again.
    Does not affect Opera data - only the import tracking.
    """
    if not email_storage:
        return {"success": False, "error": "Email storage not configured"}

    try:
        deleted = email_storage.delete_gocardless_import_record(record_id)
        if deleted:
            return {
                "success": True,
                "message": "Import record deleted - payout can now be re-imported"
            }
        else:
            return {"success": False, "error": "Record not found"}
    except Exception as e:
        logger.error(f"Error deleting GoCardless import record: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/gocardless/skip-payout")
async def skip_gocardless_payout(
    request: Request,
    payout_id: str = Query(..., description="GoCardless payout ID"),
    bank_reference: str = Query(..., description="Bank reference (e.g., INTSYSUKLTD-XM5XEF)"),
    gross_amount: float = Query(..., description="Gross amount"),
    currency: str = Query("GBP", description="Currency code"),
    payment_count: int = Query(0, description="Number of payments"),
    reason: str = Query("manual", description="Reason for skipping: 'foreign_currency', 'manual', 'duplicate'"),
    fx_amount: Optional[float] = Query(None, description="GBP equivalent amount for foreign currency payouts")
):
    """
    Skip a payout and record to history without importing.

    Use this for:
    - Foreign currency payouts that need manual posting in Opera
    - Payouts already manually entered
    - Payouts that shouldn't be imported for other reasons

    The payout will appear in import history and won't show in available payouts.
    """
    if not email_storage:
        return {"success": False, "error": "Email storage not configured"}

    try:
        # Map reason to imported_by tag
        imported_by_map = {
            'foreign_currency': f'MANUAL-{currency}',
            'manual': 'MANUAL-SKIP',
            'duplicate': 'MANUAL-DUP'
        }
        imported_by = imported_by_map.get(reason, 'MANUAL-SKIP')

        # Include currency in reference for non-GBP
        display_reference = bank_reference
        if currency and currency.upper() != 'GBP':
            display_reference = f"{bank_reference} ({currency})"

        # Save payment details if provided in request body
        payments_json = None
        try:
            body = await request.json()
            if isinstance(body, list) and len(body) > 0:
                payments_json = json.dumps([{
                    "customer_account": p.get('matched_account') or p.get('customer_account', ''),
                    "gc_customer_name": p.get('customer_name', ''),
                    "amount": p.get('amount', 0),
                    "description": p.get('description', ''),
                } for p in body])
        except Exception:
            pass  # No body or invalid JSON — payments_json stays None

        record_id = email_storage.record_gocardless_import(
            target_system='opera_se',
            payout_id=payout_id,
            source='api',
            bank_reference=display_reference,
            gross_amount=gross_amount,
            net_amount=gross_amount,  # Net unknown for skipped
            gocardless_fees=0,
            vat_on_fees=0,
            payment_count=payment_count,
            payments_json=payments_json,
            batch_ref=None,
            imported_by=imported_by,
            fx_amount=fx_amount
        )

        return {
            "success": True,
            "message": f"Payout {bank_reference} sent to history (needs manual posting)",
            "record_id": record_id,
            "reason": reason
        }
    except Exception as e:
        logger.error(f"Error skipping GoCardless payout: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/gocardless/import-from-email")
async def import_gocardless_from_email(
    email_id: int = Query(..., description="Email ID to import from"),
    bank_code: str = Query(..., description="Opera bank account code"),
    post_date: str = Query(..., description="Posting date (YYYY-MM-DD)"),
    reference: str = Query("GoCardless", description="Batch reference"),
    complete_batch: bool = Query(False, description="Complete batch immediately"),
    cbtype: str = Query(None, description="Cashbook type code"),
    gocardless_fees: float = Query(0.0, description="GoCardless fees amount (gross including VAT)"),
    vat_on_fees: float = Query(0.0, description="VAT element of fees"),
    fees_nominal_account: str = Query(None, description="Nominal account for net fees"),
    fees_vat_code: str = Query("2", description="VAT code for fees - looked up in ztax for rate and nominal"),
    currency: str = Query(None, description="Currency code from GoCardless (e.g., 'GBP'). Rejected if not home currency."),
    archive_folder: str = Query("Archive/GoCardless", description="Folder to move email after import"),
    dest_bank_account: str = Query(None, description="Payout destination bank account number (from GoCardless)"),
    dest_bank_sort_code: str = Query(None, description="Payout destination bank sort code (from GoCardless)"),
    payments: List[Dict[str, Any]] = Body(..., description="List of payments with matched customer accounts")
):
    """
    Import GoCardless batch from a scanned email.

    This endpoint takes the email ID and matched payment data, validates the period,
    and imports into Opera. If GC control bank is configured, auto-transfers net to destination.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.opera_sql_import import OperaSQLImport
        from datetime import datetime

        # Parse date
        try:
            parsed_date = datetime.strptime(post_date, '%Y-%m-%d').date()
        except ValueError:
            return {"success": False, "error": f"Invalid date format: {post_date}. Use YYYY-MM-DD"}

        # Validate posting period (Sales Ledger)
        from sql_rag.opera_config import validate_posting_period
        period_result = validate_posting_period(sql_connector, parsed_date, 'SL')
        if not period_result.is_valid:
            return {"success": False, "error": f"Cannot post to this date: {period_result.error_message}"}

        # Validate payments
        if not payments:
            return {"success": False, "error": "No payments provided"}

        validated_payments = []
        for idx, p in enumerate(payments):
            if not p.get('customer_account'):
                return {"success": False, "error": f"Payment {idx+1}: Missing customer_account"}
            if not p.get('amount'):
                return {"success": False, "error": f"Payment {idx+1}: Missing amount"}

            validated_payments.append({
                "customer_account": p['customer_account'],
                "customer_name": p.get('customer_name', ''),
                "opera_customer_name": p.get('opera_customer_name', ''),
                "amount": float(p['amount']),
                "description": p.get('description', '')[:35],
                "auto_allocate": p.get('auto_allocate', True),
                "gc_payment_id": p.get('gc_payment_id', '')
            })

        # Validate fees_nominal_account is configured if there are fees
        if gocardless_fees and gocardless_fees > 0 and not fees_nominal_account:
            return {
                "success": False,
                "error": f"GoCardless fees of £{gocardless_fees:.2f} cannot be posted: Fees Nominal Account not configured. "
                         "Please configure the Fees Nominal Account in GoCardless Settings before importing."
            }

        # Resolve GC control bank and destination bank
        settings = _load_gocardless_settings()
        gc_bank = settings.get("gocardless_bank_code") or os.environ.get("GOCARDLESS_BANK_CODE", "")
        transfer_cbtype = settings.get("gocardless_transfer_cbtype", "")

        # If payout has bank details, resolve destination bank from nbank by sort/account
        resolved_dest_bank = bank_code
        if dest_bank_sort_code or dest_bank_account:
            try:
                norm_sort = (dest_bank_sort_code or "").replace(" ", "").replace("-", "").strip()
                norm_acct = (dest_bank_account or "").replace(" ", "").strip()
                bank_lookup = sql_connector.execute_query(f"""
                    SELECT nk_acnt, RTRIM(nk_desc) as nk_desc,
                           RTRIM(nk_sort) as nk_sort, RTRIM(nk_number) as nk_number
                    FROM nbank WITH (NOLOCK)
                """)
                if bank_lookup is not None and len(bank_lookup) > 0:
                    for _, row in bank_lookup.iterrows():
                        db_sort = (row.get('nk_sort', '') or '').replace(" ", "").replace("-", "").strip()
                        db_acct = (row.get('nk_number', '') or '').replace(" ", "").strip()
                        sort_match = norm_sort and db_sort and norm_sort == db_sort
                        acct_match = norm_acct and db_acct and (db_acct.endswith(norm_acct) or norm_acct.endswith(db_acct) or db_acct == norm_acct)
                        if sort_match and acct_match:
                            resolved_dest_bank = row['nk_acnt'].strip()
                            logger.info(f"GC email import: resolved destination bank {resolved_dest_bank} from sort={norm_sort} acct={norm_acct}")
                            break
                        elif sort_match and not norm_acct:
                            resolved_dest_bank = row['nk_acnt'].strip()
                            logger.info(f"GC email import: resolved destination bank {resolved_dest_bank} from sort={norm_sort} (no account number)")
                            break
            except Exception as e:
                logger.warning(f"GC email import: bank lookup by sort/account failed: {e}")

        destination_bank = None
        if gc_bank and gc_bank.strip() and resolved_dest_bank.strip() != gc_bank.strip():
            destination_bank = resolved_dest_bank
        posting_bank = gc_bank.strip() if gc_bank and gc_bank.strip() else resolved_dest_bank

        # Validate all bank accounts exist in Opera before posting
        for check_bank in ([posting_bank] + ([destination_bank] if destination_bank else [])):
            bank_check = sql_connector.execute_query(f"""
                SELECT nk_acnt FROM nbank WITH (NOLOCK)
                WHERE RTRIM(nk_acnt) = '{check_bank.strip()}'
            """)
            if bank_check is None or len(bank_check) == 0:
                label = "GC Control bank" if check_bank == posting_bank else "Destination bank"
                return {
                    "success": False,
                    "error": f"{label} '{check_bank}' does not exist in this company's bank accounts. "
                             "Please update GoCardless Settings with valid bank codes for this company."
                }

        # Acquire bank-level import lock
        from sql_rag.import_lock import acquire_import_lock, release_import_lock
        if not acquire_import_lock(_bank_lock_key(posting_bank), locked_by="api", endpoint="gocardless-import-from-email"):
            return {"success": False, "error": f"Bank account {posting_bank} is currently being imported by another user. Please wait for the current import to complete."}

        # Import the batch
        importer = OperaSQLImport(sql_connector)
        result = importer.import_gocardless_batch(
            bank_account=posting_bank,
            payments=validated_payments,
            post_date=parsed_date,
            reference=reference,
            gocardless_fees=gocardless_fees,
            vat_on_fees=vat_on_fees,
            fees_nominal_account=fees_nominal_account,
            fees_vat_code=fees_vat_code,
            complete_batch=complete_batch,
            cbtype=cbtype,
            input_by="GOCARDLS",
            currency=currency,
            auto_allocate=True,
            destination_bank=destination_bank,
            transfer_cbtype=transfer_cbtype or None
        )

        if result.success:
            # Record the import to track this email as processed
            # Only AFTER successful Opera import - email will be filtered from future scans
            try:
                import json as json_mod
                gross_amount = sum(p.get('amount', 0) for p in payments)
                net_amount = gross_amount - gocardless_fees
                history_payments = json_mod.dumps([{
                    "customer_account": p['customer_account'],
                    "gc_customer_name": p.get('customer_name', ''),
                    "opera_customer_name": p.get('opera_customer_name', ''),
                    "amount": p['amount'],
                    "description": p.get('description', '')
                } for p in validated_payments])
                email_storage.record_gocardless_import(
                    email_id=email_id,
                    target_system='opera_se',
                    bank_reference=reference,
                    gross_amount=gross_amount,
                    net_amount=net_amount,
                    payment_count=len(payments),
                    payments_json=history_payments,
                    batch_ref=result.batch_number,
                    imported_by="GOCARDLS",
                    post_date=post_date
                )
            except Exception as track_err:
                logger.warning(f"Failed to record GoCardless import tracking: {track_err}")

            # Archive the email (move to archive folder)
            archive_status = "not_attempted"
            if archive_folder and email_storage:
                try:
                    # Get email details including message_id and provider_id
                    email_details = email_storage.get_email_by_id(email_id)
                    if email_details:
                        provider_id = email_details.get('provider_id')
                        message_id = email_details.get('message_id')
                        source_folder = email_details.get('folder_id', 'INBOX')

                        if provider_id and message_id and provider_id in email_sync_manager.providers:
                            provider = email_sync_manager.providers[provider_id]
                            # Move email to archive folder
                            move_success = await provider.move_email(
                                message_id=message_id,
                                source_folder=source_folder,
                                dest_folder=archive_folder
                            )
                            archive_status = "archived" if move_success else "move_failed"
                            if move_success:
                                logger.info(f"Archived GoCardless email {email_id} to {archive_folder}")
                            else:
                                logger.warning(f"Failed to archive email {email_id}")
                        else:
                            archive_status = "provider_not_available"
                    else:
                        archive_status = "email_not_found"
                except Exception as archive_err:
                    logger.warning(f"Failed to archive GoCardless email: {archive_err}")
                    archive_status = f"error: {str(archive_err)}"

            release_import_lock(_bank_lock_key(posting_bank))
            return {
                "success": True,
                "message": f"Successfully imported {len(payments)} payments from email",
                "email_id": email_id,
                "payments_imported": result.records_imported,
                "complete": complete_batch,
                "archive_status": archive_status
            }
        else:
            release_import_lock(_bank_lock_key(posting_bank))
            return {
                "success": False,
                "error": "; ".join(result.errors),
                "payments_processed": result.records_processed
            }

    except Exception as e:
        logger.error(f"Error importing GoCardless from email: {e}")
        try:
            release_import_lock(_bank_lock_key(posting_bank))
        except Exception:
            pass
        return {"success": False, "error": str(e)}


@router.post("/api/gocardless/archive-email")
async def archive_gocardless_email(
    email_id: int = Query(..., description="Email ID to archive"),
    archive_folder: str = Query("Archive/GoCardless", description="Folder to move email after archive")
):
    """
    Archive a GoCardless email without importing (for duplicates already in Opera).

    This marks the email as processed so it won't appear in future scans,
    and moves it to the archive folder.
    """
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not configured")

    try:
        # Record the email as processed (with no import data)
        try:
            email_storage.record_gocardless_import(
                email_id=email_id,
                target_system='archived',  # Mark as archived, not imported
                bank_reference='ARCHIVED',
                gross_amount=0,
                net_amount=0,
                payment_count=0,
                batch_ref=None,
                imported_by="ARCHIVE"
            )
        except Exception as track_err:
            logger.warning(f"Failed to record archive tracking: {track_err}")

        # Archive the email (move to archive folder)
        archive_status = "not_attempted"
        if archive_folder and email_sync_manager:
            try:
                # Get email details including message_id and provider_id
                email_details = email_storage.get_email_by_id(email_id)
                if email_details:
                    provider_id = email_details.get('provider_id')
                    message_id = email_details.get('message_id')
                    source_folder = email_details.get('folder_id', 'INBOX')

                    if provider_id and message_id and provider_id in email_sync_manager.providers:
                        provider = email_sync_manager.providers[provider_id]
                        # Move email to archive folder
                        move_success = await provider.move_email(
                            message_id=message_id,
                            source_folder=source_folder,
                            dest_folder=archive_folder
                        )
                        archive_status = "archived" if move_success else "move_failed"
                        if move_success:
                            logger.info(f"Archived GoCardless email {email_id} to {archive_folder}")
                        else:
                            logger.warning(f"Failed to archive email {email_id}")
                    else:
                        archive_status = "provider_not_available"
                else:
                    archive_status = "email_not_found"
            except Exception as archive_err:
                logger.warning(f"Failed to archive GoCardless email: {archive_err}")
                archive_status = f"error: {str(archive_err)}"

        return {
            "success": True,
            "message": "Email archived (already in Opera)",
            "email_id": email_id,
            "archive_status": archive_status
        }

    except Exception as e:
        logger.error(f"Error archiving GoCardless email: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/opera3/gocardless/import")
async def opera3_import_gocardless_batch(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    bank_code: str = Query(..., description="Opera bank account code"),
    post_date: str = Query(..., description="Posting date (YYYY-MM-DD)"),
    reference: str = Query("GoCardless", description="Batch reference"),
    complete_batch: bool = Query(False, description="Complete batch immediately"),
    cbtype: str = Query(None, description="Cashbook type code"),
    gocardless_fees: float = Query(0.0, description="GoCardless fees amount"),
    vat_on_fees: float = Query(0.0, description="VAT element of fees"),
    fees_nominal_account: str = Query(None, description="Nominal account for fees"),
    fees_payment_type: str = Query(None, description="Cashbook type code for fees entry"),
    payout_id: str = Query(None, description="GoCardless payout ID for history tracking"),
    source: str = Query("api", description="Import source: 'api' or 'email'"),
    dest_bank_account: str = Query(None, description="Payout destination bank account number (from GoCardless)"),
    dest_bank_sort_code: str = Query(None, description="Payout destination bank sort code (from GoCardless)"),
    payments: List[Dict[str, Any]] = Body(..., description="List of payments")
):
    """
    Import GoCardless batch into Opera 3 as a batch receipt.

    Creates:
    - One aentry header (batch total)
    - Multiple atran lines (one per customer)
    - Multiple stran records
    """
    try:
        from sql_rag.opera3_write_provider import get_opera3_writer, Opera3AgentRequired
        from datetime import datetime
        import json

        # Validate payments
        if not payments:
            return {"success": False, "error": "No payments provided"}

        # Validate each payment
        validated_payments = []
        for idx, p in enumerate(payments):
            if not p.get('customer_account'):
                return {"success": False, "error": f"Payment {idx+1}: Missing customer_account"}
            if not p.get('amount'):
                return {"success": False, "error": f"Payment {idx+1}: Missing amount"}
            validated_payments.append({
                "customer_account": p['customer_account'],
                "customer_name": p.get('customer_name', ''),
                "opera_customer_name": p.get('opera_customer_name', ''),
                "amount": float(p['amount']),
                "description": p.get('description', '')[:35]
            })

        # Parse date
        try:
            parsed_date = datetime.strptime(post_date, '%Y-%m-%d').date()
        except ValueError:
            return {"success": False, "error": f"Invalid date format: {post_date}. Use YYYY-MM-DD"}

        # Resolve GC control bank and destination bank
        settings = _load_gocardless_settings()
        gc_bank = settings.get("gocardless_bank_code", "")
        transfer_cbtype = settings.get("gocardless_transfer_cbtype", "")

        # If payout has bank details, resolve destination bank from Opera 3 nbank by sort/account
        from sql_rag.opera3_foxpro import Opera3Reader
        reader = Opera3Reader(data_path)
        nbank_records = reader.read_table("nbank")

        resolved_dest_bank = bank_code
        if dest_bank_sort_code or dest_bank_account:
            norm_sort = (dest_bank_sort_code or "").replace(" ", "").replace("-", "").strip()
            norm_acct = (dest_bank_account or "").replace(" ", "").strip()
            for rec in nbank_records:
                db_sort = (_o3_get_str(rec, 'nk_sort') or '').replace(" ", "").replace("-", "").strip()
                db_acct = (_o3_get_str(rec, 'nk_number') or '').replace(" ", "").strip()
                sort_match = norm_sort and db_sort and norm_sort == db_sort
                acct_match = norm_acct and db_acct and (db_acct.endswith(norm_acct) or norm_acct.endswith(db_acct) or db_acct == norm_acct)
                if sort_match and acct_match:
                    resolved_dest_bank = _o3_get_str(rec, 'nk_account').strip()
                    logger.info(f"O3 GC import: resolved destination bank {resolved_dest_bank} from sort={norm_sort} acct={norm_acct}")
                    break
                elif sort_match and not norm_acct:
                    resolved_dest_bank = _o3_get_str(rec, 'nk_account').strip()
                    logger.info(f"O3 GC import: resolved destination bank {resolved_dest_bank} from sort={norm_sort} (no account number)")
                    break

        destination_bank = None
        if gc_bank and gc_bank.strip() and resolved_dest_bank.strip() != gc_bank.strip():
            destination_bank = resolved_dest_bank
        posting_bank = gc_bank.strip() if gc_bank and gc_bank.strip() else resolved_dest_bank

        # Validate all bank accounts exist in Opera 3 nbank
        valid_banks = {_o3_get_str(r, 'nk_account') for r in nbank_records}
        for check_bank in ([posting_bank] + ([destination_bank] if destination_bank else [])):
            if check_bank.strip() not in valid_banks:
                label = "GC Control bank" if check_bank == posting_bank else "Destination bank"
                return {
                    "success": False,
                    "error": f"{label} '{check_bank}' does not exist in this company's bank accounts. "
                             "Please update GoCardless Settings with valid bank codes for this company."
                }

        # Import the batch
        try:
            importer = get_opera3_writer(data_path)
        except Opera3AgentRequired as e:
            return {"success": False, "error": str(e)}
        result = importer.import_gocardless_batch(
            bank_account=posting_bank,
            payments=validated_payments,
            post_date=parsed_date,
            reference=reference,
            gocardless_fees=gocardless_fees,
            vat_on_fees=vat_on_fees,
            fees_nominal_account=fees_nominal_account,
            fees_payment_type=fees_payment_type,
            complete_batch=complete_batch,
            cbtype=cbtype,
            input_by="GOCARDLS",
            destination_bank=destination_bank,
            transfer_cbtype=transfer_cbtype or None
        )

        if result.success:
            # Record to import history
            try:
                gross_amount = sum(p['amount'] for p in validated_payments)
                net_amount = gross_amount - gocardless_fees
                payments_json = json.dumps([{
                    "customer_account": p['customer_account'],
                    "gc_customer_name": p.get('customer_name', ''),
                    "opera_customer_name": p.get('opera_customer_name', ''),
                    "amount": p['amount'],
                    "description": p.get('description', '')
                } for p in validated_payments])

                email_storage.record_gocardless_import(
                    target_system='opera3',
                    payout_id=payout_id,
                    source=source,
                    bank_reference=reference,
                    gross_amount=gross_amount,
                    net_amount=net_amount,
                    gocardless_fees=gocardless_fees,
                    vat_on_fees=vat_on_fees,
                    payment_count=len(validated_payments),
                    payments_json=payments_json,
                    batch_ref=result.entry_number,
                    imported_by="GOCARDLS",
                    post_date=post_date
                )
                logger.info(f"Recorded Opera 3 GoCardless import to history: ref={reference}")
            except Exception as hist_err:
                logger.warning(f"Failed to record import to history: {hist_err}")

            return {
                "success": True,
                "message": f"Successfully imported {len(payments)} payments",
                "payments_imported": result.records_imported,
                "complete": complete_batch,
                "details": result.warnings
            }
        else:
            return {
                "success": False,
                "error": "; ".join(result.errors),
                "payments_processed": result.records_processed
            }

    except Exception as e:
        logger.error(f"Error importing GoCardless batch to Opera 3: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/opera3/gocardless/import-history")
async def opera3_get_gocardless_import_history(
    limit: int = Query(50, description="Maximum records to return"),
    from_date: str = Query(None, description="Filter from date (YYYY-MM-DD)"),
    to_date: str = Query(None, description="Filter to date (YYYY-MM-DD)")
):
    """Get history of GoCardless imports into Opera 3. Enriches with customer names from sname."""
    try:
        history = email_storage.get_gocardless_import_history(
            limit=limit,
            target_system='opera3',
            from_date=from_date,
            to_date=to_date
        )

        # Enrich payments_json with Opera 3 customer names and GC names
        if history:
            import json as _json
            all_accounts = set()
            for record in history:
                if record.get('payments_json'):
                    try:
                        payments = _json.loads(record['payments_json'])
                        for p in payments:
                            if p.get('customer_account'):
                                all_accounts.add(p['customer_account'])
                    except Exception:
                        pass

            # Look up names from Opera 3 FoxPro sname
            opera_names = {}
            if all_accounts:
                try:
                    from sql_rag.opera3_foxpro import Opera3FoxPro
                    data_path = current_company.get('opera3_data_path', '') if current_company else ''
                    if data_path:
                        o3 = Opera3FoxPro(data_path)
                        for acct in all_accounts:
                            try:
                                records = o3.read_table('sname', f"sn_account = '{acct}'")
                                if records:
                                    opera_names[acct] = records[0].get('SN_NAME', records[0].get('sn_name', '')).strip()
                            except Exception:
                                pass
                except Exception as e:
                    logger.debug(f"Failed to look up Opera 3 customer names for history: {e}")

            # Look up GoCardless customer names from mandates table
            gc_names = {}
            if all_accounts:
                try:
                    import sqlite3 as _sqlite3
                    from sql_rag.company_data import get_current_db_path
                    gc_db_path = get_current_db_path("gocardless_payments.db")
                    if gc_db_path and gc_db_path.exists():
                        gc_conn = _sqlite3.connect(str(gc_db_path))
                        gc_cursor = gc_conn.cursor()
                        placeholders = ','.join([f"'{a}'" for a in all_accounts])
                        gc_cursor.execute(
                            f"SELECT opera_account, opera_name FROM gocardless_mandates WHERE opera_account IN ({placeholders})"
                        )
                        for row in gc_cursor.fetchall():
                            acct = row[0].strip() if row[0] else ''
                            name = row[1].strip() if row[1] else ''
                            if acct and name:
                                gc_names[acct] = name
                        gc_conn.close()
                except Exception as e:
                    logger.debug(f"Failed to look up GC customer names for O3 history: {e}")

            for record in history:
                if record.get('payments_json'):
                    try:
                        payments = _json.loads(record['payments_json'])
                        enriched = False
                        for p in payments:
                            acct = p.get('customer_account', '')
                            if not p.get('opera_customer_name') and acct in opera_names:
                                p['opera_customer_name'] = opera_names[acct]
                                enriched = True
                            if not p.get('gc_customer_name') and acct in gc_names:
                                p['gc_customer_name'] = gc_names[acct]
                                enriched = True
                            if 'customer_name' in p and 'gc_customer_name' not in p:
                                p['gc_customer_name'] = p.pop('customer_name')
                                enriched = True
                        if enriched:
                            record['payments_json'] = _json.dumps(payments)
                    except Exception:
                        pass

        return {
            "success": True,
            "total": len(history),
            "imports": history
        }
    except Exception as e:
        logger.error(f"Error fetching Opera 3 GoCardless import history: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/opera3/gocardless/receipt-search")
async def opera3_search_gocardless_receipts(
    customer: str = Query(None, description="Customer name or account code to search"),
    from_date: str = Query(None, description="Filter from date (YYYY-MM-DD)"),
    to_date: str = Query(None, description="Filter to date (YYYY-MM-DD)"),
    limit: int = Query(200, description="Maximum results")
):
    """
    Search GoCardless receipts by customer name/account and date range (Opera 3).

    Flattens payments_json from import history into individual receipt rows.
    """
    try:
        import json as _json

        history = email_storage.get_gocardless_import_history(
            limit=1000,
            target_system='opera3',
            from_date=from_date,
            to_date=to_date
        )

        receipts = []
        search_lower = customer.lower().strip() if customer else None

        # Collect accounts for Opera 3 name lookup
        all_accounts = set()
        for record in history:
            if record.get('payments_json'):
                try:
                    payments = _json.loads(record['payments_json'])
                    for p in payments:
                        if p.get('customer_account'):
                            all_accounts.add(p['customer_account'])
                except Exception:
                    pass

        # Look up Opera 3 customer names from FoxPro sname
        opera_names = {}
        if all_accounts:
            try:
                from sql_rag.opera3_foxpro import Opera3FoxPro
                data_path = current_company.get('opera3_data_path', '') if current_company else ''
                if data_path:
                    o3 = Opera3FoxPro(data_path)
                    for acct in all_accounts:
                        try:
                            records = o3.read_table('sname', f"sn_account = '{acct}'")
                            if records:
                                opera_names[acct] = records[0].get('SN_NAME', records[0].get('sn_name', '')).strip()
                        except Exception:
                            pass
            except Exception as e:
                logger.debug(f"Failed to look up Opera 3 customer names for receipt search: {e}")

        for record in history:
            if not record.get('payments_json'):
                continue
            try:
                payments = _json.loads(record['payments_json'])
            except Exception:
                continue

            for p in payments:
                acct = p.get('customer_account', '')
                gc_name = p.get('gc_customer_name') or p.get('customer_name') or ''
                opera_name = p.get('opera_customer_name') or opera_names.get(acct, '')
                amount = p.get('amount', 0)

                if search_lower:
                    searchable = f"{acct} {gc_name} {opera_name}".lower()
                    if search_lower not in searchable:
                        continue

                receipts.append({
                    'import_id': record.get('id'),
                    'receipt_date': record.get('post_date') or record.get('import_date'),
                    'payout_id': record.get('payout_id'),
                    'bank_reference': record.get('bank_reference'),
                    'batch_ref': record.get('batch_ref'),
                    'customer_account': acct,
                    'customer_name': opera_name or gc_name,
                    'gc_customer_name': gc_name,
                    'amount': amount,
                    'currency': p.get('currency', 'GBP'),
                    'payment_id': p.get('payment_id', ''),
                    'invoice_ref': p.get('invoice_ref') or p.get('reference') or '',
                })

        receipts.sort(key=lambda r: (r['receipt_date'] or '', r['customer_name']), reverse=True)
        receipts = receipts[:limit]
        total_amount = sum(r['amount'] for r in receipts)

        return {
            "success": True,
            "total": len(receipts),
            "total_amount": round(total_amount, 2),
            "receipts": receipts
        }
    except Exception as e:
        logger.error(f"Error searching Opera 3 GoCardless receipts: {e}")
        return {"success": False, "error": friendly_db_error(str(e))}


@router.get("/api/opera3/gocardless/batch-types")
async def opera3_get_gocardless_batch_types(
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """Get available batched receipt types from Opera 3 for GoCardless import."""
    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        reader = Opera3Reader(data_path)
        records = reader.read_table("atype")

        types = []
        for r in records:
            ay_type = _o3_get_str(r, 'ay_type')
            ay_batched = _o3_get_int(r, 'ay_batched')
            if ay_type == 'R' and ay_batched == 1:
                code = _o3_get_str(r, 'ay_cbtype')
                desc = _o3_get_str(r, 'ay_desc')
                types.append({
                    "code": code,
                    "description": desc,
                    "is_gocardless": 'gocardless' in desc.lower()
                })

        types.sort(key=lambda t: t['description'])

        return {
            "success": True,
            "batch_types": types,
            "recommended": next((t for t in types if t['is_gocardless']), types[0] if types else None)
        }
    except Exception as e:
        logger.error(f"Error getting Opera 3 batch types: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/opera3/gocardless/nominal-accounts")
async def opera3_get_nominal_accounts(
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """Get nominal accounts for dropdown selection from Opera 3 nacnt table."""
    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        reader = Opera3Reader(data_path)
        records = reader.read_table("nacnt")

        accounts = []
        for r in records:
            code = _o3_get_str(r, 'na_acnt')
            if code.startswith('Z'):
                continue
            desc = _o3_get_str(r, 'na_desc')
            accounts.append({
                "code": code,
                "description": desc,
                "allow_project": _o3_get_int(r, 'na_allwprj'),
                "allow_department": _o3_get_int(r, 'na_allwjob'),
                "default_project": _o3_get_str(r, 'na_project'),
                "default_department": _o3_get_str(r, 'na_job'),
            })

        accounts.sort(key=lambda a: a['code'])
        return {"success": True, "accounts": accounts}
    except Exception as e:
        logger.error(f"Error fetching Opera 3 nominal accounts: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/opera3/gocardless/vat-codes")
async def opera3_get_vat_codes(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    as_of_date: str = Query(None, description="Date to determine applicable rate (YYYY-MM-DD)")
):
    """Get VAT codes from Opera 3 ztax table (Purchase type for fees)."""
    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        from datetime import datetime, date as date_type

        reader = Opera3Reader(data_path)
        records = reader.read_table("ztax")

        if as_of_date:
            try:
                ref_date = datetime.strptime(as_of_date, '%Y-%m-%d').date()
            except ValueError:
                ref_date = date_type.today()
        else:
            ref_date = date_type.today()

        codes = []
        for r in records:
            trantyp = _o3_get_str(r, 'tx_trantyp')
            ctrytyp = _o3_get_str(r, 'tx_ctrytyp')
            if trantyp != 'P' or ctrytyp != 'H':
                continue

            code = _o3_get_str(r, 'tx_code')
            description = _o3_get_str(r, 'tx_desc')
            rate1 = _o3_get_num(r, 'tx_rate1')
            rate2 = _o3_get_num(r, 'tx_rate2')

            # Parse dates
            date1_raw = r.get('TX_RATE1DY', r.get('tx_rate1dy'))
            date2_raw = r.get('TX_RATE2DY', r.get('tx_rate2dy'))
            date1 = None
            date2 = None
            if date1_raw and str(date1_raw) != 'None':
                if hasattr(date1_raw, 'date'):
                    date1 = date1_raw.date()
                elif isinstance(date1_raw, date_type):
                    date1 = date1_raw
            if date2_raw and str(date2_raw) != 'None':
                if hasattr(date2_raw, 'date'):
                    date2 = date2_raw.date()
                elif isinstance(date2_raw, date_type):
                    date2 = date2_raw

            applicable_rate = rate1
            if date1 and date2:
                if date2 <= ref_date and date1 <= ref_date:
                    applicable_rate = rate2 if date2 > date1 else rate1
                elif date2 <= ref_date:
                    applicable_rate = rate2
                elif date1 <= ref_date:
                    applicable_rate = rate1
            elif date2 and date2 <= ref_date:
                applicable_rate = rate2
            elif date1 and date1 <= ref_date:
                applicable_rate = rate1
            elif not date1 and not date2:
                applicable_rate = rate1

            codes.append({
                "code": code,
                "description": description,
                "rate": applicable_rate,
                "rate1": rate1,
                "rate1_date": date1.isoformat() if date1 else None,
                "rate2": rate2,
                "rate2_date": date2.isoformat() if date2 else None
            })

        codes.sort(key=lambda c: c['code'])
        return {"success": True, "codes": codes, "as_of_date": ref_date.isoformat()}
    except Exception as e:
        logger.error(f"Error fetching Opera 3 VAT codes: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/opera3/gocardless/import-config")
async def opera3_get_gocardless_import_config(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    as_of_date: str = Query(None, description="Date to determine applicable VAT rate (YYYY-MM-DD). Defaults to today.")
):
    """
    Consolidated Opera 3 endpoint returning batch_types, nominal_accounts, and vat_codes
    in a single response to reduce frontend round-trips.
    """
    batch_result = await opera3_get_gocardless_batch_types(data_path=data_path)
    nominal_result = await opera3_get_nominal_accounts(data_path=data_path)
    vat_result = await opera3_get_vat_codes(data_path=data_path, as_of_date=as_of_date)

    return {
        "success": True,
        "batch_types": batch_result.get("batch_types", []),
        "batch_types_recommended": batch_result.get("recommended"),
        "nominal_accounts": nominal_result.get("accounts", []),
        "vat_codes": vat_result.get("codes", []),
        "vat_as_of_date": vat_result.get("as_of_date"),
    }


@router.get("/api/opera3/gocardless/payment-types")
async def opera3_get_payment_types(
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """Get payment types from Opera 3 atype (for nominal payments like fees)."""
    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        reader = Opera3Reader(data_path)
        records = reader.read_table("atype")

        types = []
        for r in records:
            ay_type = _o3_get_str(r, 'ay_type')
            ay_batched = _o3_get_int(r, 'ay_batched')
            if ay_type == 'P' and ay_batched == 0:
                types.append({
                    "code": _o3_get_str(r, 'ay_cbtype'),
                    "description": _o3_get_str(r, 'ay_desc')
                })

        types.sort(key=lambda t: t['code'])
        return {"success": True, "types": types}
    except Exception as e:
        logger.error(f"Error fetching Opera 3 payment types: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/opera3/gocardless/bank-accounts")
async def opera3_get_bank_accounts(
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """Get bank accounts from Opera 3 nbank table."""
    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        reader = Opera3Reader(data_path)
        records = reader.read_table("nbank")

        accounts = []
        for r in records:
            code = _o3_get_str(r, 'nk_acnt')
            if code:
                accounts.append({
                    "code": code,
                    "description": _o3_get_str(r, 'nk_desc')
                })

        accounts.sort(key=lambda a: a['code'])
        return {"success": True, "accounts": accounts}
    except Exception as e:
        logger.error(f"Error fetching Opera 3 bank accounts: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/opera3/gocardless/match-customers")
async def opera3_match_gocardless_customers(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    payments: List[Dict[str, Any]] = Body(..., description="List of payments from parse endpoint")
):
    """Match GoCardless payment customer names to Opera 3 customer accounts using mandate lookup."""
    try:
        import re as _re
        from sql_rag.opera3_foxpro import Opera3Reader
        from sql_rag.gocardless_payments import get_payments_db

        def _normalize_company_name(name: str) -> str:
            n = name.lower().strip()
            n = _re.sub(r'\s*\([^)]*\)', '', n).strip()
            n = n.replace(' and ', ' & ')
            n = _re.sub(r'\b([a-z])\s+([a-z])\b', r'\1\2', n)
            for suffix in [' limited', ' ltd', ' ltd.', ' plc', ' inc', ' llp', ' lp',
                           ' company', ' co', ' group', ' uk', ' holdings']:
                if n.endswith(suffix):
                    n = n[:-len(suffix)].strip()
            n = n.rstrip('.,')
            return n

        # Build mandate lookups from the company-specific mandates table
        payments_db = get_payments_db()
        all_mandates = payments_db.list_mandates()

        mandate_by_id = {}
        mandate_by_customer = {}
        mandate_by_name = {}
        for m in all_mandates:
            if m.get('opera_account') and m['opera_account'] != '__UNLINKED__':
                mid = m.get('mandate_id', '').strip()
                if mid:
                    mandate_by_id[mid] = m
                cid = m.get('gocardless_customer_id', '').strip() if m.get('gocardless_customer_id') else ''
                if cid:
                    mandate_by_customer[cid] = m
                name = m.get('opera_name', '').strip()
                if name:
                    mandate_by_name[_normalize_company_name(name)] = m
                gc_name = m.get('gocardless_name', '').strip() if m.get('gocardless_name') else ''
                if gc_name:
                    mandate_by_name[_normalize_company_name(gc_name)] = m

        # Build Opera customer name lookup
        reader = Opera3Reader(data_path)
        sname_records = reader.read_table("sname")
        customers = {}
        for r in sname_records:
            stop = _o3_get_int(r, 'sn_stop')
            if stop:
                continue
            account = _o3_get_str(r, 'sn_account')
            name = _o3_get_str(r, 'sn_name')
            if account:
                customers[account] = name

        matched_payments = []
        unmatched_count = 0
        backfill_updates = []

        for payment in payments:
            customer_name = payment.get('customer_name', '')
            amount = payment.get('amount', 0)
            description = payment.get('description', '')
            mandate_id = payment.get('mandate_id', '')
            customer_id = payment.get('customer_id', '')

            best_match = None
            best_name = None
            match_method = None

            # Priority 1: Mandate lookup by mandate_id
            if mandate_id and mandate_id in mandate_by_id:
                m = mandate_by_id[mandate_id]
                best_match = m['opera_account']
                best_name = customers.get(best_match, m.get('opera_name', ''))
                match_method = f"mandate:{mandate_id}"

            # Priority 2: Mandate lookup by gocardless_customer_id
            if not best_match and customer_id and customer_id in mandate_by_customer:
                m = mandate_by_customer[customer_id]
                best_match = m['opera_account']
                best_name = customers.get(best_match, m.get('opera_name', ''))
                match_method = f"customer:{customer_id}"

            # Priority 3: Name-based matching against mandates
            if not best_match and customer_name and customer_name.lower() not in ('unknown', '', 'not provided'):
                norm_name = _normalize_company_name(customer_name)
                if norm_name in mandate_by_name:
                    m = mandate_by_name[norm_name]
                    best_match = m['opera_account']
                    best_name = customers.get(best_match, m.get('opera_name', ''))
                    match_method = f"name_exact:{norm_name}"
                else:
                    for stored_name, m in mandate_by_name.items():
                        if norm_name in stored_name or stored_name in norm_name:
                            best_match = m['opera_account']
                            best_name = customers.get(best_match, m.get('opera_name', ''))
                            match_method = f"name_contains:{stored_name}"
                            break

            # Priority 4: Name-based matching against Opera customer names
            if not best_match and customer_name and customer_name.lower() not in ('unknown', '', 'not provided'):
                norm_name = _normalize_company_name(customer_name)
                for acct, opera_name in customers.items():
                    norm_opera = _normalize_company_name(opera_name)
                    if norm_name == norm_opera:
                        best_match = acct
                        best_name = opera_name
                        match_method = f"opera_exact:{norm_name}"
                        break
                    if norm_name in norm_opera or norm_opera in norm_name:
                        best_match = acct
                        best_name = opera_name
                        match_method = f"opera_contains:{norm_opera}"
                        break

            if best_match and customer_id and match_method and 'name' in match_method:
                backfill_updates.append((best_match, customer_id, mandate_id))

            matched_payments.append({
                "customer_name": customer_name,
                "description": description,
                "amount": amount,
                "invoice_refs": payment.get('invoice_refs', []),
                "matched_account": best_match,
                "matched_name": best_name,
                "match_score": 1.0 if best_match else 0,
                "match_method": match_method,
                "match_status": "matched" if best_match else "unmatched",
                "possible_duplicate": False,
                "duplicate_warning": None
            })

            if not best_match:
                unmatched_count += 1

        # Backfill gocardless_customer_id on mandates matched by name
        if backfill_updates:
            try:
                import sqlite3
                conn = sqlite3.connect(str(payments_db.db_path))
                for opera_account, gc_customer_id, gc_mandate_id in backfill_updates:
                    conn.execute("""
                        UPDATE gocardless_mandates
                        SET gocardless_customer_id = ?,
                            updated_at = datetime('now')
                        WHERE opera_account = ?
                          AND (gocardless_customer_id IS NULL OR gocardless_customer_id = '')
                    """, (gc_customer_id, opera_account))
                conn.commit()
                conn.close()
            except Exception as e:
                logger.warning(f"GC match O3: backfill failed: {e}")

        return {
            "success": True,
            "payments": matched_payments,
            "unmatched_count": unmatched_count,
            "total_count": len(payments)
        }
    except Exception as e:
        logger.error(f"Error matching Opera 3 customers: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/opera3/gocardless/eligible-customers")
async def opera3_get_eligible_customers(
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """Get all GoCardless-eligible customers from Opera 3."""
    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        reader = Opera3Reader(data_path)
        sname_records = reader.read_table("sname")

        payments_db = get_payments_db()
        existing_mandates = payments_db.list_mandates()
        mandate_lookup = {
            m['opera_account'].strip(): m
            for m in existing_mandates
            if m.get('opera_account') and m['opera_account'] != '__UNLINKED__'
        }
        mandated_accounts = set(mandate_lookup.keys())

        customers = []
        seen_accounts = set()

        for r in sname_records:
            account = _o3_get_str(r, 'sn_account')
            if not account or account in seen_accounts:
                continue

            analsys = _o3_get_str(r, 'sn_analsys').upper()
            is_gc = analsys == 'GC'
            has_mandate = account in mandated_accounts

            if not is_gc and not has_mandate:
                continue

            seen_accounts.add(account)
            mandate = mandate_lookup.get(account)

            customers.append({
                'account': account,
                'name': _o3_get_str(r, 'sn_name'),
                'balance': _o3_get_num(r, 'sn_currbal'),
                'email': _o3_get_str(r, 'sn_email') or None,
                'has_mandate': has_mandate,
                'mandate_id': mandate.get('mandate_id') if mandate else None,
                'mandate_status': mandate.get('mandate_status') if mandate else None
            })

        customers.sort(key=lambda c: c['name'])
        with_mandate = sum(1 for c in customers if c['has_mandate'])

        return {
            "success": True,
            "customers": customers,
            "count": len(customers),
            "with_mandate": with_mandate,
            "without_mandate": len(customers) - with_mandate
        }
    except Exception as e:
        logger.error(f"Error getting Opera 3 GC eligible customers: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/opera3/gocardless/mandates/suggest-match")
async def opera3_suggest_mandate_match(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    gc_name: str = Query(..., description="GoCardless customer name to match")
):
    """Suggest best Opera 3 customer match for a GoCardless mandate name."""
    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        from difflib import SequenceMatcher

        reader = Opera3Reader(data_path)
        sname_records = reader.read_table("sname")

        def normalize(name):
            if not name:
                return ''
            n = name.upper().strip()
            for suffix in [' LTD', ' LIMITED', ' PLC', ' INC', ' LLC', ' CO', ' COMPANY', '.']:
                if n.endswith(suffix):
                    n = n[:-len(suffix)]
            return n.strip()

        gc_norm = normalize(gc_name)
        suggestions = []

        for r in sname_records:
            stop = _o3_get_int(r, 'sn_stop')
            if stop:
                continue
            account = _o3_get_str(r, 'sn_account')
            name = _o3_get_str(r, 'sn_name')
            if not account or not name:
                continue

            opera_norm = normalize(name)
            if gc_norm == opera_norm:
                score = 1.0
            elif gc_norm in opera_norm or opera_norm in gc_norm:
                score = 0.85
            else:
                score = SequenceMatcher(None, gc_norm, opera_norm).ratio()

            if score >= 0.5:
                suggestions.append({
                    'account': account,
                    'name': name,
                    'score': round(score, 3),
                    'is_gc': _o3_get_str(r, 'sn_analsys').upper() == 'GC'
                })

        suggestions.sort(key=lambda s: (-s['score'], not s['is_gc'], s['name']))

        return {
            "success": True,
            "suggestions": suggestions[:5],
            "gc_name": gc_name
        }
    except Exception as e:
        logger.error(f"Error suggesting Opera 3 mandate match: {e}")
        return {"success": True, "suggestions": []}


@router.post("/api/opera3/gocardless/mandates/sync")
async def opera3_sync_gocardless_mandates(
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """Sync mandates from GoCardless API, auto-linking to Opera 3 GC-eligible customers."""
    try:
        settings = _load_gocardless_settings()
        access_token = settings.get("api_access_token")
        if not access_token:
            return {"success": False, "error": "No API access token configured"}

        from sql_rag.gocardless_api import GoCardlessClient
        from sql_rag.opera3_foxpro import Opera3Reader

        sandbox = settings.get("api_sandbox", False)
        client = GoCardlessClient(access_token=access_token, sandbox=sandbox)

        payments_db = get_payments_db()
        synced_count = 0
        new_count = 0
        updated_count = 0
        auto_linked_count = 0
        cursor = None

        existing_mandates = payments_db.list_mandates(opera_account=None)
        existing_by_mandate_id = {}
        for m in existing_mandates:
            mid = m['mandate_id']
            if mid not in existing_by_mandate_id:
                existing_by_mandate_id[mid] = m
            elif m['opera_account'] != '__UNLINKED__':
                existing_by_mandate_id[mid] = m

        # Get GC-eligible customers from Opera 3
        gc_customers = {}
        reader = Opera3Reader(data_path)
        sname_records = reader.read_table("sname")
        for r in sname_records:
            analsys = _o3_get_str(r, 'sn_analsys').upper()
            if analsys == 'GC':
                account = _o3_get_str(r, 'sn_account')
                name = _o3_get_str(r, 'sn_name')
                email = _o3_get_str(r, 'sn_email') or None
                if name:
                    gc_customers[name.upper()] = {
                        'account': account,
                        'name': name,
                        'email': email
                    }

        def normalize_name(name):
            if not name:
                return ''
            n = name.upper().strip()
            for suffix in [' LTD', ' LIMITED', ' PLC', ' INC', ' LLC', ' CO', ' COMPANY']:
                if n.endswith(suffix):
                    n = n[:-len(suffix)]
            return n.strip()

        def find_opera_match(gc_name):
            if not gc_name:
                return None
            norm_gc = normalize_name(gc_name)
            for opera_name, data in gc_customers.items():
                if normalize_name(opera_name) == norm_gc:
                    return data
            for opera_name, data in gc_customers.items():
                norm_opera = normalize_name(opera_name)
                if norm_gc in norm_opera or norm_opera in norm_gc:
                    return data
            return None

        while True:
            mandates, next_cursor = client.list_mandates(status="active", cursor=cursor)

            for mandate in mandates:
                mandate_id = mandate.get("id")
                customer_id = mandate.get("links", {}).get("customer")
                scheme = mandate.get("scheme", "bacs")
                status = mandate.get("status", "active")

                gc_customer_name = None
                gc_customer_email = None
                if customer_id:
                    customer = client.get_customer(customer_id)
                    gc_customer_name = customer.get("company_name") or \
                                   f"{customer.get('given_name', '')} {customer.get('family_name', '')}".strip()
                    gc_customer_email = customer.get("email")

                existing = existing_by_mandate_id.get(mandate_id)

                if existing:
                    if existing['opera_account'] != '__UNLINKED__':
                        payments_db.link_mandate(
                            opera_account=existing['opera_account'],
                            mandate_id=mandate_id,
                            opera_name=existing.get('opera_name'),
                            gocardless_name=gc_customer_name,
                            gocardless_customer_id=customer_id,
                            mandate_status=status,
                            scheme=scheme,
                            email=gc_customer_email
                        )
                        updated_count += 1
                    else:
                        opera_match = find_opera_match(gc_customer_name)
                        if opera_match:
                            payments_db.link_mandate(
                                opera_account=opera_match['account'],
                                mandate_id=mandate_id,
                                opera_name=opera_match['name'],
                                gocardless_name=gc_customer_name,
                                gocardless_customer_id=customer_id,
                                mandate_status=status,
                                scheme=scheme,
                                email=gc_customer_email or opera_match.get('email')
                            )
                            auto_linked_count += 1
                        else:
                            updated_count += 1
                else:
                    opera_match = find_opera_match(gc_customer_name)
                    if opera_match:
                        payments_db.link_mandate(
                            opera_account=opera_match['account'],
                            mandate_id=mandate_id,
                            opera_name=opera_match['name'],
                            gocardless_name=gc_customer_name,
                            gocardless_customer_id=customer_id,
                            mandate_status=status,
                            scheme=scheme,
                            email=gc_customer_email or opera_match.get('email')
                        )
                        auto_linked_count += 1
                        new_count += 1
                    else:
                        payments_db.link_mandate(
                            opera_account='__UNLINKED__',
                            mandate_id=mandate_id,
                            opera_name=gc_customer_name,
                            gocardless_name=gc_customer_name,
                            gocardless_customer_id=customer_id,
                            mandate_status=status,
                            scheme=scheme,
                            email=gc_customer_email
                        )
                        new_count += 1

                synced_count += 1

            if not next_cursor:
                break
            cursor = next_cursor

        # Clean up duplicate unlinked entries
        all_mandates = payments_db.list_mandates(opera_account=None)
        linked_mandate_ids = {m['mandate_id'] for m in all_mandates if m['opera_account'] != '__UNLINKED__'}
        import sqlite3 as _sqlite3
        db_conn = _sqlite3.connect(payments_db.db_path)
        for m in all_mandates:
            if m['opera_account'] == '__UNLINKED__' and m['mandate_id'] in linked_mandate_ids:
                db_conn.execute("DELETE FROM gocardless_mandates WHERE id = ?", (m['id'],))
        db_conn.commit()
        db_conn.close()

        message = f"Synced {synced_count} mandates from GoCardless"
        if auto_linked_count > 0:
            message += f" ({auto_linked_count} auto-linked to Opera 3)"
        if new_count > 0:
            message += f", {new_count} new"
        if updated_count > 0:
            message += f", {updated_count} updated"

        return {
            "success": True,
            "message": message,
            "synced_count": synced_count,
            "new_count": new_count,
            "updated_count": updated_count,
            "auto_linked_count": auto_linked_count
        }
    except Exception as e:
        logger.error(f"Error syncing Opera 3 mandates: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/opera3/gocardless/mandates/link")
async def opera3_link_gocardless_mandate(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    opera_account: str = Body(..., description="Opera customer account code"),
    mandate_id: str = Body(..., description="GoCardless mandate ID"),
    opera_name: Optional[str] = Body(None, description="Customer name from Opera"),
    confirm: bool = Body(False, description="Confirm reassignment")
):
    """Link a GoCardless mandate to an Opera 3 customer, updating sn_analsys='GC' in FoxPro."""
    try:
        settings = _load_gocardless_settings()
        access_token = settings.get("api_access_token")

        mandate_status = "active"
        scheme = "bacs"
        customer_id = None
        email = None

        if access_token:
            from sql_rag.gocardless_api import GoCardlessClient
            sandbox = settings.get("api_sandbox", False)
            client = GoCardlessClient(access_token=access_token, sandbox=sandbox)

            mandate = client.get_mandate(mandate_id)
            if not mandate:
                return {"success": False, "error": f"Mandate {mandate_id} not found in GoCardless"}

            mandate_status = mandate.get("status", "active")
            scheme = mandate.get("scheme", "bacs")
            customer_id = mandate.get("links", {}).get("customer")

            if customer_id:
                customer = client.get_customer(customer_id)
                email = customer.get("email")

        payments_db = get_payments_db()

        # Check if re-linking
        old_account = None
        existing_mandates = payments_db.list_mandates()
        for m in existing_mandates:
            if m.get('mandate_id') == mandate_id:
                if m.get('opera_account') != '__UNLINKED__' and m.get('opera_account') != opera_account:
                    old_account = m['opera_account'].strip()

        if old_account and not confirm:
            return {
                "success": False,
                "needs_confirm": True,
                "error": f"This mandate is currently linked to {old_account}. Are you sure you want to reassign it to {opera_account}?"
            }

        if old_account:
            payments_db.unlink_mandate(mandate_id)

        result = payments_db.link_mandate(
            opera_account=opera_account,
            mandate_id=mandate_id,
            opera_name=opera_name,
            gocardless_customer_id=customer_id,
            mandate_status=mandate_status,
            scheme=scheme,
            email=email
        )

        # Update sn_analsys in FoxPro DBF
        gc_flag_info = {}
        try:
            import dbf
            from pathlib import Path

            dbf_path = None
            for name in ['sname.dbf', 'SNAME.DBF', 'Sname.dbf']:
                p = Path(data_path) / name
                if p.exists():
                    dbf_path = str(p)
                    break

            if dbf_path:
                table = dbf.Table(dbf_path)
                table.open(mode=dbf.READ_WRITE)
                try:
                    # Remove GC from old account if re-linking
                    if old_account:
                        for record in table:
                            if record.sn_account.strip().upper() == old_account.strip().upper():
                                if record.sn_analsys.strip().upper() == 'GC':
                                    with record:
                                        record.sn_analsys = ''
                                    gc_flag_info['gc_removed_from'] = old_account
                                break

                    # Set GC on new account
                    for record in table:
                        if record.sn_account.strip().upper() == opera_account.strip().upper():
                            current = record.sn_analsys.strip().upper()
                            if current != 'GC':
                                with record:
                                    record.sn_analsys = 'GC'
                                gc_flag_info['gc_set_on'] = opera_account
                                gc_flag_info['gc_set_rows'] = 1
                            break
                finally:
                    table.close()
            else:
                gc_flag_info['gc_error'] = f"sname.dbf not found in {data_path}"
        except Exception as dbf_err:
            logger.warning(f"Could not update sn_analsys in Opera 3: {dbf_err}")
            gc_flag_info['gc_error'] = str(dbf_err)

        return {
            "success": True,
            "message": f"Mandate {mandate_id} linked to Opera 3 customer {opera_account}",
            "mandate": result,
            "gc_flag": gc_flag_info
        }
    except Exception as e:
        logger.error(f"Error linking Opera 3 mandate: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/opera3/gocardless/collectable-invoices")
async def opera3_get_collectable_invoices(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    overdue_only: bool = Query(False, description="Only show overdue invoices"),
    min_amount: float = Query(0, description="Minimum invoice amount")
):
    """Get outstanding invoices collectible via GoCardless from Opera 3."""
    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        from datetime import date

        gc_settings = _load_gocardless_settings()
        sub_tag = gc_settings.get("subscription_tag", "SUB")

        reader = Opera3Reader(data_path)
        stran_records = reader.read_table("stran")
        sname_records = reader.read_table("sname")

        # Build customer name lookup
        customer_names = {}
        for r in sname_records:
            account = _o3_get_str(r, 'sn_account')
            customer_names[account] = _o3_get_str(r, 'sn_name')

        payments_db = get_payments_db()
        mandates = payments_db.list_mandates(status='active')
        mandate_lookup = {m['opera_account']: m for m in mandates}

        # Build subscription source doc lookup
        sub_account_docs = {}
        all_subs = payments_db.list_subscriptions()
        for s in all_subs:
            if s['source_doc'] and s['status'] != 'cancelled':
                sub_account_docs[s['opera_account']] = s['source_doc']

        # Check for subscription-tagged invoices via ihead
        sub_invoice_refs = set()
        try:
            ihead_records = reader.read_table("ihead")
            for r in ihead_records:
                docstat = _o3_get_str(r, 'ih_docstat')
                analsys = _o3_get_str(r, 'ih_analsys')
                if docstat == 'I' and analsys == sub_tag:
                    sub_invoice_refs.add(_o3_get_str(r, 'ih_invoice'))
        except Exception:
            pass

        invoices = []
        total_collectable = 0
        total_with_mandate = 0

        for r in stran_records:
            trtype = _o3_get_str(r, 'st_trtype')
            ovalue = _o3_get_num(r, 'st_ovalue') if 'ST_OVALUE' in r or 'st_ovalue' in r else _o3_get_num(r, 'st_trbal')
            st_type = _o3_get_int(r, 'st_type') if ('ST_TYPE' in r or 'st_type' in r) else (1 if trtype == 'I' else 2)

            if ovalue <= 0:
                continue
            if st_type not in (1, 2) and trtype != 'I':
                continue
            if min_amount > 0 and ovalue < min_amount:
                continue

            account = _o3_get_str(r, 'st_account')
            invoice_ref = _o3_get_str(r, 'st_trref')
            invoice_date = r.get('ST_TRDATE', r.get('st_trdate'))
            due_date = r.get('ST_DUEDAY', r.get('st_dueday'))

            # Parse due date
            due_date_obj = None
            if due_date:
                if hasattr(due_date, 'date'):
                    due_date_obj = due_date.date() if hasattr(due_date, 'date') else due_date
                elif isinstance(due_date, date):
                    due_date_obj = due_date

            days_overdue = 0
            if due_date_obj:
                days_overdue = (date.today() - due_date_obj).days

            if overdue_only and days_overdue <= 0:
                continue

            mandate = mandate_lookup.get(account)
            has_mandate = mandate is not None
            is_subscription = invoice_ref in sub_invoice_refs
            source_doc = sub_account_docs.get(account) if is_subscription else None

            invoice_data = {
                'opera_account': account,
                'customer_name': customer_names.get(account, ''),
                'invoice_ref': invoice_ref,
                'invoice_date': invoice_date.isoformat() if hasattr(invoice_date, 'isoformat') else str(invoice_date) if invoice_date else None,
                'due_date': due_date_obj.isoformat() if due_date_obj else None,
                'amount': ovalue,
                'amount_formatted': f"£{ovalue:,.2f}",
                'days_overdue': max(0, days_overdue),
                'is_overdue': days_overdue > 0,
                'has_mandate': has_mandate,
                'mandate_id': mandate['mandate_id'] if mandate else None,
                'mandate_status': mandate['mandate_status'] if mandate else None,
                'trans_type': 'Invoice' if trtype == 'I' or st_type == 1 else 'Credit Note',
                'is_subscription': is_subscription,
                'source_doc': source_doc,
            }

            invoices.append(invoice_data)
            if not is_subscription:
                total_collectable += ovalue
                if has_mandate:
                    total_with_mandate += ovalue

        return {
            "success": True,
            "invoices": invoices,
            "count": len(invoices),
            "total_collectable": total_collectable,
            "total_collectable_formatted": f"£{total_collectable:,.2f}",
            "total_with_mandate": total_with_mandate,
            "total_with_mandate_formatted": f"£{total_with_mandate:,.2f}",
            "mandates_available": len(mandate_lookup)
        }
    except Exception as e:
        logger.error(f"Error getting Opera 3 collectable invoices: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/opera3/gocardless/due-invoices")
async def opera3_get_due_invoices(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    advance_date: Optional[str] = Query(None, description="Show invoices due by this date (YYYY-MM-DD)"),
    include_future: bool = Query(True, description="Include invoices due after today but before advance_date")
):
    """Get outstanding invoices due for GoCardless collection from Opera 3."""
    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        from datetime import date, datetime

        gc_settings = _load_gocardless_settings()
        sub_tag = gc_settings.get("subscription_tag", "SUB")

        if advance_date:
            try:
                target_date = datetime.strptime(advance_date, '%Y-%m-%d').date()
            except ValueError:
                return {"success": False, "error": "Invalid date format. Use YYYY-MM-DD"}
        else:
            target_date = date.today()

        reader = Opera3Reader(data_path)
        stran_records = reader.read_table("stran")
        sname_records = reader.read_table("sname")

        payments_db = get_payments_db()
        mandates = payments_db.list_mandates(status='active')
        mandate_lookup = {m['opera_account'].strip(): m for m in mandates if m.get('opera_account')}
        mandated_accounts = {a for a in mandate_lookup.keys() if a != '__UNLINKED__'}

        if not mandated_accounts:
            return {
                "success": True, "customers": [], "invoices": [],
                "summary": {"total_customers": 0, "total_invoices": 0, "total_amount": 0,
                             "total_amount_formatted": "£0.00", "collectable_amount": 0,
                             "collectable_formatted": "£0.00"},
                "advance_date": target_date.isoformat(), "today": date.today().isoformat()
            }

        # Build customer info lookup
        customer_info = {}
        for r in sname_records:
            account = _o3_get_str(r, 'sn_account')
            customer_info[account] = {
                'name': _o3_get_str(r, 'sn_name'),
                'analsys': _o3_get_str(r, 'sn_analsys'),
                'email': _o3_get_str(r, 'sn_email') or None,
            }

        # Build subscription doc lookup
        sub_account_docs = {}
        all_subs = payments_db.list_subscriptions()
        for s in all_subs:
            if s['source_doc'] and s['status'] != 'cancelled':
                sub_account_docs[s['opera_account']] = s['source_doc']

        # Build lookup of invoices with active payment requests
        active_statuses = ('pending', 'pending_submission', 'submitted', 'confirmed')
        pending_invoice_requests: Dict[str, Dict] = {}
        try:
            all_requests = payments_db.list_payment_requests()
            for req in all_requests:
                if req.get('status') not in active_statuses:
                    continue
                refs = req.get('invoice_refs')
                if isinstance(refs, str):
                    try:
                        refs = json.loads(refs)
                    except Exception:
                        refs = []
                if refs:
                    for ref in refs:
                        pending_invoice_requests[ref.strip()] = {
                            'request_id': req.get('id'),
                            'status': req.get('status'),
                            'charge_date': req.get('charge_date'),
                            'amount_pence': req.get('amount_pence'),
                        }
        except Exception as e:
            logger.warning(f"Could not load pending payment requests: {e}")

        # Also check GoCardless API for active payments
        try:
            import re as _inv_re
            from sql_rag.gocardless_api import create_client_from_settings
            gc_client = create_client_from_settings(gc_settings)
            if gc_client:
                for gc_status in ('pending_submission', 'submitted', 'confirmed'):
                    gc_payments, _ = gc_client.list_payments(status=gc_status, limit=500)
                    for gcp in gc_payments:
                        desc = gcp.get('description', '') or ''
                        charge_date = gcp.get('charge_date', '')
                        amount_pence = gcp.get('amount', 0)
                        gc_status_val = gcp.get('status', gc_status)
                        found_refs = _inv_re.findall(r'INV\d+', desc, _inv_re.IGNORECASE)
                        for ref in found_refs:
                            ref_upper = ref.upper()
                            if ref_upper not in pending_invoice_requests:
                                pending_invoice_requests[ref_upper] = {
                                    'request_id': gcp.get('id', ''),
                                    'status': gc_status_val,
                                    'charge_date': charge_date,
                                    'amount_pence': amount_pence,
                                    'source': 'gocardless_api',
                                }
        except Exception as e:
            logger.warning(f"Could not check GoCardless API for active payments: {e}")

        # Check for subscription-tagged invoices via ihead
        sub_invoice_refs = set()
        try:
            ihead_records = reader.read_table("ihead")
            for r in ihead_records:
                docstat = _o3_get_str(r, 'ih_docstat')
                analsys = _o3_get_str(r, 'ih_analsys')
                if docstat == 'I' and analsys == sub_tag:
                    sub_invoice_refs.add(_o3_get_str(r, 'ih_invoice'))
        except Exception:
            pass

        invoices = []
        customers_data = {}
        total_amount = 0
        collectable_amount = 0

        for r in stran_records:
            account = _o3_get_str(r, 'st_account')
            if account not in mandated_accounts:
                continue

            trtype = _o3_get_str(r, 'st_trtype')
            trbal = _o3_get_num(r, 'st_trbal')
            if trtype != 'I' or trbal <= 0:
                continue

            invoice_ref = _o3_get_str(r, 'st_trref')
            invoice_date = r.get('ST_TRDATE', r.get('st_trdate'))
            due_date = r.get('ST_DUEDAY', r.get('st_dueday'))
            trvalue = _o3_get_num(r, 'st_trvalue')

            cust = customer_info.get(account, {})
            email = cust.get('email')
            customer_name = cust.get('name', '')
            customer_ref = _o3_get_str(r, 'st_cusref')
            is_subscription = invoice_ref in sub_invoice_refs
            source_doc = sub_account_docs.get(account) if is_subscription else None

            # Parse due date
            due_date_obj = None
            if due_date:
                if hasattr(due_date, 'date'):
                    due_date_obj = due_date.date()
                elif isinstance(due_date, date):
                    due_date_obj = due_date

            days_until_due = None
            is_overdue = False
            if due_date_obj:
                days_until_due = (due_date_obj - date.today()).days
                is_overdue = days_until_due < 0

            if not include_future and not is_overdue:
                continue
            if due_date_obj and due_date_obj > target_date:
                continue

            mandate = mandate_lookup.get(account)
            has_mandate = mandate is not None

            invoice_data = {
                'opera_account': account,
                'customer_name': customer_name,
                'invoice_ref': invoice_ref,
                'invoice_date': invoice_date.isoformat() if hasattr(invoice_date, 'isoformat') else str(invoice_date) if invoice_date else None,
                'due_date': due_date_obj.isoformat() if due_date_obj else None,
                'days_until_due': days_until_due,
                'is_overdue': is_overdue,
                'is_due_by_advance': due_date_obj <= target_date if due_date_obj else False,
                'amount': trbal,
                'amount_formatted': f"£{trbal:,.2f}",
                'original_amount': trvalue,
                'has_mandate': has_mandate,
                'mandate_id': mandate['mandate_id'] if mandate else None,
                'trans_type': 'Invoice',
                'trans_type_code': 'I',
                'customer_ref': customer_ref,
                'payment_requested': invoice_ref in pending_invoice_requests,
                'payment_request_info': pending_invoice_requests.get(invoice_ref),
                'is_subscription': is_subscription,
                'source_doc': source_doc,
            }

            invoices.append(invoice_data)
            total_amount += trbal
            if has_mandate and not is_subscription:
                collectable_amount += trbal

            if account not in customers_data:
                customers_data[account] = {
                    'account': account,
                    'name': customer_name,
                    'email': email,
                    'has_mandate': has_mandate,
                    'mandate_id': mandate['mandate_id'] if mandate else None,
                    'invoices': [],
                    'total_due': 0,
                    'invoice_count': 0
                }

            customers_data[account]['invoices'].append(invoice_data)
            customers_data[account]['total_due'] += trbal
            customers_data[account]['invoice_count'] += 1

        customers = []
        for account, cust in customers_data.items():
            cust['total_due_formatted'] = f"£{cust['total_due']:,.2f}"
            customers.append(cust)
        customers.sort(key=lambda x: x['name'])

        return {
            "success": True,
            "customers": customers,
            "invoices": invoices,
            "summary": {
                "total_customers": len(customers),
                "total_invoices": len(invoices),
                "total_amount": total_amount,
                "total_amount_formatted": f"£{total_amount:,.2f}",
                "collectable_amount": collectable_amount,
                "collectable_formatted": f"£{collectable_amount:,.2f}",
                "customers_with_mandate": sum(1 for c in customers if c['has_mandate']),
                "customers_without_mandate": sum(1 for c in customers if not c['has_mandate'])
            },
            "advance_date": target_date.isoformat(),
            "today": date.today().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting Opera 3 due invoices: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/opera3/gocardless/request-payment")
async def opera3_request_gocardless_payment(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    opera_account: str = Body(..., description="Opera customer account code"),
    invoices: List[str] = Body(..., description="List of invoice references"),
    amount: Optional[int] = Body(None, description="Amount in pence"),
    charge_date: Optional[str] = Body(None, description="Charge date (YYYY-MM-DD)"),
    description: Optional[str] = Body(None, description="Payment description")
):
    """Request payment from a customer via GoCardless (Opera 3 data source)."""
    try:
        payments_db = get_payments_db()
        mandate = payments_db.get_mandate_for_customer(opera_account)
        if not mandate:
            return {"success": False, "error": f"No active mandate found for customer {opera_account}."}

        # Calculate amount from Opera 3 if not provided
        if amount is None:
            from sql_rag.opera3_foxpro import Opera3Reader
            reader = Opera3Reader(data_path)
            stran_records = reader.read_table("stran")

            total = 0
            for r in stran_records:
                acct = _o3_get_str(r, 'st_account')
                ref = _o3_get_str(r, 'st_trref')
                if acct == opera_account and ref in invoices:
                    ovalue = _o3_get_num(r, 'st_ovalue') if ('ST_OVALUE' in r or 'st_ovalue' in r) else _o3_get_num(r, 'st_trbal')
                    total += ovalue

            if total <= 0:
                return {"success": False, "error": "Could not find specified invoices"}
            amount = int(round(total * 100))

        if amount <= 0:
            return {"success": False, "error": "Amount must be greater than zero"}

        settings = _load_gocardless_settings()

        # Build description — kept short for bank statement visibility (~18 chars on BACS)
        stmt_ref = (settings.get("request_statement_reference") or "").strip()[:10]
        if not description:
            if len(invoices) == 1:
                inv_part = invoices[0]
            else:
                inv_part = f"{invoices[0]} +{len(invoices) - 1}"
            if stmt_ref:
                description = f"{stmt_ref} {inv_part}"
            else:
                description = inv_part
        elif stmt_ref and not description.startswith(stmt_ref):
            description = f"{stmt_ref} {description}"

        access_token = settings.get("api_access_token")
        if not access_token:
            return {"success": False, "error": "GoCardless API not configured"}

        from sql_rag.gocardless_api import GoCardlessClient
        sandbox = settings.get("api_sandbox", False)
        client = GoCardlessClient(access_token=access_token, sandbox=sandbox)

        try:
            gc_payment = client.create_payment(
                amount_pence=amount,
                mandate_id=mandate['mandate_id'],
                description=description,
                charge_date=charge_date,
                metadata={"opera_account": opera_account, "invoices": ",".join(invoices)}
            )
        except Exception as gc_err:
            return {"success": False, "error": f"GoCardless API error: {str(gc_err)}"}

        payment_request = payments_db.create_payment_request(
            mandate_id=mandate['mandate_id'],
            opera_account=opera_account,
            amount_pence=amount,
            invoice_refs=invoices,
            payment_id=gc_payment.get("id"),
            charge_date=gc_payment.get("charge_date"),
            description=description
        )

        gc_status = gc_payment.get("status", "pending")
        if gc_status != "pending":
            payments_db.update_payment_request(payment_request['id'], status=gc_status)
            payment_request['status'] = gc_status

        estimated_arrival = None
        if gc_payment.get("charge_date"):
            from datetime import timedelta
            cd = datetime.strptime(gc_payment["charge_date"], "%Y-%m-%d").date()
            estimated_arrival = (cd + timedelta(days=5)).isoformat()

        return {
            "success": True,
            "message": f"Payment of £{amount/100:.2f} requested for customer {opera_account}",
            "payment_request": {
                **payment_request,
                "customer_name": mandate.get('opera_name', opera_account),
                "estimated_arrival": estimated_arrival
            }
        }
    except Exception as e:
        logger.error(f"Error requesting Opera 3 payment: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/opera3/gocardless/payment-requests/bulk")
async def opera3_request_bulk_payments(
    request: Request,
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
):
    """Request multiple payments at once (Opera 3 data source)."""
    body = await request.json()
    payment_requests = body.get("requests", body) if isinstance(body, dict) else body

    results = []
    success_count = 0
    fail_count = 0

    for req in payment_requests:
        try:
            result = await opera3_request_gocardless_payment(
                data_path=data_path,
                opera_account=req.get("opera_account"),
                invoices=req.get("invoices", []),
                amount=req.get("amount"),
                charge_date=req.get("charge_date"),
                description=req.get("description")
            )
            results.append({"opera_account": req.get("opera_account"), **result})
            if result.get("success"):
                success_count += 1
            else:
                fail_count += 1
        except Exception as e:
            results.append({"opera_account": req.get("opera_account"), "success": False, "error": str(e)})
            fail_count += 1

    return {
        "success": fail_count == 0,
        "results": results,
        "summary": {"total": len(payment_requests), "succeeded": success_count, "failed": fail_count}
    }


@router.get("/api/opera3/gocardless/repeat-documents")
async def opera3_get_repeat_documents(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    require_mandate: bool = Query(True, description="If false, return docs for all customers")
):
    """List Opera 3 repeat documents suitable for GoCardless subscriptions."""
    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        from datetime import date

        gc_settings = _load_gocardless_settings()
        sub_tag = gc_settings.get("subscription_tag", "SUB")

        reader = Opera3Reader(data_path)
        ihead_records = reader.read_table("ihead")
        itran_records = reader.read_table("itran")

        payments_db = get_payments_db()
        mandates = payments_db.list_mandates(status='active')
        mandate_lookup = {m['opera_account']: m for m in mandates}

        all_subs = payments_db.list_subscriptions()
        subs_by_account: Dict[str, List[Dict]] = {}
        for s in all_subs:
            if s['opera_account'] and s['status'] in ('active', 'paused'):
                subs_by_account.setdefault(s['opera_account'], []).append(s)

        # Build itran totals by doc
        itran_totals = {}
        for r in itran_records:
            doc = _o3_get_str(r, 'it_doc')
            if not doc:
                continue
            if doc not in itran_totals:
                itran_totals[doc] = {'nett': 0, 'vat': 0}
            itran_totals[doc]['nett'] += _o3_get_num(r, 'it_exvat')
            itran_totals[doc]['vat'] += _o3_get_num(r, 'it_vatval')

        documents = []

        for r in ihead_records:
            docstat = _o3_get_str(r, 'ih_docstat')
            if docstat != 'U':
                continue

            end_date = r.get('IH_ECONTR', r.get('ih_econtr'))
            if end_date and hasattr(end_date, 'date'):
                if end_date.date() < date.today():
                    continue
            elif end_date and isinstance(end_date, date) and end_date < date.today():
                continue

            doc_ref = _o3_get_str(r, 'ih_doc')
            account = _o3_get_str(r, 'ih_account')
            name = _o3_get_str(r, 'ih_name')
            freq_code = _o3_get_str(r, 'ih_ignore') or 'M'
            days_between = _o3_get_int(r, 'ih_dcontr')
            start_date = r.get('IH_SCONTR', r.get('ih_scontr'))
            dept = _o3_get_str(r, 'ih_job')
            analsys = _o3_get_str(r, 'ih_analsys')
            cust_ref = _o3_get_str(r, 'ih_custref')
            narr = _o3_get_str(r, 'ih_narr1')
            is_sub_tagged = analsys == sub_tag

            # Use itran totals (stored in pence)
            totals = itran_totals.get(doc_ref, {'nett': 0, 'vat': 0})
            line_nett_pence = totals['nett']
            line_vat_pence = totals['vat']
            ex_vat = line_nett_pence / 100.0
            vat = line_vat_pence / 100.0
            total_inc_vat = ex_vat + vat
            amount_pence = int(round(line_nett_pence + line_vat_pence))

            interval_unit, interval_count = FREQUENCY_MAP.get(freq_code, ('monthly', 1))
            freq_labels = {'W': 'Weekly', 'F': 'Fortnightly', 'M': 'Monthly', 'B': 'Bi-monthly', 'Q': 'Quarterly', 'H': 'Half-yearly', 'A': 'Annual', 'D': f'Every {days_between} days'}
            frequency = freq_labels.get(freq_code, freq_code)

            mandate = mandate_lookup.get(account)
            if require_mandate and not mandate:
                continue

            existing_sub = payments_db.get_subscription_by_source_doc(doc_ref)

            matching_sub = None
            if not existing_sub:
                account_subs = subs_by_account.get(account, [])
                for s in account_subs:
                    if s['amount_pence'] == amount_pence and not s['source_doc']:
                        matching_sub = s
                        break
                if not matching_sub:
                    for s in account_subs:
                        if abs(s['amount_pence'] - amount_pence) <= 100 and not s['source_doc']:
                            matching_sub = s
                            break

            mismatch = None
            if existing_sub:
                mismatches = []
                if existing_sub['amount_pence'] != amount_pence:
                    mismatches.append(f"Amount: subscription £{existing_sub['amount_pence']/100:,.2f} vs document £{total_inc_vat:,.2f}")
                if existing_sub['interval_unit'] != interval_unit or existing_sub.get('interval_count', 1) != interval_count:
                    sub_freq = existing_sub.get('frequency_label', existing_sub['interval_unit'])
                    mismatches.append(f"Frequency: subscription {sub_freq} vs document {frequency}")
                if mismatches:
                    mismatch = {
                        'details': mismatches,
                        'sub_amount_pence': existing_sub['amount_pence'],
                        'sub_amount_formatted': existing_sub.get('amount_formatted', f"£{existing_sub['amount_pence']/100:,.2f}"),
                        'doc_amount_pence': amount_pence,
                        'doc_amount_formatted': f"£{total_inc_vat:,.2f}",
                    }

            documents.append({
                'doc_ref': doc_ref,
                'opera_account': account,
                'customer_name': name,
                'frequency_code': freq_code,
                'frequency': frequency,
                'interval_unit': interval_unit,
                'interval_count': interval_count,
                'start_date': start_date.isoformat() if hasattr(start_date, 'isoformat') else str(start_date) if start_date else None,
                'end_date': end_date.isoformat() if hasattr(end_date, 'isoformat') else str(end_date) if end_date else None,
                'ex_vat': ex_vat,
                'vat': vat,
                'total_inc_vat': total_inc_vat,
                'amount_formatted': f"£{total_inc_vat:,.2f}",
                'amount_pence': amount_pence,
                'customer_ref': cust_ref,
                'narration': narr,
                'is_sub_tagged': is_sub_tagged,
                'department': dept,
                'has_mandate': True,
                'mandate_id': mandate['mandate_id'],
                'has_subscription': existing_sub is not None,
                'subscription_id': existing_sub['subscription_id'] if existing_sub else None,
                'subscription_status': existing_sub['status'] if existing_sub else None,
                'mismatch': mismatch,
                'matching_subscription': {
                    'subscription_id': matching_sub['subscription_id'],
                    'name': matching_sub['name'],
                    'amount_formatted': matching_sub['amount_formatted'],
                    'status': matching_sub['status'],
                } if matching_sub else None,
            })

        return {
            "success": True,
            "documents": documents,
            "count": len(documents),
            "with_mandate": sum(1 for d in documents if d['has_mandate']),
            "with_subscription": sum(1 for d in documents if d['has_subscription']),
            "with_match": sum(1 for d in documents if d['matching_subscription']),
        }
    except Exception as e:
        logger.error(f"Error getting Opera 3 repeat documents: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/opera3/gocardless/subscriptions")
async def opera3_list_subscriptions(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    status: Optional[str] = Query(None, description="Filter by status"),
    opera_account: Optional[str] = Query(None, description="Filter by Opera account")
):
    """List GoCardless subscriptions with Opera 3 enrichment."""
    try:
        from sql_rag.opera3_foxpro import Opera3Reader

        gc_settings = _load_gocardless_settings()
        sub_tag = gc_settings.get("subscription_tag", "SUB")

        payments_db = get_payments_db()
        subscriptions = payments_db.list_subscriptions(status=status, opera_account=opera_account)

        source_docs = [s['source_doc'] for s in subscriptions if s.get('source_doc')]
        opera_docs = {}
        if source_docs:
            reader = Opera3Reader(data_path)
            ihead_records = reader.read_table("ihead")
            itran_records = reader.read_table("itran")

            # Build itran totals
            itran_totals = {}
            for r in itran_records:
                doc = _o3_get_str(r, 'it_doc')
                if doc not in source_docs:
                    continue
                if doc not in itran_totals:
                    itran_totals[doc] = {'nett': 0, 'vat': 0}
                itran_totals[doc]['nett'] += _o3_get_num(r, 'it_exvat')
                itran_totals[doc]['vat'] += _o3_get_num(r, 'it_vatval')

            for r in ihead_records:
                doc = _o3_get_str(r, 'ih_doc')
                docstat = _o3_get_str(r, 'ih_docstat')
                if doc not in source_docs or docstat != 'U':
                    continue

                totals = itran_totals.get(doc, {'nett': 0, 'vat': 0})
                line_nett_pence = totals['nett']
                line_vat_pence = totals['vat']
                ex_vat = line_nett_pence / 100.0
                vat = line_vat_pence / 100.0
                total = ex_vat + vat
                freq_code = _o3_get_str(r, 'ih_ignore') or 'M'
                interval_unit, interval_count = FREQUENCY_MAP.get(freq_code, ('monthly', 1))
                freq_labels = {'W': 'Weekly', 'F': 'Fortnightly', 'M': 'Monthly', 'B': 'Bi-monthly', 'Q': 'Quarterly', 'H': 'Half-yearly', 'A': 'Annual'}
                opera_docs[doc] = {
                    'ex_vat': ex_vat, 'vat': vat, 'total_inc_vat': total,
                    'amount_pence': int(round(line_nett_pence + line_vat_pence)),
                    'amount_formatted': f"£{total:,.2f}",
                    'frequency_code': freq_code,
                    'frequency': freq_labels.get(freq_code, freq_code),
                    'interval_unit': interval_unit, 'interval_count': interval_count,
                    'has_sub_tag': _o3_get_str(r, 'ih_analsys') == sub_tag,
                }

        for sub in subscriptions:
            doc = sub.get('source_doc')
            if doc and doc in opera_docs:
                opera = opera_docs[doc]
                sub['opera_amount_pence'] = opera['amount_pence']
                sub['opera_amount_formatted'] = opera['amount_formatted']
                sub['opera_frequency'] = opera['frequency']
                sub['has_sub_tag'] = opera['has_sub_tag']
                mismatches = []
                if sub['amount_pence'] != opera['amount_pence']:
                    mismatches.append(f"Amount: GC {sub.get('amount_formatted', '?')} vs Opera {opera['amount_formatted']}")
                if sub['interval_unit'] != opera['interval_unit'] or sub.get('interval_count', 1) != opera['interval_count']:
                    mismatches.append(f"Frequency: GC {sub.get('frequency_label', sub['interval_unit'])} vs Opera {opera['frequency']}")
                sub['mismatch'] = {'details': mismatches} if mismatches else None
            else:
                sub['opera_amount_pence'] = None
                sub['opera_amount_formatted'] = None
                sub['opera_frequency'] = None
                sub['mismatch'] = None
                sub['has_sub_tag'] = None

        return {
            "success": True,
            "subscriptions": subscriptions,
            "count": len(subscriptions),
            "with_mismatch": sum(1 for s in subscriptions if s.get('mismatch')),
        }
    except Exception as e:
        logger.error(f"Error listing Opera 3 subscriptions: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/opera3/gocardless/subscriptions")
async def opera3_create_subscription(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    request: Request = None
):
    """Create a GoCardless subscription from an Opera 3 repeat document."""
    try:
        gc_settings = _load_gocardless_settings()
        sub_tag = gc_settings.get("subscription_tag", "SUB")

        body = await request.json()
        source_doc = body.get("source_doc")
        day_of_month = body.get("day_of_month")
        start_date = body.get("start_date")

        if not source_doc:
            return {"success": False, "error": "source_doc is required"}

        from sql_rag.opera3_foxpro import Opera3Reader
        reader = Opera3Reader(data_path)
        ihead_records = reader.read_table("ihead")
        itran_records = reader.read_table("itran")

        # Find the document
        doc_row = None
        for r in ihead_records:
            if _o3_get_str(r, 'ih_doc') == source_doc and _o3_get_str(r, 'ih_docstat') == 'U' and _o3_get_str(r, 'ih_analsys') == sub_tag:
                doc_row = r
                break

        if not doc_row:
            return {"success": False, "error": f"Repeat document '{source_doc}' not found or not marked as {sub_tag}"}

        account = _o3_get_str(doc_row, 'ih_account')
        name = _o3_get_str(doc_row, 'ih_name')
        freq_code = _o3_get_str(doc_row, 'ih_ignore') or 'M'
        cust_ref = _o3_get_str(doc_row, 'ih_custref')

        # Calculate amount from itran
        total_pence = 0
        for r in itran_records:
            if _o3_get_str(r, 'it_doc') == source_doc:
                total_pence += _o3_get_num(r, 'it_exvat') + _o3_get_num(r, 'it_vatval')

        amount_pence = int(round(total_pence))
        if amount_pence <= 0:
            ex_vat = _o3_get_num(doc_row, 'ih_exvat')
            vat = _o3_get_num(doc_row, 'ih_vat')
            amount_pence = int(round((ex_vat + vat) * 100))

        if amount_pence <= 0:
            return {"success": False, "error": f"Invalid amount: £{amount_pence/100:.2f}"}

        interval_unit, interval_count = FREQUENCY_MAP.get(freq_code, ('monthly', 1))

        payments_db = get_payments_db()
        mandate = payments_db.get_mandate_for_customer(account)
        if not mandate:
            return {"success": False, "error": f"No active GoCardless mandate for customer {account} ({name})"}

        existing = payments_db.get_subscription_by_source_doc(source_doc)
        if existing:
            return {"success": False, "error": f"Subscription already exists for {source_doc} (status: {existing['status']})"}

        settings = _load_gocardless_settings()
        access_token = settings.get("api_access_token")
        if not access_token:
            return {"success": False, "error": "GoCardless API not configured"}

        from sql_rag.gocardless_api import GoCardlessClient
        sandbox = settings.get("api_sandbox", False)
        client = GoCardlessClient(access_token=access_token, sandbox=sandbox)

        sub_name = f"{name} - {cust_ref}" if cust_ref else name
        metadata = {"opera_account": account, "source_doc": source_doc}

        gc_sub = client.create_subscription(
            mandate_id=mandate['mandate_id'],
            amount_pence=amount_pence,
            interval_unit=interval_unit,
            interval=interval_count,
            day_of_month=day_of_month,
            name=sub_name,
            start_date=start_date,
            metadata=metadata,
        )

        sub_record = payments_db.save_subscription(
            subscription_id=gc_sub.get("id", ""),
            mandate_id=mandate['mandate_id'],
            amount_pence=amount_pence,
            interval_unit=interval_unit,
            interval_count=interval_count,
            opera_account=account,
            opera_name=name,
            source_doc=source_doc,
            day_of_month=gc_sub.get("day_of_month"),
            name=sub_name,
            status=gc_sub.get("status", "active"),
            start_date=gc_sub.get("start_date"),
            end_date=gc_sub.get("end_date"),
        )

        return {"success": True, "subscription": sub_record, "gc_response": gc_sub}
    except Exception as e:
        logger.error(f"Error creating Opera 3 subscription: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/opera3/gocardless/subscriptions/{subscription_id}/sync-from-opera")
async def opera3_sync_subscription_from_opera(
    subscription_id: str,
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """Update a GoCardless subscription amount from its linked Opera 3 repeat document."""
    try:
        payments_db = get_payments_db()
        sub = payments_db.get_subscription(subscription_id)
        if not sub:
            return {"success": False, "error": f"Subscription {subscription_id} not found"}

        source_doc = sub.get('source_doc')
        if not source_doc:
            return {"success": False, "error": "Subscription is not linked to an Opera document"}

        from sql_rag.opera3_foxpro import Opera3Reader
        reader = Opera3Reader(data_path)
        itran_records = reader.read_table("itran")

        total_pence = 0
        for r in itran_records:
            if _o3_get_str(r, 'it_doc') == source_doc:
                total_pence += _o3_get_num(r, 'it_exvat') + _o3_get_num(r, 'it_vatval')

        new_amount_pence = int(round(total_pence))
        if new_amount_pence == 0:
            return {"success": False, "error": f"Opera document {source_doc} not found or has no lines"}

        old_amount_pence = sub['amount_pence']
        if new_amount_pence == old_amount_pence:
            return {"success": True, "message": "No change needed — amounts already match"}

        settings = _load_gocardless_settings()
        access_token = settings.get("api_access_token")
        if not access_token:
            return {"success": False, "error": "GoCardless API not configured"}

        from sql_rag.gocardless_api import GoCardlessClient
        sandbox = settings.get("api_sandbox", False)
        client = GoCardlessClient(access_token=access_token, sandbox=sandbox)

        gc_sub = client.update_subscription(subscription_id, amount_pence=new_amount_pence)

        payments_db.save_subscription(
            subscription_id=subscription_id,
            mandate_id=sub['mandate_id'],
            amount_pence=new_amount_pence,
            interval_unit=sub['interval_unit'],
            interval_count=sub['interval_count'],
        )

        return {
            "success": True,
            "old_amount_pence": old_amount_pence,
            "new_amount_pence": new_amount_pence,
            "old_amount_formatted": f"£{old_amount_pence/100:,.2f}",
            "new_amount_formatted": f"£{new_amount_pence/100:,.2f}",
            "subscription": payments_db.get_subscription(subscription_id),
        }
    except Exception as e:
        logger.error(f"Error syncing Opera 3 subscription from Opera: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/opera3/gocardless/scan-emails")
async def opera3_scan_gocardless_emails(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    from_date: Optional[str] = Query(None, description="Start date filter (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="End date filter (YYYY-MM-DD)"),
    include_processed: bool = Query(False, description="Include previously processed emails"),
    company_reference: Optional[str] = Query(None, description="Override company reference filter")
):
    """Scan mailbox for GoCardless payment notification emails (Opera 3 duplicate checking)."""
    if not email_storage:
        return {"success": False, "error": "Email storage not configured"}

    try:
        from datetime import datetime
        from sql_rag.gocardless_parser import parse_gocardless_email
        from sql_rag.opera3_foxpro import Opera3Reader

        # Auto-sync email inbox
        if email_sync_manager:
            try:
                if email_sync_manager.storage is not email_storage:
                    email_sync_manager.storage = email_storage
                import asyncio
                await asyncio.wait_for(email_sync_manager.sync_all_providers(), timeout=30.0)
            except Exception:
                pass

        settings = _load_gocardless_settings()
        company_ref = company_reference or settings.get('company_reference', '')

        date_from = datetime.strptime(from_date, '%Y-%m-%d') if from_date else None
        date_to = datetime.strptime(to_date, '%Y-%m-%d') if to_date else None

        imported_email_ids = set()
        imported_references = set()
        if not include_processed:
            imported_email_ids = set(email_storage.get_imported_gocardless_email_ids())
            imported_references = email_storage.get_imported_gocardless_references()

        result = email_storage.get_emails(search="gocardless", from_date=date_from, to_date=date_to, page_size=100)
        emails = result.get('emails', []) or result.get('items', [])

        if not emails:
            return {"success": True, "message": "No GoCardless emails found", "batches": [], "total_emails": 0, "company_reference": company_ref}

        # Period validation for Opera 3
        from sql_rag.opera3_config import validate_posting_period as o3_validate_posting_period

        # Read nbank for duplicate checking (limited)
        reader = Opera3Reader(data_path)

        batches = []
        processed_count = 0
        error_count = 0
        skipped_wrong_company = 0
        skipped_already_imported = 0
        skipped_duplicates = 0

        for email in emails:
            try:
                email_id = email.get('id')
                if email_id in imported_email_ids:
                    skipped_already_imported += 1
                    continue

                content = email.get('body_text') or email.get('body_html') or ''
                if not content:
                    continue

                subject = email.get('subject', '').lower()
                if not any(keyword in subject for keyword in ['payout', 'payment', 'collected', 'paid']):
                    continue

                batch = parse_gocardless_email(content)

                if company_ref:
                    batch_ref = (batch.bank_reference or '').upper()
                    if company_ref.upper() not in batch_ref and batch_ref not in company_ref.upper():
                        if company_ref.upper() not in content.upper():
                            skipped_wrong_company += 1
                            continue

                if batch.bank_reference and batch.bank_reference in imported_references:
                    skipped_already_imported += 1
                    continue

                # Foreign currency check
                is_foreign_currency = False
                home_currency_code = 'GBP'
                if batch.currency and batch.currency.upper() != 'GBP':
                    is_foreign_currency = True

                if batch.payments:
                    payment_date_str = batch.payment_date.strftime('%Y-%m-%d') if batch.payment_date else None

                    # For Opera 3, duplicate checking is simplified (no SQL queries)
                    possible_duplicate = False
                    duplicate_warning = None
                    bank_tx_warning = None
                    ref_warning = None

                    if batch.bank_reference and batch.bank_reference in imported_references:
                        possible_duplicate = True
                        ref_warning = f"Already imported: ref '{batch.bank_reference}'"

                    # Period validation
                    period_valid = True
                    period_error = None
                    if batch.payment_date:
                        try:
                            period_result = o3_validate_posting_period(data_path, batch.payment_date.date(), 'SL')
                            period_valid = period_result.is_valid
                            if not period_valid:
                                period_error = period_result.error_message
                        except Exception:
                            pass

                    batch_data = {
                        "email_id": email.get('id'),
                        "email_subject": email.get('subject'),
                        "email_date": email.get('received_at'),
                        "email_from": email.get('from_address'),
                        "possible_duplicate": possible_duplicate,
                        "duplicate_warning": duplicate_warning,
                        "bank_tx_warning": bank_tx_warning,
                        "ref_warning": ref_warning,
                        "period_valid": period_valid,
                        "period_error": period_error,
                        "is_foreign_currency": is_foreign_currency,
                        "home_currency": home_currency_code,
                        "batch": {
                            "gross_amount": batch.gross_amount,
                            "gocardless_fees": batch.gocardless_fees,
                            "vat_on_fees": batch.vat_on_fees,
                            "net_amount": batch.net_amount,
                            "bank_reference": batch.bank_reference,
                            "currency": batch.currency,
                            "payment_date": payment_date_str,
                            "payment_count": len(batch.payments),
                            "payments": [
                                {
                                    "customer_name": p.customer_name,
                                    "description": p.description,
                                    "amount": p.amount,
                                    "invoice_refs": p.invoice_refs
                                }
                                for p in batch.payments
                            ]
                        }
                    }
                    batches.append(batch_data)
                    if possible_duplicate:
                        skipped_duplicates += 1
                    processed_count += 1

            except Exception as e:
                logger.warning(f"Error parsing email {email.get('id')}: {e}")
                error_count += 1
                continue

        # Get current period info
        current_period = {}
        try:
            from sql_rag.opera3_config import get_current_period_info as o3_get_current_period_info
            current_period = o3_get_current_period_info(data_path)
        except Exception:
            pass

        return {
            "success": True,
            "total_emails": len(emails),
            "parsed_count": processed_count,
            "error_count": error_count,
            "skipped_wrong_company": skipped_wrong_company,
            "skipped_already_imported": skipped_already_imported,
            "skipped_duplicates": skipped_duplicates,
            "company_reference": company_ref,
            "current_period": {
                "year": current_period.get('np_year'),
                "period": current_period.get('np_perno')
            },
            "batches": batches
        }
    except Exception as e:
        logger.error(f"Error scanning Opera 3 GoCardless emails: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/opera3/gocardless/import-from-email")
async def opera3_import_gocardless_from_email(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    email_id: int = Query(..., description="Email ID to import from"),
    bank_code: str = Query(..., description="Opera bank account code"),
    post_date: str = Query(..., description="Posting date (YYYY-MM-DD)"),
    reference: str = Query("GoCardless", description="Batch reference"),
    complete_batch: bool = Query(False, description="Complete batch immediately"),
    cbtype: str = Query(None, description="Cashbook type code"),
    gocardless_fees: float = Query(0.0, description="GoCardless fees amount"),
    vat_on_fees: float = Query(0.0, description="VAT element of fees"),
    fees_nominal_account: str = Query(None, description="Nominal account for net fees"),
    fees_vat_code: str = Query("2", description="VAT code for fees"),
    fees_payment_type: str = Query(None, description="Cashbook type code for fees entry"),
    currency: str = Query(None, description="Currency code"),
    archive_folder: str = Query("Archive/GoCardless", description="Folder to move email after import"),
    dest_bank_account: str = Query(None, description="Payout destination bank account number (from GoCardless)"),
    dest_bank_sort_code: str = Query(None, description="Payout destination bank sort code (from GoCardless)"),
    payments: List[Dict[str, Any]] = Body(..., description="List of payments with matched customer accounts")
):
    """Import GoCardless batch from email into Opera 3."""
    try:
        from sql_rag.opera3_write_provider import get_opera3_writer, Opera3AgentRequired
        from datetime import datetime

        try:
            parsed_date = datetime.strptime(post_date, '%Y-%m-%d').date()
        except ValueError:
            return {"success": False, "error": f"Invalid date format: {post_date}. Use YYYY-MM-DD"}

        # Validate posting period
        from sql_rag.opera3_config import validate_posting_period as o3_validate_posting_period
        period_result = o3_validate_posting_period(data_path, parsed_date, 'SL')
        if not period_result.is_valid:
            return {"success": False, "error": f"Cannot post to this date: {period_result.error_message}"}

        if not payments:
            return {"success": False, "error": "No payments provided"}

        validated_payments = []
        for idx, p in enumerate(payments):
            if not p.get('customer_account'):
                return {"success": False, "error": f"Payment {idx+1}: Missing customer_account"}
            if not p.get('amount'):
                return {"success": False, "error": f"Payment {idx+1}: Missing amount"}
            validated_payments.append({
                "customer_account": p['customer_account'],
                "customer_name": p.get('customer_name', ''),
                "opera_customer_name": p.get('opera_customer_name', ''),
                "amount": float(p['amount']),
                "description": p.get('description', '')[:35],
                "auto_allocate": p.get('auto_allocate', True),
                "gc_payment_id": p.get('gc_payment_id', '')
            })

        if gocardless_fees and gocardless_fees > 0 and not fees_nominal_account:
            return {
                "success": False,
                "error": f"GoCardless fees of £{gocardless_fees:.2f} cannot be posted: Fees Nominal Account not configured."
            }

        # Resolve GC control bank and destination bank
        settings = _load_gocardless_settings()
        gc_bank = settings.get("gocardless_bank_code", "")
        transfer_cbtype = settings.get("gocardless_transfer_cbtype", "")

        # If payout has bank details, resolve destination bank from Opera 3 nbank by sort/account
        from sql_rag.opera3_foxpro import Opera3Reader
        reader = Opera3Reader(data_path)
        nbank_records = reader.read_table("nbank")

        resolved_dest_bank = bank_code
        if dest_bank_sort_code or dest_bank_account:
            norm_sort = (dest_bank_sort_code or "").replace(" ", "").replace("-", "").strip()
            norm_acct = (dest_bank_account or "").replace(" ", "").strip()
            for rec in nbank_records:
                db_sort = (_o3_get_str(rec, 'nk_sort') or '').replace(" ", "").replace("-", "").strip()
                db_acct = (_o3_get_str(rec, 'nk_number') or '').replace(" ", "").strip()
                sort_match = norm_sort and db_sort and norm_sort == db_sort
                acct_match = norm_acct and db_acct and (db_acct.endswith(norm_acct) or norm_acct.endswith(db_acct) or db_acct == norm_acct)
                if sort_match and acct_match:
                    resolved_dest_bank = _o3_get_str(rec, 'nk_account').strip()
                    logger.info(f"O3 GC email import: resolved destination bank {resolved_dest_bank} from sort={norm_sort} acct={norm_acct}")
                    break
                elif sort_match and not norm_acct:
                    resolved_dest_bank = _o3_get_str(rec, 'nk_account').strip()
                    logger.info(f"O3 GC email import: resolved destination bank {resolved_dest_bank} from sort={norm_sort} (no account number)")
                    break

        destination_bank = None
        if gc_bank and gc_bank.strip() and resolved_dest_bank.strip() != gc_bank.strip():
            destination_bank = resolved_dest_bank
        posting_bank = gc_bank.strip() if gc_bank and gc_bank.strip() else resolved_dest_bank

        # Validate all bank accounts exist in Opera 3 nbank
        valid_banks = {_o3_get_str(r, 'nk_account') for r in nbank_records}
        for check_bank in ([posting_bank] + ([destination_bank] if destination_bank else [])):
            if check_bank.strip() not in valid_banks:
                label = "GC Control bank" if check_bank == posting_bank else "Destination bank"
                return {
                    "success": False,
                    "error": f"{label} '{check_bank}' does not exist in this company's bank accounts. "
                             "Please update GoCardless Settings with valid bank codes for this company."
                }

        # Acquire bank-level lock
        from sql_rag.import_lock import acquire_import_lock, release_import_lock
        lock_key = f"opera3_{posting_bank}"
        if not acquire_import_lock(lock_key, locked_by="api", endpoint="opera3-gocardless-import-from-email"):
            return {"success": False, "error": f"Bank account {posting_bank} is currently being imported by another user."}

        try:
            try:
                importer = get_opera3_writer(data_path)
            except Opera3AgentRequired as e:
                release_import_lock(lock_key)
                return {"success": False, "error": str(e)}
            result = importer.import_gocardless_batch(
                bank_account=posting_bank,
                payments=validated_payments,
                post_date=parsed_date,
                reference=reference,
                gocardless_fees=gocardless_fees,
                vat_on_fees=vat_on_fees,
                fees_nominal_account=fees_nominal_account,
                fees_vat_code=fees_vat_code,
                complete_batch=complete_batch,
                cbtype=cbtype,
                input_by="GOCARDLS",
                currency=currency,
                auto_allocate=True,
                destination_bank=destination_bank,
                transfer_cbtype=transfer_cbtype or None
            )

            if result.success:
                # Record import
                try:
                    gross_amount = sum(p.get('amount', 0) for p in payments)
                    net_amount = gross_amount - gocardless_fees
                    history_payments = json.dumps([{
                        "customer_account": p['customer_account'],
                        "gc_customer_name": p.get('customer_name', ''),
                        "opera_customer_name": p.get('opera_customer_name', ''),
                        "amount": p['amount'],
                        "description": p.get('description', '')
                    } for p in validated_payments])
                    email_storage.record_gocardless_import(
                        email_id=email_id,
                        target_system='opera3',
                        bank_reference=reference,
                        gross_amount=gross_amount,
                        net_amount=net_amount,
                        payment_count=len(payments),
                        payments_json=history_payments,
                        batch_ref=result.batch_number,
                        imported_by="GOCARDLS",
                        post_date=post_date
                    )
                except Exception as track_err:
                    logger.warning(f"Failed to record import tracking: {track_err}")

                # Archive email
                archive_status = "not_attempted"
                if archive_folder and email_storage:
                    try:
                        email_details = email_storage.get_email_by_id(email_id)
                        if email_details:
                            provider_id = email_details.get('provider_id')
                            message_id = email_details.get('message_id')
                            source_folder = email_details.get('folder_id', 'INBOX')
                            if provider_id and message_id and provider_id in email_sync_manager.providers:
                                provider = email_sync_manager.providers[provider_id]
                                move_success = await provider.move_email(
                                    message_id=message_id,
                                    source_folder=source_folder,
                                    dest_folder=archive_folder
                                )
                                archive_status = "archived" if move_success else "move_failed"
                    except Exception:
                        archive_status = "error"

                return {
                    "success": True,
                    "message": f"Successfully imported {len(payments)} payments from email into Opera 3",
                    "email_id": email_id,
                    "payments_imported": result.records_imported,
                    "complete": complete_batch,
                    "archive_status": archive_status
                }
            else:
                return {
                    "success": False,
                    "error": "; ".join(result.errors),
                    "payments_processed": result.records_processed
                }
        finally:
            release_import_lock(lock_key)

    except Exception as e:
        logger.error(f"Error importing Opera 3 GoCardless from email: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/gocardless/payment-requests/stats")
async def get_gocardless_payment_stats():
    """Get GoCardless payment request statistics for dashboard."""
    try:
        payments_db = get_payments_db()
        stats = payments_db.get_statistics()
        return {"success": True, **stats}
    except Exception as e:
        logger.error(f"Error getting payment stats: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/gocardless/mandates")
async def list_gocardless_mandates(
    status: Optional[str] = Query(None, description="Filter by status (active, cancelled, etc)"),
    opera_account: Optional[str] = Query(None, description="Filter by Opera account code")
):
    """List all GoCardless mandates linked to Opera customers."""
    try:
        payments_db = get_payments_db()
        mandates = payments_db.list_mandates(status=status, opera_account=opera_account)
        # Filter out __UNLINKED__ entries where a linked version exists for the same mandate
        linked_mandate_ids = {m['mandate_id'] for m in mandates if m.get('opera_account') != '__UNLINKED__'}
        mandates = [m for m in mandates if m.get('opera_account') != '__UNLINKED__' or m['mandate_id'] not in linked_mandate_ids]
        # Sort by customer name alphabetically
        mandates = sorted(mandates, key=lambda m: (m.get('opera_name') or '').lower())
        return {
            "success": True,
            "mandates": mandates,
            "count": len(mandates)
        }
    except Exception as e:
        logger.error(f"Error listing mandates: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/gocardless/mandates/unlinked")
async def list_unlinked_gocardless_mandates():
    """
    List GoCardless mandates that have been synced but not yet linked to Opera customers.
    These have opera_account = '__UNLINKED__' and need manual linking.
    """
    try:
        payments_db = get_payments_db()
        all_mandates = payments_db.list_mandates(opera_account=None)
        unlinked = [m for m in all_mandates if m.get('opera_account') == '__UNLINKED__']
        # Sort by customer name alphabetically
        unlinked = sorted(unlinked, key=lambda m: (m.get('opera_name') or '').lower())
        return {
            "success": True,
            "mandates": unlinked,
            "count": len(unlinked)
        }
    except Exception as e:
        logger.error(f"Error listing unlinked mandates: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/gocardless/mandates/sync")
async def sync_gocardless_mandates():
    """
    Sync mandates from GoCardless API.
    Fetches all active mandates, auto-links to Opera GC-eligible customers by name match,
    or stores with __UNLINKED__ placeholder for manual linking.
    """
    try:
        settings = _load_gocardless_settings()
        access_token = settings.get("api_access_token")
        if not access_token:
            return {"success": False, "error": "No API access token configured"}

        from sql_rag.gocardless_api import GoCardlessClient
        sandbox = settings.get("api_sandbox", False)
        client = GoCardlessClient(access_token=access_token, sandbox=sandbox)

        payments_db = get_payments_db()
        synced_count = 0
        new_count = 0
        updated_count = 0
        auto_linked_count = 0
        cursor = None

        # Get all existing mandates to check for matches
        # If a mandate_id has both a linked and unlinked entry, prefer the linked one
        existing_mandates = payments_db.list_mandates(opera_account=None)
        existing_by_mandate_id = {}
        for m in existing_mandates:
            mid = m['mandate_id']
            if mid not in existing_by_mandate_id:
                existing_by_mandate_id[mid] = m
            elif m['opera_account'] != '__UNLINKED__':
                # Linked entry takes priority over unlinked
                existing_by_mandate_id[mid] = m

        # Get all GC-eligible Opera customers for auto-matching
        gc_customers = {}
        if sql_connector:
            gc_query = """
                SELECT sn_account, sn_name, sn_email
                FROM sname
                WHERE LTRIM(RTRIM(UPPER(sn_analsys))) = 'GC'
            """
            gc_result = sql_connector.execute_query(gc_query)
            if gc_result is not None and not gc_result.empty:
                for _, row in gc_result.iterrows():
                    account = row['sn_account'].strip() if row['sn_account'] else ''
                    name = row['sn_name'].strip().upper() if row['sn_name'] else ''
                    if name:
                        gc_customers[name] = {
                            'account': account,
                            'name': row['sn_name'].strip() if row['sn_name'] else '',
                            'email': row['sn_email'].strip() if row.get('sn_email') else None
                        }

        def normalize_name(name):
            """Normalize company name for matching"""
            if not name:
                return ''
            n = name.upper().strip()
            # Remove common suffixes
            for suffix in [' LTD', ' LIMITED', ' PLC', ' INC', ' LLC', ' CO', ' COMPANY']:
                if n.endswith(suffix):
                    n = n[:-len(suffix)]
            return n.strip()

        def find_opera_match(gc_name):
            """Try to find matching Opera GC customer by name"""
            if not gc_name:
                return None
            norm_gc = normalize_name(gc_name)
            # Exact match after normalization
            for opera_name, data in gc_customers.items():
                if normalize_name(opera_name) == norm_gc:
                    return data
            # Partial match (GC name contains Opera name or vice versa)
            for opera_name, data in gc_customers.items():
                norm_opera = normalize_name(opera_name)
                if norm_gc in norm_opera or norm_opera in norm_gc:
                    return data
            return None

        while True:
            mandates, next_cursor = client.list_mandates(status="active", cursor=cursor)

            for mandate in mandates:
                mandate_id = mandate.get("id")
                customer_id = mandate.get("links", {}).get("customer")
                scheme = mandate.get("scheme", "bacs")
                status = mandate.get("status", "active")

                # Get customer details from GoCardless
                gc_customer_name = None
                gc_customer_email = None
                if customer_id:
                    customer = client.get_customer(customer_id)
                    gc_customer_name = customer.get("company_name") or \
                                   f"{customer.get('given_name', '')} {customer.get('family_name', '')}".strip()
                    gc_customer_email = customer.get("email")

                existing = existing_by_mandate_id.get(mandate_id)

                if existing:
                    # Update existing mandate with latest details from GoCardless
                    if existing['opera_account'] != '__UNLINKED__':
                        # Already linked - just update status/scheme
                        payments_db.link_mandate(
                            opera_account=existing['opera_account'],
                            mandate_id=mandate_id,
                            opera_name=existing.get('opera_name'),
                            gocardless_name=gc_customer_name,
                            gocardless_customer_id=customer_id,
                            mandate_status=status,
                            scheme=scheme,
                            email=gc_customer_email
                        )
                        updated_count += 1
                    else:
                        # Existing but unlinked - try to auto-match now
                        opera_match = find_opera_match(gc_customer_name)
                        if opera_match:
                            payments_db.link_mandate(
                                opera_account=opera_match['account'],
                                mandate_id=mandate_id,
                                opera_name=opera_match['name'],
                                gocardless_name=gc_customer_name,
                                gocardless_customer_id=customer_id,
                                mandate_status=status,
                                scheme=scheme,
                                email=gc_customer_email or opera_match.get('email')
                            )
                            auto_linked_count += 1
                            logger.info(f"Auto-linked mandate {mandate_id} to Opera customer {opera_match['account']} ({opera_match['name']})")
                        else:
                            updated_count += 1
                else:
                    # New mandate - try to auto-match to Opera GC customer
                    opera_match = find_opera_match(gc_customer_name)
                    if opera_match:
                        payments_db.link_mandate(
                            opera_account=opera_match['account'],
                            mandate_id=mandate_id,
                            opera_name=opera_match['name'],
                            gocardless_name=gc_customer_name,
                            gocardless_customer_id=customer_id,
                            mandate_status=status,
                            scheme=scheme,
                            email=gc_customer_email or opera_match.get('email')
                        )
                        auto_linked_count += 1
                        new_count += 1
                        logger.info(f"Auto-linked new mandate {mandate_id} to Opera customer {opera_match['account']} ({opera_match['name']})")
                    else:
                        # Store with placeholder for manual linking
                        payments_db.link_mandate(
                            opera_account='__UNLINKED__',
                            mandate_id=mandate_id,
                            opera_name=gc_customer_name,  # Store GC customer name for reference
                            gocardless_name=gc_customer_name,
                            gocardless_customer_id=customer_id,
                            mandate_status=status,
                            scheme=scheme,
                            email=gc_customer_email
                        )
                        new_count += 1
                        logger.info(f"Stored unlinked mandate {mandate_id} for customer {gc_customer_name}")

                synced_count += 1

            if not next_cursor:
                break
            cursor = next_cursor

        # Clean up: remove __UNLINKED__ duplicates where a linked entry exists for the same mandate
        all_mandates = payments_db.list_mandates(opera_account=None)
        linked_mandate_ids = {m['mandate_id'] for m in all_mandates if m['opera_account'] != '__UNLINKED__'}
        import sqlite3 as _sqlite3
        db_conn = _sqlite3.connect(payments_db.db_path)
        for m in all_mandates:
            if m['opera_account'] == '__UNLINKED__' and m['mandate_id'] in linked_mandate_ids:
                db_conn.execute("DELETE FROM gocardless_mandates WHERE id = ?", (m['id'],))
                logger.info(f"Cleaned up duplicate unlinked entry for mandate {m['mandate_id']}")
        db_conn.commit()
        db_conn.close()

        message = f"Synced {synced_count} mandates from GoCardless"
        if auto_linked_count > 0:
            message += f" ({auto_linked_count} auto-linked to Opera)"
        if new_count > 0:
            message += f", {new_count} new"
        if updated_count > 0:
            message += f", {updated_count} updated"

        return {
            "success": True,
            "message": message,
            "synced_count": synced_count,
            "new_count": new_count,
            "updated_count": updated_count,
            "auto_linked_count": auto_linked_count
        }
    except Exception as e:
        logger.error(f"Error syncing mandates: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/gocardless/mandates/link")
async def link_gocardless_mandate(
    opera_account: str = Body(..., description="Opera customer account code"),
    mandate_id: str = Body(..., description="GoCardless mandate ID (MD000XXX)"),
    opera_name: Optional[str] = Body(None, description="Customer name from Opera"),
    confirm: bool = Body(False, description="Confirm reassignment")
):
    """
    Link a GoCardless mandate to an Opera customer.
    This enables payment collection for that customer.
    """
    try:
        # Verify mandate exists in GoCardless
        settings = _load_gocardless_settings()
        access_token = settings.get("api_access_token")

        # Try to get latest mandate details from GoCardless API, but fall back to local data if unavailable
        mandate_status = "active"
        scheme = "bacs"
        customer_id = None
        email = None

        if access_token:
            try:
                from sql_rag.gocardless_api import GoCardlessClient
                sandbox = settings.get("api_sandbox", False)
                client = GoCardlessClient(access_token=access_token, sandbox=sandbox)

                mandate = client.get_mandate(mandate_id)
                if mandate:
                    mandate_status = mandate.get("status", "active")
                    scheme = mandate.get("scheme", "bacs")
                    customer_id = mandate.get("links", {}).get("customer")

                    if customer_id:
                        customer = client.get_customer(customer_id)
                        email = customer.get("email")
                else:
                    logger.warning(f"Mandate {mandate_id} not found in GoCardless API — using local data")
            except Exception as e:
                logger.warning(f"Could not verify mandate with GoCardless API: {e} — using local data")

        payments_db = get_payments_db()

        # Check if this mandate was previously linked to a different account
        old_account = None
        gc_mandate_name = None
        existing_mandates = payments_db.list_mandates()
        for m in existing_mandates:
            if m.get('mandate_id') == mandate_id:
                gc_mandate_name = m.get('opera_name', '').strip()
                logger.info(f"Found existing mandate {mandate_id}: opera_account='{m.get('opera_account')}', new opera_account='{opera_account}', confirm={confirm}")
                if m.get('opera_account') != '__UNLINKED__' and m.get('opera_account') != opera_account:
                    old_account = m['opera_account'].strip()
                    logger.info(f"Re-link detected: old_account='{old_account}' -> new account='{opera_account}'")

        # Require confirmation when re-linking to a different account
        if old_account and not confirm:
            return {
                "success": False,
                "needs_confirm": True,
                "error": f"This mandate is currently linked to {old_account}. Are you sure you want to reassign it to {opera_account}?"
            }

        # Remove old link if re-linking
        if old_account:
            payments_db.unlink_mandate(mandate_id)

        mandate_result = payments_db.link_mandate(
            opera_account=opera_account,
            mandate_id=mandate_id,
            opera_name=opera_name,
            gocardless_customer_id=customer_id,
            mandate_status=mandate_status,
            scheme=scheme,
            email=email
        )

        # Move sn_analsys GC flag: remove from old account, set on new account
        gc_flag_info = {}
        if sql_connector:
            try:
                from sqlalchemy import text
                with sql_connector.engine.connect() as conn:
                    # Remove GC from old account if re-linking
                    if old_account:
                        sql_result = conn.execute(text("""
                            UPDATE sname WITH (ROWLOCK)
                            SET sn_analsys = ''
                            WHERE LTRIM(RTRIM(sn_account)) = :account
                            AND LTRIM(RTRIM(UPPER(sn_analsys))) = 'GC'
                        """), {"account": old_account.strip()})
                        rows_removed = sql_result.rowcount
                        logger.info(f"Removed sn_analsys='GC' from old account '{old_account}' — {rows_removed} row(s) updated")
                        gc_flag_info['gc_removed_from'] = old_account
                        gc_flag_info['gc_removed_rows'] = rows_removed
                        if rows_removed == 0:
                            # Check what the current value is
                            check = conn.execute(text("""
                                SELECT RTRIM(sn_account) as acct, sn_analsys
                                FROM sname
                                WHERE LTRIM(RTRIM(sn_account)) = :account
                            """), {"account": old_account.strip()})
                            row = check.fetchone()
                            if row:
                                logger.warning(f"Account {old_account} exists but sn_analsys='{row[1]}' (not 'GC')")
                                gc_flag_info['old_account_current_analsys'] = str(row[1]).strip() if row[1] else ''
                            else:
                                logger.warning(f"Account {old_account} not found in sname")
                                gc_flag_info['old_account_found'] = False
                    # Set GC on new account
                    sql_result = conn.execute(text("""
                        UPDATE sname WITH (ROWLOCK)
                        SET sn_analsys = 'GC'
                        WHERE LTRIM(RTRIM(sn_account)) = :account
                        AND (sn_analsys IS NULL OR LTRIM(RTRIM(sn_analsys)) = ''
                             OR LTRIM(RTRIM(UPPER(sn_analsys))) != 'GC')
                    """), {"account": opera_account.strip()})
                    rows_set = sql_result.rowcount
                    conn.commit()
                    logger.info(f"Set sn_analsys='GC' for customer '{opera_account}' — {rows_set} row(s) updated")
                    gc_flag_info['gc_set_on'] = opera_account
                    gc_flag_info['gc_set_rows'] = rows_set
            except Exception as e:
                logger.warning(f"Could not update sn_analsys: {e}")
                gc_flag_info['gc_error'] = str(e)

        return {
            "success": True,
            "message": f"Mandate {mandate_id} linked to Opera customer {opera_account}",
            "mandate": mandate_result,
            "gc_flag": gc_flag_info
        }
    except Exception as e:
        logger.error(f"Error linking mandate: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/gocardless/mandates/{mandate_id}/cancel")
async def cancel_gocardless_mandate(mandate_id: str):
    """
    Cancel a mandate in GoCardless and update local record.
    This is irreversible — the customer will need to set up a new mandate.
    """
    try:
        gc_settings = _load_gocardless_settings()
        from sql_rag.gocardless_api import create_client_from_settings
        client = create_client_from_settings(gc_settings)
        if not client:
            return {"success": False, "error": "GoCardless not configured"}

        # Cancel on GoCardless
        try:
            result = client.mandates.cancel(mandate_id)
            gc_status = result.status if hasattr(result, 'status') else 'cancelled'
        except Exception as gc_err:
            err_msg = str(gc_err)
            if 'already' in err_msg.lower() and 'cancel' in err_msg.lower():
                gc_status = 'cancelled'
            else:
                return {"success": False, "error": f"GoCardless API error: {err_msg}"}

        # Update local record
        payments_db = get_payments_db()
        payments_db.update_mandate_status(mandate_id, gc_status)

        return {
            "success": True,
            "message": f"Mandate {mandate_id} cancelled",
            "status": gc_status
        }
    except Exception as e:
        logger.error(f"Error cancelling mandate: {e}")
        return {"success": False, "error": str(e)}


@router.delete("/api/gocardless/mandates/{mandate_id}")
async def unlink_gocardless_mandate(mandate_id: str):
    """
    Unlink a mandate from Opera customer.
    Does NOT cancel the mandate in GoCardless - just removes local link.
    """
    try:
        payments_db = get_payments_db()
        if payments_db.unlink_mandate(mandate_id):
            return {
                "success": True,
                "message": f"Mandate {mandate_id} unlinked"
            }
        return {"success": False, "error": "Mandate not found"}
    except Exception as e:
        logger.error(f"Error unlinking mandate: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/gocardless/mandates/setup")
async def create_mandate_setup(request: Request):
    """
    Initiate a new mandate setup for an Opera customer.

    Creates a GoCardless billing request, generates an authorisation URL,
    and sends an email to the customer with a link to set up their Direct Debit.

    Request body:
        opera_account: str - Opera customer account code
        opera_name: str - Customer name
        customer_email: str - Email to send authorisation link to
        email_subject: str (optional) - Custom email subject
        email_body: str (optional) - Custom email body HTML (must contain {authorisation_url} placeholder)
    """
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    try:
        body = await request.json()
        opera_account = body.get("opera_account", "").strip()
        opera_name = body.get("opera_name", "").strip()
        customer_email = body.get("customer_email", "").strip()

        if not opera_account:
            return {"success": False, "error": "Opera customer account is required"}
        if not customer_email:
            return {"success": False, "error": "Customer email address is required"}

        # Validate email format
        if '@' not in customer_email or '.' not in customer_email.split('@')[-1]:
            return {"success": False, "error": "Invalid email address format"}

        settings = _load_gocardless_settings()
        access_token = settings.get("api_access_token")
        if not access_token:
            return {"success": False, "error": "GoCardless API access token not configured. Go to GoCardless Settings to add your API credentials."}

        from sql_rag.gocardless_api import GoCardlessClient
        sandbox = settings.get("api_sandbox", False)
        client = GoCardlessClient(access_token=access_token, sandbox=sandbox)

        # Step 1: Create billing request
        metadata = {"opera_account": opera_account}
        if opera_name:
            metadata["opera_name"] = opera_name[:50]  # GC metadata value limit

        billing_request = client.create_billing_request(
            customer_email=customer_email,
            customer_name=opera_name or None,
            metadata=metadata
        )
        brq_id = billing_request.get("id")
        if not brq_id:
            return {"success": False, "error": "Failed to create billing request in GoCardless"}

        # Step 2: Create billing request flow to get authorisation URL
        flow = client.create_billing_request_flow(billing_request_id=brq_id)
        auth_url = flow.get("authorisation_url", "")
        flow_id = flow.get("id", "")

        if not auth_url:
            return {"success": False, "error": "Failed to generate authorisation URL from GoCardless"}

        # Step 3: Save to local tracking database
        payments_db = get_payments_db()
        setup_record = payments_db.create_mandate_setup(
            opera_account=opera_account,
            opera_name=opera_name,
            customer_email=customer_email,
            billing_request_id=brq_id,
            billing_request_flow_id=flow_id,
            authorisation_url=auth_url,
        )

        # Step 4: Send email to customer
        company_name = settings.get("company_reference", "").replace("LTDLTD", " LTD").replace("LTD", " Ltd").strip()
        if not company_name:
            company_name = "Our Company"

        email_subject = body.get("email_subject") or f"Set Up Your Direct Debit — {company_name}"
        default_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
            <h2 style="color: #333;">Direct Debit Setup</h2>
            <p>Dear {opera_name or 'Customer'},</p>
            <p>We would like to invite you to set up a Direct Debit with us for convenient automated payment processing.</p>
            <p>Please click the button below to securely set up your Direct Debit mandate through GoCardless:</p>
            <p style="text-align: center; margin: 30px 0;">
                <a href="{auth_url}"
                   style="display: inline-block; padding: 14px 28px; background-color: #1a73e8; color: white;
                          text-decoration: none; border-radius: 6px; font-weight: bold; font-size: 16px;">
                    Set Up Direct Debit
                </a>
            </p>
            <p>This process is quick, secure, and protected by the <a href="https://www.directdebit.co.uk/direct-debit-guarantee/">Direct Debit Guarantee</a>.</p>
            <p>If you have any questions, please don't hesitate to contact us.</p>
            <p>Kind regards,<br>{company_name}</p>
            <hr style="border: none; border-top: 1px solid #eee; margin-top: 30px;">
            <p style="font-size: 11px; color: #999;">
                If the button above doesn't work, copy and paste this link into your browser:<br>
                <a href="{auth_url}" style="color: #999;">{auth_url}</a>
            </p>
        </body>
        </html>
        """
        email_body = body.get("email_body")
        if email_body and "{authorisation_url}" in email_body:
            email_body = email_body.replace("{authorisation_url}", auth_url)
        else:
            email_body = default_body

        # Send via configured email provider
        email_sent = False
        email_error = None
        try:
            if not email_storage:
                email_error = "Email storage not initialized"
            else:
                providers = email_storage.get_all_providers()
                enabled_provider = next((p for p in providers if p.get('enabled')), None)

                if not enabled_provider:
                    email_error = "No enabled email provider configured"
                else:
                    provider_type = enabled_provider.get('provider_type')
                    config_json = enabled_provider.get('config_json', '{}')
                    provider_config = json.loads(config_json) if config_json else {}

                    if provider_type == 'imap':
                        smtp_server = provider_config.get('server', '')
                        smtp_port = int(provider_config.get('smtp_port', 587))
                        username = provider_config.get('username', '')
                        password = provider_config.get('password', '')
                    elif provider_type == 'gmail':
                        smtp_server = 'smtp.gmail.com'
                        smtp_port = 587
                        username = provider_config.get('email', '')
                        password = provider_config.get('app_password', '') or provider_config.get('password', '')
                    elif provider_type == 'microsoft':
                        smtp_server = 'smtp.office365.com'
                        smtp_port = 587
                        username = provider_config.get('email', '')
                        password = provider_config.get('password', '')
                    else:
                        email_error = f"Unsupported provider type: {provider_type}"
                        smtp_server = None

                    if smtp_server and not email_error:
                        from_email = body.get("from_email") or provider_config.get('from_email') or provider_config.get('email') or username

                        msg = MIMEMultipart('alternative')
                        msg['Subject'] = email_subject
                        msg['From'] = from_email
                        msg['To'] = customer_email
                        msg.attach(MIMEText(email_body, 'html'))

                        with smtplib.SMTP(smtp_server, smtp_port, timeout=30) as server:
                            server.ehlo()
                            if smtp_port == 587:
                                server.starttls()
                                server.ehlo()
                            server.login(username, password)
                            server.send_message(msg)

                        email_sent = True
                        logger.info(f"Mandate setup email sent to {customer_email} for {opera_account}")
        except Exception as e:
            email_error = str(e)
            logger.error(f"Failed to send mandate setup email to {customer_email}: {e}")

        # Update setup record with email status
        if email_sent:
            payments_db.update_mandate_setup(
                setup_record['id'],
                status='email_sent',
                email_sent_at=datetime.utcnow().isoformat()
            )
        else:
            payments_db.update_mandate_setup(
                setup_record['id'],
                status='pending',
                status_detail=f"Email not sent: {email_error}"
            )

        setup_record = payments_db.get_mandate_setup(setup_record['id'])

        return {
            "success": True,
            "message": f"Mandate setup initiated for {opera_name or opera_account}",
            "setup": setup_record,
            "email_sent": email_sent,
            "email_error": email_error,
            "authorisation_url": auth_url,
        }

    except Exception as e:
        logger.error(f"Error creating mandate setup: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/gocardless/mandates/pending-setups")
async def list_pending_mandate_setups():
    """List all pending mandate setup requests with current status."""
    try:
        payments_db = get_payments_db()
        setups = payments_db.list_mandate_setups()
        return {
            "success": True,
            "setups": setups,
            "pending_count": sum(1 for s in setups if s['status'] not in ('completed', 'failed', 'cancelled')),
        }
    except Exception as e:
        logger.error(f"Error listing mandate setups: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/gocardless/mandates/check-setups")
async def check_mandate_setups():
    """
    Poll GoCardless to check status of pending mandate setup requests.

    For each pending request:
    - Fetches billing request status from GoCardless
    - If mandate created: updates local record with mandate ID
    - If mandate active: links to Opera customer, sets sn_analsys='GC'
    """
    try:
        settings = _load_gocardless_settings()
        access_token = settings.get("api_access_token")
        if not access_token:
            return {"success": False, "error": "GoCardless API access token not configured"}

        from sql_rag.gocardless_api import GoCardlessClient
        sandbox = settings.get("api_sandbox", False)
        client = GoCardlessClient(access_token=access_token, sandbox=sandbox)

        payments_db = get_payments_db()
        pending = payments_db.get_pending_mandate_setups()

        if not pending:
            return {"success": True, "message": "No pending setups to check", "updates": []}

        updates = []
        for setup in pending:
            brq_id = setup.get('billing_request_id')
            if not brq_id:
                continue

            try:
                brq = client.get_billing_request(brq_id)
                brq_status = brq.get("status", "")
                brq_links = brq.get("links", {})
                mandate_id = brq_links.get("mandate_request_mandate") or brq_links.get("mandate")
                gc_customer_id = brq_links.get("customer")

                update_fields = {}

                if gc_customer_id and gc_customer_id != setup.get('gocardless_customer_id'):
                    update_fields['gocardless_customer_id'] = gc_customer_id

                if mandate_id and mandate_id != setup.get('mandate_id'):
                    update_fields['mandate_id'] = mandate_id

                # Map billing request status to our status
                if brq_status == 'fulfilled' and mandate_id:
                    # Mandate has been created — check its actual status
                    try:
                        mandate_detail = client.get_mandate(mandate_id)
                        mandate_status = mandate_detail.get("status", "")

                        if mandate_status == 'active':
                            update_fields['status'] = 'completed'
                            update_fields['mandate_active_at'] = datetime.utcnow().isoformat()
                            update_fields['status_detail'] = f'Mandate {mandate_id} is active'
                        elif mandate_status in ('pending_customer_approval', 'pending_submission', 'submitted'):
                            update_fields['status'] = 'mandate_created'
                            update_fields['status_detail'] = f'Mandate {mandate_id} status: {mandate_status}'
                        elif mandate_status in ('cancelled', 'expired', 'failed'):
                            update_fields['status'] = 'failed'
                            update_fields['status_detail'] = f'Mandate {mandate_id} {mandate_status}'
                        else:
                            update_fields['status'] = 'mandate_created'
                            update_fields['status_detail'] = f'Mandate {mandate_id} status: {mandate_status}'
                    except Exception as me:
                        logger.warning(f"Could not check mandate {mandate_id}: {me}")
                        update_fields['status'] = 'mandate_created'
                        update_fields['status_detail'] = f'Mandate {mandate_id} created (status check failed)'

                elif brq_status in ('pending', 'action_required'):
                    if setup.get('status') == 'email_sent':
                        update_fields['status'] = 'authorisation_pending'
                        update_fields['status_detail'] = 'Awaiting customer to complete authorisation'

                elif brq_status == 'cancelled':
                    update_fields['status'] = 'cancelled'
                    update_fields['status_detail'] = 'Billing request was cancelled'

                if update_fields:
                    payments_db.update_mandate_setup(setup['id'], **update_fields)

                    # If completed (mandate active), link to Opera
                    if update_fields.get('status') == 'completed' and mandate_id:
                        _complete_mandate_setup(
                            payments_db, client, setup, mandate_id, gc_customer_id
                        )

                    updated_setup = payments_db.get_mandate_setup(setup['id'])
                    updates.append({
                        'setup_id': setup['id'],
                        'opera_account': setup['opera_account'],
                        'opera_name': setup['opera_name'],
                        'old_status': setup['status'],
                        'new_status': updated_setup['status'],
                        'mandate_id': mandate_id,
                    })

            except Exception as e:
                logger.warning(f"Error checking setup {setup['id']} (BRQ {brq_id}): {e}")
                updates.append({
                    'setup_id': setup['id'],
                    'opera_account': setup['opera_account'],
                    'error': str(e),
                })

        return {
            "success": True,
            "message": f"Checked {len(pending)} pending setups, {len(updates)} updated",
            "updates": updates,
        }

    except Exception as e:
        logger.error(f"Error checking mandate setups: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/gocardless/customer-email/{account}")
async def get_customer_email_for_mandate(account: str):
    """Get customer email from Opera for pre-filling the mandate setup form."""
    try:
        if not sql_connector:
            return {"success": True, "email": "", "name": ""}

        from sqlalchemy import text
        with sql_connector.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT RTRIM(sn_name) as name, RTRIM(sn_email) as email,
                       RTRIM(sn_contact) as contact
                FROM sname
                WHERE LTRIM(RTRIM(sn_account)) = :account
            """), {"account": account.strip()})
            row = result.fetchone()

        if not row:
            return {"success": True, "email": "", "name": ""}

        return {
            "success": True,
            "email": (row[1] or "").strip(),
            "name": (row[0] or "").strip(),
            "contact": (row[2] or "").strip(),
        }
    except Exception as e:
        logger.error(f"Error getting customer email: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/gocardless/mandates/cancel-setup/{setup_id}")
async def cancel_mandate_setup(setup_id: int):
    """Cancel a pending mandate setup request."""
    try:
        payments_db = get_payments_db()
        setup = payments_db.get_mandate_setup(setup_id)
        if not setup:
            return {"success": False, "error": "Setup request not found"}

        if setup['status'] in ('completed', 'failed', 'cancelled'):
            return {"success": False, "error": f"Cannot cancel — setup is already {setup['status']}"}

        payments_db.update_mandate_setup(
            setup_id,
            status='cancelled',
            status_detail='Cancelled by user'
        )

        return {
            "success": True,
            "message": f"Mandate setup for {setup['opera_name'] or setup['opera_account']} cancelled"
        }
    except Exception as e:
        logger.error(f"Error cancelling mandate setup: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/opera3/gocardless/mandates/setup")
async def opera3_create_mandate_setup(request: Request):
    """
    Initiate a new mandate setup for an Opera 3 customer.
    Same logic as SQL SE but reads customer email from FoxPro.
    """
    # Reuse the same logic — the GoCardless API calls are identical
    return await create_mandate_setup(request)


@router.get("/api/opera3/gocardless/mandates/pending-setups")
async def opera3_list_pending_mandate_setups():
    """List all pending mandate setup requests (Opera 3)."""
    return await list_pending_mandate_setups()


@router.post("/api/opera3/gocardless/mandates/check-setups")
async def opera3_check_mandate_setups():
    """
    Poll GoCardless to check status of pending mandate setup requests (Opera 3).
    Same GoCardless API calls, but sets sn_analsys='GC' in FoxPro instead of SQL Server.
    """
    try:
        settings = _load_gocardless_settings()
        access_token = settings.get("api_access_token")
        if not access_token:
            return {"success": False, "error": "GoCardless API access token not configured"}

        from sql_rag.gocardless_api import GoCardlessClient
        sandbox = settings.get("api_sandbox", False)
        client = GoCardlessClient(access_token=access_token, sandbox=sandbox)

        payments_db = get_payments_db()
        pending = payments_db.get_pending_mandate_setups()

        if not pending:
            return {"success": True, "message": "No pending setups to check", "updates": []}

        updates = []
        for setup in pending:
            brq_id = setup.get('billing_request_id')
            if not brq_id:
                continue

            try:
                brq = client.get_billing_request(brq_id)
                brq_status = brq.get("status", "")
                brq_links = brq.get("links", {})
                mandate_id = brq_links.get("mandate_request_mandate") or brq_links.get("mandate")
                gc_customer_id = brq_links.get("customer")

                update_fields = {}

                if gc_customer_id and gc_customer_id != setup.get('gocardless_customer_id'):
                    update_fields['gocardless_customer_id'] = gc_customer_id

                if mandate_id and mandate_id != setup.get('mandate_id'):
                    update_fields['mandate_id'] = mandate_id

                if brq_status == 'fulfilled' and mandate_id:
                    try:
                        mandate_detail = client.get_mandate(mandate_id)
                        mandate_status = mandate_detail.get("status", "")

                        if mandate_status == 'active':
                            update_fields['status'] = 'completed'
                            update_fields['mandate_active_at'] = datetime.utcnow().isoformat()
                            update_fields['status_detail'] = f'Mandate {mandate_id} is active'
                        elif mandate_status in ('pending_customer_approval', 'pending_submission', 'submitted'):
                            update_fields['status'] = 'mandate_created'
                            update_fields['status_detail'] = f'Mandate {mandate_id} status: {mandate_status}'
                        elif mandate_status in ('cancelled', 'expired', 'failed'):
                            update_fields['status'] = 'failed'
                            update_fields['status_detail'] = f'Mandate {mandate_id} {mandate_status}'
                        else:
                            update_fields['status'] = 'mandate_created'
                    except Exception:
                        update_fields['status'] = 'mandate_created'

                elif brq_status in ('pending', 'action_required'):
                    if setup.get('status') == 'email_sent':
                        update_fields['status'] = 'authorisation_pending'
                        update_fields['status_detail'] = 'Awaiting customer to complete authorisation'

                elif brq_status == 'cancelled':
                    update_fields['status'] = 'cancelled'
                    update_fields['status_detail'] = 'Billing request was cancelled'

                if update_fields:
                    payments_db.update_mandate_setup(setup['id'], **update_fields)

                    if update_fields.get('status') == 'completed' and mandate_id:
                        _complete_mandate_setup_opera3(
                            payments_db, client, setup, mandate_id, gc_customer_id
                        )

                    updated_setup = payments_db.get_mandate_setup(setup['id'])
                    updates.append({
                        'setup_id': setup['id'],
                        'opera_account': setup['opera_account'],
                        'opera_name': setup['opera_name'],
                        'old_status': setup['status'],
                        'new_status': updated_setup['status'],
                        'mandate_id': mandate_id,
                    })

            except Exception as e:
                logger.warning(f"Error checking setup {setup['id']} (BRQ {brq_id}): {e}")
                updates.append({
                    'setup_id': setup['id'],
                    'opera_account': setup['opera_account'],
                    'error': str(e),
                })

        return {
            "success": True,
            "message": f"Checked {len(pending)} pending setups, {len(updates)} updated",
            "updates": updates,
        }

    except Exception as e:
        logger.error(f"Error checking mandate setups: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/opera3/gocardless/customer-email/{account}")
async def opera3_get_customer_email_for_mandate(account: str):
    """Get customer email from Opera 3 FoxPro for pre-filling the mandate setup form."""
    try:
        from sql_rag.opera3_foxpro import Opera3FoxPro
        foxpro = Opera3FoxPro()
        results = foxpro.query_table('sname', f"RTRIM(sn_account) = '{account.strip()}'",
                                     fields=['sn_name', 'sn_email', 'sn_contact'])
        if not results:
            return {"success": True, "email": "", "name": ""}

        row = results[0]
        return {
            "success": True,
            "email": (row.get('sn_email', '') or '').strip(),
            "name": (row.get('sn_name', '') or '').strip(),
            "contact": (row.get('sn_contact', '') or '').strip(),
        }
    except Exception as e:
        logger.error(f"Error getting Opera 3 customer email: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/opera3/gocardless/mandates/cancel-setup/{setup_id}")
async def opera3_cancel_mandate_setup(setup_id: int):
    """Cancel a pending mandate setup request (Opera 3)."""
    return await cancel_mandate_setup(setup_id)


@router.get("/api/opera3/gocardless/payment-requests")
async def opera3_list_payment_requests(
    status: Optional[str] = Query(None, description="Filter by status"),
    opera_account: Optional[str] = Query(None, description="Filter by Opera account"),
    limit: int = Query(100, description="Maximum records to return")
):
    """List payment requests — same logic as SE."""
    return await list_payment_requests(status=status, opera_account=opera_account, limit=limit)


@router.get("/api/opera3/gocardless/payment-requests/stats")
async def opera3_payment_request_stats():
    """Get payment request statistics — same logic as SE."""
    return await get_gocardless_payment_stats()


@router.get("/api/opera3/gocardless/payment-requests/{request_id}")
async def opera3_get_payment_request_detail(request_id: int):
    """Get details of a specific payment request — same logic as SE."""
    return await get_payment_request(request_id)


@router.post("/api/opera3/gocardless/payment-requests/{request_id}/cancel")
async def opera3_cancel_payment_request(request_id: int):
    """Cancel a pending payment request — same logic as SE."""
    return await cancel_payment_request(request_id)


@router.post("/api/opera3/gocardless/payment-requests/sync")
async def opera3_sync_payment_requests():
    """Sync payment statuses from GoCardless API — same logic as SE."""
    return await sync_payment_statuses()


@router.get("/api/opera3/gocardless/mandates")
async def opera3_list_mandates(
    status: Optional[str] = Query(None, description="Filter by status (active, cancelled, etc)"),
    opera_account: Optional[str] = Query(None, description="Filter by Opera account code")
):
    """List all GoCardless mandates — same logic as SE."""
    return await list_gocardless_mandates(status=status, opera_account=opera_account)


@router.get("/api/opera3/gocardless/mandates/unlinked")
async def opera3_get_unlinked_mandates():
    """List unlinked GoCardless mandates — same logic as SE."""
    return await list_unlinked_gocardless_mandates()


@router.delete("/api/opera3/gocardless/mandates/{mandate_id}")
async def opera3_delete_mandate(mandate_id: str):
    """Unlink a mandate from Opera customer — same logic as SE."""
    return await unlink_gocardless_mandate(mandate_id)


@router.get("/api/opera3/gocardless/subscriptions/{subscription_id}")
async def opera3_get_subscription(subscription_id: str):
    """Get a specific subscription — same logic as SE."""
    return await get_gocardless_subscription(subscription_id)


@router.put("/api/opera3/gocardless/subscriptions/{subscription_id}")
async def opera3_update_subscription(subscription_id: str, request: Request):
    """Update a subscription (name/amount) — same logic as SE."""
    return await update_gocardless_subscription(subscription_id, request)


@router.post("/api/opera3/gocardless/subscriptions/{subscription_id}/pause")
async def opera3_pause_subscription(subscription_id: str):
    """Pause a subscription — same logic as SE."""
    return await pause_gocardless_subscription(subscription_id)


@router.post("/api/opera3/gocardless/subscriptions/{subscription_id}/resume")
async def opera3_resume_subscription(subscription_id: str):
    """Resume a subscription — same logic as SE."""
    return await resume_gocardless_subscription(subscription_id)


@router.post("/api/opera3/gocardless/subscriptions/{subscription_id}/cancel")
async def opera3_cancel_subscription(subscription_id: str):
    """Cancel a subscription — same logic as SE."""
    return await cancel_gocardless_subscription(subscription_id)


@router.post("/api/opera3/gocardless/subscriptions/link")
async def opera3_link_subscription(request: Request):
    """Link a subscription to an Opera repeat document — same logic as SE."""
    return await link_subscription_to_document(request)


@router.post("/api/opera3/gocardless/subscriptions/unlink")
async def opera3_unlink_subscription(request: Request):
    """Unlink a subscription from an Opera repeat document — same logic as SE."""
    return await unlink_subscription_from_document(request)


@router.post("/api/opera3/gocardless/subscriptions/sync")
async def opera3_sync_subscriptions():
    """Sync all subscriptions from GoCardless API — same logic as SE."""
    return await sync_gocardless_subscriptions()


@router.post("/api/opera3/gocardless/settings")
async def opera3_save_gocardless_settings(request: Request):
    """Save GoCardless settings — same logic as SE."""
    return await save_gocardless_settings(request)


@router.post("/api/opera3/gocardless/update-subscription-tags")
async def opera3_update_subscription_tags(request: Request):
    """Preview or apply subscription tag updates — same logic as SE."""
    return await update_subscription_tags(request)


@router.post("/api/opera3/gocardless/skip-payout")
async def opera3_skip_payout(
    request: Request,
    payout_id: str = Query(..., description="GoCardless payout ID"),
    bank_reference: str = Query(..., description="Bank reference (e.g., INTSYSUKLTD-XM5XEF)"),
    gross_amount: float = Query(..., description="Gross amount"),
    currency: str = Query("GBP", description="Currency code"),
    payment_count: int = Query(0, description="Number of payments"),
    reason: str = Query("manual", description="Reason for skipping: 'foreign_currency', 'manual', 'duplicate'"),
    fx_amount: Optional[float] = Query(None, description="GBP equivalent amount for foreign currency payouts")
):
    """Skip a payout without importing — same logic as SE."""
    return await skip_gocardless_payout(
        request=request, payout_id=payout_id, bank_reference=bank_reference,
        gross_amount=gross_amount, currency=currency, payment_count=payment_count,
        reason=reason, fx_amount=fx_amount
    )


@router.get("/api/opera3/gocardless/api-payouts")
async def opera3_get_api_payouts(
    status: str = Query("paid"),
    limit: int = Query(20),
    days_back: Optional[int] = Query(None)
):
    """Fetch payouts from GoCardless API — same logic as SE, uses payout_lookback_days from settings."""
    return await get_gocardless_api_payouts(status=status, limit=limit, days_back=days_back)


@router.get("/api/gocardless/eligible-customers")
async def get_gocardless_eligible_customers():
    """
    Get all GoCardless customers — both those with mandates and those flagged for GC but without.
    Combines: customers with linked mandates (from local DB) + customers with sn_analsys='GC' (from Opera).
    """
    try:
        if not sql_connector:
            return {"success": False, "error": "Database not connected"}

        payments_db = get_payments_db()

        # Get all existing mandates keyed by Opera account (excluding unlinked ones)
        existing_mandates = payments_db.list_mandates()
        mandate_lookup = {
            m['opera_account'].strip(): m
            for m in existing_mandates
            if m.get('opera_account') and m['opera_account'] != '__UNLINKED__'
        }

        # Get all mandated account codes to include in the query
        mandated_accounts = list(mandate_lookup.keys())

        # Query customers with 'GC' analysis code OR who have a linked mandate
        if mandated_accounts:
            placeholders = ', '.join(f"'{a}'" for a in mandated_accounts)
            query = f"""
                SELECT
                    sn_account,
                    sn_name,
                    sn_analsys,
                    sn_currbal,
                    sn_email
                FROM sname
                WHERE LTRIM(RTRIM(UPPER(sn_analsys))) = 'GC'
                   OR RTRIM(sn_account) IN ({placeholders})
                ORDER BY sn_name
            """
        else:
            query = """
                SELECT
                    sn_account,
                    sn_name,
                    sn_analsys,
                    sn_currbal,
                    sn_email
                FROM sname
                WHERE LTRIM(RTRIM(UPPER(sn_analsys))) = 'GC'
                ORDER BY sn_name
            """

        result = sql_connector.execute_query(query)

        customers = []
        seen_accounts = set()

        if result is not None and not result.empty:
            for _, row in result.iterrows():
                account = row['sn_account'].strip() if row['sn_account'] else ''
                if account in seen_accounts:
                    continue
                seen_accounts.add(account)
                name = row['sn_name'].strip() if row['sn_name'] else ''

                existing_mandate = mandate_lookup.get(account)

                customers.append({
                    'account': account,
                    'name': name,
                    'balance': float(row['sn_currbal']) if row['sn_currbal'] else 0,
                    'email': row['sn_email'].strip() if row.get('sn_email') else None,
                    'has_mandate': existing_mandate is not None,
                    'mandate_id': existing_mandate.get('mandate_id') if existing_mandate else None,
                    'mandate_status': existing_mandate.get('mandate_status') if existing_mandate else None
                })

        # Count stats
        with_mandate = sum(1 for c in customers if c['has_mandate'])
        without_mandate = sum(1 for c in customers if not c['has_mandate'])

        return {
            "success": True,
            "customers": customers,
            "count": len(customers),
            "with_mandate": with_mandate,
            "without_mandate": without_mandate
        }

    except Exception as e:
        logger.error(f"Error getting GC eligible customers: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/gocardless/mandates/suggest-match")
async def suggest_mandate_match(
    gc_name: str = Query(..., description="GoCardless customer name to match against Opera")
):
    """
    Suggest the best Opera customer match for a GoCardless mandate name.
    Uses fuzzy matching against the full sales ledger.
    """
    try:
        if not sql_connector:
            return {"success": True, "suggestions": []}

        from difflib import SequenceMatcher

        # Get all Opera customers
        customers_df = sql_connector.execute_query("""
            SELECT RTRIM(sn_account) as account, RTRIM(sn_name) as name,
                   RTRIM(sn_analsys) as analsys, sn_currbal as balance
            FROM sname
            WHERE sn_stop = 0
            ORDER BY sn_name
        """)

        if customers_df is None or customers_df.empty:
            return {"success": True, "suggestions": []}

        def normalize(name):
            if not name:
                return ''
            n = name.upper().strip()
            for suffix in [' LTD', ' LIMITED', ' PLC', ' INC', ' LLC', ' CO', ' COMPANY', '.']:
                if n.endswith(suffix):
                    n = n[:-len(suffix)]
            return n.strip()

        gc_norm = normalize(gc_name)
        suggestions = []

        for _, row in customers_df.iterrows():
            account = row['account'].strip() if row['account'] else ''
            name = row['name'].strip() if row['name'] else ''
            if not account or not name:
                continue

            opera_norm = normalize(name)

            # Exact match after normalization
            if gc_norm == opera_norm:
                score = 1.0
            # Containment match
            elif gc_norm in opera_norm or opera_norm in gc_norm:
                score = 0.85
            else:
                # Fuzzy match
                score = SequenceMatcher(None, gc_norm, opera_norm).ratio()

            if score >= 0.5:
                suggestions.append({
                    'account': account,
                    'name': name,
                    'score': round(score, 3),
                    'is_gc': (row.get('analsys') or '').strip().upper() == 'GC'
                })

        # Sort by score descending, GC-flagged first at same score
        suggestions.sort(key=lambda s: (-s['score'], not s['is_gc'], s['name']))

        return {
            "success": True,
            "suggestions": suggestions[:5],
            "gc_name": gc_name
        }
    except Exception as e:
        logger.error(f"Error suggesting mandate match: {e}")
        return {"success": True, "suggestions": []}


@router.get("/api/gocardless/collectable-invoices")
async def get_collectable_invoices(
    overdue_only: bool = Query(False, description="Only show overdue invoices"),
    min_amount: float = Query(0, description="Minimum invoice amount")
):
    """
    Get outstanding invoices that can be collected via GoCardless.
    Shows which invoices have mandates available.
    """
    try:
        if not sql_connector:
            return {"success": False, "error": "Database not connected"}

        gc_settings = _load_gocardless_settings()
        sub_tag = gc_settings.get("subscription_tag", "SUB")

        payments_db = get_payments_db()

        # Get all active mandates keyed by Opera account
        mandates = payments_db.list_mandates(status='active')
        mandate_lookup = {m['opera_account']: m for m in mandates}

        # Get outstanding invoices from Opera, flagging subscription invoices
        query = """
            SELECT
                st_account,
                sn_name,
                st_ref,
                st_date,
                st_duedate,
                st_type,
                st_ovalue,
                CASE WHEN EXISTS (
                    SELECT 1 FROM ihead WHERE ih_invoice = st_ref AND ih_docstat = 'I' AND RTRIM(ih_analsys) = :sub_tag
                ) THEN 1 ELSE 0 END AS is_sub
            FROM stran
            JOIN sname WITH (NOLOCK) ON st_account = sn_account
            WHERE st_ovalue > 0
              AND st_type IN (1, 2)  -- Invoices and credit notes
        """

        if min_amount > 0:
            query += f" AND st_ovalue >= {min_amount}"

        query += " ORDER BY st_account, st_date"

        invoices = []
        total_collectable = 0
        total_with_mandate = 0

        # Build lookup for subscription source docs
        sub_account_docs = {}
        all_subs = payments_db.list_subscriptions()
        for s in all_subs:
            if s['source_doc'] and s['status'] != 'cancelled':
                sub_account_docs[s['opera_account']] = s['source_doc']

        from sqlalchemy import text as sa_text
        with sql_connector.engine.connect() as conn:
            result = conn.execute(sa_text(query), {'sub_tag': sub_tag})

            for row in result:
                account = row[0].strip() if row[0] else ''
                customer_name = row[1].strip() if row[1] else ''
                invoice_ref = row[2].strip() if row[2] else ''
                invoice_date = row[3]
                due_date = row[4]
                trans_type = row[5]
                amount = float(row[6]) if row[6] else 0

                # Detect subscription invoices via sanal/hsanal sa_cusanal='SUB'
                is_subscription = bool(row[7])
                source_doc = sub_account_docs.get(account) if is_subscription else None

                # Calculate days overdue
                days_overdue = 0
                if due_date:
                    if isinstance(due_date, str):
                        due_date = datetime.strptime(due_date, '%Y-%m-%d').date()
                    days_overdue = (date.today() - due_date).days

                # Skip if filtering for overdue only
                if overdue_only and days_overdue <= 0:
                    continue

                # Check for mandate
                mandate = mandate_lookup.get(account)
                has_mandate = mandate is not None

                invoice_data = {
                    'opera_account': account,
                    'customer_name': customer_name,
                    'invoice_ref': invoice_ref,
                    'invoice_date': invoice_date.isoformat() if hasattr(invoice_date, 'isoformat') else str(invoice_date),
                    'due_date': due_date.isoformat() if hasattr(due_date, 'isoformat') else str(due_date) if due_date else None,
                    'amount': amount,
                    'amount_formatted': f"£{amount:,.2f}",
                    'days_overdue': max(0, days_overdue),
                    'is_overdue': days_overdue > 0,
                    'has_mandate': has_mandate,
                    'mandate_id': mandate['mandate_id'] if mandate else None,
                    'mandate_status': mandate['mandate_status'] if mandate else None,
                    'trans_type': 'Invoice' if trans_type == 1 else 'Credit Note',
                    'is_subscription': is_subscription,
                    'source_doc': source_doc,
                }

                invoices.append(invoice_data)
                if not is_subscription:
                    total_collectable += amount
                    if has_mandate:
                        total_with_mandate += amount

        return {
            "success": True,
            "invoices": invoices,
            "count": len(invoices),
            "total_collectable": total_collectable,
            "total_collectable_formatted": f"£{total_collectable:,.2f}",
            "total_with_mandate": total_with_mandate,
            "total_with_mandate_formatted": f"£{total_with_mandate:,.2f}",
            "mandates_available": len(mandate_lookup)
        }
    except Exception as e:
        logger.error(f"Error getting collectable invoices: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/gocardless/due-invoices")
async def get_gocardless_due_invoices(
    advance_date: Optional[str] = Query(None, description="Show invoices due by this date (YYYY-MM-DD). Defaults to today."),
    include_future: bool = Query(True, description="Include invoices due after today but before advance_date")
):
    """
    Get outstanding invoices due for GoCardless collection.

    Only shows invoices for customers who have an active GoCardless mandate.
    Use advance_date to see invoices that will be due by a future date.

    Returns invoices grouped by customer for easy batch selection.
    """
    try:
        if not sql_connector:
            return {"success": False, "error": "Database not connected"}

        gc_settings = _load_gocardless_settings()
        sub_tag = gc_settings.get("subscription_tag", "SUB")

        from datetime import date, datetime

        # Parse advance date or default to today
        if advance_date:
            try:
                target_date = datetime.strptime(advance_date, '%Y-%m-%d').date()
            except ValueError:
                return {"success": False, "error": "Invalid date format. Use YYYY-MM-DD"}
        else:
            target_date = date.today()

        payments_db = get_payments_db()

        # Get all active mandates keyed by Opera account
        mandates = payments_db.list_mandates(status='active')
        mandate_lookup = {m['opera_account'].strip(): m for m in mandates if m.get('opera_account')}

        # Build lookup of invoices that already have active payment requests
        # (pending, pending_submission, submitted, confirmed — i.e. not yet paid/failed/cancelled)
        active_statuses = ('pending', 'pending_submission', 'submitted', 'confirmed')
        pending_invoice_requests: Dict[str, Dict] = {}  # invoice_ref -> request info
        try:
            all_requests = payments_db.list_payment_requests()
            for req in all_requests:
                if req.get('status') not in active_statuses:
                    continue
                refs = req.get('invoice_refs')
                if isinstance(refs, str):
                    try:
                        refs = json.loads(refs)
                    except Exception:
                        refs = []
                if refs:
                    for ref in refs:
                        pending_invoice_requests[ref.strip()] = {
                            'request_id': req.get('id'),
                            'status': req.get('status'),
                            'charge_date': req.get('charge_date'),
                            'amount_pence': req.get('amount_pence'),
                        }
        except Exception as e:
            logger.warning(f"Could not load pending payment requests: {e}")

        # Also check GoCardless API for active payments (catches requests made
        # outside this app, e.g. via GoCardless dashboard)
        try:
            import re as _inv_re
            from sql_rag.gocardless_api import create_client_from_settings
            gc_client = create_client_from_settings(gc_settings)
            if gc_client:
                # Fetch payments that are not yet settled or failed
                for gc_status in ('pending_submission', 'submitted', 'confirmed'):
                    gc_payments, _ = gc_client.list_payments(status=gc_status, limit=500)
                    for gcp in gc_payments:
                        desc = gcp.get('description', '') or ''
                        charge_date = gcp.get('charge_date', '')
                        amount_pence = gcp.get('amount', 0)
                        gc_status_val = gcp.get('status', gc_status)
                        # Extract invoice refs from description (e.g. "INV12345" or "INV12345,INV12346")
                        found_refs = _inv_re.findall(r'INV\d+', desc, _inv_re.IGNORECASE)
                        for ref in found_refs:
                            ref_upper = ref.upper()
                            if ref_upper not in pending_invoice_requests:
                                pending_invoice_requests[ref_upper] = {
                                    'request_id': gcp.get('id', ''),
                                    'status': gc_status_val,
                                    'charge_date': charge_date,
                                    'amount_pence': amount_pence,
                                    'source': 'gocardless_api',
                                }
                logger.info(f"GoCardless API: found {len(pending_invoice_requests)} invoices with active payments")
        except Exception as e:
            logger.warning(f"Could not check GoCardless API for active payments: {e}")

        # Only query invoices for customers who have an active mandate
        mandated_accounts = [a for a in mandate_lookup.keys() if a != '__UNLINKED__']

        if not mandated_accounts:
            return {
                "success": True,
                "customers": [],
                "invoices": [],
                "summary": {
                    "total_customers": 0,
                    "total_invoices": 0,
                    "total_amount": 0,
                    "total_amount_formatted": "£0.00",
                    "collectable_amount": 0,
                    "collectable_formatted": "£0.00"
                },
                "advance_date": target_date.isoformat(),
                "today": date.today().isoformat()
            }

        # Build subscription source doc lookup
        sub_account_docs = {}
        all_subs = payments_db.list_subscriptions()
        for s in all_subs:
            if s['source_doc'] and s['status'] != 'cancelled':
                sub_account_docs[s['opera_account']] = s['source_doc']

        placeholders = ', '.join(f"'{a}'" for a in mandated_accounts)
        query = f"""
            SELECT
                st_account,
                sn_name,
                sn_analsys,
                sn_email,
                st_trref,
                st_trdate,
                st_dueday,
                st_trtype,
                st_trbal,
                st_trvalue,
                st_custref,
                CASE WHEN EXISTS (
                    SELECT 1 FROM ihead WHERE ih_invoice = st_trref AND ih_docstat = 'I' AND RTRIM(ih_analsys) = :sub_tag
                ) THEN 1 ELSE 0 END AS is_sub
            FROM stran
            JOIN sname WITH (NOLOCK) ON st_account = sn_account
            WHERE st_trbal > 0
              AND st_trtype = 'I'
              AND RTRIM(st_account) IN ({placeholders})
            ORDER BY sn_name, st_dueday, st_trref
        """

        result = sql_connector.execute_query(query, params={'sub_tag': sub_tag})

        if result is None or result.empty:
            return {
                "success": True,
                "customers": [],
                "invoices": [],
                "summary": {
                    "total_customers": 0,
                    "total_invoices": 0,
                    "total_amount": 0,
                    "total_amount_formatted": "£0.00",
                    "collectable_amount": 0,
                    "collectable_formatted": "£0.00"
                },
                "advance_date": target_date.isoformat(),
                "today": date.today().isoformat()
            }

        invoices = []
        customers_data = {}
        total_amount = 0
        collectable_amount = 0

        for _, row in result.iterrows():
            account = row['st_account'].strip() if row['st_account'] else ''
            customer_name = row['sn_name'].strip() if row['sn_name'] else ''
            invoice_ref = row['st_trref'].strip() if row['st_trref'] else ''
            invoice_date = row['st_trdate']
            due_date = row['st_dueday']
            trans_type = row['st_trtype'] if row['st_trtype'] else 'I'
            amount = float(row['st_trbal']) if row['st_trbal'] else 0
            original_amount = float(row['st_trvalue']) if row['st_trvalue'] else amount
            email = row['sn_email'].strip() if row.get('sn_email') else None
            customer_ref = row['st_custref'].strip() if row.get('st_custref') else ''
            is_subscription = bool(row.get('is_sub', 0))
            source_doc = sub_account_docs.get(account) if is_subscription else None

            # Parse due date
            due_date_obj = None
            if due_date:
                if isinstance(due_date, str):
                    try:
                        due_date_obj = datetime.strptime(due_date[:10], '%Y-%m-%d').date()
                    except:
                        pass
                elif hasattr(due_date, 'date'):
                    due_date_obj = due_date.date()
                elif isinstance(due_date, date):
                    due_date_obj = due_date

            # Calculate days until due / days overdue
            days_until_due = None
            is_overdue = False
            is_due_by_advance = False

            if due_date_obj:
                days_until_due = (due_date_obj - date.today()).days
                is_overdue = days_until_due < 0
                is_due_by_advance = due_date_obj <= target_date

            # Skip invoices not due by advance date (unless include_future is false and it's not overdue)
            if not include_future and not is_overdue:
                continue
            if due_date_obj and due_date_obj > target_date:
                continue

            # Check for mandate
            mandate = mandate_lookup.get(account)
            has_mandate = mandate is not None

            invoice_data = {
                'opera_account': account,
                'customer_name': customer_name,
                'invoice_ref': invoice_ref,
                'invoice_date': invoice_date.isoformat() if hasattr(invoice_date, 'isoformat') else str(invoice_date) if invoice_date else None,
                'due_date': due_date_obj.isoformat() if due_date_obj else None,
                'days_until_due': days_until_due,
                'is_overdue': is_overdue,
                'is_due_by_advance': is_due_by_advance,
                'amount': amount,
                'amount_formatted': f"£{amount:,.2f}",
                'original_amount': original_amount,
                'has_mandate': has_mandate,
                'mandate_id': mandate['mandate_id'] if mandate else None,
                'trans_type': 'Invoice' if trans_type == 1 else 'Credit Note',
                'trans_type_code': trans_type,
                'customer_ref': customer_ref,
                'payment_requested': invoice_ref in pending_invoice_requests,
                'payment_request_info': pending_invoice_requests.get(invoice_ref),
                'is_subscription': is_subscription,
                'source_doc': source_doc,
            }

            invoices.append(invoice_data)
            total_amount += amount

            if has_mandate and not is_subscription:
                collectable_amount += amount

            # Group by customer
            if account not in customers_data:
                customers_data[account] = {
                    'account': account,
                    'name': customer_name,
                    'email': email,
                    'has_mandate': has_mandate,
                    'mandate_id': mandate['mandate_id'] if mandate else None,
                    'invoices': [],
                    'total_due': 0,
                    'invoice_count': 0
                }

            customers_data[account]['invoices'].append(invoice_data)
            customers_data[account]['total_due'] += amount
            customers_data[account]['invoice_count'] += 1

        # Convert customers to list and add formatted totals
        customers = []
        for account, cust in customers_data.items():
            cust['total_due_formatted'] = f"£{cust['total_due']:,.2f}"
            customers.append(cust)

        # Sort customers by name
        customers.sort(key=lambda x: x['name'])

        return {
            "success": True,
            "customers": customers,
            "invoices": invoices,
            "summary": {
                "total_customers": len(customers),
                "total_invoices": len(invoices),
                "total_amount": total_amount,
                "total_amount_formatted": f"£{total_amount:,.2f}",
                "collectable_amount": collectable_amount,
                "collectable_formatted": f"£{collectable_amount:,.2f}",
                "customers_with_mandate": sum(1 for c in customers if c['has_mandate']),
                "customers_without_mandate": sum(1 for c in customers if not c['has_mandate'])
            },
            "advance_date": target_date.isoformat(),
            "today": date.today().isoformat()
        }

    except Exception as e:
        logger.error(f"Error getting due invoices: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/gocardless/payment-requests")
async def list_payment_requests(
    status: Optional[str] = Query(None, description="Filter by status"),
    opera_account: Optional[str] = Query(None, description="Filter by Opera account"),
    limit: int = Query(100, description="Maximum records to return")
):
    """List payment requests."""
    try:
        payments_db = get_payments_db()
        requests = payments_db.list_payment_requests(
            status=status,
            opera_account=opera_account,
            limit=limit
        )

        # Enhance with customer names from mandates
        mandates = payments_db.list_mandates()
        mandate_names = {m['opera_account']: m['opera_name'] for m in mandates}

        for req in requests:
            req['customer_name'] = mandate_names.get(req['opera_account'], req['opera_account'])

        return {
            "success": True,
            "requests": requests,
            "count": len(requests)
        }
    except Exception as e:
        logger.error(f"Error listing payment requests: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/gocardless/request-payment")
async def request_gocardless_payment(
    opera_account: str = Body(..., description="Opera customer account code"),
    invoices: List[str] = Body(..., description="List of invoice references"),
    amount: Optional[int] = Body(None, description="Amount in pence (if omitted, uses invoice totals)"),
    charge_date: Optional[str] = Body(None, description="Charge date (YYYY-MM-DD, default ASAP)"),
    description: Optional[str] = Body(None, description="Payment description")
):
    """
    Request payment from a customer via GoCardless Direct Debit.
    Creates a payment against their mandate.
    """
    try:
        payments_db = get_payments_db()

        # Get mandate for customer
        mandate = payments_db.get_mandate_for_customer(opera_account)
        if not mandate:
            return {
                "success": False,
                "error": f"No active mandate found for customer {opera_account}. Please set up a mandate first."
            }

        # Calculate amount if not provided
        if amount is None:
            if not sql_connector:
                return {"success": False, "error": "Database not connected - cannot calculate invoice total"}

            # Sum outstanding amounts for specified invoices
            from sqlalchemy import text as sa_text
            inv_params = {f'inv{i}': v for i, v in enumerate(invoices)}
            inv_placeholders = ','.join([f':inv{i}' for i in range(len(invoices))])
            query = f"""
                SELECT SUM(st_ovalue) FROM stran
                WHERE st_account = :account AND st_ref IN ({inv_placeholders})
            """
            with sql_connector.engine.connect() as conn:
                result = conn.execute(sa_text(query), {**inv_params, 'account': opera_account})
                row = result.fetchone()
                if row and row[0]:
                    amount = int(round(float(row[0]) * 100))  # Convert to pence
                else:
                    return {"success": False, "error": "Could not find specified invoices"}

        if amount <= 0:
            return {"success": False, "error": "Amount must be greater than zero"}

        # Get API settings
        settings = _load_gocardless_settings()

        # Build description — kept short for bank statement visibility (~18 chars on BACS).
        # Full invoice list is always in metadata.invoices regardless.
        stmt_ref = (settings.get("request_statement_reference") or "").strip()[:10]
        if not description:
            if len(invoices) == 1:
                inv_part = invoices[0]
            else:
                inv_part = f"{invoices[0]} +{len(invoices) - 1}"
            if stmt_ref:
                description = f"{stmt_ref} {inv_part}"
            else:
                description = inv_part
        elif stmt_ref and not description.startswith(stmt_ref):
            description = f"{stmt_ref} {description}"
        access_token = settings.get("api_access_token")

        if not access_token:
            return {"success": False, "error": "GoCardless API not configured"}

        from sql_rag.gocardless_api import GoCardlessClient
        sandbox = settings.get("api_sandbox", False)
        client = GoCardlessClient(access_token=access_token, sandbox=sandbox)

        # Create payment in GoCardless
        try:
            gc_payment = client.create_payment(
                amount_pence=amount,
                mandate_id=mandate['mandate_id'],
                description=description,
                charge_date=charge_date,
                metadata={
                    "opera_account": opera_account,
                    "invoices": ",".join(invoices)
                }
            )
        except Exception as gc_err:
            return {"success": False, "error": f"GoCardless API error: {str(gc_err)}"}

        # Record in local database
        payment_request = payments_db.create_payment_request(
            mandate_id=mandate['mandate_id'],
            opera_account=opera_account,
            amount_pence=amount,
            invoice_refs=invoices,
            payment_id=gc_payment.get("id"),
            charge_date=gc_payment.get("charge_date"),
            description=description
        )

        # Update status based on GC response
        gc_status = gc_payment.get("status", "pending")
        if gc_status != "pending":
            payments_db.update_payment_request(
                payment_request['id'],
                status=gc_status
            )
            payment_request['status'] = gc_status

        # Calculate estimated arrival (typically charge_date + 2-4 working days)
        estimated_arrival = None
        if gc_payment.get("charge_date"):
            from datetime import timedelta
            cd = datetime.strptime(gc_payment["charge_date"], "%Y-%m-%d").date()
            # Add 3 working days (rough estimate)
            estimated_arrival = (cd + timedelta(days=5)).isoformat()

        return {
            "success": True,
            "message": f"Payment of £{amount/100:.2f} requested for customer {opera_account}",
            "payment_request": {
                **payment_request,
                "customer_name": mandate.get('opera_name', opera_account),
                "estimated_arrival": estimated_arrival
            }
        }
    except Exception as e:
        logger.error(f"Error requesting payment: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/gocardless/payment-requests/bulk")
async def request_bulk_payments(request: Request):
    """
    Request multiple payments at once.
    Each request should have: opera_account, invoices, amount (optional)
    """
    body = await request.json()
    # Accept both {"requests": [...]} and bare [...]
    payment_requests = body.get("requests", body) if isinstance(body, dict) else body

    results = []
    success_count = 0
    fail_count = 0

    for req in payment_requests:
        try:
            # Call single payment endpoint logic
            result = await request_gocardless_payment(
                opera_account=req.get("opera_account"),
                invoices=req.get("invoices", []),
                amount=req.get("amount"),
                charge_date=req.get("charge_date"),
                description=req.get("description")
            )
            results.append({
                "opera_account": req.get("opera_account"),
                **result
            })
            if result.get("success"):
                success_count += 1
            else:
                fail_count += 1
        except Exception as e:
            results.append({
                "opera_account": req.get("opera_account"),
                "success": False,
                "error": str(e)
            })
            fail_count += 1

    return {
        "success": fail_count == 0,
        "results": results,
        "summary": {
            "total": len(payment_requests),
            "succeeded": success_count,
            "failed": fail_count
        }
    }


@router.get("/api/gocardless/payment-requests/{request_id}")
async def get_payment_request(request_id: int):
    """Get details of a specific payment request."""
    try:
        payments_db = get_payments_db()
        request = payments_db.get_payment_request(request_id)
        if not request:
            return {"success": False, "error": "Payment request not found"}

        # Get customer name from mandate
        mandate = payments_db.get_mandate_for_customer(request['opera_account'])
        if mandate:
            request['customer_name'] = mandate.get('opera_name', request['opera_account'])

        return {"success": True, "payment_request": request}
    except Exception as e:
        logger.error(f"Error getting payment request: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/gocardless/payment-requests/{request_id}/cancel")
async def cancel_payment_request(request_id: int):
    """
    Cancel a pending payment request.
    Also cancels the payment in GoCardless if possible.
    """
    try:
        payments_db = get_payments_db()
        request = payments_db.get_payment_request(request_id)

        if not request:
            return {"success": False, "error": "Payment request not found"}

        if request['status'] not in ('pending', 'pending_submission', 'pending_customer_approval'):
            return {
                "success": False,
                "error": f"Cannot cancel payment with status '{request['status']}'"
            }

        # Try to cancel in GoCardless
        if request['payment_id']:
            settings = _load_gocardless_settings()
            access_token = settings.get("api_access_token")

            if access_token:
                from sql_rag.gocardless_api import GoCardlessClient
                sandbox = settings.get("api_sandbox", False)
                client = GoCardlessClient(access_token=access_token, sandbox=sandbox)

                try:
                    client.cancel_payment(request['payment_id'])
                except Exception as gc_err:
                    logger.warning(f"Could not cancel payment in GoCardless: {gc_err}")
                    # Continue anyway - mark as cancelled locally

        # Mark as cancelled locally
        payments_db.cancel_payment_request(request_id, "Cancelled by user")

        return {
            "success": True,
            "message": f"Payment request {request_id} cancelled"
        }
    except Exception as e:
        logger.error(f"Error cancelling payment request: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/gocardless/payment-requests/sync")
async def sync_payment_statuses():
    """
    Sync payment statuses from GoCardless API.
    Updates local records with current status from GoCardless.
    """
    try:
        payments_db = get_payments_db()

        # Get all non-final payment requests
        pending_statuses = ['pending', 'pending_submission', 'pending_customer_approval',
                          'submitted', 'confirmed']

        requests_to_sync = []
        for status in pending_statuses:
            requests_to_sync.extend(payments_db.list_payment_requests(status=status))

        if not requests_to_sync:
            return {"success": True, "message": "No pending payments to sync", "updated": 0}

        settings = _load_gocardless_settings()
        access_token = settings.get("api_access_token")

        if not access_token:
            return {"success": False, "error": "GoCardless API not configured"}

        from sql_rag.gocardless_api import GoCardlessClient
        sandbox = settings.get("api_sandbox", False)
        client = GoCardlessClient(access_token=access_token, sandbox=sandbox)

        updated_count = 0
        for req in requests_to_sync:
            if not req['payment_id']:
                continue

            try:
                gc_payment = client.get_payment(req['payment_id'])
                gc_status = gc_payment.get("status")
                gc_charge_date = gc_payment.get("charge_date")

                if gc_status and gc_status != req['status']:
                    payments_db.update_payment_request(
                        req['id'],
                        status=gc_status,
                        charge_date=gc_charge_date
                    )
                    updated_count += 1
                    logger.info(f"Updated payment {req['payment_id']} status: {req['status']} -> {gc_status}")

            except Exception as e:
                logger.warning(f"Could not sync payment {req['payment_id']}: {e}")

        return {
            "success": True,
            "message": f"Synced {updated_count} payment statuses",
            "total_checked": len(requests_to_sync),
            "updated": updated_count
        }
    except Exception as e:
        logger.error(f"Error syncing payment statuses: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/gocardless/repeat-documents")
async def get_gocardless_repeat_documents(
    require_mandate: bool = Query(True, description="If false, return docs for all customers (not just those with mandates)")
):
    """
    List Opera repeat documents (ih_docstat='U') suitable for GoCardless subscriptions.
    Shows all repeat documents for customers with active GC mandates (default).
    Pass require_mandate=false to include all customers (for linking existing subscriptions).
    """
    try:
        if not sql_connector:
            return {"success": False, "error": "Database not connected"}

        gc_settings = _load_gocardless_settings()
        sub_tag = gc_settings.get("subscription_tag", "SUB")

        payments_db = get_payments_db()
        mandates = payments_db.list_mandates(status='active')
        mandate_lookup = {m['opera_account']: m for m in mandates}

        query = """
            SELECT
                ih_doc, ih_account, ih_name, ih_ignore, ih_dcontr,
                ih_scontr, ih_econtr, ih_job, ih_analsys,
                ih_custref, ih_narr1,
                COALESCE(lines.line_nett, 0) AS line_nett,
                COALESCE(lines.line_vat, 0) AS line_vat
            FROM ihead
            LEFT JOIN (
                SELECT it_doc, SUM(it_exvat) AS line_nett, SUM(it_vatval) AS line_vat
                FROM itran
                GROUP BY it_doc
            ) lines ON lines.it_doc = ih_doc
            WHERE ih_docstat = 'U'
              AND (ih_econtr IS NULL OR ih_econtr >= GETDATE())
            ORDER BY ih_account, ih_doc
        """

        # Build lookup of active GC subscriptions by (account, amount_pence)
        # for auto-matching to repeat documents
        all_subs = payments_db.list_subscriptions()
        subs_by_account: Dict[str, List[Dict]] = {}
        for s in all_subs:
            if s['opera_account'] and s['status'] in ('active', 'paused'):
                subs_by_account.setdefault(s['opera_account'], []).append(s)

        documents = []

        result = sql_connector.execute_query(query)
        if result is not None and not result.empty:
          for _, row in result.iterrows():
                doc_ref = (row['ih_doc'] or '').strip()
                account = (row['ih_account'] or '').strip()
                name = (row['ih_name'] or '').strip()
                freq_code = (row['ih_ignore'] or 'M').strip()
                days_between = row['ih_dcontr'] or 0
                start_date = row['ih_scontr']
                end_date = row['ih_econtr']
                dept = (row['ih_job'] or '').strip()
                analsys = (row['ih_analsys'] or '').strip()
                # Use itran line totals (stored in pence) for accurate amounts
                line_nett_pence = float(row['line_nett']) if row['line_nett'] else 0
                line_vat_pence = float(row['line_vat']) if row['line_vat'] else 0
                ex_vat = line_nett_pence / 100.0
                vat = line_vat_pence / 100.0
                cust_ref = (row['ih_custref'] or '').strip()
                narr = (row['ih_narr1'] or '').strip()
                is_sub_tagged = analsys == sub_tag

                total_inc_vat = ex_vat + vat
                amount_pence = int(round(line_nett_pence + line_vat_pence))

                interval_unit, interval_count = FREQUENCY_MAP.get(freq_code, ('monthly', 1))

                # Frequency label — all Opera repeat invoice frequency codes
                freq_labels = {
                    'W': 'Weekly', 'F': 'Fortnightly', 'M': 'Monthly',
                    'B': 'Bi-monthly', 'Q': 'Quarterly', 'H': 'Half-yearly',
                    'A': 'Annual', 'D': f'Every {days_between} days',
                }
                frequency = freq_labels.get(freq_code, freq_code)

                mandate = mandate_lookup.get(account)
                if require_mandate and not mandate:
                    continue

                # Check if already linked to a subscription via source_doc
                existing_sub = payments_db.get_subscription_by_source_doc(doc_ref)

                # If not linked, find matching GC subscription by account + amount
                matching_sub = None
                if not existing_sub:
                    account_subs = subs_by_account.get(account, [])
                    for s in account_subs:
                        if s['amount_pence'] == amount_pence and not s['source_doc']:
                            matching_sub = s
                            break
                    # If no exact match, try close match (within £1)
                    if not matching_sub:
                        for s in account_subs:
                            if abs(s['amount_pence'] - amount_pence) <= 100 and not s['source_doc']:
                                matching_sub = s
                                break

                # Detect mismatch between Opera document and linked subscription
                mismatch = None
                if existing_sub:
                    mismatches = []
                    if existing_sub['amount_pence'] != amount_pence:
                        mismatches.append(f"Amount: subscription £{existing_sub['amount_pence']/100:,.2f} vs document £{total_inc_vat:,.2f}")
                    if existing_sub['interval_unit'] != interval_unit or existing_sub.get('interval_count', 1) != interval_count:
                        sub_freq = existing_sub.get('frequency_label', existing_sub['interval_unit'])
                        mismatches.append(f"Frequency: subscription {sub_freq} vs document {frequency}")
                    if mismatches:
                        mismatch = {
                            'details': mismatches,
                            'sub_amount_pence': existing_sub['amount_pence'],
                            'sub_amount_formatted': existing_sub.get('amount_formatted', f"£{existing_sub['amount_pence']/100:,.2f}"),
                            'doc_amount_pence': amount_pence,
                            'doc_amount_formatted': f"£{total_inc_vat:,.2f}",
                        }

                documents.append({
                    'doc_ref': doc_ref,
                    'opera_account': account,
                    'customer_name': name,
                    'frequency_code': freq_code,
                    'frequency': frequency,
                    'interval_unit': interval_unit,
                    'interval_count': interval_count,
                    'start_date': start_date.isoformat() if hasattr(start_date, 'isoformat') else str(start_date) if start_date else None,
                    'end_date': end_date.isoformat() if hasattr(end_date, 'isoformat') else str(end_date) if end_date else None,
                    'ex_vat': ex_vat,
                    'vat': vat,
                    'total_inc_vat': total_inc_vat,
                    'amount_formatted': f"£{total_inc_vat:,.2f}",
                    'amount_pence': amount_pence,
                    'customer_ref': cust_ref,
                    'narration': narr,
                    'is_sub_tagged': is_sub_tagged,
                    'department': dept,
                    'has_mandate': mandate is not None,
                    'mandate_id': mandate['mandate_id'] if mandate else None,
                    'has_subscription': existing_sub is not None,
                    'subscription_id': existing_sub['subscription_id'] if existing_sub else None,
                    'subscription_status': existing_sub['status'] if existing_sub else None,
                    'mismatch': mismatch,
                    # Matching GC subscription that can be linked
                    'matching_subscription': {
                        'subscription_id': matching_sub['subscription_id'],
                        'name': matching_sub['name'],
                        'amount_formatted': matching_sub['amount_formatted'],
                        'status': matching_sub['status'],
                    } if matching_sub else None,
                })

        return {
            "success": True,
            "documents": documents,
            "count": len(documents),
            "with_mandate": sum(1 for d in documents if d['has_mandate']),
            "with_subscription": sum(1 for d in documents if d['has_subscription']),
            "with_match": sum(1 for d in documents if d['matching_subscription']),
        }
    except Exception as e:
        logger.error(f"Error getting repeat documents: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/gocardless/subscriptions/link")
async def link_subscription_to_document(request: Request):
    """
    Link an Opera repeat document to a GoCardless subscription.
    Supports multiple documents per subscription (adds to existing links).

    Request body:
        subscription_id: GoCardless subscription ID
        source_doc: Opera repeat document reference (ih_doc)
    """
    try:
        body = await request.json()
        subscription_id = body.get("subscription_id")
        source_doc = body.get("source_doc")

        if not subscription_id or not source_doc:
            return {"success": False, "error": "subscription_id and source_doc are required"}

        payments_db = get_payments_db()

        # Check subscription exists locally
        sub = payments_db.get_subscription(subscription_id)
        if not sub:
            return {"success": False, "error": f"Subscription {subscription_id} not found locally. Sync first."}

        # Check this doc isn't already linked to a DIFFERENT subscription
        existing_subs = payments_db.get_subscriptions_by_source_doc(source_doc)
        for existing in existing_subs:
            if existing['subscription_id'] != subscription_id:
                return {"success": False, "error": f"Document {source_doc} already linked to subscription {existing['subscription_id']}"}

        # Add document link
        added = payments_db.add_subscription_document(subscription_id, source_doc)
        if not added:
            return {"success": False, "error": f"Document {source_doc} is already linked to this subscription"}

        logger.info(f"Linked subscription {subscription_id} to repeat document {source_doc}")

        return {
            "success": True,
            "subscription": payments_db.get_subscription(subscription_id),
        }
    except Exception as e:
        logger.error(f"Error linking subscription: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/gocardless/subscriptions/unlink")
async def unlink_subscription_from_document(request: Request):
    """
    Unlink an Opera repeat document from a GoCardless subscription.
    If source_doc is provided, removes that specific link.
    If source_doc is not provided, removes ALL document links.
    """
    try:
        body = await request.json()
        subscription_id = body.get("subscription_id")
        source_doc = body.get("source_doc")

        if not subscription_id:
            return {"success": False, "error": "subscription_id is required"}

        payments_db = get_payments_db()

        sub = payments_db.get_subscription(subscription_id)
        if not sub:
            return {"success": False, "error": f"Subscription {subscription_id} not found"}

        if source_doc:
            # Remove specific document link
            removed = payments_db.remove_subscription_document(subscription_id, source_doc)
            if not removed:
                return {"success": False, "error": f"Document {source_doc} is not linked to this subscription"}
            logger.info(f"Unlinked subscription {subscription_id} from document {source_doc}")
        else:
            # Remove all document links
            for doc in sub.get('source_docs', []):
                payments_db.remove_subscription_document(subscription_id, doc)
            logger.info(f"Unlinked subscription {subscription_id} from all documents")

        return {
            "success": True,
            "subscription": payments_db.get_subscription(subscription_id),
        }
    except Exception as e:
        logger.error(f"Error unlinking subscription: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/gocardless/subscriptions")
async def list_gocardless_subscriptions(
    status: Optional[str] = Query(None, description="Filter by status"),
    opera_account: Optional[str] = Query(None, description="Filter by Opera account")
):
    """List GoCardless subscriptions with Opera enrichment and mismatch detection."""
    try:
        gc_settings = _load_gocardless_settings()
        sub_tag = gc_settings.get("subscription_tag", "SUB")

        payments_db = get_payments_db()
        subscriptions = payments_db.list_subscriptions(status=status, opera_account=opera_account)

        # Enrich subscriptions with customer names from mandates where missing
        mandates = payments_db.list_mandates()
        mandate_lookup = {m['mandate_id']: m for m in mandates if m.get('opera_account') and m['opera_account'] != '__UNLINKED__'}
        for sub in subscriptions:
            if not sub.get('opera_name') and sub.get('mandate_id'):
                linked_mandate = mandate_lookup.get(sub['mandate_id'])
                if linked_mandate:
                    sub['opera_account'] = linked_mandate['opera_account']
                    sub['opera_name'] = linked_mandate['opera_name']
                    sub['customer_from_mandate'] = True

        # Enrich with Opera document data for linked subscriptions (multi-doc)
        all_docs = set()
        for s in subscriptions:
            for doc in s.get('source_docs', []):
                all_docs.add(doc)
            # Legacy fallback
            if s.get('source_doc') and s['source_doc'] not in all_docs:
                all_docs.add(s['source_doc'])
        all_docs = list(all_docs)

        opera_docs = {}
        if all_docs and sql_connector:
            placeholders = ','.join([f':d{i}' for i in range(len(all_docs))])
            params = {f'd{i}': d for i, d in enumerate(all_docs)}
            query = f"""
                SELECT ih_doc, ih_ignore, ih_dcontr, ih_analsys,
                    COALESCE(lines.line_nett, 0) AS line_nett,
                    COALESCE(lines.line_vat, 0) AS line_vat
                FROM ihead
                LEFT JOIN (
                    SELECT it_doc, SUM(it_exvat) AS line_nett, SUM(it_vatval) AS line_vat
                    FROM itran
                    GROUP BY it_doc
                ) lines ON lines.it_doc = ih_doc
                WHERE ih_doc IN ({placeholders}) AND ih_docstat = 'U'
            """
            result = sql_connector.execute_query(query, params)
            if result is not None and not result.empty:
                for _, row in result.iterrows():
                    doc = (row['ih_doc'] or '').strip()
                    line_nett_pence = float(row['line_nett']) if row['line_nett'] else 0
                    line_vat_pence = float(row['line_vat']) if row['line_vat'] else 0
                    ex_vat = line_nett_pence / 100.0
                    vat = line_vat_pence / 100.0
                    total = ex_vat + vat
                    freq_code = (row['ih_ignore'] or 'M').strip()
                    interval_unit, interval_count = FREQUENCY_MAP.get(freq_code, ('monthly', 1))
                    freq_labels = {'W': 'Weekly', 'F': 'Fortnightly', 'M': 'Monthly', 'B': 'Bi-monthly', 'Q': 'Quarterly', 'H': 'Half-yearly', 'A': 'Annual'}
                    opera_docs[doc] = {
                        'doc_ref': doc,
                        'ex_vat': ex_vat,
                        'vat': vat,
                        'total_inc_vat': total,
                        'amount_pence': int(round(line_nett_pence + line_vat_pence)),
                        'amount_formatted': f"£{total:,.2f}",
                        'frequency_code': freq_code,
                        'frequency': freq_labels.get(freq_code, freq_code),
                        'interval_unit': interval_unit,
                        'interval_count': interval_count,
                        'has_sub_tag': (row['ih_analsys'] or '').strip() == sub_tag,
                    }

        for sub in subscriptions:
            linked_docs = sub.get('source_docs', [])
            # Build per-document detail list
            doc_details = []
            total_opera_pence = 0
            for doc in linked_docs:
                if doc in opera_docs:
                    doc_details.append(opera_docs[doc])
                    total_opera_pence += opera_docs[doc]['amount_pence']

            sub['linked_documents'] = doc_details
            sub['linked_document_count'] = len(linked_docs)

            if doc_details:
                sub['opera_amount_pence'] = total_opera_pence
                sub['opera_amount_formatted'] = f"£{total_opera_pence / 100:,.2f}"
                # Use frequency from first doc (they should all match)
                sub['opera_frequency'] = doc_details[0]['frequency']
                sub['has_sub_tag'] = all(d.get('has_sub_tag') for d in doc_details)
                # Detect mismatch (GC amount vs sum of all linked docs)
                mismatches = []
                if sub['amount_pence'] != total_opera_pence:
                    mismatches.append(f"Amount: GC {sub.get('amount_formatted', '?')} vs Opera £{total_opera_pence / 100:,.2f} ({len(doc_details)} doc{'s' if len(doc_details) > 1 else ''})")
                if sub['interval_unit'] != doc_details[0]['interval_unit'] or sub.get('interval_count', 1) != doc_details[0]['interval_count']:
                    mismatches.append(f"Frequency: GC {sub.get('frequency', sub['interval_unit'])} vs Opera {doc_details[0]['frequency']}")
                sub['mismatch'] = {'details': mismatches} if mismatches else None
            else:
                sub['opera_amount_pence'] = None
                sub['opera_amount_formatted'] = None
                sub['opera_frequency'] = None
                sub['mismatch'] = None
                sub['has_sub_tag'] = None

        return {
            "success": True,
            "subscriptions": subscriptions,
            "count": len(subscriptions),
            "with_mismatch": sum(1 for s in subscriptions if s.get('mismatch')),
        }
    except Exception as e:
        logger.error(f"Error listing subscriptions: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/gocardless/subscriptions")
async def create_gocardless_subscription(request: Request):
    """
    Create a GoCardless subscription from Opera repeat document(s).

    Request body:
        source_doc: ih_doc reference (single document) — OR —
        source_docs: list of ih_doc references (multiple documents)
        day_of_month: (optional) day of month to charge (1-28)
        start_date: (optional) first charge date YYYY-MM-DD
    """
    try:
        gc_settings = _load_gocardless_settings()
        sub_tag = gc_settings.get("subscription_tag", "SUB")

        body = await request.json()
        # Accept either source_doc (single) or source_docs (list)
        source_docs = body.get("source_docs", [])
        if not source_docs:
            single = body.get("source_doc")
            if single:
                source_docs = [single]
        day_of_month = body.get("day_of_month")
        start_date = body.get("start_date")

        if not source_docs:
            return {"success": False, "error": "source_doc or source_docs is required"}

        if not sql_connector:
            return {"success": False, "error": "Database not connected"}

        # 1. Read repeat documents from Opera
        placeholders = ','.join([f':d{i}' for i in range(len(source_docs))])
        params = {f'd{i}': d for i, d in enumerate(source_docs)}
        params['sub_tag'] = sub_tag
        query = f"""
            SELECT ih_doc, ih_account, ih_name, ih_ignore, ih_scontr, ih_econtr,
                   ih_job, ih_exvat, ih_vat, ih_custref
            FROM ihead
            WHERE ih_doc IN ({placeholders}) AND ih_docstat = 'U' AND RTRIM(ih_analsys) = :sub_tag
        """

        result = sql_connector.execute_query(query, params=params)

        if result is None or result.empty:
            return {"success": False, "error": f"No repeat documents found or not marked as {sub_tag}"}

        # Validate all docs belong to same customer
        accounts = set()
        total_amount_pence = 0
        doc_refs = []
        for _, row in result.iterrows():
            doc_ref = (row['ih_doc'] or '').strip()
            account = (row['ih_account'] or '').strip()
            accounts.add(account)
            doc_refs.append(doc_ref)

        if len(accounts) > 1:
            return {"success": False, "error": f"All documents must belong to the same customer. Found: {', '.join(accounts)}"}

        account = accounts.pop()

        # Calculate total amount from itran lines for all docs
        line_placeholders = ','.join([f':ld{i}' for i in range(len(doc_refs))])
        line_params = {f'ld{i}': d for i, d in enumerate(doc_refs)}
        line_query = f"""
            SELECT COALESCE(SUM(it_exvat), 0) AS line_nett, COALESCE(SUM(it_vatval), 0) AS line_vat
            FROM itran WHERE it_doc IN ({line_placeholders})
        """
        line_result = sql_connector.execute_query(line_query, line_params)
        if line_result is not None and not line_result.empty:
            total_amount_pence = int(round(float(line_result.iloc[0]['line_nett']) + float(line_result.iloc[0]['line_vat'])))

        if total_amount_pence <= 0:
            return {"success": False, "error": f"Invalid total amount: £{total_amount_pence/100:.2f}"}

        # Use first doc for name/frequency
        first_row = result.iloc[0]
        name = (first_row['ih_name'] or '').strip()
        freq_code = (first_row['ih_ignore'] or 'M').strip()
        cust_ref = (first_row['ih_custref'] or '').strip()
        interval_unit, interval_count = FREQUENCY_MAP.get(freq_code, ('monthly', 1))

        # 2. Look up customer's active mandate
        payments_db = get_payments_db()
        mandate = payments_db.get_mandate_for_customer(account)

        if not mandate:
            return {"success": False, "error": f"No active GoCardless mandate for customer {account} ({name})"}

        # 3. Check no doc is already linked to another subscription
        for doc_ref in doc_refs:
            existing_subs = payments_db.get_subscriptions_by_source_doc(doc_ref)
            if existing_subs:
                return {
                    "success": False,
                    "error": f"Document {doc_ref} already linked to subscription {existing_subs[0]['subscription_id']} (status: {existing_subs[0]['status']})"
                }

        # 4. Create subscription in GoCardless
        settings = _load_gocardless_settings()
        access_token = settings.get("api_access_token")
        if not access_token:
            return {"success": False, "error": "GoCardless API not configured"}

        from sql_rag.gocardless_api import GoCardlessClient
        sandbox = settings.get("api_sandbox", False)
        client = GoCardlessClient(access_token=access_token, sandbox=sandbox)

        sub_name = f"{name} - {cust_ref}" if cust_ref else name
        metadata = {
            "opera_account": account,
            "source_docs": ",".join(doc_refs),
        }

        gc_sub = client.create_subscription(
            mandate_id=mandate['mandate_id'],
            amount_pence=total_amount_pence,
            interval_unit=interval_unit,
            interval=interval_count,
            day_of_month=day_of_month,
            name=sub_name,
            start_date=start_date,
            metadata=metadata,
        )

        # 5. Store locally with first doc as legacy source_doc
        sub_record = payments_db.save_subscription(
            subscription_id=gc_sub.get("id", ""),
            mandate_id=mandate['mandate_id'],
            amount_pence=total_amount_pence,
            interval_unit=interval_unit,
            interval_count=interval_count,
            opera_account=account,
            opera_name=name,
            source_doc=doc_refs[0],
            day_of_month=gc_sub.get("day_of_month"),
            name=sub_name,
            status=gc_sub.get("status", "active"),
            start_date=gc_sub.get("start_date"),
            end_date=gc_sub.get("end_date"),
        )

        # 6. Link all documents via junction table
        gc_sub_id = gc_sub.get("id", "")
        for doc_ref in doc_refs:
            payments_db.add_subscription_document(gc_sub_id, doc_ref)

        logger.info(f"Created GC subscription {gc_sub_id} for {account} from {len(doc_refs)} doc(s): {', '.join(doc_refs)}")

        return {
            "success": True,
            "subscription": payments_db.get_subscription(gc_sub_id),
            "gc_response": gc_sub,
        }
    except Exception as e:
        logger.error(f"Error creating subscription: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/gocardless/subscriptions/{subscription_id}")
async def get_gocardless_subscription(subscription_id: str):
    """Get subscription details."""
    try:
        payments_db = get_payments_db()
        sub = payments_db.get_subscription(subscription_id)
        if not sub:
            return {"success": False, "error": f"Subscription {subscription_id} not found"}

        return {"success": True, "subscription": sub}
    except Exception as e:
        logger.error(f"Error getting subscription: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/gocardless/subscriptions/{subscription_id}/sync-from-opera")
async def sync_subscription_from_opera(subscription_id: str):
    """
    Update a GoCardless subscription amount from its linked Opera repeat documents.
    Reads the current document values (sum of all linked docs) and pushes the new amount to GoCardless.
    """
    try:
        payments_db = get_payments_db()
        sub = payments_db.get_subscription(subscription_id)
        if not sub:
            return {"success": False, "error": f"Subscription {subscription_id} not found"}

        source_docs = sub.get('source_docs', [])
        if not source_docs:
            return {"success": False, "error": "Subscription is not linked to any Opera documents"}

        if not sql_connector:
            return {"success": False, "error": "Database not connected"}

        # Read current Opera document values from itran lines (amounts in pence) for ALL linked docs
        placeholders = ','.join([f':d{i}' for i in range(len(source_docs))])
        params = {f'd{i}': d for i, d in enumerate(source_docs)}
        query = f"""
            SELECT
                COALESCE(SUM(it_exvat), 0) AS line_nett,
                COALESCE(SUM(it_vatval), 0) AS line_vat
            FROM itran
            WHERE it_doc IN ({placeholders})
        """
        result = sql_connector.execute_query(query, params)
        if result is None or result.empty or (float(result.iloc[0]['line_nett']) == 0 and float(result.iloc[0]['line_vat']) == 0):
            return {"success": False, "error": f"Opera documents not found or have no lines"}

        row = result.iloc[0]
        new_amount_pence = int(round(float(row['line_nett']) + float(row['line_vat'])))

        old_amount_pence = sub['amount_pence']
        if new_amount_pence == old_amount_pence:
            return {"success": True, "message": "No change needed — amounts already match"}

        # Update GoCardless subscription via API
        settings = _load_gocardless_settings()
        access_token = settings.get("api_access_token")
        if not access_token:
            return {"success": False, "error": "GoCardless API not configured"}

        from sql_rag.gocardless_api import GoCardlessClient
        sandbox = settings.get("api_sandbox", False)
        client = GoCardlessClient(access_token=access_token, sandbox=sandbox)

        gc_sub = client.update_subscription(subscription_id, amount_pence=new_amount_pence)

        # Update local record
        payments_db.save_subscription(
            subscription_id=subscription_id,
            mandate_id=sub['mandate_id'],
            amount_pence=new_amount_pence,
            interval_unit=sub['interval_unit'],
            interval_count=sub['interval_count'],
        )

        logger.info(f"Updated subscription {subscription_id} amount from £{old_amount_pence/100:.2f} to £{new_amount_pence/100:.2f} (from {len(source_docs)} doc(s))")

        return {
            "success": True,
            "old_amount_pence": old_amount_pence,
            "new_amount_pence": new_amount_pence,
            "old_amount_formatted": f"£{old_amount_pence/100:,.2f}",
            "new_amount_formatted": f"£{new_amount_pence/100:,.2f}",
            "subscription": payments_db.get_subscription(subscription_id),
        }
    except Exception as e:
        logger.error(f"Error syncing subscription from Opera: {e}")
        return {"success": False, "error": str(e)}


@router.put("/api/gocardless/subscriptions/{subscription_id}")
async def update_gocardless_subscription(subscription_id: str, request: Request):
    """Update a subscription (name/amount only)."""
    try:
        body = await request.json()
        name = body.get("name")
        amount_pence = body.get("amount_pence")

        settings = _load_gocardless_settings()
        access_token = settings.get("api_access_token")
        if not access_token:
            return {"success": False, "error": "GoCardless API not configured"}

        from sql_rag.gocardless_api import GoCardlessClient
        sandbox = settings.get("api_sandbox", False)
        client = GoCardlessClient(access_token=access_token, sandbox=sandbox)

        gc_sub = client.update_subscription(
            subscription_id,
            name=name,
            amount_pence=amount_pence,
        )

        # Update local record
        payments_db = get_payments_db()
        local = payments_db.get_subscription(subscription_id)
        if local:
            payments_db.save_subscription(
                subscription_id=subscription_id,
                mandate_id=local['mandate_id'],
                amount_pence=amount_pence or local['amount_pence'],
                interval_unit=local['interval_unit'],
                interval_count=local['interval_count'],
                name=name or local['name'],
                status=gc_sub.get("status", local['status']),
            )

        return {
            "success": True,
            "subscription": payments_db.get_subscription(subscription_id),
        }
    except Exception as e:
        logger.error(f"Error updating subscription: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/gocardless/subscriptions/{subscription_id}/pause")
async def pause_gocardless_subscription(subscription_id: str):
    """Pause an active subscription."""
    try:
        settings = _load_gocardless_settings()
        access_token = settings.get("api_access_token")
        if not access_token:
            return {"success": False, "error": "GoCardless API not configured"}

        from sql_rag.gocardless_api import GoCardlessClient
        sandbox = settings.get("api_sandbox", False)
        client = GoCardlessClient(access_token=access_token, sandbox=sandbox)

        gc_sub = client.pause_subscription(subscription_id)

        payments_db = get_payments_db()
        payments_db.update_subscription_status(subscription_id, gc_sub.get("status", "paused"))

        return {
            "success": True,
            "subscription": payments_db.get_subscription(subscription_id),
        }
    except Exception as e:
        logger.error(f"Error pausing subscription: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/gocardless/subscriptions/{subscription_id}/resume")
async def resume_gocardless_subscription(subscription_id: str):
    """Resume a paused subscription."""
    try:
        settings = _load_gocardless_settings()
        access_token = settings.get("api_access_token")
        if not access_token:
            return {"success": False, "error": "GoCardless API not configured"}

        from sql_rag.gocardless_api import GoCardlessClient
        sandbox = settings.get("api_sandbox", False)
        client = GoCardlessClient(access_token=access_token, sandbox=sandbox)

        gc_sub = client.resume_subscription(subscription_id)

        payments_db = get_payments_db()
        payments_db.update_subscription_status(subscription_id, gc_sub.get("status", "active"))

        return {
            "success": True,
            "subscription": payments_db.get_subscription(subscription_id),
        }
    except Exception as e:
        logger.error(f"Error resuming subscription: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/gocardless/subscriptions/{subscription_id}/cancel")
async def cancel_gocardless_subscription(subscription_id: str):
    """Cancel a subscription. Cannot be undone."""
    try:
        settings = _load_gocardless_settings()
        access_token = settings.get("api_access_token")
        if not access_token:
            return {"success": False, "error": "GoCardless API not configured"}

        from sql_rag.gocardless_api import GoCardlessClient
        sandbox = settings.get("api_sandbox", False)
        client = GoCardlessClient(access_token=access_token, sandbox=sandbox)

        gc_sub = client.cancel_subscription(subscription_id)

        payments_db = get_payments_db()
        payments_db.update_subscription_status(subscription_id, gc_sub.get("status", "cancelled"))

        return {
            "success": True,
            "subscription": payments_db.get_subscription(subscription_id),
        }
    except Exception as e:
        logger.error(f"Error cancelling subscription: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/gocardless/subscriptions/sync")
async def sync_gocardless_subscriptions():
    """
    Sync all subscriptions from GoCardless API to local database.
    Resolves mandate → customer via local mandates table.
    """
    try:
        settings = _load_gocardless_settings()
        access_token = settings.get("api_access_token")
        if not access_token:
            return {"success": False, "error": "GoCardless API not configured"}

        from sql_rag.gocardless_api import GoCardlessClient
        sandbox = settings.get("api_sandbox", False)
        client = GoCardlessClient(access_token=access_token, sandbox=sandbox)

        payments_db = get_payments_db()

        # Build mandate → Opera account lookup from local mandates
        mandates = payments_db.list_mandates()
        mandate_to_opera = {}
        for m in mandates:
            acct = m['opera_account']
            mandate_to_opera[m['mandate_id']] = {
                'opera_account': acct if acct and acct != '__UNLINKED__' else None,
                'opera_name': m['opera_name'],
            }

        # Build customer_id → Opera account lookup for resolving subscriptions via GC customer
        customer_id_to_opera = {}
        for m in mandates:
            cid = m.get('gocardless_customer_id')
            acct = m['opera_account']
            if cid and acct and acct != '__UNLINKED__':
                customer_id_to_opera[cid] = {
                    'opera_account': acct,
                    'opera_name': m['opera_name'],
                }

        # Cache GC customer lookups to avoid repeat API calls
        gc_customer_cache = {}  # customer_id → name
        gc_mandate_cache = {}   # mandate_id → customer_id

        synced = 0
        updated = 0
        cursor = None

        while True:
            gc_subs, cursor = client.list_subscriptions(limit=100, cursor=cursor)

            if not gc_subs:
                break

            for gc_sub in gc_subs:
                sub_id = gc_sub.get("id", "")
                mandate_id = gc_sub.get("links", {}).get("mandate", "")

                # Try local mandate lookup first
                opera_info = mandate_to_opera.get(mandate_id, {})

                # If no name yet, resolve customer via mandate → customer from GoCardless API
                if not opera_info.get('opera_name') and mandate_id:
                    try:
                        # Get customer_id from mandate (cached)
                        if mandate_id in gc_mandate_cache:
                            customer_id = gc_mandate_cache[mandate_id]
                        else:
                            gc_mandate = client.get_mandate(mandate_id)
                            customer_id = gc_mandate.get("links", {}).get("customer", "") if gc_mandate else ""
                            gc_mandate_cache[mandate_id] = customer_id

                        if customer_id:
                            # Check local lookup first
                            known = customer_id_to_opera.get(customer_id)
                            if known:
                                opera_info = known
                            else:
                                # Get customer name from GoCardless (cached)
                                if customer_id in gc_customer_cache:
                                    gc_name = gc_customer_cache[customer_id]
                                else:
                                    gc_cust = client.get_customer(customer_id)
                                    gc_name = gc_cust.get("company_name") or \
                                              f"{gc_cust.get('given_name', '')} {gc_cust.get('family_name', '')}".strip()
                                    gc_customer_cache[customer_id] = gc_name
                                if gc_name:
                                    opera_info = {**opera_info, 'opera_name': gc_name}
                    except Exception:
                        pass

                existing = payments_db.get_subscription(sub_id)

                payments_db.save_subscription(
                    subscription_id=sub_id,
                    mandate_id=mandate_id,
                    amount_pence=int(gc_sub.get("amount", 0)),
                    interval_unit=gc_sub.get("interval_unit", "monthly"),
                    interval_count=gc_sub.get("interval", 1),
                    opera_account=opera_info.get('opera_account'),
                    opera_name=opera_info.get('opera_name'),
                    source_doc=existing['source_doc'] if existing else None,
                    day_of_month=gc_sub.get("day_of_month"),
                    name=gc_sub.get("name"),
                    status=gc_sub.get("status", "active"),
                    start_date=gc_sub.get("start_date"),
                    end_date=gc_sub.get("end_date"),
                )

                if existing:
                    updated += 1
                else:
                    synced += 1

            if not cursor:
                break

        return {
            "success": True,
            "message": f"Synced {synced} new, updated {updated} existing subscriptions",
            "synced": synced,
            "updated": updated,
            "total": synced + updated,
        }
    except Exception as e:
        logger.error(f"Error syncing subscriptions: {e}")
        return {"success": False, "error": str(e)}


