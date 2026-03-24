"""
Supplier Onboarding API routes.

Provides endpoints for tracking and managing supplier onboarding status.
Uses SupplierStatementDB (SQLite) for onboarding state and Opera pname
for supplier details.

Supports both Opera SQL SE and Opera 3 (FoxPro) backends.
"""

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter()


class VerifyBankRequest(BaseModel):
    verified_by: str


# ============================================================
# Helper: get supplier names from Opera SQL SE
# ============================================================

def _get_supplier_names_sql() -> dict:
    """Fetch supplier account -> name mapping from Opera SQL SE pname table."""
    from api.main import sql_connector
    supplier_names = {}
    if sql_connector:
        try:
            df = sql_connector.execute_query(
                "SELECT RTRIM(pn_account) AS code, RTRIM(pn_name) AS name FROM pname WITH (NOLOCK)"
            )
            if df is not None and len(df) > 0:
                supplier_names = dict(zip(df['code'], df['name']))
        except Exception as e:
            logger.warning(f"Could not fetch supplier names from Opera SQL SE: {e}")
    return supplier_names


def _get_supplier_name_sql(account: str) -> Optional[str]:
    """Fetch a single supplier name from Opera SQL SE pname table."""
    from api.main import sql_connector
    if sql_connector:
        try:
            df = sql_connector.execute_query(
                "SELECT RTRIM(pn_name) AS name FROM pname WITH (NOLOCK) WHERE RTRIM(pn_account) = ?",
                params=[account]
            )
            if df is not None and len(df) > 0:
                return str(df.iloc[0]['name']).strip()
        except Exception:
            pass
    return None


def _get_supplier_detail_sql(account: str) -> Optional[dict]:
    """Fetch supplier details from Opera SQL SE pname table."""
    from api.main import sql_connector
    if sql_connector:
        try:
            df = sql_connector.execute_query(
                """SELECT RTRIM(pn_account) AS code, RTRIM(pn_name) AS name,
                          pn_currbal, pn_stop
                   FROM pname WITH (NOLOCK)
                   WHERE RTRIM(pn_account) = ?""",
                params=[account]
            )
            if df is not None and len(df) > 0:
                row = df.iloc[0]
                return {
                    "account": str(row['code']).strip(),
                    "name": str(row['name']).strip(),
                    "current_balance": float(row['currbal']) if row.get('currbal') is not None else None,
                    "stopped": bool(row.get('stop', 0))
                }
        except Exception as e:
            logger.warning(f"Could not fetch supplier detail for {account}: {e}")
    return None


# ============================================================
# Helper: get supplier names from Opera 3 (FoxPro)
# ============================================================

def _get_supplier_names_opera3(data_path: str) -> dict:
    """Fetch supplier account -> name mapping from Opera 3 pname table."""
    supplier_names = {}
    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        reader = Opera3Reader(data_path)
        pname_data = reader.read_table("pname")
        if pname_data:
            for p in pname_data:
                code = str(p.get("PN_ACNT", p.get("pn_acnt", ""))).strip()
                name = str(p.get("PN_NAME", p.get("pn_name", ""))).strip()
                if code:
                    supplier_names[code] = name
    except Exception as e:
        logger.warning(f"Could not fetch supplier names from Opera 3: {e}")
    return supplier_names


def _get_supplier_detail_opera3(data_path: str, account: str) -> Optional[dict]:
    """Fetch supplier details from Opera 3 pname table."""
    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        reader = Opera3Reader(data_path)
        pname_data = reader.read_table("pname")
        if pname_data:
            for p in pname_data:
                code = str(p.get("PN_ACNT", p.get("pn_acnt", ""))).strip()
                if code == account:
                    name = str(p.get("PN_NAME", p.get("pn_name", ""))).strip()
                    currbal = float(p.get("PN_CURRBAL", p.get("pn_currbal", 0)) or 0)
                    stopped = bool(int(p.get("PN_STOP", p.get("pn_stop", 0)) or 0))
                    return {
                        "account": code,
                        "name": name,
                        "current_balance": currbal,
                        "stopped": stopped
                    }
    except Exception as e:
        logger.warning(f"Could not fetch supplier detail for {account} from Opera 3: {e}")
    return None


# ============================================================
# Helper: default onboarding status
# ============================================================

def _default_onboarding_status(supplier_code: str) -> dict:
    """Return default onboarding status for a supplier with no record."""
    return {
        "supplier_code": supplier_code,
        "detected_at": None,
        "bank_verified": 0,
        "bank_verified_by": None,
        "bank_verified_at": None,
        "terms_confirmed": 0,
        "senders_configured": 0,
        "category": "standard",
        "priority": 5,
        "notes": None,
        "completed_at": None
    }


# ============================================================
# Opera SQL SE Endpoints
# ============================================================

@router.get("/api/supplier-onboarding/pending")
async def get_pending_onboarding():
    """
    Get all suppliers with incomplete onboarding.

    Fetches pending onboarding records from SQLite and enriches
    each with the supplier name from Opera SQL SE pname table.
    """
    try:
        from sql_rag.supplier_statement_db import get_supplier_statement_db
        db = get_supplier_statement_db()

        pending = db.get_pending_onboarding()

        # Fetch supplier names from Opera to enrich records
        supplier_names = _get_supplier_names_sql()

        results = []
        for record in pending:
            record["supplier_name"] = supplier_names.get(
                record["supplier_code"], record["supplier_code"]
            )
            results.append(record)

        return {"success": True, "pending": results}

    except Exception as e:
        logger.error(f"Error fetching pending onboarding: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/supplier-onboarding/{account}")
async def get_onboarding_status(account: str):
    """
    Get onboarding status for a single supplier.

    Returns the onboarding record from SQLite plus supplier details
    from Opera SQL SE. If no onboarding record exists, returns
    default status (all unchecked).
    """
    try:
        from sql_rag.supplier_statement_db import get_supplier_statement_db
        db = get_supplier_statement_db()

        status = db.get_onboarding_status(account)
        if status is None:
            status = _default_onboarding_status(account)

        # Get supplier details from Opera
        supplier_detail = _get_supplier_detail_sql(account)
        status["supplier_name"] = supplier_detail["name"] if supplier_detail else account
        status["supplier_detail"] = supplier_detail

        return {"success": True, "onboarding": status}

    except Exception as e:
        logger.error(f"Error fetching onboarding status for {account}: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/supplier-onboarding/{account}/verify-bank")
async def verify_bank(account: str, request: VerifyBankRequest):
    """
    Mark supplier bank details as verified.

    Creates an onboarding record if one does not already exist,
    then sets bank_verified=1 with the verifier name and timestamp.
    """
    try:
        from sql_rag.supplier_statement_db import get_supplier_statement_db
        db = get_supplier_statement_db()

        # Create onboarding record if it doesn't exist
        existing = db.get_onboarding_status(account)
        if existing is None:
            db.create_onboarding(account)

        now = datetime.now().isoformat()
        db.update_onboarding(
            account,
            bank_verified=1,
            bank_verified_by=request.verified_by,
            bank_verified_at=now
        )

        # Return updated status
        updated = db.get_onboarding_status(account)
        supplier_detail = _get_supplier_detail_sql(account)
        updated["supplier_name"] = supplier_detail["name"] if supplier_detail else account
        updated["supplier_detail"] = supplier_detail

        return {"success": True, "onboarding": updated}

    except Exception as e:
        logger.error(f"Error verifying bank for {account}: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/supplier-onboarding/{account}/confirm-terms")
async def confirm_terms(account: str):
    """
    Confirm supplier payment terms.

    Creates an onboarding record if one does not already exist,
    then sets terms_confirmed=1.
    """
    try:
        from sql_rag.supplier_statement_db import get_supplier_statement_db
        db = get_supplier_statement_db()

        # Create onboarding record if it doesn't exist
        existing = db.get_onboarding_status(account)
        if existing is None:
            db.create_onboarding(account)

        db.update_onboarding(account, terms_confirmed=1)

        # Return updated status
        updated = db.get_onboarding_status(account)
        supplier_detail = _get_supplier_detail_sql(account)
        updated["supplier_name"] = supplier_detail["name"] if supplier_detail else account
        updated["supplier_detail"] = supplier_detail

        return {"success": True, "onboarding": updated}

    except Exception as e:
        logger.error(f"Error confirming terms for {account}: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/supplier-onboarding/{account}/complete")
async def complete_onboarding(account: str):
    """
    Mark supplier onboarding as complete.

    Only allowed if bank_verified=1. Sets completed_at to the
    current timestamp.
    """
    try:
        from sql_rag.supplier_statement_db import get_supplier_statement_db
        db = get_supplier_statement_db()

        existing = db.get_onboarding_status(account)
        if existing is None:
            return {
                "success": False,
                "error": f"No onboarding record found for supplier {account}. "
                         "Please start the onboarding process first."
            }

        if not existing.get("bank_verified"):
            return {
                "success": False,
                "error": "Cannot complete onboarding: bank details have not been verified. "
                         "Please verify bank details before completing onboarding."
            }

        now = datetime.now().isoformat()
        db.update_onboarding(account, completed_at=now)

        # Return updated status
        updated = db.get_onboarding_status(account)
        supplier_detail = _get_supplier_detail_sql(account)
        updated["supplier_name"] = supplier_detail["name"] if supplier_detail else account
        updated["supplier_detail"] = supplier_detail

        return {"success": True, "onboarding": updated}

    except Exception as e:
        logger.error(f"Error completing onboarding for {account}: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/supplier-onboarding/detect-new")
async def detect_new_suppliers():
    """
    Detect new suppliers not yet tracked in onboarding.

    Queries Opera SQL SE pname for all supplier accounts, compares
    against the supplier_onboarding table, and creates onboarding
    records for any suppliers not yet tracked.
    """
    try:
        from api.main import sql_connector
        from sql_rag.supplier_statement_db import get_supplier_statement_db
        db = get_supplier_statement_db()

        if not sql_connector:
            return {"success": False, "error": "Opera SQL SE connection not available."}

        # Get all supplier accounts from Opera
        df = sql_connector.execute_query(
            "SELECT RTRIM(pn_account) AS code FROM pname WITH (NOLOCK)"
        )
        if df is None or len(df) == 0:
            return {"success": True, "new_count": 0, "message": "No suppliers found in Opera."}

        opera_accounts = set(str(code).strip() for code in df['code'] if str(code).strip())

        # Get all accounts already tracked in onboarding
        pending = db.get_pending_onboarding()
        # Also need completed ones - query directly for all supplier_codes
        conn = db._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT supplier_code FROM supplier_onboarding")
        tracked_accounts = set(row['supplier_code'] for row in cursor.fetchall())
        conn.close()

        # Find accounts not yet tracked
        new_accounts = opera_accounts - tracked_accounts

        # Create onboarding records for new suppliers
        created_count = 0
        for account in new_accounts:
            db.create_onboarding(account)
            created_count += 1

        return {
            "success": True,
            "new_count": created_count,
            "total_opera_suppliers": len(opera_accounts),
            "already_tracked": len(tracked_accounts),
            "message": f"Detected {created_count} new supplier(s) for onboarding."
        }

    except Exception as e:
        logger.error(f"Error detecting new suppliers: {e}")
        return {"success": False, "error": str(e)}


# ============================================================
# Opera 3 (FoxPro) Endpoints
# ============================================================

@router.get("/api/opera3/supplier-onboarding/pending")
async def opera3_get_pending_onboarding(
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """
    Get all suppliers with incomplete onboarding (Opera 3 variant).

    Fetches pending onboarding records from SQLite and enriches
    each with the supplier name from Opera 3 FoxPro pname table.
    """
    try:
        from sql_rag.supplier_statement_db import get_supplier_statement_db
        db = get_supplier_statement_db()

        pending = db.get_pending_onboarding()

        # Fetch supplier names from Opera 3 to enrich records
        supplier_names = _get_supplier_names_opera3(data_path)

        results = []
        for record in pending:
            record["supplier_name"] = supplier_names.get(
                record["supplier_code"], record["supplier_code"]
            )
            results.append(record)

        return {"success": True, "pending": results}

    except Exception as e:
        logger.error(f"Error fetching pending onboarding (Opera 3): {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/opera3/supplier-onboarding/{account}")
async def opera3_get_onboarding_status(
    account: str,
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """
    Get onboarding status for a single supplier (Opera 3 variant).

    Returns the onboarding record from SQLite plus supplier details
    from Opera 3 FoxPro. If no onboarding record exists, returns
    default status (all unchecked).
    """
    try:
        from sql_rag.supplier_statement_db import get_supplier_statement_db
        db = get_supplier_statement_db()

        status = db.get_onboarding_status(account)
        if status is None:
            status = _default_onboarding_status(account)

        # Get supplier details from Opera 3
        supplier_detail = _get_supplier_detail_opera3(data_path, account)
        status["supplier_name"] = supplier_detail["name"] if supplier_detail else account
        status["supplier_detail"] = supplier_detail

        return {"success": True, "onboarding": status}

    except Exception as e:
        logger.error(f"Error fetching onboarding status for {account} (Opera 3): {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/opera3/supplier-onboarding/detect-new")
async def opera3_detect_new_suppliers(
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """
    Detect new suppliers not yet tracked in onboarding (Opera 3 variant).

    Reads Opera 3 FoxPro pname for all supplier accounts, compares
    against the supplier_onboarding table, and creates onboarding
    records for any suppliers not yet tracked.
    """
    try:
        from sql_rag.supplier_statement_db import get_supplier_statement_db
        db = get_supplier_statement_db()

        # Get all supplier accounts from Opera 3
        supplier_names = _get_supplier_names_opera3(data_path)
        if not supplier_names:
            return {"success": True, "new_count": 0, "message": "No suppliers found in Opera 3."}

        opera_accounts = set(supplier_names.keys())

        # Get all accounts already tracked in onboarding
        conn = db._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT supplier_code FROM supplier_onboarding")
        tracked_accounts = set(row['supplier_code'] for row in cursor.fetchall())
        conn.close()

        # Find accounts not yet tracked
        new_accounts = opera_accounts - tracked_accounts

        # Create onboarding records for new suppliers
        created_count = 0
        for account in new_accounts:
            db.create_onboarding(account)
            created_count += 1

        return {
            "success": True,
            "new_count": created_count,
            "total_opera_suppliers": len(opera_accounts),
            "already_tracked": len(tracked_accounts),
            "message": f"Detected {created_count} new supplier(s) for onboarding."
        }

    except Exception as e:
        logger.error(f"Error detecting new suppliers (Opera 3): {e}")
        return {"success": False, "error": str(e)}
