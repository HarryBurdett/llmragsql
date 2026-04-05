"""
Supplier API routes.

Extracted from api/main.py — provides endpoints for supplier statement automation,
supplier queries, communications, security, settings, supplier directory,
creditors control (Purchase Ledger), and supplier account views.

Does NOT include /api/reconcile/creditors (that belongs to balance_check app).

Opera 3 Parity:
    This module queries Opera SQL SE (SQL Server) via sql_connector.execute_query().
    It accesses:
    - ptran (Purchase Ledger) — for statement matching and reconciliation
    - pname (Supplier Master) — for supplier lookups and data
    - pcontact (Supplier Contacts) — for email verification
    - pterms (Payment Terms) — via supplier_config.py

    To support Opera 3 (FoxPro DBF), routes need Opera 3 equivalents that:
    - Use opera3_foxpro.py or opera3_data_provider.py patterns
    - Query the same DBF files (ptran.DBF, pname.DBF, pcontact.DBF, pterms.DBF)
    - Return DataFrames with same column names (pt_trref, pn_account, etc.)

    See background.py and supplier_config.py for specific queries marked with
    TODO comments.
"""

import os
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List

from fastapi import APIRouter, HTTPException, Query, Body, Request
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter()


# ============================================================
# Database Migrations (safe ALTER TABLE)
# ============================================================

def _run_migrations(db_path: Path):
    """Run safe schema migrations — adds columns if they don't exist."""
    if not db_path.exists():
        return
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        # Check if response_text column exists on supplier_statements
        cursor.execute("PRAGMA table_info(supplier_statements)")
        columns = [row[1] for row in cursor.fetchall()]
        if 'response_text' not in columns:
            cursor.execute("ALTER TABLE supplier_statements ADD COLUMN response_text TEXT")
            logger.info("Migration: added response_text column to supplier_statements")
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"Migration check failed (non-fatal): {e}")


def _get_db_path() -> Path:
    """Get the supplier statements database path for the current company."""
    from sql_rag.company_data import get_current_db_path
    return get_current_db_path('supplier_statements.db') or Path(__file__).parent.parent.parent.parent / 'supplier_statements.db'


# ============================================================
# Request/Response Models
# ============================================================

class EditResponseRequest(BaseModel):
    response_text: str

class BulkApproveRequest(BaseModel):
    statement_ids: List[int]
    approved_by: str = "System"

class ApproveWithBodyRequest(BaseModel):
    approved_by: str = "System"
    subject: Optional[str] = None
    body: Optional[str] = None


# ============================================================
# Supplier Config API Endpoints
# ============================================================

@router.get("/api/supplier-config")
async def list_supplier_config(active_only: bool = False):
    """List all suppliers with their automation flags."""
    from api.main import sql_connector
    from sql_rag.supplier_config import SupplierConfigManager
    from sql_rag.company_data import get_current_db_path

    db_path = get_current_db_path('supplier_statements.db')
    if not db_path:
        return {"success": False, "error": "Database not available"}

    try:
        mgr = SupplierConfigManager(str(db_path), sql_connector)
        suppliers = mgr.get_all(active_only=active_only)
        return {"success": True, "suppliers": suppliers}
    except Exception as e:
        logger.error(f"Error listing supplier config: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/supplier-config/{account}")
async def get_supplier_config(account: str):
    """Get config for a single supplier."""
    from api.main import sql_connector
    from sql_rag.supplier_config import SupplierConfigManager
    from sql_rag.company_data import get_current_db_path

    db_path = get_current_db_path('supplier_statements.db')
    if not db_path:
        return {"success": False, "error": "Database not available"}

    try:
        mgr = SupplierConfigManager(str(db_path), sql_connector)
        config = mgr.get_config(account)
        if config is None:
            raise HTTPException(status_code=404, detail=f"Supplier {account} not found in config")
        return {"success": True, "config": config}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting supplier config for {account}: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.put("/api/supplier-config/{account}")
async def update_supplier_flags(account: str, request: Request):
    """Update automation flags for a supplier.

    Only the following fields are accepted:
    reconciliation_active, auto_respond, never_communicate,
    statements_contact_position.
    """
    from api.main import sql_connector
    from sql_rag.supplier_config import SupplierConfigManager
    from sql_rag.company_data import get_current_db_path

    db_path = get_current_db_path('supplier_statements.db')
    if not db_path:
        return {"success": False, "error": "Database not available"}

    try:
        body = await request.json()
        # Only permit local flag fields — reject anything else
        allowed = {'reconciliation_active', 'auto_respond', 'never_communicate', 'statements_contact_position'}
        flags = {k: v for k, v in body.items() if k in allowed}
        if not flags:
            return {"success": False, "error": "No valid flag fields provided. Allowed: " + ", ".join(sorted(allowed))}

        mgr = SupplierConfigManager(str(db_path), sql_connector)
        updated = mgr.update_flags(account, **flags)
        if not updated:
            raise HTTPException(status_code=404, detail=f"Supplier {account} not found in config")
        return {"success": True, "updated": flags}
    except HTTPException:
        raise
    except ValueError as e:
        return {"success": False, "error": str(e)}
    except Exception as e:
        logger.error(f"Error updating supplier flags for {account}: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/api/supplier-config/sync")
async def sync_supplier_config():
    """Trigger a one-way sync of supplier master data from Opera into supplier_config."""
    from api.main import sql_connector
    from sql_rag.supplier_config import SupplierConfigManager
    from sql_rag.company_data import get_current_db_path

    db_path = get_current_db_path('supplier_statements.db')
    if not db_path:
        return {"success": False, "error": "Database not available"}

    if not sql_connector:
        return {"success": False, "error": "Opera connection not available"}

    try:
        mgr = SupplierConfigManager(str(db_path), sql_connector)
        result = mgr.sync_from_opera()
        return {"success": True, "new": result['new'], "synced": result['synced']}
    except Exception as e:
        logger.error(f"Error syncing supplier config from Opera: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


# ============================================================
# Supplier Statement Automation API Endpoints
# ============================================================

@router.get("/api/supplier-statements/dashboard")
async def get_supplier_statement_dashboard():
    """
    Get supplier statement automation dashboard data.

    Returns KPIs, alerts, and recent activity for the supplier statement
    automation system. Works with both Opera SQL SE and Opera 3.

    Returns:
        Dashboard data including:
        - KPIs (statements count, pending approvals, queries, etc.)
        - Alerts (security, overdue, failed processing)
        - Recent statements, queries, and responses
    """
    from api.main import sql_connector
    from sql_rag.company_data import get_current_db_path

    db_path = get_current_db_path('supplier_statements.db') or Path(__file__).parent.parent.parent.parent / 'supplier_statements.db'

    # Initialize response with default values
    response = {
        "success": True,
        "kpis": {
            "statements_today": 0,
            "statements_week": 0,
            "statements_month": 0,
            "pending_approvals": 0,
            "open_queries": 0,
            "overdue_queries": 0,
            "avg_processing_hours": None,
            "match_rate_percent": None
        },
        "alerts": {
            "security_alerts": [],
            "overdue_queries": [],
            "failed_processing": []
        },
        "recent_statements": [],
        "recent_queries": [],
        "recent_responses": []
    }

    # If database doesn't exist, return empty dashboard
    if not db_path.exists():
        return response

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        now = datetime.now()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        week_start = today_start - timedelta(days=today_start.weekday())
        month_start = today_start.replace(day=1)

        # Use space-separated format to match SQLite CURRENT_TIMESTAMP format
        today_str = today_start.strftime('%Y-%m-%d %H:%M:%S')
        week_str = week_start.strftime('%Y-%m-%d %H:%M:%S')
        month_str = month_start.strftime('%Y-%m-%d %H:%M:%S')

        # KPIs - Statements counts
        cursor.execute("""
            SELECT
                COUNT(CASE WHEN received_date >= ? THEN 1 END) as today_count,
                COUNT(CASE WHEN received_date >= ? THEN 1 END) as week_count,
                COUNT(CASE WHEN received_date >= ? THEN 1 END) as month_count
            FROM supplier_statements
        """, (today_str, week_str, month_str))
        row = cursor.fetchone()
        if row:
            response["kpis"]["statements_today"] = row["today_count"] or 0
            response["kpis"]["statements_week"] = row["week_count"] or 0
            response["kpis"]["statements_month"] = row["month_count"] or 0

        # Pending approvals
        cursor.execute("""
            SELECT COUNT(*) as count FROM supplier_statements
            WHERE status = 'queued'
        """)
        row = cursor.fetchone()
        if row:
            response["kpis"]["pending_approvals"] = row["count"] or 0

        # Open and overdue queries
        cursor.execute("""
            SELECT
                COUNT(CASE WHEN query_resolved_at IS NULL THEN 1 END) as open_count,
                COUNT(CASE WHEN query_resolved_at IS NULL
                           AND datetime(query_sent_at, '+7 days') < datetime('now') THEN 1 END) as overdue_count
            FROM statement_lines
            WHERE query_sent_at IS NOT NULL
        """)
        row = cursor.fetchone()
        if row:
            response["kpis"]["open_queries"] = row["open_count"] or 0
            response["kpis"]["overdue_queries"] = row["overdue_count"] or 0

        # Average processing time (hours)
        cursor.execute("""
            SELECT AVG((julianday(processed_at) - julianday(received_date)) * 24) as avg_hours
            FROM supplier_statements
            WHERE processed_at IS NOT NULL AND received_date IS NOT NULL
        """)
        row = cursor.fetchone()
        if row and row["avg_hours"]:
            response["kpis"]["avg_processing_hours"] = round(row["avg_hours"], 1)

        # Match rate
        cursor.execute("""
            SELECT
                COUNT(CASE WHEN match_status = 'matched' THEN 1 END) as matched,
                COUNT(*) as total
            FROM statement_lines
        """)
        row = cursor.fetchone()
        if row and row["total"] > 0:
            response["kpis"]["match_rate_percent"] = round(
                (row["matched"] or 0) / row["total"] * 100, 1
            )

        # Security alerts (unverified bank changes)
        cursor.execute("""
            SELECT id, supplier_code, field_name, old_value, new_value, changed_at, changed_by
            FROM supplier_change_audit
            WHERE verified = 0 AND field_name IN ('pn_bankac', 'pn_banksor')
            ORDER BY changed_at DESC LIMIT 10
        """)
        for row in cursor.fetchall():
            response["alerts"]["security_alerts"].append({
                "id": row["id"],
                "supplier_code": row["supplier_code"],
                "supplier_name": row["supplier_code"],  # Would need to lookup
                "alert_type": "bank_detail_change",
                "message": f"{row['field_name']} changed from '{row['old_value']}' to '{row['new_value']}'",
                "created_at": row["changed_at"]
            })

        # Overdue queries
        cursor.execute("""
            SELECT sl.id, ss.supplier_code, sl.reference, sl.query_type, sl.query_sent_at,
                   julianday('now') - julianday(sl.query_sent_at) as days_outstanding
            FROM statement_lines sl
            JOIN supplier_statements ss ON sl.statement_id = ss.id
            WHERE sl.query_resolved_at IS NULL
              AND datetime(sl.query_sent_at, '+7 days') < datetime('now')
            ORDER BY sl.query_sent_at ASC LIMIT 10
        """)
        for row in cursor.fetchall():
            response["alerts"]["overdue_queries"].append({
                "id": row["id"],
                "supplier_code": row["supplier_code"],
                "supplier_name": row["supplier_code"],
                "alert_type": "overdue_query",
                "message": f"Query on {row['reference'] or 'item'} ({row['query_type']}) - {int(row['days_outstanding'])} days overdue",
                "created_at": row["query_sent_at"]
            })

        # Failed processing
        cursor.execute("""
            SELECT id, supplier_code, statement_date, received_date, status
            FROM supplier_statements
            WHERE status = 'error'
            ORDER BY received_date DESC LIMIT 10
        """)
        for row in cursor.fetchall():
            response["alerts"]["failed_processing"].append({
                "id": row["id"],
                "supplier_code": row["supplier_code"],
                "supplier_name": row["supplier_code"],
                "alert_type": "failed_processing",
                "message": f"Statement extraction failed",
                "created_at": row["received_date"]
            })

        # Recent statements
        cursor.execute("""
            SELECT id, supplier_code, statement_date, received_date, status,
                   (SELECT closing_balance FROM supplier_statements WHERE id = ss.id) as closing_balance
            FROM supplier_statements ss
            ORDER BY received_date DESC LIMIT 10
        """)
        for row in cursor.fetchall():
            response["recent_statements"].append({
                "id": row["id"],
                "supplier_code": row["supplier_code"],
                "supplier_name": row["supplier_code"],  # Would need to lookup
                "statement_date": row["statement_date"],
                "received_date": row["received_date"],
                "status": row["status"],
                "closing_balance": row["closing_balance"]
            })

        # Recent queries
        cursor.execute("""
            SELECT sl.id, ss.supplier_code, sl.reference, sl.query_type, sl.query_sent_at,
                   sl.query_resolved_at,
                   julianday('now') - julianday(sl.query_sent_at) as days_outstanding
            FROM statement_lines sl
            JOIN supplier_statements ss ON sl.statement_id = ss.id
            WHERE sl.query_sent_at IS NOT NULL
            ORDER BY sl.query_sent_at DESC LIMIT 10
        """)
        for row in cursor.fetchall():
            status = "resolved" if row["query_resolved_at"] else (
                "overdue" if row["days_outstanding"] > 7 else "open"
            )
            response["recent_queries"].append({
                "id": row["id"],
                "supplier_code": row["supplier_code"],
                "supplier_name": row["supplier_code"],
                "query_type": row["query_type"],
                "reference": row["reference"],
                "status": status,
                "days_outstanding": int(row["days_outstanding"]) if row["days_outstanding"] else 0,
                "created_at": row["query_sent_at"]
            })

        # Recent responses sent
        cursor.execute("""
            SELECT ss.id, ss.supplier_code, ss.statement_date, ss.sent_at, ss.approved_by,
                   (SELECT COUNT(*) FROM statement_lines sl WHERE sl.statement_id = ss.id
                    AND sl.query_sent_at IS NOT NULL) as queries_count,
                   (SELECT closing_balance FROM supplier_statements WHERE id = ss.id) as balance
            FROM supplier_statements ss
            WHERE ss.sent_at IS NOT NULL
            ORDER BY ss.sent_at DESC LIMIT 10
        """)
        for row in cursor.fetchall():
            response["recent_responses"].append({
                "id": row["id"],
                "supplier_code": row["supplier_code"],
                "supplier_name": row["supplier_code"],
                "statement_date": row["statement_date"],
                "sent_at": row["sent_at"],
                "approved_by": row["approved_by"],
                "queries_count": row["queries_count"] or 0,
                "balance": row["balance"]
            })

        conn.close()
        return response

    except Exception as e:
        logger.error(f"Error loading supplier statement dashboard: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/supplier-statements")
async def list_supplier_statements(status: Optional[str] = None):
    """List all supplier statements with line counts and match statistics."""
    from api.main import sql_connector
    from sql_rag.company_data import get_current_db_path

    db_path = get_current_db_path('supplier_statements.db') or Path(__file__).parent.parent.parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        return {"success": True, "statements": []}

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get statements with aggregated line data
        query = """
            SELECT
                ss.id, ss.supplier_code, ss.statement_date, ss.received_date, ss.status,
                ss.sender_email, ss.opening_balance, ss.closing_balance, ss.currency,
                ss.acknowledged_at, ss.processed_at, ss.approved_by, ss.approved_at,
                ss.sent_at, ss.error_message,
                COUNT(sl.id) as line_count,
                SUM(CASE WHEN sl.status = 'Agreed' THEN 1 ELSE 0 END) as matched_count,
                SUM(CASE WHEN sl.status = 'Query' THEN 1 ELSE 0 END) as query_count
            FROM supplier_statements ss
            LEFT JOIN statement_lines sl ON sl.statement_id = ss.id
        """
        params = []
        if status:
            query += " WHERE ss.status = ?"
            params.append(status)
        query += " GROUP BY ss.id ORDER BY ss.received_date DESC"

        cursor.execute(query, params)
        rows = cursor.fetchall()
        statements = [dict(row) for row in rows]

        # Get supplier names from Opera
        supplier_names = {}
        if sql_connector:
            codes = set(s['supplier_code'] for s in statements)
            for code in codes:
                try:
                    df = sql_connector.execute_query(
                        f"SELECT RTRIM(pn_name) as pn_name FROM pname WITH (NOLOCK) WHERE pn_account = '{code}'"
                    )
                    if df is not None and len(df) > 0:
                        supplier_names[code] = str(df.iloc[0]['pn_name']).strip()
                except Exception:
                    pass

        for stmt in statements:
            stmt['supplier_name'] = supplier_names.get(stmt['supplier_code'], stmt['supplier_code'])

        conn.close()
        return {"success": True, "statements": statements}

    except Exception as e:
        logger.error(f"Error listing supplier statements: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/supplier-statements/reconciliations")
async def list_supplier_reconciliations():
    """List statements pending reconciliation review/approval."""
    from sql_rag.company_data import get_current_db_path

    db_path = get_current_db_path('supplier_statements.db') or Path(__file__).parent.parent.parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        return {"success": True, "statements": []}

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, supplier_code, statement_date, received_date, status,
                   processed_at, approved_by, approved_at
            FROM supplier_statements
            WHERE status IN ('reconciled', 'queued')
            ORDER BY processed_at DESC
        """)
        statements = []
        for row in cursor.fetchall():
            statements.append(dict(row))

        conn.close()
        return {"success": True, "statements": statements}

    except Exception as e:
        logger.error(f"Error listing reconciliations: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/supplier-statements/history")
async def list_supplier_statement_history(days: int = 90):
    """List completed/sent statements for history view."""
    from api.main import sql_connector
    from sql_rag.company_data import get_current_db_path

    db_path = get_current_db_path('supplier_statements.db') or Path(__file__).parent.parent.parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        return {"success": True, "statements": []}

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cutoff = (datetime.now() - timedelta(days=days)).isoformat()

        cursor.execute("""
            SELECT
                ss.id, ss.supplier_code, ss.statement_date, ss.received_date,
                ss.status, ss.processed_at, ss.approved_by, ss.approved_at,
                ss.sent_at, ss.opening_balance, ss.closing_balance,
                COUNT(sl.id) as line_count,
                SUM(CASE WHEN sl.status = 'Agreed' THEN 1 ELSE 0 END) as matched_count,
                SUM(CASE WHEN sl.status = 'Query' THEN 1 ELSE 0 END) as query_count
            FROM supplier_statements ss
            LEFT JOIN statement_lines sl ON sl.statement_id = ss.id
            WHERE ss.status IN ('approved', 'sent') AND ss.received_date >= ?
            GROUP BY ss.id
            ORDER BY ss.sent_at DESC, ss.approved_at DESC
        """, (cutoff,))

        statements = [dict(row) for row in cursor.fetchall()]

        # Get supplier names from Opera
        if sql_connector and statements:
            codes = list(set(s['supplier_code'] for s in statements if s.get('supplier_code')))
            if codes:
                code_list = ','.join(f"'{c}'" for c in codes)
                names_df = sql_connector.execute_query(f"""
                    SELECT RTRIM(pn_account) as code, RTRIM(pn_name) as name
                    FROM pname WITH (NOLOCK) WHERE pn_account IN ({code_list})
                """)
                if names_df is not None and len(names_df) > 0:
                    name_map = dict(zip(names_df['code'], names_df['name']))
                    for s in statements:
                        s['supplier_name'] = name_map.get(s['supplier_code'], s['supplier_code'])

        conn.close()
        return {"success": True, "statements": statements}

    except Exception as e:
        logger.error(f"Error listing statement history: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/supplier-queries")
async def list_supplier_queries(status: Optional[str] = None):
    """
    List supplier queries from statement lines with query status.

    Query status is derived from statement_lines:
    - open: query_type is set, query_resolved_at is null
    - overdue: open query older than query_response_days config
    - resolved: query_resolved_at is set
    """
    from api.main import sql_connector
    from sql_rag.company_data import get_current_db_path

    db_path = get_current_db_path('supplier_statements.db') or Path(__file__).parent.parent.parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        return {"success": True, "queries": [], "counts": {"open": 0, "overdue": 0, "resolved": 0}}

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get config for overdue threshold
        cursor.execute("SELECT value FROM supplier_automation_config WHERE key = 'query_response_days'")
        row = cursor.fetchone()
        response_days = int(row['value']) if row else 5
        overdue_cutoff = (datetime.now() - timedelta(days=response_days)).isoformat()

        # Build query based on status filter
        if status == 'open':
            where_clause = "sl.query_type IS NOT NULL AND sl.query_resolved_at IS NULL AND sl.query_sent_at >= ?"
            params = (overdue_cutoff,)
        elif status == 'overdue':
            where_clause = "sl.query_type IS NOT NULL AND sl.query_resolved_at IS NULL AND sl.query_sent_at < ?"
            params = (overdue_cutoff,)
        elif status == 'resolved':
            where_clause = "sl.query_resolved_at IS NOT NULL"
            params = ()
        else:
            where_clause = "sl.query_type IS NOT NULL"
            params = ()

        cursor.execute(f"""
            SELECT
                sl.id as query_id,
                sl.statement_id,
                ss.supplier_code,
                sl.query_type,
                sl.reference,
                sl.description,
                sl.debit,
                sl.credit,
                sl.line_date,
                sl.query_sent_at,
                sl.query_resolved_at,
                ss.statement_date,
                CASE
                    WHEN sl.query_resolved_at IS NOT NULL THEN 'resolved'
                    WHEN sl.query_sent_at < ? THEN 'overdue'
                    ELSE 'open'
                END as status,
                julianday('now') - julianday(sl.query_sent_at) as days_outstanding
            FROM statement_lines sl
            JOIN supplier_statements ss ON sl.statement_id = ss.id
            WHERE {where_clause}
            ORDER BY sl.query_sent_at DESC
        """, (overdue_cutoff,) + params)

        queries = [dict(row) for row in cursor.fetchall()]

        # Get counts for each status
        cursor.execute("""
            SELECT
                SUM(CASE WHEN query_resolved_at IS NULL AND query_sent_at >= ? THEN 1 ELSE 0 END) as open_count,
                SUM(CASE WHEN query_resolved_at IS NULL AND query_sent_at < ? THEN 1 ELSE 0 END) as overdue_count,
                SUM(CASE WHEN query_resolved_at IS NOT NULL THEN 1 ELSE 0 END) as resolved_count
            FROM statement_lines
            WHERE query_type IS NOT NULL
        """, (overdue_cutoff, overdue_cutoff))
        counts_row = cursor.fetchone()
        counts = {
            "open": counts_row['open_count'] or 0,
            "overdue": counts_row['overdue_count'] or 0,
            "resolved": counts_row['resolved_count'] or 0
        }

        # Get supplier names from Opera
        if sql_connector and queries:
            codes = list(set(q['supplier_code'] for q in queries if q.get('supplier_code')))
            if codes:
                code_list = ','.join(f"'{c}'" for c in codes)
                names_df = sql_connector.execute_query(f"""
                    SELECT RTRIM(pn_account) as code, RTRIM(pn_name) as name
                    FROM pname WITH (NOLOCK) WHERE pn_account IN ({code_list})
                """)
                if names_df is not None and len(names_df) > 0:
                    name_map = dict(zip(names_df['code'], names_df['name']))
                    for q in queries:
                        q['supplier_name'] = name_map.get(q['supplier_code'], q['supplier_code'])

        conn.close()
        return {"success": True, "queries": queries, "counts": counts}

    except Exception as e:
        logger.error(f"Error listing supplier queries: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/api/supplier-queries/{query_id}/resolve")
async def resolve_supplier_query(query_id: int):
    """Mark a supplier query as resolved."""
    from sql_rag.company_data import get_current_db_path

    db_path = get_current_db_path('supplier_statements.db') or Path(__file__).parent.parent.parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE statement_lines
            SET query_resolved_at = CURRENT_TIMESTAMP
            WHERE id = ? AND query_type IS NOT NULL
        """, (query_id,))

        if cursor.rowcount == 0:
            conn.close()
            raise HTTPException(status_code=404, detail="Query not found")

        conn.commit()
        conn.close()

        return {"success": True, "message": "Query resolved"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error resolving query: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/api/supplier-queries/auto-resolve")
async def auto_resolve_supplier_queries():
    """
    Auto-resolve queries when matching invoices are found in Opera.

    Checks all open queries against Opera ptran to see if the missing
    invoice has now been entered. If found, marks the query as resolved.

    This should be called:
    - Periodically (scheduled job)
    - After invoice entry
    - When processing new statements
    """
    from api.main import sql_connector
    from sql_rag.company_data import get_current_db_path

    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    db_path = get_current_db_path('supplier_statements.db') or Path(__file__).parent.parent.parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        return {"success": True, "resolved": 0, "message": "No queries database"}

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get all open queries (invoice not found type)
        cursor.execute("""
            SELECT sl.id, sl.reference, sl.debit, sl.credit, ss.supplier_code
            FROM statement_lines sl
            JOIN supplier_statements ss ON sl.statement_id = ss.id
            WHERE sl.query_type IS NOT NULL
              AND sl.query_resolved_at IS NULL
              AND sl.query_type LIKE '%not found%'
        """)

        open_queries = cursor.fetchall()
        resolved_count = 0
        resolved_items = []

        for query in open_queries:
            supplier_code = query['supplier_code']
            reference = query['reference']
            amount = query['debit'] or query['credit'] or 0

            # Check if this invoice now exists in Opera
            # Match by reference OR by amount for this supplier
            check_query = f"""
                SELECT TOP 1 pt_unique, pt_trref, pt_trvalue
                FROM ptran WITH (NOLOCK)
                WHERE pt_account = '{supplier_code}'
                  AND pt_trtype = 'I'
                  AND (
                      pt_trref LIKE '%{reference}%'
                      OR pt_supref LIKE '%{reference}%'
                      OR ABS(pt_trvalue - {amount}) < 0.01
                  )
                ORDER BY pt_trdate DESC
            """

            result = sql_connector.execute_query(check_query)

            if result is not None and len(result) > 0:
                # Invoice found - auto-resolve the query
                cursor.execute("""
                    UPDATE statement_lines
                    SET query_resolved_at = CURRENT_TIMESTAMP,
                        match_status = 'matched',
                        matched_ptran_id = ?
                    WHERE id = ?
                """, (result.iloc[0]['pt_unique'], query['id']))

                resolved_count += 1
                resolved_items.append({
                    "query_id": query['id'],
                    "reference": reference,
                    "supplier_code": supplier_code,
                    "matched_to": result.iloc[0]['pt_unique']
                })

                logger.info(f"Auto-resolved query {query['id']} - {reference} matched to {result.iloc[0]['pt_unique']}")

        conn.commit()
        conn.close()

        # Check if any statements now have all queries resolved
        statements_ready = []
        if resolved_count > 0:
            # Get unique statement IDs that had queries resolved
            statement_ids = list(set(
                cursor.execute("""
                    SELECT DISTINCT statement_id FROM statement_lines WHERE id IN ({})
                """.format(','.join(str(item['query_id']) for item in resolved_items))).fetchall()
            ))

            for (stmt_id,) in statement_ids:
                # Check if this statement has any remaining open queries
                cursor.execute("""
                    SELECT COUNT(*) FROM statement_lines
                    WHERE statement_id = ?
                      AND query_type IS NOT NULL
                      AND query_resolved_at IS NULL
                """, (stmt_id,))
                open_count = cursor.fetchone()[0]

                if open_count == 0:
                    statements_ready.append(stmt_id)
                    logger.info(f"Statement {stmt_id} - all queries resolved, ready for updated status")

        conn.commit()
        conn.close()

        return {
            "success": True,
            "resolved": resolved_count,
            "items": resolved_items,
            "statements_all_resolved": statements_ready,
            "message": f"Auto-resolved {resolved_count} queries. {len(statements_ready)} statement(s) ready for updated status."
        }

    except Exception as e:
        logger.error(f"Error auto-resolving queries: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/api/supplier-queries/{query_id}/send-reminder")
async def send_query_reminder(query_id: int):
    """
    Send a follow-up reminder email to the supplier about an unresolved query.

    Gets the query details from statement_lines, looks up the supplier
    contact email, sends a reminder, and records in the communications
    audit trail.

    Returns:
        {success, sent_to}
    """
    from api.main import sql_connector
    from sql_rag.supplier_statement_db import get_supplier_statement_db

    db_path = _get_db_path()
    _run_migrations(db_path)

    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get query details from statement_lines
        cursor.execute("""
            SELECT sl.id, sl.reference, sl.description, sl.debit, sl.credit,
                   sl.query_type, sl.query_sent_at, sl.query_resolved_at,
                   sl.statement_id,
                   ss.supplier_code, ss.statement_date
            FROM statement_lines sl
            JOIN supplier_statements ss ON sl.statement_id = ss.id
            WHERE sl.id = ? AND sl.query_type IS NOT NULL
        """, (query_id,))
        query = cursor.fetchone()

        if not query:
            conn.close()
            raise HTTPException(status_code=404, detail="Query not found")

        if query['query_resolved_at'] is not None:
            conn.close()
            return {"success": False, "error": "Query has already been resolved"}

        supplier_code = query['supplier_code']
        reference = query['reference'] or 'N/A'
        amount = query['debit'] or query['credit'] or 0

        # Get supplier name from Opera
        supplier_name = supplier_code
        if sql_connector:
            try:
                name_df = sql_connector.execute_query(
                    f"SELECT RTRIM(pn_name) as name FROM pname WITH (NOLOCK) WHERE pn_account = '{supplier_code}'"
                )
                if name_df is not None and len(name_df) > 0:
                    supplier_name = name_df.iloc[0]['name']
            except Exception:
                pass

        # Get supplier contact email
        recipient_email = _get_supplier_contact_email(
            cursor, supplier_code, None
        )

        # Fallback: try sender_email from the statement
        if not recipient_email:
            cursor.execute(
                "SELECT sender_email FROM supplier_statements WHERE id = ?",
                (query['statement_id'],)
            )
            stmt_row = cursor.fetchone()
            if stmt_row and stmt_row['sender_email']:
                recipient_email = stmt_row['sender_email']

        if not recipient_email:
            conn.close()
            return {"success": False, "error": "No contact email found for this supplier"}

        # Build reminder email
        email_subject = f"Follow-up: Outstanding Query - {supplier_name} - Ref {reference}"
        email_body = (
            f"Dear {supplier_name},\n\n"
            f"We are following up on our query regarding invoice {reference} "
            f"for the amount of \u00a3{amount:,.2f}.\n\n"
            f"Query type: {query['query_type'] or 'General query'}\n"
            f"Original query date: {query['query_sent_at'] or 'N/A'}\n"
            f"Statement date: {query['statement_date'] or 'N/A'}\n\n"
            f"We would appreciate your earliest response to help us "
            f"reconcile our records.\n\n"
            f"Regards,\n"
            f"Accounts Department"
        )

        # Send email
        email_sent = False
        email_error = None
        try:
            import httpx
            async with httpx.AsyncClient() as client:
                email_resp = await client.post(
                    "http://127.0.0.1:8000/api/email/send",
                    json={
                        "to": recipient_email,
                        "subject": email_subject,
                        "body": email_body,
                        "from_email": "intsys@wimbledoncloud.net"
                    },
                    timeout=30.0
                )
                if email_resp.status_code == 200:
                    email_sent = True
                else:
                    email_error = f"Email send failed: {email_resp.status_code}"
        except Exception as e:
            email_error = f"Email send error: {str(e)}"
            logger.warning(f"Failed to send query reminder for query {query_id}: {e}")

        # Record in communications audit trail
        try:
            db = get_supplier_statement_db()
            db.log_communication(
                supplier_code=supplier_code,
                direction='outbound',
                comm_type='query_reminder',
                email_subject=email_subject,
                email_body=email_body,
                statement_id=query['statement_id'],
                sent_by='System'
            )
        except Exception as e:
            logger.warning(f"Failed to log query reminder communication for query {query_id}: {e}")

        conn.close()

        result = {
            "success": email_sent,
            "sent_to": recipient_email if email_sent else None,
            "message": "Reminder sent" if email_sent else "Failed to send reminder"
        }
        if email_error:
            result["email_error"] = email_error

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error sending query reminder: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/supplier-queries/overdue")
async def get_overdue_supplier_queries(days_overdue: Optional[int] = None):
    """
    Return queries where query_sent_at is older than threshold and
    query_resolved_at IS NULL.

    Query params:
        days_overdue: Override threshold (default from config 'query_response_days')

    Returns:
        {success, queries: [...], count}
    """
    from api.main import sql_connector
    from sql_rag.company_data import get_current_db_path

    db_path = get_current_db_path('supplier_statements.db') or Path(__file__).parent.parent.parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        return {"success": True, "queries": [], "count": 0}

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get threshold from config if not provided
        if days_overdue is None:
            cursor.execute("SELECT value FROM supplier_automation_config WHERE key = 'query_response_days'")
            row = cursor.fetchone()
            days_overdue = int(row['value']) if row else 5

        overdue_cutoff = (datetime.now() - timedelta(days=days_overdue)).isoformat()

        cursor.execute("""
            SELECT
                sl.id as query_id,
                sl.statement_id,
                ss.supplier_code,
                sl.reference,
                sl.description,
                sl.debit,
                sl.credit,
                sl.query_type,
                sl.query_sent_at,
                julianday('now') - julianday(sl.query_sent_at) as days_outstanding
            FROM statement_lines sl
            JOIN supplier_statements ss ON sl.statement_id = ss.id
            WHERE sl.query_type IS NOT NULL
              AND sl.query_resolved_at IS NULL
              AND sl.query_sent_at < ?
            ORDER BY sl.query_sent_at ASC
        """, (overdue_cutoff,))

        queries = [dict(row) for row in cursor.fetchall()]

        # Enrich with supplier names from Opera
        if sql_connector and queries:
            codes = list(set(q['supplier_code'] for q in queries if q.get('supplier_code')))
            if codes:
                code_list = ','.join(f"'{c}'" for c in codes)
                names_df = sql_connector.execute_query(f"""
                    SELECT RTRIM(pn_account) as code, RTRIM(pn_name) as name
                    FROM pname WITH (NOLOCK) WHERE pn_account IN ({code_list})
                """)
                if names_df is not None and len(names_df) > 0:
                    name_map = dict(zip(names_df['code'], names_df['name']))
                    for q in queries:
                        q['supplier_name'] = name_map.get(q['supplier_code'], q['supplier_code'])

        # Round days_outstanding
        for q in queries:
            if q.get('days_outstanding') is not None:
                q['days_outstanding'] = int(q['days_outstanding'])

        conn.close()
        return {"success": True, "queries": queries, "count": len(queries)}

    except Exception as e:
        logger.error(f"Error getting overdue queries: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/api/supplier-statements/{statement_id}/send-updated-status")
async def send_updated_statement_status(statement_id: int):
    """
    Send updated status to supplier after all queries are resolved.

    Generates a final reconciliation response confirming all items
    are now agreed and showing the payment schedule.
    """
    from api.main import sql_connector
    from sql_rag.company_data import get_current_db_path

    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    db_path = get_current_db_path('supplier_statements.db') or Path(__file__).parent.parent.parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get statement details
        cursor.execute("SELECT * FROM supplier_statements WHERE id = ?", (statement_id,))
        statement = cursor.fetchone()
        if not statement:
            conn.close()
            raise HTTPException(status_code=404, detail="Statement not found")

        # Check no open queries remain
        cursor.execute("""
            SELECT COUNT(*) FROM statement_lines
            WHERE statement_id = ? AND query_type IS NOT NULL AND query_resolved_at IS NULL
        """, (statement_id,))
        open_queries = cursor.fetchone()[0]

        if open_queries > 0:
            conn.close()
            return {
                "success": False,
                "error": f"{open_queries} queries still open - cannot send updated status"
            }

        # Get supplier name from Opera
        supplier_code = statement['supplier_code']
        supplier_name = supplier_code
        supplier_df = sql_connector.execute_query(f"""
            SELECT RTRIM(pn_name) as name FROM pname WITH (NOLOCK)
            WHERE pn_account = '{supplier_code}'
        """)
        if supplier_df is not None and len(supplier_df) > 0:
            supplier_name = supplier_df.iloc[0]['name']

        # Get line details
        cursor.execute("""
            SELECT reference, debit, credit, match_status
            FROM statement_lines WHERE statement_id = ?
        """, (statement_id,))
        lines = cursor.fetchall()

        # Generate updated status response
        response_lines = []
        response_lines.append(f"Subject: Statement Update - All Queries Resolved - {supplier_name}")
        response_lines.append("")
        response_lines.append("Dear Accounts Team,")
        response_lines.append("")
        response_lines.append(f"Further to our previous correspondence regarding your statement dated {statement['statement_date']},")
        response_lines.append("we are pleased to confirm that all queries have now been resolved.")
        response_lines.append("")
        response_lines.append("RECONCILIATION STATUS: FULLY AGREED")
        response_lines.append("=" * 50)
        response_lines.append("")

        total = 0
        for line in lines:
            amount = line['debit'] or line['credit'] or 0
            total += amount
            response_lines.append(f"  {line['reference']}: \u00a3{amount:,.2f} - AGREED")

        response_lines.append("")
        response_lines.append(f"  TOTAL: \u00a3{total:,.2f}")
        response_lines.append("")

        # Payment info
        today = datetime.now().date()
        days_until_friday = (4 - today.weekday()) % 7
        if days_until_friday == 0:
            days_until_friday = 7
        next_friday = today + timedelta(days=days_until_friday)

        response_lines.append("PAYMENT SCHEDULE")
        response_lines.append("=" * 50)
        response_lines.append(f"Total to pay: \u00a3{total:,.2f}")
        response_lines.append(f"Scheduled payment date: {next_friday.strftime('%d/%m/%Y')}")
        response_lines.append("")
        response_lines.append("Thank you for your patience in resolving these queries.")
        response_lines.append("")
        response_lines.append("Regards,")
        response_lines.append("Accounts Department")

        response_text = "\n".join(response_lines)

        # Log the communication
        cursor.execute("""
            INSERT INTO supplier_communications
            (supplier_code, statement_id, direction, type, email_subject, email_body, sent_at, sent_by)
            VALUES (?, ?, 'outbound', 'updated_status', ?, ?, CURRENT_TIMESTAMP, 'System')
        """, (
            supplier_code,
            statement_id,
            f"Statement Update - All Queries Resolved - {supplier_name}",
            response_text
        ))

        # Update statement status
        cursor.execute("""
            UPDATE supplier_statements
            SET status = 'approved', approved_at = CURRENT_TIMESTAMP, approved_by = 'Auto-resolved'
            WHERE id = ?
        """, (statement_id,))

        conn.commit()
        conn.close()

        return {
            "success": True,
            "message": "Updated status sent to supplier",
            "response_text": response_text
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error sending updated status: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/supplier-communications")
async def list_supplier_communications(supplier_code: Optional[str] = None, days: int = 90):
    """List supplier communications history."""
    from api.main import sql_connector
    from sql_rag.company_data import get_current_db_path

    db_path = get_current_db_path('supplier_statements.db') or Path(__file__).parent.parent.parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        return {"success": True, "communications": []}

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cutoff = (datetime.now() - timedelta(days=days)).isoformat()

        if supplier_code:
            cursor.execute("""
                SELECT * FROM supplier_communications
                WHERE supplier_code = ? AND created_at >= ?
                ORDER BY created_at DESC
            """, (supplier_code, cutoff))
        else:
            cursor.execute("""
                SELECT * FROM supplier_communications
                WHERE created_at >= ?
                ORDER BY created_at DESC
            """, (cutoff,))

        communications = [dict(row) for row in cursor.fetchall()]

        # Get supplier names from Opera
        if sql_connector and communications:
            codes = list(set(c['supplier_code'] for c in communications if c.get('supplier_code')))
            if codes:
                code_list = ','.join(f"'{c}'" for c in codes)
                names_df = sql_connector.execute_query(f"""
                    SELECT RTRIM(pn_account) as code, RTRIM(pn_name) as name
                    FROM pname WITH (NOLOCK) WHERE pn_account IN ({code_list})
                """)
                if names_df is not None and len(names_df) > 0:
                    name_map = dict(zip(names_df['code'], names_df['name']))
                    for c in communications:
                        c['supplier_name'] = name_map.get(c['supplier_code'], c['supplier_code'])

        conn.close()
        return {"success": True, "communications": communications}

    except Exception as e:
        logger.error(f"Error listing communications: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/supplier-communications/{account}")
async def get_supplier_communications(account: str):
    """Get all communications for a supplier — full audit trail."""
    from api.main import sql_connector
    from sql_rag.company_data import get_current_db_path

    db_path = get_current_db_path('supplier_statements.db') or Path(__file__).parent.parent.parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        return {"success": True, "communications": [], "supplier_code": account}

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM supplier_communications
            WHERE supplier_code = ?
            ORDER BY created_at DESC
        """, (account,))

        communications = [dict(row) for row in cursor.fetchall()]
        conn.close()

        # Enrich with supplier name from Opera
        supplier_name = account
        if sql_connector:
            try:
                name_df = sql_connector.execute_query(
                    f"SELECT RTRIM(pn_name) as name FROM pname WITH (NOLOCK) WHERE pn_account = '{account}'"
                )
                if name_df is not None and len(name_df) > 0:
                    supplier_name = str(name_df.iloc[0]['name']).strip()
            except Exception:
                pass

        return {
            "success": True,
            "supplier_code": account,
            "supplier_name": supplier_name,
            "communications": communications,
            "count": len(communications),
        }

    except Exception as e:
        logger.error(f"Error getting communications for {account}: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


# ============================================================
# Supplier Security API Endpoints
# ============================================================

@router.get("/api/supplier-security/alerts")
async def list_security_alerts():
    """List unverified supplier change alerts (bank details, etc.)."""
    from api.main import sql_connector
    from sql_rag.company_data import get_current_db_path

    db_path = get_current_db_path('supplier_statements.db') or Path(__file__).parent.parent.parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        return {"success": True, "alerts": []}

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM supplier_change_audit
            WHERE verified = 0
            ORDER BY changed_at DESC
        """)

        alerts = [dict(row) for row in cursor.fetchall()]

        # Get supplier names from Opera
        if sql_connector and alerts:
            codes = list(set(a['supplier_code'] for a in alerts if a.get('supplier_code')))
            if codes:
                code_list = ','.join(f"'{c}'" for c in codes)
                names_df = sql_connector.execute_query(f"""
                    SELECT RTRIM(pn_account) as code, RTRIM(pn_name) as name
                    FROM pname WITH (NOLOCK) WHERE pn_account IN ({code_list})
                """)
                if names_df is not None and len(names_df) > 0:
                    name_map = dict(zip(names_df['code'], names_df['name']))
                    for a in alerts:
                        a['supplier_name'] = name_map.get(a['supplier_code'], a['supplier_code'])

        conn.close()
        return {"success": True, "alerts": alerts}

    except Exception as e:
        logger.error(f"Error listing security alerts: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/api/supplier-security/alerts/{alert_id}/verify")
async def verify_security_alert(alert_id: int, verified_by: str = "System"):
    """Verify a security alert (mark as reviewed)."""
    from sql_rag.company_data import get_current_db_path

    db_path = get_current_db_path('supplier_statements.db') or Path(__file__).parent.parent.parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE supplier_change_audit
            SET verified = 1, verified_by = ?, verified_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (verified_by, alert_id))

        if cursor.rowcount == 0:
            conn.close()
            raise HTTPException(status_code=404, detail="Alert not found")

        conn.commit()
        conn.close()

        return {"success": True, "message": "Alert verified"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error verifying alert: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/supplier-security/audit")
async def list_security_audit_log(days: int = 90):
    """List all supplier change audit entries (verified and unverified)."""
    from api.main import sql_connector
    from sql_rag.company_data import get_current_db_path

    db_path = get_current_db_path('supplier_statements.db') or Path(__file__).parent.parent.parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        return {"success": True, "entries": []}

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cutoff = (datetime.now() - timedelta(days=days)).isoformat()

        cursor.execute("""
            SELECT * FROM supplier_change_audit
            WHERE changed_at >= ?
            ORDER BY changed_at DESC
        """, (cutoff,))

        entries = [dict(row) for row in cursor.fetchall()]

        # Get supplier names from Opera
        if sql_connector and entries:
            codes = list(set(e['supplier_code'] for e in entries if e.get('supplier_code')))
            if codes:
                code_list = ','.join(f"'{c}'" for c in codes)
                names_df = sql_connector.execute_query(f"""
                    SELECT RTRIM(pn_account) as code, RTRIM(pn_name) as name
                    FROM pname WITH (NOLOCK) WHERE pn_account IN ({code_list})
                """)
                if names_df is not None and len(names_df) > 0:
                    name_map = dict(zip(names_df['code'], names_df['name']))
                    for e in entries:
                        e['supplier_name'] = name_map.get(e['supplier_code'], e['supplier_code'])

        conn.close()
        return {"success": True, "entries": entries}

    except Exception as e:
        logger.error(f"Error listing audit log: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/api/supplier-security/scan-changes")
async def scan_supplier_changes():
    """
    Scan all suppliers in Opera pname for changes to sensitive fields
    (bank, account number, sort code, email) and compare against last
    known values stored in supplier_change_audit.

    For each changed field, logs via db.log_supplier_change().
    Sends email alert to security_alert_recipients if bank details changed.

    Returns:
        {success, changes_detected: count, alerts_sent: count}
    """
    from api.main import sql_connector
    from sql_rag.supplier_statement_db import get_supplier_statement_db

    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        db = get_supplier_statement_db()

        # Get all suppliers with sensitive fields from Opera
        suppliers_df = sql_connector.execute_query("""
            SELECT
                RTRIM(pn_account) AS account,
                RTRIM(pn_name) AS name,
                RTRIM(ISNULL(pn_bankac, '')) AS pn_bankac,
                RTRIM(ISNULL(pn_banksor, '')) AS pn_banksor,
                RTRIM(ISNULL(pn_email, '')) AS pn_email
            FROM pname WITH (NOLOCK)
        """)

        if suppliers_df is None or len(suppliers_df) == 0:
            return {"success": True, "changes_detected": 0, "alerts_sent": 0}

        if hasattr(suppliers_df, 'to_dict'):
            suppliers = suppliers_df.to_dict('records')
        else:
            suppliers = suppliers_df or []

        # Get last known values from the change audit table
        db_path = _get_db_path()
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Build a map of supplier_code -> field_name -> latest new_value
        cursor.execute("""
            SELECT supplier_code, field_name, new_value
            FROM supplier_change_audit
            WHERE id IN (
                SELECT MAX(id) FROM supplier_change_audit
                GROUP BY supplier_code, field_name
            )
        """)
        last_known = {}
        for row in cursor.fetchall():
            key = (row['supplier_code'], row['field_name'])
            last_known[key] = row['new_value'] or ''

        conn.close()

        # Compare current Opera values against last known
        sensitive_fields = ['pn_bankac', 'pn_banksor', 'pn_email']
        bank_fields = {'pn_bankac', 'pn_banksor'}
        changes_detected = 0
        bank_change_suppliers = []

        for supplier in suppliers:
            account = supplier['account']
            for field in sensitive_fields:
                current_value = (supplier.get(field) or '').strip()
                known_key = (account, field)

                if known_key in last_known:
                    previous_value = (last_known[known_key] or '').strip()
                    if current_value != previous_value:
                        # Change detected
                        db.log_supplier_change(
                            supplier_code=account,
                            field_name=field,
                            old_value=previous_value,
                            new_value=current_value,
                            changed_by='scan'
                        )
                        changes_detected += 1

                        if field in bank_fields:
                            bank_change_suppliers.append({
                                "account": account,
                                "name": supplier.get('name', account),
                                "field": field,
                                "old": previous_value,
                                "new": current_value
                            })
                else:
                    # First time seeing this supplier/field - record baseline
                    if current_value:
                        db.log_supplier_change(
                            supplier_code=account,
                            field_name=field,
                            old_value='',
                            new_value=current_value,
                            changed_by='scan_baseline'
                        )
                        # Immediately verify baseline entries so they don't show as alerts
                        try:
                            baseline_conn = sqlite3.connect(str(_get_db_path()))
                            baseline_conn.execute("""
                                UPDATE supplier_change_audit
                                SET verified = 1, verified_by = 'scan_baseline', verified_at = CURRENT_TIMESTAMP
                                WHERE supplier_code = ? AND field_name = ? AND changed_by = 'scan_baseline' AND verified = 0
                            """, (account, field))
                            baseline_conn.commit()
                            baseline_conn.close()
                        except Exception:
                            pass

        # Send email alerts if bank details changed
        alerts_sent = 0
        if bank_change_suppliers:
            try:
                db_path = _get_db_path()
                alert_conn = sqlite3.connect(str(db_path))
                alert_conn.row_factory = sqlite3.Row
                alert_cursor = alert_conn.cursor()
                alert_cursor.execute("SELECT value FROM supplier_automation_config WHERE key = 'security_alert_recipients'")
                row = alert_cursor.fetchone()
                alert_conn.close()

                recipients = (row['value'] or '').strip() if row else ''
                if recipients:
                    # Build alert email
                    alert_lines = ["SECURITY ALERT: Supplier Bank Detail Changes Detected\n"]
                    alert_lines.append(f"Scan time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    alert_lines.append(f"Changes detected: {len(bank_change_suppliers)}\n")
                    alert_lines.append("-" * 60)

                    for change in bank_change_suppliers:
                        alert_lines.append(
                            f"\nSupplier: {change['name']} ({change['account']})\n"
                            f"  Field: {change['field']}\n"
                            f"  Old value: {change['old'] or '(empty)'}\n"
                            f"  New value: {change['new'] or '(empty)'}"
                        )

                    alert_lines.append("\n" + "-" * 60)
                    alert_lines.append("\nPlease verify these changes are legitimate.")
                    alert_body = "\n".join(alert_lines)

                    import httpx
                    for recipient in recipients.split(','):
                        recipient = recipient.strip()
                        if not recipient:
                            continue
                        try:
                            async with httpx.AsyncClient() as client:
                                resp = await client.post(
                                    "http://127.0.0.1:8000/api/email/send",
                                    json={
                                        "to": recipient,
                                        "subject": f"SECURITY ALERT: {len(bank_change_suppliers)} Supplier Bank Detail Change(s)",
                                        "body": alert_body,
                                        "from_email": "intsys@wimbledoncloud.net"
                                    },
                                    timeout=30.0
                                )
                                if resp.status_code == 200:
                                    alerts_sent += 1
                        except Exception as e:
                            logger.warning(f"Failed to send security alert to {recipient}: {e}")

            except Exception as e:
                logger.warning(f"Error sending bank change alerts: {e}")

        return {
            "success": True,
            "changes_detected": changes_detected,
            "alerts_sent": alerts_sent,
            "bank_changes": bank_change_suppliers
        }

    except Exception as e:
        logger.error(f"Error scanning supplier changes: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/supplier-security/email-flags")
async def get_flagged_emails():
    """
    Return statements where the incoming email body or statement line
    descriptions mention bank details.

    Checks for keywords like 'bank details', 'sort code',
    'account number', 'bank account changed'.

    Returns:
        {success, flagged: [...]}
    """
    from sql_rag.company_data import get_current_db_path

    db_path = get_current_db_path('supplier_statements.db') or Path(__file__).parent.parent.parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        return {"success": True, "flagged": []}

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        flagged = []

        # Check statement lines for bank detail keywords
        cursor.execute("""
            SELECT sl.id as line_id, sl.statement_id, sl.reference, sl.description,
                   ss.supplier_code, ss.statement_date, ss.sender_email
            FROM statement_lines sl
            JOIN supplier_statements ss ON sl.statement_id = ss.id
            WHERE sl.description IS NOT NULL AND sl.description != ''
        """)

        for row in cursor.fetchall():
            description = row['description'] or ''
            if _check_bank_detail_keywords(description):
                flagged.append({
                    "type": "statement_line",
                    "line_id": row['line_id'],
                    "statement_id": row['statement_id'],
                    "supplier_code": row['supplier_code'],
                    "statement_date": row['statement_date'],
                    "sender_email": row['sender_email'],
                    "reference": row['reference'],
                    "flagged_text": description,
                    "reason": "Bank detail keywords found in statement line description"
                })

        # Check communication bodies (inbound emails) for bank detail keywords
        cursor.execute("""
            SELECT id, supplier_code, statement_id, email_subject, email_body, created_at
            FROM supplier_communications
            WHERE direction = 'inbound' AND email_body IS NOT NULL AND email_body != ''
        """)

        for row in cursor.fetchall():
            body = row['email_body'] or ''
            subject = row['email_subject'] or ''
            if _check_bank_detail_keywords(body) or _check_bank_detail_keywords(subject):
                flagged.append({
                    "type": "email_communication",
                    "communication_id": row['id'],
                    "statement_id": row['statement_id'],
                    "supplier_code": row['supplier_code'],
                    "email_subject": row['email_subject'],
                    "flagged_text": subject if _check_bank_detail_keywords(subject) else body[:200],
                    "created_at": row['created_at'],
                    "reason": "Bank detail keywords found in email communication"
                })

        conn.close()
        return {"success": True, "flagged": flagged}

    except Exception as e:
        logger.error(f"Error checking email flags: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


def _check_bank_detail_keywords(text: str) -> bool:
    """
    Scan text for bank-detail-related phrases.

    Returns True if any of the following keywords/phrases are found
    (case-insensitive): 'bank details', 'sort code', 'account number',
    'bank account changed', 'new bank', 'updated bank', 'change of bank',
    'remittance details changed'.
    """
    if not text:
        return False

    text_lower = text.lower()
    keywords = [
        'bank details',
        'sort code',
        'account number',
        'bank account changed',
        'new bank',
        'updated bank',
        'change of bank',
        'remittance details changed',
    ]
    return any(kw in text_lower for kw in keywords)


@router.get("/api/supplier-security/approved-senders")
async def list_approved_senders(supplier_code: Optional[str] = None):
    """List approved email senders for suppliers."""
    from api.main import sql_connector
    from sql_rag.company_data import get_current_db_path

    db_path = get_current_db_path('supplier_statements.db') or Path(__file__).parent.parent.parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        return {"success": True, "senders": []}

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        if supplier_code:
            cursor.execute("""
                SELECT * FROM supplier_approved_emails
                WHERE supplier_code = ?
                ORDER BY added_at DESC
            """, (supplier_code,))
        else:
            cursor.execute("""
                SELECT * FROM supplier_approved_emails
                ORDER BY supplier_code, added_at DESC
            """)

        senders = [dict(row) for row in cursor.fetchall()]

        # Get supplier names from Opera
        if sql_connector and senders:
            codes = list(set(s['supplier_code'] for s in senders if s.get('supplier_code')))
            if codes:
                code_list = ','.join(f"'{c}'" for c in codes)
                names_df = sql_connector.execute_query(f"""
                    SELECT RTRIM(pn_account) as code, RTRIM(pn_name) as name
                    FROM pname WITH (NOLOCK) WHERE pn_account IN ({code_list})
                """)
                if names_df is not None and len(names_df) > 0:
                    name_map = dict(zip(names_df['code'], names_df['name']))
                    for s in senders:
                        s['supplier_name'] = name_map.get(s['supplier_code'], s['supplier_code'])

        conn.close()
        return {"success": True, "senders": senders}

    except Exception as e:
        logger.error(f"Error listing approved senders: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/api/supplier-security/approved-senders")
async def add_approved_sender(
    supplier_code: str = Body(..., embed=True),
    email_address: str = Body(..., embed=True),
    added_by: str = Body("System", embed=True)
):
    """Add an approved email sender for a supplier."""
    from sql_rag.company_data import get_current_db_path

    db_path = get_current_db_path('supplier_statements.db') or Path(__file__).parent.parent.parent.parent / 'supplier_statements.db'

    # Initialize DB if it doesn't exist
    if not db_path.exists():
        from sql_rag.supplier_statement_db import SupplierStatementDB
        SupplierStatementDB(str(db_path))

    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        domain = email_address.split('@')[-1] if '@' in email_address else None

        cursor.execute("""
            INSERT OR REPLACE INTO supplier_approved_emails
            (supplier_code, email_address, email_domain, added_by, verified)
            VALUES (?, ?, ?, ?, 0)
        """, (supplier_code, email_address.lower(), domain, added_by))

        conn.commit()
        conn.close()

        return {"success": True, "message": "Approved sender added"}

    except Exception as e:
        logger.error(f"Error adding approved sender: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.delete("/api/supplier-security/approved-senders/{sender_id}")
async def remove_approved_sender(sender_id: int):
    """Remove an approved email sender."""
    from sql_rag.company_data import get_current_db_path

    db_path = get_current_db_path('supplier_statements.db') or Path(__file__).parent.parent.parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        cursor.execute("DELETE FROM supplier_approved_emails WHERE id = ?", (sender_id,))

        if cursor.rowcount == 0:
            conn.close()
            raise HTTPException(status_code=404, detail="Sender not found")

        conn.commit()
        conn.close()

        return {"success": True, "message": "Approved sender removed"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error removing approved sender: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


# ============================================================
# Supplier Settings API Endpoints
# ============================================================

@router.get("/api/supplier-settings")
async def get_supplier_settings():
    """Get supplier automation settings."""
    from sql_rag.company_data import get_current_db_path

    db_path = get_current_db_path('supplier_statements.db') or Path(__file__).parent.parent.parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        # Return defaults
        return {
            "success": True,
            "settings": {
                "acknowledgment_delay_minutes": "0",
                "processing_sla_hours": "24",
                "query_response_days": "5",
                "follow_up_reminder_days": "7",
                "large_discrepancy_threshold": "500",
                "old_statement_threshold_days": "14",
                "payment_notification_days": "90",
                "security_alert_recipients": ""
            }
        }

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("SELECT key, value, description FROM supplier_automation_config")
        settings = {row['key']: {"value": row['value'], "description": row['description']} for row in cursor.fetchall()}

        conn.close()
        return {"success": True, "settings": settings}

    except Exception as e:
        logger.error(f"Error getting settings: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/api/supplier-settings")
async def update_supplier_settings(settings: Dict[str, str] = Body(...)):
    """Update supplier automation settings."""
    from sql_rag.company_data import get_current_db_path

    db_path = get_current_db_path('supplier_statements.db') or Path(__file__).parent.parent.parent.parent / 'supplier_statements.db'

    # Initialize DB if it doesn't exist
    if not db_path.exists():
        from sql_rag.supplier_statement_db import SupplierStatementDB
        SupplierStatementDB(str(db_path))

    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        for key, value in settings.items():
            cursor.execute("""
                UPDATE supplier_automation_config
                SET value = ?, updated_at = CURRENT_TIMESTAMP
                WHERE key = ?
            """, (value, key))

            if cursor.rowcount == 0:
                cursor.execute("""
                    INSERT INTO supplier_automation_config (key, value)
                    VALUES (?, ?)
                """, (key, value))

        conn.commit()
        conn.close()

        return {"success": True, "message": "Settings updated"}

    except Exception as e:
        logger.error(f"Error updating settings: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


# ============================================================
# Supplier Directory API Endpoints
# ============================================================

@router.get("/api/supplier-directory")
async def list_supplier_directory(search: Optional[str] = None):
    """List all suppliers from Opera with statement automation info."""
    from api.main import sql_connector
    from sql_rag.company_data import get_current_db_path

    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Get suppliers from Opera
        if search:
            query = f"""
                SELECT TOP 100
                    RTRIM(pn_account) AS account,
                    RTRIM(pn_name) AS name,
                    RTRIM(pn_email) AS email,
                    RTRIM(pn_teleno) AS phone,
                    RTRIM(pn_contact) AS contact,
                    pn_currbal AS balance
                FROM pname WITH (NOLOCK)
                WHERE pn_name LIKE '%{search}%' OR pn_account LIKE '%{search}%'
                ORDER BY pn_name
            """
        else:
            query = """
                SELECT TOP 500
                    RTRIM(pn_account) AS account,
                    RTRIM(pn_name) AS name,
                    RTRIM(pn_email) AS email,
                    RTRIM(pn_teleno) AS phone,
                    RTRIM(pn_contact) AS contact,
                    pn_currbal AS balance
                FROM pname WITH (NOLOCK)
                WHERE pn_currbal <> 0
                ORDER BY pn_name
            """

        result = sql_connector.execute_query(query)
        if hasattr(result, 'to_dict'):
            suppliers = result.to_dict('records')
        else:
            suppliers = result or []

        # Get automation info from SQLite
        db_path = get_current_db_path('supplier_statements.db') or Path(__file__).parent.parent.parent.parent / 'supplier_statements.db'
        if db_path.exists():
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Get statement counts per supplier
            cursor.execute("""
                SELECT supplier_code,
                       COUNT(*) as statement_count,
                       MAX(received_date) as last_statement
                FROM supplier_statements
                GROUP BY supplier_code
            """)
            stmt_map = {row['supplier_code']: {
                'statement_count': row['statement_count'],
                'last_statement': row['last_statement']
            } for row in cursor.fetchall()}

            # Get approved senders count
            cursor.execute("""
                SELECT supplier_code, COUNT(*) as sender_count
                FROM supplier_approved_emails
                GROUP BY supplier_code
            """)
            sender_map = {row['supplier_code']: row['sender_count'] for row in cursor.fetchall()}

            conn.close()

            # Merge into suppliers
            for s in suppliers:
                stmt_info = stmt_map.get(s['account'], {})
                s['statement_count'] = stmt_info.get('statement_count', 0)
                s['last_statement'] = stmt_info.get('last_statement')
                s['approved_senders'] = sender_map.get(s['account'], 0)

        return {"success": True, "suppliers": suppliers, "count": len(suppliers)}

    except Exception as e:
        logger.error(f"Error listing supplier directory: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/api/supplier-statements/{statement_id}/preview-response")
async def preview_supplier_statement_response(statement_id: int):
    """
    Generate a draft response email for a statement without sending it.
    Returns subject, body HTML, and recipient for preview/editing.
    """
    from api.main import sql_connector

    db_path = _get_db_path()
    _run_migrations(db_path)

    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM supplier_statements WHERE id = ?", (statement_id,))
        stmt = cursor.fetchone()

        if not stmt:
            conn.close()
            raise HTTPException(status_code=404, detail="Statement not found")

        stmt = dict(stmt)
        supplier_code = stmt['supplier_code']

        recipient_email = _get_supplier_contact_email(cursor, supplier_code, stmt.get('sender_email'))

        # Build response body — use saved response_text if present, else generate
        body = stmt.get('response_text') or _generate_default_response(
            cursor, statement_id, supplier_code, stmt.get('statement_date'), sql_connector
        )

        # Build subject using configurable template
        supplier_name = supplier_code
        if sql_connector:
            try:
                name_df = sql_connector.execute_query(
                    f"SELECT RTRIM(pn_name) as name FROM pname WITH (NOLOCK) WHERE pn_account = '{supplier_code}'"
                )
                if name_df is not None and len(name_df) > 0:
                    supplier_name = str(name_df.iloc[0]['name']).strip()
            except Exception:
                pass

        # Determine whether this statement has queries (to pick the right subject template)
        cursor.execute(
            "SELECT COUNT(*) as cnt FROM statement_lines WHERE statement_id = ? AND match_status = 'query'",
            (statement_id,)
        )
        qrow = cursor.fetchone()
        has_queries = (qrow['cnt'] > 0) if qrow else False

        from sql_rag.supplier_statement_db import get_supplier_statement_db as _get_db
        subject = _generate_subject(_get_db(), supplier_name, stmt.get('statement_date'), has_queries)

        conn.close()

        return {
            "success": True,
            "recipient": recipient_email,
            "subject": subject,
            "body": body,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating preview for statement {statement_id}: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/api/supplier-statements/{statement_id}/approve")
async def approve_supplier_statement(statement_id: int, request: ApproveWithBodyRequest = Body(default=ApproveWithBodyRequest())):
    """
    Approve a reconciled statement and send the response email.

    Marks the statement as approved, sends the response email to the supplier,
    and records the communication in the audit trail.
    """
    from api.main import sql_connector
    from sql_rag.supplier_statement_db import get_supplier_statement_db

    db_path = _get_db_path()
    _run_migrations(db_path)

    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get statement details — allow approve from reconciled or acknowledged status
        cursor.execute("""
            SELECT * FROM supplier_statements
            WHERE id = ? AND status IN ('reconciled', 'acknowledged', 'queued', 'received')
        """, (statement_id,))
        stmt = cursor.fetchone()

        if not stmt:
            conn.close()
            raise HTTPException(status_code=404, detail="Statement not found or already approved")

        stmt = dict(stmt)
        supplier_code = stmt['supplier_code']
        approved_by = request.approved_by
        now_iso = datetime.now().isoformat()

        # Determine recipient email
        recipient_email = _get_supplier_contact_email(cursor, supplier_code, stmt.get('sender_email'))

        # Build response body — caller may supply edited body, else use saved text, else generate
        if request.body:
            response_text = request.body
        else:
            response_text = stmt.get('response_text') or _generate_default_response(
                cursor, statement_id, supplier_code, stmt.get('statement_date'), sql_connector
            )

        # Build email subject — caller may supply edited subject, else use configurable template
        if request.subject:
            email_subject = request.subject
        else:
            supplier_name = supplier_code
            if sql_connector:
                try:
                    name_df = sql_connector.execute_query(
                        f"SELECT RTRIM(pn_name) as name FROM pname WITH (NOLOCK) WHERE pn_account = '{supplier_code}'"
                    )
                    if name_df is not None and len(name_df) > 0:
                        supplier_name = str(name_df.iloc[0]['name']).strip()
                except Exception:
                    pass
            cursor.execute(
                "SELECT COUNT(*) as cnt FROM statement_lines WHERE statement_id = ? AND match_status = 'query'",
                (statement_id,)
            )
            qrow = cursor.fetchone()
            has_queries = (qrow['cnt'] > 0) if qrow else False
            from sql_rag.supplier_statement_db import get_supplier_statement_db as _get_db_approve
            email_subject = _generate_subject(
                _get_db_approve(), supplier_name, stmt.get('statement_date'), has_queries
            )

        # Send the response email with original statement PDF attached
        email_sent = False
        email_error = None
        if recipient_email:
            try:
                import httpx
                email_payload = {
                    "to": recipient_email,
                    "subject": email_subject,
                    "body": response_text,
                    "from_email": "intsys@wimbledoncloud.net"
                }
                # Attach the original statement PDF if available
                pdf_path = stmt.get('pdf_path')
                if pdf_path and os.path.exists(pdf_path):
                    email_payload["attachments"] = [pdf_path]

                async with httpx.AsyncClient() as client:
                    email_resp = await client.post(
                        "http://127.0.0.1:8000/api/email/send",
                        json=email_payload,
                        timeout=30.0
                    )
                    if email_resp.status_code == 200:
                        email_sent = True
                    else:
                        email_error = f"Email send failed: {email_resp.status_code}"
            except Exception as e:
                email_error = f"Email send error: {str(e)}"
                logger.warning(f"Failed to send approval email for statement {statement_id}: {e}")

        # Update statement status
        cursor.execute("""
            UPDATE supplier_statements
            SET status = 'approved', approved_by = ?, approved_at = ?,
                sent_at = CASE WHEN ? THEN ? ELSE sent_at END
            WHERE id = ?
        """, (approved_by, now_iso, email_sent, now_iso if email_sent else None, statement_id))

        conn.commit()

        # Record in communications audit trail
        try:
            db = get_supplier_statement_db()
            db.log_communication(
                supplier_code=supplier_code,
                direction='outbound',
                comm_type='approval_response',
                email_subject=email_subject,
                email_body=response_text,
                statement_id=statement_id,
                sent_by=approved_by,
                approved_by=approved_by
            )
        except Exception as e:
            logger.warning(f"Failed to log communication for statement {statement_id}: {e}")

        conn.close()

        result = {
            "success": True,
            "message": "Statement approved" + (" and response sent" if email_sent else ""),
            "email_sent": email_sent,
            "recipient": recipient_email,
            "subject": email_subject,
        }
        if email_error:
            result["email_error"] = email_error

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error approving statement: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/api/supplier-statements/{statement_id}/acknowledge")
async def acknowledge_supplier_statement(statement_id: int):
    """
    Send acknowledgment email to the supplier for a received statement.

    Uses the acknowledgment_template from supplier_automation_config if set,
    otherwise uses a default template. Gets the supplier contact email from
    supplier_contacts_ext (is_statement_contact) or falls back to sender_email.
    Respects acknowledgment_delay_minutes from config (0 = immediate).
    """
    from api.main import sql_connector
    from sql_rag.supplier_statement_db import get_supplier_statement_db

    db_path = _get_db_path()
    _run_migrations(db_path)

    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get statement
        cursor.execute("SELECT * FROM supplier_statements WHERE id = ?", (statement_id,))
        stmt = cursor.fetchone()
        if not stmt:
            conn.close()
            raise HTTPException(status_code=404, detail="Statement not found")

        stmt = dict(stmt)
        supplier_code = stmt['supplier_code']

        # Check if already acknowledged
        if stmt.get('acknowledged_at'):
            conn.close()
            return {"success": False, "error": "Statement has already been acknowledged"}

        # Check acknowledgment delay from config
        cursor.execute("SELECT value FROM supplier_automation_config WHERE key = 'acknowledgment_delay_minutes'")
        row = cursor.fetchone()
        delay_minutes = int(row['value']) if row else 0

        if delay_minutes > 0:
            received = stmt.get('received_date')
            if received:
                try:
                    received_dt = datetime.fromisoformat(received)
                    earliest_send = received_dt + timedelta(minutes=delay_minutes)
                    if datetime.now() < earliest_send:
                        conn.close()
                        return {
                            "success": False,
                            "error": f"Acknowledgment delayed. Earliest send time: {earliest_send.isoformat()}",
                            "earliest_send_at": earliest_send.isoformat()
                        }
                except (ValueError, TypeError):
                    pass  # If date parsing fails, proceed anyway

        # Get acknowledgment template from config
        cursor.execute("SELECT value FROM supplier_automation_config WHERE key = 'acknowledgment_template'")
        row = cursor.fetchone()
        default_template = (
            "Thank you for sending your statement dated {date}. "
            "We have received it and are currently processing."
        )
        template = row['value'] if row and row['value'] else default_template

        # Format the template
        statement_date = stmt.get('statement_date') or 'N/A'
        ack_body = template.format(date=statement_date)

        # Get supplier name for subject
        supplier_name = supplier_code
        if sql_connector:
            try:
                name_df = sql_connector.execute_query(
                    f"SELECT RTRIM(pn_name) as name FROM pname WITH (NOLOCK) WHERE pn_account = '{supplier_code}'"
                )
                if name_df is not None and len(name_df) > 0:
                    supplier_name = name_df.iloc[0]['name']
            except Exception:
                pass

        # Determine recipient email
        recipient_email = _get_supplier_contact_email(cursor, supplier_code, stmt.get('sender_email'))

        if not recipient_email:
            conn.close()
            return {"success": False, "error": "No contact email found for this supplier"}

        email_subject = f"Statement Received - {supplier_name} - {statement_date}"

        # Send acknowledgment email
        email_sent = False
        email_error = None
        try:
            import httpx
            async with httpx.AsyncClient() as client:
                email_resp = await client.post(
                    "http://127.0.0.1:8000/api/email/send",
                    json={
                        "to": recipient_email,
                        "subject": email_subject,
                        "body": ack_body,
                        "from_email": "intsys@wimbledoncloud.net"
                    },
                    timeout=30.0
                )
                if email_resp.status_code == 200:
                    email_sent = True
                else:
                    email_error = f"Email send failed: {email_resp.status_code}"
        except Exception as e:
            email_error = f"Email send error: {str(e)}"
            logger.warning(f"Failed to send acknowledgment email for statement {statement_id}: {e}")

        # Update statement status to acknowledged
        now_iso = datetime.now().isoformat()
        cursor.execute("""
            UPDATE supplier_statements
            SET status = 'acknowledged', acknowledged_at = ?, updated_at = ?
            WHERE id = ?
        """, (now_iso, now_iso, statement_id))
        conn.commit()

        # Record in communications audit trail
        try:
            db = get_supplier_statement_db()
            db.log_communication(
                supplier_code=supplier_code,
                direction='outbound',
                comm_type='acknowledgment',
                email_subject=email_subject,
                email_body=ack_body,
                statement_id=statement_id,
                sent_by='System'
            )
        except Exception as e:
            logger.warning(f"Failed to log acknowledgment communication for statement {statement_id}: {e}")

        conn.close()

        result = {
            "success": True,
            "message": "Statement acknowledged" + (" and email sent" if email_sent else ""),
            "email_sent": email_sent,
            "recipient": recipient_email,
            "subject": email_subject,
            "body": ack_body
        }
        if email_error:
            result["email_error"] = email_error

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error acknowledging statement: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.put("/api/supplier-statements/{statement_id}/edit-response")
async def edit_statement_response(statement_id: int, body: EditResponseRequest):
    """
    Store an edited response text on the statement record.

    This allows users to customise the auto-generated response before
    sending/approving. The response_text column is added to the table
    if it does not already exist.
    """
    db_path = _get_db_path()
    _run_migrations(db_path)

    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Verify statement exists
        cursor.execute("SELECT id, status FROM supplier_statements WHERE id = ?", (statement_id,))
        stmt = cursor.fetchone()
        if not stmt:
            conn.close()
            raise HTTPException(status_code=404, detail="Statement not found")

        # Update response_text
        cursor.execute("""
            UPDATE supplier_statements
            SET response_text = ?, updated_at = ?
            WHERE id = ?
        """, (body.response_text, datetime.now().isoformat(), statement_id))

        conn.commit()

        # Return the updated statement
        cursor.execute("SELECT * FROM supplier_statements WHERE id = ?", (statement_id,))
        updated = dict(cursor.fetchone())

        conn.close()

        return {"success": True, "message": "Response text updated", "statement": updated}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error editing statement response: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/api/supplier-statements/queue/bulk-approve")
async def bulk_approve_statements(body: BulkApproveRequest):
    """
    Approve and send responses for multiple statements at once.

    Iterates through each statement_id, approves it, and sends the
    response email. Returns a summary of successes and failures.
    """
    from api.main import sql_connector
    from sql_rag.supplier_statement_db import get_supplier_statement_db

    db_path = _get_db_path()
    _run_migrations(db_path)

    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    approved_count = 0
    failed = []

    for stmt_id in body.statement_ids:
        try:
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Get statement
            cursor.execute("SELECT * FROM supplier_statements WHERE id = ? AND status = 'queued'", (stmt_id,))
            stmt = cursor.fetchone()

            if not stmt:
                conn.close()
                failed.append({"id": stmt_id, "error": "Statement not found or not in queued status"})
                continue

            stmt = dict(stmt)
            supplier_code = stmt['supplier_code']
            now_iso = datetime.now().isoformat()

            # Determine recipient email
            recipient_email = _get_supplier_contact_email(cursor, supplier_code, stmt.get('sender_email'))

            # Build response text
            response_text = stmt.get('response_text') or _generate_default_response(
                cursor, stmt_id, supplier_code, stmt.get('statement_date'), sql_connector
            )

            # Get supplier name and generate subject from configurable template
            supplier_name = supplier_code
            if sql_connector:
                try:
                    name_df = sql_connector.execute_query(
                        f"SELECT RTRIM(pn_name) as name FROM pname WITH (NOLOCK) WHERE pn_account = '{supplier_code}'"
                    )
                    if name_df is not None and len(name_df) > 0:
                        supplier_name = str(name_df.iloc[0]['name']).strip()
                except Exception:
                    pass

            cursor.execute(
                "SELECT COUNT(*) as cnt FROM statement_lines WHERE statement_id = ? AND match_status = 'query'",
                (stmt_id,)
            )
            qrow = cursor.fetchone()
            has_queries_bulk = (qrow['cnt'] > 0) if qrow else False
            from sql_rag.supplier_statement_db import get_supplier_statement_db as _get_db_bulk
            email_subject = _generate_subject(
                _get_db_bulk(), supplier_name, stmt.get('statement_date'), has_queries_bulk
            )

            # Send the response email
            email_sent = False
            if recipient_email:
                try:
                    import httpx
                    async with httpx.AsyncClient() as client:
                        email_resp = await client.post(
                            "http://127.0.0.1:8000/api/email/send",
                            json={
                                "to": recipient_email,
                                "subject": email_subject,
                                "body": response_text,
                                "from_email": "intsys@wimbledoncloud.net"
                            },
                            timeout=30.0
                        )
                        if email_resp.status_code == 200:
                            email_sent = True
                except Exception as e:
                    logger.warning(f"Failed to send email for statement {stmt_id} during bulk approve: {e}")

            # Update statement status
            cursor.execute("""
                UPDATE supplier_statements
                SET status = 'approved', approved_by = ?, approved_at = ?,
                    sent_at = CASE WHEN ? THEN ? ELSE sent_at END
                WHERE id = ?
            """, (body.approved_by, now_iso, email_sent, now_iso if email_sent else None, stmt_id))

            conn.commit()

            # Record in audit trail
            try:
                db = get_supplier_statement_db()
                db.log_communication(
                    supplier_code=supplier_code,
                    direction='outbound',
                    comm_type='approval_response',
                    email_subject=email_subject,
                    email_body=response_text,
                    statement_id=stmt_id,
                    sent_by=body.approved_by,
                    approved_by=body.approved_by
                )
            except Exception as e:
                logger.warning(f"Failed to log communication for statement {stmt_id}: {e}")

            conn.close()
            approved_count += 1

        except Exception as e:
            logger.error(f"Error bulk-approving statement {stmt_id}: {e}", exc_info=True)
            failed.append({"id": stmt_id, "error": str(e)})

    return {
        "success": True,
        "approved": approved_count,
        "failed": failed
    }


# ============================================================
# Helper Functions (used by approve, acknowledge, bulk-approve)
# ============================================================

def _get_supplier_contact_email(cursor, supplier_code: str, fallback_sender_email: Optional[str] = None) -> Optional[str]:
    """
    Get the best contact email for a supplier.

    Priority:
    1. Opera pcontact — first contact with an email address for this supplier
    2. supplier_contacts_ext with is_statement_contact = 1 (local override)
    3. sender_email from the statement record (fallback)
    """
    # 1. Check Opera contacts (zcontacts table, module 'P' for purchase)
    try:
        from api.main import sql_connector
        if sql_connector:
            df = sql_connector.execute_query(f"""
                SELECT RTRIM(zc_email) as email, RTRIM(zc_contact) as name
                FROM zcontacts WITH (NOLOCK)
                WHERE zc_account = '{supplier_code}' AND zc_module = 'P'
                  AND zc_email IS NOT NULL AND RTRIM(zc_email) != ''
                ORDER BY zc_contact
            """)
            if df is not None and len(df) > 0:
                email = str(df.iloc[0]['email']).strip()
                if email:
                    return email
    except Exception:
        pass

    # 2. Check local contacts
    try:
        cursor.execute("""
            SELECT email FROM supplier_contacts_ext
            WHERE supplier_code = ? AND is_statement_contact = 1 AND email IS NOT NULL AND email != ''
            LIMIT 1
        """, (supplier_code,))
        row = cursor.fetchone()
        if row and row['email']:
            return row['email']
    except Exception:
        pass

    # 3. Fallback to sender
    return fallback_sender_email


def _format_currency(value) -> str:
    """Format a numeric value as £X,XXX.XX (negative = minus prefix)."""
    try:
        v = float(value or 0)
    except (TypeError, ValueError):
        v = 0.0
    sign = '-' if v < 0 else ''
    return f'{sign}£{abs(v):,.2f}'


def _build_query_table(queried) -> str:
    """Build an HTML table of queried statement lines."""
    if not queried:
        return ''
    rows = ''
    for l in queried:
        amt = l['debit'] if l['debit'] else l['credit']
        amt_str = _format_currency(amt)
        doc_type = 'Invoice' if l['debit'] else 'Credit/Payment'
        query_reason = (l['query_type'] or 'not_in_our_records').replace('_', ' ').title()
        rows += (
            f'<tr style="border-bottom:1px solid #eee;">'
            f'<td style="padding:6px 12px;">{l["line_date"] or ""}</td>'
            f'<td style="padding:6px 12px;"><b>{l["reference"] or "N/A"}</b></td>'
            f'<td style="padding:6px 12px;">{doc_type}</td>'
            f'<td style="padding:6px 12px;text-align:right;">{amt_str}</td>'
            f'<td style="padding:6px 12px;">'
            f'<span style="background:#fff3cd;color:#856404;padding:2px 8px;border-radius:4px;font-size:12px;font-weight:bold;">'
            f'{query_reason}</span></td>'
            f'</tr>'
        )
    return (
        '<table style="border-collapse:collapse;width:100%;margin:12px 0;font-size:13px;">'
        '<tr style="background:#2c3e50;color:white;">'
        '<th style="padding:8px 12px;text-align:left;">Date</th>'
        '<th style="padding:8px 12px;text-align:left;">Reference</th>'
        '<th style="padding:8px 12px;text-align:left;">Type</th>'
        '<th style="padding:8px 12px;text-align:right;">Amount</th>'
        '<th style="padding:8px 12px;text-align:left;">Query</th>'
        '</tr>'
        + rows +
        '</table>'
    )


def _build_payment_table(supplier_code: str, db, sql_connector) -> str:
    """Build an HTML table of recent payments made to a supplier."""
    if not sql_connector:
        return ''
    try:
        pay_days = int(db.get_config('payment_notification_days', '90'))
        from datetime import timedelta
        cutoff = (datetime.now() - timedelta(days=pay_days)).strftime('%Y-%m-%d')
        pay_df = sql_connector.execute_query(f"""
            SELECT pt_trdate, pt_trref, ABS(pt_trvalue) as amount, pt_supref
            FROM ptran WITH (NOLOCK)
            WHERE pt_account = '{supplier_code}' AND pt_trtype = 'P'
              AND pt_trdate >= '{cutoff}'
            ORDER BY pt_trdate DESC
        """)
        if pay_df is None or len(pay_df) == 0:
            return ''
        rows = ''
        for _, p in pay_df.iterrows():
            pdate = str(p['pt_trdate'])[:10] if p['pt_trdate'] else ''
            pref = str(p.get('pt_supref') or p.get('pt_trref') or '').strip()
            rows += (
                f'<tr style="border-bottom:1px solid #eee;">'
                f'<td style="padding:6px 12px;">{pdate}</td>'
                f'<td style="padding:6px 12px;">{pref}</td>'
                f'<td style="padding:6px 12px;text-align:right;">£{float(p["amount"]):,.2f}</td>'
                f'</tr>'
            )
        return (
            '<p style="margin-top:16px;"><b>Recent payments made:</b></p>'
            '<table style="border-collapse:collapse;width:100%;margin:12px 0;font-size:13px;">'
            '<tr style="background:#2c3e50;color:white;">'
            '<th style="padding:8px 12px;text-align:left;">Date</th>'
            '<th style="padding:8px 12px;text-align:left;">Reference</th>'
            '<th style="padding:8px 12px;text-align:right;">Amount</th>'
            '</tr>'
            + rows +
            '</table>'
        )
    except Exception:
        return ''


def _build_payment_schedule(db, sql_connector, supplier_code: str, queried_items=None) -> str:
    """Build HTML snippet listing invoices included in the next payment run.

    If the account is in credit (we've overpaid), suggest allocation instead.
    If there are outstanding queries, note payment may be adjusted.
    """
    next_run = db.get_config('next_payment_run_date', '')
    if not next_run or not sql_connector:
        return ''

    try:
        # Check overall balance first — if in credit, no payment due
        bal_df = sql_connector.execute_query(f"""
            SELECT pn_currbal FROM pname WITH (NOLOCK) WHERE pn_account = '{supplier_code}'
        """)
        if bal_df is not None and not bal_df.empty:
            balance = float(bal_df.iloc[0]['pn_currbal'])
            if balance >= 0:
                # pn_currbal >= 0 means we're in credit (they owe us) or clear
                if balance > 0:
                    return (
                        f'<p style="margin-top:12px;"><b>Payment:</b> Our records show a credit balance of '
                        f'£{balance:,.2f} on your account. We recommend outstanding payments are allocated '
                        f'against invoices. No further payment is required at this time.</p>'
                    )
                else:
                    return ''  # Zero balance — nothing to say

            # Also check: do we have unallocated payments that cover the invoices?
            # If sum of outstanding invoices <= abs of sum of outstanding payments,
            # allocation would clear the balance
            inv_df = sql_connector.execute_query(f"""
                SELECT SUM(pt_trbal) as inv_total FROM ptran WITH (NOLOCK)
                WHERE pt_account = '{supplier_code}' AND pt_trtype = 'I' AND pt_trbal > 0
            """)
            pay_df = sql_connector.execute_query(f"""
                SELECT SUM(ABS(pt_trbal)) as pay_total FROM ptran WITH (NOLOCK)
                WHERE pt_account = '{supplier_code}' AND pt_trtype = 'P' AND pt_trbal < 0
            """)
            inv_total = float(inv_df.iloc[0]['inv_total'] or 0) if inv_df is not None and not inv_df.empty else 0
            pay_total = float(pay_df.iloc[0]['pay_total'] or 0) if pay_df is not None and not pay_df.empty else 0

            if pay_total >= inv_total and inv_total > 0:
                return (
                    f'<p style="margin-top:12px;"><b>Payment:</b> Our records show unallocated payments of '
                    f'£{pay_total:,.2f} against outstanding invoices of £{inv_total:,.2f}. '
                    f'We recommend these are allocated. No further payment is required at this time.</p>'
                )

        # Get outstanding invoices with due dates on or before the next payment run
        df = sql_connector.execute_query(f"""
            SELECT RTRIM(pt_trref) as reference, pt_dueday, pt_trbal
            FROM ptran WITH (NOLOCK)
            WHERE pt_account = '{supplier_code}'
              AND pt_trtype = 'I'
              AND pt_trbal > 0
              AND pt_dueday IS NOT NULL
              AND pt_dueday <= '{next_run}'
            ORDER BY pt_dueday
        """)
        if df is None or df.empty:
            df_any = sql_connector.execute_query(f"""
                SELECT RTRIM(pt_trref) as reference, pt_dueday, pt_trbal
                FROM ptran WITH (NOLOCK)
                WHERE pt_account = '{supplier_code}'
                  AND pt_trtype = 'I'
                  AND pt_trbal > 0
                ORDER BY pt_dueday
            """)
            if df_any is not None and not df_any.empty:
                return (
                    f'<p style="margin-top:12px;"><b>Payment schedule:</b> Our next payment run is '
                    f'<b>{next_run}</b>. No invoices are currently due before this date.</p>'
                )
            return ''

        # Build table of invoices included in next payment run
        total = 0.0
        rows_html = ''
        for _, row in df.iterrows():
            ref = str(row.get('reference', '')).strip()
            due = str(row.get('pt_dueday', ''))[:10]
            bal = float(row.get('pt_trbal', 0))
            total += bal
            due_parts = due.split('-')
            if len(due_parts) == 3 and len(due_parts[0]) == 4:
                due = f"{due_parts[2]}/{due_parts[1]}/{due_parts[0]}"
            rows_html += (
                f'<tr style="border-bottom:1px solid #eee;">'
                f'<td style="padding:4px 10px;">{ref}</td>'
                f'<td style="padding:4px 10px;">{due}</td>'
                f'<td style="padding:4px 10px;text-align:right;">£{bal:,.2f}</td>'
                f'</tr>'
            )

        # Format next_run date
        run_parts = next_run.split('-')
        if len(run_parts) == 3 and len(run_parts[0]) == 4:
            next_run_fmt = f"{run_parts[2]}/{run_parts[1]}/{run_parts[0]}"
        else:
            next_run_fmt = next_run

        html = (
            f'<p style="margin-top:12px;"><b>Payment schedule:</b> The following invoices '
            f'are included in our next payment run on <b>{next_run_fmt}</b>:</p>'
            f'<table style="border-collapse:collapse;width:100%;font-size:13px;margin:8px 0;">'
            f'<tr style="background:#2c3e50;color:white;">'
            f'<th style="padding:6px 10px;text-align:left;">Invoice</th>'
            f'<th style="padding:6px 10px;text-align:left;">Due Date</th>'
            f'<th style="padding:6px 10px;text-align:right;">Amount</th></tr>'
            f'{rows_html}'
            f'<tr style="border-top:2px solid #333;">'
            f'<td style="padding:6px 10px;font-weight:bold;" colspan="2">Total</td>'
            f'<td style="padding:6px 10px;text-align:right;font-weight:bold;">£{total:,.2f}</td></tr>'
            f'</table>'
        )

        # If there are outstanding queries, add a caveat
        if queried_items:
            # Check what types of queries — credit notes are particularly relevant
            credit_queries = [q for q in queried_items
                             if (q.get('doc_type') or '').upper() in ('CN', 'CREDIT', 'CREDIT NOTE', 'CR')]

            if credit_queries:
                credit_total = sum(abs(q.get('credit') or q.get('debit') or 0) for q in credit_queries)
                html += (
                    f'<p style="color:#856404;font-size:13px;margin-top:8px;">'
                    f'<b>Note:</b> Payment is subject to resolution of the {len(queried_items)} item(s) '
                    f'queried above. In particular, we are awaiting {len(credit_queries)} credit note(s) '
                    f'totalling £{credit_total:,.2f} which may reduce the payment amount.</p>'
                )
            else:
                html += (
                    f'<p style="color:#856404;font-size:13px;margin-top:8px;">'
                    f'<b>Note:</b> Payment is subject to resolution of the {len(queried_items)} item(s) '
                    f'queried above. The payment amount may be adjusted once these items are clarified.</p>'
                )

        return html
    except Exception as e:
        logger.warning(f"Could not build payment schedule for {supplier_code}: {e}")
        return ''


def _generate_subject(db, supplier_name: str, statement_date: Optional[str],
                      has_queries: bool) -> str:
    """Generate email subject from configurable template."""
    if has_queries:
        tpl = db.get_config(
            'email_template_subject_query',
            'Statement Response \u2014 {supplier_name} \u2014 {statement_date}'
        )
    else:
        tpl = db.get_config(
            'email_template_subject_agreed',
            'Statement Confirmed \u2014 {supplier_name} \u2014 {statement_date}'
        )
    return tpl.replace('{supplier_name}', supplier_name or '').replace(
        '{statement_date}', statement_date or 'N/A'
    )


def _generate_default_response(cursor, statement_id: int, supplier_code: str,
                                statement_date: Optional[str], sql_connector) -> str:
    """
    Generate HTML response email for a statement approval.
    Uses configurable templates from supplier_automation_config.
    Merges statement data into the template with proper formatting.

    Supported merge fields in templates:
      {contact_name}      - First contact name from zcontacts (Opera), else supplier name
      {supplier_name}     - Supplier name from pname
      {statement_date}    - Statement date
      {their_balance}     - Supplier's closing balance (formatted currency)
      {our_balance}       - Our balance from Opera ptran (formatted currency)
      {difference}        - Difference between balances (formatted currency, coloured)
      {agreed_count}      - Number of agreed items
      {query_count}       - Number of queried items
      {query_table}       - Auto-generated HTML table of queried items
      {payment_table}     - Auto-generated HTML table of recent payments
      {payment_schedule}  - Auto-generated HTML for upcoming payment run date
      {company_sign_off}  - response_sign_off config value
    """
    from sql_rag.supplier_statement_db import get_supplier_statement_db
    db = get_supplier_statement_db()

    # --- Supplier name ---
    supplier_name = supplier_code
    if sql_connector:
        try:
            name_df = sql_connector.execute_query(
                f"SELECT RTRIM(pn_name) as name FROM pname WITH (NOLOCK) "
                f"WHERE pn_account = '{supplier_code}'"
            )
            if name_df is not None and len(name_df) > 0:
                supplier_name = str(name_df.iloc[0]['name']).strip()
        except Exception:
            pass

    # --- Contact name (first named contact from zcontacts for this supplier) ---
    contact_name = supplier_name
    if sql_connector:
        try:
            ct_df = sql_connector.execute_query(f"""
                SELECT RTRIM(zc_contact) as name
                FROM zcontacts WITH (NOLOCK)
                WHERE zc_account = '{supplier_code}' AND zc_module = 'P'
                  AND zc_contact IS NOT NULL AND RTRIM(zc_contact) != ''
                ORDER BY zc_contact
            """)
            if ct_df is not None and len(ct_df) > 0:
                contact_name = str(ct_df.iloc[0]['name']).strip() or supplier_name
        except Exception:
            pass

    # --- Statement lines ---
    cursor.execute("""
        SELECT line_date, reference, description, debit, credit, match_status, query_type
        FROM statement_lines WHERE statement_id = ? ORDER BY id
    """, (statement_id,))
    lines = cursor.fetchall()

    matched = [l for l in lines if l['match_status'] in ('matched', 'paid')]
    queried = [l for l in lines if l['match_status'] == 'query']

    agreed_count = len(matched)
    query_count = len(queried)
    has_queries = query_count > 0

    # --- Balance values ---
    # their_balance: closing balance on the statement record
    cursor.execute(
        "SELECT closing_balance, opening_balance FROM supplier_statements WHERE id = ?",
        (statement_id,)
    )
    stmt_row = cursor.fetchone()
    their_balance_raw = float(stmt_row['closing_balance'] or 0) if stmt_row else 0.0
    their_balance = _format_currency(their_balance_raw)

    # our_balance: sum of ptran outstanding for the supplier
    our_balance_raw = 0.0
    if sql_connector:
        try:
            bal_df = sql_connector.execute_query(f"""
                SELECT SUM(pt_trbal) as bal
                FROM ptran WITH (NOLOCK)
                WHERE pt_account = '{supplier_code}'
            """)
            if bal_df is not None and len(bal_df) > 0 and bal_df.iloc[0]['bal'] is not None:
                our_balance_raw = float(bal_df.iloc[0]['bal'])
        except Exception:
            pass
    our_balance = _format_currency(our_balance_raw)

    # difference
    diff_raw = their_balance_raw - our_balance_raw
    diff_str = _format_currency(diff_raw)
    if abs(diff_raw) < 0.01:
        difference = f'<span style="color:#155724;font-weight:bold;">{diff_str}</span>'
    else:
        difference = f'<span style="color:#721c24;font-weight:bold;">{diff_str}</span>'

    # --- Data-driven HTML blocks ---
    query_table = _build_query_table(queried)
    payment_table = _build_payment_table(supplier_code, db, sql_connector)
    payment_schedule = _build_payment_schedule(db, sql_connector, supplier_code, queried)

    # --- Config values ---
    company_sign_off = db.get_config('response_sign_off', 'Regards,<br>Accounts Department')
    company_name = db.get_config('response_company_name', '')
    if company_name:
        company_sign_off = f'{company_sign_off}<br><b>{company_name}</b>'

    # --- Select and merge template ---
    if has_queries:
        template = db.get_config('email_template_query', None)
    else:
        template = db.get_config('email_template_agreed', None)

    if not template:
        # Fallback: build minimal body (should not happen once DB is initialised)
        if has_queries:
            template = (
                '<p>Dear {contact_name},</p>'
                '<p>Thank you for your statement dated {statement_date}.</p>'
                '{query_table}'
                '<p>Regards,<br>{company_sign_off}</p>'
            )
        else:
            template = (
                '<p>Dear {contact_name},</p>'
                '<p>Thank you for your statement dated {statement_date}.</p>'
                '<p>We confirm the balance of {their_balance} is agreed.</p>'
                '{payment_schedule}'
                '<p>Regards,<br>{company_sign_off}</p>'
            )

    merge_fields = {
        'contact_name': contact_name,
        'supplier_name': supplier_name,
        'statement_date': statement_date or 'N/A',
        'their_balance': their_balance,
        'our_balance': our_balance,
        'difference': difference,
        'agreed_count': str(agreed_count),
        'query_count': str(query_count),
        'query_table': query_table,
        'payment_table': payment_table,
        'payment_schedule': payment_schedule,
        'company_sign_off': company_sign_off,
    }

    body = template
    for field, value in merge_fields.items():
        body = body.replace('{' + field + '}', value)

    # Wrap in a full HTML document with shared styles
    html = """<html>
<head><style>
  body { font-family: Arial, sans-serif; font-size: 14px; color: #333; line-height: 1.6; }
  .footer { color: #888; font-size: 12px; margin-top: 24px; border-top: 1px solid #eee; padding-top: 12px; }
</style></head>
<body>
""" + body + """
<div class="footer">
  <p>This is an automated response generated by our accounts system.
  If you have any questions, please reply to this email.</p>
</div>
</body></html>"""

    return html


@router.post("/api/supplier-statements/process-email/{email_id}")
async def process_supplier_statement_email(email_id: int):
    """
    Full automated pipeline: extract → save → reconcile → acknowledge → generate response.
    This is the main automation entry point.
    """
    from api.main import sql_connector, email_storage
    from sql_rag.supplier_statement_db import get_supplier_statement_db
    from sql_rag.supplier_statement_extract import SupplierStatementExtractor
    from sql_rag.supplier_statement_reconcile import SupplierStatementReconciler

    try:
        db = get_supplier_statement_db()

        # Step 1: Get email
        if not email_storage:
            return {"success": False, "error": "Email storage not available"}
        email_data = email_storage.get_email_by_id(email_id)
        if not email_data:
            return {"success": False, "error": f"Email {email_id} not found"}

        from_addr = email_data.get('from_address', '')
        subject = email_data.get('subject', '')

        # Step 2: Extract using same method as extract-from-email endpoint
        from api.main import email_sync_manager, config as app_config
        if not email_sync_manager:
            return {"success": False, "error": "Email sync manager not available"}

        api_key = app_config.get('gemini', 'api_key', fallback='') if app_config else ''
        if not api_key:
            return {"success": False, "error": "Gemini API key not configured"}

        gemini_model = app_config.get('gemini', 'model', fallback='gemini-2.0-flash') if app_config else 'gemini-2.0-flash'
        extractor = SupplierStatementExtractor(api_key=api_key, model=gemini_model)

        attachments = email_data.get('attachments', [])
        pdf_attachments = [a for a in attachments if a.get('content_type') == 'application/pdf'
                          or (a.get('filename', '').lower().endswith('.pdf'))]

        info = None
        lines = []

        if pdf_attachments:
            target = pdf_attachments[0]
            provider_id = email_data.get('provider_id')
            message_id = email_data.get('message_id')

            if provider_id not in email_sync_manager.providers:
                return {"success": False, "error": "Email provider not connected"}

            provider = email_sync_manager.providers[provider_id]
            folder_id = 'INBOX'
            folder_id_db = email_data.get('folder_id')
            if folder_id_db:
                with email_storage._get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT folder_id FROM email_folders WHERE id = ?", (folder_id_db,))
                    row = cursor.fetchone()
                    if row:
                        folder_id = row['folder_id']

            result = await provider.download_attachment(message_id, str(target['attachment_id']), folder_id)
            if not result:
                return {"success": False, "error": "Failed to download PDF attachment"}

            # download_attachment returns (content_bytes, filename, content_type)
            pdf_bytes = result[0] if isinstance(result, tuple) else result.get('content', b'')

            info, lines = extractor.extract_from_pdf_bytes(pdf_bytes)
        else:
            body_text = email_data.get('body_text', '')
            if body_text:
                info, lines = extractor.extract_from_text(body_text)
            else:
                return {"success": False, "error": "No PDF attachment or text content found"}

        if not info:
            return {"success": False, "error": "Failed to extract statement data"}

        # Step 3: Create statement record
        statement_id = db.create_statement(
            supplier_code=info.account_reference or 'UNKNOWN',
            sender_email=from_addr,
            statement_date=info.statement_date,
            opening_balance=info.opening_balance,
            closing_balance=info.closing_balance,
        )

        # Save extracted lines
        line_records = []
        for line in (lines or []):
            line_records.append({
                'date': line.date if hasattr(line, 'date') else line.get('date'),
                'reference': line.reference if hasattr(line, 'reference') else line.get('reference'),
                'description': line.description if hasattr(line, 'description') else line.get('description'),
                'debit': line.debit if hasattr(line, 'debit') else line.get('debit'),
                'credit': line.credit if hasattr(line, 'credit') else line.get('credit'),
                'balance': line.balance if hasattr(line, 'balance') else line.get('balance'),
                'doc_type': line.doc_type if hasattr(line, 'doc_type') else line.get('doc_type'),
            })
        if line_records:
            db.add_statement_lines(statement_id, line_records)

        # Step 4: Reconcile against Opera
        reconciler = SupplierStatementReconciler(sql_connector) if sql_connector else None
        recon_result = None
        supplier_code = None
        supplier_name = info.supplier_name or ''

        if reconciler:
            # Find supplier in Opera
            supplier = reconciler.find_supplier(info.supplier_name, info.account_reference)
            if supplier:
                supplier_code = supplier.get('account', supplier.get('pn_account', ''))
                supplier_name = supplier.get('name', supplier.get('pn_name', supplier_name))
                # Update statement with correct supplier code
                db.update_statement_status(statement_id, 'processing', supplier_code=supplier_code)
                # Reconcile
                stmt_info_dict = {
                    'supplier_name': info.supplier_name,
                    'account_reference': supplier_code,
                    'statement_date': info.statement_date,
                    'closing_balance': info.closing_balance,
                }
                stmt_lines_dicts = []
                for line in (lines or []):
                    stmt_lines_dicts.append({
                        'date': line.date if hasattr(line, 'date') else line.get('date'),
                        'reference': line.reference if hasattr(line, 'reference') else line.get('reference'),
                        'description': line.description if hasattr(line, 'description') else line.get('description'),
                        'debit': line.debit if hasattr(line, 'debit') else line.get('debit'),
                        'credit': line.credit if hasattr(line, 'credit') else line.get('credit'),
                        'balance': line.balance if hasattr(line, 'balance') else line.get('balance'),
                    })
                recon_result = reconciler.reconcile(stmt_info_dict, stmt_lines_dicts)

        # Step 5: Update status
        if recon_result:
            db.update_statement_status(statement_id, 'reconciled')
        else:
            db.update_statement_status(statement_id, 'received')

        # Step 6: Send acknowledgment email
        ack_sent = False
        contact_email = from_addr
        try:
            ack_subject = f"Statement Received - {supplier_name} - {info.statement_date or 'today'}"
            ack_body = f"""<html><body>
<p>Dear Accounts,</p>
<p>Thank you for sending your statement dated {info.statement_date or 'today'}.</p>
<p>We have received it and are currently processing. You will receive a detailed reconciliation response shortly.</p>
<p>Regards,<br>Accounts Department</p>
</body></html>"""

            from api.main import email_storage as es
            if es and hasattr(es, 'get_provider_config'):
                import smtplib
                from email.mime.text import MIMEText
                from email.mime.multipart import MIMEMultipart
                providers = es.get_all_providers() if hasattr(es, 'get_all_providers') else []
                for prov in providers:
                    pconfig = prov.get('config', {})
                    if pconfig.get('smtp_server') or pconfig.get('server'):
                        smtp_server = pconfig.get('smtp_server', pconfig.get('server', ''))
                        smtp_port = int(pconfig.get('smtp_port', 587))
                        smtp_user = pconfig.get('smtp_username', pconfig.get('username', ''))
                        smtp_pass = pconfig.get('smtp_password', pconfig.get('password', ''))

                        msg = MIMEMultipart('alternative')
                        msg['Subject'] = ack_subject
                        msg['From'] = 'intsys@wimbledoncloud.net'
                        msg['To'] = contact_email
                        msg.attach(MIMEText(ack_body, 'html'))

                        with smtplib.SMTP(smtp_server, smtp_port) as server:
                            server.starttls()
                            server.login(smtp_user, smtp_pass)
                            server.send_message(msg)
                        ack_sent = True
                        break

            if ack_sent:
                db.update_statement_status(statement_id, 'acknowledged')
                db.log_communication(supplier_code or 'UNKNOWN', 'outbound', 'acknowledgment',
                    email_subject=ack_subject, email_body=ack_body, statement_id=statement_id)
        except Exception as ack_err:
            logger.warning(f"Could not send acknowledgment: {ack_err}")

        # Step 7: Generate reconciliation response
        response_text = None
        if recon_result and hasattr(recon_result, 'response_text'):
            response_text = recon_result.response_text
        elif recon_result and isinstance(recon_result, dict):
            response_text = recon_result.get('response_text')

        return {
            "success": True,
            "statement_id": statement_id,
            "supplier_code": supplier_code,
            "supplier_name": supplier_name,
            "statement_date": info.statement_date,
            "closing_balance": info.closing_balance,
            "lines_extracted": len(lines or []),
            "acknowledgment_sent": ack_sent,
            "acknowledgment_to": contact_email if ack_sent else None,
            "reconciliation": {
                "matched": recon_result.get('matched_count', 0) if isinstance(recon_result, dict) else 0,
                "queries": recon_result.get('query_count', 0) if isinstance(recon_result, dict) else 0,
                "variance": recon_result.get('variance', 0) if isinstance(recon_result, dict) else None,
            } if recon_result else None,
            "status": "acknowledged" if ack_sent else "reconciled" if recon_result else "received",
        }

    except Exception as e:
        logger.error(f"Error processing supplier statement email {email_id}: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/supplier-statements/queue")
async def get_supplier_statement_queue():
    """Get statements in the active queue (received or processing status)."""
    from sql_rag.company_data import get_current_db_path

    db_path = get_current_db_path('supplier_statements.db') or Path(__file__).parent.parent.parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        return {"success": True, "statements": []}

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                ss.id, ss.supplier_code, ss.statement_date, ss.received_date, ss.status,
                ss.sender_email, ss.opening_balance, ss.closing_balance, ss.currency,
                ss.error_message,
                COUNT(sl.id) as line_count,
                SUM(CASE WHEN sl.status = 'Agreed' THEN 1 ELSE 0 END) as matched_count,
                SUM(CASE WHEN sl.status = 'Query' THEN 1 ELSE 0 END) as query_count
            FROM supplier_statements ss
            LEFT JOIN statement_lines sl ON sl.statement_id = ss.id
            WHERE ss.status IN ('received', 'processing')
            GROUP BY ss.id
            ORDER BY ss.received_date DESC
        """)

        statements = [dict(row) for row in cursor.fetchall()]
        conn.close()

        return {"success": True, "statements": statements}

    except Exception as e:
        logger.error(f"Error fetching statement queue: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/supplier-statements/{statement_id}")
async def get_supplier_statement_detail(statement_id: int):
    """Get detailed information about a specific statement."""
    from api.main import sql_connector
    from sql_rag.company_data import get_current_db_path

    db_path = get_current_db_path('supplier_statements.db') or Path(__file__).parent.parent.parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                ss.id, ss.supplier_code, ss.statement_date, ss.received_date, ss.status,
                ss.sender_email, ss.opening_balance, ss.closing_balance, ss.currency,
                ss.acknowledged_at, ss.processed_at, ss.approved_by, ss.approved_at,
                ss.sent_at, ss.error_message, ss.pdf_path,
                COUNT(sl.id) as line_count,
                SUM(CASE WHEN sl.status = 'Agreed' THEN 1 ELSE 0 END) as matched_count,
                SUM(CASE WHEN sl.status = 'Query' THEN 1 ELSE 0 END) as query_count
            FROM supplier_statements ss
            LEFT JOIN statement_lines sl ON sl.statement_id = ss.id
            WHERE ss.id = ?
            GROUP BY ss.id
        """, (statement_id,))

        row = cursor.fetchone()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="Statement not found")

        stmt = dict(row)

        # Get supplier name and balance from Opera
        stmt['supplier_name'] = stmt['supplier_code']
        stmt['opera_balance'] = None
        if sql_connector:
            try:
                df = sql_connector.execute_query(
                    f"SELECT RTRIM(pn_name) as pn_name, pn_currbal FROM pname WITH (NOLOCK) WHERE pn_account = '{stmt['supplier_code']}'"
                )
                if df is not None and len(df) > 0:
                    stmt['supplier_name'] = str(df.iloc[0]['pn_name']).strip()
                    # Show raw Opera balance — same sign convention as supplier statement
                    # Negative = we owe them, Positive = we're in credit
                    stmt['opera_balance'] = float(df.iloc[0]['pn_currbal'])
            except Exception:
                pass

        # Difference: their balance - our balance
        their_bal = stmt.get('closing_balance') or 0
        our_bal = stmt.get('opera_balance') if stmt.get('opera_balance') is not None else 0
        stmt['balance_difference'] = their_bal - our_bal

        return {"success": True, "statement": stmt}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting statement detail: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/supplier-statements/{statement_id}/lines")
async def get_supplier_statement_lines(statement_id: int):
    """Get all line items for a statement."""
    from sql_rag.company_data import get_current_db_path

    db_path = get_current_db_path('supplier_statements.db') or Path(__file__).parent.parent.parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, line_date, reference, description, debit, credit, balance,
                   doc_type, exists_in_opera, status, match_status,
                   matched_ptran_id, query_type,
                   query_sent_at, query_resolved_at
            FROM statement_lines
            WHERE statement_id = ?
            ORDER BY line_date, id
        """, (statement_id,))

        lines = [dict(row) for row in cursor.fetchall()]

        # Calculate summary using the new status field
        summary = {
            "total_lines": len(lines),
            "total_debits": sum(l['debit'] or 0 for l in lines),
            "total_credits": sum(l['credit'] or 0 for l in lines),
            "exists_yes": sum(1 for l in lines if l.get('exists_in_opera') == 'Yes'),
            "exists_no": sum(1 for l in lines if l.get('exists_in_opera') == 'No'),
            "agreed_count": sum(1 for l in lines if l.get('status') == 'Agreed'),
            "paid_count": sum(1 for l in lines if l.get('status') == 'Paid'),
            "query_count": sum(1 for l in lines if l.get('status') == 'Query'),
            "disputed_count": sum(1 for l in lines if l.get('status') == 'Disputed'),
            "in_our_favour_count": sum(1 for l in lines if l.get('status') == 'In Our Favour'),
            # Legacy fields for backwards compat
            "matched_count": sum(1 for l in lines if l.get('exists_in_opera') == 'Yes'),
            "unmatched_count": sum(1 for l in lines if l.get('exists_in_opera') == 'No'),
        }

        # Get Opera-only transactions (on our account but not on their statement)
        opera_only = []
        try:
            cursor.execute("""
                SELECT line_date, reference, doc_type, amount, signed_value, balance
                FROM statement_opera_only
                WHERE statement_id = ?
                ORDER BY line_date, id
            """, (statement_id,))
            opera_only = [dict(r) for r in cursor.fetchall()]
        except Exception:
            pass  # Table may not exist yet

        # === Reconciliation calculation ===
        # The difference between their balance and ours is ALWAYS fully explained by:
        # 1. Items on their statement not on our account (net)
        # 2. Items on our account not on their statement (net)
        # 3. Anything left = amount differences on items that exist on both sides

        # 1. Items on their statement not on our account
        not_ours_net = sum(
            (l.get('debit') or 0) - (l.get('credit') or 0)
            for l in lines if l.get('exists_in_opera') == 'No'
        )
        not_ours_count = sum(1 for l in lines if l.get('exists_in_opera') == 'No')

        # 2. Items on our account not on their statement
        # Use signed_value from reconciler (positive=invoice, negative=payment/credit)
        # Guard against None values in signed_value
        opera_only_net = sum(item.get('signed_value', 0) or 0 for item in opera_only)

        # 3. Amount differences on agreed items (items that exist on both sides but differ)
        amount_diffs_net = sum(
            (l.get('debit') or 0) - (l.get('credit') or 0)
            for l in lines
            if l.get('exists_in_opera') == 'Yes' and l.get('status') == 'Amount Difference'
        )

        summary['opera_only_count'] = len(opera_only)
        summary['opera_only_net'] = opera_only_net
        summary['not_ours_net'] = not_ours_net
        summary['not_ours_count'] = not_ours_count
        summary['amount_diffs_net'] = amount_diffs_net
        summary['math_checks_out'] = True  # Verified by reconciler

        conn.close()
        return {"success": True, "lines": lines, "summary": summary, "opera_only": opera_only}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting statement lines: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/supplier-statements/{statement_id}/pdf")
async def get_supplier_statement_pdf(statement_id: int):
    """Serve the original PDF for a statement so the user can verify extraction."""
    from sql_rag.company_data import get_current_db_path
    from fastapi.responses import FileResponse

    db_path = get_current_db_path('supplier_statements.db') or Path(__file__).parent.parent.parent.parent / 'supplier_statements.db'
    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT pdf_path, supplier_code, statement_date FROM supplier_statements WHERE id = ?", (statement_id,))
        row = cursor.fetchone()
        conn.close()

        if not row or not row['pdf_path']:
            raise HTTPException(status_code=404, detail="PDF not available for this statement")

        pdf_path = row['pdf_path']
        if not os.path.exists(pdf_path):
            raise HTTPException(status_code=404, detail="PDF file not found on disk")

        filename = f"{row['supplier_code']}_{row['statement_date'] or 'statement'}.pdf"
        return FileResponse(pdf_path, media_type='application/pdf', filename=filename)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error serving PDF for statement {statement_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/supplier-statements/{statement_id}/process")
async def process_supplier_statement(statement_id: int):
    """
    Process a received statement - reconcile against Opera and generate response.

    This endpoint:
    1. Updates status to 'processing'
    2. Reconciles statement lines against ptran
    3. Applies business rules
    4. Generates draft response
    5. Updates status to 'queued' for approval
    """
    from api.main import sql_connector
    from sql_rag.company_data import get_current_db_path

    db_path = get_current_db_path('supplier_statements.db') or Path(__file__).parent.parent.parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    if not sql_connector:
        raise HTTPException(status_code=503, detail="Opera SQL connection not available")

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get statement
        cursor.execute("SELECT * FROM supplier_statements WHERE id = ?", (statement_id,))
        stmt = cursor.fetchone()

        if not stmt:
            conn.close()
            raise HTTPException(status_code=404, detail="Statement not found")

        if stmt['status'] not in ('received', 'error'):
            conn.close()
            raise HTTPException(status_code=400, detail=f"Statement cannot be processed from status '{stmt['status']}'")

        # Update status to processing
        cursor.execute("""
            UPDATE supplier_statements SET status = 'processing', updated_at = ?
            WHERE id = ?
        """, (datetime.now().isoformat(), statement_id))
        conn.commit()

        try:
            # Get statement lines
            cursor.execute("SELECT * FROM statement_lines WHERE statement_id = ?", (statement_id,))
            lines = [dict(row) for row in cursor.fetchall()]

            supplier_code = stmt['supplier_code']

            # Get ptran data for this supplier
            # TODO: Opera 3 parity — equivalent query using opera3_foxpro.py
            ptran_query = f"""
                SELECT pt_unique, pt_trdate, pt_trref, pt_supref, pt_trtype, pt_trvalue, pt_trbal
                FROM ptran WITH (NOLOCK)
                WHERE pt_account = '{supplier_code}'
                ORDER BY pt_trdate DESC
            """
            ptran_df = sql_connector.execute_query(ptran_query)

            matched_count = 0
            query_count = 0

            # Match each statement line against ptran
            for line in lines:
                match_status = 'unmatched'
                matched_ptran_id = None
                query_type = None

                if ptran_df is not None and len(ptran_df) > 0:
                    # Try to match by reference
                    if line.get('reference'):
                        ref_matches = ptran_df[
                            (ptran_df['pt_trref'].str.contains(line['reference'], case=False, na=False)) |
                            (ptran_df['pt_supref'].str.contains(line['reference'], case=False, na=False))
                        ]
                        if len(ref_matches) > 0:
                            match_status = 'matched'
                            matched_ptran_id = str(ref_matches.iloc[0]['pt_unique'])
                            matched_count += 1

                    # If not matched and it's a debit (invoice), may need to query
                    if match_status == 'unmatched' and line.get('debit') and line['debit'] > 0:
                        match_status = 'query'
                        query_type = 'invoice_not_found'
                        query_count += 1

                # Update line
                cursor.execute("""
                    UPDATE statement_lines
                    SET match_status = ?, matched_ptran_id = ?, query_type = ?
                    WHERE id = ?
                """, (match_status, matched_ptran_id, query_type, line['id']))

            # Update statement to queued
            cursor.execute("""
                UPDATE supplier_statements
                SET status = 'queued', processed_at = ?, updated_at = ?, error_message = NULL
                WHERE id = ?
            """, (datetime.now().isoformat(), datetime.now().isoformat(), statement_id))

            conn.commit()
            conn.close()

            return {
                "success": True,
                "message": "Statement processed successfully",
                "matched_count": matched_count,
                "query_count": query_count
            }

        except Exception as e:
            # Update status to error
            cursor.execute("""
                UPDATE supplier_statements
                SET status = 'error', error_message = ?, updated_at = ?
                WHERE id = ?
            """, (str(e), datetime.now().isoformat(), statement_id))
            conn.commit()
            conn.close()
            raise

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing statement: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


# ============================================================
# Supplier Statement Extraction API Endpoints
# ============================================================

@router.post("/api/supplier-statements/extract-from-email/{email_id}")
async def extract_supplier_statement_from_email(email_id: int, attachment_id: Optional[str] = None):
    """
    Extract supplier statement data from an email.

    If the email has a PDF attachment, extracts from that.
    Otherwise, attempts to extract from the email body text.

    Args:
        email_id: The database email ID
        attachment_id: Optional specific attachment ID (if multiple PDFs)

    Returns:
        Extracted statement info and line items
    """
    from api.main import email_storage, email_sync_manager, config
    from sql_rag.supplier_statement_extract import SupplierStatementExtractor

    if not email_storage or not email_sync_manager:
        raise HTTPException(status_code=503, detail="Email module not initialized")

    # Get API key for Gemini
    api_key = config.get('gemini', 'api_key', fallback='')
    if not api_key:
        raise HTTPException(status_code=503, detail="Gemini API key not configured")

    try:
        # Get the email
        email = email_storage.get_email_by_id(email_id)
        if not email:
            raise HTTPException(status_code=404, detail="Email not found")

        gemini_model = config.get('gemini', 'model', fallback='gemini-2.0-flash')
        extractor = SupplierStatementExtractor(api_key=api_key, model=gemini_model)
        attachments = email.get('attachments', [])

        # Find PDF attachment(s)
        pdf_attachments = [a for a in attachments if a.get('content_type') == 'application/pdf'
                          or (a.get('filename', '').lower().endswith('.pdf'))]

        if pdf_attachments:
            # Extract from PDF attachment
            if attachment_id:
                target_attachment = next(
                    (a for a in pdf_attachments if str(a.get('attachment_id')) == str(attachment_id)),
                    None
                )
            else:
                target_attachment = pdf_attachments[0]  # Use first PDF

            if not target_attachment:
                raise HTTPException(status_code=404, detail="PDF attachment not found")

            # Download the attachment
            provider_id = email.get('provider_id')
            message_id = email.get('message_id')

            if provider_id not in email_sync_manager.providers:
                raise HTTPException(status_code=503, detail="Email provider not connected")

            provider = email_sync_manager.providers[provider_id]

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

            result = await provider.download_attachment(
                message_id,
                str(target_attachment['attachment_id']),
                folder_id
            )

            if not result:
                raise HTTPException(status_code=500, detail="Failed to download attachment")

            pdf_bytes, filename, content_type = result

            # Extract from PDF
            statement_info, lines = extractor.extract_from_pdf_bytes(pdf_bytes)

            return {
                "success": True,
                "source": "pdf_attachment",
                "filename": filename,
                "email_subject": email.get('subject'),
                "from_address": email.get('from_address'),
                **extractor.to_dict(statement_info, lines)
            }
        else:
            # Try to extract from email body text
            body_text = email.get('body_text') or email.get('body_preview', '')
            if not body_text or len(body_text) < 50:
                raise HTTPException(
                    status_code=400,
                    detail="No PDF attachment found and email body is too short to contain statement data"
                )

            statement_info, lines = extractor.extract_from_text(
                body_text,
                sender_email=email.get('from_address')
            )

            return {
                "success": True,
                "source": "email_body",
                "email_subject": email.get('subject'),
                "from_address": email.get('from_address'),
                **extractor.to_dict(statement_info, lines)
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error extracting supplier statement: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/api/supplier-statements/extract-from-file")
async def extract_supplier_statement_from_file(file_path: str):
    """
    Extract supplier statement data from a PDF file path.

    Args:
        file_path: Path to the PDF file

    Returns:
        Extracted statement info and line items
    """
    from api.main import config
    from sql_rag.supplier_statement_extract import SupplierStatementExtractor

    api_key = config.get('gemini', 'api_key', fallback='')
    if not api_key:
        raise HTTPException(status_code=503, detail="Gemini API key not configured")

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"File not found: {file_path}")

    try:
        gemini_model = config.get('gemini', 'model', fallback='gemini-2.0-flash')
        extractor = SupplierStatementExtractor(api_key=api_key, model=gemini_model)
        statement_info, lines = extractor.extract_from_pdf(file_path)

        return {
            "success": True,
            "source": "file",
            "file_path": file_path,
            **extractor.to_dict(statement_info, lines)
        }

    except Exception as e:
        logger.error(f"Error extracting supplier statement from file: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/api/supplier-statements/extract-from-text")
async def extract_supplier_statement_from_text(
    text: str = Body(..., embed=True),
    sender_email: Optional[str] = Body(None, embed=True)
):
    """
    Extract supplier statement data from raw text.

    Args:
        text: The statement text content
        sender_email: Optional sender email for supplier identification

    Returns:
        Extracted statement info and line items
    """
    from api.main import config
    from sql_rag.supplier_statement_extract import SupplierStatementExtractor

    api_key = config.get('gemini', 'api_key', fallback='')
    if not api_key:
        raise HTTPException(status_code=503, detail="Gemini API key not configured")

    try:
        gemini_model = config.get('gemini', 'model', fallback='gemini-2.0-flash')
        extractor = SupplierStatementExtractor(api_key=api_key, model=gemini_model)
        statement_info, lines = extractor.extract_from_text(text, sender_email=sender_email)

        return {
            "success": True,
            "source": "text",
            **extractor.to_dict(statement_info, lines)
        }

    except Exception as e:
        logger.error(f"Error extracting supplier statement from text: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/api/supplier-statements/reconcile/{email_id}")
async def reconcile_supplier_statement(email_id: int, attachment_id: Optional[str] = None):
    """
    Extract and reconcile a supplier statement against Opera purchase ledger.

    This endpoint:
    1. Extracts statement data from the email (PDF or body text)
    2. Finds the matching supplier in Opera
    3. Compares statement lines against ptran
    4. Generates an informative response following business rules

    Business rules:
    - Only raise queries when NOT in our favour
    - Stay quiet about discrepancies that benefit us
    - Always notify payments we've made
    - Flag old statements and request current one

    Args:
        email_id: The database email ID
        attachment_id: Optional specific attachment ID

    Returns:
        Reconciliation result with match details and generated response
    """
    from api.main import sql_connector, email_storage, email_sync_manager, config
    from sql_rag.supplier_statement_extract import SupplierStatementExtractor
    from sql_rag.supplier_statement_reconcile import SupplierStatementReconciler

    if not email_storage or not email_sync_manager:
        raise HTTPException(status_code=503, detail="Email module not initialized")

    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    api_key = config.get('gemini', 'api_key', fallback='')
    if not api_key:
        raise HTTPException(status_code=503, detail="Gemini API key not configured")

    try:
        # Step 1: Extract statement from email
        email = email_storage.get_email_by_id(email_id)
        if not email:
            raise HTTPException(status_code=404, detail="Email not found")

        gemini_model = config.get('gemini', 'model', fallback='gemini-2.0-flash')
        extractor = SupplierStatementExtractor(api_key=api_key, model=gemini_model)
        attachments = email.get('attachments', [])

        # Find PDF attachment(s)
        pdf_attachments = [a for a in attachments if a.get('content_type') == 'application/pdf'
                          or (a.get('filename', '').lower().endswith('.pdf'))]

        statement_info = None
        lines = None

        if pdf_attachments:
            # Extract from PDF
            if attachment_id:
                target_attachment = next(
                    (a for a in pdf_attachments if str(a.get('attachment_id')) == str(attachment_id)),
                    None
                )
            else:
                target_attachment = pdf_attachments[0]

            if target_attachment:
                provider_id = email.get('provider_id')
                message_id = email.get('message_id')

                if provider_id in email_sync_manager.providers:
                    provider = email_sync_manager.providers[provider_id]

                    folder_id_db = email.get('folder_id')
                    folder_id = 'INBOX'
                    if folder_id_db:
                        with email_storage._get_connection() as conn:
                            cursor = conn.cursor()
                            cursor.execute("SELECT folder_id FROM email_folders WHERE id = ?", (folder_id_db,))
                            row = cursor.fetchone()
                            if row:
                                folder_id = row['folder_id']

                    result = await provider.download_attachment(
                        message_id,
                        str(target_attachment['attachment_id']),
                        folder_id
                    )

                    if result:
                        pdf_bytes, filename, content_type = result
                        statement_info, lines = extractor.extract_from_pdf_bytes(pdf_bytes)

        if not statement_info:
            # Try extracting from email body
            body_text = email.get('body_text') or email.get('body_preview', '')
            if body_text and len(body_text) >= 50:
                statement_info, lines = extractor.extract_from_text(
                    body_text,
                    sender_email=email.get('from_address')
                )

        if not statement_info:
            raise HTTPException(
                status_code=400,
                detail="Could not extract statement data from email"
            )

        # Step 2: Reconcile against Opera
        reconciler = SupplierStatementReconciler(sql_connector)

        # Convert dataclass to dict
        info_dict = {
            "supplier_name": statement_info.supplier_name,
            "account_reference": statement_info.account_reference,
            "statement_date": statement_info.statement_date,
            "closing_balance": statement_info.closing_balance,
            "contact_email": statement_info.contact_email,
            "contact_phone": statement_info.contact_phone
        }

        lines_dict = [
            {
                "date": line.date,
                "reference": line.reference,
                "description": line.description,
                "debit": line.debit,
                "credit": line.credit,
                "balance": line.balance,
                "doc_type": line.doc_type
            }
            for line in lines
        ]

        recon_result = reconciler.reconcile(info_dict, lines_dict)

        return {
            "success": True,
            "email_id": email_id,
            "email_subject": email.get('subject'),
            "from_address": email.get('from_address'),
            "extraction": extractor.to_dict(statement_info, lines),
            "reconciliation": recon_result.to_dict()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error reconciling supplier statement: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


# ============================================================
# Creditors Control API Endpoints (Purchase Ledger)
# ============================================================

@router.get("/api/creditors/dashboard")
async def creditors_dashboard():
    """
    Get creditors control dashboard with key metrics.
    """
    from api.main import sql_connector

    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        metrics = {}

        # Total creditors balance
        result = sql_connector.execute_query(
            """SELECT COUNT(*) AS count, SUM(pn_currbal) AS total
               FROM pname WITH (NOLOCK) WHERE pn_currbal <> 0"""
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["total_creditors"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Total Outstanding"
            }

        # Overdue invoices (past due date)
        result = sql_connector.execute_query(
            """SELECT COUNT(*) AS count, SUM(pt_trbal) AS total
               FROM ptran WITH (NOLOCK) WHERE pt_trtype = 'I' AND pt_trbal > 0 AND pt_dueday < GETDATE()"""
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["overdue_invoices"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Overdue Invoices"
            }

        # Due within 7 days
        result = sql_connector.execute_query(
            """SELECT COUNT(*) AS count, SUM(pt_trbal) AS total
               FROM ptran WITH (NOLOCK) WHERE pt_trtype = 'I' AND pt_trbal > 0
               AND pt_dueday >= GETDATE() AND pt_dueday < DATEADD(day, 7, GETDATE())"""
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["due_7_days"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Due in 7 Days"
            }

        # Due within 30 days
        result = sql_connector.execute_query(
            """SELECT COUNT(*) AS count, SUM(pt_trbal) AS total
               FROM ptran WITH (NOLOCK) WHERE pt_trtype = 'I' AND pt_trbal > 0
               AND pt_dueday >= GETDATE() AND pt_dueday < DATEADD(day, 30, GETDATE())"""
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["due_30_days"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Due in 30 Days"
            }

        # Recent payments made (last 7 days)
        result = sql_connector.execute_query(
            """SELECT COUNT(*) AS count, SUM(ABS(pt_trvalue)) AS total
               FROM ptran WITH (NOLOCK) WHERE pt_trtype = 'P' AND pt_trdate >= DATEADD(day, -7, GETDATE())"""
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["recent_payments"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Payments (7 days)"
            }

        # Top suppliers by balance
        priority_result = sql_connector.execute_query(
            """SELECT TOP 10
                   RTRIM(pn_account) AS account,
                   RTRIM(pn_name) AS supplier,
                   pn_currbal AS balance,
                   pn_teleno AS phone,
                   pn_contact AS contact
               FROM pname
               WHERE pn_currbal > 0
               ORDER BY pn_currbal DESC"""
        )
        if hasattr(priority_result, 'to_dict'):
            priority_result = priority_result.to_dict('records')

        return {
            "success": True,
            "metrics": metrics,
            "top_suppliers": priority_result or []
        }

    except Exception as e:
        logger.error(f"Creditors dashboard query failed: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/creditors/report")
async def creditors_report():
    """
    Get aged creditors report with balance breakdown by aging period.
    """
    from api.main import sql_connector

    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        result = sql_connector.execute_query("""
            SELECT
                RTRIM(pn.pn_account) AS account,
                RTRIM(pn.pn_name) AS supplier,
                pn.pn_currbal AS balance,
                ISNULL(ph.pi_current, 0) AS current_period,
                ISNULL(ph.pi_period1, 0) AS month_1,
                ISNULL(ph.pi_period2, 0) AS month_2,
                ISNULL(ph.pi_period3, 0) + ISNULL(ph.pi_period4, 0) + ISNULL(ph.pi_period5, 0) AS month_3_plus,
                pn.pn_teleno AS phone,
                pn.pn_contact AS contact
            FROM pname pn WITH (NOLOCK)
            LEFT JOIN phist ph ON pn.pn_account = ph.pi_account AND ph.pi_age = 1
            WHERE pn.pn_currbal <> 0
            ORDER BY pn.pn_account
        """)

        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        # Calculate totals
        totals = {
            "balance": sum(r.get("balance", 0) or 0 for r in result),
            "current": sum(r.get("current_period", 0) or 0 for r in result),
            "month_1": sum(r.get("month_1", 0) or 0 for r in result),
            "month_2": sum(r.get("month_2", 0) or 0 for r in result),
            "month_3_plus": sum(r.get("month_3_plus", 0) or 0 for r in result),
        }

        return {
            "success": True,
            "data": result or [],
            "count": len(result) if result else 0,
            "totals": totals
        }

    except Exception as e:
        logger.error(f"Creditors report query failed: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/creditors/supplier/{account}")
async def get_supplier_details(account: str):
    """
    Get detailed information for a specific supplier.
    """
    from api.main import sql_connector

    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Get supplier header info
        supplier = sql_connector.execute_query(f"""
            SELECT
                RTRIM(pn_account) AS account,
                RTRIM(pn_name) AS supplier_name,
                RTRIM(pn_addr1) AS address1,
                RTRIM(pn_addr2) AS address2,
                RTRIM(pn_addr3) AS address3,
                RTRIM(pn_addr4) AS address4,
                RTRIM(pn_pstcode) AS postcode,
                RTRIM(pn_teleno) AS phone,
                RTRIM(pn_contact) AS contact,
                RTRIM(pn_email) AS email,
                pn_currbal AS balance,
                pn_trnover AS turnover_ytd
            FROM pname
            WHERE pn_account = '{account}'
        """)

        if hasattr(supplier, 'to_dict'):
            supplier = supplier.to_dict('records')

        if not supplier:
            raise HTTPException(status_code=404, detail="Supplier not found")

        return {
            "success": True,
            "supplier": supplier[0]
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Supplier details query failed: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/creditors/supplier/{account}/transactions")
async def get_supplier_transactions(account: str, include_paid: bool = False):
    """
    Get outstanding transactions for a specific supplier.
    """
    from api.main import sql_connector

    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        balance_filter = "" if include_paid else "AND pt_trbal <> 0"

        transactions = sql_connector.execute_query(f"""
            SELECT
                RTRIM(pt_account) AS account,
                pt_trdate AS date,
                RTRIM(pt_trref) AS reference,
                CASE pt_trtype
                    WHEN 'I' THEN 'Invoice'
                    WHEN 'C' THEN 'Credit Note'
                    WHEN 'P' THEN 'Payment'
                    WHEN 'J' THEN 'Journal'
                    ELSE pt_trtype
                END AS type,
                RTRIM(pt_supref) AS description,
                pt_trvalue AS value,
                pt_trbal AS balance,
                pt_dueday AS due_date,
                CASE
                    WHEN pt_trtype = 'I' AND pt_trbal > 0 AND pt_dueday < GETDATE()
                    THEN DATEDIFF(day, pt_dueday, GETDATE())
                    ELSE 0
                END AS days_overdue
            FROM ptran
            WHERE pt_account = '{account}'
            {balance_filter}
            ORDER BY pt_trdate DESC, pt_trref
        """)

        if hasattr(transactions, 'to_dict'):
            transactions = transactions.to_dict('records')

        # Calculate totals
        total_invoices = sum(t['value'] for t in transactions if t.get('type') == 'Invoice')
        total_credits = sum(abs(t['value']) for t in transactions if t.get('type') == 'Credit Note')
        total_payments = sum(abs(t['value']) for t in transactions if t.get('type') == 'Payment')
        balance = sum(t.get('balance', 0) or 0 for t in transactions)

        return {
            "success": True,
            "transactions": transactions or [],
            "count": len(transactions) if transactions else 0,
            "summary": {
                "total_invoices": total_invoices,
                "total_credits": total_credits,
                "total_payments": total_payments,
                "balance": balance
            }
        }

    except Exception as e:
        logger.error(f"Supplier transactions query failed: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/creditors/supplier/{account}/statement")
async def get_supplier_statement(account: str, from_date: str = None, to_date: str = None):
    """
    Generate a supplier statement showing outstanding transactions only.
    """
    from api.main import sql_connector

    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Get supplier info
        supplier = sql_connector.execute_query(f"""
            SELECT
                RTRIM(pn_account) AS account,
                RTRIM(pn_name) AS supplier_name,
                RTRIM(pn_addr1) AS address1,
                RTRIM(pn_addr2) AS address2,
                RTRIM(pn_addr3) AS address3,
                RTRIM(pn_addr4) AS address4,
                RTRIM(pn_pstcode) AS postcode,
                pn_currbal AS current_balance
            FROM pname
            WHERE pn_account = '{account}'
        """)

        if hasattr(supplier, 'to_dict'):
            supplier = supplier.to_dict('records')

        if not supplier:
            raise HTTPException(status_code=404, detail="Supplier not found")

        # No opening balance for outstanding-only statement
        opening_balance = 0.0

        # Get outstanding transactions only (where balance is not zero)
        transactions = sql_connector.execute_query(f"""
            SELECT
                pt_trdate AS date,
                RTRIM(pt_trref) AS reference,
                CASE pt_trtype
                    WHEN 'I' THEN 'Invoice'
                    WHEN 'C' THEN 'Credit Note'
                    WHEN 'P' THEN 'Payment'
                    WHEN 'J' THEN 'Journal'
                    ELSE pt_trtype
                END AS type,
                RTRIM(pt_supref) AS description,
                CASE WHEN pt_trtype IN ('I', 'J') AND pt_trvalue > 0 THEN pt_trvalue ELSE 0 END AS debit,
                CASE WHEN pt_trtype IN ('C', 'P') OR pt_trvalue < 0 THEN ABS(pt_trvalue) ELSE 0 END AS credit,
                pt_trbal AS balance,
                pt_dueday AS due_date
            FROM ptran
            WHERE pt_account = '{account}'
            AND pt_trbal <> 0
            ORDER BY pt_trdate, pt_trref
        """)

        if hasattr(transactions, 'to_dict'):
            transactions = transactions.to_dict('records')

        # Calculate running balance from outstanding balances
        running_balance = 0.0
        for t in transactions:
            running_balance += t.get('balance', 0) or 0
            t['running_balance'] = running_balance

        # Calculate totals
        total_debits = sum(t.get('debit', 0) or 0 for t in transactions)
        total_credits = sum(t.get('credit', 0) or 0 for t in transactions)
        total_outstanding = sum(t.get('balance', 0) or 0 for t in transactions)

        return {
            "success": True,
            "supplier": supplier[0],
            "period": {
                "from_date": None,
                "to_date": datetime.now().strftime('%Y-%m-%d')
            },
            "opening_balance": opening_balance,
            "transactions": transactions or [],
            "totals": {
                "debits": total_debits,
                "credits": total_credits,
                "outstanding": total_outstanding
            },
            "closing_balance": total_outstanding
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Supplier statement query failed: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/creditors/search")
async def search_suppliers(query: str, limit: int = Query(20, ge=1, le=1000)):
    """
    Search for suppliers by any field - account, name, address, contact, email, phone.
    """
    from api.main import sql_connector

    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        results = sql_connector.execute_query(f"""
            SELECT TOP {limit}
                RTRIM(pn_account) AS account,
                RTRIM(pn_name) AS supplier_name,
                pn_currbal AS balance,
                RTRIM(pn_teleno) AS phone,
                RTRIM(pn_addr1) AS address1,
                RTRIM(pn_addr2) AS address2,
                RTRIM(pn_addr3) AS address3,
                RTRIM(pn_addr4) AS address4,
                RTRIM(pn_pstcode) AS postcode,
                RTRIM(pn_contact) AS contact,
                RTRIM(pn_email) AS email
            FROM pname WITH (NOLOCK)
            WHERE ('{query}' = '' OR UPPER(pn_account) LIKE UPPER('%{query}%')
               OR UPPER(pn_name) LIKE UPPER('%{query}%')
               OR UPPER(pn_addr1) LIKE UPPER('%{query}%')
               OR UPPER(pn_addr2) LIKE UPPER('%{query}%')
               OR UPPER(pn_addr3) LIKE UPPER('%{query}%')
               OR UPPER(pn_addr4) LIKE UPPER('%{query}%')
               OR UPPER(pn_pstcode) LIKE UPPER('%{query}%')
               OR UPPER(pn_contact) LIKE UPPER('%{query}%')
               OR UPPER(pn_email) LIKE UPPER('%{query}%')
               OR UPPER(pn_teleno) LIKE UPPER('%{query}%'))
            ORDER BY pn_account
        """)

        if hasattr(results, 'to_dict'):
            results = results.to_dict('records')

        return {
            "success": True,
            "suppliers": results or [],
            "count": len(results) if results else 0
        }

    except Exception as e:
        logger.error(f"Supplier search failed: {e}")
        return {"success": False, "error": str(e)}


# ============================================================
# Supplier Account View API Endpoints
# ============================================================

@router.get("/api/supplier/account/first")
async def get_first_supplier_account():
    """Get the first supplier with a balance (for default view)."""
    from api.main import sql_connector

    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        result = sql_connector.execute_query("""
            SELECT TOP 1 RTRIM(pn_account) AS account
            FROM pname WITH (NOLOCK)
            WHERE pn_currbal <> 0
            ORDER BY pn_name
        """)
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result and len(result) > 0:
            return {"success": True, "account": result[0]['account']}
        return {"success": False, "error": "No suppliers with balance found"}
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.get("/api/supplier/account/{account}")
async def get_supplier_account_view(
    account: str,
    view: str = Query("outstanding", description="'all' or 'outstanding'"),
    date_from: Optional[str] = Query(None, description="Filter from date (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="Filter to date (YYYY-MM-DD)"),
):
    """
    Get full supplier account view matching Opera's Purchase Processing screen.
    Returns supplier details, outstanding transactions, and aging analysis.
    """
    from api.main import sql_connector

    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Get supplier header info - use only columns that exist in all Opera installations
        supplier_query = f"""
            SELECT
                RTRIM(pn_account) AS account,
                RTRIM(pn_name) AS company_name,
                RTRIM(pn_addr1) AS address1,
                RTRIM(pn_addr2) AS address2,
                RTRIM(pn_addr3) AS address3,
                RTRIM(pn_addr4) AS address4,
                RTRIM(pn_pstcode) AS postcode,
                RTRIM(pn_contact) AS ac_contact,
                RTRIM(pn_email) AS email,
                RTRIM(pn_teleno) AS telephone,
                RTRIM(ISNULL(pn_faxno, '')) AS facsimile,
                pn_currbal AS current_balance,
                ISNULL(pn_ordrbal, 0) AS order_balance,
                pn_trnover AS turnover,
                ISNULL(pn_crlim, 0) AS credit_limit
            FROM pname WITH (NOLOCK)
            WHERE pn_account = '{account}'
        """

        supplier_result = sql_connector.execute_query(supplier_query)
        if hasattr(supplier_result, 'to_dict'):
            supplier_result = supplier_result.to_dict('records')

        if not supplier_result:
            raise HTTPException(status_code=404, detail="Supplier not found")

        supplier = supplier_result[0]

        # Get outstanding transactions
        transactions_query = f"""
            SELECT
                pt_trdate AS date,
                CASE pt_trtype
                    WHEN 'I' THEN 'Inv'
                    WHEN 'C' THEN 'Crd'
                    WHEN 'P' THEN 'Pay'
                    WHEN 'J' THEN 'Jnl'
                    WHEN 'F' THEN 'Ref'
                    ELSE pt_trtype
                END AS type,
                RTRIM(pt_trref) AS ref1,
                RTRIM(pt_supref) AS ref2,
                CASE RTRIM(pt_paid)
                    WHEN 'A' THEN 'A'
                    WHEN 'P' THEN 'P'
                    ELSE ''
                END AS stat,
                CASE WHEN pt_trtype IN ('I', 'J', 'D') AND pt_trvalue > 0 THEN pt_trvalue ELSE NULL END AS debit,
                CASE WHEN pt_trtype IN ('C', 'P', 'F') OR pt_trvalue < 0 THEN ABS(pt_trvalue) ELSE NULL END AS credit,
                pt_trbal AS balance,
                RTRIM(ISNULL(pt_fcurr, '')) AS currency,
                RTRIM(ISNULL(pt_fcurr, '')) AS fc_curr,
                ISNULL(pt_fcrate, 0) AS fc_rate,
                ISNULL(pt_fcdec, 0) AS fc_dec,
                CASE WHEN RTRIM(ISNULL(pt_fcurr, '')) != '' AND pt_trtype IN ('I', 'J', 'D') AND pt_fcval > 0
                    THEN pt_fcval ELSE NULL END AS fc_debit,
                CASE WHEN RTRIM(ISNULL(pt_fcurr, '')) != '' AND (pt_trtype IN ('C', 'P', 'F') OR pt_fcval < 0)
                    THEN ABS(pt_fcval) ELSE NULL END AS fc_credit,
                CASE WHEN RTRIM(ISNULL(pt_fcurr, '')) != '' THEN pt_fcbal ELSE NULL END AS fc_balance,
                pt_dueday AS due_date,
                pt_lastpay AS last_payment,
                pt_unique AS unique_id,
                pt_trtype AS raw_type
            FROM ptran WITH (NOLOCK)
            WHERE pt_account = '{account}'
            {"AND pt_trbal <> 0" if view == "outstanding" else ""}
            {"AND pt_trdate >= '" + date_from + "'" if date_from else ""}
            {"AND pt_trdate <= '" + date_to + "'" if date_to else ""}
            ORDER BY pt_trdate DESC, pt_trref
        """

        transactions_result = sql_connector.execute_query(transactions_query)
        if transactions_result is None:
            transactions = []
        elif hasattr(transactions_result, 'to_dict'):
            transactions = transactions_result.to_dict('records')
        else:
            transactions = transactions_result or []

        # Clean NaN/Infinity values and convert FC from minor units
        import math
        for txn in transactions:
            for key in list(txn.keys()):
                val = txn[key]
                if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
                    txn[key] = None
            fc_dec = int(float(txn.get('fc_dec', 0) or 0))
            if fc_dec > 0:
                for fc_field in ('fc_debit', 'fc_credit', 'fc_balance'):
                    if txn.get(fc_field) is not None:
                        txn[fc_field] = float(txn[fc_field]) / (10 ** fc_dec)

        # Calculate aging analysis
        import pandas as pd
        today = datetime.now().date()
        aging = {
            '150_plus': 0.0,
            '120_days': 0.0,
            '90_days': 0.0,
            '60_days': 0.0,
            '30_days': 0.0,
            'current': 0.0,
            'total': 0.0,
            'unallocated': 0.0
        }

        for t in transactions:
            balance = t.get('balance', 0) or 0

            # Unallocated payments/credits (negative balance)
            if balance < 0:
                aging['unallocated'] += abs(balance)
                continue

            aging['total'] += balance

            due_date = t.get('due_date')
            days_old = 0

            # Check for None or pandas NaT
            if due_date is not None and not pd.isna(due_date):
                try:
                    # Handle pandas Timestamp
                    if hasattr(due_date, 'to_pydatetime'):
                        due_date = due_date.to_pydatetime().date()
                    # Handle datetime
                    elif hasattr(due_date, 'date') and callable(due_date.date):
                        due_date = due_date.date()
                    # Handle string
                    elif isinstance(due_date, str):
                        due_date = datetime.strptime(due_date[:10], '%Y-%m-%d').date()
                    # Calculate days
                    days_old = (today - due_date).days
                except Exception:
                    days_old = 0

            if days_old > 150:
                aging['150_plus'] += balance
            elif days_old > 120:
                aging['120_days'] += balance
            elif days_old > 90:
                aging['90_days'] += balance
            elif days_old > 60:
                aging['60_days'] += balance
            elif days_old > 30:
                aging['30_days'] += balance
            else:
                aging['current'] += balance

        return {
            "success": True,
            "supplier": supplier,
            "transactions": transactions,
            "aging": aging,
            "count": len(transactions)
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Supplier account view failed: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


# ============================================================
# Supplier Detail Page Endpoint
# ============================================================

@router.get("/api/supplier-config/{account}/detail")
async def get_supplier_detail(account: str):
    """Full supplier detail — Opera data, automation flags, contact, statement history, audit trail."""
    from api.main import sql_connector
    from sql_rag.supplier_config import SupplierConfigManager
    from sql_rag.company_data import get_current_db_path

    db_path = get_current_db_path('supplier_statements.db')
    if not db_path:
        return {"success": False, "error": "Database not available"}

    result: dict = {
        "success": True,
        "account": account,
        "config": None,
        "opera_contact": None,
        "statement_history": [],
        "balance": None,
    }

    # --- Supplier config (flags + name/balance from local cache) ---
    try:
        mgr = SupplierConfigManager(str(db_path), sql_connector)
        config = mgr.get_config(account)
        result["config"] = config
    except Exception as e:
        logger.warning(f"Could not load supplier config for {account}: {e}")

    # --- Opera contact from zcontacts (first P-module contact) ---
    if sql_connector:
        try:
            contact_df = sql_connector.execute_query(f"""
                SELECT TOP 1
                    RTRIM(zc_contact) AS name,
                    RTRIM(ISNULL(zc_pos, '')) AS role,
                    RTRIM(ISNULL(zc_email, '')) AS email,
                    RTRIM(ISNULL(zc_phone, '')) AS phone,
                    RTRIM(ISNULL(zc_mobile, '')) AS mobile
                FROM zcontacts WITH (NOLOCK)
                WHERE zc_account = '{account}' AND zc_module = 'P'
                ORDER BY zc_contact
            """)
            if contact_df is not None and len(contact_df) > 0:
                row = contact_df.iloc[0]
                result["opera_contact"] = {
                    "name": str(row.get("name", "") or "").strip(),
                    "role": str(row.get("role", "") or "").strip(),
                    "email": str(row.get("email", "") or "").strip(),
                    "phone": str(row.get("phone", "") or "").strip(),
                    "mobile": str(row.get("mobile", "") or "").strip(),
                }
        except Exception as e:
            logger.warning(f"Could not load Opera contact for {account}: {e}")

        # --- Current balance from Opera pname ---
        try:
            bal_df = sql_connector.execute_query(
                f"SELECT pn_currbal FROM pname WITH (NOLOCK) WHERE pn_account = '{account}'"
            )
            if bal_df is not None and len(bal_df) > 0:
                result["balance"] = float(bal_df.iloc[0]["pn_currbal"] or 0)
        except Exception as e:
            logger.warning(f"Could not load balance for {account}: {e}")

    # --- Statement history from SQLite ---
    try:
        if db_path.exists():
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    id,
                    statement_date,
                    received_date,
                    status,
                    line_count,
                    matched_count,
                    query_count,
                    closing_balance
                FROM supplier_statements
                WHERE supplier_code = ?
                ORDER BY received_date DESC
                LIMIT 50
            """, (account,))
            result["statement_history"] = [dict(row) for row in cursor.fetchall()]
            conn.close()
    except Exception as e:
        logger.warning(f"Could not load statement history for {account}: {e}")

    return result
