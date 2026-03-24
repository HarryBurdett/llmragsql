"""
Supplier API routes.

Extracted from api/main.py — provides endpoints for supplier statement automation,
supplier queries, communications, security, settings, supplier directory,
creditors control (Purchase Ledger), and supplier account views.

Does NOT include /api/reconcile/creditors (that belongs to balance_check app).
"""

import os
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List

from fastapi import APIRouter, HTTPException, Query, Body
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

        # KPIs - Statements counts
        cursor.execute("""
            SELECT
                COUNT(CASE WHEN received_date >= ? THEN 1 END) as today_count,
                COUNT(CASE WHEN received_date >= ? THEN 1 END) as week_count,
                COUNT(CASE WHEN received_date >= ? THEN 1 END) as month_count
            FROM supplier_statements
        """, (today_start.isoformat(), week_start.isoformat(), month_start.isoformat()))
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
                SUM(CASE WHEN sl.match_status = 'matched' THEN 1 ELSE 0 END) as matched_count,
                SUM(CASE WHEN sl.match_status = 'query' THEN 1 ELSE 0 END) as query_count
            FROM supplier_statements ss
            LEFT JOIN statement_lines sl ON sl.statement_id = ss.id
        """
        params = []
        if status:
            query += " WHERE ss.status = ?"
            params.append(status)
        query += " GROUP BY ss.id ORDER BY ss.received_date DESC"

        cursor.execute(query, params)
        statements = []

        # Try to get supplier names from Opera if SQL connector is available
        supplier_names = {}
        if sql_connector:
            try:
                name_query = "SELECT pn_account, pn_name FROM pname WITH (NOLOCK)"
                df = sql_connector.execute_query(name_query)
                if df is not None and len(df) > 0:
                    supplier_names = dict(zip(df['pn_account'], df['pn_name']))
            except Exception:
                pass  # Silently fail - will use supplier_code as name

        for row in cursor.fetchall():
            stmt = dict(row)
            stmt['supplier_name'] = supplier_names.get(stmt['supplier_code'], stmt['supplier_code'])
            statements.append(stmt)

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
                SUM(CASE WHEN sl.match_status = 'matched' THEN 1 ELSE 0 END) as matched_count,
                SUM(CASE WHEN sl.match_status = 'query' THEN 1 ELSE 0 END) as query_count
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


@router.post("/api/supplier-statements/{statement_id}/approve")
async def approve_supplier_statement(statement_id: int, approved_by: str = "System"):
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

        # Get statement details
        cursor.execute("SELECT * FROM supplier_statements WHERE id = ? AND status = 'queued'", (statement_id,))
        stmt = cursor.fetchone()

        if not stmt:
            conn.close()
            raise HTTPException(status_code=404, detail="Statement not found or not in queued status")

        stmt = dict(stmt)
        supplier_code = stmt['supplier_code']
        now_iso = datetime.now().isoformat()

        # Determine recipient email
        recipient_email = _get_supplier_contact_email(cursor, supplier_code, stmt.get('sender_email'))

        # Build response text — use edited response_text if available, else generate default
        response_text = stmt.get('response_text') or _generate_default_response(
            cursor, statement_id, supplier_code, stmt.get('statement_date'), sql_connector
        )

        # Get supplier name for email subject
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

        email_subject = f"Statement Response - {supplier_name} - {stmt.get('statement_date', 'N/A')}"

        # Send the response email
        email_sent = False
        email_error = None
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

            # Get supplier name
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

            email_subject = f"Statement Response - {supplier_name} - {stmt.get('statement_date', 'N/A')}"

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
    1. supplier_contacts_ext with is_statement_contact = 1
    2. sender_email from the statement record
    """
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
        pass  # Table may not exist yet

    return fallback_sender_email


def _generate_default_response(cursor, statement_id: int, supplier_code: str,
                                statement_date: Optional[str], sql_connector) -> str:
    """Generate a default response text for a statement approval."""
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

    # Get line details
    cursor.execute("""
        SELECT reference, debit, credit, match_status, query_type
        FROM statement_lines WHERE statement_id = ?
    """, (statement_id,))
    lines = cursor.fetchall()

    response_lines = []
    response_lines.append(f"Dear {supplier_name},")
    response_lines.append("")
    response_lines.append(f"Thank you for your statement dated {statement_date or 'N/A'}.")
    response_lines.append("")

    matched_count = sum(1 for l in lines if l['match_status'] == 'matched')
    query_count = sum(1 for l in lines if l['match_status'] == 'query')
    total_lines = len(lines)

    if total_lines > 0:
        response_lines.append(f"We have reviewed {total_lines} line(s):")
        response_lines.append(f"  - {matched_count} agreed")
        if query_count > 0:
            response_lines.append(f"  - {query_count} queried (details below)")
        response_lines.append("")

    if query_count > 0:
        response_lines.append("QUERIES:")
        response_lines.append("-" * 40)
        for line in lines:
            if line['match_status'] == 'query':
                amount = line['debit'] or line['credit'] or 0
                ref = line['reference'] or 'N/A'
                response_lines.append(f"  Ref: {ref} - Amount: {amount:,.2f} - {line['query_type'] or 'Query'}")
        response_lines.append("")
        response_lines.append("Please could you provide further details on the items queried above.")
    else:
        response_lines.append("All items have been agreed. Thank you.")

    response_lines.append("")
    response_lines.append("Regards,")
    response_lines.append("Accounts Department")

    return "\n".join(response_lines)


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
                SUM(CASE WHEN sl.match_status = 'matched' THEN 1 ELSE 0 END) as matched_count,
                SUM(CASE WHEN sl.match_status = 'query' THEN 1 ELSE 0 END) as query_count
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
                SUM(CASE WHEN sl.match_status = 'matched' THEN 1 ELSE 0 END) as matched_count,
                SUM(CASE WHEN sl.match_status = 'query' THEN 1 ELSE 0 END) as query_count
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

        # Get supplier name from Opera if available
        if sql_connector:
            try:
                name_query = f"SELECT pn_name FROM pname WITH (NOLOCK) WHERE pn_account = '{stmt['supplier_code']}'"
                df = sql_connector.execute_query(name_query)
                if df is not None and len(df) > 0:
                    stmt['supplier_name'] = df.iloc[0]['pn_name']
                else:
                    stmt['supplier_name'] = stmt['supplier_code']
            except Exception:
                stmt['supplier_name'] = stmt['supplier_code']
        else:
            stmt['supplier_name'] = stmt['supplier_code']

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
                   doc_type, match_status, matched_ptran_id, query_type,
                   query_sent_at, query_resolved_at
            FROM statement_lines
            WHERE statement_id = ?
            ORDER BY line_date, id
        """, (statement_id,))

        lines = [dict(row) for row in cursor.fetchall()]

        # Calculate summary
        summary = {
            "total_lines": len(lines),
            "total_debits": sum(l['debit'] or 0 for l in lines),
            "total_credits": sum(l['credit'] or 0 for l in lines),
            "matched_count": sum(1 for l in lines if l['match_status'] == 'matched'),
            "query_count": sum(1 for l in lines if l['match_status'] == 'query'),
            "unmatched_count": sum(1 for l in lines if l['match_status'] == 'unmatched'),
        }

        conn.close()
        return {"success": True, "lines": lines, "summary": summary}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting statement lines: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


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
               FROM pname WHERE pn_currbal <> 0"""
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
               FROM ptran WHERE pt_trtype = 'I' AND pt_trbal > 0 AND pt_dueday < GETDATE()"""
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
               FROM ptran WHERE pt_trtype = 'I' AND pt_trbal > 0
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
               FROM ptran WHERE pt_trtype = 'I' AND pt_trbal > 0
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
               FROM ptran WHERE pt_trtype = 'P' AND pt_trdate >= DATEADD(day, -7, GETDATE())"""
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
            FROM pname pn
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
async def search_suppliers(query: str):
    """
    Search for suppliers by any field - account, name, address, contact, email, phone.
    """
    from api.main import sql_connector

    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        results = sql_connector.execute_query(f"""
            SELECT TOP 20
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
            WHERE UPPER(pn_account) LIKE UPPER('%{query}%')
               OR UPPER(pn_name) LIKE UPPER('%{query}%')
               OR UPPER(pn_addr1) LIKE UPPER('%{query}%')
               OR UPPER(pn_addr2) LIKE UPPER('%{query}%')
               OR UPPER(pn_addr3) LIKE UPPER('%{query}%')
               OR UPPER(pn_addr4) LIKE UPPER('%{query}%')
               OR UPPER(pn_pstcode) LIKE UPPER('%{query}%')
               OR UPPER(pn_contact) LIKE UPPER('%{query}%')
               OR UPPER(pn_email) LIKE UPPER('%{query}%')
               OR UPPER(pn_teleno) LIKE UPPER('%{query}%')
            ORDER BY pn_name
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
async def get_supplier_account_view(account: str):
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
                pn_currbal AS current_balance,
                ISNULL(pn_ordrbal, 0) AS order_balance,
                pn_trnover AS turnover
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
                '' AS stat,
                CASE WHEN pt_trtype IN ('I', 'J') AND pt_trvalue > 0 THEN pt_trvalue ELSE NULL END AS debit,
                CASE WHEN pt_trtype IN ('C', 'P', 'F') OR pt_trvalue < 0 THEN ABS(pt_trvalue) ELSE NULL END AS credit,
                pt_trbal AS balance,
                pt_dueday AS due_date,
                pt_unique AS unique_id,
                pt_trtype AS raw_type
            FROM ptran WITH (NOLOCK)
            WHERE pt_account = '{account}'
            AND pt_trbal <> 0
            ORDER BY pt_trdate DESC, pt_trref
        """

        transactions_result = sql_connector.execute_query(transactions_query)
        if transactions_result is None:
            transactions = []
        elif hasattr(transactions_result, 'to_dict'):
            transactions = transactions_result.to_dict('records')
        else:
            transactions = transactions_result or []

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
