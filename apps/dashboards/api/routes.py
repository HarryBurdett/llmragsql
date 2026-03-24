"""
Dashboard API routes.

Extracted from api/main.py — provides endpoints for dashboards, finance summaries,
credit control, cashflow forecasts, nominal accounts, CEO KPIs, revenue analysis,
customer analysis, and executive summaries for both Opera SQL SE and Opera 3.

Does NOT include: /api/reconcile/* (balance_check app), /api/supplier* (suppliers app),
/api/gocardless/* (gocardless app).
"""

import logging
import re
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

router = APIRouter()


# ============================================================
# Helper Functions
# ============================================================

def df_to_records(df):
    """Convert DataFrame to list of dicts."""
    if hasattr(df, 'to_dict'):
        return df.to_dict('records')
    return df


# ============================================================
# Pydantic Models
# ============================================================

class CreditControlQueryRequest(BaseModel):
    question: str = Field(..., description="Natural language credit control question")


# ============================================================
# Credit Control Agent Query Definitions
# ============================================================
# Organized into categories matching the agent requirements
# Order matters - more specific patterns should be checked first

CREDIT_CONTROL_QUERIES = [
    # ========== PRE-ACTION CHECKS (run before every contact) ==========
    {
        "name": "unallocated_cash",
        "category": "pre_action",
        "keywords": ["unallocated cash", "unapplied cash", "unallocated payment", "cash on account", "credit balance"],
        "sql": """SELECT st.st_account AS account, sn.sn_name AS customer,
                  st.st_trref AS receipt_ref, st.st_trdate AS receipt_date,
                  ABS(st.st_trbal) AS unallocated_amount, sn.sn_teleno AS phone
                  FROM stran st
                  JOIN sname sn ON st.st_account = sn.sn_account
                  WHERE st.st_trtype = 'R' AND st.st_trbal < 0
                  ORDER BY ABS(st.st_trbal) DESC""",
        "description": "Unallocated cash/payments on customer accounts",
        "param_sql": """SELECT st.st_trref AS receipt_ref, st.st_trdate AS receipt_date,
                       ABS(st.st_trbal) AS unallocated_amount
                       FROM stran st WHERE st.st_account = '{account}'
                       AND st.st_trtype = 'R' AND st.st_trbal < 0"""
    },
    {
        "name": "pending_credit_notes",
        "category": "pre_action",
        "keywords": ["pending credit", "credit note", "credit notes", "credits pending", "unapplied credit"],
        "sql": """SELECT st.st_account AS account, sn.sn_name AS customer,
                  st.st_trref AS credit_ref, st.st_trdate AS credit_date,
                  ABS(st.st_trbal) AS credit_amount, st.st_memo AS reason
                  FROM stran st
                  JOIN sname sn ON st.st_account = sn.sn_account
                  WHERE st.st_trtype = 'C' AND st.st_trbal < 0
                  ORDER BY st.st_trdate DESC""",
        "description": "Pending/unallocated credit notes",
        "param_sql": """SELECT st.st_trref AS credit_ref, st.st_trdate AS credit_date,
                       ABS(st.st_trbal) AS credit_amount, st.st_memo AS reason
                       FROM stran st WHERE st.st_account = '{account}'
                       AND st.st_trtype = 'C' AND st.st_trbal < 0"""
    },
    {
        "name": "disputed_invoices",
        "category": "pre_action",
        "keywords": ["dispute", "disputed", "in dispute", "query", "queried"],
        "sql": """SELECT st.st_account AS account, sn.sn_name AS customer,
                  st.st_trref AS invoice, st.st_trdate AS invoice_date,
                  st.st_trbal AS outstanding, st.st_memo AS dispute_reason,
                  sn.sn_teleno AS phone
                  FROM stran st
                  JOIN sname sn ON st.st_account = sn.sn_account
                  WHERE st.st_dispute = 1 AND st.st_trbal > 0
                  ORDER BY st.st_trbal DESC""",
        "description": "Invoices flagged as disputed",
        "param_sql": """SELECT st.st_trref AS invoice, st.st_trdate AS invoice_date,
                       st.st_trbal AS outstanding, st.st_memo AS dispute_reason
                       FROM stran st WHERE st.st_account = '{account}'
                       AND st.st_dispute = 1 AND st.st_trbal > 0"""
    },
    {
        "name": "account_status",
        "category": "pre_action",
        "keywords": ["account status", "status check", "can we contact", "on hold", "do not contact", "insolvency", "dormant"],
        "sql": """SELECT sn_account AS account, sn_name AS customer,
                  sn_stop AS on_stop, sn_dormant AS dormant,
                  sn_currbal AS balance, sn_crlim AS credit_limit,
                  sn_crdscor AS credit_score, sn_crdnotes AS credit_notes,
                  sn_memo AS account_memo, sn_priorty AS priority,
                  sn_cmgroup AS credit_group
                  FROM sname WHERE sn_stop = 1 OR sn_dormant = 1
                  ORDER BY sn_currbal DESC""",
        "description": "Accounts with stop/hold/dormant flags",
        "param_sql": """SELECT sn_stop AS on_stop, sn_dormant AS dormant,
                       sn_crdscor AS credit_score, sn_crdnotes AS credit_notes,
                       sn_memo AS account_memo, sn_priorty AS priority
                       FROM sname WHERE sn_account = '{account}'"""
    },

    # ========== LEDGER STATE ==========
    {
        "name": "overdue_by_age",
        "category": "ledger",
        "keywords": ["overdue by age", "aged overdue", "aging bucket", "age bracket", "days overdue breakdown"],
        "sql": """SELECT
                  sn.sn_account AS account, sn.sn_name AS customer,
                  SUM(CASE WHEN DATEDIFF(day, st.st_dueday, GETDATE()) BETWEEN 0 AND 7 THEN st.st_trbal ELSE 0 END) AS days_1_7,
                  SUM(CASE WHEN DATEDIFF(day, st.st_dueday, GETDATE()) BETWEEN 8 AND 14 THEN st.st_trbal ELSE 0 END) AS days_8_14,
                  SUM(CASE WHEN DATEDIFF(day, st.st_dueday, GETDATE()) BETWEEN 15 AND 21 THEN st.st_trbal ELSE 0 END) AS days_15_21,
                  SUM(CASE WHEN DATEDIFF(day, st.st_dueday, GETDATE()) BETWEEN 22 AND 30 THEN st.st_trbal ELSE 0 END) AS days_22_30,
                  SUM(CASE WHEN DATEDIFF(day, st.st_dueday, GETDATE()) BETWEEN 31 AND 45 THEN st.st_trbal ELSE 0 END) AS days_31_45,
                  SUM(CASE WHEN DATEDIFF(day, st.st_dueday, GETDATE()) > 45 THEN st.st_trbal ELSE 0 END) AS days_45_plus,
                  SUM(st.st_trbal) AS total_overdue
                  FROM stran st
                  JOIN sname sn ON st.st_account = sn.sn_account
                  WHERE st.st_trtype = 'I' AND st.st_trbal > 0 AND st.st_dueday < GETDATE()
                  GROUP BY sn.sn_account, sn.sn_name
                  HAVING SUM(st.st_trbal) > 0
                  ORDER BY SUM(st.st_trbal) DESC""",
        "description": "Overdue invoices broken down by age bracket"
    },
    {
        "name": "customer_balance_aging",
        "category": "ledger",
        "keywords": ["balance aging", "ageing summary", "customer aging", "outstanding balance", "balance breakdown"],
        "sql": """SELECT sn.sn_account AS account, sn.sn_name AS customer,
                  sn.sn_currbal AS total_balance, sn.sn_crlim AS credit_limit,
                  sh.si_current AS current, sh.si_period1 AS period_1,
                  sh.si_period2 AS period_2, sh.si_period3 AS period_3,
                  sh.si_period4 AS period_4, sh.si_period5 AS period_5,
                  sh.si_avgdays AS avg_days_to_pay
                  FROM sname sn
                  LEFT JOIN shist sh ON sn.sn_account = sh.si_account
                  WHERE sn.sn_currbal > 0
                  ORDER BY sn.sn_currbal DESC""",
        "description": "Customer balance with aging periods"
    },
    {
        "name": "invoice_lookup",
        "category": "ledger",
        "keywords": ["invoice detail", "invoice lookup", "find invoice", "invoice number", "specific invoice"],
        "sql": None,  # Dynamic - needs invoice number extraction
        "description": "Look up specific invoice details",
        "param_sql": """SELECT st.st_account AS account, sn.sn_name AS customer,
                       st.st_trref AS invoice, st.st_custref AS your_ref,
                       st.st_trdate AS invoice_date, st.st_dueday AS due_date,
                       st.st_trvalue AS original_value, st.st_trbal AS outstanding,
                       st.st_dispute AS disputed, st.st_memo AS memo,
                       st.st_payday AS promise_date,
                       DATEDIFF(day, st.st_dueday, GETDATE()) AS days_overdue
                       FROM stran st
                       JOIN sname sn ON st.st_account = sn.sn_account
                       WHERE st.st_trref LIKE '%{invoice}%' AND st.st_trtype = 'I'"""
    },
    {
        "name": "overdue_invoices",
        "category": "ledger",
        "keywords": ["overdue", "past due", "late invoice", "unpaid invoice", "outstanding invoice"],
        "sql": """SELECT TOP 50 st.st_account AS account, sn.sn_name AS customer,
                  st.st_trref AS invoice, st.st_trdate AS invoice_date, st.st_dueday AS due_date,
                  st.st_trbal AS outstanding, DATEDIFF(day, st.st_dueday, GETDATE()) AS days_overdue,
                  st.st_dispute AS disputed, sn.sn_teleno AS phone
                  FROM stran st JOIN sname sn ON st.st_account = sn.sn_account
                  WHERE st.st_trtype = 'I' AND st.st_trbal > 0 AND st.st_dueday < GETDATE()
                  ORDER BY days_overdue DESC""",
        "description": "Overdue invoices ordered by days overdue"
    },

    # ========== CUSTOMER CONTEXT ==========
    {
        "name": "customer_master",
        "category": "customer",
        "keywords": ["customer details", "customer master", "contact details", "customer info", "customer record"],
        "sql": None,  # Dynamic - needs customer name/account extraction
        "description": "Customer master record with all details",
        "param_sql": """SELECT sn_account AS account, sn_name AS customer,
                       sn_addr1 AS address1, sn_addr2 AS address2, sn_addr3 AS city,
                       sn_addr4 AS county, sn_pstcode AS postcode,
                       sn_teleno AS phone, sn_email AS email,
                       sn_contact AS contact, sn_contac2 AS contact2,
                       sn_currbal AS balance, sn_crlim AS credit_limit,
                       sn_stop AS on_stop, sn_dormant AS dormant,
                       sn_custype AS customer_type, sn_region AS region,
                       sn_priorty AS priority, sn_cmgroup AS credit_group,
                       sn_lastinv AS last_invoice, sn_lastrec AS last_payment,
                       sn_memo AS memo, sn_crdnotes AS credit_notes
                       FROM sname WHERE sn_account = '{account}'
                       OR sn_name LIKE '%{customer}%'"""
    },
    {
        "name": "payment_history",
        "category": "customer",
        "keywords": ["payment history", "payment pattern", "how they pay", "days to pay", "payment behaviour", "payment behavior"],
        "sql": """SELECT TOP 20 st.st_account AS account, sn.sn_name AS customer,
                  st.st_trref AS receipt_ref, st.st_trdate AS payment_date,
                  ABS(st.st_trvalue) AS amount, sh.si_avgdays AS avg_days_to_pay
                  FROM stran st
                  JOIN sname sn ON st.st_account = sn.sn_account
                  LEFT JOIN shist sh ON st.st_account = sh.si_account
                  WHERE st.st_trtype = 'R'
                  ORDER BY st.st_trdate DESC""",
        "description": "Customer payment history and patterns",
        "param_sql": """SELECT st.st_trref AS receipt_ref, st.st_trdate AS payment_date,
                       ABS(st.st_trvalue) AS amount,
                       (SELECT TOP 1 si_avgdays FROM shist WHERE si_account = '{account}') AS avg_days_to_pay
                       FROM stran st WHERE st.st_account = '{account}'
                       AND st.st_trtype = 'R' ORDER BY st.st_trdate DESC"""
    },
    {
        "name": "customer_notes",
        "category": "customer",
        "keywords": ["customer notes", "memo", "notes", "comments", "history notes", "contact log"],
        "sql": """SELECT zn.zn_account AS account, sn.sn_name AS customer,
                  zn.zn_subject AS subject, zn.zn_note AS note,
                  zn.sq_date AS note_date, zn.sq_user AS created_by,
                  zn.zn_actreq AS action_required, zn.zn_actdate AS action_date,
                  zn.zn_actcomp AS action_complete, zn.zn_priority AS priority
                  FROM znotes zn
                  JOIN sname sn ON zn.zn_account = sn.sn_account
                  WHERE zn.zn_module = 'SL'
                  ORDER BY zn.sq_date DESC""",
        "description": "Customer notes and contact history",
        "param_sql": """SELECT zn.zn_subject AS subject, zn.zn_note AS note,
                       zn.sq_date AS note_date, zn.sq_user AS created_by,
                       zn.zn_actreq AS action_required, zn.zn_actdate AS action_date,
                       zn.zn_actcomp AS action_complete
                       FROM znotes zn WHERE zn.zn_account = '{account}'
                       AND zn.zn_module = 'SL' ORDER BY zn.sq_date DESC"""
    },
    {
        "name": "customer_segments",
        "category": "customer",
        "keywords": ["segment", "customer type", "strategic", "watch list", "customer category", "priority"],
        "sql": """SELECT sn_account AS account, sn_name AS customer,
                  sn_custype AS customer_type, sn_region AS region,
                  sn_priorty AS priority, sn_cmgroup AS credit_group,
                  sn_currbal AS balance, sn_crlim AS credit_limit,
                  sn_crdscor AS credit_score
                  FROM sname WHERE sn_currbal > 0
                  ORDER BY sn_priorty DESC, sn_currbal DESC""",
        "description": "Customer segments and priority flags"
    },

    # ========== PROMISE TRACKING ==========
    {
        "name": "promises_due",
        "category": "promise",
        "keywords": ["promise due", "promises today", "payment promise", "promised to pay", "ptp", "promise overdue"],
        "sql": """SELECT st.st_account AS account, sn.sn_name AS customer,
                  st.st_trref AS invoice, st.st_trbal AS outstanding,
                  st.st_payday AS promise_date,
                  DATEDIFF(day, st.st_payday, GETDATE()) AS days_since_promise,
                  sn.sn_teleno AS phone, sn.sn_contact AS contact
                  FROM stran st
                  JOIN sname sn ON st.st_account = sn.sn_account
                  WHERE st.st_payday IS NOT NULL
                  AND st.st_payday <= GETDATE()
                  AND st.st_trbal > 0
                  ORDER BY st.st_payday ASC""",
        "description": "Promises to pay due today or overdue"
    },
    {
        "name": "broken_promises",
        "category": "promise",
        "keywords": ["broken promise", "missed promise", "promise count", "failed promise", "promise history"],
        "sql": """SELECT st.st_account AS account, sn.sn_name AS customer,
                  COUNT(*) AS broken_promise_count,
                  SUM(st.st_trbal) AS total_outstanding,
                  MIN(st.st_payday) AS oldest_promise,
                  MAX(st.st_payday) AS latest_promise
                  FROM stran st
                  JOIN sname sn ON st.st_account = sn.sn_account
                  WHERE st.st_payday IS NOT NULL
                  AND st.st_payday < GETDATE()
                  AND st.st_trbal > 0
                  GROUP BY st.st_account, sn.sn_name
                  HAVING COUNT(*) > 1
                  ORDER BY COUNT(*) DESC""",
        "description": "Customers with multiple broken promises",
        "param_sql": """SELECT COUNT(*) AS broken_promise_count,
                       SUM(st_trbal) AS total_outstanding
                       FROM stran WHERE st_account = '{account}'
                       AND st_payday IS NOT NULL AND st_payday < GETDATE()
                       AND st_trbal > 0"""
    },

    # ========== MONITORING / REPORTING ==========
    {
        "name": "over_credit_limit",
        "category": "monitoring",
        "keywords": ["over credit", "exceed credit", "over limit", "exceeded limit", "above credit", "credit limit"],
        "sql": """SELECT sn_account AS account, sn_name AS customer, sn_currbal AS balance,
                  sn_crlim AS credit_limit, (sn_currbal - sn_crlim) AS over_by,
                  sn_ordrbal AS orders_pending, sn_teleno AS phone, sn_contact AS contact,
                  sn_stop AS on_stop
                  FROM sname WHERE sn_currbal > sn_crlim AND sn_crlim > 0
                  ORDER BY (sn_currbal - sn_crlim) DESC""",
        "description": "Customers over their credit limit"
    },
    {
        "name": "unallocated_cash_old",
        "category": "monitoring",
        "keywords": ["old unallocated", "unallocated over 7 days", "reconciliation", "cash reconciliation", "old cash"],
        "sql": """SELECT st.st_account AS account, sn.sn_name AS customer,
                  st.st_trref AS receipt_ref, st.st_trdate AS receipt_date,
                  ABS(st.st_trbal) AS unallocated_amount,
                  DATEDIFF(day, st.st_trdate, GETDATE()) AS days_unallocated,
                  sn.sn_teleno AS phone
                  FROM stran st
                  JOIN sname sn ON st.st_account = sn.sn_account
                  WHERE st.st_trtype = 'R' AND st.st_trbal < 0
                  AND DATEDIFF(day, st.st_trdate, GETDATE()) > 7
                  ORDER BY DATEDIFF(day, st.st_trdate, GETDATE()) DESC""",
        "description": "Unallocated cash older than 7 days (needs reconciliation)"
    },
    {
        "name": "accounts_on_stop",
        "category": "monitoring",
        "keywords": ["on stop", "stopped", "account stop", "credit stop"],
        "sql": """SELECT sn_account AS account, sn_name AS customer, sn_currbal AS balance,
                  sn_crlim AS credit_limit, sn_teleno AS phone, sn_contact AS contact,
                  sn_memo AS memo, sn_crdnotes AS credit_notes
                  FROM sname WHERE sn_stop = 1
                  ORDER BY sn_currbal DESC""",
        "description": "Accounts currently on stop"
    },
    {
        "name": "top_debtors",
        "category": "monitoring",
        "keywords": ["owes most", "top debtor", "highest balance", "biggest debt", "most money", "largest balance", "owe us"],
        "sql": """SELECT TOP 20 sn_account AS account, sn_name AS customer, sn_currbal AS balance,
                  sn_crlim AS credit_limit, sn_lastrec AS last_payment, sn_teleno AS phone,
                  CASE WHEN sn_stop = 1 THEN 'ON STOP'
                       WHEN sn_currbal > sn_crlim AND sn_crlim > 0 THEN 'OVER LIMIT'
                       ELSE 'OK' END AS status
                  FROM sname WHERE sn_currbal > 0 ORDER BY sn_currbal DESC""",
        "description": "Top 20 customers by outstanding balance"
    },
    {
        "name": "recent_payments",
        "category": "monitoring",
        "keywords": ["recent payment", "last payment", "receipts", "paid recently", "who paid"],
        "sql": """SELECT TOP 20 st.st_account AS account, sn.sn_name AS customer,
                  st.st_trdate AS payment_date, st.st_trref AS reference,
                  ABS(st.st_trvalue) AS amount
                  FROM stran st JOIN sname sn ON st.st_account = sn.sn_account
                  WHERE st.st_trtype = 'R' ORDER BY st.st_trdate DESC""",
        "description": "Recent payments received"
    },
    {
        "name": "aged_debt",
        "category": "monitoring",
        "keywords": ["aged debt", "aging", "debt age", "how old", "debt summary"],
        "sql": """SELECT sn_account AS account, sn_name AS customer, sn_currbal AS balance,
                  sn_lastrec AS last_payment, DATEDIFF(day, sn_lastrec, GETDATE()) AS days_since_payment,
                  CASE WHEN sn_currbal > sn_crlim AND sn_crlim > 0 THEN 'OVER LIMIT'
                       WHEN sn_stop = 1 THEN 'ON STOP' ELSE 'OK' END AS status
                  FROM sname WHERE sn_currbal > 0 ORDER BY sn_currbal DESC""",
        "description": "Aged debt summary with payment history"
    },

    # ========== LOOKUP (Dynamic) ==========
    {
        "name": "customer_lookup",
        "category": "lookup",
        "keywords": ["details for", "info for", "lookup", "find customer", "about customer", "tell me about"],
        "sql": None,  # Dynamic - needs customer name extraction
        "description": "Look up specific customer details"
    }
]


# Helper function to get query by name (for programmatic API access)
def get_query_by_name(name: str) -> dict:
    """Get a specific query definition by name."""
    for q in CREDIT_CONTROL_QUERIES:
        if q["name"] == name:
            return q
    return None


# ============================================================
# SQL SE Credit Control Endpoints
# ============================================================

@router.get("/api/credit-control/queries")
async def list_credit_control_queries():
    """List all available credit control query types."""
    queries = []
    for q in CREDIT_CONTROL_QUERIES:
        queries.append({
            "name": q["name"],
            "category": q.get("category", "general"),
            "description": q["description"],
            "keywords": q["keywords"],
            "has_param_sql": "param_sql" in q
        })
    return {"queries": queries}


@router.post("/api/credit-control/query-param")
async def credit_control_query_param(query_name: str, account: str = None, customer: str = None, invoice: str = None):
    """
    Execute a parameterized credit control query.
    For use by the credit control agent with specific parameters.
    """
    from api.main import sql_connector
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    query_def = get_query_by_name(query_name)
    if not query_def:
        raise HTTPException(status_code=404, detail=f"Query '{query_name}' not found")

    if "param_sql" not in query_def:
        raise HTTPException(status_code=400, detail=f"Query '{query_name}' does not support parameters")

    try:
        sql = query_def["param_sql"]
        if account:
            sql = sql.replace("{account}", account)
        if customer:
            sql = sql.replace("{customer}", customer)
        if invoice:
            sql = sql.replace("{invoice}", invoice)

        result = sql_connector.execute_query(sql)
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        return {
            "success": True,
            "query_name": query_name,
            "category": query_def.get("category", "general"),
            "description": query_def["description"],
            "data": result or [],
            "count": len(result) if result else 0
        }
    except Exception as e:
        logger.error(f"Parameterized query failed: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/credit-control/dashboard")
async def credit_control_dashboard():
    """
    Get summary dashboard data for credit control.
    Returns key metrics in a single API call.
    """
    from api.main import sql_connector
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        metrics = {}

        # Total outstanding balance
        result = sql_connector.execute_query(
            "SELECT COUNT(*) AS count, SUM(sn_currbal) AS total FROM sname WHERE sn_currbal > 0"
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["total_debt"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Total Outstanding"
            }

        # Over credit limit
        result = sql_connector.execute_query(
            """SELECT COUNT(*) AS count, SUM(sn_currbal - sn_crlim) AS total
               FROM sname WHERE sn_currbal > sn_crlim AND sn_crlim > 0"""
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["over_credit_limit"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Over Credit Limit"
            }

        # Accounts on stop
        result = sql_connector.execute_query(
            "SELECT COUNT(*) AS count, SUM(sn_currbal) AS total FROM sname WHERE sn_stop = 1"
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["accounts_on_stop"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Accounts On Stop"
            }

        # Overdue invoices
        result = sql_connector.execute_query(
            """SELECT COUNT(*) AS count, SUM(st_trbal) AS total
               FROM stran WHERE st_trtype = 'I' AND st_trbal > 0 AND st_dueday < GETDATE()"""
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["overdue_invoices"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Overdue Invoices"
            }

        # Recent payments (last 7 days)
        result = sql_connector.execute_query(
            """SELECT COUNT(*) AS count, SUM(ABS(st_trvalue)) AS total
               FROM stran WHERE st_trtype = 'R' AND st_trdate >= DATEADD(day, -7, GETDATE())"""
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["recent_payments"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Payments (7 days)"
            }

        # Promises due today or overdue
        result = sql_connector.execute_query(
            """SELECT COUNT(*) AS count, SUM(st_trbal) AS total
               FROM stran WHERE st_payday IS NOT NULL
               AND st_payday <= GETDATE() AND st_trbal > 0"""
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["promises_due"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Promises Due"
            }

        # Disputed invoices
        result = sql_connector.execute_query(
            """SELECT COUNT(*) AS count, SUM(st_trbal) AS total
               FROM stran WHERE st_dispute = 1 AND st_trbal > 0"""
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["disputed"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "In Dispute"
            }

        # Unallocated cash
        result = sql_connector.execute_query(
            """SELECT COUNT(*) AS count, SUM(ABS(st_trbal)) AS total
               FROM stran WHERE st_trtype = 'R' AND st_trbal < 0"""
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["unallocated_cash"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Unallocated Cash"
            }

        # Priority actions - customers needing attention
        priority_result = sql_connector.execute_query(
            """SELECT TOP 10 sn_account AS account, sn_name AS customer, sn_currbal AS balance,
                      sn_crlim AS credit_limit, sn_teleno AS phone, sn_contact AS contact,
                      CASE WHEN sn_stop = 1 THEN 'ON_STOP'
                           WHEN sn_currbal > sn_crlim AND sn_crlim > 0 THEN 'OVER_LIMIT'
                           ELSE 'HIGH_BALANCE' END AS priority_reason
               FROM sname
               WHERE sn_currbal > 0 AND (sn_stop = 1 OR sn_currbal > sn_crlim)
               ORDER BY sn_currbal DESC"""
        )
        if hasattr(priority_result, 'to_dict'):
            priority_result = priority_result.to_dict('records')

        return {
            "success": True,
            "metrics": metrics,
            "priority_actions": priority_result or []
        }

    except Exception as e:
        logger.error(f"Dashboard query failed: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/credit-control/debtors-report")
async def credit_control_debtors_report():
    """
    Get aged debtors report with balance breakdown by aging period.
    Columns: Account, Customer, Balance, Current, 1 Month, 2 Month, 3 Month+
    """
    from api.main import sql_connector
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Query joins sname (customer) with shist (aging history) to get aged balances
        # si_age = 1 is the most recent aging period
        result = sql_connector.execute_query("""
            SELECT
                sn.sn_account AS account,
                sn.sn_name AS customer,
                sn.sn_currbal AS balance,
                ISNULL(sh.si_current, 0) AS current_period,
                ISNULL(sh.si_period1, 0) AS month_1,
                ISNULL(sh.si_period2, 0) AS month_2,
                ISNULL(sh.si_period3, 0) + ISNULL(sh.si_period4, 0) + ISNULL(sh.si_period5, 0) AS month_3_plus,
                sn.sn_crlim AS credit_limit,
                sn.sn_teleno AS phone,
                sn.sn_contact AS contact,
                sn.sn_stop AS on_stop
            FROM sname sn
            LEFT JOIN shist sh ON sn.sn_account = sh.si_account AND sh.si_age = 1
            WHERE sn.sn_currbal <> 0
            ORDER BY sn.sn_account
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
        logger.error(f"Debtors report query failed: {e}")
        return {"success": False, "error": str(e)}


# ============================================================
# Nominal Ledger Endpoints
# ============================================================

@router.get("/api/nominal/trial-balance")
async def nominal_trial_balance(year: int = 2026):
    """
    Get summary trial balance from the nominal ledger.
    Returns account balances with debit/credit columns for the specified year.
    Only includes transactions dated within the specified year.
    """
    from api.main import sql_connector
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Query the ntran (nominal transactions) table filtered by year
        # Join with nacnt to get account descriptions
        # Sum all transactions for each account within the specified year
        result = sql_connector.execute_query(f"""
            SELECT
                RTRIM(t.nt_acnt) AS account_code,
                RTRIM(n.na_desc) AS description,
                RTRIM(t.nt_type) AS account_type,
                RTRIM(t.nt_subt) AS subtype,
                0 AS opening_balance,
                ISNULL(SUM(t.nt_value), 0) AS ytd_movement,
                CASE
                    WHEN ISNULL(SUM(t.nt_value), 0) > 0 THEN ISNULL(SUM(t.nt_value), 0)
                    ELSE 0
                END AS debit,
                CASE
                    WHEN ISNULL(SUM(t.nt_value), 0) < 0 THEN ABS(ISNULL(SUM(t.nt_value), 0))
                    ELSE 0
                END AS credit
            FROM ntran t
            LEFT JOIN nacnt n ON RTRIM(t.nt_acnt) = RTRIM(n.na_acnt)
            WHERE t.nt_year = {year}
            GROUP BY t.nt_acnt, n.na_desc, t.nt_type, t.nt_subt
            HAVING ISNULL(SUM(t.nt_value), 0) <> 0
            ORDER BY t.nt_acnt
        """)

        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        # Calculate totals
        total_debit = sum(r.get("debit", 0) or 0 for r in result)
        total_credit = sum(r.get("credit", 0) or 0 for r in result)

        # Group by account type for summary
        type_summary = {}
        type_names = {
            'A': 'Fixed Assets',
            'B': 'Current Assets',
            'C': 'Current Liabilities',
            'D': 'Capital & Reserves',
            'E': 'Sales',
            'F': 'Cost of Sales',
            'G': 'Overheads',
            'H': 'Other'
        }
        for r in result:
            atype = r.get("account_type", "?").strip()
            if atype not in type_summary:
                type_summary[atype] = {
                    "name": type_names.get(atype, f"Type {atype}"),
                    "debit": 0,
                    "credit": 0,
                    "count": 0
                }
            type_summary[atype]["debit"] += r.get("debit", 0) or 0
            type_summary[atype]["credit"] += r.get("credit", 0) or 0
            type_summary[atype]["count"] += 1

        return {
            "success": True,
            "year": year,
            "data": result or [],
            "count": len(result) if result else 0,
            "totals": {
                "debit": total_debit,
                "credit": total_credit,
                "difference": total_debit - total_credit
            },
            "by_type": type_summary
        }

    except Exception as e:
        logger.error(f"Trial balance query failed: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/nominal/statutory-accounts")
async def nominal_statutory_accounts(year: int = 2026):
    """
    Generate UK statutory accounts (P&L and Balance Sheet) from ntran.
    Returns formatted accounts following UK GAAP structure.
    """
    from api.main import sql_connector
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Query ntran for all transactions in the year, grouped by account type/subtype
        result = sql_connector.execute_query(f"""
            SELECT
                RTRIM(t.nt_type) AS account_type,
                RTRIM(t.nt_subt) AS subtype,
                RTRIM(t.nt_acnt) AS account_code,
                RTRIM(n.na_desc) AS description,
                ISNULL(SUM(t.nt_value), 0) AS value
            FROM ntran t
            LEFT JOIN nacnt n ON RTRIM(t.nt_acnt) = RTRIM(n.na_acnt)
            WHERE t.nt_year = {year}
            GROUP BY t.nt_type, t.nt_subt, t.nt_acnt, n.na_desc
            HAVING ISNULL(SUM(t.nt_value), 0) <> 0
            ORDER BY t.nt_type, t.nt_subt, t.nt_acnt
        """)

        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        # UK Statutory Account Structure
        # P&L: E=Sales (Credit), F=Cost of Sales (Debit), G=Overheads (Debit)
        # Balance Sheet: A=Fixed Assets, B=Current Assets, C=Current Liabilities, D=Capital & Reserves

        # Build P&L
        turnover = []
        cost_of_sales = []
        administrative_expenses = []
        other_income = []

        # Build Balance Sheet
        fixed_assets = []
        current_assets = []
        current_liabilities = []
        capital_reserves = []

        for r in result:
            atype = (r.get("account_type") or "").strip()
            value = r.get("value", 0) or 0
            item = {
                "code": r.get("account_code", "").strip(),
                "description": r.get("description", ""),
                "value": value
            }

            if atype == "E":  # Sales (typically credit/negative in ledger)
                turnover.append(item)
            elif atype == "F":  # Cost of Sales
                cost_of_sales.append(item)
            elif atype == "G":  # Overheads/Admin
                administrative_expenses.append(item)
            elif atype == "H":  # Other
                other_income.append(item)
            elif atype == "A":  # Fixed Assets
                fixed_assets.append(item)
            elif atype == "B":  # Current Assets
                current_assets.append(item)
            elif atype == "C":  # Current Liabilities
                current_liabilities.append(item)
            elif atype == "D":  # Capital & Reserves
                capital_reserves.append(item)

        # Calculate P&L totals (Sales are negative/credit, so negate for display)
        total_turnover = -sum(i["value"] for i in turnover)  # Negate credits
        total_cos = sum(i["value"] for i in cost_of_sales)
        gross_profit = total_turnover - total_cos

        total_admin = sum(i["value"] for i in administrative_expenses)
        total_other = -sum(i["value"] for i in other_income)  # Negate if credit
        operating_profit = gross_profit - total_admin + total_other

        # Calculate Balance Sheet totals
        total_fixed = sum(i["value"] for i in fixed_assets)
        total_current_assets = sum(i["value"] for i in current_assets)
        total_current_liab = -sum(i["value"] for i in current_liabilities)  # Liabilities are credits
        net_current_assets = total_current_assets - total_current_liab
        total_assets_less_liab = total_fixed + net_current_assets
        total_capital = -sum(i["value"] for i in capital_reserves)  # Capital is credit

        return {
            "success": True,
            "year": year,
            "profit_and_loss": {
                "turnover": {
                    "items": turnover,
                    "total": total_turnover
                },
                "cost_of_sales": {
                    "items": cost_of_sales,
                    "total": total_cos
                },
                "gross_profit": gross_profit,
                "administrative_expenses": {
                    "items": administrative_expenses,
                    "total": total_admin
                },
                "other_operating_income": {
                    "items": other_income,
                    "total": total_other
                },
                "operating_profit": operating_profit,
                "profit_before_tax": operating_profit,
                "profit_after_tax": operating_profit  # Simplified - no tax calc
            },
            "balance_sheet": {
                "fixed_assets": {
                    "items": fixed_assets,
                    "total": total_fixed
                },
                "current_assets": {
                    "items": current_assets,
                    "total": total_current_assets
                },
                "current_liabilities": {
                    "items": current_liabilities,
                    "total": total_current_liab
                },
                "net_current_assets": net_current_assets,
                "total_assets_less_current_liabilities": total_assets_less_liab,
                "capital_and_reserves": {
                    "items": capital_reserves,
                    "total": total_capital
                }
            }
        }

    except Exception as e:
        logger.error(f"Statutory accounts query failed: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/credit-control/query")
async def credit_control_query(request: CreditControlQueryRequest):
    """
    Answer credit control questions using LIVE SQL queries.
    More accurate than RAG for precise financial data.
    Supports all 15+ credit control agent query types.
    """
    from api.main import sql_connector

    # Backend validation - check for empty question
    if not request.question or not request.question.strip():
        return {
            "success": False,
            "error": "Question is required. Please enter a credit control question.",
            "description": "Validation Error",
            "count": 0,
            "data": []
        }

    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    question = request.question.lower()

    try:
        # Find matching query based on keywords (list is ordered by priority)
        matched_query = None
        for query_info in CREDIT_CONTROL_QUERIES:
            if any(kw in question for kw in query_info["keywords"]):
                matched_query = query_info
                break

        # Handle dynamic lookups that need parameter extraction
        if matched_query and matched_query["name"] == "customer_lookup":
            # Extract customer name from question
            words = request.question.split()
            search_terms = [w for w in words if len(w) > 2 and w.lower() not in
                          ["customer", "account", "find", "lookup", "details", "for", "info", "what", "about", "the", "tell", "me"]]
            if search_terms:
                search = "%".join(search_terms)
                sql = f"""SELECT sn_account AS account, sn_name AS customer, sn_currbal AS balance,
                          sn_crlim AS credit_limit, sn_stop AS on_stop, sn_dormant AS dormant,
                          sn_teleno AS phone, sn_email AS email, sn_contact AS contact,
                          sn_lastrec AS last_payment, sn_lastinv AS last_invoice,
                          sn_priorty AS priority, sn_cmgroup AS credit_group,
                          sn_memo AS memo, sn_crdnotes AS credit_notes
                          FROM sname WHERE sn_name LIKE '%{search}%' OR sn_account LIKE '%{search}%'"""
            else:
                return {"success": False, "error": "Could not extract customer name from question"}

        elif matched_query and matched_query["name"] == "invoice_lookup":
            # Extract invoice number from question
            invoice_match = re.search(r'([A-Z0-9]{4,})', request.question.upper())
            if invoice_match:
                invoice = invoice_match.group(1)
                sql = f"""SELECT st.st_account AS account, sn.sn_name AS customer,
                         st.st_trref AS invoice, st.st_custref AS your_ref,
                         st.st_trdate AS invoice_date, st.st_dueday AS due_date,
                         st.st_trvalue AS original_value, st.st_trbal AS outstanding,
                         st.st_dispute AS disputed, st.st_memo AS memo,
                         st.st_payday AS promise_date,
                         DATEDIFF(day, st.st_dueday, GETDATE()) AS days_overdue
                         FROM stran st
                         JOIN sname sn ON st.st_account = sn.sn_account
                         WHERE st.st_trref LIKE '%{invoice}%' AND st.st_trtype = 'I'"""
            else:
                return {"success": False, "error": "Could not extract invoice number from question"}

        elif matched_query and matched_query["name"] == "customer_master":
            # Extract customer name/account from question
            words = request.question.split()
            search_terms = [w for w in words if len(w) > 2 and w.lower() not in
                          ["customer", "master", "details", "record", "for", "info", "get", "show"]]
            if search_terms:
                search = "%".join(search_terms)
                sql = matched_query.get("param_sql", "").replace("{account}", search).replace("{customer}", search)
            else:
                # Return all customers if no specific one mentioned
                sql = """SELECT TOP 50 sn_account AS account, sn_name AS customer,
                        sn_currbal AS balance, sn_crlim AS credit_limit,
                        sn_teleno AS phone, sn_email AS email
                        FROM sname WHERE sn_currbal > 0 ORDER BY sn_currbal DESC"""

        elif matched_query:
            sql = matched_query["sql"]
        else:
            # Default: return summary of problem accounts
            sql = """SELECT TOP 20 sn_account AS account, sn_name AS customer, sn_currbal AS balance,
                     sn_crlim AS credit_limit,
                     CASE WHEN sn_currbal > sn_crlim AND sn_crlim > 0 THEN 'OVER LIMIT'
                          WHEN sn_stop = 1 THEN 'ON STOP' ELSE 'OK' END AS status
                     FROM sname WHERE sn_currbal > 0 ORDER BY sn_currbal DESC"""
            matched_query = {"name": "summary", "category": "general", "description": "Summary of accounts with balances"}

        # Execute the SQL
        result = sql_connector.execute_query(sql)
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        if not result:
            return {
                "success": True,
                "query_type": matched_query["name"] if matched_query else "unknown",
                "category": matched_query.get("category", "general") if matched_query else "general",
                "description": matched_query["description"] if matched_query else "No matching query",
                "data": [],
                "count": 0,
                "summary": "No results found"
            }

        # Generate a summary based on query type
        query_name = matched_query["name"] if matched_query else "unknown"
        count = len(result)

        # Custom summaries for different query types
        if query_name == "top_debtors":
            total = sum(r.get("balance", 0) or 0 for r in result)
            summary = f"Top {count} debtors owe a total of \u00a3{total:,.2f}. Highest: {result[0]['customer'].strip()} (\u00a3{result[0]['balance']:,.2f})"
        elif query_name == "over_credit_limit":
            total_over = sum(r.get("over_by", 0) or 0 for r in result)
            summary = f"{count} customers are over their credit limit by a total of \u00a3{total_over:,.2f}"
        elif query_name == "accounts_on_stop":
            total_value = sum(r.get("balance", 0) or 0 for r in result)
            summary = f"{count} accounts on stop with total debt of \u00a3{total_value:,.2f}"
        elif query_name == "overdue_invoices":
            total_overdue = sum(r.get("outstanding", 0) or 0 for r in result)
            summary = f"{count} overdue invoices totaling \u00a3{total_overdue:,.2f}"
        elif query_name == "unallocated_cash":
            total_unalloc = sum(r.get("unallocated_amount", 0) or 0 for r in result)
            summary = f"{count} receipts with \u00a3{total_unalloc:,.2f} unallocated cash"
        elif query_name == "pending_credit_notes":
            total_credits = sum(r.get("credit_amount", 0) or 0 for r in result)
            summary = f"{count} pending credit notes totaling \u00a3{total_credits:,.2f}"
        elif query_name == "disputed_invoices":
            total_disputed = sum(r.get("outstanding", 0) or 0 for r in result)
            summary = f"{count} invoices in dispute totaling \u00a3{total_disputed:,.2f}"
        elif query_name == "promises_due":
            total_promised = sum(r.get("outstanding", 0) or 0 for r in result)
            summary = f"{count} promises due/overdue totaling \u00a3{total_promised:,.2f}"
        elif query_name == "broken_promises":
            total_broken = sum(r.get("broken_promise_count", 0) or 0 for r in result)
            summary = f"{count} customers with {total_broken} total broken promises"
        elif query_name == "unallocated_cash_old":
            total_old = sum(r.get("unallocated_amount", 0) or 0 for r in result)
            summary = f"{count} receipts with \u00a3{total_old:,.2f} unallocated for over 7 days - needs reconciliation"
        elif query_name == "overdue_by_age":
            total = sum(r.get("total_overdue", 0) or 0 for r in result)
            summary = f"{count} customers with \u00a3{total:,.2f} overdue debt across all age brackets"
        elif query_name == "customer_balance_aging":
            total = sum(r.get("total_balance", 0) or 0 for r in result)
            summary = f"{count} customers with \u00a3{total:,.2f} total outstanding balance"
        elif query_name == "recent_payments":
            total_paid = sum(r.get("amount", 0) or 0 for r in result)
            summary = f"{count} recent payments totaling \u00a3{total_paid:,.2f}"
        elif query_name == "account_status":
            summary = f"{count} accounts with stop, hold, or dormant flags"
        elif query_name == "customer_notes":
            summary = f"{count} notes/contact log entries found"
        else:
            summary = f"Found {count} records"

        return {
            "success": True,
            "query_type": query_name,
            "category": matched_query.get("category", "general") if matched_query else "general",
            "description": matched_query["description"] if matched_query else "Custom query",
            "data": result,
            "count": count,
            "summary": summary,
            "sql_used": sql
        }

    except Exception as e:
        logger.error(f"Credit control query failed: {e}")
        return {"success": False, "error": str(e)}


# ============================================================
# Cashflow Forecast Endpoints
# ============================================================

@router.get("/api/cashflow/forecast")
async def cashflow_forecast(years_history: int = 3):
    """
    Generate cashflow forecast based on historical transaction patterns.
    Analyzes receipts (money in) and payments (money out) to predict monthly cashflow.
    """
    from api.main import sql_connector
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        current_year = datetime.now().year
        current_month = datetime.now().month

        # First, get the date range of available data
        date_range_sql = """
            SELECT
                MIN(st_trdate) as min_date,
                MAX(st_trdate) as max_date,
                DATEDIFF(year, MIN(st_trdate), MAX(st_trdate)) + 1 as years_span
            FROM stran
            WHERE st_trtype = 'R'
        """
        date_range = sql_connector.execute_query(date_range_sql)
        if hasattr(date_range, 'to_dict'):
            date_range = date_range.to_dict('records')

        # Use all available data if years_history covers more than available
        # Or limit to requested years from the most recent data
        use_all_data = True  # Default to using all available historical data

        # Get historical receipts by month (from sales ledger)
        if use_all_data:
            receipts_sql = """
                SELECT
                    YEAR(st_trdate) AS year,
                    MONTH(st_trdate) AS month,
                    COUNT(*) AS transaction_count,
                    SUM(ABS(st_trvalue)) AS total_amount
                FROM stran
                WHERE st_trtype = 'R'
                GROUP BY YEAR(st_trdate), MONTH(st_trdate)
                ORDER BY year, month
            """
        else:
            receipts_sql = f"""
                SELECT
                    YEAR(st_trdate) AS year,
                    MONTH(st_trdate) AS month,
                    COUNT(*) AS transaction_count,
                    SUM(ABS(st_trvalue)) AS total_amount
                FROM stran
                WHERE st_trtype = 'R'
                AND st_trdate >= DATEADD(year, -{years_history}, GETDATE())
                GROUP BY YEAR(st_trdate), MONTH(st_trdate)
                ORDER BY year, month
            """
        receipts_result = sql_connector.execute_query(receipts_sql)
        if hasattr(receipts_result, 'to_dict'):
            receipts_result = receipts_result.to_dict('records')

        # Get historical payments by month (from purchase ledger)
        if use_all_data:
            payments_sql = """
                SELECT
                    YEAR(pt_trdate) AS year,
                    MONTH(pt_trdate) AS month,
                    COUNT(*) AS transaction_count,
                    SUM(ABS(pt_trvalue)) AS total_amount
                FROM ptran
                WHERE pt_trtype = 'P'
                GROUP BY YEAR(pt_trdate), MONTH(pt_trdate)
                ORDER BY year, month
            """
        else:
            payments_sql = f"""
                SELECT
                    YEAR(pt_trdate) AS year,
                    MONTH(pt_trdate) AS month,
                    COUNT(*) AS transaction_count,
                    SUM(ABS(pt_trvalue)) AS total_amount
                FROM ptran
                WHERE pt_trtype = 'P'
                AND pt_trdate >= DATEADD(year, -{years_history}, GETDATE())
                GROUP BY YEAR(pt_trdate), MONTH(pt_trdate)
                ORDER BY year, month
            """
        payments_result = sql_connector.execute_query(payments_sql)
        if hasattr(payments_result, 'to_dict'):
            payments_result = payments_result.to_dict('records')

        # Get payroll history (weekly pay converted to monthly estimates)
        # whist uses tax year format (1819 = tax year 2018/19, April to April)
        # wh_period is the week number (1-52)
        payroll_sql = """
            SELECT
                wh_year as tax_year,
                wh_period as week,
                SUM(CAST(wh_net AS FLOAT)) as net_pay,
                SUM(CAST(wh_erni AS FLOAT)) as employer_ni
            FROM whist
            GROUP BY wh_year, wh_period
            ORDER BY wh_year DESC, wh_period DESC
        """
        payroll_result = sql_connector.execute_query(payroll_sql)
        if hasattr(payroll_result, 'to_dict'):
            payroll_result = payroll_result.to_dict('records')

        # Get recurring expenses from nominal ledger by period
        # Focus on key expense categories that represent cash outflows
        expenses_sql = """
            SELECT
                nt_year as year,
                nt_period as period,
                SUM(CASE WHEN nt_acnt LIKE 'W%' THEN CAST(nt_value AS FLOAT) ELSE 0 END) as payroll_related,
                SUM(CASE WHEN nt_acnt IN ('Q125') THEN CAST(nt_value AS FLOAT) ELSE 0 END) as rent_rates,
                SUM(CASE WHEN nt_acnt IN ('Q120') THEN CAST(nt_value AS FLOAT) ELSE 0 END) as utilities,
                SUM(CASE WHEN nt_acnt LIKE 'Q13%' THEN CAST(nt_value AS FLOAT) ELSE 0 END) as insurance,
                SUM(CASE WHEN nt_acnt IN ('U230') THEN CAST(nt_value AS FLOAT) ELSE 0 END) as loan_interest
            FROM ntran
            WHERE RTRIM(nt_type) IN ('45', 'H')  -- Expenses / Overheads (H is Opera letter code)
            GROUP BY nt_year, nt_period
            ORDER BY nt_year DESC, nt_period DESC
        """
        expenses_result = sql_connector.execute_query(expenses_sql)
        if hasattr(expenses_result, 'to_dict'):
            expenses_result = expenses_result.to_dict('records')

        # Build historical data structure
        historical_receipts = defaultdict(list)  # month -> [amounts]
        historical_payments = defaultdict(list)  # month -> [amounts]
        historical_payroll = defaultdict(list)  # month -> [amounts]
        historical_recurring = defaultdict(list)  # month -> [amounts]

        for r in receipts_result or []:
            month = int(r['month'])
            amount = float(r['total_amount'] or 0)
            historical_receipts[month].append(amount)

        for p in payments_result or []:
            month = int(p['month'])
            amount = float(p['total_amount'] or 0)
            historical_payments[month].append(amount)

        # Process payroll data - convert weekly to monthly
        # Tax year 1819 starts April 2018, 1718 starts April 2017, etc.
        # Week 1-4 = April (month 4), Week 5-8 = May (month 5), etc.
        for pay in payroll_result or []:
            week = int(pay['week'])
            net_pay = float(pay['net_pay'] or 0)
            employer_ni = float(pay['employer_ni'] or 0)
            total_payroll = net_pay + employer_ni

            # Convert week to month (approximate)
            # Week 1-4 = Month 4 (April), Week 5-8 = Month 5 (May), etc.
            month_offset = (week - 1) // 4
            month = ((month_offset + 3) % 12) + 1  # Tax year starts in April

            historical_payroll[month].append(total_payroll)

        # Process recurring expenses - ntran uses calendar periods (1-12)
        for exp in expenses_result or []:
            period = int(exp['period'])
            if 1 <= period <= 12:
                rent = float(exp['rent_rates'] or 0)
                utilities = float(exp['utilities'] or 0)
                insurance = float(exp['insurance'] or 0)
                loan_interest = float(exp['loan_interest'] or 0)
                total_recurring = rent + utilities + insurance + loan_interest
                if total_recurring > 0:
                    historical_recurring[period].append(total_recurring)

        # Calculate averages and build forecast for current year
        forecast = []
        month_names = ['', 'January', 'February', 'March', 'April', 'May', 'June',
                       'July', 'August', 'September', 'October', 'November', 'December']

        annual_receipts_total = 0
        annual_payments_total = 0
        annual_payroll_total = 0
        annual_recurring_total = 0

        for month in range(1, 13):
            receipts_history = historical_receipts.get(month, [])
            payments_history = historical_payments.get(month, [])
            payroll_history = historical_payroll.get(month, [])
            recurring_history = historical_recurring.get(month, [])

            avg_receipts = sum(receipts_history) / len(receipts_history) if receipts_history else 0
            avg_payments = sum(payments_history) / len(payments_history) if payments_history else 0
            avg_payroll = sum(payroll_history) / len(payroll_history) if payroll_history else 0
            avg_recurring = sum(recurring_history) / len(recurring_history) if recurring_history else 0

            # Total expected payments includes purchase payments + payroll + recurring expenses
            total_expected_payments = avg_payments + avg_payroll + avg_recurring
            net_cashflow = avg_receipts - total_expected_payments

            # Determine if this is actual or forecast
            is_actual = month < current_month
            is_current = month == current_month

            annual_receipts_total += avg_receipts
            annual_payments_total += avg_payments
            annual_payroll_total += avg_payroll
            annual_recurring_total += avg_recurring

            forecast.append({
                "month": month,
                "month_name": month_names[month],
                "expected_receipts": round(avg_receipts, 2),
                "expected_payments": round(total_expected_payments, 2),
                "purchase_payments": round(avg_payments, 2),
                "payroll": round(avg_payroll, 2),
                "recurring_expenses": round(avg_recurring, 2),
                "net_cashflow": round(net_cashflow, 2),
                "receipts_data_points": len(receipts_history),
                "payments_data_points": len(payments_history) + len(payroll_history) + len(recurring_history),
                "status": "actual" if is_actual else ("current" if is_current else "forecast")
            })

        # Get actual YTD data for comparison
        ytd_receipts_sql = f"""
            SELECT SUM(ABS(st_trvalue)) AS total
            FROM stran
            WHERE st_trtype = 'R' AND YEAR(st_trdate) = {current_year}
        """
        ytd_receipts = sql_connector.execute_query(ytd_receipts_sql)
        if hasattr(ytd_receipts, 'to_dict'):
            ytd_receipts = ytd_receipts.to_dict('records')
        ytd_receipts_value = float(ytd_receipts[0]['total'] or 0) if ytd_receipts else 0

        ytd_payments_sql = f"""
            SELECT SUM(ABS(pt_trvalue)) AS total
            FROM ptran
            WHERE pt_trtype = 'P' AND YEAR(pt_trdate) = {current_year}
        """
        ytd_payments = sql_connector.execute_query(ytd_payments_sql)
        if hasattr(ytd_payments, 'to_dict'):
            ytd_payments = ytd_payments.to_dict('records')
        ytd_payments_value = float(ytd_payments[0]['total'] or 0) if ytd_payments else 0

        # Get bank balances from nominal ledger using nbank to identify bank accounts
        bank_sql = """
            SELECT n.na_acnt AS account,
                   n.na_desc AS description,
                   (ISNULL(n.na_ytddr, 0) - ISNULL(n.na_ytdcr, 0)) AS balance
            FROM nacnt n WITH (NOLOCK)
            INNER JOIN nbank b WITH (NOLOCK) ON RTRIM(n.na_acnt) = RTRIM(b.nk_acnt)
            ORDER BY n.na_acnt
        """
        bank_result = sql_connector.execute_query(bank_sql)
        if hasattr(bank_result, 'to_dict'):
            bank_result = bank_result.to_dict('records')

        total_bank_balance = sum(float(b['balance'] or 0) for b in bank_result or [])

        # Total expected payments includes all categories
        total_annual_payments = annual_payments_total + annual_payroll_total + annual_recurring_total

        return {
            "success": True,
            "forecast_year": current_year,
            "years_of_history": years_history,
            "monthly_forecast": forecast,
            "summary": {
                "annual_expected_receipts": round(annual_receipts_total, 2),
                "annual_expected_payments": round(total_annual_payments, 2),
                "annual_purchase_payments": round(annual_payments_total, 2),
                "annual_payroll": round(annual_payroll_total, 2),
                "annual_recurring_expenses": round(annual_recurring_total, 2),
                "annual_expected_net": round(annual_receipts_total - total_annual_payments, 2),
                "ytd_actual_receipts": round(ytd_receipts_value, 2),
                "ytd_actual_payments": round(ytd_payments_value, 2),
                "ytd_actual_net": round(ytd_receipts_value - ytd_payments_value, 2),
                "current_bank_balance": round(total_bank_balance, 2)
            },
            "bank_accounts": bank_result or []
        }

    except Exception as e:
        logger.error(f"Cashflow forecast failed: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/cashflow/history")
async def cashflow_history():
    """
    Get detailed cashflow history by year and month.
    """
    from api.main import sql_connector
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Get all receipts by year/month
        receipts_sql = """
            SELECT
                YEAR(st_trdate) AS year,
                MONTH(st_trdate) AS month,
                COUNT(*) AS count,
                SUM(ABS(st_trvalue)) AS total
            FROM stran
            WHERE st_trtype = 'R'
            GROUP BY YEAR(st_trdate), MONTH(st_trdate)
            ORDER BY year DESC, month DESC
        """
        receipts = sql_connector.execute_query(receipts_sql)
        if hasattr(receipts, 'to_dict'):
            receipts = receipts.to_dict('records')

        # Get all payments by year/month
        payments_sql = """
            SELECT
                YEAR(pt_trdate) AS year,
                MONTH(pt_trdate) AS month,
                COUNT(*) AS count,
                SUM(ABS(pt_trvalue)) AS total
            FROM ptran
            WHERE pt_trtype = 'P'
            GROUP BY YEAR(pt_trdate), MONTH(pt_trdate)
            ORDER BY year DESC, month DESC
        """
        payments = sql_connector.execute_query(payments_sql)
        if hasattr(payments, 'to_dict'):
            payments = payments.to_dict('records')

        # Combine into yearly summary
        yearly_data = {}
        for r in receipts or []:
            year = int(r['year'])
            month = int(r['month'])
            if year not in yearly_data:
                yearly_data[year] = {"receipts": {}, "payments": {}}
            yearly_data[year]["receipts"][month] = float(r['total'] or 0)

        for p in payments or []:
            year = int(p['year'])
            month = int(p['month'])
            if year not in yearly_data:
                yearly_data[year] = {"receipts": {}, "payments": {}}
            yearly_data[year]["payments"][month] = float(p['total'] or 0)

        # Format output
        history = []
        for year in sorted(yearly_data.keys(), reverse=True):
            year_receipts = sum(yearly_data[year]["receipts"].values())
            year_payments = sum(yearly_data[year]["payments"].values())
            history.append({
                "year": year,
                "total_receipts": round(year_receipts, 2),
                "total_payments": round(year_payments, 2),
                "net_cashflow": round(year_receipts - year_payments, 2),
                "monthly_receipts": yearly_data[year]["receipts"],
                "monthly_payments": yearly_data[year]["payments"]
            })

        return {
            "success": True,
            "history": history
        }

    except Exception as e:
        logger.error(f"Cashflow history failed: {e}")
        return {"success": False, "error": str(e)}


# ============================================================
# Sales & CEO Dashboard Endpoints
# ============================================================

@router.get("/api/dashboard/available-years")
async def get_available_years():
    """Get years with transaction data and determine the best default year."""
    from api.main import sql_connector, current_company
    try:
        # Get years from nominal transactions - support both E/F codes and numeric 30/35 codes
        # Note: RTRIM needed as nt_type may have trailing spaces
        df = sql_connector.execute_query("""
            SELECT DISTINCT nt_year as year,
                   COUNT(*) as transaction_count,
                   SUM(CASE
                       WHEN RTRIM(nt_type) = 'E' THEN ABS(nt_value)
                       WHEN RTRIM(nt_type) = '30' THEN ABS(nt_value)
                       ELSE 0
                   END) as revenue
            FROM ntran
            WHERE RTRIM(nt_type) IN ('E', 'F', '30', '35')
            AND nt_year >= 2015
            GROUP BY nt_year
            ORDER BY nt_year DESC
        """)
        data = df_to_records(df)

        # Find the most recent year with meaningful data
        years_with_data = []
        latest_year_with_data = None
        for row in data:
            year = int(row['year'])
            revenue = float(row['revenue'] or 0)
            if revenue > 1000:  # Has meaningful revenue
                years_with_data.append({
                    "year": year,
                    "transaction_count": row['transaction_count'],
                    "revenue": round(revenue, 2)
                })
                if latest_year_with_data is None:
                    latest_year_with_data = year

        return {
            "success": True,
            "years": years_with_data,
            "default_year": latest_year_with_data or 2024,
            "current_company": current_company.get("name") if current_company else None
        }
    except Exception as e:
        return {"success": False, "error": str(e), "years": [], "default_year": 2024}


@router.get("/api/dashboard/sales-categories")
async def get_sales_categories():
    """Get sales categories/segments for the current company's data."""
    from api.main import sql_connector
    try:
        # Try to get categories from invoice lines with analysis codes
        df = sql_connector.execute_query("""
            SELECT
                COALESCE(NULLIF(RTRIM(it_anal), ''), 'Uncategorised') as category,
                COUNT(*) as line_count,
                SUM(it_value) / 100.0 as total_value
            FROM itran
            WHERE it_value > 0
            GROUP BY COALESCE(NULLIF(RTRIM(it_anal), ''), 'Uncategorised')
            HAVING SUM(it_value) > 0
            ORDER BY SUM(it_value) DESC
        """)
        data = df_to_records(df)

        if data:
            return {
                "success": True,
                "source": "invoice_lines",
                "categories": data
            }

        # Fallback to stock groups if no analysis codes
        df = sql_connector.execute_query("""
            SELECT
                COALESCE(sg.sg_desc, 'Other') as category,
                COUNT(*) as line_count,
                SUM(it.it_value) / 100.0 as total_value
            FROM itran it
            LEFT JOIN cname cn ON it.it_prodcode = cn.cn_prodcode
            LEFT JOIN sgroup sg ON cn.cn_catag = sg.sg_group
            WHERE it.it_value > 0
            GROUP BY COALESCE(sg.sg_desc, 'Other')
            HAVING SUM(it.it_value) > 0
            ORDER BY SUM(it.it_value) DESC
        """)
        data = df_to_records(df)

        return {
            "success": True,
            "source": "stock_groups",
            "categories": data if data else []
        }
    except Exception as e:
        return {"success": False, "error": str(e), "categories": []}


@router.get("/api/dashboard/ceo-kpis")
async def get_ceo_kpis(year: int = 2026):
    """Get CEO-level KPIs: MTD, QTD, YTD sales, growth, customer metrics."""
    from api.main import sql_connector
    try:
        from datetime import datetime as dt

        current_date = dt.now()
        current_month = current_date.month
        current_quarter = (current_month - 1) // 3 + 1

        # Get current year and previous year sales - support both E/F and 30/35 type codes
        # Note: RTRIM needed as nt_type may have trailing spaces
        df = sql_connector.execute_query(f"""
            SELECT
                nt_year,
                nt_period,
                SUM(CASE
                    WHEN RTRIM(nt_type) = 'E' THEN -nt_value
                    WHEN RTRIM(nt_type) = '30' THEN -nt_value
                    ELSE 0
                END) as revenue,
                SUM(CASE
                    WHEN RTRIM(nt_type) = 'F' THEN nt_value
                    WHEN RTRIM(nt_type) = '35' THEN nt_value
                    ELSE 0
                END) as cost_of_sales
            FROM ntran
            WHERE RTRIM(nt_type) IN ('E', 'F', '30', '35')
            AND nt_year IN ({year}, {year - 1})
            GROUP BY nt_year, nt_period
            ORDER BY nt_year, nt_period
        """)
        data = df_to_records(df)

        # Process data
        current_year_data = {}
        prev_year_data = {}
        for row in data:
            y = int(row['nt_year'])
            p = int(row['nt_period']) if row['nt_period'] else 0
            if y == year:
                current_year_data[p] = row['revenue'] or 0
            else:
                prev_year_data[p] = row['revenue'] or 0

        # Calculate MTD, QTD, YTD
        ytd = sum(current_year_data.get(p, 0) for p in range(1, current_month + 1))
        mtd = current_year_data.get(current_month, 0)

        quarter_start = (current_quarter - 1) * 3 + 1
        qtd = sum(current_year_data.get(p, 0) for p in range(quarter_start, current_month + 1))

        # Previous year comparisons
        ytd_prev = sum(prev_year_data.get(p, 0) for p in range(1, current_month + 1))
        yoy_growth = ((ytd - ytd_prev) / ytd_prev * 100) if ytd_prev != 0 else 0

        # Rolling averages (use previous year data for full year average)
        all_months = sorted(prev_year_data.keys())
        monthly_values = [prev_year_data[m] for m in all_months if m > 0]
        avg_3m = sum(monthly_values[-3:]) / min(3, len(monthly_values)) if monthly_values else 0
        avg_6m = sum(monthly_values[-6:]) / min(6, len(monthly_values)) if monthly_values else 0
        avg_12m = sum(monthly_values[-12:]) / min(12, len(monthly_values)) if monthly_values else 0

        # Active customers (had transactions this year)
        cust_df = sql_connector.execute_query(f"""
            SELECT COUNT(DISTINCT st_account) as active_customers
            FROM stran
            WHERE st_trtype = 'I' AND YEAR(st_trdate) = {year}
        """)
        cust_data = df_to_records(cust_df)
        active_customers = cust_data[0]['active_customers'] if cust_data else 0

        # Revenue per customer
        rev_per_cust = ytd / active_customers if active_customers > 0 else 0

        return {
            "success": True,
            "kpis": {
                "mtd": round(mtd, 2),
                "qtd": round(qtd, 2),
                "ytd": round(ytd, 2),
                "yoy_growth_percent": round(yoy_growth, 1),
                "avg_monthly_3m": round(avg_3m, 2),
                "avg_monthly_6m": round(avg_6m, 2),
                "avg_monthly_12m": round(avg_12m, 2),
                "active_customers": active_customers,
                "revenue_per_customer": round(rev_per_cust, 2),
                "year": year,
                "month": current_month,
                "quarter": current_quarter
            }
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.get("/api/dashboard/revenue-over-time")
async def get_revenue_over_time(year: int = 2026):
    """Get monthly revenue breakdown by category."""
    from api.main import sql_connector
    try:
        # Get monthly totals - simpler approach that works across company types
        df = sql_connector.execute_query(f"""
            SELECT
                nt_year,
                nt_period as month,
                SUM(-nt_value) as revenue
            FROM ntran
            WHERE RTRIM(nt_type) IN ('E', '30')
            AND nt_year IN ({year}, {year - 1})
            GROUP BY nt_year, nt_period
            ORDER BY nt_year, nt_period
        """)
        data = df_to_records(df)

        # Organize data by year and month
        data_by_year = {year: {}, year - 1: {}}

        for row in data:
            y = int(row['nt_year'])
            m = int(row['month']) if row['month'] else 0
            rev = row['revenue'] or 0
            data_by_year[y][m] = rev

        # Build monthly series
        months = []
        for m in range(1, 13):
            month_data = {
                "month": m,
                "month_name": ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"][m],
                "current_total": data_by_year[year].get(m, 0),
                "previous_total": data_by_year[year - 1].get(m, 0)
            }
            months.append(month_data)

        return {
            "success": True,
            "year": year,
            "months": months
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.get("/api/dashboard/revenue-composition")
async def get_revenue_composition(year: int = 2026):
    """Get revenue breakdown by category with comparison to previous year."""
    from api.main import sql_connector
    try:
        # Get revenue by nominal account description - works for all company types
        df = sql_connector.execute_query(f"""
            SELECT
                nt_year,
                COALESCE(NULLIF(RTRIM(na.na_subt), ''), 'Other') as category,
                SUM(-nt_value) as revenue
            FROM ntran nt
            LEFT JOIN nacnt na ON RTRIM(nt.nt_acnt) = RTRIM(na.na_acnt) AND na.na_year = nt.nt_year
            WHERE RTRIM(nt.nt_type) IN ('E', '30')
            AND nt.nt_year IN ({year}, {year - 1})
            GROUP BY nt.nt_year, COALESCE(NULLIF(RTRIM(na.na_subt), ''), 'Other')
            ORDER BY SUM(-nt_value) DESC
        """)
        data = df_to_records(df)

        current_year = {}
        prev_year = {}

        for row in data:
            y = int(row['nt_year'])
            cat = row['category']
            rev = row['revenue'] or 0
            if y == year:
                current_year[cat] = rev
            else:
                prev_year[cat] = rev

        # Calculate totals and percentages
        current_total = sum(current_year.values())
        prev_total = sum(prev_year.values())

        categories = []
        all_cats = set(current_year.keys()) | set(prev_year.keys())

        for cat in sorted(all_cats, key=lambda x: current_year.get(x, 0), reverse=True):
            curr = current_year.get(cat, 0)
            prev = prev_year.get(cat, 0)
            categories.append({
                "category": cat,
                "current_year": round(curr, 2),
                "previous_year": round(prev, 2),
                "current_percent": round(curr / current_total * 100, 1) if current_total else 0,
                "previous_percent": round(prev / prev_total * 100, 1) if prev_total else 0,
                "change_percent": round((curr - prev) / prev * 100, 1) if prev else 0
            })

        return {
            "success": True,
            "year": year,
            "current_total": round(current_total, 2),
            "previous_total": round(prev_total, 2),
            "categories": categories
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.get("/api/dashboard/top-customers")
async def get_top_customers(year: int = 2026, limit: int = 20):
    """Get top customers by revenue with trends."""
    from api.main import sql_connector
    try:
        df = sql_connector.execute_query(f"""
            SELECT
                RTRIM(t.st_account) as account_code,
                RTRIM(s.sn_name) as customer_name,
                SUM(CASE WHEN YEAR(t.st_trdate) = {year} THEN t.st_trvalue ELSE 0 END) as current_year,
                SUM(CASE WHEN YEAR(t.st_trdate) = {year - 1} THEN t.st_trvalue ELSE 0 END) as previous_year,
                COUNT(DISTINCT CASE WHEN YEAR(t.st_trdate) = {year} THEN t.st_trref END) as invoice_count
            FROM stran t
            INNER JOIN sname s ON RTRIM(t.st_account) = RTRIM(s.sn_account)
            WHERE t.st_trtype = 'I'
            AND YEAR(t.st_trdate) IN ({year}, {year - 1})
            GROUP BY t.st_account, s.sn_name
            HAVING SUM(CASE WHEN YEAR(t.st_trdate) = {year} THEN t.st_trvalue ELSE 0 END) > 0
            ORDER BY SUM(CASE WHEN YEAR(t.st_trdate) = {year} THEN t.st_trvalue ELSE 0 END) DESC
        """)
        data = df_to_records(df)

        total_revenue = sum(row['current_year'] or 0 for row in data)

        customers = []
        cumulative = 0
        for row in data[:limit]:
            curr = row['current_year'] or 0
            prev = row['previous_year'] or 0
            cumulative += curr

            trend = 'stable'
            if prev > 0:
                change = (curr - prev) / prev * 100
                if change > 10:
                    trend = 'up'
                elif change < -10:
                    trend = 'down'

            customers.append({
                "account_code": row['account_code'],
                "customer_name": row['customer_name'],
                "current_year": round(curr, 2),
                "previous_year": round(prev, 2),
                "percent_of_total": round(curr / total_revenue * 100, 1) if total_revenue else 0,
                "cumulative_percent": round(cumulative / total_revenue * 100, 1) if total_revenue else 0,
                "invoice_count": row['invoice_count'],
                "trend": trend,
                "change_percent": round((curr - prev) / prev * 100, 1) if prev else 0
            })

        return {
            "success": True,
            "year": year,
            "total_revenue": round(total_revenue, 2),
            "customers": customers
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.get("/api/dashboard/customer-concentration")
async def get_customer_concentration(year: int = 2026):
    """Get customer concentration analysis."""
    from api.main import sql_connector
    try:
        df = sql_connector.execute_query(f"""
            SELECT
                RTRIM(st_account) as account_code,
                SUM(st_trvalue) as revenue
            FROM stran
            WHERE st_trtype = 'I' AND YEAR(st_trdate) = {year}
            GROUP BY st_account
            ORDER BY SUM(st_trvalue) DESC
        """)
        customers = df_to_records(df)
        total_revenue = sum(c['revenue'] or 0 for c in customers)
        total_customers = len(customers)

        # Calculate concentration metrics
        top_1 = customers[0]['revenue'] if customers else 0
        top_3 = sum(c['revenue'] or 0 for c in customers[:3])
        top_5 = sum(c['revenue'] or 0 for c in customers[:5])
        top_10 = sum(c['revenue'] or 0 for c in customers[:10])

        concentration = {
            "total_customers": total_customers,
            "total_revenue": round(total_revenue, 2),
            "top_1_percent": round(top_1 / total_revenue * 100, 1) if total_revenue else 0,
            "top_3_percent": round(top_3 / total_revenue * 100, 1) if total_revenue else 0,
            "top_5_percent": round(top_5 / total_revenue * 100, 1) if total_revenue else 0,
            "top_10_percent": round(top_10 / total_revenue * 100, 1) if total_revenue else 0,
            "risk_level": "low"
        }

        # Set risk flag
        if concentration["top_5_percent"] > 50:
            concentration["risk_level"] = "high"
        elif concentration["top_5_percent"] > 40:
            concentration["risk_level"] = "medium"

        return {
            "success": True,
            "year": year,
            "concentration": concentration
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.get("/api/dashboard/customer-lifecycle")
async def get_customer_lifecycle(year: int = 2026):
    """Get customer lifecycle analysis - new, lost, by age band."""
    from api.main import sql_connector
    try:
        # Get first transaction date per customer
        df = sql_connector.execute_query(f"""
            SELECT
                RTRIM(st_account) as account_code,
                MIN(st_trdate) as first_transaction,
                MAX(st_trdate) as last_transaction,
                SUM(CASE WHEN YEAR(st_trdate) = {year} THEN st_trvalue ELSE 0 END) as current_revenue,
                SUM(CASE WHEN YEAR(st_trdate) = {year - 1} THEN st_trvalue ELSE 0 END) as prev_revenue
            FROM stran
            WHERE st_trtype = 'I'
            GROUP BY st_account
        """)
        data = df_to_records(df)

        new_customers = 0
        lost_customers = 0
        age_bands = {
            "less_than_1_year": {"count": 0, "revenue": 0},
            "1_to_3_years": {"count": 0, "revenue": 0},
            "3_to_5_years": {"count": 0, "revenue": 0},
            "over_5_years": {"count": 0, "revenue": 0}
        }

        for row in data:
            first = row['first_transaction']
            curr_rev = row['current_revenue'] or 0
            prev_rev = row['prev_revenue'] or 0

            if not first:
                continue

            # New customer (first transaction this year)
            if first.year == year:
                new_customers += 1

            # Lost/dormant (had revenue last year, none this year)
            if prev_rev > 0 and curr_rev == 0:
                lost_customers += 1

            # Age bands (for active customers)
            if curr_rev > 0:
                years_active = year - first.year
                if years_active < 1:
                    age_bands["less_than_1_year"]["count"] += 1
                    age_bands["less_than_1_year"]["revenue"] += curr_rev
                elif years_active < 3:
                    age_bands["1_to_3_years"]["count"] += 1
                    age_bands["1_to_3_years"]["revenue"] += curr_rev
                elif years_active < 5:
                    age_bands["3_to_5_years"]["count"] += 1
                    age_bands["3_to_5_years"]["revenue"] += curr_rev
                else:
                    age_bands["over_5_years"]["count"] += 1
                    age_bands["over_5_years"]["revenue"] += curr_rev

        # Round revenue figures
        for band in age_bands.values():
            band["revenue"] = round(band["revenue"], 2)

        return {
            "success": True,
            "year": year,
            "new_customers": new_customers,
            "lost_customers": lost_customers,
            "age_bands": age_bands
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.get("/api/dashboard/margin-by-category")
async def get_margin_by_category(year: int = 2026):
    """Get gross margin analysis by revenue category."""
    from api.main import sql_connector
    try:
        # Get total revenue and COS - works for all company types
        # Note: RTRIM needed as nt_type may have trailing spaces
        df = sql_connector.execute_query(f"""
            SELECT
                'Total' as category,
                SUM(CASE
                    WHEN RTRIM(nt_type) = 'E' THEN -nt_value
                    WHEN RTRIM(nt_type) = '30' THEN -nt_value
                    ELSE 0
                END) as revenue,
                SUM(CASE
                    WHEN RTRIM(nt_type) = 'F' THEN nt_value
                    WHEN RTRIM(nt_type) = '35' THEN nt_value
                    ELSE 0
                END) as cost_of_sales
            FROM ntran
            WHERE RTRIM(nt_type) IN ('E', 'F', '30', '35')
            AND nt_year = {year}
        """)
        data = df_to_records(df)

        # Consolidate by category (some categories appear twice due to E and F codes)
        categories = {}
        for row in data:
            cat = row['category']
            if cat not in categories:
                categories[cat] = {"revenue": 0, "cost_of_sales": 0}
            categories[cat]["revenue"] += row['revenue'] or 0
            categories[cat]["cost_of_sales"] += row['cost_of_sales'] or 0

        # Calculate margins
        margins = []
        for cat, data in categories.items():
            rev = data["revenue"]
            cos = data["cost_of_sales"]
            gp = rev - cos
            gp_pct = (gp / rev * 100) if rev > 0 else 0

            margins.append({
                "category": cat,
                "revenue": round(rev, 2),
                "cost_of_sales": round(cos, 2),
                "gross_profit": round(gp, 2),
                "gross_margin_percent": round(gp_pct, 1)
            })

        margins.sort(key=lambda x: x['revenue'], reverse=True)

        # Total
        total_rev = sum(m['revenue'] for m in margins)
        total_cos = sum(m['cost_of_sales'] for m in margins)
        total_gp = total_rev - total_cos

        return {
            "success": True,
            "year": year,
            "categories": margins,
            "totals": {
                "revenue": round(total_rev, 2),
                "cost_of_sales": round(total_cos, 2),
                "gross_profit": round(total_gp, 2),
                "gross_margin_percent": round(total_gp / total_rev * 100, 1) if total_rev else 0
            }
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


# ============================================================
# Finance Dashboard Endpoints
# ============================================================

@router.get("/api/dashboard/finance-summary")
async def get_finance_summary(year: int = 2024):
    """Get financial summary: P&L overview, Balance Sheet summary, Key ratios."""
    from api.main import sql_connector
    try:
        # Get P&L summary by type - using ntran for YTD values by year
        # Note: RTRIM needed as nt_type may have trailing spaces
        pl_df = sql_connector.execute_query(f"""
            SELECT
                RTRIM(nt_type) as type,
                CASE RTRIM(nt_type)
                    WHEN '30' THEN 'Sales'
                    WHEN 'E' THEN 'Sales'
                    WHEN '35' THEN 'Cost of Sales'
                    WHEN 'F' THEN 'Cost of Sales'
                    WHEN '40' THEN 'Other Income'
                    WHEN 'G' THEN 'Other Income'
                    WHEN '45' THEN 'Overheads'
                    WHEN 'H' THEN 'Overheads'
                    ELSE 'Other'
                END as type_name,
                SUM(nt_value) as ytd_movement
            FROM ntran
            WHERE RTRIM(nt_type) IN ('30', '35', '40', '45', 'E', 'F', 'G', 'H')
            AND nt_year = {year}
            GROUP BY RTRIM(nt_type)
            ORDER BY RTRIM(nt_type)
        """)
        pl_data = df_to_records(pl_df)

        # Aggregate P&L - handle both letter and number codes
        sales = sum(row['ytd_movement'] for row in pl_data if row['type'] in ('30', 'E'))
        cos = sum(row['ytd_movement'] for row in pl_data if row['type'] in ('35', 'F'))
        other_income = sum(row['ytd_movement'] for row in pl_data if row['type'] in ('40', 'G'))
        overheads = sum(row['ytd_movement'] for row in pl_data if row['type'] in ('45', 'H'))

        gross_profit = -sales - cos  # Sales are negative (credits)
        operating_profit = gross_profit + (-other_income) - overheads

        # Get Balance Sheet summary from nacnt using YTD dr/cr fields
        # Opera uses letter codes: A=Fixed Assets, B=Current Assets, C=Current Liabilities, D=Capital
        bs_df = sql_connector.execute_query(f"""
            SELECT
                RTRIM(na_type) as type,
                CASE RTRIM(na_type)
                    WHEN 'A' THEN 'Fixed Assets'
                    WHEN '05' THEN 'Fixed Assets'
                    WHEN 'B' THEN 'Current Assets'
                    WHEN '10' THEN 'Current Assets'
                    WHEN 'C' THEN 'Current Liabilities'
                    WHEN '15' THEN 'Current Liabilities'
                    WHEN 'D' THEN 'Capital & Reserves'
                    WHEN '20' THEN 'Long Term Liabilities'
                    WHEN '25' THEN 'Capital & Reserves'
                    ELSE 'Other'
                END as type_name,
                SUM(na_ytddr - na_ytdcr) as balance
            FROM nacnt
            WHERE RTRIM(na_type) IN ('A', 'B', 'C', 'D', '05', '10', '15', '20', '25')
            GROUP BY RTRIM(na_type)
            ORDER BY RTRIM(na_type)
        """)
        bs_data = df_to_records(bs_df)

        bs_summary = {}
        for row in bs_data:
            bs_summary[row['type_name']] = round(row['balance'] or 0, 2)

        # Calculate key financial figures
        fixed_assets = bs_summary.get('Fixed Assets', 0)
        current_assets = bs_summary.get('Current Assets', 0)
        current_liabilities = abs(bs_summary.get('Current Liabilities', 0))
        net_current_assets = current_assets - current_liabilities

        # Ratios
        current_ratio = current_assets / current_liabilities if current_liabilities > 0 else 0
        gross_margin = (gross_profit / -sales * 100) if sales != 0 else 0
        operating_margin = (operating_profit / -sales * 100) if sales != 0 else 0

        return {
            "success": True,
            "year": year,
            "profit_and_loss": {
                "sales": round(-sales, 2),
                "cost_of_sales": round(cos, 2),
                "gross_profit": round(gross_profit, 2),
                "other_income": round(-other_income, 2),
                "overheads": round(overheads, 2),
                "operating_profit": round(operating_profit, 2)
            },
            "balance_sheet": {
                "fixed_assets": fixed_assets,
                "current_assets": current_assets,
                "current_liabilities": current_liabilities,
                "net_current_assets": round(net_current_assets, 2),
                "total_assets": round(fixed_assets + current_assets, 2)
            },
            "ratios": {
                "gross_margin_percent": round(gross_margin, 1),
                "operating_margin_percent": round(operating_margin, 1),
                "current_ratio": round(current_ratio, 2)
            }
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.get("/api/dashboard/finance-monthly")
async def get_finance_monthly(year: int = 2024):
    """Get monthly P&L breakdown for finance view."""
    from api.main import sql_connector
    try:
        # Support both E/F/H codes and numeric 30/35/45 codes
        # Note: RTRIM needed as nt_type may have trailing spaces
        df = sql_connector.execute_query(f"""
            SELECT
                nt_period as month,
                SUM(CASE
                    WHEN RTRIM(nt_type) = 'E' THEN -nt_value
                    WHEN RTRIM(nt_type) = '30' THEN -nt_value
                    ELSE 0
                END) as revenue,
                SUM(CASE
                    WHEN RTRIM(nt_type) = 'F' THEN nt_value
                    WHEN RTRIM(nt_type) = '35' THEN nt_value
                    ELSE 0
                END) as cost_of_sales,
                SUM(CASE
                    WHEN RTRIM(nt_type) = 'H' THEN nt_value
                    WHEN RTRIM(nt_type) = '45' THEN nt_value
                    ELSE 0
                END) as overheads
            FROM ntran
            WHERE nt_year = {year}
            AND RTRIM(nt_type) IN ('E', 'F', 'H', '30', '35', '45')
            GROUP BY nt_period
            ORDER BY nt_period
        """)
        data = df_to_records(df)

        month_names = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                       'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

        months = []
        ytd_revenue = 0
        ytd_cos = 0
        ytd_overheads = 0

        for row in data:
            m = int(row['month']) if row['month'] else 0
            if m < 1 or m > 12:
                continue

            revenue = row['revenue'] or 0
            cos = row['cost_of_sales'] or 0
            overheads = row['overheads'] or 0
            gross_profit = revenue - cos
            net_profit = gross_profit - overheads

            ytd_revenue += revenue
            ytd_cos += cos
            ytd_overheads += overheads

            months.append({
                "month": m,
                "month_name": month_names[m],
                "revenue": round(revenue, 2),
                "cost_of_sales": round(cos, 2),
                "gross_profit": round(gross_profit, 2),
                "overheads": round(overheads, 2),
                "net_profit": round(net_profit, 2),
                "gross_margin_percent": round(gross_profit / revenue * 100, 1) if revenue > 0 else 0
            })

        return {
            "success": True,
            "year": year,
            "months": months,
            "ytd": {
                "revenue": round(ytd_revenue, 2),
                "cost_of_sales": round(ytd_cos, 2),
                "gross_profit": round(ytd_revenue - ytd_cos, 2),
                "overheads": round(ytd_overheads, 2),
                "net_profit": round(ytd_revenue - ytd_cos - ytd_overheads, 2)
            }
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.get("/api/dashboard/sales-by-product")
async def get_sales_by_product(year: int = 2024):
    """Get sales breakdown by product category - adapts to company data structure."""
    from api.main import sql_connector

    # Analysis code description mapping for Company Z style codes
    analysis_code_descriptions = {
        'SALE': 'Vehicle Sales',
        'ACCE': 'Accessories',
        'CONS': 'Consumables',
        'MAIN': 'Maintenance',
        'SERV': 'Services',
        'LCON': 'Lease Contracts',
        'MCON': 'Maintenance Contracts',
        'CONT': 'Contracts',
    }

    def get_category_description(code: str) -> str:
        """Get a human-readable description for an analysis code."""
        if not code:
            return 'Other'
        code = code.strip()
        # Try to match the prefix (first 4 chars)
        prefix = code[:4].upper() if len(code) >= 4 else code.upper()
        if prefix in analysis_code_descriptions:
            # Include the suffix number if present
            suffix = code[4:].strip() if len(code) > 4 else ''
            base_desc = analysis_code_descriptions[prefix]
            if suffix:
                return f"{base_desc} ({suffix})"
            return base_desc
        return code  # Return the code itself if no mapping found

    def get_nominal_descriptions() -> dict:
        """Get nominal account descriptions from database."""
        try:
            desc_df = sql_connector.execute_query("""
                SELECT RTRIM(na_acnt) as acnt, RTRIM(na_desc) as descr
                FROM nacnt
                WHERE RTRIM(na_type) = 'E' OR na_acnt LIKE 'E%'
                GROUP BY na_acnt, na_desc
            """)
            desc_data = df_to_records(desc_df)
            return {row['acnt']: row['descr'] for row in desc_data if row.get('acnt')}
        except Exception:
            return {}

    try:
        # Get nominal account descriptions (for E codes like E1010, E1020)
        nominal_descriptions = get_nominal_descriptions()

        # Try using itran with it_doc and it_lineval (common to both schemas)
        try:
            df = sql_connector.execute_query(f"""
                SELECT
                    COALESCE(NULLIF(RTRIM(it.it_anal), ''), 'Other') as category_code,
                    COUNT(DISTINCT it.it_doc) as invoice_count,
                    COUNT(*) as line_count,
                    SUM(it.it_lineval) / 100.0 as total_value
                FROM itran it
                WHERE YEAR(it.it_date) = {year}
                AND it.it_lineval > 0
                GROUP BY COALESCE(NULLIF(RTRIM(it.it_anal), ''), 'Other')
                HAVING SUM(it.it_lineval) > 0
                ORDER BY SUM(it.it_lineval) DESC
            """)
            data = df_to_records(df)
            # Add descriptions - check both Company Z mapping and nominal ledger
            for row in data:
                code = row.get('category_code', '')
                # First check if it's a Company Z style code (SALE01, ACCE02, etc.)
                if code and len(code) >= 4 and code[:4].upper() in analysis_code_descriptions:
                    row['category'] = get_category_description(code)
                # Otherwise check nominal ledger descriptions (E codes like E1010)
                elif code in nominal_descriptions:
                    row['category'] = nominal_descriptions[code]
                else:
                    row['category'] = code
        except Exception:
            data = None

        if not data:
            # Fallback: Try using it_value instead of it_lineval
            try:
                df = sql_connector.execute_query(f"""
                    SELECT
                        COALESCE(NULLIF(RTRIM(it.it_anal), ''), 'Other') as category_code,
                        COUNT(DISTINCT ih.ih_invno) as invoice_count,
                        COUNT(*) as line_count,
                        SUM(it.it_value) / 100.0 as total_value
                    FROM itran it
                    JOIN ihead ih ON it.it_invno = ih.ih_invno
                    WHERE YEAR(ih.ih_invdat) = {year}
                    AND it.it_value > 0
                    GROUP BY COALESCE(NULLIF(RTRIM(it.it_anal), ''), 'Other')
                    HAVING SUM(it.it_value) > 0
                    ORDER BY SUM(it.it_value) DESC
                """)
                data = df_to_records(df)
                # Add descriptions from nominal ledger
                for row in data:
                    code = row.get('category_code', '')
                    row['category'] = nominal_descriptions.get(code, code)
            except Exception:
                data = None

        if not data:
            # Fallback: try nominal ledger categories
            df = sql_connector.execute_query(f"""
                SELECT
                    CASE
                        WHEN nt_acnt LIKE 'E1%' THEN 'Primary Sales'
                        WHEN nt_acnt LIKE 'E2%' THEN 'Secondary Sales'
                        WHEN nt_acnt LIKE 'E3%' THEN 'Services'
                        WHEN nt_acnt LIKE 'E4%' THEN 'Other Revenue'
                        ELSE 'Miscellaneous'
                    END as category,
                    COUNT(*) as line_count,
                    SUM(-nt_value) as total_value
                FROM ntran
                WHERE RTRIM(nt_type) = 'E'
                AND nt_year = {year}
                GROUP BY CASE
                    WHEN nt_acnt LIKE 'E1%' THEN 'Primary Sales'
                    WHEN nt_acnt LIKE 'E2%' THEN 'Secondary Sales'
                    WHEN nt_acnt LIKE 'E3%' THEN 'Services'
                    WHEN nt_acnt LIKE 'E4%' THEN 'Other Revenue'
                    ELSE 'Miscellaneous'
                END
                ORDER BY SUM(-nt_value) DESC
            """)
            data = df_to_records(df)

        total_value = sum(row.get('total_value', 0) or 0 for row in data)

        categories = []
        for row in data:
            value = row.get('total_value', 0) or 0
            categories.append({
                "category": row.get('category', 'Unknown'),
                "category_code": row.get('category_code', ''),
                "invoice_count": row.get('invoice_count', 0),
                "line_count": row.get('line_count', 0),
                "value": round(value, 2),
                "percent_of_total": round(value / total_value * 100, 1) if total_value > 0 else 0
            })

        return {
            "success": True,
            "year": year,
            "categories": categories,
            "total_value": round(total_value, 2)
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


# ============================================================
# Enhanced Sales Dashboard Endpoints
# ============================================================

@router.get("/api/dashboard/executive-summary")
async def get_executive_summary(year: int = 2026):
    """
    Executive Summary KPIs - what a Sales Director should see first.
    Provides at-a-glance performance metrics with like-for-like comparisons.
    """
    from api.main import sql_connector
    from datetime import datetime as dt

    try:
        current_date = dt.now()
        current_month = current_date.month
        current_quarter = (current_month - 1) // 3 + 1

        # Get monthly revenue data for both years with type breakdown
        df = sql_connector.execute_query(f"""
            SELECT
                nt_year,
                nt_period as month,
                SUM(CASE WHEN RTRIM(nt_type) IN ('E', '30') THEN -nt_value ELSE 0 END) as revenue
            FROM ntran
            WHERE RTRIM(nt_type) IN ('E', 'F', '30', '35')
            AND nt_year IN ({year}, {year - 1}, {year - 2})
            GROUP BY nt_year, nt_period
            ORDER BY nt_year, nt_period
        """)
        data = df_to_records(df)

        # Organize by year and month
        revenue_by_year = {}
        for row in data:
            y = int(row['nt_year'])
            m = int(row['month']) if row['month'] else 0
            if y not in revenue_by_year:
                revenue_by_year[y] = {}
            revenue_by_year[y][m] = float(row['revenue'] or 0)

        # Calculate KPIs
        curr_year = revenue_by_year.get(year, {})
        prev_year = revenue_by_year.get(year - 1, {})

        # Current month vs same month last year
        curr_month_rev = curr_year.get(current_month, 0)
        prev_month_rev = prev_year.get(current_month, 0)
        month_yoy_change = ((curr_month_rev - prev_month_rev) / prev_month_rev * 100) if prev_month_rev else 0

        # Current quarter vs same quarter last year
        quarter_months = list(range((current_quarter - 1) * 3 + 1, current_quarter * 3 + 1))
        curr_qtd = sum(curr_year.get(m, 0) for m in quarter_months if m <= current_month)
        prev_qtd = sum(prev_year.get(m, 0) for m in quarter_months if m <= current_month)
        quarter_yoy_change = ((curr_qtd - prev_qtd) / prev_qtd * 100) if prev_qtd else 0

        # YTD comparison (same months as current year)
        curr_ytd = sum(curr_year.get(m, 0) for m in range(1, current_month + 1))
        prev_ytd = sum(prev_year.get(m, 0) for m in range(1, current_month + 1))
        ytd_yoy_change = ((curr_ytd - prev_ytd) / prev_ytd * 100) if prev_ytd else 0

        # Rolling 12 months (last 12 complete months)
        rolling_12 = 0
        prev_rolling_12 = 0
        for i in range(12):
            m = current_month - i
            y = year if m > 0 else year - 1
            m = m if m > 0 else m + 12
            rolling_12 += revenue_by_year.get(y, {}).get(m, 0)
            # Previous period
            py = y - 1
            prev_rolling_12 += revenue_by_year.get(py, {}).get(m, 0)

        rolling_12_change = ((rolling_12 - prev_rolling_12) / prev_rolling_12 * 100) if prev_rolling_12 else 0

        # Monthly run rate (based on last 3 months)
        recent_months = []
        for i in range(3):
            m = current_month - i
            y = year if m > 0 else year - 1
            m = m if m > 0 else m + 12
            recent_months.append(revenue_by_year.get(y, {}).get(m, 0))
        monthly_run_rate = sum(recent_months) / len(recent_months) if recent_months else 0
        annual_run_rate = monthly_run_rate * 12

        # Full year projection
        months_elapsed = current_month
        projected_full_year = (curr_ytd / months_elapsed * 12) if months_elapsed > 0 else 0
        prev_full_year = sum(prev_year.get(m, 0) for m in range(1, 13))
        projection_vs_prior = ((projected_full_year - prev_full_year) / prev_full_year * 100) if prev_full_year else 0

        return {
            "success": True,
            "year": year,
            "period": {
                "current_month": current_month,
                "current_quarter": current_quarter,
                "months_elapsed": months_elapsed
            },
            "kpis": {
                # Current month metrics
                "current_month": {
                    "value": round(curr_month_rev, 2),
                    "prior_year": round(prev_month_rev, 2),
                    "yoy_change_percent": round(month_yoy_change, 1),
                    "trend": "up" if month_yoy_change > 0 else "down" if month_yoy_change < 0 else "flat"
                },
                # Quarter metrics
                "quarter_to_date": {
                    "value": round(curr_qtd, 2),
                    "prior_year": round(prev_qtd, 2),
                    "yoy_change_percent": round(quarter_yoy_change, 1),
                    "trend": "up" if quarter_yoy_change > 0 else "down" if quarter_yoy_change < 0 else "flat"
                },
                # YTD metrics
                "year_to_date": {
                    "value": round(curr_ytd, 2),
                    "prior_year": round(prev_ytd, 2),
                    "yoy_change_percent": round(ytd_yoy_change, 1),
                    "trend": "up" if ytd_yoy_change > 0 else "down" if ytd_yoy_change < 0 else "flat"
                },
                # Rolling 12 months
                "rolling_12_months": {
                    "value": round(rolling_12, 2),
                    "prior_period": round(prev_rolling_12, 2),
                    "change_percent": round(rolling_12_change, 1),
                    "trend": "up" if rolling_12_change > 0 else "down" if rolling_12_change < 0 else "flat"
                },
                # Run rate and projections
                "monthly_run_rate": round(monthly_run_rate, 2),
                "annual_run_rate": round(annual_run_rate, 2),
                "projected_full_year": round(projected_full_year, 2),
                "prior_full_year": round(prev_full_year, 2),
                "projection_vs_prior_percent": round(projection_vs_prior, 1)
            }
        }
    except Exception as e:
        logger.error(f"Executive summary error: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/dashboard/revenue-by-category-detailed")
async def get_revenue_by_category_detailed(year: int = 2026):
    """
    Detailed revenue breakdown by sales category with YoY comparison.
    Categories: Recurring (Support/AMC), Consultancy, Cloud/Hosting, Software Resale.
    """
    from api.main import sql_connector
    try:
        # Get revenue by nominal account subtype - provides category breakdown
        df = sql_connector.execute_query(f"""
            SELECT
                nt.nt_year,
                nt.nt_period as month,
                COALESCE(NULLIF(RTRIM(na.na_subt), ''),
                    CASE
                        WHEN nt.nt_acnt LIKE 'E1%' OR nt.nt_acnt LIKE '30%' THEN 'Primary Revenue'
                        WHEN nt.nt_acnt LIKE 'E2%' THEN 'Secondary Revenue'
                        WHEN nt.nt_acnt LIKE 'E3%' THEN 'Service Revenue'
                        ELSE 'Other Revenue'
                    END
                ) as category,
                SUM(-nt.nt_value) as revenue
            FROM ntran nt
            LEFT JOIN nacnt na ON RTRIM(nt.nt_acnt) = RTRIM(na.na_acnt) AND na.na_year = nt.nt_year
            WHERE RTRIM(nt.nt_type) IN ('E', '30')
            AND nt.nt_year IN ({year}, {year - 1})
            GROUP BY nt.nt_year, nt.nt_period,
                COALESCE(NULLIF(RTRIM(na.na_subt), ''),
                    CASE
                        WHEN nt.nt_acnt LIKE 'E1%' OR nt.nt_acnt LIKE '30%' THEN 'Primary Revenue'
                        WHEN nt.nt_acnt LIKE 'E2%' THEN 'Secondary Revenue'
                        WHEN nt.nt_acnt LIKE 'E3%' THEN 'Service Revenue'
                        ELSE 'Other Revenue'
                    END
                )
            ORDER BY nt.nt_year, nt.nt_period
        """)
        data = df_to_records(df)

        # Aggregate by category and year
        category_totals = {}
        monthly_by_category = {}

        for row in data:
            y = int(row['nt_year'])
            m = int(row['month']) if row['month'] else 0
            cat = row['category']
            rev = float(row['revenue'] or 0)

            if cat not in category_totals:
                category_totals[cat] = {year: 0, year - 1: 0}
                monthly_by_category[cat] = {year: {}, year - 1: {}}

            category_totals[cat][y] = category_totals[cat].get(y, 0) + rev
            monthly_by_category[cat][y][m] = monthly_by_category[cat][y].get(m, 0) + rev

        # Build response
        categories = []
        total_current = 0
        total_previous = 0

        for cat, totals in sorted(category_totals.items(), key=lambda x: -x[1].get(year, 0)):
            curr = totals.get(year, 0)
            prev = totals.get(year - 1, 0)
            total_current += curr
            total_previous += prev

            change_pct = ((curr - prev) / prev * 100) if prev else 0

            # Monthly trend for this category
            monthly_trend = []
            for m in range(1, 13):
                monthly_trend.append({
                    "month": m,
                    "current": monthly_by_category[cat][year].get(m, 0),
                    "previous": monthly_by_category[cat][year - 1].get(m, 0)
                })

            categories.append({
                "category": cat,
                "current_year": round(curr, 2),
                "previous_year": round(prev, 2),
                "change_amount": round(curr - prev, 2),
                "change_percent": round(change_pct, 1),
                "percent_of_total": round(curr / total_current * 100, 1) if total_current else 0,
                "trend": "up" if change_pct > 5 else "down" if change_pct < -5 else "stable",
                "monthly_trend": monthly_trend
            })

        return {
            "success": True,
            "year": year,
            "summary": {
                "total_current": round(total_current, 2),
                "total_previous": round(total_previous, 2),
                "total_change_percent": round((total_current - total_previous) / total_previous * 100, 1) if total_previous else 0
            },
            "categories": categories
        }
    except Exception as e:
        logger.error(f"Revenue by category error: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/dashboard/new-vs-existing-revenue")
async def get_new_vs_existing_revenue(year: int = 2026):
    """
    Split revenue between new business (first invoice this year or last year)
    vs existing customers (invoiced in prior years).
    Critical for understanding growth sources.
    """
    from api.main import sql_connector
    try:
        # Get customer first transaction dates and revenue by year
        df = sql_connector.execute_query(f"""
            WITH CustomerHistory AS (
                SELECT
                    RTRIM(st_account) as account,
                    MIN(st_trdate) as first_invoice_date,
                    SUM(CASE WHEN YEAR(st_trdate) = {year} THEN st_trvalue ELSE 0 END) as current_year_rev,
                    SUM(CASE WHEN YEAR(st_trdate) = {year - 1} THEN st_trvalue ELSE 0 END) as prev_year_rev,
                    SUM(CASE WHEN YEAR(st_trdate) < {year - 1} THEN st_trvalue ELSE 0 END) as older_rev
                FROM stran
                WHERE st_trtype = 'I'
                GROUP BY st_account
            )
            SELECT
                account,
                first_invoice_date,
                current_year_rev,
                prev_year_rev,
                older_rev
            FROM CustomerHistory
            WHERE current_year_rev > 0 OR prev_year_rev > 0
        """)
        data = df_to_records(df)

        # Classify customers
        new_customers_current = {"count": 0, "revenue": 0}  # First invoice this year
        new_customers_prev = {"count": 0, "revenue": 0}     # First invoice last year (still "new-ish")
        existing_customers = {"count": 0, "revenue": 0}     # Invoiced before last year

        for row in data:
            first_date = row['first_invoice_date']
            curr_rev = float(row['current_year_rev'] or 0)

            if first_date:
                first_year = first_date.year if hasattr(first_date, 'year') else int(str(first_date)[:4])

                if first_year == year and curr_rev > 0:
                    new_customers_current["count"] += 1
                    new_customers_current["revenue"] += curr_rev
                elif first_year == year - 1 and curr_rev > 0:
                    new_customers_prev["count"] += 1
                    new_customers_prev["revenue"] += curr_rev
                elif curr_rev > 0:
                    existing_customers["count"] += 1
                    existing_customers["revenue"] += curr_rev

        total_revenue = new_customers_current["revenue"] + new_customers_prev["revenue"] + existing_customers["revenue"]
        total_customers = new_customers_current["count"] + new_customers_prev["count"] + existing_customers["count"]

        return {
            "success": True,
            "year": year,
            "summary": {
                "total_revenue": round(total_revenue, 2),
                "total_customers": total_customers
            },
            "new_business": {
                "this_year": {
                    "customers": new_customers_current["count"],
                    "revenue": round(new_customers_current["revenue"], 2),
                    "percent_of_total": round(new_customers_current["revenue"] / total_revenue * 100, 1) if total_revenue else 0,
                    "avg_per_customer": round(new_customers_current["revenue"] / new_customers_current["count"], 2) if new_customers_current["count"] else 0
                },
                "last_year_acquired": {
                    "customers": new_customers_prev["count"],
                    "revenue": round(new_customers_prev["revenue"], 2),
                    "percent_of_total": round(new_customers_prev["revenue"] / total_revenue * 100, 1) if total_revenue else 0,
                    "avg_per_customer": round(new_customers_prev["revenue"] / new_customers_prev["count"], 2) if new_customers_prev["count"] else 0
                }
            },
            "existing_business": {
                "customers": existing_customers["count"],
                "revenue": round(existing_customers["revenue"], 2),
                "percent_of_total": round(existing_customers["revenue"] / total_revenue * 100, 1) if total_revenue else 0,
                "avg_per_customer": round(existing_customers["revenue"] / existing_customers["count"], 2) if existing_customers["count"] else 0
            }
        }
    except Exception as e:
        logger.error(f"New vs existing revenue error: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/dashboard/customer-churn-analysis")
async def get_customer_churn_analysis(year: int = 2026):
    """
    Customer churn and retention analysis.
    Shows customers lost, at risk, and retention rate.
    """
    from api.main import sql_connector
    try:
        df = sql_connector.execute_query(f"""
            SELECT
                RTRIM(t.st_account) as account,
                RTRIM(s.sn_name) as customer_name,
                SUM(CASE WHEN YEAR(t.st_trdate) = {year} THEN t.st_trvalue ELSE 0 END) as current_year,
                SUM(CASE WHEN YEAR(t.st_trdate) = {year - 1} THEN t.st_trvalue ELSE 0 END) as prev_year,
                SUM(CASE WHEN YEAR(t.st_trdate) = {year - 2} THEN t.st_trvalue ELSE 0 END) as two_years_ago,
                MAX(t.st_trdate) as last_invoice_date
            FROM stran t
            INNER JOIN sname s ON RTRIM(t.st_account) = RTRIM(s.sn_account)
            WHERE t.st_trtype = 'I'
            AND YEAR(t.st_trdate) >= {year - 2}
            GROUP BY t.st_account, s.sn_name
        """)
        data = df_to_records(df)

        churned = []  # Had revenue last year, none this year
        at_risk = []  # Revenue dropped >50% vs last year
        growing = []  # Revenue up vs last year
        stable = []   # Revenue within +/- 20%
        declining = []  # Revenue down 20-50%

        total_churned_revenue = 0
        total_at_risk_revenue = 0

        for row in data:
            curr = float(row['current_year'] or 0)
            prev = float(row['prev_year'] or 0)

            if prev > 0 and curr == 0:
                # Churned customer
                churned.append({
                    "account": row['account'],
                    "customer_name": row['customer_name'],
                    "last_year_revenue": round(prev, 2),
                    "last_invoice": str(row['last_invoice_date'])[:10] if row['last_invoice_date'] else None
                })
                total_churned_revenue += prev
            elif prev > 0 and curr > 0:
                change_pct = (curr - prev) / prev * 100

                if change_pct < -50:
                    at_risk.append({
                        "account": row['account'],
                        "customer_name": row['customer_name'],
                        "current_revenue": round(curr, 2),
                        "previous_revenue": round(prev, 2),
                        "change_percent": round(change_pct, 1)
                    })
                    total_at_risk_revenue += prev - curr  # Revenue at risk
                elif change_pct > 20:
                    growing.append({
                        "account": row['account'],
                        "customer_name": row['customer_name'],
                        "current_revenue": round(curr, 2),
                        "previous_revenue": round(prev, 2),
                        "change_percent": round(change_pct, 1)
                    })
                elif change_pct < -20:
                    declining.append({
                        "account": row['account'],
                        "customer_name": row['customer_name'],
                        "current_revenue": round(curr, 2),
                        "previous_revenue": round(prev, 2),
                        "change_percent": round(change_pct, 1)
                    })
                else:
                    stable.append({
                        "account": row['account'],
                        "customer_name": row['customer_name'],
                        "current_revenue": round(curr, 2)
                    })

        # Sort by revenue impact
        churned.sort(key=lambda x: -x['last_year_revenue'])
        at_risk.sort(key=lambda x: x['change_percent'])
        growing.sort(key=lambda x: -x['change_percent'])

        # Calculate retention rate
        prev_year_customers = len([d for d in data if (d['prev_year'] or 0) > 0])
        retained_customers = prev_year_customers - len(churned)
        retention_rate = (retained_customers / prev_year_customers * 100) if prev_year_customers else 0

        return {
            "success": True,
            "year": year,
            "summary": {
                "retention_rate": round(retention_rate, 1),
                "churned_count": len(churned),
                "churned_revenue": round(total_churned_revenue, 2),
                "at_risk_count": len(at_risk),
                "at_risk_revenue": round(total_at_risk_revenue, 2),
                "growing_count": len(growing),
                "stable_count": len(stable),
                "declining_count": len(declining)
            },
            "churned_customers": churned[:10],  # Top 10 by revenue
            "at_risk_customers": at_risk[:10],   # Top 10 most at risk
            "growing_customers": growing[:10]    # Top 10 growing
        }
    except Exception as e:
        logger.error(f"Churn analysis error: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/dashboard/forward-indicators")
async def get_forward_indicators(year: int = 2026):
    """
    Forward-looking indicators: run rate, recurring revenue base, risk flags.
    Helps predict future performance.
    """
    from api.main import sql_connector
    from datetime import datetime as dt

    try:
        current_date = dt.now()
        current_month = current_date.month

        # Get monthly revenue for trend analysis
        df = sql_connector.execute_query(f"""
            SELECT
                nt_year,
                nt_period as month,
                SUM(CASE WHEN RTRIM(nt_type) IN ('E', '30') THEN -nt_value ELSE 0 END) as revenue
            FROM ntran
            WHERE RTRIM(nt_type) IN ('E', '30')
            AND nt_year IN ({year}, {year - 1})
            GROUP BY nt_year, nt_period
            ORDER BY nt_year, nt_period
        """)
        data = df_to_records(df)

        # Organize data
        revenue_by_month = {year: {}, year - 1: {}}
        for row in data:
            y = int(row['nt_year'])
            m = int(row['month']) if row['month'] else 0
            revenue_by_month[y][m] = float(row['revenue'] or 0)

        # Calculate various run rates
        last_3_months = sum(revenue_by_month[year].get(m, 0) for m in range(max(1, current_month - 2), current_month + 1))
        avg_3_month = last_3_months / min(3, current_month)

        last_6_months = sum(revenue_by_month[year].get(m, 0) for m in range(max(1, current_month - 5), current_month + 1))
        avg_6_month = last_6_months / min(6, current_month)

        ytd = sum(revenue_by_month[year].get(m, 0) for m in range(1, current_month + 1))
        avg_ytd = ytd / current_month if current_month > 0 else 0

        # Trend direction (comparing recent 3 months vs prior 3 months)
        recent_3 = sum(revenue_by_month[year].get(m, 0) for m in range(max(1, current_month - 2), current_month + 1))
        prior_3_start = max(1, current_month - 5)
        prior_3_end = max(1, current_month - 2)
        prior_3 = sum(revenue_by_month[year].get(m, 0) for m in range(prior_3_start, prior_3_end))

        trend_direction = "accelerating" if recent_3 > prior_3 * 1.1 else "decelerating" if recent_3 < prior_3 * 0.9 else "stable"

        # Previous year same period comparison
        prev_ytd = sum(revenue_by_month[year - 1].get(m, 0) for m in range(1, current_month + 1))
        prev_full_year = sum(revenue_by_month[year - 1].get(m, 0) for m in range(1, 13))

        # Projections
        projection_conservative = avg_ytd * 12  # Based on YTD average
        projection_optimistic = avg_3_month * 12  # Based on recent trend
        projection_midpoint = (projection_conservative + projection_optimistic) / 2

        # Risk flags
        risk_flags = []

        # Check for declining trend
        if trend_direction == "decelerating":
            risk_flags.append({
                "type": "trend",
                "severity": "medium",
                "message": "Revenue trend is decelerating vs prior quarter"
            })

        # Check for underperformance vs prior year
        if ytd < prev_ytd * 0.9:
            risk_flags.append({
                "type": "yoy_performance",
                "severity": "high" if ytd < prev_ytd * 0.8 else "medium",
                "message": f"YTD revenue {round((1 - ytd/prev_ytd) * 100, 1)}% below prior year"
            })

        # Check for projection below prior year
        if projection_midpoint < prev_full_year * 0.95:
            risk_flags.append({
                "type": "projection",
                "severity": "medium",
                "message": f"Full year projection tracking below prior year"
            })

        return {
            "success": True,
            "year": year,
            "current_month": current_month,
            "run_rates": {
                "monthly_3m_avg": round(avg_3_month, 2),
                "monthly_6m_avg": round(avg_6_month, 2),
                "monthly_ytd_avg": round(avg_ytd, 2),
                "annual_3m_basis": round(avg_3_month * 12, 2),
                "annual_6m_basis": round(avg_6_month * 12, 2),
                "annual_ytd_basis": round(avg_ytd * 12, 2)
            },
            "trend": {
                "direction": trend_direction,
                "recent_3_months": round(recent_3, 2),
                "prior_3_months": round(prior_3, 2)
            },
            "projections": {
                "conservative": round(projection_conservative, 2),
                "optimistic": round(projection_optimistic, 2),
                "midpoint": round(projection_midpoint, 2),
                "prior_year_actual": round(prev_full_year, 2),
                "vs_prior_year_percent": round((projection_midpoint - prev_full_year) / prev_full_year * 100, 1) if prev_full_year else 0
            },
            "risk_flags": risk_flags,
            "risk_level": "high" if any(f['severity'] == 'high' for f in risk_flags) else "medium" if risk_flags else "low"
        }
    except Exception as e:
        logger.error(f"Forward indicators error: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/dashboard/monthly-comparison")
async def get_monthly_comparison(year: int = 2026):
    """
    Detailed month-by-month comparison: current year vs same month last year.
    Shows seasonality patterns and identifies anomalies.
    """
    from api.main import sql_connector
    try:
        df = sql_connector.execute_query(f"""
            SELECT
                nt_year,
                nt_period as month,
                SUM(CASE WHEN RTRIM(nt_type) IN ('E', '30') THEN -nt_value ELSE 0 END) as revenue,
                SUM(CASE WHEN RTRIM(nt_type) IN ('F', '35') THEN nt_value ELSE 0 END) as cost_of_sales
            FROM ntran
            WHERE RTRIM(nt_type) IN ('E', 'F', '30', '35')
            AND nt_year IN ({year}, {year - 1}, {year - 2})
            GROUP BY nt_year, nt_period
            ORDER BY nt_year, nt_period
        """)
        data = df_to_records(df)

        # Organize by year
        by_year = {year: {}, year - 1: {}, year - 2: {}}
        for row in data:
            y = int(row['nt_year'])
            m = int(row['month']) if row['month'] else 0
            by_year[y][m] = {
                "revenue": float(row['revenue'] or 0),
                "cost_of_sales": float(row['cost_of_sales'] or 0)
            }

        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                       'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

        months = []
        ytd_current = 0
        ytd_previous = 0

        for m in range(1, 13):
            curr = by_year[year].get(m, {"revenue": 0, "cost_of_sales": 0})
            prev = by_year[year - 1].get(m, {"revenue": 0, "cost_of_sales": 0})
            two_yr = by_year[year - 2].get(m, {"revenue": 0, "cost_of_sales": 0})

            curr_rev = curr["revenue"]
            prev_rev = prev["revenue"]
            two_yr_rev = two_yr["revenue"]

            ytd_current += curr_rev
            ytd_previous += prev_rev

            yoy_change = ((curr_rev - prev_rev) / prev_rev * 100) if prev_rev else 0

            # Calculate gross margin
            curr_gp = curr_rev - curr["cost_of_sales"]
            curr_margin = (curr_gp / curr_rev * 100) if curr_rev else 0

            months.append({
                "month": m,
                "month_name": month_names[m - 1],
                "current_year": round(curr_rev, 2),
                "previous_year": round(prev_rev, 2),
                "two_years_ago": round(two_yr_rev, 2),
                "yoy_change_amount": round(curr_rev - prev_rev, 2),
                "yoy_change_percent": round(yoy_change, 1),
                "gross_profit": round(curr_gp, 2),
                "gross_margin_percent": round(curr_margin, 1),
                "ytd_current": round(ytd_current, 2),
                "ytd_previous": round(ytd_previous, 2),
                "ytd_variance": round(ytd_current - ytd_previous, 2)
            })

        return {
            "success": True,
            "year": year,
            "months": months,
            "totals": {
                "current_year": round(sum(m["current_year"] for m in months), 2),
                "previous_year": round(sum(m["previous_year"] for m in months), 2),
                "two_years_ago": round(sum(m["two_years_ago"] for m in months), 2)
            }
        }
    except Exception as e:
        logger.error(f"Monthly comparison error: {e}")
        return {"success": False, "error": str(e)}


# ============================================================
# Opera 3 (FoxPro) Dashboard Endpoints
# ============================================================

@router.get("/api/opera3/credit-control/dashboard")
async def opera3_credit_control_dashboard(data_path: str = Query(..., description="Path to Opera 3 company data folder")):
    """
    Get credit control dashboard from Opera 3 FoxPro data.
    Mirrors /api/credit-control/dashboard but reads from DBF files.
    """
    try:
        from api.main import _get_opera3_provider
        provider = _get_opera3_provider(data_path)
        metrics = provider.get_credit_control_metrics()
        priority_actions = provider.get_priority_customers(limit=10)

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "metrics": metrics,
            "priority_actions": priority_actions
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 credit control dashboard failed: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/opera3/credit-control/debtors-report")
async def opera3_debtors_report(data_path: str = Query(..., description="Path to Opera 3 company data folder")):
    """
    Get aged debtors report from Opera 3 FoxPro data.
    Mirrors /api/credit-control/debtors-report but reads from DBF files.
    """
    try:
        from api.main import _get_opera3_provider
        provider = _get_opera3_provider(data_path)
        data = provider.get_customer_aging()

        # Calculate totals
        totals = {
            "balance": sum(r.get("balance", 0) or 0 for r in data),
            "current": sum(r.get("current", 0) or 0 for r in data),
            "month_1": sum(r.get("month1", 0) or 0 for r in data),
            "month_2": sum(r.get("month2", 0) or 0 for r in data),
            "month_3_plus": sum(r.get("month3_plus", 0) or 0 for r in data),
        }

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "data": data,
            "count": len(data),
            "totals": totals
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 debtors report failed: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/opera3/nominal/trial-balance")
async def opera3_trial_balance(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    year: int = Query(2026, description="Financial year")
):
    """
    Get trial balance from Opera 3 FoxPro data.
    Mirrors /api/nominal/trial-balance but reads from DBF files.
    """
    try:
        from api.main import _get_opera3_provider
        provider = _get_opera3_provider(data_path)
        data = provider.get_nominal_trial_balance(year)

        # Calculate totals
        total_debit = sum(r.get("debit", 0) or 0 for r in data)
        total_credit = sum(r.get("credit", 0) or 0 for r in data)

        # Group by account type for summary
        type_names = {
            'A': 'Fixed Assets',
            'B': 'Current Assets',
            'C': 'Current Liabilities',
            'D': 'Capital & Reserves',
            'E': 'Sales',
            'F': 'Cost of Sales',
            'G': 'Overheads',
            'H': 'Other'
        }
        type_summary = {}
        for r in data:
            atype = (r.get("account_type") or "?").strip()
            if atype not in type_summary:
                type_summary[atype] = {
                    "name": type_names.get(atype, f"Type {atype}"),
                    "debit": 0,
                    "credit": 0,
                    "count": 0
                }
            type_summary[atype]["debit"] += r.get("debit", 0) or 0
            type_summary[atype]["credit"] += r.get("credit", 0) or 0
            type_summary[atype]["count"] += 1

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "year": year,
            "data": data,
            "count": len(data),
            "totals": {
                "debit": total_debit,
                "credit": total_credit,
                "difference": total_debit - total_credit
            },
            "by_type": type_summary
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 trial balance failed: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/opera3/dashboard/finance-summary")
async def opera3_finance_summary(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    year: int = Query(2024, description="Financial year")
):
    """
    Get financial summary from Opera 3 FoxPro data.
    Mirrors /api/dashboard/finance-summary but reads from DBF files.
    """
    try:
        from api.main import _get_opera3_provider
        provider = _get_opera3_provider(data_path)
        summary = provider.get_finance_summary(year)

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "year": year,
            **summary
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 finance summary failed: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/opera3/dashboard/finance-monthly")
async def opera3_finance_monthly(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    year: int = Query(2024, description="Financial year")
):
    """
    Get monthly P&L breakdown from Opera 3 FoxPro data.
    Mirrors /api/dashboard/finance-monthly but reads from DBF files.
    """
    try:
        from api.main import _get_opera3_provider
        provider = _get_opera3_provider(data_path)
        months = provider.get_nominal_monthly(year)

        # Calculate YTD totals
        ytd_revenue = sum(m['revenue'] for m in months)
        ytd_cos = sum(m['cost_of_sales'] for m in months)
        ytd_overheads = sum(m['overheads'] for m in months)

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "year": year,
            "months": months,
            "ytd": {
                "revenue": round(ytd_revenue, 2),
                "cost_of_sales": round(ytd_cos, 2),
                "gross_profit": round(ytd_revenue - ytd_cos, 2),
                "overheads": round(ytd_overheads, 2),
                "net_profit": round(ytd_revenue - ytd_cos - ytd_overheads, 2)
            }
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 finance monthly failed: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/opera3/dashboard/executive-summary")
async def opera3_executive_summary(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    year: int = Query(2026, description="Financial year")
):
    """
    Get executive KPIs from Opera 3 FoxPro data.
    Mirrors /api/dashboard/executive-summary but reads from DBF files.
    """
    try:
        from api.main import _get_opera3_provider
        provider = _get_opera3_provider(data_path)
        summary = provider.get_executive_summary(year)

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "year": year,
            **summary
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 executive summary failed: {e}")
        return {"success": False, "error": str(e)}
