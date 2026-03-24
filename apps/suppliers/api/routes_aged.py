"""
Aged Creditors Analysis API routes.

Provides endpoints for aged creditors reporting with aging buckets,
per-supplier drill-down, and historical trend analysis.

Works with both Opera SQL SE and Opera 3 (FoxPro) backends.
"""

import logging
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, HTTPException, Query

logger = logging.getLogger(__name__)

router = APIRouter()


# ============================================================
# Aging Bucket Helpers
# ============================================================

def _classify_aging_bucket(days_old: int) -> str:
    """Classify a number of days into an aging bucket name."""
    if days_old <= 30:
        return "current"
    elif days_old <= 60:
        return "days_30"
    elif days_old <= 90:
        return "days_60"
    elif days_old <= 120:
        return "days_90"
    else:
        return "days_120_plus"


def _empty_buckets() -> Dict[str, float]:
    """Return a zeroed aging bucket dictionary matching Opera's layout."""
    return {
        "days_90": 0.0,
        "days_60": 0.0,
        "days_30": 0.0,
        "current": 0.0,
        "total": 0.0,
        "unallocated": 0.0,
    }


def _parse_date_value(val) -> Optional[date]:
    """Parse a date value from SQL or FoxPro into a Python date."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    if isinstance(val, str):
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y"):
            try:
                return datetime.strptime(val.strip()[:10], fmt).date()
            except (ValueError, IndexError):
                continue
    return None


def _last_day_of_month(year: int, month: int) -> date:
    """Return the last day of the given month."""
    if month == 12:
        return date(year + 1, 1, 1) - timedelta(days=1)
    return date(year, month + 1, 1) - timedelta(days=1)


# ============================================================
# Opera SQL SE Endpoints
# ============================================================

@router.get("/api/creditors/aged")
async def aged_creditors_summary():
    """
    Get aged creditors summary grouped by supplier.

    Uses Opera's phist table (pi_current, pi_period1-5) for aging buckets,
    which matches Opera's own aged creditors report exactly.
    Falls back to ptran-based calculation if phist is unavailable.
    """
    from api.main import sql_connector

    if not sql_connector:
        raise HTTPException(status_code=503, detail="Database connection not available")

    try:
        # Use Opera's phist table — the authoritative source for aged balances
        query = """
            SELECT
                RTRIM(pn.pn_account) AS account,
                RTRIM(pn.pn_name) AS name,
                pn.pn_currbal AS balance,
                ISNULL(ph.pi_current, 0) AS pi_current,
                ISNULL(ph.pi_period1, 0) AS pi_period1,
                ISNULL(ph.pi_period2, 0) AS pi_period2,
                ISNULL(ph.pi_period3, 0) AS pi_period3,
                ISNULL(ph.pi_period4, 0) + ISNULL(ph.pi_period5, 0) AS pi_period45
            FROM pname pn WITH (NOLOCK)
            LEFT JOIN phist ph WITH (NOLOCK) ON pn.pn_account = ph.pi_account AND ph.pi_age = 1
            WHERE pn.pn_currbal <> 0
            ORDER BY pn.pn_account
        """

        df = sql_connector.execute_query(query)

        if df is None or len(df) == 0:
            return {
                "success": True,
                "summary": _empty_buckets(),
                "suppliers": [],
            }

        suppliers = []
        summary = _empty_buckets()

        for _, row in df.iterrows():
            account = str(row.get("account", "")).strip()
            name = str(row.get("name", "")).strip()
            current = float(row.get("pi_current", 0) or 0)
            d30 = float(row.get("pi_period1", 0) or 0)
            d60 = float(row.get("pi_period2", 0) or 0)
            d90 = float(row.get("pi_period3", 0) or 0)
            d120 = float(row.get("pi_period45", 0) or 0)
            balance = float(row.get("balance", 0) or 0)
            aging_sum = current + d30 + d60 + d90 + d120
            unallocated = round(balance - aging_sum, 2)

            suppliers.append({
                "account": account,
                "name": name,
                "days_90": round(d90, 2),
                "days_60": round(d60, 2),
                "days_30": round(d30, 2),
                "current": round(current, 2),
                "balance": round(balance, 2),
                "unallocated": unallocated,
            })

            summary["days_90"] += d90
            summary["days_60"] += d60
            summary["days_30"] += d30
            summary["current"] += current
            summary["total"] += balance
            summary["unallocated"] = summary.get("unallocated", 0) + unallocated

        # Round summary
        for k in summary:
            summary[k] = round(summary[k], 2)

        # Sort by absolute total descending
        suppliers.sort(key=lambda s: abs(s["balance"]), reverse=True)

        return {
            "success": True,
            "summary": summary,
            "suppliers": suppliers,
        }

    except Exception as e:
        logger.error(f"Error generating aged creditors summary: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/creditors/aged/trend")
async def aged_creditors_trend(
    months: int = Query(default=6, ge=1, le=24, description="Number of months of trend data"),
):
    """
    Get aged creditors trend over the specified number of months.

    For each month-end, reconstructs what the aged balances were at that point
    by looking at ptran transaction dates and balances that were outstanding
    at each month-end snapshot.

    This uses current outstanding items and their original dates to approximate
    historical aging. Transactions that have since been paid are included by
    querying all ptran records (not just outstanding) and computing balances
    as at each month-end.
    """
    from api.main import sql_connector

    if not sql_connector:
        raise HTTPException(status_code=503, detail="Database connection not available")

    try:
        today = date.today()

        # Determine month-end dates for the trend
        month_ends: List[date] = []
        current_year = today.year
        current_month = today.month

        for i in range(months):
            m = current_month - i
            y = current_year
            while m <= 0:
                m += 12
                y -= 1
            month_ends.append(_last_day_of_month(y, m))

        month_ends.reverse()

        # Get the earliest month-end to limit query scope
        earliest = month_ends[0] - timedelta(days=365)

        query = f"""
            SELECT
                p.pt_trdate,
                p.pt_trvalue,
                p.pt_trbal,
                p.pt_trtype
            FROM ptran p WITH (NOLOCK)
            WHERE p.pt_trbal != 0
              AND p.pt_trdate >= '{earliest.strftime('%Y-%m-%d')}'
        """

        df = sql_connector.execute_query(query)

        trend: List[Dict[str, Any]] = []

        for month_end in month_ends:
            buckets = _empty_buckets()

            if df is not None and len(df) > 0:
                for _, row in df.iterrows():
                    tr_date = _parse_date_value(row.get("pt_trdate"))
                    balance = float(row.get("pt_trbal", 0))

                    if tr_date is None:
                        continue

                    # Only include transactions that existed at this month-end
                    if tr_date > month_end:
                        continue

                    # For historical reconstruction, we use pt_trbal as current outstanding.
                    # If the transaction was already outstanding at that month-end, count it.
                    if balance == 0:
                        continue

                    days_old = (month_end - tr_date).days
                    if days_old < 0:
                        days_old = 0
                    bucket = _classify_aging_bucket(days_old)

                    buckets[bucket] += balance
                    buckets["total"] += balance

            for key in ("current", "days_30", "days_60", "days_90", "days_120_plus", "total"):
                buckets[key] = round(buckets[key], 2)

            trend.append({
                "month": month_end.strftime("%Y-%m"),
                **buckets,
            })

        return {
            "success": True,
            "trend": trend,
        }

    except Exception as e:
        logger.error(f"Error generating aged creditors trend: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/creditors/aged/{account}")
async def aged_creditors_detail(account: str):
    """
    Get aged creditors detail for a specific supplier.

    Returns individual outstanding invoices/credit notes/debit notes grouped
    into aging buckets with transaction-level detail.
    """
    from api.main import sql_connector

    if not sql_connector:
        raise HTTPException(status_code=503, detail="Database connection not available")

    try:
        today = date.today()

        # Get supplier name
        name_query = f"""
            SELECT RTRIM(pn_name) AS name
            FROM pname WITH (NOLOCK)
            WHERE pn_account = '{account}'
        """
        name_df = sql_connector.execute_query(name_query)
        supplier_name = ""
        if name_df is not None and len(name_df) > 0:
            supplier_name = str(name_df.iloc[0]["name"]).strip()

        if not supplier_name:
            raise HTTPException(status_code=404, detail=f"Supplier account '{account}' not found")

        # Get outstanding transactions
        query = f"""
            SELECT
                RTRIM(p.pt_trref) AS ref,
                p.pt_trdate,
                p.pt_trbal AS amount,
                p.pt_trtype
            FROM ptran p WITH (NOLOCK)
            WHERE p.pt_account = '{account}'
              AND p.pt_trbal != 0
            ORDER BY p.pt_trdate
        """

        df = sql_connector.execute_query(query)

        aging: Dict[str, List[Dict[str, Any]]] = {
            "current": [],
            "days_30": [],
            "days_60": [],
            "days_90": [],
            "days_120_plus": [],
        }
        totals = _empty_buckets()

        if df is not None and len(df) > 0:
            for _, row in df.iterrows():
                tr_date = _parse_date_value(row.get("pt_trdate"))
                amount = float(row.get("amount", 0))
                ref = str(row.get("ref", "")).strip()

                if tr_date is None:
                    continue

                days_old = (today - tr_date).days
                if days_old < 0:
                    days_old = 0
                bucket = _classify_aging_bucket(days_old)

                aging[bucket].append({
                    "ref": ref,
                    "date": tr_date.strftime("%Y-%m-%d"),
                    "amount": round(amount, 2),
                    "days_old": days_old,
                })

                totals[bucket] += amount
                totals["total"] += amount

        for key in ("current", "days_30", "days_60", "days_90", "days_120_plus", "total"):
            totals[key] = round(totals[key], 2)

        return {
            "success": True,
            "supplier": {
                "account": account,
                "name": supplier_name,
            },
            "aging": aging,
            "totals": totals,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating aged creditors detail for {account}: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


# ============================================================
# Opera 3 (FoxPro) Helpers
# ============================================================

def _o3_get_str(record, field, default=""):
    """Get string from Opera 3 record (handles uppercase/lowercase field names)."""
    val = record.get(field.upper(), record.get(field.lower(), record.get(field, default)))
    if val is None:
        return default
    return str(val).strip()


def _o3_get_num(record, field, default=0.0):
    """Get numeric from Opera 3 record (handles uppercase/lowercase field names)."""
    val = record.get(field.upper(), record.get(field.lower(), record.get(field, default)))
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _o3_parse_date(record, field) -> Optional[date]:
    """Parse a date field from an Opera 3 record into a Python date."""
    val = record.get(field.upper(), record.get(field.lower(), record.get(field)))
    return _parse_date_value(val)


# ============================================================
# Opera 3 (FoxPro) Endpoints
# ============================================================

@router.get("/api/opera3/creditors/aged")
async def opera3_aged_creditors_summary(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
):
    """
    Get aged creditors summary from Opera 3 FoxPro data.

    Reads ptran and pname from Opera 3 DBF files and groups outstanding
    balances into aging buckets per supplier.
    """
    from sql_rag.opera3_foxpro import Opera3Reader

    try:
        reader = Opera3Reader(data_path)
        today = date.today()

        # Build supplier name lookup
        pname_records = reader.read_table("pname")
        supplier_names: Dict[str, str] = {}
        for rec in pname_records:
            acct = _o3_get_str(rec, "pn_account")
            name = _o3_get_str(rec, "pn_name")
            if acct:
                supplier_names[acct.upper()] = name

        # Read outstanding ptran records
        ptran_records = reader.read_table("ptran")

        suppliers: Dict[str, Dict[str, Any]] = {}
        summary = _empty_buckets()

        for rec in ptran_records:
            trtype = _o3_get_str(rec, "pt_trtype")
            if balance == 0:
                continue

            balance = _o3_get_num(rec, "pt_trbal")
            if balance == 0:
                continue

            account = _o3_get_str(rec, "pt_account").upper()
            if not account:
                continue

            tr_date = _o3_parse_date(rec, "pt_trdate")
            if tr_date is None:
                continue

            days_old = (today - tr_date).days
            if days_old < 0:
                days_old = 0
            bucket = _classify_aging_bucket(days_old)

            name = supplier_names.get(account, account)

            if account not in suppliers:
                suppliers[account] = {
                    "account": account,
                    "name": name,
                    **_empty_buckets(),
                }

            suppliers[account][bucket] += balance
            suppliers[account]["total"] += balance
            summary[bucket] += balance
            summary["total"] += balance

        # Round all values
        for s in suppliers.values():
            for key in ("current", "days_30", "days_60", "days_90", "days_120_plus", "total"):
                s[key] = round(s[key], 2)
        for key in ("current", "days_30", "days_60", "days_90", "days_120_plus", "total"):
            summary[key] = round(summary[key], 2)

        supplier_list = sorted(suppliers.values(), key=lambda x: x["total"], reverse=True)

        return {
            "success": True,
            "summary": summary,
            "suppliers": supplier_list,
        }

    except Exception as e:
        logger.error(f"Error generating Opera 3 aged creditors summary: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/opera3/creditors/aged/trend")
async def opera3_aged_creditors_trend(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    months: int = Query(default=6, ge=1, le=24, description="Number of months of trend data"),
):
    """
    Get aged creditors trend from Opera 3 FoxPro data.

    For each month-end, reconstructs the aging profile using current outstanding
    ptran records and their original transaction dates.
    """
    from sql_rag.opera3_foxpro import Opera3Reader

    try:
        reader = Opera3Reader(data_path)
        today = date.today()

        # Determine month-end dates
        month_ends: List[date] = []
        current_year = today.year
        current_month = today.month

        for i in range(months):
            m = current_month - i
            y = current_year
            while m <= 0:
                m += 12
                y -= 1
            month_ends.append(_last_day_of_month(y, m))

        month_ends.reverse()

        earliest = month_ends[0] - timedelta(days=365)

        # Read ptran
        ptran_records = reader.read_table("ptran")

        # Filter to relevant records
        filtered_records: List[Dict[str, Any]] = []
        for rec in ptran_records:
            trtype = _o3_get_str(rec, "pt_trtype")
            if balance == 0:
                continue

            balance = _o3_get_num(rec, "pt_trbal")
            if balance == 0:
                continue

            tr_date = _o3_parse_date(rec, "pt_trdate")
            if tr_date is None:
                continue

            if tr_date < earliest:
                continue

            filtered_records.append({
                "tr_date": tr_date,
                "balance": balance,
            })

        trend: List[Dict[str, Any]] = []

        for month_end in month_ends:
            buckets = _empty_buckets()

            for rec in filtered_records:
                tr_date = rec["tr_date"]
                balance = rec["balance"]

                # Only include transactions that existed at this month-end
                if tr_date > month_end:
                    continue

                days_old = (month_end - tr_date).days
                if days_old < 0:
                    days_old = 0
                bucket = _classify_aging_bucket(days_old)

                buckets[bucket] += balance
                buckets["total"] += balance

            for key in ("current", "days_30", "days_60", "days_90", "days_120_plus", "total"):
                buckets[key] = round(buckets[key], 2)

            trend.append({
                "month": month_end.strftime("%Y-%m"),
                **buckets,
            })

        return {
            "success": True,
            "trend": trend,
        }

    except Exception as e:
        logger.error(f"Error generating Opera 3 aged creditors trend: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/opera3/creditors/aged/{account}")
async def opera3_aged_creditors_detail(
    account: str,
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
):
    """
    Get aged creditors detail for a specific supplier from Opera 3 FoxPro data.

    Returns individual outstanding invoices/credit notes/debit notes grouped
    into aging buckets with transaction-level detail.
    """
    from sql_rag.opera3_foxpro import Opera3Reader

    try:
        reader = Opera3Reader(data_path)
        today = date.today()

        # Get supplier name
        pname_records = reader.read_table("pname")
        supplier_name = ""
        for rec in pname_records:
            acct = _o3_get_str(rec, "pn_account").upper()
            if acct == account.upper():
                supplier_name = _o3_get_str(rec, "pn_name")
                break

        if not supplier_name:
            raise HTTPException(status_code=404, detail=f"Supplier account '{account}' not found")

        # Read ptran for this supplier
        ptran_records = reader.read_table("ptran")

        aging: Dict[str, List[Dict[str, Any]]] = {
            "current": [],
            "days_30": [],
            "days_60": [],
            "days_90": [],
            "days_120_plus": [],
        }
        totals = _empty_buckets()

        for rec in ptran_records:
            rec_account = _o3_get_str(rec, "pt_account").upper()
            if rec_account != account.upper():
                continue

            trtype = _o3_get_str(rec, "pt_trtype")
            if balance == 0:
                continue

            balance = _o3_get_num(rec, "pt_trbal")
            if balance == 0:
                continue

            tr_date = _o3_parse_date(rec, "pt_trdate")
            if tr_date is None:
                continue

            ref = _o3_get_str(rec, "pt_trref")
            days_old = (today - tr_date).days
            if days_old < 0:
                days_old = 0
            bucket = _classify_aging_bucket(days_old)

            aging[bucket].append({
                "ref": ref,
                "date": tr_date.strftime("%Y-%m-%d"),
                "amount": round(balance, 2),
                "days_old": days_old,
            })

            totals[bucket] += balance
            totals["total"] += balance

        for key in ("current", "days_30", "days_60", "days_90", "days_120_plus", "total"):
            totals[key] = round(totals[key], 2)

        return {
            "success": True,
            "supplier": {
                "account": account,
                "name": supplier_name,
            },
            "aging": aging,
            "totals": totals,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating Opera 3 aged creditors detail for {account}: {e}", exc_info=True)
        return {"success": False, "error": str(e)}
