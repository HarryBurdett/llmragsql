"""
Cashflow Forecast API routes — read-only forward cashflow view against
Opera SE. Mirrors the SAM plugin at apps-sam/cashflow/.

The forecast combines four signals:
  1. Current bank position (nbank.nk_curbal, foreign currencies excluded)
  2. Outstanding debtors (stran, due date = st_dueday if set on the invoice,
     otherwise st_trdate + DEFAULT_TERMS_DAYS [30])
  3. Outstanding creditors (ptran, due date = pt_dueday if set, otherwise
     pt_trdate + DEFAULT_TERMS_DAYS [30])
  4. Recurring entries (arhead + arline projected forward via ae_nxtpost + ae_freq)
  5. Historical 12-month averages for months ≥ 3 ahead (avoids double-counting)

NOTE: Opera stores customer/supplier terms in separate `sterms`/`pterms`
tables, not on sname/pname directly. We use st_dueday/pt_dueday — which
Opera populates from terms at invoice time — rather than re-joining.

Read-only against Opera. NOLOCK on every read.
"""
import logging
from datetime import datetime, date, timedelta
from typing import Optional, Dict, List, Tuple

from fastapi import APIRouter, Query

logger = logging.getLogger(__name__)
router = APIRouter()


def _pad2(n: int) -> str:
    return f"{n:02d}"


def _ym_key(year: int, month: int) -> str:
    return f"{year}-{_pad2(month)}"


_MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
           'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']


def _month_label(year: int, month: int) -> str:
    return f"{_MONTHS[month - 1]} {year}"


def _parse_iso_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    s = str(s).strip()[:10]
    try:
        return datetime.strptime(s, '%Y-%m-%d').date()
    except (ValueError, TypeError):
        return None


def _add_months(d: date, n: int) -> date:
    m = d.month - 1 + n
    y = d.year + m // 12
    m = (m % 12) + 1
    return date(y, m, 1)


def _add_days(d: date, n: int) -> date:
    return d + timedelta(days=n)


def _diff_months(frm: date, to: date) -> int:
    return (to.year - frm.year) * 12 + (to.month - frm.month)


def _recurring_occurrences(first: date, freq: str, every: int,
                           remaining: int, horizon: date) -> List[date]:
    """Generate occurrences within horizon, capped by remaining."""
    step = max(1, int(every or 1))
    out: List[date] = []
    cursor = first
    count = 0
    for _ in range(365 * 2):  # safety
        if cursor > horizon:
            break
        if remaining > 0 and count >= remaining:
            break
        out.append(cursor)
        count += 1
        f = (freq or 'M').upper()
        if f == 'D':
            cursor = _add_days(cursor, step)
        elif f == 'W':
            cursor = _add_days(cursor, 7 * step)
        elif f == 'M':
            cursor = _add_months(cursor, step)
        elif f == 'Q':
            cursor = _add_months(cursor, 3 * step)
        elif f in ('A', 'Y'):
            cursor = _add_months(cursor, 12 * step)
        else:
            cursor = _add_months(cursor, step)
    return out


@router.get("/api/cashflow-forecast")
async def cashflow_forecast(
    as_of_date: Optional[str] = Query(None, description="YYYY-MM-DD (defaults to today)"),
    months: int = Query(12, ge=1, le=24, description="Months ahead (1..24)"),
):
    """
    Forward cashflow forecast — current position + 12-month projection.

    Returns:
      {
        success: bool,
        as_of_date: str,
        current_position: { bank_total, bank_accounts, debtors_outstanding,
                            creditors_outstanding, net_working_capital },
        monthly_forecast: [ { month, label, expected_receipts, expected_payments,
                              net_cashflow, running_balance, sources } ],
        totals: { total_receipts, total_payments, net_position,
                  opening_balance, closing_balance, lowest_balance,
                  lowest_balance_month },
        assumptions: [ str ],
      }
    """
    from apps.core.adapters.factory import get_opera_sql
    sql_connector = get_opera_sql()
    if not sql_connector:
        return {"success": False, "error": "No Opera database connection"}

    as_of = _parse_iso_date(as_of_date) or date.today()
    months_ahead = max(1, min(24, int(months)))
    horizon = _add_months(as_of, months_ahead)
    assumptions: List[str] = []

    # ----------------------------------------------------------------
    # 1. Current bank position (nbank, exclude foreign currency)
    # ----------------------------------------------------------------
    bank_accounts = []
    bank_total = 0.0
    try:
        bank_rows = sql_connector.execute_query("""
            SELECT
                RTRIM(nk_acnt) AS code,
                RTRIM(ISNULL(nk_desc, '')) AS description,
                ISNULL(nk_curbal, 0) AS balance_pence,
                RTRIM(ISNULL(nk_fcurr, '')) AS fcurr
            FROM nbank WITH (NOLOCK)
            ORDER BY nk_acnt
        """)
        if hasattr(bank_rows, 'to_dict'):
            bank_rows = bank_rows.to_dict('records')
        for r in bank_rows or []:
            if str(r.get('fcurr', '')).strip():
                continue  # skip foreign currency
            balance = float(r.get('balance_pence') or 0) / 100.0
            bank_accounts.append({
                "code": str(r.get('code', '')).strip(),
                "description": str(r.get('description', '')).strip(),
                "balance": round(balance, 2),
            })
            bank_total += balance
    except Exception as e:
        logger.error(f"Bank position read failed: {e}")
        return {"success": False, "error": f"Bank position read failed: {e}"}

    # ----------------------------------------------------------------
    # 2. Outstanding debtors — bucket by expected receipt date.
    #    Opera stores customer terms in a separate `sterms` table
    #    rather than on sname directly. We use st_dueday when set
    #    (Opera populates it from terms at invoice time); otherwise
    #    default to st_trdate + 30 days.
    # ----------------------------------------------------------------
    commitments_in: Dict[str, float] = {}
    debtors_outstanding = 0.0
    DEFAULT_TERMS_DAYS = 30
    try:
        rows = sql_connector.execute_query("""
            SELECT
                RTRIM(st.st_account) AS account,
                st.st_trdate,
                st.st_dueday,
                ISNULL(st.st_trbal, 0) AS trbal
            FROM stran st WITH (NOLOCK)
            LEFT JOIN sname s WITH (NOLOCK)
                ON RTRIM(st.st_account) = RTRIM(s.sn_account)
            WHERE st.st_trtype = 'I'
              AND st.st_trbal > 0
              AND ISNULL(s.sn_dormant, 0) = 0
        """)
        if hasattr(rows, 'to_dict'):
            rows = rows.to_dict('records')
        for r in rows or []:
            trbal = float(r.get('trbal') or 0)
            if trbal <= 0:
                continue
            debtors_outstanding += trbal
            tr_date = _parse_iso_date(str(r.get('st_trdate'))) if r.get('st_trdate') else None
            due_date = _parse_iso_date(str(r.get('st_dueday'))) if r.get('st_dueday') else None
            expected = due_date or (tr_date + timedelta(days=DEFAULT_TERMS_DAYS) if tr_date else None)
            if not expected:
                continue
            if expected < as_of:
                key = _ym_key(as_of.year, as_of.month)
            elif expected > horizon:
                continue
            else:
                key = _ym_key(expected.year, expected.month)
            commitments_in[key] = commitments_in.get(key, 0.0) + trbal
    except Exception as e:
        assumptions.append(f"Debtors lookup failed ({e}) — falling back to historical averages only")

    # ----------------------------------------------------------------
    # 3. Outstanding creditors — bucket by expected payment date
    # ----------------------------------------------------------------
    commitments_out: Dict[str, float] = {}
    creditors_outstanding = 0.0
    try:
        rows = sql_connector.execute_query("""
            SELECT
                RTRIM(pt.pt_account) AS account,
                pt.pt_trdate,
                pt.pt_dueday,
                ISNULL(pt.pt_trbal, 0) AS trbal
            FROM ptran pt WITH (NOLOCK)
            LEFT JOIN pname p WITH (NOLOCK)
                ON RTRIM(pt.pt_account) = RTRIM(p.pn_account)
            WHERE pt.pt_trtype = 'I'
              AND pt.pt_trbal > 0
              AND ISNULL(p.pn_dormant, 0) = 0
        """)
        if hasattr(rows, 'to_dict'):
            rows = rows.to_dict('records')
        for r in rows or []:
            trbal = float(r.get('trbal') or 0)
            if trbal <= 0:
                continue
            creditors_outstanding += trbal
            tr_date = _parse_iso_date(str(r.get('pt_trdate'))) if r.get('pt_trdate') else None
            due_date = _parse_iso_date(str(r.get('pt_dueday'))) if r.get('pt_dueday') else None
            expected = due_date or (tr_date + timedelta(days=DEFAULT_TERMS_DAYS) if tr_date else None)
            if not expected:
                continue
            if expected < as_of:
                key = _ym_key(as_of.year, as_of.month)
            elif expected > horizon:
                continue
            else:
                key = _ym_key(expected.year, expected.month)
            commitments_out[key] = commitments_out.get(key, 0.0) + trbal
    except Exception as e:
        assumptions.append(f"Creditors lookup failed ({e}) — falling back to historical averages only")

    # ----------------------------------------------------------------
    # 4. Recurring entries (arhead + arline)
    # ----------------------------------------------------------------
    recurring_in: Dict[str, float] = {}
    recurring_out: Dict[str, float] = {}
    try:
        rows = sql_connector.execute_query("""
            SELECT
                RTRIM(ae.ae_entry) AS entry,
                ae.ae_nxtpost,
                RTRIM(ISNULL(ae.ae_freq, 'M')) AS freq,
                ISNULL(ae.ae_every, 1) AS every_n,
                ISNULL(ae.ae_posted, 0) AS posted,
                ISNULL(ae.ae_topost, 0) AS topost,
                ISNULL(at.at_value, 0) AS value_pence
            FROM arhead ae WITH (NOLOCK)
            LEFT JOIN arline at WITH (NOLOCK)
                ON RTRIM(ae.ae_entry) = RTRIM(at.at_entry)
            WHERE ae.ae_nxtpost IS NOT NULL
        """)
        if hasattr(rows, 'to_dict'):
            rows = rows.to_dict('records')
        for r in rows or []:
            first = _parse_iso_date(str(r.get('ae_nxtpost'))) if r.get('ae_nxtpost') else None
            if not first:
                continue
            remaining = max(0, int(r.get('topost') or 0) - int(r.get('posted') or 0))
            amount = float(r.get('value_pence') or 0) / 100.0
            if amount == 0:
                continue
            for occ in _recurring_occurrences(
                first, str(r.get('freq') or 'M'), int(r.get('every_n') or 1),
                remaining, horizon
            ):
                if occ < as_of:
                    continue
                key = _ym_key(occ.year, occ.month)
                if amount >= 0:
                    recurring_in[key] = recurring_in.get(key, 0.0) + amount
                else:
                    recurring_out[key] = recurring_out.get(key, 0.0) + abs(amount)
    except Exception as e:
        assumptions.append(f"Recurring entries lookup failed ({e}) — recurring postings excluded from forecast")

    # ----------------------------------------------------------------
    # 5. Historical averages by calendar month
    # ----------------------------------------------------------------
    hist_receipts: Dict[int, float] = {}
    hist_payments: Dict[int, float] = {}
    try:
        rows = sql_connector.execute_query("""
            SELECT MONTH(st_trdate) AS m,
                   SUM(ABS(ISNULL(st_trvalue, 0))) AS total
            FROM stran WITH (NOLOCK)
            WHERE st_trtype = 'R'
              AND st_trdate >= DATEADD(YEAR, -1, GETDATE())
            GROUP BY MONTH(st_trdate)
        """)
        if hasattr(rows, 'to_dict'):
            rows = rows.to_dict('records')
        for r in rows or []:
            m = int(r.get('m') or 0)
            if 1 <= m <= 12:
                hist_receipts[m] = float(r.get('total') or 0)
    except Exception as e:
        assumptions.append(f"Historical receipts lookup failed ({e})")
    try:
        rows = sql_connector.execute_query("""
            SELECT MONTH(pt_trdate) AS m,
                   SUM(ABS(ISNULL(pt_trvalue, 0))) AS total
            FROM ptran WITH (NOLOCK)
            WHERE pt_trtype = 'P'
              AND pt_trdate >= DATEADD(YEAR, -1, GETDATE())
            GROUP BY MONTH(pt_trdate)
        """)
        if hasattr(rows, 'to_dict'):
            rows = rows.to_dict('records')
        for r in rows or []:
            m = int(r.get('m') or 0)
            if 1 <= m <= 12:
                hist_payments[m] = float(r.get('total') or 0)
    except Exception as e:
        assumptions.append(f"Historical payments lookup failed ({e})")

    # ----------------------------------------------------------------
    # 6. Walk forward month-by-month
    # ----------------------------------------------------------------
    monthly: List[Dict] = []
    running_balance = bank_total
    total_receipts = 0.0
    total_payments = 0.0
    lowest_balance = running_balance
    lowest_month: Optional[str] = None

    start = date(as_of.year, as_of.month, 1)
    for i in range(months_ahead):
        cursor = _add_months(start, i)
        y, m = cursor.year, cursor.month
        key = _ym_key(y, m)

        c_in = commitments_in.get(key, 0.0)
        c_out = commitments_out.get(key, 0.0)
        r_in = recurring_in.get(key, 0.0)
        r_out = recurring_out.get(key, 0.0)

        # Historicals only for months ≥ 3 ahead to avoid double-counting
        months_ahead_i = _diff_months(as_of, cursor)
        use_history = months_ahead_i >= 2
        h_in = hist_receipts.get(m, 0.0) if use_history else 0.0
        h_out = hist_payments.get(m, 0.0) if use_history else 0.0

        receipts = c_in + r_in + h_in
        payments = c_out + r_out + h_out
        net = receipts - payments
        running_balance += net
        total_receipts += receipts
        total_payments += payments
        if running_balance < lowest_balance:
            lowest_balance = running_balance
            lowest_month = _month_label(y, m)

        monthly.append({
            "month": key,
            "label": _month_label(y, m),
            "expected_receipts": round(receipts, 2),
            "expected_payments": round(payments, 2),
            "net_cashflow": round(net, 2),
            "running_balance": round(running_balance, 2),
            "sources": {
                "commitments_in": round(c_in, 2),
                "commitments_out": round(c_out, 2),
                "recurring_in": round(r_in, 2),
                "recurring_out": round(r_out, 2),
                "historical_in": round(h_in, 2),
                "historical_out": round(h_out, 2),
            }
        })

    standing = [
        'Months 1–2 use known commitments (outstanding invoices) + scheduled recurring entries.',
        'Months 3+ blend known commitments with 12-month historical averages by calendar month.',
        'Expected payment dates use st_dueday/pt_dueday when set on the invoice; otherwise trdate + 30 days.',
        'Foreign-currency bank accounts are excluded from the opening balance.',
    ]
    assumptions = standing + assumptions

    return {
        "success": True,
        "as_of_date": as_of.strftime('%Y-%m-%d'),
        "current_position": {
            "bank_total": round(bank_total, 2),
            "bank_accounts": bank_accounts,
            "debtors_outstanding": round(debtors_outstanding, 2),
            "creditors_outstanding": round(creditors_outstanding, 2),
            "net_working_capital": round(
                bank_total + debtors_outstanding - creditors_outstanding, 2
            ),
        },
        "monthly_forecast": monthly,
        "totals": {
            "total_receipts": round(total_receipts, 2),
            "total_payments": round(total_payments, 2),
            "net_position": round(total_receipts - total_payments, 2),
            "opening_balance": round(bank_total, 2),
            "closing_balance": round(running_balance, 2),
            "lowest_balance": round(lowest_balance, 2),
            "lowest_balance_month": lowest_month,
        },
        "assumptions": assumptions,
    }
