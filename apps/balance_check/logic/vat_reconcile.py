"""Pure helpers for the VAT reconciliation route handler.

Audit cross-cutting F9: reconcile_vat in
apps/balance_check/api/routes.py is 661 lines. The natural seams:

  1. Quarter detection (most-recent zvtran or nvat date → fiscal Q)
  2. VAT-codes-with-rates fetch (ztax + date-based rate selection)
  3. VAT-by-code aggregation, repeated four times:
       - zvtran uncommitted output (va_done=0, va_vattype='S')
       - zvtran uncommitted input  (va_done=0, va_vattype='P')
       - nvat   committed   output (nv_vattype='S')
       - nvat   committed   input  (nv_vattype='P')
  4. NL movement summary across VAT accounts (ntran + nacnt desc)
  5. Variance computation + reporting

This module extracts (2)-(4). Phase (1) is already
get_vat_quarter_dates in apps.balance_check.logic (or wherever it
lives), and (5) is small and stays inline.
"""
from __future__ import annotations

from datetime import date as _date
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple


def _to_records(result):
    if hasattr(result, 'to_dict'):
        return result.to_dict('records')
    return result or []


# ====================================================================
# Phase 2 — VAT codes + applicable rate
# ====================================================================


@dataclass
class VatCodesResult:
    vat_codes: List[Dict[str, Any]]
    output_nominal_accounts: Set[str] = field(default_factory=set)
    input_nominal_accounts: Set[str] = field(default_factory=set)


def _pick_applicable_rate(rate1, rate2, date1, date2, ref_date):
    """Choose the most recent effective rate <= ref_date.

    Mirrors the original handler's logic at lines ~2406-2416 of
    apps/balance_check/api/routes.py.
    """
    if date1 and date2:
        if date2 <= ref_date and date1 <= ref_date:
            return rate2 if date2 > date1 else rate1
        if date2 <= ref_date:
            return rate2
        if date1 <= ref_date:
            return rate1
        return rate1  # neither effective yet → fall back to rate1
    if date2 and date2 <= ref_date:
        return rate2
    return rate1


def _coerce_rate_date(d):
    """Convert FoxPro/SQL Server date-or-NaT-or-None into a date or None."""
    try:
        if d is not None and d == d:  # NaT/NaN check (NaN != NaN)
            if hasattr(d, 'date'):
                return d.date()
            return d
    except (TypeError, ValueError):
        pass
    return None


def fetch_vat_codes_with_rates(
    connector,
    ref_date: _date,
) -> VatCodesResult:
    """Read ztax (Home country VAT codes) and compute the applicable
    rate for `ref_date`.

    Returns a VatCodesResult with:
      - vat_codes:  list of {code, description, rate, type, nominal_account}
                     suitable for direct JSON serialisation
      - output_nominal_accounts: NL accounts behind 'S' (sales/output) codes
      - input_nominal_accounts:  NL accounts behind 'P' (purchase/input)
    """
    sql = """
        SELECT tx_code, tx_desc, tx_rate1, tx_rate1dy, tx_rate2, tx_rate2dy, tx_trantyp, tx_nominal
        FROM ztax WITH (NOLOCK)
        WHERE tx_ctrytyp = 'H'
        ORDER BY tx_trantyp, tx_code
    """
    rows = _to_records(connector.execute_query(sql))

    vat_codes: List[Dict[str, Any]] = []
    output_nominals: Set[str] = set()
    input_nominals: Set[str] = set()

    for row in rows:
        code = row['tx_code'].strip() if row['tx_code'] else ''
        nominal = row['tx_nominal'].strip() if row['tx_nominal'] else ''
        vat_type = row['tx_trantyp'].strip() if row['tx_trantyp'] else ''

        rate1 = float(row['tx_rate1'] or 0)
        rate2 = float(row['tx_rate2'] or 0)
        date1 = _coerce_rate_date(row.get('tx_rate1dy'))
        date2 = _coerce_rate_date(row.get('tx_rate2dy'))

        applicable_rate = _pick_applicable_rate(rate1, rate2, date1, date2, ref_date)

        vat_codes.append({
            "code": code,
            "description": row['tx_desc'].strip() if row['tx_desc'] else '',
            "rate": applicable_rate,
            "type": vat_type,
            "nominal_account": nominal,
        })

        if nominal:
            if vat_type == 'S':
                output_nominals.add(nominal)
            elif vat_type == 'P':
                input_nominals.add(nominal)

    return VatCodesResult(
        vat_codes=vat_codes,
        output_nominal_accounts=output_nominals,
        input_nominal_accounts=input_nominals,
    )


# ====================================================================
# Phase 3 — VAT-by-code aggregation (zvtran uncommitted | nvat committed)
# ====================================================================


@dataclass
class VatAggregate:
    total_vat: float
    by_code: List[Dict[str, Any]]


def fetch_zvtran_aggregate(
    connector,
    *,
    vattype: str,        # 'S' or 'P'
    quarter_start: str,
    quarter_end: str,
    include_net: bool = True,
) -> VatAggregate:
    """Aggregate uncommitted (va_done=0) VAT transactions from zvtran.

    Mirrors the SQL at lines ~2441-2454 (output) and ~2472-2485
    (input) of the original handler.
    """
    sql = f"""
        SELECT
            va_anvat AS vat_code,
            COUNT(*) AS transaction_count,
            SUM(va_vatval) AS vat_amount,
            SUM(va_trvalue) AS net_amount
        FROM zvtran WITH (NOLOCK)
        WHERE va_vattype = '{vattype}'
          AND va_done = 0
          AND va_taxdate >= '{quarter_start}'
          AND va_taxdate <= '{quarter_end}'
        GROUP BY va_anvat
        ORDER BY va_anvat
    """
    rows = _to_records(connector.execute_query(sql))

    total = 0.0
    by_code: List[Dict[str, Any]] = []
    for row in rows:
        vat_amount = float(row['vat_amount'] or 0)
        total += vat_amount
        item = {
            "vat_code": row['vat_code'].strip() if row['vat_code'] else '',
            "transaction_count": int(row['transaction_count'] or 0),
            "vat_amount": round(vat_amount, 2),
        }
        if include_net:
            item["net_amount"] = round(float(row['net_amount'] or 0), 2)
        by_code.append(item)

    return VatAggregate(total_vat=total, by_code=by_code)


def fetch_nvat_aggregate(
    connector,
    *,
    vattype: str,        # 'S' or 'P'
    period_start: str,
    period_end: str,
) -> VatAggregate:
    """Aggregate committed VAT transactions from nvat for a date range.

    Mirrors the SQL at lines ~2598-2609 (output) and ~2633-2644
    (input) of the original handler.
    """
    sql = f"""
        SELECT
            nv_vatcode AS vat_code,
            COUNT(*) AS transaction_count,
            SUM(nv_vatval) AS vat_amount
        FROM nvat WITH (NOLOCK)
        WHERE nv_vattype = '{vattype}'
          AND nv_date >= '{period_start}'
          AND nv_date <= '{period_end}'
        GROUP BY nv_vatcode
        ORDER BY nv_vatcode
    """
    rows = _to_records(connector.execute_query(sql))

    total = 0.0
    by_code: List[Dict[str, Any]] = []
    for row in rows:
        vat_amount = float(row['vat_amount'] or 0)
        total += vat_amount
        by_code.append({
            "vat_code": row['vat_code'].strip() if row['vat_code'] else '',
            "transaction_count": int(row['transaction_count'] or 0),
            "vat_amount": round(vat_amount, 2),
        })

    return VatAggregate(total_vat=total, by_code=by_code)


# ====================================================================
# Phase 4 — per-account NL movement summary
# ====================================================================


@dataclass
class NlMovementResult:
    accounts: List[Dict[str, Any]]
    output_total: float
    input_total: float


def fetch_nl_vat_movements(
    connector,
    *,
    output_nominal_accounts: Set[str],
    input_nominal_accounts: Set[str],
    period_start: str,
    period_end: str,
) -> NlMovementResult:
    """For each VAT-related NL account, fetch ntran movement and
    summarise. Mirrors lines ~2526-2580 of the original handler.

    For Output VAT (sales) the credit total feeds the liability;
    for Input VAT (purchases) the debit total represents reclaimable.
    """
    all_accounts = output_nominal_accounts.union(input_nominal_accounts)
    movements: List[Dict[str, Any]] = []
    output_total = 0.0
    input_total = 0.0

    for acnt in all_accounts:
        ntran_sql = f"""
            SELECT
                SUM(CASE WHEN nt_value > 0 THEN nt_value ELSE 0 END) AS debits,
                SUM(CASE WHEN nt_value < 0 THEN ABS(nt_value) ELSE 0 END) AS credits,
                SUM(nt_value) AS net,
                COUNT(*) AS transaction_count
            FROM ntran WITH (NOLOCK)
            WHERE nt_acnt = '{acnt}'
              AND nt_entr >= '{period_start}'
              AND nt_entr <= '{period_end}'
        """
        rows = _to_records(connector.execute_query(ntran_sql))
        if not rows or not rows[0]:
            continue

        row = rows[0]
        debits = float(row['debits'] or 0)
        credits = float(row['credits'] or 0)
        net = float(row['net'] or 0)
        txn_count = int(row['transaction_count'] or 0)

        if txn_count <= 0:
            continue

        is_output = acnt in output_nominal_accounts
        is_input = acnt in input_nominal_accounts

        # Description from nacnt
        nacnt_sql = f"SELECT RTRIM(na_desc) AS description FROM nacnt WITH (NOLOCK) WHERE na_acnt = '{acnt}'"
        desc_rows = _to_records(connector.execute_query(nacnt_sql))
        description = desc_rows[0]['description'] if desc_rows else ''

        movements.append({
            "account": acnt,
            "description": description,
            "type": "Output" if is_output else ("Input" if is_input else "Mixed"),
            "debits": round(debits, 2),
            "credits": round(credits, 2),
            "net": round(net, 2),
            "transaction_count": txn_count,
        })

        if is_output:
            output_total += credits
        if is_input:
            input_total += debits

    return NlMovementResult(
        accounts=movements,
        output_total=output_total,
        input_total=input_total,
    )
