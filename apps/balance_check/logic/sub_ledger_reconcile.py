"""Pure helper functions for sub-ledger ↔ control-account reconciliation.

Audit cross-cutting F9: the balance_check route handlers
reconcile_creditors (836 lines) and reconcile_debtors (768 lines)
share the same overall shape:

  1. Outstanding totals from the sub-ledger transaction table
     (ptran / stran).
  2. Breakdown by transaction type.
  3. Master-table totals (pname / sname).
  4. Per-account master-vs-txn variance reconciliation.
  5. Transfer-file pending entries (pnoml / snoml).
  6. Transfer-file posted/pending summary.
  7. Control-account-side YTD movement from nacnt + ntran.

They differ only in which table/field names to use. This module
extracts the shared shape into pure helpers parameterised by a
LedgerSpec.

These helpers take an SQL connector and execute READ-ONLY queries
with NOLOCK hints (consistent with CLAUDE.md locking rules for
read paths). They return plain dicts/lists so the route handler
can assemble the JSON response.

Behaviour is preserved exactly — the helpers were extracted by
copying the SQL verbatim from the original handlers, parameterising
only on the LedgerSpec field names.

Used by:
    apps/balance_check/api/routes.py::reconcile_creditors
    apps/balance_check/api/routes.py::reconcile_debtors
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, Any


@dataclass(frozen=True)
class LedgerSpec:
    """Identifies a sub-ledger triple (master table, txn table, transfer file).

    Field names are spelt out exactly as they appear in the Opera schema
    snapshot (see /Users/maccb/llmragsql/scripts/opera_snapshot.json).
    """
    # Sub-ledger transaction table (ptran / stran).
    txn_table: str
    txn_account_field: str          # pt_account / st_account
    txn_balance_field: str          # pt_trbal / st_trbal
    txn_type_field: str             # pt_trtype / st_trtype

    # Master table (pname / sname).
    master_table: str
    master_account_field: str       # pn_account / sn_account
    master_name_field: str          # pn_name / sn_name
    master_balance_field: str       # pn_currbal / sn_currbal

    # Transfer file (pnoml / snoml).
    transfer_table: str             # pnoml / snoml
    transfer_done_field: str        # px_done / sx_done


# Constants for the two real ledgers.
CREDITORS = LedgerSpec(
    txn_table='ptran',
    txn_account_field='pt_account',
    txn_balance_field='pt_trbal',
    txn_type_field='pt_trtype',
    master_table='pname',
    master_account_field='pn_account',
    master_name_field='pn_name',
    master_balance_field='pn_currbal',
    transfer_table='pnoml',
    transfer_done_field='px_done',
)

DEBTORS = LedgerSpec(
    txn_table='stran',
    txn_account_field='st_account',
    txn_balance_field='st_trbal',
    txn_type_field='st_trtype',
    master_table='sname',
    master_account_field='sn_account',
    master_name_field='sn_name',
    master_balance_field='sn_currbal',
    transfer_table='snoml',
    transfer_done_field='sx_done',
)


def _to_records(result):
    """Normalise a connector result (DataFrame or list) to list of dicts."""
    if hasattr(result, 'to_dict'):
        return result.to_dict('records')
    return result or []


def fetch_outstanding(connector, spec: LedgerSpec) -> Dict[str, Any]:
    """Total outstanding balance + count for the sub-ledger.

    Excludes orphan rows (where the account no longer exists in the
    master table) so the figure compares like-for-like with the
    master-balance check.
    """
    sql = f"""
        SELECT
            COUNT(*) AS transaction_count,
            SUM({spec.txn_balance_field}) AS total_outstanding
        FROM {spec.txn_table} WITH (NOLOCK)
        WHERE {spec.txn_balance_field} <> 0
          AND RTRIM({spec.txn_account_field}) IN (
              SELECT RTRIM({spec.master_account_field}) FROM {spec.master_table} WITH (NOLOCK)
          )
    """
    rows = _to_records(connector.execute_query(sql))
    total = float(rows[0]['total_outstanding'] or 0) if rows else 0.0
    count = int(rows[0]['transaction_count'] or 0) if rows else 0
    return {'total_outstanding': total, 'transaction_count': count}


def fetch_breakdown_by_type(connector, spec: LedgerSpec, type_descriptions: Dict[str, str]) -> List[Dict[str, Any]]:
    """Breakdown of outstanding balance by transaction type."""
    sql = f"""
        SELECT
            {spec.txn_type_field} AS type,
            COUNT(*) AS count,
            SUM({spec.txn_balance_field}) AS total
        FROM {spec.txn_table} WITH (NOLOCK)
        WHERE {spec.txn_balance_field} <> 0
          AND RTRIM({spec.txn_account_field}) IN (
              SELECT RTRIM({spec.master_account_field}) FROM {spec.master_table} WITH (NOLOCK)
          )
        GROUP BY {spec.txn_type_field}
        ORDER BY {spec.txn_type_field}
    """
    rows = _to_records(connector.execute_query(sql))
    breakdown = []
    for row in rows:
        type_code = row['type'].strip() if row['type'] else 'Unknown'
        breakdown.append({
            'type': type_code,
            'description': type_descriptions.get(type_code, type_code),
            'count': int(row['count'] or 0),
            'total': round(float(row['total'] or 0), 2),
        })
    return breakdown


def fetch_master_totals(connector, spec: LedgerSpec) -> Dict[str, Any]:
    """Total balance and count from the master table (only non-zero balances)."""
    label = 'supplier_count' if spec.master_table == 'pname' else 'customer_count'
    sql = f"""
        SELECT
            COUNT(*) AS {label},
            SUM({spec.master_balance_field}) AS total_balance
        FROM {spec.master_table} WITH (NOLOCK)
        WHERE {spec.master_balance_field} <> 0
    """
    rows = _to_records(connector.execute_query(sql))
    total = float(rows[0]['total_balance'] or 0) if rows else 0.0
    count = int(rows[0][label] or 0) if rows else 0
    return {'total_balance': total, 'count': count}


def fetch_master_txn_variance(connector, spec: LedgerSpec) -> List[Dict[str, Any]]:
    """Per-account rows where master balance disagrees with txn-table balance.

    Used to highlight specific accounts that need investigation when
    the sub-ledger ↔ control-account reconciliation breaks.
    """
    sql = f"""
        SELECT
            m.{spec.master_account_field} AS account,
            RTRIM(m.{spec.master_name_field}) AS name,
            m.{spec.master_balance_field} AS master_balance,
            COALESCE(t.txn_balance, 0) AS transaction_balance,
            m.{spec.master_balance_field} - COALESCE(t.txn_balance, 0) AS variance
        FROM {spec.master_table} m WITH (NOLOCK)
        LEFT JOIN (
            SELECT {spec.txn_account_field}, SUM({spec.txn_balance_field}) AS txn_balance
            FROM {spec.txn_table} WITH (NOLOCK)
            GROUP BY {spec.txn_account_field}
        ) t ON RTRIM(m.{spec.master_account_field}) = RTRIM(t.{spec.txn_account_field})
        WHERE ABS(m.{spec.master_balance_field} - COALESCE(t.txn_balance, 0)) >= 0.01
        ORDER BY ABS(m.{spec.master_balance_field} - COALESCE(t.txn_balance, 0)) DESC
    """
    try:
        rows = _to_records(connector.execute_query(sql))
    except Exception:
        rows = []
    out = []
    for row in rows:
        out.append({
            'account': row['account'].strip() if row['account'] else '',
            'name': row['name'] or '',
            'master_balance': round(float(row['master_balance'] or 0), 2),
            'transaction_balance': round(float(row['transaction_balance'] or 0), 2),
            'variance': round(float(row['variance'] or 0), 2),
        })
    return out


def fetch_transfer_file_pending(connector, spec: LedgerSpec) -> List[Dict[str, Any]]:
    """Transactions sitting in the transfer file (pnoml/snoml) waiting to post to NL.

    Done flag = 'Y' means already posted; anything else is pending.
    Column naming differs slightly between pnoml (px_*) and snoml
    (sx_*) so the SELECT spells the columns explicitly.
    """
    if spec.transfer_table == 'pnoml':
        sql = """
            SELECT
                px_nacnt AS nominal_account,
                px_type AS type,
                px_date AS date,
                px_value AS value,
                px_tref AS reference,
                px_comment AS comment,
                px_done AS status
            FROM pnoml WITH (NOLOCK)
            WHERE px_done <> 'Y' OR px_done IS NULL
            ORDER BY px_date DESC
        """
    else:  # snoml
        sql = """
            SELECT
                sx_nacnt AS nominal_account,
                sx_type AS type,
                sx_date AS date,
                sx_value AS value,
                sx_tref AS reference,
                sx_comment AS comment,
                sx_done AS status
            FROM snoml WITH (NOLOCK)
            WHERE sx_done <> 'Y' OR sx_done IS NULL
            ORDER BY sx_date DESC
        """
    try:
        rows = _to_records(connector.execute_query(sql))
    except Exception:
        rows = []
    out = []
    for row in rows:
        tr_date = row['date']
        if hasattr(tr_date, 'strftime'):
            tr_date = tr_date.strftime('%Y-%m-%d')
        out.append({
            'nominal_account': row['nominal_account'].strip() if row['nominal_account'] else '',
            'type': row['type'].strip() if row['type'] else '',
            'date': str(tr_date) if tr_date else '',
            'value': round(float(row['value'] or 0), 2),
            'reference': row['reference'].strip() if row['reference'] else '',
            'comment': row['comment'].strip() if row['comment'] else '',
        })
    return out


def fetch_transfer_file_summary(connector, spec: LedgerSpec) -> Dict[str, Dict[str, Any]]:
    """Posted vs pending counts/totals from the transfer file."""
    if spec.transfer_table == 'pnoml':
        sql = """
            SELECT
                CASE WHEN px_done = 'Y' THEN 'Posted' ELSE 'Pending' END AS status,
                COUNT(*) AS count,
                SUM(px_value) AS total
            FROM pnoml WITH (NOLOCK)
            GROUP BY CASE WHEN px_done = 'Y' THEN 'Posted' ELSE 'Pending' END
        """
    else:
        sql = """
            SELECT
                CASE WHEN sx_done = 'Y' THEN 'Posted' ELSE 'Pending' END AS status,
                COUNT(*) AS count,
                SUM(sx_value) AS total
            FROM snoml WITH (NOLOCK)
            GROUP BY CASE WHEN sx_done = 'Y' THEN 'Posted' ELSE 'Pending' END
        """
    try:
        rows = _to_records(connector.execute_query(sql))
    except Exception:
        rows = []
    posted = {'count': 0, 'total': 0.0}
    pending = {'count': 0, 'total': 0.0}
    for row in rows:
        if row['status'] == 'Posted':
            posted = {'count': int(row['count'] or 0), 'total': round(float(row['total'] or 0), 2)}
        else:
            pending = {'count': int(row['count'] or 0), 'total': round(float(row['total'] or 0), 2)}
    return {'posted': posted, 'pending': pending}
