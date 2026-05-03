# sql_rag/duplicate_check_se.py
"""Opera SQL SE DataSource for the duplicate-check function.

All queries use:
  - WITH (NOLOCK) per project locking rules.
  - Signed comparison (ABS(value - signed) < tolerance) — NOT ABS-on-ABS.
  - Explicit type filter — at_type / st_trtype / pt_trtype.
"""
from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional


class OperaSEDataSource:
    def __init__(self, sql_connector: Any) -> None:
        self._sql = sql_connector

    def find_aentry_by_signed_value(
        self,
        bank_code: str,
        date_from: date,
        date_to: date,
        signed_pence: int,
        expected_at_type: int,
        exclude_entry_numbers: Optional[List[str]],
    ) -> List[Dict[str, Any]]:
        excl_clause = ''
        if exclude_entry_numbers:
            quoted = ','.join(
                f"'{e.replace(chr(39), chr(39)+chr(39))}'"
                for e in exclude_entry_numbers
            )
            excl_clause = f" AND RTRIM(e.ae_entry) NOT IN ({quoted})"
        query = f"""
            SELECT TOP 5 a.at_entry as ae_entry, a.at_value as ae_value, a.at_type
            FROM atran a WITH (NOLOCK)
            JOIN aentry e WITH (NOLOCK)
              ON e.ae_entry = a.at_entry AND e.ae_acnt = a.at_acnt
            WHERE a.at_acnt = '{bank_code}'
            AND a.at_pstdate BETWEEN '{date_from.isoformat()}' AND '{date_to.isoformat()}'
            AND ABS(a.at_value - {signed_pence}) < 1
            AND a.at_type = {expected_at_type}
            {excl_clause}
        """
        df = self._sql.execute_query(query)
        if df is None or df.empty:
            return []
        return [
            {'ae_entry': str(row['ae_entry']).strip(),
             'ae_value': row.get('ae_value'),
             'at_type': row.get('at_type')}
            for _, row in df.iterrows()
        ]

    def find_stran_by_signed_value(
        self,
        account_code: str,
        date_from: date,
        date_to: date,
        signed_pounds: float,
        st_trtype: str,
    ) -> List[Dict[str, Any]]:
        query = f"""
            SELECT TOP 5 st_trref, st_trvalue, st_trtype
            FROM stran WITH (NOLOCK)
            WHERE RTRIM(st_account) = '{account_code}'
            AND st_trdate BETWEEN '{date_from.isoformat()}' AND '{date_to.isoformat()}'
            AND ABS(st_trvalue - {signed_pounds}) < 0.01
            AND st_trtype = '{st_trtype}'
        """
        df = self._sql.execute_query(query)
        if df is None or df.empty:
            return []
        return [
            {'st_trref': (row.get('st_trref') or '').strip(),
             'st_trvalue': row.get('st_trvalue'),
             'st_trtype': row.get('st_trtype')}
            for _, row in df.iterrows()
        ]

    def find_ptran_by_signed_value(
        self,
        account_code: str,
        date_from: date,
        date_to: date,
        signed_pounds: float,
        pt_trtype: str,
    ) -> List[Dict[str, Any]]:
        query = f"""
            SELECT TOP 5 pt_trref, pt_trvalue, pt_trtype
            FROM ptran WITH (NOLOCK)
            WHERE RTRIM(pt_account) = '{account_code}'
            AND pt_trdate BETWEEN '{date_from.isoformat()}' AND '{date_to.isoformat()}'
            AND ABS(pt_trvalue - {signed_pounds}) < 0.01
            AND pt_trtype = '{pt_trtype}'
        """
        df = self._sql.execute_query(query)
        if df is None or df.empty:
            return []
        return [
            {'pt_trref': (row.get('pt_trref') or '').strip(),
             'pt_trvalue': row.get('pt_trvalue'),
             'pt_trtype': row.get('pt_trtype')}
            for _, row in df.iterrows()
        ]
