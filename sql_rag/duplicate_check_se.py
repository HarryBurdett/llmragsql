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

from sql_rag.opera_open_items import OPEN_FOR_REC_SQL


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
            AND e.{OPEN_FOR_REC_SQL.replace('AND ', 'AND e.')}
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

    def read_nbank(self, bank_code: str):
        """Read the four nbank fields the bank-rec self-heal rule needs.

        Returns NbankSnapshot or None if the bank does not exist in
        nbank. WITH (NOLOCK) per business-rules/locking-protocol.md.
        Pence → pounds conversion is done in SQL (nk_recbal / 100.0).

        Spec: docs/superpowers/specs/2026-05-05-bank-rec-self-heal-design.md
        """
        from sql_rag.bank_rec_heal import NbankSnapshot

        query = f"""
            SELECT nk_recbal / 100.0 AS recbal_pounds,
                   nk_lststdt        AS lststdt,
                   nk_lststno        AS lststno
            FROM nbank WITH (NOLOCK)
            WHERE RTRIM(nk_acnt) = '{bank_code}'
        """
        df = self._sql.execute_query(query)
        if df is None or df.empty:
            return None
        row = df.iloc[0]
        recbal = row.get('recbal_pounds')
        lststdt = row.get('lststdt')
        lststno = row.get('lststno')
        if lststdt is not None and hasattr(lststdt, 'date'):
            try:
                lststdt = lststdt.date()
            except Exception:
                pass
        return NbankSnapshot(
            bank_code=bank_code,
            recbal_pounds=float(recbal) if recbal is not None else None,
            lststdt=lststdt,
            lststno=int(lststno) if lststno is not None else None,
        )

    def count_reconciled_aentry(
        self,
        bank_code: str,
        statement_number: int,
    ) -> int:
        """Count aentry rows reconciled in the given statement number for this bank.

        Used by the bank-rec self-heal to populate reconciled_count when
        flipping is_reconciled=0 → 1 for rows that have a stored
        statement_number. WITH (NOLOCK) per locking protocol.
        """
        query = f"""
            SELECT COUNT(*) AS cnt
            FROM aentry WITH (NOLOCK)
            WHERE RTRIM(ae_acnt) = '{bank_code}'
              AND ae_frstat = {int(statement_number)}
              AND ae_reclnum > 0
        """
        df = self._sql.execute_query(query)
        if df is None or df.empty:
            return 0
        cnt = df.iloc[0].get('cnt', 0)
        return int(cnt) if cnt is not None else 0

    def count_reconciled_aentry_in_period(
        self,
        bank_code: str,
        period_start: date,
        period_end: date,
    ) -> int:
        """Date-based fallback for the bank-rec self-heal: count aentry
        rows on this bank whose ae_recdate falls in [period_start,
        period_end] and ae_reclnum>0.

        Used for legacy bank_statement_imports rows where
        statement_number is NULL — same answer as the by-statement-number
        count, derived from the period dates instead.
        """
        query = f"""
            SELECT COUNT(*) AS cnt
            FROM aentry WITH (NOLOCK)
            WHERE RTRIM(ae_acnt) = '{bank_code}'
              AND ae_recdate BETWEEN '{period_start.isoformat()}' AND '{period_end.isoformat()}'
              AND ae_reclnum > 0
        """
        df = self._sql.execute_query(query)
        if df is None or df.empty:
            return 0
        cnt = df.iloc[0].get('cnt', 0)
        return int(cnt) if cnt is not None else 0
