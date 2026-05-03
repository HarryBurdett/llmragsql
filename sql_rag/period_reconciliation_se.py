# sql_rag/period_reconciliation_se.py
"""Opera SE DataSource for the period-reconciliation function.

Wraps SQLConnector queries against aentry to satisfy the DataSource
protocol used by sql_rag.period_reconciliation.check_period_reconciled.
"""
from __future__ import annotations

from datetime import date
from typing import Any


class OperaSEDataSource:
    """DataSource for Opera SQL SE.

    Parameters
    ----------
    sql_connector : SQLConnector-like
        Anything with an `execute_query(sql) -> DataFrame-like` method.
        We only call execute_query; pyodbc / SQLAlchemy are concerns of
        the caller.
    """

    def __init__(self, sql_connector: Any) -> None:
        self._sql = sql_connector

    def query_historical_recbals(self, bank_code: str) -> set[int]:
        """Return the set of historical reconcile-batch boundary balances
        on this bank, in pence (integer-rounded).
        """
        df = self._sql.execute_query(f"""
            SELECT DISTINCT ae_recbal
            FROM aentry WITH (NOLOCK)
            WHERE ae_acnt = '{bank_code}'
              AND ae_reclnum > 0
              AND ae_recbal IS NOT NULL
        """)
        if df is None or df.empty:
            return set()
        return {
            int(round(float(v)))
            for v in df['ae_recbal']
            if v is not None
        }

    def query_unreconciled_in_period(
        self, bank_code: str, period_start: date, period_end: date
    ) -> int:
        """Count aentry rows in the period for this bank with no reclnum."""
        df = self._sql.execute_query(f"""
            SELECT COUNT(*) AS n
            FROM aentry WITH (NOLOCK)
            WHERE ae_acnt = '{bank_code}'
              AND ae_lstdate BETWEEN '{period_start.isoformat()}' AND '{period_end.isoformat()}'
              AND (ae_reclnum IS NULL OR ae_reclnum = 0)
        """)
        if df is None or df.empty:
            return 0
        return int(df.iloc[0]['n'])
