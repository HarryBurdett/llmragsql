# sql_rag/period_reconciliation_o3.py
"""Opera 3 (FoxPro DBF) DataSource for the period-reconciliation function.

Wraps a FoxPro reader (anything with read_table(name) -> list[dict]) to
satisfy the DataSource protocol.
"""
from __future__ import annotations

from datetime import date
from typing import Any, Iterable


def _row_get(row: Any, *keys: str) -> Any:
    """Look up the first non-None value among case-insensitive variants
    of the given keys. Opera 3 DBF readers return either dict-like rows
    with uppercase or lowercase keys, depending on driver.
    """
    for k in keys:
        for variant in (k, k.upper(), k.lower()):
            if isinstance(row, dict) and variant in row:
                v = row[variant]
                if v is not None:
                    return v
            elif hasattr(row, variant):
                v = getattr(row, variant)
                if v is not None:
                    return v
    return None


class Opera3DataSource:
    """DataSource for Opera 3 (FoxPro DBF).

    Parameters
    ----------
    reader : object with `read_table(name) -> Iterable[row]`
        The Opera 3 FoxPro reader. Rows may be dicts or namedtuple-like.
    """

    def __init__(self, reader: Any) -> None:
        self._reader = reader

    def _aentry_rows(self) -> Iterable:
        return self._reader.read_table('aentry')

    def query_historical_recbals(self, bank_code: str) -> set[int]:
        out: set[int] = set()
        for row in self._aentry_rows():
            acnt = _row_get(row, 'ae_acnt')
            if not acnt or str(acnt).strip() != bank_code:
                continue
            reclnum = _row_get(row, 'ae_reclnum') or 0
            if float(reclnum) <= 0:
                continue
            recbal = _row_get(row, 'ae_recbal')
            if recbal is None:
                continue
            out.add(int(round(float(recbal))))
        return out

    def query_unreconciled_in_period(
        self, bank_code: str, period_start: date, period_end: date
    ) -> int:
        n = 0
        for row in self._aentry_rows():
            acnt = _row_get(row, 'ae_acnt')
            if not acnt or str(acnt).strip() != bank_code:
                continue
            reclnum = _row_get(row, 'ae_reclnum') or 0
            if float(reclnum) > 0:
                continue
            lstdate = _row_get(row, 'ae_lstdate')
            if lstdate is None:
                continue
            # Normalise to date
            if hasattr(lstdate, 'date'):
                lstdate = lstdate.date()
            if period_start <= lstdate <= period_end:
                n += 1
        return n
