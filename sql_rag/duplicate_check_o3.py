# sql_rag/duplicate_check_o3.py
"""Opera 3 (FoxPro DBF) DataSource for the duplicate-check function."""
from __future__ import annotations

from datetime import date
from typing import Any, Dict, Iterable, List, Optional


def _row_get(row: Any, *keys: str) -> Any:
    """Case-insensitive dict-or-attr lookup."""
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


def _normalise_date(v: Any) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, date) and not hasattr(v, 'hour'):
        return v
    if hasattr(v, 'date'):
        return v.date()
    return None


class Opera3DataSource:
    def __init__(self, reader: Any) -> None:
        self._reader = reader

    def find_aentry_by_signed_value(
        self,
        bank_code: str,
        date_from: date,
        date_to: date,
        signed_pence: int,
        expected_at_type: int,
        exclude_entry_numbers: Optional[List[str]],
    ) -> List[Dict[str, Any]]:
        excluded = set(exclude_entry_numbers or [])
        out: List[Dict[str, Any]] = []
        for row in self._reader.read_table('atran'):
            acnt = _row_get(row, 'at_acnt')
            if not acnt or str(acnt).strip() != bank_code:
                continue
            entry = _row_get(row, 'at_entry')
            entry_str = str(entry).strip() if entry is not None else ''
            if entry_str in excluded:
                continue
            value = _row_get(row, 'at_value')
            if value is None or abs(float(value) - signed_pence) >= 1:
                continue
            at_type = _row_get(row, 'at_type')
            if at_type is None or int(at_type) != int(expected_at_type):
                continue
            d = _normalise_date(_row_get(row, 'at_pstdate'))
            if d is None or not (date_from <= d <= date_to):
                continue
            out.append({
                'ae_entry': entry_str,
                'ae_value': value,
                'at_type': at_type,
            })
        return out

    def find_stran_by_signed_value(
        self,
        account_code: str,
        date_from: date,
        date_to: date,
        signed_pounds: float,
        st_trtype: str,
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for row in self._reader.read_table('stran'):
            acnt = _row_get(row, 'st_account')
            if not acnt or str(acnt).strip() != account_code:
                continue
            tr_type = _row_get(row, 'st_trtype')
            if tr_type is None or str(tr_type).strip() != st_trtype:
                continue
            value = _row_get(row, 'st_trvalue')
            if value is None or abs(float(value) - signed_pounds) >= 0.01:
                continue
            d = _normalise_date(_row_get(row, 'st_trdate'))
            if d is None or not (date_from <= d <= date_to):
                continue
            out.append({
                'st_trref': str(_row_get(row, 'st_trref') or '').strip(),
                'st_trvalue': value,
                'st_trtype': tr_type,
            })
        return out

    def find_ptran_by_signed_value(
        self,
        account_code: str,
        date_from: date,
        date_to: date,
        signed_pounds: float,
        pt_trtype: str,
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for row in self._reader.read_table('ptran'):
            acnt = _row_get(row, 'pt_account')
            if not acnt or str(acnt).strip() != account_code:
                continue
            tr_type = _row_get(row, 'pt_trtype')
            if tr_type is None or str(tr_type).strip() != pt_trtype:
                continue
            value = _row_get(row, 'pt_trvalue')
            if value is None or abs(float(value) - signed_pounds) >= 0.01:
                continue
            d = _normalise_date(_row_get(row, 'pt_trdate'))
            if d is None or not (date_from <= d <= date_to):
                continue
            out.append({
                'pt_trref': str(_row_get(row, 'pt_trref') or '').strip(),
                'pt_trvalue': value,
                'pt_trtype': tr_type,
            })
        return out
