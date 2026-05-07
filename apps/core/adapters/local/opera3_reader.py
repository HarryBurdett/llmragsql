"""Local Opera3Reader adapter — wraps Opera3Reader for in-process use."""
from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


class LocalOpera3ReaderAdapter:
    """Wraps sql_rag.opera3_foxpro.Opera3Reader.

    Construction uses the OPERA3_DATA_PATH env var. Each instance
    holds one path; per-company switching builds a new adapter.
    """

    def __init__(self, data_path: Optional[str] = None):
        from apps.core.env_config import env_str
        self._data_path = data_path or env_str('OPERA3_DATA_PATH', '')
        self._reader = None

    def _ensure_reader(self):
        if self._reader is not None:
            return self._reader
        if not self._data_path:
            raise RuntimeError(
                "OPERA3_DATA_PATH is not set; cannot read Opera 3 DBFs"
            )
        from sql_rag.opera3_foxpro import Opera3Reader
        self._reader = Opera3Reader(self._data_path)
        return self._reader

    def read_table(
        self,
        table_name: str,
        *,
        where: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        reader = self._ensure_reader()
        records = reader.read_table(table_name)
        if where:
            # Simple equality filter — provider expects 'col=value'
            try:
                col, _, val = where.partition('=')
                col = col.strip()
                val = val.strip().strip("'\"")
                records = [r for r in records if str(r.get(col, '')).strip() == val]
            except Exception:
                logger.warning(f"Could not apply where filter {where!r}; ignoring")
        if limit is not None:
            records = records[:limit]
        return records

    def get_company_info(self) -> dict[str, Any]:
        reader = self._ensure_reader()
        # Opera 3 stores company info in cdef.dbf; reader exposes
        # this differently across versions. Defer to read_table.
        try:
            rows = reader.read_table('cdef')
            return rows[0] if rows else {}
        except Exception as e:
            logger.warning(f"Could not read Opera 3 company info: {e}")
            return {}
