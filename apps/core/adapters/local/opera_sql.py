"""Local OperaSQL adapter — wraps the in-process SQL connector.

Phase B local adapter. Resolves the active company's SQLConnector
through the existing apps.core.state machinery and forwards calls.
Behaviour is identical to today's `from api.main import sql_connector;
sql_connector.execute_query(...)`.

When the apps move to per-container deployment (Phase B-final) or
SAM (Phase C), this adapter is replaced by an HTTP client. Apps
keep using the same OperaSQLPort interface either way.
"""
from __future__ import annotations

import logging
from typing import Any, Mapping, Optional

logger = logging.getLogger(__name__)


class LocalOperaSQLAdapter:
    """In-process Opera SQL adapter.

    Resolves the per-company connector lazily on each call (so
    company switches mid-request work correctly). The underlying
    SQLConnector handles pooling + locking hints + per-company
    isolation.

    `bool(adapter)` is True iff an underlying connector is currently
    resolvable. This preserves the legacy `if not sql_connector:`
    pattern that route handlers use to fail fast when no company
    is active.
    """

    def __bool__(self) -> bool:
        return self._resolve_connector() is not None

    def execute_query(
        self,
        sql: str,
        params: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        connector = self._resolve_connector()
        if connector is None:
            raise RuntimeError(
                "No SQL connector available — likely no active "
                "company context. Set the request company before "
                "calling this port."
            )
        return connector.execute_query(sql, dict(params) if params else None)

    def execute_non_query(
        self,
        sql: str,
        params: Optional[Mapping[str, Any]] = None,
    ) -> int:
        connector = self._resolve_connector()
        if connector is None:
            raise RuntimeError(
                "No SQL connector available — likely no active "
                "company context."
            )
        # SQLConnector.execute_query handles both — non-query
        # returns rowcount-equivalent. Forward the call.
        result = connector.execute_query(
            sql, dict(params) if params else None
        )
        # The legacy connector returns a DataFrame even for non-
        # queries (or None). Normalise to int row-count.
        if result is None:
            return 0
        if hasattr(result, '__len__'):
            return len(result)
        return int(result) if isinstance(result, (int, float)) else 0

    @staticmethod
    def _resolve_connector():
        """Get the per-company connector via apps.core.state.

        Falls back to api.main.sql_connector for backwards compat
        with code paths that haven't fully migrated to per-company
        connectors yet.
        """
        try:
            from apps.core import state
            if state.sql_connector is not None:
                return state.sql_connector
        except Exception:
            pass

        try:
            from api.main import sql_connector
            return sql_connector
        except Exception:
            return None
