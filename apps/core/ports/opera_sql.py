"""OperaSQL port — interface for executing queries against Opera SQL Server.

The local adapter wraps today's in-process SQLConnector. The HTTP
adapter (Phase B+) calls a core-opera-se gateway service. The SAM
adapter (Phase C) calls SAM's Opera SQL service.

All adapters honour per-company context — the contextvars-based
company isolation in apps/core/state.py still works through the
port.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping, Optional, Protocol, runtime_checkable


@runtime_checkable
class OperaSQLPort(Protocol):
    """Execute parameterised SQL against the active company's Opera
    database. The port is company-scoped — each call uses whatever
    company context is set in the current request.

    Implementations are responsible for:
      - Connection pooling
      - NOLOCK on read paths (per CLAUDE.md locking rules)
      - ROWLOCK on write paths
      - Per-company connector resolution (multi-tenant safety)

    The shape of returned values is a pandas DataFrame OR a list of
    dicts depending on the underlying adapter. New code should not
    assume DataFrame; use the helper `to_records()` to normalise.
    """

    def execute_query(
        self,
        sql: str,
        params: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        """Execute a SELECT (or other returning query) and return
        the result. The result type depends on the adapter — wrap
        in `to_records()` if you need plain dicts."""
        ...

    def execute_non_query(
        self,
        sql: str,
        params: Optional[Mapping[str, Any]] = None,
    ) -> int:
        """Execute INSERT/UPDATE/DELETE; return rows affected."""
        ...


def to_records(result: Any) -> list[dict[str, Any]]:
    """Normalise a query result to list[dict] regardless of adapter.

    pandas DataFrame → DataFrame.to_dict('records')
    Already a list  → returned as-is
    None            → []
    """
    if result is None:
        return []
    if hasattr(result, 'to_dict'):
        return result.to_dict('records')
    return list(result)
