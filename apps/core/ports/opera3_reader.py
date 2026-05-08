"""Opera3Reader port — read access to Opera 3 via the SAM-hosted Agent.

🆕 **Architecture update:** the Opera 3 Agent has been expanded by SAM
to handle BOTH reads and writes. Our containers no longer access DBF
files directly via SMB / file system — they call SAM's Agent over HTTP
for every read.

Used when `OPERA_VERSION=3`. The agent's URL comes from
`OPERA3_AGENT_URL` (per-tenant; SAM populates it).

Backwards compatibility: standalone (pre-SAM) deployments that read
DBFs directly continue to work via `LocalOpera3ReaderAdapter` reading
from `OPERA3_DATA_PATH`. SAM-hosted deployments use the HTTP client
adapter that talks to SAM's Opera 3 Agent.

WRITES go through `Opera3WriterPort` against the same agent.
"""
from __future__ import annotations

from typing import Any, Iterable, Optional, Protocol, runtime_checkable


@runtime_checkable
class Opera3ReaderPort(Protocol):
    """Read DBF tables from Opera 3.

    Implementations:
      - `RemoteOpera3ReaderAdapter` (preferred, post-SAM) — HTTP
        client to SAM's Opera 3 Agent
      - `LocalOpera3ReaderAdapter` (legacy / standalone) — direct
        DBF read via the file system / SMB
    """

    def read_table(
        self,
        table_name: str,
        *,
        where: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """Read records from a DBF table.

        Args:
            table_name: e.g. 'nbank', 'pname', 'sname', 'ntran'
            where: optional simple filter expression (provider-
                specific syntax — keep it simple: equality only)
            limit: cap rows returned

        Returns plain list of dicts — adapters convert FoxPro
        record types to Python primitives.
        """
        ...

    def get_company_info(self) -> dict[str, Any]:
        """Return the active company's metadata (name, address,
        config flags)."""
        ...
