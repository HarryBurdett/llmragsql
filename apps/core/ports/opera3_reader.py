"""Opera3Reader port — read-only access to FoxPro DBF files.

Used when OPERA_VERSION=3. The local adapter wraps Opera3Reader
which reads DBF files directly via the file system (or SMB mount).
The HTTP adapter calls a core-opera3 gateway service that owns the
file-system access.

WRITES go through Opera3WriterPort, which talks to the existing
Windows Write Agent — not through this port.
"""
from __future__ import annotations

from typing import Any, Iterable, Optional, Protocol, runtime_checkable


@runtime_checkable
class Opera3ReaderPort(Protocol):
    """Read DBF tables from Opera 3.

    All adapters are read-only. Writes go through Opera3WriterPort
    (which calls the Windows Write Agent).
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
