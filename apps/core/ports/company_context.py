"""CompanyContext port — per-request tenant + system context.

The local adapter wraps today's contextvars-based machinery in
apps.core.state. The SAM adapter (Phase C) reads the tenant from
the SAM-issued JWT instead of looking it up via session cookie.

This port is read-only — context is set by the auth middleware
during request entry; apps just read it.
"""
from __future__ import annotations

from typing import Any, Optional, Protocol, runtime_checkable


@runtime_checkable
class CompanyContextPort(Protocol):
    """Per-request multi-tenant context."""

    def get_company_id(self) -> Optional[str]:
        """The active company ID for this request, or None if
        no company has been selected."""
        ...

    def get_company(self) -> Optional[dict[str, Any]]:
        """Full active-company dict — name, settings, address, etc.
        — or None when no active company."""
        ...

    def get_active_system_id(self) -> Optional[str]:
        """The active system ID (Opera SE / Opera 3 / etc.)."""
        ...
