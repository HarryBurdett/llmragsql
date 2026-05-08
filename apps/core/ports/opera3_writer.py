"""Opera3Writer port — write access to Opera 3 via the SAM-hosted Agent.

🆕 **Architecture update:** the Opera 3 Agent has been expanded by SAM
to handle BOTH reads and writes, replacing the original write-only
Windows-deployed agent. Our containers now call SAM's Agent over HTTP
for every Opera 3 operation; no SMB mount or direct DBF access is
required.

Reads go through `Opera3ReaderPort` (also HTTP to SAM's Agent).
Writes go through this port. Both adapters point at the same agent
URL — `OPERA3_AGENT_URL` (per-tenant; SAM populates it).

Backwards compatibility: standalone deployments using the legacy
customer-deployed Windows Write Agent continue to work via the
`OPERA3_WRITE_AGENT_URL` env var. The local adapter prefers
`OPERA3_AGENT_URL` when set (SAM mode), falling back to
`OPERA3_WRITE_AGENT_URL` (legacy mode).

The agent (whether SAM-hosted or legacy customer-deployed) handles
FoxPro write locking correctly — that's the reason it's a separate
service rather than direct file access from our containers.
"""
from __future__ import annotations

from typing import Any, Optional, Protocol, runtime_checkable


@runtime_checkable
class Opera3WriterPort(Protocol):
    """Submit a write to Opera 3 via the Agent.

    The agent's API surface is intentionally minimal — it executes a
    posting transaction (insert+update set) atomically. This port
    wraps the HTTP client.
    """

    def is_available(self) -> bool:
        """True if the Opera 3 Agent is reachable. Used by route
        handlers to fail fast with a friendly error instead of
        hanging on the agent's HTTP call."""
        ...

    def submit_posting(
        self,
        posting_payload: dict[str, Any],
        *,
        timeout_seconds: int = 60,
    ) -> dict[str, Any]:
        """Submit a posting payload to the agent. The payload shape
        is defined by the agent's API — see opera3_agent/service.py
        for the legacy schema; SAM's expanded agent accepts the
        same shape plus tenant-context headers from the JWT.

        Returns the agent's response dict (success, error, IDs of
        rows created, etc.). Raises on non-2xx.
        """
        ...
