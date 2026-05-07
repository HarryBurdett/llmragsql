"""Opera3Writer port — write access to Opera 3 via the Windows Write Agent.

⚠️ The Write Agent stays as-is per the SAM-readiness directive.
This port wraps the HTTP client that talks to the existing Windows
service — both today's local deployment and SAM-hosted future
deployments use the same Write Agent.

The agent handles FoxPro write locking correctly (it must run on
Windows for that). Our containers, regardless of OS, call it over
HTTP.
"""
from __future__ import annotations

from typing import Any, Optional, Protocol, runtime_checkable


@runtime_checkable
class Opera3WriterPort(Protocol):
    """Submit a write to Opera 3 via the Windows Write Agent.

    Today and tomorrow this is HTTP; the underlying agent stays
    Windows-native. The port is small (one method) because the
    agent's API surface is intentionally minimal — it executes a
    posting transaction (insert+update set) atomically.
    """

    def is_available(self) -> bool:
        """True if the Write Agent is reachable. Used by route
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
        is defined by the agent's API — see opera3_agent/service.py.

        Returns the agent's response dict (success, error, IDs of
        rows created, etc.). Raises on non-2xx.
        """
        ...
