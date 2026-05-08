"""Local Opera3Writer adapter — HTTP client to the Opera 3 Agent.

🆕 **Architecture update:** SAM has expanded the Opera 3 Agent to
handle both reads and writes (replacing the legacy write-only
Windows agent). The agent URL is now `OPERA3_AGENT_URL` (per-tenant,
populated by SAM).

For standalone (pre-SAM) deployments, the legacy
`OPERA3_WRITE_AGENT_URL` env var still works as a fallback for
write operations.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


class LocalOpera3WriterAdapter:
    """Wraps sql_rag.opera3_write_provider for the Write Agent HTTP API."""

    def is_available(self) -> bool:
        try:
            from sql_rag.opera3_write_provider import is_agent_available
            return bool(is_agent_available())
        except Exception as e:
            logger.warning(f"Opera 3 Write Agent availability check failed: {e}")
            return False

    def submit_posting(
        self,
        posting_payload: dict[str, Any],
        *,
        timeout_seconds: int = 60,
    ) -> dict[str, Any]:
        try:
            from sql_rag.opera3_write_provider import get_opera3_writer
            writer = get_opera3_writer()
            return writer.submit_posting(
                posting_payload, timeout_seconds=timeout_seconds,
            )
        except AttributeError:
            # Older write provider may have different method name —
            # delegate to the appropriate entry point per agent version.
            raise NotImplementedError(
                "Opera 3 writer provider does not expose submit_posting; "
                "this adapter expects a unified API."
            )
