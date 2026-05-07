"""EmailSync port — IMAP poller control + post-sync hooks.

Apps don't poll IMAP directly. core-email's IMAP poller runs in
its own container (Phase B) or in-process (Phase A monolith) and
exposes:

  - is_syncing()          status check
  - trigger_sync()        force a sync now (rate-limited)
  - subscribe(callback)   register a post-sync callback

The post-sync callback mechanism lets apps react to "new emails
arrived" — e.g. suppliers app re-runs the bank-detail-change scan
after each sync (per audit F1 fix).
"""
from __future__ import annotations

from typing import Any, Awaitable, Callable, Optional, Protocol, runtime_checkable


@runtime_checkable
class EmailSyncPort(Protocol):
    """IMAP poller control + post-sync subscription."""

    def is_syncing(self) -> bool:
        """True if a sync is currently running."""
        ...

    async def trigger_sync(
        self,
        *,
        force: bool = False,
        timeout_seconds: int = 60,
    ) -> dict[str, Any]:
        """Trigger a sync for the active company. If a sync ran
        recently (default cooldown 5 min), returns immediately
        unless `force=True`.

        Returns {'started': bool, 'reason': str, ...}.
        """
        ...

    def subscribe(
        self,
        callback: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        """Register a coroutine to run after each sync completes.
        The callback receives the sync result dict.

        Used by suppliers app for periodic_bank_detail_scan
        (audit F1).
        """
        ...
