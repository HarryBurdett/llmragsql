"""Local EmailSync adapter — wraps the in-process EmailSyncManager."""
from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, Optional

logger = logging.getLogger(__name__)


class LocalEmailSyncAdapter:
    """Wraps the in-process email_sync_manager singleton."""

    def is_syncing(self) -> bool:
        manager = self._resolve_manager()
        if manager is None:
            return False
        try:
            status = manager.get_sync_status()
            return any(p.get('is_syncing') for p in status.get('providers', []))
        except Exception:
            return False

    async def trigger_sync(
        self,
        *,
        force: bool = False,
        timeout_seconds: int = 60,
    ) -> dict[str, Any]:
        manager = self._resolve_manager()
        if manager is None:
            return {'started': False, 'reason': 'no_sync_manager'}
        try:
            return await manager.sync_all(force=force, timeout_seconds=timeout_seconds)
        except TypeError:
            # Older signature without these kwargs
            try:
                return await manager.sync_all()
            except Exception as e:
                return {'started': False, 'reason': f'error: {e}'}
        except Exception as e:
            return {'started': False, 'reason': f'error: {e}'}

    def subscribe(
        self,
        callback: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        manager = self._resolve_manager()
        if manager is None:
            logger.warning("No email_sync_manager — subscription dropped")
            return
        if not hasattr(manager, 'post_sync_callbacks'):
            manager.post_sync_callbacks = []
        manager.post_sync_callbacks.append(callback)

    @staticmethod
    def _resolve_manager():
        try:
            from apps.core import state
            if state.email_sync_manager is not None:
                return state.email_sync_manager
        except Exception:
            pass
        try:
            from api.main import email_sync_manager
            return email_sync_manager
        except Exception:
            return None
