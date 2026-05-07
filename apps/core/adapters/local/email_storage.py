"""Local EmailStorage adapter — wraps the in-process EmailStorage.

Resolves the per-company EmailStorage instance via apps.core.state
and forwards calls. Identical behaviour to today's `from api.main
import email_storage`.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

logger = logging.getLogger(__name__)


class LocalEmailStorageAdapter:
    """In-process email storage adapter.

    `bool(adapter)` is True iff an underlying storage is resolvable.
    Preserves the legacy `if not email_storage:` pattern that route
    handlers use to fail fast when storage hasn't been initialised.
    """

    def __bool__(self) -> bool:
        return self._resolve_storage() is not None

    def get_emails(
        self,
        from_date: Optional[datetime] = None,
        page: int = 1,
        page_size: int = 100,
        **filters: Any,
    ) -> dict[str, Any]:
        storage = self._resolve_storage()
        if storage is None:
            return {'emails': [], 'total': 0}
        return storage.get_emails(
            from_date=from_date,
            page=page,
            page_size=page_size,
            **filters,
        )

    def get_email_by_id(self, email_id: int) -> Optional[dict[str, Any]]:
        storage = self._resolve_storage()
        if storage is None:
            return None
        return storage.get_email_by_id(email_id)

    def record_bank_statement_import(
        self,
        *,
        bank_code: str,
        filename: str,
        transactions_imported: int,
        source: str,
        target_system: str,
        email_id: Optional[int] = None,
        attachment_id: Optional[str] = None,
        total_receipts: float = 0,
        total_payments: float = 0,
        imported_by: str = '',
        **extra: Any,
    ) -> int:
        storage = self._resolve_storage()
        if storage is None:
            raise RuntimeError("Email storage unavailable")
        return storage.record_bank_statement_import(
            bank_code=bank_code,
            filename=filename,
            transactions_imported=transactions_imported,
            source=source,
            target_system=target_system,
            email_id=email_id,
            attachment_id=attachment_id,
            total_receipts=total_receipts,
            total_payments=total_payments,
            imported_by=imported_by,
            **extra,
        )

    def get_reconciled_statement_keys(
        self, bank_code: Optional[str] = None,
    ) -> set[tuple[Any, ...]]:
        storage = self._resolve_storage()
        if storage is None:
            return set()
        try:
            if bank_code is not None:
                return storage.get_reconciled_statement_keys(bank_code)
            return storage.get_reconciled_statement_keys()
        except TypeError:
            # Older signature without bank_code parameter
            return storage.get_reconciled_statement_keys()

    def get_reconciled_filenames(self) -> set[str]:
        storage = self._resolve_storage()
        if storage is None:
            return set()
        return storage.get_reconciled_filenames()

    @staticmethod
    def _resolve_storage():
        """Get per-company EmailStorage via apps.core.state.

        Falls back to api.main.email_storage for legacy compat.
        """
        try:
            from apps.core import state
            if state.email_storage is not None:
                return state.email_storage
        except Exception:
            pass

        try:
            from api.main import email_storage
            return email_storage
        except Exception:
            return None
