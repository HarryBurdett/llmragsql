"""EmailStorage port — interface for the shared email + attachment store.

The local adapter wraps the in-process EmailStorage SQLite. The HTTP
adapter calls the core-email service for cross-container access.

Used by bank-reconcile (statement scan), gocardless (payout scan),
suppliers (statement scan).
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional, Protocol, runtime_checkable


@runtime_checkable
class EmailStoragePort(Protocol):
    """Read-only email metadata + attachment access from the
    application's perspective. Apps don't write emails — the IMAP
    poller does.

    All operations are company-scoped (the underlying storage uses
    per-company SQLite files).
    """

    def get_emails(
        self,
        from_date: Optional[datetime] = None,
        page: int = 1,
        page_size: int = 100,
        **filters: Any,
    ) -> dict[str, Any]:
        """Page through emails. Returns {'emails': [...], 'total': N}."""
        ...

    def get_email_by_id(self, email_id: int) -> Optional[dict[str, Any]]:
        """Return one email's full record including attachments
        list, or None."""
        ...

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
        """Append an audit row for a statement import. Returns the
        new import_id."""
        ...

    def get_reconciled_statement_keys(
        self, bank_code: Optional[str] = None,
    ) -> set[tuple[Any, ...]]:
        """Returns the set of (email_id, attachment_id) tuples for
        statements already fully reconciled. Used during scan to
        suppress already-processed statements."""
        ...

    def get_reconciled_filenames(self) -> set[str]:
        """Filenames of statements already fully reconciled."""
        ...
