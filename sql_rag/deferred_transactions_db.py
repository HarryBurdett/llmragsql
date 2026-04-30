"""Audit-only SQLite store for bank-statement rows the user marked as
'Awaiting manual entry' (deferred) at Stage 3 of the reconciliation flow.

Failure to write must NOT block the import workflow — callers should wrap
record() in a try/except and log a warning on failure. This module deliberately
does not raise to the caller for write failures.

Schema is created on first use. Concurrent access from multiple processes is
safe via SQLite's default file locking; the workflow is single-writer per
import endpoint so contention is negligible.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from typing import Optional

logger = logging.getLogger(__name__)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS deferred_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bank_code TEXT NOT NULL,
    statement_date TEXT,
    amount REAL,
    description TEXT,
    deferred_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deferred_by TEXT
);

CREATE INDEX IF NOT EXISTS idx_deferred_bank_date
    ON deferred_transactions(bank_code, statement_date);
"""


class DeferredTransactionsDB:
    """Tiny SQLite wrapper for the deferred-transactions audit table.

    Usage:
        db = DeferredTransactionsDB("/path/to/deferred.db")
        db.record(bank_code="BC010", statement_date="2026-04-17",
                  amount=123.45, description="...", deferred_by="admin")
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        with self._connect() as conn:
            conn.executescript(_SCHEMA)
            conn.commit()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def record(
        self,
        *,
        bank_code: str,
        statement_date: Optional[str],
        amount: Optional[float],
        description: str,
        deferred_by: str,
    ) -> None:
        """Insert one audit row. Failures log a warning and are swallowed —
        an audit-only write must never block the user's workflow."""
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO deferred_transactions
                        (bank_code, statement_date, amount, description, deferred_by)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (bank_code, statement_date, amount, description, deferred_by),
                )
                conn.commit()
        except Exception as e:
            logger.warning(
                "Failed to record deferred transaction (bank=%s, amount=%s): %s",
                bank_code, amount, e,
            )

    def count_for_bank(self, bank_code: str) -> int:
        """Test/diagnostic helper: how many deferred rows exist for a bank."""
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT COUNT(*) FROM deferred_transactions WHERE bank_code = ?",
                (bank_code,),
            )
            return int(cur.fetchone()[0])

    def count_for_statement(
        self,
        bank_code: str,
        period_start: Optional[str],
        period_end: Optional[str],
    ) -> int:
        """Count deferred rows for a bank, optionally filtered to a statement period.

        If `period_start` or `period_end` is None or empty, the period filter is
        skipped and behaviour is equivalent to `count_for_bank(bank_code)`. The
        period bounds are inclusive (`statement_date BETWEEN start AND end`).
        """
        with self._connect() as conn:
            if period_start and period_end:
                cur = conn.execute(
                    """
                    SELECT COUNT(*) FROM deferred_transactions
                    WHERE bank_code = ?
                      AND statement_date IS NOT NULL
                      AND statement_date >= ?
                      AND statement_date <= ?
                    """,
                    (bank_code, period_start, period_end),
                )
            else:
                cur = conn.execute(
                    "SELECT COUNT(*) FROM deferred_transactions WHERE bank_code = ?",
                    (bank_code,),
                )
            return int(cur.fetchone()[0])
