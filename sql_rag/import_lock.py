"""
Bank-level import lock to prevent concurrent imports to the same bank account.

Uses a local SQLite database (import_locks.db) to coordinate between
concurrent API requests. This does NOT affect Opera desktop users - it only
prevents SQL RAG from running two imports to the same bank simultaneously.

The lock is acquired at the API endpoint level (before the import transaction)
and released in a finally block (after the import completes or fails).
"""

import logging
import sqlite3
import time
import threading
from pathlib import Path
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# SQLite database path - same directory pattern as lock_monitor.db
IMPORT_LOCK_DB_PATH = Path(__file__).parent.parent / "import_locks.db"

# Lock expiry time in seconds (auto-cleanup of stale locks)
LOCK_EXPIRY_SECONDS = 300  # 5 minutes

# Thread lock for SQLite operations
_db_lock = threading.Lock()


def _get_connection() -> sqlite3.Connection:
    """Get SQLite connection, creating the table if needed."""
    conn = sqlite3.connect(str(IMPORT_LOCK_DB_PATH), timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS import_locks (
            bank_code TEXT PRIMARY KEY,
            locked_at REAL NOT NULL,
            locked_by TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            description TEXT DEFAULT ''
        )
    """)
    conn.commit()
    return conn


def _cleanup_stale_locks(conn: sqlite3.Connection):
    """Remove locks older than LOCK_EXPIRY_SECONDS."""
    cutoff = time.time() - LOCK_EXPIRY_SECONDS
    removed = conn.execute(
        "DELETE FROM import_locks WHERE locked_at < ?", (cutoff,)
    ).rowcount
    if removed > 0:
        conn.commit()
        logger.warning(f"Cleaned up {removed} stale import lock(s)")


def acquire_import_lock(
    bank_code: str,
    locked_by: str = "unknown",
    endpoint: str = "unknown",
    description: str = ""
) -> bool:
    """
    Attempt to acquire an import lock for a bank account.

    Args:
        bank_code: Opera bank account code (e.g., 'BC010')
        locked_by: Identifier for the lock holder
        endpoint: API endpoint name
        description: Optional description of the import

    Returns:
        True if lock acquired, False if bank is already locked.
    """
    with _db_lock:
        conn = _get_connection()
        try:
            _cleanup_stale_locks(conn)

            row = conn.execute(
                "SELECT locked_at, locked_by, endpoint FROM import_locks WHERE bank_code = ?",
                (bank_code,)
            ).fetchone()

            if row:
                locked_at, by, ep = row
                age = time.time() - locked_at
                logger.warning(
                    f"Import lock for {bank_code} already held by {by} "
                    f"via {ep} ({age:.0f}s ago)"
                )
                return False

            conn.execute(
                "INSERT INTO import_locks (bank_code, locked_at, locked_by, endpoint, description) "
                "VALUES (?, ?, ?, ?, ?)",
                (bank_code, time.time(), locked_by, endpoint, description)
            )
            conn.commit()
            logger.info(f"Acquired import lock for {bank_code} by {locked_by} via {endpoint}")
            return True
        finally:
            conn.close()


def release_import_lock(bank_code: str):
    """
    Release an import lock for a bank account. Safe to call even if no lock exists.
    """
    with _db_lock:
        conn = _get_connection()
        try:
            removed = conn.execute(
                "DELETE FROM import_locks WHERE bank_code = ?", (bank_code,)
            ).rowcount
            conn.commit()
            if removed:
                logger.info(f"Released import lock for {bank_code}")
        finally:
            conn.close()


def get_active_locks() -> list:
    """Get all currently active import locks (diagnostic)."""
    with _db_lock:
        conn = _get_connection()
        try:
            _cleanup_stale_locks(conn)
            rows = conn.execute(
                "SELECT bank_code, locked_at, locked_by, endpoint, description FROM import_locks"
            ).fetchall()
            now = time.time()
            return [
                {
                    "bank_code": r[0],
                    "locked_at": r[1],
                    "locked_by": r[2],
                    "endpoint": r[3],
                    "description": r[4],
                    "age_seconds": round(now - r[1], 1)
                }
                for r in rows
            ]
        finally:
            conn.close()


class ImportLockError(Exception):
    """Raised when a bank-level import lock cannot be acquired."""
    pass


@contextmanager
def import_lock(bank_code: str, locked_by: str = "unknown", endpoint: str = "unknown", description: str = ""):
    """
    Context manager for bank-level import locks.

    Usage:
        with import_lock("BC010", locked_by="api", endpoint="import-from-pdf"):
            # do import work
            ...

    Raises:
        ImportLockError if the lock cannot be acquired.
    """
    if not acquire_import_lock(bank_code, locked_by, endpoint, description):
        raise ImportLockError(
            f"Bank account {bank_code} is currently being imported by another user. "
            "Please wait for the current import to complete before starting another."
        )
    try:
        yield
    finally:
        release_import_lock(bank_code)
