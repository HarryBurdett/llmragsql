"""
Write-Ahead Log for Opera 3 Write Agent.

Records every write operation BEFORE execution, enabling:
- Crash recovery: detect incomplete operations on restart
- Audit trail: complete history of all writes with parameters
- Compensation tracking: know exactly what was written for rollback
- Monitoring: recent operation history for diagnostics

Storage: SQLite database alongside the agent (NOT in Opera data).
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List, Dict, Any
from uuid import uuid4

logger = logging.getLogger(__name__)


class OperationStatus(str, Enum):
    """Lifecycle states for a write operation."""
    PENDING = "PENDING"                         # Recorded, writes not started
    IN_PROGRESS = "IN_PROGRESS"                 # Writes executing
    VERIFYING = "VERIFYING"                     # Writes done, verification running
    COMPLETED = "COMPLETED"                     # Verified and committed
    FAILED = "FAILED"                           # Write failed
    COMPENSATING = "COMPENSATING"               # Compensation (undo) in progress
    COMPENSATED = "COMPENSATED"                 # Successfully compensated
    COMPENSATION_FAILED = "COMPENSATION_FAILED" # Compensation failed — manual review


@dataclass
class WALOperation:
    """A single write operation record."""
    id: str
    operation_type: str
    status: OperationStatus
    params: Dict[str, Any]
    started_at: str
    completed_at: Optional[str] = None
    error_message: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    snapshot: Optional[Dict[str, Any]] = None
    compensation_log: Optional[List[str]] = None
    verification_details: Optional[str] = None


class WriteAheadLog:
    """SQLite-backed write-ahead log for Opera 3 write operations.

    Every import operation is recorded BEFORE writes begin. The WAL tracks
    the full lifecycle: PENDING → IN_PROGRESS → VERIFYING → COMPLETED.

    On agent restart, incomplete operations are detected and handled.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Create WAL tables if they don't exist."""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS operations (
                    id TEXT PRIMARY KEY,
                    operation_type TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'PENDING',
                    params_json TEXT,
                    snapshot_json TEXT,
                    result_json TEXT,
                    compensation_log_json TEXT,
                    verification_details TEXT,
                    error_message TEXT,
                    started_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_op_status
                    ON operations(status);
                CREATE INDEX IF NOT EXISTS idx_op_started
                    ON operations(started_at);
            """)
            conn.commit()
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Lifecycle methods
    # ------------------------------------------------------------------

    def begin_operation(self, operation_type: str, params: dict) -> str:
        """Record a new operation BEFORE any writes begin.

        Returns the operation ID (UUID).
        """
        op_id = str(uuid4())
        now = _now_iso()

        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                """INSERT INTO operations
                   (id, operation_type, status, params_json, started_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (op_id, operation_type, OperationStatus.PENDING.value,
                 json.dumps(params, default=str), now, now),
            )
            conn.commit()
        finally:
            conn.close()

        logger.info(f"WAL: BEGIN {operation_type} [{op_id[:8]}]")
        return op_id

    def mark_in_progress(self, op_id: str, snapshot: dict | None = None):
        """Mark operation as in-progress (writes starting)."""
        self._update(op_id, OperationStatus.IN_PROGRESS, snapshot=snapshot)

    def mark_verifying(self, op_id: str, result: dict | None = None):
        """Mark operation as verifying (writes done, checking results)."""
        self._update(op_id, OperationStatus.VERIFYING, result=result)

    def mark_completed(self, op_id: str, result: dict | None = None):
        """Mark operation as successfully completed and verified."""
        now = _now_iso()
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                """UPDATE operations
                   SET status = ?, result_json = ?, completed_at = ?, updated_at = ?
                   WHERE id = ?""",
                (OperationStatus.COMPLETED.value,
                 json.dumps(result, default=str) if result else None,
                 now, now, op_id),
            )
            conn.commit()
        finally:
            conn.close()
        logger.info(f"WAL: COMPLETED [{op_id[:8]}]")

    def mark_failed(self, op_id: str, error: str):
        """Mark operation as failed."""
        self._update(op_id, OperationStatus.FAILED, error=error)
        logger.error(f"WAL: FAILED [{op_id[:8]}]: {error[:200]}")

    def mark_compensating(self, op_id: str):
        """Mark that compensation (undo) is in progress."""
        self._update(op_id, OperationStatus.COMPENSATING)

    def mark_compensated(self, op_id: str, compensation_log: list):
        """Mark operation as successfully compensated (undone)."""
        now = _now_iso()
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                """UPDATE operations
                   SET status = ?, compensation_log_json = ?,
                       completed_at = ?, updated_at = ?
                   WHERE id = ?""",
                (OperationStatus.COMPENSATED.value,
                 json.dumps(compensation_log), now, now, op_id),
            )
            conn.commit()
        finally:
            conn.close()
        logger.warning(
            f"WAL: COMPENSATED [{op_id[:8]}] ({len(compensation_log)} steps)"
        )

    def mark_compensation_failed(self, op_id: str, error: str):
        """Mark compensation as failed — needs manual intervention."""
        self._update(op_id, OperationStatus.COMPENSATION_FAILED, error=error)
        logger.critical(
            f"WAL: COMPENSATION FAILED [{op_id[:8]}]: {error[:200]}"
        )

    def set_verification_details(self, op_id: str, details: str):
        """Store verification result details."""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                "UPDATE operations SET verification_details = ?, updated_at = ? WHERE id = ?",
                (details, _now_iso(), op_id),
            )
            conn.commit()
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Query methods
    # ------------------------------------------------------------------

    def get_operation(self, op_id: str) -> WALOperation | None:
        """Get a specific operation by ID."""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM operations WHERE id = ?", (op_id,)
            ).fetchone()
            return self._row_to_op(row) if row else None
        finally:
            conn.close()

    def get_incomplete_operations(self) -> List[WALOperation]:
        """Find operations that didn't complete (for crash recovery)."""
        incomplete = (
            OperationStatus.PENDING.value,
            OperationStatus.IN_PROGRESS.value,
            OperationStatus.VERIFYING.value,
            OperationStatus.FAILED.value,
            OperationStatus.COMPENSATING.value,
        )
        placeholders = ",".join("?" * len(incomplete))

        conn = sqlite3.connect(self.db_path)
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"SELECT * FROM operations WHERE status IN ({placeholders}) "
                "ORDER BY started_at",
                incomplete,
            ).fetchall()
            return [self._row_to_op(r) for r in rows]
        finally:
            conn.close()

    def get_recent_operations(self, limit: int = 50) -> List[WALOperation]:
        """Get recent operations for monitoring/audit."""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM operations ORDER BY started_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [self._row_to_op(r) for r in rows]
        finally:
            conn.close()

    def get_stats(self) -> Dict[str, Any]:
        """Get summary statistics for monitoring."""
        conn = sqlite3.connect(self.db_path)
        try:
            total = conn.execute("SELECT COUNT(*) FROM operations").fetchone()[0]
            by_status = {}
            for row in conn.execute(
                "SELECT status, COUNT(*) FROM operations GROUP BY status"
            ).fetchall():
                by_status[row[0]] = row[1]

            last_op = None
            row = conn.execute(
                "SELECT operation_type, status, started_at FROM operations "
                "ORDER BY started_at DESC LIMIT 1"
            ).fetchone()
            if row:
                last_op = {
                    "type": row[0], "status": row[1], "started_at": row[2]
                }

            return {
                "total_operations": total,
                "by_status": by_status,
                "last_operation": last_op,
            }
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    def cleanup_old(self, days: int = 90):
        """Remove completed/compensated operations older than N days."""
        conn = sqlite3.connect(self.db_path)
        try:
            result = conn.execute(
                """DELETE FROM operations
                   WHERE status IN (?, ?)
                     AND started_at < datetime('now', ?)""",
                (OperationStatus.COMPLETED.value,
                 OperationStatus.COMPENSATED.value,
                 f"-{days} days"),
            )
            conn.commit()
            if result.rowcount > 0:
                logger.info(f"WAL: Cleaned up {result.rowcount} old operations")
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _update(self, op_id: str, status: OperationStatus, *,
                snapshot: dict | None = None, result: dict | None = None,
                error: str | None = None):
        """Generic status update."""
        conn = sqlite3.connect(self.db_path)
        try:
            parts = ["status = ?", "updated_at = ?"]
            vals: list = [status.value, _now_iso()]

            if snapshot is not None:
                parts.append("snapshot_json = ?")
                vals.append(json.dumps(snapshot, default=str))
            if result is not None:
                parts.append("result_json = ?")
                vals.append(json.dumps(result, default=str))
            if error is not None:
                parts.append("error_message = ?")
                vals.append(error)

            vals.append(op_id)
            conn.execute(
                f"UPDATE operations SET {', '.join(parts)} WHERE id = ?", vals
            )
            conn.commit()
        finally:
            conn.close()

    def _row_to_op(self, row) -> WALOperation:
        """Convert a database row to WALOperation."""
        return WALOperation(
            id=row["id"],
            operation_type=row["operation_type"],
            status=OperationStatus(row["status"]),
            params=json.loads(row["params_json"]) if row["params_json"] else {},
            started_at=row["started_at"],
            completed_at=row["completed_at"],
            error_message=row["error_message"],
            result=json.loads(row["result_json"]) if row["result_json"] else None,
            snapshot=json.loads(row["snapshot_json"]) if row["snapshot_json"] else None,
            compensation_log=(
                json.loads(row["compensation_log_json"])
                if row["compensation_log_json"] else None
            ),
            verification_details=row["verification_details"],
        )


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
