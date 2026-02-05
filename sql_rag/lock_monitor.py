"""
Opera SQL SE Lock Monitor

Monitors SQL Server locks and blocking to identify record-level conflicts.
Logs events to a monitoring table and provides summary reports.
"""

import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import pyodbc
from sqlalchemy import create_engine, text
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class LockEvent:
    """Represents a lock/blocking event."""
    timestamp: datetime
    blocked_session: int
    blocking_session: int
    blocked_user: str
    blocking_user: str
    table_name: str
    lock_type: str
    wait_time_ms: int
    blocked_query: str
    blocking_query: str


@dataclass
class LockSummary:
    """Summary statistics for lock monitoring."""
    total_events: int
    unique_tables: int
    total_wait_time_ms: int
    avg_wait_time_ms: float
    max_wait_time_ms: int
    most_blocked_tables: List[Dict[str, Any]]
    most_blocking_users: List[Dict[str, Any]]
    hourly_distribution: List[Dict[str, Any]]
    recent_events: List[Dict[str, Any]]


class LockMonitor:
    """
    Monitors SQL Server locks and blocking events.

    Can be configured to poll at regular intervals and log events
    to a monitoring table for historical analysis.
    """

    # SQL to create the monitoring table
    CREATE_TABLE_SQL = """
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'lock_monitor_events')
    BEGIN
        CREATE TABLE lock_monitor_events (
            id INT IDENTITY(1,1) PRIMARY KEY,
            timestamp DATETIME NOT NULL DEFAULT GETDATE(),
            blocked_session INT,
            blocking_session INT,
            blocked_user NVARCHAR(128),
            blocking_user NVARCHAR(128),
            table_name NVARCHAR(256),
            lock_type NVARCHAR(60),
            wait_time_ms INT,
            blocked_query NVARCHAR(MAX),
            blocking_query NVARCHAR(MAX),
            INDEX IX_lock_monitor_timestamp (timestamp),
            INDEX IX_lock_monitor_table (table_name)
        )
    END
    """

    # SQL to get current blocking information
    CURRENT_LOCKS_SQL = """
    SELECT
        r.session_id as blocked_session,
        r.blocking_session_id as blocking_session,
        COALESCE(s1.login_name, 'Unknown') as blocked_user,
        COALESCE(s2.login_name, 'Unknown') as blocking_user,
        COALESCE(OBJECT_NAME(p.object_id), 'Unknown') as table_name,
        l.request_mode as lock_type,
        r.wait_time as wait_time_ms,
        COALESCE(t1.text, '') as blocked_query,
        COALESCE(t2.text, '') as blocking_query
    FROM sys.dm_exec_requests r
    INNER JOIN sys.dm_tran_locks l ON r.session_id = l.request_session_id
    LEFT JOIN sys.partitions p ON l.resource_associated_entity_id = p.hobt_id
    LEFT JOIN sys.dm_exec_sessions s1 ON r.session_id = s1.session_id
    LEFT JOIN sys.dm_exec_sessions s2 ON r.blocking_session_id = s2.session_id
    OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) t1
    OUTER APPLY sys.dm_exec_sql_text(
        (SELECT most_recent_sql_handle FROM sys.dm_exec_connections WHERE session_id = r.blocking_session_id)
    ) t2
    WHERE r.blocking_session_id > 0
    AND l.request_status = 'WAIT'
    """

    # SQL to insert a lock event
    INSERT_EVENT_SQL = """
    INSERT INTO lock_monitor_events
        (timestamp, blocked_session, blocking_session, blocked_user, blocking_user,
         table_name, lock_type, wait_time_ms, blocked_query, blocking_query)
    VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    # SQL for summary statistics
    SUMMARY_SQL = """
    SELECT
        COUNT(*) as total_events,
        COUNT(DISTINCT table_name) as unique_tables,
        SUM(wait_time_ms) as total_wait_time_ms,
        AVG(wait_time_ms) as avg_wait_time_ms,
        MAX(wait_time_ms) as max_wait_time_ms
    FROM lock_monitor_events
    WHERE timestamp >= ?
    """

    MOST_BLOCKED_TABLES_SQL = """
    SELECT TOP 10
        table_name,
        COUNT(*) as block_count,
        SUM(wait_time_ms) as total_wait_ms,
        AVG(wait_time_ms) as avg_wait_ms
    FROM lock_monitor_events
    WHERE timestamp >= ?
    GROUP BY table_name
    ORDER BY block_count DESC
    """

    MOST_BLOCKING_USERS_SQL = """
    SELECT TOP 10
        blocking_user,
        COUNT(*) as block_count,
        SUM(wait_time_ms) as total_wait_ms,
        COUNT(DISTINCT blocked_user) as users_blocked
    FROM lock_monitor_events
    WHERE timestamp >= ?
    GROUP BY blocking_user
    ORDER BY block_count DESC
    """

    HOURLY_DISTRIBUTION_SQL = """
    SELECT
        DATEPART(HOUR, timestamp) as hour,
        COUNT(*) as event_count,
        AVG(wait_time_ms) as avg_wait_ms
    FROM lock_monitor_events
    WHERE timestamp >= ?
    GROUP BY DATEPART(HOUR, timestamp)
    ORDER BY hour
    """

    RECENT_EVENTS_SQL = """
    SELECT TOP 50
        timestamp,
        blocked_session,
        blocking_session,
        blocked_user,
        blocking_user,
        table_name,
        lock_type,
        wait_time_ms,
        LEFT(blocked_query, 200) as blocked_query,
        LEFT(blocking_query, 200) as blocking_query
    FROM lock_monitor_events
    ORDER BY timestamp DESC
    """

    def __init__(self, connection_string: str):
        """
        Initialize the lock monitor.

        Args:
            connection_string: SQL Server connection string
        """
        self.connection_string = connection_string
        self._monitoring = False
        self._monitor_thread: Optional[threading.Thread] = None
        self._poll_interval = 5  # seconds
        self._min_wait_time = 1000  # Only log blocks > 1 second
        self._engine = None

    def _get_engine(self):
        """Get or create SQLAlchemy engine."""
        if self._engine is None:
            self._engine = create_engine(
                self.connection_string,
                pool_pre_ping=True,
                pool_recycle=300
            )
        return self._engine

    def _get_connection(self):
        """Get a raw pyodbc connection for certain operations."""
        # Extract connection params from SQLAlchemy URL
        # Format: mssql+pyodbc://user:pass@server/database?driver=...
        return self._get_engine().raw_connection()

    def initialize_table(self) -> bool:
        """
        Create the monitoring table if it doesn't exist.

        Returns:
            True if successful
        """
        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                conn.execute(text(self.CREATE_TABLE_SQL))
                conn.commit()
            logger.info("Lock monitor table initialized")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize lock monitor table: {e}")
            raise

    def get_current_locks(self) -> List[LockEvent]:
        """
        Get current blocking events.

        Returns:
            List of current lock events
        """
        events = []
        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                result = conn.execute(text(self.CURRENT_LOCKS_SQL))
                for row in result:
                    event = LockEvent(
                        timestamp=datetime.now(),
                        blocked_session=row.blocked_session,
                        blocking_session=row.blocking_session,
                        blocked_user=row.blocked_user or 'Unknown',
                        blocking_user=row.blocking_user or 'Unknown',
                        table_name=row.table_name or 'Unknown',
                        lock_type=row.lock_type or 'Unknown',
                        wait_time_ms=row.wait_time_ms or 0,
                        blocked_query=(row.blocked_query or '')[:2000],
                        blocking_query=(row.blocking_query or '')[:2000]
                    )
                    events.append(event)
        except Exception as e:
            logger.error(f"Error getting current locks: {e}")
            raise
        return events

    def log_events(self, events: List[LockEvent]) -> int:
        """
        Log lock events to the monitoring table.

        Args:
            events: List of lock events to log

        Returns:
            Number of events logged
        """
        if not events:
            return 0

        logged = 0
        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                for event in events:
                    if event.wait_time_ms >= self._min_wait_time:
                        conn.execute(
                            text("""
                                INSERT INTO lock_monitor_events
                                    (timestamp, blocked_session, blocking_session, blocked_user, blocking_user,
                                     table_name, lock_type, wait_time_ms, blocked_query, blocking_query)
                                VALUES
                                    (:ts, :bs, :bks, :bu, :bku, :tn, :lt, :wt, :bq, :bkq)
                            """),
                            {
                                'ts': event.timestamp,
                                'bs': event.blocked_session,
                                'bks': event.blocking_session,
                                'bu': event.blocked_user,
                                'bku': event.blocking_user,
                                'tn': event.table_name,
                                'lt': event.lock_type,
                                'wt': event.wait_time_ms,
                                'bq': event.blocked_query,
                                'bkq': event.blocking_query
                            }
                        )
                        logged += 1
                conn.commit()
        except Exception as e:
            logger.error(f"Error logging lock events: {e}")
            raise
        return logged

    def get_summary(self, hours: int = 24) -> LockSummary:
        """
        Get summary statistics for lock events.

        Args:
            hours: Number of hours to include in summary

        Returns:
            LockSummary with statistics
        """
        since = datetime.now() - timedelta(hours=hours)

        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                # Basic stats
                result = conn.execute(text(self.SUMMARY_SQL), {'1': since}).fetchone()
                if result and result.total_events:
                    total_events = result.total_events
                    unique_tables = result.unique_tables
                    total_wait_time = result.total_wait_time_ms or 0
                    avg_wait_time = float(result.avg_wait_time_ms or 0)
                    max_wait_time = result.max_wait_time_ms or 0
                else:
                    total_events = 0
                    unique_tables = 0
                    total_wait_time = 0
                    avg_wait_time = 0.0
                    max_wait_time = 0

                # Most blocked tables
                result = conn.execute(text(self.MOST_BLOCKED_TABLES_SQL), {'1': since})
                most_blocked = [
                    {
                        'table_name': row.table_name,
                        'block_count': row.block_count,
                        'total_wait_ms': row.total_wait_ms,
                        'avg_wait_ms': float(row.avg_wait_ms or 0)
                    }
                    for row in result
                ]

                # Most blocking users
                result = conn.execute(text(self.MOST_BLOCKING_USERS_SQL), {'1': since})
                most_blocking = [
                    {
                        'user': row.blocking_user,
                        'block_count': row.block_count,
                        'total_wait_ms': row.total_wait_ms,
                        'users_blocked': row.users_blocked
                    }
                    for row in result
                ]

                # Hourly distribution
                result = conn.execute(text(self.HOURLY_DISTRIBUTION_SQL), {'1': since})
                hourly = [
                    {
                        'hour': row.hour,
                        'event_count': row.event_count,
                        'avg_wait_ms': float(row.avg_wait_ms or 0)
                    }
                    for row in result
                ]

                # Recent events
                result = conn.execute(text(self.RECENT_EVENTS_SQL))
                recent = [
                    {
                        'timestamp': row.timestamp.isoformat() if row.timestamp else None,
                        'blocked_session': row.blocked_session,
                        'blocking_session': row.blocking_session,
                        'blocked_user': row.blocked_user,
                        'blocking_user': row.blocking_user,
                        'table_name': row.table_name,
                        'lock_type': row.lock_type,
                        'wait_time_ms': row.wait_time_ms,
                        'blocked_query': row.blocked_query,
                        'blocking_query': row.blocking_query
                    }
                    for row in result
                ]

                return LockSummary(
                    total_events=total_events,
                    unique_tables=unique_tables,
                    total_wait_time_ms=total_wait_time,
                    avg_wait_time_ms=avg_wait_time,
                    max_wait_time_ms=max_wait_time,
                    most_blocked_tables=most_blocked,
                    most_blocking_users=most_blocking,
                    hourly_distribution=hourly,
                    recent_events=recent
                )

        except Exception as e:
            logger.error(f"Error getting lock summary: {e}")
            raise

    def _monitor_loop(self):
        """Background monitoring loop."""
        logger.info(f"Lock monitoring started (interval: {self._poll_interval}s)")

        while self._monitoring:
            try:
                events = self.get_current_locks()
                if events:
                    logged = self.log_events(events)
                    if logged > 0:
                        logger.debug(f"Logged {logged} lock events")
            except Exception as e:
                logger.error(f"Error in monitor loop: {e}")

            # Sleep in small intervals to allow quick shutdown
            for _ in range(self._poll_interval * 2):
                if not self._monitoring:
                    break
                time.sleep(0.5)

        logger.info("Lock monitoring stopped")

    def start_monitoring(self, poll_interval: int = 5, min_wait_time: int = 1000):
        """
        Start background lock monitoring.

        Args:
            poll_interval: Seconds between polls
            min_wait_time: Minimum wait time (ms) to log an event
        """
        if self._monitoring:
            logger.warning("Monitoring already running")
            return

        self._poll_interval = poll_interval
        self._min_wait_time = min_wait_time
        self._monitoring = True

        # Initialize table first
        self.initialize_table()

        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()

    def stop_monitoring(self):
        """Stop background lock monitoring."""
        self._monitoring = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=10)
            self._monitor_thread = None

    @property
    def is_monitoring(self) -> bool:
        """Check if monitoring is active."""
        return self._monitoring

    def clear_old_events(self, days: int = 30) -> int:
        """
        Clear events older than specified days.

        Args:
            days: Number of days to keep

        Returns:
            Number of events deleted
        """
        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                result = conn.execute(
                    text("DELETE FROM lock_monitor_events WHERE timestamp < :cutoff"),
                    {'cutoff': datetime.now() - timedelta(days=days)}
                )
                conn.commit()
                return result.rowcount
        except Exception as e:
            logger.error(f"Error clearing old events: {e}")
            raise


# Global monitor instances keyed by connection name
_monitors: Dict[str, LockMonitor] = {}


def get_monitor(name: str, connection_string: Optional[str] = None) -> Optional[LockMonitor]:
    """
    Get or create a lock monitor instance.

    Args:
        name: Name for this monitor instance
        connection_string: SQL Server connection string (required for new instances)

    Returns:
        LockMonitor instance or None if not found and no connection string provided
    """
    if name in _monitors:
        return _monitors[name]

    if connection_string:
        monitor = LockMonitor(connection_string)
        _monitors[name] = monitor
        return monitor

    return None


def list_monitors() -> List[str]:
    """List all active monitor names."""
    return list(_monitors.keys())


def remove_monitor(name: str) -> bool:
    """
    Remove and stop a monitor instance.

    Args:
        name: Monitor name to remove

    Returns:
        True if removed, False if not found
    """
    if name in _monitors:
        _monitors[name].stop_monitoring()
        del _monitors[name]
        return True
    return False
