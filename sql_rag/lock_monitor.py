"""
Opera SQL SE Lock Monitor

Monitors SQL Server locks and blocking to identify record-level conflicts.
Logs events to a LOCAL SQLite database (never modifies Opera SE tables).
"""

import logging
import threading
import time
import sqlite3
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from sqlalchemy import create_engine, text
from pathlib import Path

logger = logging.getLogger(__name__)

# Local SQLite database for storing lock events (never in Opera SE)
LOCK_MONITOR_DB_PATH = Path(__file__).parent.parent / "lock_monitor.db"


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
    # Enhanced details
    database_name: str = ""
    schema_name: str = ""
    index_name: str = ""
    resource_type: str = ""  # KEY, PAGE, RID, OBJECT, etc.
    resource_description: str = ""  # Actual key/page being locked
    lock_mode: str = ""  # S, X, U, IS, IX, etc.
    blocking_lock_mode: str = ""
    # Service/Application identification
    blocked_program: str = ""  # Application/service name causing the blocked session
    blocking_program: str = ""  # Application/service name holding the lock
    blocked_host: str = ""  # Host machine of blocked session
    blocking_host: str = ""  # Host machine of blocking session
    blocking_host_process_id: int = 0  # Process ID on the blocking host


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
    most_blocking_programs: List[Dict[str, Any]]  # Services/applications causing locks
    hourly_distribution: List[Dict[str, Any]]
    recent_events: List[Dict[str, Any]]


class LockMonitor:
    """
    Monitors SQL Server locks and blocking events.

    IMPORTANT: All event logging is stored in a LOCAL SQLite database.
    This utility NEVER modifies the Opera SE database structure.

    Can be configured to poll at regular intervals and log events
    for historical analysis.
    """

    # SQL to get current blocking information (READ-ONLY query against SQL Server)
    # Enhanced query with detailed lock information AND service/application identification
    CURRENT_LOCKS_SQL = """
    SELECT
        r.session_id as blocked_session,
        r.blocking_session_id as blocking_session,
        COALESCE(s1.login_name, 'Unknown') as blocked_user,
        COALESCE(s2.login_name, 'Unknown') as blocking_user,
        COALESCE(OBJECT_NAME(p.object_id), OBJECT_NAME(l.resource_associated_entity_id), 'Unknown') as table_name,
        l.request_mode as lock_type,
        r.wait_time as wait_time_ms,
        COALESCE(t1.text, '') as blocked_query,
        COALESCE(t2.text, '') as blocking_query,
        -- Enhanced details
        DB_NAME(l.resource_database_id) as database_name,
        COALESCE(OBJECT_SCHEMA_NAME(p.object_id), '') as schema_name,
        COALESCE(i.name, '') as index_name,
        l.resource_type as resource_type,
        l.resource_description as resource_description,
        l.request_mode as lock_mode,
        COALESCE(bl.request_mode, '') as blocking_lock_mode,
        -- Service/Application identification (KEY for identifying which service causes locks)
        COALESCE(s1.program_name, 'Unknown') as blocked_program,
        COALESCE(s2.program_name, 'Unknown') as blocking_program,
        COALESCE(s1.host_name, 'Unknown') as blocked_host,
        COALESCE(s2.host_name, 'Unknown') as blocking_host,
        COALESCE(s2.host_process_id, 0) as blocking_host_process_id
    FROM sys.dm_exec_requests r
    INNER JOIN sys.dm_tran_locks l ON r.session_id = l.request_session_id
    LEFT JOIN sys.partitions p ON l.resource_associated_entity_id = p.hobt_id
    LEFT JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
    LEFT JOIN sys.dm_exec_sessions s1 ON r.session_id = s1.session_id
    LEFT JOIN sys.dm_exec_sessions s2 ON r.blocking_session_id = s2.session_id
    LEFT JOIN sys.dm_tran_locks bl ON bl.request_session_id = r.blocking_session_id
        AND bl.resource_database_id = l.resource_database_id
        AND bl.resource_associated_entity_id = l.resource_associated_entity_id
        AND bl.request_status = 'GRANT'
    OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) t1
    OUTER APPLY sys.dm_exec_sql_text(
        (SELECT most_recent_sql_handle FROM sys.dm_exec_connections WHERE session_id = r.blocking_session_id)
    ) t2
    WHERE r.blocking_session_id > 0
    AND l.request_status = 'WAIT'
    """

    def __init__(self, connection_string: str, name: str = "default"):
        """
        Initialize the lock monitor.

        Args:
            connection_string: SQL Server connection string (for READ-ONLY monitoring)
            name: Name identifier for this monitor instance
        """
        self.connection_string = connection_string
        self.name = name
        self._monitoring = False
        self._monitor_thread: Optional[threading.Thread] = None
        self._poll_interval = 5  # seconds
        self._min_wait_time = 1000  # Only log blocks > 1 second
        self._engine = None
        self._sqlite_conn = None

    def _get_engine(self):
        """Get or create SQLAlchemy engine for SQL Server (READ-ONLY)."""
        if self._engine is None:
            self._engine = create_engine(
                self.connection_string,
                pool_pre_ping=True,
                pool_recycle=300
            )
        return self._engine

    def _get_sqlite_conn(self):
        """Get SQLite connection for LOCAL event storage."""
        if self._sqlite_conn is None:
            self._sqlite_conn = sqlite3.connect(str(LOCK_MONITOR_DB_PATH), check_same_thread=False)
            self._sqlite_conn.row_factory = sqlite3.Row
        return self._sqlite_conn

    def initialize_table(self) -> bool:
        """
        Create the LOCAL SQLite monitoring table if it doesn't exist.
        This NEVER touches the Opera SE database.

        Returns:
            True if successful
        """
        try:
            conn = self._get_sqlite_conn()
            conn.execute("""
                CREATE TABLE IF NOT EXISTS lock_monitor_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    monitor_name TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    blocked_session INTEGER,
                    blocking_session INTEGER,
                    blocked_user TEXT,
                    blocking_user TEXT,
                    table_name TEXT,
                    lock_type TEXT,
                    wait_time_ms INTEGER,
                    blocked_query TEXT,
                    blocking_query TEXT,
                    database_name TEXT,
                    schema_name TEXT,
                    index_name TEXT,
                    resource_type TEXT,
                    resource_description TEXT,
                    lock_mode TEXT,
                    blocking_lock_mode TEXT,
                    blocked_program TEXT,
                    blocking_program TEXT,
                    blocked_host TEXT,
                    blocking_host TEXT,
                    blocking_host_process_id INTEGER
                )
            """)
            # Add columns if they don't exist (for existing databases)
            try:
                conn.execute("ALTER TABLE lock_monitor_events ADD COLUMN blocked_program TEXT")
            except:
                pass
            try:
                conn.execute("ALTER TABLE lock_monitor_events ADD COLUMN blocking_program TEXT")
            except:
                pass
            try:
                conn.execute("ALTER TABLE lock_monitor_events ADD COLUMN blocked_host TEXT")
            except:
                pass
            try:
                conn.execute("ALTER TABLE lock_monitor_events ADD COLUMN blocking_host TEXT")
            except:
                pass
            try:
                conn.execute("ALTER TABLE lock_monitor_events ADD COLUMN blocking_host_process_id INTEGER")
            except:
                pass
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_lock_monitor_timestamp
                ON lock_monitor_events(monitor_name, timestamp)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_lock_monitor_table
                ON lock_monitor_events(monitor_name, table_name)
            """)
            conn.commit()

            # Also test SQL Server connection (READ-ONLY)
            engine = self._get_engine()
            with engine.connect() as sql_conn:
                sql_conn.execute(text("SELECT 1"))

            logger.info(f"Lock monitor '{self.name}' initialized (local SQLite + SQL Server connection)")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize lock monitor: {e}")
            raise

    def get_current_locks(self) -> List[LockEvent]:
        """
        Get current blocking events from SQL Server (READ-ONLY).

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
                        blocking_query=(row.blocking_query or '')[:2000],
                        # Enhanced details
                        database_name=row.database_name or '',
                        schema_name=row.schema_name or '',
                        index_name=row.index_name or '',
                        resource_type=row.resource_type or '',
                        resource_description=row.resource_description or '',
                        lock_mode=row.lock_mode or '',
                        blocking_lock_mode=row.blocking_lock_mode or '',
                        # Service/Application identification
                        blocked_program=row.blocked_program or 'Unknown',
                        blocking_program=row.blocking_program or 'Unknown',
                        blocked_host=row.blocked_host or 'Unknown',
                        blocking_host=row.blocking_host or 'Unknown',
                        blocking_host_process_id=row.blocking_host_process_id or 0
                    )
                    events.append(event)
        except Exception as e:
            logger.error(f"Error getting current locks: {e}")
            raise
        return events

    def log_events(self, events: List[LockEvent]) -> int:
        """
        Log lock events to LOCAL SQLite database.
        NEVER writes to Opera SE.

        Args:
            events: List of lock events to log

        Returns:
            Number of events logged
        """
        if not events:
            return 0

        logged = 0
        try:
            conn = self._get_sqlite_conn()
            for event in events:
                if event.wait_time_ms >= self._min_wait_time:
                    conn.execute(
                        """
                        INSERT INTO lock_monitor_events
                            (monitor_name, timestamp, blocked_session, blocking_session, blocked_user, blocking_user,
                             table_name, lock_type, wait_time_ms, blocked_query, blocking_query,
                             database_name, schema_name, index_name, resource_type, resource_description,
                             lock_mode, blocking_lock_mode,
                             blocked_program, blocking_program, blocked_host, blocking_host, blocking_host_process_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            self.name,
                            event.timestamp.isoformat(),
                            event.blocked_session,
                            event.blocking_session,
                            event.blocked_user,
                            event.blocking_user,
                            event.table_name,
                            event.lock_type,
                            event.wait_time_ms,
                            event.blocked_query,
                            event.blocking_query,
                            event.database_name,
                            event.schema_name,
                            event.index_name,
                            event.resource_type,
                            event.resource_description,
                            event.lock_mode,
                            event.blocking_lock_mode,
                            event.blocked_program,
                            event.blocking_program,
                            event.blocked_host,
                            event.blocking_host,
                            event.blocking_host_process_id
                        )
                    )
                    logged += 1
            conn.commit()
        except Exception as e:
            logger.error(f"Error logging lock events: {e}")
            raise
        return logged

    def get_summary(self, hours: int = 24) -> LockSummary:
        """
        Get summary statistics for lock events from LOCAL SQLite database.

        Args:
            hours: Number of hours to include in summary

        Returns:
            LockSummary with statistics
        """
        since = (datetime.now() - timedelta(hours=hours)).isoformat()

        try:
            conn = self._get_sqlite_conn()

            # Basic stats
            result = conn.execute("""
                SELECT
                    COUNT(*) as total_events,
                    COUNT(DISTINCT table_name) as unique_tables,
                    SUM(wait_time_ms) as total_wait_time_ms,
                    AVG(wait_time_ms) as avg_wait_time_ms,
                    MAX(wait_time_ms) as max_wait_time_ms
                FROM lock_monitor_events
                WHERE monitor_name = ? AND timestamp >= ?
            """, (self.name, since)).fetchone()

            if result and result['total_events']:
                total_events = result['total_events']
                unique_tables = result['unique_tables']
                total_wait_time = result['total_wait_time_ms'] or 0
                avg_wait_time = float(result['avg_wait_time_ms'] or 0)
                max_wait_time = result['max_wait_time_ms'] or 0
            else:
                total_events = 0
                unique_tables = 0
                total_wait_time = 0
                avg_wait_time = 0.0
                max_wait_time = 0

            # Most blocked tables
            result = conn.execute("""
                SELECT
                    table_name,
                    COUNT(*) as block_count,
                    SUM(wait_time_ms) as total_wait_ms,
                    AVG(wait_time_ms) as avg_wait_ms
                FROM lock_monitor_events
                WHERE monitor_name = ? AND timestamp >= ?
                GROUP BY table_name
                ORDER BY block_count DESC
                LIMIT 10
            """, (self.name, since))
            most_blocked = [
                {
                    'table_name': row['table_name'],
                    'block_count': row['block_count'],
                    'total_wait_ms': row['total_wait_ms'],
                    'avg_wait_ms': float(row['avg_wait_ms'] or 0)
                }
                for row in result
            ]

            # Most blocking users
            result = conn.execute("""
                SELECT
                    blocking_user,
                    COUNT(*) as block_count,
                    SUM(wait_time_ms) as total_wait_ms,
                    COUNT(DISTINCT blocked_user) as users_blocked
                FROM lock_monitor_events
                WHERE monitor_name = ? AND timestamp >= ?
                GROUP BY blocking_user
                ORDER BY block_count DESC
                LIMIT 10
            """, (self.name, since))
            most_blocking = [
                {
                    'user': row['blocking_user'],
                    'block_count': row['block_count'],
                    'total_wait_ms': row['total_wait_ms'],
                    'users_blocked': row['users_blocked']
                }
                for row in result
            ]

            # Most blocking programs/services (KEY for identifying problematic services)
            result = conn.execute("""
                SELECT
                    blocking_program,
                    blocking_host,
                    COUNT(*) as block_count,
                    SUM(wait_time_ms) as total_wait_ms,
                    AVG(wait_time_ms) as avg_wait_ms,
                    COUNT(DISTINCT table_name) as tables_affected,
                    COUNT(DISTINCT blocked_program) as programs_blocked
                FROM lock_monitor_events
                WHERE monitor_name = ? AND timestamp >= ?
                GROUP BY blocking_program, blocking_host
                ORDER BY block_count DESC
                LIMIT 15
            """, (self.name, since))
            most_blocking_programs = [
                {
                    'program': row['blocking_program'],
                    'host': row['blocking_host'],
                    'block_count': row['block_count'],
                    'total_wait_ms': row['total_wait_ms'],
                    'avg_wait_ms': float(row['avg_wait_ms'] or 0),
                    'tables_affected': row['tables_affected'],
                    'programs_blocked': row['programs_blocked']
                }
                for row in result
            ]

            # Hourly distribution
            result = conn.execute("""
                SELECT
                    CAST(strftime('%H', timestamp) AS INTEGER) as hour,
                    COUNT(*) as event_count,
                    AVG(wait_time_ms) as avg_wait_ms
                FROM lock_monitor_events
                WHERE monitor_name = ? AND timestamp >= ?
                GROUP BY hour
                ORDER BY hour
            """, (self.name, since))
            hourly = [
                {
                    'hour': row['hour'],
                    'event_count': row['event_count'],
                    'avg_wait_ms': float(row['avg_wait_ms'] or 0)
                }
                for row in result
            ]

            # Recent events (including program/service info)
            result = conn.execute("""
                SELECT
                    timestamp,
                    blocked_session,
                    blocking_session,
                    blocked_user,
                    blocking_user,
                    table_name,
                    lock_type,
                    wait_time_ms,
                    SUBSTR(blocked_query, 1, 200) as blocked_query,
                    SUBSTR(blocking_query, 1, 200) as blocking_query,
                    COALESCE(blocked_program, 'Unknown') as blocked_program,
                    COALESCE(blocking_program, 'Unknown') as blocking_program,
                    COALESCE(blocked_host, 'Unknown') as blocked_host,
                    COALESCE(blocking_host, 'Unknown') as blocking_host
                FROM lock_monitor_events
                WHERE monitor_name = ?
                ORDER BY timestamp DESC
                LIMIT 50
            """, (self.name,))
            recent = [
                {
                    'timestamp': row['timestamp'],
                    'blocked_session': row['blocked_session'],
                    'blocking_session': row['blocking_session'],
                    'blocked_user': row['blocked_user'],
                    'blocking_user': row['blocking_user'],
                    'table_name': row['table_name'],
                    'lock_type': row['lock_type'],
                    'wait_time_ms': row['wait_time_ms'],
                    'blocked_query': row['blocked_query'],
                    'blocking_query': row['blocking_query'],
                    'blocked_program': row['blocked_program'],
                    'blocking_program': row['blocking_program'],
                    'blocked_host': row['blocked_host'],
                    'blocking_host': row['blocking_host']
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
                most_blocking_programs=most_blocking_programs,
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

    # SQL to get ALL current connections (for identifying what to disconnect before exclusive ops)
    CURRENT_CONNECTIONS_SQL = """
    SELECT
        s.session_id,
        s.login_name as user_name,
        s.program_name,
        s.host_name,
        s.host_process_id,
        s.status,
        s.login_time,
        s.last_request_start_time,
        s.last_request_end_time,
        COALESCE(DB_NAME(s.database_id), 'master') as database_name,
        c.client_net_address,
        COALESCE(t.text, '') as last_query
    FROM sys.dm_exec_sessions s
    LEFT JOIN sys.dm_exec_connections c ON s.session_id = c.session_id
    OUTER APPLY sys.dm_exec_sql_text(c.most_recent_sql_handle) t
    WHERE s.is_user_process = 1
    ORDER BY s.program_name, s.host_name, s.login_name
    """

    def get_current_connections(self) -> List[Dict[str, Any]]:
        """
        Get ALL current connections to the database (READ-ONLY).
        Useful for identifying services/applications that need to be stopped
        before exclusive operations like database restores.

        Returns:
            List of connection details including program name, host, and user
        """
        connections = []
        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                result = conn.execute(text(self.CURRENT_CONNECTIONS_SQL))
                for row in result:
                    connections.append({
                        'session_id': row.session_id,
                        'user_name': row.user_name or 'Unknown',
                        'program_name': row.program_name or 'Unknown',
                        'host_name': row.host_name or 'Unknown',
                        'host_process_id': row.host_process_id or 0,
                        'status': row.status or 'Unknown',
                        'login_time': str(row.login_time) if row.login_time else '',
                        'last_request_start': str(row.last_request_start_time) if row.last_request_start_time else '',
                        'last_request_end': str(row.last_request_end_time) if row.last_request_end_time else '',
                        'database_name': row.database_name or '',
                        'client_address': row.client_net_address or '',
                        'last_query': (row.last_query or '')[:500]
                    })
        except Exception as e:
            logger.error(f"Error getting current connections: {e}")
            raise
        return connections

    def get_connections_summary(self) -> Dict[str, Any]:
        """
        Get a summary of current connections grouped by program/service.
        Helps identify which services are connected and may need to be stopped.

        Returns:
            Dictionary with connection counts by program, host, and detailed list
        """
        connections = self.get_current_connections()

        # Group by program
        by_program = {}
        for conn in connections:
            prog = conn['program_name']
            if prog not in by_program:
                by_program[prog] = {
                    'count': 0,
                    'hosts': set(),
                    'users': set(),
                    'sessions': []
                }
            by_program[prog]['count'] += 1
            by_program[prog]['hosts'].add(conn['host_name'])
            by_program[prog]['users'].add(conn['user_name'])
            by_program[prog]['sessions'].append(conn['session_id'])

        # Convert sets to lists for JSON serialization
        programs = [
            {
                'program': prog,
                'connection_count': data['count'],
                'hosts': list(data['hosts']),
                'users': list(data['users']),
                'session_ids': data['sessions']
            }
            for prog, data in sorted(by_program.items(), key=lambda x: -x[1]['count'])
        ]

        # Group by host
        by_host = {}
        for conn in connections:
            host = conn['host_name']
            if host not in by_host:
                by_host[host] = {'count': 0, 'programs': set()}
            by_host[host]['count'] += 1
            by_host[host]['programs'].add(conn['program_name'])

        hosts = [
            {
                'host': host,
                'connection_count': data['count'],
                'programs': list(data['programs'])
            }
            for host, data in sorted(by_host.items(), key=lambda x: -x[1]['count'])
        ]

        return {
            'total_connections': len(connections),
            'by_program': programs,
            'by_host': hosts,
            'connections': connections
        }

    def clear_old_events(self, days: int = 30) -> int:
        """
        Clear events older than specified days from LOCAL SQLite database.

        Args:
            days: Number of days to keep

        Returns:
            Number of events deleted
        """
        try:
            cutoff = (datetime.now() - timedelta(days=days)).isoformat()
            conn = self._get_sqlite_conn()
            cursor = conn.execute(
                "DELETE FROM lock_monitor_events WHERE monitor_name = ? AND timestamp < ?",
                (self.name, cutoff)
            )
            conn.commit()
            return cursor.rowcount
        except Exception as e:
            logger.error(f"Error clearing old events: {e}")
            raise


# Global monitor instances keyed by connection name
_monitors: Dict[str, LockMonitor] = {}


def _init_config_table():
    """Create configuration table for persisting monitor settings."""
    try:
        conn = sqlite3.connect(str(LOCK_MONITOR_DB_PATH))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS lock_monitor_config (
                name TEXT PRIMARY KEY,
                connection_string TEXT,
                server TEXT,
                port TEXT,
                database_name TEXT,
                username TEXT,
                use_windows_auth INTEGER DEFAULT 0,
                auto_start INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error creating config table: {e}")


def save_monitor_config(name: str, connection_string: str, server: str = None,
                        port: str = None, database: str = None, username: str = None,
                        use_windows_auth: bool = False, auto_start: bool = False):
    """Save monitor configuration for persistence across restarts.

    NOTE: Connection string is only stored if using Windows Auth (no password).
    For password auth, only parameters are saved and user must re-enter password.
    """
    try:
        _init_config_table()
        conn = sqlite3.connect(str(LOCK_MONITOR_DB_PATH))
        # Only store connection string if using Windows Auth (no password in string)
        stored_conn_str = connection_string if use_windows_auth else None
        conn.execute("""
            INSERT OR REPLACE INTO lock_monitor_config
            (name, connection_string, server, port, database_name, username, use_windows_auth, auto_start)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (name, stored_conn_str, server, port, database, username,
              1 if use_windows_auth else 0, 1 if auto_start else 0))
        conn.commit()
        conn.close()
        logger.info(f"Saved monitor config for '{name}' (Windows Auth: {use_windows_auth})")
    except Exception as e:
        logger.error(f"Error saving monitor config: {e}")


def delete_monitor_config(name: str):
    """Delete saved monitor configuration."""
    try:
        conn = sqlite3.connect(str(LOCK_MONITOR_DB_PATH))
        conn.execute("DELETE FROM lock_monitor_config WHERE name = ?", (name,))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error deleting monitor config: {e}")


def load_saved_monitors():
    """Load and reconnect saved monitor configurations.

    Only auto-reconnects Windows Auth monitors (no password needed).
    Password auth monitors are listed in saved configs but require manual reconnection.
    """
    try:
        _init_config_table()
        conn = sqlite3.connect(str(LOCK_MONITOR_DB_PATH))
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("SELECT * FROM lock_monitor_config WHERE use_windows_auth = 1")
        configs = cursor.fetchall()
        conn.close()

        loaded = 0
        for config in configs:
            name = config['name']
            connection_string = config['connection_string']
            if name not in _monitors and connection_string:
                try:
                    monitor = LockMonitor(connection_string, name=name)
                    # Test connection before adding
                    monitor.initialize_table()
                    _monitors[name] = monitor
                    loaded += 1
                    logger.info(f"Auto-reconnected saved monitor: {name} (Windows Auth)")

                    # Auto-start if configured
                    if config['auto_start']:
                        monitor.start_monitoring()
                except Exception as e:
                    logger.warning(f"Could not auto-reconnect saved monitor '{name}': {e}")

        if loaded > 0:
            logger.info(f"Auto-reconnected {loaded} Windows Auth monitor(s)")
        return loaded
    except Exception as e:
        logger.error(f"Error loading saved monitors: {e}")
        return 0


def get_saved_configs() -> List[Dict[str, Any]]:
    """Get list of saved monitor configurations (without connection strings for security)."""
    try:
        _init_config_table()
        conn = sqlite3.connect(str(LOCK_MONITOR_DB_PATH))
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("""
            SELECT name, server, port, database_name, username, use_windows_auth, auto_start, created_at
            FROM lock_monitor_config
        """)
        configs = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return configs
    except Exception as e:
        logger.error(f"Error getting saved configs: {e}")
        return []


# Load saved monitors on module import
try:
    load_saved_monitors()
except Exception as e:
    logger.warning(f"Could not load saved monitors on startup: {e}")


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
        monitor = LockMonitor(connection_string, name=name)
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
    # Also delete saved config
    delete_monitor_config(name)

    if name in _monitors:
        _monitors[name].stop_monitoring()
        del _monitors[name]
        return True
    return False
