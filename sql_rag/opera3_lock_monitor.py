"""
Opera 3 File Lock Monitor

Monitors FoxPro DBF file locking for Opera 3 installations.
Detects which files are locked and by which processes.

Works on:
- Windows: Uses pywin32 to query file handles
- macOS/Linux: Uses lsof to check file locks

IMPORTANT: All monitoring data stored in LOCAL SQLite database.
This module NEVER modifies Opera 3 files.
"""

import logging
import threading
import time
import sqlite3
import subprocess
import platform
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

# Local SQLite database for storing lock events
OPERA3_LOCK_MONITOR_DB = Path(__file__).parent.parent / "opera3_lock_monitor.db"

# Common Opera 3 DBF files to monitor
OPERA3_TABLES = [
    'sname', 'stran', 'shist',  # Sales ledger
    'pname', 'ptran', 'phist',  # Purchase ledger
    'ntran', 'nacnt', 'nbudg',  # Nominal ledger
    'aentry', 'atran',          # Bank/cashbook
    'stock', 'stkhst',          # Stock
    'sinvh', 'sinvd',           # Sales invoices
    'pinvh', 'pinvd',           # Purchase invoices
]


@dataclass
class FileLockEvent:
    """Represents a file lock detection event."""
    timestamp: datetime
    file_path: str
    file_name: str
    table_name: str
    process_id: Optional[int]
    process_name: Optional[str]
    lock_type: str  # 'read', 'write', 'exclusive'
    user: Optional[str]


@dataclass
class Opera3LockSummary:
    """Summary of Opera 3 file lock activity."""
    total_events: int
    unique_files: int
    unique_processes: int
    most_locked_files: List[Dict[str, Any]]
    most_active_processes: List[Dict[str, Any]]
    recent_events: List[Dict[str, Any]]
    hourly_distribution: List[Dict[str, Any]]


class Opera3LockMonitor:
    """
    Monitors file locks on Opera 3 DBF files.

    Detects which processes have locks on Opera 3 data files
    and logs the activity for analysis.

    IMPORTANT: Read-only monitoring - never modifies Opera 3 files.
    All event data stored in local SQLite database.
    """

    def __init__(self, data_path: str, name: str = "opera3"):
        """
        Initialize the Opera 3 lock monitor.

        Args:
            data_path: Path to Opera 3 company data folder
            name: Name identifier for this monitor
        """
        self.data_path = Path(data_path)
        self.name = name
        self._monitoring = False
        self._monitor_thread: Optional[threading.Thread] = None
        self._poll_interval = 5
        self._sqlite_conn: Optional[sqlite3.Connection] = None
        self._platform = platform.system()

    def _get_sqlite_conn(self) -> sqlite3.Connection:
        """Get SQLite connection for local event storage."""
        if self._sqlite_conn is None:
            self._sqlite_conn = sqlite3.connect(str(OPERA3_LOCK_MONITOR_DB), check_same_thread=False)
            self._sqlite_conn.row_factory = sqlite3.Row
        return self._sqlite_conn

    def initialize(self) -> bool:
        """
        Initialize the monitor and create local storage table.

        Returns:
            True if successful
        """
        try:
            # Verify data path exists
            if not self.data_path.exists():
                raise FileNotFoundError(f"Opera 3 data path not found: {self.data_path}")

            # Create local SQLite table
            conn = self._get_sqlite_conn()
            conn.execute("""
                CREATE TABLE IF NOT EXISTS opera3_lock_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    monitor_name TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    file_path TEXT,
                    file_name TEXT,
                    table_name TEXT,
                    process_id INTEGER,
                    process_name TEXT,
                    lock_type TEXT,
                    user TEXT
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_opera3_lock_timestamp
                ON opera3_lock_events(monitor_name, timestamp)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_opera3_lock_file
                ON opera3_lock_events(monitor_name, table_name)
            """)
            conn.commit()

            logger.info(f"Opera 3 lock monitor '{self.name}' initialized for {self.data_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize Opera 3 lock monitor: {e}")
            raise

    def get_current_locks(self) -> List[FileLockEvent]:
        """
        Get current file locks on Opera 3 DBF files.

        Returns:
            List of current file lock events
        """
        events = []

        if self._platform == 'Windows':
            events = self._get_locks_windows()
        elif self._platform == 'Darwin':  # macOS
            events = self._get_locks_macos()
        else:  # Linux
            events = self._get_locks_linux()

        return events

    def _get_locks_windows(self) -> List[FileLockEvent]:
        """Get file locks on Windows using handle.exe or pywin32."""
        events = []

        try:
            # Try using handle.exe from Sysinternals (if available)
            # Or fall back to checking if files are accessible
            for table in OPERA3_TABLES:
                dbf_path = self.data_path / f"{table}.dbf"
                if dbf_path.exists():
                    lock_info = self._check_file_lock_windows(dbf_path)
                    if lock_info:
                        events.append(FileLockEvent(
                            timestamp=datetime.now(),
                            file_path=str(dbf_path),
                            file_name=dbf_path.name,
                            table_name=table,
                            process_id=lock_info.get('pid'),
                            process_name=lock_info.get('process'),
                            lock_type=lock_info.get('type', 'unknown'),
                            user=lock_info.get('user')
                        ))
        except Exception as e:
            logger.error(f"Error getting Windows file locks: {e}")

        return events

    def _check_file_lock_windows(self, file_path: Path) -> Optional[Dict]:
        """Check if a file is locked on Windows."""
        try:
            # Try to open file exclusively - if it fails, it's locked
            try:
                with open(file_path, 'r+b') as f:
                    pass
                return None  # File not locked
            except PermissionError:
                return {'type': 'exclusive', 'pid': None, 'process': 'Unknown'}
            except IOError:
                return {'type': 'read', 'pid': None, 'process': 'Unknown'}
        except Exception:
            return None

    def _get_locks_macos(self) -> List[FileLockEvent]:
        """Get file locks on macOS using lsof."""
        events = []

        try:
            # Use lsof to find open files in the data directory
            result = subprocess.run(
                ['lsof', '+D', str(self.data_path)],
                capture_output=True,
                text=True,
                timeout=10
            )

            if result.returncode == 0 and result.stdout:
                lines = result.stdout.strip().split('\n')[1:]  # Skip header
                for line in lines:
                    parts = line.split()
                    if len(parts) >= 9:
                        process_name = parts[0]
                        pid = int(parts[1]) if parts[1].isdigit() else None
                        user = parts[2]
                        file_path = parts[-1]

                        # Check if it's a DBF file we care about
                        file_name = os.path.basename(file_path).lower()
                        table_name = file_name.replace('.dbf', '')

                        if table_name in OPERA3_TABLES:
                            events.append(FileLockEvent(
                                timestamp=datetime.now(),
                                file_path=file_path,
                                file_name=file_name,
                                table_name=table_name,
                                process_id=pid,
                                process_name=process_name,
                                lock_type='open',
                                user=user
                            ))

        except subprocess.TimeoutExpired:
            logger.warning("lsof command timed out")
        except FileNotFoundError:
            logger.warning("lsof not found - cannot monitor file locks on macOS")
        except Exception as e:
            logger.error(f"Error getting macOS file locks: {e}")

        return events

    def _get_locks_linux(self) -> List[FileLockEvent]:
        """Get file locks on Linux using lsof or /proc."""
        # Similar to macOS implementation
        return self._get_locks_macos()

    def log_events(self, events: List[FileLockEvent]) -> int:
        """
        Log file lock events to local SQLite database.

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
                conn.execute("""
                    INSERT INTO opera3_lock_events
                        (monitor_name, timestamp, file_path, file_name, table_name,
                         process_id, process_name, lock_type, user)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    self.name,
                    event.timestamp.isoformat(),
                    event.file_path,
                    event.file_name,
                    event.table_name,
                    event.process_id,
                    event.process_name,
                    event.lock_type,
                    event.user
                ))
                logged += 1
            conn.commit()
        except Exception as e:
            logger.error(f"Error logging Opera 3 lock events: {e}")

        return logged

    def get_summary(self, hours: int = 24) -> Opera3LockSummary:
        """
        Get summary statistics for Opera 3 file lock events.

        Args:
            hours: Number of hours to include

        Returns:
            Opera3LockSummary with statistics
        """
        since = (datetime.now() - timedelta(hours=hours)).isoformat()

        try:
            conn = self._get_sqlite_conn()

            # Basic stats
            result = conn.execute("""
                SELECT
                    COUNT(*) as total_events,
                    COUNT(DISTINCT file_name) as unique_files,
                    COUNT(DISTINCT process_name) as unique_processes
                FROM opera3_lock_events
                WHERE monitor_name = ? AND timestamp >= ?
            """, (self.name, since)).fetchone()

            total_events = result['total_events'] if result else 0
            unique_files = result['unique_files'] if result else 0
            unique_processes = result['unique_processes'] if result else 0

            # Most locked files
            result = conn.execute("""
                SELECT table_name, COUNT(*) as lock_count
                FROM opera3_lock_events
                WHERE monitor_name = ? AND timestamp >= ?
                GROUP BY table_name
                ORDER BY lock_count DESC
                LIMIT 10
            """, (self.name, since))
            most_locked = [
                {'table_name': row['table_name'], 'lock_count': row['lock_count']}
                for row in result
            ]

            # Most active processes
            result = conn.execute("""
                SELECT process_name, COUNT(*) as access_count,
                       COUNT(DISTINCT table_name) as tables_accessed
                FROM opera3_lock_events
                WHERE monitor_name = ? AND timestamp >= ?
                GROUP BY process_name
                ORDER BY access_count DESC
                LIMIT 10
            """, (self.name, since))
            most_active = [
                {
                    'process': row['process_name'],
                    'access_count': row['access_count'],
                    'tables_accessed': row['tables_accessed']
                }
                for row in result
            ]

            # Hourly distribution
            result = conn.execute("""
                SELECT
                    CAST(strftime('%H', timestamp) AS INTEGER) as hour,
                    COUNT(*) as event_count
                FROM opera3_lock_events
                WHERE monitor_name = ? AND timestamp >= ?
                GROUP BY hour
                ORDER BY hour
            """, (self.name, since))
            hourly = [
                {'hour': row['hour'], 'event_count': row['event_count']}
                for row in result
            ]

            # Recent events
            result = conn.execute("""
                SELECT timestamp, file_name, table_name, process_name, lock_type, user
                FROM opera3_lock_events
                WHERE monitor_name = ?
                ORDER BY timestamp DESC
                LIMIT 50
            """, (self.name,))
            recent = [
                {
                    'timestamp': row['timestamp'],
                    'file_name': row['file_name'],
                    'table_name': row['table_name'],
                    'process': row['process_name'],
                    'lock_type': row['lock_type'],
                    'user': row['user']
                }
                for row in result
            ]

            return Opera3LockSummary(
                total_events=total_events,
                unique_files=unique_files,
                unique_processes=unique_processes,
                most_locked_files=most_locked,
                most_active_processes=most_active,
                recent_events=recent,
                hourly_distribution=hourly
            )

        except Exception as e:
            logger.error(f"Error getting Opera 3 lock summary: {e}")
            raise

    def _monitor_loop(self):
        """Background monitoring loop."""
        logger.info(f"Opera 3 lock monitoring started for {self.data_path}")

        while self._monitoring:
            try:
                events = self.get_current_locks()
                if events:
                    logged = self.log_events(events)
                    if logged > 0:
                        logger.debug(f"Logged {logged} Opera 3 lock events")
            except Exception as e:
                logger.error(f"Error in Opera 3 monitor loop: {e}")

            # Sleep in intervals for quick shutdown
            for _ in range(self._poll_interval * 2):
                if not self._monitoring:
                    break
                time.sleep(0.5)

        logger.info("Opera 3 lock monitoring stopped")

    def start_monitoring(self, poll_interval: int = 5):
        """
        Start background file lock monitoring.

        Args:
            poll_interval: Seconds between polls
        """
        if self._monitoring:
            logger.warning("Opera 3 monitoring already running")
            return

        self._poll_interval = poll_interval
        self._monitoring = True

        self.initialize()

        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()

    def stop_monitoring(self):
        """Stop background monitoring."""
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
            cutoff = (datetime.now() - timedelta(days=days)).isoformat()
            conn = self._get_sqlite_conn()
            cursor = conn.execute(
                "DELETE FROM opera3_lock_events WHERE monitor_name = ? AND timestamp < ?",
                (self.name, cutoff)
            )
            conn.commit()
            return cursor.rowcount
        except Exception as e:
            logger.error(f"Error clearing old Opera 3 lock events: {e}")
            return 0


# Global monitor instances
_opera3_monitors: Dict[str, Opera3LockMonitor] = {}


def get_opera3_monitor(name: str, data_path: Optional[str] = None) -> Optional[Opera3LockMonitor]:
    """
    Get or create an Opera 3 lock monitor instance.

    Args:
        name: Name for this monitor
        data_path: Path to Opera 3 data folder (required for new instances)

    Returns:
        Opera3LockMonitor instance or None
    """
    if name in _opera3_monitors:
        return _opera3_monitors[name]

    if data_path:
        monitor = Opera3LockMonitor(data_path, name=name)
        _opera3_monitors[name] = monitor
        return monitor

    return None


def list_opera3_monitors() -> List[str]:
    """List all Opera 3 monitor names."""
    return list(_opera3_monitors.keys())


def remove_opera3_monitor(name: str) -> bool:
    """Remove an Opera 3 monitor."""
    if name in _opera3_monitors:
        _opera3_monitors[name].stop_monitoring()
        del _opera3_monitors[name]
        return True
    return False
