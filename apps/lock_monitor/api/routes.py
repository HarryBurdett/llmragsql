"""
Lock Monitor API routes.

Extracted from api/main.py — provides endpoints for both SQL Server (Opera SE)
and Opera 3 (FoxPro) lock monitoring.
"""

import logging
from datetime import datetime
from fastapi import APIRouter, Query

logger = logging.getLogger(__name__)

router = APIRouter()


# ============================================================
# SQL Server Lock Monitor Endpoints
# ============================================================

@router.post("/api/lock-monitor/connect")
async def lock_monitor_connect(
    name: str = Query(..., description="Name for this connection"),
    server: str = Query(..., description="SQL Server hostname"),
    database: str = Query(..., description="Database name"),
    port: str = Query("1433", description="SQL Server port"),
    username: str = Query(None, description="Username (optional for Windows auth)"),
    password: str = Query(None, description="Password (optional for Windows auth)"),
    driver: str = Query("ODBC Driver 18 for SQL Server", description="ODBC driver name")
):
    """
    Connect to a SQL Server instance for lock monitoring.
    Creates a named monitor that can be started/stopped.
    """
    try:
        from sql_rag.lock_monitor import get_monitor, save_monitor_config
        import urllib.parse

        # Build server string with port if not default
        server_with_port = f"{server}:{port}" if port and port != "1433" else server

        # Build connection string
        use_windows_auth = not (username and password)
        if username and password:
            encoded_password = urllib.parse.quote_plus(password)
            conn_str = (
                f"mssql+pyodbc://{username}:{encoded_password}@{server_with_port}/{database}"
                f"?driver={driver.replace(' ', '+')}&TrustServerCertificate=yes"
            )
        else:
            conn_str = (
                f"mssql+pyodbc://@{server_with_port}/{database}"
                f"?driver={driver.replace(' ', '+')}&trusted_connection=yes&TrustServerCertificate=yes"
            )

        monitor = get_monitor(name, conn_str)

        # Test connection by initializing table
        monitor.initialize_table()

        # Save config for persistence (connection string includes credentials for reconnect)
        save_monitor_config(
            name=name,
            connection_string=conn_str,
            server=server,
            port=port,
            database=database,
            username=username,
            use_windows_auth=use_windows_auth
        )

        return {
            "success": True,
            "name": name,
            "server": server,
            "database": database,
            "message": f"Connected to {server}/{database} as '{name}'"
        }

    except Exception as e:
        logger.error(f"Lock monitor connect error: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/lock-monitor/test-connection")
async def lock_monitor_test_connection(
    server: str = Query(..., description="SQL Server hostname"),
    port: str = Query("1433", description="SQL Server port"),
    username: str = Query(None, description="Username (optional for Windows auth)"),
    password: str = Query(None, description="Password (optional for Windows auth)"),
    driver: str = Query("ODBC Driver 18 for SQL Server", description="ODBC driver name")
):
    """
    Test SQL Server connection and return list of available databases (companies).
    Connects to 'master' database to list all user databases on the server.
    """
    try:
        from sqlalchemy import create_engine, text
        import urllib.parse

        # Build server string with port if not default
        server_with_port = f"{server}:{port}" if port and port != "1433" else server

        # Connect to master database to list all databases
        if username and password:
            encoded_password = urllib.parse.quote_plus(password)
            conn_str = (
                f"mssql+pyodbc://{username}:{encoded_password}@{server_with_port}/master"
                f"?driver={driver.replace(' ', '+')}&TrustServerCertificate=yes"
            )
        else:
            conn_str = (
                f"mssql+pyodbc://@{server_with_port}/master"
                f"?driver={driver.replace(' ', '+')}&trusted_connection=yes&TrustServerCertificate=yes"
            )

        # Test connection and list databases
        engine = create_engine(conn_str, connect_args={"timeout": 10})
        databases = []

        with engine.connect() as conn:
            # Get list of user databases that the user has access to
            result = conn.execute(text("""
                SELECT name
                FROM sys.databases
                WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
                  AND state_desc = 'ONLINE'
                  AND HAS_DBACCESS(name) = 1
                ORDER BY name
            """))

            for row in result:
                db_name = row[0]
                databases.append({
                    "code": db_name,
                    "name": db_name
                })

        engine.dispose()

        if not databases:
            return {
                "success": False,
                "error": "No user databases found on server",
                "databases": []
            }

        return {
            "success": True,
            "message": f"Found {len(databases)} databases",
            "databases": databases
        }

    except Exception as e:
        logger.error(f"Lock monitor test connection error: {e}")
        return {"success": False, "error": str(e), "databases": []}


@router.post("/api/lock-monitor/{name}/start")
async def lock_monitor_start(
    name: str,
    poll_interval: int = Query(5, description="Seconds between polls"),
    min_wait_time: int = Query(1000, description="Minimum wait time (ms) to log")
):
    """Start lock monitoring for a named connection."""
    try:
        from sql_rag.lock_monitor import get_monitor

        monitor = get_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found. Connect first."}

        if monitor.is_monitoring:
            return {"success": True, "message": "Monitoring already running", "status": "running"}

        monitor.start_monitoring(poll_interval=poll_interval, min_wait_time=min_wait_time)

        return {
            "success": True,
            "name": name,
            "status": "running",
            "poll_interval": poll_interval,
            "min_wait_time": min_wait_time
        }

    except Exception as e:
        logger.error(f"Lock monitor start error: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/lock-monitor/{name}/stop")
async def lock_monitor_stop(name: str):
    """Stop lock monitoring for a named connection."""
    try:
        from sql_rag.lock_monitor import get_monitor

        monitor = get_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        monitor.stop_monitoring()

        return {"success": True, "name": name, "status": "stopped"}

    except Exception as e:
        logger.error(f"Lock monitor stop error: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/lock-monitor/{name}/status")
async def lock_monitor_status(name: str):
    """Get status of a named lock monitor."""
    try:
        from sql_rag.lock_monitor import get_monitor

        monitor = get_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        return {
            "success": True,
            "name": name,
            "is_monitoring": monitor.is_monitoring
        }

    except Exception as e:
        logger.error(f"Lock monitor status error: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/lock-monitor/{name}/disconnect")
async def lock_monitor_disconnect(name: str):
    """
    Disconnect from monitor without deleting the saved configuration.
    Stops monitoring and closes the connection, but keeps config for reconnection.
    """
    try:
        from sql_rag.lock_monitor import get_monitor, _monitors

        monitor = get_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        # Stop monitoring if active
        if monitor.is_monitoring:
            monitor.stop_monitoring()

        # Dispose the engine to close all connections
        if monitor._engine:
            monitor._engine.dispose()
            monitor._engine = None

        # Remove from active monitors (but keep saved config)
        if name in _monitors:
            del _monitors[name]

        return {
            "success": True,
            "name": name,
            "message": f"Disconnected from '{name}'. Configuration saved for reconnection."
        }

    except Exception as e:
        logger.error(f"Lock monitor disconnect error: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/lock-monitor/{name}/current")
async def lock_monitor_current_locks(name: str):
    """Get current blocking events (live snapshot)."""
    try:
        from sql_rag.lock_monitor import get_monitor
        from dataclasses import asdict

        monitor = get_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        events = monitor.get_current_locks()

        return {
            "success": True,
            "name": name,
            "timestamp": datetime.now().isoformat(),
            "event_count": len(events),
            "events": [
                {
                    "blocked_session": e.blocked_session,
                    "blocking_session": e.blocking_session,
                    "blocked_user": e.blocked_user,
                    "blocking_user": e.blocking_user,
                    "table_name": e.table_name,
                    "lock_type": e.lock_type,
                    "wait_time_ms": e.wait_time_ms,
                    "blocked_query": e.blocked_query[:500] if e.blocked_query else "",
                    "blocking_query": e.blocking_query[:500] if e.blocking_query else "",
                    # Enhanced details
                    "database_name": e.database_name,
                    "schema_name": e.schema_name,
                    "index_name": e.index_name,
                    "resource_type": e.resource_type,
                    "resource_description": e.resource_description,
                    "lock_mode": e.lock_mode,
                    "blocking_lock_mode": e.blocking_lock_mode
                }
                for e in events
            ]
        }

    except Exception as e:
        logger.error(f"Lock monitor current locks error: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/lock-monitor/{name}/summary")
async def lock_monitor_summary(
    name: str,
    hours: int = Query(24, description="Number of hours to include in summary")
):
    """Get summary report of lock events."""
    try:
        from sql_rag.lock_monitor import get_monitor
        from dataclasses import asdict

        monitor = get_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        summary = monitor.get_summary(hours=hours)

        return {
            "success": True,
            "name": name,
            "hours": hours,
            "summary": asdict(summary)
        }

    except Exception as e:
        logger.error(f"Lock monitor summary error: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/lock-monitor/list")
async def lock_monitor_list():
    """List all configured lock monitors and saved configs."""
    try:
        from sql_rag.lock_monitor import list_monitors, get_monitor, get_saved_configs

        # Active monitors
        monitors = []
        active_names = set()
        for name in list_monitors():
            monitor = get_monitor(name)
            monitors.append({
                "name": name,
                "is_monitoring": monitor.is_monitoring if monitor else False,
                "connected": True
            })
            active_names.add(name)

        # Saved configs that aren't currently connected (need password re-entry)
        saved_configs = get_saved_configs()
        for config in saved_configs:
            if config['name'] not in active_names:
                monitors.append({
                    "name": config['name'],
                    "is_monitoring": False,
                    "connected": False,
                    "server": config.get('server'),
                    "database": config.get('database_name'),
                    "username": config.get('username'),
                    "use_windows_auth": bool(config.get('use_windows_auth')),
                    "needs_password": not bool(config.get('use_windows_auth'))
                })

        return {"success": True, "monitors": monitors}

    except Exception as e:
        logger.error(f"Lock monitor list error: {e}")
        return {"success": False, "error": str(e)}


@router.delete("/api/lock-monitor/{name}")
async def lock_monitor_remove(name: str):
    """Remove a lock monitor connection."""
    try:
        from sql_rag.lock_monitor import remove_monitor

        if remove_monitor(name):
            return {"success": True, "message": f"Monitor '{name}' removed"}
        else:
            return {"success": False, "error": f"Monitor '{name}' not found"}

    except Exception as e:
        logger.error(f"Lock monitor remove error: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/lock-monitor/{name}/clear-old")
async def lock_monitor_clear_old(
    name: str,
    days: int = Query(30, description="Keep events from last N days")
):
    """Clear lock events older than specified days."""
    try:
        from sql_rag.lock_monitor import get_monitor

        monitor = get_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        deleted = monitor.clear_old_events(days=days)

        return {
            "success": True,
            "name": name,
            "deleted_count": deleted,
            "kept_days": days
        }

    except Exception as e:
        logger.error(f"Lock monitor clear old error: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/lock-monitor/{name}/connections")
async def lock_monitor_connections(name: str):
    """
    Get ALL current connections to the database.
    Shows which services/applications are connected - useful for identifying
    what needs to be stopped before exclusive operations like database restores.
    """
    try:
        from sql_rag.lock_monitor import get_monitor

        monitor = get_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        summary = monitor.get_connections_summary()

        return {
            "success": True,
            "name": name,
            "total_connections": summary['total_connections'],
            "by_program": summary['by_program'],
            "by_host": summary['by_host'],
            "connections": summary['connections']
        }

    except Exception as e:
        logger.error(f"Lock monitor connections error: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/lock-monitor/{name}/kill-connections")
async def lock_monitor_kill_connections(
    name: str,
    exclude_self: bool = Query(True, description="Exclude the current connection from being killed")
):
    """
    Kill all connections to the database to prepare for exclusive operations like restore.

    WARNING: This will forcefully disconnect all users from the database.
    Use with caution - any uncommitted transactions will be rolled back.
    """
    try:
        from sql_rag.lock_monitor import get_monitor
        from sqlalchemy import text

        monitor = get_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        engine = monitor._get_engine()

        # Get database name from current connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT DB_NAME() as db_name"))
            database_name = result.fetchone()[0]

        # Get current connections before killing
        summary_before = monitor.get_connections_summary()
        connections_before = summary_before['total_connections']

        # Kill connections using SQL Server commands
        # Get list of SPIDs to kill (excluding system processes and our own)
        spid_query = """
            SELECT session_id, program_name, host_name, login_name
            FROM sys.dm_exec_sessions
            WHERE is_user_process = 1
              AND session_id != @@SPID
              AND session_id > 50
        """

        connections_to_kill = []
        killed_count = 0
        failed_count = 0
        errors = []

        with engine.connect() as conn:
            result = conn.execute(text(spid_query))
            rows = result.fetchall()

            for row in rows:
                spid = row[0]
                program = row[1] or 'Unknown'
                host = row[2] or 'Unknown'
                login = row[3] or 'Unknown'

                connections_to_kill.append({
                    'session_id': spid,
                    'program': program,
                    'host': host,
                    'login': login
                })

                # Kill the connection
                try:
                    kill_query = f"KILL {spid}"
                    conn.execute(text(kill_query))
                    killed_count += 1
                except Exception as kill_error:
                    failed_count += 1
                    errors.append(f"SPID {spid}: {str(kill_error)}")

            conn.commit()

        # Get connections after killing
        summary_after = monitor.get_connections_summary()
        connections_after = summary_after['total_connections']

        return {
            "success": True,
            "name": name,
            "database": database_name,
            "connections_before": connections_before,
            "connections_killed": killed_count,
            "connections_failed": failed_count,
            "connections_after": connections_after,
            "killed_sessions": connections_to_kill[:20],  # Limit detail for large lists
            "errors": errors[:10] if errors else None,
            "message": f"Killed {killed_count} connections. {connections_after} connections remaining."
        }

    except Exception as e:
        logger.error(f"Lock monitor kill connections error: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/lock-monitor/{name}/set-single-user")
async def lock_monitor_set_single_user(name: str):
    """
    Set database to SINGLE_USER mode with ROLLBACK IMMEDIATE.
    This kills all other connections and prevents new ones.
    Use this before restore operations.
    """
    try:
        from sql_rag.lock_monitor import get_monitor
        from sqlalchemy import text

        monitor = get_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        engine = monitor._get_engine()

        # Get database name first
        with engine.connect() as conn:
            result = conn.execute(text("SELECT DB_NAME() as db_name"))
            database_name = result.fetchone()[0]

        # ALTER DATABASE must run outside of a transaction - use autocommit
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            alter_query = f"ALTER DATABASE [{database_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
            conn.execute(text(alter_query))

        return {
            "success": True,
            "name": name,
            "database": database_name,
            "mode": "SINGLE_USER",
            "message": f"Database {database_name} set to SINGLE_USER mode. All other connections terminated."
        }

    except Exception as e:
        logger.error(f"Lock monitor set single user error: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/lock-monitor/{name}/set-multi-user")
async def lock_monitor_set_multi_user(name: str, database: str = Query(None)):
    """
    Set database back to MULTI_USER mode after restore operations.
    Connects via master database to avoid SINGLE_USER lock issues.
    """
    try:
        from sql_rag.lock_monitor import get_monitor, get_saved_configs
        from sqlalchemy import text, create_engine
        import re

        monitor = get_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        # Get database name from parameter, config, or connection string
        database_name = database
        if not database_name:
            # Try to get from saved config
            configs = get_saved_configs()
            for cfg in configs:
                if cfg.get('name') == name:
                    database_name = cfg.get('database_name')
                    break

        if not database_name:
            # Parse from connection string
            conn_str = monitor.connection_string
            match = re.search(r'Database=([^;]+)', conn_str, re.IGNORECASE)
            if match:
                database_name = match.group(1)

        if not database_name:
            return {"success": False, "error": "Could not determine database name. Please provide 'database' parameter."}

        # Create a connection to master database to run ALTER DATABASE
        # Replace the database in connection string with master
        master_conn_str = re.sub(
            r'Database=[^;]+',
            'Database=master',
            monitor.connection_string,
            flags=re.IGNORECASE
        )
        master_engine = create_engine(master_conn_str)

        # ALTER DATABASE must run outside of a transaction - use autocommit
        with master_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            alter_query = f"ALTER DATABASE [{database_name}] SET MULTI_USER"
            conn.execute(text(alter_query))

        master_engine.dispose()

        return {
            "success": True,
            "name": name,
            "database": database_name,
            "mode": "MULTI_USER",
            "message": f"Database {database_name} set to MULTI_USER mode. Normal operations can resume."
        }

    except Exception as e:
        logger.error(f"Lock monitor set multi user error: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/lock-monitor/{name}/blocking-services")
async def lock_monitor_blocking_services(
    name: str,
    hours: int = Query(24, description="Hours of history to analyze")
):
    """
    Get summary of which services/programs have been causing locks.
    Helps identify problematic services that may need configuration changes
    or to be stopped during maintenance.
    """
    try:
        from sql_rag.lock_monitor import get_monitor

        monitor = get_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        summary = monitor.get_summary(hours=hours)

        return {
            "success": True,
            "name": name,
            "hours_analyzed": hours,
            "total_lock_events": summary.total_events,
            "blocking_services": summary.most_blocking_programs,
            "blocking_users": summary.most_blocking_users,
            "affected_tables": summary.most_blocked_tables
        }

    except Exception as e:
        logger.error(f"Lock monitor blocking services error: {e}")
        return {"success": False, "error": str(e)}


# ============================================================
# Opera 3 Lock Monitor Endpoints
# ============================================================

@router.post("/api/opera3-lock-monitor/connect")
async def opera3_lock_monitor_connect(
    name: str = Query(..., description="Name for this monitor"),
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """Connect to an Opera 3 installation for file lock monitoring."""
    try:
        from sql_rag.opera3_lock_monitor import get_opera3_monitor

        monitor = get_opera3_monitor(name, data_path)
        monitor.initialize()

        return {
            "success": True,
            "name": name,
            "data_path": data_path,
            "message": f"Connected to Opera 3 at {data_path}"
        }

    except Exception as e:
        logger.error(f"Opera 3 lock monitor connect error: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/opera3-lock-monitor/list-companies")
async def opera3_lock_monitor_list_companies(
    base_path: str = Query(..., description="Path to Opera 3 installation (e.g., C:\\Apps\\O3 Server VFP)")
):
    """
    List available companies from an Opera 3 installation.
    Reads from seqco.dbf in the System folder.
    """
    try:
        from sql_rag.opera3_foxpro import Opera3System

        system = Opera3System(base_path)
        companies = system.get_companies()

        if not companies:
            return {
                "success": False,
                "error": "No companies found in Opera 3 installation",
                "companies": []
            }

        return {
            "success": True,
            "message": f"Found {len(companies)} companies",
            "companies": [
                {
                    "code": c["code"],
                    "name": c["name"],
                    "data_path": c.get("data_path", "")
                }
                for c in companies
            ]
        }

    except ImportError:
        return {"success": False, "error": "dbfread package not installed", "companies": []}
    except FileNotFoundError as e:
        return {"success": False, "error": f"Path not found: {str(e)}", "companies": []}
    except Exception as e:
        logger.error(f"Opera 3 list companies error: {e}")
        return {"success": False, "error": str(e), "companies": []}


@router.post("/api/opera3-lock-monitor/{name}/start")
async def opera3_lock_monitor_start(
    name: str,
    poll_interval: int = Query(5, description="Seconds between polls")
):
    """Start Opera 3 file lock monitoring."""
    try:
        from sql_rag.opera3_lock_monitor import get_opera3_monitor

        monitor = get_opera3_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found. Connect first."}

        if monitor.is_monitoring:
            return {"success": True, "message": "Monitoring already running", "status": "running"}

        monitor.start_monitoring(poll_interval=poll_interval)

        return {
            "success": True,
            "name": name,
            "status": "running",
            "poll_interval": poll_interval
        }

    except Exception as e:
        logger.error(f"Opera 3 lock monitor start error: {e}")
        return {"success": False, "error": str(e)}


@router.post("/api/opera3-lock-monitor/{name}/stop")
async def opera3_lock_monitor_stop(name: str):
    """Stop Opera 3 file lock monitoring."""
    try:
        from sql_rag.opera3_lock_monitor import get_opera3_monitor

        monitor = get_opera3_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        monitor.stop_monitoring()

        return {"success": True, "name": name, "status": "stopped"}

    except Exception as e:
        logger.error(f"Opera 3 lock monitor stop error: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/opera3-lock-monitor/{name}/status")
async def opera3_lock_monitor_status(name: str):
    """Get status of Opera 3 lock monitor."""
    try:
        from sql_rag.opera3_lock_monitor import get_opera3_monitor

        monitor = get_opera3_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        return {
            "success": True,
            "name": name,
            "is_monitoring": monitor.is_monitoring,
            "data_path": str(monitor.data_path)
        }

    except Exception as e:
        logger.error(f"Opera 3 lock monitor status error: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/opera3-lock-monitor/{name}/current")
async def opera3_lock_monitor_current(name: str):
    """Get current file locks on Opera 3 files."""
    try:
        from sql_rag.opera3_lock_monitor import get_opera3_monitor

        monitor = get_opera3_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        events = monitor.get_current_locks()

        return {
            "success": True,
            "name": name,
            "timestamp": datetime.now().isoformat(),
            "event_count": len(events),
            "events": [
                {
                    "file_name": e.file_name,
                    "table_name": e.table_name,
                    "process": e.process_name,
                    "process_id": e.process_id,
                    "lock_type": e.lock_type,
                    "user": e.user
                }
                for e in events
            ]
        }

    except Exception as e:
        logger.error(f"Opera 3 lock monitor current error: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/opera3-lock-monitor/{name}/summary")
async def opera3_lock_monitor_summary(
    name: str,
    hours: int = Query(24, description="Number of hours to include")
):
    """Get summary of Opera 3 file lock activity."""
    try:
        from sql_rag.opera3_lock_monitor import get_opera3_monitor
        from dataclasses import asdict

        monitor = get_opera3_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        summary = monitor.get_summary(hours=hours)

        return {
            "success": True,
            "name": name,
            "hours": hours,
            "summary": asdict(summary)
        }

    except Exception as e:
        logger.error(f"Opera 3 lock monitor summary error: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/opera3-lock-monitor/list")
async def opera3_lock_monitor_list():
    """List all Opera 3 lock monitors."""
    try:
        from sql_rag.opera3_lock_monitor import list_opera3_monitors, get_opera3_monitor

        monitors = []
        for name in list_opera3_monitors():
            monitor = get_opera3_monitor(name)
            monitors.append({
                "name": name,
                "is_monitoring": monitor.is_monitoring if monitor else False,
                "data_path": str(monitor.data_path) if monitor else None
            })

        return {"success": True, "monitors": monitors}

    except Exception as e:
        logger.error(f"Opera 3 lock monitor list error: {e}")
        return {"success": False, "error": str(e)}


@router.delete("/api/opera3-lock-monitor/{name}")
async def opera3_lock_monitor_remove(name: str):
    """Remove an Opera 3 lock monitor."""
    try:
        from sql_rag.opera3_lock_monitor import remove_opera3_monitor

        if remove_opera3_monitor(name):
            return {"success": True, "message": f"Monitor '{name}' removed"}
        else:
            return {"success": False, "error": f"Monitor '{name}' not found"}

    except Exception as e:
        logger.error(f"Opera 3 lock monitor remove error: {e}")
        return {"success": False, "error": str(e)}
