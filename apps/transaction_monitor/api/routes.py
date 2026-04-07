"""
Transaction Monitor API — endpoints for managing connections,
starting/stopping the monitor, and viewing captured transactions.
"""
from fastapi import APIRouter, Body, Query, HTTPException
from typing import Optional, Dict, List
import logging

from sql_rag.transaction_monitor_db import TransactionMonitorDB
from sql_rag.transaction_monitor import TransactionMonitor, OperaMonitorConnection

logger = logging.getLogger(__name__)
router = APIRouter()

# Singleton instances
_db: Optional[TransactionMonitorDB] = None
_monitor: Optional[TransactionMonitor] = None


def _get_db() -> TransactionMonitorDB:
    global _db
    if _db is None:
        _db = TransactionMonitorDB()
    return _db


def _get_monitor() -> TransactionMonitor:
    global _monitor
    if _monitor is None:
        _monitor = TransactionMonitor(db=_get_db())
    return _monitor


# Transaction type checklist — what we're trying to capture
TRANSACTION_TYPES = [
    {"category": "Cashbook", "types": [
        "Sales Receipt", "Purchase Payment", "Sales Refund", "Purchase Refund",
        "Nominal Payment", "Nominal Receipt", "Bank Transfer",
    ]},
    {"category": "Sales Ledger", "types": [
        "Sales Invoice", "Sales Credit Note", "Sales Allocation",
    ]},
    {"category": "Purchase Ledger", "types": [
        "Purchase Invoice", "Purchase Credit Note", "Purchase Allocation",
    ]},
    {"category": "Nominal", "types": ["Nominal Journal"]},
    {"category": "Sales Order Processing", "types": ["Sales Order"]},
    {"category": "Purchase Order Processing", "types": ["Purchase Order"]},
    {"category": "Stock", "types": ["Stock Adjustment", "Stock Transfer"]},
    {"category": "Master Records", "types": ["Customer Master", "Supplier Master"]},
]


# ============================================================
# Connection management
# ============================================================

@router.get("/api/monitor/connections")
async def list_connections():
    """List all saved monitor connections."""
    db = _get_db()
    return {"success": True, "connections": db.list_connections()}


@router.post("/api/monitor/connections")
async def add_connection(
    name: str = Body(...),
    server_host: str = Body(...),
    database_name: str = Body(...),
    username: str = Body(...),
    password: str = Body(...),
    server_port: int = Body(1433),
):
    """Add a new monitor connection."""
    db = _get_db()
    cid = db.add_connection(name, server_host, database_name, username, password, server_port)
    return {"success": True, "connection_id": cid, "message": f"Connection '{name}' saved"}


@router.put("/api/monitor/connections/{connection_id}")
async def update_connection(
    connection_id: int,
    name: str = Body(None),
    server_host: str = Body(None),
    database_name: str = Body(None),
    username: str = Body(None),
    password: str = Body(None),
    server_port: int = Body(None),
):
    """Update an existing connection."""
    db = _get_db()
    updates = {k: v for k, v in {
        "name": name, "server_host": server_host, "database_name": database_name,
        "username": username, "password_encrypted": password, "server_port": server_port,
    }.items() if v is not None}
    if updates:
        db.update_connection(connection_id, **updates)
    return {"success": True, "message": "Connection updated"}


@router.delete("/api/monitor/connections/{connection_id}")
async def delete_connection(connection_id: int):
    """Delete a connection and all its data."""
    monitor = _get_monitor()
    if monitor.is_running and monitor._connection_id == connection_id:
        monitor.stop()
    db = _get_db()
    db.delete_connection(connection_id)
    return {"success": True, "message": "Connection deleted"}


@router.post("/api/monitor/connections/{connection_id}/test")
async def test_connection(connection_id: int):
    """Test a saved connection — verifies it's an Opera database."""
    db = _get_db()
    conn_info = db.get_connection(connection_id)
    if not conn_info:
        return {"success": False, "error": "Connection not found"}

    mc = OperaMonitorConnection(
        server_host=conn_info['server_host'],
        database_name=conn_info['database_name'],
        username=conn_info['username'],
        password=conn_info['password_encrypted'],
        server_port=conn_info.get('server_port', 1433),
    )
    result = mc.test_connection()

    # If successful, also load Opera users
    if result.get("success"):
        try:
            mc.connect()
            users = mc.get_opera_users()
            db.set_opera_users(connection_id, users)
            result["opera_users"] = len(users)
            mc.close()
        except Exception:
            pass

    return result


# ============================================================
# Monitor control
# ============================================================

@router.post("/api/monitor/start")
async def start_monitor(
    connection_id: int = Body(...),
    poll_interval: float = Body(5.0),
):
    """Start monitoring a connection."""
    monitor = _get_monitor()
    try:
        monitor.start(connection_id, poll_interval)
        return {"success": True, "message": "Monitor started"}
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.post("/api/monitor/stop")
async def stop_monitor():
    """Stop the current monitor."""
    monitor = _get_monitor()
    monitor.stop()
    return {"success": True, "message": "Monitor stopped"}


@router.get("/api/monitor/status")
async def get_monitor_status():
    """Get current monitor status and stats."""
    monitor = _get_monitor()
    stats = monitor.stats

    # Add connection name if active
    if stats.get("connection_id"):
        db = _get_db()
        conn = db.get_connection(stats["connection_id"])
        if conn:
            stats["connection_name"] = conn["name"]

    return {"success": True, **stats}


@router.post("/api/monitor/scan-now")
async def scan_now(connection_id: int = Body(...)):
    """Run a single poll cycle on demand (doesn't require monitor running)."""
    db = _get_db()
    conn_info = db.get_connection(connection_id)
    if not conn_info:
        return {"success": False, "error": "Connection not found"}

    monitor = _get_monitor()
    if monitor.is_running:
        return {"success": False, "error": "Monitor is already running — use the background monitor"}

    try:
        mc = OperaMonitorConnection(
            server_host=conn_info['server_host'],
            database_name=conn_info['database_name'],
            username=conn_info['username'],
            password=conn_info['password_encrypted'],
            server_port=conn_info.get('server_port', 1433),
        )
        mc.connect()

        # Initialize watermarks if needed
        existing = db.get_watermarks(connection_id)
        if not existing:
            watermarks = mc.discover_tables_with_id()
            db.update_watermarks_batch(connection_id, watermarks)

        # Load opera users
        users = mc.get_opera_users()
        monitor._opera_users = {u['code'].strip().upper() for u in users if u.get('code')}
        monitor._connection = mc
        monitor._connection_id = connection_id

        # Single poll
        before_count = monitor._stats["total_captured"]
        monitor._poll_once()
        captured = monitor._stats["total_captured"] - before_count

        mc.close()
        monitor._connection = None
        monitor._connection_id = None

        return {"success": True, "message": f"Scan complete — {captured} transaction(s) captured"}
    except Exception as e:
        return {"success": False, "error": str(e)}


# ============================================================
# Captured transactions
# ============================================================

@router.get("/api/monitor/transactions")
async def list_transactions(
    connection_id: Optional[int] = Query(None),
    transaction_type: Optional[str] = Query(None),
    verified_only: bool = Query(False),
    limit: int = Query(100),
):
    """List captured transactions."""
    db = _get_db()
    txns = db.get_captured_transactions(
        connection_id=connection_id,
        transaction_type=transaction_type,
        verified_only=verified_only,
        limit=limit,
    )
    # Parse JSON fields
    for t in txns:
        for json_field in ['tables_json', 'rows_json', 'field_summary_json']:
            if t.get(json_field):
                try:
                    t[json_field.replace('_json', '')] = __import__('json').loads(t[json_field])
                except Exception:
                    t[json_field.replace('_json', '')] = None
            del t[json_field]
    return {"success": True, "transactions": txns, "count": len(txns)}


@router.get("/api/monitor/transactions/{transaction_id}")
async def get_transaction(transaction_id: int):
    """Get full detail of a captured transaction."""
    db = _get_db()
    txns = db.get_captured_transactions(limit=1)
    # Get by ID
    conn = db._get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM captured_transactions WHERE id = ?", (transaction_id,))
    row = cursor.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Transaction not found")
    result = dict(row)
    import json
    for json_field in ['tables_json', 'rows_json', 'field_summary_json']:
        if result.get(json_field):
            try:
                result[json_field.replace('_json', '')] = json.loads(result[json_field])
            except Exception:
                result[json_field.replace('_json', '')] = None
        if json_field in result:
            del result[json_field]
    return {"success": True, "transaction": result}


@router.get("/api/monitor/suspicious")
async def list_suspicious(
    connection_id: Optional[int] = Query(None),
    limit: int = Query(50),
):
    """List suspicious (unverified) transactions for review."""
    db = _get_db()
    txns = db.get_suspicious_transactions(connection_id=connection_id, limit=limit)
    return {"success": True, "transactions": txns, "count": len(txns)}


# ============================================================
# Coverage dashboard
# ============================================================

@router.get("/api/monitor/coverage")
async def get_coverage(connection_id: Optional[int] = Query(None)):
    """Get transaction type coverage — what's captured vs missing."""
    db = _get_db()
    captured = db.get_coverage_summary(connection_id=connection_id)

    checklist = []
    total_types = 0
    captured_types = 0

    for category in TRANSACTION_TYPES:
        for type_name in category["types"]:
            total_types += 1
            count = captured.get(type_name, 0)
            if count > 0:
                captured_types += 1
            checklist.append({
                "category": category["category"],
                "type": type_name,
                "captured": count,
                "status": "captured" if count > 0 else "missing",
            })

    # Include any captured types not in the checklist (unexpected types)
    known_types = {t for cat in TRANSACTION_TYPES for t in cat["types"]}
    for type_name, count in captured.items():
        if type_name not in known_types and type_name != "Unknown":
            checklist.append({
                "category": "Other",
                "type": type_name,
                "captured": count,
                "status": "captured",
            })

    return {
        "success": True,
        "total_types": total_types,
        "captured_types": captured_types,
        "coverage_pct": round(captured_types / total_types * 100) if total_types else 0,
        "checklist": checklist,
    }
