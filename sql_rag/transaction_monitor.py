"""
Opera Transaction Monitor — real-time passive monitoring engine.

Connects to any Opera SQL Server with read-only access, polls for new
rows by ID watermark, groups related rows into transactions using
Opera's linking fields, classifies by type, and saves to the library.

Zero impact on the target system: SELECT WITH (NOLOCK) only.
"""
import logging
import threading
import time
import json
from typing import Dict, List, Optional, Any, Set
from datetime import datetime
from collections import defaultdict

logger = logging.getLogger(__name__)


# Transaction classification rules: table patterns → type
CLASSIFICATION_RULES = [
    # Cashbook types (check at_type on atran)
    {"tables": {"aentry", "atran"}, "check": "at_type", "rules": {
        4: "Sales Receipt",
        5: "Purchase Payment",
        3: "Sales Refund",
        6: "Purchase Refund",
        1: "Nominal Payment",
        2: "Nominal Receipt",
        8: "Bank Transfer",
    }},
    # Sales ledger (no aentry)
    {"tables": {"stran"}, "no_tables": {"aentry"}, "check": "st_trtype", "rules": {
        "I": "Sales Invoice",
        "C": "Sales Credit Note",
    }},
    # Purchase ledger (no aentry)
    {"tables": {"ptran"}, "no_tables": {"aentry"}, "check": "pt_trtype", "rules": {
        "I": "Purchase Invoice",
        "C": "Purchase Credit Note",
    }},
    # Sales orders
    {"tables": {"ihead"}, "classification": "Sales Order"},
    # Purchase orders
    {"tables": {"ohead"}, "classification": "Purchase Order"},
    # Nominal journal (ntran only, no cashbook)
    {"tables": {"ntran"}, "no_tables": {"aentry", "stran", "ptran"}, "classification": "Nominal Journal"},
    # Allocations
    {"tables": {"salloc"}, "classification": "Sales Allocation"},
    {"tables": {"palloc"}, "classification": "Purchase Allocation"},
    # Master records
    {"tables": {"sname"}, "no_tables": {"stran", "aentry"}, "classification": "Customer Master"},
    {"tables": {"pname"}, "no_tables": {"ptran", "aentry"}, "classification": "Supplier Master"},
]


class OperaMonitorConnection:
    """Manages a pyodbc connection to a target Opera SQL Server."""

    def __init__(self, server_host: str, database_name: str, username: str,
                 password: str, server_port: int = 1433):
        self.server_host = server_host
        self.server_port = server_port
        self.database_name = database_name
        self.username = username
        self.password = password
        self._conn = None

    def connect(self):
        import pyodbc
        # Auto-detect available SQL Server ODBC driver (same logic as sql_connector.py)
        driver = "ODBC Driver 17 for SQL Server"
        try:
            drivers = pyodbc.drivers()
            for d in drivers:
                if 'SQL Server' in d:
                    driver = d
                    break
        except Exception:
            pass

        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={self.server_host},{self.server_port};"
            f"DATABASE={self.database_name};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"TrustServerCertificate=yes;"
        )
        self._conn = pyodbc.connect(conn_str, timeout=10)
        self._conn.autocommit = True
        return True

    def close(self):
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    @property
    def connected(self):
        if not self._conn:
            return False
        try:
            cursor = self._conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except Exception:
            return False

    def execute_query(self, sql: str) -> List[Dict]:
        """Execute a read-only query and return rows as dicts."""
        if not self._conn:
            raise ConnectionError("Not connected")
        cursor = self._conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = []
        for row in cursor.fetchall():
            rows.append({col: self._serialize_value(val) for col, val in zip(columns, row)})
        cursor.close()
        return rows

    @staticmethod
    def _serialize_value(val):
        """Convert values to JSON-safe types."""
        if val is None:
            return None
        if isinstance(val, (int, float, bool)):
            return val
        if isinstance(val, bytes):
            return val.hex()
        if isinstance(val, datetime):
            return val.isoformat()
        if hasattr(val, 'isoformat'):
            return val.isoformat()
        return str(val).strip()

    def test_connection(self) -> Dict:
        """Test the connection and verify it's an Opera database."""
        try:
            self.connect()
            # Check for key Opera tables
            tables = self.execute_query("""
                SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """)
            table_names = {t['TABLE_NAME'].lower() for t in tables}
            opera_markers = {'aentry', 'atran', 'nparm', 'sname', 'pname', 'ntran'}
            found_markers = opera_markers & table_names

            if len(found_markers) < 4:
                return {"success": False, "error": "Database does not appear to be an Opera installation",
                        "tables_found": len(tables), "opera_tables_found": list(found_markers)}

            # Get company info if possible
            company_name = ""
            try:
                rows = self.execute_query("SELECT TOP 1 cp_name FROM compny WITH (NOLOCK)")
                if rows:
                    company_name = rows[0].get('cp_name', '')
            except Exception:
                pass

            return {
                "success": True,
                "tables_found": len(tables),
                "opera_tables_found": list(found_markers),
                "company_name": company_name,
                "message": f"Connected — {len(tables)} tables, Opera database confirmed"
            }
        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            self.close()

    def get_opera_users(self) -> List[Dict]:
        """Get legitimate Opera user codes from alogin."""
        try:
            return self.execute_query("""
                SELECT RTRIM(al_login) as code, RTRIM(al_name) as name
                FROM alogin WITH (NOLOCK)
            """)
        except Exception:
            return []

    def discover_tables_with_id(self) -> Dict[str, int]:
        """Discover all tables with an 'id' column and their current MAX(id)."""
        tables = self.execute_query("""
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS
            WHERE COLUMN_NAME = 'id' AND TABLE_SCHEMA = 'dbo'
            ORDER BY TABLE_NAME
        """)
        watermarks = {}
        for t in tables:
            table_name = t['TABLE_NAME']
            try:
                rows = self.execute_query(f"SELECT MAX(id) as max_id FROM [{table_name}] WITH (NOLOCK)")
                max_id = rows[0]['max_id'] if rows and rows[0]['max_id'] is not None else 0
                watermarks[table_name] = max_id
            except Exception:
                pass
        return watermarks

    def snapshot_all_tables(self) -> Dict[str, List[Dict]]:
        """
        Take a complete snapshot of ALL tables in the database.
        Used to detect UPDATEs on existing rows (not just INSERTs).
        Only snapshots tables small enough to be practical (< 50,000 rows).
        """
        tables = self.execute_query("""
            SELECT t.TABLE_NAME, p.rows as row_count
            FROM INFORMATION_SCHEMA.TABLES t
            JOIN sys.partitions p ON p.object_id = OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME)
                AND p.index_id IN (0, 1)
            WHERE t.TABLE_TYPE = 'BASE TABLE'
            ORDER BY t.TABLE_NAME
        """)
        snapshots = {}
        for t in tables:
            table_name = t['TABLE_NAME']
            row_count = t.get('row_count', 0) or 0
            if row_count > 50000:
                continue  # Skip very large tables — too slow to snapshot every cycle
            try:
                rows = self.execute_query(f"SELECT * FROM [{table_name}] WITH (NOLOCK)")
                snapshots[table_name] = rows
            except Exception:
                pass
        return snapshots

    def diff_snapshots(self, before: Dict[str, List[Dict]],
                       after: Dict[str, List[Dict]]) -> Dict[str, List[Dict]]:
        """
        Compare two full snapshots and return all changed rows across ALL tables.
        Detects field-level changes on existing rows (UPDATEs).
        New rows (INSERTs) are handled separately by watermark polling.

        Returns {table_name: [{"row_key": k, "changes": [{field, before, after}], "full_row_after": row}, ...]}
        """
        all_changes = {}
        for table in before:
            if table not in after:
                continue
            before_rows = before[table]
            after_rows = after[table]
            if not before_rows or not after_rows:
                continue

            # Use 'id' as primary key if available, otherwise first column
            key_col = 'id' if 'id' in before_rows[0] else list(before_rows[0].keys())[0]

            before_by_key = {}
            for r in before_rows:
                k = r.get(key_col)
                if k is not None:
                    before_by_key[str(k).strip()] = r

            for r in after_rows:
                k = r.get(key_col)
                if k is None:
                    continue
                key_str = str(k).strip()
                old = before_by_key.get(key_str)
                if old is None:
                    continue  # New row — handled by watermark polling

                # Compare every field
                row_changes = []
                for field in r:
                    old_val = old.get(field)
                    new_val = r[field]
                    if old_val != new_val:
                        row_changes.append({
                            "field": field,
                            "before": old_val,
                            "after": new_val,
                        })
                if row_changes:
                    if table not in all_changes:
                        all_changes[table] = []
                    all_changes[table].append({
                        "row_key": key_str,
                        "changes": row_changes,
                        "full_row_after": r,
                    })
        return all_changes


class TransactionMonitor:
    """
    Background monitor that polls an Opera database for new transactions.
    """

    def __init__(self, db):
        """
        Args:
            db: TransactionMonitorDB instance
        """
        self.db = db
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._connection: Optional[OperaMonitorConnection] = None
        self._connection_id: Optional[int] = None
        self._poll_interval: float = 5.0
        self._pending_groups: Dict[str, Dict] = {}  # Incomplete transaction groups
        self._opera_users: Set[str] = set()
        self._last_snapshot: Optional[Dict[str, List[Dict]]] = None  # For UPDATE detection
        self._stats = {
            "started_at": None,
            "total_captured": 0,
            "verified": 0,
            "suspicious": 0,
            "last_activity": None,
            "polls": 0,
            "errors": 0,
        }

    @property
    def is_running(self):
        return self._running

    @property
    def stats(self):
        return {**self._stats, "is_running": self._running, "connection_id": self._connection_id}

    def start(self, connection_id: int, poll_interval: float = 5.0):
        """Start monitoring a connection."""
        if self._running:
            self.stop()

        conn_info = self.db.get_connection(connection_id)
        if not conn_info:
            raise ValueError(f"Connection {connection_id} not found")

        self._connection_id = connection_id
        self._poll_interval = poll_interval
        self._connection = OperaMonitorConnection(
            server_host=conn_info['server_host'],
            database_name=conn_info['database_name'],
            username=conn_info['username'],
            password=conn_info['password_encrypted'],
            server_port=conn_info.get('server_port', 1433),
        )

        self._running = True
        self._stats = {
            "started_at": datetime.now().isoformat(),
            "total_captured": 0,
            "verified": 0,
            "suspicious": 0,
            "last_activity": None,
            "polls": 0,
            "errors": 0,
        }

        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()
        logger.info(f"Monitor started for connection {connection_id}: {conn_info['name']}")

    def stop(self):
        """Stop monitoring."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=10)
            self._thread = None
        if self._connection:
            self._connection.close()
            self._connection = None
        self._connection_id = None
        logger.info("Monitor stopped")

    def _poll_loop(self):
        """Main polling loop — runs in background thread."""
        retry_delay = 5
        max_retry = 60

        while self._running:
            try:
                # Ensure connected
                if not self._connection.connected:
                    logger.info("Connecting to target database...")
                    self._connection.connect()
                    retry_delay = 5  # Reset on successful connect

                    # Load Opera users for verification
                    users = self._connection.get_opera_users()
                    self._opera_users = {u['code'].strip().upper() for u in users if u.get('code')}
                    self.db.set_opera_users(self._connection_id,
                                            [{'code': u['code'], 'name': u.get('name', '')} for u in users])
                    logger.info(f"Loaded {len(self._opera_users)} Opera user codes")

                    # Initialize watermarks if needed
                    existing = self.db.get_watermarks(self._connection_id)
                    if not existing:
                        logger.info("Discovering tables and setting initial watermarks...")
                        watermarks = self._connection.discover_tables_with_id()
                        self.db.update_watermarks_batch(self._connection_id, watermarks)
                        logger.info(f"Initialized watermarks for {len(watermarks)} tables")

                    self.db.update_connection(self._connection_id,
                                              last_connected_at=datetime.now().isoformat(),
                                              is_active=1)

                    # Take initial snapshot of all tables for UPDATE detection
                    logger.info("Taking initial snapshot of all tables for change detection...")
                    self._last_snapshot = self._connection.snapshot_all_tables()
                    logger.info(f"Snapshotted {len(self._last_snapshot)} tables")

                # Poll for new rows (INSERTs) AND detect field changes (UPDATEs)
                self._poll_once()
                self._stats["polls"] += 1

            except Exception as e:
                self._stats["errors"] += 1
                logger.warning(f"Monitor poll error: {e}")
                if self._connection:
                    self._connection.close()
                # Exponential backoff on error
                time.sleep(min(retry_delay, max_retry))
                retry_delay = min(retry_delay * 2, max_retry)
                continue

            time.sleep(self._poll_interval)

        # Cleanup on stop
        if self._connection_id:
            self.db.update_connection(self._connection_id, is_active=0)

    def _poll_once(self):
        """Single poll cycle: check all tables for new rows."""
        watermarks = self.db.get_watermarks(self._connection_id)
        new_rows_by_table: Dict[str, List[Dict]] = {}

        for table_name, last_id in watermarks.items():
            try:
                rows = self._connection.execute_query(
                    f"SELECT * FROM [{table_name}] WITH (NOLOCK) WHERE id > {last_id} ORDER BY id"
                )
                if rows:
                    new_rows_by_table[table_name] = rows
                    new_max = max(r.get('id', last_id) for r in rows)
                    self.db.update_watermark(self._connection_id, table_name, new_max)
            except Exception:
                pass  # Table may not be readable, skip silently

        # Detect UPDATE changes by comparing full table snapshots
        update_changes: Dict[str, List[Dict]] = {}
        if self._last_snapshot is not None:
            try:
                current_snapshot = self._connection.snapshot_all_tables()
                update_changes = self._connection.diff_snapshots(self._last_snapshot, current_snapshot)
                self._last_snapshot = current_snapshot
            except Exception as e:
                logger.warning(f"Snapshot diff error: {e}")

        if new_rows_by_table or update_changes:
            self._process_new_rows(new_rows_by_table, update_changes)

        # Finalize any pending groups older than 30 seconds
        self._finalize_stale_groups()

    def _process_new_rows(self, new_rows_by_table: Dict[str, List[Dict]],
                          update_changes: Dict[str, List[Dict]] = None):
        """Group new rows into transactions, attach any UPDATE changes, and save."""
        groups = self._group_rows(new_rows_by_table)

        # Attach UPDATE changes to relevant groups (or create standalone change records)
        if update_changes:
            for group in groups:
                group["field_changes"] = update_changes  # All changes from this poll cycle
            # If there are UPDATE changes but no new rows, still record the changes
            if not groups and update_changes:
                groups.append({
                    "tables": {},
                    "timestamp": datetime.now().isoformat(),
                    "field_changes": update_changes,
                })

        for group in groups:
            self._save_group(group)

    def _group_rows(self, new_rows_by_table: Dict[str, List[Dict]]) -> List[Dict]:
        """
        Group related rows into transaction groups using Opera's linking fields.
        Returns list of groups, each with: tables, rows, linking_key.
        """
        groups = []

        # Strategy: start from primary tables and follow links
        # 1. Cashbook: start from aentry, follow to atran, ntran, etc.
        aentry_rows = new_rows_by_table.get('aentry', [])
        atran_rows = new_rows_by_table.get('atran', [])
        used_rows: Dict[str, Set[int]] = defaultdict(set)  # table -> set of row ids used

        for ae_row in aentry_rows:
            group = {"tables": {}, "timestamp": datetime.now().isoformat()}
            ae_entry = ae_row.get('ae_entry')
            ae_cbtype = ae_row.get('ae_cbtype')
            ae_acnt = ae_row.get('ae_acnt')

            group["tables"]["aentry"] = [ae_row]
            used_rows["aentry"].add(ae_row.get("id", 0))

            # Find matching atran rows
            matching_atran = [r for r in atran_rows
                              if r.get('at_entry') == ae_entry
                              and r.get('at_cbtype') == ae_cbtype
                              and r.get('at_acnt') == ae_acnt
                              and r.get('id', 0) not in used_rows['atran']]
            if matching_atran:
                group["tables"]["atran"] = matching_atran
                for r in matching_atran:
                    used_rows["atran"].add(r.get("id", 0))

            # Find matching ntran by journal number
            for tbl_name in ['anoml', 'ntran', 'stran', 'ptran', 'snoml', 'pnoml',
                              'nacnt', 'nbank', 'sname', 'pname', 'zvtran', 'nvat', 'nhist']:
                tbl_rows = new_rows_by_table.get(tbl_name, [])
                if not tbl_rows:
                    continue
                # Match by various linking fields
                matched = self._find_linked_rows(tbl_name, tbl_rows, group, used_rows)
                if matched:
                    group["tables"][tbl_name] = matched
                    for r in matched:
                        used_rows[tbl_name].add(r.get("id", 0))

            groups.append(group)

        # 2. Sales/Purchase ledger transactions (no aentry)
        for primary_table, type_field in [('stran', 'st_trtype'), ('ptran', 'pt_trtype')]:
            for row in new_rows_by_table.get(primary_table, []):
                if row.get('id', 0) in used_rows[primary_table]:
                    continue
                group = {"tables": {primary_table: [row]}, "timestamp": datetime.now().isoformat()}
                used_rows[primary_table].add(row.get("id", 0))

                # Follow to transfer files and ntran
                for tbl_name in ['snoml', 'pnoml', 'ntran', 'nacnt', 'sname', 'pname', 'zvtran', 'nvat', 'nhist']:
                    tbl_rows = new_rows_by_table.get(tbl_name, [])
                    if tbl_rows:
                        matched = self._find_linked_rows(tbl_name, tbl_rows, group, used_rows)
                        if matched:
                            group["tables"][tbl_name] = matched
                            for r in matched:
                                used_rows[tbl_name].add(r.get("id", 0))

                groups.append(group)

        # 3. Order processing
        for primary_table in ['ihead', 'ohead']:
            for row in new_rows_by_table.get(primary_table, []):
                if row.get('id', 0) in used_rows[primary_table]:
                    continue
                order_field = 'ih_order' if primary_table == 'ihead' else 'oh_order'
                line_table = 'iline' if primary_table == 'ihead' else 'oline'
                order_num = row.get(order_field)

                group = {"tables": {primary_table: [row]}, "timestamp": datetime.now().isoformat()}
                used_rows[primary_table].add(row.get("id", 0))

                # Find order lines
                line_rows = [r for r in new_rows_by_table.get(line_table, [])
                             if r.get(order_field.replace('h_', 'l_'), r.get(order_field)) == order_num
                             and r.get('id', 0) not in used_rows[line_table]]
                if line_rows:
                    group["tables"][line_table] = line_rows
                    for r in line_rows:
                        used_rows[line_table].add(r.get("id", 0))

                groups.append(group)

        # 4. Nominal journals (ntran only, not already claimed)
        ntran_unclaimed = [r for r in new_rows_by_table.get('ntran', [])
                           if r.get('id', 0) not in used_rows['ntran']]
        if ntran_unclaimed:
            # Group by journal number
            by_jrnl: Dict[Any, List] = defaultdict(list)
            for r in ntran_unclaimed:
                by_jrnl[r.get('nt_jrnl')].append(r)
            for jrnl, rows in by_jrnl.items():
                group = {"tables": {"ntran": rows}, "timestamp": datetime.now().isoformat()}
                for r in rows:
                    used_rows['ntran'].add(r.get("id", 0))
                groups.append(group)

        # 5. Master records (unclaimed sname/pname/nacnt/stitem)
        for master_table in ['sname', 'pname', 'nacnt', 'stitem', 'nbank']:
            for row in new_rows_by_table.get(master_table, []):
                if row.get('id', 0) in used_rows[master_table]:
                    continue
                group = {"tables": {master_table: [row]}, "timestamp": datetime.now().isoformat()}
                used_rows[master_table].add(row.get("id", 0))
                groups.append(group)

        # 6. Allocations
        for alloc_table in ['salloc', 'palloc']:
            for row in new_rows_by_table.get(alloc_table, []):
                if row.get('id', 0) in used_rows[alloc_table]:
                    continue
                group = {"tables": {alloc_table: [row]}, "timestamp": datetime.now().isoformat()}
                used_rows[alloc_table].add(row.get("id", 0))
                groups.append(group)

        return groups

    def _find_linked_rows(self, table_name: str, candidate_rows: List[Dict],
                          group: Dict, used_rows: Dict[str, Set[int]]) -> List[Dict]:
        """Find rows in candidate_rows that link to the existing group."""
        matched = []

        # Extract linking values from the group
        group_jrnls = set()
        group_accounts = set()
        group_refs = set()
        group_uniques = set()

        for tbl, rows in group["tables"].items():
            for r in rows:
                for key in ['nt_jrnl', 'at_jrnl']:
                    if r.get(key):
                        group_jrnls.add(r[key])
                for key in ['ae_acnt', 'at_acnt', 'st_account', 'pt_account']:
                    if r.get(key):
                        group_accounts.add(str(r[key]).strip())
                for key in ['at_unique', 'ax_unique', 'sx_unique', 'px_unique']:
                    if r.get(key):
                        group_uniques.add(str(r[key]).strip())
                for key in ['ae_entref', 'st_trref', 'pt_trref']:
                    if r.get(key):
                        group_refs.add(str(r[key]).strip())

        for r in candidate_rows:
            if r.get('id', 0) in used_rows.get(table_name, set()):
                continue

            # Try to match by journal number
            for key in ['nt_jrnl', 'ax_jrnl', 'sx_jrnl', 'px_jrnl']:
                if r.get(key) and r[key] in group_jrnls:
                    matched.append(r)
                    break
            else:
                # Try unique ID
                for key in ['at_unique', 'ax_unique', 'sx_unique', 'px_unique']:
                    if r.get(key) and str(r[key]).strip() in group_uniques:
                        matched.append(r)
                        break

        return matched

    def _finalize_stale_groups(self):
        """Finalize pending groups older than 30 seconds."""
        # For now, all groups are finalized immediately in _process_new_rows
        # This can be enhanced later for cross-poll-cycle grouping
        pass

    def _save_group(self, group: Dict):
        """Classify and save a transaction group with both INSERTs and UPDATEs."""
        tables_present = set(group["tables"].keys())
        all_rows = {}
        for tbl, rows in group["tables"].items():
            all_rows[tbl] = rows

        # Include field changes (UPDATEs on existing rows) — same detail as transaction snapshot tool
        field_changes = group.get("field_changes", {})
        if field_changes:
            tables_present.update(field_changes.keys())
            # Store changes alongside inserted rows so the full picture is captured
            all_rows["_field_changes"] = field_changes

        # Classify
        classification = self._classify(tables_present, all_rows)

        # Extract input_by
        input_by = ""
        for tbl in ['aentry', 'stran', 'ptran', 'ntran', 'ihead', 'ohead']:
            if tbl in all_rows:
                for r in all_rows[tbl]:
                    val = r.get('ae_input') or r.get('st_input') or r.get('pt_input') or \
                          r.get('nt_input') or r.get('ih_input') or r.get('oh_input') or ''
                    if val:
                        input_by = str(val).strip()
                        break
                if input_by:
                    break

        # Verify
        is_verified = input_by.upper() in self._opera_users if input_by else False
        is_suspicious = not is_verified
        suspicious_reason = ""
        if is_suspicious:
            if not input_by:
                suspicious_reason = "No input_by field found"
            else:
                suspicious_reason = f"input_by '{input_by}' not in Opera user list"

        # Build field summary — which fields were populated on new rows, which changed on existing
        field_summary = {
            "inserted_tables": {tbl: list(rows[0].keys()) if rows else [] for tbl, rows in group["tables"].items()},
            "updated_tables": {tbl: [
                {"row_key": c["row_key"], "fields_changed": [ch["field"] for ch in c["changes"]]}
                for c in changes
            ] for tbl, changes in field_changes.items()},
        }

        # Save
        self.db.save_transaction(
            connection_id=self._connection_id,
            transaction_type=classification,
            classification=classification,
            is_verified=is_verified,
            is_suspicious=is_suspicious,
            suspicious_reason=suspicious_reason,
            input_by=input_by,
            tables=list(tables_present),
            rows=all_rows,
            field_summary=field_summary,
        )

        self._stats["total_captured"] += 1
        if is_verified:
            self._stats["verified"] += 1
        else:
            self._stats["suspicious"] += 1
        self._stats["last_activity"] = datetime.now().isoformat()

        logger.info(f"Captured: {classification} (input_by={input_by}, verified={is_verified}, tables={list(tables_present)})")

    def _classify(self, tables_present: Set[str], all_rows: Dict) -> str:
        """Classify a transaction group by its table/field pattern."""
        for rule in CLASSIFICATION_RULES:
            required = rule.get("tables", set())
            excluded = rule.get("no_tables", set())

            if not required.issubset(tables_present):
                continue
            if excluded and excluded & tables_present:
                continue

            # Direct classification
            if "classification" in rule:
                return rule["classification"]

            # Check field value
            check_field = rule.get("check")
            if check_field:
                rules_map = rule.get("rules", {})
                for tbl in required:
                    for row in all_rows.get(tbl, []):
                        val = row.get(check_field)
                        if val in rules_map:
                            return rules_map[val]
                        # Try string/int conversion
                        try:
                            if int(val) in rules_map:
                                return rules_map[int(val)]
                        except (ValueError, TypeError):
                            pass
                        if str(val).strip() in rules_map:
                            return rules_map[str(val).strip()]

        return "Unknown"
