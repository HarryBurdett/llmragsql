"""
SQLite database for the Opera Transaction Monitor.
Stores connection settings, watermarks, and captured transactions.
Separate from the main app's databases — this is a development/admin tool.
"""
import sqlite3
import json
from typing import List, Dict, Any, Optional
from pathlib import Path
from datetime import datetime


class TransactionMonitorDB:
    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = str(Path(__file__).parent.parent / "data" / "transaction_monitor.db")
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_db(self):
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS monitor_connections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                server_host TEXT NOT NULL,
                server_port INTEGER DEFAULT 1433,
                database_name TEXT NOT NULL,
                username TEXT NOT NULL,
                password_encrypted TEXT NOT NULL,
                is_active INTEGER DEFAULT 0,
                last_connected_at DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS table_watermarks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connection_id INTEGER NOT NULL,
                table_name TEXT NOT NULL,
                last_id INTEGER DEFAULT 0,
                row_count INTEGER DEFAULT 0,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(connection_id, table_name)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS captured_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connection_id INTEGER NOT NULL,
                transaction_type TEXT,
                classification TEXT,
                is_verified INTEGER DEFAULT 0,
                is_suspicious INTEGER DEFAULT 0,
                suspicious_reason TEXT,
                input_by TEXT,
                tables_json TEXT,
                rows_json TEXT,
                field_summary_json TEXT,
                captured_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS opera_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connection_id INTEGER NOT NULL,
                user_code TEXT NOT NULL,
                user_name TEXT,
                UNIQUE(connection_id, user_code)
            )
        """)

        # Indices
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_watermarks_conn ON table_watermarks(connection_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_captured_conn ON captured_transactions(connection_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_captured_type ON captured_transactions(transaction_type)")

        conn.commit()
        conn.close()

    # --- Connection management ---

    def add_connection(self, name: str, server_host: str, database_name: str,
                       username: str, password: str, server_port: int = 1433) -> int:
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO monitor_connections (name, server_host, server_port, database_name, username, password_encrypted)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (name, server_host, server_port, database_name, username, password))
        conn.commit()
        cid = cursor.lastrowid
        conn.close()
        return cid

    def get_connection(self, connection_id: int) -> Optional[Dict]:
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM monitor_connections WHERE id = ?", (connection_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None

    def list_connections(self) -> List[Dict]:
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM monitor_connections ORDER BY name")
        rows = [dict(r) for r in cursor.fetchall()]
        conn.close()
        return rows

    def update_connection(self, connection_id: int, **kwargs):
        conn = self._get_connection()
        sets = []
        values = []
        for key, val in kwargs.items():
            sets.append(f"{key} = ?")
            values.append(val)
        values.append(connection_id)
        conn.execute(f"UPDATE monitor_connections SET {', '.join(sets)} WHERE id = ?", values)
        conn.commit()
        conn.close()

    def delete_connection(self, connection_id: int):
        conn = self._get_connection()
        conn.execute("DELETE FROM table_watermarks WHERE connection_id = ?", (connection_id,))
        conn.execute("DELETE FROM captured_transactions WHERE connection_id = ?", (connection_id,))
        conn.execute("DELETE FROM opera_users WHERE connection_id = ?", (connection_id,))
        conn.execute("DELETE FROM monitor_connections WHERE id = ?", (connection_id,))
        conn.commit()
        conn.close()

    # --- Watermarks ---

    def get_watermarks(self, connection_id: int) -> Dict[str, int]:
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT table_name, last_id FROM table_watermarks WHERE connection_id = ?", (connection_id,))
        result = {r['table_name']: r['last_id'] for r in cursor.fetchall()}
        conn.close()
        return result

    def update_watermark(self, connection_id: int, table_name: str, last_id: int):
        conn = self._get_connection()
        conn.execute("""
            INSERT INTO table_watermarks (connection_id, table_name, last_id, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(connection_id, table_name) DO UPDATE SET last_id = ?, updated_at = CURRENT_TIMESTAMP
        """, (connection_id, table_name, last_id, last_id))
        conn.commit()
        conn.close()

    def update_watermarks_batch(self, connection_id: int, watermarks: Dict[str, int]):
        conn = self._get_connection()
        for table_name, last_id in watermarks.items():
            conn.execute("""
                INSERT INTO table_watermarks (connection_id, table_name, last_id, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(connection_id, table_name) DO UPDATE SET last_id = ?, updated_at = CURRENT_TIMESTAMP
            """, (connection_id, table_name, last_id, last_id))
        conn.commit()
        conn.close()

    # --- Opera users ---

    def set_opera_users(self, connection_id: int, users: List[Dict]):
        conn = self._get_connection()
        conn.execute("DELETE FROM opera_users WHERE connection_id = ?", (connection_id,))
        for u in users:
            conn.execute("""
                INSERT INTO opera_users (connection_id, user_code, user_name)
                VALUES (?, ?, ?)
            """, (connection_id, u.get('code', ''), u.get('name', '')))
        conn.commit()
        conn.close()

    def get_opera_users(self, connection_id: int) -> List[str]:
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT user_code FROM opera_users WHERE connection_id = ?", (connection_id,))
        codes = [r['user_code'].strip().upper() for r in cursor.fetchall()]
        conn.close()
        return codes

    # --- Captured transactions ---

    def save_transaction(self, connection_id: int, transaction_type: str,
                         classification: str, is_verified: bool,
                         is_suspicious: bool, suspicious_reason: str,
                         input_by: str, tables: Dict, rows: Dict,
                         field_summary: Dict = None) -> int:
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO captured_transactions
                (connection_id, transaction_type, classification, is_verified,
                 is_suspicious, suspicious_reason, input_by, tables_json, rows_json, field_summary_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (connection_id, transaction_type, classification,
              1 if is_verified else 0, 1 if is_suspicious else 0,
              suspicious_reason, input_by,
              json.dumps(tables), json.dumps(rows),
              json.dumps(field_summary) if field_summary else None))
        conn.commit()
        tid = cursor.lastrowid
        conn.close()
        return tid

    def get_captured_transactions(self, connection_id: int = None,
                                   transaction_type: str = None,
                                   verified_only: bool = False,
                                   limit: int = 100) -> List[Dict]:
        conn = self._get_connection()
        cursor = conn.cursor()
        where = []
        params = []
        if connection_id:
            where.append("connection_id = ?")
            params.append(connection_id)
        if transaction_type:
            where.append("transaction_type = ?")
            params.append(transaction_type)
        if verified_only:
            where.append("is_verified = 1")
        where_clause = f"WHERE {' AND '.join(where)}" if where else ""
        cursor.execute(f"SELECT * FROM captured_transactions {where_clause} ORDER BY captured_at DESC LIMIT ?",
                       params + [limit])
        rows = [dict(r) for r in cursor.fetchall()]
        conn.close()
        return rows

    def get_coverage_summary(self, connection_id: int = None) -> Dict[str, int]:
        """Count distinct verified transaction types captured."""
        conn = self._get_connection()
        cursor = conn.cursor()
        where = "WHERE is_verified = 1"
        params = []
        if connection_id:
            where += " AND connection_id = ?"
            params.append(connection_id)
        cursor.execute(f"""
            SELECT transaction_type, COUNT(*) as count
            FROM captured_transactions {where}
            GROUP BY transaction_type
            ORDER BY transaction_type
        """, params)
        result = {r['transaction_type']: r['count'] for r in cursor.fetchall()}
        conn.close()
        return result

    def get_suspicious_transactions(self, connection_id: int = None, limit: int = 50) -> List[Dict]:
        conn = self._get_connection()
        cursor = conn.cursor()
        where = "WHERE is_suspicious = 1"
        params = []
        if connection_id:
            where += " AND connection_id = ?"
            params.append(connection_id)
        cursor.execute(f"SELECT * FROM captured_transactions {where} ORDER BY captured_at DESC LIMIT ?",
                       params + [limit])
        rows = [dict(r) for r in cursor.fetchall()]
        conn.close()
        return rows
