"""
Supplier Config Manager.

Maintains a local SQLite cache of supplier data synced from Opera's pname table,
plus local automation flags that are never overwritten by Opera sync.

The sync is strictly read-only from Opera — it never writes back to Opera.
"""

import sqlite3
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Fields synced from Opera (may be overwritten during sync)
_OPERA_FIELDS = {'name', 'balance', 'payment_terms_days', 'payment_method'}

# Local-only automation flags (NEVER overwritten during sync)
_LOCAL_FLAG_FIELDS = {
    'reconciliation_active',
    'auto_respond',
    'never_communicate',
    'statements_contact_position',
}


class SupplierConfigManager:
    """
    Manages the local supplier_config SQLite table.

    Syncs supplier master data from Opera's pname table and stores local
    automation flags alongside it.  Local flags are NEVER overwritten when
    syncing from Opera.
    """

    def __init__(self, db_path: str, sql_connector):
        """
        Initialise the manager.

        Args:
            db_path: Absolute path to the SQLite database file.
            sql_connector: An SQLConnector instance configured for Opera SQL SE.
        """
        self.db_path = db_path
        self.sql = sql_connector
        self._ensure_table()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_connection(self) -> sqlite3.Connection:
        """Open and return a SQLite connection with row_factory set."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_table(self):
        """Create supplier_config table if it does not already exist."""
        conn = self._get_connection()
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS supplier_config (
                    account_code TEXT PRIMARY KEY,
                    name TEXT,
                    balance DECIMAL(15,2),
                    payment_terms_days INTEGER DEFAULT 30,
                    payment_method TEXT,
                    reconciliation_active BOOLEAN DEFAULT 1,
                    auto_respond BOOLEAN DEFAULT 1,
                    never_communicate BOOLEAN DEFAULT 0,
                    statements_contact_position TEXT,
                    last_synced DATETIME,
                    last_statement_date DATE,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
        finally:
            conn.close()

    def _get_payment_terms(self, account_code: str, terms_profile: str) -> int:
        """
        Resolve payment terms days for a supplier.

        Checks for an account-specific override in pterms first; falls back
        to the supplier's terms profile, then defaults to 30.

        Args:
            account_code: Opera supplier account code (already stripped).
            terms_profile: The pn_tprfl value from pname (may be empty).

        Returns:
            Payment terms in days (integer).
        """
        try:
            # Account-specific override
            df = self.sql.execute_query(f"""
                SELECT TOP 1 pr_termday
                FROM pterms WITH (NOLOCK)
                WHERE pr_account = '{account_code}'
                ORDER BY id
            """)
            if not df.empty:
                val = df.iloc[0]['pr_termday']
                if val is not None:
                    return int(val)

            # Profile-level fallback
            if terms_profile:
                df2 = self.sql.execute_query(f"""
                    SELECT TOP 1 pr_termday
                    FROM pterms WITH (NOLOCK)
                    WHERE pr_code = '{terms_profile}'
                    ORDER BY id
                """)
                if not df2.empty:
                    val = df2.iloc[0]['pr_termday']
                    if val is not None:
                        return int(val)

        except Exception as exc:
            logger.warning(
                "Could not resolve payment terms for %s: %s", account_code, exc
            )

        return 30  # sensible default

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def sync_from_opera(self) -> Dict[str, int]:
        """
        One-way sync from Opera pname into supplier_config.

        Reads all non-dormant suppliers from Opera and upserts them into
        supplier_config.  Local automation flags are NEVER overwritten.

        Returns:
            dict with keys 'synced' (updated existing) and 'new' (inserted).
        """
        df = self.sql.execute_query("""
            SELECT
                pn_account,
                RTRIM(pn_name) AS pn_name,
                pn_currbal,
                RTRIM(ISNULL(pn_paymeth, '')) AS pn_paymeth,
                pn_dormant,
                RTRIM(ISNULL(pn_tprfl, '')) AS pn_tprfl
            FROM pname WITH (NOLOCK)
            WHERE pn_dormant = 0 OR pn_dormant IS NULL
        """)

        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        count_new = 0
        count_synced = 0

        conn = self._get_connection()
        try:
            for _, row in df.iterrows():
                account_code = str(row['pn_account']).strip()
                if not account_code:
                    continue

                name = str(row['pn_name']).strip() if row['pn_name'] is not None else ''
                balance = float(row['pn_currbal']) if row['pn_currbal'] is not None else 0.0
                payment_method = str(row['pn_paymeth']).strip() if row['pn_paymeth'] is not None else ''
                terms_profile = str(row['pn_tprfl']).strip() if row['pn_tprfl'] is not None else ''
                payment_terms_days = self._get_payment_terms(account_code, terms_profile)

                # Check if already exists
                existing = conn.execute(
                    "SELECT account_code FROM supplier_config WHERE account_code = ?",
                    (account_code,)
                ).fetchone()

                if existing:
                    # Update Opera-sourced fields only — never touch local flags
                    conn.execute("""
                        UPDATE supplier_config
                        SET name = ?,
                            balance = ?,
                            payment_terms_days = ?,
                            payment_method = ?,
                            last_synced = ?,
                            updated_at = ?
                        WHERE account_code = ?
                    """, (name, balance, payment_terms_days, payment_method,
                          now, now, account_code))
                    count_synced += 1
                else:
                    # Insert with defaults for local flags
                    conn.execute("""
                        INSERT INTO supplier_config
                            (account_code, name, balance, payment_terms_days,
                             payment_method, last_synced)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (account_code, name, balance, payment_terms_days,
                          payment_method, now))
                    count_new += 1

            conn.commit()
        finally:
            conn.close()

        logger.info(
            "sync_from_opera: %d new, %d updated suppliers", count_new, count_synced
        )
        return {'new': count_new, 'synced': count_synced}

    def get_config(self, account_code: str) -> Optional[Dict[str, Any]]:
        """
        Return all config fields for a single supplier.

        Args:
            account_code: Opera supplier account code.

        Returns:
            dict of all supplier_config fields, or None if not found.
        """
        conn = self._get_connection()
        try:
            row = conn.execute(
                "SELECT * FROM supplier_config WHERE account_code = ?",
                (account_code,)
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def update_flags(self, account_code: str, **flags) -> bool:
        """
        Update only local automation flag fields for a supplier.

        Allowed flag names: reconciliation_active, auto_respond,
        never_communicate, statements_contact_position.

        Args:
            account_code: Opera supplier account code.
            **flags: Flag name/value pairs to update.

        Returns:
            True if the supplier was found and updated, False otherwise.

        Raises:
            ValueError: If any flag name is not a valid local flag field.
        """
        invalid = set(flags.keys()) - _LOCAL_FLAG_FIELDS
        if invalid:
            raise ValueError(
                f"Cannot update Opera-synced fields via update_flags: {invalid}. "
                f"Allowed fields: {_LOCAL_FLAG_FIELDS}"
            )

        if not flags:
            return False

        set_clauses = ', '.join(f'{k} = ?' for k in flags)
        params = list(flags.values()) + [
            datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            account_code,
        ]

        conn = self._get_connection()
        try:
            cursor = conn.execute(
                f"UPDATE supplier_config SET {set_clauses}, updated_at = ? "
                f"WHERE account_code = ?",
                params,
            )
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()

    def get_all(self, active_only: bool = False) -> List[Dict[str, Any]]:
        """
        Return all suppliers from supplier_config.

        Args:
            active_only: If True, only return suppliers with
                         reconciliation_active = 1.

        Returns:
            List of supplier config dicts ordered by account_code.
        """
        conn = self._get_connection()
        try:
            if active_only:
                rows = conn.execute(
                    "SELECT * FROM supplier_config "
                    "WHERE reconciliation_active = 1 "
                    "ORDER BY account_code"
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM supplier_config ORDER BY account_code"
                ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def refresh_balance(self, account_code: str) -> Optional[float]:
        """
        Refresh just the balance for a single supplier from Opera.

        Args:
            account_code: Opera supplier account code.

        Returns:
            The updated balance, or None if the supplier was not found in Opera.
        """
        df = self.sql.execute_query(f"""
            SELECT pn_currbal
            FROM pname WITH (NOLOCK)
            WHERE pn_account = '{account_code}'
        """)

        if df.empty:
            logger.warning("refresh_balance: account %s not found in Opera", account_code)
            return None

        balance = float(df.iloc[0]['pn_currbal']) if df.iloc[0]['pn_currbal'] is not None else 0.0
        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

        conn = self._get_connection()
        try:
            conn.execute(
                "UPDATE supplier_config "
                "SET balance = ?, last_synced = ?, updated_at = ? "
                "WHERE account_code = ?",
                (balance, now, now, account_code),
            )
            conn.commit()
        finally:
            conn.close()

        logger.info("refresh_balance: %s balance updated to %.2f", account_code, balance)
        return balance
