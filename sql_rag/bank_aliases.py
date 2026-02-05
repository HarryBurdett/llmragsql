"""
Bank Import Alias Manager

Manages a learning/alias table that remembers successful bank name to account matches.
This allows instant matching for previously seen bank statement names without fuzzy matching.

IMPORTANT: All aliases are stored in a LOCAL SQLite database.
This module NEVER modifies the Opera SE database structure.
"""

import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any
from pathlib import Path

logger = logging.getLogger(__name__)

# Local SQLite database for storing bank aliases (never in Opera SE)
BANK_ALIASES_DB_PATH = Path(__file__).parent.parent / "bank_aliases.db"


@dataclass
class BankAlias:
    """Represents a saved bank name alias"""
    id: int
    bank_name: str
    ledger_type: str  # 'S' = supplier, 'C' = customer
    account_code: str
    account_name: Optional[str] = None
    match_score: Optional[float] = None
    created_date: Optional[datetime] = None
    created_by: Optional[str] = None
    last_used: Optional[datetime] = None
    use_count: int = 1
    active: bool = True


class BankAliasManager:
    """
    Manages bank statement name aliases for fast lookups.

    The alias table maps bank statement names to Opera account codes,
    allowing instant matching for previously seen names.

    IMPORTANT: All data is stored in a LOCAL SQLite database.
    This class NEVER modifies the Opera SE database.

    Usage:
        manager = BankAliasManager()

        # Look up existing alias
        account = manager.lookup_alias("HARROWDEN IT", "S")

        # Save new alias after successful match
        manager.save_alias("HARROWDEN IT", "S", "H031", 0.85)
    """

    def __init__(self, db_path: Optional[Path] = None):
        """
        Initialize the alias manager.

        Args:
            db_path: Optional path to SQLite database (defaults to bank_aliases.db)
        """
        self.db_path = db_path or BANK_ALIASES_DB_PATH
        self._conn: Optional[sqlite3.Connection] = None
        self._alias_cache: Dict[str, Dict[str, str]] = {}  # {ledger_type: {bank_name: account_code}}
        self._cache_loaded = False
        self._ensure_table_exists()

    def _get_conn(self) -> sqlite3.Connection:
        """Get SQLite connection."""
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def _ensure_table_exists(self) -> None:
        """Create the alias table if it doesn't exist in LOCAL SQLite."""
        try:
            conn = self._get_conn()
            conn.execute("""
                CREATE TABLE IF NOT EXISTS bank_import_aliases (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bank_name TEXT NOT NULL,
                    ledger_type TEXT NOT NULL,
                    account_code TEXT NOT NULL,
                    account_name TEXT,
                    match_score REAL,
                    created_date TEXT DEFAULT CURRENT_TIMESTAMP,
                    created_by TEXT,
                    last_used TEXT,
                    use_count INTEGER DEFAULT 1,
                    active INTEGER DEFAULT 1,
                    UNIQUE(bank_name, ledger_type)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_bank_aliases_lookup
                ON bank_import_aliases(ledger_type, bank_name, active)
            """)
            conn.commit()
            logger.debug("Bank aliases table initialized (local SQLite)")
        except Exception as e:
            logger.warning(f"Could not create alias table: {e}")

    def _load_cache(self) -> None:
        """Load all active aliases into memory cache."""
        if self._cache_loaded:
            return

        try:
            conn = self._get_conn()
            cursor = conn.execute("""
                SELECT bank_name, ledger_type, account_code
                FROM bank_import_aliases
                WHERE active = 1
            """)

            self._alias_cache = {'S': {}, 'C': {}}

            for row in cursor:
                ledger_type = row['ledger_type'].strip()
                bank_name = row['bank_name'].strip().upper()
                account_code = row['account_code'].strip()

                if ledger_type in self._alias_cache:
                    self._alias_cache[ledger_type][bank_name] = account_code

            self._cache_loaded = True
            total = sum(len(v) for v in self._alias_cache.values())
            logger.info(f"Loaded {total} aliases into cache")

        except Exception as e:
            logger.warning(f"Could not load alias cache: {e}")
            self._alias_cache = {'S': {}, 'C': {}}
            self._cache_loaded = True

    def _invalidate_cache(self) -> None:
        """Invalidate the alias cache to force reload."""
        self._cache_loaded = False
        self._alias_cache = {}

    def lookup_alias(self, bank_name: str, ledger_type: str) -> Optional[str]:
        """
        Look up an alias for a bank statement name.

        Args:
            bank_name: Name from bank statement
            ledger_type: 'S' for supplier, 'C' for customer

        Returns:
            Account code if alias exists, None otherwise
        """
        if not bank_name or not ledger_type:
            return None

        self._load_cache()

        bank_name_upper = bank_name.strip().upper()
        ledger_type = ledger_type.strip().upper()

        if ledger_type not in self._alias_cache:
            return None

        account_code = self._alias_cache[ledger_type].get(bank_name_upper)

        if account_code:
            # Record usage asynchronously (don't block lookup)
            try:
                self._record_usage_async(bank_name, ledger_type)
            except Exception:
                pass  # Don't fail lookup if usage recording fails

        return account_code

    def _record_usage_async(self, bank_name: str, ledger_type: str) -> None:
        """Record alias usage (update last_used and use_count)."""
        try:
            conn = self._get_conn()
            conn.execute("""
                UPDATE bank_import_aliases
                SET last_used = datetime('now'),
                    use_count = use_count + 1
                WHERE bank_name = ?
                AND ledger_type = ?
                AND active = 1
            """, (bank_name, ledger_type))
            conn.commit()
        except Exception as e:
            logger.debug(f"Could not record alias usage: {e}")

    def record_usage(self, bank_name: str, ledger_type: str) -> bool:
        """
        Update last_used and use_count when alias is used.

        Args:
            bank_name: Name from bank statement
            ledger_type: 'S' for supplier, 'C' for customer

        Returns:
            True if usage was recorded, False otherwise
        """
        try:
            conn = self._get_conn()
            cursor = conn.execute("""
                UPDATE bank_import_aliases
                SET last_used = datetime('now'),
                    use_count = use_count + 1
                WHERE bank_name = ?
                AND ledger_type = ?
                AND active = 1
            """, (bank_name, ledger_type))
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error recording alias usage: {e}")
            return False

    def save_alias(self, bank_name: str, ledger_type: str,
                   account_code: str, match_score: float,
                   account_name: Optional[str] = None,
                   created_by: str = 'BANK_IMPORT') -> bool:
        """
        Save a new alias after successful match.

        Uses upsert logic - updates if exists, inserts if new.

        Args:
            bank_name: Name from bank statement
            ledger_type: 'S' for supplier, 'C' for customer
            account_code: Opera account code
            match_score: Score when alias was created (0-1)
            account_name: Optional cached account name
            created_by: User/system that created the alias

        Returns:
            True if alias was saved, False otherwise
        """
        if not bank_name or not ledger_type or not account_code:
            return False

        bank_name = bank_name.strip()
        ledger_type = ledger_type.strip().upper()
        account_code = account_code.strip()

        if ledger_type not in ('S', 'C'):
            logger.error(f"Invalid ledger type: {ledger_type}")
            return False

        try:
            conn = self._get_conn()

            # SQLite upsert using INSERT OR REPLACE
            conn.execute("""
                INSERT INTO bank_import_aliases
                    (bank_name, ledger_type, account_code, account_name, match_score,
                     created_by, last_used, use_count, active)
                VALUES (?, ?, ?, ?, ?, ?, datetime('now'), 1, 1)
                ON CONFLICT(bank_name, ledger_type) DO UPDATE SET
                    account_code = excluded.account_code,
                    account_name = excluded.account_name,
                    match_score = excluded.match_score,
                    last_used = datetime('now'),
                    use_count = use_count + 1,
                    active = 1
            """, (
                bank_name,
                ledger_type,
                account_code,
                account_name[:35] if account_name else None,
                round(match_score * 100, 2),  # Store as percentage
                created_by
            ))
            conn.commit()

            # Update cache
            self._load_cache()
            if ledger_type in self._alias_cache:
                self._alias_cache[ledger_type][bank_name.upper()] = account_code

            logger.info(f"Saved alias: '{bank_name}' -> {account_code} ({ledger_type})")
            return True

        except Exception as e:
            logger.error(f"Error saving alias: {e}")
            return False

    def delete_alias(self, bank_name: str, ledger_type: str) -> bool:
        """
        Soft-delete an alias (set active=0).

        Args:
            bank_name: Name from bank statement
            ledger_type: 'S' for supplier, 'C' for customer

        Returns:
            True if alias was deleted, False otherwise
        """
        try:
            conn = self._get_conn()
            cursor = conn.execute("""
                UPDATE bank_import_aliases
                SET active = 0
                WHERE bank_name = ?
                AND ledger_type = ?
            """, (bank_name, ledger_type))
            conn.commit()
            affected = cursor.rowcount

            if affected > 0:
                # Remove from cache
                self._load_cache()
                bank_name_upper = bank_name.strip().upper()
                if ledger_type in self._alias_cache and bank_name_upper in self._alias_cache[ledger_type]:
                    del self._alias_cache[ledger_type][bank_name_upper]

                logger.info(f"Deleted alias: '{bank_name}' ({ledger_type})")

            return affected > 0

        except Exception as e:
            logger.error(f"Error deleting alias: {e}")
            return False

    def get_aliases_for_account(self, account_code: str, ledger_type: Optional[str] = None) -> List[BankAlias]:
        """
        Get all bank names that map to an account.

        Args:
            account_code: Opera account code
            ledger_type: Optional filter by 'S' or 'C'

        Returns:
            List of BankAlias objects
        """
        try:
            conn = self._get_conn()

            if ledger_type:
                cursor = conn.execute("""
                    SELECT id, bank_name, ledger_type, account_code, account_name,
                           match_score, created_date, created_by, last_used, use_count, active
                    FROM bank_import_aliases
                    WHERE account_code = ?
                    AND ledger_type = ?
                    AND active = 1
                    ORDER BY use_count DESC, last_used DESC
                """, (account_code, ledger_type))
            else:
                cursor = conn.execute("""
                    SELECT id, bank_name, ledger_type, account_code, account_name,
                           match_score, created_date, created_by, last_used, use_count, active
                    FROM bank_import_aliases
                    WHERE account_code = ?
                    AND active = 1
                    ORDER BY use_count DESC, last_used DESC
                """, (account_code,))

            aliases = []
            for row in cursor:
                aliases.append(BankAlias(
                    id=row['id'],
                    bank_name=row['bank_name'].strip(),
                    ledger_type=row['ledger_type'].strip(),
                    account_code=row['account_code'].strip(),
                    account_name=row['account_name'].strip() if row['account_name'] else None,
                    match_score=float(row['match_score']) / 100 if row['match_score'] else None,
                    created_date=datetime.fromisoformat(row['created_date']) if row['created_date'] else None,
                    created_by=row['created_by'].strip() if row['created_by'] else None,
                    last_used=datetime.fromisoformat(row['last_used']) if row['last_used'] else None,
                    use_count=int(row['use_count']),
                    active=bool(row['active'])
                ))

            return aliases

        except Exception as e:
            logger.error(f"Error getting aliases for account {account_code}: {e}")
            return []

    def get_all_aliases(self, active_only: bool = True) -> List[BankAlias]:
        """
        Get all aliases.

        Args:
            active_only: Only return active aliases

        Returns:
            List of BankAlias objects
        """
        try:
            conn = self._get_conn()

            if active_only:
                cursor = conn.execute("""
                    SELECT id, bank_name, ledger_type, account_code, account_name,
                           match_score, created_date, created_by, last_used, use_count, active
                    FROM bank_import_aliases
                    WHERE active = 1
                    ORDER BY ledger_type, bank_name
                """)
            else:
                cursor = conn.execute("""
                    SELECT id, bank_name, ledger_type, account_code, account_name,
                           match_score, created_date, created_by, last_used, use_count, active
                    FROM bank_import_aliases
                    ORDER BY ledger_type, bank_name
                """)

            aliases = []
            for row in cursor:
                aliases.append(BankAlias(
                    id=row['id'],
                    bank_name=row['bank_name'].strip(),
                    ledger_type=row['ledger_type'].strip(),
                    account_code=row['account_code'].strip(),
                    account_name=row['account_name'].strip() if row['account_name'] else None,
                    match_score=float(row['match_score']) / 100 if row['match_score'] else None,
                    created_date=datetime.fromisoformat(row['created_date']) if row['created_date'] else None,
                    created_by=row['created_by'].strip() if row['created_by'] else None,
                    last_used=datetime.fromisoformat(row['last_used']) if row['last_used'] else None,
                    use_count=int(row['use_count']),
                    active=bool(row['active'])
                ))

            return aliases

        except Exception as e:
            logger.error(f"Error getting all aliases: {e}")
            return []

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get alias table statistics.

        Returns:
            Dictionary with statistics
        """
        try:
            conn = self._get_conn()
            cursor = conn.execute("""
                SELECT
                    COUNT(*) as total_aliases,
                    SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) as active_aliases,
                    SUM(CASE WHEN ledger_type = 'S' AND active = 1 THEN 1 ELSE 0 END) as supplier_aliases,
                    SUM(CASE WHEN ledger_type = 'C' AND active = 1 THEN 1 ELSE 0 END) as customer_aliases,
                    SUM(use_count) as total_uses,
                    AVG(match_score) as avg_match_score,
                    MAX(last_used) as last_used
                FROM bank_import_aliases
            """)

            row = cursor.fetchone()

            if not row or row['total_aliases'] == 0:
                return {
                    'total_aliases': 0,
                    'active_aliases': 0,
                    'supplier_aliases': 0,
                    'customer_aliases': 0,
                    'total_uses': 0,
                    'avg_match_score': 0,
                    'last_used': None
                }

            return {
                'total_aliases': int(row['total_aliases'] or 0),
                'active_aliases': int(row['active_aliases'] or 0),
                'supplier_aliases': int(row['supplier_aliases'] or 0),
                'customer_aliases': int(row['customer_aliases'] or 0),
                'total_uses': int(row['total_uses'] or 0),
                'avg_match_score': float(row['avg_match_score'] or 0) / 100,
                'last_used': row['last_used']
            }

        except Exception as e:
            logger.error(f"Error getting alias statistics: {e}")
            return {}
