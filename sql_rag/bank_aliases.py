"""
Bank Import Alias Manager for Opera SQL SE

Manages a learning/alias table that remembers successful bank name to account matches.
This allows instant matching for previously seen bank statement names without fuzzy matching.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any

from sqlalchemy import text
from sql_rag.sql_connector import SQLConnector

logger = logging.getLogger(__name__)


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

    Usage:
        manager = BankAliasManager(sql_connector)

        # Look up existing alias
        account = manager.lookup_alias("HARROWDEN IT", "S")

        # Save new alias after successful match
        manager.save_alias("HARROWDEN IT", "S", "H031", 0.85)
    """

    TABLE_NAME = "bank_import_aliases"

    CREATE_TABLE_SQL = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table}' AND xtype='U')
    CREATE TABLE {table} (
        id INT IDENTITY(1,1) PRIMARY KEY,
        bank_name VARCHAR(100) NOT NULL,
        ledger_type CHAR(1) NOT NULL,
        account_code VARCHAR(8) NOT NULL,
        account_name VARCHAR(35),
        match_score DECIMAL(5,2),
        created_date DATETIME DEFAULT GETDATE(),
        created_by VARCHAR(20),
        last_used DATETIME,
        use_count INT DEFAULT 1,
        active BIT DEFAULT 1,
        CONSTRAINT UQ_bank_alias UNIQUE(bank_name, ledger_type)
    )
    """.format(table=TABLE_NAME)

    def __init__(self, sql_connector: SQLConnector):
        """
        Initialize the alias manager.

        Args:
            sql_connector: SQLConnector instance for database access
        """
        self.sql = sql_connector
        self._alias_cache: Dict[str, Dict[str, str]] = {}  # {ledger_type: {bank_name: account_code}}
        self._cache_loaded = False
        self._ensure_table_exists()

    def _ensure_table_exists(self) -> None:
        """Create the alias table if it doesn't exist."""
        try:
            # Check if table exists first
            df = self.sql.execute_query(f"""
                SELECT COUNT(*) as cnt FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_NAME = '{self.TABLE_NAME}'
            """)
            if df.iloc[0]['cnt'] > 0:
                logger.debug(f"Table {self.TABLE_NAME} already exists")
                return

            # Create table using direct connection to avoid transaction issues
            with self.sql.engine.connect() as conn:
                conn.execute(text(f"""
                    CREATE TABLE {self.TABLE_NAME} (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        bank_name VARCHAR(100) NOT NULL,
                        ledger_type CHAR(1) NOT NULL,
                        account_code VARCHAR(8) NOT NULL,
                        account_name VARCHAR(35),
                        match_score DECIMAL(5,2),
                        created_date DATETIME DEFAULT GETDATE(),
                        created_by VARCHAR(20),
                        last_used DATETIME,
                        use_count INT DEFAULT 1,
                        active BIT DEFAULT 1,
                        CONSTRAINT UQ_bank_alias UNIQUE(bank_name, ledger_type)
                    )
                """))
                conn.commit()
            logger.info(f"Created {self.TABLE_NAME} table")
        except Exception as e:
            logger.warning(f"Could not create alias table (may already exist): {e}")

    def _load_cache(self) -> None:
        """Load all active aliases into memory cache."""
        if self._cache_loaded:
            return

        try:
            df = self.sql.execute_query(f"""
                SELECT bank_name, ledger_type, account_code
                FROM {self.TABLE_NAME}
                WHERE active = 1
            """)

            self._alias_cache = {'S': {}, 'C': {}}

            for _, row in df.iterrows():
                ledger_type = row['ledger_type'].strip()
                bank_name = row['bank_name'].strip().upper()
                account_code = row['account_code'].strip()

                if ledger_type in self._alias_cache:
                    self._alias_cache[ledger_type][bank_name] = account_code

            self._cache_loaded = True
            logger.info(f"Loaded {len(df)} aliases into cache")

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
            with self.sql.engine.connect() as conn:
                conn.execute(text(f"""
                    UPDATE {self.TABLE_NAME}
                    SET last_used = GETDATE(),
                        use_count = use_count + 1
                    WHERE bank_name = :bank_name
                    AND ledger_type = :ledger_type
                    AND active = 1
                """), {'bank_name': bank_name, 'ledger_type': ledger_type})
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
            with self.sql.engine.connect() as conn:
                result = conn.execute(text(f"""
                    UPDATE {self.TABLE_NAME}
                    SET last_used = GETDATE(),
                        use_count = use_count + 1
                    WHERE bank_name = :bank_name
                    AND ledger_type = :ledger_type
                    AND active = 1
                """), {'bank_name': bank_name, 'ledger_type': ledger_type})
                conn.commit()
                return result.rowcount > 0
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
            # Try to insert, handle duplicate with update
            # Using MERGE for upsert behavior
            with self.sql.engine.connect() as conn:
                conn.execute(text(f"""
                    MERGE {self.TABLE_NAME} AS target
                    USING (SELECT :bank_name AS bank_name, :ledger_type AS ledger_type) AS source
                    ON target.bank_name = source.bank_name AND target.ledger_type = source.ledger_type
                    WHEN MATCHED THEN
                        UPDATE SET
                            account_code = :account_code,
                            account_name = :account_name,
                            match_score = :match_score,
                            last_used = GETDATE(),
                            use_count = use_count + 1,
                            active = 1
                    WHEN NOT MATCHED THEN
                        INSERT (bank_name, ledger_type, account_code, account_name, match_score, created_by, last_used, use_count, active)
                        VALUES (:bank_name, :ledger_type, :account_code, :account_name, :match_score, :created_by, GETDATE(), 1, 1);
                """), {
                    'bank_name': bank_name,
                    'ledger_type': ledger_type,
                    'account_code': account_code,
                    'account_name': account_name[:35] if account_name else None,
                    'match_score': round(match_score * 100, 2),  # Store as percentage
                    'created_by': created_by
                })
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
            with self.sql.engine.connect() as conn:
                result = conn.execute(text(f"""
                    UPDATE {self.TABLE_NAME}
                    SET active = 0
                    WHERE bank_name = :bank_name
                    AND ledger_type = :ledger_type
                """), {'bank_name': bank_name, 'ledger_type': ledger_type})
                conn.commit()
                affected = result.rowcount

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
            query = f"""
                SELECT id, bank_name, ledger_type, account_code, account_name,
                       match_score, created_date, created_by, last_used, use_count, active
                FROM {self.TABLE_NAME}
                WHERE account_code = :account_code
                AND active = 1
            """
            params = {'account_code': account_code}

            if ledger_type:
                query += " AND ledger_type = :ledger_type"
                params['ledger_type'] = ledger_type

            query += " ORDER BY use_count DESC, last_used DESC"

            df = self.sql.execute_query(query, params)

            aliases = []
            for _, row in df.iterrows():
                aliases.append(BankAlias(
                    id=row['id'],
                    bank_name=row['bank_name'].strip(),
                    ledger_type=row['ledger_type'].strip(),
                    account_code=row['account_code'].strip(),
                    account_name=row['account_name'].strip() if row['account_name'] else None,
                    match_score=float(row['match_score']) / 100 if row['match_score'] else None,
                    created_date=row['created_date'],
                    created_by=row['created_by'].strip() if row['created_by'] else None,
                    last_used=row['last_used'],
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
            query = f"""
                SELECT id, bank_name, ledger_type, account_code, account_name,
                       match_score, created_date, created_by, last_used, use_count, active
                FROM {self.TABLE_NAME}
            """

            if active_only:
                query += " WHERE active = 1"

            query += " ORDER BY ledger_type, bank_name"

            df = self.sql.execute_query(query)

            aliases = []
            for _, row in df.iterrows():
                aliases.append(BankAlias(
                    id=row['id'],
                    bank_name=row['bank_name'].strip(),
                    ledger_type=row['ledger_type'].strip(),
                    account_code=row['account_code'].strip(),
                    account_name=row['account_name'].strip() if row['account_name'] else None,
                    match_score=float(row['match_score']) / 100 if row['match_score'] else None,
                    created_date=row['created_date'],
                    created_by=row['created_by'].strip() if row['created_by'] else None,
                    last_used=row['last_used'],
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
            df = self.sql.execute_query(f"""
                SELECT
                    COUNT(*) as total_aliases,
                    SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) as active_aliases,
                    SUM(CASE WHEN ledger_type = 'S' AND active = 1 THEN 1 ELSE 0 END) as supplier_aliases,
                    SUM(CASE WHEN ledger_type = 'C' AND active = 1 THEN 1 ELSE 0 END) as customer_aliases,
                    SUM(use_count) as total_uses,
                    AVG(match_score) as avg_match_score,
                    MAX(last_used) as last_used
                FROM {self.TABLE_NAME}
            """)

            if df.empty:
                return {
                    'total_aliases': 0,
                    'active_aliases': 0,
                    'supplier_aliases': 0,
                    'customer_aliases': 0,
                    'total_uses': 0,
                    'avg_match_score': 0,
                    'last_used': None
                }

            row = df.iloc[0]
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
