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

            # Additional tables for enhanced features
            # Match configuration per user/installation
            conn.execute("""
                CREATE TABLE IF NOT EXISTS match_config (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_code TEXT,
                    min_match_score REAL DEFAULT 0.6,
                    learn_threshold REAL DEFAULT 0.8,
                    ambiguity_threshold REAL DEFAULT 0.15,
                    use_phonetic INTEGER DEFAULT 1,
                    use_levenshtein INTEGER DEFAULT 1,
                    use_ngram INTEGER DEFAULT 1,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Duplicate override decisions
            conn.execute("""
                CREATE TABLE IF NOT EXISTS duplicate_overrides (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    transaction_hash TEXT NOT NULL,
                    override_reason TEXT,
                    user_code TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(transaction_hash)
                )
            """)

            # Import sessions for save/load functionality
            conn.execute("""
                CREATE TABLE IF NOT EXISTS import_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT UNIQUE NOT NULL,
                    filename TEXT,
                    bank_code TEXT,
                    file_format TEXT,
                    transactions_json TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    expires_at TEXT
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_import_sessions_id
                ON import_sessions(session_id)
            """)

            # AI suggestions tracking
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ai_suggestions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bank_name TEXT NOT NULL,
                    suggested_account TEXT,
                    suggestion_type TEXT,
                    confidence REAL,
                    reason TEXT,
                    accepted INTEGER,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
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

    def _ensure_correction_tables_exist(self) -> None:
        """Create correction-related tables if they don't exist."""
        try:
            conn = self._get_conn()

            # Table for recording corrections
            conn.execute("""
                CREATE TABLE IF NOT EXISTS alias_corrections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bank_name TEXT NOT NULL,
                    wrong_account TEXT NOT NULL,
                    correct_account TEXT NOT NULL,
                    ledger_type TEXT,
                    corrected_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    corrected_by TEXT DEFAULT 'USER'
                )
            """)

            # Table for negative examples (things NOT to match)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS negative_aliases (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bank_name TEXT NOT NULL,
                    wrong_account TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(bank_name, wrong_account)
                )
            """)

            # Indexes for faster lookups
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_corrections_bank_name
                ON alias_corrections(bank_name)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_negative_lookup
                ON negative_aliases(bank_name, wrong_account)
            """)

            conn.commit()
            logger.debug("Correction tables initialized")
        except Exception as e:
            logger.warning(f"Could not create correction tables: {e}")

    def record_correction(
        self,
        bank_name: str,
        wrong_account: str,
        correct_account: str,
        ledger_type: str,
        account_name: Optional[str] = None,
        corrected_by: str = 'USER'
    ) -> bool:
        """
        Record a user correction and learn from it.

        This method:
        1. Records the correction for audit purposes
        2. Saves the correct mapping as a new alias (with 100% confidence)
        3. Saves the wrong mapping as a negative example to avoid future false positives

        Args:
            bank_name: Name from bank statement
            wrong_account: The incorrectly matched account
            correct_account: The user's corrected account
            ledger_type: 'S' for supplier, 'C' for customer
            account_name: Optional name of the correct account
            corrected_by: User who made the correction

        Returns:
            True if correction was recorded successfully
        """
        if not bank_name or not wrong_account or not correct_account:
            return False

        self._ensure_correction_tables_exist()

        try:
            conn = self._get_conn()

            # 1. Record the correction for audit
            conn.execute("""
                INSERT INTO alias_corrections
                    (bank_name, wrong_account, correct_account, ledger_type, corrected_by)
                VALUES (?, ?, ?, ?, ?)
            """, (bank_name, wrong_account, correct_account, ledger_type, corrected_by))

            # 2. Save correct mapping as alias (with max confidence)
            self.save_alias(
                bank_name=bank_name,
                ledger_type=ledger_type,
                account_code=correct_account,
                match_score=1.0,  # User-confirmed = 100% confidence
                account_name=account_name,
                created_by=f'CORRECTION:{corrected_by}'
            )

            # 3. Save negative example to avoid future false positives
            self._save_negative_example(bank_name, wrong_account)

            conn.commit()
            logger.info(f"Recorded correction: '{bank_name}' was {wrong_account}, should be {correct_account}")
            return True

        except Exception as e:
            logger.error(f"Error recording correction: {e}")
            return False

    def _save_negative_example(self, bank_name: str, wrong_account: str) -> bool:
        """
        Save a negative example (bank name should NOT match this account).

        Args:
            bank_name: Name from bank statement
            wrong_account: Account that was incorrectly matched

        Returns:
            True if saved successfully
        """
        try:
            conn = self._get_conn()
            conn.execute("""
                INSERT OR IGNORE INTO negative_aliases (bank_name, wrong_account)
                VALUES (?, ?)
            """, (bank_name.strip().upper(), wrong_account.strip()))
            conn.commit()
            return True
        except Exception as e:
            logger.debug(f"Could not save negative example: {e}")
            return False

    def is_negative_match(self, bank_name: str, account: str) -> bool:
        """
        Check if a bank name to account mapping is a known bad match.

        Args:
            bank_name: Name from bank statement
            account: Potential account match

        Returns:
            True if this is a known incorrect match
        """
        if not bank_name or not account:
            return False

        self._ensure_correction_tables_exist()

        try:
            conn = self._get_conn()
            cursor = conn.execute("""
                SELECT 1 FROM negative_aliases
                WHERE bank_name = ? AND wrong_account = ?
                LIMIT 1
            """, (bank_name.strip().upper(), account.strip()))
            return cursor.fetchone() is not None
        except Exception as e:
            logger.debug(f"Error checking negative match: {e}")
            return False

    def get_corrections_for_name(self, bank_name: str) -> List[Dict[str, Any]]:
        """
        Get correction history for a bank name.

        Args:
            bank_name: Name from bank statement

        Returns:
            List of correction records
        """
        self._ensure_correction_tables_exist()

        try:
            conn = self._get_conn()
            cursor = conn.execute("""
                SELECT wrong_account, correct_account, ledger_type, corrected_at, corrected_by
                FROM alias_corrections
                WHERE bank_name = ?
                ORDER BY corrected_at DESC
            """, (bank_name,))

            corrections = []
            for row in cursor:
                corrections.append({
                    'wrong_account': row['wrong_account'],
                    'correct_account': row['correct_account'],
                    'ledger_type': row['ledger_type'],
                    'corrected_at': row['corrected_at'],
                    'corrected_by': row['corrected_by']
                })

            return corrections
        except Exception as e:
            logger.error(f"Error getting corrections: {e}")
            return []

    def get_negative_matches(self, bank_name: str) -> List[str]:
        """
        Get list of accounts that should NOT be matched to this bank name.

        Args:
            bank_name: Name from bank statement

        Returns:
            List of account codes to exclude from matching
        """
        self._ensure_correction_tables_exist()

        try:
            conn = self._get_conn()
            cursor = conn.execute("""
                SELECT wrong_account FROM negative_aliases
                WHERE bank_name = ?
            """, (bank_name.strip().upper(),))

            return [row['wrong_account'] for row in cursor]
        except Exception as e:
            logger.error(f"Error getting negative matches: {e}")
            return []


class EnhancedAliasManager(BankAliasManager):
    """
    Extended alias manager with correction-based learning.

    Provides additional functionality:
    - Learning from user corrections
    - Negative examples to avoid repeated mistakes
    - Enhanced lookup that filters out known bad matches
    """

    def lookup_alias_with_filter(
        self,
        bank_name: str,
        ledger_type: str,
        exclude_accounts: Optional[List[str]] = None
    ) -> Optional[str]:
        """
        Look up alias while filtering out known bad matches.

        Args:
            bank_name: Name from bank statement
            ledger_type: 'S' for supplier, 'C' for customer
            exclude_accounts: Additional accounts to exclude

        Returns:
            Account code if found and not filtered, None otherwise
        """
        # Get base alias lookup
        account = self.lookup_alias(bank_name, ledger_type)

        if not account:
            return None

        # Check if this is a negative match
        if self.is_negative_match(bank_name, account):
            logger.debug(f"Filtered out negative match: {bank_name} -> {account}")
            return None

        # Check additional exclusions
        if exclude_accounts and account in exclude_accounts:
            return None

        return account

    def get_learning_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about the learning system.

        Returns:
            Dict with correction and negative match statistics
        """
        self._ensure_correction_tables_exist()

        stats = self.get_statistics()

        try:
            conn = self._get_conn()

            # Count corrections
            cursor = conn.execute("SELECT COUNT(*) as cnt FROM alias_corrections")
            stats['total_corrections'] = cursor.fetchone()['cnt']

            # Count negative examples
            cursor = conn.execute("SELECT COUNT(*) as cnt FROM negative_aliases")
            stats['negative_examples'] = cursor.fetchone()['cnt']

            # Most corrected names
            cursor = conn.execute("""
                SELECT bank_name, COUNT(*) as correction_count
                FROM alias_corrections
                GROUP BY bank_name
                ORDER BY correction_count DESC
                LIMIT 5
            """)
            stats['most_corrected'] = [
                {'bank_name': row['bank_name'], 'count': row['correction_count']}
                for row in cursor
            ]

        except Exception as e:
            logger.error(f"Error getting learning statistics: {e}")

        return stats
