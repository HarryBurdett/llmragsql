"""
Opera 3 FoxPro Data Import Module

Imports transactions into Opera 3 FoxPro DBF files.
Replicates the exact pattern Opera uses when users manually enter transactions.

REQUIREMENTS:
- dbf package (pip install dbf) for reading AND writing DBF files

WARNING: Direct DBF writes bypass Opera's application-level locking.
Use with caution and preferably when Opera is not running.
Consider using Opera's standard import mechanisms for production.

TABLES WRITTEN:
- aentry: Cashbook Entry Header
- atran: Cashbook Transaction
- ntran: Nominal Ledger (2 rows per transaction - double-entry)
- ptran: Purchase Ledger Transaction (for payments)
- stran: Sales Ledger Transaction (for receipts)
- palloc: Purchase Allocation
- salloc: Sales Allocation
- pname: Supplier Master (balance update)
- sname: Customer Master (balance update)
"""

from __future__ import annotations

import os
import logging
import uuid
import time
import fcntl
from typing import Optional, List, Dict, Any, Tuple, TYPE_CHECKING
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from decimal import Decimal
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# Locking configuration - matches SQL SE approach
LOCK_TIMEOUT_SECONDS = 5  # Equivalent to SQL SE's 5000ms LOCK_TIMEOUT
LOCK_RETRY_INTERVAL = 0.1  # Seconds between retry attempts

try:
    import dbf
    DBF_WRITE_AVAILABLE = True
except ImportError:
    dbf = None  # type: ignore
    DBF_WRITE_AVAILABLE = False
    logger.warning("dbf package not installed. Install with: pip install dbf")


@dataclass
class Opera3ImportResult:
    """Result of an Opera 3 import operation"""
    success: bool
    records_processed: int = 0
    records_imported: int = 0
    records_failed: int = 0
    entry_number: str = ""
    journal_number: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    new_reconciled_balance: Optional[float] = None  # New balance after reconciliation (pounds)


class OperaUniqueIdGenerator:
    """
    Generate unique IDs matching Opera's format.

    Opera uses IDs like '_7E30YB5IX' which are:
    - Underscore prefix
    - 9 base-36 encoded characters (timestamp/sequence)
    """

    # Base-36 characters (0-9, A-Z)
    CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"

    _last_time = 0
    _sequence = 0

    @classmethod
    def generate(cls) -> str:
        """Generate a unique ID in Opera's format (10 chars: _ + 9 alphanumeric)"""
        import time as time_module
        current_time = int(time_module.time() * 1000)  # Milliseconds

        if current_time == cls._last_time:
            cls._sequence += 1
        else:
            cls._sequence = 0
            cls._last_time = current_time

        # Combine time and sequence
        combined = (current_time << 8) + (cls._sequence & 0xFF)

        # Convert to base-36
        result = []
        while combined > 0:
            result.append(cls.CHARS[combined % 36])
            combined //= 36

        # Pad to 9 characters and prefix with underscore
        id_str = ''.join(reversed(result)).zfill(9)
        return f"_{id_str[-9:]}"

    @classmethod
    def generate_multiple(cls, count: int) -> List[str]:
        """Generate multiple unique IDs with different sequences"""
        ids = []
        for _ in range(count):
            ids.append(cls.generate())
            cls._sequence += 1  # Ensure different IDs even in same millisecond
        return ids


class FileLockTimeout(Exception):
    """Raised when file lock cannot be acquired within timeout"""
    pass


# =========================================================================
# CASHBOOK TRANSACTION TYPE CONSTANTS
# =========================================================================
# These are the internal at_type values used by Opera - NOT user-configurable
# The ae_cbtype/at_cbtype codes are user-defined in atype table

class CashbookTransactionType:
    """
    Internal transaction type codes used in atran.at_type field.

    These are FIXED by Opera and determine the transaction category.
    The user-visible type codes (P1, R2, etc.) are stored in atype table
    and mapped to ae_cbtype/at_cbtype fields.
    """
    NOMINAL_PAYMENT = 1      # Cashbook payment to nominal account (no ledger)
    NOMINAL_RECEIPT = 2      # Cashbook receipt from nominal account (no ledger)
    SALES_REFUND = 3         # Refund to customer (money out, reduces debtors)
    SALES_RECEIPT = 4        # Receipt from customer (money in, reduces debtors)
    PURCHASE_PAYMENT = 5     # Payment to supplier (money out, reduces creditors)
    PURCHASE_REFUND = 6      # Refund from supplier (money in, reduces creditors)
    BANK_TRANSFER = 8        # Internal bank transfer


class AtypeCategory:
    """
    atype.ay_type categories - defines whether a type is Payment, Receipt, or Transfer.
    """
    PAYMENT = 'P'    # Money going out (payments, refunds to customers)
    RECEIPT = 'R'    # Money coming in (receipts, refunds from suppliers)
    TRANSFER = 'T'   # Internal bank transfers


# Mapping from transaction context to required atype category and at_type
TRANSACTION_TYPE_MAP = {
    'purchase_payment': {
        'ay_type': AtypeCategory.PAYMENT,
        'at_type': CashbookTransactionType.PURCHASE_PAYMENT,
        'description': 'Payment to supplier'
    },
    'purchase_refund': {
        'ay_type': AtypeCategory.RECEIPT,
        'at_type': CashbookTransactionType.PURCHASE_REFUND,
        'description': 'Refund from supplier'
    },
    'sales_receipt': {
        'ay_type': AtypeCategory.RECEIPT,
        'at_type': CashbookTransactionType.SALES_RECEIPT,
        'description': 'Receipt from customer'
    },
    'sales_refund': {
        'ay_type': AtypeCategory.PAYMENT,
        'at_type': CashbookTransactionType.SALES_REFUND,
        'description': 'Refund to customer'
    },
    'nominal_payment': {
        'ay_type': AtypeCategory.PAYMENT,
        'at_type': CashbookTransactionType.NOMINAL_PAYMENT,
        'description': 'Payment to nominal account'
    },
    'nominal_receipt': {
        'ay_type': AtypeCategory.RECEIPT,
        'at_type': CashbookTransactionType.NOMINAL_RECEIPT,
        'description': 'Receipt from nominal account'
    },
    'bank_transfer': {
        'ay_type': AtypeCategory.TRANSFER,
        'at_type': CashbookTransactionType.BANK_TRANSFER,
        'description': 'Internal bank transfer'
    },
}


class Opera3FoxProImport:
    """
    Import handler for Opera 3 FoxPro DBF files.

    Replicates Opera's transaction patterns when writing directly to DBF files.

    LOCKING STRATEGY (equivalent to SQL SE):
    - Uses file-level locking with timeout (equivalent to SQL SE's LOCK_TIMEOUT)
    - Acquires locks on all required tables before starting transaction
    - Minimizes lock duration by preparing all data before acquiring locks
    - Releases locks immediately after transaction completes

    WARNING: Direct DBF writes bypass Opera's application-level locking.
    Use with caution, preferably when Opera is not in use.
    """

    def __init__(self, data_path: str, encoding: str = 'cp1252',
                 lock_timeout: float = LOCK_TIMEOUT_SECONDS):
        """
        Initialize the Opera 3 importer.

        Args:
            data_path: Path to Opera 3 company data folder
            encoding: Character encoding for DBF files (default: cp1252)
            lock_timeout: Maximum seconds to wait for file lock (default: 5)
        """
        if not DBF_WRITE_AVAILABLE:
            raise ImportError(
                "dbf package required for writing. Install with: pip install dbf"
            )

        self.data_path = Path(data_path)
        self.encoding = encoding
        self.lock_timeout = lock_timeout
        self._table_cache: Dict[str, Any] = {}  # dbf.Table when available
        self._lock_files: Dict[str, int] = {}  # file descriptors for locks
        self._nacnt_type_cache: Dict[str, tuple] = {}  # Cache for nacnt type/subtype lookups
        self._financial_year_cache = None  # Cache for nparm financial year

        if not self.data_path.exists():
            raise FileNotFoundError(f"Opera 3 data path not found: {data_path}")

    @contextmanager
    def _acquire_file_lock(self, filepath: Path, exclusive: bool = True):
        """
        Acquire a file lock with timeout (equivalent to SQL SE's LOCK_TIMEOUT).

        Args:
            filepath: Path to file to lock
            exclusive: True for exclusive write lock, False for shared read lock

        Raises:
            FileLockTimeout: If lock cannot be acquired within timeout
        """
        lock_path = filepath.with_suffix('.lck')
        lock_type = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        lock_type |= fcntl.LOCK_NB  # Non-blocking

        start_time = time.time()
        fd = None

        try:
            # Create/open lock file
            fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR)

            # Try to acquire lock with retry until timeout
            while True:
                try:
                    fcntl.flock(fd, lock_type)
                    logger.debug(f"Acquired lock on {filepath.name}")
                    break
                except BlockingIOError:
                    elapsed = time.time() - start_time
                    if elapsed >= self.lock_timeout:
                        raise FileLockTimeout(
                            f"Could not acquire lock on {filepath.name} "
                            f"within {self.lock_timeout} seconds. "
                            f"Another user may have the file open."
                        )
                    time.sleep(LOCK_RETRY_INTERVAL)

            yield fd

        finally:
            if fd is not None:
                try:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                    os.close(fd)
                    logger.debug(f"Released lock on {filepath.name}")
                except Exception as e:
                    logger.warning(f"Error releasing lock: {e}")

    @contextmanager
    def _transaction_lock(self, table_names: List[str]):
        """
        Acquire locks on multiple tables for a transaction.

        Equivalent to SQL SE's transaction with UPDLOCK, ROWLOCK hints.
        Acquires all locks before proceeding to prevent deadlocks.

        Args:
            table_names: List of table names to lock

        Yields:
            None (tables are locked for duration of context)
        """
        acquired_locks = []
        try:
            # Sort table names to prevent deadlocks (consistent lock order)
            for table_name in sorted(table_names):
                try:
                    dbf_path = self._get_dbf_path(table_name)
                    lock_ctx = self._acquire_file_lock(dbf_path, exclusive=True)
                    fd = lock_ctx.__enter__()
                    acquired_locks.append((table_name, lock_ctx, fd))
                except FileNotFoundError:
                    # Table doesn't exist - skip locking (will fail later on write)
                    pass

            logger.debug(f"Acquired transaction locks on: {[t[0] for t in acquired_locks]}")
            yield

        finally:
            # Release locks in reverse order
            for table_name, lock_ctx, fd in reversed(acquired_locks):
                try:
                    lock_ctx.__exit__(None, None, None)
                except Exception as e:
                    logger.warning(f"Error releasing lock on {table_name}: {e}")

    def _get_dbf_path(self, table_name: str) -> Path:
        """Get the path to a DBF file"""
        # Try lowercase first
        dbf_path = self.data_path / f"{table_name.lower()}.dbf"
        if dbf_path.exists():
            return dbf_path

        # Try uppercase
        dbf_path = self.data_path / f"{table_name.upper()}.DBF"
        if dbf_path.exists():
            return dbf_path

        # Search for any case match
        for f in self.data_path.glob("*.dbf"):
            if f.stem.lower() == table_name.lower():
                return f
        for f in self.data_path.glob("*.DBF"):
            if f.stem.lower() == table_name.lower():
                return f

        raise FileNotFoundError(f"Table not found: {table_name}")

    def _open_table(self, table_name: str) -> Any:
        """Open a DBF table for reading/writing"""
        if table_name in self._table_cache:
            return self._table_cache[table_name]

        dbf_path = self._get_dbf_path(table_name)
        table = dbf.Table(str(dbf_path), codepage=self.encoding)
        table.open(dbf.READ_WRITE)
        self._table_cache[table_name] = table
        return table

    def _close_all_tables(self):
        """Close all open tables"""
        for table in self._table_cache.values():
            try:
                table.close()
            except Exception:
                pass
        self._table_cache.clear()

    def _get_next_entry_number(self, cb_type: str = 'P5') -> str:
        """
        Get next available entry number for cashbook entries.

        Args:
            cb_type: Cashbook type ('P5' for payments, 'R2' for receipts)

        Returns:
            Entry number like 'P500000001' or 'R200000001'
        """
        table = self._open_table('aentry')
        max_num = 0

        for record in table:
            if record.ae_cbtype.strip() == cb_type:
                entry = record.ae_entry.strip()
                if entry.startswith(cb_type):
                    try:
                        num = int(entry[2:])
                        if num > max_num:
                            max_num = num
                    except ValueError:
                        pass

        return f"{cb_type}{max_num + 1:08d}"

    def _get_next_journal(self, count: int = 1) -> int:
        """
        Allocate the next journal number(s) from nparm.np_nexjrnl.

        Opera maintains a sequential journal counter in nparm. This method reads
        the current value and advances the counter by `count`.

        Args:
            count: Number of journal numbers to allocate (default 1).
                   Returns the FIRST number; caller uses first..first+count-1.

        Returns:
            The first allocated journal number.
        """
        table = self._open_table('nparm')
        next_journal = 1
        for record in table:
            next_journal = int(record.np_nexjrnl or 1)
            with record:
                record.np_nexjrnl = next_journal + count
            break  # nparm has only one row
        logger.debug(f"Allocated journal number(s) {next_journal}..{next_journal + count - 1} from nparm")
        return next_journal

    def _get_nacnt_type(self, account: str):
        """
        Look up and cache the na_type/na_subt for a nominal account.

        Args:
            account: Nominal account code

        Returns:
            Tuple of (na_type, na_subt) or None if not found.
        """
        account_key = account.strip().upper()
        if account_key in self._nacnt_type_cache:
            return self._nacnt_type_cache[account_key]

        table = self._open_table('nacnt')
        for record in table:
            if record.na_acnt.strip().upper() == account_key:
                na_type = str(record.na_type) if record.na_type else 'B '
                na_subt = str(record.na_subt) if record.na_subt else 'BB'
                self._nacnt_type_cache[account_key] = (na_type, na_subt)
                return (na_type, na_subt)

        return None

    def _get_financial_year(self):
        """Look up and cache the current financial year from nparm."""
        if self._financial_year_cache is None:
            table = self._open_table('nparm')
            for record in table:
                self._financial_year_cache = int(record.np_year or 2026)
                break
        return self._financial_year_cache

    def _update_nhist(self, account: str, value: float, period: int, year: int = None):
        """
        Update nhist (nominal history) after posting to ntran.

        Opera maintains nhist as period-level balance snapshots per nominal account.
        Records are keyed by (account, type, subtype, centre, year, period).

        Key differences from nacnt:
        - nhist stores nh_ptdcr as NEGATIVE values (nacnt stores positive)
        - nhist tracks nh_bal (net balance for the period)
        - Records are updated in-place if they exist, or a new record is appended

        Args:
            account: Nominal account code
            value: Transaction value in POUNDS (positive=DR, negative=CR)
            period: Posting period
            year: Financial year (looked up from nparm if None)
        """
        type_info = self._get_nacnt_type(account)
        if not type_info:
            logger.warning(f"Cannot update nhist - account {account} not found in nacnt")
            return
        na_type, na_subt = type_info

        if year is None:
            year = self._get_financial_year()

        cost_centre = '    '
        account_key = account.strip().upper()

        try:
            table = self._open_table('nhist')
            found = False

            for record in table:
                if (record.nh_nacnt.strip().upper() == account_key
                        and str(record.nh_ntype) == na_type
                        and str(record.nh_nsubt) == na_subt
                        and str(record.nh_ncntr).strip() == cost_centre.strip()
                        and int(record.nh_year or 0) == year
                        and int(record.nh_period or 0) == period):
                    # UPDATE existing record
                    with record:
                        bal = float(record.nh_bal or 0)
                        record.nh_bal = bal + value
                        if value >= 0:
                            record.nh_ptddr = float(record.nh_ptddr or 0) + value
                        else:
                            record.nh_ptdcr = float(record.nh_ptdcr or 0) + value  # stored as negative
                    found = True
                    logger.debug(f"Updated nhist for {account} period {period}/{year}: value={value}")
                    break

            if not found:
                # INSERT new period row
                ptddr = value if value >= 0 else 0
                ptdcr = value if value < 0 else 0  # stored as negative
                table.append({
                    'nh_rectype': 1,
                    'nh_ntype': na_type,
                    'nh_nsubt': na_subt,
                    'nh_nacnt': f'{account.strip():<8}',
                    'nh_ncntr': cost_centre,
                    'nh_job': '        ',
                    'nh_project': '        ',
                    'nh_year': year,
                    'nh_period': period,
                    'nh_bal': value,
                    'nh_budg': 0,
                    'nh_rbudg': 0,
                    'nh_ptddr': ptddr,
                    'nh_ptdcr': ptdcr,
                    'nh_fbal': 0,
                })
                logger.debug(f"Inserted nhist for {account} period {period}/{year}: value={value}")

        except Exception as e:
            logger.error(f"Failed to update nhist for {account}: {e}")
            raise

    # =========================================================================
    # ATYPE (Payment/Receipt Type) METHODS
    # =========================================================================

    def get_available_types(self, category: str = None) -> List[Dict[str, Any]]:
        """
        Get available payment/receipt types from atype table.

        Args:
            category: Optional filter - 'P' (Payment), 'R' (Receipt), 'T' (Transfer)

        Returns:
            List of type dictionaries with ay_cbtype, ay_desc, ay_type, ay_entry
        """
        try:
            table = self._open_table('atype')
            types = []

            for record in table:
                rec_category = record.ay_type.strip() if hasattr(record, 'ay_type') else ''
                if category and rec_category != category:
                    continue

                types.append({
                    'code': record.ay_cbtype.strip() if hasattr(record, 'ay_cbtype') else '',
                    'description': record.ay_desc.strip() if hasattr(record, 'ay_desc') else '',
                    'category': rec_category,
                    'next_entry': record.ay_entry.strip() if hasattr(record, 'ay_entry') else ''
                })

            # Sort by category, then prioritise codes whose first letter matches
            # the category (e.g. for Receipt 'R': R2, R3, R4 before PR)
            types.sort(key=lambda x: (x['category'], 0 if x['code'] and x['code'][0] == x['category'] else 1, x['code']))
            return types

        except Exception as e:
            logger.error(f"Error getting atype list: {e}")
            return []

    def validate_cbtype(self, cbtype: str, required_category: str = None) -> Dict[str, Any]:
        """
        Validate a cashbook type code exists in atype.

        Args:
            cbtype: Type code to validate (e.g., 'P1', 'R2', 'P5')
            required_category: If specified, validates type has this category ('P', 'R', 'T')

        Returns:
            Dictionary with valid, code, description, category, next_entry, error
        """
        try:
            table = self._open_table('atype')

            for record in table:
                code = record.ay_cbtype.strip() if hasattr(record, 'ay_cbtype') else ''
                if code == cbtype:
                    category = record.ay_type.strip() if hasattr(record, 'ay_type') else ''

                    if required_category and category != required_category:
                        category_names = {'P': 'Payment', 'R': 'Receipt', 'T': 'Transfer'}
                        return {
                            'valid': False,
                            'code': cbtype,
                            'category': category,
                            'error': f"Type '{cbtype}' is category '{category}' ({category_names.get(category, category)}), "
                                     f"but '{required_category}' ({category_names.get(required_category, required_category)}) is required"
                        }

                    return {
                        'valid': True,
                        'code': cbtype,
                        'description': record.ay_desc.strip() if hasattr(record, 'ay_desc') else '',
                        'category': category,
                        'next_entry': record.ay_entry.strip() if hasattr(record, 'ay_entry') else ''
                    }

            return {
                'valid': False,
                'code': cbtype,
                'error': f"Type code '{cbtype}' not found in atype table"
            }

        except Exception as e:
            logger.error(f"Error validating cbtype '{cbtype}': {e}")
            return {
                'valid': False,
                'code': cbtype,
                'error': str(e)
            }

    def get_next_entry_from_atype(self, cbtype: str) -> str:
        """
        Get the next entry number for a given type code from atype.

        Args:
            cbtype: Type code (e.g., 'P1', 'R2', 'P5')

        Returns:
            Next entry number string (e.g., 'P100008025')
        """
        try:
            table = self._open_table('atype')

            for record in table:
                code = record.ay_cbtype.strip() if hasattr(record, 'ay_cbtype') else ''
                if code == cbtype:
                    entry = record.ay_entry.strip() if hasattr(record, 'ay_entry') else ''
                    return entry if entry else f"{cbtype}{1:08d}"

            logger.warning(f"Type code '{cbtype}' not found, generating fallback entry number")
            return f"{cbtype}{1:08d}"

        except Exception as e:
            logger.error(f"Error getting next entry for '{cbtype}': {e}")
            return f"{cbtype}{1:08d}"

    def increment_atype_entry(self, cbtype: str) -> str:
        """
        Increment the ay_entry counter for a type code and return the next available entry.

        Includes a defensive check: if the entry number from atype already exists
        in aentry (e.g. Opera created it directly, or counter was reset), skip
        forward until an unused entry number is found.

        Args:
            cbtype: Type code (e.g., 'P1', 'R2', 'P5')

        Returns:
            Entry number to use for this transaction (guaranteed not in aentry)
        """
        try:
            table = self._open_table('atype')

            for record in table:
                code = record.ay_cbtype.strip() if hasattr(record, 'ay_cbtype') else ''
                if code == cbtype:
                    current_entry = record.ay_entry.strip() if hasattr(record, 'ay_entry') else f"{cbtype}{0:08d}"

                    # Parse entry number
                    prefix_len = len(cbtype)
                    try:
                        current_num = int(current_entry[prefix_len:])
                    except ValueError:
                        current_num = 0

                    # Defensive check: verify entry doesn't already exist in aentry.
                    # If ay_entry got out of sync, skip forward to avoid batch collision.
                    entry_to_use = current_entry
                    entry_num = current_num
                    skipped = 0
                    try:
                        aentry_table = self._open_table('aentry')
                        existing_entries = set()
                        for ae_rec in aentry_table:
                            ae_cbtype = ae_rec.ae_cbtype.strip() if hasattr(ae_rec, 'ae_cbtype') else ''
                            ae_entry = ae_rec.ae_entry.strip() if hasattr(ae_rec, 'ae_entry') else ''
                            if ae_cbtype == cbtype:
                                existing_entries.add(ae_entry)

                        while entry_to_use in existing_entries:
                            logger.warning(f"Entry {entry_to_use} already exists in aentry for cbtype {cbtype}, skipping")
                            skipped += 1
                            entry_num += 1
                            entry_to_use = f"{cbtype}{entry_num:08d}"
                            if skipped > 100:
                                raise ValueError(f"Unable to find unused entry number for cbtype '{cbtype}' after 100 attempts")
                    except ValueError:
                        raise
                    except Exception as e:
                        logger.warning(f"Could not verify aentry for collision check: {e}, proceeding with {entry_to_use}")

                    # Update atype to one past the entry we're using
                    next_entry = f"{cbtype}{entry_num + 1:08d}"
                    with record:
                        record.ay_entry = next_entry

                    if skipped > 0:
                        logger.warning(f"Skipped {skipped} existing entries for {cbtype}: atype counter was behind. Using {entry_to_use}, updated atype to {next_entry}")
                    else:
                        logger.debug(f"Incremented atype entry for {cbtype}: {current_entry} -> {next_entry}")

                    return entry_to_use

            raise ValueError(f"Type code '{cbtype}' not found in atype")

        except Exception as e:
            logger.error(f"Error incrementing atype entry for '{cbtype}': {e}")
            raise

    def get_default_cbtype(self, transaction_type: str) -> Optional[str]:
        """
        Get a default type code for a transaction type.

        Args:
            transaction_type: One of 'purchase_payment', 'purchase_refund',
                            'sales_receipt', 'sales_refund', 'nominal_payment', 'nominal_receipt'

        Returns:
            Default type code or None if not found
        """
        type_info = TRANSACTION_TYPE_MAP.get(transaction_type)
        if not type_info:
            return None

        required_category = type_info['ay_type']
        types = self.get_available_types(required_category)

        return types[0]['code'] if types else None

    def _get_supplier_name(self, supplier_account: str) -> Optional[str]:
        """Get supplier name from pname table"""
        try:
            table = self._open_table('pname')
            for record in table:
                if record.pn_account.strip().upper() == supplier_account.upper():
                    return record.pn_name.strip()
        except Exception as e:
            logger.error(f"Error getting supplier name: {e}")
        return None

    def _get_customer_name(self, customer_account: str) -> Optional[str]:
        """Get customer name from sname table"""
        try:
            table = self._open_table('sname')
            for record in table:
                if record.sn_account.strip().upper() == customer_account.upper():
                    return record.sn_name.strip()
        except Exception as e:
            logger.error(f"Error getting customer name: {e}")
        return None

    def _get_bank_name(self, bank_account: str) -> Optional[str]:
        """Get bank description from nbank table"""
        try:
            table = self._open_table('nbank')
            for record in table:
                if record.nk_acnt.strip().upper() == bank_account.upper():
                    return record.nk_desc.strip()
        except Exception as e:
            logger.error(f"Error getting bank name: {e}")
        return None

    def _get_supplier_control_account(self, supplier_account: str) -> str:
        """Get creditors control account for a supplier"""
        # Get company default from Opera config (nparm) — NEVER hardcode account codes
        try:
            from sql_rag.opera3_config import Opera3Config
            config = Opera3Config(self.data_path)
            defaults = config.get_control_accounts()
            default_control = defaults.creditors_control
        except Exception as e:
            raise ValueError(
                f"Cannot determine creditors control account: Opera3Config failed ({e}). "
                "Control accounts vary by company — they must be read from Opera configuration."
            )

        try:
            # Get supplier's profile code
            pname_table = self._open_table('pname')
            profile_code = None
            for record in pname_table:
                if record.pn_account.strip().upper() == supplier_account.upper():
                    profile_code = record.pn_sprfl.strip() if hasattr(record, 'pn_sprfl') else ''
                    break

            if not profile_code:
                return default_control

            # Look up control account from profile
            try:
                pprfls_table = self._open_table('pprfls')
                for record in pprfls_table:
                    if record.pc_code.strip().upper() == profile_code.upper():
                        control = record.pc_crdctrl.strip() if hasattr(record, 'pc_crdctrl') else ''
                        if control:
                            return control
                        break
            except FileNotFoundError:
                pass

            return default_control

        except Exception as e:
            logger.error(f"Error getting supplier control account: {e}")
            return default_control

    def _get_customer_control_account(self, customer_account: str) -> str:
        """Get debtors control account for a customer"""
        # Get company default from Opera config (nparm) — NEVER hardcode account codes
        try:
            from sql_rag.opera3_config import Opera3Config
            config = Opera3Config(self.data_path)
            defaults = config.get_control_accounts()
            default_control = defaults.debtors_control
        except Exception as e:
            raise ValueError(
                f"Cannot determine debtors control account: Opera3Config failed ({e}). "
                "Control accounts vary by company — they must be read from Opera configuration."
            )

        try:
            # Get customer's profile code
            sname_table = self._open_table('sname')
            profile_code = None
            for record in sname_table:
                if record.sn_account.strip().upper() == customer_account.upper():
                    profile_code = record.sn_sprfl.strip() if hasattr(record, 'sn_sprfl') else ''
                    break

            if not profile_code:
                return default_control

            # Look up control account from profile
            try:
                sprfls_table = self._open_table('sprfls')
                for record in sprfls_table:
                    if record.sc_code.strip().upper() == profile_code.upper():
                        control = record.sc_dbtctrl.strip() if hasattr(record, 'sc_dbtctrl') else ''
                        if control:
                            return control
                        break
            except FileNotFoundError:
                pass

            return default_control

        except Exception as e:
            logger.error(f"Error getting customer control account: {e}")
            return default_control

    def _update_supplier_balance(self, supplier_account: str, amount_change: float):
        """Update supplier balance in pname"""
        try:
            table = self._open_table('pname')
            for record in table:
                if record.pn_account.strip().upper() == supplier_account.upper():
                    with record:
                        record.pn_currbal = float(record.pn_currbal or 0) + amount_change
                    break
        except Exception as e:
            logger.error(f"Error updating supplier balance: {e}")
            raise  # Fail the transaction - supplier balance must be updated correctly

    def _update_customer_balance(self, customer_account: str, amount_change: float):
        """Update customer balance in sname"""
        try:
            table = self._open_table('sname')
            for record in table:
                if record.sn_account.strip().upper() == customer_account.upper():
                    with record:
                        record.sn_currbal = float(record.sn_currbal or 0) + amount_change
                    break
        except Exception as e:
            logger.error(f"Error updating customer balance: {e}")
            raise  # Fail the transaction - customer balance must be updated correctly

    def _update_nacnt_balance(self, account: str, value: float, period: int, year: int = None):
        """
        Update nacnt (nominal account balance) after posting to ntran.

        Opera updates nacnt whenever it posts to ntran. This ensures the
        nominal account balances stay in sync with the transaction totals.

        Args:
            account: Nominal account code (e.g., 'BC010', 'BB020')
            value: Transaction value in POUNDS (positive=DR, negative=CR)
            period: Posting period (1-12 for Jan-Dec)

        The update pattern based on Opera's behavior:
        - Positive value (DEBIT): na_ptddr += value, na_ytddr += value
        - Negative value (CREDIT): na_ptdcr += ABS(value), na_ytdcr += ABS(value)
        - Always: na_balc{period} += value (net balance per period)
        """
        if period < 1 or period > 24:
            logger.warning(f"Invalid period {period} for nacnt update, skipping")
            return

        try:
            table = self._open_table('nacnt')
            found = False

            for record in table:
                if record.na_acnt.strip().upper() == account.upper():
                    with record:
                        # Get current values, defaulting to 0 if None
                        ptddr = float(record.na_ptddr or 0)
                        ptdcr = float(record.na_ptdcr or 0)
                        ytddr = float(record.na_ytddr or 0)
                        ytdcr = float(record.na_ytdcr or 0)

                        # Update period balance field (na_balc01-na_balc24)
                        period_field = f"na_balc{period:02d}"
                        period_bal = float(getattr(record, period_field, 0) or 0)

                        if value >= 0:
                            # DEBIT entry
                            record.na_ptddr = ptddr + value
                            record.na_ytddr = ytddr + value
                        else:
                            # CREDIT entry
                            abs_value = abs(value)
                            record.na_ptdcr = ptdcr + abs_value
                            record.na_ytdcr = ytdcr + abs_value

                        # Period balance always gets the signed value
                        setattr(record, period_field, period_bal + value)

                    found = True
                    logger.debug(f"Updated nacnt for {account}: value={value}, period={period}")
                    break

            if not found:
                raise ValueError(f"nacnt update: account {account} not found in nacnt table")

        except Exception as e:
            logger.error(f"Failed to update nacnt for {account}: {e}")
            raise  # Fail the transaction - nacnt must be updated correctly

        # Also update nhist (nominal history) — Opera always updates both together
        try:
            self._update_nhist(account, value, period, year)
        except Exception as e:
            logger.error(f"Failed to update nhist for {account}: {e}")
            raise

    def _update_nbank_balance(self, bank_account: str, amount_pounds: float):
        """
        Update nbank.nk_curbal (bank current balance) after posting cashbook transactions.

        Opera updates nbank whenever cashbook transactions are posted. This ensures
        the bank balance stays in sync with the cashbook transaction totals.

        Args:
            bank_account: Bank nominal account code (e.g., 'BC010', 'BC026')
            amount_pounds: Transaction value in POUNDS (positive=receipt/increases balance,
                          negative=payment/decreases balance)

        Note: nbank.nk_curbal is stored in PENCE, so we convert pounds to pence.
        """
        # Convert pounds to pence for nbank storage
        amount_pence = int(round(amount_pounds * 100))

        try:
            table = self._open_table('nbank')
            found = False

            for record in table:
                if record.nk_acnt.strip().upper() == bank_account.upper():
                    with record:
                        current_bal = int(record.nk_curbal or 0)
                        record.nk_curbal = current_bal + amount_pence
                    found = True
                    logger.debug(f"Updated nbank for {bank_account}: amount_pounds={amount_pounds}, amount_pence={amount_pence}")
                    break

            if not found:
                # Bank account may not exist in nbank - log warning but don't fail
                logger.warning(f"nbank update: account {bank_account} not found - may not be a bank account")

        except Exception as e:
            logger.error(f"Failed to update nbank for {bank_account}: {e}")
            raise  # Fail the transaction - bank balance must be updated correctly

    def import_purchase_payment(
        self,
        bank_account: str,
        supplier_account: str,
        amount_pounds: float,
        reference: str,
        post_date: date,
        input_by: str = "IMPORT",
        creditors_control: str = None,
        payment_type: str = "Direct Cr",
        cbtype: str = None,
        validate_only: bool = False
    ) -> Opera3ImportResult:
        """
        Import a purchase payment into Opera 3.

        Creates records in:
        1. aentry (Cashbook Entry Header)
        2. atran (Cashbook Transaction)
        3. ntran (Nominal Ledger - 2 rows for double-entry)
        4. ptran (Purchase Ledger Transaction)
        5. palloc (Purchase Allocation)
        6. pname (Balance update)
        7. atype (Entry counter update)

        Args:
            bank_account: Bank account code (e.g., 'BC010')
            supplier_account: Supplier account code
            amount_pounds: Payment amount in POUNDS (positive value)
            reference: Payment reference
            post_date: Posting date
            input_by: User code for audit trail
            creditors_control: Creditors control account (auto-detected if None)
            payment_type: Payment type description
            cbtype: Cashbook type code from atype (e.g., 'P5'). Must be Payment type (ay_type='P').
                   If None, uses first available Payment type.
            validate_only: If True, only validate without inserting

        Returns:
            Opera3ImportResult with operation details
        """
        errors = []
        warnings = []

        # =====================
        # VALIDATE/GET CBTYPE
        # =====================
        if cbtype is None:
            cbtype = self.get_default_cbtype('purchase_payment')
            if cbtype is None:
                return Opera3ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=["No Payment type codes found in atype table"]
                )
            logger.debug(f"Using default cbtype for purchase payment: {cbtype}")

        # Validate the type code
        type_validation = self.validate_cbtype(cbtype, required_category=AtypeCategory.PAYMENT)
        if not type_validation['valid']:
            return Opera3ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[type_validation['error']]
            )

        # Get the correct at_type for purchase payments (always 5)
        at_type = CashbookTransactionType.PURCHASE_PAYMENT

        try:
            # =====================
            # PERIOD VALIDATION (Purchase Ledger)
            # =====================
            from sql_rag.opera3_config import Opera3Config, get_period_posting_decision
            config = Opera3Config(str(self.data_path), self.encoding)
            period_result = config.validate_posting_period(post_date, ledger_type='PL')

            if not period_result.is_valid:
                return Opera3ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[period_result.error_message]
                )

            # Get period posting decision (determines if we post to nominal, transfer file, or both)
            posting_decision = get_period_posting_decision(config, post_date, 'PL')

            # Get supplier name
            supplier_name = self._get_supplier_name(supplier_account)
            if not supplier_name:
                errors.append(f"Supplier account '{supplier_account}' not found")
                return Opera3ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=errors
                )

            # Get control account
            if creditors_control is None:
                creditors_control = self._get_supplier_control_account(supplier_account)

            if validate_only:
                return Opera3ImportResult(
                    success=True,
                    records_processed=1,
                    records_imported=1,
                    warnings=["Validation passed - no records inserted (validate_only=True)"]
                )

            # =====================
            # PREPARE DATA BEFORE ACQUIRING LOCKS (minimize lock duration)
            # =====================
            amount_pence = int(round(amount_pounds * 100))

            if isinstance(post_date, str):
                post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

            period, year = config.get_period_for_date(post_date)

            now = datetime.now()

            # Build trnref like Opera does
            ntran_comment = f"{reference[:50]:<50}"
            ntran_trnref = f"{supplier_name[:30]:<30}{payment_type:<10}(RT)     "

            # Generate unique IDs
            unique_ids = OperaUniqueIdGenerator.generate_multiple(3)
            atran_unique = unique_ids[0]
            ntran_pstid_bank = unique_ids[1]
            ntran_pstid_control = unique_ids[2]

            # =====================
            # ACQUIRE LOCKS AND EXECUTE TRANSACTION
            # Equivalent to SQL SE's BEGIN TRAN with UPDLOCK, ROWLOCK
            # =====================
            # Include transfer file tables in lock list
            tables_to_lock = ['aentry', 'atran', 'ptran', 'pname', 'nparm']
            if posting_decision.post_to_nominal:
                tables_to_lock.extend(['ntran', 'nacnt', 'nbank', 'nhist'])  # Include balance tables
            if posting_decision.post_to_transfer_file:
                tables_to_lock.append('anoml')  # Opera uses anoml for both sides of payment

            # Add atype to lock list for entry number update
            tables_to_lock.append('atype')

            with self._transaction_lock(tables_to_lock):
                # Get next entry number from atype and increment counter
                entry_number = self.increment_atype_entry(cbtype)
                journal_number = self._get_next_journal()

                logger.info(f"PURCHASE_PAYMENT_DEBUG: Starting import for supplier={supplier_account}")
                logger.info(f"PURCHASE_PAYMENT_DEBUG: amount_pounds={amount_pounds}, entry={entry_number}")

                # 1. INSERT INTO aentry
                aentry_table = self._open_table('aentry')
                aentry_table.append({
                    'ae_acnt': bank_account[:8],
                    'ae_cntr': '    ',
                    'ae_cbtype': cbtype,
                    'ae_entry': entry_number[:12],
                    'ae_reclnum': 0,
                    'ae_lstdate': post_date,
                    'ae_frstat': 0,
                    'ae_tostat': 0,
                    'ae_statln': 0,
                    'ae_entref': reference[:20],
                    'ae_value': -amount_pence,
                    'ae_recbal': 0,
                    'ae_remove': 0,
                    'ae_tmpstat': 0,
                    'ae_complet': 1,  # Always complete — NL transfer via anoml when real-time update is off
                    'ae_postgrp': 0,
                    'sq_crdate': now.date(),
                    'sq_crtime': now.strftime('%H:%M:%S')[:8],
                    'sq_cruser': input_by[:8],
                    'ae_comment': '',
                })

                # 2. INSERT INTO atran
                atran_table = self._open_table('atran')
                atran_table.append({
                    'at_acnt': bank_account[:8],
                    'at_cntr': '    ',
                    'at_cbtype': cbtype,
                    'at_entry': entry_number[:12],
                    'at_inputby': input_by[:8],
                    'at_type': at_type,
                    'at_pstdate': post_date,
                    'at_sysdate': post_date,
                    'at_tperiod': 1,
                    'at_value': -amount_pence,
                    'at_disc': 0,
                    'at_fcurr': '   ',
                    'at_fcexch': 1.0,
                    'at_fcmult': 0,
                    'at_fcdec': 2,
                    'at_account': supplier_account[:8],
                    'at_name': supplier_name[:35],
                    'at_comment': '',
                    'at_payee': '        ',
                    'at_payname': '',
                    'at_sort': '        ',
                    'at_number': '         ',
                    'at_remove': 0,
                    'at_chqprn': 0,
                    'at_chqlst': 0,
                    'at_bacprn': 0,
                    'at_ccdprn': 0,
                    'at_ccdno': '',
                    'at_payslp': 0,
                    'at_pysprn': 0,
                    'at_cash': 0,
                    'at_remit': 0,
                    'at_unique': atran_unique[:10],
                    'at_postgrp': 0,
                    'at_ccauth': '0       ',
                    'at_refer': reference[:20],
                    'at_srcco': 'I',
                })

                # 3. Nominal postings - CONDITIONAL based on period posting decision
                if posting_decision.post_to_nominal:
                    ntran_table = self._open_table('ntran')
                    bank_type = self._get_nacnt_type(bank_account) or ('B ', 'BC')
                    control_type = self._get_nacnt_type(creditors_control) or ('C ', 'CA')
                    # INSERT INTO ntran - CREDIT Bank
                    ntran_table.append({
                        'nt_acnt': bank_account[:8],
                        'nt_cntr': '    ',
                        'nt_type': bank_type[0],
                        'nt_subt': bank_type[1],
                        'nt_jrnl': journal_number,
                        'nt_ref': '',
                        'nt_inp': input_by[:10],
                        'nt_trtype': 'A',
                        'nt_cmnt': ntran_comment[:50],
                        'nt_trnref': ntran_trnref[:50],
                        'nt_entr': post_date,
                        'nt_value': -amount_pounds,
                        'nt_year': year,
                        'nt_period': period,
                        'nt_rvrse': 0,
                        'nt_prevyr': 0,
                        'nt_consol': 0,
                        'nt_fcurr': '   ',
                        'nt_fvalue': 0,
                        'nt_fcrate': 0,
                        'nt_fcmult': 0,
                        'nt_fcdec': 0,
                        'nt_srcco': 'I',
                        'nt_cdesc': '',
                        'nt_project': '        ',
                        'nt_job': '        ',
                        'nt_posttyp': 'P',
                        'nt_pstgrp': 0,
                        'nt_pstid': ntran_pstid_bank[:10],
                        'nt_srcnlid': 0,
                        'nt_recurr': 0,
                        'nt_perpost': 0,
                        'nt_rectify': 0,
                        'nt_recjrnl': 0,
                        'nt_vatanal': 0,
                        'nt_distrib': 0,
                    })

                    # INSERT INTO ntran - DEBIT Creditors Control
                    ntran_table.append({
                        'nt_acnt': creditors_control[:8],
                        'nt_cntr': '    ',
                        'nt_type': control_type[0],
                        'nt_subt': control_type[1],
                        'nt_jrnl': journal_number,
                        'nt_ref': '',
                        'nt_inp': input_by[:10],
                        'nt_trtype': 'A',
                        'nt_cmnt': ntran_comment[:50],
                        'nt_trnref': ntran_trnref[:50],
                        'nt_entr': post_date,
                        'nt_value': amount_pounds,
                        'nt_year': year,
                        'nt_period': period,
                        'nt_rvrse': 0,
                        'nt_prevyr': 0,
                        'nt_consol': 0,
                        'nt_fcurr': '   ',
                        'nt_fvalue': 0,
                        'nt_fcrate': 0,
                        'nt_fcmult': 0,
                        'nt_fcdec': 0,
                        'nt_srcco': 'I',
                        'nt_cdesc': '',
                        'nt_project': '        ',
                        'nt_job': '        ',
                        'nt_posttyp': 'P',
                        'nt_pstgrp': 0,
                        'nt_pstid': ntran_pstid_control[:10],
                        'nt_srcnlid': 0,
                        'nt_recurr': 0,
                        'nt_perpost': 0,
                        'nt_rectify': 0,
                        'nt_recjrnl': 0,
                        'nt_vatanal': 0,
                        'nt_distrib': 0,
                    })

                    # Update nacnt balances for both accounts
                    self._update_nacnt_balance(bank_account, -amount_pounds, period, year)
                    self._update_nacnt_balance(creditors_control, amount_pounds, period, year)

                # Update nbank balance (payment decreases bank balance) - ALWAYS when atran created
                self._update_nbank_balance(bank_account, -amount_pounds)

                # 4. INSERT INTO transfer files (anoml only - Opera uses anoml for both sides of payment)
                if posting_decision.post_to_transfer_file:
                    done_flag = posting_decision.transfer_file_done_flag
                    jrnl_num = journal_number if posting_decision.post_to_nominal else 0

                    try:
                        anoml_table = self._open_table('anoml')

                        # anoml record 1 - Bank account (credit - money going out)
                        anoml_table.append({
                            'ax_nacnt': bank_account[:10],
                            'ax_ncntr': '    ',
                            'ax_source': 'P',
                            'ax_date': post_date,
                            'ax_value': -amount_pounds,
                            'ax_tref': reference[:20],
                            'ax_comment': ntran_comment[:50],
                            'ax_done': done_flag,
                            'ax_fcurr': '   ',
                            'ax_fvalue': 0,
                            'ax_fcrate': 0,
                            'ax_fcmult': 0,
                            'ax_fcdec': 0,
                            'ax_srcco': 'I',
                            'ax_unique': atran_unique[:10],
                            'ax_project': '        ',
                            'ax_job': '        ',
                            'ax_jrnl': jrnl_num,
                            'ax_nlpdate': post_date,
                        })

                        # anoml record 2 - Creditors control account (debit - reducing liability)
                        anoml_table.append({
                            'ax_nacnt': creditors_control[:10],
                            'ax_ncntr': '    ',
                            'ax_source': 'P',
                            'ax_date': post_date,
                            'ax_value': amount_pounds,
                            'ax_tref': reference[:20],
                            'ax_comment': ntran_comment[:50],
                            'ax_done': done_flag,
                            'ax_fcurr': '   ',
                            'ax_fvalue': 0,
                            'ax_fcrate': 0,
                            'ax_fcmult': 0,
                            'ax_fcdec': 0,
                            'ax_srcco': 'I',
                            'ax_unique': atran_unique[:10],
                            'ax_project': '        ',
                            'ax_job': '        ',
                            'ax_jrnl': jrnl_num,
                            'ax_nlpdate': post_date,
                        })
                    except FileNotFoundError:
                        logger.warning("anoml table not found - skipping transfer file")

                # 5. INSERT INTO ptran
                ptran_table = self._open_table('ptran')
                ptran_table.append({
                    'pt_account': supplier_account[:8],
                    'pt_trdate': post_date,
                    'pt_trref': reference[:20],
                    'pt_supref': payment_type[:20],
                    'pt_trtype': 'P',
                    'pt_trvalue': -amount_pounds,
                    'pt_vatval': 0,
                    'pt_trbal': -amount_pounds,
                    'pt_paid': ' ',
                    'pt_crdate': post_date,
                    'pt_advance': 'N',
                    'pt_payflag': 0,
                    'pt_set1day': 0,
                    'pt_set1': 0,
                    'pt_set2day': 0,
                    'pt_set2': 0,
                    'pt_held': ' ',
                    'pt_fcurr': '   ',
                    'pt_fcrate': 0,
                    'pt_fcdec': 0,
                    'pt_fcval': 0,
                    'pt_fcbal': 0,
                    'pt_adval': 0,
                    'pt_fadval': 0,
                    'pt_fcmult': 0,
                    'pt_cbtype': cbtype,
                    'pt_entry': entry_number[:12],
                    'pt_unique': atran_unique[:10],
                    'pt_suptype': '   ',
                    'pt_euro': 0,
                    'pt_nlpdate': post_date,  # Nominal Ledger Post Date
                })

                # NOTE: No palloc created at posting time -- allocation happens separately

                # 7. Update supplier balance
                self._update_supplier_balance(supplier_account, -amount_pounds)

                # Build list of tables updated based on what was actually done
                tables_updated = ["aentry", "atran", "ptran", "pname"]
                if posting_decision.post_to_nominal:
                    tables_updated.insert(2, "ntran (2)")
                if posting_decision.post_to_transfer_file:
                    tables_updated.append("anoml (2)")  # Opera uses anoml for both bank and control

                posting_mode = "Current period - posted to nominal" if posting_decision.post_to_nominal else "Different period - transfer file only (pending NL post)"

                logger.info(f"Successfully imported purchase payment: {entry_number} for £{amount_pounds:.2f} - {posting_mode}")

            # Post-commit ledger verification — ensures ptran was created
            self.verify_ledger_after_import('ptran', cbtype, entry_number, 1, supplier_account)

            # Return result after releasing locks
            return Opera3ImportResult(
                success=True,
                records_processed=1,
                records_imported=1,
                entry_number=entry_number,
                journal_number=journal_number,
                warnings=[
                    f"Entry number: {entry_number}",
                    f"Journal number: {journal_number}",
                    f"Amount: £{amount_pounds:.2f}",
                    f"Posting mode: {posting_mode}",
                    f"Tables updated: {', '.join(tables_updated)}"
                ]
            )

        except Exception as e:
            logger.error(f"Failed to import purchase payment: {e}")
            return Opera3ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[str(e)]
            )
        finally:
            self._close_all_tables()

    def import_sales_receipt(
        self,
        bank_account: str,
        customer_account: str,
        amount_pounds: float,
        reference: str,
        post_date: date,
        input_by: str = "IMPORT",
        debtors_control: str = None,
        receipt_type: str = "BACS",
        cbtype: str = None,
        validate_only: bool = False
    ) -> Opera3ImportResult:
        """
        Import a sales receipt into Opera 3.

        Creates records in:
        1. aentry (Cashbook Entry Header)
        2. atran (Cashbook Transaction)
        3. ntran (Nominal Ledger - 2 rows for double-entry)
        4. stran (Sales Ledger Transaction)
        5. salloc (Sales Allocation)
        6. sname (Balance update)
        7. atype (Entry counter update)

        Args:
            bank_account: Bank account code (e.g., 'BC010')
            customer_account: Customer account code
            amount_pounds: Receipt amount in POUNDS (positive value)
            reference: Receipt reference
            post_date: Posting date
            input_by: User code for audit trail
            debtors_control: Debtors control account (auto-detected if None)
            receipt_type: Receipt type description
            cbtype: Cashbook type code from atype (e.g., 'R2'). Must be Receipt type (ay_type='R').
                   If None, uses first available Receipt type.
            validate_only: If True, only validate without inserting

        Returns:
            Opera3ImportResult with operation details
        """
        errors = []
        warnings = []

        # =====================
        # VALIDATE/GET CBTYPE
        # =====================
        if cbtype is None:
            cbtype = self.get_default_cbtype('sales_receipt')
            if cbtype is None:
                return Opera3ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=["No Receipt type codes found in atype table"]
                )
            logger.debug(f"Using default cbtype for sales receipt: {cbtype}")

        # Validate the type code
        type_validation = self.validate_cbtype(cbtype, required_category=AtypeCategory.RECEIPT)
        if not type_validation['valid']:
            return Opera3ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[type_validation['error']]
            )

        # Get the correct at_type for sales receipts (always 4)
        at_type = CashbookTransactionType.SALES_RECEIPT

        try:
            # =====================
            # PERIOD VALIDATION (Sales Ledger)
            # =====================
            from sql_rag.opera3_config import Opera3Config, get_period_posting_decision
            config = Opera3Config(str(self.data_path), self.encoding)
            period_result = config.validate_posting_period(post_date, ledger_type='SL')

            if not period_result.is_valid:
                return Opera3ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[period_result.error_message]
                )

            # Get period posting decision (determines if we post to nominal, transfer file, or both)
            posting_decision = get_period_posting_decision(config, post_date, 'SL')

            # Get customer name
            customer_name = self._get_customer_name(customer_account)
            if not customer_name:
                errors.append(f"Customer account '{customer_account}' not found")
                return Opera3ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=errors
                )

            # Get control account
            if debtors_control is None:
                debtors_control = self._get_customer_control_account(customer_account)

            if validate_only:
                return Opera3ImportResult(
                    success=True,
                    records_processed=1,
                    records_imported=1,
                    warnings=["Validation passed - no records inserted (validate_only=True)"]
                )

            # =====================
            # PREPARE DATA BEFORE ACQUIRING LOCKS (minimize lock duration)
            # =====================
            amount_pence = int(round(amount_pounds * 100))

            if isinstance(post_date, str):
                post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

            period, year = config.get_period_for_date(post_date)

            now = datetime.now()

            # Build trnref like Opera does
            ntran_comment = f"{reference[:50]:<50}"
            ntran_trnref = f"{customer_name[:30]:<30}{receipt_type:<10}(RT)     "

            # Generate unique IDs
            unique_ids = OperaUniqueIdGenerator.generate_multiple(3)
            atran_unique = unique_ids[0]
            ntran_pstid_bank = unique_ids[1]
            ntran_pstid_control = unique_ids[2]

            # =====================
            # ACQUIRE LOCKS AND EXECUTE TRANSACTION
            # Equivalent to SQL SE's BEGIN TRAN with UPDLOCK, ROWLOCK
            # =====================
            # Include transfer file tables in lock list
            tables_to_lock = ['aentry', 'atran', 'stran', 'sname', 'atype', 'nparm']
            if posting_decision.post_to_nominal:
                tables_to_lock.extend(['ntran', 'nacnt', 'nbank', 'nhist'])  # Include balance tables
            if posting_decision.post_to_transfer_file:
                tables_to_lock.append('anoml')  # Opera uses anoml for both sides of receipt

            with self._transaction_lock(tables_to_lock):
                # Get next entry number from atype and increment counter
                entry_number = self.increment_atype_entry(cbtype)
                journal_number = self._get_next_journal()

                logger.info(f"SALES_RECEIPT_DEBUG: Starting import for customer={customer_account}")
                logger.info(f"SALES_RECEIPT_DEBUG: amount_pounds={amount_pounds}, entry={entry_number}")

                # 1. INSERT INTO aentry
                aentry_table = self._open_table('aentry')
                aentry_table.append({
                    'ae_acnt': bank_account[:8],
                    'ae_cntr': '    ',
                'ae_cbtype': cbtype,
                'ae_entry': entry_number[:12],
                'ae_reclnum': 0,
                'ae_lstdate': post_date,
                'ae_frstat': 0,
                'ae_tostat': 0,
                'ae_statln': 0,
                'ae_entref': reference[:20],
                'ae_value': amount_pence,  # Positive for receipts
                'ae_recbal': 0,
                'ae_remove': 0,
                'ae_tmpstat': 0,
                'ae_complet': 1,  # Always complete — NL transfer via anoml when real-time update is off
                'ae_postgrp': 0,
                'sq_crdate': now.date(),
                'sq_crtime': now.strftime('%H:%M:%S')[:8],
                'sq_cruser': input_by[:8],
                    'ae_comment': '',
                })

                # 2. INSERT INTO atran
                atran_table = self._open_table('atran')
                atran_table.append({
                    'at_acnt': bank_account[:8],
                    'at_cntr': '    ',
                    'at_cbtype': cbtype,
                    'at_entry': entry_number[:12],
                    'at_inputby': input_by[:8],
                    'at_type': at_type,
                    'at_pstdate': post_date,
                    'at_sysdate': post_date,
                    'at_tperiod': 1,
                    'at_value': amount_pence,  # Positive for receipts
                    'at_disc': 0,
                    'at_fcurr': '   ',
                    'at_fcexch': 1.0,
                    'at_fcmult': 0,
                    'at_fcdec': 2,
                    'at_account': customer_account[:8],
                    'at_name': customer_name[:35],
                    'at_comment': '',
                    'at_payee': '        ',
                    'at_payname': '',
                    'at_sort': '        ',
                    'at_number': '         ',
                    'at_remove': 0,
                    'at_chqprn': 0,
                    'at_chqlst': 0,
                    'at_bacprn': 0,
                    'at_ccdprn': 0,
                    'at_ccdno': '',
                    'at_payslp': 0,
                    'at_pysprn': 0,
                    'at_cash': 0,
                    'at_remit': 0,
                    'at_unique': atran_unique[:10],
                    'at_postgrp': 0,
                    'at_ccauth': '0       ',
                    'at_refer': reference[:20],
                    'at_srcco': 'I',
                })

                # 3. Nominal postings - CONDITIONAL based on period posting decision
                if posting_decision.post_to_nominal:
                    ntran_table = self._open_table('ntran')
                    bank_type = self._get_nacnt_type(bank_account) or ('B ', 'BC')
                    control_type = self._get_nacnt_type(debtors_control) or ('B ', 'BB')
                    # INSERT INTO ntran - DEBIT Bank (money coming in)
                    ntran_table.append({
                        'nt_acnt': bank_account[:8],
                        'nt_cntr': '    ',
                        'nt_type': bank_type[0],
                        'nt_subt': bank_type[1],
                        'nt_jrnl': journal_number,
                        'nt_ref': '',
                        'nt_inp': input_by[:10],
                        'nt_trtype': 'A',
                        'nt_cmnt': ntran_comment[:50],
                        'nt_trnref': ntran_trnref[:50],
                        'nt_entr': post_date,
                        'nt_value': amount_pounds,  # Positive for receipts
                        'nt_year': year,
                        'nt_period': period,
                        'nt_rvrse': 0,
                        'nt_prevyr': 0,
                        'nt_consol': 0,
                        'nt_fcurr': '   ',
                        'nt_fvalue': 0,
                        'nt_fcrate': 0,
                        'nt_fcmult': 0,
                        'nt_fcdec': 0,
                        'nt_srcco': 'I',
                        'nt_cdesc': '',
                        'nt_project': '        ',
                        'nt_job': '        ',
                        'nt_posttyp': 'R',
                        'nt_pstgrp': 0,
                        'nt_pstid': ntran_pstid_bank[:10],
                        'nt_srcnlid': 0,
                        'nt_recurr': 0,
                        'nt_perpost': 0,
                        'nt_rectify': 0,
                        'nt_recjrnl': 0,
                        'nt_vatanal': 0,
                        'nt_distrib': 0,
                    })

                    # INSERT INTO ntran - CREDIT Debtors Control (matches SQL SE pattern)
                    ntran_table.append({
                        'nt_acnt': debtors_control[:8],
                        'nt_cntr': '    ',
                        'nt_type': control_type[0],
                        'nt_subt': control_type[1],
                        'nt_jrnl': journal_number,
                        'nt_ref': '',
                        'nt_inp': input_by[:10],
                        'nt_trtype': 'A',
                        'nt_cmnt': ntran_comment[:50],
                        'nt_trnref': ntran_trnref[:50],
                        'nt_entr': post_date,
                        'nt_value': -amount_pounds,  # Negative to credit debtors
                        'nt_year': year,
                        'nt_period': period,
                        'nt_rvrse': 0,
                        'nt_prevyr': 0,
                        'nt_consol': 0,
                        'nt_fcurr': '   ',
                        'nt_fvalue': 0,
                        'nt_fcrate': 0,
                        'nt_fcmult': 0,
                        'nt_fcdec': 0,
                        'nt_srcco': 'I',
                        'nt_cdesc': '',
                        'nt_project': '        ',
                        'nt_job': '        ',
                        'nt_posttyp': 'R',
                        'nt_pstgrp': 0,
                        'nt_pstid': ntran_pstid_control[:10],
                        'nt_srcnlid': 0,
                        'nt_recurr': 0,
                        'nt_perpost': 0,
                        'nt_rectify': 0,
                        'nt_recjrnl': 0,
                        'nt_vatanal': 0,
                        'nt_distrib': 0,
                    })

                    # Update nacnt balances for both accounts
                    self._update_nacnt_balance(bank_account, amount_pounds, period, year)
                    self._update_nacnt_balance(debtors_control, -amount_pounds, period, year)

                # Update nbank balance (receipt increases bank balance) - ALWAYS when atran created
                self._update_nbank_balance(bank_account, amount_pounds)

                # 4. INSERT INTO transfer files (anoml only - Opera uses anoml for both sides of receipt)
                if posting_decision.post_to_transfer_file:
                    done_flag = posting_decision.transfer_file_done_flag
                    jrnl_num = journal_number if posting_decision.post_to_nominal else 0

                    try:
                        anoml_table = self._open_table('anoml')

                        # anoml record 1 - Bank account (debit - money coming in)
                        anoml_table.append({
                            'ax_nacnt': bank_account[:10],
                            'ax_ncntr': '    ',
                            'ax_source': 'S',
                            'ax_date': post_date,
                            'ax_value': amount_pounds,
                            'ax_tref': reference[:20],
                            'ax_comment': ntran_comment[:50],
                            'ax_done': done_flag,
                            'ax_fcurr': '   ',
                            'ax_fvalue': 0,
                            'ax_fcrate': 0,
                            'ax_fcmult': 0,
                            'ax_fcdec': 0,
                            'ax_srcco': 'I',
                            'ax_unique': atran_unique[:10],
                            'ax_project': '        ',
                            'ax_job': '        ',
                            'ax_jrnl': jrnl_num,
                            'ax_nlpdate': post_date,
                        })

                        # anoml record 2 - Debtors control account (credit - reducing asset)
                        anoml_table.append({
                            'ax_nacnt': debtors_control[:10],
                            'ax_ncntr': '    ',
                            'ax_source': 'S',
                            'ax_date': post_date,
                            'ax_value': -amount_pounds,
                            'ax_tref': reference[:20],
                            'ax_comment': ntran_comment[:50],
                            'ax_done': done_flag,
                            'ax_fcurr': '   ',
                            'ax_fvalue': 0,
                            'ax_fcrate': 0,
                            'ax_fcmult': 0,
                            'ax_fcdec': 0,
                            'ax_srcco': 'I',
                            'ax_unique': atran_unique[:10],
                            'ax_project': '        ',
                            'ax_job': '        ',
                            'ax_jrnl': jrnl_num,
                            'ax_nlpdate': post_date,
                        })
                    except FileNotFoundError:
                        logger.warning("anoml table not found - skipping transfer file")

                # 5. INSERT INTO stran
                stran_table = self._open_table('stran')
                stran_table.append({
                    'st_account': customer_account[:8],
                    'st_trdate': post_date,
                    'st_trref': reference[:20],
                    'st_cusref': receipt_type[:20],
                    'st_trtype': 'R',
                    'st_trvalue': -amount_pounds,  # Negative for receipt (reduces debt)
                    'st_vatval': 0,
                    'st_trbal': -amount_pounds,
                    'st_paid': ' ',
                    'st_crdate': post_date,
                    'st_advance': 'N',
                    'st_payflag': 0,
                    'st_set1day': 0,
                    'st_set1': 0,
                    'st_set2day': 0,
                    'st_set2': 0,
                    'st_held': ' ',
                    'st_fcurr': '   ',
                    'st_fcrate': 0,
                    'st_fcdec': 0,
                    'st_fcval': 0,
                    'st_fcbal': 0,
                    'st_adval': 0,
                    'st_fadval': 0,
                    'st_fcmult': 0,
                    'st_cbtype': cbtype,
                    'st_entry': entry_number[:12],
                    'st_unique': atran_unique[:10],
                    'st_custype': '   ',
                    'st_euro': 0,
                    'st_nlpdate': post_date,
                })

                # NOTE: No salloc created at posting time -- allocation happens separately

                # 7. Update customer balance
                self._update_customer_balance(customer_account, -amount_pounds)

                # Build list of tables updated based on what was actually done
                tables_updated = ["aentry", "atran", "stran", "sname"]
                if posting_decision.post_to_nominal:
                    tables_updated.insert(2, "ntran (2)")
                if posting_decision.post_to_transfer_file:
                    tables_updated.append("anoml (2)")  # Opera uses anoml for both bank and control

                posting_mode = "Current period - posted to nominal" if posting_decision.post_to_nominal else "Different period - transfer file only (pending NL post)"

                logger.info(f"Successfully imported sales receipt: {entry_number} for £{amount_pounds:.2f} - {posting_mode}")

            # Post-commit ledger verification — ensures stran was created
            self.verify_ledger_after_import('stran', cbtype, entry_number, 1, customer_account)

            # Return result after releasing locks
            return Opera3ImportResult(
                success=True,
                records_processed=1,
                records_imported=1,
                entry_number=entry_number,
                journal_number=journal_number,
                warnings=[
                    f"Entry number: {entry_number}",
                    f"Journal number: {journal_number}",
                    f"Amount: £{amount_pounds:.2f}",
                    f"Posting mode: {posting_mode}",
                    f"Tables updated: {', '.join(tables_updated)}"
                ]
            )

        except Exception as e:
            logger.error(f"Failed to import sales receipt: {e}")
            return Opera3ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[str(e)]
            )
        finally:
            self._close_all_tables()

    def import_sales_refund(
        self,
        bank_account: str,
        customer_account: str,
        amount_pounds: float,
        reference: str,
        post_date: date,
        input_by: str = "IMPORT",
        debtors_control: str = None,
        payment_method: str = "BACS",
        cbtype: str = None,
        validate_only: bool = False,
        comment: str = ""
    ) -> Opera3ImportResult:
        """
        Import a sales refund into Opera 3.

        A sales refund is money OUT to a customer (at_type=3).
        This INCREASES the debtors balance (customer now owes more or refund is pending).

        Creates records in: aentry, atran, stran, ntran (2), anoml (2), sname, nbank, nacnt.
        """
        errors = []

        # Validate/get cbtype — sales refund is a PAYMENT category (money out)
        if cbtype is None:
            cbtype = self.get_default_cbtype('sales_refund')
            if cbtype is None:
                return Opera3ImportResult(
                    success=False, records_processed=1, records_failed=1,
                    errors=["No Payment type codes found in atype table"]
                )

        type_validation = self.validate_cbtype(cbtype, required_category=AtypeCategory.PAYMENT)
        if not type_validation['valid']:
            return Opera3ImportResult(
                success=False, records_processed=1, records_failed=1,
                errors=[type_validation['error']]
            )

        at_type = CashbookTransactionType.SALES_REFUND

        try:
            from sql_rag.opera3_config import Opera3Config, get_period_posting_decision
            config = Opera3Config(str(self.data_path), self.encoding)
            period_result = config.validate_posting_period(post_date, ledger_type='SL')

            if not period_result.is_valid:
                return Opera3ImportResult(
                    success=False, records_processed=1, records_failed=1,
                    errors=[period_result.error_message]
                )

            posting_decision = get_period_posting_decision(config, post_date, 'SL')

            customer_name = self._get_customer_name(customer_account)
            if not customer_name:
                return Opera3ImportResult(
                    success=False, records_processed=1, records_failed=1,
                    errors=[f"Customer account '{customer_account}' not found"]
                )

            if debtors_control is None:
                debtors_control = self._get_customer_control_account(customer_account)

            if validate_only:
                return Opera3ImportResult(
                    success=True, records_processed=1, records_imported=1,
                    warnings=["Validation passed - no records inserted (validate_only=True)"]
                )

            # Prepare data before locks
            amount_pence = int(round(amount_pounds * 100))
            if isinstance(post_date, str):
                post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

            period, year = config.get_period_for_date(post_date)
            now = datetime.now()

            ntran_comment = f"{comment or reference[:50]:<50}"
            ntran_trnref = f"{customer_name[:30]:<30}{payment_method:<10}(RF)     "

            unique_ids = OperaUniqueIdGenerator.generate_multiple(3)
            atran_unique = unique_ids[0]
            ntran_pstid_bank = unique_ids[1]
            ntran_pstid_control = unique_ids[2]

            tables_to_lock = ['aentry', 'atran', 'stran', 'sname', 'atype', 'nparm']
            if posting_decision.post_to_nominal:
                tables_to_lock.extend(['ntran', 'nacnt', 'nbank', 'nhist'])
            if posting_decision.post_to_transfer_file:
                tables_to_lock.append('anoml')

            with self._transaction_lock(tables_to_lock):
                entry_number = self.increment_atype_entry(cbtype)
                journal_number = self._get_next_journal()

                # 1. aentry — NEGATIVE value (money out)
                aentry_table = self._open_table('aentry')
                aentry_table.append({
                    'ae_acnt': bank_account[:8],
                    'ae_cntr': '    ',
                    'ae_cbtype': cbtype,
                    'ae_entry': entry_number[:12],
                    'ae_reclnum': 0,
                    'ae_lstdate': post_date,
                    'ae_frstat': 0,
                    'ae_tostat': 0,
                    'ae_statln': 0,
                    'ae_entref': reference[:20],
                    'ae_value': -amount_pence,
                    'ae_recbal': 0,
                    'ae_remove': 0,
                    'ae_tmpstat': 0,
                    'ae_complet': 1,  # Always complete — NL transfer via anoml when real-time update is off
                    'ae_postgrp': 0,
                    'sq_crdate': now.date(),
                    'sq_crtime': now.strftime('%H:%M:%S')[:8],
                    'sq_cruser': input_by[:8],
                    'ae_comment': (comment or '')[:40],
                })

                # 2. atran — at_type=3, NEGATIVE value
                atran_table = self._open_table('atran')
                atran_table.append({
                    'at_acnt': bank_account[:8],
                    'at_cntr': '    ',
                    'at_cbtype': cbtype,
                    'at_entry': entry_number[:12],
                    'at_inputby': input_by[:8],
                    'at_type': at_type,
                    'at_pstdate': post_date,
                    'at_sysdate': post_date,
                    'at_tperiod': 1,
                    'at_value': -amount_pence,
                    'at_disc': 0,
                    'at_fcurr': '   ',
                    'at_fcexch': 1.0,
                    'at_fcmult': 0,
                    'at_fcdec': 2,
                    'at_account': customer_account[:8],
                    'at_name': customer_name[:35],
                    'at_comment': (comment or '')[:40],
                    'at_payee': '        ',
                    'at_payname': '',
                    'at_sort': '        ',
                    'at_number': '         ',
                    'at_remove': 0,
                    'at_chqprn': 0,
                    'at_chqlst': 0,
                    'at_bacprn': 0,
                    'at_ccdprn': 0,
                    'at_ccdno': '',
                    'at_payslp': 0,
                    'at_pysprn': 0,
                    'at_cash': 0,
                    'at_remit': 0,
                    'at_unique': atran_unique[:10],
                    'at_postgrp': 0,
                    'at_ccauth': '0       ',
                    'at_refer': reference[:20],
                    'at_srcco': 'I',
                })

                # 3. ntran — CREDIT bank (money out), DEBIT debtors control (increasing debtors)
                if posting_decision.post_to_nominal:
                    ntran_table = self._open_table('ntran')
                    bank_type = self._get_nacnt_type(bank_account) or ('B ', 'BC')
                    control_type = self._get_nacnt_type(debtors_control) or ('B ', 'BB')

                    # CREDIT Bank (money going out)
                    ntran_table.append({
                        'nt_acnt': bank_account[:8],
                        'nt_cntr': '    ',
                        'nt_type': bank_type[0],
                        'nt_subt': bank_type[1],
                        'nt_jrnl': journal_number,
                        'nt_ref': '',
                        'nt_inp': input_by[:10],
                        'nt_trtype': 'A',
                        'nt_cmnt': ntran_comment[:50],
                        'nt_trnref': ntran_trnref[:50],
                        'nt_entr': post_date,
                        'nt_value': -amount_pounds,
                        'nt_pstid': ntran_pstid_bank[:10],
                        'nt_posttyp': 'S',
                        'nt_period': period,
                        'nt_year': year,
                    })

                    # DEBIT Debtors Control (increasing debtors)
                    ntran_table.append({
                        'nt_acnt': debtors_control[:8],
                        'nt_cntr': '    ',
                        'nt_type': control_type[0],
                        'nt_subt': control_type[1],
                        'nt_jrnl': journal_number,
                        'nt_ref': '',
                        'nt_inp': input_by[:10],
                        'nt_trtype': 'A',
                        'nt_cmnt': ntran_comment[:50],
                        'nt_trnref': ntran_trnref[:50],
                        'nt_entr': post_date,
                        'nt_value': amount_pounds,
                        'nt_pstid': ntran_pstid_control[:10],
                        'nt_posttyp': 'S',
                        'nt_period': period,
                        'nt_year': year,
                    })

                    self._update_nacnt_balance(bank_account, -amount_pounds, period, year)
                    self._update_nacnt_balance(debtors_control, amount_pounds, period, year)
                    self._update_nbank_balance(bank_account, -amount_pounds)

                # 4. anoml transfer file
                if posting_decision.post_to_transfer_file:
                    try:
                        anoml_table = self._open_table('anoml')
                        anoml_table.append({
                            'ax_acnt': bank_account[:8],
                            'ax_cntr': '    ',
                            'ax_cbtype': cbtype,
                            'ax_entry': entry_number[:12],
                            'ax_value': -amount_pounds,
                            'ax_source': 'S',
                            'ax_unique': atran_unique[:10],
                            'ax_done': 'Y' if posting_decision.post_to_nominal else 'N',
                        })
                        anoml_table.append({
                            'ax_acnt': debtors_control[:8],
                            'ax_cntr': '    ',
                            'ax_cbtype': cbtype,
                            'ax_entry': entry_number[:12],
                            'ax_value': amount_pounds,
                            'ax_source': 'S',
                            'ax_unique': atran_unique[:10],
                            'ax_done': 'Y' if posting_decision.post_to_nominal else 'N',
                        })
                    except FileNotFoundError:
                        logger.warning("anoml table not found - skipping transfer file")

                # 5. stran — type='F' (Refund), POSITIVE value (increases debtors)
                stran_table = self._open_table('stran')
                stran_table.append({
                    'st_account': customer_account[:8],
                    'st_trdate': post_date,
                    'st_trref': reference[:20],
                    'st_cusref': payment_method[:20],
                    'st_trtype': 'F',
                    'st_trvalue': amount_pounds,
                    'st_vatval': 0,
                    'st_trbal': amount_pounds,
                    'st_paid': ' ',
                    'st_crdate': post_date,
                    'st_advance': 'N',
                    'st_payflag': 0,
                    'st_set1day': 0,
                    'st_set1': 0,
                    'st_set2day': 0,
                    'st_set2': 0,
                    'st_held': ' ',
                    'st_fcurr': '   ',
                    'st_fcrate': 0,
                    'st_fcdec': 0,
                    'st_fcval': 0,
                    'st_fcbal': 0,
                    'st_adval': 0,
                    'st_fadval': 0,
                    'st_fcmult': 0,
                    'st_cbtype': cbtype,
                    'st_entry': entry_number[:12],
                    'st_unique': atran_unique[:10],
                    'st_custype': '   ',
                    'st_euro': 0,
                    'st_nlpdate': post_date,
                })

                # NOTE: No salloc created at posting time -- allocation happens separately

                # 6. Update customer balance — refund INCREASES what they owe
                self._update_customer_balance(customer_account, amount_pounds)

                tables_updated = ["aentry", "atran", "stran", "sname"]
                if posting_decision.post_to_nominal:
                    tables_updated.insert(2, "ntran (2)")
                if posting_decision.post_to_transfer_file:
                    tables_updated.append("anoml (2)")

            # Post-commit ledger verification — ensures stran was created
            self.verify_ledger_after_import('stran', cbtype, entry_number, 1, customer_account)

            return Opera3ImportResult(
                success=True,
                records_processed=1,
                records_imported=1,
                details=[
                    f"Sales refund for customer {customer_account}: £{amount_pounds:.2f}",
                    f"Entry: {entry_number}, Journal: {journal_number}",
                    f"Tables updated: {', '.join(tables_updated)}"
                ]
            )

        except Exception as e:
            logger.error(f"Failed to import sales refund: {e}")
            return Opera3ImportResult(
                success=False, records_processed=1, records_failed=1,
                errors=[str(e)]
            )
        finally:
            self._close_all_tables()

    def import_purchase_refund(
        self,
        bank_account: str,
        supplier_account: str,
        amount_pounds: float,
        reference: str,
        post_date: date,
        input_by: str = "IMPORT",
        creditors_control: str = None,
        payment_type: str = "Direct Cr",
        cbtype: str = None,
        validate_only: bool = False,
        comment: str = ""
    ) -> Opera3ImportResult:
        """
        Import a purchase refund into Opera 3.

        A purchase refund is money IN from a supplier (at_type=6).
        This INCREASES the creditors balance (debit note / refund pending).

        Creates records in: aentry, atran, ptran, ntran (2), anoml (2), pname, nbank, nacnt.
        """
        errors = []

        # Validate/get cbtype — purchase refund is a RECEIPT category (money in)
        if cbtype is None:
            cbtype = self.get_default_cbtype('purchase_refund')
            if cbtype is None:
                return Opera3ImportResult(
                    success=False, records_processed=1, records_failed=1,
                    errors=["No Receipt type codes found in atype table"]
                )

        type_validation = self.validate_cbtype(cbtype, required_category=AtypeCategory.RECEIPT)
        if not type_validation['valid']:
            return Opera3ImportResult(
                success=False, records_processed=1, records_failed=1,
                errors=[type_validation['error']]
            )

        at_type = CashbookTransactionType.PURCHASE_REFUND

        try:
            from sql_rag.opera3_config import Opera3Config, get_period_posting_decision
            config = Opera3Config(str(self.data_path), self.encoding)
            period_result = config.validate_posting_period(post_date, ledger_type='PL')

            if not period_result.is_valid:
                return Opera3ImportResult(
                    success=False, records_processed=1, records_failed=1,
                    errors=[period_result.error_message]
                )

            posting_decision = get_period_posting_decision(config, post_date, 'PL')

            supplier_name = self._get_supplier_name(supplier_account)
            if not supplier_name:
                return Opera3ImportResult(
                    success=False, records_processed=1, records_failed=1,
                    errors=[f"Supplier account '{supplier_account}' not found"]
                )

            if creditors_control is None:
                creditors_control = self._get_supplier_control_account(supplier_account)

            if validate_only:
                return Opera3ImportResult(
                    success=True, records_processed=1, records_imported=1,
                    warnings=["Validation passed - no records inserted (validate_only=True)"]
                )

            # Prepare data before locks
            amount_pence = int(round(amount_pounds * 100))
            if isinstance(post_date, str):
                post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

            period, year = config.get_period_for_date(post_date)
            now = datetime.now()

            ntran_comment = f"{comment or reference[:50]:<50}"
            ntran_trnref = f"{supplier_name[:30]:<30}{payment_type:<10}(RF)     "

            unique_ids = OperaUniqueIdGenerator.generate_multiple(3)
            atran_unique = unique_ids[0]
            ntran_pstid_bank = unique_ids[1]
            ntran_pstid_control = unique_ids[2]

            tables_to_lock = ['aentry', 'atran', 'ptran', 'pname', 'atype', 'nparm']
            if posting_decision.post_to_nominal:
                tables_to_lock.extend(['ntran', 'nacnt', 'nbank', 'nhist'])
            if posting_decision.post_to_transfer_file:
                tables_to_lock.append('anoml')

            with self._transaction_lock(tables_to_lock):
                entry_number = self.increment_atype_entry(cbtype)
                journal_number = self._get_next_journal()

                # 1. aentry — POSITIVE value (money in)
                aentry_table = self._open_table('aentry')
                aentry_table.append({
                    'ae_acnt': bank_account[:8],
                    'ae_cntr': '    ',
                    'ae_cbtype': cbtype,
                    'ae_entry': entry_number[:12],
                    'ae_reclnum': 0,
                    'ae_lstdate': post_date,
                    'ae_frstat': 0,
                    'ae_tostat': 0,
                    'ae_statln': 0,
                    'ae_entref': reference[:20],
                    'ae_value': amount_pence,
                    'ae_recbal': 0,
                    'ae_remove': 0,
                    'ae_tmpstat': 0,
                    'ae_complet': 1,  # Always complete — NL transfer via anoml when real-time update is off
                    'ae_postgrp': 0,
                    'sq_crdate': now.date(),
                    'sq_crtime': now.strftime('%H:%M:%S')[:8],
                    'sq_cruser': input_by[:8],
                    'ae_comment': (comment or '')[:40],
                })

                # 2. atran — at_type=6, POSITIVE value (money in)
                atran_table = self._open_table('atran')
                atran_table.append({
                    'at_acnt': bank_account[:8],
                    'at_cntr': '    ',
                    'at_cbtype': cbtype,
                    'at_entry': entry_number[:12],
                    'at_inputby': input_by[:8],
                    'at_type': at_type,
                    'at_pstdate': post_date,
                    'at_sysdate': post_date,
                    'at_tperiod': 1,
                    'at_value': amount_pence,
                    'at_disc': 0,
                    'at_fcurr': '   ',
                    'at_fcexch': 1.0,
                    'at_fcmult': 0,
                    'at_fcdec': 2,
                    'at_account': supplier_account[:8],
                    'at_name': supplier_name[:35],
                    'at_comment': (comment or '')[:40],
                    'at_payee': '        ',
                    'at_payname': '',
                    'at_sort': '        ',
                    'at_number': '         ',
                    'at_remove': 0,
                    'at_chqprn': 0,
                    'at_chqlst': 0,
                    'at_bacprn': 0,
                    'at_ccdprn': 0,
                    'at_ccdno': '',
                    'at_payslp': 0,
                    'at_pysprn': 0,
                    'at_cash': 0,
                    'at_remit': 0,
                    'at_unique': atran_unique[:10],
                    'at_postgrp': 0,
                    'at_ccauth': '0       ',
                    'at_refer': reference[:20],
                    'at_srcco': 'I',
                })

                # 3. ntran — DEBIT bank (money in), CREDIT creditors control (reducing liability)
                if posting_decision.post_to_nominal:
                    ntran_table = self._open_table('ntran')
                    bank_type = self._get_nacnt_type(bank_account) or ('B ', 'BC')
                    control_type = self._get_nacnt_type(creditors_control) or ('B ', 'BB')

                    # DEBIT Bank (money coming in)
                    ntran_table.append({
                        'nt_acnt': bank_account[:8],
                        'nt_cntr': '    ',
                        'nt_type': bank_type[0],
                        'nt_subt': bank_type[1],
                        'nt_jrnl': journal_number,
                        'nt_ref': '',
                        'nt_inp': input_by[:10],
                        'nt_trtype': 'A',
                        'nt_cmnt': ntran_comment[:50],
                        'nt_trnref': ntran_trnref[:50],
                        'nt_entr': post_date,
                        'nt_value': amount_pounds,
                        'nt_pstid': ntran_pstid_bank[:10],
                        'nt_posttyp': 'P',
                        'nt_period': period,
                        'nt_year': year,
                    })

                    # CREDIT Creditors Control (reducing liability)
                    ntran_table.append({
                        'nt_acnt': creditors_control[:8],
                        'nt_cntr': '    ',
                        'nt_type': control_type[0],
                        'nt_subt': control_type[1],
                        'nt_jrnl': journal_number,
                        'nt_ref': '',
                        'nt_inp': input_by[:10],
                        'nt_trtype': 'A',
                        'nt_cmnt': ntran_comment[:50],
                        'nt_trnref': ntran_trnref[:50],
                        'nt_entr': post_date,
                        'nt_value': -amount_pounds,
                        'nt_pstid': ntran_pstid_control[:10],
                        'nt_posttyp': 'P',
                        'nt_period': period,
                        'nt_year': year,
                    })

                    self._update_nacnt_balance(bank_account, amount_pounds, period, year)
                    self._update_nacnt_balance(creditors_control, -amount_pounds, period, year)
                    self._update_nbank_balance(bank_account, amount_pounds)

                # 4. anoml transfer file
                if posting_decision.post_to_transfer_file:
                    try:
                        anoml_table = self._open_table('anoml')
                        anoml_table.append({
                            'ax_acnt': bank_account[:8],
                            'ax_cntr': '    ',
                            'ax_cbtype': cbtype,
                            'ax_entry': entry_number[:12],
                            'ax_value': amount_pounds,
                            'ax_source': 'P',
                            'ax_unique': atran_unique[:10],
                            'ax_done': 'Y' if posting_decision.post_to_nominal else 'N',
                        })
                        anoml_table.append({
                            'ax_acnt': creditors_control[:8],
                            'ax_cntr': '    ',
                            'ax_cbtype': cbtype,
                            'ax_entry': entry_number[:12],
                            'ax_value': -amount_pounds,
                            'ax_source': 'P',
                            'ax_unique': atran_unique[:10],
                            'ax_done': 'Y' if posting_decision.post_to_nominal else 'N',
                        })
                    except FileNotFoundError:
                        logger.warning("anoml table not found - skipping transfer file")

                # 5. ptran — type='F' (Refund), POSITIVE value (debit note)
                ptran_table = self._open_table('ptran')
                ptran_table.append({
                    'pt_account': supplier_account[:8],
                    'pt_trdate': post_date,
                    'pt_trref': reference[:20],
                    'pt_supref': payment_type[:20],
                    'pt_trtype': 'F',
                    'pt_trvalue': amount_pounds,
                    'pt_vatval': 0,
                    'pt_trbal': amount_pounds,
                    'pt_paid': ' ',
                    'pt_crdate': post_date,
                    'pt_advance': 'N',
                    'pt_payflag': 0,
                    'pt_set1day': 0,
                    'pt_set1': 0,
                    'pt_set2day': 0,
                    'pt_set2': 0,
                    'pt_held': ' ',
                    'pt_fcurr': '   ',
                    'pt_fcrate': 0,
                    'pt_fcdec': 0,
                    'pt_fcval': 0,
                    'pt_fcbal': 0,
                    'pt_adval': 0,
                    'pt_fadval': 0,
                    'pt_fcmult': 0,
                    'pt_cbtype': cbtype,
                    'pt_entry': entry_number[:12],
                    'pt_unique': atran_unique[:10],
                    'pt_suptype': '   ',
                    'pt_euro': 0,
                    'pt_nlpdate': post_date,
                })

                # NOTE: No palloc created at posting time -- allocation happens separately

                # 6. Update supplier balance — refund INCREASES (debit note)
                self._update_supplier_balance(supplier_account, amount_pounds)

                tables_updated = ["aentry", "atran", "ptran", "pname"]
                if posting_decision.post_to_nominal:
                    tables_updated.insert(2, "ntran (2)")
                if posting_decision.post_to_transfer_file:
                    tables_updated.append("anoml (2)")

            # Post-commit ledger verification — ensures ptran was created
            self.verify_ledger_after_import('ptran', cbtype, entry_number, 1, supplier_account)

            return Opera3ImportResult(
                success=True,
                records_processed=1,
                records_imported=1,
                details=[
                    f"Purchase refund for supplier {supplier_account}: £{amount_pounds:.2f}",
                    f"Entry: {entry_number}, Journal: {journal_number}",
                    f"Tables updated: {', '.join(tables_updated)}"
                ]
            )

        except Exception as e:
            logger.error(f"Failed to import purchase refund: {e}")
            return Opera3ImportResult(
                success=False, records_processed=1, records_failed=1,
                errors=[str(e)]
            )
        finally:
            self._close_all_tables()

    def import_bank_transfer(
        self,
        source_bank: str,
        dest_bank: str,
        amount_pounds: float,
        reference: str,
        post_date: date,
        comment: str = "",
        input_by: str = "SQLRAG",
        post_to_nominal: bool = True,
        cbtype: str = None
    ) -> Dict[str, Any]:
        """
        Import a bank transfer between two bank accounts in Opera 3.

        Creates paired records: 2x aentry, 2x atran (at_type=8), 2x anoml,
        2x ntran, nacnt updates for both banks, nbank updates for both banks.
        No stran/ptran/sname/pname involved.

        Args:
            source_bank: Source bank nominal account (money goes out)
            dest_bank: Destination bank nominal account (money comes in)
            amount_pounds: Transfer amount in pounds (positive)
            reference: Transfer reference
            post_date: Posting date
            comment: Optional comment
            input_by: User code for audit trail
            post_to_nominal: Whether to post to nominal ledger
            cbtype: Cashbook type code (auto-detected if None)

        Returns:
            Dict with success status and details
        """
        if source_bank == dest_bank:
            return {'success': False, 'error': 'Source and destination bank cannot be the same'}

        if amount_pounds <= 0:
            return {'success': False, 'error': 'Transfer amount must be positive'}

        # Get transfer cbtype
        if cbtype is None:
            cbtype = self.get_default_cbtype('bank_transfer')
            if cbtype is None:
                return {'success': False, 'error': 'No Transfer type codes found in atype table'}

        type_validation = self.validate_cbtype(cbtype, required_category=AtypeCategory.TRANSFER)
        if not type_validation['valid']:
            return {'success': False, 'error': type_validation['error']}

        try:
            from sql_rag.opera3_config import Opera3Config, get_period_posting_decision
            config = Opera3Config(str(self.data_path), self.encoding)
            period_result = config.validate_posting_period(post_date, ledger_type='NL')

            if not period_result.is_valid:
                return {'success': False, 'error': period_result.error_message}

            posting_decision = get_period_posting_decision(config, post_date, 'NL')

            # Validate both banks exist
            source_name = self._get_bank_name(source_bank)
            dest_name = self._get_bank_name(dest_bank)
            if not source_name:
                return {'success': False, 'error': f"Source bank '{source_bank}' not found"}
            if not dest_name:
                return {'success': False, 'error': f"Destination bank '{dest_bank}' not found"}

            # Prepare data before locks
            amount_pence = int(round(amount_pounds * 100))
            if isinstance(post_date, str):
                post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

            period, year = config.get_period_for_date(post_date)
            now = datetime.now()

            # Process banks in alphabetical order for consistent lock ordering
            banks_ordered = sorted([source_bank, dest_bank])

            # Generate shared unique ID (both atran/anoml records share it)
            unique_ids = OperaUniqueIdGenerator.generate_multiple(3)
            shared_unique = unique_ids[0]
            ntran_pstid_source = unique_ids[1]
            ntran_pstid_dest = unique_ids[2]

            tables_to_lock = ['aentry', 'atran', 'atype', 'nparm']
            if post_to_nominal and posting_decision.post_to_nominal:
                tables_to_lock.extend(['ntran', 'nacnt', 'nbank', 'nhist'])
            if posting_decision.post_to_transfer_file:
                tables_to_lock.append('anoml')

            with self._transaction_lock(tables_to_lock):
                # Get TWO separate entry numbers (one per bank)
                source_entry = self.increment_atype_entry(cbtype)
                dest_entry = self.increment_atype_entry(cbtype)
                journal_number = self._get_next_journal()

                # === SOURCE BANK (money out) ===
                aentry_table = self._open_table('aentry')
                aentry_table.append({
                    'ae_acnt': source_bank[:8],
                    'ae_cntr': '    ',
                    'ae_cbtype': cbtype,
                    'ae_entry': source_entry[:12],
                    'ae_reclnum': 0,
                    'ae_lstdate': post_date,
                    'ae_frstat': 0,
                    'ae_tostat': 0,
                    'ae_statln': 0,
                    'ae_entref': reference[:20],
                    'ae_value': -amount_pence,
                    'ae_recbal': 0,
                    'ae_remove': 0,
                    'ae_tmpstat': 0,
                    'ae_complet': 1,  # Always complete — NL transfer via anoml when real-time update is off
                    'ae_postgrp': 0,
                    'sq_crdate': now.date(),
                    'sq_crtime': now.strftime('%H:%M:%S')[:8],
                    'sq_cruser': input_by[:8],
                    'ae_comment': (comment or '')[:40],
                })

                atran_table = self._open_table('atran')
                atran_table.append({
                    'at_acnt': source_bank[:8],
                    'at_cntr': '    ',
                    'at_cbtype': cbtype,
                    'at_entry': source_entry[:12],
                    'at_inputby': input_by[:8],
                    'at_type': CashbookTransactionType.BANK_TRANSFER,
                    'at_pstdate': post_date,
                    'at_sysdate': post_date,
                    'at_tperiod': 1,
                    'at_value': -amount_pence,
                    'at_disc': 0,
                    'at_fcurr': '   ',
                    'at_fcexch': 1.0,
                    'at_fcmult': 0,
                    'at_fcdec': 2,
                    'at_account': dest_bank[:8],
                    'at_name': dest_name[:35],
                    'at_comment': (comment or '')[:40],
                    'at_payee': '        ',
                    'at_payname': '',
                    'at_sort': '        ',
                    'at_number': '         ',
                    'at_remove': 0,
                    'at_chqprn': 0,
                    'at_chqlst': 0,
                    'at_bacprn': 0,
                    'at_ccdprn': 0,
                    'at_ccdno': '',
                    'at_payslp': 0,
                    'at_pysprn': 0,
                    'at_cash': 0,
                    'at_remit': 0,
                    'at_unique': shared_unique[:10],
                    'at_postgrp': 0,
                    'at_ccauth': '0       ',
                    'at_refer': reference[:20],
                    'at_srcco': 'I',
                })

                # === DESTINATION BANK (money in) ===
                aentry_table.append({
                    'ae_acnt': dest_bank[:8],
                    'ae_cntr': '    ',
                    'ae_cbtype': cbtype,
                    'ae_entry': dest_entry[:12],
                    'ae_reclnum': 0,
                    'ae_lstdate': post_date,
                    'ae_frstat': 0,
                    'ae_tostat': 0,
                    'ae_statln': 0,
                    'ae_entref': reference[:20],
                    'ae_value': amount_pence,
                    'ae_recbal': 0,
                    'ae_remove': 0,
                    'ae_tmpstat': 0,
                    'ae_complet': 1,  # Always complete — NL transfer via anoml when real-time update is off
                    'ae_postgrp': 0,
                    'sq_crdate': now.date(),
                    'sq_crtime': now.strftime('%H:%M:%S')[:8],
                    'sq_cruser': input_by[:8],
                    'ae_comment': (comment or '')[:40],
                })

                atran_table.append({
                    'at_acnt': dest_bank[:8],
                    'at_cntr': '    ',
                    'at_cbtype': cbtype,
                    'at_entry': dest_entry[:12],
                    'at_inputby': input_by[:8],
                    'at_type': CashbookTransactionType.BANK_TRANSFER,
                    'at_pstdate': post_date,
                    'at_sysdate': post_date,
                    'at_tperiod': 1,
                    'at_value': amount_pence,
                    'at_disc': 0,
                    'at_fcurr': '   ',
                    'at_fcexch': 1.0,
                    'at_fcmult': 0,
                    'at_fcdec': 2,
                    'at_account': source_bank[:8],
                    'at_name': source_name[:35],
                    'at_comment': (comment or '')[:40],
                    'at_payee': '        ',
                    'at_payname': '',
                    'at_sort': '        ',
                    'at_number': '         ',
                    'at_remove': 0,
                    'at_chqprn': 0,
                    'at_chqlst': 0,
                    'at_bacprn': 0,
                    'at_ccdprn': 0,
                    'at_ccdno': '',
                    'at_payslp': 0,
                    'at_pysprn': 0,
                    'at_cash': 0,
                    'at_remit': 0,
                    'at_unique': shared_unique[:10],
                    'at_postgrp': 0,
                    'at_ccauth': '0       ',
                    'at_refer': reference[:20],
                    'at_srcco': 'I',
                })

                # === NOMINAL POSTINGS ===
                do_nominal = post_to_nominal and posting_decision.post_to_nominal
                if do_nominal:
                    ntran_table = self._open_table('ntran')
                    source_type = self._get_nacnt_type(source_bank) or ('B ', 'BC')
                    dest_type = self._get_nacnt_type(dest_bank) or ('B ', 'BC')

                    ntran_comment = f"{comment or reference[:50]:<50}"
                    # Source ntran: trnref contains dest bank name
                    source_trnref = f"{dest_name[:30]:<30}Transfer (TF)       "
                    # Dest ntran: trnref contains source bank name
                    dest_trnref = f"{source_name[:30]:<30}Transfer (TF)       "

                    # CREDIT Source Bank (money going out)
                    ntran_table.append({
                        'nt_acnt': source_bank[:8],
                        'nt_cntr': '    ',
                        'nt_type': source_type[0],
                        'nt_subt': source_type[1],
                        'nt_jrnl': journal_number,
                        'nt_ref': '',
                        'nt_inp': input_by[:10],
                        'nt_trtype': 'A',
                        'nt_cmnt': ntran_comment[:50],
                        'nt_trnref': source_trnref[:50],
                        'nt_entr': post_date,
                        'nt_value': -amount_pounds,
                        'nt_pstid': ntran_pstid_source[:10],
                        'nt_posttyp': 'T',
                        'nt_period': period,
                        'nt_year': year,
                    })

                    # DEBIT Dest Bank (money coming in)
                    ntran_table.append({
                        'nt_acnt': dest_bank[:8],
                        'nt_cntr': '    ',
                        'nt_type': dest_type[0],
                        'nt_subt': dest_type[1],
                        'nt_jrnl': journal_number,
                        'nt_ref': '',
                        'nt_inp': input_by[:10],
                        'nt_trtype': 'A',
                        'nt_cmnt': ntran_comment[:50],
                        'nt_trnref': dest_trnref[:50],
                        'nt_entr': post_date,
                        'nt_value': amount_pounds,
                        'nt_pstid': ntran_pstid_dest[:10],
                        'nt_posttyp': 'T',
                        'nt_period': period,
                        'nt_year': year,
                    })

                    self._update_nacnt_balance(source_bank, -amount_pounds, period, year)
                    self._update_nacnt_balance(dest_bank, amount_pounds, period, year)
                    self._update_nbank_balance(source_bank, -amount_pounds)
                    self._update_nbank_balance(dest_bank, amount_pounds)

                # === ANOML TRANSFER FILE ===
                if posting_decision.post_to_transfer_file:
                    try:
                        anoml_table = self._open_table('anoml')
                        done_flag = 'Y' if do_nominal else 'N'

                        anoml_table.append({
                            'ax_acnt': source_bank[:8],
                            'ax_cntr': '    ',
                            'ax_cbtype': cbtype,
                            'ax_entry': source_entry[:12],
                            'ax_value': -amount_pounds,
                            'ax_source': 'A',
                            'ax_unique': shared_unique[:10],
                            'ax_done': done_flag,
                        })
                        anoml_table.append({
                            'ax_acnt': dest_bank[:8],
                            'ax_cntr': '    ',
                            'ax_cbtype': cbtype,
                            'ax_entry': dest_entry[:12],
                            'ax_value': amount_pounds,
                            'ax_source': 'A',
                            'ax_unique': shared_unique[:10],
                            'ax_done': done_flag,
                        })
                    except FileNotFoundError:
                        logger.warning("anoml table not found - skipping transfer file")

            tables_updated = ["aentry (2)", "atran (2)"]
            if do_nominal:
                tables_updated.extend(["ntran (2)", "nacnt (2)", "nbank (2)"])
            if posting_decision.post_to_transfer_file:
                tables_updated.append("anoml (2)")

            return {
                'success': True,
                'source_entry': source_entry,
                'dest_entry': dest_entry,
                'journal': journal_number,
                'amount_pounds': amount_pounds,
                'tables_updated': tables_updated,
                'message': f"Bank transfer £{amount_pounds:.2f} from {source_bank} to {dest_bank}"
            }

        except Exception as e:
            logger.error(f"Failed to import bank transfer: {e}")
            return {'success': False, 'error': str(e)}
        finally:
            self._close_all_tables()

    def check_duplicate_payment(
        self,
        bank_account: str,
        post_date: date,
        amount_pounds: float
    ) -> bool:
        """
        Check if a payment already exists (duplicate detection).

        Args:
            bank_account: Bank account code
            post_date: Posting date
            amount_pounds: Amount in pounds

        Returns:
            True if duplicate found, False otherwise
        """
        try:
            amount_pence = int(round(amount_pounds * 100))
            atran_table = self._open_table('atran')

            for record in atran_table:
                if (record.at_acnt.strip() == bank_account and
                    record.at_pstdate == post_date and
                    abs(abs(record.at_value) - amount_pence) < 1):
                    return True

            return False
        except Exception as e:
            logger.error(f"Error checking for duplicate: {e}")
            return False
        finally:
            self._close_all_tables()

    def check_duplicate_receipt(
        self,
        bank_account: str,
        post_date: date,
        amount_pounds: float
    ) -> bool:
        """
        Check if a receipt already exists (duplicate detection).

        Args:
            bank_account: Bank account code
            post_date: Posting date
            amount_pounds: Amount in pounds

        Returns:
            True if duplicate found, False otherwise
        """
        return self.check_duplicate_payment(bank_account, post_date, amount_pounds)

    def check_duplicate_before_posting(
        self,
        bank_account: str,
        transaction_date,
        amount_pounds: float,
        account_code: str = '',
        account_type: str = 'nominal',
        date_tolerance_days: int = 1
    ) -> Dict[str, Any]:
        """
        Pre-flight duplicate check before posting a transaction to Opera 3.

        Checks cashbook (atran), and optionally sales/purchase ledger,
        for an existing entry with the same date (±tolerance), amount, and account.

        Args:
            bank_account: Bank account code (e.g. 'BC010')
            transaction_date: Date of transaction (date object or 'YYYY-MM-DD' string)
            amount_pounds: Transaction amount in pounds (positive)
            account_code: Customer/supplier/nominal code
            account_type: 'customer', 'supplier', or 'nominal'
            date_tolerance_days: Days either side of date to check (default 1)

        Returns:
            Dict with 'is_duplicate' bool and 'details' string if found
        """
        from datetime import timedelta

        if isinstance(transaction_date, str):
            txn_date = date.fromisoformat(transaction_date[:10])
        else:
            txn_date = transaction_date

        date_from = txn_date - timedelta(days=date_tolerance_days)
        date_to = txn_date + timedelta(days=date_tolerance_days)
        amount_pence = int(round(amount_pounds * 100))

        try:
            # Check 1: Cashbook (atran) - amounts in PENCE
            atran_table = self._open_table('atran')
            for record in atran_table:
                if not record.at_acnt or record.at_acnt.strip() != bank_account:
                    continue
                rec_date = record.at_pstdate
                if rec_date and date_from <= rec_date <= date_to:
                    if abs(abs(record.at_value) - amount_pence) < 1:
                        entry = getattr(record, 'at_entry', '?')
                        return {
                            'is_duplicate': True,
                            'location': 'cashbook',
                            'details': f"Entry {entry} already exists in cashbook",
                            'entry_number': str(entry)
                        }

            # Check 2: Sales Ledger for customer receipts
            if account_type == 'customer' and account_code:
                stran_table = self._open_table('stran')
                for record in stran_table:
                    if not record.st_account or record.st_account.strip() != account_code:
                        continue
                    if getattr(record, 'st_trtype', '') != 'R':
                        continue
                    rec_date = record.st_trdate
                    if rec_date and date_from <= rec_date <= date_to:
                        if abs(abs(record.st_trvalue) - amount_pounds) < 0.01:
                            ref = (getattr(record, 'st_trref', '') or '').strip()
                            return {
                                'is_duplicate': True,
                                'location': 'sales_ledger',
                                'details': f"Receipt already exists in sales ledger for {account_code} (ref: {ref})",
                                'entry_number': ref
                            }

            # Check 3: Purchase Ledger for supplier payments
            if account_type == 'supplier' and account_code:
                ptran_table = self._open_table('ptran')
                for record in ptran_table:
                    if not record.pt_account or record.pt_account.strip() != account_code:
                        continue
                    if getattr(record, 'pt_trtype', '') != 'P':
                        continue
                    rec_date = record.pt_trdate
                    if rec_date and date_from <= rec_date <= date_to:
                        if abs(abs(record.pt_trvalue) - amount_pounds) < 0.01:
                            ref = (getattr(record, 'pt_trref', '') or '').strip()
                            return {
                                'is_duplicate': True,
                                'location': 'purchase_ledger',
                                'details': f"Payment already exists in purchase ledger for {account_code} (ref: {ref})",
                                'entry_number': ref
                            }

            return {'is_duplicate': False, 'details': ''}

        except Exception as e:
            logger.error(f"Error in pre-posting duplicate check: {e}")
            return {'is_duplicate': False, 'details': ''}
        finally:
            self._close_all_tables()

    def import_gocardless_batch(
        self,
        bank_account: str,
        payments: List[Dict[str, Any]],
        post_date: date,
        reference: str = "GoCardless",
        gocardless_fees: float = 0.0,
        vat_on_fees: float = 0.0,
        fees_nominal_account: str = None,
        fees_vat_code: str = "2",
        fees_payment_type: str = None,
        complete_batch: bool = False,
        input_by: str = "GOCARDLS",
        cbtype: str = None,
        validate_only: bool = False,
        auto_allocate: bool = False,
        currency: str = None,
        destination_bank: str = None,
        transfer_cbtype: str = None
    ) -> Opera3ImportResult:
        """
        Import a GoCardless batch receipt into Opera 3.

        This is a COMPLETE implementation matching Opera SQL SE functionality.

        Creates:
        - One aentry header (batch total)
        - Multiple atran lines (one per customer payment)
        - Multiple stran records (one per customer)
        - ntran nominal ledger entries (double-entry)
        - anoml transfer file entries
        - Customer balance updates (sname.sn_currbal)
        - nacnt nominal account balance updates
        - nbank bank balance updates
        - Optional: fees posting with VAT tracking

        Args:
            bank_account: Bank account code (e.g., 'BC010')
            payments: List of payment dicts with:
                - customer_account: Customer code
                - amount: Amount in POUNDS
                - description: Payment description/reference
            post_date: Posting date
            reference: Batch reference (default 'GoCardless')
            gocardless_fees: Total GoCardless fees (gross including VAT)
            vat_on_fees: VAT element of fees
            fees_nominal_account: Nominal account for net fees (e.g., 'GA400')
            fees_vat_code: VAT code for fees (default '2' standard rate)
            fees_payment_type: Cashbook type code for fees entry (e.g., 'NP'). If None, uses first non-batched Payment type.
            complete_batch: If True, posts to nominal ledger immediately
            input_by: User code for audit trail
            cbtype: Cashbook type code (must be Receipt type)
            validate_only: If True, only validate without inserting
            auto_allocate: If True, auto-allocate receipts to invoices

        Returns:
            Opera3ImportResult with details of the operation
        """
        errors = []
        warnings = []

        if not payments:
            return Opera3ImportResult(
                success=False,
                records_processed=0,
                records_failed=0,
                errors=["No payments provided"]
            )

        # Validate fees configuration - MUST have fees_nominal_account if fees > 0
        if gocardless_fees > 0 and not fees_nominal_account:
            return Opera3ImportResult(
                success=False,
                records_processed=len(payments),
                records_failed=len(payments),
                errors=[
                    f"GoCardless fees of £{gocardless_fees:.2f} cannot be posted: fees_nominal_account not configured. "
                    "Please configure the Fees Nominal Account in GoCardless Settings before importing."
                ]
            )

        # Get/validate cbtype
        if cbtype is None:
            cbtype = self.get_default_cbtype('sales_receipt')
            if cbtype is None:
                return Opera3ImportResult(
                    success=False,
                    records_processed=len(payments),
                    records_failed=len(payments),
                    errors=["No Receipt type codes found in atype table"]
                )

        # Validate type code
        type_validation = self.validate_cbtype(cbtype, required_category=AtypeCategory.RECEIPT)
        if not type_validation['valid']:
            return Opera3ImportResult(
                success=False,
                records_processed=len(payments),
                records_failed=len(payments),
                errors=[type_validation['error']]
            )

        try:
            # Period validation and posting decision
            from sql_rag.opera3_config import Opera3Config, get_period_posting_decision
            config = Opera3Config(str(self.data_path), self.encoding)
            period_result = config.validate_posting_period(post_date, ledger_type='SL')

            if not period_result.is_valid:
                return Opera3ImportResult(
                    success=False,
                    records_processed=len(payments),
                    records_failed=len(payments),
                    errors=[period_result.error_message]
                )

            # Get period posting decision
            posting_decision = get_period_posting_decision(config, post_date, 'SL')

            # Build customer info dictionary (validates all accounts)
            customer_info = {}
            for payment in payments:
                customer_account = payment.get('customer_account')
                if not customer_account:
                    errors.append("Payment missing customer_account")
                    continue
                customer_name = self._get_customer_name(customer_account)
                if not customer_name:
                    errors.append(f"Customer account '{customer_account}' not found")
                else:
                    customer_info[customer_account] = {
                        'name': customer_name,
                        'control': self._get_customer_control_account(customer_account)
                    }

            if errors:
                return Opera3ImportResult(
                    success=False,
                    records_processed=len(payments),
                    records_failed=len(payments),
                    errors=errors
                )

            if validate_only:
                return Opera3ImportResult(
                    success=True,
                    records_processed=len(payments),
                    records_imported=len(payments),
                    warnings=[f"Validation passed for {len(payments)} payments totalling £{sum(p['amount'] for p in payments):.2f}"]
                )

            # Calculate totals
            gross_total = sum(p['amount'] for p in payments)
            net_fees = abs(gocardless_fees) - abs(vat_on_fees)
            gross_pence = int(round(gross_total * 100))

            if isinstance(post_date, str):
                post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

            period, year = config.get_period_for_date(post_date)
            now = datetime.now()

            # Tables to lock - always include nominal tables for complete posting
            tables_to_lock = ['aentry', 'atran', 'stran', 'sname', 'atype', 'nparm']
            if posting_decision.post_to_nominal:
                tables_to_lock.extend(['ntran', 'nacnt', 'nbank', 'nhist'])
            if posting_decision.post_to_transfer_file:
                tables_to_lock.append('anoml')

            with self._transaction_lock(tables_to_lock):
                # Get next entry number
                entry_number = self.increment_atype_entry(cbtype)
                if posting_decision.post_to_nominal:
                    journal_count = len(payments) + (1 if gocardless_fees > 0 else 0)
                    journal_number = self._get_next_journal(count=journal_count)
                else:
                    journal_number = 0

                # Create aentry header
                aentry_table = self._open_table('aentry')
                aentry_table.append({
                    'ae_acnt': bank_account[:8],
                    'ae_cntr': '    ',
                    'ae_cbtype': cbtype,
                    'ae_entry': entry_number[:12],
                    'ae_reclnum': 0,
                    'ae_lstdate': post_date,
                    'ae_frstat': 0,
                    'ae_tostat': 0,
                    'ae_statln': 0,
                    'ae_entref': reference[:20] if reference else 'GoCardless',
                    'ae_value': gross_pence,
                    'ae_recbal': 0,
                    'ae_remove': 0,
                    'ae_tmpstat': 0,
                    'ae_complet': 1,  # Always complete — NL transfer via anoml when real-time update is off
                    'ae_postgrp': 0,
                    'sq_crdate': now.date(),
                    'sq_crtime': now.strftime('%H:%M:%S')[:8],
                    'sq_cruser': input_by[:8],
                    'ae_comment': 'GoCardless batch import',
                })

                # Process each payment
                atran_table = self._open_table('atran')
                stran_table = self._open_table('stran')

                # Look up nacnt types for ntran entries (once, before loop)
                bank_type = self._get_nacnt_type(bank_account) or ('B ', 'BC')
                # Control type will be looked up per-customer inside loop (may differ per customer)

                for idx, payment in enumerate(payments):
                    customer_account = payment['customer_account'].strip()
                    amount_pounds = float(payment['amount'])
                    amount_pence = int(round(amount_pounds * 100))
                    description = payment.get('description', '')[:35]

                    cust = customer_info[customer_account]
                    customer_name = cust['name']
                    debtors_control = cust['control']

                    # Generate unique IDs
                    atran_unique = OperaUniqueIdGenerator.generate()
                    stran_unique = atran_unique  # Must match atran — Opera shares unique ID
                    ntran_pstid = OperaUniqueIdGenerator.generate()

                    # Create atran line
                    atran_table.append({
                        'at_acnt': bank_account[:8],
                        'at_cntr': '    ',
                        'at_cbtype': cbtype,
                        'at_entry': entry_number[:12],
                        'at_inputby': input_by[:8],
                        'at_type': CashbookTransactionType.SALES_RECEIPT,
                        'at_pstdate': post_date,
                        'at_sysdate': post_date,
                        'at_tperiod': 1,
                        'at_value': amount_pence,
                        'at_disc': 0,
                        'at_fcurr': '   ',
                        'at_fcexch': 1.0,
                        'at_fcmult': 0,
                        'at_fcdec': 2,
                        'at_account': customer_account[:8],
                        'at_name': customer_name[:35],
                        'at_comment': description,
                        'at_payee': '        ',
                        'at_payname': '',
                        'at_sort': '        ',
                        'at_number': '         ',
                        'at_remove': 0,
                        'at_chqprn': 0,
                        'at_chqlst': 0,
                        'at_bacprn': 0,
                        'at_ccdprn': 0,
                        'at_ccdno': '',
                        'at_payslp': 0,
                        'at_pysprn': 0,
                        'at_cash': 0,
                        'at_remit': 0,
                        'at_unique': atran_unique[:10],
                        'at_postgrp': 0,
                        'at_ccauth': '0       ',
                        'at_refer': reference[:20],
                        'at_srcco': 'I',
                    })

                    # Create stran record
                    stran_table.append({
                        'st_account': customer_account[:8],
                        'st_trdate': post_date,
                        'st_trref': reference[:20],
                        'st_cusref': 'GoCardless',
                        'st_trtype': 'R',
                        'st_trvalue': -amount_pounds,
                        'st_vatval': 0,
                        'st_trbal': -amount_pounds,
                        'st_paid': ' ',
                        'st_crdate': post_date,
                        'st_advance': 'N',
                        'st_payflag': 0,
                        'st_set1day': 0,
                        'st_set1': 0,
                        'st_set2day': 0,
                        'st_set2': 0,
                        'st_held': ' ',
                        'st_fcurr': '   ',
                        'st_fcrate': 0,
                        'st_fcdec': 0,
                        'st_fcval': 0,
                        'st_fcbal': 0,
                        'st_adval': 0,
                        'st_fadval': 0,
                        'st_fcmult': 0,
                        'st_cbtype': cbtype,
                        'st_entry': entry_number[:12],
                        'st_unique': stran_unique[:10],
                        'st_custype': '   ',
                        'st_euro': 0,
                        'st_nlpdate': post_date,
                    })

                    # NOTE: No salloc created at posting time -- allocation happens separately

                    # Create ntran (nominal ledger) entries
                    if posting_decision.post_to_nominal:
                        ntran_table = self._open_table('ntran')
                        ntran_comment = f"{description[:50]:<50}"
                        ntran_trnref = f"{customer_name[:30]:<30}GoCardless (RT)     "
                        control_type = self._get_nacnt_type(debtors_control) or ('B ', 'BB')

                        # DEBIT Bank (money coming in)
                        ntran_table.append({
                            'nt_acnt': bank_account[:8],
                            'nt_cntr': '    ',
                            'nt_type': bank_type[0],
                            'nt_subt': bank_type[1],
                            'nt_jrnl': journal_number,
                            'nt_ref': '',
                            'nt_inp': input_by[:10],
                            'nt_trtype': 'A',
                            'nt_cmnt': ntran_comment[:50],
                            'nt_trnref': ntran_trnref[:50],
                            'nt_entr': post_date,
                            'nt_value': amount_pounds,
                            'nt_year': year,
                            'nt_period': period,
                            'nt_rvrse': 0,
                            'nt_prevyr': 0,
                            'nt_consol': 0,
                            'nt_fcurr': '   ',
                            'nt_fvalue': 0,
                            'nt_fcrate': 0,
                            'nt_fcmult': 0,
                            'nt_fcdec': 0,
                            'nt_srcco': 'I',
                            'nt_cdesc': '',
                            'nt_project': '        ',
                            'nt_job': '        ',
                            'nt_posttyp': 'R',
                            'nt_pstgrp': 0,
                            'nt_pstid': ntran_pstid[:10],
                            'nt_srcnlid': 0,
                            'nt_recurr': 0,
                            'nt_perpost': 0,
                            'nt_rectify': 0,
                            'nt_recjrnl': 0,
                            'nt_vatanal': 0,
                            'nt_distrib': 0,
                        })

                        # CREDIT Debtors Control (reduce asset)
                        ntran_table.append({
                            'nt_acnt': debtors_control[:8],
                            'nt_cntr': '    ',
                            'nt_type': control_type[0],
                            'nt_subt': control_type[1],
                            'nt_jrnl': journal_number,
                            'nt_ref': '',
                            'nt_inp': input_by[:10],
                            'nt_trtype': 'A',
                            'nt_cmnt': ntran_comment[:50],
                            'nt_trnref': ntran_trnref[:50],
                            'nt_entr': post_date,
                            'nt_value': -amount_pounds,
                            'nt_year': year,
                            'nt_period': period,
                            'nt_rvrse': 0,
                            'nt_prevyr': 0,
                            'nt_consol': 0,
                            'nt_fcurr': '   ',
                            'nt_fvalue': 0,
                            'nt_fcrate': 0,
                            'nt_fcmult': 0,
                            'nt_fcdec': 0,
                            'nt_srcco': 'I',
                            'nt_cdesc': '',
                            'nt_project': '        ',
                            'nt_job': '        ',
                            'nt_posttyp': 'R',
                            'nt_pstgrp': 0,
                            'nt_pstid': ntran_pstid[:10],
                            'nt_srcnlid': 0,
                            'nt_recurr': 0,
                            'nt_perpost': 0,
                            'nt_rectify': 0,
                            'nt_recjrnl': 0,
                            'nt_vatanal': 0,
                            'nt_distrib': 0,
                        })

                        # Update nacnt balances
                        self._update_nacnt_balance(bank_account, amount_pounds, period, year)
                        self._update_nacnt_balance(debtors_control, -amount_pounds, period, year)

                        journal_number += 1

                    # Update nbank balance (GoCardless receipt increases bank) - ALWAYS when atran created
                    self._update_nbank_balance(bank_account, amount_pounds)

                    # Create anoml transfer file entries
                    if posting_decision.post_to_transfer_file:
                        try:
                            anoml_table = self._open_table('anoml')
                            done_flag = posting_decision.transfer_file_done_flag
                            jrnl_num = journal_number - 1 if posting_decision.post_to_nominal else 0

                            # Bank account (debit)
                            anoml_table.append({
                                'ax_nacnt': bank_account[:10],
                                'ax_ncntr': '    ',
                                'ax_source': 'S',
                                'ax_date': post_date,
                                'ax_value': amount_pounds,
                                'ax_tref': reference[:20],
                                'ax_comment': description[:50],
                                'ax_done': done_flag,
                                'ax_fcurr': '   ',
                                'ax_fvalue': 0,
                                'ax_fcrate': 0,
                                'ax_fcmult': 0,
                                'ax_fcdec': 0,
                                'ax_srcco': 'I',
                                'ax_unique': atran_unique[:10],
                                'ax_project': '        ',
                                'ax_job': '        ',
                                'ax_jrnl': jrnl_num,
                                'ax_nlpdate': post_date,
                            })

                            # Debtors control (credit)
                            anoml_table.append({
                                'ax_nacnt': debtors_control[:10],
                                'ax_ncntr': '    ',
                                'ax_source': 'S',
                                'ax_date': post_date,
                                'ax_value': -amount_pounds,
                                'ax_tref': reference[:20],
                                'ax_comment': description[:50],
                                'ax_done': done_flag,
                                'ax_fcurr': '   ',
                                'ax_fvalue': 0,
                                'ax_fcrate': 0,
                                'ax_fcmult': 0,
                                'ax_fcdec': 0,
                                'ax_srcco': 'I',
                                'ax_unique': atran_unique[:10],
                                'ax_project': '        ',
                                'ax_job': '        ',
                                'ax_jrnl': jrnl_num,
                                'ax_nlpdate': post_date,
                            })
                        except FileNotFoundError:
                            logger.warning("anoml table not found - skipping transfer file")

                    # Update customer balance
                    self._update_customer_balance(customer_account, -amount_pounds)

                # Post GoCardless fees if provided
                if gocardless_fees > 0 and fees_nominal_account:
                    gross_fees = abs(gocardless_fees)

                    # Look up VAT code from ztax to get rate and nominal account
                    vat_info = self.get_vat_rate(fees_vat_code, 'P', post_date)
                    vat_nominal_account = vat_info.get('nominal', '')
                    vat_rate_from_ztax = vat_info.get('rate', 20.0)
                    logger.debug(f"VAT lookup for fees: code={fees_vat_code}, nominal={vat_nominal_account}, rate={vat_rate_from_ztax}%")

                    if posting_decision.post_to_nominal:
                        ntran_table = self._open_table('ntran')
                        fees_unique = OperaUniqueIdGenerator.generate()
                        fees_comment = "GoCardless fees"
                        fees_acct_type = self._get_nacnt_type(fees_nominal_account) or ('P ', 'HA')
                        fees_bank_type = self._get_nacnt_type(bank_account) or ('B ', 'BC')

                        # DR Fees expense (NET amount)
                        ntran_table.append({
                            'nt_acnt': fees_nominal_account[:8],
                            'nt_cntr': '    ',
                            'nt_type': fees_acct_type[0],
                            'nt_subt': fees_acct_type[1],
                            'nt_jrnl': journal_number,
                            'nt_ref': '',
                            'nt_inp': input_by[:10],
                            'nt_trtype': 'A',
                            'nt_cmnt': fees_comment,
                            'nt_trnref': fees_comment,
                            'nt_entr': post_date,
                            'nt_value': net_fees,
                            'nt_year': year,
                            'nt_period': period,
                            'nt_rvrse': 0,
                            'nt_prevyr': 0,
                            'nt_consol': 0,
                            'nt_fcurr': '   ',
                            'nt_fvalue': 0,
                            'nt_fcrate': 0,
                            'nt_fcmult': 0,
                            'nt_fcdec': 0,
                            'nt_srcco': 'I',
                            'nt_cdesc': '',
                            'nt_project': '        ',
                            'nt_job': '        ',
                            'nt_posttyp': 'N',
                            'nt_pstgrp': 0,
                            'nt_pstid': fees_unique[:10],
                            'nt_srcnlid': 0,
                            'nt_recurr': 0,
                            'nt_perpost': 0,
                            'nt_rectify': 0,
                            'nt_recjrnl': 0,
                            'nt_vatanal': 0,
                            'nt_distrib': 0,
                        })
                        self._update_nacnt_balance(fees_nominal_account, net_fees, period, year)

                        # DR VAT Input if VAT > 0
                        if vat_on_fees > 0:
                            vat_nominal = vat_nominal_account
                            vat_unique = OperaUniqueIdGenerator.generate()
                            vat_acct_type = self._get_nacnt_type(vat_nominal) or ('B ', 'BB')
                            ntran_table.append({
                                'nt_acnt': vat_nominal[:8],
                                'nt_cntr': '    ',
                                'nt_type': vat_acct_type[0],
                                'nt_subt': vat_acct_type[1],
                                'nt_jrnl': journal_number,
                                'nt_ref': '',
                                'nt_inp': input_by[:10],
                                'nt_trtype': 'A',
                                'nt_cmnt': f"{fees_comment} VAT",
                                'nt_trnref': fees_comment,
                                'nt_entr': post_date,
                                'nt_value': abs(vat_on_fees),
                                'nt_year': year,
                                'nt_period': period,
                                'nt_rvrse': 0,
                                'nt_prevyr': 0,
                                'nt_consol': 0,
                                'nt_fcurr': '   ',
                                'nt_fvalue': 0,
                                'nt_fcrate': 0,
                                'nt_fcmult': 0,
                                'nt_fcdec': 0,
                                'nt_srcco': 'I',
                                'nt_cdesc': '',
                                'nt_project': '        ',
                                'nt_job': '        ',
                                'nt_posttyp': 'N',
                                'nt_pstgrp': 0,
                                'nt_pstid': vat_unique[:10],
                                'nt_srcnlid': 0,
                                'nt_recurr': 0,
                                'nt_perpost': 0,
                                'nt_rectify': 0,
                                'nt_recjrnl': 0,
                                'nt_vatanal': 0,
                                'nt_distrib': 0,
                            })
                            self._update_nacnt_balance(vat_nominal, abs(vat_on_fees), period, year)

                            # Create zvtran for VAT return tracking
                            try:
                                vat_rate = vat_rate_from_ztax
                                zvtran_table = self._open_table('zvtran')
                                zvtran_table.append({
                                    'va_source': 'N',
                                    'va_account': fees_nominal_account[:8],
                                    'va_laccnt': fees_nominal_account[:8],
                                    'va_trdate': post_date,
                                    'va_taxdate': post_date,
                                    'va_ovrdate': post_date,
                                    'va_trref': reference[:20],
                                    'va_trtype': 'I',
                                    'va_country': 'GB',
                                    'va_fcurr': '   ',
                                    'va_trvalue': net_fees,
                                    'va_fcval': 0,
                                    'va_vatval': abs(vat_on_fees),
                                    'va_cost': 0,
                                    'va_vatctry': 'H',
                                    'va_vattype': 'P',
                                    'va_anvat': fees_vat_code[:3],
                                    'va_vatrate': vat_rate,
                                    'va_box1': 0,
                                    'va_box2': 0,
                                    'va_box4': 1,
                                    'va_box6': 0,
                                    'va_box7': 1,
                                    'va_box8': 0,
                                    'va_box9': 0,
                                    'va_done': 0,
                                    'va_import': 0,
                                    'va_export': 0,
                                })
                                logger.debug(f"Created zvtran for GoCardless fees VAT: £{vat_on_fees:.2f}")
                            except Exception as zvt_err:
                                logger.warning(f"Failed to create zvtran for fees VAT: {zvt_err}")

                            # Create nvat for VAT return tracking
                            try:
                                nvat_table = self._open_table('nvat')
                                nvat_table.append({
                                    'nv_acnt': vat_nominal[:8],
                                    'nv_cntr': '',
                                    'nv_date': post_date,
                                    'nv_crdate': post_date,
                                    'nv_taxdate': post_date,
                                    'nv_ref': reference[:20],
                                    'nv_type': 'P',
                                    'nv_advance': 0,
                                    'nv_value': net_fees,
                                    'nv_vatval': abs(vat_on_fees),
                                    'nv_vatctry': ' ',
                                    'nv_vattype': 'P',
                                    'nv_vatcode': fees_vat_code.strip(),
                                    'nv_vatrate': vat_rate,
                                    'nv_comment': 'GoCardless fees VAT',
                                })
                                logger.debug(f"Created nvat for GoCardless fees VAT: £{vat_on_fees:.2f}")
                            except Exception as nvat_err:
                                logger.warning(f"Failed to create nvat for fees VAT: {nvat_err}")

                        # CR Bank (fees reduce bank)
                        ntran_table.append({
                            'nt_acnt': bank_account[:8],
                            'nt_cntr': '    ',
                            'nt_type': fees_bank_type[0],
                            'nt_subt': fees_bank_type[1],
                            'nt_jrnl': journal_number,
                            'nt_ref': '',
                            'nt_inp': input_by[:10],
                            'nt_trtype': 'A',
                            'nt_cmnt': fees_comment,
                            'nt_trnref': fees_comment,
                            'nt_entr': post_date,
                            'nt_value': -gross_fees,
                            'nt_year': year,
                            'nt_period': period,
                            'nt_rvrse': 0,
                            'nt_prevyr': 0,
                            'nt_consol': 0,
                            'nt_fcurr': '   ',
                            'nt_fvalue': 0,
                            'nt_fcrate': 0,
                            'nt_fcmult': 0,
                            'nt_fcdec': 0,
                            'nt_srcco': 'I',
                            'nt_cdesc': '',
                            'nt_project': '        ',
                            'nt_job': '        ',
                            'nt_posttyp': 'N',
                            'nt_pstgrp': 0,
                            'nt_pstid': fees_unique[:10],
                            'nt_srcnlid': 0,
                            'nt_recurr': 0,
                            'nt_perpost': 0,
                            'nt_rectify': 0,
                            'nt_recjrnl': 0,
                            'nt_vatanal': 0,
                            'nt_distrib': 0,
                        })
                        self._update_nacnt_balance(bank_account, -gross_fees, period, year)

                    # Create SEPARATE cashbook entry for fees (not part of receipts batch)
                    # This ensures fees appear as a distinct payment in cashbook
                    gross_fees_pence = int(round(gross_fees * 100))
                    net_fees_val = gross_fees - abs(vat_on_fees)

                    # Use configured fees payment type, or find a non-batched payment type
                    if fees_payment_type:
                        fees_cbtype = fees_payment_type.strip()
                    else:
                        # Find a non-batched payment type from atype
                        atype_table = self._open_table('atype')
                        fees_cbtype = 'NP'  # fallback
                        for at in atype_table:
                            ay_type = str(getattr(at, 'ay_type', '') or '').strip()
                            ay_batched = getattr(at, 'ay_batched', False)
                            if ay_type == 'P' and not ay_batched:
                                fees_cbtype = str(getattr(at, 'ay_cbtype', 'NP') or 'NP').strip()
                                break

                    fees_entry_number = self.increment_atype_entry(fees_cbtype)
                    fees_unique = OperaUniqueIdGenerator.generate()

                    # Create aentry header for fees
                    aentry_table = self._open_table('aentry')
                    aentry_table.append({
                        'ae_acnt': bank_account[:8],
                        'ae_cntr': '    ',
                        'ae_cbtype': fees_cbtype[:4],
                        'ae_entry': fees_entry_number[:10],
                        'ae_reclnum': 0,
                        'ae_lstdate': post_date,
                        'ae_frstat': 0,
                        'ae_tostat': 0,
                        'ae_statln': 0,
                        'ae_entref': reference[:20],
                        'ae_value': -gross_fees_pence,
                        'ae_recbal': 0,
                        'ae_remove': 0,
                        'ae_tmpstat': 0,
                        'ae_complet': 1,  # Always complete — NL transfer via anoml when real-time update is off
                        'ae_postgrp': 0,
                        'sq_crdate': now.date(),
                        'sq_crtime': now.strftime('%H:%M:%S')[:8],
                        'sq_cruser': input_by[:8],
                        'ae_comment': 'GoCardless fees',
                    })

                    # Create atran for fees
                    atran_table = self._open_table('atran')
                    if vat_on_fees > 0:
                        # Line 1: Net fees to expense account
                        net_fees_pence = int(round(net_fees_val * 100))
                        atran_table.append({
                            'at_acnt': bank_account[:8],
                            'at_cntr': '    ',
                            'at_cbtype': fees_cbtype[:4],
                            'at_entry': fees_entry_number[:10],
                            'at_inputby': input_by[:8],
                            'at_type': CashbookTransactionType.NOMINAL_PAYMENT,
                            'at_pstdate': post_date,
                            'at_sysdate': post_date,
                            'at_tperiod': 1,
                            'at_value': -net_fees_pence,
                            'at_disc': 0,
                            'at_fcurr': '   ',
                            'at_fcexch': 1.0,
                            'at_fcmult': 0,
                            'at_fcdec': 2,
                            'at_account': fees_nominal_account[:8],
                            'at_name': 'GoCardless fees'[:35],
                            'at_comment': '',
                            'at_payee': '        ',
                            'at_payname': '',
                            'at_sort': '        ',
                            'at_number': '         ',
                            'at_remove': 0,
                            'at_chqprn': 0,
                            'at_chqlst': 0,
                            'at_bacprn': 0,
                            'at_ccdprn': 0,
                            'at_ccdno': '',
                            'at_payslp': 0,
                            'at_pysprn': 0,
                            'at_cash': 0,
                            'at_remit': 0,
                            'at_unique': fees_unique[:10],
                            'at_postgrp': 0,
                            'at_ccauth': '0       ',
                            'at_refer': reference[:20],
                            'at_srcco': 'I',
                            'at_ecb': 0,
                            'at_ecbtype': ' ',
                            'at_atpycd': '      ',
                            'at_bsref': '',
                            'at_bsname': '',
                            'at_vattycd': '  ',
                            'at_project': '        ',
                            'at_job': '        ',
                        })

                        # Line 2: VAT to VAT input account
                        vat_unique = OperaUniqueIdGenerator.generate()
                        vat_pence = int(round(abs(vat_on_fees) * 100))
                        vat_nominal = vat_nominal_account
                        atran_table.append({
                            'at_acnt': bank_account[:8],
                            'at_cntr': '   1',
                            'at_cbtype': fees_cbtype[:4],
                            'at_entry': fees_entry_number[:10],
                            'at_inputby': input_by[:8],
                            'at_type': CashbookTransactionType.NOMINAL_PAYMENT,
                            'at_pstdate': post_date,
                            'at_sysdate': post_date,
                            'at_tperiod': 1,
                            'at_value': -vat_pence,
                            'at_disc': 0,
                            'at_fcurr': '   ',
                            'at_fcexch': 1.0,
                            'at_fcmult': 0,
                            'at_fcdec': 2,
                            'at_account': vat_nominal[:8],
                            'at_name': 'GoCardless fees VAT'[:35],
                            'at_comment': '',
                            'at_payee': '        ',
                            'at_payname': '',
                            'at_sort': '        ',
                            'at_number': '         ',
                            'at_remove': 0,
                            'at_chqprn': 0,
                            'at_chqlst': 0,
                            'at_bacprn': 0,
                            'at_ccdprn': 0,
                            'at_ccdno': '',
                            'at_payslp': 0,
                            'at_pysprn': 0,
                            'at_cash': 0,
                            'at_remit': 0,
                            'at_unique': vat_unique[:10],
                            'at_postgrp': 0,
                            'at_ccauth': '0       ',
                            'at_refer': reference[:20],
                            'at_srcco': 'I',
                            'at_ecb': 0,
                            'at_ecbtype': ' ',
                            'at_atpycd': '      ',
                            'at_bsref': '',
                            'at_bsname': '',
                            'at_vattycd': '  ',
                            'at_project': '        ',
                            'at_job': '        ',
                        })
                        logger.debug(f"Created 2 atran lines for fees: net £{net_fees_val:.2f} to {fees_nominal_account}, VAT £{vat_on_fees:.2f}")
                    else:
                        # Single line for gross fees (no VAT)
                        atran_table.append({
                            'at_acnt': bank_account[:8],
                            'at_cntr': '    ',
                            'at_cbtype': fees_cbtype[:4],
                            'at_entry': fees_entry_number[:10],
                            'at_inputby': input_by[:8],
                            'at_type': CashbookTransactionType.NOMINAL_PAYMENT,
                            'at_pstdate': post_date,
                            'at_sysdate': post_date,
                            'at_tperiod': 1,
                            'at_value': -gross_fees_pence,
                            'at_disc': 0,
                            'at_fcurr': '   ',
                            'at_fcexch': 1.0,
                            'at_fcmult': 0,
                            'at_fcdec': 2,
                            'at_account': fees_nominal_account[:8],
                            'at_name': 'GoCardless fees'[:35],
                            'at_comment': '',
                            'at_payee': '        ',
                            'at_payname': '',
                            'at_sort': '        ',
                            'at_number': '         ',
                            'at_remove': 0,
                            'at_chqprn': 0,
                            'at_chqlst': 0,
                            'at_bacprn': 0,
                            'at_ccdprn': 0,
                            'at_ccdno': '',
                            'at_payslp': 0,
                            'at_pysprn': 0,
                            'at_cash': 0,
                            'at_remit': 0,
                            'at_unique': fees_unique[:10],
                            'at_postgrp': 0,
                            'at_ccauth': '0       ',
                            'at_refer': reference[:20],
                            'at_srcco': 'I',
                            'at_ecb': 0,
                            'at_ecbtype': ' ',
                            'at_atpycd': '      ',
                            'at_bsref': '',
                            'at_bsname': '',
                            'at_vattycd': '  ',
                            'at_project': '        ',
                            'at_job': '        ',
                        })

                    tables_updated.add('aentry')
                    tables_updated.add('atran')
                    logger.debug(f"Created separate aentry/atran for GoCardless fees: {fees_entry_number}")

                    # Update nbank balance (GoCardless fees decrease bank) - ALWAYS when atran created
                    self._update_nbank_balance(bank_account, -gross_fees)

                    # Transfer file for fees
                    if posting_decision.post_to_transfer_file:
                        try:
                            anoml_table = self._open_table('anoml')
                            done_flag = posting_decision.transfer_file_done_flag
                            jrnl_num = journal_number if posting_decision.post_to_nominal else 0

                            # Bank (credit - fees reduce bank)
                            anoml_table.append({
                                'ax_nacnt': bank_account[:10],
                                'ax_ncntr': '    ',
                                'ax_source': 'A',
                                'ax_date': post_date,
                                'ax_value': -gross_fees,
                                'ax_tref': reference[:20],
                                'ax_comment': 'GoCardless fees',
                                'ax_done': done_flag,
                                'ax_fcurr': '   ',
                                'ax_fvalue': 0,
                                'ax_fcrate': 0,
                                'ax_fcmult': 0,
                                'ax_fcdec': 0,
                                'ax_srcco': 'I',
                                'ax_unique': fees_unique[:10] if posting_decision.post_to_nominal else '',
                                'ax_project': '        ',
                                'ax_job': '        ',
                                'ax_jrnl': jrnl_num,
                                'ax_nlpdate': post_date,
                            })

                            # Fees expense (debit)
                            anoml_table.append({
                                'ax_nacnt': fees_nominal_account[:10],
                                'ax_ncntr': '    ',
                                'ax_source': 'A',
                                'ax_date': post_date,
                                'ax_value': net_fees,
                                'ax_tref': reference[:20],
                                'ax_comment': 'GoCardless fees',
                                'ax_done': done_flag,
                                'ax_fcurr': '   ',
                                'ax_fvalue': 0,
                                'ax_fcrate': 0,
                                'ax_fcmult': 0,
                                'ax_fcdec': 0,
                                'ax_srcco': 'I',
                                'ax_unique': fees_unique[:10] if posting_decision.post_to_nominal else '',
                                'ax_project': '        ',
                                'ax_job': '        ',
                                'ax_jrnl': jrnl_num,
                                'ax_nlpdate': post_date,
                            })

                            # VAT (debit) if applicable
                            if vat_on_fees > 0:
                                anoml_table.append({
                                    'ax_nacnt': vat_nominal_account,
                                    'ax_ncntr': '    ',
                                    'ax_source': 'A',
                                    'ax_date': post_date,
                                    'ax_value': abs(vat_on_fees),
                                    'ax_tref': reference[:20],
                                    'ax_comment': 'GoCardless fees VAT',
                                    'ax_done': done_flag,
                                    'ax_fcurr': '   ',
                                    'ax_fvalue': 0,
                                    'ax_fcrate': 0,
                                    'ax_fcmult': 0,
                                    'ax_fcdec': 0,
                                    'ax_srcco': 'I',
                                    'ax_unique': vat_unique[:10] if posting_decision.post_to_nominal else '',
                                    'ax_project': '        ',
                                    'ax_job': '        ',
                                    'ax_jrnl': jrnl_num,
                                    'ax_nlpdate': post_date,
                                })
                        except FileNotFoundError:
                            logger.warning("anoml table not found - skipping fees transfer file")

                    warnings.append(f"Fees posted: £{gocardless_fees:.2f} (Net: £{net_fees:.2f}, VAT: £{vat_on_fees:.2f})")

                # Build posting mode message
                if posting_decision.post_to_nominal:
                    posting_mode = "Current period - posted to nominal ledger"
                else:
                    posting_mode = "Different period - transfer file only (pending NL post)"

                warnings.append(f"Entry number: {entry_number}")
                warnings.append(f"Payments: {len(payments)}")
                warnings.append(f"Gross amount: £{gross_total:.2f}")
                warnings.append(f"Posting mode: {posting_mode}")

                tables_updated = ["aentry", "atran", "stran", "sname"]
                if posting_decision.post_to_nominal:
                    tables_updated.extend(["ntran", "nacnt", "nbank"])
                if posting_decision.post_to_transfer_file:
                    tables_updated.append("anoml")
                warnings.append(f"Tables updated: {', '.join(tables_updated)}")

                logger.info(f"Successfully imported GoCardless batch: {entry_number} with {len(payments)} payments totalling £{gross_total:.2f} - {posting_mode}")

            # Auto-allocate receipts to invoices if requested
            allocation_results = []
            if auto_allocate:
                for payment in payments:
                    customer_account = payment['customer_account'].strip()
                    amount = float(payment['amount'])
                    pay_description = payment.get('description', '')

                    # Check per-payment auto_allocate flag (defaults to True if not specified)
                    if not payment.get('auto_allocate', True):
                        allocation_results.append(
                            f"Allocation disabled for {customer_account}: posted on account"
                        )
                        continue

                    alloc_result = self.auto_allocate_receipt(
                        customer_account=customer_account,
                        receipt_ref=reference,
                        receipt_amount=amount,
                        allocation_date=post_date,
                        bank_account=bank_account,
                        description=pay_description,
                        gc_payment_id=payment.get('gc_payment_id')
                    )

                    if alloc_result['success']:
                        method_note = f" ({alloc_result.get('allocation_method', '')})" if alloc_result.get('allocation_method') == 'payment_request' else ''
                        allocation_results.append(
                            f"Auto-allocated {customer_account}: £{alloc_result['allocated_amount']:.2f} to {len(alloc_result['allocations'])} invoice(s){method_note}"
                        )
                    else:
                        allocation_results.append(
                            f"Allocation skipped for {customer_account}: {alloc_result['message']}"
                        )

                if allocation_results:
                    warnings.extend(allocation_results)

            # Post-commit ledger verification — ensures stran records were created for all payments
            self.verify_ledger_after_import('stran', cbtype, entry_number, len(payments))

            # Auto-transfer net amount from GC Control bank to destination bank
            transfer_msg = None
            net_payout = gross_total - gocardless_fees
            if destination_bank and destination_bank.strip() != bank_account.strip():
                try:
                    transfer_result = self.import_bank_transfer(
                        source_bank=bank_account,
                        dest_bank=destination_bank,
                        amount_pounds=net_payout,
                        reference=reference[:20],
                        post_date=post_date,
                        comment="GoCardless payout transfer",
                        input_by=input_by,
                        cbtype=transfer_cbtype
                    )
                    if transfer_result.get('success'):
                        transfer_msg = f"Net £{net_payout:.2f} transferred from {bank_account} to {destination_bank}"
                        logger.info(f"GoCardless auto-transfer: {transfer_msg}")
                    else:
                        transfer_error = transfer_result.get('error', 'Unknown error')
                        transfer_msg = f"Transfer to {destination_bank} failed: {transfer_error} — post manually"
                        logger.error(f"GoCardless auto-transfer failed: {transfer_error}")
                except Exception as te:
                    transfer_msg = f"Transfer to {destination_bank} failed: {te} — post manually"
                    logger.error(f"GoCardless auto-transfer exception: {te}")

            if transfer_msg:
                warnings.append(transfer_msg)

            return Opera3ImportResult(
                success=True,
                records_processed=len(payments),
                records_imported=len(payments),
                entry_number=entry_number,
                journal_number=journal_number - 1 if posting_decision.post_to_nominal else 0,
                warnings=warnings
            )

        except Exception as e:
            logger.error(f"Error importing GoCardless batch: {e}")
            import traceback
            traceback.print_exc()
            return Opera3ImportResult(
                success=False,
                records_processed=len(payments),
                errors=[str(e)]
            )
        finally:
            self._close_all_tables()

    def import_nominal_entry(
        self,
        bank_account: str,
        nominal_account: str,
        amount_pounds: float,
        reference: str,
        post_date: date,
        description: str = "",
        input_by: str = "IMPORT",
        is_receipt: bool = False,
        cbtype: str = None,
        validate_only: bool = False,
        project_code: str = "",
        department_code: str = "",
        vat_code: str = ""
    ) -> Opera3ImportResult:
        """
        Import a nominal-only entry into Opera 3.

        Creates a cashbook entry that posts directly to a nominal account
        without going through sales or purchase ledger.

        Creates records in:
        1. aentry (Cashbook Entry Header)
        2. atran (Cashbook Transaction)
        3. ntran (Nominal Ledger - 2 rows for double-entry)
        4. anoml (Transfer file - 2 rows)
        5. atype (Entry counter update)
        6. nacnt/nhist/nbank (Balance updates)

        Args:
            bank_account: Bank account code (e.g., 'BC010')
            nominal_account: Nominal account code (e.g., '7502')
            amount_pounds: Amount in POUNDS (always positive)
            reference: Transaction reference (max 20 chars)
            post_date: Posting date
            description: Transaction description/comment
            input_by: User code for audit trail (max 8 chars)
            is_receipt: If True, money IN. If False, money OUT.
            cbtype: Cashbook type code from atype. If None, uses first available.
            validate_only: If True, only validate without inserting
            project_code: Project code for Advanced Nominal analysis (max 8 chars)
            department_code: Department code for Advanced Nominal analysis (max 8 chars)

        Returns:
            Opera3ImportResult with details of the operation
        """
        errors = []
        warnings = []

        # Determine transaction type
        if is_receipt:
            required_category = AtypeCategory.RECEIPT
            at_type = CashbookTransactionType.NOMINAL_RECEIPT
            type_name = 'nominal_receipt'
        else:
            required_category = AtypeCategory.PAYMENT
            at_type = CashbookTransactionType.NOMINAL_PAYMENT
            type_name = 'nominal_payment'

        # =====================
        # VALIDATE/GET CBTYPE
        # =====================
        if cbtype is None:
            cbtype = self.get_default_cbtype(type_name)
            if cbtype is None:
                return Opera3ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[f"No {required_category} type codes found in atype table"]
                )
            logger.debug(f"Using default cbtype for {type_name}: {cbtype}")

        type_validation = self.validate_cbtype(cbtype, required_category=required_category)
        if not type_validation['valid']:
            return Opera3ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[type_validation['error']]
            )

        try:
            # =====================
            # PERIOD VALIDATION (Nominal Ledger)
            # =====================
            from sql_rag.opera3_config import Opera3Config, get_period_posting_decision
            config = Opera3Config(str(self.data_path), self.encoding)
            period_result = config.validate_posting_period(post_date, ledger_type='NL')

            if not period_result.is_valid:
                return Opera3ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[period_result.error_message]
                )

            posting_decision = get_period_posting_decision(config, post_date, 'NL')

            # =====================
            # VALIDATE ACCOUNTS
            # =====================
            # Validate bank account
            try:
                nbank_records = self._read_table_safe('nbank') if hasattr(self, '_read_table_safe') else []
                if not nbank_records:
                    from sql_rag.opera3_foxpro import Opera3Reader
                    reader = Opera3Reader(str(self.data_path), encoding=self.encoding)
                    nbank_records = reader.read_table('nbank')
                bank_found = any(
                    (r.get('NK_ACNT', r.get('nk_acnt', '')) or '').strip() == bank_account
                    for r in nbank_records
                )
                if not bank_found:
                    errors.append(f"Bank account '{bank_account}' not found in nbank")
            except Exception as e:
                logger.warning(f"Could not validate bank account: {e}")

            # Validate nominal account and read project/department flags
            nominal_name = nominal_account
            na_allwprj = 0
            na_allwjob = 0
            try:
                nacnt_records = []
                try:
                    from sql_rag.opera3_foxpro import Opera3Reader
                    reader = Opera3Reader(str(self.data_path), encoding=self.encoding)
                    nacnt_records = reader.read_table('nacnt')
                except Exception:
                    pass

                nominal_row = None
                for r in nacnt_records:
                    acnt = (r.get('NA_ACNT', r.get('na_acnt', '')) or '').strip()
                    if acnt == nominal_account:
                        nominal_row = r
                        break

                if nominal_row is None:
                    errors.append(f"Nominal account '{nominal_account}' not found in nacnt")
                else:
                    nominal_name = (nominal_row.get('NA_DESC', nominal_row.get('na_desc', '')) or '').strip() or nominal_account
                    na_allwprj = int(nominal_row.get('NA_ALLWPRJ', nominal_row.get('na_allwprj', 0)) or 0)
                    na_allwjob = int(nominal_row.get('NA_ALLWJOB', nominal_row.get('na_allwjob', 0)) or 0)

                    # Apply defaults if no code provided
                    # Opera values: 1=Do Not Use, 2=Optional, 3=Mandatory
                    if not project_code and na_allwprj > 1:
                        default_proj = (nominal_row.get('NA_PROJECT', nominal_row.get('na_project', '')) or '').strip()
                        if default_proj:
                            project_code = default_proj
                    if not department_code and na_allwjob > 1:
                        default_job = (nominal_row.get('NA_JOB', nominal_row.get('na_job', '')) or '').strip()
                        if default_job:
                            department_code = default_job

                    # Mandatory checks — Opera values: 1=Do Not Use, 2=Optional, 3=Mandatory
                    if na_allwprj == 3 and not project_code:
                        errors.append(f"Project code is mandatory for nominal account '{nominal_account}'")
                    if na_allwjob == 3 and not department_code:
                        errors.append(f"Department code is mandatory for nominal account '{nominal_account}'")

                    # Validate codes exist in master tables
                    if project_code:
                        try:
                            nproj_records = reader.read_table('nproj')
                            proj_found = any(
                                (r.get('NR_PROJECT', r.get('nr_project', '')) or '').strip() == project_code
                                for r in nproj_records
                            )
                            if not proj_found:
                                errors.append(f"Project code '{project_code}' not found in nproj")
                        except Exception:
                            logger.debug("nproj table not available, skipping project validation")

                    if department_code:
                        try:
                            njob_records = reader.read_table('njob')
                            dept_found = any(
                                (r.get('NO_JOB', r.get('no_job', '')) or '').strip() == department_code
                                for r in njob_records
                            )
                            if not dept_found:
                                errors.append(f"Department code '{department_code}' not found in njob")
                        except Exception:
                            logger.debug("njob table not available, skipping department validation")
            except Exception as e:
                logger.warning(f"Could not validate nominal account: {e}")

            if errors:
                return Opera3ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=errors
                )

            if validate_only:
                return Opera3ImportResult(
                    success=True,
                    records_processed=1,
                    records_imported=1,
                    warnings=["Validation passed - no records inserted (validate_only=True)"]
                )

            # =====================
            # PREPARE DATA
            # =====================
            amount_pence = int(round(amount_pounds * 100))
            entry_value = amount_pence if is_receipt else -amount_pence

            if isinstance(post_date, str):
                post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

            period, year = config.get_period_for_date(post_date)
            now = datetime.now()

            # Pad project/department codes to 8 chars
            project_padded = f"{(project_code or '')[:8]:<8}"
            department_padded = f"{(department_code or '')[:8]:<8}"

            ntran_comment = f"{description[:50]:<50}" if description else f"{reference[:50]:<50}"
            ntran_trnref = f"{nominal_name[:30]:<30}{reference[:20]:<20}"

            # =====================
            # VAT CALCULATION (if vat_code provided)
            # =====================
            has_vat = False
            vat_amount = 0.0
            net_amount = amount_pounds
            vat_nominal_account = ''
            vat_rate = 0.0

            if vat_code and vat_code.strip() and vat_code.strip().upper() not in ('', 'N/A', 'NONE'):
                vat_type = 'P' if not is_receipt else 'S'
                vat_info = self.get_vat_rate(vat_code.strip(), vat_type, post_date)
                vat_rate = vat_info.get('rate', 0.0)
                if vat_rate > 0:
                    has_vat = True
                    vat_nominal_account = vat_info.get('nominal', '')
                    vat_amount = round(amount_pounds * vat_rate / (100 + vat_rate), 2)
                    net_amount = round(amount_pounds - vat_amount, 2)
                    logger.info(f"NOMINAL_ENTRY_DEBUG: Opera 3 VAT split - gross={amount_pounds}, net={net_amount}, vat={vat_amount}, rate={vat_rate}%, vat_nominal={vat_nominal_account}")

            # Generate unique IDs
            id_count = 5 if has_vat else 3
            unique_ids = OperaUniqueIdGenerator.generate_multiple(id_count)
            atran_unique = unique_ids[0]
            ntran_pstid_bank = unique_ids[1]
            ntran_pstid_nominal = unique_ids[2]
            if has_vat:
                atran_vat_unique = unique_ids[3]
                ntran_pstid_vat = unique_ids[4]

            # Double-entry values (pounds for ntran/anoml)
            # Bank always gets GROSS; nominal gets NET when VAT applies
            if is_receipt:
                bank_ntran_value = amount_pounds   # Debit bank (gross)
                nominal_ntran_value = -net_amount  # Credit nominal (net when VAT)
                vat_ntran_value = -vat_amount if has_vat else 0  # Credit VAT
            else:
                bank_ntran_value = -amount_pounds  # Credit bank (gross)
                nominal_ntran_value = net_amount   # Debit nominal (net when VAT)
                vat_ntran_value = vat_amount if has_vat else 0  # Debit VAT

            # =====================
            # ACQUIRE LOCKS AND EXECUTE
            # =====================
            tables_to_lock = ['aentry', 'atran', 'atype', 'nparm']
            if posting_decision.post_to_nominal:
                tables_to_lock.extend(['ntran', 'nacnt', 'nbank', 'nhist'])
            if posting_decision.post_to_transfer_file:
                tables_to_lock.append('anoml')
            if has_vat:
                tables_to_lock.extend(['zvtran', 'nvat'])

            with self._transaction_lock(tables_to_lock):
                entry_number = self.increment_atype_entry(cbtype)
                journal_number = self._get_next_journal()

                logger.info(f"NOMINAL_ENTRY_DEBUG: Opera 3 import - bank={bank_account}, nominal={nominal_account}, "
                           f"amount={amount_pounds}, is_receipt={is_receipt}, project='{project_code}', department='{department_code}', vat_code='{vat_code}', has_vat={has_vat}")

                # 1. INSERT INTO aentry (GROSS amount in pence)
                ae_complet_flag = 1  # Always complete — NL transfer via anoml when real-time update is off
                aentry_table = self._open_table('aentry')
                aentry_table.append({
                    'ae_acnt': bank_account[:8],
                    'ae_cntr': '    ',
                    'ae_cbtype': cbtype,
                    'ae_entry': entry_number[:12],
                    'ae_reclnum': 0,
                    'ae_lstdate': post_date,
                    'ae_frstat': 0,
                    'ae_tostat': 0,
                    'ae_statln': 0,
                    'ae_entref': reference[:20],
                    'ae_value': entry_value,
                    'ae_recbal': 0,
                    'ae_remove': 0,
                    'ae_tmpstat': 0,
                    'ae_complet': ae_complet_flag,
                    'ae_postgrp': 0,
                    'sq_crdate': now.date(),
                    'sq_crtime': now.strftime('%H:%M:%S')[:8],
                    'sq_cruser': input_by[:8],
                    'ae_comment': description[:40] if description else '',
                })

                # 2. INSERT INTO atran
                atran_table = self._open_table('atran')
                if has_vat:
                    # VAT split: 2 atran lines (net to nominal, VAT to VAT account)
                    net_pence = int(round(net_amount * 100))
                    vat_pence = int(round(vat_amount * 100))
                    net_entry_value = net_pence if is_receipt else -net_pence
                    vat_entry_value = vat_pence if is_receipt else -vat_pence

                    # Line 1 - NET amount to nominal account
                    atran_table.append({
                        'at_acnt': bank_account[:8],
                        'at_cntr': '    ',
                        'at_cbtype': cbtype,
                        'at_entry': entry_number[:12],
                        'at_inputby': input_by[:8],
                        'at_type': at_type,
                        'at_pstdate': post_date,
                        'at_sysdate': post_date,
                        'at_tperiod': 1,
                        'at_value': net_entry_value,
                        'at_disc': 0,
                        'at_fcurr': '   ',
                        'at_fcexch': 1.0,
                        'at_fcmult': 0,
                        'at_fcdec': 2,
                        'at_account': nominal_account[:8],
                        'at_name': nominal_name[:35],
                        'at_comment': description[:50] if description else '',
                        'at_payee': '        ',
                        'at_payname': '',
                        'at_sort': '        ',
                        'at_number': '         ',
                        'at_remove': 0,
                        'at_chqprn': 0,
                        'at_chqlst': 0,
                        'at_bacprn': 0,
                        'at_ccdprn': 0,
                        'at_ccdno': '',
                        'at_payslp': 0,
                        'at_pysprn': 0,
                        'at_cash': 0,
                        'at_remit': 0,
                        'at_unique': atran_unique[:10],
                        'at_postgrp': 0,
                        'at_ccauth': '0       ',
                        'at_refer': reference[:20],
                        'at_srcco': 'I',
                        'at_project': project_padded,
                        'at_job': department_padded,
                    })

                    # Line 2 - VAT amount to VAT nominal account
                    atran_table.append({
                        'at_acnt': bank_account[:8],
                        'at_cntr': '   1',
                        'at_cbtype': cbtype,
                        'at_entry': entry_number[:12],
                        'at_inputby': input_by[:8],
                        'at_type': at_type,
                        'at_pstdate': post_date,
                        'at_sysdate': post_date,
                        'at_tperiod': 1,
                        'at_value': vat_entry_value,
                        'at_disc': 0,
                        'at_fcurr': '   ',
                        'at_fcexch': 1.0,
                        'at_fcmult': 0,
                        'at_fcdec': 2,
                        'at_account': vat_nominal_account[:8],
                        'at_name': f"{nominal_name[:31]} VAT",
                        'at_comment': description[:50] if description else '',
                        'at_payee': '        ',
                        'at_payname': '',
                        'at_sort': '        ',
                        'at_number': '         ',
                        'at_remove': 0,
                        'at_chqprn': 0,
                        'at_chqlst': 0,
                        'at_bacprn': 0,
                        'at_ccdprn': 0,
                        'at_ccdno': '',
                        'at_payslp': 0,
                        'at_pysprn': 0,
                        'at_cash': 0,
                        'at_remit': 0,
                        'at_unique': atran_vat_unique[:10],
                        'at_postgrp': 0,
                        'at_ccauth': '0       ',
                        'at_refer': reference[:20],
                        'at_srcco': 'I',
                        'at_project': '        ',
                        'at_job': '        ',
                    })
                else:
                    # No VAT: single atran line with full amount
                    atran_table.append({
                        'at_acnt': bank_account[:8],
                        'at_cntr': '    ',
                        'at_cbtype': cbtype,
                        'at_entry': entry_number[:12],
                        'at_inputby': input_by[:8],
                        'at_type': at_type,
                        'at_pstdate': post_date,
                        'at_sysdate': post_date,
                        'at_tperiod': 1,
                        'at_value': entry_value,
                        'at_disc': 0,
                        'at_fcurr': '   ',
                        'at_fcexch': 1.0,
                        'at_fcmult': 0,
                        'at_fcdec': 2,
                        'at_account': nominal_account[:8],
                        'at_name': nominal_name[:35],
                        'at_comment': description[:50] if description else '',
                        'at_payee': '        ',
                        'at_payname': '',
                        'at_sort': '        ',
                        'at_number': '         ',
                        'at_remove': 0,
                        'at_chqprn': 0,
                        'at_chqlst': 0,
                        'at_bacprn': 0,
                        'at_ccdprn': 0,
                        'at_ccdno': '',
                        'at_payslp': 0,
                        'at_pysprn': 0,
                        'at_cash': 0,
                        'at_remit': 0,
                        'at_unique': atran_unique[:10],
                        'at_postgrp': 0,
                        'at_ccauth': '0       ',
                        'at_refer': reference[:20],
                        'at_srcco': 'I',
                        'at_project': project_padded,
                        'at_job': department_padded,
                    })

                # 3. Nominal postings
                if posting_decision.post_to_nominal:
                    ntran_table = self._open_table('ntran')
                    bank_type = self._get_nacnt_type(bank_account) or ('B ', 'BC')
                    nominal_type = self._get_nacnt_type(nominal_account) or ('B ', 'BB')

                    # Bank side ntran (GROSS amount, blank project/department)
                    ntran_table.append({
                        'nt_acnt': bank_account[:8],
                        'nt_cntr': '    ',
                        'nt_type': bank_type[0],
                        'nt_subt': bank_type[1],
                        'nt_jrnl': journal_number,
                        'nt_ref': '',
                        'nt_inp': input_by[:10],
                        'nt_trtype': 'A',
                        'nt_cmnt': ntran_comment[:50],
                        'nt_trnref': ntran_trnref[:50],
                        'nt_entr': post_date,
                        'nt_value': bank_ntran_value,
                        'nt_year': year,
                        'nt_period': period,
                        'nt_rvrse': 0,
                        'nt_prevyr': 0,
                        'nt_consol': 0,
                        'nt_fcurr': '   ',
                        'nt_fvalue': 0,
                        'nt_fcrate': 0,
                        'nt_fcmult': 0,
                        'nt_fcdec': 0,
                        'nt_srcco': 'I',
                        'nt_cdesc': '',
                        'nt_project': '        ',
                        'nt_job': '        ',
                        'nt_posttyp': 'S',
                        'nt_pstgrp': 0,
                        'nt_pstid': ntran_pstid_bank[:10],
                        'nt_srcnlid': 0,
                        'nt_recurr': 0,
                        'nt_perpost': 0,
                        'nt_rectify': 0,
                        'nt_recjrnl': 0,
                        'nt_vatanal': 0,
                        'nt_distrib': 0,
                    })

                    # Nominal side ntran (NET when VAT, with project/department)
                    ntran_table.append({
                        'nt_acnt': nominal_account[:8],
                        'nt_cntr': '    ',
                        'nt_type': nominal_type[0],
                        'nt_subt': nominal_type[1],
                        'nt_jrnl': journal_number,
                        'nt_ref': '',
                        'nt_inp': input_by[:10],
                        'nt_trtype': 'A',
                        'nt_cmnt': ntran_comment[:50],
                        'nt_trnref': ntran_trnref[:50],
                        'nt_entr': post_date,
                        'nt_value': nominal_ntran_value,
                        'nt_year': year,
                        'nt_period': period,
                        'nt_rvrse': 0,
                        'nt_prevyr': 0,
                        'nt_consol': 0,
                        'nt_fcurr': '   ',
                        'nt_fvalue': 0,
                        'nt_fcrate': 0,
                        'nt_fcmult': 0,
                        'nt_fcdec': 0,
                        'nt_srcco': 'I',
                        'nt_cdesc': '',
                        'nt_project': project_padded,
                        'nt_job': department_padded,
                        'nt_posttyp': 'S',
                        'nt_pstgrp': 0,
                        'nt_pstid': ntran_pstid_nominal[:10],
                        'nt_srcnlid': 0,
                        'nt_recurr': 0,
                        'nt_perpost': 0,
                        'nt_rectify': 0,
                        'nt_recjrnl': 0,
                        'nt_vatanal': 0,
                        'nt_distrib': 0,
                    })

                    # VAT nominal account ntran (only when VAT applies)
                    if has_vat:
                        vat_acct_type = self._get_nacnt_type(vat_nominal_account) or ('B ', 'BB')
                        ntran_table.append({
                            'nt_acnt': vat_nominal_account[:8],
                            'nt_cntr': '    ',
                            'nt_type': vat_acct_type[0],
                            'nt_subt': vat_acct_type[1],
                            'nt_jrnl': journal_number,
                            'nt_ref': '',
                            'nt_inp': input_by[:10],
                            'nt_trtype': 'A',
                            'nt_cmnt': f"{ntran_comment[:46]} VAT",
                            'nt_trnref': ntran_trnref[:50],
                            'nt_entr': post_date,
                            'nt_value': vat_ntran_value,
                            'nt_year': year,
                            'nt_period': period,
                            'nt_rvrse': 0,
                            'nt_prevyr': 0,
                            'nt_consol': 0,
                            'nt_fcurr': '   ',
                            'nt_fvalue': 0,
                            'nt_fcrate': 0,
                            'nt_fcmult': 0,
                            'nt_fcdec': 0,
                            'nt_srcco': 'I',
                            'nt_cdesc': '',
                            'nt_project': '        ',
                            'nt_job': '        ',
                            'nt_posttyp': 'S',
                            'nt_pstgrp': 0,
                            'nt_pstid': ntran_pstid_vat[:10],
                            'nt_srcnlid': 0,
                            'nt_recurr': 0,
                            'nt_perpost': 0,
                            'nt_rectify': 0,
                            'nt_recjrnl': 0,
                            'nt_vatanal': 0,
                            'nt_distrib': 0,
                        })

                    # Update balances
                    self._update_nacnt_balance(bank_account, bank_ntran_value, period, year)
                    self._update_nacnt_balance(nominal_account, nominal_ntran_value, period, year)
                    if has_vat:
                        self._update_nacnt_balance(vat_nominal_account, vat_ntran_value, period, year)

                # Update nbank balance - ALWAYS when atran created (GROSS)
                self._update_nbank_balance(bank_account, bank_ntran_value)

                # 4. Transfer file records (anoml)
                if posting_decision.post_to_transfer_file:
                    done_flag = posting_decision.transfer_file_done_flag
                    jrnl_num = journal_number if posting_decision.post_to_nominal else 0

                    try:
                        anoml_table = self._open_table('anoml')

                        # Bank side anoml (GROSS, blank project/department)
                        anoml_table.append({
                            'ax_nacnt': bank_account[:10],
                            'ax_ncntr': '    ',
                            'ax_source': 'A',
                            'ax_date': post_date,
                            'ax_value': bank_ntran_value,
                            'ax_tref': reference[:20],
                            'ax_comment': ntran_comment[:50],
                            'ax_done': done_flag,
                            'ax_fcurr': '   ',
                            'ax_fvalue': 0,
                            'ax_fcrate': 0,
                            'ax_fcmult': 0,
                            'ax_fcdec': 0,
                            'ax_srcco': 'I',
                            'ax_unique': atran_unique[:10],
                            'ax_project': '        ',
                            'ax_job': '        ',
                            'ax_jrnl': jrnl_num,
                            'ax_nlpdate': post_date,
                        })

                        # Nominal side anoml (NET when VAT, with project/department)
                        anoml_table.append({
                            'ax_nacnt': nominal_account[:10],
                            'ax_ncntr': '    ',
                            'ax_source': 'A',
                            'ax_date': post_date,
                            'ax_value': nominal_ntran_value,
                            'ax_tref': reference[:20],
                            'ax_comment': ntran_comment[:50],
                            'ax_done': done_flag,
                            'ax_fcurr': '   ',
                            'ax_fvalue': 0,
                            'ax_fcrate': 0,
                            'ax_fcmult': 0,
                            'ax_fcdec': 0,
                            'ax_srcco': 'I',
                            'ax_unique': atran_unique[:10],
                            'ax_project': project_padded,
                            'ax_job': department_padded,
                            'ax_jrnl': jrnl_num,
                            'ax_nlpdate': post_date,
                        })

                        # VAT side anoml (only when VAT applies)
                        if has_vat:
                            anoml_table.append({
                                'ax_nacnt': vat_nominal_account[:10],
                                'ax_ncntr': '    ',
                                'ax_source': 'A',
                                'ax_date': post_date,
                                'ax_value': vat_ntran_value,
                                'ax_tref': reference[:20],
                                'ax_comment': f"{ntran_comment[:46]} VAT",
                                'ax_done': done_flag,
                                'ax_fcurr': '   ',
                                'ax_fvalue': 0,
                                'ax_fcrate': 0,
                                'ax_fcmult': 0,
                                'ax_fcdec': 0,
                                'ax_srcco': 'I',
                                'ax_unique': atran_vat_unique[:10],
                                'ax_project': '        ',
                                'ax_job': '        ',
                                'ax_jrnl': jrnl_num,
                                'ax_nlpdate': post_date,
                            })
                    except FileNotFoundError:
                        logger.warning("anoml table not found - skipping transfer file")

                # 5. VAT TRACKING (zvtran + nvat) - only when VAT applies
                if has_vat:
                    desc_clean = description.replace(chr(10), " ").replace(chr(13), " ") if description else ""

                    if is_receipt:
                        va_vattype = 'S'
                        nv_vattype = 'S'
                    else:
                        va_vattype = 'P'
                        nv_vattype = 'P'

                    try:
                        zvtran_table = self._open_table('zvtran')
                        zvtran_table.append({
                            'va_source': 'N',
                            'va_account': nominal_account[:8],
                            'va_laccnt': nominal_account[:8],
                            'va_trdate': post_date,
                            'va_taxdate': post_date,
                            'va_ovrdate': post_date,
                            'va_trref': reference[:20],
                            'va_trtype': 'I',
                            'va_country': 'GB',
                            'va_fcurr': '   ',
                            'va_trvalue': net_amount,
                            'va_fcval': 0,
                            'va_vatval': vat_amount,
                            'va_cost': 0,
                            'va_vatctry': 'H',
                            'va_vattype': va_vattype,
                            'va_anvat': vat_code.strip(),
                            'va_vatrate': vat_rate,
                            'va_box1': 1 if is_receipt else 0,
                            'va_box2': 0,
                            'va_box4': 0 if is_receipt else 1,
                            'va_box6': 1 if is_receipt else 0,
                            'va_box7': 0 if is_receipt else 1,
                            'va_box8': 0,
                            'va_box9': 0,
                            'va_done': 0,
                            'va_import': 0,
                            'va_export': 0,
                        })
                    except FileNotFoundError:
                        logger.warning("zvtran table not found - skipping VAT analysis")

                    try:
                        nvat_table = self._open_table('nvat')
                        nvat_table.append({
                            'nv_acnt': vat_nominal_account[:8],
                            'nv_cntr': '',
                            'nv_date': post_date,
                            'nv_crdate': post_date,
                            'nv_taxdate': post_date,
                            'nv_ref': reference[:20],
                            'nv_type': nv_vattype,
                            'nv_advance': 0,
                            'nv_value': net_amount,
                            'nv_vatval': vat_amount,
                            'nv_vatctry': ' ',
                            'nv_vattype': nv_vattype,
                            'nv_vatcode': vat_code.strip() if vat_code else 'S',
                            'nv_vatrate': vat_rate,
                            'nv_comment': f"{desc_clean[:40]} VAT" if desc_clean else f"{reference[:40]} VAT",
                        })
                    except FileNotFoundError:
                        logger.warning("nvat table not found - skipping VAT return tracking")

                if has_vat:
                    tables_updated = ["aentry", "atran (2)"]
                else:
                    tables_updated = ["aentry", "atran"]
                if posting_decision.post_to_nominal:
                    tables_updated.append(f"ntran ({3 if has_vat else 2})")
                if posting_decision.post_to_transfer_file:
                    tables_updated.append(f"anoml ({3 if has_vat else 2})")
                if has_vat:
                    tables_updated.extend(["zvtran", "nvat"])

                entry_type = "Nominal Receipt" if is_receipt else "Nominal Payment"
                vat_info_msg = []
                if has_vat:
                    vat_info_msg = [
                        f"VAT split: net £{net_amount:.2f} + VAT £{vat_amount:.2f} (code {vat_code.strip()}, {vat_rate}%)"
                    ]
                return Opera3ImportResult(
                    success=True,
                    records_processed=1,
                    records_imported=1,
                    entry_number=entry_number,
                    warnings=[
                        f"Created {entry_type} {entry_number}",
                        f"Amount: £{amount_pounds:.2f}",
                        f"Bank: {bank_account}, Nominal: {nominal_account}",
                        *vat_info_msg,
                        f"Tables updated: {', '.join(tables_updated)}"
                    ]
                )

        except Exception as e:
            logger.error(f"Failed to import nominal entry (Opera 3): {e}")
            import traceback
            traceback.print_exc()
            return Opera3ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[str(e)]
            )
        finally:
            self._close_all_tables()

    # =========================================================================
    # AUTO-ALLOCATION (Opera 3 parity with SQL SE)
    # =========================================================================

    def auto_allocate_receipt(
        self,
        customer_account: str,
        receipt_ref: str,
        receipt_amount: float,
        allocation_date: date,
        bank_account: str = "",
        description: str = None,
        gc_payment_id: str = None
    ) -> Dict[str, Any]:
        """
        Automatically allocate a receipt to matching outstanding invoices.

        Allocation rules (in priority order):
        0. If gc_payment_id provided, look up the original payment request for stored
           invoice_refs. Check each invoice's current state — allocate to those still
           outstanding, skip any already paid manually in Opera, leave remainder on account.
        1. If invoice reference(s) found in description (e.g., "INV26241") AND their
           total matches the receipt exactly -> allocate to those specific invoices
        2. If receipt amount equals TOTAL outstanding balance on account AND there are
           1+ invoices -> allocate to ALL invoices (clears whole account, no ambiguity)

        Args:
            customer_account: Customer code (e.g., 'K009')
            receipt_ref: Receipt reference in stran
            receipt_amount: Receipt amount in POUNDS (positive value)
            allocation_date: Date to use for allocation
            bank_account: Bank account code for salloc record
            description: Description to search for invoice references (optional)
            gc_payment_id: GoCardless payment ID (e.g., 'PM000XXX') for payment request
                          invoice lookup. When provided, uses stored invoice_refs from
                          the original collection request for precise allocation.

        Returns:
            Dict with allocation results
        """
        import re

        result = {
            "success": False,
            "allocated_amount": 0.0,
            "allocations": [],
            "message": ""
        }

        try:
            # Get the receipt from stran
            # When multiple payments from the same customer share the same reference
            # (e.g., GoCardless batch), pick the one closest to the expected amount
            stran_table = self._open_table('stran')
            receipt_candidates = []
            for record in stran_table:
                if (record.st_account.strip().upper() == customer_account.upper()
                        and record.st_trref.strip() == receipt_ref
                        and record.st_trtype.strip() == 'R'
                        and float(record.st_trbal or 0) < 0):
                    receipt_candidates.append(record)

            if not receipt_candidates:
                result["message"] = f"Receipt {receipt_ref} not found or already allocated"
                return result

            # Pick the receipt closest to the expected amount
            receipt_record = min(receipt_candidates, key=lambda r: abs(abs(float(r.st_trbal)) - receipt_amount))
            receipt_balance = abs(float(receipt_record.st_trbal))
            receipt_unique = receipt_record.st_unique.strip() if hasattr(receipt_record, 'st_unique') and receipt_record.st_unique else ''
            receipt_custref = receipt_record.st_custref.strip() if hasattr(receipt_record, 'st_custref') and receipt_record.st_custref else ''

            if receipt_balance <= 0:
                result["message"] = "Receipt already fully allocated"
                return result

            # Get outstanding invoices for customer
            invoices = []
            for record in stran_table:
                if (record.st_account.strip().upper() == customer_account.upper()
                        and record.st_trtype.strip() == 'I'
                        and float(record.st_trbal or 0) > 0):
                    invoices.append({
                        'ref': record.st_trref.strip(),
                        'custref': record.st_custref.strip() if hasattr(record, 'st_custref') and record.st_custref else '',
                        'balance': float(record.st_trbal),
                        'date': record.st_trdate,
                        'unique': record.st_unique.strip() if hasattr(record, 'st_unique') and record.st_unique else '',
                    })

            if not invoices:
                result["message"] = "No outstanding invoices found for customer"
                return result

            # Build list of invoices to allocate
            invoices_to_allocate = []
            allocation_method = None

            total_outstanding = round(sum(inv['balance'] for inv in invoices), 2)
            receipt_rounded = round(receipt_amount, 2)

            # RULE 0: Payment request invoice lookup (GoCardless collections)
            if gc_payment_id and not allocation_method:
                try:
                    from sql_rag.gocardless_payments import get_payments_db
                    payments_db = get_payments_db()
                    payment_request = payments_db.get_payment_request_by_payment_id(gc_payment_id)

                    if payment_request and payment_request.get('invoice_refs'):
                        pr_invoice_refs = payment_request['invoice_refs']
                        logger.info(f"Auto-allocate: payment request {gc_payment_id} has invoice_refs: {pr_invoice_refs}")

                        pr_invoices_to_allocate = []
                        skipped_invoices = []
                        for inv_ref in pr_invoice_refs:
                            found = False
                            for inv in invoices:
                                if inv['ref'].upper() == inv_ref.strip().upper():
                                    if inv['balance'] > 0.005:
                                        pr_invoices_to_allocate.append({
                                            'ref': inv['ref'],
                                            'custref': inv['custref'],
                                            'amount': inv['balance'],
                                            'full_allocation': True,
                                            'unique': inv['unique']
                                        })
                                    else:
                                        skipped_invoices.append(f"{inv_ref} (already paid)")
                                    found = True
                                    break
                            if not found:
                                skipped_invoices.append(f"{inv_ref} (not found/outstanding)")

                        if pr_invoices_to_allocate:
                            total_pr_invoice_balance = round(sum(a['amount'] for a in pr_invoices_to_allocate), 2)

                            if receipt_rounded >= total_pr_invoice_balance:
                                invoices_to_allocate = pr_invoices_to_allocate
                                allocation_method = "payment_request"
                                if skipped_invoices:
                                    logger.info(f"Auto-allocate: skipped invoices (already paid in Opera): {skipped_invoices}")
                            elif receipt_rounded < total_pr_invoice_balance:
                                remaining = receipt_rounded
                                for inv_alloc in pr_invoices_to_allocate:
                                    if remaining <= 0.005:
                                        break
                                    alloc_amt = min(inv_alloc['amount'], remaining)
                                    inv_alloc['amount'] = alloc_amt
                                    inv_alloc['full_allocation'] = abs(alloc_amt - float(inv_alloc['amount'])) < 0.01
                                    remaining -= alloc_amt
                                invoices_to_allocate = [a for a in pr_invoices_to_allocate if a['amount'] > 0.005]
                                allocation_method = "payment_request"
                        elif skipped_invoices:
                            logger.info(f"Auto-allocate: all payment request invoices already paid: {skipped_invoices}")
                except Exception as e:
                    logger.warning(f"Auto-allocate: payment request lookup failed for {gc_payment_id}: {e}")

            # RULE 1: Try to match by invoice reference in description
            inv_matches = []
            if description:
                inv_matches = re.findall(r'INV\d+', description.upper())

            if inv_matches:
                for inv_ref in inv_matches:
                    for inv in invoices:
                        if inv['ref'].upper() == inv_ref:
                            if inv['balance'] > 0:
                                invoices_to_allocate.append({
                                    'ref': inv['ref'],
                                    'custref': inv['custref'],
                                    'amount': inv['balance'],
                                    'full_allocation': True,
                                    'unique': inv['unique']
                                })
                            break

                if invoices_to_allocate:
                    total_invoice_balance = round(sum(a['amount'] for a in invoices_to_allocate), 2)
                    if receipt_rounded == total_invoice_balance:
                        allocation_method = "invoice_reference"
                    else:
                        inv_details = [f"{a['ref']} (£{a['amount']:.2f})" for a in invoices_to_allocate]
                        result["message"] = (
                            f"Invoice reference(s) found but amounts do not match: "
                            f"receipt £{receipt_rounded:.2f} vs invoice total £{total_invoice_balance:.2f}. "
                            f"Found: {inv_details}"
                        )
                        return result

            # RULE 2: If no invoice ref match, check if receipt clears whole account
            if not allocation_method:
                invoice_count = len(invoices)
                if receipt_rounded == total_outstanding and invoice_count >= 1:
                    invoices_to_allocate = []
                    for inv in invoices:
                        if inv['balance'] > 0:
                            invoices_to_allocate.append({
                                'ref': inv['ref'],
                                'custref': inv['custref'],
                                'amount': inv['balance'],
                                'full_allocation': True,
                                'unique': inv['unique']
                            })
                    allocation_method = "clears_account" if invoice_count >= 2 else "single_invoice_match"
                else:
                    if inv_matches:
                        result["message"] = f"Invoice reference(s) {inv_matches} not found in outstanding invoices"
                    else:
                        result["message"] = (
                            f"Cannot auto-allocate: no invoice reference in description and "
                            f"receipt £{receipt_rounded:.2f} does not clear account total £{total_outstanding:.2f}"
                        )
                    return result

            # Amounts verified - proceed with allocation
            total_invoice_amount = round(sum(a['amount'] for a in invoices_to_allocate), 2)
            if allocation_method == "payment_request" and receipt_rounded > total_invoice_amount:
                total_to_allocate = total_invoice_amount
                receipt_fully_allocated = False
            else:
                total_to_allocate = receipt_amount
                receipt_fully_allocated = True

            if isinstance(allocation_date, str):
                allocation_date = datetime.strptime(allocation_date, '%Y-%m-%d').date()

            with self._transaction_lock(['stran', 'salloc']):
                # Get next al_payflag from salloc
                salloc_table = self._open_table('salloc')
                max_payflag = 0
                for record in salloc_table:
                    if record.al_account.strip().upper() == customer_account.upper():
                        pf = int(record.al_payflag or 0)
                        if pf > max_payflag:
                            max_payflag = pf
                next_payflag = max_payflag + 1

                # Update receipt in stran
                new_receipt_bal = receipt_balance - total_to_allocate
                receipt_paid_flag = 'A' if receipt_fully_allocated else ' '

                for record in stran_table:
                    if (record.st_account.strip().upper() == customer_account.upper()
                            and record.st_trref.strip() == receipt_ref
                            and record.st_trtype.strip() == 'R'
                            and (not receipt_unique or (hasattr(record, 'st_unique') and record.st_unique.strip() == receipt_unique))):
                        with record:
                            record.st_trbal = -new_receipt_bal
                            record.st_paid = receipt_paid_flag
                            if receipt_fully_allocated:
                                record.st_payday = allocation_date
                            record.st_payflag = next_payflag
                        break

                # Insert salloc record for receipt
                if receipt_fully_allocated:
                    if allocation_method == "payment_request":
                        alloc_ref2 = "AUTO:GC_REQ"
                    elif allocation_method == "invoice_reference":
                        alloc_ref2 = "AUTO:INV_REF"
                    else:
                        alloc_ref2 = "AUTO:CLR_ACCT"
                    salloc_table.append({
                        'al_account': customer_account[:8],
                        'al_date': allocation_date,
                        'al_ref1': receipt_ref[:20],
                        'al_ref2': alloc_ref2[:20],
                        'al_type': 'R',
                        'al_val': -receipt_balance,
                        'al_dval': 0,
                        'al_origval': -receipt_balance,
                        'al_payind': 'A',
                        'al_payflag': next_payflag,
                        'al_payday': allocation_date,
                        'al_ctype': 'O',
                        'al_rem': ' ',
                        'al_cheq': ' ',
                        'al_payee': customer_account[:30],
                        'al_fcurr': '   ',
                        'al_fval': 0,
                        'al_fdval': 0,
                        'al_forigvl': 0,
                        'al_fdec': 0,
                        'al_unique': 0,
                        'al_acnt': bank_account[:8],
                        'al_cntr': '    ',
                        'al_advind': 0,
                        'al_advtran': 0,
                        'al_preprd': 0,
                        'al_bacsid': 0,
                        'al_adjsv': 0,
                    })

                # Update each invoice and create salloc records
                for alloc in invoices_to_allocate:
                    inv_ref = alloc['ref']
                    alloc_amount = alloc['amount']

                    for record in stran_table:
                        if (record.st_account.strip().upper() == customer_account.upper()
                                and record.st_trref.strip() == inv_ref
                                and record.st_trtype.strip() == 'I'):
                            new_inv_bal = float(record.st_trbal) - alloc_amount
                            inv_paid_flag = 'P' if new_inv_bal < 0.01 else ' '
                            inv_date = record.st_trdate

                            with record:
                                record.st_trbal = new_inv_bal
                                record.st_paid = inv_paid_flag
                                if new_inv_bal < 0.01:
                                    record.st_payday = allocation_date
                                record.st_payflag = next_payflag

                            # Insert salloc record for invoice (if fully paid)
                            if new_inv_bal < 0.01:
                                salloc_table.append({
                                    'al_account': customer_account[:8],
                                    'al_date': inv_date,
                                    'al_ref1': inv_ref[:20],
                                    'al_ref2': alloc['custref'][:20] if alloc['custref'] else ' ',
                                    'al_type': 'I',
                                    'al_val': alloc_amount,
                                    'al_dval': 0,
                                    'al_origval': alloc_amount,
                                    'al_payind': 'A',
                                    'al_payflag': next_payflag,
                                    'al_payday': allocation_date,
                                    'al_ctype': 'O',
                                    'al_rem': ' ',
                                    'al_cheq': ' ',
                                    'al_payee': customer_account[:30],
                                    'al_fcurr': '   ',
                                    'al_fval': 0,
                                    'al_fdval': 0,
                                    'al_forigvl': 0,
                                    'al_fdec': 0,
                                    'al_unique': 0,
                                    'al_acnt': bank_account[:8],
                                    'al_cntr': '    ',
                                    'al_advind': 0,
                                    'al_advtran': 0,
                                    'al_preprd': 0,
                                    'al_bacsid': 0,
                                    'al_adjsv': 0,
                                })
                            break

            result["success"] = True
            result["allocated_amount"] = total_to_allocate
            result["allocations"] = invoices_to_allocate
            result["receipt_fully_allocated"] = receipt_fully_allocated
            result["allocation_method"] = allocation_method

            if allocation_method == "payment_request":
                result["message"] = f"Allocated £{total_to_allocate:.2f} to {len(invoices_to_allocate)} invoice(s) from payment request"
            elif allocation_method == "invoice_reference":
                result["message"] = f"Allocated £{total_to_allocate:.2f} to {len(invoices_to_allocate)} invoice(s) by reference"
            else:
                result["message"] = f"Allocated £{total_to_allocate:.2f} to {len(invoices_to_allocate)} invoice(s) - clears account"

            logger.info(f"Auto-allocated receipt {receipt_ref} for {customer_account}: £{total_to_allocate:.2f} to {len(invoices_to_allocate)} invoices ({allocation_method})")
            return result

        except Exception as e:
            logger.error(f"Auto-allocation failed for {receipt_ref}: {e}")
            result["message"] = f"Allocation failed: {str(e)}"
            return result

    def auto_allocate_payment(
        self,
        supplier_account: str,
        payment_ref: str,
        payment_amount: float,
        allocation_date: date,
        bank_account: str = "",
        description: str = None
    ) -> Dict[str, Any]:
        """
        Automatically allocate a payment to matching outstanding supplier invoices.

        Same allocation rules as auto_allocate_receipt but for purchase ledger:
        1. If invoice reference(s) found in description AND their total matches
           the payment exactly -> allocate to those specific invoices
        2. If payment amount equals TOTAL outstanding balance on account AND there are
           1+ invoices -> allocate to ALL invoices (clears whole account, no ambiguity)

        Args:
            supplier_account: Supplier code (e.g., 'P001')
            payment_ref: Payment reference in ptran
            payment_amount: Payment amount in POUNDS (positive value)
            allocation_date: Date to use for allocation
            bank_account: Bank account code for palloc record
            description: Description to search for invoice references (optional)

        Returns:
            Dict with allocation results
        """
        import re

        result = {
            "success": False,
            "allocated_amount": 0.0,
            "allocations": [],
            "message": ""
        }

        try:
            # Get the payment from ptran
            ptran_table = self._open_table('ptran')
            payment_record = None
            for record in ptran_table:
                if (record.pt_account.strip().upper() == supplier_account.upper()
                        and record.pt_trref.strip() == payment_ref
                        and record.pt_trtype.strip() == 'P'
                        and float(record.pt_trbal or 0) < 0):
                    payment_record = record
                    break

            if payment_record is None:
                result["message"] = f"Payment {payment_ref} not found or already allocated"
                return result

            payment_balance = abs(float(payment_record.pt_trbal))
            payment_suppref = payment_record.pt_suppref.strip() if hasattr(payment_record, 'pt_suppref') and payment_record.pt_suppref else ''

            if payment_balance <= 0:
                result["message"] = "Payment already fully allocated"
                return result

            # Get outstanding invoices for supplier
            invoices = []
            for record in ptran_table:
                if (record.pt_account.strip().upper() == supplier_account.upper()
                        and record.pt_trtype.strip() == 'I'
                        and float(record.pt_trbal or 0) > 0):
                    invoices.append({
                        'ref': record.pt_trref.strip(),
                        'suppref': record.pt_suppref.strip() if hasattr(record, 'pt_suppref') and record.pt_suppref else '',
                        'balance': float(record.pt_trbal),
                        'date': record.pt_trdate,
                        'unique': record.pt_unique.strip() if hasattr(record, 'pt_unique') and record.pt_unique else '',
                    })

            if not invoices:
                result["message"] = "No outstanding invoices found for supplier"
                return result

            # Build list of invoices to allocate
            invoices_to_allocate = []
            allocation_method = None

            total_outstanding = round(sum(inv['balance'] for inv in invoices), 2)
            payment_rounded = round(payment_amount, 2)

            # RULE 1: Try to match by invoice reference in description
            inv_matches = []
            if description:
                inv_matches = re.findall(r'(?:PI|INV|PINV|P/INV)[\s-]?\d+', description.upper())
                if not inv_matches:
                    # Look for references in ptran pt_suppref format
                    for inv in invoices:
                        suppref = inv['suppref']
                        if suppref and suppref.upper() in description.upper():
                            inv_matches.append(suppref)

            if inv_matches:
                for inv_ref_pattern in inv_matches:
                    inv_ref_clean = re.sub(r'[\s-]', '', inv_ref_pattern.upper())
                    for inv in invoices:
                        inv_trref = inv['ref'].upper()
                        inv_suppref = inv['suppref'].upper()
                        inv_trref_clean = re.sub(r'[\s-]', '', inv_trref)
                        inv_suppref_clean = re.sub(r'[\s-]', '', inv_suppref)

                        if inv_ref_clean == inv_trref_clean or inv_ref_clean == inv_suppref_clean or inv_ref_pattern.upper() == inv_suppref:
                            if inv['balance'] > 0:
                                already_added = any(a['ref'] == inv['ref'] for a in invoices_to_allocate)
                                if not already_added:
                                    invoices_to_allocate.append({
                                        'ref': inv['ref'],
                                        'suppref': inv['suppref'],
                                        'amount': inv['balance'],
                                        'full_allocation': True,
                                        'unique': inv['unique']
                                    })
                            break

                if invoices_to_allocate:
                    total_invoice_balance = round(sum(a['amount'] for a in invoices_to_allocate), 2)
                    if payment_rounded == total_invoice_balance:
                        allocation_method = "invoice_reference"
                    else:
                        inv_details = [f"{a['ref']} (£{a['amount']:.2f})" for a in invoices_to_allocate]
                        result["message"] = (
                            f"Invoice reference(s) found but amounts do not match: "
                            f"payment £{payment_rounded:.2f} vs invoice total £{total_invoice_balance:.2f}. "
                            f"Found: {inv_details}"
                        )
                        return result

            # RULE 2: If no invoice ref match, check if payment clears whole account
            if not allocation_method:
                invoice_count = len(invoices)
                if payment_rounded == total_outstanding and invoice_count >= 1:
                    invoices_to_allocate = []
                    for inv in invoices:
                        if inv['balance'] > 0:
                            invoices_to_allocate.append({
                                'ref': inv['ref'],
                                'suppref': inv['suppref'],
                                'amount': inv['balance'],
                                'full_allocation': True,
                                'unique': inv['unique']
                            })
                    allocation_method = "clears_account" if invoice_count >= 2 else "single_invoice_match"
                else:
                    if inv_matches:
                        result["message"] = f"Invoice reference(s) {inv_matches} not found in outstanding invoices"
                    else:
                        result["message"] = (
                            f"Cannot auto-allocate: no invoice reference in description and "
                            f"payment £{payment_rounded:.2f} does not clear account total £{total_outstanding:.2f}"
                        )
                    return result

            # Amounts verified - proceed with allocation
            total_to_allocate = payment_amount
            payment_fully_allocated = True

            if isinstance(allocation_date, str):
                allocation_date = datetime.strptime(allocation_date, '%Y-%m-%d').date()

            with self._transaction_lock(['ptran', 'palloc']):
                # Get next al_payflag from palloc
                palloc_table = self._open_table('palloc')
                max_payflag = 0
                for record in palloc_table:
                    if record.al_account.strip().upper() == supplier_account.upper():
                        pf = int(record.al_payflag or 0)
                        if pf > max_payflag:
                            max_payflag = pf
                next_payflag = max_payflag + 1

                # Update payment in ptran
                new_payment_bal = payment_balance - total_to_allocate
                payment_paid_flag = 'A' if payment_fully_allocated else ' '

                for record in ptran_table:
                    if (record.pt_account.strip().upper() == supplier_account.upper()
                            and record.pt_trref.strip() == payment_ref
                            and record.pt_trtype.strip() == 'P'):
                        with record:
                            record.pt_trbal = -new_payment_bal
                            record.pt_paid = payment_paid_flag
                            if payment_fully_allocated:
                                record.pt_payday = allocation_date
                            record.pt_payflag = next_payflag
                        break

                # Insert palloc record for payment
                if payment_fully_allocated:
                    alloc_ref2 = "AUTO:INV_REF" if allocation_method == "invoice_reference" else "AUTO:CLR_ACCT"
                    palloc_table.append({
                        'al_account': supplier_account[:8],
                        'al_date': allocation_date,
                        'al_ref1': payment_ref[:20],
                        'al_ref2': alloc_ref2[:20],
                        'al_type': 'P',
                        'al_val': -payment_balance,
                        'al_dval': 0,
                        'al_origval': -payment_balance,
                        'al_payind': 'A',
                        'al_payflag': next_payflag,
                        'al_payday': allocation_date,
                        'al_ctype': 'O',
                        'al_rem': ' ',
                        'al_cheq': ' ',
                        'al_payee': supplier_account[:30],
                        'al_fcurr': '   ',
                        'al_fval': 0,
                        'al_fdval': 0,
                        'al_forigvl': 0,
                        'al_fdec': 0,
                        'al_unique': 0,
                        'al_acnt': bank_account[:8],
                        'al_cntr': '    ',
                        'al_advind': 0,
                        'al_advtran': 0,
                        'al_preprd': 0,
                        'al_bacsid': 0,
                        'al_adjsv': 0,
                    })

                # Update each invoice and create palloc records
                for alloc in invoices_to_allocate:
                    inv_ref = alloc['ref']
                    alloc_amount = alloc['amount']

                    for record in ptran_table:
                        if (record.pt_account.strip().upper() == supplier_account.upper()
                                and record.pt_trref.strip() == inv_ref
                                and record.pt_trtype.strip() == 'I'):
                            new_inv_bal = float(record.pt_trbal) - alloc_amount
                            inv_paid_flag = 'P' if new_inv_bal < 0.01 else ' '
                            inv_date = record.pt_trdate

                            with record:
                                record.pt_trbal = new_inv_bal
                                record.pt_paid = inv_paid_flag
                                if new_inv_bal < 0.01:
                                    record.pt_payday = allocation_date
                                record.pt_payflag = next_payflag

                            # Insert palloc record for invoice (if fully paid)
                            if new_inv_bal < 0.01:
                                palloc_table.append({
                                    'al_account': supplier_account[:8],
                                    'al_date': inv_date,
                                    'al_ref1': inv_ref[:20],
                                    'al_ref2': alloc['suppref'][:20] if alloc['suppref'] else ' ',
                                    'al_type': 'I',
                                    'al_val': alloc_amount,
                                    'al_dval': 0,
                                    'al_origval': alloc_amount,
                                    'al_payind': 'A',
                                    'al_payflag': next_payflag,
                                    'al_payday': allocation_date,
                                    'al_ctype': 'O',
                                    'al_rem': ' ',
                                    'al_cheq': ' ',
                                    'al_payee': supplier_account[:30],
                                    'al_fcurr': '   ',
                                    'al_fval': 0,
                                    'al_fdval': 0,
                                    'al_forigvl': 0,
                                    'al_fdec': 0,
                                    'al_unique': 0,
                                    'al_acnt': bank_account[:8],
                                    'al_cntr': '    ',
                                    'al_advind': 0,
                                    'al_advtran': 0,
                                    'al_preprd': 0,
                                    'al_bacsid': 0,
                                    'al_adjsv': 0,
                                })
                            break

            result["success"] = True
            result["allocated_amount"] = total_to_allocate
            result["allocations"] = invoices_to_allocate
            result["payment_fully_allocated"] = payment_fully_allocated
            result["allocation_method"] = allocation_method

            if allocation_method == "invoice_reference":
                result["message"] = f"Allocated £{total_to_allocate:.2f} to {len(invoices_to_allocate)} invoice(s) by reference"
            else:
                result["message"] = f"Allocated £{total_to_allocate:.2f} to {len(invoices_to_allocate)} invoice(s) - clears account"

            logger.info(f"Auto-allocated payment {payment_ref} for {supplier_account}: £{total_to_allocate:.2f} to {len(invoices_to_allocate)} invoices ({allocation_method})")
            return result

        except Exception as e:
            logger.error(f"Auto-allocation failed for payment {payment_ref}: {e}")
            result["message"] = f"Allocation failed: {str(e)}"
            return result

    # =========================================================================
    # BANK RECONCILIATION
    # =========================================================================

    def mark_entries_reconciled(
        self,
        bank_account: str,
        entries: List[Dict[str, Any]],
        statement_number: int,
        statement_date: date = None,
        reconciliation_date: date = None,
        partial: bool = False
    ) -> Opera3ImportResult:
        """
        Mark cashbook entries as reconciled in Opera 3 FoxPro DBF files.

        Updates aentry records with reconciliation info and updates nbank
        master record with new reconciled balance.

        When partial=True, entries are marked with statement line numbers but
        nk_recbal is NOT updated — the bank's reconciled balance only advances
        on full reconciliation.

        Args:
            bank_account: Bank account code (e.g., 'BC010')
            entries: List of entries to reconcile, each containing:
                - entry_number: The ae_entry value
                - statement_line: Statement line number (10, 20, 30, etc.)
            statement_number: Bank statement number
            statement_date: Date on the bank statement (defaults to today)
            reconciliation_date: Date of reconciliation (defaults to today)
            partial: If True, skip nk_recbal update (partial reconciliation)

        Returns:
            Opera3ImportResult with details of the reconciliation
        """
        if not entries:
            return Opera3ImportResult(
                success=False,
                errors=["No entries provided for reconciliation"]
            )

        if statement_date is None:
            statement_date = date.today()
        if reconciliation_date is None:
            reconciliation_date = date.today()

        try:
          with self._transaction_lock(['aentry', 'nbank']):
            # Open required tables
            aentry_table = self._open_table('aentry', mode=dbf.READ_WRITE)
            nbank_table = self._open_table('nbank', mode=dbf.READ_WRITE)

            if not aentry_table or not nbank_table:
                return Opera3ImportResult(
                    success=False,
                    errors=["Could not open required tables (aentry, nbank)"]
                )

            # Get current nbank state
            current_rec_balance = 0
            current_rec_line = 0
            nbank_record = None
            nbank_record_num = None

            for i, record in enumerate(nbank_table):
                nk_acnt = str(record.nk_acnt).strip().upper() if hasattr(record, 'nk_acnt') else str(record.NK_ACNT).strip().upper()
                if nk_acnt == bank_account.upper():
                    nbank_record = record
                    nbank_record_num = i
                    current_rec_balance = float(getattr(record, 'nk_recbal', 0) or getattr(record, 'NK_RECBAL', 0) or 0)
                    current_rec_line = int(getattr(record, 'nk_lstrecl', 0) or getattr(record, 'NK_LSTRECL', 0) or 0)
                    break

            if nbank_record is None:
                return Opera3ImportResult(
                    success=False,
                    errors=[f"Bank account {bank_account} not found in nbank"]
                )

            rec_batch_number = current_rec_line

            # Build entry lookup map and validate
            entry_map = {e['entry_number']: e for e in entries}
            entry_numbers = set(entry_map.keys())
            found_entries = {}

            # Find and validate entries in aentry — re-open for fresh scan
            aentry_table = self._open_table('aentry', mode=dbf.READ_WRITE)
            for i, record in enumerate(aentry_table):
                ae_entry = str(getattr(record, 'ae_entry', '') or getattr(record, 'AE_ENTRY', '')).strip()
                if ae_entry in entry_numbers:
                    ae_acnt = str(getattr(record, 'ae_acnt', '') or getattr(record, 'AE_ACNT', '')).strip().upper()
                    ae_reclnum = int(getattr(record, 'ae_reclnum', 0) or getattr(record, 'AE_RECLNUM', 0) or 0)
                    ae_value = float(getattr(record, 'ae_value', 0) or getattr(record, 'AE_VALUE', 0) or 0)

                    if ae_acnt == bank_account.upper():
                        found_entries[ae_entry] = {
                            'record_num': i,
                            'value': ae_value,
                            'reclnum': ae_reclnum
                        }

            # Validate all entries exist and are not already reconciled
            errors = []
            total_value = 0
            for entry in entries:
                entry_num = entry['entry_number']
                if entry_num not in found_entries:
                    errors.append(f"Entry {entry_num} not found for bank {bank_account}")
                elif found_entries[entry_num]['reclnum'] != 0:
                    errors.append(f"Entry {entry_num} already reconciled (reclnum={found_entries[entry_num]['reclnum']})")
                else:
                    total_value += found_entries[entry_num]['value']

            if errors:
                return Opera3ImportResult(
                    success=False,
                    errors=errors
                )

            # Clear ae_tmpstat ONLY on entries we are about to reconcile
            # Never touch other entries — they may belong to a different
            # statement or an in-progress Opera reconciliation session
            cleared = 0
            with aentry_table:
                for entry in entries:
                    entry_num = entry['entry_number']
                    if entry_num in found_entries:
                        entry_info = found_entries[entry_num]
                        aentry_table.goto(entry_info['record_num'])
                        ae_tmpstat = int(getattr(aentry_table.current_record, 'ae_tmpstat', 0) or 0)
                        if ae_tmpstat != 0:
                            aentry_table.write(aentry_table.current_record, {'ae_tmpstat': 0})
                            cleared += 1
            if cleared > 0:
                logger.info(f"Cleared ae_tmpstat on {cleared} entries being reconciled")

            # Calculate new reconciled balance
            new_rec_balance = current_rec_balance + total_value

            # Sort entries by statement line for correct running balance
            sorted_entries = sorted(entries, key=lambda e: e.get('statement_line', 0))

            # Update each aentry record with running balance
            running_balance = current_rec_balance
            updated_count = 0

            # Re-open table for the main update pass
            aentry_table = self._open_table('aentry', mode=dbf.READ_WRITE)
            with aentry_table:
                for entry in sorted_entries:
                    entry_num = entry['entry_number']
                    stmt_line = entry.get('statement_line', 0)
                    entry_info = found_entries[entry_num]

                    # Calculate running balance for this entry
                    running_balance += entry_info['value']

                    # Go to the record
                    aentry_table.goto(entry_info['record_num'])

                    if partial:
                        # Partial: use ae_tmpstat (temporary line number)
                        # exactly as Opera does — entries appear pre-ticked in
                        # Opera Cashbook > Reconcile
                        aentry_table.write(aentry_table.current_record, {
                            'ae_tmpstat': stmt_line
                        })
                        logger.info(f"Set tmpstat for {entry_num} (stmt {statement_number}/{stmt_line})")
                    else:
                        # Full: set permanent reconciliation fields
                        aentry_table.write(aentry_table.current_record, {
                            'ae_reclnum': rec_batch_number,
                            'ae_recdate': reconciliation_date,
                            'ae_statln': stmt_line,
                            'ae_frstat': statement_number,
                            'ae_tostat': statement_number,
                            'ae_tmpstat': 0,
                            'ae_recbal': int(running_balance)
                        })
                        logger.info(f"Marked {entry_num} reconciled (line {stmt_line}, running bal: {running_balance/100:.2f})")
                    updated_count += 1

            # Update nbank master record
            new_rec_line = rec_batch_number + 1

            with nbank_table:
                nbank_table.goto(nbank_record_num)
                if partial:
                    # Partial: update statement tracking + batch counter, NOT nk_recbal
                    # Matches Opera's behaviour exactly
                    max_stmt_line = max(e.get('statement_line', 0) for e in sorted_entries) if sorted_entries else 0
                    nbank_table.write(nbank_table.current_record, {
                        'nk_lstrecl': new_rec_line,
                        'nk_lststno': statement_number,
                        'nk_lststdt': statement_date,
                        'nk_reclnum': new_rec_line,
                        'nk_recldte': reconciliation_date,
                        'nk_recstfr': statement_number,
                        'nk_recstto': statement_number,
                        'nk_recstdt': statement_date,
                        'nk_recstln': max_stmt_line
                    })
                    logger.info(f"Partial reconciliation — nk_recbal NOT updated (remains at {current_rec_balance/100:.2f})")
                else:
                    # Full: update everything including nk_recbal
                    # Reset nk_reccfwd to 0 — reconciliation is complete, no statement
                    # in progress. Ensures Opera's reconcile dialog shows Statement Balance = 0.
                    max_stmt_line = max(e.get('statement_line', 0) for e in sorted_entries) if sorted_entries else 0
                    nbank_table.write(nbank_table.current_record, {
                        'nk_recbal': int(new_rec_balance),
                        'nk_reccfwd': 0,
                        'nk_lstrecl': new_rec_line,
                        'nk_lststno': statement_number,
                        'nk_lststdt': statement_date,
                        'nk_reclnum': new_rec_line,
                        'nk_recldte': reconciliation_date,
                        'nk_recstfr': statement_number,
                        'nk_recstto': statement_number,
                        'nk_recstdt': statement_date,
                        'nk_recstln': max_stmt_line
                    })

            # Re-read nk_recbal to verify write
            verified_rec_balance = None
            try:
                nbank_table2 = self._open_table('nbank', mode=dbf.READ_ONLY)
                if nbank_table2:
                    for record in nbank_table2:
                        nk_acnt = str(getattr(record, 'nk_acnt', '') or getattr(record, 'NK_ACNT', '')).strip().upper()
                        if nk_acnt == bank_account.upper():
                            verified_rec_balance = float(getattr(record, 'nk_recbal', 0) or getattr(record, 'NK_RECBAL', 0) or 0) / 100.0
                            break
            except Exception:
                pass

            # Convert pence to pounds for reporting
            total_pounds = total_value / 100.0
            new_rec_pounds = new_rec_balance / 100.0

            if partial:
                logger.info(f"Opera 3 partial bank reconciliation: {updated_count} entries, £{total_pounds:,.2f} (nk_recbal unchanged)")
                return Opera3ImportResult(
                    success=True,
                    records_processed=len(entries),
                    records_imported=updated_count,
                    new_reconciled_balance=verified_rec_balance if verified_rec_balance is not None else current_rec_balance / 100.0,
                    warnings=[
                        f"Partial reconciliation: {updated_count} entries marked with statement line numbers",
                        f"Reconciled balance unchanged: £{verified_rec_balance:,.2f}" if verified_rec_balance else "Reconciled balance unchanged",
                        f"Complete remaining items in Opera Cashbook > Reconcile",
                        f"Statement number: {statement_number}",
                        f"Reconciliation batch: {rec_batch_number}"
                    ]
                )

            logger.info(f"Opera 3 bank reconciliation complete: {updated_count} entries, £{total_pounds:,.2f}")

            return Opera3ImportResult(
                success=True,
                records_processed=len(entries),
                records_imported=updated_count,
                new_reconciled_balance=verified_rec_balance if verified_rec_balance is not None else new_rec_pounds,
                warnings=[
                    f"Reconciled {updated_count} entries totalling £{total_pounds:,.2f}",
                    f"New reconciled balance: £{new_rec_pounds:,.2f}",
                    f"Statement number: {statement_number}",
                    f"Reconciliation batch: {rec_batch_number}"
                ]
            )

        except Exception as e:
            logger.error(f"Error marking entries reconciled in Opera 3: {e}")
            import traceback
            traceback.print_exc()
            return Opera3ImportResult(
                success=False,
                errors=[str(e)]
            )
        finally:
            self._close_all_tables()

    # =========================================================================
    # RECURRING ENTRIES PROCESSING
    # =========================================================================

    def _get_any_vat_nominal(self) -> str:
        """Get the VAT nominal account from this company's ztax table.
        Reads the first available tx_nominal as a dynamic fallback."""
        try:
            reader = Opera3Reader(str(self.data_path), encoding=self.encoding)
            ztax_records = reader.read_table('ztax')
            for r in ztax_records:
                ctrytyp = str(r.get('TX_CTRYTYP', r.get('tx_ctrytyp', ''))).strip()
                if ctrytyp != 'H':
                    continue
                nominal = str(r.get('TX_NOMINAL', r.get('tx_nominal', ''))).strip()
                if nominal:
                    return nominal
        except Exception:
            pass
        return ''

    def get_vat_rate(self, vat_code: str, vat_type: str = 'S', as_of_date: date = None) -> dict:
        """
        Look up VAT rate and nominal account from ztax FoxPro table.

        Args:
            vat_code: VAT code (e.g., '1', '2', 'S')
            vat_type: 'S' for Sales, 'P' for Purchase
            as_of_date: Date to check rate for (defaults to today)

        Returns:
            Dictionary with rate, nominal, description, found
        """
        if as_of_date is None:
            as_of_date = date.today()

        try:
            from sql_rag.opera3_foxpro import Opera3Reader
            reader = Opera3Reader(str(self.data_path), encoding=self.encoding)
            ztax_records = reader.read_table('ztax')

            if not ztax_records:
                fallback = self._get_any_vat_nominal()
                return {'rate': 0.0, 'nominal': fallback, 'description': 'Unknown', 'found': False}

            # Find matching record: code + transaction type + home country
            match = None
            for r in ztax_records:
                code = str(r.get('TX_CODE', r.get('tx_code', ''))).strip()
                trantyp = str(r.get('TX_TRANTYP', r.get('tx_trantyp', ''))).strip()
                ctrytyp = str(r.get('TX_CTRYTYP', r.get('tx_ctrytyp', ''))).strip()
                if code == vat_code and trantyp == vat_type and ctrytyp == 'H':
                    match = r
                    break

            # Fallback: try without transaction type
            if not match:
                for r in ztax_records:
                    code = str(r.get('TX_CODE', r.get('tx_code', ''))).strip()
                    ctrytyp = str(r.get('TX_CTRYTYP', r.get('tx_ctrytyp', ''))).strip()
                    if code == vat_code and ctrytyp == 'H':
                        match = r
                        break

            if not match:
                fallback = self._get_any_vat_nominal()
                return {'rate': 0.0, 'nominal': fallback, 'description': 'Unknown', 'found': False}

            # Get rate (tx_rate1 / tx_rate2 date logic)
            rate1 = float(match.get('TX_RATE1', match.get('tx_rate1', 0)) or 0)
            rate2 = match.get('TX_RATE2', match.get('tx_rate2'))
            rate2_date = match.get('TX_RATE2DY', match.get('tx_rate2dy'))
            nominal = str(match.get('TX_NOMINAL', match.get('tx_nominal', ''))).strip()
            desc = str(match.get('TX_DESC', match.get('tx_desc', ''))).strip()

            if not nominal:
                nominal = self._get_any_vat_nominal()

            rate = rate1
            if rate2_date is not None and rate2 is not None:
                if hasattr(rate2_date, 'date'):
                    rate2_date = rate2_date.date()
                elif isinstance(rate2_date, str) and rate2_date.strip():
                    try:
                        rate2_date = date.fromisoformat(rate2_date[:10])
                    except (ValueError, TypeError):
                        rate2_date = None
                if rate2_date and as_of_date >= rate2_date:
                    rate = float(rate2)

            return {'rate': rate, 'nominal': nominal, 'description': desc, 'found': True}

        except Exception as e:
            logger.warning(f"Failed to look up VAT rate for code {vat_code}: {e}")
            fallback = self._get_any_vat_nominal()
            return {'rate': 0.0, 'nominal': fallback, 'description': 'Unknown', 'found': False}

    def post_recurring_entry(
        self,
        bank_account: str,
        entry_ref: str,
        override_date: date = None,
        input_by: str = "RECUR"
    ) -> Opera3ImportResult:
        """
        Post a recurring entry from arhead/arline — supports multi-line and VAT.

        Creates 1 aentry header + N atran detail lines + per-line ntran/anoml.
        For sales/purchase types, also creates stran/ptran + salloc/palloc.
        VAT lines get additional ntran for VAT account + zvtran/nvat records.
        Schedule advancement is atomic within the transaction lock.

        Args:
            bank_account: Bank account code (e.g., 'BC010')
            entry_ref: Recurring entry reference (ae_entry)
            override_date: Override posting date (uses ae_nxtpost if None)
            input_by: User code for audit trail
        """
        from sql_rag.opera3_config import Opera3Config, get_period_posting_decision
        from dateutil.relativedelta import relativedelta

        TYPE_NAMES = {1: 'Nominal Payment', 2: 'Nominal Receipt', 3: 'Sales Refund', 4: 'Sales Receipt', 5: 'Purchase Payment', 6: 'Purchase Refund'}

        try:
            from sql_rag.opera3_foxpro import Opera3Reader
            reader = Opera3Reader(str(self.data_path), encoding=self.encoding)

            arhead = reader.read_table("arhead")
            arline_data = reader.read_table("arline")

            if not arhead or not arline_data:
                return Opera3ImportResult(
                    success=False, records_processed=1, records_failed=1,
                    errors=["Cannot read arhead/arline tables"]
                )

            # Find the header
            bank_upper = bank_account.strip().upper()
            entry_upper = entry_ref.strip().upper()
            header = None
            for h in arhead:
                acnt = str(h.get("AE_ACNT", h.get("ae_acnt", ""))).strip().upper()
                entry = str(h.get("AE_ENTRY", h.get("ae_entry", ""))).strip().upper()
                if acnt == bank_upper and entry == entry_upper:
                    header = h
                    break

            if not header:
                return Opera3ImportResult(
                    success=False, records_processed=1, records_failed=1,
                    errors=[f"Recurring entry {entry_ref} not found for bank {bank_account}"]
                )

            ae_type = int(header.get("AE_TYPE", header.get("ae_type", 0)) or 0)
            ae_desc = str(header.get("AE_DESC", header.get("ae_desc", ""))).strip()
            ae_freq = str(header.get("AE_FREQ", header.get("ae_freq", ""))).strip()
            ae_every = int(header.get("AE_EVERY", header.get("ae_every", 1)) or 1)
            ae_nxtpost = header.get("AE_NXTPOST", header.get("ae_nxtpost"))
            ae_posted = int(header.get("AE_POSTED", header.get("ae_posted", 0)) or 0)
            ae_topost = int(header.get("AE_TOPOST", header.get("ae_topost", 0)) or 0)

            if ae_topost != 0 and ae_posted >= ae_topost:
                return Opera3ImportResult(
                    success=False, records_processed=1, records_failed=1,
                    errors=[f"Recurring entry {entry_ref} is exhausted ({ae_posted}/{ae_topost} posted)"]
                )

            if ae_type not in (1, 2, 3, 4, 5, 6):
                return Opera3ImportResult(
                    success=False, records_processed=1, records_failed=1,
                    errors=[f"Unsupported recurring entry type {ae_type} — process in Opera"]
                )

            # Determine posting date
            if override_date:
                post_date = override_date
            elif ae_nxtpost:
                if isinstance(ae_nxtpost, str):
                    post_date = date.fromisoformat(ae_nxtpost[:10])
                elif hasattr(ae_nxtpost, 'date'):
                    post_date = ae_nxtpost.date()
                else:
                    post_date = ae_nxtpost
            else:
                return Opera3ImportResult(
                    success=False, records_processed=1, records_failed=1,
                    errors=[f"Recurring entry {entry_ref} has no next posting date"]
                )

            # Find matching arline records (ALL lines, not just first)
            matching_lines = []
            for l in arline_data:
                l_entry = str(l.get("AT_ENTRY", l.get("at_entry", ""))).strip().upper()
                l_acnt = str(l.get("AT_ACNT", l.get("at_acnt", ""))).strip().upper()
                if l_entry == entry_upper and l_acnt == bank_upper:
                    matching_lines.append(l)

            if not matching_lines:
                return Opera3ImportResult(
                    success=False, records_processed=1, records_failed=1,
                    errors=[f"No detail lines found for recurring entry {entry_ref}"]
                )

            # Parse lines including VAT fields
            parsed_lines = []
            for l in matching_lines:
                vat_code_raw = l.get('AT_VATCDE', l.get('at_vatcde'))
                vat_code = str(vat_code_raw).strip() if vat_code_raw is not None else ''
                vat_val = int(l.get('AT_VATVAL', l.get('at_vatval', 0)) or 0)
                has_vat = bool(vat_code and vat_code not in ('', '0', 'N', 'Z', 'E') and abs(vat_val) > 0)

                parsed_lines.append({
                    'account': str(l.get("AT_ACCOUNT", l.get("at_account", ""))).strip(),
                    'cbtype': str(l.get("AT_CBTYPE", l.get("at_cbtype", ""))).strip() or None,
                    'value_pence': int(l.get("AT_VALUE", l.get("at_value", 0)) or 0),
                    'reference': str(l.get("AT_ENTREF", l.get("at_entref", ""))).strip() or ae_desc,
                    'comment': str(l.get("AT_COMMENT", l.get("at_comment", ""))).strip(),
                    'project': str(l.get("AT_PROJECT", l.get("at_project", ""))).strip(),
                    'department': str(l.get("AT_JOB", l.get("at_job", ""))).strip(),
                    'vat_code': vat_code if has_vat else None,
                    'vat_pence': abs(vat_val) if has_vat else 0,
                    'has_vat': has_vat,
                })

            # Use cbtype from first line
            cbtype = parsed_lines[0]['cbtype'] or ('NR' if ae_type == 2 else 'NP')

            # Total amount (pence)
            is_receipt = ae_type in (2, 4, 6)
            total_pence = sum(abs(ln['value_pence']) for ln in parsed_lines)
            total_entry_value = total_pence if is_receipt else -total_pence

            period, year = config.get_period_for_date(post_date)
            now = datetime.now()

            # Period posting decision — use correct ledger type for period checks
            config = Opera3Config(str(self.data_path), self.encoding)
            ledger_type = 'SL' if ae_type in (3, 4) else ('PL' if ae_type in (5, 6) else 'NL')
            period_result = config.validate_posting_period(post_date, ledger_type=ledger_type)
            if not period_result.is_valid:
                return Opera3ImportResult(
                    success=False, records_processed=1, records_failed=1,
                    errors=[period_result.error_message]
                )
            posting_decision = get_period_posting_decision(config, post_date, ledger_type)

            # Pre-fetch customer/supplier info for sales/purchase types
            account_info = {}
            if ae_type in (3, 4):
                for ln in parsed_lines:
                    acct = ln['account']
                    if acct and acct not in account_info:
                        sname_records = reader.read_table('sname')
                        for r in sname_records:
                            sa = str(r.get('SN_ACCOUNT', r.get('sn_account', ''))).strip()
                            if sa == acct:
                                account_info[acct] = {
                                    'name': str(r.get('SN_NAME', r.get('sn_name', ''))).strip()[:35],
                                    'type': str(r.get('SN_CUSTYPE', r.get('sn_custype', ''))).strip()[:3],
                                }
                                break
                        if acct not in account_info:
                            return Opera3ImportResult(
                                success=False, records_processed=1, records_failed=1,
                                errors=[f"Customer account '{acct}' not found"]
                            )
            elif ae_type in (5, 6):
                for ln in parsed_lines:
                    acct = ln['account']
                    if acct and acct not in account_info:
                        pname_records = reader.read_table('pname')
                        for r in pname_records:
                            pa = str(r.get('PN_ACCOUNT', r.get('pn_account', ''))).strip()
                            if pa == acct:
                                account_info[acct] = {
                                    'name': str(r.get('PN_NAME', r.get('pn_name', ''))).strip()[:35],
                                    'type': str(r.get('PN_SUPTYPE', r.get('pn_suptype', ''))).strip()[:3],
                                }
                                break
                        if acct not in account_info:
                            return Opera3ImportResult(
                                success=False, records_processed=1, records_failed=1,
                                errors=[f"Supplier account '{acct}' not found"]
                            )

            # Build tables to lock — include arhead for atomic advancement
            tables_to_lock = ['aentry', 'atran', 'atype', 'arhead', 'nparm']
            if posting_decision.post_to_nominal:
                tables_to_lock.extend(['ntran', 'nacnt', 'nbank', 'nhist'])
            if posting_decision.post_to_transfer_file:
                tables_to_lock.append('anoml')
            if ae_type in (3, 4):
                tables_to_lock.extend(['stran', 'sname'])
            elif ae_type in (5, 6):
                tables_to_lock.extend(['ptran', 'pname'])

            # Check if any line has VAT — need zvtran/nvat tables
            any_vat = any(ln['has_vat'] for ln in parsed_lines)
            if any_vat:
                tables_to_lock.extend(['zvtran', 'nvat'])

            tables_updated = set()

            with self._transaction_lock(tables_to_lock):
                entry_number = self.increment_atype_entry(cbtype)
                tables_updated.add('atype')

                safe_desc = ae_desc[:40] if ae_desc else ''
                ae_complet_flag = 1  # Always complete — NL transfer via anoml when real-time update is off

                # 1. aentry header (total amount)
                aentry_table = self._open_table('aentry')
                aentry_table.append({
                    'ae_acnt': bank_account[:8],
                    'ae_cntr': '    ',
                    'ae_cbtype': cbtype,
                    'ae_entry': entry_number[:12],
                    'ae_reclnum': 0,
                    'ae_lstdate': post_date,
                    'ae_frstat': 0,
                    'ae_tostat': 0,
                    'ae_statln': 0,
                    'ae_entref': parsed_lines[0]['reference'][:20],
                    'ae_value': total_entry_value,
                    'ae_recbal': 0,
                    'ae_remove': 0,
                    'ae_tmpstat': 0,
                    'ae_complet': ae_complet_flag,
                    'ae_postgrp': 0,
                    'sq_crdate': now.date(),
                    'sq_crtime': now.strftime('%H:%M:%S')[:8],
                    'sq_cruser': input_by[:8],
                    'ae_comment': safe_desc,
                })
                tables_updated.add('aentry')

                total_bank_ntran = 0.0  # Track total bank movement for nbank update

                # 2. Process each arline
                for idx, ln in enumerate(parsed_lines):
                    acct = ln['account']
                    gross_pence = abs(ln['value_pence'])
                    gross_pounds = round(gross_pence / 100.0, 2)
                    vat_pence = ln['vat_pence']
                    vat_pounds = round(vat_pence / 100.0, 2) if ln['has_vat'] else 0.0
                    net_pounds = round(gross_pounds - vat_pounds, 2) if ln['has_vat'] else gross_pounds
                    reference = ln['reference'][:20]
                    comment_raw = ln['comment'] or ae_desc
                    safe_comment = comment_raw[:50] if comment_raw else ''
                    project_padded = f"{(ln['project'] or '')[:8]:<8}"
                    department_padded = f"{(ln['department'] or '')[:8]:<8}"

                    # Generate unique IDs for this line
                    unique_ids = OperaUniqueIdGenerator.generate_multiple(4)
                    atran_unique = unique_ids[0]
                    ntran_pstid_bank = unique_ids[1]
                    ntran_pstid_target = unique_ids[2]
                    ntran_pstid_vat = unique_ids[3]
                    ledger_unique = atran_unique  # Must match atran — Opera shares unique ID

                    # atran value: positive for receipts, negative for payments
                    atran_value_pence = gross_pence if is_receipt else -gross_pence

                    # Determine at_type for atran — ae_type maps directly to at_type codes
                    at_type_code = ae_type

                    # Look up account name
                    if ae_type in (1, 2):
                        acct_name = acct
                        try:
                            nacnt_records = reader.read_table('nacnt')
                            for r in nacnt_records:
                                na = str(r.get('NA_ACNT', r.get('na_acnt', ''))).strip()
                                if na == acct:
                                    acct_name = str(r.get('NA_DESC', r.get('na_desc', ''))).strip()[:35] or acct
                                    break
                        except Exception:
                            pass
                    else:
                        acct_name = account_info.get(acct, {}).get('name', acct)[:35]

                    # 2a. atran
                    atran_table = self._open_table('atran')
                    atran_table.append({
                        'at_acnt': bank_account[:8],
                        'at_cntr': '    ',
                        'at_cbtype': cbtype,
                        'at_entry': entry_number[:12],
                        'at_inputby': input_by[:8],
                        'at_type': at_type_code,
                        'at_pstdate': post_date,
                        'at_sysdate': post_date,
                        'at_tperiod': 1,
                        'at_value': atran_value_pence,
                        'at_disc': 0,
                        'at_fcurr': '   ',
                        'at_fcexch': 1.0,
                        'at_fcmult': 0,
                        'at_fcdec': 2,
                        'at_account': acct[:8],
                        'at_name': acct_name[:35],
                        'at_comment': safe_comment[:50],
                        'at_payee': '        ',
                        'at_payname': '',
                        'at_sort': '        ',
                        'at_number': '         ',
                        'at_remove': 0,
                        'at_chqprn': 0,
                        'at_chqlst': 0,
                        'at_bacprn': 0,
                        'at_ccdprn': 0,
                        'at_ccdno': '',
                        'at_payslp': 0,
                        'at_pysprn': 0,
                        'at_cash': 0,
                        'at_remit': 0,
                        'at_unique': atran_unique[:10],
                        'at_postgrp': 0,
                        'at_ccauth': '0       ',
                        'at_refer': reference[:20],
                        'at_srcco': 'I',
                        'at_project': project_padded,
                        'at_job': department_padded,
                    })
                    tables_updated.add('atran')

                    # 2b. Sales/Purchase ledger records
                    if ae_type == 3:
                        # Sales refund — money out to customer, INCREASES debtors balance
                        info = account_info[acct]
                        stran_table = self._open_table('stran')
                        stran_table.append({
                            'st_account': acct[:8],
                            'st_trdate': post_date,
                            'st_trref': reference[:20],
                            'st_cusref': 'BACS',
                            'st_trtype': 'F',
                            'st_trvalue': gross_pounds,
                            'st_vatval': 0,
                            'st_trbal': gross_pounds,
                            'st_paid': ' ',
                            'st_crdate': post_date,
                            'st_advance': 'N',
                            'st_payflag': 0,
                            'st_set1day': 0,
                            'st_set1': 0,
                            'st_set2day': 0,
                            'st_set2': 0,
                            'st_held': ' ',
                            'st_fcurr': '   ',
                            'st_fcrate': 0,
                            'st_fcdec': 0,
                            'st_fcval': 0,
                            'st_fcbal': 0,
                            'st_adval': 0,
                            'st_fadval': 0,
                            'st_fcmult': 0,
                            'st_cbtype': cbtype,
                            'st_entry': entry_number[:12],
                            'st_unique': atran_unique[:10],
                            'st_custype': info.get('type', '   ')[:3],
                            'st_euro': 0,
                            'st_nlpdate': post_date,
                        })

                        # NOTE: No salloc created at posting time -- allocation happens separately

                        self._update_customer_balance(acct, gross_pounds)
                        tables_updated.update(['stran', 'sname'])

                    elif ae_type == 4:
                        # Sales receipt — money in from customer, DECREASES debtors balance
                        info = account_info[acct]
                        stran_table = self._open_table('stran')
                        stran_table.append({
                            'st_account': acct[:8],
                            'st_trdate': post_date,
                            'st_trref': reference[:20],
                            'st_cusref': 'BACS',
                            'st_trtype': 'R',
                            'st_trvalue': -gross_pounds,
                            'st_vatval': 0,
                            'st_trbal': -gross_pounds,
                            'st_paid': ' ',
                            'st_crdate': post_date,
                            'st_advance': 'N',
                            'st_payflag': 0,
                            'st_set1day': 0,
                            'st_set1': 0,
                            'st_set2day': 0,
                            'st_set2': 0,
                            'st_held': ' ',
                            'st_fcurr': '   ',
                            'st_fcrate': 0,
                            'st_fcdec': 0,
                            'st_fcval': 0,
                            'st_fcbal': 0,
                            'st_adval': 0,
                            'st_fadval': 0,
                            'st_fcmult': 0,
                            'st_cbtype': cbtype,
                            'st_entry': entry_number[:12],
                            'st_unique': atran_unique[:10],
                            'st_custype': info.get('type', '   ')[:3],
                            'st_euro': 0,
                            'st_nlpdate': post_date,
                        })

                        # NOTE: No salloc created at posting time -- allocation happens separately

                        self._update_customer_balance(acct, -gross_pounds)
                        tables_updated.update(['stran', 'sname'])

                    elif ae_type == 5:
                        info = account_info[acct]
                        ptran_table = self._open_table('ptran')
                        ptran_table.append({
                            'pt_account': acct[:8],
                            'pt_trdate': post_date,
                            'pt_trref': reference[:20],
                            'pt_supref': 'Direct Cr',
                            'pt_trtype': 'P',
                            'pt_trvalue': -gross_pounds,
                            'pt_vatval': 0,
                            'pt_trbal': -gross_pounds,
                            'pt_paid': ' ',
                            'pt_crdate': post_date,
                            'pt_advance': 'N',
                            'pt_payflag': 0,
                            'pt_set1day': 0,
                            'pt_set1': 0,
                            'pt_set2day': 0,
                            'pt_set2': 0,
                            'pt_held': ' ',
                            'pt_fcurr': '   ',
                            'pt_fcrate': 0,
                            'pt_fcdec': 0,
                            'pt_fcval': 0,
                            'pt_fcbal': 0,
                            'pt_adval': 0,
                            'pt_fadval': 0,
                            'pt_fcmult': 0,
                            'pt_cbtype': cbtype,
                            'pt_entry': entry_number[:12],
                            'pt_unique': atran_unique[:10],
                            'pt_suptype': info.get('type', '   ')[:3],
                            'pt_euro': 0,
                            'pt_nlpdate': post_date,
                        })

                        # NOTE: No palloc created at posting time -- allocation happens separately

                        self._update_supplier_balance(acct, -gross_pounds)
                        tables_updated.update(['ptran', 'pname'])

                    elif ae_type == 6:
                        # Purchase refund — money in from supplier, INCREASES creditors balance
                        info = account_info[acct]
                        ptran_table = self._open_table('ptran')
                        ptran_table.append({
                            'pt_account': acct[:8],
                            'pt_trdate': post_date,
                            'pt_trref': reference[:20],
                            'pt_supref': 'Direct Cr',
                            'pt_trtype': 'F',
                            'pt_trvalue': gross_pounds,
                            'pt_vatval': 0,
                            'pt_trbal': gross_pounds,
                            'pt_paid': ' ',
                            'pt_crdate': post_date,
                            'pt_advance': 'N',
                            'pt_payflag': 0,
                            'pt_set1day': 0,
                            'pt_set1': 0,
                            'pt_set2day': 0,
                            'pt_set2': 0,
                            'pt_held': ' ',
                            'pt_fcurr': '   ',
                            'pt_fcrate': 0,
                            'pt_fcdec': 0,
                            'pt_fcval': 0,
                            'pt_fcbal': 0,
                            'pt_adval': 0,
                            'pt_fadval': 0,
                            'pt_fcmult': 0,
                            'pt_cbtype': cbtype,
                            'pt_entry': entry_number[:12],
                            'pt_unique': atran_unique[:10],
                            'pt_suptype': info.get('type', '   ')[:3],
                            'pt_euro': 0,
                            'pt_nlpdate': post_date,
                        })

                        # NOTE: No palloc created at posting time -- allocation happens separately

                        self._update_supplier_balance(acct, gross_pounds)
                        tables_updated.update(['ptran', 'pname'])

                    # 2c. Determine target account (needed for both ntran and anoml)
                    if ae_type in (1, 2):
                        target_account = acct
                        nt_posttyp = 'S'
                    elif ae_type in (3, 4):
                        line_control = self._get_customer_control_account(acct)
                        target_account = line_control
                        nt_posttyp = 'S'
                    else:  # ae_type in (5, 6)
                        line_control = self._get_supplier_control_account(acct)
                        target_account = line_control
                        nt_posttyp = 'P'

                    # Compute ntran/anoml values
                    if is_receipt:
                        bank_ntran_value = gross_pounds
                        target_ntran_value = -net_pounds if ln['has_vat'] else -gross_pounds
                    else:
                        bank_ntran_value = -gross_pounds
                        target_ntran_value = net_pounds if ln['has_vat'] else gross_pounds

                    total_bank_ntran += bank_ntran_value

                    # ntran double-entry (+ optional VAT third entry)
                    if posting_decision.post_to_nominal:
                        journal_number = self._get_next_journal()
                        ntran_comment = safe_comment[:50]
                        ntran_table = self._open_table('ntran')
                        bank_type = self._get_nacnt_type(bank_account) or ('B ', 'BC')
                        target_type = self._get_nacnt_type(target_account) or ('B ', 'BB')
                        ntran_trnref = f"{acct_name[:30]:<30}{reference:<20}"

                        # Bank ntran
                        ntran_table.append({
                            'nt_acnt': bank_account[:8],
                            'nt_cntr': '    ',
                            'nt_type': bank_type[0],
                            'nt_subt': bank_type[1],
                            'nt_jrnl': journal_number,
                            'nt_ref': '',
                            'nt_inp': input_by[:10],
                            'nt_trtype': 'A',
                            'nt_cmnt': ntran_comment[:50],
                            'nt_trnref': ntran_trnref[:50],
                            'nt_entr': post_date,
                            'nt_value': bank_ntran_value,
                            'nt_year': year,
                            'nt_period': period,
                            'nt_rvrse': 0,
                            'nt_prevyr': 0,
                            'nt_consol': 0,
                            'nt_fcurr': '   ',
                            'nt_fvalue': 0,
                            'nt_fcrate': 0,
                            'nt_fcmult': 0,
                            'nt_fcdec': 0,
                            'nt_srcco': 'I',
                            'nt_cdesc': '',
                            'nt_project': '        ',
                            'nt_job': '        ',
                            'nt_posttyp': nt_posttyp,
                            'nt_pstgrp': 0,
                            'nt_pstid': ntran_pstid_bank[:10],
                            'nt_srcnlid': 0,
                            'nt_recurr': 0,
                            'nt_perpost': 0,
                            'nt_rectify': 0,
                            'nt_recjrnl': 0,
                            'nt_vatanal': 0,
                            'nt_distrib': 0,
                        })
                        self._update_nacnt_balance(bank_account, bank_ntran_value, period, year)

                        # Target ntran (nominal/control account)
                        ntran_table.append({
                            'nt_acnt': target_account[:8],
                            'nt_cntr': '    ',
                            'nt_type': target_type[0],
                            'nt_subt': target_type[1],
                            'nt_jrnl': journal_number,
                            'nt_ref': '',
                            'nt_inp': input_by[:10],
                            'nt_trtype': 'A',
                            'nt_cmnt': ntran_comment[:50],
                            'nt_trnref': ntran_trnref[:50],
                            'nt_entr': post_date,
                            'nt_value': target_ntran_value,
                            'nt_year': year,
                            'nt_period': period,
                            'nt_rvrse': 0,
                            'nt_prevyr': 0,
                            'nt_consol': 0,
                            'nt_fcurr': '   ',
                            'nt_fvalue': 0,
                            'nt_fcrate': 0,
                            'nt_fcmult': 0,
                            'nt_fcdec': 0,
                            'nt_srcco': 'I',
                            'nt_cdesc': '',
                            'nt_project': project_padded,
                            'nt_job': department_padded,
                            'nt_posttyp': nt_posttyp,
                            'nt_pstgrp': 0,
                            'nt_pstid': ntran_pstid_target[:10],
                            'nt_srcnlid': 0,
                            'nt_recurr': 0,
                            'nt_perpost': 0,
                            'nt_rectify': 0,
                            'nt_recjrnl': 0,
                            'nt_vatanal': 0,
                            'nt_distrib': 0,
                        })
                        self._update_nacnt_balance(target_account, target_ntran_value, period, year)

                        # VAT ntran (3rd entry if VAT present)
                        if ln['has_vat']:
                            vat_type_code = 'P' if ae_type in (1, 5, 6) else 'S'
                            vat_info = self.get_vat_rate(ln['vat_code'], vat_type_code, post_date)
                            vat_nominal = vat_info.get('nominal', '')
                            vat_rate = vat_info.get('rate', 20.0)
                            vat_acct_type = self._get_nacnt_type(vat_nominal) or ('B ', 'BB')

                            vat_ntran_value = vat_pounds if not is_receipt else -vat_pounds

                            ntran_table.append({
                                'nt_acnt': vat_nominal[:8],
                                'nt_cntr': '    ',
                                'nt_type': vat_acct_type[0],
                                'nt_subt': vat_acct_type[1],
                                'nt_jrnl': journal_number,
                                'nt_ref': '',
                                'nt_inp': input_by[:10],
                                'nt_trtype': 'A',
                                'nt_cmnt': f"{ntran_comment[:45]} VAT"[:50],
                                'nt_trnref': ntran_trnref[:50],
                                'nt_entr': post_date,
                                'nt_value': vat_ntran_value,
                                'nt_year': year,
                                'nt_period': period,
                                'nt_rvrse': 0,
                                'nt_prevyr': 0,
                                'nt_consol': 0,
                                'nt_fcurr': '   ',
                                'nt_fvalue': 0,
                                'nt_fcrate': 0,
                                'nt_fcmult': 0,
                                'nt_fcdec': 0,
                                'nt_srcco': 'I',
                                'nt_cdesc': '',
                                'nt_project': '        ',
                                'nt_job': '        ',
                                'nt_posttyp': 'N',
                                'nt_pstgrp': 0,
                                'nt_pstid': ntran_pstid_vat[:10],
                                'nt_srcnlid': 0,
                                'nt_recurr': 0,
                                'nt_perpost': 0,
                                'nt_rectify': 0,
                                'nt_recjrnl': 0,
                                'nt_vatanal': 0,
                                'nt_distrib': 0,
                            })
                            self._update_nacnt_balance(vat_nominal, vat_ntran_value, period, year)

                            # zvtran
                            try:
                                va_source = 'N' if ae_type in (1, 2) else ('S' if ae_type in (3, 4) else 'P')
                                zvtran_table = self._open_table('zvtran')
                                zvtran_table.append({
                                    'va_source': va_source,
                                    'va_account': (target_account if va_source == 'N' else acct)[:8],
                                    'va_laccnt': target_account[:8],
                                    'va_trdate': post_date,
                                    'va_taxdate': post_date,
                                    'va_ovrdate': post_date,
                                    'va_trref': reference[:20],
                                    'va_trtype': 'I',
                                    'va_country': 'GB',
                                    'va_fcurr': '   ',
                                    'va_trvalue': net_pounds,
                                    'va_fcval': 0,
                                    'va_vatval': vat_pounds,
                                    'va_cost': 0,
                                    'va_vatctry': 'H',
                                    'va_vattype': vat_type_code,
                                    'va_anvat': ln['vat_code'][:3],
                                    'va_vatrate': vat_rate,
                                    'va_box1': 1 if vat_type_code == 'S' else 0,
                                    'va_box2': 0,
                                    'va_box4': 1 if vat_type_code == 'P' else 0,
                                    'va_box6': 1 if vat_type_code == 'S' else 0,
                                    'va_box7': 1 if vat_type_code == 'P' else 0,
                                    'va_box8': 0,
                                    'va_box9': 0,
                                    'va_done': 0,
                                    'va_import': 0,
                                    'va_export': 0,
                                })
                            except Exception as zvt_err:
                                logger.warning(f"Failed to create zvtran for recurring VAT: {zvt_err}")

                            # nvat
                            try:
                                nvat_table = self._open_table('nvat')
                                nvat_table.append({
                                    'nv_acnt': vat_nominal[:8],
                                    'nv_cntr': '',
                                    'nv_date': post_date,
                                    'nv_crdate': post_date,
                                    'nv_taxdate': post_date,
                                    'nv_ref': reference[:20],
                                    'nv_type': vat_type_code,
                                    'nv_advance': 0,
                                    'nv_value': net_pounds,
                                    'nv_vatval': vat_pounds,
                                    'nv_vatctry': ' ',
                                    'nv_vattype': vat_type_code,
                                    'nv_vatcode': ln['vat_code'][:3],
                                    'nv_vatrate': vat_rate,
                                    'nv_comment': f"Recurring entry {entry_ref}"[:50],
                                })
                            except Exception as nvat_err:
                                logger.warning(f"Failed to create nvat for recurring VAT: {nvat_err}")

                            tables_updated.update(['zvtran', 'nvat'])

                        tables_updated.update(['ntran', 'nacnt'])

                    # 2d. anoml transfer file records
                    if posting_decision.post_to_transfer_file:
                        done_flag = posting_decision.transfer_file_done_flag
                        jrnl_num = journal_number if posting_decision.post_to_nominal else 0
                        ax_source = 'A' if ae_type in (1, 2) else ('S' if ae_type in (3, 4) else 'P')

                        try:
                            anoml_table = self._open_table('anoml')

                            # Bank side anoml
                            anoml_table.append({
                                'ax_nacnt': bank_account[:10],
                                'ax_ncntr': '    ',
                                'ax_source': ax_source,
                                'ax_date': post_date,
                                'ax_value': bank_ntran_value,
                                'ax_tref': reference[:20],
                                'ax_comment': safe_comment[:50],
                                'ax_done': done_flag,
                                'ax_fcurr': '   ',
                                'ax_fvalue': 0,
                                'ax_fcrate': 0,
                                'ax_fcmult': 0,
                                'ax_fcdec': 0,
                                'ax_srcco': 'I',
                                'ax_unique': atran_unique[:10],
                                'ax_project': '        ',
                                'ax_job': '        ',
                                'ax_jrnl': jrnl_num,
                                'ax_nlpdate': post_date,
                            })

                            # Target side anoml — always use control/nominal account
                            anoml_table.append({
                                'ax_nacnt': target_account[:10],
                                'ax_ncntr': '    ',
                                'ax_source': ax_source,
                                'ax_date': post_date,
                                'ax_value': target_ntran_value,
                                'ax_tref': reference[:20],
                                'ax_comment': safe_comment[:50],
                                'ax_done': done_flag,
                                'ax_fcurr': '   ',
                                'ax_fvalue': 0,
                                'ax_fcrate': 0,
                                'ax_fcmult': 0,
                                'ax_fcdec': 0,
                                'ax_srcco': 'I',
                                'ax_unique': atran_unique[:10],
                                'ax_project': project_padded,
                                'ax_job': department_padded,
                                'ax_jrnl': jrnl_num,
                                'ax_nlpdate': post_date,
                            })

                            # VAT anoml if applicable
                            if ln['has_vat'] and posting_decision.post_to_nominal:
                                anoml_table.append({
                                    'ax_nacnt': vat_nominal[:10],
                                    'ax_ncntr': '    ',
                                    'ax_source': ax_source,
                                    'ax_date': post_date,
                                    'ax_value': vat_ntran_value,
                                    'ax_tref': reference[:20],
                                    'ax_comment': f"{ntran_comment[:45]} VAT"[:50],
                                    'ax_done': done_flag,
                                    'ax_fcurr': '   ',
                                    'ax_fvalue': 0,
                                    'ax_fcrate': 0,
                                    'ax_fcmult': 0,
                                    'ax_fcdec': 0,
                                    'ax_srcco': 'I',
                                    'ax_unique': ntran_pstid_vat[:10],
                                    'ax_project': '        ',
                                    'ax_job': '        ',
                                    'ax_jrnl': jrnl_num,
                                    'ax_nlpdate': post_date,
                                })

                            tables_updated.add('anoml')
                        except FileNotFoundError:
                            logger.warning("anoml table not found - skipping transfer file")

                # 3. Update nbank (total bank movement) - ALWAYS when atran created
                if total_bank_ntran != 0:
                    self._update_nbank_balance(bank_account, total_bank_ntran)
                    tables_updated.add('nbank')

                # 4. Advance recurring entry schedule (atomic within lock)
                self._advance_recurring_entry_in_lock(entry_ref, bank_account, post_date, ae_freq, ae_every, input_by)
                tables_updated.add('arhead')

            # Post-commit ledger verification — ensures stran/ptran were created
            if ae_type in (3, 4):
                self.verify_ledger_after_import('stran', cbtype, entry_number, len(parsed_lines))
            elif ae_type in (5, 6):
                self.verify_ledger_after_import('ptran', cbtype, entry_number, len(parsed_lines))

            type_name = TYPE_NAMES.get(ae_type, f'Type {ae_type}')
            total_pounds = round(total_pence / 100.0, 2)
            return Opera3ImportResult(
                success=True,
                records_processed=1,
                records_imported=1,
                entry_number=entry_number,
                warnings=[
                    f"Posted recurring {type_name}: {entry_ref} → entry {entry_number}",
                    f"Amount: £{total_pounds:.2f} ({len(parsed_lines)} line{'s' if len(parsed_lines) > 1 else ''})",
                    f"Tables updated: {', '.join(sorted(tables_updated))}"
                ]
            )

        except Exception as e:
            logger.error(f"Failed to post recurring entry {entry_ref}: {e}")
            import traceback
            traceback.print_exc()
            return Opera3ImportResult(
                success=False, records_processed=1, records_failed=1,
                errors=[str(e)]
            )
        finally:
            self._close_all_tables()

    def _advance_recurring_entry_in_lock(
        self,
        entry_ref: str,
        bank_account: str,
        posting_date: date,
        freq: str,
        every: int,
        input_by: str = "RECUR"
    ) -> None:
        """
        Advance recurring entry schedule within an existing transaction lock.
        Called from post_recurring_entry to ensure atomicity — if this fails,
        the entire posting is within the same lock so FoxPro records
        appended in the same lock context remain but schedule is not advanced.
        """
        from dateutil.relativedelta import relativedelta

        table = self._open_table('arhead')
        bank_upper = bank_account.strip().upper()
        entry_upper = entry_ref.strip().upper()

        for record in table:
            rec_entry = record.ae_entry.strip().upper()
            rec_acnt = record.ae_acnt.strip().upper()
            if rec_entry == entry_upper and rec_acnt == bank_upper:
                current_nxtpost = record.ae_nxtpost
                if hasattr(current_nxtpost, 'date'):
                    current_nxtpost = current_nxtpost.date()
                elif isinstance(current_nxtpost, str):
                    current_nxtpost = date.fromisoformat(current_nxtpost[:10])

                freq_upper = freq.upper().strip()
                if freq_upper == 'D':
                    next_date = current_nxtpost + timedelta(days=every)
                elif freq_upper == 'W':
                    next_date = current_nxtpost + timedelta(weeks=every)
                elif freq_upper == 'M':
                    next_date = current_nxtpost + relativedelta(months=every)
                elif freq_upper == 'Q':
                    next_date = current_nxtpost + relativedelta(months=3 * every)
                elif freq_upper == 'Y':
                    next_date = current_nxtpost + relativedelta(years=every)
                else:
                    next_date = current_nxtpost + relativedelta(months=every)
                    logger.warning(f"Unknown frequency '{freq}' for {entry_ref}, defaulting to monthly")

                with record:
                    record.ae_posted = int(record.ae_posted or 0) + 1
                    record.ae_lstpost = posting_date
                    record.ae_nxtpost = next_date

                logger.info(f"Advanced recurring entry {entry_ref}: posted={posting_date}, next={next_date}")
                break

    def _advance_recurring_entry(
        self,
        entry_ref: str,
        bank_account: str,
        posting_date: date,
        freq: str,
        every: int,
        input_by: str = "RECUR"
    ) -> None:
        """
        Advance a recurring entry schedule (standalone version).
        Opens arhead independently. For use outside of post_recurring_entry.
        """
        from dateutil.relativedelta import relativedelta

        table = self._open_table('arhead')
        bank_upper = bank_account.strip().upper()
        entry_upper = entry_ref.strip().upper()

        for record in table:
            rec_entry = record.ae_entry.strip().upper()
            rec_acnt = record.ae_acnt.strip().upper()
            if rec_entry == entry_upper and rec_acnt == bank_upper:
                current_nxtpost = record.ae_nxtpost
                if hasattr(current_nxtpost, 'date'):
                    current_nxtpost = current_nxtpost.date()
                elif isinstance(current_nxtpost, str):
                    current_nxtpost = date.fromisoformat(current_nxtpost[:10])

                freq_upper = freq.upper().strip()
                if freq_upper == 'D':
                    next_date = current_nxtpost + timedelta(days=every)
                elif freq_upper == 'W':
                    next_date = current_nxtpost + timedelta(weeks=every)
                elif freq_upper == 'M':
                    next_date = current_nxtpost + relativedelta(months=every)
                elif freq_upper == 'Q':
                    next_date = current_nxtpost + relativedelta(months=3 * every)
                elif freq_upper == 'Y':
                    next_date = current_nxtpost + relativedelta(years=every)
                else:
                    next_date = current_nxtpost + relativedelta(months=every)
                    logger.warning(f"Unknown frequency '{freq}' for {entry_ref}, defaulting to monthly")

                with record:
                    record.ae_posted = int(record.ae_posted or 0) + 1
                    record.ae_lstpost = posting_date
                    record.ae_nxtpost = next_date

                logger.info(f"Advanced recurring entry {entry_ref}: posted={posting_date}, next={next_date}")
                break

    def verify_ledger_after_import(
        self, table_name: str, cbtype: str, entry_number: str,
        expected_count: int, account: str = None
    ):
        """
        Post-commit verification that stran/ptran records were created.

        Reads from the open DBF table and logs CRITICAL if records are missing.
        This is a defensive safety net — if records are missing after a
        supposedly successful import, something is seriously wrong.

        Args:
            table_name: 'stran' or 'ptran'
            cbtype: Cashbook type code (e.g. 'R1', 'P1')
            entry_number: Entry number from aentry/atype
            expected_count: How many records should exist
            account: Optional account filter (for single-record checks)
        """
        try:
            col_map = {
                'stran': ('st_cbtype', 'st_entry', 'st_account'),
                'ptran': ('pt_cbtype', 'pt_entry', 'pt_account'),
            }
            if table_name not in col_map:
                return
            type_col, entry_col, acct_col = col_map[table_name]

            table = self._open_table(table_name)
            entry_str = str(entry_number).strip()
            cbtype_str = cbtype.strip()
            count = 0

            for record in table:
                rec_cbtype = str(getattr(record, type_col, '')).strip()
                rec_entry = str(getattr(record, entry_col, '')).strip()
                if rec_cbtype == cbtype_str and rec_entry == entry_str:
                    if account and expected_count == 1:
                        rec_acct = str(getattr(record, acct_col, '')).strip()
                        if rec_acct != account.strip():
                            continue
                    count += 1

            if count < expected_count:
                logger.critical(
                    f"POST-COMMIT VERIFICATION FAILED: {table_name} expected {expected_count} "
                    f"record(s) for cbtype={cbtype}/entry={entry_number}, found {count}. "
                    f"Data integrity issue — cashbook posted but {table_name} missing."
                )
            else:
                logger.debug(
                    f"Ledger verification OK: {table_name} has {count} record(s) "
                    f"for cbtype={cbtype}/entry={entry_number}"
                )
        except Exception as e:
            # Never break the import workflow for a verification failure
            logger.warning(f"Ledger verification query failed for {table_name}: {e}")
