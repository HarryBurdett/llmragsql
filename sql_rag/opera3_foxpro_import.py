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

            # Sort by category then code
            types.sort(key=lambda x: (x['category'], x['code']))
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
        Increment the ay_entry counter for a type code and return the current value.

        Args:
            cbtype: Type code (e.g., 'P1', 'R2', 'P5')

        Returns:
            Entry number to use for this transaction
        """
        try:
            table = self._open_table('atype')

            for record in table:
                code = record.ay_cbtype.strip() if hasattr(record, 'ay_cbtype') else ''
                if code == cbtype:
                    current_entry = record.ay_entry.strip() if hasattr(record, 'ay_entry') else f"{cbtype}{0:08d}"

                    # Parse and increment
                    prefix_len = len(cbtype)
                    try:
                        current_num = int(current_entry[prefix_len:])
                    except ValueError:
                        current_num = 0

                    next_num = current_num + 1
                    next_entry = f"{cbtype}{next_num:08d}"

                    # Update the record
                    with record:
                        record.ay_entry = next_entry

                    logger.debug(f"Incremented atype entry for {cbtype}: {current_entry} -> {next_entry}")

                    # Return the entry number to USE (the one before increment)
                    return current_entry

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

    def _get_supplier_control_account(self, supplier_account: str) -> str:
        """Get creditors control account for a supplier"""
        default_control = 'CA030'

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
        default_control = 'BB020'

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

    def _update_nacnt_balance(self, account: str, value: float, period: int):
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
            self._update_nhist(account, value, period)
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
            posting_decision = get_period_posting_decision(config, post_date)

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
            amount_pence = int(amount_pounds * 100)

            if isinstance(post_date, str):
                post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

            year = post_date.year
            period = post_date.month

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
            tables_to_lock = ['aentry', 'atran', 'ptran', 'palloc', 'pname']
            if posting_decision.post_to_nominal:
                tables_to_lock.extend(['ntran', 'nacnt', 'nbank'])  # Include balance tables
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
                    'ae_complet': 1,
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
                    self._update_nacnt_balance(bank_account, -amount_pounds, period)
                    self._update_nacnt_balance(creditors_control, amount_pounds, period)
                    # Update nbank balance (payment decreases bank balance)
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

                # 6. INSERT INTO palloc
                palloc_table = self._open_table('palloc')
                palloc_table.append({
                    'al_account': supplier_account[:8],
                    'al_date': post_date,
                    'al_ref1': reference[:20],
                    'al_ref2': payment_type[:20],
                    'al_type': 'P',
                    'al_val': -amount_pounds,
                    'al_dval': 0,
                    'al_origval': -amount_pounds,
                    'al_payind': 'P',
                    'al_payflag': 0,
                    'al_payday': post_date,
                    'al_ctype': 'O',
                    'al_rem': ' ',
                    'al_cheq': ' ',
                    'al_payee': supplier_name[:30],
                    'al_fcurr': '   ',
                    'al_fval': 0,
                    'al_fdval': 0,
                    'al_forigvl': 0,
                    'al_fdec': 0,
                    'al_unique': 0,  # Will need ptran ID lookup
                    'al_acnt': bank_account[:8],
                    'al_cntr': '    ',
                    'al_advind': 0,
                    'al_advtran': 0,
                    'al_preprd': 0,
                    'al_bacsid': 0,
                    'al_adjsv': 0,
                })

                # 7. Update supplier balance
                self._update_supplier_balance(supplier_account, -amount_pounds)

                # Build list of tables updated based on what was actually done
                tables_updated = ["aentry", "atran", "ptran", "palloc", "pname"]
                if posting_decision.post_to_nominal:
                    tables_updated.insert(2, "ntran (2)")
                if posting_decision.post_to_transfer_file:
                    tables_updated.append("anoml (2)")  # Opera uses anoml for both bank and control

                posting_mode = "Current period - posted to nominal" if posting_decision.post_to_nominal else "Different period - transfer file only (pending NL post)"

                logger.info(f"Successfully imported purchase payment: {entry_number} for £{amount_pounds:.2f} - {posting_mode}")

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
            posting_decision = get_period_posting_decision(config, post_date)

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
            amount_pence = int(amount_pounds * 100)

            if isinstance(post_date, str):
                post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

            year = post_date.year
            period = post_date.month

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
            tables_to_lock = ['aentry', 'atran', 'stran', 'salloc', 'sname', 'atype']
            if posting_decision.post_to_nominal:
                tables_to_lock.extend(['ntran', 'nacnt', 'nbank'])  # Include balance tables
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
                'ae_complet': 1,
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
                    self._update_nacnt_balance(bank_account, amount_pounds, period)
                    self._update_nacnt_balance(debtors_control, -amount_pounds, period)
                    # Update nbank balance (receipt increases bank balance)
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
                })

                # 6. INSERT INTO salloc
                salloc_table = self._open_table('salloc')
                salloc_table.append({
                    'al_account': customer_account[:8],
                    'al_date': post_date,
                    'al_ref1': reference[:20],
                    'al_ref2': receipt_type[:20],
                    'al_type': 'R',
                    'al_val': -amount_pounds,
                    'al_dval': 0,
                    'al_origval': -amount_pounds,
                    'al_payind': 'R',
                    'al_payflag': 0,
                    'al_payday': post_date,
                    'al_ctype': 'O',
                    'al_rem': ' ',
                    'al_cheq': ' ',
                    'al_payee': customer_name[:30],
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

                # 7. Update customer balance
                self._update_customer_balance(customer_account, -amount_pounds)

                # Build list of tables updated based on what was actually done
                tables_updated = ["aentry", "atran", "stran", "salloc", "sname"]
                if posting_decision.post_to_nominal:
                    tables_updated.insert(2, "ntran (2)")
                if posting_decision.post_to_transfer_file:
                    tables_updated.append("anoml (2)")  # Opera uses anoml for both bank and control

                posting_mode = "Current period - posted to nominal" if posting_decision.post_to_nominal else "Different period - transfer file only (pending NL post)"

                logger.info(f"Successfully imported sales receipt: {entry_number} for £{amount_pounds:.2f} - {posting_mode}")

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
            amount_pence = int(amount_pounds * 100)
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
        auto_allocate: bool = False
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
            posting_decision = get_period_posting_decision(config, post_date)

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
            gross_pence = int(gross_total * 100)

            if isinstance(post_date, str):
                post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

            year = post_date.year
            period = post_date.month
            now = datetime.now()

            # Tables to lock - always include nominal tables for complete posting
            tables_to_lock = ['aentry', 'atran', 'stran', 'salloc', 'sname', 'atype']
            if posting_decision.post_to_nominal:
                tables_to_lock.extend(['ntran', 'nacnt', 'nbank'])
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
                    'ae_complet': 1 if posting_decision.post_to_nominal else 0,
                    'ae_postgrp': 0,
                    'sq_crdate': now.date(),
                    'sq_crtime': now.strftime('%H:%M:%S')[:8],
                    'sq_cruser': input_by[:8],
                    'ae_comment': 'GoCardless batch import',
                })

                # Process each payment
                atran_table = self._open_table('atran')
                stran_table = self._open_table('stran')
                salloc_table = self._open_table('salloc')

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
                    stran_unique = OperaUniqueIdGenerator.generate()
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
                    })

                    # Create salloc record (self-allocation)
                    salloc_table.append({
                        'al_account': customer_account[:8],
                        'al_date': post_date,
                        'al_ref1': reference[:20],
                        'al_ref2': 'GoCardless',
                        'al_type': 'R',
                        'al_val': -amount_pounds,
                        'al_dval': 0,
                        'al_origval': -amount_pounds,
                        'al_payind': 'R',
                        'al_payflag': 0,
                        'al_payday': post_date,
                        'al_ctype': 'O',
                        'al_rem': ' ',
                        'al_cheq': ' ',
                        'al_payee': customer_name[:30],
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
                        self._update_nacnt_balance(bank_account, amount_pounds, period)
                        self._update_nacnt_balance(debtors_control, -amount_pounds, period)

                        # Update nbank balance (receipt increases bank)
                        self._update_nbank_balance(bank_account, amount_pounds)

                        journal_number += 1

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
                        self._update_nacnt_balance(fees_nominal_account, net_fees, period)

                        # DR VAT Input if VAT > 0
                        if vat_on_fees > 0:
                            vat_nominal = 'BB040'  # Default VAT input account
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
                            self._update_nacnt_balance(vat_nominal, abs(vat_on_fees), period)

                            # Create zvtran for VAT return tracking
                            try:
                                vat_rate = (abs(vat_on_fees) / net_fees * 100) if net_fees > 0 else 20.0
                                zvtran_table = self._open_table('zvtran')
                                zvtran_table.append({
                                    'va_source': 'N',
                                    'va_account': 'GOCARDLS',
                                    'va_laccnt': fees_nominal_account[:8],
                                    'va_trdate': post_date,
                                    'va_taxdate': post_date,
                                    'va_ovrdate': post_date,
                                    'va_trref': reference[:20],
                                    'va_trtype': 'B',
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
                                    'nv_vatcode': 'S',
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
                        self._update_nacnt_balance(bank_account, -gross_fees, period)
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
                                    'ax_nacnt': 'BB040',
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

                tables_updated = ["aentry", "atran", "stran", "salloc", "sname"]
                if posting_decision.post_to_nominal:
                    tables_updated.extend(["ntran", "nacnt", "nbank"])
                if posting_decision.post_to_transfer_file:
                    tables_updated.append("anoml")
                warnings.append(f"Tables updated: {', '.join(tables_updated)}")

                logger.info(f"Successfully imported GoCardless batch: {entry_number} with {len(payments)} payments totalling £{gross_total:.2f} - {posting_mode}")

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
        department_code: str = ""
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

            posting_decision = get_period_posting_decision(config, post_date)

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
                    if not project_code and na_allwprj > 0:
                        default_proj = (nominal_row.get('NA_PROJECT', nominal_row.get('na_project', '')) or '').strip()
                        if default_proj:
                            project_code = default_proj
                    if not department_code and na_allwjob > 0:
                        default_job = (nominal_row.get('NA_JOB', nominal_row.get('na_job', '')) or '').strip()
                        if default_job:
                            department_code = default_job

                    # Mandatory checks
                    if na_allwprj == 2 and not project_code:
                        errors.append(f"Project code is mandatory for nominal account '{nominal_account}'")
                    if na_allwjob == 2 and not department_code:
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
            amount_pence = int(amount_pounds * 100)
            entry_value = amount_pence if is_receipt else -amount_pence

            if isinstance(post_date, str):
                post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

            year = post_date.year
            period = post_date.month
            now = datetime.now()

            # Pad project/department codes to 8 chars
            project_padded = f"{(project_code or '')[:8]:<8}"
            department_padded = f"{(department_code or '')[:8]:<8}"

            ntran_comment = f"{description[:50]:<50}" if description else f"{reference[:50]:<50}"
            ntran_trnref = f"{nominal_name[:30]:<30}{reference[:20]:<20}"

            # Generate unique IDs
            unique_ids = OperaUniqueIdGenerator.generate_multiple(3)
            atran_unique = unique_ids[0]
            ntran_pstid_bank = unique_ids[1]
            ntran_pstid_nominal = unique_ids[2]

            # Double-entry values (pounds for ntran/anoml)
            if is_receipt:
                bank_ntran_value = amount_pounds   # Debit bank
                nominal_ntran_value = -amount_pounds  # Credit nominal
            else:
                bank_ntran_value = -amount_pounds  # Credit bank
                nominal_ntran_value = amount_pounds   # Debit nominal

            # =====================
            # ACQUIRE LOCKS AND EXECUTE
            # =====================
            tables_to_lock = ['aentry', 'atran', 'atype']
            if posting_decision.post_to_nominal:
                tables_to_lock.extend(['ntran', 'nacnt', 'nbank'])
            if posting_decision.post_to_transfer_file:
                tables_to_lock.append('anoml')

            with self._transaction_lock(tables_to_lock):
                entry_number = self.increment_atype_entry(cbtype)
                journal_number = self._get_next_journal()

                logger.info(f"NOMINAL_ENTRY_DEBUG: Opera 3 import - bank={bank_account}, nominal={nominal_account}, "
                           f"amount={amount_pounds}, is_receipt={is_receipt}, project='{project_code}', department='{department_code}'")

                # 1. INSERT INTO aentry
                ae_complet_flag = 1 if posting_decision.post_to_nominal else 0
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

                    # Bank side ntran (blank project/department)
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

                    # Nominal side ntran (with project/department)
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

                    # Update balances
                    self._update_nacnt_balance(bank_account, bank_ntran_value, period)
                    self._update_nacnt_balance(nominal_account, nominal_ntran_value, period)
                    self._update_nbank_balance(bank_account, bank_ntran_value)

                # 4. Transfer file records (anoml)
                if posting_decision.post_to_transfer_file:
                    done_flag = posting_decision.transfer_file_done_flag
                    jrnl_num = journal_number if posting_decision.post_to_nominal else 0

                    try:
                        anoml_table = self._open_table('anoml')

                        # Bank side anoml (blank project/department)
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

                        # Nominal side anoml (with project/department)
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
                    except FileNotFoundError:
                        logger.warning("anoml table not found - skipping transfer file")

                tables_updated = ["aentry", "atran"]
                if posting_decision.post_to_nominal:
                    tables_updated.append("ntran (2)")
                if posting_decision.post_to_transfer_file:
                    tables_updated.append("anoml (2)")

                entry_type = "Nominal Receipt" if is_receipt else "Nominal Payment"
                return Opera3ImportResult(
                    success=True,
                    records_processed=1,
                    records_imported=1,
                    entry_number=entry_number,
                    warnings=[
                        f"Created {entry_type} {entry_number}",
                        f"Amount: £{amount_pounds:.2f}",
                        f"Bank: {bank_account}, Nominal: {nominal_account}",
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

            # Clear any existing incomplete reconciliation (ae_tmpstat)
            # for this bank — ensures clean state
            cleared = 0
            with aentry_table:
                for i, record in enumerate(aentry_table):
                    ae_acnt = str(getattr(record, 'ae_acnt', '') or getattr(record, 'AE_ACNT', '')).strip().upper()
                    ae_tmpstat = int(getattr(record, 'ae_tmpstat', 0) or getattr(record, 'AE_TMPSTAT', 0) or 0)
                    if ae_acnt == bank_account.upper() and ae_tmpstat != 0:
                        aentry_table.goto(i)
                        aentry_table.write(aentry_table.current_record, {'ae_tmpstat': 0})
                        cleared += 1
            if cleared > 0:
                logger.info(f"Cleared {cleared} existing ae_tmpstat entries for {bank_account}")

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

            # Calculate new reconciled balance
            new_rec_balance = current_rec_balance + total_value

            # Sort entries by statement line for correct running balance
            sorted_entries = sorted(entries, key=lambda e: e.get('statement_line', 0))

            # Update each aentry record with running balance
            running_balance = current_rec_balance
            updated_count = 0

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
                    nbank_table.write(nbank_table.current_record, {
                        'nk_lstrecl': new_rec_line,
                        'nk_lststno': statement_number,
                        'nk_lststdt': statement_date,
                        'nk_reclnum': new_rec_line,
                        'nk_recldte': reconciliation_date,
                        'nk_recstfr': statement_number,
                        'nk_recstto': statement_number,
                        'nk_recstdt': statement_date
                    })
                    logger.info(f"Partial reconciliation — nk_recbal NOT updated (remains at {current_rec_balance/100:.2f})")
                else:
                    # Full: update everything including nk_recbal
                    nbank_table.write(nbank_table.current_record, {
                        'nk_recbal': int(new_rec_balance),
                        'nk_lstrecl': new_rec_line,
                        'nk_lststno': statement_number,
                        'nk_lststdt': statement_date,
                        'nk_reclnum': new_rec_line,
                        'nk_recldte': reconciliation_date,
                        'nk_recstfr': statement_number,
                        'nk_recstto': statement_number,
                        'nk_recstdt': statement_date
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
                return {'rate': 0.0, 'nominal': 'CA060', 'description': 'Unknown', 'found': False}

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
                return {'rate': 0.0, 'nominal': 'CA060', 'description': 'Unknown', 'found': False}

            # Get rate (tx_rate1 / tx_rate2 date logic)
            rate1 = float(match.get('TX_RATE1', match.get('tx_rate1', 0)) or 0)
            rate2 = match.get('TX_RATE2', match.get('tx_rate2'))
            rate2_date = match.get('TX_RATE2DY', match.get('tx_rate2dy'))
            nominal = str(match.get('TX_NOMINAL', match.get('tx_nominal', 'CA060'))).strip()
            desc = str(match.get('TX_DESC', match.get('tx_desc', ''))).strip()

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

            return {'rate': rate, 'nominal': nominal or 'CA060', 'description': desc, 'found': True}

        except Exception as e:
            logger.warning(f"Failed to look up VAT rate for code {vat_code}: {e}")
            return {'rate': 0.0, 'nominal': 'BB040' if vat_type == 'P' else 'CA060', 'description': 'Unknown', 'found': False}

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

        TYPE_NAMES = {1: 'Nominal Payment', 2: 'Nominal Receipt', 4: 'Sales Receipt', 5: 'Purchase Payment'}

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

            if ae_type not in (1, 2, 4, 5):
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
            is_receipt = ae_type in (2, 4)
            total_pence = sum(abs(ln['value_pence']) for ln in parsed_lines)
            total_entry_value = total_pence if is_receipt else -total_pence

            year = post_date.year
            period = post_date.month
            now = datetime.now()

            # Period posting decision
            config = Opera3Config(str(self.data_path), self.encoding)
            period_result = config.validate_posting_period(post_date, ledger_type='NL')
            if not period_result.is_valid:
                return Opera3ImportResult(
                    success=False, records_processed=1, records_failed=1,
                    errors=[period_result.error_message]
                )
            posting_decision = get_period_posting_decision(config, post_date)

            # Pre-fetch customer/supplier info for sales/purchase types
            account_info = {}
            if ae_type == 4:
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
            elif ae_type == 5:
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
            tables_to_lock = ['aentry', 'atran', 'atype', 'arhead']
            if posting_decision.post_to_nominal:
                tables_to_lock.extend(['ntran', 'nacnt', 'nbank'])
            if posting_decision.post_to_transfer_file:
                tables_to_lock.append('anoml')
            if ae_type == 4:
                tables_to_lock.extend(['stran', 'salloc', 'sname'])
            elif ae_type == 5:
                tables_to_lock.extend(['ptran', 'palloc', 'pname'])

            # Check if any line has VAT — need zvtran/nvat tables
            any_vat = any(ln['has_vat'] for ln in parsed_lines)
            if any_vat:
                tables_to_lock.extend(['zvtran', 'nvat'])

            tables_updated = set()

            with self._transaction_lock(tables_to_lock):
                entry_number = self.increment_atype_entry(cbtype)
                tables_updated.add('atype')

                safe_desc = ae_desc[:40] if ae_desc else ''
                ae_complet_flag = 1 if posting_decision.post_to_nominal else 0

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
                    unique_ids = OperaUniqueIdGenerator.generate_multiple(5)
                    atran_unique = unique_ids[0]
                    ntran_pstid_bank = unique_ids[1]
                    ntran_pstid_target = unique_ids[2]
                    ntran_pstid_vat = unique_ids[3]
                    ledger_unique = unique_ids[4]

                    # atran value: positive for receipts, negative for payments
                    atran_value_pence = gross_pence if is_receipt else -gross_pence

                    # Determine at_type for atran
                    if ae_type == 1:
                        at_type_code = CashbookTransactionType.NOMINAL_PAYMENT
                    elif ae_type == 2:
                        at_type_code = CashbookTransactionType.NOMINAL_RECEIPT
                    elif ae_type == 4:
                        at_type_code = CashbookTransactionType.SALES_RECEIPT
                    else:  # ae_type == 5
                        at_type_code = CashbookTransactionType.PURCHASE_PAYMENT

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
                    if ae_type == 4:
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
                        })

                        salloc_table = self._open_table('salloc')
                        salloc_table.append({
                            'al_account': acct[:8],
                            'al_date': post_date,
                            'al_ref1': reference[:20],
                            'al_ref2': 'BACS',
                            'al_type': 'R',
                            'al_val': -gross_pounds,
                            'al_dval': 0,
                            'al_origval': -gross_pounds,
                            'al_payind': 'R',
                            'al_payflag': 0,
                            'al_payday': post_date,
                            'al_ctype': 'O',
                            'al_rem': ' ',
                            'al_cheq': ' ',
                            'al_payee': acct_name[:30],
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

                        self._update_customer_balance(acct, -gross_pounds)
                        tables_updated.update(['stran', 'salloc', 'sname'])

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

                        palloc_table = self._open_table('palloc')
                        palloc_table.append({
                            'al_account': acct[:8],
                            'al_date': post_date,
                            'al_ref1': reference[:20],
                            'al_ref2': 'Direct Cr',
                            'al_type': 'P',
                            'al_val': -gross_pounds,
                            'al_dval': 0,
                            'al_origval': -gross_pounds,
                            'al_payind': 'P',
                            'al_payflag': 0,
                            'al_payday': post_date,
                            'al_ctype': 'O',
                            'al_rem': ' ',
                            'al_cheq': ' ',
                            'al_payee': acct_name[:30],
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

                        self._update_supplier_balance(acct, -gross_pounds)
                        tables_updated.update(['ptran', 'palloc', 'pname'])

                    # 2c. ntran double-entry (+ optional VAT third entry)
                    if posting_decision.post_to_nominal:
                        journal_number = self._get_next_journal()
                        ntran_comment = safe_comment[:50]
                        ntran_table = self._open_table('ntran')
                        bank_type = self._get_nacnt_type(bank_account) or ('B ', 'BC')

                        # Determine target account and nt_posttyp
                        if ae_type in (1, 2):
                            target_account = acct
                            target_type = self._get_nacnt_type(acct) or ('B ', 'BB')
                            ntran_trnref = f"{acct_name[:30]:<30}{reference:<20}"
                            nt_posttyp = 'S'
                        elif ae_type == 4:
                            line_control = self._get_customer_control_account(acct)
                            target_account = line_control
                            target_type = self._get_nacnt_type(line_control) or ('B ', 'BB')
                            ntran_trnref = f"{acct_name[:30]:<30}{reference:<20}"
                            nt_posttyp = 'R'
                        else:  # ae_type == 5
                            line_control = self._get_supplier_control_account(acct)
                            target_account = line_control
                            target_type = self._get_nacnt_type(line_control) or ('B ', 'BB')
                            ntran_trnref = f"{acct_name[:30]:<30}{reference:<20}"
                            nt_posttyp = 'P'

                        # Bank side value (always gross)
                        if is_receipt:
                            bank_ntran_value = gross_pounds
                            target_ntran_value = -net_pounds if ln['has_vat'] else -gross_pounds
                        else:
                            bank_ntran_value = -gross_pounds
                            target_ntran_value = net_pounds if ln['has_vat'] else gross_pounds

                        total_bank_ntran += bank_ntran_value

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
                        self._update_nacnt_balance(bank_account, bank_ntran_value, period)

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
                        self._update_nacnt_balance(target_account, target_ntran_value, period)

                        # VAT ntran (3rd entry if VAT present)
                        if ln['has_vat']:
                            vat_type_code = 'P' if ae_type in (1, 5) else 'S'
                            vat_info = self.get_vat_rate(ln['vat_code'], vat_type_code, post_date)
                            vat_nominal = vat_info.get('nominal', 'BB040' if vat_type_code == 'P' else 'CA060')
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
                            self._update_nacnt_balance(vat_nominal, vat_ntran_value, period)

                            # zvtran
                            try:
                                va_source = 'N' if ae_type in (1, 2) else ('S' if ae_type == 4 else 'P')
                                zvtran_table = self._open_table('zvtran')
                                zvtran_table.append({
                                    'va_source': va_source,
                                    'va_account': acct[:8],
                                    'va_laccnt': target_account[:8],
                                    'va_trdate': post_date,
                                    'va_taxdate': post_date,
                                    'va_ovrdate': post_date,
                                    'va_trref': reference[:20],
                                    'va_trtype': 'B',
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
                        ax_source = 'A' if ae_type in (1, 2) else ('S' if ae_type == 4 else 'P')

                        try:
                            anoml_table = self._open_table('anoml')

                            # Bank side anoml
                            anoml_table.append({
                                'ax_nacnt': bank_account[:10],
                                'ax_ncntr': '    ',
                                'ax_source': ax_source,
                                'ax_date': post_date,
                                'ax_value': bank_ntran_value if posting_decision.post_to_nominal else (gross_pounds if is_receipt else -gross_pounds),
                                'ax_tref': reference[:20],
                                'ax_comment': (ntran_comment if posting_decision.post_to_nominal else safe_comment)[:50],
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

                            # Target side anoml
                            target_anoml_acct = target_account if posting_decision.post_to_nominal else acct
                            target_anoml_val = target_ntran_value if posting_decision.post_to_nominal else (-gross_pounds if is_receipt else gross_pounds)
                            anoml_table.append({
                                'ax_nacnt': target_anoml_acct[:10],
                                'ax_ncntr': '    ',
                                'ax_source': ax_source,
                                'ax_date': post_date,
                                'ax_value': target_anoml_val,
                                'ax_tref': reference[:20],
                                'ax_comment': (ntran_comment if posting_decision.post_to_nominal else safe_comment)[:50],
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

                # 3. Update nbank (total bank movement)
                if posting_decision.post_to_nominal and total_bank_ntran != 0:
                    self._update_nbank_balance(bank_account, total_bank_ntran)
                    tables_updated.add('nbank')

                # 4. Advance recurring entry schedule (atomic within lock)
                self._advance_recurring_entry_in_lock(entry_ref, bank_account, post_date, ae_freq, ae_every, input_by)
                tables_updated.add('arhead')

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
