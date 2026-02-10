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
from datetime import datetime, date
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

    def _get_next_journal_number(self) -> int:
        """Get next available journal number for ntran"""
        table = self._open_table('ntran')
        max_journal = 0

        for record in table:
            if record.nt_jrnl > max_journal:
                max_journal = record.nt_jrnl

        return max_journal + 1

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
            from sql_rag.opera3_config import Opera3Config
            config = Opera3Config(str(self.data_path), self.encoding)
            period_result = config.validate_posting_period(post_date, ledger_type='PL')

            if not period_result.is_valid:
                return Opera3ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[period_result.error_message]
                )

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
                journal_number = self._get_next_journal_number()

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
                    # INSERT INTO ntran - CREDIT Bank
                    ntran_table.append({
                        'nt_acnt': bank_account[:8],
                        'nt_cntr': '    ',
                        'nt_type': 'B ',
                        'nt_subt': 'BC',
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
                        'nt_type': 'C ',
                        'nt_subt': 'CA',
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
            from sql_rag.opera3_config import Opera3Config
            config = Opera3Config(str(self.data_path), self.encoding)
            period_result = config.validate_posting_period(post_date, ledger_type='SL')

            if not period_result.is_valid:
                return Opera3ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[period_result.error_message]
                )

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
                journal_number = self._get_next_journal_number()

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
                    # INSERT INTO ntran - DEBIT Bank (money coming in)
                    ntran_table.append({
                        'nt_acnt': bank_account[:8],
                        'nt_cntr': '    ',
                        'nt_type': 'B ',
                        'nt_subt': 'BC',
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
                        'nt_type': 'B ',  # Same as SQL SE: 'B ' for both bank entries
                        'nt_subt': 'BB',  # BB for debtors control (not DB)
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

    def import_gocardless_batch(
        self,
        bank_account: str,
        payments: List[Dict[str, Any]],
        post_date: date,
        reference: str = "GoCardless",
        gocardless_fees: float = 0.0,
        vat_on_fees: float = 0.0,
        fees_nominal_account: str = None,
        complete_batch: bool = False,
        input_by: str = "GOCARDLS",
        cbtype: str = None,
        validate_only: bool = False
    ) -> Opera3ImportResult:
        """
        Import a GoCardless batch receipt into Opera 3.

        Creates a true Opera batch with:
        - One aentry header (batch total)
        - Multiple atran lines (one per customer payment)
        - Multiple stran records (one per customer)
        - If complete_batch=True: ntran records, customer balance updates
        - If complete_batch=False: leaves for review in Opera (ae_complet=False)

        Args:
            bank_account: Bank account code (e.g., 'BC010')
            payments: List of payment dicts with:
                - customer_account: Customer code
                - amount: Amount in POUNDS
                - description: Payment description/reference
            post_date: Posting date
            reference: Batch reference (default 'GoCardless')
            gocardless_fees: Total GoCardless fees (optional)
            vat_on_fees: VAT on fees (optional)
            fees_nominal_account: Nominal account for fees
            complete_batch: If True, completes batch immediately
            input_by: User code for audit trail
            cbtype: Cashbook type code (must be batched Receipt type)
            validate_only: If True, only validate without inserting

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

        # Validate fees configuration
        if gocardless_fees > 0 and not fees_nominal_account:
            return Opera3ImportResult(
                success=False,
                records_processed=len(payments),
                records_failed=len(payments),
                errors=[
                    f"GoCardless fees of £{gocardless_fees:.2f} cannot be posted: fees_nominal_account not configured."
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
            # Period validation
            from sql_rag.opera3_config import Opera3Config
            config = Opera3Config(str(self.data_path), self.encoding)
            period_result = config.validate_posting_period(post_date, ledger_type='SL')

            if not period_result.is_valid:
                return Opera3ImportResult(
                    success=False,
                    records_processed=len(payments),
                    records_failed=len(payments),
                    errors=[period_result.error_message]
                )

            # Validate all customer accounts exist
            for payment in payments:
                customer_account = payment.get('customer_account')
                if not customer_account:
                    errors.append("Payment missing customer_account")
                    continue
                customer_name = self._get_customer_name(customer_account)
                if not customer_name:
                    errors.append(f"Customer account '{customer_account}' not found")

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
                    warnings=["Validation passed - no records inserted (validate_only=True)"]
                )

            # Calculate totals
            gross_total = sum(p['amount'] for p in payments)
            net_total = gross_total - gocardless_fees
            gross_pence = int(gross_total * 100)

            if isinstance(post_date, str):
                post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

            year = post_date.year
            period = post_date.month
            now = datetime.now()

            # Tables to lock
            tables_to_lock = ['aentry', 'atran', 'stran', 'salloc', 'sname', 'atype']
            if complete_batch:
                tables_to_lock.extend(['ntran', 'nacnt', 'nbank', 'anoml'])

            with self._transaction_lock(tables_to_lock):
                # Get next entry number
                entry_number = self.increment_atype_entry(cbtype)

                # Get bank account details
                bank_data = self._get_bank_account(bank_account)
                if not bank_data:
                    return Opera3ImportResult(
                        success=False,
                        records_processed=len(payments),
                        errors=[f"Bank account '{bank_account}' not found"]
                    )

                bank_nominal = bank_data.get('nk_nominal', bank_account)
                counter = bank_data.get('nk_cntr', '01')

                # Create aentry header
                aentry_table = self._open_table('aentry', writable=True)
                aentry_table.append({
                    'ae_acnt': bank_account,
                    'ae_cntr': counter,
                    'ae_cbtype': cbtype,
                    'ae_entry': entry_number,
                    'ae_entref': reference[:15] if reference else 'GoCardless',
                    'ae_entdate': post_date,
                    'ae_period': period,
                    'ae_year': year,
                    'ae_value': gross_pence,
                    'ae_bvalue': gross_pence,
                    'ae_status': '',
                    'ae_inpby': input_by[:8],
                    'ae_inpdate': now.date(),
                    'ae_complet': complete_batch,
                    'ae_batched': True
                })

                # Create atran and stran for each payment
                atran_table = self._open_table('atran', writable=True)
                stran_table = self._open_table('stran', writable=True)
                salloc_table = self._open_table('salloc', writable=True)
                sname_table = self._open_table('sname', writable=True)

                line_no = 0
                for payment in payments:
                    line_no += 1
                    customer_account = payment['customer_account']
                    amount = payment['amount']
                    description = payment.get('description', '')[:35]
                    amount_pence = int(amount * 100)

                    customer_name = self._get_customer_name(customer_account)
                    debtors_control = self._get_customer_control_account(customer_account)

                    # Create atran line
                    atran_unique = OperaUniqueIdGenerator.generate()
                    atran_table.append({
                        'at_acnt': bank_account,
                        'at_cntr': counter,
                        'at_cbtype': cbtype,
                        'at_entry': entry_number,
                        'at_line': line_no,
                        'at_type': CashbookTransactionType.SALES_RECEIPT,
                        'at_account': customer_account,
                        'at_detail': description,
                        'at_value': amount_pence,
                        'at_pstdate': post_date,
                        'at_period': period,
                        'at_year': year,
                        'at_unique': atran_unique
                    })

                    # Create stran record
                    stran_unique = OperaUniqueIdGenerator.generate()
                    stran_table.append({
                        'st_account': customer_account,
                        'st_type': 'R',  # Receipt
                        'st_ref': reference[:10] if reference else 'GC',
                        'st_secref': description[:20],
                        'st_date': post_date,
                        'st_detail': description,
                        'st_value': -amount_pence,  # Negative for receipt
                        'st_period': period,
                        'st_year': year,
                        'st_unique': stran_unique,
                        'st_status': ' ',
                        'st_allocd': True
                    })

                    # Create salloc record (self-allocation)
                    salloc_table.append({
                        'sa_account': customer_account,
                        'sa_unique': stran_unique,
                        'sa_aunique': stran_unique,
                        'sa_value': amount_pence
                    })

                    # Update customer balance if completing batch
                    if complete_batch:
                        for record in sname_table:
                            if record.sn_account.strip() == customer_account:
                                with record:
                                    record.sn_currbal = record.sn_currbal - amount_pence
                                break

                # Add warnings about what was created
                warnings.append(f"Receipts entry: {cbtype}{entry_number}")
                warnings.append(f"Payments: {len(payments)}")
                warnings.append(f"Gross amount: £{gross_total:.2f}")

                if complete_batch:
                    warnings.append("Batch status: Completed")
                    # TODO: Create ntran entries for complete batches
                else:
                    warnings.append("Batch status: Open for review")

            return Opera3ImportResult(
                success=True,
                records_processed=len(payments),
                records_imported=len(payments),
                entry_number=f"{cbtype}{entry_number}",
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
