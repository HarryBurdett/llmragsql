"""
Opera SQL SE Import Module

This module provides import functionality for Opera SQL SE (SQL Server Edition).
Unlike the COM automation module, this works directly with SQL Server and can
run from any platform (Windows, Mac, Linux).

Opera SQL SE stores all data in SQL Server tables, so we can:
1. Query for stored procedures that handle imports
2. Write directly to staging tables
3. Use Opera's SQL-based import mechanisms

IMPORTANT: Direct table writes bypass Opera's business logic validation.
Always prefer using Opera's stored procedures or import utilities when available.
"""

import logging
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, date
import csv
import json
import time
import string
from sqlalchemy import text

logger = logging.getLogger(__name__)


# =========================================================================
# OPERA UNIQUE ID GENERATOR
# =========================================================================

class OperaUniqueIdGenerator:
    """
    Generates unique IDs in Opera's format.

    Opera uses IDs like '_7E00XM9II' which appear to be:
    - Underscore prefix
    - Base-36 encoded timestamp/sequence

    This replicates that pattern for our imports.
    """

    # Base-36 characters (0-9, A-Z)
    CHARS = string.digits + string.ascii_uppercase

    _last_time = 0
    _sequence = 0

    @classmethod
    def generate(cls) -> str:
        """Generate a unique ID in Opera's format."""
        current_time = int(time.time() * 1000)  # Milliseconds

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
    def generate_multiple(cls, count: int) -> list:
        """Generate multiple unique IDs with slight delays to ensure uniqueness."""
        ids = []
        for _ in range(count):
            ids.append(cls.generate())
            cls._sequence += 1  # Ensure different IDs even in same millisecond
        return ids


class ImportType(Enum):
    """Types of imports supported for Opera SQL SE"""
    SALES_INVOICES = "sales_invoices"
    PURCHASE_INVOICES = "purchase_invoices"
    NOMINAL_JOURNALS = "nominal_journals"
    CUSTOMERS = "customers"
    SUPPLIERS = "suppliers"
    PRODUCTS = "products"
    SALES_ORDERS = "sales_orders"
    PURCHASE_ORDERS = "purchase_orders"
    RECEIPTS = "receipts"
    PAYMENTS = "payments"
    SALES_RECEIPTS = "sales_receipts"


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
    NOMINAL_PAYMENT = 1.0      # Cashbook payment to nominal account (no ledger)
    NOMINAL_RECEIPT = 2.0     # Cashbook receipt from nominal account (no ledger)
    SALES_REFUND = 3.0        # Refund to customer (money out, reduces debtors)
    SALES_RECEIPT = 4.0       # Receipt from customer (money in, reduces debtors)
    PURCHASE_PAYMENT = 5.0    # Payment to supplier (money out, reduces creditors)
    PURCHASE_REFUND = 6.0     # Refund from supplier (money in, reduces creditors)


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


@dataclass
class ImportResult:
    """Result of an import operation"""
    success: bool
    records_processed: int = 0
    records_imported: int = 0
    records_failed: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    entry_number: Optional[str] = None  # Opera cashbook entry number (ae_entry)
    transaction_ref: Optional[str] = None  # Reference stored in Opera
    new_reconciled_balance: Optional[float] = None  # New balance after reconciliation (pounds)


@dataclass
class ValidationError:
    """Validation error for a single record"""
    row_number: int
    field: str
    value: Any
    message: str


# =========================================================================
# LOCKING CONFIGURATION
# =========================================================================

# Lock timeout in milliseconds - prevents indefinite blocking
# If a lock cannot be acquired within this time, the operation fails
LOCK_TIMEOUT_MS = 5000  # 5 seconds

# SQL hints for different scenarios:
# - NOLOCK: For read-only validation queries (dirty reads acceptable)
# - ROWLOCK: For single-row updates to minimize blocking
# - UPDLOCK, ROWLOCK: For sequence generation (lock only the row being read)
# - READPAST: Skip locked rows (useful for batch processing)

def get_lock_timeout_sql() -> str:
    """SQL to set lock timeout for the current session."""
    return f"SET LOCK_TIMEOUT {LOCK_TIMEOUT_MS}"


def get_next_sequence_sql(table: str, column: str, prefix: str = '',
                          filter_column: str = None, filter_value: str = None) -> str:
    """
    Generate optimized SQL for getting next sequence number.

    Uses a targeted approach that only locks the specific row being read,
    minimizing impact on other users.

    Args:
        table: Table name (e.g., 'aentry', 'ntran')
        column: Column to get max value from (e.g., 'ae_entry', 'nt_jrnl')
        prefix: Optional prefix to strip (e.g., 'R2' for entry numbers)
        filter_column: Optional column to filter by
        filter_value: Optional value to filter by

    Returns:
        SQL string for getting next sequence number
    """
    if prefix:
        # For prefixed sequences like R200000001
        select_expr = f"ISNULL(MAX(CAST(SUBSTRING({column}, {len(prefix)+1}, 10) AS INT)), 0) + 1"
    else:
        # For simple numeric sequences
        select_expr = f"ISNULL(MAX({column}), 0) + 1"

    # Use PAGLOCK instead of table-level HOLDLOCK for better concurrency
    # UPDLOCK ensures we get a consistent read before insert
    hints = "WITH (UPDLOCK, ROWLOCK)"

    if filter_column and filter_value:
        return f"""
            SELECT {select_expr} as next_num
            FROM {table} {hints}
            WHERE {filter_column} = '{filter_value}'
        """
    else:
        return f"""
            SELECT {select_expr} as next_num
            FROM {table} {hints}
        """


class OperaSQLImport:
    """
    Import handler for Opera SQL SE.

    Works directly with SQL Server - no Windows or COM required.
    Can run from any platform that can connect to SQL Server.
    """

    def __init__(self, sql_connector):
        """
        Initialize with an existing SQL connector.

        Args:
            sql_connector: SQLConnector instance connected to Opera SQL SE database
        """
        self.sql = sql_connector
        self._stored_procs = None
        self._table_schemas = {}
        self._vat_cache = {}  # Cache for VAT code lookups
        self._control_accounts = None  # Loaded on first use

    def get_control_accounts(self):
        """
        Get control account codes from Opera configuration.

        Returns:
            OperaControlAccounts with debtors and creditors control codes
        """
        if self._control_accounts is None:
            try:
                from sql_rag.opera_config import get_control_accounts
                self._control_accounts = get_control_accounts(self.sql)
            except Exception as e:
                logger.warning(f"Could not load control accounts from config: {e}")
                # Use defaults
                from dataclasses import dataclass
                @dataclass
                class DefaultControlAccounts:
                    debtors_control: str = "BB020"
                    creditors_control: str = "CA030"
                    source: str = "default"
                self._control_accounts = DefaultControlAccounts()

        return self._control_accounts

    def get_home_currency(self) -> Dict[str, Any]:
        """
        Look up home currency from zxchg table.

        The home currency has xc_home = 1 (True).

        Returns:
            Dictionary with:
                - code: Currency code (e.g., 'GBP')
                - description: Currency description (e.g., 'Sterling')
                - found: True if home currency was found
        """
        if hasattr(self, '_home_currency_cache') and self._home_currency_cache:
            return self._home_currency_cache

        try:
            result = self.sql.execute_query("""
                SELECT xc_curr, xc_desc
                FROM zxchg
                WHERE xc_home = 1
            """)
            if result is not None and len(result) > 0:
                self._home_currency_cache = {
                    'code': result.iloc[0]['xc_curr'].strip(),
                    'description': result.iloc[0]['xc_desc'].strip(),
                    'found': True
                }
            else:
                # Default to GBP if not found
                self._home_currency_cache = {
                    'code': 'GBP',
                    'description': 'Sterling (default)',
                    'found': False
                }
        except Exception as e:
            logger.warning(f"Could not look up home currency: {e}")
            self._home_currency_cache = {
                'code': 'GBP',
                'description': 'Sterling (default)',
                'found': False
            }

        return self._home_currency_cache

    def get_vat_rate(self, vat_code: str, vat_type: str = 'S', as_of_date: date = None) -> Dict[str, Any]:
        """
        Look up VAT rate and nominal account from ztax table.

        VAT rates can change over time:
        - tx_rate1 is the original rate
        - tx_rate2 is the updated rate (when changed)
        - tx_rate2dy is the date when rate2 became effective

        Args:
            vat_code: VAT code (e.g., '1', '2', 'Z', 'E', 'N')
            vat_type: 'S' for Sales, 'P' for Purchase
            as_of_date: Date to check rate for (defaults to today)

        Returns:
            Dictionary with:
                - rate: VAT rate as decimal (e.g., 20.0 for 20%)
                - nominal: VAT nominal account code
                - description: VAT code description
                - found: True if VAT code was found
        """
        # Default to today's date if not provided
        if as_of_date is None:
            as_of_date = date.today()

        # Include date in cache key since rate depends on date
        cache_key = f"{vat_code}_{vat_type}_{as_of_date}"
        if cache_key in self._vat_cache:
            return self._vat_cache[cache_key]

        try:
            df = self.sql.execute_query(f"""
                SELECT
                    tx_code, tx_desc, tx_rate1, tx_rate2,
                    tx_rate1dy, tx_rate2dy, tx_nominal
                FROM ztax
                WHERE RTRIM(tx_code) = '{vat_code}'
                AND tx_trantyp = '{vat_type}'
                AND tx_ctrytyp = 'H'
            """)

            if df.empty:
                # Try without transaction type filter
                df = self.sql.execute_query(f"""
                    SELECT
                        tx_code, tx_desc, tx_rate1, tx_rate2,
                        tx_rate1dy, tx_rate2dy, tx_nominal
                    FROM ztax
                    WHERE RTRIM(tx_code) = '{vat_code}'
                    AND tx_ctrytyp = 'H'
                """)

            if df.empty:
                return {'rate': 0.0, 'nominal': 'CA060', 'description': 'Unknown', 'found': False}

            row = df.iloc[0]

            # Determine which rate to use based on date
            # tx_rate2 is the updated rate, tx_rate2dy is when it became effective
            # Use tx_rate2 if transaction date >= rate change date, otherwise tx_rate1
            rate = float(row['tx_rate1']) if row['tx_rate1'] else 0.0

            if row['tx_rate2dy'] is not None:
                rate2_date = row['tx_rate2dy']
                if isinstance(rate2_date, str):
                    rate2_date = datetime.strptime(rate2_date, '%Y-%m-%d').date()
                elif hasattr(rate2_date, 'date'):
                    rate2_date = rate2_date.date()

                # If transaction date is on or after the rate change date, use the new rate
                if as_of_date >= rate2_date and row['tx_rate2'] is not None:
                    rate = float(row['tx_rate2'])

            result = {
                'rate': rate,
                'nominal': row['tx_nominal'].strip() if row['tx_nominal'] else 'CA060',
                'description': row['tx_desc'].strip() if row['tx_desc'] else '',
                'found': True
            }

            self._vat_cache[cache_key] = result
            return result

        except Exception as e:
            logger.error(f"Error looking up VAT code {vat_code}: {e}")
            return {'rate': 0.0, 'nominal': 'CA060', 'description': 'Error', 'found': False}

    # =========================================================================
    # NACNT (Nominal Account Balance) UPDATE METHODS
    # =========================================================================

    def update_nacnt_balance(self, conn, account: str, value: float, period: int):
        """
        Update nacnt (nominal account balance) after posting to ntran.

        Opera updates nacnt whenever it posts to ntran. This ensures the
        nominal account balances stay in sync with the transaction totals.

        Args:
            conn: Active database connection (within transaction)
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

        # Period balance column: na_balc01 for period 1, na_balc02 for period 2, etc.
        period_col = f"na_balc{period:02d}"

        if value >= 0:
            # DEBIT entry - update debit columns
            nacnt_sql = f"""
                UPDATE nacnt WITH (ROWLOCK)
                SET na_ptddr = ISNULL(na_ptddr, 0) + {value},
                    na_ytddr = ISNULL(na_ytddr, 0) + {value},
                    {period_col} = ISNULL({period_col}, 0) + {value},
                    datemodified = GETDATE()
                WHERE RTRIM(na_acnt) = '{account}'
            """
        else:
            # CREDIT entry - update credit columns with absolute value
            abs_value = abs(value)
            nacnt_sql = f"""
                UPDATE nacnt WITH (ROWLOCK)
                SET na_ptdcr = ISNULL(na_ptdcr, 0) + {abs_value},
                    na_ytdcr = ISNULL(na_ytdcr, 0) + {abs_value},
                    {period_col} = ISNULL({period_col}, 0) + {value},
                    datemodified = GETDATE()
                WHERE RTRIM(na_acnt) = '{account}'
            """

        try:
            result = conn.execute(text(nacnt_sql))
            if result.rowcount == 0:
                raise ValueError(f"nacnt update affected 0 rows for account {account} - account may not exist in nacnt table")
            logger.debug(f"Updated nacnt for {account}: value={value}, period={period}")
        except Exception as e:
            logger.error(f"Failed to update nacnt for {account}: {e}")
            raise  # Fail the transaction - nacnt must be updated correctly

    def update_nbank_balance(self, conn, bank_account: str, amount_pounds: float):
        """
        Update nbank.nk_curbal (bank current balance) after posting cashbook transactions.

        Opera updates nbank whenever cashbook transactions are posted. This ensures
        the bank balance stays in sync with the cashbook transaction totals.

        Args:
            conn: Active database connection (within transaction)
            bank_account: Bank nominal account code (e.g., 'BC010', 'BC026')
            amount_pounds: Transaction value in POUNDS (positive=receipt/increases balance,
                          negative=payment/decreases balance)

        Note: nbank.nk_curbal is stored in PENCE, so we convert pounds to pence.
        """
        # Convert pounds to pence for nbank storage
        amount_pence = int(round(amount_pounds * 100))

        nbank_sql = f"""
            UPDATE nbank WITH (ROWLOCK)
            SET nk_curbal = ISNULL(nk_curbal, 0) + {amount_pence},
                datemodified = GETDATE()
            WHERE RTRIM(nk_acnt) = '{bank_account}'
        """

        try:
            result = conn.execute(text(nbank_sql))
            if result.rowcount == 0:
                # Bank account may not exist in nbank - log warning but don't fail
                # Some nominal accounts used for payments might not be bank accounts
                logger.warning(f"nbank update affected 0 rows for account {bank_account} - may not be a bank account")
            else:
                logger.debug(f"Updated nbank for {bank_account}: amount_pounds={amount_pounds}, amount_pence={amount_pence}")
        except Exception as e:
            logger.error(f"Failed to update nbank for {bank_account}: {e}")
            raise  # Fail the transaction - bank balance must be updated correctly

    def get_bank_accounts_for_transfer(self) -> List[Dict[str, Any]]:
        """
        Get list of bank accounts valid for transfers.
        Returns non-foreign-currency bank accounts from nbank.
        """
        try:
            df = self.sql.execute_query("""
                SELECT nk_acnt, nk_name, nk_fcurr
                FROM nbank WITH (NOLOCK)
                WHERE ISNULL(RTRIM(nk_fcurr), '') = ''
                ORDER BY nk_acnt
            """)

            if df.empty:
                return []

            return [
                {
                    'code': row['nk_acnt'].strip(),
                    'name': row['nk_name'].strip() if row['nk_name'] else ''
                }
                for _, row in df.iterrows()
            ]
        except Exception as e:
            logger.error(f"Error getting bank accounts: {e}")
            return []

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
            query = """
                SELECT ay_cbtype, ay_desc, ay_type, ay_entry
                FROM atype
            """
            if category:
                query += f" WHERE RTRIM(ay_type) = '{category}'"
            query += " ORDER BY ay_type, ay_cbtype"

            df = self.sql.execute_query(query)
            if df.empty:
                return []

            return [
                {
                    'code': row['ay_cbtype'].strip(),
                    'description': row['ay_desc'].strip() if row['ay_desc'] else '',
                    'category': row['ay_type'].strip() if row['ay_type'] else '',
                    'next_entry': row['ay_entry'].strip() if row['ay_entry'] else ''
                }
                for _, row in df.iterrows()
            ]
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
            Dictionary with:
                - valid: True if type exists (and category matches if specified)
                - code: The type code
                - description: Type description
                - category: Type category (P/R/T)
                - next_entry: Current next entry number
                - error: Error message if not valid
        """
        try:
            df = self.sql.execute_query(f"""
                SELECT ay_cbtype, ay_desc, ay_type, ay_entry
                FROM atype
                WHERE RTRIM(ay_cbtype) = '{cbtype}'
            """)

            if df.empty:
                return {
                    'valid': False,
                    'code': cbtype,
                    'error': f"Type code '{cbtype}' not found in atype table"
                }

            row = df.iloc[0]
            category = row['ay_type'].strip() if row['ay_type'] else ''

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
                'description': row['ay_desc'].strip() if row['ay_desc'] else '',
                'category': category,
                'next_entry': row['ay_entry'].strip() if row['ay_entry'] else ''
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

        This reads the ay_entry field which tracks the next available entry number
        for each type (e.g., 'P100008025' means next P1 entry is P100008025).

        Args:
            cbtype: Type code (e.g., 'P1', 'R2', 'P5')

        Returns:
            Next entry number string (e.g., 'P100008025')
        """
        try:
            df = self.sql.execute_query(f"""
                SELECT ay_entry
                FROM atype
                WHERE RTRIM(ay_cbtype) = '{cbtype}'
            """)

            if df.empty:
                logger.warning(f"Type code '{cbtype}' not found, generating fallback entry number")
                return f"{cbtype}{1:08d}"

            entry = df.iloc[0]['ay_entry']
            return entry.strip() if entry else f"{cbtype}{1:08d}"

        except Exception as e:
            logger.error(f"Error getting next entry for '{cbtype}': {e}")
            return f"{cbtype}{1:08d}"

    def increment_atype_entry(self, conn, cbtype: str) -> str:
        """
        Increment the ay_entry counter for a type code and return the current value.

        This should be called within a transaction to ensure atomicity.
        The returned entry number is the one to use for the current transaction.

        Args:
            conn: Active database connection (within transaction)
            cbtype: Type code (e.g., 'P1', 'R2', 'P5')

        Returns:
            Entry number to use for this transaction
        """
        from sqlalchemy import text

        # Get current entry with lock
        result = conn.execute(text(f"""
            SELECT ay_entry
            FROM atype WITH (UPDLOCK, ROWLOCK)
            WHERE RTRIM(ay_cbtype) = '{cbtype}'
        """))
        row = result.fetchone()

        if not row:
            raise ValueError(f"Type code '{cbtype}' not found in atype")

        current_entry = row[0].strip() if row[0] else f"{cbtype}{0:08d}"

        # Parse and increment
        # Entry format is like 'P100008024' - prefix + 8-digit number
        prefix_len = len(cbtype)
        try:
            current_num = int(current_entry[prefix_len:])
        except ValueError:
            current_num = 0

        next_num = current_num + 1
        next_entry = f"{cbtype}{next_num:08d}"

        # Update atype with new entry number
        conn.execute(text(f"""
            UPDATE atype
            SET ay_entry = '{next_entry}',
                datemodified = GETDATE()
            WHERE RTRIM(ay_cbtype) = '{cbtype}'
        """))

        logger.debug(f"Incremented atype entry for {cbtype}: {current_entry} -> {next_entry}")

        # Return the entry number to USE (the one before increment)
        return current_entry

    def get_default_cbtype(self, transaction_type: str) -> Optional[str]:
        """
        Get a default type code for a transaction type.

        This finds the first available type with the correct category.

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

    def discover_import_capabilities(self) -> Dict[str, Any]:
        """
        Discover what import capabilities exist in the Opera SQL SE database.
        Looks for stored procedures, staging tables, and import utilities.
        """
        capabilities = {
            "stored_procedures": [],
            "import_related_procs": [],
            "staging_tables": [],
            "import_tables": [],
            "has_audit_triggers": False,
            "all_procedures": []
        }

        try:
            # Find ALL stored procedures (to see what Pegasus provides)
            df = self.sql.execute_query("""
                SELECT
                    s.name as schema_name,
                    p.name as procedure_name,
                    p.type_desc,
                    p.create_date,
                    p.modify_date
                FROM sys.procedures p
                JOIN sys.schemas s ON p.schema_id = s.schema_id
                ORDER BY s.name, p.name
            """)
            capabilities["all_procedures"] = df.to_dict('records') if not df.empty else []

            # Find stored procedures related to imports
            df = self.sql.execute_query("""
                SELECT
                    name,
                    type_desc,
                    create_date,
                    modify_date
                FROM sys.objects
                WHERE type IN ('P', 'FN', 'IF', 'TF')
                AND (
                    name LIKE '%import%'
                    OR name LIKE '%load%'
                    OR name LIKE '%insert%'
                    OR name LIKE '%staging%'
                    OR name LIKE '%post%'
                    OR name LIKE '%process%'
                )
                ORDER BY name
            """)
            capabilities["import_related_procs"] = df.to_dict('records') if not df.empty else []
            capabilities["stored_procedures"] = df.to_dict('records') if not df.empty else []

            # Find staging tables
            df = self.sql.execute_query("""
                SELECT
                    t.name as table_name,
                    s.name as schema_name
                FROM sys.tables t
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE t.name LIKE '%staging%'
                   OR t.name LIKE '%import%'
                   OR t.name LIKE '%temp%'
                ORDER BY t.name
            """)
            capabilities["staging_tables"] = df.to_dict('records') if not df.empty else []

            # Check for audit triggers (indicates we should be careful with direct writes)
            df = self.sql.execute_query("""
                SELECT COUNT(*) as trigger_count
                FROM sys.triggers
                WHERE name LIKE '%audit%' OR name LIKE '%log%'
            """)
            # Convert to Python bool to avoid numpy.bool serialization issues
            capabilities["has_audit_triggers"] = bool(df.iloc[0]['trigger_count'] > 0) if not df.empty else False

        except Exception as e:
            logger.error(f"Error discovering capabilities: {e}")
            capabilities["error"] = str(e)

        return capabilities

    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """Get the schema for a table to understand required fields."""
        if table_name in self._table_schemas:
            return self._table_schemas[table_name]

        try:
            df = self.sql.execute_query(f"""
                SELECT
                    c.name as column_name,
                    t.name as data_type,
                    c.max_length,
                    c.precision,
                    c.scale,
                    c.is_nullable,
                    c.is_identity,
                    ISNULL(d.definition, '') as default_value
                FROM sys.columns c
                JOIN sys.types t ON c.user_type_id = t.user_type_id
                LEFT JOIN sys.default_constraints d ON c.default_object_id = d.object_id
                WHERE c.object_id = OBJECT_ID('{table_name}')
                ORDER BY c.column_id
            """)
            schema = df.to_dict('records') if not df.empty else []
            self._table_schemas[table_name] = schema
            return schema
        except Exception as e:
            logger.error(f"Error getting schema for {table_name}: {e}")
            return []

    # =========================================================================
    # CUSTOMER IMPORT
    # =========================================================================

    def import_customers(
        self,
        customers: List[Dict[str, Any]],
        update_existing: bool = False,
        validate_only: bool = False
    ) -> ImportResult:
        """
        Import customer records into Opera SQL SE.

        Args:
            customers: List of customer dictionaries with fields:
                - account (required): Customer account code
                - name (required): Customer name
                - address1-5: Address lines
                - postcode: Postal code
                - telephone: Phone number
                - email: Email address
                - credit_limit: Credit limit
                - payment_terms: Payment terms code
                - vat_code: Default VAT code
            update_existing: If True, update existing customers
            validate_only: If True, only validate without importing

        Returns:
            ImportResult with details of the operation
        """
        errors = []
        warnings = []
        imported = 0
        failed = 0

        for idx, customer in enumerate(customers, 1):
            try:
                # Validate required fields
                if not customer.get('account'):
                    errors.append(f"Row {idx}: Missing required field 'account'")
                    failed += 1
                    continue
                if not customer.get('name'):
                    errors.append(f"Row {idx}: Missing required field 'name'")
                    failed += 1
                    continue

                # Check if customer exists
                existing = self.sql.execute_query(f"""
                    SELECT sl_account FROM slcust
                    WHERE RTRIM(sl_account) = '{customer['account']}'
                """)

                exists = not existing.empty

                if exists and not update_existing:
                    warnings.append(f"Row {idx}: Customer {customer['account']} already exists, skipping")
                    continue

                if validate_only:
                    imported += 1
                    continue

                # Build the SQL
                if exists and update_existing:
                    # Update existing customer
                    sql = self._build_customer_update(customer)
                else:
                    # Insert new customer
                    sql = self._build_customer_insert(customer)

                # Execute (would need write capability)
                # For now, just log what would be done
                logger.info(f"Would execute: {sql}")
                imported += 1

            except Exception as e:
                errors.append(f"Row {idx}: {str(e)}")
                failed += 1

        return ImportResult(
            success=failed == 0,
            records_processed=len(customers),
            records_imported=imported,
            records_failed=failed,
            errors=errors,
            warnings=warnings
        )

    def _build_customer_insert(self, customer: Dict[str, Any]) -> str:
        """Build INSERT statement for customer"""
        fields = ['sl_account', 'sl_name']
        values = [f"'{customer['account']}'", f"'{customer['name']}'"]

        field_mapping = {
            'address1': 'sl_addr1',
            'address2': 'sl_addr2',
            'address3': 'sl_addr3',
            'address4': 'sl_addr4',
            'address5': 'sl_addr5',
            'postcode': 'sl_postcode',
            'telephone': 'sl_telephone',
            'email': 'sl_email',
            'credit_limit': 'sl_credit_limit',
            'payment_terms': 'sl_terms',
            'vat_code': 'sl_vat_code'
        }

        for source, target in field_mapping.items():
            if customer.get(source):
                fields.append(target)
                val = customer[source]
                if isinstance(val, (int, float)):
                    values.append(str(val))
                else:
                    values.append(f"'{val}'")

        return f"INSERT INTO slcust ({', '.join(fields)}) VALUES ({', '.join(values)})"

    def _build_customer_update(self, customer: Dict[str, Any]) -> str:
        """Build UPDATE statement for customer"""
        sets = [f"sl_name = '{customer['name']}'"]

        field_mapping = {
            'address1': 'sl_addr1',
            'address2': 'sl_addr2',
            'address3': 'sl_addr3',
            'address4': 'sl_addr4',
            'address5': 'sl_addr5',
            'postcode': 'sl_postcode',
            'telephone': 'sl_telephone',
            'email': 'sl_email',
            'credit_limit': 'sl_credit_limit',
            'payment_terms': 'sl_terms',
            'vat_code': 'sl_vat_code'
        }

        for source, target in field_mapping.items():
            if customer.get(source):
                val = customer[source]
                if isinstance(val, (int, float)):
                    sets.append(f"{target} = {val}")
                else:
                    sets.append(f"{target} = '{val}'")

        return f"UPDATE slcust SET {', '.join(sets)} WHERE RTRIM(sl_account) = '{customer['account']}'"

    # =========================================================================
    # NOMINAL JOURNAL IMPORT
    # =========================================================================

    def import_nominal_journals(
        self,
        journals: List[Dict[str, Any]],
        validate_only: bool = False,
        auto_balance: bool = True
    ) -> ImportResult:
        """
        Import nominal journal entries into Opera SQL SE.

        Args:
            journals: List of journal entry dictionaries with fields:
                - account (required): Nominal account code
                - date (required): Transaction date
                - reference: Transaction reference
                - description: Transaction description
                - debit: Debit amount (use this OR credit, not both)
                - credit: Credit amount
                - department: Department code
                - cost_centre: Cost centre code
            validate_only: If True, only validate without importing
            auto_balance: If True, check that debits = credits

        Returns:
            ImportResult with details of the operation
        """
        errors = []
        warnings = []
        imported = 0
        failed = 0

        total_debits = 0
        total_credits = 0

        for idx, journal in enumerate(journals, 1):
            try:
                # Validate required fields
                if not journal.get('account'):
                    errors.append(f"Row {idx}: Missing required field 'account'")
                    failed += 1
                    continue
                if not journal.get('date'):
                    errors.append(f"Row {idx}: Missing required field 'date'")
                    failed += 1
                    continue

                # Check account exists
                existing = self.sql.execute_query(f"""
                    SELECT nl_account FROM nlacct
                    WHERE RTRIM(nl_account) = '{journal['account']}'
                """)

                if existing.empty:
                    errors.append(f"Row {idx}: Nominal account {journal['account']} not found")
                    failed += 1
                    continue

                # Track debits/credits for balance check
                debit = float(journal.get('debit', 0) or 0)
                credit = float(journal.get('credit', 0) or 0)
                total_debits += debit
                total_credits += credit

                if validate_only:
                    imported += 1
                    continue

                # Build insert SQL
                sql = self._build_journal_insert(journal)
                logger.info(f"Would execute: {sql}")
                imported += 1

            except Exception as e:
                errors.append(f"Row {idx}: {str(e)}")
                failed += 1

        # Check balance
        if auto_balance and abs(total_debits - total_credits) > 0.01:
            errors.append(f"Journal does not balance: Debits={total_debits:.2f}, Credits={total_credits:.2f}")
            return ImportResult(
                success=False,
                records_processed=len(journals),
                records_imported=0,
                records_failed=len(journals),
                errors=errors,
                warnings=warnings
            )

        return ImportResult(
            success=failed == 0,
            records_processed=len(journals),
            records_imported=imported,
            records_failed=failed,
            errors=errors,
            warnings=warnings
        )

    def _build_journal_insert(self, journal: Dict[str, Any]) -> str:
        """Build INSERT statement for nominal journal"""
        debit = float(journal.get('debit', 0) or 0)
        credit = float(journal.get('credit', 0) or 0)
        value = debit - credit  # Positive = debit, negative = credit

        # Format date
        trans_date = journal['date']
        if isinstance(trans_date, str):
            trans_date = f"'{trans_date}'"
        elif isinstance(trans_date, (date, datetime)):
            trans_date = f"'{trans_date.strftime('%Y-%m-%d')}'"

        return f"""
            INSERT INTO ntran (
                nt_account, nt_date, nt_ref, nt_details, nt_value,
                nt_type, nt_year, nt_period
            ) VALUES (
                '{journal['account']}',
                {trans_date},
                '{journal.get('reference', '')}',
                '{journal.get('description', '')}',
                {value},
                'J',
                YEAR({trans_date}),
                MONTH({trans_date})
            )
        """

    # =========================================================================
    # SALES INVOICE IMPORT
    # =========================================================================

    def import_sales_invoices(
        self,
        invoices: List[Dict[str, Any]],
        validate_only: bool = False
    ) -> ImportResult:
        """
        Import sales invoices into Opera SQL SE.

        Each invoice should contain:
            - customer_account (required): Customer account code
            - invoice_date (required): Invoice date
            - invoice_number: Invoice number (auto-generated if blank)
            - lines: List of invoice lines with:
                - product_code: Product/stock code
                - description: Line description
                - quantity: Quantity
                - unit_price: Unit price
                - vat_code: VAT code
                - nominal_code: Nominal account for posting
        """
        errors = []
        warnings = []
        imported = 0
        failed = 0

        for idx, invoice in enumerate(invoices, 1):
            try:
                # Validate
                if not invoice.get('customer_account'):
                    errors.append(f"Invoice {idx}: Missing customer_account")
                    failed += 1
                    continue

                if not invoice.get('invoice_date'):
                    errors.append(f"Invoice {idx}: Missing invoice_date")
                    failed += 1
                    continue

                # Check customer exists
                existing = self.sql.execute_query(f"""
                    SELECT sl_account FROM slcust
                    WHERE RTRIM(sl_account) = '{invoice['customer_account']}'
                """)

                if existing.empty:
                    errors.append(f"Invoice {idx}: Customer {invoice['customer_account']} not found")
                    failed += 1
                    continue

                if validate_only:
                    imported += 1
                    continue

                # Would insert invoice header and lines here
                logger.info(f"Would import invoice for customer {invoice['customer_account']}")
                imported += 1

            except Exception as e:
                errors.append(f"Invoice {idx}: {str(e)}")
                failed += 1

        return ImportResult(
            success=failed == 0,
            records_processed=len(invoices),
            records_imported=imported,
            records_failed=failed,
            errors=errors,
            warnings=warnings
        )

    # =========================================================================
    # CSV FILE IMPORT
    # =========================================================================

    def import_from_csv(
        self,
        import_type: ImportType,
        file_path: str,
        field_mapping: Optional[Dict[str, str]] = None,
        validate_only: bool = False
    ) -> ImportResult:
        """
        Import data from a CSV file.

        Args:
            import_type: Type of data to import
            file_path: Path to CSV file
            field_mapping: Optional mapping of CSV columns to Opera fields
            validate_only: Only validate, don't import

        Returns:
            ImportResult with details
        """
        try:
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                records = list(reader)
        except Exception as e:
            return ImportResult(
                success=False,
                records_processed=0,
                errors=[f"Failed to read CSV file: {e}"]
            )

        # Apply field mapping if provided
        if field_mapping:
            mapped_records = []
            for record in records:
                mapped = {}
                for csv_field, opera_field in field_mapping.items():
                    if csv_field in record:
                        mapped[opera_field] = record[csv_field]
                mapped_records.append(mapped)
            records = mapped_records

        # Route to appropriate import method
        if import_type == ImportType.CUSTOMERS:
            return self.import_customers(records, validate_only=validate_only)
        elif import_type == ImportType.NOMINAL_JOURNALS:
            return self.import_nominal_journals(records, validate_only=validate_only)
        elif import_type == ImportType.SALES_INVOICES:
            return self.import_sales_invoices(records, validate_only=validate_only)
        else:
            return ImportResult(
                success=False,
                errors=[f"Import type {import_type.value} not yet implemented"]
            )


    # =========================================================================
    # SALES RECEIPT IMPORT (Replicates Opera's exact pattern)
    # =========================================================================

    def import_sales_receipt(
        self,
        bank_account: str,
        customer_account: str,
        amount_pounds: float,
        reference: str,
        post_date: date,
        input_by: str = "IMPORT",
        sales_ledger_control: str = None,
        payment_method: str = "BACS",
        cbtype: str = None,
        validate_only: bool = False,
        comment: str = ""
    ) -> ImportResult:
        """
        Import a sales receipt into Opera SQL SE.

        This replicates the EXACT pattern Opera uses when a user manually
        enters a sales receipt, creating records in:
        1. aentry (Cashbook Entry Header)
        2. atran (Cashbook Transaction)
        3. ntran (Nominal Ledger - 2 rows for double-entry)
        4. stran (Sales Ledger Transaction)
        5. salloc (Sales Allocation)
        6. atype (Entry counter update)

        Args:
            bank_account: Bank account code (e.g., 'BC010')
            customer_account: Customer account code (e.g., 'A046')
            amount_pounds: Receipt amount in POUNDS (e.g., 100.00)
            reference: Your reference (e.g., 'inv12345')
            post_date: Posting date
            input_by: User code for audit trail (max 8 chars)
            sales_ledger_control: Sales ledger control account (auto-detected from config if None)
            payment_method: Payment method description (default 'BACS')
            cbtype: Cashbook type code from atype (e.g., 'R2'). Must be Receipt type (ay_type='R').
                   If None, uses first available Receipt type.
            validate_only: If True, only validate without inserting

        Returns:
            ImportResult with details of the operation
        """
        errors = []
        warnings = []

        # =====================
        # VALIDATE/GET CBTYPE
        # =====================
        if cbtype is None:
            cbtype = self.get_default_cbtype('sales_receipt')
            if cbtype is None:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=["No Receipt type codes found in atype table"]
                )
            logger.debug(f"Using default cbtype for sales receipt: {cbtype}")

        # Validate the type code
        type_validation = self.validate_cbtype(cbtype, required_category=AtypeCategory.RECEIPT)
        if not type_validation['valid']:
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[type_validation['error']]
            )

        # Get the correct at_type for sales receipts (always 4.0)
        at_type = CashbookTransactionType.SALES_RECEIPT

        # Get control account - check customer profile first, then fall back to default
        if sales_ledger_control is None:
            from sql_rag.opera_config import get_customer_control_account
            sales_ledger_control = get_customer_control_account(self.sql, customer_account)
            logger.debug(f"Using debtors control for customer {customer_account}: {sales_ledger_control}")

        try:
            # =====================
            # PERIOD POSTING DECISION
            # =====================
            from sql_rag.opera_config import get_period_posting_decision
            posting_decision = get_period_posting_decision(self.sql, post_date, 'SL')

            if not posting_decision.can_post:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[posting_decision.error_message]
                )

            # =====================
            # VALIDATION (using NOLOCK to avoid blocking other users)
            # =====================

            # Validate bank account exists by checking if it's been used in atran before
            bank_check = self.sql.execute_query(f"""
                SELECT TOP 1 at_acnt FROM atran WITH (NOLOCK)
                WHERE RTRIM(at_acnt) = '{bank_account}'
            """)
            if bank_check.empty:
                warnings.append(f"Bank account '{bank_account}' has not been used before - verify it's correct")

            # Validate customer exists by checking sname (Sales Ledger Master) first
            # This is the authoritative source for customer names
            sname_check = self.sql.execute_query(f"""
                SELECT sn_name, sn_region, sn_terrtry, sn_custype FROM sname WITH (NOLOCK)
                WHERE RTRIM(sn_account) = '{customer_account}'
            """)
            if not sname_check.empty:
                customer_name = sname_check.iloc[0]['sn_name'].strip()
                customer_region = sname_check.iloc[0]['sn_region'].strip() if sname_check.iloc[0]['sn_region'] else 'K'
                customer_terr = sname_check.iloc[0]['sn_terrtry'].strip() if sname_check.iloc[0]['sn_terrtry'] else '001'
                customer_type = sname_check.iloc[0]['sn_custype'].strip() if sname_check.iloc[0]['sn_custype'] else 'DD1'
            else:
                # Fall back to atran history if not in sname
                customer_check = self.sql.execute_query(f"""
                    SELECT TOP 1 at_account, at_name FROM atran WITH (NOLOCK)
                    WHERE RTRIM(at_account) = '{customer_account}'
                """)
                if not customer_check.empty:
                    customer_name = customer_check.iloc[0]['at_name'].strip()
                    customer_region = 'K'
                    customer_terr = '001'
                    customer_type = 'DD1'
                else:
                    errors.append(f"Customer account '{customer_account}' not found")

            # Validate nominal accounts exist by checking ntran
            bank_nominal_check = self.sql.execute_query(f"""
                SELECT TOP 1 nt_acnt FROM ntran WITH (NOLOCK)
                WHERE RTRIM(nt_acnt) = '{bank_account}'
            """)
            if bank_nominal_check.empty:
                warnings.append(f"Bank nominal account '{bank_account}' has not been used before - verify it's correct")

            control_check = self.sql.execute_query(f"""
                SELECT TOP 1 nt_acnt FROM ntran WITH (NOLOCK)
                WHERE RTRIM(nt_acnt) = '{sales_ledger_control}'
            """)
            if control_check.empty:
                warnings.append(f"Sales ledger control account '{sales_ledger_control}' has not been used before - verify it's correct")

            if errors:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=errors
                )

            if validate_only:
                return ImportResult(
                    success=True,
                    records_processed=1,
                    records_imported=1,
                    warnings=["Validation passed - no records inserted (validate_only=True)"]
                )

            # =====================
            # CONVERT AMOUNTS
            # =====================
            amount_pence = int(round(amount_pounds * 100))  # aentry/atran use pence - must round to avoid floating point truncation
            # ntran uses pounds

            # Format date
            if isinstance(post_date, str):
                post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

            year = post_date.year
            period = post_date.month

            # Get current timestamp for created/modified
            now = datetime.now()
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')
            date_str = now.strftime('%Y-%m-%d')
            time_str = now.strftime('%H:%M:%S')

            # Generate unique IDs (Opera's format) - these are timestamp-based so safe outside transaction
            unique_ids = OperaUniqueIdGenerator.generate_multiple(5)
            aentry_id = unique_ids[0]  # Not used directly but for reference
            atran_unique = unique_ids[1]  # Shared between atran and stran
            ntran_pstid_debit = unique_ids[2]
            ntran_pstid_credit = unique_ids[3]
            stran_unique = unique_ids[4]

            # =====================
            # INSERT RECORDS WITHIN TRANSACTION
            # All sequence numbers generated inside transaction with row-level locking
            # to prevent conflicts with other users
            # =====================

            # Execute all operations within a single transaction
            with self.sql.engine.begin() as conn:
                # Set lock timeout to prevent indefinite blocking of other users
                conn.execute(text(get_lock_timeout_sql()))

                # Get next entry number from atype and increment counter
                # This is the proper Opera way - atype tracks entry numbers per type
                entry_number = self.increment_atype_entry(conn, cbtype)

                # Get next journal number with UPDLOCK, ROWLOCK for minimal blocking
                journal_result = conn.execute(text("""
                    SELECT ISNULL(MAX(nt_jrnl), 0) + 1 as next_journal
                    FROM ntran WITH (UPDLOCK, ROWLOCK)
                """))
                next_journal = journal_result.scalar() or 1

                # 1. INSERT INTO aentry (Cashbook Entry Header)
                # ae_complet should only be 1 if we're posting to nominal ledger
                ae_complet_flag = 1 if posting_decision.post_to_nominal else 0
                # Sanitize comment for SQL - remove newlines, escape quotes
                safe_comment = comment.replace(chr(10), ' ').replace(chr(13), ' ').replace("'", "''") if comment else ''
                aentry_sql = f"""
                    INSERT INTO aentry (
                        ae_acnt, ae_cntr, ae_cbtype, ae_entry, ae_reclnum,
                        ae_lstdate, ae_frstat, ae_tostat, ae_statln, ae_entref,
                        ae_value, ae_recbal, ae_remove, ae_tmpstat, ae_complet,
                        ae_postgrp, sq_crdate, sq_crtime, sq_cruser, ae_comment,
                        ae_payid, ae_batchid, ae_brwptr, datecreated, datemodified, state
                    ) VALUES (
                        '{bank_account}', '    ', '{cbtype}', '{entry_number}', 0,
                        '{post_date}', 0, 0, 0, '{reference[:20]}',
                        {amount_pence}, 0, 0, 0, {ae_complet_flag},
                        0, '{date_str}', '{time_str[:8]}', '{input_by[:8]}', '{safe_comment[:40]}',
                        0, 0, '  ', '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(aentry_sql))

                # 2. INSERT INTO atran (Cashbook Transaction)
                # Build transaction reference like Opera does
                trnref = f"{customer_name[:30]:<30}BACS       (RT)     "

                # 2. INSERT INTO atran (Cashbook Transaction)
                atran_sql = f"""
                    INSERT INTO atran (
                        at_acnt, at_cntr, at_cbtype, at_entry, at_inputby,
                        at_type, at_pstdate, at_sysdate, at_tperiod, at_value,
                        at_disc, at_fcurr, at_fcexch, at_fcmult, at_fcdec,
                        at_account, at_name, at_comment, at_payee, at_payname,
                        at_sort, at_number, at_remove, at_chqprn, at_chqlst,
                        at_bacprn, at_ccdprn, at_ccdno, at_payslp, at_pysprn,
                        at_cash, at_remit, at_unique, at_postgrp, at_ccauth,
                        at_refer, at_srcco, at_ecb, at_ecbtype, at_atpycd,
                        at_bsref, at_bsname, at_vattycd, at_project, at_job,
                        at_bic, at_iban, at_memo, datecreated, datemodified, state
                    ) VALUES (
                        '{bank_account}', '    ', '{cbtype}', '{entry_number}', '{input_by[:8]}',
                        {at_type}, '{post_date}', '{post_date}', 1, {amount_pence},
                        0, '   ', 1.0, 0, 2,
                        '{customer_account}', '{customer_name[:35]}', '{safe_comment[:50]}', '        ', '',
                        '        ', '         ', 0, 0, 0,
                        0, 0, '', 0, 0,
                        0, 0, '{atran_unique}', 0, '0       ',
                        '{reference[:20]}', 'I', 0, ' ', '      ',
                        '', '', '  ', '        ', '        ',
                        '', '', '', '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(atran_sql))

                # 3. Nominal postings - CONDITIONAL based on period posting decision
                # Use comment (full description) for nt_cmnt, fall back to reference
                ntran_comment = f"{(safe_comment or reference)[:50]:<50}"
                ntran_trnref = f"{customer_name[:30]:<30}BACS       (RT)     "

                if posting_decision.post_to_nominal:
                    # INSERT INTO ntran - DEBIT (Bank Account +amount)
                    ntran_debit_sql = f"""
                        INSERT INTO ntran (
                            nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                            nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                            nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                            nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                            nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                            nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                            nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                            nt_distrib, datecreated, datemodified, state
                        ) VALUES (
                            '{bank_account}', '    ', 'B ', 'BC', {next_journal},
                            '', '{input_by[:10]}', 'A', '{ntran_comment}', '{ntran_trnref}',
                            '{post_date}', {amount_pounds}, {year}, {period}, 0,
                            0, 0, '   ', 0, 0,
                            0, 0, 'I', '', '        ',
                            '        ', 'S', 0, '{ntran_pstid_debit}', 0,
                            0, 0, 0, 0, 0,
                            0, '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(ntran_debit_sql))
                    # Update nacnt balance for bank account (DEBIT)
                    self.update_nacnt_balance(conn, bank_account, amount_pounds, period)
                    # Update nbank balance (receipt increases bank balance)
                    self.update_nbank_balance(conn, bank_account, amount_pounds)

                    # INSERT INTO ntran - CREDIT (Sales Ledger Control -amount)
                    ntran_credit_sql = f"""
                        INSERT INTO ntran (
                            nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                            nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                            nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                            nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                            nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                            nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                            nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                            nt_distrib, datecreated, datemodified, state
                        ) VALUES (
                            '{sales_ledger_control}', '    ', 'B ', 'BB', {next_journal},
                            '', '{input_by[:10]}', 'A', '{ntran_comment}', '{ntran_trnref}',
                            '{post_date}', {-amount_pounds}, {year}, {period}, 0,
                            0, 0, '   ', 0, 0,
                            0, 0, 'I', '', '        ',
                            '        ', 'S', 0, '{ntran_pstid_credit}', 0,
                            0, 0, 0, 0, 0,
                            0, '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(ntran_credit_sql))
                    # Update nacnt balance for sales ledger control (CREDIT)
                    self.update_nacnt_balance(conn, sales_ledger_control, -amount_pounds, period)

                # 4. INSERT INTO transfer files (anoml only - Opera uses anoml for both sides of receipt)
                if posting_decision.post_to_transfer_file:
                    done_flag = posting_decision.transfer_file_done_flag
                    jrnl_num = next_journal if posting_decision.post_to_nominal else 0

                    # anoml record 1 - Bank account (debit - money coming in)
                    anoml_bank_sql = f"""
                        INSERT INTO anoml (
                            ax_nacnt, ax_ncntr, ax_source, ax_date, ax_value, ax_tref,
                            ax_comment, ax_done, ax_fcurr, ax_fvalue, ax_fcrate, ax_fcmult, ax_fcdec,
                            ax_srcco, ax_unique, ax_project, ax_job, ax_jrnl, ax_nlpdate,
                            datecreated, datemodified, state
                        ) VALUES (
                            '{bank_account}', '    ', 'S', '{post_date}', {amount_pounds}, '{reference[:20]}',
                            '{ntran_comment[:50]}', '{done_flag}', '   ', 0, 0, 0, 0,
                            'I', '{atran_unique}', '        ', '        ', {jrnl_num}, '{post_date}',
                            '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(anoml_bank_sql))

                    # anoml record 2 - Debtors control account (credit - reducing asset)
                    anoml_control_sql = f"""
                        INSERT INTO anoml (
                            ax_nacnt, ax_ncntr, ax_source, ax_date, ax_value, ax_tref,
                            ax_comment, ax_done, ax_fcurr, ax_fvalue, ax_fcrate, ax_fcmult, ax_fcdec,
                            ax_srcco, ax_unique, ax_project, ax_job, ax_jrnl, ax_nlpdate,
                            datecreated, datemodified, state
                        ) VALUES (
                            '{sales_ledger_control}', '    ', 'S', '{post_date}', {-amount_pounds}, '{reference[:20]}',
                            '{ntran_comment[:50]}', '{done_flag}', '   ', 0, 0, 0, 0,
                            'I', '{atran_unique}', '        ', '        ', {jrnl_num}, '{post_date}',
                            '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(anoml_control_sql))

                # 5. INSERT INTO stran (Sales Ledger Transaction)
                # Values are NEGATIVE for receipts (money received reduces customer debt)
                stran_memo = f"Payment received - {reference[:50]}"
                stran_sql = f"""
                    INSERT INTO stran (
                        st_account, st_trdate, st_trref, st_custref, st_trtype,
                        st_trvalue, st_vatval, st_trbal, st_paid, st_crdate,
                        st_advance, st_memo, st_payflag, st_set1day, st_set1,
                        st_set2day, st_set2, st_dueday, st_fcurr, st_fcrate,
                        st_fcdec, st_fcval, st_fcbal, st_fcmult, st_dispute,
                        st_edi, st_editx, st_edivn, st_txtrep, st_binrep,
                        st_advallc, st_cbtype, st_entry, st_unique, st_region,
                        st_terr, st_type, st_fadval, st_delacc, st_euro,
                        st_payadvl, st_eurind, st_origcur, st_fullamt, st_fullcb,
                        st_fullnar, st_cash, st_rcode, st_ruser, st_revchrg,
                        st_nlpdate, st_adjsv, st_fcvat, st_taxpoin,
                        datecreated, datemodified, state
                    ) VALUES (
                        '{customer_account}', '{post_date}', '{reference[:20]}', '{payment_method[:20]}', 'R',
                        {-amount_pounds}, 0, {-amount_pounds}, ' ', '{post_date}',
                        'N', '{stran_memo[:200]}', 0, 0, 0,
                        0, 0, '{post_date}', '   ', 0,
                        0, 0, 0, 0, 0,
                        0, 0, 0, '', 0,
                        0, '{cbtype}', '{entry_number}', '{stran_unique}', '{customer_region[:3]}',
                        '{customer_terr[:3]}', '{customer_type[:3]}', 0, '{customer_account}', 0,
                        0, ' ', '   ', 0, '  ',
                        '          ', 0, '    ', '        ', 0,
                        '{post_date}', 0, 0, '{post_date}',
                        '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(stran_sql))

                # Get the stran ID we just inserted for salloc
                stran_id_result = conn.execute(text("""
                    SELECT TOP 1 id FROM stran
                    WHERE st_unique = :unique_id
                    ORDER BY id DESC
                """), {"unique_id": stran_unique})
                stran_row = stran_id_result.fetchone()
                stran_id = stran_row[0] if stran_row else 0

                # 6. INSERT INTO salloc (Sales Allocation)
                salloc_sql = f"""
                    INSERT INTO salloc (
                        al_account, al_date, al_ref1, al_ref2, al_type,
                        al_val, al_payind, al_payflag, al_payday, al_fcurr,
                        al_fval, al_fdec, al_advind, al_acnt, al_cntr,
                        al_preprd, al_unique, al_adjsv,
                        datecreated, datemodified, state
                    ) VALUES (
                        '{customer_account}', '{post_date}', '{reference[:20]}', '{payment_method[:20]}', 'R',
                        {-amount_pounds}, 'A', 0, '{post_date}', '   ',
                        0, 0, 0, '{bank_account}', '    ',
                        0, {stran_id}, 0,
                        '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(salloc_sql))

                # 7. UPDATE sname.sn_currbal with row-level lock (reduce customer balance - they paid us)
                sname_update_sql = f"""
                    UPDATE sname WITH (ROWLOCK)
                    SET sn_currbal = sn_currbal - {amount_pounds},
                        datemodified = '{now_str}'
                    WHERE RTRIM(sn_account) = '{customer_account}'
                """
                conn.execute(text(sname_update_sql))

            # Build list of tables updated based on what was actually done
            tables_updated = ["aentry", "atran", "stran", "salloc", "sname"]
            if posting_decision.post_to_nominal:
                tables_updated.insert(2, "ntran (2)")
            if posting_decision.post_to_transfer_file:
                tables_updated.append("anoml (2)")  # Opera uses anoml for both bank and control

            posting_mode = "Current period - posted to nominal" if posting_decision.post_to_nominal else "Different period - transfer file only (pending NL post)"

            logger.info(f"Successfully imported sales receipt: {entry_number} for £{amount_pounds:.2f} - {posting_mode}")

            return ImportResult(
                success=True,
                records_processed=1,
                records_imported=1,
                entry_number=entry_number,
                transaction_ref=reference[:20],
                warnings=[
                    f"Entry number: {entry_number}",
                    f"Journal number: {next_journal}",
                    f"Amount: £{amount_pounds:.2f}",
                    f"Posting mode: {posting_mode}",
                    f"Tables updated: {', '.join(tables_updated)}"
                ]
            )

        except Exception as e:
            logger.error(f"Failed to import sales receipt: {e}")
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[str(e)]
            )

    def import_sales_receipts_batch(
        self,
        receipts: List[Dict[str, Any]],
        validate_only: bool = False
    ) -> ImportResult:
        """
        Import multiple sales receipts.

        Each receipt dictionary should contain:
            - bank_account: Bank account code (e.g., 'BC010')
            - customer_account: Customer account code (e.g., 'A046')
            - amount: Receipt amount in POUNDS
            - reference: Your reference
            - post_date: Posting date (YYYY-MM-DD string or date object)
            - input_by: (optional) User code, defaults to 'IMPORT'
            - payment_method: (optional) Payment method, defaults to 'BACS'

        Returns:
            ImportResult with combined details
        """
        total_processed = 0
        total_imported = 0
        total_failed = 0
        all_errors = []
        all_warnings = []

        for idx, receipt in enumerate(receipts, 1):
            try:
                result = self.import_sales_receipt(
                    bank_account=receipt['bank_account'],
                    customer_account=receipt['customer_account'],
                    amount_pounds=float(receipt['amount']),
                    reference=receipt.get('reference', ''),
                    post_date=receipt['post_date'],
                    input_by=receipt.get('input_by', 'IMPORT'),
                    payment_method=receipt.get('payment_method', 'BACS'),
                    validate_only=validate_only
                )

                total_processed += 1
                if result.success:
                    total_imported += 1
                    all_warnings.extend([f"Receipt {idx}: {w}" for w in result.warnings])
                else:
                    total_failed += 1
                    all_errors.extend([f"Receipt {idx}: {e}" for e in result.errors])

            except Exception as e:
                total_processed += 1
                total_failed += 1
                all_errors.append(f"Receipt {idx}: {str(e)}")

        return ImportResult(
            success=total_failed == 0,
            records_processed=total_processed,
            records_imported=total_imported,
            records_failed=total_failed,
            errors=all_errors,
            warnings=all_warnings
        )


    # =========================================================================
    # SALES REFUND IMPORT (at_type=3 - Money going OUT to customer)
    # Mirrors import_sales_receipt but with inverted signs
    # =========================================================================

    def import_sales_refund(
        self,
        bank_account: str,
        customer_account: str,
        amount_pounds: float,
        reference: str,
        post_date: date,
        input_by: str = "IMPORT",
        sales_ledger_control: str = None,
        payment_method: str = "BACS",
        cbtype: str = None,
        validate_only: bool = False,
        comment: str = ""
    ) -> ImportResult:
        """
        Import a sales refund into Opera SQL SE.

        This posts a refund TO a customer (money going out). Creates records in:
        1. aentry (Cashbook Entry Header) - NEGATIVE amount (money out)
        2. atran (Cashbook Transaction) - at_type=3 (SALES_REFUND), NEGATIVE amount
        3. ntran (Nominal Ledger) - Bank CR (-amount), Debtors DR (+amount)
        4. stran (Sales Ledger) - st_trtype='F', POSITIVE value (increases balance)
        5. salloc (Sales Allocation)
        6. atype (Entry counter update)

        Args:
            bank_account: Bank account code (e.g., 'BC010')
            customer_account: Customer account code (e.g., 'A046')
            amount_pounds: Refund amount in POUNDS (e.g., 100.00)
            reference: Your reference
            post_date: Posting date
            input_by: User code for audit trail (max 8 chars)
            sales_ledger_control: Sales ledger control account (auto-detected if None)
            payment_method: Payment method description (default 'BACS')
            cbtype: Cashbook type code from atype. Must be Payment type (ay_type='P').
                   If None, uses first available Payment type.
            validate_only: If True, only validate without inserting
        """
        errors = []
        warnings = []

        # VALIDATE/GET CBTYPE - Sales refund uses PAYMENT category (money going out)
        if cbtype is None:
            cbtype = self.get_default_cbtype('sales_refund')
            if cbtype is None:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=["No Payment type codes found in atype table for sales refund"]
                )
            logger.debug(f"Using default cbtype for sales refund: {cbtype}")

        type_validation = self.validate_cbtype(cbtype, required_category=AtypeCategory.PAYMENT)
        if not type_validation['valid']:
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[type_validation['error']]
            )

        at_type = CashbookTransactionType.SALES_REFUND  # 3.0

        if sales_ledger_control is None:
            from sql_rag.opera_config import get_customer_control_account
            sales_ledger_control = get_customer_control_account(self.sql, customer_account)
            logger.debug(f"Using debtors control for customer {customer_account}: {sales_ledger_control}")

        try:
            # =====================
            # PERIOD POSTING DECISION
            # =====================
            from sql_rag.opera_config import get_period_posting_decision
            posting_decision = get_period_posting_decision(self.sql, post_date, 'SL')

            if not posting_decision.can_post:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[posting_decision.error_message]
                )

            # Validate bank account
            bank_check = self.sql.execute_query(f"""
                SELECT TOP 1 at_acnt FROM atran WITH (NOLOCK)
                WHERE RTRIM(at_acnt) = '{bank_account}'
            """)
            if bank_check.empty:
                warnings.append(f"Bank account '{bank_account}' has not been used before - verify it's correct")

            # Validate customer exists
            sname_check = self.sql.execute_query(f"""
                SELECT sn_name, sn_region, sn_terrtry, sn_custype FROM sname WITH (NOLOCK)
                WHERE RTRIM(sn_account) = '{customer_account}'
            """)
            if not sname_check.empty:
                customer_name = sname_check.iloc[0]['sn_name'].strip()
                customer_region = sname_check.iloc[0]['sn_region'].strip() if sname_check.iloc[0]['sn_region'] else 'K'
                customer_terr = sname_check.iloc[0]['sn_terrtry'].strip() if sname_check.iloc[0]['sn_terrtry'] else '001'
                customer_type = sname_check.iloc[0]['sn_custype'].strip() if sname_check.iloc[0]['sn_custype'] else 'DD1'
            else:
                customer_check = self.sql.execute_query(f"""
                    SELECT TOP 1 at_account, at_name FROM atran WITH (NOLOCK)
                    WHERE RTRIM(at_account) = '{customer_account}'
                """)
                if not customer_check.empty:
                    customer_name = customer_check.iloc[0]['at_name'].strip()
                    customer_region = 'K'
                    customer_terr = '001'
                    customer_type = 'DD1'
                else:
                    errors.append(f"Customer account '{customer_account}' not found")

            if errors:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=errors
                )

            if validate_only:
                return ImportResult(
                    success=True,
                    records_processed=1,
                    records_imported=1,
                    warnings=["Validation passed - no records inserted (validate_only=True)"]
                )

            # CONVERT AMOUNTS
            amount_pence = int(round(amount_pounds * 100))

            if isinstance(post_date, str):
                post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

            year = post_date.year
            period = post_date.month

            now = datetime.now()
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')
            date_str = now.strftime('%Y-%m-%d')
            time_str = now.strftime('%H:%M:%S')

            unique_ids = OperaUniqueIdGenerator.generate_multiple(5)
            aentry_id = unique_ids[0]
            atran_unique = unique_ids[1]
            ntran_pstid_debit = unique_ids[2]
            ntran_pstid_credit = unique_ids[3]
            stran_unique = unique_ids[4]

            with self.sql.engine.begin() as conn:
                conn.execute(text(get_lock_timeout_sql()))

                entry_number = self.increment_atype_entry(conn, cbtype)

                journal_result = conn.execute(text("""
                    SELECT ISNULL(MAX(nt_jrnl), 0) + 1 as next_journal
                    FROM ntran WITH (UPDLOCK, ROWLOCK)
                """))
                next_journal = journal_result.scalar() or 1

                # 1. aentry - NEGATIVE amount (money going out)
                # ae_complet should only be 1 if we're posting to nominal ledger
                ae_complet_flag = 1 if posting_decision.post_to_nominal else 0
                safe_comment = comment.replace(chr(10), ' ').replace(chr(13), ' ').replace("'", "''") if comment else ''
                aentry_sql = f"""
                    INSERT INTO aentry (
                        ae_acnt, ae_cntr, ae_cbtype, ae_entry, ae_reclnum,
                        ae_lstdate, ae_frstat, ae_tostat, ae_statln, ae_entref,
                        ae_value, ae_recbal, ae_remove, ae_tmpstat, ae_complet,
                        ae_postgrp, sq_crdate, sq_crtime, sq_cruser, ae_comment,
                        ae_payid, ae_batchid, ae_brwptr, datecreated, datemodified, state
                    ) VALUES (
                        '{bank_account}', '    ', '{cbtype}', '{entry_number}', 0,
                        '{post_date}', 0, 0, 0, '{reference[:20]}',
                        {-amount_pence}, 0, 0, 0, {ae_complet_flag},
                        0, '{date_str}', '{time_str[:8]}', '{input_by[:8]}', '{safe_comment[:40]}',
                        0, 0, '  ', '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(aentry_sql))

                # 2. atran - at_type=3 (SALES_REFUND), NEGATIVE amount
                trnref = f"{customer_name[:30]:<30}BACS       (RT)     "
                atran_sql = f"""
                    INSERT INTO atran (
                        at_acnt, at_cntr, at_cbtype, at_entry, at_inputby,
                        at_type, at_pstdate, at_sysdate, at_tperiod, at_value,
                        at_disc, at_fcurr, at_fcexch, at_fcmult, at_fcdec,
                        at_account, at_name, at_comment, at_payee, at_payname,
                        at_sort, at_number, at_remove, at_chqprn, at_chqlst,
                        at_bacprn, at_ccdprn, at_ccdno, at_payslp, at_pysprn,
                        at_cash, at_remit, at_unique, at_postgrp, at_ccauth,
                        at_refer, at_srcco, at_ecb, at_ecbtype, at_atpycd,
                        at_bsref, at_bsname, at_vattycd, at_project, at_job,
                        at_bic, at_iban, at_memo, datecreated, datemodified, state
                    ) VALUES (
                        '{bank_account}', '    ', '{cbtype}', '{entry_number}', '{input_by[:8]}',
                        {at_type}, '{post_date}', '{post_date}', 1, {-amount_pence},
                        0, '   ', 1.0, 0, 2,
                        '{customer_account}', '{customer_name[:35]}', '{safe_comment[:50]}', '        ', '',
                        '        ', '         ', 0, 0, 0,
                        0, 0, '', 0, 0,
                        0, 0, '{atran_unique}', 0, '0       ',
                        '{reference[:20]}', 'I', 0, ' ', '      ',
                        '', '', '  ', '        ', '        ',
                        '', '', '', '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(atran_sql))

                # 3. Nominal postings - Bank CR (money out), Debtors DR (increase asset - owed back)
                ntran_comment = f"{(safe_comment or reference)[:50]:<50}"
                ntran_trnref = f"{customer_name[:30]:<30}BACS       (RT)     "

                if posting_decision.post_to_nominal:
                    # Bank account CREDIT (-amount, money going out)
                    ntran_bank_sql = f"""
                        INSERT INTO ntran (
                            nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                            nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                            nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                            nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                            nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                            nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                            nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                            nt_distrib, datecreated, datemodified, state
                        ) VALUES (
                            '{bank_account}', '    ', 'B ', 'BC', {next_journal},
                            '', '{input_by[:10]}', 'A', '{ntran_comment}', '{ntran_trnref}',
                            '{post_date}', {-amount_pounds}, {year}, {period}, 0,
                            0, 0, '   ', 0, 0,
                            0, 0, 'I', '', '        ',
                            '        ', 'S', 0, '{ntran_pstid_debit}', 0,
                            0, 0, 0, 0, 0,
                            0, '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(ntran_bank_sql))
                    # Update nacnt balance for bank account (CREDIT - money going out)
                    self.update_nacnt_balance(conn, bank_account, -amount_pounds, period)
                    # Update nbank balance (refund decreases bank balance)
                    self.update_nbank_balance(conn, bank_account, -amount_pounds)

                    # Debtors control DEBIT (+amount, increasing debtors)
                    ntran_control_sql = f"""
                        INSERT INTO ntran (
                            nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                            nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                            nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                            nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                            nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                            nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                            nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                            nt_distrib, datecreated, datemodified, state
                        ) VALUES (
                            '{sales_ledger_control}', '    ', 'B ', 'BB', {next_journal},
                            '', '{input_by[:10]}', 'A', '{ntran_comment}', '{ntran_trnref}',
                            '{post_date}', {amount_pounds}, {year}, {period}, 0,
                            0, 0, '   ', 0, 0,
                            0, 0, 'I', '', '        ',
                            '        ', 'S', 0, '{ntran_pstid_credit}', 0,
                            0, 0, 0, 0, 0,
                            0, '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(ntran_control_sql))
                    # Update nacnt balance for sales ledger control (DEBIT - increasing debtors)
                    self.update_nacnt_balance(conn, sales_ledger_control, amount_pounds, period)

                # 4. anoml transfer file
                if posting_decision.post_to_transfer_file:
                    done_flag = posting_decision.transfer_file_done_flag
                    jrnl_num = next_journal if posting_decision.post_to_nominal else 0

                    # Bank account (credit - money going out)
                    anoml_bank_sql = f"""
                        INSERT INTO anoml (
                            ax_nacnt, ax_ncntr, ax_source, ax_date, ax_value, ax_tref,
                            ax_comment, ax_done, ax_fcurr, ax_fvalue, ax_fcrate, ax_fcmult, ax_fcdec,
                            ax_srcco, ax_unique, ax_project, ax_job, ax_jrnl, ax_nlpdate,
                            datecreated, datemodified, state
                        ) VALUES (
                            '{bank_account}', '    ', 'S', '{post_date}', {-amount_pounds}, '{reference[:20]}',
                            '{ntran_comment[:50]}', '{done_flag}', '   ', 0, 0, 0, 0,
                            'I', '{atran_unique}', '        ', '        ', {jrnl_num}, '{post_date}',
                            '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(anoml_bank_sql))

                    # Debtors control (debit - increasing debtors)
                    anoml_control_sql = f"""
                        INSERT INTO anoml (
                            ax_nacnt, ax_ncntr, ax_source, ax_date, ax_value, ax_tref,
                            ax_comment, ax_done, ax_fcurr, ax_fvalue, ax_fcrate, ax_fcmult, ax_fcdec,
                            ax_srcco, ax_unique, ax_project, ax_job, ax_jrnl, ax_nlpdate,
                            datecreated, datemodified, state
                        ) VALUES (
                            '{sales_ledger_control}', '    ', 'S', '{post_date}', {amount_pounds}, '{reference[:20]}',
                            '{ntran_comment[:50]}', '{done_flag}', '   ', 0, 0, 0, 0,
                            'I', '{atran_unique}', '        ', '        ', {jrnl_num}, '{post_date}',
                            '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(anoml_control_sql))

                # 5. stran - st_trtype='F' (Refund), POSITIVE value (increases customer debt)
                stran_memo = f"Refund to customer - {reference[:50]}"
                stran_sql = f"""
                    INSERT INTO stran (
                        st_account, st_trdate, st_trref, st_custref, st_trtype,
                        st_trvalue, st_vatval, st_trbal, st_paid, st_crdate,
                        st_advance, st_memo, st_payflag, st_set1day, st_set1,
                        st_set2day, st_set2, st_dueday, st_fcurr, st_fcrate,
                        st_fcdec, st_fcval, st_fcbal, st_fcmult, st_dispute,
                        st_edi, st_editx, st_edivn, st_txtrep, st_binrep,
                        st_advallc, st_cbtype, st_entry, st_unique, st_region,
                        st_terr, st_type, st_fadval, st_delacc, st_euro,
                        st_payadvl, st_eurind, st_origcur, st_fullamt, st_fullcb,
                        st_fullnar, st_cash, st_rcode, st_ruser, st_revchrg,
                        st_nlpdate, st_adjsv, st_fcvat, st_taxpoin,
                        datecreated, datemodified, state
                    ) VALUES (
                        '{customer_account}', '{post_date}', '{reference[:20]}', '{payment_method[:20]}', 'F',
                        {amount_pounds}, 0, {amount_pounds}, ' ', '{post_date}',
                        'N', '{stran_memo[:200]}', 0, 0, 0,
                        0, 0, '{post_date}', '   ', 0,
                        0, 0, 0, 0, 0,
                        0, 0, 0, '', 0,
                        0, '{cbtype}', '{entry_number}', '{stran_unique}', '{customer_region[:3]}',
                        '{customer_terr[:3]}', '{customer_type[:3]}', 0, '{customer_account}', 0,
                        0, ' ', '   ', 0, '  ',
                        '          ', 0, '    ', '        ', 0,
                        '{post_date}', 0, 0, '{post_date}',
                        '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(stran_sql))

                # Get stran ID for salloc
                stran_id_result = conn.execute(text("""
                    SELECT TOP 1 id FROM stran
                    WHERE st_unique = :unique_id
                    ORDER BY id DESC
                """), {"unique_id": stran_unique})
                stran_row = stran_id_result.fetchone()
                stran_id = stran_row[0] if stran_row else 0

                # 6. salloc - al_type='F' (Refund)
                salloc_sql = f"""
                    INSERT INTO salloc (
                        al_account, al_date, al_ref1, al_ref2, al_type,
                        al_val, al_payind, al_payflag, al_payday, al_fcurr,
                        al_fval, al_fdec, al_advind, al_acnt, al_cntr,
                        al_preprd, al_unique, al_adjsv,
                        datecreated, datemodified, state
                    ) VALUES (
                        '{customer_account}', '{post_date}', '{reference[:20]}', '{payment_method[:20]}', 'F',
                        {amount_pounds}, 'A', 0, '{post_date}', '   ',
                        0, 0, 0, '{bank_account}', '    ',
                        0, {stran_id}, 0,
                        '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(salloc_sql))

                # 7. Update sname balance - INCREASE (refund adds to what they owe)
                sname_update_sql = f"""
                    UPDATE sname WITH (ROWLOCK)
                    SET sn_currbal = sn_currbal + {amount_pounds},
                        datemodified = '{now_str}'
                    WHERE RTRIM(sn_account) = '{customer_account}'
                """
                conn.execute(text(sname_update_sql))

            tables_updated = ["aentry", "atran", "stran", "salloc", "sname"]
            if posting_decision.post_to_nominal:
                tables_updated.insert(2, "ntran (2)")
            if posting_decision.post_to_transfer_file:
                tables_updated.append("anoml (2)")

            posting_mode = "Current period - posted to nominal" if posting_decision.post_to_nominal else "Different period - transfer file only (pending NL post)"

            logger.info(f"Successfully imported sales refund: {entry_number} for £{amount_pounds:.2f} - {posting_mode}")

            return ImportResult(
                success=True,
                records_processed=1,
                records_imported=1,
                entry_number=entry_number,
                transaction_ref=reference[:20],
                warnings=[
                    f"Entry number: {entry_number}",
                    f"Journal number: {next_journal}",
                    f"Amount: £{amount_pounds:.2f}",
                    f"Posting mode: {posting_mode}",
                    f"Tables updated: {', '.join(tables_updated)}"
                ]
            )

        except Exception as e:
            logger.error(f"Failed to import sales refund: {e}")
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[str(e)]
            )

    # =========================================================================
    # PURCHASE PAYMENT IMPORT (Replicates Opera's exact pattern)
    # =========================================================================

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
        validate_only: bool = False,
        comment: str = ""
    ) -> ImportResult:
        """
        Import a purchase payment into Opera SQL SE.

        This replicates the EXACT pattern Opera uses when a user manually
        enters a supplier payment, creating records in:
        1. aentry (Cashbook Entry Header)
        2. atran (Cashbook Transaction)
        3. ntran (Nominal Ledger - 2 rows for double-entry)
        4. ptran (Purchase Ledger Transaction)
        5. palloc (Purchase Allocation)
        6. atype (Entry counter update)

        Args:
            bank_account: Bank account code (e.g., 'BC010')
            supplier_account: Supplier account code (e.g., 'W034')
            amount_pounds: Payment amount in POUNDS (e.g., 100.00)
            reference: Your reference (e.g., 'test')
            post_date: Posting date
            input_by: User code for audit trail (max 8 chars)
            creditors_control: Creditors control account (auto-detected from config if None)
            payment_type: Payment type description (default 'Direct Cr')
            cbtype: Cashbook type code from atype (e.g., 'P5'). Must be Payment type (ay_type='P').
                   If None, uses first available Payment type.
            validate_only: If True, only validate without inserting

        Returns:
            ImportResult with details of the operation
        """
        errors = []
        warnings = []

        # =====================
        # VALIDATE/GET CBTYPE
        # =====================
        if cbtype is None:
            cbtype = self.get_default_cbtype('purchase_payment')
            if cbtype is None:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=["No Payment type codes found in atype table"]
                )
            logger.debug(f"Using default cbtype for purchase payment: {cbtype}")

        # Validate the type code
        type_validation = self.validate_cbtype(cbtype, required_category=AtypeCategory.PAYMENT)
        if not type_validation['valid']:
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[type_validation['error']]
            )

        # Get the correct at_type for purchase payments (always 5.0)
        at_type = CashbookTransactionType.PURCHASE_PAYMENT

        # Get control account - check supplier profile first, then fall back to default
        if creditors_control is None:
            from sql_rag.opera_config import get_supplier_control_account
            creditors_control = get_supplier_control_account(self.sql, supplier_account)
            logger.debug(f"Using creditors control for supplier {supplier_account}: {creditors_control}")

        try:
            # =====================
            # PERIOD POSTING DECISION
            # =====================
            from sql_rag.opera_config import get_period_posting_decision
            posting_decision = get_period_posting_decision(self.sql, post_date, 'PL')

            if not posting_decision.can_post:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[posting_decision.error_message]
                )

            # =====================
            # VALIDATION (using NOLOCK to avoid blocking other users)
            # =====================

            # Validate bank account exists
            bank_check = self.sql.execute_query(f"""
                SELECT TOP 1 at_acnt FROM atran WITH (NOLOCK)
                WHERE RTRIM(at_acnt) = '{bank_account}'
            """)
            if bank_check.empty:
                warnings.append(f"Bank account '{bank_account}' has not been used before - verify it's correct")

            # Validate supplier exists by checking pname (Purchase Ledger Master) first
            pname_check = self.sql.execute_query(f"""
                SELECT pn_name FROM pname WITH (NOLOCK)
                WHERE RTRIM(pn_account) = '{supplier_account}'
            """)
            if not pname_check.empty:
                supplier_name = pname_check.iloc[0]['pn_name'].strip()
            else:
                # Fall back to atran history if not in pname
                supplier_check = self.sql.execute_query(f"""
                    SELECT TOP 1 at_account, at_name FROM atran WITH (NOLOCK)
                    WHERE RTRIM(at_account) = '{supplier_account}'
                """)
                if not supplier_check.empty:
                    supplier_name = supplier_check.iloc[0]['at_name'].strip()
                else:
                    errors.append(f"Supplier account '{supplier_account}' not found")

            if errors:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=errors
                )

            if validate_only:
                return ImportResult(
                    success=True,
                    records_processed=1,
                    records_imported=1,
                    warnings=["Validation passed - no records inserted (validate_only=True)"]
                )

            # =====================
            # CONVERT AMOUNTS & PREPARE VARIABLES
            # =====================
            amount_pence = int(round(amount_pounds * 100))

            # DEBUG LOGGING - capture exact values being used
            logger.info(f"PURCHASE_PAYMENT_DEBUG: Starting import for supplier={supplier_account}")
            logger.info(f"PURCHASE_PAYMENT_DEBUG: amount_pounds={amount_pounds}, amount_pence={amount_pence}")
            logger.info(f"PURCHASE_PAYMENT_DEBUG: supplier_name={supplier_name}")

            if isinstance(post_date, str):
                post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

            year = post_date.year
            period = post_date.month

            now = datetime.now()
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')
            date_str = now.strftime('%Y-%m-%d')
            time_str = now.strftime('%H:%M:%S')

            # Sanitize comment for SQL - remove newlines, escape quotes
            safe_comment = comment.replace(chr(10), ' ').replace(chr(13), ' ').replace("'", "''") if comment else ''

            # Build trnref like Opera does
            # Use comment (full description) for nt_cmnt, fall back to reference
            ntran_comment = f"{(safe_comment or reference)[:50]:<50}"
            ntran_trnref = f"{supplier_name[:30]:<30}{payment_type:<10}(RT)     "

            # Generate unique IDs (Opera uses same unique ID for atran and ptran)
            unique_ids = OperaUniqueIdGenerator.generate_multiple(3)
            atran_unique = unique_ids[0]  # Shared between atran and ptran
            ntran_pstid_bank = unique_ids[1]
            ntran_pstid_control = unique_ids[2]

            # =====================
            # EXECUTE ALL OPERATIONS IN A SINGLE TRANSACTION WITH LOCKING
            # =====================
            with self.sql.engine.begin() as conn:
                # Set lock timeout to prevent indefinite blocking of other users
                conn.execute(text(get_lock_timeout_sql()))

                # Get next entry number from atype and increment counter
                # This is the proper Opera way - atype tracks entry numbers per type
                entry_number = self.increment_atype_entry(conn, cbtype)

                # Get next journal number with UPDLOCK, ROWLOCK for minimal blocking
                journal_result = conn.execute(text("""
                    SELECT ISNULL(MAX(nt_jrnl), 0) + 1 as next_journal
                    FROM ntran WITH (UPDLOCK, ROWLOCK)
                """))
                next_journal = journal_result.scalar() or 1

                # 1. INSERT INTO aentry (Cashbook Entry Header) - NEGATIVE for payment
                # ae_complet should only be 1 if we're posting to nominal ledger
                ae_complet_flag = 1 if posting_decision.post_to_nominal else 0
                aentry_sql = f"""
                    INSERT INTO aentry (
                        ae_acnt, ae_cntr, ae_cbtype, ae_entry, ae_reclnum,
                        ae_lstdate, ae_frstat, ae_tostat, ae_statln, ae_entref,
                        ae_value, ae_recbal, ae_remove, ae_tmpstat, ae_complet,
                        ae_postgrp, sq_crdate, sq_crtime, sq_cruser, ae_comment,
                        ae_payid, ae_batchid, ae_brwptr, datecreated, datemodified, state
                    ) VALUES (
                        '{bank_account}', '    ', '{cbtype}', '{entry_number}', 0,
                        '{post_date}', 0, 0, 0, '{reference[:20]}',
                        {-amount_pence}, 0, 0, 0, {ae_complet_flag},
                        0, '{date_str}', '{time_str[:8]}', '{input_by[:8]}', '{safe_comment[:40]}',
                        0, 0, '  ', '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(aentry_sql))

                # 2. INSERT INTO atran (Cashbook Transaction)
                atran_sql = f"""
                    INSERT INTO atran (
                        at_acnt, at_cntr, at_cbtype, at_entry, at_inputby,
                        at_type, at_pstdate, at_sysdate, at_tperiod, at_value,
                        at_disc, at_fcurr, at_fcexch, at_fcmult, at_fcdec,
                        at_account, at_name, at_comment, at_payee, at_payname,
                        at_sort, at_number, at_remove, at_chqprn, at_chqlst,
                        at_bacprn, at_ccdprn, at_ccdno, at_payslp, at_pysprn,
                        at_cash, at_remit, at_unique, at_postgrp, at_ccauth,
                        at_refer, at_srcco, at_ecb, at_ecbtype, at_atpycd,
                        at_bsref, at_bsname, at_vattycd, at_project, at_job,
                        at_bic, at_iban, at_memo, datecreated, datemodified, state
                    ) VALUES (
                        '{bank_account}', '    ', '{cbtype}', '{entry_number}', '{input_by[:8]}',
                        {at_type}, '{post_date}', '{post_date}', 1, {-amount_pence},
                        0, '   ', 1.0, 0, 2,
                        '{supplier_account}', '{supplier_name[:35]}', '{safe_comment[:50]}', '        ', '',
                        '        ', '         ', 0, 0, 0,
                        0, 0, '', 0, 0,
                        0, 0, '{atran_unique}', 0, '0       ',
                        '{reference[:20]}', 'I', 0, ' ', '      ',
                        '', '', '  ', '        ', '        ',
                        '', '', '', '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(atran_sql))

                # 3. Nominal postings - CONDITIONAL based on period posting decision
                if posting_decision.post_to_nominal:
                    # INSERT INTO ntran - CREDIT Bank (money going out)
                    # nt_type='B ', nt_subt='BC', nt_posttyp='P'
                    logger.info(f"PURCHASE_PAYMENT_DEBUG: ntran bank value will be: {-amount_pounds}")
                    ntran_bank_sql = f"""
                        INSERT INTO ntran (
                            nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                            nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                            nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                            nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                            nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                            nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                            nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                            nt_distrib, datecreated, datemodified, state
                        ) VALUES (
                            '{bank_account}', '    ', 'B ', 'BC', {next_journal},
                            '', '{input_by[:10]}', 'A', '{ntran_comment}', '{ntran_trnref}',
                            '{post_date}', {-amount_pounds}, {year}, {period}, 0,
                            0, 0, '   ', 0, 0,
                            0, 0, 'I', '', '        ',
                            '        ', 'P', 0, '{ntran_pstid_bank}', 0,
                            0, 0, 0, 0, 0,
                            0, '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(ntran_bank_sql))
                    # Update nacnt balance for bank account (CREDIT - money going out)
                    self.update_nacnt_balance(conn, bank_account, -amount_pounds, period)
                    # Update nbank balance (payment decreases bank balance)
                    self.update_nbank_balance(conn, bank_account, -amount_pounds)

                    # INSERT INTO ntran - DEBIT Creditors Control (CA030)
                    # nt_type='C ', nt_subt='CA', nt_posttyp='P'
                    logger.info(f"PURCHASE_PAYMENT_DEBUG: ntran control value will be: {amount_pounds}")
                    ntran_control_sql = f"""
                        INSERT INTO ntran (
                            nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                            nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                            nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                            nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                            nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                            nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                            nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                            nt_distrib, datecreated, datemodified, state
                        ) VALUES (
                            '{creditors_control}', '    ', 'C ', 'CA', {next_journal},
                            '', '{input_by[:10]}', 'A', '{ntran_comment}', '{ntran_trnref}',
                            '{post_date}', {amount_pounds}, {year}, {period}, 0,
                            0, 0, '   ', 0, 0,
                            0, 0, 'I', '', '        ',
                            '        ', 'P', 0, '{ntran_pstid_control}', 0,
                            0, 0, 0, 0, 0,
                            0, '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(ntran_control_sql))
                    # Update nacnt balance for creditors control (DEBIT - reducing liability)
                    self.update_nacnt_balance(conn, creditors_control, amount_pounds, period)

                # 4. INSERT INTO transfer files (anoml only - Opera uses anoml for both sides of payment)
                if posting_decision.post_to_transfer_file:
                    done_flag = posting_decision.transfer_file_done_flag
                    jrnl_num = next_journal if posting_decision.post_to_nominal else 0

                    # anoml record 1 - Bank account (credit - money going out)
                    anoml_bank_sql = f"""
                        INSERT INTO anoml (
                            ax_nacnt, ax_ncntr, ax_source, ax_date, ax_value, ax_tref,
                            ax_comment, ax_done, ax_fcurr, ax_fvalue, ax_fcrate, ax_fcmult, ax_fcdec,
                            ax_srcco, ax_unique, ax_project, ax_job, ax_jrnl, ax_nlpdate,
                            datecreated, datemodified, state
                        ) VALUES (
                            '{bank_account}', '    ', 'P', '{post_date}', {-amount_pounds}, '{reference[:20]}',
                            '{ntran_comment[:50]}', '{done_flag}', '   ', 0, 0, 0, 0,
                            'I', '{atran_unique}', '        ', '        ', {jrnl_num}, '{post_date}',
                            '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(anoml_bank_sql))

                    # anoml record 2 - Creditors control account (debit - reducing liability)
                    anoml_control_sql = f"""
                        INSERT INTO anoml (
                            ax_nacnt, ax_ncntr, ax_source, ax_date, ax_value, ax_tref,
                            ax_comment, ax_done, ax_fcurr, ax_fvalue, ax_fcrate, ax_fcmult, ax_fcdec,
                            ax_srcco, ax_unique, ax_project, ax_job, ax_jrnl, ax_nlpdate,
                            datecreated, datemodified, state
                        ) VALUES (
                            '{creditors_control}', '    ', 'P', '{post_date}', {amount_pounds}, '{reference[:20]}',
                            '{ntran_comment[:50]}', '{done_flag}', '   ', 0, 0, 0, 0,
                            'I', '{atran_unique}', '        ', '        ', {jrnl_num}, '{post_date}',
                            '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(anoml_control_sql))

                # 5. INSERT INTO ptran (Purchase Ledger Transaction)
                ptran_sql = f"""
                    INSERT INTO ptran (
                        pt_account, pt_trdate, pt_trref, pt_supref, pt_trtype,
                        pt_trvalue, pt_vatval, pt_trbal, pt_paid, pt_crdate,
                        pt_advance, pt_payflag, pt_set1day, pt_set1, pt_set2day,
                        pt_set2, pt_held, pt_fcurr, pt_fcrate, pt_fcdec,
                        pt_fcval, pt_fcbal, pt_adval, pt_fadval, pt_fcmult,
                        pt_cbtype, pt_entry, pt_unique, pt_suptype, pt_euro,
                        pt_payadvl, pt_origcur, pt_eurind, pt_revchrg, pt_nlpdate,
                        pt_adjsv, pt_vatset1, pt_vatset2, pt_pyroute, pt_fcvat,
                        datecreated, datemodified, state
                    ) VALUES (
                        '{supplier_account}', '{post_date}', '{reference[:20]}', '{payment_type[:20]}', 'P',
                        {-amount_pounds}, 0, {-amount_pounds}, ' ', '{post_date}',
                        'N', 0, 0, 0, 0,
                        0, ' ', '   ', 0, 0,
                        0, 0, 0, 0, 0,
                        '{cbtype}', '{entry_number}', '{atran_unique}', '   ', 0,
                        0, '   ', ' ', 0, '{post_date}',
                        0, 0, 0, 0, 0,
                        '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(ptran_sql))

                # Get the ptran ID we just inserted
                ptran_id_result = conn.execute(text("""
                    SELECT TOP 1 id FROM ptran
                    WHERE pt_unique = :unique_id
                    ORDER BY id DESC
                """), {"unique_id": atran_unique})
                ptran_row = ptran_id_result.fetchone()
                ptran_id = ptran_row[0] if ptran_row else 0

                # 6. INSERT INTO palloc (Purchase Allocation)
                palloc_sql = f"""
                    INSERT INTO palloc (
                        al_account, al_date, al_ref1, al_ref2, al_type,
                        al_val, al_dval, al_origval, al_payind, al_payflag,
                        al_payday, al_ctype, al_rem, al_cheq, al_payee,
                        al_fcurr, al_fval, al_fdval, al_forigvl, al_fdec,
                        al_unique, al_acnt, al_cntr, al_advind, al_advtran,
                        al_preprd, al_bacsid, al_adjsv,
                        datecreated, datemodified, state
                    ) VALUES (
                        '{supplier_account}', '{post_date}', '{reference[:20]}', '{payment_type[:20]}', 'P',
                        {-amount_pounds}, 0, {-amount_pounds}, 'P', 0,
                        '{post_date}', 'O', ' ', ' ', '{supplier_name[:30]}',
                        '   ', 0, 0, 0, 0,
                        {ptran_id}, '{bank_account}', '    ', 0, 0,
                        0, 0, 0,
                        '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(palloc_sql))

                # 7. UPDATE pname.pn_currbal with ROWLOCK (reduce supplier balance - we paid them)
                pname_update_sql = f"""
                    UPDATE pname WITH (ROWLOCK)
                    SET pn_currbal = pn_currbal - {amount_pounds},
                        datemodified = '{now_str}'
                    WHERE RTRIM(pn_account) = '{supplier_account}'
                """
                conn.execute(text(pname_update_sql))

            # Build list of tables updated based on what was actually done
            tables_updated = ["aentry", "atran", "ptran", "palloc", "pname"]
            if posting_decision.post_to_nominal:
                tables_updated.insert(2, "ntran (2)")
            if posting_decision.post_to_transfer_file:
                tables_updated.append("anoml (2)")  # Opera uses anoml for both bank and control

            posting_mode = "Current period - posted to nominal" if posting_decision.post_to_nominal else "Different period - transfer file only (pending NL post)"

            logger.info(f"Successfully imported purchase payment: {entry_number} for £{amount_pounds:.2f} - {posting_mode}")

            return ImportResult(
                success=True,
                records_processed=1,
                records_imported=1,
                entry_number=entry_number,
                transaction_ref=reference[:20],
                warnings=[
                    f"Entry number: {entry_number}",
                    f"Journal number: {next_journal}",
                    f"Amount: £{amount_pounds:.2f}",
                    f"Posting mode: {posting_mode}",
                    f"Tables updated: {', '.join(tables_updated)}"
                ]
            )

        except Exception as e:
            logger.error(f"Failed to import purchase payment: {e}")
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[str(e)]
            )

    # =========================================================================
    # NOMINAL ENTRY IMPORT (Bank charges, interest, etc.)
    # =========================================================================

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
        validate_only: bool = False
    ) -> ImportResult:
        """
        Import a nominal-only entry into Opera SQL SE.

        This creates a cashbook entry that posts directly to a nominal account
        without going through sales or purchase ledger. Typical uses:
        - Bank charges (payment to nominal)
        - Interest received (receipt from nominal)
        - Miscellaneous payments/receipts

        Creates records in:
        1. aentry (Cashbook Entry Header)
        2. atran (Cashbook Transaction)
        3. ntran (Nominal Ledger - 2 rows for double-entry)
        4. atype (Entry counter update)

        Args:
            bank_account: Bank account code (e.g., 'BC010')
            nominal_account: Nominal account code (e.g., '7502' for bank charges)
            amount_pounds: Amount in POUNDS (always positive)
            reference: Transaction reference (max 20 chars)
            post_date: Posting date
            description: Transaction description/comment
            input_by: User code for audit trail (max 8 chars)
            is_receipt: If True, money IN (nominal receipt). If False, money OUT (nominal payment)
            cbtype: Cashbook type code from atype. If None, uses first available.
            validate_only: If True, only validate without inserting

        Returns:
            ImportResult with details of the operation
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
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[f"No {required_category} type codes found in atype table"]
                )
            logger.debug(f"Using default cbtype for {type_name}: {cbtype}")

        # Validate the type code
        type_validation = self.validate_cbtype(cbtype, required_category=required_category)
        if not type_validation['valid']:
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[type_validation['error']]
            )

        try:
            # =====================
            # PERIOD POSTING DECISION
            # =====================
            from sql_rag.opera_config import get_period_posting_decision
            posting_decision = get_period_posting_decision(self.sql, post_date, 'NL')

            if not posting_decision.can_post:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[posting_decision.error_message]
                )

            year = post_date.year
            period = post_date.month

            # =====================
            # VALIDATION
            # =====================

            # Validate bank account exists
            bank_check = self.sql.execute_query(f"""
                SELECT TOP 1 nk_acnt, nk_desc FROM nbank WITH (NOLOCK)
                WHERE RTRIM(nk_acnt) = '{bank_account}'
            """)
            if bank_check.empty:
                errors.append(f"Bank account '{bank_account}' not found in nbank")
            else:
                bank_name = bank_check.iloc[0]['nk_desc'].strip() if bank_check.iloc[0]['nk_desc'] else bank_account

            # Validate nominal account exists
            nominal_check = self.sql.execute_query(f"""
                SELECT TOP 1 na_acnt, na_desc FROM nacnt WITH (NOLOCK)
                WHERE RTRIM(na_acnt) = '{nominal_account}'
            """)
            if nominal_check.empty:
                errors.append(f"Nominal account '{nominal_account}' not found in nacnt")
            else:
                nominal_name = nominal_check.iloc[0]['na_desc'].strip() if nominal_check.iloc[0]['na_desc'] else nominal_account

            if errors:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=errors
                )

            if validate_only:
                return ImportResult(
                    success=True,
                    records_processed=1,
                    records_imported=1,
                    warnings=["Validation passed - no records inserted (validate_only=True)"]
                )

            # =====================
            # CONVERT AMOUNTS & PREPARE VARIABLES
            # =====================
            amount_pence = int(round(amount_pounds * 100))

            # For payments (money out), value is negative in aentry/atran
            # For receipts (money in), value is positive
            entry_value = amount_pence if is_receipt else -amount_pence

            if isinstance(post_date, str):
                post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

            now = datetime.now()
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')
            date_str = now.strftime('%Y-%m-%d')
            time_str = now.strftime('%H:%M:%S')

            # Generate unique IDs
            unique_ids = OperaUniqueIdGenerator.generate_multiple(3)
            atran_unique = unique_ids[0]
            ntran_pstid_bank = unique_ids[1]
            ntran_pstid_nominal = unique_ids[2]

            logger.info(f"NOMINAL_ENTRY_DEBUG: Starting import - bank={bank_account}, nominal={nominal_account}, amount={amount_pounds}, is_receipt={is_receipt}")

            # =====================
            # INSERT RECORDS WITHIN TRANSACTION
            # =====================

            with self.sql.engine.begin() as conn:
                # Set lock timeout
                conn.execute(text(get_lock_timeout_sql()))

                # Get entry number from atype
                entry_number = self.increment_atype_entry(conn, cbtype)

                # Get next journal number
                journal_result = conn.execute(text("""
                    SELECT ISNULL(MAX(nt_jrnl), 0) + 1 as next_journal
                    FROM ntran WITH (UPDLOCK, ROWLOCK)
                """))
                next_journal = journal_result.scalar() or 1

                # ae_complet = 1 if posting to nominal
                ae_complet_flag = 1 if posting_decision.post_to_nominal else 0

                # 1. INSERT INTO aentry (Cashbook Entry Header)
                aentry_sql = f"""
                    INSERT INTO aentry (
                        ae_acnt, ae_cntr, ae_cbtype, ae_entry, ae_reclnum,
                        ae_lstdate, ae_frstat, ae_tostat, ae_statln, ae_entref,
                        ae_value, ae_recbal, ae_remove, ae_tmpstat, ae_complet,
                        ae_postgrp, sq_crdate, sq_crtime, sq_cruser, ae_comment,
                        ae_payid, ae_batchid, ae_brwptr, datecreated, datemodified, state
                    ) VALUES (
                        '{bank_account}', '    ', '{cbtype}', '{entry_number}', 0,
                        '{post_date}', 0, 0, 0, '{reference[:20]}',
                        {entry_value}, 0, 0, 0, {ae_complet_flag},
                        0, '{date_str}', '{time_str[:8]}', '{input_by[:8]}', '{description.replace(chr(10), " ").replace(chr(13), " ")[:40].replace("'", "''")}',
                        0, 0, '  ', '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(aentry_sql))

                # 2. INSERT INTO atran (Cashbook Transaction)
                # at_name is the nominal account description
                atran_sql = f"""
                    INSERT INTO atran (
                        at_acnt, at_cntr, at_cbtype, at_entry, at_inputby,
                        at_type, at_pstdate, at_sysdate, at_tperiod, at_value,
                        at_disc, at_fcurr, at_fcexch, at_fcmult, at_fcdec,
                        at_account, at_name, at_comment, at_payee, at_payname,
                        at_sort, at_number, at_remove, at_chqprn, at_chqlst,
                        at_bacprn, at_ccdprn, at_ccdno, at_payslp, at_pysprn,
                        at_cash, at_remit, at_unique, at_postgrp, at_ccauth,
                        at_refer, at_srcco, at_ecb, at_ecbtype, at_atpycd,
                        at_bsref, at_bsname, at_vattycd, at_project, at_job,
                        at_bic, at_iban, at_memo, datecreated, datemodified, state
                    ) VALUES (
                        '{bank_account}', '    ', '{cbtype}', '{entry_number}', '{input_by[:8]}',
                        {at_type}, '{post_date}', '{post_date}', 1, {entry_value},
                        0, '   ', 1.0, 0, 2,
                        '{nominal_account}', '{nominal_name[:35]}', '{description.replace(chr(10), " ").replace(chr(13), " ")[:50].replace("'", "''")}', '        ', '',
                        '        ', '         ', 0, 0, 0,
                        0, 0, '', 0, 0,
                        0, 0, '{atran_unique}', 0, '0       ',
                        '{reference[:20]}', 'I', 0, ' ', '      ',
                        '', '', '  ', '        ', '        ',
                        '', '', '', '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(atran_sql))

                # 3. INSERT INTO ntran (Nominal Ledger - 2 rows for double-entry)
                if posting_decision.post_to_nominal:
                    ntran_comment = f"{description.replace(chr(10), ' ').replace(chr(13), ' ')[:40].replace(chr(39), chr(39)+chr(39))}" if description else f"{reference[:40]}"
                    ntran_trnref = f"{nominal_name[:30]:<30}{reference[:20]:<20}"

                    # For PAYMENT (money out):
                    #   Bank account: CREDIT (negative) - money leaving
                    #   Nominal account: DEBIT (positive) - expense
                    # For RECEIPT (money in):
                    #   Bank account: DEBIT (positive) - money arriving
                    #   Nominal account: CREDIT (negative) - income

                    if is_receipt:
                        bank_ntran_value = amount_pounds  # Debit
                        nominal_ntran_value = -amount_pounds  # Credit
                    else:
                        bank_ntran_value = -amount_pounds  # Credit
                        nominal_ntran_value = amount_pounds  # Debit

                    # Bank account ntran
                    ntran_bank_sql = f"""
                        INSERT INTO ntran (
                            nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                            nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                            nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                            nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                            nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                            nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                            nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                            nt_distrib, datecreated, datemodified, state
                        ) VALUES (
                            '{bank_account}', '    ', 'B ', 'BB', {next_journal},
                            '', '{input_by[:10]}', 'A', '{ntran_comment}', '{ntran_trnref}',
                            '{post_date}', {bank_ntran_value}, {year}, {period}, 0,
                            0, 0, '   ', 0, 0,
                            0, 0, 'I', '', '        ',
                            '        ', 'S', 0, '{ntran_pstid_bank}', 0,
                            0, 0, 0, 0, 0,
                            0, '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(ntran_bank_sql))
                    # Update nacnt balance for bank
                    self.update_nacnt_balance(conn, bank_account, bank_ntran_value, period)
                    # Update nbank balance
                    self.update_nbank_balance(conn, bank_account, bank_ntran_value)

                    # Nominal account ntran
                    ntran_nominal_sql = f"""
                        INSERT INTO ntran (
                            nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                            nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                            nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                            nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                            nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                            nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                            nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                            nt_distrib, datecreated, datemodified, state
                        ) VALUES (
                            '{nominal_account}', '    ', 'B ', 'BB', {next_journal},
                            '', '{input_by[:10]}', 'A', '{ntran_comment}', '{ntran_trnref}',
                            '{post_date}', {nominal_ntran_value}, {year}, {period}, 0,
                            0, 0, '   ', 0, 0,
                            0, 0, 'I', '', '        ',
                            '        ', 'S', 0, '{ntran_pstid_nominal}', 0,
                            0, 0, 0, 0, 0,
                            0, '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(ntran_nominal_sql))
                    # Update nacnt balance for nominal account
                    self.update_nacnt_balance(conn, nominal_account, nominal_ntran_value, period)

            # Build success message
            tables_updated = ["aentry", "atran"]
            if posting_decision.post_to_nominal:
                tables_updated.append("ntran (2)")

            entry_type = "Nominal Receipt" if is_receipt else "Nominal Payment"
            return ImportResult(
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
            logger.error(f"Failed to import nominal entry: {e}")
            import traceback
            traceback.print_exc()
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[str(e)]
            )

    # =========================================================================
    # BANK TRANSFER IMPORT
    # =========================================================================

    def import_bank_transfer(
        self,
        source_bank: str,
        dest_bank: str,
        amount_pounds: float,
        reference: str,
        post_date: date,
        comment: str = "",
        input_by: str = "SQLRAG",
        post_to_nominal: bool = True
    ) -> ImportResult:
        """
        Import a bank transfer between two Opera bank accounts.

        Creates paired entries in:
        - aentry (2 records)
        - atran (2 records with at_type=8)
        - anoml (2 records with ax_source='A')
        - ntran (2 records if post_to_nominal=True)
        - Updates nbank balances for both accounts
        - Updates nacnt balances for both accounts

        Args:
            source_bank: Source bank account code (e.g., 'BC010')
            dest_bank: Destination bank account code (e.g., 'BC020')
            amount_pounds: Transfer amount (positive value)
            reference: Transaction reference (max 20 chars)
            post_date: Date of transfer
            comment: Optional comment
            input_by: User who entered (max 8 chars)
            post_to_nominal: Whether to post to nominal ledger

        Returns:
            ImportResult with source_entry, dest_entry, success status
        """
        errors = []
        warnings = []

        # Validate source != dest
        if source_bank.strip() == dest_bank.strip():
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=["Source and destination bank accounts must be different"]
            )

        # Validate amount is positive
        if amount_pounds <= 0:
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=["Transfer amount must be positive"]
            )

        try:
            # =====================
            # VALIDATE BANK ACCOUNTS
            # =====================
            source_bank_check = self.sql.execute_query(f"""
                SELECT nk_acnt, nk_name, nk_fcurr FROM nbank WITH (NOLOCK)
                WHERE RTRIM(nk_acnt) = '{source_bank}'
            """)
            if source_bank_check.empty:
                errors.append(f"Source bank account '{source_bank}' not found in nbank")
            else:
                source_name = source_bank_check.iloc[0]['nk_name'].strip()
                if source_bank_check.iloc[0]['nk_fcurr'] and source_bank_check.iloc[0]['nk_fcurr'].strip():
                    errors.append(f"Source bank '{source_bank}' is a foreign currency account - transfers not supported")

            dest_bank_check = self.sql.execute_query(f"""
                SELECT nk_acnt, nk_name, nk_fcurr FROM nbank WITH (NOLOCK)
                WHERE RTRIM(nk_acnt) = '{dest_bank}'
            """)
            if dest_bank_check.empty:
                errors.append(f"Destination bank account '{dest_bank}' not found in nbank")
            else:
                dest_name = dest_bank_check.iloc[0]['nk_name'].strip()
                if dest_bank_check.iloc[0]['nk_fcurr'] and dest_bank_check.iloc[0]['nk_fcurr'].strip():
                    errors.append(f"Destination bank '{dest_bank}' is a foreign currency account - transfers not supported")

            if errors:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=errors
                )

            # =====================
            # PERIOD POSTING DECISION
            # =====================
            from sql_rag.opera_config import get_period_posting_decision
            posting_decision = get_period_posting_decision(self.sql, post_date, 'CB')

            if not posting_decision.can_post:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[posting_decision.error_message]
                )

            # =====================
            # CONVERT AMOUNTS
            # =====================
            amount_pence = int(round(amount_pounds * 100))  # aentry/atran use pence

            # Format date
            if isinstance(post_date, str):
                post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

            year = post_date.year
            period = post_date.month

            # Get current timestamp for created/modified
            now = datetime.now()
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')
            date_str = now.strftime('%Y-%m-%d')
            time_str = now.strftime('%H:%M:%S')

            # Generate unique IDs - SHARED unique ID for both entries (Opera pattern)
            shared_unique = OperaUniqueIdGenerator.generate()
            ntran_pstid_source = OperaUniqueIdGenerator.generate()
            ntran_pstid_dest = OperaUniqueIdGenerator.generate()

            # Bank transfer uses cbtype='T1' (Bank Transfer)
            cbtype = 'T1'

            # =====================
            # INSERT RECORDS WITHIN TRANSACTION
            # =====================

            with self.sql.engine.begin() as conn:
                # Set lock timeout to prevent indefinite blocking of other users
                conn.execute(text(get_lock_timeout_sql()))

                # Get TWO sequential entry numbers from atype
                source_entry = self.increment_atype_entry(conn, cbtype)
                dest_entry = self.increment_atype_entry(conn, cbtype)

                # Get next journal number
                journal_result = conn.execute(text("""
                    SELECT ISNULL(MAX(nt_jrnl), 0) + 1 as next_journal
                    FROM ntran WITH (UPDLOCK, ROWLOCK)
                """))
                next_journal = journal_result.scalar() or 1

                # ae_complet flag: 1 if posting to nominal, 0 otherwise
                ae_complet_flag = 1 if post_to_nominal else 0

                # Build transaction reference
                trnref = f"{reference[:20]:<30}Bank Transfer(RT)    "

                # =====================
                # 1. SOURCE BANK - aentry (money going OUT = negative)
                # =====================
                aentry_source_sql = f"""
                    INSERT INTO aentry (
                        ae_acnt, ae_cntr, ae_cbtype, ae_entry, ae_reclnum,
                        ae_lstdate, ae_frstat, ae_tostat, ae_statln, ae_entref,
                        ae_value, ae_recbal, ae_remove, ae_tmpstat, ae_complet,
                        ae_postgrp, sq_crdate, sq_crtime, sq_cruser, ae_comment,
                        ae_payid, ae_batchid, ae_brwptr, datecreated, datemodified, state
                    ) VALUES (
                        '{source_bank}', '    ', '{cbtype}', '{source_entry}', 0,
                        '{post_date}', 0, 0, 0, '{reference[:20]}',
                        {-amount_pence}, 0, 0, 0, {ae_complet_flag},
                        0, '{date_str}', '{time_str[:8]}', '{input_by[:8]}', '{comment.replace(chr(10), " ").replace(chr(13), " ")[:40].replace("'", "''")}',
                        0, 0, '  ', '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(aentry_source_sql))

                # =====================
                # 2. DEST BANK - aentry (money coming IN = positive)
                # =====================
                aentry_dest_sql = f"""
                    INSERT INTO aentry (
                        ae_acnt, ae_cntr, ae_cbtype, ae_entry, ae_reclnum,
                        ae_lstdate, ae_frstat, ae_tostat, ae_statln, ae_entref,
                        ae_value, ae_recbal, ae_remove, ae_tmpstat, ae_complet,
                        ae_postgrp, sq_crdate, sq_crtime, sq_cruser, ae_comment,
                        ae_payid, ae_batchid, ae_brwptr, datecreated, datemodified, state
                    ) VALUES (
                        '{dest_bank}', '    ', '{cbtype}', '{dest_entry}', 0,
                        '{post_date}', 0, 0, 0, '{reference[:20]}',
                        {amount_pence}, 0, 0, 0, {ae_complet_flag},
                        0, '{date_str}', '{time_str[:8]}', '{input_by[:8]}', '{comment.replace(chr(10), " ").replace(chr(13), " ")[:40].replace("'", "''")}',
                        0, 0, '  ', '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(aentry_dest_sql))

                # =====================
                # 3. SOURCE BANK - atran (at_type=8, at_account=counterpart bank)
                # =====================
                atran_source_sql = f"""
                    INSERT INTO atran (
                        at_acnt, at_cntr, at_cbtype, at_entry, at_inputby,
                        at_type, at_pstdate, at_sysdate, at_tperiod, at_value,
                        at_disc, at_fcurr, at_fcexch, at_fcmult, at_fcdec,
                        at_account, at_name, at_comment, at_payee, at_payname,
                        at_sort, at_number, at_remove, at_chqprn, at_chqlst,
                        at_bacprn, at_ccdprn, at_ccdno, at_payslp, at_pysprn,
                        at_cash, at_remit, at_unique, at_postgrp, at_ccauth,
                        at_refer, at_srcco, at_ecb, at_ecbtype, at_atpycd,
                        at_bsref, at_bsname, at_vattycd, at_project, at_job,
                        at_bic, at_iban, at_memo, datecreated, datemodified, state
                    ) VALUES (
                        '{source_bank}', '    ', '{cbtype}', '{source_entry}', '{input_by[:8]}',
                        8, '{post_date}', '{post_date}', 1, {-amount_pence},
                        0, '   ', 1.0, 0, 2,
                        '{dest_bank}', '{dest_name[:35]}', '{comment.replace(chr(10), " ").replace(chr(13), " ")[:40].replace("'", "''")}', '        ', '',
                        '        ', '         ', 0, 0, 0,
                        0, 0, '', 0, 0,
                        0, 0, '{shared_unique}', 0, '0       ',
                        '{reference[:20]}', 'I', 0, ' ', '      ',
                        '', '', '  ', '        ', '        ',
                        '', '', '', '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(atran_source_sql))

                # =====================
                # 4. DEST BANK - atran (at_type=8, at_account=counterpart bank)
                # =====================
                atran_dest_sql = f"""
                    INSERT INTO atran (
                        at_acnt, at_cntr, at_cbtype, at_entry, at_inputby,
                        at_type, at_pstdate, at_sysdate, at_tperiod, at_value,
                        at_disc, at_fcurr, at_fcexch, at_fcmult, at_fcdec,
                        at_account, at_name, at_comment, at_payee, at_payname,
                        at_sort, at_number, at_remove, at_chqprn, at_chqlst,
                        at_bacprn, at_ccdprn, at_ccdno, at_payslp, at_pysprn,
                        at_cash, at_remit, at_unique, at_postgrp, at_ccauth,
                        at_refer, at_srcco, at_ecb, at_ecbtype, at_atpycd,
                        at_bsref, at_bsname, at_vattycd, at_project, at_job,
                        at_bic, at_iban, at_memo, datecreated, datemodified, state
                    ) VALUES (
                        '{dest_bank}', '    ', '{cbtype}', '{dest_entry}', '{input_by[:8]}',
                        8, '{post_date}', '{post_date}', 1, {amount_pence},
                        0, '   ', 1.0, 0, 2,
                        '{source_bank}', '{source_name[:35]}', '{comment.replace(chr(10), " ").replace(chr(13), " ")[:40].replace("'", "''")}', '        ', '',
                        '        ', '         ', 0, 0, 0,
                        0, 0, '', 0, 0,
                        0, 0, '{shared_unique}', 0, '0       ',
                        '{reference[:20]}', 'I', 0, ' ', '      ',
                        '', '', '  ', '        ', '        ',
                        '', '', '', '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(atran_dest_sql))

                # =====================
                # 5. anoml records (ax_source='A' for Admin/Transfer)
                # =====================
                done_flag = 'Y' if post_to_nominal else 'N'
                jrnl_num = next_journal if post_to_nominal else 0

                # anoml for source bank (negative - money going out)
                anoml_source_sql = f"""
                    INSERT INTO anoml (
                        ax_nacnt, ax_ncntr, ax_source, ax_date, ax_value, ax_tref,
                        ax_comment, ax_done, ax_fcurr, ax_fvalue, ax_fcrate, ax_fcmult, ax_fcdec,
                        ax_srcco, ax_unique, ax_project, ax_job, ax_jrnl, ax_nlpdate,
                        datecreated, datemodified, state
                    ) VALUES (
                        '{source_bank}', '    ', 'A', '{post_date}', {-amount_pounds}, '{reference[:20]}',
                        '{comment.replace(chr(10), " ").replace(chr(13), " ")[:40].replace("'", "''")}', '{done_flag}', '   ', 0, 0, 0, 0,
                        'I', '{shared_unique}', '        ', '        ', {jrnl_num}, '{post_date}',
                        '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(anoml_source_sql))

                # anoml for dest bank (positive - money coming in)
                anoml_dest_sql = f"""
                    INSERT INTO anoml (
                        ax_nacnt, ax_ncntr, ax_source, ax_date, ax_value, ax_tref,
                        ax_comment, ax_done, ax_fcurr, ax_fvalue, ax_fcrate, ax_fcmult, ax_fcdec,
                        ax_srcco, ax_unique, ax_project, ax_job, ax_jrnl, ax_nlpdate,
                        datecreated, datemodified, state
                    ) VALUES (
                        '{dest_bank}', '    ', 'A', '{post_date}', {amount_pounds}, '{reference[:20]}',
                        '{comment.replace(chr(10), " ").replace(chr(13), " ")[:40].replace("'", "''")}', '{done_flag}', '   ', 0, 0, 0, 0,
                        'I', '{shared_unique}', '        ', '        ', {jrnl_num}, '{post_date}',
                        '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(anoml_dest_sql))

                # =====================
                # 6. Nominal postings (if post_to_nominal=True)
                # =====================
                if post_to_nominal:
                    ntran_comment = f"{reference[:50]:<50}"

                    # ntran for source bank (negative/debit - balance decreasing)
                    ntran_source_sql = f"""
                        INSERT INTO ntran (
                            nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                            nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                            nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                            nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                            nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                            nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                            nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                            nt_distrib, datecreated, datemodified, state
                        ) VALUES (
                            '{source_bank}', '    ', 'B ', 'BC', {next_journal},
                            '', '{input_by[:10]}', 'A', '{ntran_comment}', '{trnref}',
                            '{post_date}', {-amount_pounds}, {year}, {period}, 0,
                            0, 0, '   ', 0, 0,
                            0, 0, 'I', '', '        ',
                            '        ', 'T', 0, '{ntran_pstid_source}', 0,
                            0, 0, 0, 0, 0,
                            0, '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(ntran_source_sql))
                    # Update nacnt for source bank (negative value = credit)
                    self.update_nacnt_balance(conn, source_bank, -amount_pounds, period)
                    # Update nbank for source bank (decrease balance)
                    self.update_nbank_balance(conn, source_bank, -amount_pounds)

                    # ntran for dest bank (positive/credit - balance increasing)
                    ntran_dest_sql = f"""
                        INSERT INTO ntran (
                            nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                            nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                            nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                            nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                            nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                            nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                            nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                            nt_distrib, datecreated, datemodified, state
                        ) VALUES (
                            '{dest_bank}', '    ', 'B ', 'BC', {next_journal},
                            '', '{input_by[:10]}', 'A', '{ntran_comment}', '{trnref}',
                            '{post_date}', {amount_pounds}, {year}, {period}, 0,
                            0, 0, '   ', 0, 0,
                            0, 0, 'I', '', '        ',
                            '        ', 'T', 0, '{ntran_pstid_dest}', 0,
                            0, 0, 0, 0, 0,
                            0, '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(ntran_dest_sql))
                    # Update nacnt for dest bank (positive value = debit)
                    self.update_nacnt_balance(conn, dest_bank, amount_pounds, period)
                    # Update nbank for dest bank (increase balance)
                    self.update_nbank_balance(conn, dest_bank, amount_pounds)

            # Build result
            tables_updated = ["aentry (2)", "atran (2)", "anoml (2)"]
            if post_to_nominal:
                tables_updated.append("ntran (2)")
                tables_updated.append("nacnt (2)")
                tables_updated.append("nbank (2)")

            posting_mode = "Posted to nominal" if post_to_nominal else "Transfer file only"

            logger.info(f"Successfully imported bank transfer: {source_entry}/{dest_entry} for £{amount_pounds:.2f} from {source_bank} to {dest_bank}")

            return ImportResult(
                success=True,
                records_processed=1,
                records_imported=1,
                entry_number=f"{source_entry}/{dest_entry}",
                transaction_ref=reference[:20],
                warnings=[
                    f"Source entry: {source_entry}",
                    f"Dest entry: {dest_entry}",
                    f"Amount: £{amount_pounds:.2f}",
                    f"From: {source_bank} ({source_name})",
                    f"To: {dest_bank} ({dest_name})",
                    f"Posting mode: {posting_mode}",
                    f"Tables updated: {', '.join(tables_updated)}"
                ]
            )

        except Exception as e:
            logger.error(f"Failed to import bank transfer: {e}")
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[str(e)]
            )

    # =========================================================================
    # STOCK ADJUSTMENTS
    # =========================================================================

    def import_stock_adjustment(
        self,
        stock_ref: str,
        warehouse: str,
        quantity: float,
        reason: str = "Adjustment",
        reference: str = "",
        adjust_date: date = None,
        unit_cost: float = None,
        input_by: str = "SQLRAG"
    ) -> ImportResult:
        """
        Import a stock adjustment (increase or decrease) for a product.

        Creates records in:
        - ctran (stock transaction audit trail)
        - Updates cstwh (warehouse stock level)
        - Updates cname (product total stock)

        Args:
            stock_ref: Product stock reference (e.g., 'WIDGET001')
            warehouse: Warehouse code (e.g., 'MAIN')
            quantity: Adjustment quantity (positive = add, negative = remove)
            reason: Reason for adjustment (stored in ct_comnt)
            reference: Optional reference number
            adjust_date: Date of adjustment (defaults to today)
            unit_cost: Unit cost for this adjustment (uses product cost if None)
            input_by: User who made adjustment (max 8 chars)

        Returns:
            ImportResult with transaction details
        """
        errors = []
        warnings = []

        if adjust_date is None:
            adjust_date = date.today()

        if isinstance(adjust_date, str):
            adjust_date = datetime.strptime(adjust_date, '%Y-%m-%d').date()

        try:
            # =====================
            # VALIDATE PRODUCT EXISTS
            # =====================
            product_check = self.sql.execute_query(f"""
                SELECT cn_ref, cn_desc, cn_cost, cn_instock, cn_freest
                FROM cname WITH (NOLOCK)
                WHERE RTRIM(cn_ref) = '{stock_ref}'
            """)
            if product_check.empty:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[f"Product '{stock_ref}' not found in cname"]
                )

            product_desc = product_check.iloc[0]['cn_desc'].strip() if product_check.iloc[0]['cn_desc'] else ''
            product_cost = float(product_check.iloc[0]['cn_cost'] or 0)
            current_total_stock = float(product_check.iloc[0]['cn_instock'] or 0)
            current_total_free = float(product_check.iloc[0]['cn_freest'] or 0)

            # Use provided cost or product's standard cost
            if unit_cost is None:
                unit_cost = product_cost

            # =====================
            # VALIDATE WAREHOUSE EXISTS
            # =====================
            warehouse_check = self.sql.execute_query(f"""
                SELECT cw_code, cw_desc FROM cware WITH (NOLOCK)
                WHERE RTRIM(cw_code) = '{warehouse}'
            """)
            if warehouse_check.empty:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[f"Warehouse '{warehouse}' not found in cware"]
                )

            warehouse_desc = warehouse_check.iloc[0]['cw_desc'].strip() if warehouse_check.iloc[0]['cw_desc'] else ''

            # =====================
            # GET CURRENT WAREHOUSE STOCK
            # =====================
            cstwh_check = self.sql.execute_query(f"""
                SELECT cs_instock, cs_freest
                FROM cstwh WITH (NOLOCK)
                WHERE RTRIM(cs_ref) = '{stock_ref}' AND RTRIM(cs_whar) = '{warehouse}'
            """)

            if cstwh_check.empty:
                # No cstwh record - will need to create one
                current_wh_stock = 0
                current_wh_free = 0
                need_create_cstwh = True
            else:
                current_wh_stock = float(cstwh_check.iloc[0]['cs_instock'] or 0)
                current_wh_free = float(cstwh_check.iloc[0]['cs_freest'] or 0)
                need_create_cstwh = False

            # Check for negative resulting stock
            new_wh_stock = current_wh_stock + quantity
            new_total_stock = current_total_stock + quantity
            if new_wh_stock < 0:
                warnings.append(f"Warning: Adjustment will result in negative warehouse stock ({new_wh_stock:.2f})")
            if new_total_stock < 0:
                warnings.append(f"Warning: Adjustment will result in negative total stock ({new_total_stock:.2f})")

            # =====================
            # GENERATE UNIQUE ID AND TIMESTAMP
            # =====================
            unique_id = OperaUniqueIdGenerator.generate()
            now = datetime.now()
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')
            time_str = now.strftime('%H:%M:%S')

            # =====================
            # INSERT/UPDATE WITHIN TRANSACTION
            # =====================
            with self.sql.engine.begin() as conn:
                conn.execute(text(get_lock_timeout_sql()))

                # 1. CREATE CTRAN RECORD (Stock Transaction)
                ctran_sql = f"""
                    INSERT INTO ctran (
                        ct_ref, ct_loc, ct_type, ct_date, ct_crdate, ct_quan,
                        ct_cost, ct_sell, ct_comnt, ct_referen, ct_account,
                        ct_time, ct_unique, datecreated, datemodified, state
                    ) VALUES (
                        '{stock_ref}', '{warehouse}', 'A', '{adjust_date}', '{adjust_date}', {quantity},
                        {unit_cost}, 0, '{reason[:35]}', '{reference[:10]}', '{input_by[:8]}',
                        '{time_str[:8]}', '{unique_id}', '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(ctran_sql))

                # 2. UPDATE OR CREATE CSTWH (Warehouse Stock)
                if need_create_cstwh:
                    # Create new cstwh record
                    cstwh_sql = f"""
                        INSERT INTO cstwh (
                            cs_ref, cs_whar, cs_instock, cs_freest, cs_alloc, cs_order,
                            cs_saleord, datecreated, datemodified, state
                        ) VALUES (
                            '{stock_ref}', '{warehouse}', {quantity}, {quantity}, 0, 0,
                            0, '{now_str}', '{now_str}', 1
                        )
                    """
                else:
                    # Update existing cstwh record
                    cstwh_sql = f"""
                        UPDATE cstwh WITH (ROWLOCK)
                        SET cs_instock = cs_instock + {quantity},
                            cs_freest = cs_freest + {quantity},
                            datemodified = '{now_str}'
                        WHERE RTRIM(cs_ref) = '{stock_ref}' AND RTRIM(cs_whar) = '{warehouse}'
                    """
                conn.execute(text(cstwh_sql))

                # 3. UPDATE CNAME (Product Total Stock)
                cname_sql = f"""
                    UPDATE cname WITH (ROWLOCK)
                    SET cn_instock = cn_instock + {quantity},
                        cn_freest = cn_freest + {quantity},
                        datemodified = '{now_str}'
                    WHERE RTRIM(cn_ref) = '{stock_ref}'
                """
                conn.execute(text(cname_sql))

            logger.info(f"Stock adjustment: {stock_ref} in {warehouse} by {quantity:+.2f} (reason: {reason})")

            return ImportResult(
                success=True,
                records_processed=1,
                records_imported=1,
                transaction_ref=unique_id,
                warnings=[
                    f"Product: {stock_ref} - {product_desc}",
                    f"Warehouse: {warehouse} - {warehouse_desc}",
                    f"Adjustment: {quantity:+.2f} units",
                    f"New warehouse stock: {new_wh_stock:.2f}",
                    f"New total stock: {new_total_stock:.2f}",
                    f"Transaction ID: {unique_id}"
                ] + warnings
            )

        except Exception as e:
            logger.error(f"Failed to import stock adjustment: {e}")
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[str(e)]
            )

    def import_stock_transfer(
        self,
        stock_ref: str,
        from_warehouse: str,
        to_warehouse: str,
        quantity: float,
        reason: str = "Transfer",
        reference: str = "",
        transfer_date: date = None,
        unit_cost: float = None,
        input_by: str = "SQLRAG"
    ) -> ImportResult:
        """
        Import a stock transfer between warehouses.

        Creates records in:
        - ctran (2 records: issue from source, receipt to destination)
        - Updates cstwh for both warehouses
        - cname totals unchanged (movement within company)

        Args:
            stock_ref: Product stock reference
            from_warehouse: Source warehouse code
            to_warehouse: Destination warehouse code
            quantity: Quantity to transfer (must be positive)
            reason: Reason for transfer
            reference: Optional reference number
            transfer_date: Date of transfer (defaults to today)
            unit_cost: Unit cost (uses product cost if None)
            input_by: User who made transfer

        Returns:
            ImportResult with transaction details
        """
        errors = []
        warnings = []

        if quantity <= 0:
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=["Transfer quantity must be positive"]
            )

        if from_warehouse.strip() == to_warehouse.strip():
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=["Source and destination warehouse must be different"]
            )

        if transfer_date is None:
            transfer_date = date.today()

        if isinstance(transfer_date, str):
            transfer_date = datetime.strptime(transfer_date, '%Y-%m-%d').date()

        try:
            # =====================
            # VALIDATE PRODUCT EXISTS
            # =====================
            product_check = self.sql.execute_query(f"""
                SELECT cn_ref, cn_desc, cn_cost
                FROM cname WITH (NOLOCK)
                WHERE RTRIM(cn_ref) = '{stock_ref}'
            """)
            if product_check.empty:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[f"Product '{stock_ref}' not found"]
                )

            product_desc = product_check.iloc[0]['cn_desc'].strip() if product_check.iloc[0]['cn_desc'] else ''
            product_cost = float(product_check.iloc[0]['cn_cost'] or 0)

            if unit_cost is None:
                unit_cost = product_cost

            # =====================
            # VALIDATE WAREHOUSES
            # =====================
            for wh, wh_name in [(from_warehouse, 'Source'), (to_warehouse, 'Destination')]:
                wh_check = self.sql.execute_query(f"""
                    SELECT cw_code FROM cware WITH (NOLOCK)
                    WHERE RTRIM(cw_code) = '{wh}'
                """)
                if wh_check.empty:
                    errors.append(f"{wh_name} warehouse '{wh}' not found")

            if errors:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=errors
                )

            # =====================
            # CHECK SOURCE WAREHOUSE STOCK
            # =====================
            source_check = self.sql.execute_query(f"""
                SELECT cs_instock, cs_freest
                FROM cstwh WITH (NOLOCK)
                WHERE RTRIM(cs_ref) = '{stock_ref}' AND RTRIM(cs_whar) = '{from_warehouse}'
            """)

            if source_check.empty:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[f"No stock record for {stock_ref} in warehouse {from_warehouse}"]
                )

            source_stock = float(source_check.iloc[0]['cs_instock'] or 0)
            source_free = float(source_check.iloc[0]['cs_freest'] or 0)

            if source_free < quantity:
                warnings.append(f"Warning: Transfer quantity ({quantity:.2f}) exceeds free stock ({source_free:.2f})")

            # =====================
            # CHECK/CREATE DESTINATION WAREHOUSE RECORD
            # =====================
            dest_check = self.sql.execute_query(f"""
                SELECT cs_instock FROM cstwh WITH (NOLOCK)
                WHERE RTRIM(cs_ref) = '{stock_ref}' AND RTRIM(cs_whar) = '{to_warehouse}'
            """)
            need_create_dest = dest_check.empty

            # =====================
            # GENERATE UNIQUE IDs
            # =====================
            unique_out = OperaUniqueIdGenerator.generate()
            unique_in = OperaUniqueIdGenerator.generate()
            now = datetime.now()
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')
            time_str = now.strftime('%H:%M:%S')

            # =====================
            # INSERT/UPDATE WITHIN TRANSACTION
            # =====================
            with self.sql.engine.begin() as conn:
                conn.execute(text(get_lock_timeout_sql()))

                # 1. CTRAN - Issue from source (negative)
                ctran_out_sql = f"""
                    INSERT INTO ctran (
                        ct_ref, ct_loc, ct_type, ct_date, ct_crdate, ct_quan,
                        ct_cost, ct_sell, ct_comnt, ct_referen, ct_account,
                        ct_time, ct_unique, datecreated, datemodified, state
                    ) VALUES (
                        '{stock_ref}', '{from_warehouse}', 'T', '{transfer_date}', '{transfer_date}', {-quantity},
                        {unit_cost}, 0, 'Transfer to {to_warehouse} - {reason[:15]}', '{reference[:10]}', '{input_by[:8]}',
                        '{time_str[:8]}', '{unique_out}', '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(ctran_out_sql))

                # 2. CTRAN - Receipt to destination (positive)
                ctran_in_sql = f"""
                    INSERT INTO ctran (
                        ct_ref, ct_loc, ct_type, ct_date, ct_crdate, ct_quan,
                        ct_cost, ct_sell, ct_comnt, ct_referen, ct_account,
                        ct_time, ct_unique, datecreated, datemodified, state
                    ) VALUES (
                        '{stock_ref}', '{to_warehouse}', 'T', '{transfer_date}', '{transfer_date}', {quantity},
                        {unit_cost}, 0, 'Transfer from {from_warehouse} - {reason[:15]}', '{reference[:10]}', '{input_by[:8]}',
                        '{time_str[:8]}', '{unique_in}', '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(ctran_in_sql))

                # 3. UPDATE SOURCE CSTWH (decrease)
                cstwh_source_sql = f"""
                    UPDATE cstwh WITH (ROWLOCK)
                    SET cs_instock = cs_instock - {quantity},
                        cs_freest = cs_freest - {quantity},
                        datemodified = '{now_str}'
                    WHERE RTRIM(cs_ref) = '{stock_ref}' AND RTRIM(cs_whar) = '{from_warehouse}'
                """
                conn.execute(text(cstwh_source_sql))

                # 4. UPDATE OR CREATE DEST CSTWH (increase)
                if need_create_dest:
                    cstwh_dest_sql = f"""
                        INSERT INTO cstwh (
                            cs_ref, cs_whar, cs_instock, cs_freest, cs_alloc, cs_order,
                            cs_saleord, datecreated, datemodified, state
                        ) VALUES (
                            '{stock_ref}', '{to_warehouse}', {quantity}, {quantity}, 0, 0,
                            0, '{now_str}', '{now_str}', 1
                        )
                    """
                else:
                    cstwh_dest_sql = f"""
                        UPDATE cstwh WITH (ROWLOCK)
                        SET cs_instock = cs_instock + {quantity},
                            cs_freest = cs_freest + {quantity},
                            datemodified = '{now_str}'
                        WHERE RTRIM(cs_ref) = '{stock_ref}' AND RTRIM(cs_whar) = '{to_warehouse}'
                    """
                conn.execute(text(cstwh_dest_sql))

                # Note: cname totals not updated - transfer doesn't change overall company stock

            logger.info(f"Stock transfer: {stock_ref} x{quantity:.2f} from {from_warehouse} to {to_warehouse}")

            return ImportResult(
                success=True,
                records_processed=1,
                records_imported=1,
                transaction_ref=f"{unique_out}/{unique_in}",
                warnings=[
                    f"Product: {stock_ref} - {product_desc}",
                    f"Quantity: {quantity:.2f} units",
                    f"From: {from_warehouse} -> To: {to_warehouse}",
                    f"Transaction IDs: {unique_out} (out), {unique_in} (in)"
                ] + warnings
            )

        except Exception as e:
            logger.error(f"Failed to import stock transfer: {e}")
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[str(e)]
            )

    def import_purchase_payments_batch(
        self,
        payments: List[Dict[str, Any]],
        validate_only: bool = False
    ) -> ImportResult:
        """
        Import multiple purchase payments.

        Each payment dictionary should contain:
            - bank_account: Bank account code (e.g., 'BC010')
            - supplier_account: Supplier account code (e.g., 'P001')
            - amount: Payment amount in POUNDS
            - reference: Your reference
            - post_date: Posting date (YYYY-MM-DD string or date object)
            - input_by: (optional) User code, defaults to 'IMPORT'
        """
        total_processed = 0
        total_imported = 0
        total_failed = 0
        all_errors = []
        all_warnings = []

        for idx, payment in enumerate(payments, 1):
            try:
                result = self.import_purchase_payment(
                    bank_account=payment['bank_account'],
                    supplier_account=payment['supplier_account'],
                    amount_pounds=float(payment['amount']),
                    reference=payment.get('reference', ''),
                    post_date=payment['post_date'],
                    input_by=payment.get('input_by', 'IMPORT'),
                    validate_only=validate_only
                )

                total_processed += 1
                if result.success:
                    total_imported += 1
                    all_warnings.extend([f"Payment {idx}: {w}" for w in result.warnings])
                else:
                    total_failed += 1
                    all_errors.extend([f"Payment {idx}: {e}" for e in result.errors])

            except Exception as e:
                total_processed += 1
                total_failed += 1
                all_errors.append(f"Payment {idx}: {str(e)}")

        return ImportResult(
            success=total_failed == 0,
            records_processed=total_processed,
            records_imported=total_imported,
            records_failed=total_failed,
            errors=all_errors,
            warnings=all_warnings
        )

    # =========================================================================
    # PURCHASE REFUND IMPORT (at_type=6 - Money coming IN from supplier)
    # Mirrors import_purchase_payment but with inverted signs
    # =========================================================================

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
    ) -> ImportResult:
        """
        Import a purchase refund into Opera SQL SE.

        This posts a refund FROM a supplier (money coming in). Creates records in:
        1. aentry (Cashbook Entry Header) - POSITIVE amount (money in)
        2. atran (Cashbook Transaction) - at_type=6 (PURCHASE_REFUND), POSITIVE amount
        3. ntran (Nominal Ledger) - Bank DR (+amount), Creditors CR (-amount)
        4. ptran (Purchase Ledger) - pt_trtype='F', POSITIVE value
        5. palloc (Purchase Allocation)
        6. atype (Entry counter update)

        Args:
            bank_account: Bank account code (e.g., 'BC010')
            supplier_account: Supplier account code (e.g., 'W034')
            amount_pounds: Refund amount in POUNDS (e.g., 100.00)
            reference: Your reference
            post_date: Posting date
            input_by: User code for audit trail (max 8 chars)
            creditors_control: Creditors control account (auto-detected if None)
            payment_type: Payment type description (default 'Direct Cr')
            cbtype: Cashbook type code from atype. Must be Receipt type (ay_type='R').
                   If None, uses first available Receipt type.
            validate_only: If True, only validate without inserting
        """
        errors = []
        warnings = []

        # VALIDATE/GET CBTYPE - Purchase refund uses RECEIPT category (money coming in)
        if cbtype is None:
            cbtype = self.get_default_cbtype('purchase_refund')
            if cbtype is None:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=["No Receipt type codes found in atype table for purchase refund"]
                )
            logger.debug(f"Using default cbtype for purchase refund: {cbtype}")

        type_validation = self.validate_cbtype(cbtype, required_category=AtypeCategory.RECEIPT)
        if not type_validation['valid']:
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[type_validation['error']]
            )

        at_type = CashbookTransactionType.PURCHASE_REFUND  # 6.0

        if creditors_control is None:
            from sql_rag.opera_config import get_supplier_control_account
            creditors_control = get_supplier_control_account(self.sql, supplier_account)
            logger.debug(f"Using creditors control for supplier {supplier_account}: {creditors_control}")

        try:
            # =====================
            # PERIOD POSTING DECISION
            # =====================
            from sql_rag.opera_config import get_period_posting_decision
            posting_decision = get_period_posting_decision(self.sql, post_date, 'PL')

            if not posting_decision.can_post:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[posting_decision.error_message]
                )

            # Validate bank account
            bank_check = self.sql.execute_query(f"""
                SELECT TOP 1 at_acnt FROM atran WITH (NOLOCK)
                WHERE RTRIM(at_acnt) = '{bank_account}'
            """)
            if bank_check.empty:
                warnings.append(f"Bank account '{bank_account}' has not been used before - verify it's correct")

            # Validate supplier exists
            pname_check = self.sql.execute_query(f"""
                SELECT pn_name FROM pname WITH (NOLOCK)
                WHERE RTRIM(pn_account) = '{supplier_account}'
            """)
            if not pname_check.empty:
                supplier_name = pname_check.iloc[0]['pn_name'].strip()
            else:
                supplier_check = self.sql.execute_query(f"""
                    SELECT TOP 1 at_account, at_name FROM atran WITH (NOLOCK)
                    WHERE RTRIM(at_account) = '{supplier_account}'
                """)
                if not supplier_check.empty:
                    supplier_name = supplier_check.iloc[0]['at_name'].strip()
                else:
                    errors.append(f"Supplier account '{supplier_account}' not found")

            if errors:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=errors
                )

            if validate_only:
                return ImportResult(
                    success=True,
                    records_processed=1,
                    records_imported=1,
                    warnings=["Validation passed - no records inserted (validate_only=True)"]
                )

            # CONVERT AMOUNTS
            amount_pence = int(round(amount_pounds * 100))

            if isinstance(post_date, str):
                post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

            year = post_date.year
            period = post_date.month

            now = datetime.now()
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')
            date_str = now.strftime('%Y-%m-%d')
            time_str = now.strftime('%H:%M:%S')

            # Sanitize comment for SQL - remove newlines, escape quotes
            safe_comment = comment.replace(chr(10), ' ').replace(chr(13), ' ').replace("'", "''") if comment else ''

            # Use comment (full description) for nt_cmnt, fall back to reference
            ntran_comment = f"{(safe_comment or reference)[:50]:<50}"
            ntran_trnref = f"{supplier_name[:30]:<30}{payment_type:<10}(RT)     "

            unique_ids = OperaUniqueIdGenerator.generate_multiple(3)
            atran_unique = unique_ids[0]
            ntran_pstid_bank = unique_ids[1]
            ntran_pstid_control = unique_ids[2]

            # ae_complet should only be 1 if we're posting to nominal ledger
            ae_complet_flag = 1 if posting_decision.post_to_nominal else 0

            with self.sql.engine.begin() as conn:
                conn.execute(text(get_lock_timeout_sql()))

                entry_number = self.increment_atype_entry(conn, cbtype)

                journal_result = conn.execute(text("""
                    SELECT ISNULL(MAX(nt_jrnl), 0) + 1 as next_journal
                    FROM ntran WITH (UPDLOCK, ROWLOCK)
                """))
                next_journal = journal_result.scalar() or 1

                # 1. aentry - POSITIVE amount (money coming in)
                aentry_sql = f"""
                    INSERT INTO aentry (
                        ae_acnt, ae_cntr, ae_cbtype, ae_entry, ae_reclnum,
                        ae_lstdate, ae_frstat, ae_tostat, ae_statln, ae_entref,
                        ae_value, ae_recbal, ae_remove, ae_tmpstat, ae_complet,
                        ae_postgrp, sq_crdate, sq_crtime, sq_cruser, ae_comment,
                        ae_payid, ae_batchid, ae_brwptr, datecreated, datemodified, state
                    ) VALUES (
                        '{bank_account}', '    ', '{cbtype}', '{entry_number}', 0,
                        '{post_date}', 0, 0, 0, '{reference[:20]}',
                        {amount_pence}, 0, 0, 0, {ae_complet_flag},
                        0, '{date_str}', '{time_str[:8]}', '{input_by[:8]}', '{safe_comment[:40]}',
                        0, 0, '  ', '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(aentry_sql))

                # 2. atran - at_type=6 (PURCHASE_REFUND), POSITIVE amount
                atran_sql = f"""
                    INSERT INTO atran (
                        at_acnt, at_cntr, at_cbtype, at_entry, at_inputby,
                        at_type, at_pstdate, at_sysdate, at_tperiod, at_value,
                        at_disc, at_fcurr, at_fcexch, at_fcmult, at_fcdec,
                        at_account, at_name, at_comment, at_payee, at_payname,
                        at_sort, at_number, at_remove, at_chqprn, at_chqlst,
                        at_bacprn, at_ccdprn, at_ccdno, at_payslp, at_pysprn,
                        at_cash, at_remit, at_unique, at_postgrp, at_ccauth,
                        at_refer, at_srcco, at_ecb, at_ecbtype, at_atpycd,
                        at_bsref, at_bsname, at_vattycd, at_project, at_job,
                        at_bic, at_iban, at_memo, datecreated, datemodified, state
                    ) VALUES (
                        '{bank_account}', '    ', '{cbtype}', '{entry_number}', '{input_by[:8]}',
                        {at_type}, '{post_date}', '{post_date}', 1, {amount_pence},
                        0, '   ', 1.0, 0, 2,
                        '{supplier_account}', '{supplier_name[:35]}', '{safe_comment[:50]}', '        ', '',
                        '        ', '         ', 0, 0, 0,
                        0, 0, '', 0, 0,
                        0, 0, '{atran_unique}', 0, '0       ',
                        '{reference[:20]}', 'I', 0, ' ', '      ',
                        '', '', '  ', '        ', '        ',
                        '', '', '', '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(atran_sql))

                # 3. Nominal postings - Bank DR (money in), Creditors CR (reduce liability)
                if posting_decision.post_to_nominal:
                    # Bank account DEBIT (+amount, money coming in)
                    ntran_bank_sql = f"""
                        INSERT INTO ntran (
                            nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                            nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                            nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                            nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                            nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                            nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                            nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                            nt_distrib, datecreated, datemodified, state
                        ) VALUES (
                            '{bank_account}', '    ', 'B ', 'BC', {next_journal},
                            '', '{input_by[:10]}', 'A', '{ntran_comment}', '{ntran_trnref}',
                            '{post_date}', {amount_pounds}, {year}, {period}, 0,
                            0, 0, '   ', 0, 0,
                            0, 0, 'I', '', '        ',
                            '        ', 'P', 0, '{ntran_pstid_bank}', 0,
                            0, 0, 0, 0, 0,
                            0, '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(ntran_bank_sql))
                    # Update nacnt balance for bank account (DEBIT - money coming in)
                    self.update_nacnt_balance(conn, bank_account, amount_pounds, period)
                    # Update nbank balance (purchase refund increases bank balance)
                    self.update_nbank_balance(conn, bank_account, amount_pounds)

                    # Creditors control CREDIT (-amount, reducing liability)
                    ntran_control_sql = f"""
                        INSERT INTO ntran (
                            nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                            nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                            nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                            nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                            nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                            nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                            nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                            nt_distrib, datecreated, datemodified, state
                        ) VALUES (
                            '{creditors_control}', '    ', 'C ', 'CA', {next_journal},
                            '', '{input_by[:10]}', 'A', '{ntran_comment}', '{ntran_trnref}',
                            '{post_date}', {-amount_pounds}, {year}, {period}, 0,
                            0, 0, '   ', 0, 0,
                            0, 0, 'I', '', '        ',
                            '        ', 'P', 0, '{ntran_pstid_control}', 0,
                            0, 0, 0, 0, 0,
                            0, '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(ntran_control_sql))
                    # Update nacnt balance for creditors control (CREDIT - increasing liability back)
                    self.update_nacnt_balance(conn, creditors_control, -amount_pounds, period)

                # 4. anoml transfer file
                if posting_decision.post_to_transfer_file:
                    done_flag = posting_decision.transfer_file_done_flag
                    jrnl_num = next_journal if posting_decision.post_to_nominal else 0

                    # Bank account (debit - money coming in)
                    anoml_bank_sql = f"""
                        INSERT INTO anoml (
                            ax_nacnt, ax_ncntr, ax_source, ax_date, ax_value, ax_tref,
                            ax_comment, ax_done, ax_fcurr, ax_fvalue, ax_fcrate, ax_fcmult, ax_fcdec,
                            ax_srcco, ax_unique, ax_project, ax_job, ax_jrnl, ax_nlpdate,
                            datecreated, datemodified, state
                        ) VALUES (
                            '{bank_account}', '    ', 'P', '{post_date}', {amount_pounds}, '{reference[:20]}',
                            '{ntran_comment[:50]}', '{done_flag}', '   ', 0, 0, 0, 0,
                            'I', '{atran_unique}', '        ', '        ', {jrnl_num}, '{post_date}',
                            '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(anoml_bank_sql))

                    # Creditors control (credit - reducing liability)
                    anoml_control_sql = f"""
                        INSERT INTO anoml (
                            ax_nacnt, ax_ncntr, ax_source, ax_date, ax_value, ax_tref,
                            ax_comment, ax_done, ax_fcurr, ax_fvalue, ax_fcrate, ax_fcmult, ax_fcdec,
                            ax_srcco, ax_unique, ax_project, ax_job, ax_jrnl, ax_nlpdate,
                            datecreated, datemodified, state
                        ) VALUES (
                            '{creditors_control}', '    ', 'P', '{post_date}', {-amount_pounds}, '{reference[:20]}',
                            '{ntran_comment[:50]}', '{done_flag}', '   ', 0, 0, 0, 0,
                            'I', '{atran_unique}', '        ', '        ', {jrnl_num}, '{post_date}',
                            '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(anoml_control_sql))

                # 5. ptran - pt_trtype='F' (Refund), POSITIVE value
                ptran_sql = f"""
                    INSERT INTO ptran (
                        pt_account, pt_trdate, pt_trref, pt_supref, pt_trtype,
                        pt_trvalue, pt_vatval, pt_trbal, pt_paid, pt_crdate,
                        pt_advance, pt_payflag, pt_set1day, pt_set1, pt_set2day,
                        pt_set2, pt_held, pt_fcurr, pt_fcrate, pt_fcdec,
                        pt_fcval, pt_fcbal, pt_adval, pt_fadval, pt_fcmult,
                        pt_cbtype, pt_entry, pt_unique, pt_suptype, pt_euro,
                        pt_payadvl, pt_origcur, pt_eurind, pt_revchrg, pt_nlpdate,
                        pt_adjsv, pt_vatset1, pt_vatset2, pt_pyroute, pt_fcvat,
                        datecreated, datemodified, state
                    ) VALUES (
                        '{supplier_account}', '{post_date}', '{reference[:20]}', '{payment_type[:20]}', 'F',
                        {amount_pounds}, 0, {amount_pounds}, ' ', '{post_date}',
                        'N', 0, 0, 0, 0,
                        0, ' ', '   ', 0, 0,
                        0, 0, 0, 0, 0,
                        '{cbtype}', '{entry_number}', '{atran_unique}', '   ', 0,
                        0, '   ', ' ', 0, '{post_date}',
                        0, 0, 0, 0, 0,
                        '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(ptran_sql))

                # Get ptran ID for palloc
                ptran_id_result = conn.execute(text("""
                    SELECT TOP 1 id FROM ptran
                    WHERE pt_unique = :unique_id
                    ORDER BY id DESC
                """), {"unique_id": atran_unique})
                ptran_row = ptran_id_result.fetchone()
                ptran_id = ptran_row[0] if ptran_row else 0

                # 6. palloc - al_type='F' (Refund)
                palloc_sql = f"""
                    INSERT INTO palloc (
                        al_account, al_date, al_ref1, al_ref2, al_type,
                        al_val, al_dval, al_origval, al_payind, al_payflag,
                        al_payday, al_ctype, al_rem, al_cheq, al_payee,
                        al_fcurr, al_fval, al_fdval, al_forigvl, al_fdec,
                        al_unique, al_acnt, al_cntr, al_advind, al_advtran,
                        al_preprd, al_bacsid, al_adjsv,
                        datecreated, datemodified, state
                    ) VALUES (
                        '{supplier_account}', '{post_date}', '{reference[:20]}', '{payment_type[:20]}', 'F',
                        {amount_pounds}, 0, {amount_pounds}, 'P', 0,
                        '{post_date}', 'O', ' ', ' ', '{supplier_name[:30]}',
                        '   ', 0, 0, 0, 0,
                        {ptran_id}, '{bank_account}', '    ', 0, 0,
                        0, 0, 0,
                        '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(palloc_sql))

                # 7. Update pname balance - INCREASE (refund increases what they owe us back)
                pname_update_sql = f"""
                    UPDATE pname WITH (ROWLOCK)
                    SET pn_currbal = pn_currbal + {amount_pounds},
                        datemodified = '{now_str}'
                    WHERE RTRIM(pn_account) = '{supplier_account}'
                """
                conn.execute(text(pname_update_sql))

            tables_updated = ["aentry", "atran", "ptran", "palloc", "pname"]
            if posting_decision.post_to_nominal:
                tables_updated.insert(2, "ntran (2)")
            if posting_decision.post_to_transfer_file:
                tables_updated.append("anoml (2)")

            posting_mode = "Current period - posted to nominal" if posting_decision.post_to_nominal else "Different period - transfer file only (pending NL post)"

            logger.info(f"Successfully imported purchase refund: {entry_number} for £{amount_pounds:.2f} - {posting_mode}")

            return ImportResult(
                success=True,
                records_processed=1,
                records_imported=1,
                entry_number=entry_number,
                transaction_ref=reference[:20],
                warnings=[
                    f"Entry number: {entry_number}",
                    f"Journal number: {next_journal}",
                    f"Amount: £{amount_pounds:.2f}",
                    f"Posting mode: {posting_mode}",
                    f"Tables updated: {', '.join(tables_updated)}"
                ]
            )

        except Exception as e:
            logger.error(f"Failed to import purchase refund: {e}")
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[str(e)]
            )

    # =========================================================================
    # SALES INVOICE IMPORT (Posts to Sales Ledger)
    # Replicates Opera's exact pattern from snapshot analysis
    # Creates: 1x stran, 3x ntran, 1x nhist
    # =========================================================================

    def import_sales_invoice(
        self,
        customer_account: str,
        invoice_number: str,
        net_amount: float,
        vat_amount: float,
        post_date: date,
        customer_ref: str = "",
        sales_nominal: str = "E4030",
        vat_nominal: str = "CA060",
        debtors_control: str = None,
        department: str = "U999",
        payment_days: int = 14,
        input_by: str = "IMPORT",
        description: str = "",
        validate_only: bool = False
    ) -> ImportResult:
        """
        Import a sales invoice into Opera SQL SE.

        This replicates the EXACT pattern Opera uses when a user manually
        enters a sales invoice, creating records in:
        1. stran (Sales Ledger Transaction)
        2. ntran (Nominal Ledger - 3 rows for double-entry)
        3. nhist (Nominal History for P&L account)

        Args:
            customer_account: Customer account code (e.g., 'A046')
            invoice_number: Invoice number/reference
            net_amount: Net amount in POUNDS (before VAT)
            vat_amount: VAT amount in POUNDS
            post_date: Posting date
            customer_ref: Customer's reference (e.g., 'PO12345')
            sales_nominal: Sales P&L account (default 'E4030')
            vat_nominal: VAT output account (default 'CA060')
            debtors_control: Debtors control account (loaded from config if not specified)
            department: Department code (default 'U999')
            payment_days: Days until payment due (default 14)
            input_by: User code for audit trail (max 8 chars)
            description: Invoice description
            validate_only: If True, only validate without inserting

        Returns:
            ImportResult with details of the operation
        """
        errors = []
        warnings = []
        gross_amount = net_amount + vat_amount

        # Get control account - check customer profile first, then fall back to default
        if debtors_control is None:
            from sql_rag.opera_config import get_customer_control_account
            debtors_control = get_customer_control_account(self.sql, customer_account)
            logger.debug(f"Using debtors control for customer {customer_account}: {debtors_control}")

        try:
            # =====================
            # PERIOD VALIDATION
            # =====================
            from sql_rag.opera_config import validate_posting_period
            period_result = validate_posting_period(self.sql, post_date, ledger_type='SL')
            if not period_result.is_valid:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[period_result.error_message]
                )

            # =====================
            # VALIDATION
            # =====================

            # Validate customer exists in sname (Sales Ledger Name/Master)
            customer_check = self.sql.execute_query(f"""
                SELECT TOP 1 sn_name, sn_region, sn_terrtry, sn_custype
                FROM sname
                WHERE RTRIM(sn_account) = '{customer_account}'
            """)
            if customer_check.empty:
                errors.append(f"Customer account '{customer_account}' not found in sname")
            else:
                customer_name = customer_check.iloc[0]['sn_name'].strip()
                customer_region = customer_check.iloc[0]['sn_region'].strip() if customer_check.iloc[0]['sn_region'] else 'K'
                customer_terr = customer_check.iloc[0]['sn_terrtry'].strip() if customer_check.iloc[0]['sn_terrtry'] else '001'
                customer_type = customer_check.iloc[0]['sn_custype'].strip() if customer_check.iloc[0]['sn_custype'] else 'DD1'

            if errors:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=errors
                )

            if validate_only:
                return ImportResult(
                    success=True,
                    records_processed=1,
                    records_imported=1,
                    warnings=["Validation passed - no records inserted (validate_only=True)"]
                )

            # =====================
            # PREPARE VARIABLES
            # =====================

            # Generate unique IDs
            unique_ids = OperaUniqueIdGenerator.generate_multiple(4)
            stran_unique = unique_ids[0]
            ntran_pstid_vat = unique_ids[1]
            ntran_pstid_sales = unique_ids[2]
            ntran_pstid_control = unique_ids[3]

            if isinstance(post_date, str):
                post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

            year = post_date.year
            period = post_date.month

            # Calculate due date
            from datetime import timedelta
            due_date = post_date + timedelta(days=payment_days)

            now = datetime.now()
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')

            # Build reference strings like Opera does
            ntran_comment = f"{invoice_number[:20]:<20} {description[:29]:<29}"
            ntran_trnref = f"{customer_name[:30]:<30}{customer_ref[:20]:<20}"

            # Build memo for stran like Opera does
            stran_memo = f"Analysis of Invoice {invoice_number[:20]:<20} Dated {post_date.strftime('%d/%m/%Y')}  NL Posting Date {post_date.strftime('%d/%m/%Y')}\r\n\r\n"
            stran_memo += f"Sales Code     Goods  <--- VAT --->       Qty        Cost  Nominal          Project   Department     \r\n\r\n"
            stran_memo += f"{debtors_control:<14}{net_amount:>6.2f}  2  {vat_amount:>10.2f}      0.00        0.00  {sales_nominal:<14}{customer_account:<10}{department:<8}\r\n"
            stran_memo += f"                    {description[:20]:<20}\r\n"

            # Get the account type from the sales nominal
            sales_subt = sales_nominal[:2] if len(sales_nominal) >= 2 else 'E4'

            # =====================
            # EXECUTE ALL OPERATIONS IN A SINGLE TRANSACTION WITH LOCKING
            # =====================
            with self.sql.engine.begin() as conn:
                # Set lock timeout to prevent indefinite blocking of other users
                conn.execute(text(get_lock_timeout_sql()))

                # Get next journal number with UPDLOCK, ROWLOCK for minimal blocking
                journal_result = conn.execute(text("""
                    SELECT ISNULL(MAX(nt_jrnl), 0) + 1 as next_journal
                    FROM ntran WITH (UPDLOCK, ROWLOCK)
                """))
                next_journal = journal_result.scalar() or 1

                # 1. INSERT INTO stran (Sales Ledger Transaction)
                stran_sql = f"""
                    INSERT INTO stran (
                        st_account, st_trdate, st_trref, st_custref, st_trtype,
                        st_trvalue, st_vatval, st_trbal, st_paid, st_crdate,
                        st_advance, st_memo, st_payflag, st_set1day, st_set1,
                        st_set2day, st_set2, st_dueday, st_fcurr, st_fcrate,
                        st_fcdec, st_fcval, st_fcbal, st_fcmult, st_dispute,
                        st_edi, st_editx, st_edivn, st_txtrep, st_binrep,
                        st_advallc, st_cbtype, st_entry, st_unique, st_region,
                        st_terr, st_type, st_fadval, st_delacc, st_euro,
                        st_payadvl, st_eurind, st_origcur, st_fullamt, st_fullcb,
                        st_fullnar, st_cash, st_rcode, st_ruser, st_revchrg,
                        st_nlpdate, st_adjsv, st_fcvat, st_taxpoin,
                        datecreated, datemodified, state
                    ) VALUES (
                        '{customer_account}', '{post_date}', '{invoice_number[:20]}', '{customer_ref[:20]}', 'I',
                        {gross_amount}, {vat_amount}, {gross_amount}, ' ', '{post_date}',
                        'N', '{stran_memo[:2000]}', 0, 0, 0,
                        0, 0, '{due_date}', '   ', 0,
                        0, 0, 0, 0, 0,
                        0, 0, 0, '', 0,
                        0, '  ', '          ', '{stran_unique}', '{customer_region[:3]}',
                        '{customer_terr[:3]}', '{customer_type[:3]}', 0, '{customer_account}', 0,
                        0, ' ', '   ', 0, '  ',
                        '          ', 0, '    ', '        ', 0,
                        '{post_date}', 0, 0, '{post_date}',
                        '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(stran_sql))

                # 2. INSERT INTO ntran - CREDIT VAT (nt_type='C ', nt_subt='CA')
                if vat_amount > 0:
                    ntran_vat_sql = f"""
                        INSERT INTO ntran (
                            nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                            nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                            nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                            nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                            nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                            nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                            nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                            nt_distrib, datecreated, datemodified, state
                        ) VALUES (
                            '{vat_nominal}', '    ', 'C ', 'CA', {next_journal},
                            '          ', '{input_by[:10]}', 'S', '{ntran_comment}', '{ntran_trnref}',
                            '{post_date}', {-vat_amount}, {year}, {period}, 0,
                            0, 0, '   ', 0, 0,
                            0, 0, 'I', '', '        ',
                            '        ', 'I', 0, '{ntran_pstid_vat}', 0,
                            0, 0, 0, 0, 0,
                            0, '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(ntran_vat_sql))
                    # Update nacnt balance for VAT account (CREDIT)
                    self.update_nacnt_balance(conn, vat_nominal, -vat_amount, period)

                # 3. INSERT INTO ntran - CREDIT Sales (nt_type='E ', nt_subt from account)
                ntran_sales_sql = f"""
                    INSERT INTO ntran (
                        nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                        nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                        nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                        nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                        nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                        nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                        nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                        nt_distrib, datecreated, datemodified, state
                    ) VALUES (
                        '{sales_nominal}', '    ', 'E ', '{sales_subt}', {next_journal},
                        '          ', '{input_by[:10]}', 'S', '{ntran_comment}', '{ntran_trnref}',
                        '{post_date}', {-net_amount}, {year}, {period}, 0,
                        0, 0, '   ', 0, 0,
                        0, 0, 'I', '', '{customer_account}',
                        '{department}', 'I', 0, '{ntran_pstid_sales}', 0,
                        0, 0, 0, 0, 0,
                        0, '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(ntran_sales_sql))
                # Update nacnt balance for sales account (CREDIT)
                self.update_nacnt_balance(conn, sales_nominal, -net_amount, period)

                # 4. INSERT INTO ntran - DEBIT Debtors Control (nt_type='B ', nt_subt='BB')
                ntran_control_sql = f"""
                    INSERT INTO ntran (
                        nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                        nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                        nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                        nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                        nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                        nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                        nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                        nt_distrib, datecreated, datemodified, state
                    ) VALUES (
                        '{debtors_control}', '    ', 'B ', 'BB', {next_journal},
                        '          ', '{input_by[:10]}', 'S', '', 'Sales Ledger Transfer (RT)                        ',
                        '{post_date}', {gross_amount}, {year}, {period}, 0,
                        0, 0, '   ', 0, 0,
                        0, 0, 'I', '', '        ',
                        '        ', 'I', 0, '{ntran_pstid_control}', 0,
                        0, 0, 0, 0, 0,
                        0, '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(ntran_control_sql))
                # Update nacnt balance for debtors control (DEBIT)
                self.update_nacnt_balance(conn, debtors_control, gross_amount, period)

                # 5. INSERT INTO nhist (Nominal History for P&L account)
                nhist_sql = f"""
                    INSERT INTO nhist (
                        nh_rectype, nh_ntype, nh_nsubt, nh_nacnt, nh_ncntr,
                        nh_job, nh_project, nh_year, nh_period, nh_bal,
                        nh_budg, nh_rbudg, nh_ptddr, nh_ptdcr, nh_fbal,
                        datecreated, datemodified, state
                    ) VALUES (
                        1, 'E ', '{sales_subt}', '{sales_nominal}', '    ',
                        '{department}', '{customer_account}', {year}, {period}, {-net_amount},
                        0, 0, 0, {-net_amount}, 0,
                        '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(nhist_sql))

                # 6. zvtran - VAT analysis record (for VAT reporting)
                if vat_amount > 0:
                    zvtran_unique = OperaUniqueIdGenerator.generate()
                    vat_rate = (vat_amount / net_amount * 100) if net_amount > 0 else 20.0
                    zvtran_sql = f"""
                        INSERT INTO zvtran (
                            va_source, va_account, va_laccnt, va_trdate, va_taxdate,
                            va_ovrdate, va_trref, va_trtype, va_country, va_fcurr,
                            va_trvalue, va_fcval, va_vatval, va_cost, va_vatctry,
                            va_vattype, va_anvat, va_vatrate, va_box1, va_box2,
                            va_box4, va_box6, va_box7, va_box8, va_box9,
                            va_done, va_import, va_export,
                            datecreated, datemodified, state
                        ) VALUES (
                            'S', '{customer_account}', '{sales_nominal}', '{post_date}', '{post_date}',
                            '{post_date}', '{invoice_number[:20]}', 'I', 'GB', '   ',
                            {net_amount}, 0, {vat_amount}, 0, 'H',
                            'S', '2', {vat_rate}, 1, 0,
                            0, 1, 0, 0, 0,
                            0, 0, 0,
                            '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(zvtran_sql))

                    # 7. nvat - VAT return tracking record
                    # nv_vattype: 'S' = Sales (output VAT, payable to HMRC)
                    nvat_sql = f"""
                        INSERT INTO nvat (
                            nv_acnt, nv_cntr, nv_date, nv_crdate, nv_taxdate,
                            nv_ref, nv_type, nv_advance, nv_value, nv_vatval,
                            nv_vatctry, nv_vattype, nv_vatcode, nv_vatrate, nv_comment,
                            datecreated, datemodified, state
                        ) VALUES (
                            '{vat_nominal}', '', '{post_date}', '{post_date}', '{post_date}',
                            '{invoice_number[:20]}', 'S', 0, {net_amount}, {vat_amount},
                            ' ', 'S', 'S', {vat_rate}, 'Sales Invoice VAT',
                            '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(nvat_sql))

                # UPDATE sname.sn_currbal with ROWLOCK (increase customer balance - we invoiced them)
                sname_update_sql = f"""
                    UPDATE sname WITH (ROWLOCK)
                    SET sn_currbal = sn_currbal + {gross_amount},
                        datemodified = '{now_str}'
                    WHERE RTRIM(sn_account) = '{customer_account}'
                """
                conn.execute(text(sname_update_sql))

            logger.info(f"Successfully imported sales invoice: {invoice_number} for £{gross_amount:.2f}")

            return ImportResult(
                success=True,
                records_processed=1,
                records_imported=1,
                warnings=[
                    f"Invoice: {invoice_number}",
                    f"Journal number: {next_journal}",
                    f"Gross: £{gross_amount:.2f} (Net: £{net_amount:.2f} + VAT: £{vat_amount:.2f})",
                    f"Tables updated: stran, ntran (3), nhist, sname"
                ]
            )

        except Exception as e:
            logger.error(f"Failed to import sales invoice: {e}")
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[str(e)]
            )

    # =========================================================================
    # PURCHASE INVOICE IMPORT (Posts to Purchase Ledger)
    # =========================================================================

    def import_purchase_invoice_posting(
        self,
        supplier_account: str,
        invoice_number: str,
        net_amount: float,
        vat_amount: float,
        post_date: date,
        nominal_account: str = "HA010",
        vat_account: str = "BB040",
        purchase_ledger_control: str = None,
        input_by: str = "IMPORT",
        description: str = "",
        validate_only: bool = False
    ) -> ImportResult:
        """
        Import a purchase invoice posting into Opera SQL SE.

        This creates the nominal ledger entries for a purchase invoice:
        - Credit: Purchase Ledger Control (we owe supplier)
        - Debit: Expense Account (cost incurred)
        - Debit: VAT Account (VAT reclaimable)

        Args:
            supplier_account: Supplier account code (e.g., 'P001')
            invoice_number: Invoice number/reference
            net_amount: Net amount in POUNDS (before VAT)
            vat_amount: VAT amount in POUNDS
            post_date: Posting date
            nominal_account: Expense nominal account (default 'HA010')
            vat_account: VAT account (default 'BB040')
            purchase_ledger_control: Purchase ledger control (loaded from config if not specified)
            input_by: User code for audit trail
            description: Invoice description
            validate_only: If True, only validate without inserting

        Returns:
            ImportResult with details of the operation
        """
        errors = []
        warnings = []
        gross_amount = net_amount + vat_amount

        # Get control account - check supplier profile first, then fall back to default
        if purchase_ledger_control is None:
            from sql_rag.opera_config import get_supplier_control_account
            purchase_ledger_control = get_supplier_control_account(self.sql, supplier_account)
            logger.debug(f"Using creditors control for supplier {supplier_account}: {purchase_ledger_control}")

        try:
            # =====================
            # PERIOD VALIDATION
            # =====================
            from sql_rag.opera_config import validate_posting_period
            period_result = validate_posting_period(self.sql, post_date, ledger_type='PL')
            if not period_result.is_valid:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[period_result.error_message]
                )

            # =====================
            # VALIDATION
            # =====================

            # Validate supplier exists by checking pname (Purchase Ledger Master) first
            pname_check = self.sql.execute_query(f"""
                SELECT pn_name FROM pname
                WHERE RTRIM(pn_account) = '{supplier_account}'
            """)
            if not pname_check.empty:
                supplier_name = pname_check.iloc[0]['pn_name'].strip()
            else:
                # Fall back to atran history if not in pname
                supplier_check = self.sql.execute_query(f"""
                    SELECT TOP 1 at_account, at_name FROM atran
                    WHERE RTRIM(at_account) = '{supplier_account}'
                """)
                if not supplier_check.empty:
                    supplier_name = supplier_check.iloc[0]['at_name'].strip()
                else:
                    errors.append(f"Supplier account '{supplier_account}' not found")

            if errors:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=errors
                )

            if validate_only:
                return ImportResult(
                    success=True,
                    records_processed=1,
                    records_imported=1,
                    warnings=["Validation passed - no records inserted (validate_only=True)"]
                )

            # =====================
            # PREPARE VARIABLES
            # =====================

            unique_ids = OperaUniqueIdGenerator.generate_multiple(3)
            ntran_pstid_control = unique_ids[0]
            ntran_pstid_expense = unique_ids[1]
            ntran_pstid_vat = unique_ids[2]

            if isinstance(post_date, str):
                post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

            year = post_date.year
            period = post_date.month

            now = datetime.now()
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')

            ntran_comment = f"{invoice_number[:20]} {description[:29]:<29}"
            ntran_trnref = f"{supplier_name[:30]:<30}Invoice             "

            # =====================
            # EXECUTE ALL OPERATIONS IN A SINGLE TRANSACTION WITH LOCKING
            # =====================
            with self.sql.engine.begin() as conn:
                # Set lock timeout to prevent indefinite blocking of other users
                conn.execute(text(get_lock_timeout_sql()))

                # Get next journal number with UPDLOCK, ROWLOCK for minimal blocking
                journal_result = conn.execute(text("""
                    SELECT ISNULL(MAX(nt_jrnl), 0) + 1 as next_journal
                    FROM ntran WITH (UPDLOCK, ROWLOCK)
                """))
                next_journal = journal_result.scalar() or 1

                # 1. CREDIT Purchase Ledger Control (Gross - we owe this)
                ntran_control_sql = f"""
                    INSERT INTO ntran (
                        nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                        nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                        nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                        nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                        nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                        nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                        nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                        nt_distrib, datecreated, datemodified, state
                    ) VALUES (
                        '{purchase_ledger_control}', '    ', 'B ', 'BB', {next_journal},
                        '{invoice_number[:10]}', '{input_by[:10]}', 'A', '{ntran_comment}', '{ntran_trnref}',
                        '{post_date}', {-gross_amount}, {year}, {period}, 0,
                        0, 0, '   ', 0, 0,
                        0, 0, 'I', '', '        ',
                        '        ', 'S', 0, '{ntran_pstid_control}', 0,
                        0, 0, 0, 0, 0,
                        0, '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(ntran_control_sql))
                # Update nacnt balance for purchase ledger control (CREDIT)
                self.update_nacnt_balance(conn, purchase_ledger_control, -gross_amount, period)

                # 2. DEBIT Expense Account (Net - cost incurred)
                ntran_expense_sql = f"""
                    INSERT INTO ntran (
                        nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                        nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                        nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                        nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                        nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                        nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                        nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                        nt_distrib, datecreated, datemodified, state
                    ) VALUES (
                        '{nominal_account}', '    ', 'P ', 'HA', {next_journal},
                        '{invoice_number[:10]}', '{input_by[:10]}', 'A', '{ntran_comment}', '{ntran_trnref}',
                        '{post_date}', {net_amount}, {year}, {period}, 0,
                        0, 0, '   ', 0, 0,
                        0, 0, 'I', '', '        ',
                        '        ', 'S', 0, '{ntran_pstid_expense}', 0,
                        0, 0, 0, 0, 0,
                        0, '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(ntran_expense_sql))
                # Update nacnt balance for expense account (DEBIT)
                self.update_nacnt_balance(conn, nominal_account, net_amount, period)

                # 3. DEBIT VAT Account (VAT reclaimable)
                if vat_amount > 0:
                    ntran_vat_sql = f"""
                        INSERT INTO ntran (
                            nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                            nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                            nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                            nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                            nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                            nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                            nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                            nt_distrib, datecreated, datemodified, state
                        ) VALUES (
                            '{vat_account}', '    ', 'B ', 'BB', {next_journal},
                            '{invoice_number[:10]}', '{input_by[:10]}', 'A', '{ntran_comment}', '{ntran_trnref}',
                            '{post_date}', {vat_amount}, {year}, {period}, 0,
                            0, 0, '   ', 0, 0,
                            0, 0, 'I', '', '        ',
                            '        ', 'S', 0, '{ntran_pstid_vat}', 0,
                            0, 0, 0, 0, 0,
                            0, '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(ntran_vat_sql))
                    # Update nacnt balance for VAT account (DEBIT)
                    self.update_nacnt_balance(conn, vat_account, vat_amount, period)

                    # 4. zvtran - VAT analysis record (for VAT reporting)
                    zvtran_unique = OperaUniqueIdGenerator.generate()
                    vat_rate = (vat_amount / net_amount * 100) if net_amount > 0 else 20.0
                    zvtran_sql = f"""
                        INSERT INTO zvtran (
                            zv_acnt, zv_cntr, zv_jrnl, zv_date, zv_period,
                            zv_year, zv_value, zv_vatval, zv_vatrate, zv_nacnt,
                            zv_ncntr, zv_srcco, zv_pstid,
                            datecreated, datemodified, state
                        ) VALUES (
                            '{supplier_account}', '    ', {next_journal}, '{post_date}', {period},
                            {year}, {net_amount}, {vat_amount}, {vat_rate}, '{vat_account}',
                            '    ', 'I', '{zvtran_unique}',
                            '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(zvtran_sql))

                    # 5. nvat - VAT return tracking record (CRITICAL for VAT returns)
                    # nv_vattype: 'P' = Purchase (input VAT, reclaimable from HMRC)
                    nvat_sql = f"""
                        INSERT INTO nvat (
                            nv_acnt, nv_cntr, nv_date, nv_crdate, nv_taxdate,
                            nv_ref, nv_type, nv_advance, nv_value, nv_vatval,
                            nv_vatctry, nv_vattype, nv_vatcode, nv_vatrate, nv_comment,
                            datecreated, datemodified, state
                        ) VALUES (
                            '{vat_account}', '', '{post_date}', '{post_date}', '{post_date}',
                            '{invoice_number[:20]}', 'P', 0, {net_amount}, {vat_amount},
                            ' ', 'P', 'S', {vat_rate}, 'Purchase Invoice VAT',
                            '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(nvat_sql))

            logger.info(f"Successfully imported purchase invoice posting: {invoice_number} for £{gross_amount:.2f}")

            # Build tables list based on what was created
            tables_updated = ["ntran (3)", "nacnt (3)"]
            if vat_amount > 0:
                tables_updated.extend(["zvtran", "nvat"])

            return ImportResult(
                success=True,
                records_processed=1,
                records_imported=1,
                warnings=[
                    f"Invoice: {invoice_number}",
                    f"Journal number: {next_journal}",
                    f"Gross: £{gross_amount:.2f} (Net: £{net_amount:.2f} + VAT: £{vat_amount:.2f})",
                    f"Tables updated: {', '.join(tables_updated)}"
                ]
            )

        except Exception as e:
            logger.error(f"Failed to import purchase invoice posting: {e}")
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[str(e)]
            )

    # =========================================================================
    # NOMINAL JOURNAL IMPORT (General Ledger Journals)
    # =========================================================================

    def import_nominal_journal(
        self,
        lines: List[Dict[str, Any]],
        reference: str,
        post_date: date,
        input_by: str = "IMPORT",
        description: str = "",
        validate_only: bool = False
    ) -> ImportResult:
        """
        Import a nominal journal into Opera SQL SE.

        Each line should have:
        - account: Nominal account code
        - amount: Amount (positive = debit, negative = credit)
        - description: (optional) Line description

        The journal MUST balance (total of all amounts = 0)

        Args:
            lines: List of journal lines
            reference: Journal reference
            post_date: Posting date
            input_by: User code for audit trail
            description: Journal description
            validate_only: If True, only validate without inserting

        Returns:
            ImportResult with details of the operation
        """
        errors = []
        warnings = []

        try:
            # =====================
            # PERIOD VALIDATION
            # =====================
            from sql_rag.opera_config import validate_posting_period
            period_result = validate_posting_period(self.sql, post_date, ledger_type='NL')
            if not period_result.is_valid:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[period_result.error_message]
                )

            # =====================
            # VALIDATION
            # =====================

            # Check journal balances
            total = sum(float(line.get('amount', 0)) for line in lines)
            if abs(total) > 0.01:
                errors.append(f"Journal does not balance. Total: £{total:.2f} (should be £0.00)")

            if len(lines) < 2:
                errors.append("Journal must have at least 2 lines")

            # Validate accounts exist
            for idx, line in enumerate(lines, 1):
                if not line.get('account'):
                    errors.append(f"Line {idx}: Missing account code")
                    continue

                account_check = self.sql.execute_query(f"""
                    SELECT TOP 1 nt_acnt FROM ntran
                    WHERE RTRIM(nt_acnt) = '{line['account']}'
                """)
                if account_check.empty:
                    warnings.append(f"Line {idx}: Account '{line['account']}' has not been used before")

            if errors:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=errors
                )

            if validate_only:
                return ImportResult(
                    success=True,
                    records_processed=1,
                    records_imported=1,
                    warnings=["Validation passed - no records inserted (validate_only=True)"] + warnings
                )

            # =====================
            # PREPARE VARIABLES
            # =====================

            if isinstance(post_date, str):
                post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

            year = post_date.year
            period = post_date.month

            now = datetime.now()
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')

            # =====================
            # EXECUTE ALL OPERATIONS IN A SINGLE TRANSACTION WITH LOCKING
            # =====================
            with self.sql.engine.begin() as conn:
                # Set lock timeout to prevent indefinite blocking of other users
                conn.execute(text(get_lock_timeout_sql()))

                # Get next journal number with UPDLOCK, ROWLOCK for minimal blocking
                journal_result = conn.execute(text("""
                    SELECT ISNULL(MAX(nt_jrnl), 0) + 1 as next_journal
                    FROM ntran WITH (UPDLOCK, ROWLOCK)
                """))
                next_journal = journal_result.scalar() or 1

                # Insert all journal lines
                for line in lines:
                    unique_id = OperaUniqueIdGenerator.generate()
                    amount = float(line['amount'])
                    line_desc = line.get('description', description)[:50]
                    ntran_comment = f"{reference[:20]} {line_desc:<29}"

                    sql = f"""
                        INSERT INTO ntran (
                            nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                            nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                            nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                            nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                            nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                            nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                            nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                            nt_distrib, datecreated, datemodified, state
                        ) VALUES (
                            '{line['account']}', '    ', 'J ', 'JN', {next_journal},
                            '{reference[:10]}', '{input_by[:10]}', 'A', '{ntran_comment}', 'Journal             ',
                            '{post_date}', {amount}, {year}, {period}, 0,
                            0, 0, '   ', 0, 0,
                            0, 0, 'I', '', '        ',
                            '        ', 'J', 0, '{unique_id}', 0,
                            0, 0, 0, 0, 0,
                            0, '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(sql))
                    # Update nacnt balance for journal line
                    self.update_nacnt_balance(conn, line['account'], amount, period)

            total_debits = sum(float(l['amount']) for l in lines if float(l['amount']) > 0)
            logger.info(f"Successfully imported nominal journal: {reference} with {len(lines)} lines, £{total_debits:.2f}")

            return ImportResult(
                success=True,
                records_processed=1,
                records_imported=1,
                warnings=[
                    f"Reference: {reference}",
                    f"Journal number: {next_journal}",
                    f"Lines: {len(lines)}",
                    f"Total debits: £{total_debits:.2f}"
                ] + warnings
            )

        except Exception as e:
            logger.error(f"Failed to import nominal journal: {e}")
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[str(e)]
            )

    # =========================================================================
    # GOCARDLESS BATCH IMPORT
    # Creates a true Opera batch receipt with one header and multiple detail lines
    # =========================================================================

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
        currency: str = None,
        auto_allocate: bool = False,
        destination_bank: str = None
    ) -> ImportResult:
        """
        Import a GoCardless batch receipt into Opera SQL SE.

        Creates a true Opera batch with:
        - One aentry header (batch total)
        - Multiple atran lines (one per customer payment)
        - Multiple stran records (one per customer)
        - If complete_batch=True: ntran and anoml records, customer balance updates
        - If complete_batch=False: leaves for review in Opera (ae_complet=False)

        Args:
            bank_account: Bank account code (e.g., 'BC010')
            payments: List of payment dicts with:
                - customer_account: Customer code (e.g., 'A046')
                - amount: Amount in POUNDS
                - description: Payment description/reference
            post_date: Posting date
            reference: Batch reference (default 'GoCardless')
            gocardless_fees: Total GoCardless fees (gross including VAT) to post to nominal (optional)
            vat_on_fees: VAT element of fees (default 0.0) - posted to VAT input account with zvtran
            fees_nominal_account: Nominal account for net fees (e.g., 'GA400')
            fees_vat_code: VAT code for fees (default '2' standard rate) - looked up in ztax to get rate and nominal
            fees_payment_type: Cashbook type code for fees entry (e.g., 'NP'). If None, uses first non-batched Payment type.
            complete_batch: If True, completes batch immediately (creates ntran/anoml)
            input_by: User code for audit trail
            cbtype: Cashbook type code (must be batched Receipt type). Auto-detects GoCardless type if None.
            validate_only: If True, only validate without inserting
            currency: Currency code from GoCardless (e.g., 'GBP', 'EUR'). Rejected if not home currency.
            auto_allocate: If True, automatically allocate receipts to matching invoices
            destination_bank: If set (and different from bank_account), auto-transfer net
                amount from bank_account (GC Control) to destination_bank (actual bank).
                This creates a single net entry on the actual bank for easy reconciliation.

        Returns:
            ImportResult with details of the operation
        """
        errors = []
        warnings = []

        if not payments:
            return ImportResult(
                success=False,
                records_processed=0,
                records_failed=0,
                errors=["No payments provided"]
            )

        # Validate currency matches home currency
        if currency:
            home_currency = self.get_home_currency()
            if currency.upper() != home_currency['code'].upper():
                return ImportResult(
                    success=False,
                    records_processed=len(payments),
                    records_failed=len(payments),
                    errors=[
                        f"GoCardless batch is in {currency} but home currency is {home_currency['code']} ({home_currency['description']}). "
                        "Foreign currency GoCardless batches are not supported. Please process this batch manually."
                    ]
                )

        # Validate fees configuration - MUST have fees_nominal_account if fees > 0
        if gocardless_fees > 0 and not fees_nominal_account:
            return ImportResult(
                success=False,
                records_processed=len(payments),
                records_failed=len(payments),
                errors=[
                    f"GoCardless fees of £{gocardless_fees:.2f} cannot be posted: fees_nominal_account not configured. "
                    "Please configure the Fees Nominal Account in GoCardless Settings before importing."
                ]
            )

        # Calculate totals
        gross_amount = sum(p.get('amount', 0) for p in payments)
        net_amount = gross_amount - abs(gocardless_fees)
        total_pence = int(gross_amount * 100)

        # =====================
        # VALIDATE/GET CBTYPE
        # =====================
        if cbtype is None:
            # Try to find a GoCardless type, or use a batched receipt type
            cbtype_result = self.sql.execute_query("""
                SELECT ay_cbtype FROM atype
                WHERE ay_type = 'R' AND ay_batched = 1
                AND (ay_desc LIKE '%GoCardless%' OR ay_desc LIKE '%gocardless%')
            """)
            if cbtype_result is not None and len(cbtype_result) > 0:
                cbtype = cbtype_result.iloc[0]['ay_cbtype'].strip()
            else:
                # Fall back to any batched receipt type
                cbtype_result = self.sql.execute_query("""
                    SELECT TOP 1 ay_cbtype FROM atype
                    WHERE ay_type = 'R' AND ay_batched = 1
                """)
                if cbtype_result is not None and len(cbtype_result) > 0:
                    cbtype = cbtype_result.iloc[0]['ay_cbtype'].strip()
                else:
                    return ImportResult(
                        success=False,
                        records_processed=len(payments),
                        records_failed=len(payments),
                        errors=["No batched Receipt type codes found in atype table"]
                    )
            logger.debug(f"Using cbtype for GoCardless batch: {cbtype}")

        # Validate the type code is a batched receipt type
        type_validation = self.validate_cbtype(cbtype, required_category=AtypeCategory.RECEIPT)
        if not type_validation['valid']:
            return ImportResult(
                success=False,
                records_processed=len(payments),
                records_failed=len(payments),
                errors=[type_validation['error']]
            )

        # =====================
        # VALIDATE CUSTOMERS
        # =====================
        customer_info = {}
        for idx, payment in enumerate(payments):
            customer_account = payment.get('customer_account', '').strip()
            if not customer_account:
                errors.append(f"Payment {idx+1}: Missing customer account")
                continue

            # Get customer details from sname
            sname_check = self.sql.execute_query(f"""
                SELECT sn_name, sn_region, sn_terrtry, sn_custype FROM sname WITH (NOLOCK)
                WHERE RTRIM(sn_account) = '{customer_account}'
            """)
            if sname_check is not None and len(sname_check) > 0:
                customer_info[customer_account] = {
                    'name': sname_check.iloc[0]['sn_name'].strip(),
                    'region': sname_check.iloc[0]['sn_region'].strip() if sname_check.iloc[0]['sn_region'] else 'K',
                    'terr': sname_check.iloc[0]['sn_terrtry'].strip() if sname_check.iloc[0]['sn_terrtry'] else '001',
                    'type': sname_check.iloc[0]['sn_custype'].strip() if sname_check.iloc[0]['sn_custype'] else 'DD1'
                }
            else:
                errors.append(f"Payment {idx+1}: Customer account '{customer_account}' not found")

        if errors:
            return ImportResult(
                success=False,
                records_processed=len(payments),
                records_failed=len(payments),
                errors=errors
            )

        if validate_only:
            return ImportResult(
                success=True,
                records_processed=len(payments),
                records_imported=len(payments),
                warnings=[f"Validation passed for {len(payments)} payments totalling £{gross_amount:.2f}"]
            )

        try:
            # =====================
            # PERIOD POSTING DECISION
            # =====================
            from sql_rag.opera_config import get_period_posting_decision, get_customer_control_account
            posting_decision = get_period_posting_decision(self.sql, post_date, 'SL')

            if not posting_decision.can_post:
                return ImportResult(
                    success=False,
                    records_processed=len(payments),
                    records_failed=len(payments),
                    errors=[posting_decision.error_message]
                )

            # Format date
            if isinstance(post_date, str):
                post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

            year = post_date.year
            period = post_date.month

            # Get current timestamp
            now = datetime.now()
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')
            date_str = now.strftime('%Y-%m-%d')
            time_str = now.strftime('%H:%M:%S')

            # Generate unique IDs for each payment + fees + VAT
            unique_ids = OperaUniqueIdGenerator.generate_multiple(len(payments) * 3 + 3)

            # Execute all operations within a single transaction
            with self.sql.engine.begin() as conn:
                conn.execute(text(get_lock_timeout_sql()))

                # Get next entry number from atype
                entry_number = self.increment_atype_entry(conn, cbtype)

                # Get next journal number (if completing)
                journal_result = conn.execute(text("""
                    SELECT ISNULL(MAX(nt_jrnl), 0) + 1 as next_journal
                    FROM ntran WITH (UPDLOCK, ROWLOCK)
                """))
                next_journal = journal_result.scalar() or 1

                # 1. INSERT aentry (Batch Header)
                # ae_complet = 1 if completing, 0 if leaving for review
                aentry_sql = f"""
                    INSERT INTO aentry (
                        ae_acnt, ae_cntr, ae_cbtype, ae_entry, ae_reclnum,
                        ae_lstdate, ae_frstat, ae_tostat, ae_statln, ae_entref,
                        ae_value, ae_recbal, ae_remove, ae_tmpstat, ae_complet,
                        ae_postgrp, sq_crdate, sq_crtime, sq_cruser, ae_comment,
                        ae_payid, ae_batchid, ae_brwptr, datecreated, datemodified, state
                    ) VALUES (
                        '{bank_account}', '    ', '{cbtype}', '{entry_number}', 0,
                        '{post_date}', 0, 0, 0, '{reference[:20]}',
                        {total_pence}, 0, 0, 0, {1 if complete_batch else 0},
                        0, '{date_str}', '{time_str[:8]}', '{input_by[:8]}', 'GoCardless batch import',
                        0, 0, '  ', '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(aentry_sql))

                # 2. INSERT atran lines and stran records for each payment
                for idx, payment in enumerate(payments):
                    customer_account = payment['customer_account'].strip()
                    amount_pounds = float(payment['amount'])
                    amount_pence = int(round(amount_pounds * 100))
                    description = payment.get('description', '')[:35].replace("'", "''")

                    cust = customer_info[customer_account]
                    customer_name = cust['name'].replace("'", "''")

                    # Get unique IDs for this payment
                    atran_unique = unique_ids[idx * 3]
                    stran_unique = unique_ids[idx * 3 + 1]
                    ntran_pstid = unique_ids[idx * 3 + 2]

                    # Get customer's control account
                    sales_ledger_control = get_customer_control_account(self.sql, customer_account)

                    # INSERT atran
                    atran_sql = f"""
                        INSERT INTO atran (
                            at_acnt, at_cntr, at_cbtype, at_entry, at_inputby,
                            at_type, at_pstdate, at_sysdate, at_tperiod, at_value,
                            at_disc, at_fcurr, at_fcexch, at_fcmult, at_fcdec,
                            at_account, at_name, at_comment, at_payee, at_payname,
                            at_sort, at_number, at_remove, at_chqprn, at_chqlst,
                            at_bacprn, at_ccdprn, at_ccdno, at_payslp, at_pysprn,
                            at_cash, at_remit, at_unique, at_postgrp, at_ccauth,
                            at_refer, at_srcco, at_ecb, at_ecbtype, at_atpycd,
                            at_bsref, at_bsname, at_vattycd, at_project, at_job,
                            at_bic, at_iban, at_memo, datecreated, datemodified, state
                        ) VALUES (
                            '{bank_account}', '    ', '{cbtype}', '{entry_number}', '{input_by[:8]}',
                            {CashbookTransactionType.SALES_RECEIPT}, '{post_date}', '{post_date}', 1, {amount_pence},
                            0, '   ', 1.0, 0, 2,
                            '{customer_account}', '{customer_name[:35]}', '{description}', '        ', '',
                            '        ', '         ', 0, 0, 0,
                            0, 0, '', 0, 0,
                            0, 0, '{atran_unique}', 0, '0       ',
                            '{reference[:20]}', 'I', 0, ' ', '      ',
                            '', '', '  ', '        ', '        ',
                            '', '', '', '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(atran_sql))

                    # INSERT stran
                    stran_memo = f"GoCardless - {description}".replace("'", "''")
                    stran_sql = f"""
                        INSERT INTO stran (
                            st_account, st_trdate, st_trref, st_custref, st_trtype,
                            st_trvalue, st_vatval, st_trbal, st_paid, st_crdate,
                            st_advance, st_memo, st_payflag, st_set1day, st_set1,
                            st_set2day, st_set2, st_dueday, st_fcurr, st_fcrate,
                            st_fcdec, st_fcval, st_fcbal, st_fcmult, st_dispute,
                            st_edi, st_editx, st_edivn, st_txtrep, st_binrep,
                            st_advallc, st_cbtype, st_entry, st_unique, st_region,
                            st_terr, st_type, st_fadval, st_delacc, st_euro,
                            st_payadvl, st_eurind, st_origcur, st_fullamt, st_fullcb,
                            st_fullnar, st_cash, st_rcode, st_ruser, st_revchrg,
                            st_nlpdate, st_adjsv, st_fcvat, st_taxpoin,
                            datecreated, datemodified, state
                        ) VALUES (
                            '{customer_account}', '{post_date}', '{reference[:20]}', 'GoCardless', 'R',
                            {-amount_pounds}, 0, {-amount_pounds}, ' ', '{post_date}',
                            'N', '{stran_memo[:200]}', 0, 0, 0,
                            0, 0, '{post_date}', '   ', 0,
                            0, 0, 0, 0, 0,
                            0, 0, 0, '', 0,
                            0, '{cbtype}', '{entry_number}', '{stran_unique}', '{cust["region"][:3]}',
                            '{cust["terr"][:3]}', '{cust["type"][:3]}', 0, '{customer_account}', 0,
                            0, ' ', '   ', 0, '  ',
                            '          ', 0, '    ', '        ', 0,
                            '{post_date}', 0, 0, '{post_date}',
                            '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(stran_sql))

                    # Create ntran (nominal ledger) - ALWAYS posted
                    if posting_decision.post_to_nominal:
                        ntran_comment = f"{description[:50]:<50}".replace("'", "''")
                        ntran_trnref = f"{customer_name[:30]:<30}GoCardless (RT)     ".replace("'", "''")

                        # ntran DEBIT (Bank +amount)
                        ntran_debit_sql = f"""
                            INSERT INTO ntran (
                                nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                                nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                                nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                                nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                                nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                                nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                                nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                                nt_distrib, datecreated, datemodified, state
                            ) VALUES (
                                '{bank_account}', '    ', 'B ', 'BC', {next_journal},
                                '', '{input_by[:10]}', 'A', '{ntran_comment}', '{ntran_trnref}',
                                '{post_date}', {amount_pounds}, {year}, {period}, 0,
                                0, 0, '   ', 0, 0,
                                0, 0, 'I', '', '        ',
                                '        ', 'S', 0, '{ntran_pstid}', 0,
                                0, 0, 0, 0, 0,
                                0, '{now_str}', '{now_str}', 1
                            )
                        """
                        conn.execute(text(ntran_debit_sql))
                        # Update nacnt balance for bank account (DEBIT)
                        self.update_nacnt_balance(conn, bank_account, amount_pounds, period)
                        # Update nbank balance (GoCardless receipt increases bank balance)
                        self.update_nbank_balance(conn, bank_account, amount_pounds)

                        # ntran CREDIT (Debtors Control -amount)
                        ntran_credit_sql = f"""
                            INSERT INTO ntran (
                                nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                                nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                                nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                                nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                                nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                                nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                                nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                                nt_distrib, datecreated, datemodified, state
                            ) VALUES (
                                '{sales_ledger_control}', '    ', 'B ', 'BB', {next_journal},
                                '', '{input_by[:10]}', 'A', '{ntran_comment}', '{ntran_trnref}',
                                '{post_date}', {-amount_pounds}, {year}, {period}, 0,
                                0, 0, '   ', 0, 0,
                                0, 0, 'I', '', '        ',
                                '        ', 'S', 0, '{ntran_pstid}', 0,
                                0, 0, 0, 0, 0,
                                0, '{now_str}', '{now_str}', 1
                            )
                        """
                        conn.execute(text(ntran_credit_sql))
                        # Update nacnt balance for debtors control (CREDIT)
                        self.update_nacnt_balance(conn, sales_ledger_control, -amount_pounds, period)
                        next_journal += 1

                    # Create anoml records (transfer file) for batched cashbook types
                    if posting_decision.post_to_transfer_file:
                        jrnl_num = next_journal - 1 if posting_decision.post_to_nominal else 0

                        # anoml Bank account - ax_done flag from posting decision
                        done_flag = posting_decision.transfer_file_done_flag
                        anoml_bank_sql = f"""
                            INSERT INTO anoml (
                                ax_nacnt, ax_ncntr, ax_source, ax_date, ax_value, ax_tref,
                                ax_comment, ax_done, ax_fcurr, ax_fvalue, ax_fcrate, ax_fcmult, ax_fcdec,
                                ax_srcco, ax_unique, ax_project, ax_job, ax_jrnl, ax_nlpdate,
                                datecreated, datemodified, state
                            ) VALUES (
                                '{bank_account}', '    ', 'S', '{post_date}', {amount_pounds}, '{reference[:20]}',
                                '{description[:50]}', '{done_flag}', '   ', 0, 0, 0, 0,
                                'I', '{atran_unique}', '        ', '        ', {jrnl_num}, '{post_date}',
                                '{now_str}', '{now_str}', 1
                            )
                        """
                        conn.execute(text(anoml_bank_sql))

                        # anoml Debtors control - ax_done flag from posting decision
                        anoml_control_sql = f"""
                            INSERT INTO anoml (
                                ax_nacnt, ax_ncntr, ax_source, ax_date, ax_value, ax_tref,
                                ax_comment, ax_done, ax_fcurr, ax_fvalue, ax_fcrate, ax_fcmult, ax_fcdec,
                                ax_srcco, ax_unique, ax_project, ax_job, ax_jrnl, ax_nlpdate,
                                datecreated, datemodified, state
                            ) VALUES (
                                '{sales_ledger_control}', '    ', 'S', '{post_date}', {-amount_pounds}, '{reference[:20]}',
                                '{description[:50]}', '{done_flag}', '   ', 0, 0, 0, 0,
                                'I', '{atran_unique}', '        ', '        ', {jrnl_num}, '{post_date}',
                                '{now_str}', '{now_str}', 1
                            )
                        """
                        conn.execute(text(anoml_control_sql))

                    # Update customer balance - ALWAYS updated
                    sname_update_sql = f"""
                        UPDATE sname WITH (ROWLOCK)
                        SET sn_currbal = sn_currbal - {amount_pounds},
                            datemodified = '{now_str}'
                        WHERE RTRIM(sn_account) = '{customer_account}'
                    """
                    conn.execute(text(sname_update_sql))

                # 3. Post GoCardless fees if provided - ALWAYS posted as SEPARATE cashbook entry
                # Fees are split into: Net fees (expense) + VAT (reclaimable)
                vat_nominal_used = None  # Track which VAT account was used for result message
                vat_code_used = '2'  # Default
                fees_entry_number = None  # Track fees entry number for result message
                if gocardless_fees > 0 and fees_nominal_account:
                    fees_unique = unique_ids[-1]
                    fees_vat_unique = unique_ids[-2] if len(unique_ids) > 1 else OperaUniqueIdGenerator.generate()
                    fees_comment = "GoCardless fees"

                    # Calculate net fees (excluding VAT)
                    net_fees = abs(gocardless_fees) - abs(vat_on_fees)
                    gross_fees = abs(gocardless_fees)

                    # Look up VAT code from ztax to get rate and nominal account
                    # This is done fresh each time to ensure correct rate/account is used
                    vat_code_used = fees_vat_code
                    vat_info = self.get_vat_rate(fees_vat_code, 'P', post_date)
                    vat_nominal_account = vat_info.get('nominal', 'BB040')  # Fallback if lookup fails
                    vat_nominal_used = vat_nominal_account
                    vat_rate = vat_info.get('rate', 20.0)
                    logger.debug(f"VAT lookup for fees: code={fees_vat_code}, nominal={vat_nominal_account}, rate={vat_rate}%")

                    # Nominal posting for fees (expense DR, VAT DR, bank CR)
                    if posting_decision.post_to_nominal:
                        # DR Fees expense (NET amount only)
                        fees_dr_sql = f"""
                            INSERT INTO ntran (
                                nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                                nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                                nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                                nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                                nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                                nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                                nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                                nt_distrib, datecreated, datemodified, state
                            ) VALUES (
                                '{fees_nominal_account}', '    ', 'P ', 'HA', {next_journal},
                                '', '{input_by[:10]}', 'A', '{fees_comment}', '{fees_comment}',
                                '{post_date}', {net_fees}, {year}, {period}, 0,
                                0, 0, '   ', 0, 0,
                                0, 0, 'I', '', '        ',
                                '        ', 'N', 0, '{fees_unique}', 0,
                                0, 0, 0, 0, 0,
                                0, '{now_str}', '{now_str}', 1
                            )
                        """
                        conn.execute(text(fees_dr_sql))
                        # Update nacnt balance for fees account (DEBIT net amount)
                        self.update_nacnt_balance(conn, fees_nominal_account, net_fees, period)

                        # DR VAT Input (reclaimable VAT) - only if VAT > 0
                        if vat_on_fees > 0:
                            vat_dr_sql = f"""
                                INSERT INTO ntran (
                                    nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                                    nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                                    nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                                    nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                                    nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                                    nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                                    nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                                    nt_distrib, datecreated, datemodified, state
                                ) VALUES (
                                    '{vat_nominal_account}', '    ', 'B ', 'BB', {next_journal},
                                    '', '{input_by[:10]}', 'A', '{fees_comment} VAT', '{fees_comment}',
                                    '{post_date}', {abs(vat_on_fees)}, {year}, {period}, 0,
                                    0, 0, '   ', 0, 0,
                                    0, 0, 'I', '', '        ',
                                    '        ', 'N', 0, '{fees_vat_unique}', 0,
                                    0, 0, 0, 0, 0,
                                    0, '{now_str}', '{now_str}', 1
                                )
                            """
                            conn.execute(text(vat_dr_sql))
                            # Update nacnt balance for VAT input account (DEBIT)
                            self.update_nacnt_balance(conn, vat_nominal_account, abs(vat_on_fees), period)

                        # CR Bank (gross fees reduce bank receipt)
                        fees_cr_sql = f"""
                            INSERT INTO ntran (
                                nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                                nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                                nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                                nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                                nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                                nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                                nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                                nt_distrib, datecreated, datemodified, state
                            ) VALUES (
                                '{bank_account}', '    ', 'B ', 'BB', {next_journal},
                                '', '{input_by[:10]}', 'A', '{fees_comment}', '{fees_comment}',
                                '{post_date}', {-gross_fees}, {year}, {period}, 0,
                                0, 0, '   ', 0, 0,
                                0, 0, 'I', '', '        ',
                                '        ', 'N', 0, '{fees_unique}', 0,
                                0, 0, 0, 0, 0,
                                0, '{now_str}', '{now_str}', 1
                            )
                        """
                        conn.execute(text(fees_cr_sql))
                        # Update nacnt balance for bank account fees (CREDIT gross)
                        self.update_nacnt_balance(conn, bank_account, -gross_fees, period)
                        # Update nbank balance (GoCardless fees decrease bank balance)
                        self.update_nbank_balance(conn, bank_account, -gross_fees)

                    # Create SEPARATE cashbook entry for fees (not part of receipts batch)
                    # This ensures fees appear as a distinct payment in cashbook
                    gross_fees_pence = int(round(gross_fees * 100))

                    # Get next entry number for the fees entry
                    # Use TRY_CAST to handle non-numeric ae_entry values (e.g., 'P100004680')
                    fees_entry_result = conn.execute(text(f"""
                        SELECT ISNULL(MAX(TRY_CAST(ae_entry AS INT)), 0) + 1 AS next_entry
                        FROM aentry WHERE ae_acnt = '{bank_account}'
                        AND TRY_CAST(ae_entry AS INT) IS NOT NULL
                    """))
                    fees_entry_number = str(fees_entry_result.fetchone()[0]).zfill(8)

                    # Use configured fees payment type, or find a non-batched payment type
                    if fees_payment_type:
                        fees_cbtype = fees_payment_type.strip()
                        logger.debug(f"Using configured fees payment type: {fees_cbtype}")
                    else:
                        fees_cbtype_result = conn.execute(text("""
                            SELECT TOP 1 ay_cbtype FROM atype
                            WHERE ay_type = 'P' AND ay_batched = 0
                            ORDER BY ay_cbtype
                        """))
                        fees_cbtype_row = fees_cbtype_result.fetchone()
                        fees_cbtype = fees_cbtype_row[0] if fees_cbtype_row else 'NP'  # NP = Nominal Payment fallback

                    # Create aentry header for fees
                    fees_aentry_sql = f"""
                        INSERT INTO aentry (
                            ae_acnt, ae_cntr, ae_cbtype, ae_entry, ae_reclnum,
                            ae_lstdate, ae_frstat, ae_tostat, ae_statln, ae_entref,
                            ae_value, ae_recbal, ae_remove, ae_tmpstat, ae_complet,
                            ae_postgrp, sq_crdate, sq_crtime, sq_cruser, ae_comment,
                            ae_payid, ae_batchid, ae_brwptr, datecreated, datemodified, state
                        ) VALUES (
                            '{bank_account}', '    ', '{fees_cbtype}', '{fees_entry_number}', 0,
                            '{post_date}', 0, 0, 0, '{reference[:20]}',
                            {-gross_fees_pence}, 0, 0, 0, {1 if posting_decision.post_to_nominal else 0},
                            0, '{date_str}', '{time_str[:8]}', '{input_by[:8]}', 'GoCardless fees',
                            0, 0, '  ', '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(fees_aentry_sql))
                    logger.debug(f"Created separate aentry for GoCardless fees: {fees_entry_number}")

                    # Create atran for fees under the new entry
                    # If VAT > 0, create two lines: net fees + VAT (Opera data integrity requirement)
                    if vat_on_fees > 0:
                        # Line 1: Net fees to expense account
                        net_fees_pence = int(round(net_fees * 100))
                        fees_atran_net_sql = f"""
                            INSERT INTO atran (
                                at_acnt, at_cntr, at_cbtype, at_entry, at_inputby,
                                at_type, at_pstdate, at_sysdate, at_tperiod, at_value,
                                at_disc, at_fcurr, at_fcexch, at_fcmult, at_fcdec,
                                at_account, at_name, at_comment, at_payee, at_payname,
                                at_sort, at_number, at_remove, at_chqprn, at_chqlst,
                                at_bacprn, at_ccdprn, at_ccdno, at_payslp, at_pysprn,
                                at_cash, at_remit, at_unique, at_postgrp, at_ccauth,
                                at_refer, at_srcco, at_ecb, at_ecbtype, at_atpycd,
                                at_bsref, at_bsname, at_vattycd, at_project, at_job,
                                at_bic, at_iban, at_memo, datecreated, datemodified, state
                            ) VALUES (
                                '{bank_account}', '    ', '{fees_cbtype}', '{fees_entry_number}', '{input_by[:8]}',
                                {CashbookTransactionType.NOMINAL_PAYMENT}, '{post_date}', '{post_date}', 1, {-net_fees_pence},
                                0, '   ', 1.0, 0, 2,
                                '{fees_nominal_account}', '{fees_comment[:35]}', '', '        ', '',
                                '        ', '         ', 0, 0, 0,
                                0, 0, '', 0, 0,
                                0, 0, '{fees_unique}', 0, '0       ',
                                '{reference[:20]}', 'I', 0, ' ', '      ',
                                '', '', '  ', '        ', '        ',
                                '', '', '', '{now_str}', '{now_str}', 1
                            )
                        """
                        conn.execute(text(fees_atran_net_sql))

                        # Line 2: VAT to VAT input account
                        vat_pence = int(round(abs(vat_on_fees) * 100))
                        fees_atran_vat_sql = f"""
                            INSERT INTO atran (
                                at_acnt, at_cntr, at_cbtype, at_entry, at_inputby,
                                at_type, at_pstdate, at_sysdate, at_tperiod, at_value,
                                at_disc, at_fcurr, at_fcexch, at_fcmult, at_fcdec,
                                at_account, at_name, at_comment, at_payee, at_payname,
                                at_sort, at_number, at_remove, at_chqprn, at_chqlst,
                                at_bacprn, at_ccdprn, at_ccdno, at_payslp, at_pysprn,
                                at_cash, at_remit, at_unique, at_postgrp, at_ccauth,
                                at_refer, at_srcco, at_ecb, at_ecbtype, at_atpycd,
                                at_bsref, at_bsname, at_vattycd, at_project, at_job,
                                at_bic, at_iban, at_memo, datecreated, datemodified, state
                            ) VALUES (
                                '{bank_account}', '   1', '{fees_cbtype}', '{fees_entry_number}', '{input_by[:8]}',
                                {CashbookTransactionType.NOMINAL_PAYMENT}, '{post_date}', '{post_date}', 1, {-vat_pence},
                                0, '   ', 1.0, 0, 2,
                                '{vat_nominal_account}', '{fees_comment[:35]} VAT', '', '        ', '',
                                '        ', '         ', 0, 0, 0,
                                0, 0, '', 0, 0,
                                0, 0, '{fees_vat_unique}', 0, '0       ',
                                '{reference[:20]}', 'I', 0, ' ', '      ',
                                '', '', '  ', '        ', '        ',
                                '', '', '', '{now_str}', '{now_str}', 1
                            )
                        """
                        conn.execute(text(fees_atran_vat_sql))
                        logger.debug(f"Created 2 atran lines for fees: net £{net_fees:.2f} to {fees_nominal_account}, VAT £{vat_on_fees:.2f} to {vat_nominal_account}")
                    else:
                        # Single line for gross fees (no VAT)
                        fees_atran_sql = f"""
                            INSERT INTO atran (
                                at_acnt, at_cntr, at_cbtype, at_entry, at_inputby,
                                at_type, at_pstdate, at_sysdate, at_tperiod, at_value,
                                at_disc, at_fcurr, at_fcexch, at_fcmult, at_fcdec,
                                at_account, at_name, at_comment, at_payee, at_payname,
                                at_sort, at_number, at_remove, at_chqprn, at_chqlst,
                                at_bacprn, at_ccdprn, at_ccdno, at_payslp, at_pysprn,
                                at_cash, at_remit, at_unique, at_postgrp, at_ccauth,
                                at_refer, at_srcco, at_ecb, at_ecbtype, at_atpycd,
                                at_bsref, at_bsname, at_vattycd, at_project, at_job,
                                at_bic, at_iban, at_memo, datecreated, datemodified, state
                            ) VALUES (
                                '{bank_account}', '    ', '{fees_cbtype}', '{fees_entry_number}', '{input_by[:8]}',
                                {CashbookTransactionType.NOMINAL_PAYMENT}, '{post_date}', '{post_date}', 1, {-gross_fees_pence},
                                0, '   ', 1.0, 0, 2,
                                '{fees_nominal_account}', '{fees_comment[:35]}', '', '        ', '',
                                '        ', '         ', 0, 0, 0,
                                0, 0, '', 0, 0,
                                0, 0, '{fees_unique}', 0, '0       ',
                                '{reference[:20]}', 'I', 0, ' ', '      ',
                                '', '', '  ', '        ', '        ',
                                '', '', '', '{now_str}', '{now_str}', 1
                            )
                        """
                        conn.execute(text(fees_atran_sql))

                    # Create anoml transfer file records for fees
                    if posting_decision.post_to_transfer_file:
                        jrnl_num = next_journal if posting_decision.post_to_nominal else 0
                        fees_done_flag = posting_decision.transfer_file_done_flag

                        # anoml Bank account (credit - fees reduce bank balance)
                        anoml_fees_bank_sql = f"""
                            INSERT INTO anoml (
                                ax_nacnt, ax_ncntr, ax_source, ax_date, ax_value, ax_tref,
                                ax_comment, ax_done, ax_fcurr, ax_fvalue, ax_fcrate, ax_fcmult, ax_fcdec,
                                ax_srcco, ax_unique, ax_project, ax_job, ax_jrnl, ax_nlpdate,
                                datecreated, datemodified, state
                            ) VALUES (
                                '{bank_account}', '    ', 'A', '{post_date}', {-gross_fees}, '{reference[:20]}',
                                '{fees_comment[:50]}', '{fees_done_flag}', '   ', 0, 0, 0, 0,
                                'I', '{fees_unique}', '        ', '        ', {jrnl_num}, '{post_date}',
                                '{now_str}', '{now_str}', 1
                            )
                        """
                        conn.execute(text(anoml_fees_bank_sql))

                        # anoml Fees expense account (debit - net amount)
                        anoml_fees_expense_sql = f"""
                            INSERT INTO anoml (
                                ax_nacnt, ax_ncntr, ax_source, ax_date, ax_value, ax_tref,
                                ax_comment, ax_done, ax_fcurr, ax_fvalue, ax_fcrate, ax_fcmult, ax_fcdec,
                                ax_srcco, ax_unique, ax_project, ax_job, ax_jrnl, ax_nlpdate,
                                datecreated, datemodified, state
                            ) VALUES (
                                '{fees_nominal_account}', '    ', 'A', '{post_date}', {net_fees}, '{reference[:20]}',
                                '{fees_comment[:50]}', '{fees_done_flag}', '   ', 0, 0, 0, 0,
                                'I', '{fees_unique}', '        ', '        ', {jrnl_num}, '{post_date}',
                                '{now_str}', '{now_str}', 1
                            )
                        """
                        conn.execute(text(anoml_fees_expense_sql))

                        # anoml VAT account (debit - VAT amount) if VAT > 0
                        if vat_on_fees > 0:
                            anoml_fees_vat_sql = f"""
                                INSERT INTO anoml (
                                    ax_nacnt, ax_ncntr, ax_source, ax_date, ax_value, ax_tref,
                                    ax_comment, ax_done, ax_fcurr, ax_fvalue, ax_fcrate, ax_fcmult, ax_fcdec,
                                    ax_srcco, ax_unique, ax_project, ax_job, ax_jrnl, ax_nlpdate,
                                    datecreated, datemodified, state
                                ) VALUES (
                                    '{vat_nominal_account}', '    ', 'A', '{post_date}', {abs(vat_on_fees)}, '{reference[:20]}',
                                    '{fees_comment[:50]} VAT', '{fees_done_flag}', '   ', 0, 0, 0, 0,
                                    'I', '{fees_vat_unique}', '        ', '        ', {jrnl_num}, '{post_date}',
                                    '{now_str}', '{now_str}', 1
                                )
                            """
                            conn.execute(text(anoml_fees_vat_sql))

                    # Create zvtran entry for VAT tracking (for VAT return)
                        if vat_on_fees > 0:
                            zvtran_unique = OperaUniqueIdGenerator.generate()
                            zvtran_sql = f"""
                                INSERT INTO zvtran (
                                    va_source, va_account, va_laccnt, va_trdate, va_taxdate,
                                    va_ovrdate, va_trref, va_trtype, va_country, va_fcurr,
                                    va_trvalue, va_fcval, va_vatval, va_cost, va_vatctry,
                                    va_vattype, va_anvat, va_vatrate, va_box1, va_box2,
                                    va_box4, va_box6, va_box7, va_box8, va_box9,
                                    va_done, va_import, va_export,
                                    datecreated, datemodified, state
                                ) VALUES (
                                    'N', 'GOCARDLS', '{fees_nominal_account}', '{post_date}', '{post_date}',
                                    '{post_date}', '{reference[:20]}', 'B', 'GB', '   ',
                                    {net_fees}, 0, {abs(vat_on_fees)}, 0, 'H',
                                    'P', '{fees_vat_code}', {vat_rate}, 0, 0,
                                    1, 0, 1, 0, 0,
                                    0, 0, 0,
                                    '{now_str}', '{now_str}', 1
                                )
                            """
                            conn.execute(text(zvtran_sql))
                            logger.debug(f"Created zvtran for GoCardless fees VAT: £{vat_on_fees:.2f} (code={fees_vat_code}, rate={vat_rate}%, nominal={vat_nominal_account})")

                            # Create nvat record for VAT return tracking
                            # nv_vattype: 'P' = Purchase (input VAT, reclaimable)
                            # nv_vatcode: 'S' = Standard rate
                            nvat_sql = f"""
                                INSERT INTO nvat (
                                    nv_acnt, nv_cntr, nv_date, nv_crdate, nv_taxdate,
                                    nv_ref, nv_type, nv_advance, nv_value, nv_vatval,
                                    nv_vatctry, nv_vattype, nv_vatcode, nv_vatrate, nv_comment,
                                    datecreated, datemodified, state
                                ) VALUES (
                                    '{vat_nominal_account}', '', '{post_date}', '{post_date}', '{post_date}',
                                    '{reference[:20]}', 'P', 0, {net_fees}, {abs(vat_on_fees)},
                                    ' ', 'P', 'S', {vat_rate}, 'GoCardless fees VAT',
                                    '{now_str}', '{now_str}', 1
                                )
                            """
                            conn.execute(text(nvat_sql))
                            logger.debug(f"Created nvat for GoCardless fees VAT: £{vat_on_fees:.2f} (type=P, rate={vat_rate}%)")

            batch_status = "Completed" if complete_batch else "Open for review"
            logger.info(f"Successfully imported GoCardless batch: {entry_number} with {len(payments)} payments totalling £{gross_amount:.2f} - Posted to nominal and transfer file")

            # Auto-allocate receipts to invoices if requested
            allocation_results = []
            if auto_allocate:
                for payment in payments:
                    customer_account = payment['customer_account'].strip()
                    amount = float(payment['amount'])
                    description = payment.get('description', '')

                    alloc_result = self.auto_allocate_receipt(
                        customer_account=customer_account,
                        receipt_ref=reference,
                        receipt_amount=amount,
                        allocation_date=post_date,
                        bank_account=bank_account,
                        description=description
                    )

                    if alloc_result['success']:
                        allocation_results.append(
                            f"Auto-allocated {customer_account}: £{alloc_result['allocated_amount']:.2f} to {len(alloc_result['allocations'])} invoice(s)"
                        )
                    else:
                        allocation_results.append(
                            f"Allocation skipped for {customer_account}: {alloc_result['message']}"
                        )

            # Build fees detail message
            fees_detail = None
            fees_entry_msg = None
            if gocardless_fees > 0 and fees_nominal_account:
                if vat_on_fees > 0:
                    fees_detail = f"GoCardless fees: £{gocardless_fees:.2f} (Net: £{gocardless_fees - vat_on_fees:.2f} + VAT: £{vat_on_fees:.2f})"
                else:
                    fees_detail = f"GoCardless fees: £{gocardless_fees:.2f}"
                fees_entry_msg = f"Fees posted as separate payment (entry {fees_entry_number})"

            # Build allocation summary for warnings
            allocation_summary = None
            if auto_allocate and allocation_results:
                successful_allocs = [r for r in allocation_results if "Auto-allocated" in r]
                if successful_allocs:
                    allocation_summary = f"Auto-allocation: {len(successful_allocs)} receipt(s) allocated"

            # Auto-transfer net amount from GC Control bank to destination bank
            transfer_msg = None
            if destination_bank and destination_bank.strip() != bank_account.strip():
                try:
                    transfer_result = self.import_bank_transfer(
                        source_bank=bank_account,
                        dest_bank=destination_bank,
                        amount_pounds=net_amount,
                        reference=reference[:20],
                        post_date=post_date,
                        comment=f"GoCardless payout transfer",
                        input_by=input_by
                    )
                    if transfer_result.get('success'):
                        transfer_msg = f"Net £{net_amount:.2f} transferred from {bank_account} to {destination_bank}"
                        logger.info(f"GoCardless auto-transfer: {transfer_msg}")
                    else:
                        transfer_error = transfer_result.get('error', 'Unknown error')
                        transfer_msg = f"Transfer to {destination_bank} failed: {transfer_error} — post manually"
                        logger.error(f"GoCardless auto-transfer failed: {transfer_error}")
                except Exception as te:
                    transfer_msg = f"Transfer to {destination_bank} failed: {te} — post manually"
                    logger.error(f"GoCardless auto-transfer exception: {te}")

            return ImportResult(
                success=True,
                records_processed=len(payments),
                records_imported=len(payments),
                warnings=[
                    f"Receipts entry: {entry_number}",
                    f"Payments: {len(payments)}",
                    f"Gross amount: £{gross_amount:.2f}",
                    fees_detail,
                    fees_entry_msg,
                    f"Net to bank: £{net_amount:.2f}" if gocardless_fees else None,
                    f"VAT £{vat_on_fees:.2f} posted to {vat_nominal_used} (code {vat_code_used})" if vat_on_fees > 0 and vat_nominal_used else None,
                    transfer_msg,
                    f"Batch status: {batch_status}",
                    "Posted to nominal ledger, transfer file (anoml), and zvtran (VAT)" if vat_on_fees > 0 else "Posted to nominal ledger and transfer file (anoml)",
                    allocation_summary
                ] + (allocation_results if auto_allocate else [])
            )

        except Exception as e:
            logger.error(f"Failed to import GoCardless batch: {e}")
            return ImportResult(
                success=False,
                records_processed=len(payments),
                records_failed=len(payments),
                errors=[str(e)]
            )

    # =========================================================================
    # AUTO-ALLOCATION OF RECEIPTS TO INVOICES
    # =========================================================================

    def auto_allocate_receipt(
        self,
        customer_account: str,
        receipt_ref: str,
        receipt_amount: float,
        allocation_date: date,
        bank_account: str = "BC010",
        description: str = None
    ) -> Dict[str, Any]:
        """
        Automatically allocate a receipt to matching outstanding invoices.

        Allocation rules (in order):
        1. If invoice reference(s) found in description (e.g., "INV26241") AND their
           total matches the receipt exactly -> allocate to those specific invoices
        2. If receipt amount equals TOTAL outstanding balance on account AND there are
           2+ invoices -> allocate to ALL invoices (clears whole account, no ambiguity)

        Does NOT allocate:
        - Based on amount matching to individual invoices alone (may have duplicates)
        - Single invoice with no reference (GoCardless should include INV ref for single invoice)

        Args:
            customer_account: Customer code (e.g., 'K009')
            receipt_ref: Receipt reference in stran (e.g., 'INTSYSUKLTD-CK3P72')
            receipt_amount: Receipt amount in POUNDS (positive value)
            allocation_date: Date to use for allocation
            bank_account: Bank account code for salloc record
            description: Description to search for invoice references (optional)

        Returns:
            Dict with allocation results:
            - success: bool
            - allocated_amount: float
            - allocations: List of allocated invoices
            - message: str
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
            receipt_df = self.sql.execute_query(f"""
                SELECT st_trref, st_trvalue, st_trbal, st_paid, st_custref, st_unique
                FROM stran WITH (NOLOCK)
                WHERE st_account = '{customer_account}'
                  AND RTRIM(st_trref) = '{receipt_ref}'
                  AND st_trtype = 'R'
                  AND st_trbal < 0
            """)

            if receipt_df is None or len(receipt_df) == 0:
                result["message"] = f"Receipt {receipt_ref} not found or already allocated"
                return result

            receipt = receipt_df.iloc[0]
            receipt_balance = abs(float(receipt['st_trbal']))
            receipt_custref = receipt['st_custref'].strip() if receipt['st_custref'] else ''
            receipt_unique = receipt['st_unique'].strip() if receipt['st_unique'] else ''

            if receipt_balance <= 0:
                result["message"] = "Receipt already fully allocated"
                return result

            # Get outstanding invoices for customer
            invoices_df = self.sql.execute_query(f"""
                SELECT st_trref, st_trvalue, st_trbal, st_custref, st_trdate, st_unique
                FROM stran WITH (NOLOCK)
                WHERE st_account = '{customer_account}'
                  AND st_trtype = 'I'
                  AND st_trbal > 0
                ORDER BY st_trdate ASC, st_trref ASC
            """)

            if invoices_df is None or len(invoices_df) == 0:
                result["message"] = "No outstanding invoices found for customer"
                return result

            # Build list of invoices to allocate
            invoices_to_allocate = []
            allocation_method = None

            # Calculate total outstanding on account
            total_outstanding = round(sum(float(inv['st_trbal']) for _, inv in invoices_df.iterrows()), 2)
            receipt_rounded = round(receipt_amount, 2)

            # RULE 1: Try to match by invoice reference in description
            inv_matches = []
            if description:
                inv_matches = re.findall(r'INV\d+', description.upper())

            if inv_matches:
                # Found invoice reference(s) - try to match to specific invoices
                for inv_ref in inv_matches:
                    for _, inv in invoices_df.iterrows():
                        if inv['st_trref'].strip().upper() == inv_ref:
                            inv_balance = float(inv['st_trbal'])
                            if inv_balance > 0:
                                invoices_to_allocate.append({
                                    'ref': inv['st_trref'].strip(),
                                    'custref': inv['st_custref'].strip() if inv['st_custref'] else '',
                                    'amount': inv_balance,
                                    'full_allocation': True,
                                    'unique': inv['st_unique'].strip() if inv['st_unique'] else ''
                                })
                            break

                if invoices_to_allocate:
                    # Check if found invoices total matches receipt exactly
                    total_invoice_balance = round(sum(a['amount'] for a in invoices_to_allocate), 2)

                    if receipt_rounded == total_invoice_balance:
                        allocation_method = "invoice_reference"
                    else:
                        # Invoice refs found but amounts don't match
                        inv_details = [f"{a['ref']} (£{a['amount']:.2f})" for a in invoices_to_allocate]
                        result["message"] = (
                            f"Invoice reference(s) found but amounts do not match: "
                            f"receipt £{receipt_rounded:.2f} vs invoice total £{total_invoice_balance:.2f}. "
                            f"Found: {inv_details}"
                        )
                        return result

            # RULE 2: If no invoice ref match, check if receipt clears whole account
            # Now also handles single invoice case - if amount matches exactly, it's safe to allocate
            if not allocation_method:
                invoice_count = len(invoices_df)

                if receipt_rounded == total_outstanding and invoice_count >= 1:
                    # Receipt clears the whole account - allocate to ALL invoices
                    # For single invoice: amount match is sufficient (no ambiguity about target)
                    # For multiple invoices: clears entire balance (no ambiguity)
                    invoices_to_allocate = []
                    for _, inv in invoices_df.iterrows():
                        inv_balance = float(inv['st_trbal'])
                        if inv_balance > 0:
                            invoices_to_allocate.append({
                                'ref': inv['st_trref'].strip(),
                                'custref': inv['st_custref'].strip() if inv['st_custref'] else '',
                                'amount': inv_balance,
                                'full_allocation': True,
                                'unique': inv['st_unique'].strip() if inv['st_unique'] else ''
                            })
                    allocation_method = "clears_account" if invoice_count >= 2 else "single_invoice_match"
                else:
                    # Cannot allocate - no invoice ref and doesn't clear account
                    if inv_matches:
                        result["message"] = f"Invoice reference(s) {inv_matches} not found in outstanding invoices"
                    else:
                        result["message"] = (
                            f"Cannot auto-allocate: no invoice reference in description and "
                            f"receipt £{receipt_rounded:.2f} does not clear account total £{total_outstanding:.2f}"
                        )
                    return result

            # Amounts verified - proceed with allocation
            total_to_allocate = receipt_amount
            receipt_fully_allocated = True

            # Format date
            if isinstance(allocation_date, str):
                allocation_date = datetime.strptime(allocation_date, '%Y-%m-%d').date()
            alloc_date_str = allocation_date.strftime('%Y-%m-%d')
            now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Execute allocation within transaction
            with self.sql.engine.begin() as conn:
                conn.execute(text(get_lock_timeout_sql()))

                # Get next al_unique values
                max_unique_result = conn.execute(text("""
                    SELECT ISNULL(MAX(al_unique), 0) as max_unique FROM salloc WITH (UPDLOCK)
                """))
                next_unique = int(max_unique_result.scalar() or 0) + 1

                # Update receipt in stran
                new_receipt_bal = receipt_balance - total_to_allocate
                receipt_paid_flag = 'A' if receipt_fully_allocated else ' '
                receipt_payday = f"'{alloc_date_str}'" if receipt_fully_allocated else 'NULL'

                conn.execute(text(f"""
                    UPDATE stran WITH (ROWLOCK)
                    SET st_trbal = {-new_receipt_bal},
                        st_paid = '{receipt_paid_flag}',
                        st_payday = {receipt_payday},
                        datemodified = '{now_str}'
                    WHERE st_account = '{customer_account}'
                      AND RTRIM(st_trref) = '{receipt_ref}'
                      AND st_trtype = 'R'
                """))

                # Insert salloc record for receipt (if fully allocated)
                # al_ref2 indicates allocation method for audit trail
                if receipt_fully_allocated:
                    alloc_ref2 = "AUTO:INV_REF" if allocation_method == "invoice_reference" else "AUTO:CLR_ACCT"
                    conn.execute(text(f"""
                        INSERT INTO salloc (
                            al_account, al_date, al_ref1, al_ref2, al_type, al_val,
                            al_payind, al_payflag, al_payday, al_fcurr, al_fval, al_fdec,
                            al_advind, al_acnt, al_cntr, al_preprd, al_unique, al_adjsv,
                            datecreated, datemodified, state
                        ) VALUES (
                            '{customer_account}', '{receipt_df.iloc[0]["st_trdate"] if "st_trdate" in receipt_df.columns else alloc_date_str}',
                            '{receipt_ref}', '{alloc_ref2}', 'R', {-receipt_balance},
                            'A', 89, '{alloc_date_str}', '   ', 0, 0,
                            0, '{bank_account}', '    ', 0, {next_unique}, 0,
                            '{now_str}', '{now_str}', 1
                        )
                    """))
                    next_unique += 1

                # Update each invoice and create salloc records
                for alloc in invoices_to_allocate:
                    inv_ref = alloc['ref']
                    alloc_amount = alloc['amount']
                    is_full = alloc['full_allocation']
                    inv_custref = alloc['custref']

                    # Get current invoice balance
                    inv_current = conn.execute(text(f"""
                        SELECT st_trbal, st_trdate FROM stran WITH (NOLOCK)
                        WHERE st_account = '{customer_account}'
                          AND RTRIM(st_trref) = '{inv_ref}'
                          AND st_trtype = 'I'
                    """)).fetchone()

                    if inv_current:
                        new_inv_bal = float(inv_current[0]) - alloc_amount
                        inv_date = inv_current[1]
                        inv_paid_flag = 'P' if new_inv_bal < 0.01 else ' '
                        inv_payday = f"'{alloc_date_str}'" if new_inv_bal < 0.01 else 'NULL'

                        # Update invoice
                        conn.execute(text(f"""
                            UPDATE stran WITH (ROWLOCK)
                            SET st_trbal = {new_inv_bal},
                                st_paid = '{inv_paid_flag}',
                                st_payday = {inv_payday},
                                datemodified = '{now_str}'
                            WHERE st_account = '{customer_account}'
                              AND RTRIM(st_trref) = '{inv_ref}'
                              AND st_trtype = 'I'
                        """))

                        # Insert salloc record for invoice (if fully paid)
                        if new_inv_bal < 0.01:
                            conn.execute(text(f"""
                                INSERT INTO salloc (
                                    al_account, al_date, al_ref1, al_ref2, al_type, al_val,
                                    al_payind, al_payflag, al_payday, al_fcurr, al_fval, al_fdec,
                                    al_advind, al_acnt, al_cntr, al_preprd, al_unique, al_adjsv,
                                    datecreated, datemodified, state
                                ) VALUES (
                                    '{customer_account}', '{inv_date}',
                                    '{inv_ref}', '{inv_custref[:20]}', 'I', {alloc_amount},
                                    'A', 89, '{alloc_date_str}', '   ', 0, 0,
                                    0, '{bank_account}', '    ', 0, {next_unique}, 0,
                                    '{now_str}', '{now_str}', 1
                                )
                            """))
                            next_unique += 1

            result["success"] = True
            result["allocated_amount"] = total_to_allocate
            result["allocations"] = invoices_to_allocate
            result["receipt_fully_allocated"] = receipt_fully_allocated
            result["allocation_method"] = allocation_method

            if allocation_method == "invoice_reference":
                result["message"] = f"Allocated £{total_to_allocate:.2f} to {len(invoices_to_allocate)} invoice(s) by reference"
            else:  # clears_account
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
        bank_account: str = "BC010",
        description: str = None
    ) -> Dict[str, Any]:
        """
        Automatically allocate a payment to matching outstanding supplier invoices.

        Same allocation rules as auto_allocate_receipt:
        1. If invoice reference(s) found in description AND their total matches
           the payment exactly -> allocate to those specific invoices
        2. If payment amount equals TOTAL outstanding balance on account AND there are
           2+ invoices -> allocate to ALL invoices (clears whole account, no ambiguity)

        Does NOT allocate:
        - Based on amount matching to individual invoices alone (may have duplicates)
        - Single invoice with no reference (dangerous assumption)

        Args:
            supplier_account: Supplier code (e.g., 'P001')
            payment_ref: Payment reference in ptran (e.g., 'BACS-12345')
            payment_amount: Payment amount in POUNDS (positive value)
            allocation_date: Date to use for allocation
            bank_account: Bank account code for palloc record
            description: Description to search for invoice references (optional)

        Returns:
            Dict with allocation results:
            - success: bool
            - allocated_amount: float
            - allocations: List of allocated invoices
            - message: str
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
            payment_df = self.sql.execute_query(f"""
                SELECT pt_trref, pt_trvalue, pt_trbal, pt_paid, pt_suppref, pt_unique
                FROM ptran WITH (NOLOCK)
                WHERE pt_account = '{supplier_account}'
                  AND RTRIM(pt_trref) = '{payment_ref}'
                  AND pt_trtype = 'P'
                  AND pt_trbal < 0
            """)

            if payment_df is None or len(payment_df) == 0:
                result["message"] = f"Payment {payment_ref} not found or already allocated"
                return result

            payment = payment_df.iloc[0]
            payment_balance = abs(float(payment['pt_trbal']))
            payment_suppref = payment['pt_suppref'].strip() if payment['pt_suppref'] else ''
            payment_unique = payment['pt_unique'].strip() if payment['pt_unique'] else ''

            if payment_balance <= 0:
                result["message"] = "Payment already fully allocated"
                return result

            # Get outstanding invoices for supplier
            invoices_df = self.sql.execute_query(f"""
                SELECT pt_trref, pt_trvalue, pt_trbal, pt_suppref, pt_trdate, pt_unique
                FROM ptran WITH (NOLOCK)
                WHERE pt_account = '{supplier_account}'
                  AND pt_trtype = 'I'
                  AND pt_trbal > 0
                ORDER BY pt_trdate ASC, pt_trref ASC
            """)

            if invoices_df is None or len(invoices_df) == 0:
                result["message"] = "No outstanding invoices found for supplier"
                return result

            # Build list of invoices to allocate
            invoices_to_allocate = []
            allocation_method = None

            # Calculate total outstanding on account
            total_outstanding = round(sum(float(inv['pt_trbal']) for _, inv in invoices_df.iterrows()), 2)
            payment_rounded = round(payment_amount, 2)

            # RULE 1: Try to match by invoice reference in description
            # Look for common invoice patterns (PI, INV, or supplier-specific patterns)
            inv_matches = []
            if description:
                # Try various invoice reference patterns
                inv_matches = re.findall(r'(?:PI|INV|PINV|P/INV)[\s-]?\d+', description.upper())
                # Also try generic numeric references that might be supplier invoice numbers
                if not inv_matches:
                    # Look for references in ptran pt_suppref format
                    for _, inv in invoices_df.iterrows():
                        suppref = inv['pt_suppref'].strip() if inv['pt_suppref'] else ''
                        if suppref and suppref.upper() in description.upper():
                            inv_matches.append(suppref)

            if inv_matches:
                # Found invoice reference(s) - try to match to specific invoices
                for inv_ref_pattern in inv_matches:
                    inv_ref_clean = re.sub(r'[\s-]', '', inv_ref_pattern.upper())
                    for _, inv in invoices_df.iterrows():
                        inv_trref = inv['pt_trref'].strip().upper()
                        inv_suppref = (inv['pt_suppref'].strip().upper() if inv['pt_suppref'] else '')
                        inv_trref_clean = re.sub(r'[\s-]', '', inv_trref)
                        inv_suppref_clean = re.sub(r'[\s-]', '', inv_suppref)

                        if inv_ref_clean == inv_trref_clean or inv_ref_clean == inv_suppref_clean or inv_ref_pattern.upper() == inv_suppref:
                            inv_balance = float(inv['pt_trbal'])
                            if inv_balance > 0:
                                # Check not already added
                                already_added = any(a['ref'] == inv['pt_trref'].strip() for a in invoices_to_allocate)
                                if not already_added:
                                    invoices_to_allocate.append({
                                        'ref': inv['pt_trref'].strip(),
                                        'suppref': inv['pt_suppref'].strip() if inv['pt_suppref'] else '',
                                        'amount': inv_balance,
                                        'full_allocation': True,
                                        'unique': inv['pt_unique'].strip() if inv['pt_unique'] else ''
                                    })
                            break

                if invoices_to_allocate:
                    # Check if found invoices total matches payment exactly
                    total_invoice_balance = round(sum(a['amount'] for a in invoices_to_allocate), 2)

                    if payment_rounded == total_invoice_balance:
                        allocation_method = "invoice_reference"
                    else:
                        # Invoice refs found but amounts don't match
                        inv_details = [f"{a['ref']} (£{a['amount']:.2f})" for a in invoices_to_allocate]
                        result["message"] = (
                            f"Invoice reference(s) found but amounts do not match: "
                            f"payment £{payment_rounded:.2f} vs invoice total £{total_invoice_balance:.2f}. "
                            f"Found: {inv_details}"
                        )
                        return result

            # RULE 2: If no invoice ref match, check if payment clears whole account
            # Now also handles single invoice case - if amount matches exactly, it's safe to allocate
            if not allocation_method:
                invoice_count = len(invoices_df)

                if payment_rounded == total_outstanding and invoice_count >= 1:
                    # Payment clears the whole account - allocate to ALL invoices
                    # For single invoice: amount match is sufficient (no ambiguity about target)
                    # For multiple invoices: clears entire balance (no ambiguity)
                    invoices_to_allocate = []
                    for _, inv in invoices_df.iterrows():
                        inv_balance = float(inv['pt_trbal'])
                        if inv_balance > 0:
                            invoices_to_allocate.append({
                                'ref': inv['pt_trref'].strip(),
                                'suppref': inv['pt_suppref'].strip() if inv['pt_suppref'] else '',
                                'amount': inv_balance,
                                'full_allocation': True,
                                'unique': inv['pt_unique'].strip() if inv['pt_unique'] else ''
                            })
                    allocation_method = "clears_account" if invoice_count >= 2 else "single_invoice_match"
                else:
                    # Cannot allocate - no invoice ref and doesn't clear account
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

            # Format date
            if isinstance(allocation_date, str):
                allocation_date = datetime.strptime(allocation_date, '%Y-%m-%d').date()
            alloc_date_str = allocation_date.strftime('%Y-%m-%d')
            now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Execute allocation within transaction
            with self.sql.engine.begin() as conn:
                conn.execute(text(get_lock_timeout_sql()))

                # Get next pl_unique values
                max_unique_result = conn.execute(text("""
                    SELECT ISNULL(MAX(pl_unique), 0) as max_unique FROM palloc WITH (UPDLOCK)
                """))
                next_unique = int(max_unique_result.scalar() or 0) + 1

                # Update payment in ptran
                new_payment_bal = payment_balance - total_to_allocate
                payment_paid_flag = 'A' if payment_fully_allocated else ' '
                payment_payday = f"'{alloc_date_str}'" if payment_fully_allocated else 'NULL'

                conn.execute(text(f"""
                    UPDATE ptran WITH (ROWLOCK)
                    SET pt_trbal = {-new_payment_bal},
                        pt_paid = '{payment_paid_flag}',
                        pt_payday = {payment_payday},
                        datemodified = '{now_str}'
                    WHERE pt_account = '{supplier_account}'
                      AND RTRIM(pt_trref) = '{payment_ref}'
                      AND pt_trtype = 'P'
                """))

                # Insert palloc record for payment (if fully allocated)
                # pl_ref2 indicates allocation method for audit trail
                if payment_fully_allocated:
                    alloc_ref2 = "AUTO:INV_REF" if allocation_method == "invoice_reference" else "AUTO:CLR_ACCT"
                    conn.execute(text(f"""
                        INSERT INTO palloc (
                            pl_account, pl_date, pl_ref1, pl_ref2, pl_type, pl_val,
                            pl_payind, pl_payflag, pl_payday, pl_fcurr, pl_fval, pl_fdec,
                            pl_advind, pl_acnt, pl_cntr, pl_preprd, pl_unique, pl_adjsv,
                            datecreated, datemodified, state
                        ) VALUES (
                            '{supplier_account}', '{payment_df.iloc[0]["pt_trdate"] if "pt_trdate" in payment_df.columns else alloc_date_str}',
                            '{payment_ref}', '{alloc_ref2}', 'P', {-payment_balance},
                            'A', 89, '{alloc_date_str}', '   ', 0, 0,
                            0, '{bank_account}', '    ', 0, {next_unique}, 0,
                            '{now_str}', '{now_str}', 1
                        )
                    """))
                    next_unique += 1

                # Update each invoice and create palloc records
                for alloc in invoices_to_allocate:
                    inv_ref = alloc['ref']
                    alloc_amount = alloc['amount']
                    is_full = alloc['full_allocation']
                    inv_suppref = alloc['suppref']

                    # Get current invoice balance
                    inv_current = conn.execute(text(f"""
                        SELECT pt_trbal, pt_trdate FROM ptran WITH (NOLOCK)
                        WHERE pt_account = '{supplier_account}'
                          AND RTRIM(pt_trref) = '{inv_ref}'
                          AND pt_trtype = 'I'
                    """)).fetchone()

                    if inv_current:
                        new_inv_bal = float(inv_current[0]) - alloc_amount
                        inv_date = inv_current[1]
                        inv_paid_flag = 'P' if new_inv_bal < 0.01 else ' '
                        inv_payday = f"'{alloc_date_str}'" if new_inv_bal < 0.01 else 'NULL'

                        # Update invoice
                        conn.execute(text(f"""
                            UPDATE ptran WITH (ROWLOCK)
                            SET pt_trbal = {new_inv_bal},
                                pt_paid = '{inv_paid_flag}',
                                pt_payday = {inv_payday},
                                datemodified = '{now_str}'
                            WHERE pt_account = '{supplier_account}'
                              AND RTRIM(pt_trref) = '{inv_ref}'
                              AND pt_trtype = 'I'
                        """))

                        # Insert palloc record for invoice (if fully paid)
                        if new_inv_bal < 0.01:
                            conn.execute(text(f"""
                                INSERT INTO palloc (
                                    pl_account, pl_date, pl_ref1, pl_ref2, pl_type, pl_val,
                                    pl_payind, pl_payflag, pl_payday, pl_fcurr, pl_fval, pl_fdec,
                                    pl_advind, pl_acnt, pl_cntr, pl_preprd, pl_unique, pl_adjsv,
                                    datecreated, datemodified, state
                                ) VALUES (
                                    '{supplier_account}', '{inv_date}',
                                    '{inv_ref}', '{inv_suppref[:20]}', 'I', {alloc_amount},
                                    'A', 89, '{alloc_date_str}', '   ', 0, 0,
                                    0, '{bank_account}', '    ', 0, {next_unique}, 0,
                                    '{now_str}', '{now_str}', 1
                                )
                            """))
                            next_unique += 1

            result["success"] = True
            result["allocated_amount"] = total_to_allocate
            result["allocations"] = invoices_to_allocate
            result["payment_fully_allocated"] = payment_fully_allocated
            result["allocation_method"] = allocation_method

            if allocation_method == "invoice_reference":
                result["message"] = f"Allocated £{total_to_allocate:.2f} to {len(invoices_to_allocate)} invoice(s) by reference"
            else:  # clears_account
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
        reconciliation_date: date = None
    ) -> ImportResult:
        """
        Mark cashbook entries as reconciled (replicates Opera's Bank Reconciliation).

        This function updates aentry records to mark them as reconciled and updates
        the nbank master record with the new reconciled balance.

        Args:
            bank_account: Bank account code (e.g., 'BC010')
            entries: List of entries to reconcile, each containing:
                - entry_number: The ae_entry value (e.g., 'P100008036')
                - statement_line: Statement line number (10, 20, 30, etc.)
            statement_number: Bank statement number
            statement_date: Date on the bank statement (defaults to today)
            reconciliation_date: Date of reconciliation (defaults to today)

        Returns:
            ImportResult with details of the reconciliation

        Example:
            result = opera_import.mark_entries_reconciled(
                bank_account='BC010',
                entries=[
                    {'entry_number': 'P100008036', 'statement_line': 10},
                    {'entry_number': 'PR00000534', 'statement_line': 20},
                ],
                statement_number=86918,
                statement_date=date(2026, 2, 8)
            )
        """
        if not entries:
            return ImportResult(
                success=False,
                errors=["No entries provided for reconciliation"]
            )

        if statement_date is None:
            statement_date = date.today()
        if reconciliation_date is None:
            reconciliation_date = date.today()

        # Format dates
        if isinstance(statement_date, str):
            statement_date = datetime.strptime(statement_date, '%Y-%m-%d').date()
        if isinstance(reconciliation_date, str):
            reconciliation_date = datetime.strptime(reconciliation_date, '%Y-%m-%d').date()

        stmt_date_str = statement_date.strftime('%Y-%m-%d')
        rec_date_str = reconciliation_date.strftime('%Y-%m-%d')
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        try:
            with self.sql.engine.connect() as conn:
                trans = conn.begin()
                try:
                    # 1. Get current nbank state
                    nbank_result = conn.execute(text(f"""
                        SELECT nk_lstrecl, nk_recbal, nk_curbal, nk_lststno
                        FROM nbank WITH (NOLOCK)
                        WHERE nk_acnt = '{bank_account}'
                    """))
                    nbank_row = nbank_result.fetchone()
                    if not nbank_row:
                        raise ValueError(f"Bank account {bank_account} not found in nbank")

                    current_rec_line = int(nbank_row[0])  # nk_lstrecl
                    current_rec_balance = float(nbank_row[1])  # nk_recbal (in pence)
                    current_balance = float(nbank_row[2])  # nk_curbal (in pence)

                    # The reconciliation batch number to assign to entries
                    rec_batch_number = current_rec_line

                    # 2. Get the entries to reconcile and validate they exist
                    entry_numbers = [e['entry_number'] for e in entries]
                    entry_list = "', '".join(entry_numbers)

                    validate_result = conn.execute(text(f"""
                        SELECT ae_entry, ae_value, ae_reclnum
                        FROM aentry WITH (NOLOCK)
                        WHERE ae_acnt = '{bank_account}'
                          AND ae_entry IN ('{entry_list}')
                    """))
                    found_entries = {row[0]: {'value': float(row[1]), 'reclnum': float(row[2])}
                                     for row in validate_result}

                    # Validate all entries exist and are not already reconciled
                    errors = []
                    total_value = 0
                    for entry in entries:
                        entry_num = entry['entry_number']
                        if entry_num not in found_entries:
                            errors.append(f"Entry {entry_num} not found")
                        elif found_entries[entry_num]['reclnum'] != 0:
                            errors.append(f"Entry {entry_num} already reconciled (reclnum={found_entries[entry_num]['reclnum']})")
                        else:
                            total_value += found_entries[entry_num]['value']

                    if errors:
                        trans.rollback()
                        return ImportResult(
                            success=False,
                            errors=errors
                        )

                    # 3. Calculate new reconciled balance (final total after all entries)
                    # total_value is in pence, add to current_rec_balance
                    new_rec_balance = current_rec_balance + total_value

                    # 4. Update each aentry record with running balance
                    # Sort entries by statement line number to ensure correct running balance order
                    sorted_entries = sorted(entries, key=lambda e: e.get('statement_line', 0))

                    # Start running balance from current reconciled balance
                    running_balance = current_rec_balance

                    for entry in sorted_entries:
                        entry_num = entry['entry_number']
                        stmt_line = entry.get('statement_line', 0)
                        entry_value = found_entries[entry_num]['value']

                        # ae_recbal is the running reconciled balance AFTER this entry
                        # Add this entry's value to get the cumulative balance at this point
                        running_balance += entry_value
                        entry_rec_bal = running_balance

                        # ae_statln = line number only (N6, max 999999)
                        # ae_frstat/ae_tostat = statement number (N8)
                        # Opera displays combined as "86911/10" but stores separately
                        update_sql = f"""
                            UPDATE aentry WITH (ROWLOCK)
                            SET ae_reclnum = {rec_batch_number},
                                ae_recdate = '{rec_date_str}',
                                ae_statln = {stmt_line},
                                ae_frstat = {statement_number},
                                ae_tostat = {statement_number},
                                ae_tmpstat = 0,
                                ae_recbal = {int(entry_rec_bal)},
                                datemodified = '{now_str}'
                            WHERE ae_acnt = '{bank_account}'
                              AND ae_entry = '{entry_num}'
                        """
                        conn.execute(text(update_sql))
                        logger.info(f"Marked {entry_num} as reconciled (batch {rec_batch_number}, stmt {statement_number}/{stmt_line}, running bal: {entry_rec_bal/100:.2f})")

                    # 5. Update nbank master record
                    new_rec_line = rec_batch_number + 1  # Increment for next batch

                    nbank_update_sql = f"""
                        UPDATE nbank WITH (ROWLOCK)
                        SET nk_recbal = {int(new_rec_balance)},
                            nk_lstrecl = {new_rec_line},
                            nk_lststno = {statement_number},
                            nk_lststdt = '{stmt_date_str}',
                            nk_reclnum = {new_rec_line},
                            nk_recldte = '{rec_date_str}',
                            nk_recstfr = {statement_number},
                            nk_recstto = {statement_number},
                            nk_recstdt = '{stmt_date_str}',
                            datemodified = '{now_str}'
                        WHERE nk_acnt = '{bank_account}'
                    """
                    conn.execute(text(nbank_update_sql))

                    trans.commit()

                    # Re-read nk_recbal to verify it was written correctly
                    verify_result = conn.execute(text(f"""
                        SELECT nk_recbal FROM nbank WITH (NOLOCK)
                        WHERE nk_acnt = '{bank_account}'
                    """))
                    verify_row = verify_result.fetchone()
                    verified_rec_balance = float(verify_row[0]) / 100.0 if verify_row else None

                    # Convert pence to pounds for reporting
                    total_pounds = total_value / 100.0
                    new_rec_pounds = new_rec_balance / 100.0
                    remaining_pounds = (current_balance - new_rec_balance) / 100.0

                    logger.info(f"Bank reconciliation complete: {len(entries)} entries, £{total_pounds:,.2f}")

                    return ImportResult(
                        success=True,
                        records_processed=len(entries),
                        records_imported=len(entries),
                        new_reconciled_balance=verified_rec_balance,
                        warnings=[
                            f"Reconciled {len(entries)} entries totalling £{total_pounds:,.2f}",
                            f"New reconciled balance: £{new_rec_pounds:,.2f}",
                            f"Remaining unreconciled: £{remaining_pounds:,.2f}",
                            f"Statement number: {statement_number}",
                            f"Reconciliation batch: {rec_batch_number}"
                        ]
                    )

                except Exception as e:
                    trans.rollback()
                    raise

        except Exception as e:
            logger.error(f"Failed to mark entries reconciled: {e}")
            return ImportResult(
                success=False,
                errors=[str(e)]
            )

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
        Pre-flight duplicate check before posting a transaction to Opera.

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
        from datetime import date, timedelta

        if isinstance(transaction_date, str):
            txn_date = date.fromisoformat(transaction_date[:10])
        else:
            txn_date = transaction_date

        date_from = (txn_date - timedelta(days=date_tolerance_days)).strftime('%Y-%m-%d')
        date_to = (txn_date + timedelta(days=date_tolerance_days)).strftime('%Y-%m-%d')
        amount_pence = int(round(amount_pounds * 100))

        # Check 1: Cashbook (atran) - amounts in PENCE
        query = f"""
            SELECT TOP 1 a.at_entry, a.at_pstdate, a.at_value, e.ae_entref, e.ae_comment
            FROM atran a WITH (NOLOCK)
            JOIN aentry e WITH (NOLOCK) ON e.ae_entry = a.at_entry AND e.ae_acnt = a.at_acnt
            WHERE a.at_acnt = '{bank_account}'
            AND a.at_pstdate BETWEEN '{date_from}' AND '{date_to}'
            AND ABS(ABS(a.at_value) - {amount_pence}) < 1
        """
        df = self.sql.execute_query(query)
        if df is not None and len(df) > 0:
            row = df.iloc[0]
            entry = row.get('at_entry', '?')
            ref = (row.get('ae_entref', '') or '').strip()
            comment = (row.get('ae_comment', '') or '').strip()
            return {
                'is_duplicate': True,
                'location': 'cashbook',
                'details': f"Entry {entry} already exists in cashbook (ref: {ref}, {comment})",
                'entry_number': str(entry)
            }

        # Check 2: Sales Ledger for customer receipts
        if account_type == 'customer' and account_code:
            query = f"""
                SELECT TOP 1 st_trref, st_trdate, st_trvalue
                FROM stran WITH (NOLOCK)
                WHERE RTRIM(st_account) = '{account_code}'
                AND st_trdate BETWEEN '{date_from}' AND '{date_to}'
                AND ABS(ABS(st_trvalue) - {amount_pounds}) < 0.01
                AND st_trtype = 'R'
            """
            df = self.sql.execute_query(query)
            if df is not None and len(df) > 0:
                row = df.iloc[0]
                ref = (row.get('st_trref', '') or '').strip()
                return {
                    'is_duplicate': True,
                    'location': 'sales_ledger',
                    'details': f"Receipt already exists in sales ledger for {account_code} (ref: {ref})",
                    'entry_number': ref
                }

        # Check 3: Purchase Ledger for supplier payments
        if account_type == 'supplier' and account_code:
            query = f"""
                SELECT TOP 1 pt_trref, pt_trdate, pt_trvalue
                FROM ptran WITH (NOLOCK)
                WHERE RTRIM(pt_account) = '{account_code}'
                AND pt_trdate BETWEEN '{date_from}' AND '{date_to}'
                AND ABS(ABS(pt_trvalue) - {amount_pounds}) < 0.01
                AND pt_trtype = 'P'
            """
            df = self.sql.execute_query(query)
            if df is not None and len(df) > 0:
                row = df.iloc[0]
                ref = (row.get('pt_trref', '') or '').strip()
                return {
                    'is_duplicate': True,
                    'location': 'purchase_ledger',
                    'details': f"Payment already exists in purchase ledger for {account_code} (ref: {ref})",
                    'entry_number': ref
                }

        return {'is_duplicate': False, 'details': ''}

    def get_unreconciled_entries(self, bank_account: str, include_incomplete: bool = False) -> List[Dict[str, Any]]:
        """
        Get list of unreconciled cashbook entries for a bank account.

        Args:
            bank_account: Bank account code (e.g., 'BC010')
            include_incomplete: If False (default), only returns completed batches (ae_complet=1).
                               If True, includes incomplete/hidden batches (ae_complet=0).

        Returns:
            List of unreconciled entries with details.
            Each entry includes 'is_complete' flag indicating if batch is complete.
            Incomplete batches are "hidden" Opera transactions that haven't been posted to NL.
        """
        query = f"""
            SELECT ae_entry, ae_value/100.0 as value_pounds, ae_lstdate,
                   ae_cbtype, ae_entref, ae_comment, ae_complet
            FROM aentry WITH (NOLOCK)
            WHERE ae_acnt = '{bank_account}'
              AND ae_reclnum = 0
              {'AND ae_complet = 1' if not include_incomplete else ''}
            ORDER BY ae_lstdate, ae_entry
        """
        df = self.sql.execute_query(query)
        if df is None or len(df) == 0:
            return []

        # Add is_complete flag to each entry
        records = df.to_dict('records')
        for r in records:
            r['is_complete'] = bool(r.get('ae_complet', 0))
        return records

    def get_reconciliation_status(self, bank_account: str) -> Dict[str, Any]:
        """
        Get current reconciliation status for a bank account.

        Args:
            bank_account: Bank account code (e.g., 'BC010')

        Returns:
            Dict with reconciliation status including balances and counts
        """
        # Get nbank status
        nbank_query = f"""
            SELECT nk_recbal/100.0 as reconciled_balance,
                   nk_curbal/100.0 as current_balance,
                   nk_lstrecl as last_rec_line,
                   nk_lststno as last_stmt_no,
                   nk_lststdt as last_stmt_date,
                   nk_recldte as last_rec_date
            FROM nbank WITH (NOLOCK)
            WHERE nk_acnt = '{bank_account}'
        """
        nbank_df = self.sql.execute_query(nbank_query)

        # Get unreconciled summary (only completed batches)
        unrec_query = f"""
            SELECT COUNT(*) as count, COALESCE(SUM(ae_value), 0)/100.0 as total
            FROM aentry WITH (NOLOCK)
            WHERE ae_acnt = '{bank_account}'
              AND ae_reclnum = 0
              AND ae_complet = 1
        """
        unrec_df = self.sql.execute_query(unrec_query)

        if nbank_df is None or len(nbank_df) == 0:
            return {'error': f'Bank account {bank_account} not found'}

        nbank = nbank_df.iloc[0]
        unrec = unrec_df.iloc[0] if unrec_df is not None and len(unrec_df) > 0 else {'count': 0, 'total': 0}

        return {
            'bank_account': bank_account,
            'reconciled_balance': float(nbank['reconciled_balance']),
            'current_balance': float(nbank['current_balance']),
            'unreconciled_difference': float(nbank['current_balance'] - nbank['reconciled_balance']),
            'unreconciled_count': int(unrec['count']),
            'unreconciled_total': float(unrec['total']),
            'last_rec_line': int(nbank['last_rec_line']),
            'last_stmt_no': int(nbank['last_stmt_no']) if nbank['last_stmt_no'] else None,
            'last_stmt_date': nbank['last_stmt_date'],
            'last_rec_date': nbank['last_rec_date']
        }

    # =========================================================================
    # AUTO-RECONCILIATION METHODS
    # =========================================================================

    def validate_statement_for_reconciliation(
        self,
        bank_account: str,
        opening_balance: float,
        closing_balance: float,
        statement_number: Optional[int] = None,
        statement_date: Optional[date] = None
    ) -> Dict[str, Any]:
        """
        Validate that a statement is ready for reconciliation.

        Checks:
        1. Opening balance matches Opera's expected (nk_recbal)
        2. Statement number is correct sequence

        Args:
            bank_account: Bank account code (e.g., 'BC010')
            opening_balance: Statement opening balance (in pounds)
            closing_balance: Statement closing balance (in pounds)
            statement_number: Statement number from bank (if present)
            statement_date: Statement date

        Returns:
            Dict with validation result and expected values
        """
        try:
            # Get current bank state
            query = f"""
                SELECT nk_recbal/100.0 as expected_opening,
                       nk_lststno as last_statement_number,
                       nk_curbal/100.0 as current_balance
                FROM nbank WITH (NOLOCK)
                WHERE nk_acnt = '{bank_account}'
            """
            df = self.sql.execute_query(query)

            if df is None or len(df) == 0:
                return {
                    'valid': False,
                    'error_message': f'Bank account {bank_account} not found'
                }

            row = df.iloc[0]
            expected_opening = float(row['expected_opening'])
            last_stmt_no = int(row['last_statement_number']) if row['last_statement_number'] else 0
            next_stmt_no = last_stmt_no + 1

            # Use statement number from bank if provided, else use Opera's sequence
            effective_stmt_no = statement_number if statement_number else next_stmt_no

            # Check opening balance matches (within 1 penny tolerance for rounding)
            opening_matches = abs(opening_balance - expected_opening) < 0.01

            if not opening_matches:
                return {
                    'valid': False,
                    'expected_opening': expected_opening,
                    'statement_opening': opening_balance,
                    'difference': round(opening_balance - expected_opening, 2),
                    'opening_matches': False,
                    'next_statement_number': effective_stmt_no,
                    'error_message': f'Opening balance mismatch: Statement shows £{opening_balance:,.2f}, Opera expects £{expected_opening:,.2f}'
                }

            return {
                'valid': True,
                'expected_opening': expected_opening,
                'statement_opening': opening_balance,
                'statement_closing': closing_balance,
                'opening_matches': True,
                'next_statement_number': effective_stmt_no,
                'statement_date': statement_date.isoformat() if statement_date else None,
                'error_message': None
            }

        except Exception as e:
            logger.error(f"Failed to validate statement: {e}")
            return {
                'valid': False,
                'error_message': str(e)
            }

    def match_statement_to_cashbook(
        self,
        bank_account: str,
        statement_transactions: List[Dict[str, Any]],
        date_tolerance_days: int = 3
    ) -> Dict[str, Any]:
        """
        Match statement lines to unreconciled cashbook entries.

        Matching tiers:
        1. Auto-match: Exact reference + exact amount (from imported entries)
        2. Suggested match: Approx date + exact amount + same account
        3. Unmatched: No match found

        Args:
            bank_account: Bank account code
            statement_transactions: List of statement lines, each with:
                - line_number: Position on statement (1, 2, 3...)
                - date: Transaction date
                - amount: Amount in pounds (positive for receipts, negative for payments)
                - reference: Bank reference
                - description: Transaction description
            date_tolerance_days: Days tolerance for suggested matches

        Returns:
            Dict with:
                - auto_matched: List of confident matches
                - suggested_matched: List of probable matches for user review
                - unmatched_statement: Statement lines with no match
                - unmatched_cashbook: Cashbook entries not on statement
        """
        try:
            # Get all unreconciled entries for this bank (both complete and incomplete)
            # Note: No JOIN to atran - one aentry can have multiple atran lines
            # which would cause duplicates. We only need aentry fields for matching.
            # Include incomplete entries (ae_complet = 0) as they still represent
            # real bank transactions that should be reconcilable.
            query = f"""
                SELECT ae_entry, ae_value/100.0 as amount_pounds, ae_lstdate,
                       ae_entref, ae_comment, ae_cbtype, ae_complet
                FROM aentry WITH (NOLOCK)
                WHERE ae_acnt = '{bank_account}'
                  AND ae_reclnum = 0
                ORDER BY ae_lstdate, ae_entry
            """
            df = self.sql.execute_query(query)

            if df is None:
                df_records = []
            else:
                df_records = df.to_dict('records')

            logger.info(f"Statement matching: {len(df_records)} unreconciled entries for bank {bank_account}, {len(statement_transactions)} statement lines")
            if df_records:
                sample = df_records[0]
                logger.info(f"  Sample Opera entry: entry={sample.get('ae_entry')}, amount={sample.get('amount_pounds')}, date={str(sample.get('ae_lstdate',''))[:10]}, ref='{sample.get('ae_entref','')}'")
            if statement_transactions:
                sample_st = statement_transactions[0]
                logger.info(f"  Sample statement line: line={sample_st.get('line_number')}, amount={sample_st.get('amount')}, date={sample_st.get('date')}, ref='{sample_st.get('reference','')}'")

            # Build lookup structures for cashbook entries
            # Key by reference (for exact match)
            entries_by_ref = {}
            # Key by amount (for amount-based matching)
            entries_by_amount = {}
            # Track which entries are matched
            matched_entry_numbers = set()

            for entry in df_records:
                ref = str(entry.get('ae_entref', '')).strip().upper()
                amount = round(float(entry.get('amount_pounds', 0)), 2)
                entry_num = entry.get('ae_entry')

                if ref:
                    if ref not in entries_by_ref:
                        entries_by_ref[ref] = []
                    entries_by_ref[ref].append(entry)

                if amount not in entries_by_amount:
                    entries_by_amount[amount] = []
                entries_by_amount[amount].append(entry)

            auto_matched = []
            suggested_matched = []
            unmatched_statement = []

            for stmt_line in statement_transactions:
                line_num = stmt_line.get('line_number', 0)
                stmt_date = stmt_line.get('date')
                stmt_amount = round(float(stmt_line.get('amount', 0)), 2)
                stmt_ref = str(stmt_line.get('reference', '')).strip().upper()
                stmt_desc = stmt_line.get('description', '')

                # Convert date if string
                if isinstance(stmt_date, str):
                    stmt_date = datetime.strptime(stmt_date[:10], '%Y-%m-%d').date()

                matched = False
                match_entry = None
                match_confidence = 0

                # Tier 1: Exact reference + amount match
                if stmt_ref and stmt_ref in entries_by_ref:
                    for entry in entries_by_ref[stmt_ref]:
                        entry_num = entry.get('ae_entry')
                        if entry_num in matched_entry_numbers:
                            continue
                        entry_amount = round(float(entry.get('amount_pounds', 0)), 2)
                        if abs(entry_amount - stmt_amount) < 0.01:
                            match_entry = entry
                            match_confidence = 100
                            matched = True
                            break

                # Tier 2: Amount + approximate date match (for existing entries)
                if not matched and stmt_amount in entries_by_amount:
                    for entry in entries_by_amount[stmt_amount]:
                        entry_num = entry.get('ae_entry')
                        if entry_num in matched_entry_numbers:
                            continue

                        entry_date = entry.get('ae_lstdate')
                        if entry_date:
                            if isinstance(entry_date, str):
                                entry_date = datetime.strptime(entry_date[:10], '%Y-%m-%d').date()
                            elif hasattr(entry_date, 'date'):
                                entry_date = entry_date.date()

                            # Check date within tolerance
                            if stmt_date and entry_date:
                                date_diff = abs((stmt_date - entry_date).days)
                                if date_diff <= date_tolerance_days:
                                    match_entry = entry
                                    # Confidence based on date proximity
                                    if date_diff == 0:
                                        match_confidence = 90
                                    elif date_diff <= 1:
                                        match_confidence = 85
                                    else:
                                        match_confidence = 75
                                    matched = True
                                    break

                if matched and match_entry:
                    entry_num = match_entry.get('ae_entry')
                    matched_entry_numbers.add(entry_num)

                    match_record = {
                        'statement_line': line_num,
                        'statement_date': stmt_date.isoformat() if stmt_date else None,
                        'statement_amount': stmt_amount,
                        'statement_reference': stmt_ref,
                        'statement_description': stmt_desc,
                        'statement_balance': stmt_line.get('balance'),
                        'entry_number': entry_num,
                        'entry_date': str(match_entry.get('ae_lstdate', ''))[:10],
                        'entry_amount': round(float(match_entry.get('amount_pounds', 0)), 2),
                        'entry_reference': str(match_entry.get('ae_entref', '')).strip(),
                        'entry_description': str(match_entry.get('ae_comment', '')).strip(),
                        'confidence': match_confidence
                    }

                    if match_confidence >= 95:
                        auto_matched.append(match_record)
                    else:
                        suggested_matched.append(match_record)
                else:
                    unmatched_statement.append({
                        'statement_line': line_num,
                        'statement_date': stmt_date.isoformat() if stmt_date else None,
                        'statement_amount': stmt_amount,
                        'statement_reference': stmt_ref,
                        'statement_description': stmt_desc,
                        'statement_balance': stmt_line.get('balance')
                    })

            # Find cashbook entries not matched to any statement line
            unmatched_cashbook = []
            for entry in df_records:
                entry_num = entry.get('ae_entry')
                if entry_num not in matched_entry_numbers:
                    unmatched_cashbook.append({
                        'entry_number': entry_num,
                        'entry_date': str(entry.get('ae_lstdate', ''))[:10],
                        'entry_amount': round(float(entry.get('amount_pounds', 0)), 2),
                        'entry_reference': str(entry.get('ae_entref', '')).strip(),
                        'entry_description': str(entry.get('ae_comment', '')).strip()
                    })

            logger.info(f"Statement matching result: auto={len(auto_matched)}, suggested={len(suggested_matched)}, unmatched_stmt={len(unmatched_statement)}, unmatched_cb={len(unmatched_cashbook)}")
            if unmatched_statement:
                for u in unmatched_statement[:3]:
                    logger.info(f"  Unmatched stmt line {u.get('statement_line')}: amount={u.get('statement_amount')}, date={u.get('statement_date')}, ref='{u.get('statement_reference','')}'")
                # Log amounts available in Opera for comparison
                stmt_amounts = {round(float(s.get('amount', 0)), 2) for s in statement_transactions}
                opera_amounts = set(entries_by_amount.keys())
                common = stmt_amounts & opera_amounts
                only_stmt = stmt_amounts - opera_amounts
                only_opera = opera_amounts - stmt_amounts
                if only_stmt:
                    logger.info(f"  Statement amounts NOT in Opera: {sorted(list(only_stmt))[:10]}")
                if only_opera:
                    logger.info(f"  Opera amounts NOT in statement: {sorted(list(only_opera))[:10]}")
                if common:
                    logger.info(f"  Common amounts: {sorted(list(common))[:10]}")

            return {
                'success': True,
                'auto_matched': auto_matched,
                'suggested_matched': suggested_matched,
                'unmatched_statement': unmatched_statement,
                'unmatched_cashbook': unmatched_cashbook,
                'summary': {
                    'total_statement_lines': len(statement_transactions),
                    'auto_matched_count': len(auto_matched),
                    'suggested_matched_count': len(suggested_matched),
                    'unmatched_statement_count': len(unmatched_statement),
                    'unmatched_cashbook_count': len(unmatched_cashbook)
                }
            }

        except Exception as e:
            logger.error(f"Failed to match statement to cashbook: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def calculate_statement_line_numbers(
        self,
        total_lines: int,
        matched_positions: List[int],
        unmatched_positions: List[int]
    ) -> Dict[int, int]:
        """
        Calculate statement line numbers (ae_statln) with gap logic.

        Opera uses line numbers 10, 20, 30... with gaps (1-9, 11-19...) for
        inserting unmatched items later.

        If >9 unmatched items exist before a matched item, the gap must be larger.

        Args:
            total_lines: Total number of statement lines
            matched_positions: List of statement line positions that are matched (1-based)
            unmatched_positions: List of statement line positions that are unmatched

        Returns:
            Dict mapping statement position (1-based) to ae_statln value
        """
        line_numbers = {}
        matched_set = set(matched_positions)
        unmatched_set = set(unmatched_positions)

        # For each position, count unmatched items before it
        current_line = 0
        for pos in range(1, total_lines + 1):
            if pos in matched_set:
                # Count unmatched items before this position
                unmatched_before = sum(1 for p in unmatched_set if p < pos)

                # Calculate line number with sufficient gap
                # Each unmatched item needs a slot of 10
                # So if there are N unmatched before, matched item needs line >= (N+1) * 10
                min_line = (unmatched_before + 1) * 10

                # Also ensure we're past the previous assigned line
                if current_line >= min_line:
                    min_line = current_line + 10

                line_numbers[pos] = min_line
                current_line = min_line

        return line_numbers

    def complete_reconciliation(
        self,
        bank_account: str,
        statement_number: int,
        statement_date: date,
        closing_balance: float,
        matched_entries: List[Dict[str, Any]],
        statement_transactions: List[Dict[str, Any]],
        partial: bool = False
    ) -> ImportResult:
        """
        Complete bank reconciliation - mark all matched entries as reconciled.

        This validates the closing balance and updates all Opera tables.
        When partial=True, skips closing balance validation (unmatched lines
        remain for completion in Opera Cashbook > Reconcile).

        Args:
            bank_account: Bank account code
            statement_number: Statement number to assign
            statement_date: Statement date
            closing_balance: Expected closing balance from statement (pounds)
            matched_entries: List of matched entries, each with:
                - entry_number: Opera entry number (ae_entry)
                - statement_line: Position on statement (1-based)
            statement_transactions: Original statement transactions for line number calculation
            partial: If True, skip closing balance validation (partial reconciliation)

        Returns:
            ImportResult with reconciliation outcome
        """
        if not matched_entries:
            return ImportResult(
                success=False,
                errors=["No entries to reconcile"]
            )

        try:
            # Get current bank state
            status = self.get_reconciliation_status(bank_account)
            if 'error' in status:
                return ImportResult(success=False, errors=[status['error']])

            expected_opening = status['reconciled_balance']

            # Calculate what closing balance should be
            # Get values of matched entries
            entry_numbers = [e['entry_number'] for e in matched_entries]
            entry_list = "', '".join(entry_numbers)

            query = f"""
                SELECT ae_entry, ae_value
                FROM aentry WITH (NOLOCK)
                WHERE ae_acnt = '{bank_account}'
                  AND ae_entry IN ('{entry_list}')
            """
            df = self.sql.execute_query(query)

            if df is None or len(df) == 0:
                return ImportResult(success=False, errors=["Could not find entries to reconcile"])

            # Sum all entry values (in pence)
            total_value_pence = sum(float(row['ae_value']) for _, row in df.iterrows())
            total_value_pounds = total_value_pence / 100.0

            calculated_closing = expected_opening + total_value_pounds

            # Validate closing balance (within 1 penny tolerance)
            # Skip validation for partial reconciliation (unmatched lines remain)
            if not partial and abs(calculated_closing - closing_balance) >= 0.01:
                return ImportResult(
                    success=False,
                    errors=[
                        f"Closing balance mismatch: calculated £{calculated_closing:,.2f}, "
                        f"statement shows £{closing_balance:,.2f}. "
                        f"Difference: £{abs(calculated_closing - closing_balance):,.2f}"
                    ]
                )

            # Calculate statement line numbers with gap logic
            matched_positions = [e['statement_line'] for e in matched_entries]
            total_lines = len(statement_transactions)
            unmatched_positions = [i for i in range(1, total_lines + 1) if i not in matched_positions]

            line_numbers = self.calculate_statement_line_numbers(
                total_lines, matched_positions, unmatched_positions
            )

            # Build entries list for mark_entries_reconciled
            entries_with_lines = []
            for entry in matched_entries:
                stmt_pos = entry['statement_line']
                entries_with_lines.append({
                    'entry_number': entry['entry_number'],
                    'statement_line': line_numbers.get(stmt_pos, stmt_pos * 10)
                })

            # Call existing mark_entries_reconciled
            result = self.mark_entries_reconciled(
                bank_account=bank_account,
                entries=entries_with_lines,
                statement_number=statement_number,
                statement_date=statement_date,
                reconciliation_date=date.today()
            )

            if result.success:
                if partial:
                    result.warnings.append(
                        f"Partial reconciliation - matched entries posted with line numbers. "
                        f"Complete remaining items in Opera Cashbook > Reconcile."
                    )
                else:
                    result.warnings.append(f"Closing balance validated: £{closing_balance:,.2f}")

            return result

        except Exception as e:
            logger.error(f"Failed to complete reconciliation: {e}")
            return ImportResult(
                success=False,
                errors=[str(e)]
            )

    # =========================================================================
    # BANK TRANSFER IMPORT (at_type=8 - Internal transfer between bank accounts)
    # Creates paired entries in both source and destination bank accounts
    # =========================================================================

    def import_bank_transfer(
        self,
        source_bank: str,
        dest_bank: str,
        amount_pounds: float,
        reference: str,
        post_date: date,
        comment: str = "",
        input_by: str = "SQLRAG",
        post_to_nominal: bool = True
    ) -> Dict[str, Any]:
        """
        Import a bank transfer between two Opera bank accounts.

        Creates paired entries in:
        - aentry (2 records - one per bank with opposite signs)
        - atran (2 records with at_type=8)
        - anoml (2 records with ax_source='A')
        - ntran (2 records if post_to_nominal=True)
        - Updates nbank balances for both accounts
        - Updates nacnt balances for both accounts

        Args:
            source_bank: Source bank account code (e.g., 'BC010')
            dest_bank: Destination bank account code (e.g., 'BC020')
            amount_pounds: Transfer amount (positive value)
            reference: Transaction reference (max 20 chars)
            post_date: Date of transfer
            comment: Optional comment
            input_by: User who entered (max 8 chars)
            post_to_nominal: Whether to post to nominal ledger

        Returns:
            Dict with source_entry, dest_entry, success status
        """
        errors = []
        warnings = []

        # Validate amount is positive
        if amount_pounds <= 0:
            return {
                'success': False,
                'error': "Transfer amount must be positive"
            }

        # Validate source and dest are different
        if source_bank.strip().upper() == dest_bank.strip().upper():
            return {
                'success': False,
                'error': "Source and destination bank accounts must be different"
            }

        try:
            # =====================
            # VALIDATE BANK ACCOUNTS
            # =====================

            # Check source bank exists and is not foreign currency
            source_check = self.sql.execute_query(f"""
                SELECT nk_acnt, nk_desc, nk_fcurr
                FROM nbank WITH (NOLOCK)
                WHERE RTRIM(nk_acnt) = '{source_bank}'
            """)
            if source_check.empty:
                return {
                    'success': False,
                    'error': f"Source bank account '{source_bank}' not found in nbank"
                }
            source_name = source_check.iloc[0]['nk_desc'].strip()
            source_fcurr = source_check.iloc[0]['nk_fcurr']
            if source_fcurr and source_fcurr.strip():
                return {
                    'success': False,
                    'error': f"Source bank '{source_bank}' is a foreign currency account - transfers not supported"
                }

            # Check dest bank exists and is not foreign currency
            dest_check = self.sql.execute_query(f"""
                SELECT nk_acnt, nk_desc, nk_fcurr
                FROM nbank WITH (NOLOCK)
                WHERE RTRIM(nk_acnt) = '{dest_bank}'
            """)
            if dest_check.empty:
                return {
                    'success': False,
                    'error': f"Destination bank account '{dest_bank}' not found in nbank"
                }
            dest_name = dest_check.iloc[0]['nk_desc'].strip()
            dest_fcurr = dest_check.iloc[0]['nk_fcurr']
            if dest_fcurr and dest_fcurr.strip():
                return {
                    'success': False,
                    'error': f"Destination bank '{dest_bank}' is a foreign currency account - transfers not supported"
                }

            # =====================
            # GET TRANSFER TYPE CODE (T1)
            # =====================
            # Bank transfers use T-prefixed type codes
            transfer_type = self.get_default_cbtype_for_transfer()
            if not transfer_type:
                return {
                    'success': False,
                    'error': "No Transfer type codes (ay_type='T') found in atype table"
                }

            # =====================
            # PERIOD POSTING DECISION
            # =====================
            from sql_rag.opera_config import get_period_posting_decision
            # Bank transfers post to nominal ledger, so use 'NL' for period check
            posting_decision = get_period_posting_decision(self.sql, post_date, 'NL')

            if not posting_decision.can_post:
                return {
                    'success': False,
                    'error': posting_decision.error_message
                }

            # =====================
            # PREPARE AMOUNTS AND VARIABLES
            # =====================
            amount_pence = int(round(amount_pounds * 100))

            if isinstance(post_date, str):
                post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

            year = post_date.year
            period = post_date.month

            now = datetime.now()
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')
            date_str = now.strftime('%Y-%m-%d')
            time_str = now.strftime('%H:%M:%S')

            # Generate shared unique ID for paired entries
            shared_unique = OperaUniqueIdGenerator.generate()

            # Build trnref like Opera does for bank transfers
            ntran_comment = f"{reference[:50]:<50}"
            source_ntran_trnref = f"{dest_name[:30]:<30}Transfer  (RT)     "
            dest_ntran_trnref = f"{source_name[:30]:<30}Transfer  (RT)     "

            # Generate unique IDs for ntran records
            unique_ids = OperaUniqueIdGenerator.generate_multiple(2)
            ntran_pstid_source = unique_ids[0]
            ntran_pstid_dest = unique_ids[1]

            # Bank transfer at_type is 8
            AT_TYPE_TRANSFER = 8.0

            # =====================
            # LOCK ORDERING: To prevent deadlocks when two concurrent transfers
            # go in opposite directions (A->B and B->A), we ALWAYS process banks
            # in alphabetical order. The "first" and "second" banks may differ
            # from source/dest, but the SIGNS remain correct for source/dest.
            # =====================
            banks_ordered = sorted([source_bank, dest_bank])
            first_bank = banks_ordered[0]
            second_bank = banks_ordered[1]
            # Determine if source is first or second in lock order
            source_is_first = (source_bank == first_bank)

            # =====================
            # EXECUTE ALL OPERATIONS IN A SINGLE TRANSACTION
            # =====================
            with self.sql.engine.begin() as conn:
                # Set lock timeout
                conn.execute(text(get_lock_timeout_sql()))

                # Get entry numbers - TWO entries, one for each bank
                # Allocate in lock order to maintain consistency
                first_entry = self.increment_atype_entry(conn, transfer_type)
                second_entry = self.increment_atype_entry(conn, transfer_type)

                # Map back to source/dest
                source_entry = first_entry if source_is_first else second_entry
                dest_entry = second_entry if source_is_first else first_entry

                # Get next journal number
                journal_result = conn.execute(text("""
                    SELECT ISNULL(MAX(nt_jrnl), 0) + 1 as next_journal
                    FROM ntran WITH (UPDLOCK, ROWLOCK)
                """))
                next_journal = journal_result.scalar() or 1

                # ae_complet flag - only 1 if posting to nominal
                ae_complet_flag = 1 if posting_decision.post_to_nominal else 0

                # =====================
                # 1. INSERT SOURCE BANK aentry (NEGATIVE - money going OUT)
                # =====================
                aentry_source_sql = f"""
                    INSERT INTO aentry (
                        ae_acnt, ae_cntr, ae_cbtype, ae_entry, ae_reclnum,
                        ae_lstdate, ae_frstat, ae_tostat, ae_statln, ae_entref,
                        ae_value, ae_recbal, ae_remove, ae_tmpstat, ae_complet,
                        ae_postgrp, sq_crdate, sq_crtime, sq_cruser, ae_comment,
                        ae_payid, ae_batchid, ae_brwptr, datecreated, datemodified, state
                    ) VALUES (
                        '{source_bank}', '    ', '{transfer_type}', '{source_entry}', 0,
                        '{post_date}', 0, 0, 0, '{reference[:20]}',
                        {-amount_pence}, 0, 0, 0, {ae_complet_flag},
                        0, '{date_str}', '{time_str[:8]}', '{input_by[:8]}', '{comment.replace(chr(10), " ").replace(chr(13), " ")[:40].replace("'", "''")}',
                        0, 0, '  ', '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(aentry_source_sql))

                # =====================
                # 2. INSERT DEST BANK aentry (POSITIVE - money coming IN)
                # =====================
                aentry_dest_sql = f"""
                    INSERT INTO aentry (
                        ae_acnt, ae_cntr, ae_cbtype, ae_entry, ae_reclnum,
                        ae_lstdate, ae_frstat, ae_tostat, ae_statln, ae_entref,
                        ae_value, ae_recbal, ae_remove, ae_tmpstat, ae_complet,
                        ae_postgrp, sq_crdate, sq_crtime, sq_cruser, ae_comment,
                        ae_payid, ae_batchid, ae_brwptr, datecreated, datemodified, state
                    ) VALUES (
                        '{dest_bank}', '    ', '{transfer_type}', '{dest_entry}', 0,
                        '{post_date}', 0, 0, 0, '{reference[:20]}',
                        {amount_pence}, 0, 0, 0, {ae_complet_flag},
                        0, '{date_str}', '{time_str[:8]}', '{input_by[:8]}', '{comment.replace(chr(10), " ").replace(chr(13), " ")[:40].replace("'", "''")}',
                        0, 0, '  ', '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(aentry_dest_sql))

                # =====================
                # 3. INSERT SOURCE BANK atran (at_type=8, at_account=dest bank)
                # =====================
                atran_source_sql = f"""
                    INSERT INTO atran (
                        at_acnt, at_cntr, at_cbtype, at_entry, at_inputby,
                        at_type, at_pstdate, at_sysdate, at_tperiod, at_value,
                        at_disc, at_fcurr, at_fcexch, at_fcmult, at_fcdec,
                        at_account, at_name, at_comment, at_payee, at_payname,
                        at_sort, at_number, at_remove, at_chqprn, at_chqlst,
                        at_bacprn, at_ccdprn, at_ccdno, at_payslp, at_pysprn,
                        at_cash, at_remit, at_unique, at_postgrp, at_ccauth,
                        at_refer, at_srcco, at_ecb, at_ecbtype, at_atpycd,
                        at_bsref, at_bsname, at_vattycd, at_project, at_job,
                        at_bic, at_iban, at_memo, datecreated, datemodified, state
                    ) VALUES (
                        '{source_bank}', '    ', '{transfer_type}', '{source_entry}', '{input_by[:8]}',
                        {AT_TYPE_TRANSFER}, '{post_date}', '{post_date}', 1, {-amount_pence},
                        0, '   ', 1.0, 0, 2,
                        '{dest_bank}', '{dest_name[:35]}', '{comment.replace(chr(10), " ").replace(chr(13), " ")[:40].replace("'", "''")}', '        ', '',
                        '        ', '         ', 0, 0, 0,
                        0, 0, '', 0, 0,
                        0, 0, '{shared_unique}', 0, '0       ',
                        '{reference[:20]}', 'I', 0, ' ', '      ',
                        '', '', '  ', '        ', '        ',
                        '', '', '', '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(atran_source_sql))

                # =====================
                # 4. INSERT DEST BANK atran (at_type=8, at_account=source bank)
                # =====================
                atran_dest_sql = f"""
                    INSERT INTO atran (
                        at_acnt, at_cntr, at_cbtype, at_entry, at_inputby,
                        at_type, at_pstdate, at_sysdate, at_tperiod, at_value,
                        at_disc, at_fcurr, at_fcexch, at_fcmult, at_fcdec,
                        at_account, at_name, at_comment, at_payee, at_payname,
                        at_sort, at_number, at_remove, at_chqprn, at_chqlst,
                        at_bacprn, at_ccdprn, at_ccdno, at_payslp, at_pysprn,
                        at_cash, at_remit, at_unique, at_postgrp, at_ccauth,
                        at_refer, at_srcco, at_ecb, at_ecbtype, at_atpycd,
                        at_bsref, at_bsname, at_vattycd, at_project, at_job,
                        at_bic, at_iban, at_memo, datecreated, datemodified, state
                    ) VALUES (
                        '{dest_bank}', '    ', '{transfer_type}', '{dest_entry}', '{input_by[:8]}',
                        {AT_TYPE_TRANSFER}, '{post_date}', '{post_date}', 1, {amount_pence},
                        0, '   ', 1.0, 0, 2,
                        '{source_bank}', '{source_name[:35]}', '{comment.replace(chr(10), " ").replace(chr(13), " ")[:40].replace("'", "''")}', '        ', '',
                        '        ', '         ', 0, 0, 0,
                        0, 0, '', 0, 0,
                        0, 0, '{shared_unique}', 0, '0       ',
                        '{reference[:20]}', 'I', 0, ' ', '      ',
                        '', '', '  ', '        ', '        ',
                        '', '', '', '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(atran_dest_sql))

                # =====================
                # 5. Nominal postings - CONDITIONAL based on period posting decision
                # IMPORTANT: Process in LOCK ORDER (alphabetical by bank code) to prevent deadlocks
                # =====================
                if posting_decision.post_to_nominal:
                    # Determine values for first and second bank (in lock order)
                    # Source bank always has negative value (money out), dest has positive (money in)
                    first_value = -amount_pounds if source_is_first else amount_pounds
                    second_value = amount_pounds if source_is_first else -amount_pounds
                    first_trnref = source_ntran_trnref if source_is_first else dest_ntran_trnref
                    second_trnref = dest_ntran_trnref if source_is_first else source_ntran_trnref
                    first_pstid = ntran_pstid_source if source_is_first else ntran_pstid_dest
                    second_pstid = ntran_pstid_dest if source_is_first else ntran_pstid_source

                    # ntran for FIRST bank (in lock order)
                    ntran_first_sql = f"""
                        INSERT INTO ntran (
                            nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                            nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                            nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                            nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                            nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                            nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                            nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                            nt_distrib, datecreated, datemodified, state
                        ) VALUES (
                            '{first_bank}', '    ', 'B ', 'BC', {next_journal},
                            '', '{input_by[:10]}', 'A', '{ntran_comment}', '{first_trnref}',
                            '{post_date}', {first_value}, {year}, {period}, 0,
                            0, 0, '   ', 0, 0,
                            0, 0, 'I', '', '        ',
                            '        ', 'T', 0, '{first_pstid}', 0,
                            0, 0, 0, 0, 0,
                            0, '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(ntran_first_sql))
                    # Update nacnt and nbank for FIRST bank
                    self.update_nacnt_balance(conn, first_bank, first_value, period)
                    self.update_nbank_balance(conn, first_bank, first_value)

                    # ntran for SECOND bank (in lock order)
                    ntran_second_sql = f"""
                        INSERT INTO ntran (
                            nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                            nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                            nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                            nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                            nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                            nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                            nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                            nt_distrib, datecreated, datemodified, state
                        ) VALUES (
                            '{second_bank}', '    ', 'B ', 'BC', {next_journal},
                            '', '{input_by[:10]}', 'A', '{ntran_comment}', '{second_trnref}',
                            '{post_date}', {second_value}, {year}, {period}, 0,
                            0, 0, '   ', 0, 0,
                            0, 0, 'I', '', '        ',
                            '        ', 'T', 0, '{second_pstid}', 0,
                            0, 0, 0, 0, 0,
                            0, '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(ntran_second_sql))
                    # Update nacnt and nbank for SECOND bank
                    self.update_nacnt_balance(conn, second_bank, second_value, period)
                    self.update_nbank_balance(conn, second_bank, second_value)

                # =====================
                # 6. INSERT INTO anoml (transfer file) - ax_source='A' for Admin
                # IMPORTANT: Process in LOCK ORDER to prevent deadlocks
                # =====================
                if posting_decision.post_to_transfer_file:
                    done_flag = posting_decision.transfer_file_done_flag
                    jrnl_num = next_journal if posting_decision.post_to_nominal else 0

                    # anoml for FIRST bank (in lock order)
                    first_anoml_value = -amount_pounds if source_is_first else amount_pounds
                    anoml_first_sql = f"""
                        INSERT INTO anoml (
                            ax_nacnt, ax_ncntr, ax_source, ax_date, ax_value, ax_tref,
                            ax_comment, ax_done, ax_fcurr, ax_fvalue, ax_fcrate, ax_fcmult, ax_fcdec,
                            ax_srcco, ax_unique, ax_project, ax_job, ax_jrnl, ax_nlpdate,
                            datecreated, datemodified, state
                        ) VALUES (
                            '{first_bank}', '    ', 'A', '{post_date}', {first_anoml_value}, '{reference[:20]}',
                            '{ntran_comment[:50]}', '{done_flag}', '   ', 0, 0, 0, 0,
                            'I', '{shared_unique}', '        ', '        ', {jrnl_num}, '{post_date}',
                            '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(anoml_first_sql))

                    # anoml for SECOND bank (in lock order)
                    second_anoml_value = amount_pounds if source_is_first else -amount_pounds
                    anoml_second_sql = f"""
                        INSERT INTO anoml (
                            ax_nacnt, ax_ncntr, ax_source, ax_date, ax_value, ax_tref,
                            ax_comment, ax_done, ax_fcurr, ax_fvalue, ax_fcrate, ax_fcmult, ax_fcdec,
                            ax_srcco, ax_unique, ax_project, ax_job, ax_jrnl, ax_nlpdate,
                            datecreated, datemodified, state
                        ) VALUES (
                            '{second_bank}', '    ', 'A', '{post_date}', {second_anoml_value}, '{reference[:20]}',
                            '{ntran_comment[:50]}', '{done_flag}', '   ', 0, 0, 0, 0,
                            'I', '{shared_unique}', '        ', '        ', {jrnl_num}, '{post_date}',
                            '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(anoml_second_sql))

            # Build summary
            tables_updated = ["aentry (2)", "atran (2)"]
            if posting_decision.post_to_nominal:
                tables_updated.append("ntran (2)")
                tables_updated.append("nbank (2)")
                tables_updated.append("nacnt (2)")
            if posting_decision.post_to_transfer_file:
                tables_updated.append("anoml (2)")

            posting_mode = "Current period - posted to nominal" if posting_decision.post_to_nominal else "Different period - transfer file only"

            logger.info(f"Successfully imported bank transfer: {source_entry}/{dest_entry} for £{amount_pounds:.2f}")

            return {
                'success': True,
                'source_entry': source_entry,
                'dest_entry': dest_entry,
                'source_bank': source_bank,
                'dest_bank': dest_bank,
                'amount': amount_pounds,
                'journal_number': next_journal if posting_decision.post_to_nominal else None,
                'shared_unique': shared_unique,
                'posting_mode': posting_mode,
                'tables_updated': tables_updated,
                'message': f"Transfer created: {source_bank} -> {dest_bank} for £{amount_pounds:.2f}"
            }

        except Exception as e:
            logger.error(f"Failed to import bank transfer: {e}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'error': str(e)
            }

    def get_default_cbtype_for_transfer(self) -> Optional[str]:
        """
        Get the first available Transfer type code from atype (ay_type='T').

        Returns:
            Transfer type code (e.g., 'T1') or None if not found
        """
        try:
            df = self.sql.execute_query("""
                SELECT ay_cbtype
                FROM atype
                WHERE ay_type = 'T'
                ORDER BY ay_cbtype
            """)
            if df.empty:
                return None
            return df.iloc[0]['ay_cbtype'].strip()
        except Exception as e:
            logger.error(f"Error getting transfer type: {e}")
            return None

    def get_bank_accounts_for_transfer(self) -> List[Dict[str, str]]:
        """
        Get list of bank accounts valid for transfers (non-foreign currency).

        Returns:
            List of dicts with 'code' and 'name' keys
        """
        try:
            df = self.sql.execute_query("""
                SELECT nk_acnt, nk_desc, nk_fcurr
                FROM nbank
                ORDER BY nk_acnt
            """)
            if df.empty:
                return []

            accounts = []
            for _, row in df.iterrows():
                fcurr = row['nk_fcurr']
                # Skip foreign currency accounts (fcurr is not blank)
                if fcurr and fcurr.strip():
                    continue
                accounts.append({
                    'code': row['nk_acnt'].strip(),
                    'name': row['nk_desc'].strip() if row['nk_desc'] else ''
                })
            return accounts

        except Exception as e:
            logger.error(f"Error getting bank accounts: {e}")
            return []


# =========================================================================
# CSV FILE IMPORT CLASSES
# =========================================================================

class SalesInvoiceFileImport:
    """
    Import sales invoices from a CSV file.

    CSV columns:
        - customer_account (required): Customer account code
        - invoice_number (required): Invoice number/reference
        - net_amount (required): Net amount in pounds
        - vat_code (required): VAT code (e.g., '2' for standard 20%)
        - post_date (required): Posting date (YYYY-MM-DD)
        - description (optional): Invoice description
        - sales_nominal (optional): Sales nominal account (default E4030)
        - customer_ref (optional): Customer's reference/PO number
    """

    def __init__(self, sql_connector=None):
        if sql_connector is None:
            from sql_rag.sql_connector import SQLConnector
            sql_connector = SQLConnector()
        self.sql = sql_connector
        self.opera_import = OperaSQLImport(sql_connector)

    def import_file(self, filepath: str, validate_only: bool = False) -> ImportResult:
        """
        Import sales invoices from a CSV file.

        Args:
            filepath: Path to CSV file
            validate_only: If True, only validate without posting

        Returns:
            ImportResult with details of all imports
        """
        total_processed = 0
        total_imported = 0
        total_failed = 0
        all_errors = []
        all_warnings = []

        try:
            with open(filepath, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
        except Exception as e:
            return ImportResult(
                success=False,
                records_processed=0,
                errors=[f"Failed to read CSV file: {e}"]
            )

        for idx, row in enumerate(rows, 1):
            total_processed += 1

            try:
                # Validate required fields
                customer_account = row.get('customer_account', '').strip()
                invoice_number = row.get('invoice_number', '').strip()
                net_amount_str = row.get('net_amount', '').strip()
                vat_code = row.get('vat_code', '').strip()
                post_date_str = row.get('post_date', '').strip()

                if not customer_account:
                    all_errors.append(f"Row {idx}: Missing customer_account")
                    total_failed += 1
                    continue
                if not invoice_number:
                    all_errors.append(f"Row {idx}: Missing invoice_number")
                    total_failed += 1
                    continue
                if not net_amount_str:
                    all_errors.append(f"Row {idx}: Missing net_amount")
                    total_failed += 1
                    continue
                if not vat_code:
                    all_errors.append(f"Row {idx}: Missing vat_code")
                    total_failed += 1
                    continue
                if not post_date_str:
                    all_errors.append(f"Row {idx}: Missing post_date")
                    total_failed += 1
                    continue

                # Parse values
                try:
                    net_amount = float(net_amount_str)
                except ValueError:
                    all_errors.append(f"Row {idx}: Invalid net_amount '{net_amount_str}'")
                    total_failed += 1
                    continue

                try:
                    post_date = datetime.strptime(post_date_str, '%Y-%m-%d').date()
                except ValueError:
                    try:
                        post_date = datetime.strptime(post_date_str, '%d/%m/%Y').date()
                    except ValueError:
                        all_errors.append(f"Row {idx}: Invalid post_date '{post_date_str}' (use YYYY-MM-DD or DD/MM/YYYY)")
                        total_failed += 1
                        continue

                # Look up VAT rate
                vat_info = self.opera_import.get_vat_rate(vat_code, 'S', post_date)
                if not vat_info['found']:
                    all_warnings.append(f"Row {idx}: VAT code '{vat_code}' not found, using 0%")

                vat_rate = vat_info['rate']
                vat_amount = net_amount * (vat_rate / 100.0)
                vat_nominal = vat_info['nominal']

                # Optional fields
                description = row.get('description', '').strip()
                sales_nominal = row.get('sales_nominal', 'E4030').strip()
                customer_ref = row.get('customer_ref', '').strip()

                # Import the invoice
                result = self._import_single_invoice(
                    customer_account=customer_account,
                    invoice_number=invoice_number,
                    net_amount=net_amount,
                    vat_amount=vat_amount,
                    vat_code=vat_code,
                    vat_rate=vat_rate,
                    post_date=post_date,
                    description=description,
                    sales_nominal=sales_nominal,
                    vat_nominal=vat_nominal,
                    customer_ref=customer_ref,
                    validate_only=validate_only
                )

                if result.success:
                    total_imported += 1
                    all_warnings.extend([f"Row {idx}: {w}" for w in result.warnings])
                else:
                    total_failed += 1
                    all_errors.extend([f"Row {idx}: {e}" for e in result.errors])

            except Exception as e:
                total_failed += 1
                all_errors.append(f"Row {idx}: {str(e)}")

        return ImportResult(
            success=total_failed == 0,
            records_processed=total_processed,
            records_imported=total_imported,
            records_failed=total_failed,
            errors=all_errors,
            warnings=all_warnings
        )

    def _import_single_invoice(
        self,
        customer_account: str,
        invoice_number: str,
        net_amount: float,
        vat_amount: float,
        vat_code: str,
        vat_rate: float,
        post_date: date,
        description: str,
        sales_nominal: str,
        vat_nominal: str,
        customer_ref: str,
        validate_only: bool
    ) -> ImportResult:
        """Import a single sales invoice with VAT tracking."""
        errors = []
        warnings = []
        gross_amount = net_amount + vat_amount

        try:
            # Validate customer
            customer_check = self.sql.execute_query(f"""
                SELECT TOP 1 sn_name, sn_region, sn_terrtry, sn_custype
                FROM sname
                WHERE RTRIM(sn_account) = '{customer_account}'
            """)
            if customer_check.empty:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[f"Customer '{customer_account}' not found"]
                )

            customer_name = customer_check.iloc[0]['sn_name'].strip()
            customer_region = customer_check.iloc[0]['sn_region'].strip() if customer_check.iloc[0]['sn_region'] else 'K'
            customer_terr = customer_check.iloc[0]['sn_terrtry'].strip() if customer_check.iloc[0]['sn_terrtry'] else '001'
            customer_type = customer_check.iloc[0]['sn_custype'].strip() if customer_check.iloc[0]['sn_custype'] else 'DD1'

            if validate_only:
                return ImportResult(
                    success=True,
                    records_processed=1,
                    records_imported=1,
                    warnings=[f"Validation passed: {invoice_number} £{gross_amount:.2f}"]
                )

            # Generate sequence numbers
            journal_result = self.sql.execute_query("""
                SELECT ISNULL(MAX(nt_jrnl), 0) + 1 as next_journal FROM ntran
            """)
            next_journal = journal_result.iloc[0]['next_journal']

            unique_ids = OperaUniqueIdGenerator.generate_multiple(5)
            stran_unique = unique_ids[0]
            ntran_pstid_vat = unique_ids[1]
            ntran_pstid_sales = unique_ids[2]
            ntran_pstid_control = unique_ids[3]
            zvtran_unique = unique_ids[4]

            if isinstance(post_date, str):
                post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

            year = post_date.year
            period = post_date.month

            from datetime import timedelta
            due_date = post_date + timedelta(days=14)

            now = datetime.now()
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')

            # Get control account - check customer profile first, then fall back to default
            from sql_rag.opera_config import get_customer_control_account
            debtors_control = get_customer_control_account(self.sql, customer_account)
            department = "U999"
            sales_subt = sales_nominal[:2] if len(sales_nominal) >= 2 else 'E4'

            ntran_comment = f"{invoice_number[:20]:<20} {description[:29]:<29}"
            ntran_trnref = f"{customer_name[:30]:<30}{customer_ref[:20]:<20}"

            stran_memo = f"Analysis of Invoice {invoice_number[:20]} Dated {post_date.strftime('%d/%m/%Y')}"

            # Execute all inserts
            with self.sql.engine.begin() as conn:
                # 1. stran
                stran_sql = f"""
                    INSERT INTO stran (
                        st_account, st_trdate, st_trref, st_custref, st_trtype,
                        st_trvalue, st_vatval, st_trbal, st_paid, st_crdate,
                        st_advance, st_memo, st_payflag, st_set1day, st_set1,
                        st_set2day, st_set2, st_dueday, st_fcurr, st_fcrate,
                        st_fcdec, st_fcval, st_fcbal, st_fcmult, st_dispute,
                        st_edi, st_editx, st_edivn, st_txtrep, st_binrep,
                        st_advallc, st_cbtype, st_entry, st_unique, st_region,
                        st_terr, st_type, st_fadval, st_delacc, st_euro,
                        st_payadvl, st_eurind, st_origcur, st_fullamt, st_fullcb,
                        st_fullnar, st_cash, st_rcode, st_ruser, st_revchrg,
                        st_nlpdate, st_adjsv, st_fcvat, st_taxpoin,
                        datecreated, datemodified, state
                    ) VALUES (
                        '{customer_account}', '{post_date}', '{invoice_number[:20]}', '{customer_ref[:20]}', 'I',
                        {gross_amount}, {vat_amount}, {gross_amount}, ' ', '{post_date}',
                        'N', '{stran_memo[:200]}', 0, 0, 0,
                        0, 0, '{due_date}', '   ', 0,
                        0, 0, 0, 0, 0,
                        0, 0, 0, '', 0,
                        0, '  ', '          ', '{stran_unique}', '{customer_region[:3]}',
                        '{customer_terr[:3]}', '{customer_type[:3]}', 0, '{customer_account}', 0,
                        0, ' ', '   ', 0, '  ',
                        '          ', 0, '    ', '        ', 0,
                        '{post_date}', 0, 0, '{post_date}',
                        '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(stran_sql))

                # 2. ntran - VAT
                if vat_amount > 0:
                    ntran_vat_sql = f"""
                        INSERT INTO ntran (
                            nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                            nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                            nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                            nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                            nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                            nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                            nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                            nt_distrib, datecreated, datemodified, state
                        ) VALUES (
                            '{vat_nominal}', '    ', 'C ', 'CA', {next_journal},
                            '          ', 'IMPORT', 'S', '{ntran_comment}', '{ntran_trnref}',
                            '{post_date}', {-vat_amount}, {year}, {period}, 0,
                            0, 0, '   ', 0, 0,
                            0, 0, 'I', '', '        ',
                            '        ', 'I', 0, '{ntran_pstid_vat}', 0,
                            0, 0, 0, 0, 0,
                            0, '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(ntran_vat_sql))
                    # Update nacnt balance for VAT account (CREDIT)
                    self.update_nacnt_balance(conn, vat_nominal, -vat_amount, period)

                # 3. ntran - Sales
                ntran_sales_sql = f"""
                    INSERT INTO ntran (
                        nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                        nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                        nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                        nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                        nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                        nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                        nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                        nt_distrib, datecreated, datemodified, state
                    ) VALUES (
                        '{sales_nominal}', '    ', 'E ', '{sales_subt}', {next_journal},
                        '          ', 'IMPORT', 'S', '{ntran_comment}', '{ntran_trnref}',
                        '{post_date}', {-net_amount}, {year}, {period}, 0,
                        0, 0, '   ', 0, 0,
                        0, 0, 'I', '', '{customer_account}',
                        '{department}', 'I', 0, '{ntran_pstid_sales}', 0,
                        0, 0, 0, 0, 0,
                        0, '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(ntran_sales_sql))
                # Update nacnt balance for sales account (CREDIT)
                self.update_nacnt_balance(conn, sales_nominal, -net_amount, period)

                # 4. ntran - Debtors Control
                ntran_control_sql = f"""
                    INSERT INTO ntran (
                        nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                        nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                        nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                        nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                        nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                        nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                        nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                        nt_distrib, datecreated, datemodified, state
                    ) VALUES (
                        '{debtors_control}', '    ', 'B ', 'BB', {next_journal},
                        '          ', 'IMPORT', 'S', '', 'Sales Ledger Transfer (RT)                        ',
                        '{post_date}', {gross_amount}, {year}, {period}, 0,
                        0, 0, '   ', 0, 0,
                        0, 0, 'I', '', '        ',
                        '        ', 'I', 0, '{ntran_pstid_control}', 0,
                        0, 0, 0, 0, 0,
                        0, '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(ntran_control_sql))
                # Update nacnt balance for debtors control (DEBIT)
                self.update_nacnt_balance(conn, debtors_control, gross_amount, period)

                # 5. zvtran - VAT transaction
                if vat_amount > 0:
                    zvtran_sql = f"""
                        INSERT INTO zvtran (
                            va_source, va_account, va_laccnt, va_trdate, va_taxdate,
                            va_ovrdate, va_trref, va_trtype, va_country, va_fcurr,
                            va_trvalue, va_fcval, va_vatval, va_cost, va_vatctry,
                            va_vattype, va_anvat, va_vatrate, va_box1, va_box2,
                            va_box4, va_box6, va_box7, va_box8, va_box9,
                            va_done, va_import, va_export,
                            datecreated, datemodified, state
                        ) VALUES (
                            'S', '{customer_account}', '{sales_nominal}', '{post_date}', '{post_date}',
                            '{post_date}', '{invoice_number[:20]}', 'I', 'GB', '   ',
                            {net_amount}, 0, {vat_amount}, 0, 'H',
                            'S', '{vat_code}', {vat_rate}, 1, 0,
                            0, 1, 0, 0, 0,
                            0, 0, 0,
                            '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(zvtran_sql))

                    # 5b. nvat - VAT return tracking record
                    # nv_vattype: 'S' = Sales (output VAT, payable to HMRC)
                    nvat_sql = f"""
                        INSERT INTO nvat (
                            nv_acnt, nv_cntr, nv_date, nv_crdate, nv_taxdate,
                            nv_ref, nv_type, nv_advance, nv_value, nv_vatval,
                            nv_vatctry, nv_vattype, nv_vatcode, nv_vatrate, nv_comment,
                            datecreated, datemodified, state
                        ) VALUES (
                            '{vat_nominal}', '', '{post_date}', '{post_date}', '{post_date}',
                            '{invoice_number[:20]}', 'S', 0, {net_amount}, {vat_amount},
                            ' ', 'S', 'S', {vat_rate}, 'Sales Invoice VAT',
                            '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(nvat_sql))

                # 6. Update sname balance
                sname_update_sql = f"""
                    UPDATE sname
                    SET sn_currbal = sn_currbal + {gross_amount},
                        datemodified = '{now_str}'
                    WHERE RTRIM(sn_account) = '{customer_account}'
                """
                conn.execute(text(sname_update_sql))

            return ImportResult(
                success=True,
                records_processed=1,
                records_imported=1,
                warnings=[
                    f"Invoice: {invoice_number}",
                    f"Gross: £{gross_amount:.2f} (Net: £{net_amount:.2f} + VAT: £{vat_amount:.2f})",
                    f"Tables: stran, ntran, zvtran, sname"
                ]
            )

        except Exception as e:
            logger.error(f"Failed to import sales invoice: {e}")
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[str(e)]
            )


class PurchaseInvoiceFileImport:
    """
    Import purchase invoices from a CSV file.

    CSV columns:
        - supplier_account (required): Supplier account code
        - invoice_number (required): Invoice number/reference
        - net_amount (required): Net amount in pounds
        - vat_code (required): VAT code (e.g., '2' for standard 20%)
        - post_date (required): Posting date (YYYY-MM-DD)
        - description (optional): Invoice description
        - nominal_account (optional): Expense nominal account (default HA010)
        - supplier_ref (optional): Supplier's reference
    """

    def __init__(self, sql_connector=None):
        if sql_connector is None:
            from sql_rag.sql_connector import SQLConnector
            sql_connector = SQLConnector()
        self.sql = sql_connector
        self.opera_import = OperaSQLImport(sql_connector)

    def import_file(self, filepath: str, validate_only: bool = False) -> ImportResult:
        """
        Import purchase invoices from a CSV file.

        Args:
            filepath: Path to CSV file
            validate_only: If True, only validate without posting

        Returns:
            ImportResult with details of all imports
        """
        total_processed = 0
        total_imported = 0
        total_failed = 0
        all_errors = []
        all_warnings = []

        try:
            with open(filepath, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
        except Exception as e:
            return ImportResult(
                success=False,
                records_processed=0,
                errors=[f"Failed to read CSV file: {e}"]
            )

        for idx, row in enumerate(rows, 1):
            total_processed += 1

            try:
                # Validate required fields
                supplier_account = row.get('supplier_account', '').strip()
                invoice_number = row.get('invoice_number', '').strip()
                net_amount_str = row.get('net_amount', '').strip()
                vat_code = row.get('vat_code', '').strip()
                post_date_str = row.get('post_date', '').strip()

                if not supplier_account:
                    all_errors.append(f"Row {idx}: Missing supplier_account")
                    total_failed += 1
                    continue
                if not invoice_number:
                    all_errors.append(f"Row {idx}: Missing invoice_number")
                    total_failed += 1
                    continue
                if not net_amount_str:
                    all_errors.append(f"Row {idx}: Missing net_amount")
                    total_failed += 1
                    continue
                if not vat_code:
                    all_errors.append(f"Row {idx}: Missing vat_code")
                    total_failed += 1
                    continue
                if not post_date_str:
                    all_errors.append(f"Row {idx}: Missing post_date")
                    total_failed += 1
                    continue

                # Parse values
                try:
                    net_amount = float(net_amount_str)
                except ValueError:
                    all_errors.append(f"Row {idx}: Invalid net_amount '{net_amount_str}'")
                    total_failed += 1
                    continue

                try:
                    post_date = datetime.strptime(post_date_str, '%Y-%m-%d').date()
                except ValueError:
                    try:
                        post_date = datetime.strptime(post_date_str, '%d/%m/%Y').date()
                    except ValueError:
                        all_errors.append(f"Row {idx}: Invalid post_date '{post_date_str}'")
                        total_failed += 1
                        continue

                # Look up VAT rate
                vat_info = self.opera_import.get_vat_rate(vat_code, 'P', post_date)
                if not vat_info['found']:
                    all_warnings.append(f"Row {idx}: VAT code '{vat_code}' not found, using 0%")

                vat_rate = vat_info['rate']
                vat_amount = net_amount * (vat_rate / 100.0)
                vat_nominal = vat_info['nominal']

                # Optional fields
                description = row.get('description', '').strip()
                nominal_account = row.get('nominal_account', 'HA010').strip()
                supplier_ref = row.get('supplier_ref', '').strip()

                # Import the invoice
                result = self._import_single_invoice(
                    supplier_account=supplier_account,
                    invoice_number=invoice_number,
                    net_amount=net_amount,
                    vat_amount=vat_amount,
                    vat_code=vat_code,
                    vat_rate=vat_rate,
                    post_date=post_date,
                    description=description,
                    nominal_account=nominal_account,
                    vat_nominal=vat_nominal,
                    supplier_ref=supplier_ref,
                    validate_only=validate_only
                )

                if result.success:
                    total_imported += 1
                    all_warnings.extend([f"Row {idx}: {w}" for w in result.warnings])
                else:
                    total_failed += 1
                    all_errors.extend([f"Row {idx}: {e}" for e in result.errors])

            except Exception as e:
                total_failed += 1
                all_errors.append(f"Row {idx}: {str(e)}")

        return ImportResult(
            success=total_failed == 0,
            records_processed=total_processed,
            records_imported=total_imported,
            records_failed=total_failed,
            errors=all_errors,
            warnings=all_warnings
        )

    def _import_single_invoice(
        self,
        supplier_account: str,
        invoice_number: str,
        net_amount: float,
        vat_amount: float,
        vat_code: str,
        vat_rate: float,
        post_date: date,
        description: str,
        nominal_account: str,
        vat_nominal: str,
        supplier_ref: str,
        validate_only: bool
    ) -> ImportResult:
        """Import a single purchase invoice with VAT tracking."""
        errors = []
        warnings = []
        gross_amount = net_amount + vat_amount

        try:
            # Validate supplier
            supplier_check = self.sql.execute_query(f"""
                SELECT TOP 1 pn_name FROM pname
                WHERE RTRIM(pn_account) = '{supplier_account}'
            """)
            if supplier_check.empty:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[f"Supplier '{supplier_account}' not found"]
                )

            supplier_name = supplier_check.iloc[0]['pn_name'].strip()

            if validate_only:
                return ImportResult(
                    success=True,
                    records_processed=1,
                    records_imported=1,
                    warnings=[f"Validation passed: {invoice_number} £{gross_amount:.2f}"]
                )

            # Generate sequence numbers
            journal_result = self.sql.execute_query("""
                SELECT ISNULL(MAX(nt_jrnl), 0) + 1 as next_journal FROM ntran
            """)
            next_journal = journal_result.iloc[0]['next_journal']

            unique_ids = OperaUniqueIdGenerator.generate_multiple(5)
            ptran_unique = unique_ids[0]
            ntran_pstid_control = unique_ids[1]
            ntran_pstid_expense = unique_ids[2]
            ntran_pstid_vat = unique_ids[3]
            zvtran_unique = unique_ids[4]

            if isinstance(post_date, str):
                post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

            year = post_date.year
            period = post_date.month

            from datetime import timedelta
            due_date = post_date + timedelta(days=30)

            now = datetime.now()
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')

            # Get control account - check supplier profile first, then fall back to default
            from sql_rag.opera_config import get_supplier_control_account
            purchase_ledger_control = get_supplier_control_account(self.sql, supplier_account)
            vat_input_account = "BB040"

            ntran_comment = f"{invoice_number[:20]} {description[:29]:<29}"
            ntran_trnref = f"{supplier_name[:30]:<30}Invoice             "

            # Execute all inserts
            with self.sql.engine.begin() as conn:
                # 1. ptran - Purchase Ledger Transaction
                ptran_sql = f"""
                    INSERT INTO ptran (
                        pt_account, pt_trdate, pt_trref, pt_supref, pt_trtype,
                        pt_trvalue, pt_vatval, pt_trbal, pt_paid, pt_crdate,
                        pt_advance, pt_payflag, pt_set1day, pt_set1, pt_set2day,
                        pt_set2, pt_held, pt_fcurr, pt_fcrate, pt_fcdec,
                        pt_fcval, pt_fcbal, pt_adval, pt_fadval, pt_fcmult,
                        pt_cbtype, pt_entry, pt_unique, pt_suptype, pt_euro,
                        pt_payadvl, pt_origcur, pt_eurind, pt_revchrg, pt_nlpdate,
                        pt_adjsv, pt_vatset1, pt_vatset2, pt_pyroute, pt_fcvat,
                        datecreated, datemodified, state
                    ) VALUES (
                        '{supplier_account}', '{post_date}', '{invoice_number[:20]}', '{supplier_ref[:20]}', 'I',
                        {gross_amount}, {vat_amount}, {gross_amount}, ' ', '{post_date}',
                        'N', 0, 0, 0, 0,
                        0, ' ', '   ', 0, 0,
                        0, 0, 0, 0, 0,
                        '  ', '          ', '{ptran_unique}', '   ', 0,
                        0, '   ', ' ', 0, '{post_date}',
                        0, 0, 0, 0, 0,
                        '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(ptran_sql))

                # 2. ntran - Credit Purchase Ledger Control
                ntran_control_sql = f"""
                    INSERT INTO ntran (
                        nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                        nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                        nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                        nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                        nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                        nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                        nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                        nt_distrib, datecreated, datemodified, state
                    ) VALUES (
                        '{purchase_ledger_control}', '    ', 'C ', 'CA', {next_journal},
                        '{invoice_number[:10]}', 'IMPORT', 'P', '{ntran_comment}', '{ntran_trnref}',
                        '{post_date}', {-gross_amount}, {year}, {period}, 0,
                        0, 0, '   ', 0, 0,
                        0, 0, 'I', '', '        ',
                        '        ', 'I', 0, '{ntran_pstid_control}', 0,
                        0, 0, 0, 0, 0,
                        0, '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(ntran_control_sql))
                # Update nacnt balance for purchase ledger control (CREDIT)
                self.update_nacnt_balance(conn, purchase_ledger_control, -gross_amount, period)

                # 3. ntran - Debit Expense Account
                ntran_expense_sql = f"""
                    INSERT INTO ntran (
                        nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                        nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                        nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                        nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                        nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                        nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                        nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                        nt_distrib, datecreated, datemodified, state
                    ) VALUES (
                        '{nominal_account}', '    ', 'H ', 'HA', {next_journal},
                        '{invoice_number[:10]}', 'IMPORT', 'P', '{ntran_comment}', '{ntran_trnref}',
                        '{post_date}', {net_amount}, {year}, {period}, 0,
                        0, 0, '   ', 0, 0,
                        0, 0, 'I', '', '        ',
                        '        ', 'I', 0, '{ntran_pstid_expense}', 0,
                        0, 0, 0, 0, 0,
                        0, '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(ntran_expense_sql))
                # Update nacnt balance for expense account (DEBIT)
                self.update_nacnt_balance(conn, nominal_account, net_amount, period)

                # 4. ntran - Debit VAT Input
                if vat_amount > 0:
                    ntran_vat_sql = f"""
                        INSERT INTO ntran (
                            nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
                            nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
                            nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
                            nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
                            nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
                            nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
                            nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
                            nt_distrib, datecreated, datemodified, state
                        ) VALUES (
                            '{vat_input_account}', '    ', 'B ', 'BB', {next_journal},
                            '{invoice_number[:10]}', 'IMPORT', 'P', '{ntran_comment}', '{ntran_trnref}',
                            '{post_date}', {vat_amount}, {year}, {period}, 0,
                            0, 0, '   ', 0, 0,
                            0, 0, 'I', '', '        ',
                            '        ', 'I', 0, '{ntran_pstid_vat}', 0,
                            0, 0, 0, 0, 0,
                            0, '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(ntran_vat_sql))
                    # Update nacnt balance for VAT input account (DEBIT)
                    self.update_nacnt_balance(conn, vat_input_account, vat_amount, period)

                # 5. zvtran - VAT transaction
                if vat_amount > 0:
                    zvtran_sql = f"""
                        INSERT INTO zvtran (
                            va_source, va_account, va_laccnt, va_trdate, va_taxdate,
                            va_ovrdate, va_trref, va_trtype, va_country, va_fcurr,
                            va_trvalue, va_fcval, va_vatval, va_cost, va_vatctry,
                            va_vattype, va_anvat, va_vatrate, va_box1, va_box2,
                            va_box4, va_box6, va_box7, va_box8, va_box9,
                            va_done, va_import, va_export,
                            datecreated, datemodified, state
                        ) VALUES (
                            'P', '{supplier_account}', '{nominal_account}', '{post_date}', '{post_date}',
                            '{post_date}', '{invoice_number[:20]}', 'I', 'GB', '   ',
                            {net_amount}, 0, {vat_amount}, 0, 'H',
                            'P', '{vat_code}', {vat_rate}, 0, 0,
                            1, 0, 1, 0, 0,
                            0, 0, 0,
                            '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(zvtran_sql))

                    # 5b. nvat - VAT return tracking record
                    # nv_vattype: 'P' = Purchase (input VAT, reclaimable)
                    nvat_sql = f"""
                        INSERT INTO nvat (
                            nv_acnt, nv_cntr, nv_date, nv_crdate, nv_taxdate,
                            nv_ref, nv_type, nv_advance, nv_value, nv_vatval,
                            nv_vatctry, nv_vattype, nv_vatcode, nv_vatrate, nv_comment,
                            datecreated, datemodified, state
                        ) VALUES (
                            '{vat_input_account}', '', '{post_date}', '{post_date}', '{post_date}',
                            '{invoice_number[:20]}', 'P', 0, {net_amount}, {vat_amount},
                            ' ', 'P', 'S', {vat_rate}, 'Purchase Invoice VAT',
                            '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(nvat_sql))

                # 6. Update pname balance
                pname_update_sql = f"""
                    UPDATE pname
                    SET pn_currbal = pn_currbal + {gross_amount},
                        datemodified = '{now_str}'
                    WHERE RTRIM(pn_account) = '{supplier_account}'
                """
                conn.execute(text(pname_update_sql))

            return ImportResult(
                success=True,
                records_processed=1,
                records_imported=1,
                warnings=[
                    f"Invoice: {invoice_number}",
                    f"Gross: £{gross_amount:.2f} (Net: £{net_amount:.2f} + VAT: £{vat_amount:.2f})",
                    f"Tables: ptran, ntran, zvtran, pname"
                ]
            )

        except Exception as e:
            logger.error(f"Failed to import purchase invoice: {e}")
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[str(e)]
            )


    # =========================================================================
    # SALES ORDER PROCESSING (SOP) WRITE OPERATIONS
    # =========================================================================

    _sop_params = None  # Cache for SOP parameters

    def get_sop_parameters(self) -> Dict[str, Any]:
        """
        Get SOP company options from iparm.

        Returns dict with key settings:
        - force_allocate: Force allocation on orders (ip_forceal)
        - update_transactions: Create stock transactions (ip_updtran)
        - stock_memo: Stock memo handling (ip_stkmemo)
        - auto_extend: Auto extend pricing (ip_autoext)
        - show_cost: Show costs (ip_showcst)
        - restrict_products: Restricted product access (ip_restric)
        - warn_margin: Warning on margin (ip_wrnmarg)
        """
        if self._sop_params is not None:
            return self._sop_params

        result = self.sql.execute_query("""
            SELECT ip_forceal, ip_updtran, ip_stkmemo, ip_autoext, ip_showcst,
                   ip_restric, ip_wrnmarg, ip_save, ip_picking, ip_delivry,
                   ip_headfir, ip_condate, ip_ovrallc, ip_suggasm
            FROM iparm WITH (NOLOCK)
        """)

        if result.empty:
            logger.warning("Could not read iparm - using defaults")
            self._sop_params = {
                'force_allocate': True,
                'update_transactions': True,
                'stock_memo': True,
                'auto_extend': True,
                'show_cost': True,
                'restrict_products': False,
                'warn_margin': 'A',
                'picking_required': False,
                'delivery_required': False,
                'override_allocation': False,
            }
            return self._sop_params

        row = result.iloc[0]

        def to_bool(val) -> bool:
            if isinstance(val, bool):
                return val
            if val is None:
                return False
            return str(val).upper() in ('Y', 'YES', 'TRUE', '1')

        self._sop_params = {
            'force_allocate': to_bool(row.get('ip_forceal')),
            'update_transactions': to_bool(row.get('ip_updtran')),
            'stock_memo': to_bool(row.get('ip_stkmemo')),
            'auto_extend': to_bool(row.get('ip_autoext')),
            'show_cost': to_bool(row.get('ip_showcst')),
            'restrict_products': to_bool(row.get('ip_restric')),
            'warn_margin': str(row.get('ip_wrnmarg', 'A')).strip(),
            'picking_required': to_bool(row.get('ip_picking')),
            'delivery_required': to_bool(row.get('ip_delivry')),
            'override_allocation': to_bool(row.get('ip_ovrallc')),
            'suggest_assembly': to_bool(row.get('ip_suggasm')),
        }

        logger.info(f"Loaded SOP parameters: force_allocate={self._sop_params['force_allocate']}, "
                   f"update_transactions={self._sop_params['update_transactions']}")

        return self._sop_params

    def get_sales_ledger_parameters(self) -> Dict[str, Any]:
        """
        Get Sales Ledger parameters from sparm.

        Returns key settings for posting:
        - bank_nominal: Default bank nominal for receipts (sp_banknom)
        - discount_nominal: Discount nominal account (sp_discnom)
        - nl_company_id: NL company ID for SOP (sp_nlcoid)
        - receipt_types: Receipt cashbook types (sp_rcbty01-03)
        """
        result = self.sql.execute_query("""
            SELECT sp_banknom, sp_discnom, sp_nlcoid, sp_rcbty01, sp_rcbty02, sp_rcbty03,
                   sp_fcbty01, sp_rec01, sp_rec02, sp_rec03
            FROM sparm WITH (NOLOCK)
        """)

        if result.empty:
            logger.warning("Could not read sparm - using defaults")
            return {
                'bank_nominal': 'BC010',
                'discount_nominal': 'FB010',
                'nl_company_id': 'I',
                'receipt_types': ['R1', 'R2', 'R4'],
            }

        row = result.iloc[0]
        return {
            'bank_nominal': str(row.get('sp_banknom', 'BC010')).strip(),
            'discount_nominal': str(row.get('sp_discnom', 'FB010')).strip(),
            'nl_company_id': str(row.get('sp_nlcoid', 'I')).strip(),
            'receipt_types': [
                str(row.get('sp_rcbty01', 'R1')).strip(),
                str(row.get('sp_rcbty02', 'R2')).strip(),
                str(row.get('sp_rcbty03', 'R4')).strip(),
            ],
            'receipt_names': [
                str(row.get('sp_rec01', 'CHEQUE')).strip(),
                str(row.get('sp_rec02', 'BACS')).strip(),
                str(row.get('sp_rec03', 'GoCard DD')).strip(),
            ],
        }

    def get_next_sop_numbers(self) -> Dict[str, str]:
        """
        Get next document numbers from iparm and increment them.

        Returns dict with keys: doc_no, quot_no, ord_no, prof_no, inv_no, del_no, cred_no
        """
        result = self.sql.execute_query("""
            SELECT ip_docno, ip_quotno, ip_orderno, ip_profno, ip_invno, ip_deliv, ip_credno
            FROM iparm WITH (NOLOCK)
        """)

        if result.empty:
            raise ValueError("Could not read SOP parameters from iparm")

        row = result.iloc[0]
        return {
            'doc_no': str(row['ip_docno']).strip(),
            'quot_no': str(row['ip_quotno']).strip(),
            'ord_no': str(row['ip_orderno']).strip(),
            'prof_no': str(row['ip_profno']).strip(),
            'inv_no': str(row['ip_invno']).strip(),
            'del_no': str(row['ip_deliv']).strip(),
            'cred_no': str(row['ip_credno']).strip(),
        }

    def _increment_sop_number(self, current: str) -> str:
        """
        Increment an SOP number like DOC30451 -> DOC30452.

        Handles formats like: DOC30451, QUO10361, ORD16532, etc.
        """
        # Find where the numeric part starts
        prefix = ''
        for i, c in enumerate(current):
            if c.isdigit():
                prefix = current[:i]
                num_part = current[i:]
                break
        else:
            # No digits found, just append 1
            return current + '1'

        # Increment the number, preserving leading zeros
        new_num = int(num_part) + 1
        return prefix + str(new_num).zfill(len(num_part))

    def import_sales_quote(
        self,
        customer_account: str,
        lines: List[Dict[str, Any]],
        quote_date: date = None,
        customer_ref: str = "",
        warehouse: str = "MAIN",
        expiry_days: int = 30,
        notes: str = "",
        input_by: str = "SQLRAG"
    ) -> ImportResult:
        """
        Create a new sales quote document.

        Creates records in:
        - ihead (document header)
        - itran (document lines)
        - Updates iparm (next document numbers)

        Args:
            customer_account: Customer account code (e.g., 'V013')
            lines: List of line items, each dict with:
                - stock_ref: Product reference (optional for service lines)
                - description: Line description
                - quantity: Quantity
                - price: Unit price (ex VAT)
                - vat_code: VAT code (default 'S' for standard)
                - warehouse: Override warehouse for this line
            quote_date: Date of quote (defaults to today)
            customer_ref: Customer's reference
            warehouse: Default warehouse for lines
            expiry_days: Days until quote expires
            notes: Notes/narration
            input_by: User creating the quote

        Returns:
            ImportResult with quote_number in transaction_ref
        """
        errors = []
        warnings = []

        if quote_date is None:
            quote_date = date.today()

        if isinstance(quote_date, str):
            quote_date = datetime.strptime(quote_date, '%Y-%m-%d').date()

        try:
            # =====================
            # VALIDATE CUSTOMER
            # =====================
            customer_check = self.sql.execute_query(f"""
                SELECT sn_account, sn_name, sn_addr1, sn_addr2, sn_addr3, sn_addr4, sn_postcode
                FROM sname WITH (NOLOCK)
                WHERE RTRIM(sn_account) = '{customer_account}'
            """)

            if customer_check.empty:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[f"Customer '{customer_account}' not found in sname"]
                )

            cust = customer_check.iloc[0]
            cust_name = str(cust['sn_name'] or '').strip()[:35]
            cust_addr1 = str(cust['sn_addr1'] or '').strip()[:35]
            cust_addr2 = str(cust['sn_addr2'] or '').strip()[:35]
            cust_addr3 = str(cust['sn_addr3'] or '').strip()[:35]
            cust_addr4 = str(cust['sn_addr4'] or '').strip()[:35]
            cust_postcode = str(cust['sn_postcode'] or '').strip()[:10]

            # =====================
            # VALIDATE WAREHOUSE
            # =====================
            wh_check = self.sql.execute_query(f"""
                SELECT cw_code FROM cware WITH (NOLOCK)
                WHERE RTRIM(cw_code) = '{warehouse}'
            """)
            if wh_check.empty:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[f"Warehouse '{warehouse}' not found"]
                )

            # =====================
            # VALIDATE LINES
            # =====================
            if not lines:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=["No lines provided for quote"]
                )

            # Calculate totals
            total_ex_vat = 0.0
            total_vat = 0.0
            validated_lines = []

            for idx, line in enumerate(lines):
                line_qty = float(line.get('quantity', 1))
                line_price = float(line.get('price', 0))
                line_val = line_qty * line_price
                vat_code = line.get('vat_code', 'S')

                # Look up VAT rate
                vat_rate = 20.0  # Default
                if vat_code in self._vat_cache:
                    vat_rate = self._vat_cache[vat_code]
                else:
                    vat_check = self.sql.execute_query(f"""
                        SELECT vc_rate FROM vat WITH (NOLOCK)
                        WHERE RTRIM(vc_code) = '{vat_code}'
                    """)
                    if not vat_check.empty:
                        vat_rate = float(vat_check.iloc[0]['vc_rate'] or 20)
                        self._vat_cache[vat_code] = vat_rate

                vat_val = line_val * vat_rate / 100

                validated_lines.append({
                    'stock_ref': str(line.get('stock_ref', ''))[:20],
                    'description': str(line.get('description', ''))[:40],
                    'quantity': line_qty,
                    'price': line_price,
                    'line_val': line_val,
                    'vat_code': vat_code,
                    'vat_rate': vat_rate,
                    'vat_val': vat_val,
                    'warehouse': str(line.get('warehouse', warehouse))[:6],
                    'line_no': idx + 1
                })

                total_ex_vat += line_val
                total_vat += vat_val

            # =====================
            # GET NEXT DOC NUMBERS
            # =====================
            now = datetime.now()
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')
            expiry_date = quote_date + __import__('datetime').timedelta(days=expiry_days)

            with self.sql.engine.begin() as conn:
                conn.execute(text(get_lock_timeout_sql()))

                # Get and increment document numbers
                num_result = conn.execute(text("""
                    SELECT ip_docno, ip_quotno FROM iparm WITH (UPDLOCK, ROWLOCK)
                """)).fetchone()

                doc_no = str(num_result[0]).strip()
                quot_no = str(num_result[1]).strip()

                next_doc = self._increment_sop_number(doc_no)
                next_quot = self._increment_sop_number(quot_no)

                # Update iparm
                conn.execute(text(f"""
                    UPDATE iparm SET ip_docno = '{next_doc}', ip_quotno = '{next_quot}',
                    datemodified = '{now_str}'
                """))

                # =====================
                # CREATE IHEAD (Quote Header)
                # =====================
                ihead_sql = f"""
                    INSERT INTO ihead (
                        ih_doc, ih_account, ih_name, ih_addr1, ih_addr2, ih_addr3, ih_addr4, ih_addpc,
                        ih_docstat, ih_date, ih_quodate, ih_quotat, ih_expiry, ih_validto,
                        ih_exvat, ih_vat, ih_custref, ih_loc, ih_origin,
                        ih_narr1, ih_raised, datecreated, datemodified, state
                    ) VALUES (
                        '{doc_no}', '{customer_account}', '{cust_name}', '{cust_addr1}', '{cust_addr2}',
                        '{cust_addr3}', '{cust_addr4}', '{cust_postcode}',
                        'Q', '{quote_date}', '{quote_date}', '{quot_no}', '{expiry_date}', '{expiry_date}',
                        {total_ex_vat}, {total_vat}, '{customer_ref[:20]}', '{warehouse}', 0,
                        '{notes[:60]}', '{input_by[:8]}', '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(ihead_sql))

                # =====================
                # CREATE ITRAN (Quote Lines)
                # =====================
                for line in validated_lines:
                    itran_sql = f"""
                        INSERT INTO itran (
                            it_doc, it_lineno, it_stock, it_desc, it_quan, it_price,
                            it_lineval, it_vat, it_vatval, it_vattyp, it_vatpct,
                            it_cwcode, it_exvat, it_cost, it_date,
                            datecreated, datemodified, state
                        ) VALUES (
                            '{doc_no}', {line['line_no']}, '{line['stock_ref']}', '{line['description']}',
                            {line['quantity']}, {line['price']},
                            {line['line_val'] + line['vat_val']}, {line['vat_val']}, {line['vat_val']},
                            '{line['vat_code']}', {line['vat_rate']},
                            '{line['warehouse']}', {line['line_val']}, 0, '{quote_date}',
                            '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(itran_sql))

            logger.info(f"Created quote {quot_no} (doc {doc_no}) for {customer_account}: £{total_ex_vat:.2f} + VAT")

            return ImportResult(
                success=True,
                records_processed=1,
                records_imported=1,
                entry_number=doc_no,
                transaction_ref=quot_no,
                warnings=[
                    f"Quote: {quot_no}",
                    f"Document: {doc_no}",
                    f"Customer: {customer_account} - {cust_name}",
                    f"Lines: {len(validated_lines)}",
                    f"Total: £{total_ex_vat:.2f} + £{total_vat:.2f} VAT",
                    f"Valid until: {expiry_date}"
                ]
            )

        except Exception as e:
            logger.error(f"Failed to create sales quote: {e}")
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[str(e)]
            )

    def convert_quote_to_order(
        self,
        document_no: str,
        order_date: date = None,
        input_by: str = "SQLRAG"
    ) -> ImportResult:
        """
        Convert a quote to a sales order.

        Updates ihead to change status from Q to O and assigns order number.

        Args:
            document_no: Document number (DOC...) or quote number (QUO...)
            order_date: Date for the order (defaults to today)
            input_by: User performing conversion

        Returns:
            ImportResult with order_number in transaction_ref
        """
        if order_date is None:
            order_date = date.today()

        if isinstance(order_date, str):
            order_date = datetime.strptime(order_date, '%Y-%m-%d').date()

        try:
            # Find the document
            search_col = 'ih_quotat' if document_no.upper().startswith('QUO') else 'ih_doc'

            doc_check = self.sql.execute_query(f"""
                SELECT ih_doc, ih_docstat, ih_quotat, ih_sorder
                FROM ihead WITH (NOLOCK)
                WHERE RTRIM({search_col}) = '{document_no}'
            """)

            if doc_check.empty:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[f"Document '{document_no}' not found"]
                )

            row = doc_check.iloc[0]
            doc_no = str(row['ih_doc']).strip()
            current_status = str(row['ih_docstat']).strip()
            current_order = str(row['ih_sorder']).strip() if row['ih_sorder'] else ''

            if current_status != 'Q':
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[f"Document is not a quote (status: {current_status}). Cannot convert."]
                )

            if current_order:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[f"Document already has order number: {current_order}"]
                )

            now = datetime.now()
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')

            with self.sql.engine.begin() as conn:
                conn.execute(text(get_lock_timeout_sql()))

                # Get next order number
                num_result = conn.execute(text("""
                    SELECT ip_orderno FROM iparm WITH (UPDLOCK, ROWLOCK)
                """)).fetchone()

                ord_no = str(num_result[0]).strip()
                next_ord = self._increment_sop_number(ord_no)

                # Update iparm
                conn.execute(text(f"""
                    UPDATE iparm SET ip_orderno = '{next_ord}', datemodified = '{now_str}'
                """))

                # Update ihead - change status to Order
                conn.execute(text(f"""
                    UPDATE ihead WITH (ROWLOCK)
                    SET ih_docstat = 'O',
                        ih_sorder = '{ord_no}',
                        ih_orddate = '{order_date}',
                        datemodified = '{now_str}'
                    WHERE RTRIM(ih_doc) = '{doc_no}'
                """))

            logger.info(f"Converted quote {document_no} to order {ord_no}")

            return ImportResult(
                success=True,
                records_processed=1,
                records_imported=1,
                entry_number=doc_no,
                transaction_ref=ord_no,
                warnings=[
                    f"Quote converted to Order",
                    f"Order Number: {ord_no}",
                    f"Document: {doc_no}",
                    f"Order Date: {order_date}"
                ]
            )

        except Exception as e:
            logger.error(f"Failed to convert quote to order: {e}")
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[str(e)]
            )

    def import_sales_order(
        self,
        customer_account: str,
        lines: List[Dict[str, Any]],
        order_date: date = None,
        customer_ref: str = "",
        warehouse: str = "MAIN",
        auto_allocate: bool = None,  # None = use company setting
        notes: str = "",
        input_by: str = "SQLRAG"
    ) -> ImportResult:
        """
        Create a new sales order directly (bypassing quote stage).

        Creates records in:
        - ihead (document header with status 'O')
        - itran (document lines)
        - Updates iparm (next document numbers)
        - Allocates stock based on auto_allocate or company setting (ip_forceal)

        Company Options Used:
        - ip_forceal: If Y and auto_allocate is None, will auto-allocate stock
        - ip_updtran: If Y, creates stock transaction records (ctran)

        Args:
            customer_account: Customer account code
            lines: List of line items (same format as import_sales_quote)
            order_date: Date of order (defaults to today)
            customer_ref: Customer's reference
            warehouse: Default warehouse for lines
            auto_allocate: Whether to allocate stock (None = use company ip_forceal setting)
            notes: Notes/narration
            input_by: User creating the order

        Returns:
            ImportResult with order_number in transaction_ref
        """
        errors = []
        warnings = []

        if order_date is None:
            order_date = date.today()

        if isinstance(order_date, str):
            order_date = datetime.strptime(order_date, '%Y-%m-%d').date()

        try:
            # =====================
            # LOAD COMPANY OPTIONS
            # =====================
            sop_params = self.get_sop_parameters()

            # If auto_allocate not specified, use company setting
            if auto_allocate is None:
                auto_allocate = sop_params.get('force_allocate', False)
                logger.info(f"Using company setting for allocation: ip_forceal={auto_allocate}")
            else:
                logger.info(f"Using caller-specified allocation setting: {auto_allocate}")
            # =====================
            # VALIDATE CUSTOMER
            # =====================
            customer_check = self.sql.execute_query(f"""
                SELECT sn_account, sn_name, sn_addr1, sn_addr2, sn_addr3, sn_addr4, sn_postcode
                FROM sname WITH (NOLOCK)
                WHERE RTRIM(sn_account) = '{customer_account}'
            """)

            if customer_check.empty:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[f"Customer '{customer_account}' not found"]
                )

            cust = customer_check.iloc[0]
            cust_name = str(cust['sn_name'] or '').strip()[:35]
            cust_addr1 = str(cust['sn_addr1'] or '').strip()[:35]
            cust_addr2 = str(cust['sn_addr2'] or '').strip()[:35]
            cust_addr3 = str(cust['sn_addr3'] or '').strip()[:35]
            cust_addr4 = str(cust['sn_addr4'] or '').strip()[:35]
            cust_postcode = str(cust['sn_postcode'] or '').strip()[:10]

            # =====================
            # VALIDATE WAREHOUSE
            # =====================
            wh_check = self.sql.execute_query(f"""
                SELECT cw_code FROM cware WITH (NOLOCK)
                WHERE RTRIM(cw_code) = '{warehouse}'
            """)
            if wh_check.empty:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[f"Warehouse '{warehouse}' not found"]
                )

            # =====================
            # VALIDATE LINES
            # =====================
            if not lines:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=["No lines provided for order"]
                )

            # Calculate totals and validate stock
            total_ex_vat = 0.0
            total_vat = 0.0
            validated_lines = []

            for idx, line in enumerate(lines):
                line_qty = float(line.get('quantity', 1))
                line_price = float(line.get('price', 0))
                line_val = line_qty * line_price
                vat_code = line.get('vat_code', 'S')
                stock_ref = str(line.get('stock_ref', ''))[:20]
                line_wh = str(line.get('warehouse', warehouse))[:6]

                # Look up VAT rate
                vat_rate = 20.0
                if vat_code in self._vat_cache:
                    vat_rate = self._vat_cache[vat_code]
                else:
                    vat_check = self.sql.execute_query(f"""
                        SELECT vc_rate FROM vat WITH (NOLOCK)
                        WHERE RTRIM(vc_code) = '{vat_code}'
                    """)
                    if not vat_check.empty:
                        vat_rate = float(vat_check.iloc[0]['vc_rate'] or 20)
                        self._vat_cache[vat_code] = vat_rate

                vat_val = line_val * vat_rate / 100

                # Check stock availability if stock item and auto_allocate
                available_stock = 0
                if stock_ref and auto_allocate:
                    stock_check = self.sql.execute_query(f"""
                        SELECT cs_freest FROM cstwh WITH (NOLOCK)
                        WHERE RTRIM(cs_ref) = '{stock_ref}' AND RTRIM(cs_whar) = '{line_wh}'
                    """)
                    if not stock_check.empty:
                        available_stock = float(stock_check.iloc[0]['cs_freest'] or 0)
                        if available_stock < line_qty:
                            warnings.append(f"Line {idx+1}: Only {available_stock:.2f} available (requested {line_qty:.2f})")

                validated_lines.append({
                    'stock_ref': stock_ref,
                    'description': str(line.get('description', ''))[:40],
                    'quantity': line_qty,
                    'price': line_price,
                    'line_val': line_val,
                    'vat_code': vat_code,
                    'vat_rate': vat_rate,
                    'vat_val': vat_val,
                    'warehouse': line_wh,
                    'line_no': idx + 1,
                    'available_stock': available_stock
                })

                total_ex_vat += line_val
                total_vat += vat_val

            # =====================
            # CREATE ORDER
            # =====================
            now = datetime.now()
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')

            with self.sql.engine.begin() as conn:
                conn.execute(text(get_lock_timeout_sql()))

                # Get and increment document numbers
                num_result = conn.execute(text("""
                    SELECT ip_docno, ip_orderno FROM iparm WITH (UPDLOCK, ROWLOCK)
                """)).fetchone()

                doc_no = str(num_result[0]).strip()
                ord_no = str(num_result[1]).strip()

                next_doc = self._increment_sop_number(doc_no)
                next_ord = self._increment_sop_number(ord_no)

                # Update iparm
                conn.execute(text(f"""
                    UPDATE iparm SET ip_docno = '{next_doc}', ip_orderno = '{next_ord}',
                    datemodified = '{now_str}'
                """))

                # =====================
                # CREATE IHEAD (Order Header)
                # =====================
                ihead_sql = f"""
                    INSERT INTO ihead (
                        ih_doc, ih_account, ih_name, ih_addr1, ih_addr2, ih_addr3, ih_addr4, ih_addpc,
                        ih_docstat, ih_date, ih_orddate, ih_sorder,
                        ih_exvat, ih_vat, ih_custref, ih_loc, ih_origin,
                        ih_narr1, ih_raised, datecreated, datemodified, state
                    ) VALUES (
                        '{doc_no}', '{customer_account}', '{cust_name}', '{cust_addr1}', '{cust_addr2}',
                        '{cust_addr3}', '{cust_addr4}', '{cust_postcode}',
                        'O', '{order_date}', '{order_date}', '{ord_no}',
                        {total_ex_vat}, {total_vat}, '{customer_ref[:20]}', '{warehouse}', 0,
                        '{notes[:60]}', '{input_by[:8]}', '{now_str}', '{now_str}', 1
                    )
                """
                conn.execute(text(ihead_sql))

                # =====================
                # CREATE ITRAN (Order Lines)
                # =====================
                for line in validated_lines:
                    qty_to_alloc = min(line['quantity'], line['available_stock']) if auto_allocate else 0

                    itran_sql = f"""
                        INSERT INTO itran (
                            it_doc, it_lineno, it_stock, it_desc, it_quan, it_price,
                            it_lineval, it_vat, it_vatval, it_vattyp, it_vatpct,
                            it_cwcode, it_exvat, it_cost, it_date,
                            it_qtyallc, it_qtypick, it_qtydelv, it_qtyinv,
                            datecreated, datemodified, state
                        ) VALUES (
                            '{doc_no}', {line['line_no']}, '{line['stock_ref']}', '{line['description']}',
                            {line['quantity']}, {line['price']},
                            {line['line_val'] + line['vat_val']}, {line['vat_val']}, {line['vat_val']},
                            '{line['vat_code']}', {line['vat_rate']},
                            '{line['warehouse']}', {line['line_val']}, 0, '{order_date}',
                            {qty_to_alloc}, 0, 0, 0,
                            '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(itran_sql))

                    # Update stock allocation if auto_allocate
                    if auto_allocate and qty_to_alloc > 0 and line['stock_ref']:
                        # Update cstwh - reduce free stock, increase allocated
                        conn.execute(text(f"""
                            UPDATE cstwh WITH (ROWLOCK)
                            SET cs_freest = cs_freest - {qty_to_alloc},
                                cs_alloc = cs_alloc + {qty_to_alloc},
                                cs_saleord = cs_saleord + {line['quantity']},
                                datemodified = '{now_str}'
                            WHERE RTRIM(cs_ref) = '{line['stock_ref']}' AND RTRIM(cs_whar) = '{line['warehouse']}'
                        """))

                        # Update cname totals
                        conn.execute(text(f"""
                            UPDATE cname WITH (ROWLOCK)
                            SET cn_freest = cn_freest - {qty_to_alloc},
                                cn_alloc = cn_alloc + {qty_to_alloc},
                                cn_saleord = cn_saleord + {line['quantity']},
                                datemodified = '{now_str}'
                            WHERE RTRIM(cn_ref) = '{line['stock_ref']}'
                        """))

            logger.info(f"Created order {ord_no} (doc {doc_no}) for {customer_account}: £{total_ex_vat:.2f} + VAT")

            return ImportResult(
                success=True,
                records_processed=1,
                records_imported=1,
                entry_number=doc_no,
                transaction_ref=ord_no,
                warnings=[
                    f"Order: {ord_no}",
                    f"Document: {doc_no}",
                    f"Customer: {customer_account} - {cust_name}",
                    f"Lines: {len(validated_lines)}",
                    f"Total: £{total_ex_vat:.2f} + £{total_vat:.2f} VAT",
                    f"Auto-allocate: {'Yes' if auto_allocate else 'No'}"
                ] + warnings
            )

        except Exception as e:
            logger.error(f"Failed to create sales order: {e}")
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[str(e)]
            )

    def allocate_order_stock(
        self,
        document_no: str,
        line_no: int = None,
        input_by: str = "SQLRAG"
    ) -> ImportResult:
        """
        Allocate available stock to an order's lines.

        For each line with a stock item:
        - Checks available free stock in warehouse
        - Allocates up to the line quantity
        - Updates cstwh, cname, and itran

        Args:
            document_no: Document number or order number
            line_no: Specific line number to allocate (None = all lines)
            input_by: User performing allocation

        Returns:
            ImportResult with allocation details
        """
        try:
            # Find the document
            search_col = 'ih_sorder' if document_no.upper().startswith('ORD') else 'ih_doc'

            doc_check = self.sql.execute_query(f"""
                SELECT ih_doc, ih_docstat, ih_sorder
                FROM ihead WITH (NOLOCK)
                WHERE RTRIM({search_col}) = '{document_no}'
            """)

            if doc_check.empty:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[f"Document '{document_no}' not found"]
                )

            row = doc_check.iloc[0]
            doc_no = str(row['ih_doc']).strip()
            current_status = str(row['ih_docstat']).strip()

            if current_status not in ('O', 'P'):  # Order or Proforma
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[f"Document status '{current_status}' cannot be allocated"]
                )

            # Get lines to allocate
            line_filter = f"AND it_lineno = {line_no}" if line_no else ""
            lines = self.sql.execute_query(f"""
                SELECT it_lineno, it_stock, it_quan, it_qtyallc, it_cwcode
                FROM itran WITH (NOLOCK)
                WHERE RTRIM(it_doc) = '{doc_no}' AND RTRIM(it_stock) != ''
                {line_filter}
            """)

            if lines.empty:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=["No stock lines found to allocate"]
                )

            now = datetime.now()
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')

            allocations = []

            with self.sql.engine.begin() as conn:
                conn.execute(text(get_lock_timeout_sql()))

                for _, line in lines.iterrows():
                    stock_ref = str(line['it_stock']).strip()
                    warehouse = str(line['it_cwcode']).strip()
                    qty_ordered = float(line['it_quan'] or 0)
                    qty_already_alloc = float(line['it_qtyallc'] or 0)
                    qty_needed = qty_ordered - qty_already_alloc

                    if qty_needed <= 0:
                        continue  # Already fully allocated

                    # Check available stock
                    stock_result = conn.execute(text(f"""
                        SELECT cs_freest FROM cstwh WITH (UPDLOCK, ROWLOCK)
                        WHERE RTRIM(cs_ref) = '{stock_ref}' AND RTRIM(cs_whar) = '{warehouse}'
                    """)).fetchone()

                    if not stock_result:
                        allocations.append(f"Line {line['it_lineno']}: No stock record for {stock_ref} in {warehouse}")
                        continue

                    free_stock = float(stock_result[0] or 0)
                    qty_to_alloc = min(qty_needed, free_stock)

                    if qty_to_alloc <= 0:
                        allocations.append(f"Line {line['it_lineno']}: No free stock for {stock_ref}")
                        continue

                    # Update itran
                    conn.execute(text(f"""
                        UPDATE itran WITH (ROWLOCK)
                        SET it_qtyallc = it_qtyallc + {qty_to_alloc},
                            it_dteallc = '{date.today()}',
                            datemodified = '{now_str}'
                        WHERE RTRIM(it_doc) = '{doc_no}' AND it_lineno = {line['it_lineno']}
                    """))

                    # Update cstwh
                    conn.execute(text(f"""
                        UPDATE cstwh WITH (ROWLOCK)
                        SET cs_freest = cs_freest - {qty_to_alloc},
                            cs_alloc = cs_alloc + {qty_to_alloc},
                            datemodified = '{now_str}'
                        WHERE RTRIM(cs_ref) = '{stock_ref}' AND RTRIM(cs_whar) = '{warehouse}'
                    """))

                    # Update cname
                    conn.execute(text(f"""
                        UPDATE cname WITH (ROWLOCK)
                        SET cn_freest = cn_freest - {qty_to_alloc},
                            cn_alloc = cn_alloc + {qty_to_alloc},
                            datemodified = '{now_str}'
                        WHERE RTRIM(cn_ref) = '{stock_ref}'
                    """))

                    allocations.append(f"Line {line['it_lineno']}: Allocated {qty_to_alloc:.2f} of {stock_ref}")

            logger.info(f"Stock allocation for {document_no}: {len(allocations)} lines processed")

            return ImportResult(
                success=True,
                records_processed=len(lines),
                records_imported=len(allocations),
                entry_number=doc_no,
                warnings=allocations
            )

        except Exception as e:
            logger.error(f"Failed to allocate stock: {e}")
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[str(e)]
            )

    def invoice_sales_order(
        self,
        document_no: str,
        invoice_date: date = None,
        tax_point_date: date = None,
        post_to_nominal: bool = True,
        issue_stock: bool = True,
        invoice_type: str = "IT1",
        input_by: str = "SQLRAG"
    ) -> ImportResult:
        """
        Create an invoice from a sales order.

        This is the most complex SOP operation, creating records in:
        - ihead: Update status to 'I', assign invoice number
        - itran: Update it_qtyinv for lines
        - ctran: Stock issue transactions (if issue_stock=True)
        - cstwh/cname: Update stock levels (if issue_stock=True)
        - stran: Sales ledger transaction
        - snoml: Sales ledger transfer file
        - ntran: Nominal ledger postings (if post_to_nominal=True)
        - nacnt: Nominal account balance updates
        - zvtran: VAT analysis
        - nvat: VAT return tracking
        - sname: Update customer balance

        Company Options Used:
        - ip_updtran: Whether to update stock transactions
        - ip_updst: When stock is issued (D=Delivery, I=Invoice)
        - sparm settings for nominal accounts

        Args:
            document_no: Document number or order number to invoice
            invoice_date: Invoice date (defaults to today)
            tax_point_date: Tax point date for VAT (defaults to invoice_date)
            post_to_nominal: Whether to post to nominal ledger
            issue_stock: Whether to issue stock (reduce stock levels)
            invoice_type: Transaction type code for stran (default 'IT1')
            input_by: User creating the invoice

        Returns:
            ImportResult with invoice_number in transaction_ref
        """
        warnings = []

        if invoice_date is None:
            invoice_date = date.today()

        if isinstance(invoice_date, str):
            invoice_date = datetime.strptime(invoice_date, '%Y-%m-%d').date()

        if tax_point_date is None:
            tax_point_date = invoice_date

        if isinstance(tax_point_date, str):
            tax_point_date = datetime.strptime(tax_point_date, '%Y-%m-%d').date()

        try:
            # =====================
            # LOAD COMPANY OPTIONS
            # =====================
            sop_params = self.get_sop_parameters()
            sl_params = self.get_sales_ledger_parameters()

            # =====================
            # FIND THE DOCUMENT
            # =====================
            search_col = 'ih_sorder' if document_no.upper().startswith('ORD') else 'ih_doc'

            doc_check = self.sql.execute_query(f"""
                SELECT ih_doc, ih_account, ih_name, ih_docstat, ih_sorder, ih_invoice,
                       ih_exvat, ih_vat, ih_loc, ih_fcurr
                FROM ihead WITH (NOLOCK)
                WHERE RTRIM({search_col}) = '{document_no}'
            """)

            if doc_check.empty:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[f"Document '{document_no}' not found"]
                )

            doc = doc_check.iloc[0]
            doc_no = str(doc['ih_doc']).strip()
            customer_account = str(doc['ih_account']).strip()
            customer_name = str(doc['ih_name'] or '').strip()
            current_status = str(doc['ih_docstat']).strip()
            current_invoice = str(doc['ih_invoice'] or '').strip()
            net_value = float(doc['ih_exvat'] or 0)
            vat_value = float(doc['ih_vat'] or 0)
            gross_value = net_value + vat_value
            warehouse = str(doc['ih_loc'] or 'MAIN').strip()

            if current_status not in ('O', 'D'):  # Order or Delivery can be invoiced
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[f"Document status '{current_status}' cannot be invoiced. Must be Order (O) or Delivery (D)."]
                )

            if current_invoice:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[f"Document already has invoice number: {current_invoice}"]
                )

            # =====================
            # GET DOCUMENT LINES
            # =====================
            lines = self.sql.execute_query(f"""
                SELECT it_lineno, it_stock, it_desc, it_quan, it_price, it_exvat, it_vat,
                       it_vattyp, it_vatpct, it_cwcode, it_qtyallc, it_qtyinv, it_cost
                FROM itran WITH (NOLOCK)
                WHERE RTRIM(it_doc) = '{doc_no}'
            """)

            if lines.empty:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=["No lines found for document"]
                )

            # =====================
            # GET CONTROL ACCOUNTS
            # =====================
            control_accts = self.get_control_accounts()
            debtors_control = control_accts.debtors_control  # BB020

            # Get VAT output account
            vat_output_acct = 'CA060'  # Default VAT output

            # =====================
            # GENERATE IDS AND TIMESTAMPS
            # =====================
            unique_id = OperaUniqueIdGenerator.generate()
            now = datetime.now()
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')

            # Get period from invoice date
            period = invoice_date.month
            year = invoice_date.year

            # =====================
            # START TRANSACTION
            # =====================
            with self.sql.engine.begin() as conn:
                conn.execute(text(get_lock_timeout_sql()))

                # ----- 1. GET NEXT INVOICE NUMBER -----
                inv_result = conn.execute(text("""
                    SELECT ip_invno FROM iparm WITH (UPDLOCK, ROWLOCK)
                """)).fetchone()

                inv_no = str(inv_result[0]).strip()
                next_inv = self._increment_sop_number(inv_no)

                conn.execute(text(f"""
                    UPDATE iparm SET ip_invno = '{next_inv}', datemodified = '{now_str}'
                """))

                # ----- 2. UPDATE IHEAD -----
                conn.execute(text(f"""
                    UPDATE ihead WITH (ROWLOCK)
                    SET ih_docstat = 'I',
                        ih_invoice = '{inv_no}',
                        ih_invdate = '{invoice_date}',
                        ih_taxpoin = '{tax_point_date}',
                        datemodified = '{now_str}'
                    WHERE RTRIM(ih_doc) = '{doc_no}'
                """))

                # ----- 3. PROCESS EACH LINE -----
                sales_by_account = {}  # Accumulate sales by nominal account
                vat_total = 0.0

                for _, line in lines.iterrows():
                    stock_ref = str(line['it_stock'] or '').strip()
                    line_no = int(line['it_lineno'] or 0)
                    qty = float(line['it_quan'] or 0)
                    qty_to_inv = qty - float(line['it_qtyinv'] or 0)
                    line_net = float(line['it_exvat'] or 0)
                    line_vat = float(line['it_vat'] or 0)
                    line_cost = float(line['it_cost'] or 0)
                    vat_code = str(line['it_vattyp'] or 'S').strip()
                    vat_rate = float(line['it_vatpct'] or 20)
                    line_wh = str(line['it_cwcode'] or warehouse).strip()
                    line_desc = str(line['it_desc'] or '').strip()

                    if qty_to_inv <= 0:
                        continue  # Already invoiced

                    # Update itran - mark as invoiced
                    conn.execute(text(f"""
                        UPDATE itran WITH (ROWLOCK)
                        SET it_qtyinv = it_quan,
                            it_dteinv = '{invoice_date}',
                            datemodified = '{now_str}'
                        WHERE RTRIM(it_doc) = '{doc_no}' AND it_lineno = {line_no}
                    """))

                    # ----- 3a. ISSUE STOCK (if applicable) -----
                    if issue_stock and stock_ref and sop_params.get('update_transactions', True):
                        # Get product's sales nominal account
                        prod_check = conn.execute(text(f"""
                            SELECT cn_salesac FROM cname WITH (NOLOCK)
                            WHERE RTRIM(cn_ref) = '{stock_ref}'
                        """)).fetchone()

                        sales_nominal = str(prod_check[0]).strip() if prod_check and prod_check[0] else 'E1000'

                        # Create ctran record (stock issue)
                        ctran_id = OperaUniqueIdGenerator.generate()
                        conn.execute(text(f"""
                            INSERT INTO ctran (
                                ct_ref, ct_loc, ct_type, ct_date, ct_crdate, ct_quan,
                                ct_cost, ct_sell, ct_comnt, ct_referen, ct_account,
                                ct_time, ct_unique, datecreated, datemodified, state
                            ) VALUES (
                                '{stock_ref}', '{line_wh}', 'S', '{invoice_date}', '{invoice_date}',
                                -{qty_to_inv}, {line_cost}, {line_net / qty_to_inv if qty_to_inv > 0 else 0},
                                'Invoice {inv_no}', '{inv_no[:10]}', '{customer_account[:8]}',
                                '{now.strftime('%H:%M:%S')}', '{ctran_id}', '{now_str}', '{now_str}', 1
                            )
                        """))

                        # Update cstwh - reduce stock
                        conn.execute(text(f"""
                            UPDATE cstwh WITH (ROWLOCK)
                            SET cs_instock = cs_instock - {qty_to_inv},
                                cs_alloc = CASE WHEN cs_alloc >= {qty_to_inv} THEN cs_alloc - {qty_to_inv} ELSE 0 END,
                                cs_saleord = CASE WHEN cs_saleord >= {qty_to_inv} THEN cs_saleord - {qty_to_inv} ELSE 0 END,
                                datemodified = '{now_str}'
                            WHERE RTRIM(cs_ref) = '{stock_ref}' AND RTRIM(cs_whar) = '{line_wh}'
                        """))

                        # Update cname - reduce total stock
                        conn.execute(text(f"""
                            UPDATE cname WITH (ROWLOCK)
                            SET cn_instock = cn_instock - {qty_to_inv},
                                cn_alloc = CASE WHEN cn_alloc >= {qty_to_inv} THEN cn_alloc - {qty_to_inv} ELSE 0 END,
                                cn_saleord = CASE WHEN cn_saleord >= {qty_to_inv} THEN cn_saleord - {qty_to_inv} ELSE 0 END,
                                datemodified = '{now_str}'
                            WHERE RTRIM(cn_ref) = '{stock_ref}'
                        """))
                    else:
                        # Service item - use default sales account or line's analysis
                        sales_nominal = 'E1000'  # Default

                    # Accumulate sales by nominal account
                    if sales_nominal not in sales_by_account:
                        sales_by_account[sales_nominal] = {'net': 0.0, 'vat': 0.0, 'desc': line_desc}
                    sales_by_account[sales_nominal]['net'] += line_net
                    sales_by_account[sales_nominal]['vat'] += line_vat
                    vat_total += line_vat

                # ----- 4. CREATE STRAN (Sales Ledger Transaction) -----
                conn.execute(text(f"""
                    INSERT INTO stran (
                        st_account, st_type, st_trref, st_trdate, st_crdate, st_trvalue,
                        st_vatval, st_unique, st_taxpoin, st_fullamt, st_trbal,
                        datecreated, datemodified, state
                    ) VALUES (
                        '{customer_account}', '{invoice_type}', '{inv_no}', '{invoice_date}',
                        '{invoice_date}', {gross_value},
                        {vat_value}, '{unique_id}', '{tax_point_date}', {gross_value}, {gross_value},
                        '{now_str}', '{now_str}', 1
                    )
                """))

                # ----- 5. CREATE SNOML (Transfer File) entries -----
                # One entry per sales account, plus one for VAT
                for sales_acct, amounts in sales_by_account.items():
                    # Sales account entry (credit = positive in snoml but negative in ntran)
                    conn.execute(text(f"""
                        INSERT INTO snoml (
                            sx_nacnt, sx_type, sx_tref, sx_date, sx_value, sx_unique,
                            sx_done, sx_comment, datecreated, datemodified, state
                        ) VALUES (
                            '{sales_acct}', 'I', '{inv_no}', '{invoice_date}',
                            {amounts['net']}, '{unique_id}',
                            '{'Y' if post_to_nominal else 'N'}', '{customer_name[:30]} {amounts['desc'][:20]}',
                            '{now_str}', '{now_str}', 1
                        )
                    """))

                # VAT entry
                if vat_total > 0:
                    conn.execute(text(f"""
                        INSERT INTO snoml (
                            sx_nacnt, sx_type, sx_tref, sx_date, sx_value, sx_unique,
                            sx_done, sx_comment, datecreated, datemodified, state
                        ) VALUES (
                            '{vat_output_acct}', 'I', '{inv_no}', '{invoice_date}',
                            {vat_total}, '{unique_id}',
                            '{'Y' if post_to_nominal else 'N'}', '{customer_name[:30]} VAT',
                            '{now_str}', '{now_str}', 1
                        )
                    """))

                # ----- 6. CREATE NTRAN and UPDATE NACNT (if post_to_nominal) -----
                if post_to_nominal:
                    # Get next journal number
                    jrnl_result = conn.execute(text("""
                        SELECT ISNULL(MAX(nt_jrnl), 0) + 1 FROM ntran
                    """)).fetchone()
                    jrnl_no = int(jrnl_result[0])

                    # Debit debtors control account
                    conn.execute(text(f"""
                        INSERT INTO ntran (
                            nt_acnt, nt_type, nt_trnref, nt_ref, nt_value, nt_posttyp,
                            nt_period, nt_year, nt_jrnl, nt_inp,
                            datecreated, datemodified, state
                        ) VALUES (
                            '{debtors_control}', 'D', '{customer_name[:30]}', '{inv_no}',
                            {gross_value}, 'I', {period}, {year}, {jrnl_no}, '{input_by[:8]}',
                            '{now_str}', '{now_str}', 1
                        )
                    """))
                    self.update_nacnt_balance(debtors_control, gross_value, period, year, conn)

                    # Credit sales accounts
                    for sales_acct, amounts in sales_by_account.items():
                        conn.execute(text(f"""
                            INSERT INTO ntran (
                                nt_acnt, nt_type, nt_trnref, nt_ref, nt_value, nt_posttyp,
                                nt_period, nt_year, nt_jrnl, nt_inp,
                                datecreated, datemodified, state
                            ) VALUES (
                                '{sales_acct}', 'E', '{customer_name[:30]}', '{inv_no}',
                                -{amounts['net']}, 'I', {period}, {year}, {jrnl_no}, '{input_by[:8]}',
                                '{now_str}', '{now_str}', 1
                            )
                        """))
                        self.update_nacnt_balance(sales_acct, -amounts['net'], period, year, conn)

                    # Credit VAT output
                    if vat_total > 0:
                        conn.execute(text(f"""
                            INSERT INTO ntran (
                                nt_acnt, nt_type, nt_trnref, nt_ref, nt_value, nt_posttyp,
                                nt_period, nt_year, nt_jrnl, nt_inp,
                                datecreated, datemodified, state
                            ) VALUES (
                                '{vat_output_acct}', 'C', '{customer_name[:30]}', '{inv_no}',
                                -{vat_total}, 'I', {period}, {year}, {jrnl_no}, '{input_by[:8]}',
                                '{now_str}', '{now_str}', 1
                            )
                        """))
                        self.update_nacnt_balance(vat_output_acct, -vat_total, period, year, conn)

                # ----- 7. CREATE ZVTRAN (VAT Analysis) -----
                if vat_total > 0:
                    conn.execute(text(f"""
                        INSERT INTO zvtran (
                            va_account, va_source, va_trtype, va_trdate, va_taxdate,
                            va_trref, va_trvalue, va_vatval, va_vattype, va_vatrate,
                            va_done, datecreated, datemodified, state
                        ) VALUES (
                            '{customer_account}', 'S', 'I', '{invoice_date}', '{tax_point_date}',
                            '{inv_no}', {net_value}, {vat_total}, 'S', 20,
                            'Y', '{now_str}', '{now_str}', 1
                        )
                    """))

                # ----- 8. CREATE NVAT (VAT Return Tracking) -----
                if vat_total > 0:
                    conn.execute(text(f"""
                        INSERT INTO nvat (
                            nv_acnt, nv_date, nv_crdate, nv_taxdate, nv_ref, nv_type,
                            nv_value, nv_vatval, nv_vattype, nv_vatcode, nv_vatrate,
                            nv_comment, datecreated, datemodified, state
                        ) VALUES (
                            '{vat_output_acct}', '{invoice_date}', '{invoice_date}', '{tax_point_date}',
                            '{inv_no}', 'I', {net_value}, {vat_total}, 'S', 'S', 20,
                            'Sales Invoice', '{now_str}', '{now_str}', 1
                        )
                    """))

                # ----- 9. UPDATE CUSTOMER BALANCE -----
                conn.execute(text(f"""
                    UPDATE sname WITH (ROWLOCK)
                    SET sn_currbal = sn_currbal + {gross_value},
                        datemodified = '{now_str}'
                    WHERE RTRIM(sn_account) = '{customer_account}'
                """))

            logger.info(f"Created invoice {inv_no} from {doc_no} for {customer_account}: "
                       f"£{net_value:.2f} + £{vat_value:.2f} VAT = £{gross_value:.2f}")

            return ImportResult(
                success=True,
                records_processed=1,
                records_imported=1,
                entry_number=doc_no,
                transaction_ref=inv_no,
                warnings=[
                    f"Invoice: {inv_no}",
                    f"Customer: {customer_account} - {customer_name}",
                    f"Net: £{net_value:.2f}",
                    f"VAT: £{vat_value:.2f}",
                    f"Gross: £{gross_value:.2f}",
                    f"Posted to Nominal: {'Yes' if post_to_nominal else 'No'}",
                    f"Stock Issued: {'Yes' if issue_stock else 'No'}",
                    f"Tables: stran, snoml, ntran, zvtran, nvat, sname"
                ] + warnings
            )

        except Exception as e:
            logger.error(f"Failed to create invoice: {e}")
            import traceback
            traceback.print_exc()
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[str(e)]
            )


    # =========================================================================
    # POP (PURCHASE ORDER PROCESSING) METHODS
    # =========================================================================

    def get_pop_parameters(self) -> Dict[str, Any]:
        """
        Get POP company options from dparm table.

        Returns dict with:
        - next_po_ref: Next committed PO reference (dp_dcref)
        - default_warehouse: Default warehouse code
        - approve_required: Whether PO approval is required
        """
        try:
            result = self.sql.execute_query("""
                SELECT TOP 1
                    dp_dcref         -- Next committed PO reference
                FROM dparm WITH (NOLOCK)
            """)

            if result is None or result.empty:
                logger.warning("No dparm record found - using defaults")
                return {
                    'next_po_ref': 'CPO00001',
                    'default_warehouse': 'MAIN',
                    'approve_required': False
                }

            row = result.iloc[0]

            # Get default warehouse from sprfls or use default
            wh_result = self.sql.execute_query("""
                SELECT TOP 1 sc_warehse FROM sprfls WITH (NOLOCK)
            """)
            default_warehouse = 'MAIN'
            if wh_result is not None and not wh_result.empty:
                default_warehouse = str(wh_result.iloc[0].get('sc_warehse', 'MAIN') or 'MAIN').strip()

            return {
                'next_po_ref': str(row.get('dp_dcref', 'CPO00001')).strip(),
                'default_warehouse': default_warehouse,
                'approve_required': False  # pparm.pp_approve controls PL approval, not POP
            }
        except Exception as e:
            logger.error(f"Error getting POP parameters: {e}")
            return {
                'next_po_ref': 'CPO00001',
                'default_warehouse': 'MAIN',
                'approve_required': False
            }

    def _get_next_grn_ref(self, conn) -> str:
        """
        Get next GRN reference number.
        GRN refs are generated from MAX(ch_ref) + 1, not stored in dparm.
        """
        try:
            result = conn.execute(text("""
                SELECT ISNULL(MAX(ch_ref), '0000000000') AS max_ref FROM cghead
            """)).fetchone()

            max_ref = str(result[0]).strip() if result else '0000000000'

            # Parse and increment - format is typically numeric (e.g., '0000000001')
            try:
                num = int(max_ref) + 1
                return str(num).zfill(10)
            except ValueError:
                # Non-numeric format - try to increment
                return self._increment_pop_number(max_ref)
        except Exception as e:
            logger.warning(f"Error getting next GRN ref: {e}")
            return '0000000001'

    def get_purchase_ledger_parameters(self) -> Dict[str, Any]:
        """
        Get Purchase Ledger company parameters from pparm.

        Returns dict with:
        - vat_input_account: Nominal account for VAT input
        - default_currency: Default currency code
        """
        try:
            result = self.sql.execute_query("""
                SELECT TOP 1
                    pp_vatpnom,      -- VAT input nominal
                    pp_fcurr         -- Default currency
                FROM pparm WITH (NOLOCK)
            """)

            if result is None or result.empty:
                return {
                    'vat_input_account': 'CA050',  # Default VAT input
                    'default_currency': '   '  # Blank = GBP
                }

            row = result.iloc[0]
            return {
                'vat_input_account': str(row.get('pp_vatpnom', 'CA050')).strip() or 'CA050',
                'default_currency': str(row.get('pp_fcurr', '   ')).strip()
            }
        except Exception as e:
            logger.error(f"Error getting PL parameters: {e}")
            return {
                'vat_input_account': 'CA050',
                'default_currency': '   '
            }

    def _increment_pop_number(self, current: str) -> str:
        """
        Increment a POP reference number (CPO00001 -> CPO00002).

        Args:
            current: Current reference like 'CPO00001' or 'GRN00001'

        Returns:
            Next reference number
        """
        # Find where letters end and numbers begin
        for i, c in enumerate(current):
            if c.isdigit():
                prefix = current[:i]
                num_part = current[i:]
                try:
                    num = int(num_part) + 1
                    return f"{prefix}{str(num).zfill(len(num_part))}"
                except ValueError:
                    pass
                break
        # Fallback - just append 1
        return current + '1'

    def import_purchase_order(
        self,
        supplier_account: str,
        lines: List[Dict[str, Any]],
        po_date: date = None,
        delivery_name: str = None,
        delivery_address: List[str] = None,
        warehouse: str = None,
        currency: str = None,
        exchange_rate: float = 1.0,
        reference: str = "",
        narrative: str = "",
        contact: str = "",
        input_by: str = "SQLRAG"
    ) -> ImportResult:
        """
        Create a purchase order in Opera.

        Creates records in:
        - dohead: PO header
        - doline: PO lines
        - cstwh/cname: Update CS_ORDER/CN_ONORDER (stock on order)

        Args:
            supplier_account: Supplier account code (must exist in pname)
            lines: List of line dicts with:
                - stock_ref: Stock reference (or blank for non-stock)
                - supplier_ref: Supplier's reference (optional)
                - description: Line description
                - quantity: Quantity required
                - unit_price: Price per unit
                - discount_percent: Line discount % (optional)
                - warehouse: Warehouse code (optional, uses header default)
                - required_date: Date required (optional)
                - ledger_account: Nominal account for non-stock (optional)
            po_date: PO date (defaults to today)
            delivery_name: Delivery name
            delivery_address: List of up to 4 address lines + postcode
            warehouse: Default warehouse
            currency: Currency code (blank = GBP)
            exchange_rate: Exchange rate
            reference: External reference
            narrative: PO narrative
            contact: Contact name
            input_by: User creating the PO

        Returns:
            ImportResult with PO number in transaction_ref
        """
        warnings = []

        if po_date is None:
            po_date = date.today()

        if isinstance(po_date, str):
            po_date = datetime.strptime(po_date, '%Y-%m-%d').date()

        try:
            # Get company options
            pop_params = self.get_pop_parameters()

            if not warehouse:
                warehouse = pop_params.get('default_warehouse', 'MAIN')

            if currency is None:
                pl_params = self.get_purchase_ledger_parameters()
                currency = pl_params.get('default_currency', '   ')

            # Validate supplier exists
            supplier_check = self.sql.execute_query(f"""
                SELECT pn_acnt, pn_name, pn_addr1, pn_addr2, pn_addr3, pn_addr4, pn_pcode
                FROM pname WITH (NOLOCK)
                WHERE RTRIM(pn_acnt) = '{supplier_account}'
            """)

            if supplier_check is None or supplier_check.empty:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[f"Supplier account not found: {supplier_account}"]
                )

            supplier = supplier_check.iloc[0]
            supplier_name = str(supplier['pn_name'] or '').strip()

            # Default delivery address from supplier
            if not delivery_name:
                delivery_name = supplier_name
            if not delivery_address or len(delivery_address) == 0:
                delivery_address = [
                    str(supplier.get('pn_addr1', '') or '').strip(),
                    str(supplier.get('pn_addr2', '') or '').strip(),
                    str(supplier.get('pn_addr3', '') or '').strip(),
                    str(supplier.get('pn_addr4', '') or '').strip(),
                    str(supplier.get('pn_pcode', '') or '').strip()
                ]

            # Pad delivery address to 5 elements
            while len(delivery_address) < 5:
                delivery_address.append('')

            # Validate lines
            if not lines or len(lines) == 0:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=["At least one line is required"]
                )

            # Timestamps
            unique_id = OperaUniqueIdGenerator.generate()
            now = datetime.now()
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')

            # Calculate totals
            total_value = 0.0
            for line in lines:
                qty = float(line.get('quantity', 0))
                price = float(line.get('unit_price', 0))
                disc = float(line.get('discount_percent', 0))
                line_value = qty * price * (1 - disc / 100)
                total_value += line_value

            # Start transaction
            with self.sql.engine.begin() as conn:
                conn.execute(text(get_lock_timeout_sql()))

                # Get next PO number from dparm.dp_dcref
                po_result = conn.execute(text("""
                    SELECT dp_dcref FROM dparm WITH (UPDLOCK, ROWLOCK)
                """)).fetchone()

                if po_result:
                    po_ref = str(po_result[0]).strip()
                    next_po_ref = self._increment_pop_number(po_ref)

                    conn.execute(text(f"""
                        UPDATE dparm SET dp_dcref = '{next_po_ref}', datemodified = '{now_str}'
                    """))
                else:
                    po_ref = 'CPO00001'
                    next_po_ref = 'CPO00002'

                # Create PO header in dohead
                conn.execute(text(f"""
                    INSERT INTO dohead (
                        dc_ref, dc_account, dc_totval, dc_odisc, dc_cwcode,
                        dc_delnam, dc_delad1, dc_delad2, dc_delad3, dc_delad4, dc_deladpc,
                        dc_contact, dc_currcy, dc_exrate, dc_ref2, dc_narr1,
                        dc_cancel, dc_printed, dc_porder,
                        sq_crdate, datecreated, datemodified, state
                    ) VALUES (
                        '{po_ref}', '{supplier_account}', {total_value}, 0, '{warehouse}',
                        '{delivery_name[:30]}', '{delivery_address[0][:30]}', '{delivery_address[1][:30]}',
                        '{delivery_address[2][:30]}', '{delivery_address[3][:30]}', '{delivery_address[4][:10]}',
                        '{contact[:30]}', '{currency[:3]}', {exchange_rate}, '{reference[:20]}', '{narrative[:60]}',
                        0, 0, '',
                        '{po_date}', '{now_str}', '{now_str}', 1
                    )
                """))

                # Create PO lines
                line_no = 0
                for line in lines:
                    line_no += 1
                    stock_ref = str(line.get('stock_ref', '') or '').strip()[:16]
                    supplier_ref = str(line.get('supplier_ref', '') or '').strip()[:16]
                    description = str(line.get('description', '') or '').strip()[:30]
                    qty = float(line.get('quantity', 0))
                    price = float(line.get('unit_price', 0))
                    disc = float(line.get('discount_percent', 0))
                    line_wh = str(line.get('warehouse', warehouse) or warehouse).strip()[:4]
                    req_date = line.get('required_date')
                    ledger_acct = str(line.get('ledger_account', '') or '').strip()[:6]

                    if req_date and isinstance(req_date, str):
                        req_date = datetime.strptime(req_date, '%Y-%m-%d').date()
                    elif not req_date:
                        req_date = po_date

                    line_value = qty * price * (1 - disc / 100)

                    conn.execute(text(f"""
                        INSERT INTO doline (
                            do_dcref, do_dcline, do_account, do_cnref, do_supref,
                            do_desc, do_cwcode, do_reqqty, do_recqty, do_retqty, do_invqty,
                            do_price, do_value, do_discp, do_reqdat, do_ledger,
                            datecreated, datemodified, state
                        ) VALUES (
                            '{po_ref}', {line_no}, '{supplier_account}', '{stock_ref}', '{supplier_ref}',
                            '{description}', '{line_wh}', {qty}, 0, 0, 0,
                            {price}, {line_value}, {disc}, '{req_date}', '{ledger_acct}',
                            '{now_str}', '{now_str}', 1
                        )
                    """))

                    # Update stock on order (if stock item)
                    if stock_ref:
                        # Update cstwh - warehouse stock on order
                        conn.execute(text(f"""
                            UPDATE cstwh WITH (ROWLOCK)
                            SET cs_order = cs_order + {qty},
                                datemodified = '{now_str}'
                            WHERE RTRIM(cs_ref) = '{stock_ref}' AND RTRIM(cs_whar) = '{line_wh}'
                        """))

                        # Update cname - total stock on order
                        conn.execute(text(f"""
                            UPDATE cname WITH (ROWLOCK)
                            SET cn_onorder = cn_onorder + {qty},
                                datemodified = '{now_str}'
                            WHERE RTRIM(cn_ref) = '{stock_ref}'
                        """))

            logger.info(f"Created PO {po_ref} for {supplier_account} ({supplier_name}): "
                       f"£{total_value:.2f} with {line_no} lines")

            return ImportResult(
                success=True,
                records_processed=1,
                records_imported=1,
                transaction_ref=po_ref,
                warnings=[
                    f"PO Number: {po_ref}",
                    f"Supplier: {supplier_account} - {supplier_name}",
                    f"Lines: {line_no}",
                    f"Total Value: £{total_value:.2f}",
                    f"Stock on order updated for stock items"
                ] + warnings
            )

        except Exception as e:
            logger.error(f"Failed to create purchase order: {e}")
            import traceback
            traceback.print_exc()
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[str(e)]
            )

    def create_grn(
        self,
        lines: List[Dict[str, Any]],
        grn_date: date = None,
        delivery_ref: str = "",
        received_by: str = "SQLRAG",
        update_stock: bool = True
    ) -> ImportResult:
        """
        Create a Goods Received Note in Opera.

        Creates records in:
        - cghead: GRN header
        - cgline: GRN lines
        - ctran: Stock receipt transactions (if update_stock=True)
        - cstwh/cname: Update stock levels (if update_stock=True)

        If PO references are provided, also:
        - Updates doline.do_recqty (received quantity)
        - Reduces CS_ORDER/CN_ONORDER

        Args:
            lines: List of line dicts with:
                - stock_ref: Stock reference
                - supplier_account: Supplier account
                - supplier_ref: Supplier's reference (optional)
                - description: Item description
                - quantity: Quantity received
                - unit_cost: Cost per unit
                - warehouse: Warehouse code
                - po_number: PO reference (optional - for matching)
                - po_line: PO line number (optional - for matching)
            grn_date: GRN date (defaults to today)
            delivery_ref: Carrier's delivery reference
            received_by: User receiving the goods
            update_stock: Whether to update stock levels

        Returns:
            ImportResult with GRN number in transaction_ref
        """
        warnings = []

        if grn_date is None:
            grn_date = date.today()

        if isinstance(grn_date, str):
            grn_date = datetime.strptime(grn_date, '%Y-%m-%d').date()

        try:
            if not lines or len(lines) == 0:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=["At least one line is required"]
                )

            # Timestamps
            unique_id = OperaUniqueIdGenerator.generate()
            now = datetime.now()
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')
            time_str = now.strftime('%H:%M:%S')

            pop_params = self.get_pop_parameters()

            # Start transaction
            with self.sql.engine.begin() as conn:
                conn.execute(text(get_lock_timeout_sql()))

                # Get next GRN number (generated from MAX(ch_ref), not stored in dparm)
                grn_ref = self._get_next_grn_ref(conn)

                # Create GRN header
                conn.execute(text(f"""
                    INSERT INTO cghead (
                        ch_ref, ch_date, ch_time, ch_dref, ch_user, ch_status,
                        ch_delchg, ch_vat,
                        sq_crdate, datecreated, datemodified, state
                    ) VALUES (
                        '{grn_ref}', '{grn_date}', '{time_str}', '{delivery_ref[:20]}', '{received_by[:8]}', 0,
                        0, 0,
                        '{grn_date}', '{now_str}', '{now_str}', 1
                    )
                """))

                # Create GRN lines
                line_no = 0
                total_value = 0.0
                po_updates = {}  # Track PO line updates

                for line in lines:
                    line_no += 1
                    stock_ref = str(line.get('stock_ref', '') or '').strip()[:16]
                    supplier_account = str(line.get('supplier_account', '') or '').strip()[:8]
                    supplier_ref = str(line.get('supplier_ref', '') or '').strip()[:16]
                    description = str(line.get('description', '') or '').strip()[:30]
                    qty = float(line.get('quantity', 0))
                    cost = float(line.get('unit_cost', 0))
                    warehouse = str(line.get('warehouse', pop_params.get('default_warehouse', 'MAIN')) or 'MAIN').strip()[:4]
                    po_number = str(line.get('po_number', '') or '').strip()
                    po_line = int(line.get('po_line', 0) or 0)

                    line_value = qty * cost
                    total_value += line_value

                    # Insert cgline
                    conn.execute(text(f"""
                        INSERT INTO cgline (
                            ci_chref, ci_line, ci_account, ci_supref, ci_cnref, ci_desc,
                            ci_qtyrcv, ci_qtyrel, ci_qtyret, ci_qtymat,
                            ci_cost, ci_value, ci_bkware,
                            ci_dcref, ci_dcline,
                            datecreated, datemodified, state
                        ) VALUES (
                            '{grn_ref}', {line_no}, '{supplier_account}', '{supplier_ref}', '{stock_ref}', '{description}',
                            {qty}, {qty if update_stock else 0}, 0, {qty if po_number else 0},
                            {cost}, {line_value}, '{warehouse}',
                            '{po_number}', {po_line},
                            '{now_str}', '{now_str}', 1
                        )
                    """))

                    # Update stock if enabled
                    if update_stock and stock_ref:
                        # Create ctran (stock receipt)
                        ctran_id = OperaUniqueIdGenerator.generate()
                        conn.execute(text(f"""
                            INSERT INTO ctran (
                                ct_ref, ct_loc, ct_type, ct_date, ct_crdate, ct_quan,
                                ct_cost, ct_sell, ct_comnt, ct_referen, ct_account,
                                ct_time, ct_unique, datecreated, datemodified, state
                            ) VALUES (
                                '{stock_ref}', '{warehouse}', 'R', '{grn_date}', '{grn_date}',
                                {qty}, {cost}, 0,
                                'GRN {grn_ref}', '{grn_ref[:10]}', '{supplier_account[:8]}',
                                '{time_str}', '{ctran_id}', '{now_str}', '{now_str}', 1
                            )
                        """))

                        # Update cstwh - increase stock in warehouse
                        conn.execute(text(f"""
                            UPDATE cstwh WITH (ROWLOCK)
                            SET cs_instock = cs_instock + {qty},
                                datemodified = '{now_str}'
                            WHERE RTRIM(cs_ref) = '{stock_ref}' AND RTRIM(cs_whar) = '{warehouse}'
                        """))

                        # Update cname - increase total stock
                        conn.execute(text(f"""
                            UPDATE cname WITH (ROWLOCK)
                            SET cn_instock = cn_instock + {qty},
                                cn_lastcst = {cost},
                                datemodified = '{now_str}'
                            WHERE RTRIM(cn_ref) = '{stock_ref}'
                        """))

                    # Track PO updates
                    if po_number and po_line > 0:
                        key = (po_number, po_line)
                        if key not in po_updates:
                            po_updates[key] = 0
                        po_updates[key] += qty

                # Update PO lines (received quantity)
                for (po_number, po_line), recv_qty in po_updates.items():
                    # Update received quantity on PO
                    conn.execute(text(f"""
                        UPDATE doline WITH (ROWLOCK)
                        SET do_recqty = do_recqty + {recv_qty},
                            datemodified = '{now_str}'
                        WHERE do_dcref = '{po_number}' AND do_dcline = {po_line}
                    """))

                    # Get stock ref from PO line to update on-order
                    po_line_check = conn.execute(text(f"""
                        SELECT do_cnref, do_cwcode FROM doline WITH (NOLOCK)
                        WHERE do_dcref = '{po_number}' AND do_dcline = {po_line}
                    """)).fetchone()

                    if po_line_check and po_line_check[0]:
                        po_stock_ref = str(po_line_check[0]).strip()
                        po_warehouse = str(po_line_check[1]).strip()

                        # Reduce stock on order (warehouse)
                        conn.execute(text(f"""
                            UPDATE cstwh WITH (ROWLOCK)
                            SET cs_order = CASE WHEN cs_order >= {recv_qty} THEN cs_order - {recv_qty} ELSE 0 END,
                                datemodified = '{now_str}'
                            WHERE RTRIM(cs_ref) = '{po_stock_ref}' AND RTRIM(cs_whar) = '{po_warehouse}'
                        """))

                        # Reduce stock on order (total)
                        conn.execute(text(f"""
                            UPDATE cname WITH (ROWLOCK)
                            SET cn_onorder = CASE WHEN cn_onorder >= {recv_qty} THEN cn_onorder - {recv_qty} ELSE 0 END,
                                datemodified = '{now_str}'
                            WHERE RTRIM(cn_ref) = '{po_stock_ref}'
                        """))

                        warnings.append(f"Updated PO {po_number} line {po_line}: received {recv_qty}")

            logger.info(f"Created GRN {grn_ref}: {line_no} lines, £{total_value:.2f}")

            return ImportResult(
                success=True,
                records_processed=1,
                records_imported=1,
                transaction_ref=grn_ref,
                warnings=[
                    f"GRN Number: {grn_ref}",
                    f"Lines: {line_no}",
                    f"Total Value: £{total_value:.2f}",
                    f"Stock Updated: {'Yes' if update_stock else 'No'}"
                ] + warnings
            )

        except Exception as e:
            logger.error(f"Failed to create GRN: {e}")
            import traceback
            traceback.print_exc()
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[str(e)]
            )

    def receive_po_lines(
        self,
        po_number: str,
        lines_to_receive: List[Dict[str, Any]] = None,
        grn_date: date = None,
        delivery_ref: str = "",
        received_by: str = "SQLRAG"
    ) -> ImportResult:
        """
        Receive goods against a purchase order.

        This is a convenience method that creates a GRN automatically linked to PO lines.
        If lines_to_receive is not specified, receives all outstanding quantities.

        Args:
            po_number: PO reference to receive against
            lines_to_receive: Optional list of dicts with:
                - line_number: PO line number
                - quantity: Quantity to receive (defaults to outstanding)
                - unit_cost: Cost override (optional)
            grn_date: GRN date (defaults to today)
            delivery_ref: Carrier's delivery reference
            received_by: User receiving goods

        Returns:
            ImportResult with GRN number
        """
        try:
            # Get PO header
            po_check = self.sql.execute_query(f"""
                SELECT dc_ref, dc_account, dc_cwcode, dc_cancel
                FROM dohead WITH (NOLOCK)
                WHERE dc_ref = '{po_number}'
            """)

            if po_check is None or po_check.empty:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[f"Purchase order not found: {po_number}"]
                )

            po_header = po_check.iloc[0]
            if po_header.get('dc_cancel'):
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[f"Purchase order {po_number} is cancelled"]
                )

            supplier_account = str(po_header['dc_account']).strip()
            default_warehouse = str(po_header.get('dc_cwcode', 'MAIN') or 'MAIN').strip()

            # Get PO lines
            po_lines = self.sql.execute_query(f"""
                SELECT do_dcline, do_cnref, do_supref, do_desc, do_cwcode,
                       do_reqqty, do_recqty, do_price
                FROM doline WITH (NOLOCK)
                WHERE do_dcref = '{po_number}'
                ORDER BY do_dcline
            """)

            if po_lines is None or po_lines.empty:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=[f"No lines found for PO {po_number}"]
                )

            # Build GRN lines
            grn_lines = []
            receive_map = {}
            if lines_to_receive:
                for lr in lines_to_receive:
                    receive_map[int(lr['line_number'])] = {
                        'quantity': float(lr.get('quantity', 0)),
                        'unit_cost': lr.get('unit_cost')
                    }

            for _, po_line in po_lines.iterrows():
                line_no = int(po_line['do_dcline'])
                stock_ref = str(po_line['do_cnref'] or '').strip()
                supplier_ref = str(po_line['do_supref'] or '').strip()
                description = str(po_line['do_desc'] or '').strip()
                warehouse = str(po_line['do_cwcode'] or default_warehouse).strip()
                qty_ordered = float(po_line['do_reqqty'] or 0)
                qty_received = float(po_line['do_recqty'] or 0)
                unit_cost = float(po_line['do_price'] or 0)
                outstanding = qty_ordered - qty_received

                if outstanding <= 0:
                    continue  # Fully received

                # Determine quantity to receive
                if lines_to_receive:
                    if line_no not in receive_map:
                        continue  # Not in list to receive
                    recv_qty = receive_map[line_no].get('quantity', outstanding)
                    if receive_map[line_no].get('unit_cost') is not None:
                        unit_cost = float(receive_map[line_no]['unit_cost'])
                else:
                    recv_qty = outstanding  # Receive all outstanding

                if recv_qty > outstanding:
                    recv_qty = outstanding  # Can't receive more than outstanding

                if recv_qty > 0:
                    grn_lines.append({
                        'stock_ref': stock_ref,
                        'supplier_account': supplier_account,
                        'supplier_ref': supplier_ref,
                        'description': description,
                        'quantity': recv_qty,
                        'unit_cost': unit_cost,
                        'warehouse': warehouse,
                        'po_number': po_number,
                        'po_line': line_no
                    })

            if len(grn_lines) == 0:
                return ImportResult(
                    success=False,
                    records_processed=1,
                    records_failed=1,
                    errors=["No lines with outstanding quantities to receive"]
                )

            # Create the GRN
            return self.create_grn(
                lines=grn_lines,
                grn_date=grn_date,
                delivery_ref=delivery_ref,
                received_by=received_by,
                update_stock=True
            )

        except Exception as e:
            logger.error(f"Failed to receive PO lines: {e}")
            import traceback
            traceback.print_exc()
            return ImportResult(
                success=False,
                records_processed=1,
                records_failed=1,
                errors=[str(e)]
            )


def get_opera_sql_import(sql_connector) -> OperaSQLImport:
    """Factory function to create an OperaSQLImport instance"""
    return OperaSQLImport(sql_connector)
