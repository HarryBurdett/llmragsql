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
        validate_only: bool = False
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
            posting_decision = get_period_posting_decision(self.sql, post_date)

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
            amount_pence = int(amount_pounds * 100)  # aentry/atran use pence
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
                        0, '{date_str}', '{time_str[:8]}', '{input_by[:8]}', '',
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
                        '{customer_account}', '{customer_name[:35]}', '', '        ', '',
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
                ntran_comment = f"{reference[:50]:<50}"
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
        validate_only: bool = False
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
            # PERIOD VALIDATION (Sales Ledger)
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
            amount_pence = int(amount_pounds * 100)

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
                        0, '{date_str}', '{time_str[:8]}', '{input_by[:8]}', '',
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
                        '{customer_account}', '{customer_name[:35]}', '', '        ', '',
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
                ntran_comment = f"{reference[:50]:<50}"
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
        validate_only: bool = False
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
            posting_decision = get_period_posting_decision(self.sql, post_date)

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
            amount_pence = int(amount_pounds * 100)

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

            # Build trnref like Opera does
            ntran_comment = f"{reference[:50]:<50}"
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
                        0, '{date_str}', '{time_str[:8]}', '{input_by[:8]}', '',
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
                        '{supplier_account}', '{supplier_name[:35]}', '', '        ', '',
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
        validate_only: bool = False
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
            posting_decision = get_period_posting_decision(self.sql, post_date)

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
            amount_pence = int(amount_pounds * 100)

            if isinstance(post_date, str):
                post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

            year = post_date.year
            period = post_date.month

            now = datetime.now()
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')
            date_str = now.strftime('%Y-%m-%d')
            time_str = now.strftime('%H:%M:%S')

            ntran_comment = f"{reference[:50]:<50}"
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
                        0, '{date_str}', '{time_str[:8]}', '{input_by[:8]}', '',
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
                        '{supplier_account}', '{supplier_name[:35]}', '', '        ', '',
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

            logger.info(f"Successfully imported purchase invoice posting: {invoice_number} for £{gross_amount:.2f}")

            return ImportResult(
                success=True,
                records_processed=1,
                records_imported=1,
                warnings=[
                    f"Invoice: {invoice_number}",
                    f"Journal number: {next_journal}",
                    f"Gross: £{gross_amount:.2f} (Net: £{net_amount:.2f} + VAT: £{vat_amount:.2f})"
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
        fees_nominal_account: str = None,
        complete_batch: bool = False,
        input_by: str = "GOCARDLS",
        cbtype: str = None,
        validate_only: bool = False
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
            gocardless_fees: Total GoCardless fees to post to nominal (optional)
            fees_nominal_account: Nominal account for fees (e.g., 'GA400')
            complete_batch: If True, completes batch immediately (creates ntran/anoml)
            input_by: User code for audit trail
            cbtype: Cashbook type code (must be batched Receipt type). Auto-detects GoCardless type if None.
            validate_only: If True, only validate without inserting

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
            posting_decision = get_period_posting_decision(self.sql, post_date)

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

            # Generate unique IDs for each payment
            unique_ids = OperaUniqueIdGenerator.generate_multiple(len(payments) * 3 + 2)

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
                    amount_pence = int(amount_pounds * 100)
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

                        # anoml Bank account - ax_done='Y' indicates posted to nominal
                        anoml_bank_sql = f"""
                            INSERT INTO anoml (
                                ax_nacnt, ax_ncntr, ax_source, ax_date, ax_value, ax_tref,
                                ax_comment, ax_done, ax_fcurr, ax_fvalue, ax_fcrate, ax_fcmult, ax_fcdec,
                                ax_srcco, ax_unique, ax_project, ax_job, ax_jrnl, ax_nlpdate,
                                datecreated, datemodified, state
                            ) VALUES (
                                '{bank_account}', '    ', 'S', '{post_date}', {amount_pounds}, '{reference[:20]}',
                                '{description[:50]}', 'Y', '   ', 0, 0, 0, 0,
                                'I', '{atran_unique}', '        ', '        ', {jrnl_num}, '{post_date}',
                                '{now_str}', '{now_str}', 1
                            )
                        """
                        conn.execute(text(anoml_bank_sql))

                        # anoml Debtors control - ax_done='Y' indicates posted to nominal
                        anoml_control_sql = f"""
                            INSERT INTO anoml (
                                ax_nacnt, ax_ncntr, ax_source, ax_date, ax_value, ax_tref,
                                ax_comment, ax_done, ax_fcurr, ax_fvalue, ax_fcrate, ax_fcmult, ax_fcdec,
                                ax_srcco, ax_unique, ax_project, ax_job, ax_jrnl, ax_nlpdate,
                                datecreated, datemodified, state
                            ) VALUES (
                                '{sales_ledger_control}', '    ', 'S', '{post_date}', {-amount_pounds}, '{reference[:20]}',
                                '{description[:50]}', 'Y', '   ', 0, 0, 0, 0,
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

                # 3. Post GoCardless fees if provided - ALWAYS posted
                if gocardless_fees > 0 and fees_nominal_account:
                    fees_unique = unique_ids[-1]
                    fees_comment = "GoCardless fees"

                    # Nominal posting for fees (expense DR, bank CR)
                    if posting_decision.post_to_nominal:
                        # DR Fees expense
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
                                '{fees_nominal_account}', '    ', 'B ', 'BC', {next_journal},
                                '', '{input_by[:10]}', 'A', '{fees_comment}', '{fees_comment}',
                                '{post_date}', {abs(gocardless_fees)}, {year}, {period}, 0,
                                0, 0, '   ', 0, 0,
                                0, 0, 'I', '', '        ',
                                '        ', 'N', 0, '{fees_unique}', 0,
                                0, 0, 0, 0, 0,
                                0, '{now_str}', '{now_str}', 1
                            )
                        """
                        conn.execute(text(fees_dr_sql))
                        # Update nacnt balance for fees account (DEBIT)
                        self.update_nacnt_balance(conn, fees_nominal_account, abs(gocardless_fees), period)

                        # CR Bank (fees reduce bank receipt)
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
                                '{post_date}', {-abs(gocardless_fees)}, {year}, {period}, 0,
                                0, 0, '   ', 0, 0,
                                0, 0, 'I', '', '        ',
                                '        ', 'N', 0, '{fees_unique}', 0,
                                0, 0, 0, 0, 0,
                                0, '{now_str}', '{now_str}', 1
                            )
                        """
                        conn.execute(text(fees_cr_sql))
                        # Update nacnt balance for bank account fees (CREDIT)
                        self.update_nacnt_balance(conn, bank_account, -abs(gocardless_fees), period)
                        # Update nbank balance (GoCardless fees decrease bank balance)
                        self.update_nbank_balance(conn, bank_account, -abs(gocardless_fees))

            batch_status = "Completed" if complete_batch else "Open for review"
            logger.info(f"Successfully imported GoCardless batch: {entry_number} with {len(payments)} payments totalling £{gross_amount:.2f} - Posted to nominal and transfer file")

            return ImportResult(
                success=True,
                records_processed=len(payments),
                records_imported=len(payments),
                warnings=[
                    f"Entry number: {entry_number}",
                    f"Payments: {len(payments)}",
                    f"Gross amount: £{gross_amount:.2f}",
                    f"GoCardless fees: £{gocardless_fees:.2f}" if gocardless_fees else None,
                    f"Net amount: £{net_amount:.2f}" if gocardless_fees else None,
                    f"Batch status: {batch_status}",
                    "Posted to nominal ledger and transfer file (anoml)"
                ]
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
                            va_box3, va_box4, va_box5, va_box6, va_box7,
                            va_box8, va_box9, va_done, va_import, va_export,
                            datecreated, datemodified, state
                        ) VALUES (
                            'S', '{customer_account}', '{sales_nominal}', '{post_date}', '{post_date}',
                            '{post_date}', '{invoice_number[:20]}', 'I', 'GB', '   ',
                            {net_amount}, 0, {vat_amount}, 0, 'H',
                            'S', '{vat_code}', {vat_rate}, 1, 0,
                            0, 0, 0, 1, 0,
                            0, 0, 0, 0, 0,
                            '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(zvtran_sql))

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
                            va_box3, va_box4, va_box5, va_box6, va_box7,
                            va_box8, va_box9, va_done, va_import, va_export,
                            datecreated, datemodified, state
                        ) VALUES (
                            'P', '{supplier_account}', '{nominal_account}', '{post_date}', '{post_date}',
                            '{post_date}', '{invoice_number[:20]}', 'I', 'GB', '   ',
                            {net_amount}, 0, {vat_amount}, 0, 'H',
                            'P', '{vat_code}', {vat_rate}, 0, 0,
                            0, 1, 0, 0, 1,
                            0, 0, 0, 0, 0,
                            '{now_str}', '{now_str}', 1
                        )
                    """
                    conn.execute(text(zvtran_sql))

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


def get_opera_sql_import(sql_connector) -> OperaSQLImport:
    """Factory function to create an OperaSQLImport instance"""
    return OperaSQLImport(sql_connector)
