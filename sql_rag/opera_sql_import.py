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
        sales_ledger_control: str = "BB020",
        validate_only: bool = False
    ) -> ImportResult:
        """
        Import a sales receipt into Opera SQL SE.

        This replicates the EXACT pattern Opera uses when a user manually
        enters a sales receipt, creating records in:
        1. aentry (Cashbook Entry Header)
        2. atran (Cashbook Transaction)
        3. ntran (Nominal Ledger - 2 rows for double-entry)

        Args:
            bank_account: Bank account code (e.g., 'BC010')
            customer_account: Customer account code (e.g., 'A046')
            amount_pounds: Receipt amount in POUNDS (e.g., 100.00)
            reference: Your reference (e.g., 'inv12345')
            post_date: Posting date
            input_by: User code for audit trail (max 8 chars)
            sales_ledger_control: Sales ledger control account (default 'BB020')
            validate_only: If True, only validate without inserting

        Returns:
            ImportResult with details of the operation
        """
        errors = []
        warnings = []

        try:
            # =====================
            # VALIDATION
            # =====================

            # Validate bank account exists by checking if it's been used in atran before
            bank_check = self.sql.execute_query(f"""
                SELECT TOP 1 at_acnt FROM atran
                WHERE RTRIM(at_acnt) = '{bank_account}'
            """)
            if bank_check.empty:
                warnings.append(f"Bank account '{bank_account}' has not been used before - verify it's correct")

            # Validate customer exists by checking sname (Sales Ledger Master) first
            # This is the authoritative source for customer names
            sname_check = self.sql.execute_query(f"""
                SELECT sn_name FROM sname
                WHERE RTRIM(sn_account) = '{customer_account}'
            """)
            if not sname_check.empty:
                customer_name = sname_check.iloc[0]['sn_name'].strip()
            else:
                # Fall back to atran history if not in sname
                customer_check = self.sql.execute_query(f"""
                    SELECT TOP 1 at_account, at_name FROM atran
                    WHERE RTRIM(at_account) = '{customer_account}'
                """)
                if not customer_check.empty:
                    customer_name = customer_check.iloc[0]['at_name'].strip()
                else:
                    errors.append(f"Customer account '{customer_account}' not found")

            # Validate nominal accounts exist by checking ntran
            bank_nominal_check = self.sql.execute_query(f"""
                SELECT TOP 1 nt_acnt FROM ntran
                WHERE RTRIM(nt_acnt) = '{bank_account}'
            """)
            if bank_nominal_check.empty:
                warnings.append(f"Bank nominal account '{bank_account}' has not been used before - verify it's correct")

            control_check = self.sql.execute_query(f"""
                SELECT TOP 1 nt_acnt FROM ntran
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
            # GENERATE SEQUENCE NUMBERS
            # =====================

            # Get next entry number (R2 type)
            entry_num_result = self.sql.execute_query("""
                SELECT ISNULL(MAX(CAST(SUBSTRING(ae_entry, 3, 10) AS INT)), 0) + 1 as next_num
                FROM aentry
                WHERE ae_cbtype = 'R2'
            """)
            next_entry_num = entry_num_result.iloc[0]['next_num']
            entry_number = f"R2{next_entry_num:08d}"

            # Get next journal number
            journal_result = self.sql.execute_query("""
                SELECT ISNULL(MAX(nt_jrnl), 0) + 1 as next_journal
                FROM ntran
            """)
            next_journal = journal_result.iloc[0]['next_journal']

            # Generate unique IDs (Opera's format)
            unique_ids = OperaUniqueIdGenerator.generate_multiple(4)
            aentry_id = unique_ids[0]  # Not used directly but for reference
            atran_unique = unique_ids[1]
            ntran_pstid_debit = unique_ids[2]
            ntran_pstid_credit = unique_ids[3]

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

            # =====================
            # INSERT RECORDS (in Opera's order)
            # =====================

            # Get current timestamp for created/modified
            now = datetime.now()
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')
            date_str = now.strftime('%Y-%m-%d')
            time_str = now.strftime('%H:%M:%S')

            # 1. INSERT INTO aentry (Cashbook Entry Header)
            aentry_sql = f"""
                INSERT INTO aentry (
                    ae_acnt, ae_cntr, ae_cbtype, ae_entry, ae_reclnum,
                    ae_lstdate, ae_frstat, ae_tostat, ae_statln, ae_entref,
                    ae_value, ae_recbal, ae_remove, ae_tmpstat, ae_complet,
                    ae_postgrp, sq_crdate, sq_crtime, sq_cruser, ae_comment,
                    ae_payid, ae_batchid, ae_brwptr, datecreated, datemodified, state
                ) VALUES (
                    '{bank_account}', '    ', 'R2', '{entry_number}', 0,
                    '{post_date}', 0, 0, 0, '{reference[:20]}',
                    {amount_pence}, 0, 0, 0, 1,
                    0, '{date_str}', '{time_str[:8]}', '{input_by[:8]}', '',
                    0, 0, '  ', '{now_str}', '{now_str}', 1
                )
            """

            # 2. INSERT INTO atran (Cashbook Transaction)
            # Build transaction reference like Opera does
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
                    '{bank_account}', '    ', 'R2', '{entry_number}', '{input_by[:8]}',
                    4, '{post_date}', '{post_date}', 1, {amount_pence},
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

            # 3. INSERT INTO ntran - DEBIT (Bank Account +amount)
            ntran_comment = f"{reference[:50]:<50}"
            ntran_trnref = f"{customer_name[:30]:<30}BACS       (RT)     "

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

            # 4. INSERT INTO ntran - CREDIT (Sales Ledger Control -amount)
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

            # Execute all inserts in order
            with self.sql.engine.begin() as conn:
                conn.execute(text(aentry_sql))
                conn.execute(text(atran_sql))
                conn.execute(text(ntran_debit_sql))
                conn.execute(text(ntran_credit_sql))

            logger.info(f"Successfully imported sales receipt: {entry_number} for £{amount_pounds:.2f}")

            return ImportResult(
                success=True,
                records_processed=1,
                records_imported=1,
                warnings=[
                    f"Entry number: {entry_number}",
                    f"Journal number: {next_journal}",
                    f"Amount: £{amount_pounds:.2f}"
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
        creditors_control: str = "CA030",
        payment_type: str = "Direct Cr",
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

        Args:
            bank_account: Bank account code (e.g., 'BC010')
            supplier_account: Supplier account code (e.g., 'W034')
            amount_pounds: Payment amount in POUNDS (e.g., 100.00)
            reference: Your reference (e.g., 'test')
            post_date: Posting date
            input_by: User code for audit trail (max 8 chars)
            creditors_control: Creditors control account (default 'CA030')
            payment_type: Payment type description (default 'Direct Cr')
            validate_only: If True, only validate without inserting

        Returns:
            ImportResult with details of the operation
        """
        errors = []
        warnings = []

        try:
            # =====================
            # VALIDATION
            # =====================

            # Validate bank account exists
            bank_check = self.sql.execute_query(f"""
                SELECT TOP 1 at_acnt FROM atran
                WHERE RTRIM(at_acnt) = '{bank_account}'
            """)
            if bank_check.empty:
                warnings.append(f"Bank account '{bank_account}' has not been used before - verify it's correct")

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
            # GENERATE SEQUENCE NUMBERS
            # =====================

            # Get next entry number (P5 type for payments)
            entry_num_result = self.sql.execute_query("""
                SELECT ISNULL(MAX(CAST(SUBSTRING(ae_entry, 3, 10) AS INT)), 0) + 1 as next_num
                FROM aentry
                WHERE ae_cbtype = 'P5'
            """)
            next_entry_num = entry_num_result.iloc[0]['next_num']
            entry_number = f"P5{next_entry_num:08d}"

            # Get next journal number
            journal_result = self.sql.execute_query("""
                SELECT ISNULL(MAX(nt_jrnl), 0) + 1 as next_journal
                FROM ntran
            """)
            next_journal = journal_result.iloc[0]['next_journal']

            # Generate unique IDs (Opera uses same unique ID for atran and ptran)
            unique_ids = OperaUniqueIdGenerator.generate_multiple(3)
            atran_unique = unique_ids[0]  # Shared between atran and ptran
            ntran_pstid_bank = unique_ids[1]
            ntran_pstid_control = unique_ids[2]

            # =====================
            # CONVERT AMOUNTS
            # =====================
            amount_pence = int(amount_pounds * 100)

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

            # 1. INSERT INTO aentry (Cashbook Entry Header) - NEGATIVE for payment
            aentry_sql = f"""
                INSERT INTO aentry (
                    ae_acnt, ae_cntr, ae_cbtype, ae_entry, ae_reclnum,
                    ae_lstdate, ae_frstat, ae_tostat, ae_statln, ae_entref,
                    ae_value, ae_recbal, ae_remove, ae_tmpstat, ae_complet,
                    ae_postgrp, sq_crdate, sq_crtime, sq_cruser, ae_comment,
                    ae_payid, ae_batchid, ae_brwptr, datecreated, datemodified, state
                ) VALUES (
                    '{bank_account}', '    ', 'P5', '{entry_number}', 0,
                    '{post_date}', 0, 0, 0, '{reference[:20]}',
                    {-amount_pence}, 0, 0, 0, 1,
                    0, '{date_str}', '{time_str[:8]}', '{input_by[:8]}', '',
                    0, 0, '  ', '{now_str}', '{now_str}', 1
                )
            """

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
                    '{bank_account}', '    ', 'P5', '{entry_number}', '{input_by[:8]}',
                    5, '{post_date}', '{post_date}', 1, {-amount_pence},
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

            # 3. INSERT INTO ntran - CREDIT Bank (money going out)
            # nt_type='B ', nt_subt='BC', nt_posttyp='P'

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

            # 4. INSERT INTO ntran - DEBIT Creditors Control (CA030)
            # nt_type='C ', nt_subt='CA', nt_posttyp='P'
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
                    'P5', '{entry_number}', '{atran_unique}', '   ', 0,
                    0, '   ', ' ', 0, '{post_date}',
                    0, 0, 0, 0, 0,
                    '{now_str}', '{now_str}', 1
                )
            """

            # Execute first batch and get ptran ID for palloc
            with self.sql.engine.begin() as conn:
                conn.execute(text(aentry_sql))
                conn.execute(text(atran_sql))
                conn.execute(text(ntran_bank_sql))
                conn.execute(text(ntran_control_sql))
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

            logger.info(f"Successfully imported purchase payment: {entry_number} for £{amount_pounds:.2f}")

            return ImportResult(
                success=True,
                records_processed=1,
                records_imported=1,
                warnings=[
                    f"Entry number: {entry_number}",
                    f"Journal number: {next_journal}",
                    f"Amount: £{amount_pounds:.2f}",
                    f"Tables updated: aentry, atran, ntran (2), ptran, palloc"
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
        debtors_control: str = "BB020",
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
            debtors_control: Debtors control account (default 'BB020')
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

        try:
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
            # GENERATE SEQUENCE NUMBERS
            # =====================

            journal_result = self.sql.execute_query("""
                SELECT ISNULL(MAX(nt_jrnl), 0) + 1 as next_journal
                FROM ntran
            """)
            next_journal = journal_result.iloc[0]['next_journal']

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

            # =====================
            # INSERT RECORDS (in Opera's order)
            # =====================

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

            # 2. INSERT INTO ntran - CREDIT VAT (nt_type='C ', nt_subt='CA')
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

            # 3. INSERT INTO ntran - CREDIT Sales (nt_type='E ', nt_subt from account)
            # Get the account type from the sales nominal
            sales_subt = sales_nominal[:2] if len(sales_nominal) >= 2 else 'E4'
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

            # Execute all inserts in order
            with self.sql.engine.begin() as conn:
                conn.execute(text(stran_sql))
                if vat_amount > 0:
                    conn.execute(text(ntran_vat_sql))
                conn.execute(text(ntran_sales_sql))
                conn.execute(text(ntran_control_sql))
                conn.execute(text(nhist_sql))

            logger.info(f"Successfully imported sales invoice: {invoice_number} for £{gross_amount:.2f}")

            return ImportResult(
                success=True,
                records_processed=1,
                records_imported=1,
                warnings=[
                    f"Invoice: {invoice_number}",
                    f"Journal number: {next_journal}",
                    f"Gross: £{gross_amount:.2f} (Net: £{net_amount:.2f} + VAT: £{vat_amount:.2f})",
                    f"Tables updated: stran, ntran (3), nhist"
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
        purchase_ledger_control: str = "BB010",
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
            purchase_ledger_control: Purchase ledger control (default 'BB010')
            input_by: User code for audit trail
            description: Invoice description
            validate_only: If True, only validate without inserting

        Returns:
            ImportResult with details of the operation
        """
        errors = []
        warnings = []
        gross_amount = net_amount + vat_amount

        try:
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
            # GENERATE SEQUENCE NUMBERS
            # =====================

            journal_result = self.sql.execute_query("""
                SELECT ISNULL(MAX(nt_jrnl), 0) + 1 as next_journal
                FROM ntran
            """)
            next_journal = journal_result.iloc[0]['next_journal']

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

            # 3. DEBIT VAT Account (VAT reclaimable)
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

            # Execute all inserts
            with self.sql.engine.begin() as conn:
                conn.execute(text(ntran_control_sql))
                conn.execute(text(ntran_expense_sql))
                if vat_amount > 0:
                    conn.execute(text(ntran_vat_sql))

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
            # GENERATE SEQUENCE NUMBERS
            # =====================

            journal_result = self.sql.execute_query("""
                SELECT ISNULL(MAX(nt_jrnl), 0) + 1 as next_journal
                FROM ntran
            """)
            next_journal = journal_result.iloc[0]['next_journal']

            if isinstance(post_date, str):
                post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

            year = post_date.year
            period = post_date.month

            now = datetime.now()
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')

            # Build SQL for each line
            sql_statements = []
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
                sql_statements.append(sql)

            # Execute all inserts
            with self.sql.engine.begin() as conn:
                for sql in sql_statements:
                    conn.execute(text(sql))

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


def get_opera_sql_import(sql_connector) -> OperaSQLImport:
    """Factory function to create an OperaSQLImport instance"""
    return OperaSQLImport(sql_connector)
