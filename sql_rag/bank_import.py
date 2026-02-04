"""
Bank Statement Import Module for Opera SQL SE

Processes bank statement CSV files and imports:
- Sales receipts (customer payments)
- Purchase payments (supplier payments)

Matches transactions by fuzzy name matching against customer/supplier master files.
Only imports unposted transactions.

Uses shared matching module (bank_matching.py) for fuzzy matching logic.
"""

import csv
import re
from datetime import datetime, date
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple, Any, Union

import logging
from sql_rag.sql_connector import SQLConnector
from sql_rag.opera_sql_import import OperaSQLImport, ImportResult
from sql_rag.bank_matching import BankMatcher, MatchCandidate, MatchResult

logger = logging.getLogger(__name__)


@dataclass
class BankTransaction:
    """Represents a single bank statement transaction"""
    row_number: int
    date: date
    amount: float
    subcategory: str
    memo: str
    name: str  # Extracted from memo
    reference: str  # Extracted from memo

    # Matching results
    match_type: Optional[str] = None  # 'customer', 'supplier', or None
    matched_account: Optional[str] = None
    matched_name: Optional[str] = None
    match_score: float = 0.0

    # Import status
    action: Optional[str] = None  # 'sales_receipt', 'purchase_payment', 'skip'
    skip_reason: Optional[str] = None
    imported: bool = False
    import_result: Optional[ImportResult] = None

    @property
    def is_receipt(self) -> bool:
        return self.amount > 0

    @property
    def is_payment(self) -> bool:
        return self.amount < 0

    @property
    def abs_amount(self) -> float:
        return abs(self.amount)


@dataclass
class BankImportResult:
    """Results of a bank statement import"""
    filename: str
    total_transactions: int = 0
    matched_transactions: int = 0
    imported_transactions: int = 0
    skipped_transactions: int = 0
    already_posted: int = 0
    errors: List[str] = field(default_factory=list)
    transactions: List[BankTransaction] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        if self.matched_transactions == 0:
            return 0.0
        return self.imported_transactions / self.matched_transactions * 100


class BankStatementImport:
    """
    Imports bank statements into Opera SQL SE

    Workflow:
    1. Parse CSV file
    2. Extract name and reference from memo field
    3. Match names against customers (sname) and suppliers (pname)
    4. Determine transaction type (sales receipt vs purchase payment)
    5. Check if already posted
    6. Import unposted transactions
    """

    # Names/patterns to skip (not real customer/supplier names)
    SKIP_PATTERNS = [
        r'^GC\s+C\d+',  # GoCardless references like "GC C1"
        r'^HMRC',  # Tax payments
        r'^SALARY',
        r'^UBER',
        r'^LINKEDIN',
        r'^SCREWFIX',
        r'^Amazon\.co\.uk',
        r'^EDF\s+ENERGY',
        r'^O2\s*$',
        r'^WWW\.',
    ]

    # Subcategories that are typically not customer/supplier transactions
    SKIP_SUBCATEGORIES = [
        'Direct Debit',
        'Standing Order',
        'Debit',
    ]

    def __init__(self,
                 bank_code: str = "BC010",
                 min_match_score: float = 0.6,
                 learn_threshold: float = 0.8,
                 use_aliases: bool = True,
                 use_extended_fields: bool = True):
        """
        Initialize bank statement importer

        Args:
            bank_code: Opera bank account code (default BC010)
            min_match_score: Minimum fuzzy match score (0-1) to consider a match
            learn_threshold: Minimum score (0-1) to save as alias for future (default 0.8)
            use_aliases: Enable alias lookup from learned matches (default True)
            use_extended_fields: Use payee/search keys for matching (default True)
        """
        self.bank_code = bank_code
        self.min_match_score = min_match_score
        self.learn_threshold = learn_threshold
        self.use_aliases = use_aliases
        self.use_extended_fields = use_extended_fields

        self.sql_connector = SQLConnector()
        self.opera_import = OperaSQLImport(self.sql_connector)

        # Initialize shared matcher
        self.matcher = BankMatcher(min_score=self.min_match_score)

        # Initialize alias manager if enabled
        self.alias_manager = None
        if self.use_aliases:
            try:
                from sql_rag.bank_aliases import BankAliasManager
                self.alias_manager = BankAliasManager(self.sql_connector)
            except Exception as e:
                logger.warning(f"Could not initialize alias manager: {e}")

        # Legacy dict format for backward compatibility
        self._customer_names: Dict[str, str] = {}  # account -> name
        self._supplier_names: Dict[str, str] = {}  # account -> name

        self._load_master_files()

    @staticmethod
    def get_available_bank_accounts() -> List[Dict[str, Any]]:
        """
        Get list of available bank accounts from Opera's nbank table.

        Returns:
            List of dictionaries with bank account details:
                - code: Bank account code (e.g., 'BC010')
                - description: Bank account description
                - sort_code: Bank sort code
                - account_number: Bank account number
                - balance: Current balance
                - type: Account type (Bank Account, Petty Cash, etc.)
        """
        sql = SQLConnector()
        df = sql.execute_query("""
            SELECT
                RTRIM(nk_acnt) as code,
                RTRIM(nk_desc) as description,
                RTRIM(nk_sort) as sort_code,
                RTRIM(nk_number) as account_number,
                nk_curbal as balance,
                CASE
                    WHEN nk_petty = 1 THEN 'Petty Cash'
                    ELSE 'Bank Account'
                END as type
            FROM nbank
            ORDER BY nk_acnt
        """)
        return df.to_dict('records') if not df.empty else []

    def _load_master_files(self):
        """Load customer and supplier data from Opera into the shared matcher"""
        customers: Dict[str, MatchCandidate] = {}
        suppliers: Dict[str, MatchCandidate] = {}

        # Load customers with extended fields
        if self.use_extended_fields:
            customer_query = """
                SELECT
                    sn_account,
                    RTRIM(sn_name) as name,
                    RTRIM(ISNULL(sn_key1, '')) as key1,
                    RTRIM(ISNULL(sn_key2, '')) as key2,
                    RTRIM(ISNULL(sn_key3, '')) as key3,
                    RTRIM(ISNULL(sn_key4, '')) as key4,
                    RTRIM(ISNULL(sn_bankac, '')) as bank_account,
                    RTRIM(ISNULL(sn_banksor, '')) as bank_sort,
                    RTRIM(ISNULL(sn_vendor, '')) as vendor_ref
                FROM sname
            """
        else:
            customer_query = "SELECT sn_account, RTRIM(sn_name) as name FROM sname"

        df = self.sql_connector.execute_query(customer_query)

        for _, row in df.iterrows():
            account = row['sn_account'].strip()
            name = row['name'].strip()

            # Legacy format
            self._customer_names[account] = name

            # Extended format for matcher
            if self.use_extended_fields:
                search_keys = [
                    row.get('key1', ''), row.get('key2', ''),
                    row.get('key3', ''), row.get('key4', '')
                ]
                search_keys = [k for k in search_keys if k]  # Remove empty

                customers[account] = MatchCandidate(
                    account=account,
                    primary_name=name,
                    search_keys=search_keys,
                    bank_account=row.get('bank_account', ''),
                    bank_sort=row.get('bank_sort', ''),
                    vendor_ref=row.get('vendor_ref', '')
                )
            else:
                customers[account] = MatchCandidate(
                    account=account,
                    primary_name=name
                )

        # Load suppliers with extended fields
        if self.use_extended_fields:
            supplier_query = """
                SELECT
                    pn_account,
                    RTRIM(pn_name) as name,
                    RTRIM(ISNULL(pn_payee, '')) as payee,
                    RTRIM(ISNULL(pn_key1, '')) as key1,
                    RTRIM(ISNULL(pn_key2, '')) as key2,
                    RTRIM(ISNULL(pn_key3, '')) as key3,
                    RTRIM(ISNULL(pn_key4, '')) as key4,
                    RTRIM(ISNULL(pn_bankac, '')) as bank_account,
                    RTRIM(ISNULL(pn_banksor, '')) as bank_sort
                FROM pname
            """
        else:
            supplier_query = "SELECT pn_account, RTRIM(pn_name) as name FROM pname"

        df = self.sql_connector.execute_query(supplier_query)

        for _, row in df.iterrows():
            account = row['pn_account'].strip()
            name = row['name'].strip()

            # Legacy format
            self._supplier_names[account] = name

            # Extended format for matcher
            if self.use_extended_fields:
                search_keys = [
                    row.get('key1', ''), row.get('key2', ''),
                    row.get('key3', ''), row.get('key4', '')
                ]
                search_keys = [k for k in search_keys if k]  # Remove empty

                suppliers[account] = MatchCandidate(
                    account=account,
                    primary_name=name,
                    payee_name=row.get('payee', ''),
                    search_keys=search_keys,
                    bank_account=row.get('bank_account', ''),
                    bank_sort=row.get('bank_sort', '')
                )
            else:
                suppliers[account] = MatchCandidate(
                    account=account,
                    primary_name=name
                )

        # Load into shared matcher
        self.matcher.load_customers(customers)
        self.matcher.load_suppliers(suppliers)

    def _parse_memo(self, memo: str) -> Tuple[str, str]:
        """
        Parse memo field to extract name and reference

        Memo format: "NAME                 \tREFERENCE"

        Returns:
            Tuple of (name, reference)
        """
        if not memo:
            return "", ""

        # Split by tab
        parts = memo.split('\t')
        name = parts[0].strip() if parts else ""
        reference = parts[1].strip() if len(parts) > 1 else ""

        return name, reference

    def _should_skip(self, name: str, subcategory: str) -> Optional[str]:
        """
        Check if transaction should be skipped

        Returns:
            Skip reason if should skip, None otherwise
        """
        # Check subcategory
        if subcategory in self.SKIP_SUBCATEGORIES:
            return f"Subcategory '{subcategory}' excluded"

        # Check name patterns
        for pattern in self.SKIP_PATTERNS:
            if re.match(pattern, name, re.IGNORECASE):
                return f"Name matches skip pattern: {pattern}"

        # Skip empty names
        if not name:
            return "Empty name"

        return None

    def _match_transaction(self, txn: BankTransaction) -> None:
        """
        Match transaction to customer or supplier.

        Matching strategy:
        1. Check alias table first (fast path for previously seen names)
        2. Fuzzy match using shared matcher
        3. Save successful matches as aliases for future

        Updates transaction with match results
        """
        # Determine expected ledger type based on transaction direction
        expected_type = 'C' if txn.is_receipt else 'S'

        # Step 1: Check alias table first (fast path)
        if self.alias_manager:
            alias_account = self.alias_manager.lookup_alias(txn.name, expected_type)
            if alias_account:
                # Found alias - use it directly
                if expected_type == 'C' and alias_account in self.matcher.customers:
                    candidate = self.matcher.customers[alias_account]
                    txn.match_type = 'customer'
                    txn.matched_account = alias_account
                    txn.matched_name = candidate.primary_name
                    txn.match_score = 1.0  # Perfect match from alias
                    txn.action = 'sales_receipt'
                    logger.debug(f"Alias match: '{txn.name}' -> {alias_account} (customer)")
                    return
                elif expected_type == 'S' and alias_account in self.matcher.suppliers:
                    candidate = self.matcher.suppliers[alias_account]
                    txn.match_type = 'supplier'
                    txn.matched_account = alias_account
                    txn.matched_name = candidate.primary_name
                    txn.match_score = 1.0  # Perfect match from alias
                    txn.action = 'purchase_payment'
                    logger.debug(f"Alias match: '{txn.name}' -> {alias_account} (supplier)")
                    return

        # Step 2: Fuzzy match using shared matcher
        cust_result = self.matcher.match_customer(txn.name)
        supp_result = self.matcher.match_supplier(txn.name)

        # Skip if name matches both customer AND supplier above threshold
        # (could be same entity in both ledgers - ambiguous)
        if cust_result.is_match and supp_result.is_match:
            txn.action = 'skip'
            txn.skip_reason = f'Matches both customer ({cust_result.name}) and supplier ({supp_result.name}) - ambiguous'
            return

        # Determine best match based on transaction direction
        if txn.is_receipt:
            # Receipt: must be customer match (sales receipt)
            if cust_result.is_match:
                txn.match_type = 'customer'
                txn.matched_account = cust_result.account
                txn.matched_name = cust_result.name
                txn.match_score = cust_result.score
                txn.action = 'sales_receipt'

                # Step 3: Save alias if score is high enough
                if self.alias_manager and cust_result.score >= self.learn_threshold:
                    self.alias_manager.save_alias(
                        bank_name=txn.name,
                        ledger_type='C',
                        account_code=cust_result.account,
                        match_score=cust_result.score,
                        account_name=cust_result.name
                    )
            else:
                txn.action = 'skip'
                txn.skip_reason = f'No customer match found (best score: {cust_result.score:.2f})'
        else:
            # Payment: must be supplier match (purchase payment)
            if supp_result.is_match:
                txn.match_type = 'supplier'
                txn.matched_account = supp_result.account
                txn.matched_name = supp_result.name
                txn.match_score = supp_result.score
                txn.action = 'purchase_payment'

                # Step 3: Save alias if score is high enough
                if self.alias_manager and supp_result.score >= self.learn_threshold:
                    self.alias_manager.save_alias(
                        bank_name=txn.name,
                        ledger_type='S',
                        account_code=supp_result.account,
                        match_score=supp_result.score,
                        account_name=supp_result.name
                    )
            else:
                txn.action = 'skip'
                txn.skip_reason = f'No supplier match found (best score: {supp_result.score:.2f})'

    def _is_already_posted(self, txn: BankTransaction) -> bool:
        """
        Check if transaction has already been posted to Opera

        Checks atran (cashbook) for matching date, amount, and reference
        Note: atran stores amounts in PENCE, so multiply by 100
        Note: Payments have NEGATIVE at_value, receipts have POSITIVE at_value
              We compare absolute values to handle both cases
        """
        # Check cashbook for existing transaction
        # Use ABS(at_value) because payments are stored as negative values
        amount_pence = int(txn.abs_amount * 100)
        query = f"""
            SELECT COUNT(*) as cnt FROM atran
            WHERE at_acnt = '{self.bank_code}'
            AND at_pstdate = '{txn.date.strftime('%Y-%m-%d')}'
            AND ABS(ABS(at_value) - {amount_pence}) < 1
        """
        df = self.sql_connector.execute_query(query)

        return df.iloc[0]['cnt'] > 0 if len(df) > 0 else False

    def parse_csv(self, filepath: str) -> List[BankTransaction]:
        """
        Parse bank statement CSV file

        Args:
            filepath: Path to CSV file

        Returns:
            List of BankTransaction objects
        """
        transactions = []

        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is row 1)
                # Skip empty rows
                if not row.get('Date') or not row.get('Amount'):
                    continue

                # Parse date (DD/MM/YYYY format)
                try:
                    txn_date = datetime.strptime(row['Date'].strip(), '%d/%m/%Y').date()
                except ValueError:
                    continue

                # Parse amount
                try:
                    amount = float(row['Amount'].replace(',', ''))
                except ValueError:
                    continue

                # Parse memo
                memo = row.get('Memo', '').strip()
                name, reference = self._parse_memo(memo)

                txn = BankTransaction(
                    row_number=row_num,
                    date=txn_date,
                    amount=amount,
                    subcategory=row.get('Subcategory', '').strip(),
                    memo=memo,
                    name=name,
                    reference=reference
                )

                transactions.append(txn)

        return transactions

    def process_transactions(self, transactions: List[BankTransaction],
                            check_posted: bool = True) -> None:
        """
        Process transactions: skip checks, matching, and posted checks

        Args:
            transactions: List of transactions to process
            check_posted: Whether to check if already posted
        """
        for txn in transactions:
            # Check if should skip
            skip_reason = self._should_skip(txn.name, txn.subcategory)
            if skip_reason:
                txn.action = 'skip'
                txn.skip_reason = skip_reason
                continue

            # Match to customer/supplier
            self._match_transaction(txn)

            # Check if already posted
            if check_posted and txn.action in ('sales_receipt', 'purchase_payment'):
                if self._is_already_posted(txn):
                    txn.action = 'skip'
                    txn.skip_reason = 'Already posted'

    def import_transaction(self, txn: BankTransaction, validate_only: bool = False) -> ImportResult:
        """
        Import a single transaction to Opera

        Args:
            txn: Transaction to import
            validate_only: If True, only validate without posting

        Returns:
            ImportResult
        """
        if txn.action == 'sales_receipt':
            # Import as sales receipt
            # Use subcategory as payment method (e.g., 'Funds Transfer', 'Counter Credit')
            payment_method = txn.subcategory if txn.subcategory else 'BACS'
            logger.info(f"BANK_IMPORT_DEBUG: Importing SALES_RECEIPT - account={txn.matched_account}, "
                       f"amount={txn.amount}, abs_amount={txn.abs_amount}, name={txn.matched_name}")
            result = self.opera_import.import_sales_receipt(
                bank_account=self.bank_code,
                customer_account=txn.matched_account,
                amount_pounds=txn.abs_amount,
                reference=txn.reference,
                post_date=txn.date,
                input_by='BANK_IMPORT',
                payment_method=payment_method[:20],
                validate_only=validate_only
            )
        elif txn.action == 'purchase_payment':
            # Import as purchase payment
            logger.info(f"BANK_IMPORT_DEBUG: Importing PURCHASE_PAYMENT - account={txn.matched_account}, "
                       f"amount={txn.amount}, abs_amount={txn.abs_amount}, name={txn.matched_name}")
            result = self.opera_import.import_purchase_payment(
                bank_account=self.bank_code,
                supplier_account=txn.matched_account,
                amount_pounds=txn.abs_amount,
                reference=txn.reference,
                post_date=txn.date,
                input_by='BANK_IMPORT',
                validate_only=validate_only
            )
        else:
            result = ImportResult(
                success=False,
                message=f"Cannot import: {txn.skip_reason or 'Unknown action'}"
            )

        txn.import_result = result
        txn.imported = result.success
        return result

    def import_file(self, filepath: str, validate_only: bool = False) -> BankImportResult:
        """
        Import a bank statement CSV file

        Args:
            filepath: Path to CSV file
            validate_only: If True, only validate without posting

        Returns:
            BankImportResult with details of import
        """
        result = BankImportResult(filename=filepath)

        # Parse CSV
        try:
            transactions = self.parse_csv(filepath)
            result.total_transactions = len(transactions)
            result.transactions = transactions
        except Exception as e:
            result.errors.append(f"Error parsing CSV: {str(e)}")
            return result

        # Process transactions
        self.process_transactions(transactions)

        # Count matched
        for txn in transactions:
            if txn.action in ('sales_receipt', 'purchase_payment'):
                result.matched_transactions += 1
            elif txn.skip_reason == 'Already posted':
                result.already_posted += 1

        # Import matched transactions
        for txn in transactions:
            if txn.action in ('sales_receipt', 'purchase_payment'):
                try:
                    import_result = self.import_transaction(txn, validate_only)
                    if import_result.success:
                        result.imported_transactions += 1
                    else:
                        result.errors.append(
                            f"Row {txn.row_number}: {import_result.message}"
                        )
                except Exception as e:
                    result.errors.append(f"Row {txn.row_number}: {str(e)}")

        result.skipped_transactions = (
            result.total_transactions -
            result.matched_transactions -
            result.already_posted
        )

        return result

    def preview_file(self, filepath: str) -> BankImportResult:
        """
        Preview what would be imported without actually importing

        Args:
            filepath: Path to CSV file

        Returns:
            BankImportResult with preview details
        """
        result = BankImportResult(filename=filepath)

        # Parse and process
        try:
            transactions = self.parse_csv(filepath)
            result.total_transactions = len(transactions)
            result.transactions = transactions
        except Exception as e:
            result.errors.append(f"Error parsing CSV: {str(e)}")
            return result

        self.process_transactions(transactions)

        # Count by action
        for txn in transactions:
            if txn.action in ('sales_receipt', 'purchase_payment'):
                result.matched_transactions += 1
            elif txn.skip_reason == 'Already posted':
                result.already_posted += 1

        result.skipped_transactions = (
            result.total_transactions -
            result.matched_transactions -
            result.already_posted
        )

        return result

    def get_preview_summary(self, result: BankImportResult) -> str:
        """
        Generate a human-readable preview summary

        Args:
            result: BankImportResult from preview_file()

        Returns:
            Formatted summary string
        """
        lines = [
            f"Bank Statement Import Preview: {result.filename}",
            f"=" * 60,
            f"Total transactions: {result.total_transactions}",
            f"To import: {result.matched_transactions}",
            f"Already posted: {result.already_posted}",
            f"Skipped: {result.skipped_transactions}",
            "",
            "Transactions to import:",
            "-" * 60
        ]

        for txn in result.transactions:
            if txn.action in ('sales_receipt', 'purchase_payment'):
                action = "RECEIPT" if txn.action == 'sales_receipt' else "PAYMENT"
                lines.append(
                    f"  {txn.date} | {action:8} | {txn.abs_amount:>10,.2f} | "
                    f"{txn.matched_account} - {txn.matched_name} ({txn.match_score:.0%})"
                )

        if result.skipped_transactions > 0:
            lines.extend([
                "",
                "Skipped transactions:",
                "-" * 60
            ])
            for txn in result.transactions:
                if txn.action == 'skip' and txn.skip_reason != 'Already posted':
                    lines.append(
                        f"  {txn.date} | {txn.name[:25]:25} | {txn.amount:>10,.2f} | "
                        f"{txn.skip_reason}"
                    )

        return "\n".join(lines)

    def generate_audit_report(self, result: BankImportResult) -> Dict[str, Any]:
        """
        Generate a comprehensive audit report of import results.

        Returns a dictionary with:
        - summary: Overall statistics
        - imported: List of successfully imported transactions
        - not_imported: List of transactions not imported with reasons
        - by_category: Breakdown by skip reason category

        Args:
            result: BankImportResult from import_file() or preview_file()

        Returns:
            Dictionary containing audit report data
        """
        # Categorize transactions
        imported = []
        not_imported = []
        by_reason = {}

        for txn in result.transactions:
            txn_data = {
                "row": txn.row_number,
                "date": txn.date.isoformat(),
                "amount": txn.amount,
                "abs_amount": txn.abs_amount,
                "type": "Receipt" if txn.is_receipt else "Payment",
                "memo_name": txn.name,
                "memo_reference": txn.reference,
                "subcategory": txn.subcategory
            }

            # Check if transaction was/will be imported
            is_importable = txn.action in ('sales_receipt', 'purchase_payment')

            if txn.imported or is_importable:
                txn_data.update({
                    "action": txn.action,
                    "matched_account": txn.matched_account,
                    "matched_name": txn.matched_name,
                    "match_score": round(txn.match_score * 100) if txn.match_score else 0
                })
                imported.append(txn_data)
            else:
                # Categorize the skip reason
                reason = txn.skip_reason or "Unknown"

                # Simplify reason for categorization
                if "Subcategory" in reason:
                    category = "Excluded subcategory"
                elif "skip pattern" in reason:
                    category = "Excluded pattern"
                elif "No customer match" in reason:
                    category = "No customer match"
                elif "No supplier match" in reason:
                    category = "No supplier match"
                elif "Already posted" in reason:
                    category = "Already posted"
                elif "ambiguous" in reason:
                    category = "Ambiguous (both ledgers)"
                else:
                    category = "Other"

                txn_data.update({
                    "reason": reason,
                    "category": category,
                    "matched_account": txn.matched_account,
                    "matched_name": txn.matched_name,
                    "match_score": round(txn.match_score * 100) if txn.match_score else 0
                })
                not_imported.append(txn_data)

                # Count by category
                if category not in by_reason:
                    by_reason[category] = {"count": 0, "total_amount": 0}
                by_reason[category]["count"] += 1
                by_reason[category]["total_amount"] += txn.abs_amount

        # Calculate totals
        total_imported_amount = sum(t["abs_amount"] for t in imported)
        total_not_imported_amount = sum(t["abs_amount"] for t in not_imported)

        return {
            "summary": {
                "filename": result.filename,
                "total_transactions": result.total_transactions,
                "imported_count": len(imported),
                "imported_amount": round(total_imported_amount, 2),
                "not_imported_count": len(not_imported),
                "not_imported_amount": round(total_not_imported_amount, 2),
                "success_rate": round(len(imported) / result.total_transactions * 100, 1) if result.total_transactions > 0 else 0
            },
            "imported": imported,
            "not_imported": not_imported,
            "by_category": by_reason,
            "errors": result.errors
        }

    def get_audit_report_text(self, result: BankImportResult) -> str:
        """
        Generate a human-readable audit report.

        Args:
            result: BankImportResult from import_file() or preview_file()

        Returns:
            Formatted text report
        """
        report = self.generate_audit_report(result)
        summary = report["summary"]

        lines = [
            "=" * 80,
            "BANK STATEMENT IMPORT AUDIT REPORT",
            "=" * 80,
            f"File: {summary['filename']}",
            f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "SUMMARY",
            "-" * 40,
            f"Total transactions:     {summary['total_transactions']:>8}",
            f"Imported:               {summary['imported_count']:>8}  (£{summary['imported_amount']:>12,.2f})",
            f"Not imported:           {summary['not_imported_count']:>8}  (£{summary['not_imported_amount']:>12,.2f})",
            f"Success rate:           {summary['success_rate']:>7.1f}%",
            "",
        ]

        # Breakdown by category
        if report["by_category"]:
            lines.extend([
                "NOT IMPORTED - BY CATEGORY",
                "-" * 40
            ])
            for category, data in sorted(report["by_category"].items(), key=lambda x: -x[1]["count"]):
                lines.append(f"  {category:30} {data['count']:>5}  (£{data['total_amount']:>10,.2f})")
            lines.append("")

        # Imported transactions
        if report["imported"]:
            lines.extend([
                "IMPORTED TRANSACTIONS",
                "-" * 80,
                f"{'Date':<12} {'Type':<8} {'Amount':>12} {'Account':<10} {'Name':<25} {'Match%':>6}",
                "-" * 80
            ])
            for txn in report["imported"]:
                lines.append(
                    f"{txn['date']:<12} {txn['type']:<8} {txn['abs_amount']:>12,.2f} "
                    f"{txn['matched_account']:<10} {txn['matched_name'][:25]:<25} {txn['match_score']:>5}%"
                )
            lines.append("")

        # Not imported transactions
        if report["not_imported"]:
            lines.extend([
                "NOT IMPORTED TRANSACTIONS",
                "-" * 100,
                f"{'Date':<12} {'Type':<8} {'Amount':>12} {'Name':<25} {'Reason':<40}",
                "-" * 100
            ])
            for txn in report["not_imported"]:
                reason = txn['reason'][:40] if len(txn['reason']) > 40 else txn['reason']
                lines.append(
                    f"{txn['date']:<12} {txn['type']:<8} {txn['amount']:>12,.2f} "
                    f"{txn['memo_name'][:25]:<25} {reason:<40}"
                )
            lines.append("")

        # Errors
        if report["errors"]:
            lines.extend([
                "ERRORS",
                "-" * 40
            ])
            for error in report["errors"]:
                lines.append(f"  {error}")
            lines.append("")

        lines.append("=" * 80)
        lines.append("END OF REPORT")
        lines.append("=" * 80)

        return "\n".join(lines)

    def export_audit_to_csv(self, result: BankImportResult, filepath: str) -> str:
        """
        Export audit report to CSV file for Excel.

        Creates a CSV with all transactions showing:
        - Status (Imported/Not Imported)
        - Date, Amount, Type
        - Matched account and name (if any)
        - Match score
        - Reason (if not imported)

        Args:
            result: BankImportResult from import_file() or preview_file()
            filepath: Path to save CSV file

        Returns:
            Path to saved file
        """
        report = self.generate_audit_report(result)

        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            # Write header
            writer.writerow([
                'Status', 'Row', 'Date', 'Type', 'Amount', 'Subcategory',
                'Memo Name', 'Memo Reference', 'Matched Account', 'Matched Name',
                'Match Score %', 'Category', 'Reason'
            ])

            # Write imported transactions
            for txn in report["imported"]:
                writer.writerow([
                    'Imported',
                    txn['row'],
                    txn['date'],
                    txn['type'],
                    txn['amount'],
                    txn['subcategory'],
                    txn['memo_name'],
                    txn['memo_reference'],
                    txn['matched_account'] or '',
                    txn['matched_name'] or '',
                    txn['match_score'],
                    txn['action'],
                    ''
                ])

            # Write not imported transactions
            for txn in report["not_imported"]:
                writer.writerow([
                    'Not Imported',
                    txn['row'],
                    txn['date'],
                    txn['type'],
                    txn['amount'],
                    txn['subcategory'],
                    txn['memo_name'],
                    txn['memo_reference'],
                    txn['matched_account'] or '',
                    txn['matched_name'] or '',
                    txn['match_score'],
                    txn['category'],
                    txn['reason']
                ])

        return filepath

    def save_audit_report(self, result: BankImportResult, base_path: str) -> Dict[str, str]:
        """
        Save audit report in multiple formats.

        Args:
            result: BankImportResult from import_file() or preview_file()
            base_path: Base path without extension (e.g., '/path/to/report')

        Returns:
            Dictionary with paths to saved files
        """
        import json

        paths = {}

        # Save text report
        txt_path = f"{base_path}.txt"
        with open(txt_path, 'w') as f:
            f.write(self.get_audit_report_text(result))
        paths['text'] = txt_path

        # Save CSV report
        csv_path = f"{base_path}.csv"
        self.export_audit_to_csv(result, csv_path)
        paths['csv'] = csv_path

        # Save JSON report
        json_path = f"{base_path}.json"
        with open(json_path, 'w') as f:
            json.dump(self.generate_audit_report(result), f, indent=2)
        paths['json'] = json_path

        return paths
