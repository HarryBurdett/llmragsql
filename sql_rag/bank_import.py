"""
Bank Statement Import Module for Opera SQL SE

Processes bank statement CSV files and imports:
- Sales receipts (customer payments)
- Purchase payments (supplier payments)

Matches transactions by fuzzy name matching against customer/supplier master files.
Only imports unposted transactions.
"""

import csv
import re
from datetime import datetime, date
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple
from difflib import SequenceMatcher

from sql_rag.sql_connector import SQLConnector
from sql_rag.opera_sql_import import OperaSQLImport, ImportResult


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

    def __init__(self, bank_code: str = "BC010", min_match_score: float = 0.6):
        """
        Initialize bank statement importer

        Args:
            bank_code: Opera bank account code (default BC010)
            min_match_score: Minimum fuzzy match score (0-1) to consider a match
        """
        self.bank_code = bank_code
        self.min_match_score = min_match_score
        self.sql_connector = SQLConnector()
        self.opera_import = OperaSQLImport(self.sql_connector)

        # Cache customer and supplier names
        self._customers: Dict[str, str] = {}  # account -> name
        self._suppliers: Dict[str, str] = {}  # account -> name
        self._load_master_files()

    def _load_master_files(self):
        """Load customer and supplier names from Opera"""
        # Load customers
        df = self.sql_connector.execute_query(
            "SELECT sn_account, RTRIM(sn_name) as name FROM sname"
        )
        for _, row in df.iterrows():
            self._customers[row['sn_account'].strip()] = row['name'].strip()

        # Load suppliers
        df = self.sql_connector.execute_query(
            "SELECT pn_account, RTRIM(pn_name) as name FROM pname"
        )
        for _, row in df.iterrows():
            self._suppliers[row['pn_account'].strip()] = row['name'].strip()

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

    def _fuzzy_match(self, name: str, candidates: Dict[str, str]) -> Tuple[Optional[str], Optional[str], float]:
        """
        Find best fuzzy match for name in candidates

        Args:
            name: Name to match
            candidates: Dict of account -> name

        Returns:
            Tuple of (matched_account, matched_name, score)
        """
        if not name:
            return None, None, 0.0

        name_upper = name.upper()
        best_account = None
        best_name = None
        best_score = 0.0

        for account, candidate_name in candidates.items():
            candidate_upper = candidate_name.upper()

            # Try exact prefix match first (bank names are often truncated)
            if candidate_upper.startswith(name_upper) or name_upper.startswith(candidate_upper):
                score = min(len(name_upper), len(candidate_upper)) / max(len(name_upper), len(candidate_upper))
                # Boost score for prefix matches
                score = min(1.0, score + 0.3)
            else:
                # Fall back to sequence matching
                score = SequenceMatcher(None, name_upper, candidate_upper).ratio()

            if score > best_score:
                best_score = score
                best_account = account
                best_name = candidate_name

        return best_account, best_name, best_score

    def _match_transaction(self, txn: BankTransaction) -> None:
        """
        Match transaction to customer or supplier

        Updates transaction with match results
        """
        # Try to match against customers
        cust_account, cust_name, cust_score = self._fuzzy_match(txn.name, self._customers)

        # Try to match against suppliers
        supp_account, supp_name, supp_score = self._fuzzy_match(txn.name, self._suppliers)

        # Skip if name matches both customer AND supplier above threshold
        # (could be same entity in both ledgers - ambiguous)
        if cust_score >= self.min_match_score and supp_score >= self.min_match_score:
            txn.action = 'skip'
            txn.skip_reason = f'Matches both customer ({cust_name}) and supplier ({supp_name}) - ambiguous'
            return

        # Determine best match based on transaction direction
        if txn.is_receipt:
            # Receipt: must be customer match (sales receipt)
            if cust_score >= self.min_match_score:
                txn.match_type = 'customer'
                txn.matched_account = cust_account
                txn.matched_name = cust_name
                txn.match_score = cust_score
                txn.action = 'sales_receipt'
            else:
                txn.action = 'skip'
                txn.skip_reason = f'No customer match found (best score: {cust_score:.2f})'
        else:
            # Payment: must be supplier match (purchase payment)
            if supp_score >= self.min_match_score:
                txn.match_type = 'supplier'
                txn.matched_account = supp_account
                txn.matched_name = supp_name
                txn.match_score = supp_score
                txn.action = 'purchase_payment'
            else:
                txn.action = 'skip'
                txn.skip_reason = f'No supplier match found (best score: {supp_score:.2f})'

    def _is_already_posted(self, txn: BankTransaction) -> bool:
        """
        Check if transaction has already been posted to Opera

        Checks atran (cashbook) for matching date, amount, and reference
        Note: atran stores amounts in PENCE, so multiply by 100
        """
        # Check cashbook for existing transaction
        amount_pence = txn.abs_amount * 100
        query = f"""
            SELECT COUNT(*) as cnt FROM atran
            WHERE at_acnt = '{self.bank_code}'
            AND at_pstdate = '{txn.date.strftime('%Y-%m-%d')}'
            AND ABS(at_value - {amount_pence}) < 1
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
            result = self.opera_import.import_sales_receipt(
                bank_account=self.bank_code,
                customer_account=txn.matched_account,
                amount_pounds=txn.abs_amount,
                reference=txn.reference,
                post_date=txn.date,
                input_by='BANK_IMPORT',
                validate_only=validate_only
            )
        elif txn.action == 'purchase_payment':
            # Import as purchase payment
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
