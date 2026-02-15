"""
Bank Statement Import Module for Opera SQL SE

Processes bank statement files (CSV, OFX, QIF, MT940) and imports:
- Sales receipts (customer payments)
- Purchase payments (supplier payments)

Features:
- Multi-format parser support (CSV, OFX, QIF, MT940)
- Enhanced fuzzy matching with phonetic/Levenshtein/n-gram algorithms
- Import fingerprinting for duplicate prevention
- AI-assisted categorization (optional)
- Correction-based learning

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

# Import new modules for enhanced functionality
try:
    from sql_rag.bank_parsers import (
        ParsedTransaction, detect_and_parse, parse_file, detect_format
    )
    PARSERS_AVAILABLE = True
except ImportError:
    PARSERS_AVAILABLE = False

try:
    from sql_rag.bank_duplicates import (
        EnhancedDuplicateDetector, generate_import_fingerprint, DuplicateCandidate
    )
    DUPLICATES_AVAILABLE = True
except ImportError:
    DUPLICATES_AVAILABLE = False

try:
    from sql_rag.bank_matching import EnhancedBankMatcher
    ENHANCED_MATCHING_AVAILABLE = True
except ImportError:
    ENHANCED_MATCHING_AVAILABLE = False

try:
    from sql_rag.bank_aliases import EnhancedAliasManager
    ENHANCED_ALIASES_AVAILABLE = True
except ImportError:
    ENHANCED_ALIASES_AVAILABLE = False

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

    # Additional fields from multi-format parsers
    fit_id: str = ""  # Bank's unique transaction ID (from OFX)
    check_number: str = ""

    # Matching results
    match_type: Optional[str] = None  # 'customer', 'supplier', or None
    matched_account: Optional[str] = None
    matched_name: Optional[str] = None
    match_score: float = 0.0
    match_source: str = ""  # 'alias', 'fuzzy', 'enhanced', 'ai'

    # Import status
    action: Optional[str] = None  # 'sales_receipt', 'purchase_payment', 'skip', 'manual'
    skip_reason: Optional[str] = None
    imported: bool = False
    import_result: Optional[ImportResult] = None

    # Fingerprinting for duplicate detection
    fingerprint: Optional[str] = None  # Format: BKIMP:{hash8}:{YYYYMMDD}

    # Duplicate detection results
    duplicate_candidates: List[Any] = field(default_factory=list)
    is_duplicate: bool = False

    # Manual override (for UI editing)
    manual_account: Optional[str] = None  # User-selected account override
    manual_ledger_type: Optional[str] = None  # 'C' or 'S'

    # Refund detection
    refund_credit_note: Optional[str] = None
    refund_credit_amount: Optional[float] = None

    # Repeat entry detection
    repeat_entry_ref: Optional[str] = None  # arhead.ae_entry reference
    repeat_entry_desc: Optional[str] = None  # Description from arhead
    repeat_entry_next_date: Optional[date] = None  # ae_nxtpost
    repeat_entry_posted: Optional[int] = None  # ae_posted (times posted)
    repeat_entry_total: Optional[int] = None  # ae_topost (times to post, 0=unlimited)

    # Period validation
    period_valid: bool = True  # Whether transaction date is in valid period
    period_error: Optional[str] = None  # Period validation error message
    original_date: Optional[date] = None  # Original date before any override

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


@dataclass
class AuditReportLine:
    """Single line in the audit report"""
    row: int
    date: str
    amount: float
    name: str
    action: str
    matched_to: str
    match_score: float
    status: str  # 'WILL_IMPORT', 'SKIP', 'ALREADY_POSTED', 'ERROR'
    reason: str = ""


class BankImportAuditReport:
    """
    Generates a detailed audit report for bank statement imports.

    Displays all transactions with their proposed actions, allowing
    the user to review and approve/reject before actual import.
    """

    def __init__(self, result: BankImportResult, bank_code: str):
        self.result = result
        self.bank_code = bank_code
        self.lines: List[AuditReportLine] = []
        self._generate_lines()

    def _generate_lines(self):
        """Generate audit report lines from import result"""
        for txn in self.result.transactions:
            if txn.action == 'sales_receipt':
                action = 'RECEIPT'
                status = 'WILL_IMPORT'
                matched_to = f"{txn.matched_account} ({txn.matched_name})"
            elif txn.action == 'purchase_payment':
                action = 'PAYMENT'
                status = 'WILL_IMPORT'
                matched_to = f"{txn.matched_account} ({txn.matched_name})"
            elif txn.skip_reason and 'Already' in txn.skip_reason:
                action = '-'
                status = 'ALREADY_POSTED'
                matched_to = txn.matched_name or '-'
            else:
                action = '-'
                status = 'SKIP'
                matched_to = txn.matched_name or '-'

            self.lines.append(AuditReportLine(
                row=txn.row_number,
                date=txn.date.strftime('%d/%m/%Y'),
                amount=txn.amount,
                name=txn.name[:30] if txn.name else '-',
                action=action,
                matched_to=matched_to[:35] if matched_to else '-',
                match_score=txn.match_score,
                status=status,
                reason=txn.skip_reason or ''
            ))

    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics for the audit report"""
        will_import = [l for l in self.lines if l.status == 'WILL_IMPORT']
        receipts = [l for l in will_import if l.action == 'RECEIPT']
        payments = [l for l in will_import if l.action == 'PAYMENT']
        skipped = [l for l in self.lines if l.status == 'SKIP']
        already_posted = [l for l in self.lines if l.status == 'ALREADY_POSTED']

        return {
            'total_transactions': len(self.lines),
            'will_import': len(will_import),
            'receipts': {
                'count': len(receipts),
                'total': sum(l.amount for l in receipts)
            },
            'payments': {
                'count': len(payments),
                'total': sum(l.amount for l in payments)
            },
            'skipped': {
                'count': len(skipped),
                'reasons': self._group_skip_reasons(skipped)
            },
            'already_posted': len(already_posted),
            'net_effect': sum(l.amount for l in will_import)
        }

    def _group_skip_reasons(self, skipped_lines: List[AuditReportLine]) -> Dict[str, int]:
        """Group skipped transactions by reason"""
        reasons = {}
        for line in skipped_lines:
            reason = line.reason or 'Unknown'
            # Simplify reason for grouping
            if 'No supplier match' in reason:
                key = 'No supplier match'
            elif 'No customer match' in reason:
                key = 'No customer match'
            elif 'ambiguous' in reason.lower():
                key = 'Ambiguous match'
            elif 'pattern' in reason.lower():
                key = 'Excluded by pattern'
            elif 'Subcategory' in reason:
                key = 'Excluded subcategory'
            elif 'refund' in reason.lower():
                key = 'Possible refund (needs review)'
            else:
                key = reason[:40]
            reasons[key] = reasons.get(key, 0) + 1
        return reasons

    def format_report(self, include_skipped: bool = True, include_details: bool = True) -> str:
        """
        Format the audit report as a string for display.

        Args:
            include_skipped: Include skipped transactions in detail section
            include_details: Include transaction-level details

        Returns:
            Formatted audit report string
        """
        summary = self.get_summary()
        lines = []

        # Header
        lines.append("=" * 80)
        lines.append("BANK IMPORT AUDIT REPORT")
        lines.append("=" * 80)
        lines.append(f"File: {self.result.filename}")
        lines.append(f"Bank Account: {self.bank_code}")
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")

        # Summary Section
        lines.append("-" * 80)
        lines.append("SUMMARY")
        lines.append("-" * 80)
        lines.append(f"Total transactions in file:     {summary['total_transactions']:>6}")
        lines.append(f"Will be imported:               {summary['will_import']:>6}")
        lines.append(f"  - Receipts (money in):        {summary['receipts']['count']:>6}  £{summary['receipts']['total']:>12,.2f}")
        lines.append(f"  - Payments (money out):       {summary['payments']['count']:>6}  £{summary['payments']['total']:>12,.2f}")
        lines.append(f"Already posted (duplicates):    {summary['already_posted']:>6}")
        lines.append(f"Skipped (no match/excluded):    {summary['skipped']['count']:>6}")
        lines.append("")
        lines.append(f"NET BANK EFFECT:                        £{summary['net_effect']:>12,.2f}")
        lines.append("")

        # Skip Reasons
        if summary['skipped']['reasons']:
            lines.append("-" * 80)
            lines.append("SKIP REASONS BREAKDOWN")
            lines.append("-" * 80)
            for reason, count in sorted(summary['skipped']['reasons'].items(), key=lambda x: -x[1]):
                lines.append(f"  {reason:<45} {count:>3}")
            lines.append("")

        # Transactions to Import
        if include_details:
            will_import = [l for l in self.lines if l.status == 'WILL_IMPORT']
            if will_import:
                lines.append("-" * 80)
                lines.append("TRANSACTIONS TO IMPORT")
                lines.append("-" * 80)
                lines.append(f"{'Row':>4} {'Date':<10} {'Amount':>12} {'Type':<8} {'Name':<25} {'Matched To':<30} {'Score':<5}")
                lines.append("-" * 80)
                for l in will_import:
                    lines.append(
                        f"{l.row:>4} {l.date:<10} £{l.amount:>10,.2f} {l.action:<8} "
                        f"{l.name:<25} {l.matched_to:<30} {l.match_score:.0%}"
                    )
                lines.append("")

        # Skipped Transactions
        if include_details and include_skipped:
            skipped = [l for l in self.lines if l.status in ('SKIP', 'ALREADY_POSTED')]
            if skipped:
                lines.append("-" * 80)
                lines.append("SKIPPED TRANSACTIONS")
                lines.append("-" * 80)
                lines.append(f"{'Row':>4} {'Date':<10} {'Amount':>12} {'Status':<15} {'Name':<20} {'Reason':<30}")
                lines.append("-" * 80)
                for l in skipped:
                    reason_short = l.reason[:30] if l.reason else '-'
                    lines.append(
                        f"{l.row:>4} {l.date:<10} £{l.amount:>10,.2f} {l.status:<15} "
                        f"{l.name:<20} {reason_short}"
                    )
                lines.append("")

        # Footer
        lines.append("=" * 80)
        if summary['will_import'] > 0:
            lines.append("REVIEW THE ABOVE AND CONFIRM TO PROCEED WITH IMPORT")
        else:
            lines.append("NO TRANSACTIONS TO IMPORT")
        lines.append("=" * 80)

        return "\n".join(lines)

    def format_json(self) -> Dict[str, Any]:
        """Format the audit report as JSON for API responses"""
        summary = self.get_summary()
        return {
            'filename': self.result.filename,
            'bank_code': self.bank_code,
            'generated': datetime.now().isoformat(),
            'summary': summary,
            'transactions': [
                {
                    'row': l.row,
                    'date': l.date,
                    'amount': l.amount,
                    'name': l.name,
                    'action': l.action,
                    'matched_to': l.matched_to,
                    'match_score': l.match_score,
                    'status': l.status,
                    'reason': l.reason
                }
                for l in self.lines
            ]
        }


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
                 use_extended_fields: bool = True,
                 use_enhanced_matching: bool = True,
                 use_fingerprinting: bool = True):
        """
        Initialize bank statement importer

        Args:
            bank_code: Opera bank account code (default BC010)
            min_match_score: Minimum fuzzy match score (0-1) to consider a match
            learn_threshold: Minimum score (0-1) to save as alias for future (default 0.8)
            use_aliases: Enable alias lookup from learned matches (default True)
            use_extended_fields: Use payee/search keys for matching (default True)
            use_enhanced_matching: Use enhanced matching (phonetic, Levenshtein, n-gram)
            use_fingerprinting: Enable import fingerprinting for duplicate prevention
        """
        self.bank_code = bank_code
        self.min_match_score = min_match_score
        self.learn_threshold = learn_threshold
        self.use_aliases = use_aliases
        self.use_extended_fields = use_extended_fields
        self.use_enhanced_matching = use_enhanced_matching and ENHANCED_MATCHING_AVAILABLE
        self.use_fingerprinting = use_fingerprinting and DUPLICATES_AVAILABLE

        self.sql_connector = SQLConnector()
        self.opera_import = OperaSQLImport(self.sql_connector)

        # Initialize matcher (enhanced or basic)
        if self.use_enhanced_matching:
            self.matcher = EnhancedBankMatcher(min_score=self.min_match_score)
            logger.info("Using enhanced matching (phonetic, Levenshtein, n-gram)")
        else:
            self.matcher = BankMatcher(min_score=self.min_match_score)

        # Initialize alias manager (enhanced or basic)
        self.alias_manager = None
        if self.use_aliases:
            try:
                if ENHANCED_ALIASES_AVAILABLE:
                    self.alias_manager = EnhancedAliasManager()
                    logger.info("Using enhanced alias manager with correction learning")
                else:
                    from sql_rag.bank_aliases import BankAliasManager
                    self.alias_manager = BankAliasManager()
            except Exception as e:
                logger.warning(f"Could not initialize alias manager: {e}")

        # Initialize duplicate detector
        self.duplicate_detector = None
        if self.use_fingerprinting:
            try:
                self.duplicate_detector = EnhancedDuplicateDetector(self.sql_connector)
                logger.info("Using enhanced duplicate detection with fingerprinting")
            except Exception as e:
                logger.warning(f"Could not initialize duplicate detector: {e}")

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

    @staticmethod
    def find_bank_account_by_details(sort_code: str, account_number: str) -> Optional[str]:
        """
        Find Opera bank account code by sort code and account number.

        Args:
            sort_code: Bank sort code (e.g., '20-96-89')
            account_number: Bank account number (e.g., '90764205')

        Returns:
            Bank account code (e.g., 'BC010') if found, None otherwise
        """
        if not sort_code or not account_number:
            return None

        # Normalize - remove spaces and dashes for comparison
        sort_normalized = sort_code.replace(' ', '').replace('-', '')
        account_normalized = account_number.replace(' ', '')

        sql = SQLConnector()
        df = sql.execute_query("""
            SELECT RTRIM(nk_acnt) as code,
                   RTRIM(nk_sort) as sort_code,
                   RTRIM(nk_number) as account_number
            FROM nbank WITH (NOLOCK)
            WHERE nk_sort IS NOT NULL AND nk_number IS NOT NULL
        """)

        for _, row in df.iterrows():
            db_sort = (row['sort_code'] or '').replace(' ', '').replace('-', '')
            db_account = (row['account_number'] or '').replace(' ', '')

            if db_sort == sort_normalized and db_account == account_normalized:
                return row['code']

        return None

    @staticmethod
    def find_bank_account_by_details_from_csv(filepath: str) -> Optional[str]:
        """
        Detect which Opera bank account a CSV file belongs to.

        Reads the first row of the CSV to extract bank details and finds
        the matching Opera bank account.

        Args:
            filepath: Path to CSV file

        Returns:
            Bank account code (e.g., 'BC010') if found, None otherwise
        """
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                first_row = next(reader, None)

                if not first_row:
                    return None

                # Extract account details (format: "20-96-89 90764205")
                account_field = first_row.get('Account', '').strip()
                if not account_field:
                    return None

                # Parse sort code and account number
                parts = account_field.split(' ', 1)
                if len(parts) != 2:
                    return None

                csv_sort_code = parts[0].strip()
                csv_account_number = parts[1].strip()

                # Find matching Opera bank account
                return BankStatementImport.find_bank_account_by_details(csv_sort_code, csv_account_number)

        except Exception as e:
            logger.warning(f"Error detecting bank from CSV: {e}")
            return None

    def validate_bank_account_from_csv(self, filepath: str) -> Tuple[bool, str, Optional[str]]:
        """
        Validate that the CSV bank details match the configured bank account.

        Reads the first transaction to extract bank details and validates against
        the configured bank_code.

        Args:
            filepath: Path to CSV file

        Returns:
            Tuple of (is_valid, message, detected_bank_code)
        """
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                first_row = next(reader, None)

                if not first_row:
                    return False, "CSV file is empty", None

                # Extract account details (format: "20-96-89 90764205")
                account_field = first_row.get('Account', '').strip()
                if not account_field:
                    return True, "No bank account in CSV - using configured bank", None

                # Parse sort code and account number
                parts = account_field.split(' ', 1)
                if len(parts) != 2:
                    return True, f"Could not parse bank details: {account_field}", None

                csv_sort_code = parts[0].strip()
                csv_account_number = parts[1].strip()

                # Find matching Opera bank account
                detected_code = self.find_bank_account_by_details(csv_sort_code, csv_account_number)

                if not detected_code:
                    return False, f"Bank account {csv_sort_code} {csv_account_number} not found in Opera", None

                # Check if it matches configured bank
                if detected_code != self.bank_code:
                    return False, (f"CSV is for bank {detected_code} ({csv_sort_code} {csv_account_number}) "
                                   f"but configured bank is {self.bank_code}"), detected_code

                return True, f"Bank account verified: {self.bank_code} ({csv_sort_code} {csv_account_number})", detected_code

        except Exception as e:
            return False, f"Error validating bank account: {e}", None

    def _load_master_files(self):
        """Load customer and supplier data from Opera into the shared matcher"""
        customers: Dict[str, MatchCandidate] = {}
        suppliers: Dict[str, MatchCandidate] = {}

        # Load customers with extended fields
        # Use NOLOCK to avoid blocking other users during read operations
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
                FROM sname WITH (NOLOCK)
            """
        else:
            customer_query = "SELECT sn_account, RTRIM(sn_name) as name FROM sname WITH (NOLOCK)"

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
        # Use NOLOCK to avoid blocking other users during read operations
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
                FROM pname WITH (NOLOCK)
            """
        else:
            supplier_query = "SELECT pn_account, RTRIM(pn_name) as name FROM pname WITH (NOLOCK)"

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

    def _check_repeat_entry(self, txn: BankTransaction) -> bool:
        """Check if transaction matches an UNPOSTED repeat entry in arhead/arline.

        Compares bank statement transactions with Opera's repeat entries to detect
        transactions that are handled by Opera's auto-posting routine.

        Only matches repeat entries that haven't been fully posted yet (ae_posted < ae_topost
        or ae_topost = 0 for unlimited).

        Matching criteria:
        - Bank account matches (ae_acnt)
        - Amount matches (at_value in pence)
        - Date is within +/- 5 days of ae_nxtpost (next posting date)
        - Repeat entry is not fully posted

        Returns True if matched as repeat entry, False otherwise.
        """
        try:
            # Step 1: Check alias table first (fast path for previously learned names)
            if self.alias_manager:
                alias_match = self.alias_manager.lookup_repeat_entry_alias(txn.name, self.bank_code)
                if alias_match:
                    entry_ref = alias_match['entry_ref']
                    entry_desc = alias_match['entry_desc']
                    use_count = alias_match.get('use_count', 1)

                    # Validate the entry still exists and is active in Opera
                    validate_query = f"""
                        SELECT h.ae_entry, h.ae_desc, h.ae_nxtpost, h.ae_freq, h.ae_every,
                               h.ae_posted, h.ae_topost
                        FROM arhead h WITH (NOLOCK)
                        WHERE h.ae_entry = '{entry_ref}'
                          AND RTRIM(h.ae_acnt) = '{self.bank_code}'
                          AND (h.ae_topost = 0 OR h.ae_posted < h.ae_topost)
                    """
                    df = self.sql_connector.execute_query(validate_query)

                    if df is not None and len(df) > 0:
                        # Alias is valid - use it directly
                        best = df.iloc[0]
                        next_post_date = best.get('ae_nxtpost')
                        if next_post_date is not None:
                            if hasattr(next_post_date, 'date'):
                                next_post_date = next_post_date.date()
                            elif isinstance(next_post_date, str):
                                next_post_date = datetime.strptime(next_post_date[:10], '%Y-%m-%d').date()

                        # Don't match if transaction date is too far before the next_post_date (historical)
                        # Allow 10 days tolerance before the next_post_date
                        from datetime import timedelta
                        tolerance_days = 10
                        if next_post_date and txn.date < (next_post_date - timedelta(days=tolerance_days)):
                            logger.debug(f"Alias found for '{txn.name}' but transaction date {txn.date} is more than {tolerance_days} days before next_post_date {next_post_date} - skipping")
                            return False

                        txn.action = 'repeat_entry'
                        txn.skip_reason = None
                        txn.repeat_entry_ref = entry_ref
                        txn.repeat_entry_desc = entry_desc or str(best.get('ae_desc', '')).strip()
                        txn.repeat_entry_next_date = next_post_date
                        txn.repeat_entry_posted = int(best.get('ae_posted', 0) or 0)
                        txn.repeat_entry_total = int(best.get('ae_topost', 0) or 0)

                        freq_map = {'D': 'Daily', 'W': 'Weekly', 'M': 'Monthly', 'Q': 'Quarterly', 'Y': 'Yearly'}
                        freq = str(best.get('ae_freq', '')).strip().upper()
                        every = int(best.get('ae_every', 1) or 1)
                        freq_desc = freq_map.get(freq, freq)
                        if every > 1:
                            freq_desc = f"Every {every} {freq_desc.lower()}s"

                        logger.info(f"Repeat entry matched (alias, {use_count} uses): '{txn.name}' -> {entry_ref} ({txn.repeat_entry_desc}) - {freq_desc}")
                        return True
                    else:
                        logger.debug(f"Alias found for '{txn.name}' -> {entry_ref} but entry no longer active, falling through to amount/ref match")

            # Step 2: Amount in pence for comparison with arline.at_value
            # Use absolute value since bank statement signs may differ from Opera storage
            amount_pence_abs = abs(int(txn.amount * 100))

            # Build search terms from transaction name, reference, and memo
            # Clean up for SQL LIKE matching
            search_terms = []
            for text in [txn.name, txn.reference, txn.memo]:
                if text and len(text.strip()) >= 3:
                    # Clean and escape for SQL
                    clean = text.strip().replace("'", "''").upper()
                    # Extract key words (at least 3 chars)
                    words = [w for w in clean.split() if len(w) >= 3]
                    search_terms.extend(words[:3])  # Limit to first 3 words

            # Build reference match conditions
            ref_conditions = []
            for term in search_terms[:5]:  # Limit to 5 terms
                ref_conditions.append(f"UPPER(h.ae_desc) LIKE '%{term}%'")
                ref_conditions.append(f"UPPER(l.at_comment) LIKE '%{term}%'")

            ref_match_sql = f"OR ({' OR '.join(ref_conditions)})" if ref_conditions else ""

            # Query arhead + arline for matching UNPOSTED repeat entries
            # Match by: amount (within 10p) OR reference/name matches description
            # Only match where ae_posted < ae_topost (or ae_topost = 0 for unlimited)
            query = f"""
                SELECT h.ae_entry, h.ae_desc, h.ae_nxtpost, h.ae_freq, h.ae_every,
                       h.ae_posted, h.ae_topost, h.ae_type,
                       l.at_value, l.at_account, l.at_cbtype, l.at_comment,
                       CASE WHEN ABS(ABS(l.at_value) - {amount_pence_abs}) < 10 THEN 1 ELSE 0 END as amount_match,
                       CASE WHEN {' OR '.join(ref_conditions) if ref_conditions else '1=0'} THEN 1 ELSE 0 END as ref_match
                FROM arhead h WITH (NOLOCK)
                JOIN arline l WITH (NOLOCK) ON h.ae_entry = l.at_entry AND h.ae_acnt = l.at_acnt
                WHERE RTRIM(h.ae_acnt) = '{self.bank_code}'
                  AND (h.ae_topost = 0 OR h.ae_posted < h.ae_topost)  -- Only unposted entries
                  AND (
                      ABS(ABS(l.at_value) - {amount_pence_abs}) < 10  -- Amount matches (10p tolerance)
                      {ref_match_sql}  -- OR reference/name matches description
                  )
                ORDER BY
                    CASE WHEN ABS(ABS(l.at_value) - {amount_pence_abs}) < 10 THEN 0 ELSE 1 END,  -- Prefer amount matches
                    ABS(DATEDIFF(day, h.ae_nxtpost, '{txn.date.isoformat()}')) ASC
            """

            logger.debug(f"Checking repeat entries for {txn.name}: amount={amount_pence_abs}p, date={txn.date}, bank={self.bank_code}, search_terms={search_terms}")
            df = self.sql_connector.execute_query(query)

            if df is None or len(df) == 0:
                logger.debug(f"No repeat entry match found for amount={amount_pence_abs}p or refs={search_terms} on bank {self.bank_code}")
                return False

            best = df.iloc[0]
            amount_matched = best.get('amount_match', 0) == 1
            ref_matched = best.get('ref_match', 0) == 1
            match_type = "amount" if amount_matched else ("reference" if ref_matched else "unknown")

            logger.debug(f"Potential repeat entry match: {best.get('ae_entry')} - {best.get('ae_desc')} - "
                        f"at_value={best.get('at_value')}p, ae_nxtpost={best.get('ae_nxtpost')}, match_type={match_type}")

            # Parse next_post_date for validation
            next_post_date = best.get('ae_nxtpost')
            if next_post_date is not None:
                if hasattr(next_post_date, 'date'):
                    next_post_date = next_post_date.date()
                elif isinstance(next_post_date, str):
                    next_post_date = datetime.strptime(next_post_date[:10], '%Y-%m-%d').date()

            # Don't match if transaction date is too far before the next_post_date (historical)
            # Allow 10 days tolerance before the next_post_date
            from datetime import timedelta
            tolerance_days = 10
            if next_post_date and txn.date < (next_post_date - timedelta(days=tolerance_days)):
                logger.debug(f"Repeat entry amount/ref match found but transaction date {txn.date} is more than {tolerance_days} days before next_post_date {next_post_date} - skipping")
                return False

            # Match found - amount or reference matches, entry is active, and date is valid
            txn.action = 'repeat_entry'
            txn.skip_reason = None
            txn.repeat_entry_ref = str(best.get('ae_entry', '')).strip()
            txn.repeat_entry_desc = str(best.get('ae_desc', '')).strip() or str(best.get('at_comment', '')).strip()
            txn.repeat_entry_next_date = next_post_date
            txn.repeat_entry_posted = int(best.get('ae_posted', 0) or 0)
            txn.repeat_entry_total = int(best.get('ae_topost', 0) or 0)

            # Frequency description
            freq_map = {'D': 'Daily', 'W': 'Weekly', 'M': 'Monthly', 'Q': 'Quarterly', 'Y': 'Yearly'}
            freq = str(best.get('ae_freq', '')).strip().upper()
            every = int(best.get('ae_every', 1) or 1)
            freq_desc = freq_map.get(freq, freq)
            if every > 1:
                freq_desc = f"Every {every} {freq_desc.lower()}s"

            logger.info(f"Repeat entry matched ({match_type}): '{txn.name}' -> {txn.repeat_entry_ref} ({txn.repeat_entry_desc}) - {freq_desc}")
            return True

        except Exception as e:
            logger.warning(f"Error checking repeat entries: {e}")

        return False

    def _check_customer_refund(self, txn: BankTransaction, customer_code: str, amount: float) -> bool:
        """Check stran for unallocated credit notes or overpayments matching this payment.

        Checks for:
        - Credit notes (st_trtype='C') with negative balance (unallocated credit)
        - Overpayments (st_trtype='R') with negative balance (customer overpaid)
        """
        query = f"""
            SELECT TOP 5 st_unique, st_trtype, st_trvalue, st_trbal, st_trdate, st_trref
            FROM stran WITH (NOLOCK)
            WHERE RTRIM(st_account) = '{customer_code}'
              AND st_trtype IN ('C', 'R')
              AND st_trbal < 0
            ORDER BY ABS(ABS(st_trbal) - {amount}) ASC
        """
        try:
            df = self.sql_connector.execute_query(query)
            if df is not None and len(df) > 0:
                best = df.iloc[0]
                txn.action = 'sales_refund'
                txn.match_type = 'customer'
                txn.matched_account = customer_code
                txn.skip_reason = None
                txn.refund_credit_note = str(best.get('st_trref', '')).strip() if best.get('st_trref') else ''
                txn.refund_credit_amount = abs(float(best.get('st_trbal', 0)))
                logger.debug(f"Customer refund detected: {customer_code} credit note {txn.refund_credit_note} for £{txn.refund_credit_amount:.2f}")
                return True
        except Exception as e:
            logger.warning(f"Error checking customer refund for {customer_code}: {e}")
        return False

    def _check_purchase_refund(self, txn: BankTransaction, supplier_code: str, amount: float) -> bool:
        """Check ptran for unallocated credit notes or overpayments matching this receipt.

        Checks for:
        - Credit notes (pt_trtype='C') with positive balance (unallocated credit)
        - Overpayments (pt_trtype='P') with positive balance (we overpaid supplier)
        """
        query = f"""
            SELECT TOP 5 pt_unique, pt_trtype, pt_trvalue, pt_trbal, pt_trdate, pt_trref
            FROM ptran WITH (NOLOCK)
            WHERE RTRIM(pt_account) = '{supplier_code}'
              AND pt_trtype IN ('C', 'P')
              AND pt_trbal > 0
            ORDER BY ABS(pt_trbal - {amount}) ASC
        """
        try:
            df = self.sql_connector.execute_query(query)
            if df is not None and len(df) > 0:
                best = df.iloc[0]
                txn.action = 'purchase_refund'
                txn.match_type = 'supplier'
                txn.matched_account = supplier_code
                txn.skip_reason = None
                txn.refund_credit_note = str(best.get('pt_trref', '')).strip() if best.get('pt_trref') else ''
                txn.refund_credit_amount = abs(float(best.get('pt_trbal', 0)))
                logger.debug(f"Purchase refund detected: {supplier_code} credit note {txn.refund_credit_note} for £{txn.refund_credit_amount:.2f}")
                return True
        except Exception as e:
            logger.warning(f"Error checking purchase refund for {supplier_code}: {e}")
        return False

    def _match_transaction(self, txn: BankTransaction) -> None:
        """
        Match transaction to customer or supplier.

        Matching strategy:
        0. Check repeat entries first (auto-posted by Opera)
        1. Check alias table (fast path for previously seen names)
        2. Fuzzy match using shared matcher
        3. Save successful matches as aliases for future

        Updates transaction with match results
        """
        # Step 0: Check repeat entries first - these are handled by Opera's auto-post
        if self._check_repeat_entry(txn):
            return  # Matched as repeat entry - no further matching needed

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

        # Handle ambiguous matches (both customer AND supplier match above threshold)
        if cust_result.is_match and supp_result.is_match:
            score_diff = abs(cust_result.score - supp_result.score)

            # If scores are very similar (<0.15 difference), it's truly ambiguous
            if score_diff < 0.15:
                txn.action = 'skip'
                txn.skip_reason = f'Matches both customer ({cust_result.name}) and supplier ({supp_result.name}) - ambiguous'
                return

            # If one score is significantly higher, prefer that match based on transaction direction
            # For receipts, prefer customer if customer score is higher
            # For payments, prefer supplier if supplier score is higher
            if txn.is_receipt:
                if cust_result.score > supp_result.score:
                    # Customer is better match for a receipt - use it
                    pass  # Fall through to receipt handling below
                else:
                    txn.action = 'skip'
                    txn.skip_reason = f'Matches both - supplier score ({supp_result.score:.2f}) higher than customer ({cust_result.score:.2f}) for receipt'
                    return
            else:  # Payment
                if supp_result.score > cust_result.score:
                    # Supplier is better match for a payment - use it
                    pass  # Fall through to payment handling below
                else:
                    # Customer score is higher for a payment - could be a customer refund
                    # Check for unallocated credit note
                    if self._check_customer_refund(txn, cust_result.account, txn.abs_amount):
                        txn.matched_name = cust_result.name
                        txn.match_score = cust_result.score
                        txn.match_source = 'fuzzy'
                        return
                    txn.action = 'skip'
                    txn.skip_reason = f'Matches both - customer score ({cust_result.score:.2f}) higher than supplier ({supp_result.score:.2f}) for payment but no unallocated credit note found'
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
            elif supp_result.is_match and supp_result.score >= 0.8:
                # Receipt with strong supplier match but no customer - check for purchase refund
                if self._check_purchase_refund(txn, supp_result.account, txn.abs_amount):
                    txn.matched_name = supp_result.name
                    txn.match_score = supp_result.score
                    txn.match_source = 'fuzzy'
                else:
                    txn.action = 'skip'
                    txn.skip_reason = f'Receipt matches supplier {supp_result.name} but no unallocated credit note found'
            else:
                txn.action = 'skip'
                txn.skip_reason = f'No customer match found (best score: {cust_result.score:.2f})'
        else:
            # Payment: check for supplier match first
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
            elif cust_result.is_match and cust_result.score >= 0.8:
                # Payment with strong customer match but no supplier - check for customer refund
                if self._check_customer_refund(txn, cust_result.account, txn.abs_amount):
                    txn.matched_name = cust_result.name
                    txn.match_score = cust_result.score
                    txn.match_source = 'fuzzy'
                else:
                    txn.action = 'skip'
                    txn.skip_reason = f'Payment matches customer {cust_result.name} but no unallocated credit note found'
            else:
                txn.action = 'skip'
                txn.skip_reason = f'No supplier match found (best score: {supp_result.score:.2f})'

    def _is_already_posted(self, txn: BankTransaction) -> Tuple[bool, str]:
        """
        Check if transaction has already been posted to Opera

        Uses multiple detection strategies:
        1. Fingerprint check (definitive - highest priority)
        2. atran (cashbook) - bank account transactions
        3. ptran (purchase ledger) - for supplier payments
        4. stran (sales ledger) - for customer receipts

        Returns:
            Tuple of (is_posted, reason) where reason explains where duplicate was found
        """
        # Priority 1: Fingerprint check (definitive if enabled)
        if self.use_fingerprinting and self.duplicate_detector:
            # Generate fingerprint for this transaction
            if DUPLICATES_AVAILABLE:
                txn.fingerprint = generate_import_fingerprint(txn.name, txn.amount, txn.date)

                # Check using enhanced duplicate detector
                candidates = self.duplicate_detector.find_duplicates(
                    name=txn.name,
                    amount=txn.amount,
                    txn_date=txn.date,
                    account=txn.matched_account,
                    bank_code=self.bank_code,
                    fit_id=txn.fit_id,
                    reference=txn.reference
                )

                # Store candidates for UI display
                txn.duplicate_candidates = candidates

                # Fingerprint match is definitive
                for c in candidates:
                    if c.match_type == 'fingerprint':
                        txn.is_duplicate = True
                        return True, f"Already imported (fingerprint match): {c.table}.{c.record_id}"

                # High confidence matches are also considered duplicates
                for c in candidates:
                    if c.confidence >= 0.9:
                        txn.is_duplicate = True
                        return True, f"Already posted ({c.match_type}): {c.table}.{c.record_id}"

        # Fallback to legacy duplicate detection
        date_str = txn.date.strftime('%Y-%m-%d')
        amount_pounds = txn.abs_amount

        # Check 1: Cashbook (atran) - amounts in PENCE
        # Use NOLOCK to avoid blocking other users during validation
        amount_pence = int(amount_pounds * 100)
        query = f"""
            SELECT COUNT(*) as cnt FROM atran WITH (NOLOCK)
            WHERE at_acnt = '{self.bank_code}'
            AND at_pstdate = '{date_str}'
            AND ABS(ABS(at_value) - {amount_pence}) < 1
        """
        df = self.sql_connector.execute_query(query)
        if int(df.iloc[0]['cnt']) > 0:
            txn.is_duplicate = True
            return True, "Already in cashbook (atran)"

        # Check 2: Purchase Ledger (ptran) - for supplier payments
        # Only check if we have a matched supplier
        if txn.action == 'purchase_payment' and txn.matched_account:
            # ptran amounts are in POUNDS, payments are usually negative or have type 'P'
            query = f"""
                SELECT COUNT(*) as cnt FROM ptran WITH (NOLOCK)
                WHERE RTRIM(pt_account) = '{txn.matched_account}'
                AND pt_trdate = '{date_str}'
                AND ABS(ABS(pt_trvalue) - {amount_pounds}) < 0.01
                AND pt_trtype = 'P'
            """
            df = self.sql_connector.execute_query(query)
            if int(df.iloc[0]['cnt']) > 0:
                txn.is_duplicate = True
                return True, f"Already in purchase ledger (ptran) for {txn.matched_account}"

        # Check 3: Sales Ledger (stran) - for customer receipts
        # Only check if we have a matched customer
        if txn.action == 'sales_receipt' and txn.matched_account:
            # stran amounts are in POUNDS, receipts have type 'R'
            query = f"""
                SELECT COUNT(*) as cnt FROM stran WITH (NOLOCK)
                WHERE RTRIM(st_account) = '{txn.matched_account}'
                AND st_trdate = '{date_str}'
                AND ABS(ABS(st_trvalue) - {amount_pounds}) < 0.01
                AND st_trtype = 'R'
            """
            df = self.sql_connector.execute_query(query)
            if int(df.iloc[0]['cnt']) > 0:
                txn.is_duplicate = True
                return True, f"Already in sales ledger (stran) for {txn.matched_account}"

        return False, ""

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

    def parse_file(self, filepath: str, format_override: Optional[str] = None) -> Tuple[List[BankTransaction], str]:
        """
        Parse bank statement file with auto-format detection.

        Supports CSV, OFX, QIF, and MT940 formats.

        Args:
            filepath: Path to bank statement file
            format_override: Optional format override ('CSV', 'OFX', 'QIF', 'MT940')

        Returns:
            Tuple of (transactions, detected_format)
        """
        if not PARSERS_AVAILABLE:
            logger.warning("Multi-format parsers not available, falling back to CSV")
            return self.parse_csv(filepath), "CSV"

        try:
            # Use new parser system
            if format_override:
                parsed_txns = parse_file(filepath, format_override)
                detected_format = format_override.upper()
            else:
                parsed_txns, detected_format = detect_and_parse(filepath)

            logger.info(f"Parsed {len(parsed_txns)} transactions from {detected_format} file")

            # Convert ParsedTransaction to BankTransaction
            transactions = []
            for i, ptxn in enumerate(parsed_txns, start=1):
                txn = BankTransaction(
                    row_number=i,
                    date=ptxn.date,
                    amount=ptxn.amount,
                    subcategory=ptxn.subcategory,
                    memo=ptxn.memo,
                    name=ptxn.name,
                    reference=ptxn.reference,
                    fit_id=ptxn.fit_id,
                    check_number=ptxn.check_number
                )

                # Generate fingerprint for duplicate detection
                if self.use_fingerprinting and DUPLICATES_AVAILABLE:
                    txn.fingerprint = generate_import_fingerprint(ptxn.name, ptxn.amount, ptxn.date)

                transactions.append(txn)

            return transactions, detected_format

        except Exception as e:
            logger.warning(f"Multi-format parsing failed, falling back to CSV: {e}")
            return self.parse_csv(filepath), "CSV"

    def parse_content(self, content: str, filename: str, format_override: Optional[str] = None) -> Tuple[List[BankTransaction], str]:
        """
        Parse bank statement from content string (for email attachments).

        Args:
            content: File content as string
            filename: Original filename (for format detection)
            format_override: Optional format to use instead of auto-detect

        Returns:
            Tuple of (transactions list, detected format name)
        """
        if not PARSERS_AVAILABLE:
            logger.warning("Multi-format parsers not available for content parsing")
            raise ValueError("Multi-format parsers required for content-based parsing")

        try:
            from sql_rag.bank_parsers import detect_format, get_parser, PARSERS

            if format_override:
                parser = get_parser(format_override)
                if not parser:
                    raise ValueError(f"Unknown format: {format_override}")
                parsed_txns = parser.parse(content, filename)
                detected_format = format_override.upper()
            else:
                # Auto-detect format from content
                detected_format = detect_format(content, filename)
                if not detected_format:
                    raise ValueError(f"Could not detect format for content (filename: {filename})")

                parser = get_parser(detected_format)
                parsed_txns = parser.parse(content, filename)

            logger.info(f"Parsed {len(parsed_txns)} transactions from content as {detected_format}")

            # Convert ParsedTransaction to BankTransaction
            transactions = []
            for i, ptxn in enumerate(parsed_txns, start=1):
                txn = BankTransaction(
                    row_number=i,
                    date=ptxn.date,
                    amount=ptxn.amount,
                    subcategory=ptxn.subcategory,
                    memo=ptxn.memo,
                    name=ptxn.name,
                    reference=ptxn.reference,
                    fit_id=ptxn.fit_id,
                    check_number=ptxn.check_number
                )

                # Generate fingerprint for duplicate detection
                if self.use_fingerprinting and DUPLICATES_AVAILABLE:
                    txn.fingerprint = generate_import_fingerprint(ptxn.name, ptxn.amount, ptxn.date)

                transactions.append(txn)

            return transactions, detected_format

        except Exception as e:
            logger.error(f"Error parsing content: {e}")
            raise

    def validate_bank_from_content(self, content: str) -> Tuple[bool, str, Optional[str]]:
        """
        Validate bank account from CSV content string.

        Reads the first transaction to extract bank details and validates against
        the configured bank_code.

        Args:
            content: CSV file content as string

        Returns:
            Tuple of (is_valid, message, detected_bank_code)
        """
        try:
            import io
            reader = csv.DictReader(io.StringIO(content))
            first_row = next(reader, None)

            if not first_row:
                return False, "CSV content is empty", None

            # Extract account details (format: "20-96-89 90764205")
            account_field = first_row.get('Account', '').strip()
            if not account_field:
                return True, "No bank account in CSV - using configured bank", None

            # Parse sort code and account number
            parts = account_field.split(' ', 1)
            if len(parts) != 2:
                return True, f"Could not parse bank details: {account_field}", None

            csv_sort_code = parts[0].strip()
            csv_account_number = parts[1].strip()

            # Find matching Opera bank account
            detected_code = self.find_bank_account_by_details(csv_sort_code, csv_account_number)

            if not detected_code:
                return False, f"Bank account {csv_sort_code} {csv_account_number} not found in Opera", None

            # Check if it matches configured bank
            if detected_code != self.bank_code:
                return False, (f"CSV is for bank {detected_code} ({csv_sort_code} {csv_account_number}) "
                               f"but configured bank is {self.bank_code}"), detected_code

            return True, f"Bank account verified: {self.bank_code} ({csv_sort_code} {csv_account_number})", detected_code

        except Exception as e:
            return False, f"Error validating bank account from content: {e}", None

    @staticmethod
    def detect_file_format(filepath: str) -> Optional[str]:
        """
        Detect the format of a bank statement file without parsing.

        Args:
            filepath: Path to bank statement file

        Returns:
            Format name ('CSV', 'OFX', 'QIF', 'MT940') or None
        """
        if not PARSERS_AVAILABLE:
            return 'CSV'

        try:
            from pathlib import Path
            path = Path(filepath)

            if not path.exists():
                return None

            # Read first part of file
            try:
                content = path.read_text(encoding='utf-8')[:1000]
            except UnicodeDecodeError:
                content = path.read_text(encoding='latin-1')[:1000]

            return detect_format(content, path.name)
        except Exception as e:
            logger.warning(f"Error detecting file format: {e}")
            return None

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

            # Check if already posted - check ALL transactions including unmatched
            # This catches transactions that are in Opera but don't match any customer/supplier
            if check_posted:
                is_posted, posted_reason = self._is_already_posted(txn)
                if is_posted:
                    txn.is_duplicate = True
                    # For repeat entries, keep the action for visibility in Repeat Entries tab
                    if txn.action == 'repeat_entry':
                        txn.skip_reason = f'Already posted: {posted_reason}'
                    else:
                        txn.action = 'skip'
                        txn.skip_reason = f'Already posted: {posted_reason}'

    def import_transaction(self, txn: BankTransaction, validate_only: bool = False) -> ImportResult:
        """
        Import a single transaction to Opera

        Args:
            txn: Transaction to import
            validate_only: If True, only validate without posting

        Returns:
            ImportResult
        """
        # Use manual override if user selected different account
        account_to_use = txn.manual_account or txn.matched_account

        if txn.action == 'sales_receipt':
            # Import as sales receipt
            # Use subcategory as payment method (e.g., 'Funds Transfer', 'Counter Credit')
            payment_method = txn.subcategory if txn.subcategory else 'BACS'
            logger.info(f"BANK_IMPORT_DEBUG: Importing SALES_RECEIPT - account={account_to_use}, "
                       f"amount={txn.amount}, abs_amount={txn.abs_amount}, name={txn.matched_name}")
            result = self.opera_import.import_sales_receipt(
                bank_account=self.bank_code,
                customer_account=account_to_use,
                amount_pounds=txn.abs_amount,
                reference=txn.reference,
                post_date=txn.date,
                input_by='BANK_IMPORT',
                payment_method=payment_method[:20],
                validate_only=validate_only
            )
        elif txn.action == 'purchase_payment':
            # Import as purchase payment
            logger.info(f"BANK_IMPORT_DEBUG: Importing PURCHASE_PAYMENT - account={account_to_use}, "
                       f"amount={txn.amount}, abs_amount={txn.abs_amount}, name={txn.matched_name}")
            result = self.opera_import.import_purchase_payment(
                bank_account=self.bank_code,
                supplier_account=account_to_use,
                amount_pounds=txn.abs_amount,
                reference=txn.reference,
                post_date=txn.date,
                input_by='BANK_IMPORT',
                validate_only=validate_only
            )
        elif txn.action == 'sales_refund':
            # Import as sales refund (payment to customer)
            logger.info(f"BANK_IMPORT_DEBUG: Importing SALES_REFUND - account={account_to_use}, "
                       f"amount={txn.amount}, abs_amount={txn.abs_amount}, name={txn.matched_name}")
            result = self.opera_import.import_sales_refund(
                bank_account=self.bank_code,
                customer_account=account_to_use,
                amount_pounds=txn.abs_amount,
                reference=txn.reference,
                post_date=txn.date,
                input_by='BANK_IMPORT',
                validate_only=validate_only
            )
        elif txn.action == 'purchase_refund':
            # Import as purchase refund (receipt from supplier)
            logger.info(f"BANK_IMPORT_DEBUG: Importing PURCHASE_REFUND - account={account_to_use}, "
                       f"amount={txn.amount}, abs_amount={txn.abs_amount}, name={txn.matched_name}")
            result = self.opera_import.import_purchase_refund(
                bank_account=self.bank_code,
                supplier_account=account_to_use,
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

        # Update at_refer with fingerprint for duplicate prevention (if successful)
        if result.success and not validate_only and self.use_fingerprinting and txn.fingerprint:
            self._store_import_fingerprint(txn, result)

        return result

    def _store_import_fingerprint(self, txn: BankTransaction, result: ImportResult) -> None:
        """
        Store import fingerprint in at_refer field after successful import.

        This enables definitive duplicate detection for future imports.

        Args:
            txn: The imported transaction
            result: The import result (contains entry number)
        """
        if not txn.fingerprint:
            return

        try:
            # Get the entry number from the result
            entry_number = None
            if hasattr(result, 'entry_number'):
                entry_number = result.entry_number
            elif hasattr(result, 'details') and isinstance(result.details, dict):
                entry_number = result.details.get('entry_number')

            if not entry_number:
                logger.debug("No entry number in result, cannot store fingerprint")
                return

            # Update at_refer to include the fingerprint
            # Format: BKIMP:{hash8}:{YYYYMMDD}
            update_sql = f"""
                UPDATE atran WITH (ROWLOCK)
                SET at_refer = '{txn.fingerprint[:20]}'
                WHERE at_entry = '{entry_number}'
                AND at_acnt = '{self.bank_code}'
            """
            self.sql_connector.execute_query(update_sql)
            logger.debug(f"Stored fingerprint {txn.fingerprint} for entry {entry_number}")

        except Exception as e:
            # Don't fail the import if fingerprint storage fails
            logger.warning(f"Could not store import fingerprint: {e}")

    def import_file(self, filepath: str, validate_only: bool = False,
                    skip_bank_validation: bool = False) -> BankImportResult:
        """
        Import a bank statement CSV file

        Args:
            filepath: Path to CSV file
            validate_only: If True, only validate without posting
            skip_bank_validation: If True, skip bank account validation

        Returns:
            BankImportResult with details of import
        """
        result = BankImportResult(filename=filepath)

        # Validate bank account from CSV matches configured bank
        if not skip_bank_validation:
            is_valid, message, detected_code = self.validate_bank_account_from_csv(filepath)
            if not is_valid:
                result.errors.append(f"Bank validation failed: {message}")
                logger.error(f"Bank validation failed for {filepath}: {message}")
                return result
            logger.info(message)

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

    def get_audit_report_for_approval(self, filepath: str) -> Tuple[BankImportAuditReport, BankImportResult]:
        """
        Generate an audit report for user approval before import.

        This is the first step in a two-step import process:
        1. Call this to get the audit report and display to user
        2. If user approves, call import_approved() with the result

        Args:
            filepath: Path to CSV file

        Returns:
            Tuple of (BankImportAuditReport, BankImportResult)
        """
        # Preview the file (no actual import)
        result = self.preview_file(filepath)

        # Generate audit report
        audit_report = BankImportAuditReport(result, self.bank_code)

        return audit_report, result

    def import_approved(self, result: BankImportResult, validate_only: bool = False) -> BankImportResult:
        """
        Execute import after user has approved the audit report.

        This is the second step after get_audit_report_for_approval().
        Only imports the transactions that were marked for import in the preview.

        Args:
            result: BankImportResult from get_audit_report_for_approval()
            validate_only: If True, only validate without actual posting

        Returns:
            Updated BankImportResult with import results
        """
        # Import only the matched transactions
        for txn in result.transactions:
            if txn.action in ('sales_receipt', 'purchase_payment'):
                try:
                    import_result = self.import_transaction(txn, validate_only)
                    if import_result.success:
                        result.imported_transactions += 1
                        txn.imported = True
                    else:
                        result.errors.append(
                            f"Row {txn.row_number}: {import_result.message}"
                        )
                except Exception as e:
                    result.errors.append(f"Row {txn.row_number}: {str(e)}")

        return result

    def import_interactive(self, filepath: str, auto_approve: bool = False) -> Tuple[bool, BankImportResult, str]:
        """
        Interactive import with audit report display and confirmation prompt.

        For CLI usage, this method:
        1. Generates and displays the audit report
        2. Prompts user for confirmation (unless auto_approve=True)
        3. If approved, executes the import

        Args:
            filepath: Path to CSV file
            auto_approve: If True, skip confirmation prompt and proceed

        Returns:
            Tuple of (approved: bool, result: BankImportResult, report_text: str)
        """
        # Generate audit report
        audit_report, result = self.get_audit_report_for_approval(filepath)

        # Format report for display
        report_text = audit_report.format_report(include_skipped=True, include_details=True)

        # Check if there's anything to import
        summary = audit_report.get_summary()
        if summary['will_import'] == 0:
            return False, result, report_text

        # If auto-approve, proceed with import
        if auto_approve:
            result = self.import_approved(result)
            return True, result, report_text

        # Otherwise, return for manual approval
        # Caller should display report_text and ask for confirmation
        # Then call import_approved() if user confirms
        return None, result, report_text  # None indicates waiting for user decision

    def print_audit_report(self, filepath: str) -> BankImportAuditReport:
        """
        Print the audit report to stdout for CLI usage.

        Args:
            filepath: Path to CSV file

        Returns:
            The BankImportAuditReport object for further processing
        """
        audit_report, result = self.get_audit_report_for_approval(filepath)
        print(audit_report.format_report())
        return audit_report
