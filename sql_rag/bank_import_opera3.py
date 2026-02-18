"""
Bank Statement Import Module for Opera 3 (FoxPro)

Processes bank statement CSV files and matches transactions against
Opera 3 customer/supplier master files stored in FoxPro DBF format.

Uses the shared matching module (bank_matching.py) for fuzzy matching logic.

NOW SUPPORTS FULL IMPORTS (parity with SQL SE version):
- Two-step workflow: audit report -> approval -> import
- Direct DBF writes with file-level locking (equivalent to SQL SE row locks)
- Same transaction pattern as SQL SE (aentry, atran, ntran, ptran/stran, palloc/salloc)

WARNING: Direct DBF writes bypass Opera's application-level locking.
Use with caution, preferably when Opera 3 is not running.

Features:
- Comprehensive audit report with approval workflow
- Control account lookup from customer/supplier profiles
- Enhanced ambiguous match handling with score difference check
- Customer refund detection for payments
- Bank account validation from nbank table
- File-level locking with timeout (equivalent to SQL SE LOCK_TIMEOUT)
"""

import csv
import re
import logging
from datetime import datetime, date
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Tuple
from pathlib import Path

from sql_rag.bank_matching import (
    BankMatcher, MatchCandidate, MatchResult,
    create_match_candidate_from_dict
)
from sql_rag.opera3_foxpro import Opera3Reader, Opera3System

# Import the new FoxPro import module
try:
    from sql_rag.opera3_foxpro_import import Opera3FoxProImport, Opera3ImportResult, FileLockTimeout
    FOXPRO_IMPORT_AVAILABLE = True
except ImportError:
    FOXPRO_IMPORT_AVAILABLE = False
    Opera3FoxProImport = None
    Opera3ImportResult = None
    FileLockTimeout = None

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

    # Status
    action: Optional[str] = None  # 'sales_receipt', 'purchase_payment', 'skip', 'repeat_entry', 'bank_transfer'
    skip_reason: Optional[str] = None

    # Bank transfer details (for inter-bank transfers)
    bank_transfer_details: Optional[Dict[str, Any]] = None  # dest_bank, reference, comment, cashbook_type

    # Repeat entry detection
    repeat_entry_ref: Optional[str] = None  # arhead.ae_entry reference
    repeat_entry_desc: Optional[str] = None  # Description from arhead
    repeat_entry_next_date: Optional[date] = None  # ae_nxtpost

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
class MatchPreviewResult:
    """Results of a bank statement matching preview"""
    filename: str
    total_transactions: int = 0
    matched_transactions: int = 0
    skipped_transactions: int = 0
    already_posted: int = 0
    errors: List[str] = field(default_factory=list)
    transactions: List[BankTransaction] = field(default_factory=list)

    @property
    def match_rate(self) -> float:
        if self.total_transactions == 0:
            return 0.0
        return self.matched_transactions / self.total_transactions * 100


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
    control_account: str = ""  # Control account that would be used


class Opera3AuditReport:
    """
    Generates a detailed audit report for Opera 3 bank statement matching.

    Displays all transactions with their proposed actions, allowing
    the user to review before manual import via Opera.
    """

    def __init__(self, result: MatchPreviewResult, data_path: str, bank_code: str = ""):
        self.result = result
        self.data_path = data_path
        self.bank_code = bank_code
        self.lines: List[AuditReportLine] = []
        self._generate_lines()

    def _generate_lines(self):
        """Generate audit report lines from match result"""
        for txn in self.result.transactions:
            if txn.action == 'sales_receipt':
                action = 'RECEIPT'
                status = 'MATCHED'
                matched_to = f"{txn.matched_account} ({txn.matched_name})"
            elif txn.action == 'purchase_payment':
                action = 'PAYMENT'
                status = 'MATCHED'
                matched_to = f"{txn.matched_account} ({txn.matched_name})"
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
        matched = [l for l in self.lines if l.status == 'MATCHED']
        receipts = [l for l in matched if l.action == 'RECEIPT']
        payments = [l for l in matched if l.action == 'PAYMENT']
        skipped = [l for l in self.lines if l.status == 'SKIP']

        return {
            'total_transactions': len(self.lines),
            'matched': len(matched),
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
            'net_effect': sum(l.amount for l in matched)
        }

    def _group_skip_reasons(self, skipped_lines: List[AuditReportLine]) -> Dict[str, int]:
        """Group skipped transactions by reason"""
        reasons = {}
        for line in skipped_lines:
            reason = line.reason or 'Unknown'
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
        lines.append("OPERA 3 BANK STATEMENT AUDIT REPORT")
        lines.append("=" * 80)
        lines.append(f"File: {self.result.filename}")
        lines.append(f"Data Path: {self.data_path}")
        if self.bank_code:
            lines.append(f"Bank Account: {self.bank_code}")
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")
        lines.append("NOTE: This is a PREVIEW only. Import via Opera 3 standard mechanisms.")
        lines.append("")

        # Summary Section
        lines.append("-" * 80)
        lines.append("SUMMARY")
        lines.append("-" * 80)
        lines.append(f"Total transactions in file:     {summary['total_transactions']:>6}")
        lines.append(f"Matched (ready for import):     {summary['matched']:>6}")
        lines.append(f"  - Receipts (money in):        {summary['receipts']['count']:>6}  £{summary['receipts']['total']:>12,.2f}")
        lines.append(f"  - Payments (money out):       {summary['payments']['count']:>6}  £{summary['payments']['total']:>12,.2f}")
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

        # Matched Transactions
        if include_details:
            matched = [l for l in self.lines if l.status == 'MATCHED']
            if matched:
                lines.append("-" * 80)
                lines.append("MATCHED TRANSACTIONS (Ready for Opera 3 Import)")
                lines.append("-" * 80)
                lines.append(f"{'Row':>4} {'Date':<10} {'Amount':>12} {'Type':<8} {'Name':<25} {'Matched To':<30} {'Score':<5}")
                lines.append("-" * 80)
                for l in matched:
                    lines.append(
                        f"{l.row:>4} {l.date:<10} £{l.amount:>10,.2f} {l.action:<8} "
                        f"{l.name:<25} {l.matched_to:<30} {l.match_score:.0%}"
                    )
                lines.append("")

        # Skipped Transactions
        if include_details and include_skipped:
            skipped = [l for l in self.lines if l.status == 'SKIP']
            if skipped:
                lines.append("-" * 80)
                lines.append("SKIPPED TRANSACTIONS")
                lines.append("-" * 80)
                lines.append(f"{'Row':>4} {'Date':<10} {'Amount':>12} {'Name':<20} {'Reason':<35}")
                lines.append("-" * 80)
                for l in skipped:
                    reason_short = l.reason[:35] if l.reason else '-'
                    lines.append(
                        f"{l.row:>4} {l.date:<10} £{l.amount:>10,.2f} "
                        f"{l.name:<20} {reason_short}"
                    )
                lines.append("")

        # Footer
        lines.append("=" * 80)
        if summary['matched'] > 0:
            lines.append("USE OPERA 3 STANDARD IMPORT TO PROCESS MATCHED TRANSACTIONS")
        else:
            lines.append("NO TRANSACTIONS MATCHED")
        lines.append("=" * 80)

        return "\n".join(lines)

    def format_json(self) -> Dict[str, Any]:
        """Format the audit report as JSON for API responses"""
        summary = self.get_summary()
        return {
            'filename': self.result.filename,
            'data_path': self.data_path,
            'bank_code': self.bank_code,
            'generated': datetime.now().isoformat(),
            'note': 'Preview only - import via Opera 3 standard mechanisms',
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


class BankStatementMatcherOpera3:
    """
    Bank statement matcher for Opera 3 (FoxPro) data.

    Matches bank statement transactions against Opera 3 customer/supplier
    master files using the shared fuzzy matching algorithm.

    This is a read-only matcher for preview purposes. Actual imports
    should be done through Opera's standard import mechanisms.

    Usage:
        matcher = BankStatementMatcherOpera3(r"C:\\Apps\\O3 Server VFP\\COMPANY01")
        result = matcher.preview_file("/path/to/bank_statement.csv")
        print(matcher.get_preview_summary(result))
    """

    # Names/patterns to skip (not real customer/supplier names)
    SKIP_PATTERNS = [
        r'^GC\s+C\d+',  # GoCardless references
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
                 data_path: str,
                 min_match_score: float = 0.6,
                 use_extended_fields: bool = True):
        """
        Initialize Opera 3 bank statement matcher.

        Args:
            data_path: Path to Opera 3 company data folder (e.g., C:\\Apps\\O3 Server VFP\\COMPANY01)
            min_match_score: Minimum fuzzy match score (0-1) to consider a match
            use_extended_fields: Use payee/search keys for matching (default True)
        """
        self.data_path = Path(data_path)
        self.min_match_score = min_match_score
        self.use_extended_fields = use_extended_fields

        # Initialize Opera 3 reader
        self.reader = Opera3Reader(str(self.data_path))

        # Initialize shared matcher
        self.matcher = BankMatcher(min_score=self.min_match_score)

        # Load master files
        self._load_master_files()

    @classmethod
    def from_company_code(cls,
                          company_code: str,
                          base_path: str = r"C:\Apps\O3 Server VFP",
                          **kwargs) -> 'BankStatementMatcherOpera3':
        """
        Create matcher from company code using Opera 3 system.

        Args:
            company_code: Opera 3 company code (from seqco.dbf)
            base_path: Base path to Opera 3 installation
            **kwargs: Additional arguments passed to __init__

        Returns:
            BankStatementMatcherOpera3 instance
        """
        system = Opera3System(base_path)
        data_path = system.get_company_data_path(company_code)
        if not data_path:
            raise FileNotFoundError(f"Company data not found: {company_code}")
        return cls(str(data_path), **kwargs)

    def _load_master_files(self):
        """Load customer and supplier data from Opera 3 DBF files"""
        customers: Dict[str, MatchCandidate] = {}
        suppliers: Dict[str, MatchCandidate] = {}

        # Load customers from sname.dbf
        try:
            customer_records = self.reader.read_table("sname")
            for record in customer_records:
                candidate = create_match_candidate_from_dict(record, is_supplier=False)
                if candidate.account and candidate.primary_name:
                    customers[candidate.account] = candidate
            logger.info(f"Loaded {len(customers)} customers from Opera 3")
        except FileNotFoundError:
            logger.warning("Customer master file (sname.dbf) not found")
        except Exception as e:
            logger.error(f"Error loading customers: {e}")

        # Load suppliers from pname.dbf
        try:
            supplier_records = self.reader.read_table("pname")
            for record in supplier_records:
                candidate = create_match_candidate_from_dict(record, is_supplier=True)
                if candidate.account and candidate.primary_name:
                    suppliers[candidate.account] = candidate
            logger.info(f"Loaded {len(suppliers)} suppliers from Opera 3")
        except FileNotFoundError:
            logger.warning("Supplier master file (pname.dbf) not found")
        except Exception as e:
            logger.error(f"Error loading suppliers: {e}")

        # Load into shared matcher
        self.matcher.load_customers(customers)
        self.matcher.load_suppliers(suppliers)

    def _parse_memo(self, memo: str) -> tuple:
        """
        Parse memo field to extract name and reference.

        Memo format: "NAME                 \\tREFERENCE"

        Returns:
            Tuple of (name, reference)
        """
        if not memo:
            return "", ""

        parts = memo.split('\t')
        name = parts[0].strip() if parts else ""
        reference = parts[1].strip() if len(parts) > 1 else ""

        return name, reference

    def _should_skip(self, name: str, subcategory: str) -> Optional[str]:
        """
        Check if transaction should be skipped.

        Returns:
            Skip reason if should skip, None otherwise
        """
        if subcategory in self.SKIP_SUBCATEGORIES:
            return f"Subcategory '{subcategory}' excluded"

        for pattern in self.SKIP_PATTERNS:
            if re.match(pattern, name, re.IGNORECASE):
                return f"Name matches skip pattern: {pattern}"

        if not name:
            return "Empty name"

        return None

    def _check_repeat_entry(self, txn: BankTransaction, bank_code: str) -> bool:
        """Check if transaction matches an UNPOSTED repeat entry in arhead/arline.

        Compares bank statement transactions with Opera 3's repeat entries to detect
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
            # Amount in pence for comparison with arline.at_value
            amount_pence = int(txn.amount * 100)  # Keep sign (receipts positive, payments negative)

            # Read repeat entry tables
            arhead_records = self.reader.read_table("arhead")
            arline_records = self.reader.read_table("arline")

            if not arhead_records or not arline_records:
                return False

            # Filter arhead for this bank account with remaining posts
            bank_code_upper = bank_code.strip().upper()
            matching_headers = []
            for h in arhead_records:
                ae_acnt = str(h.get('AE_ACNT', '')).strip().upper()
                ae_posted = float(h.get('AE_POSTED', 0) or 0)
                ae_topost = float(h.get('AE_TOPOST', 0) or 0)

                if ae_acnt == bank_code_upper:
                    # Only unposted entries (ae_topost=0 for unlimited or ae_posted < ae_topost)
                    if ae_topost == 0 or ae_posted < ae_topost:
                        matching_headers.append(h)

            if not matching_headers:
                return False

            # Find lines matching amount
            for header in matching_headers:
                ae_entry = str(header.get('AE_ENTRY', '')).strip()

                for line in arline_records:
                    at_entry = str(line.get('AT_ENTRY', '')).strip()
                    at_acnt = str(line.get('AT_ACNT', '')).strip().upper()

                    if at_entry == ae_entry and at_acnt == bank_code_upper:
                        at_value = int(float(line.get('AT_VALUE', 0) or 0))

                        # Check amount match (10p tolerance)
                        if abs(at_value - amount_pence) < 10:
                            # Check date proximity
                            next_post_raw = header.get('AE_NXTPOST')
                            if next_post_raw:
                                if isinstance(next_post_raw, str):
                                    try:
                                        next_post_date = datetime.strptime(next_post_raw[:10], '%Y-%m-%d').date()
                                    except ValueError:
                                        next_post_date = None
                                elif hasattr(next_post_raw, 'date'):
                                    next_post_date = next_post_raw.date()
                                else:
                                    next_post_date = next_post_raw

                                if next_post_date:
                                    date_diff = abs((txn.date - next_post_date).days)
                                    if date_diff > 5:
                                        continue  # Date too far, check next

                                    # Found matching repeat entry
                                    txn.action = 'repeat_entry'
                                    txn.skip_reason = None
                                    txn.repeat_entry_ref = ae_entry
                                    txn.repeat_entry_desc = str(header.get('AE_DESC', '')).strip() or str(line.get('AT_COMMENT', '')).strip()
                                    txn.repeat_entry_next_date = next_post_date

                                    logger.info(f"Repeat entry matched: '{txn.name}' -> {txn.repeat_entry_ref} ({txn.repeat_entry_desc})")
                                    return True

        except Exception as e:
            logger.warning(f"Error checking repeat entries: {e}")

        return False

    def _match_transaction(self, txn: BankTransaction, bank_code: str = "BC010") -> None:
        """
        Match transaction to customer or supplier using shared matcher.

        Updates transaction with match results.

        Enhanced features (parity with SQL SE):
        - Repeat entry detection (checked first)
        - Score difference check for ambiguous matches
        - Customer refund detection for payments
        """
        # Step 0: Check repeat entries first - these are handled by Opera's auto-post
        if self._check_repeat_entry(txn, bank_code):
            return  # Matched as repeat entry - no further matching needed

        # Use shared matcher
        cust_result = self.matcher.match_customer(txn.name)
        supp_result = self.matcher.match_supplier(txn.name)

        # Handle ambiguous matches with score difference check
        if cust_result.is_match and supp_result.is_match:
            score_diff = abs(cust_result.score - supp_result.score)

            # If scores are very similar (<0.15 difference), it's truly ambiguous
            if score_diff < 0.15:
                txn.action = 'skip'
                txn.skip_reason = f'Matches both customer ({cust_result.name}) and supplier ({supp_result.name}) - ambiguous'
                return

            # Otherwise, use the better match
            if txn.is_receipt:
                # For receipts, prefer customer match
                if cust_result.score >= supp_result.score:
                    txn.match_type = 'customer'
                    txn.matched_account = cust_result.account
                    txn.matched_name = cust_result.name
                    txn.match_score = cust_result.score
                    txn.action = 'sales_receipt'
                    return
            else:
                # For payments, if customer score is significantly higher, flag as possible refund
                if cust_result.score > supp_result.score:
                    txn.action = 'skip'
                    txn.skip_reason = f'Matches both - customer score ({cust_result.score:.2f}) higher than supplier ({supp_result.score:.2f}) for payment (possible customer refund?)'
                    txn.matched_name = cust_result.name
                    txn.match_score = cust_result.score
                    return

        # Determine best match based on transaction direction
        if txn.is_receipt:
            if cust_result.is_match:
                txn.match_type = 'customer'
                txn.matched_account = cust_result.account
                txn.matched_name = cust_result.name
                txn.match_score = cust_result.score
                txn.action = 'sales_receipt'
            else:
                txn.action = 'skip'
                txn.skip_reason = f'No customer match found (best score: {cust_result.score:.2f})'
        else:
            # Payment - check supplier first
            if supp_result.is_match:
                txn.match_type = 'supplier'
                txn.matched_account = supp_result.account
                txn.matched_name = supp_result.name
                txn.match_score = supp_result.score
                txn.action = 'purchase_payment'
            elif cust_result.is_match:
                # Payment but matches customer - possible refund
                txn.action = 'skip'
                txn.skip_reason = f'No supplier match but matches customer {cust_result.name} ({cust_result.score:.2f}) - possible refund to customer'
                txn.matched_name = cust_result.name
                txn.match_score = cust_result.score
            else:
                txn.action = 'skip'
                txn.skip_reason = f'No supplier match found (best score: {supp_result.score:.2f})'

    def parse_csv(self, filepath: str) -> List[BankTransaction]:
        """
        Parse bank statement CSV file.

        Args:
            filepath: Path to CSV file

        Returns:
            List of BankTransaction objects
        """
        transactions = []

        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for row_num, row in enumerate(reader, start=2):
                if not row.get('Date') or not row.get('Amount'):
                    continue

                try:
                    txn_date = datetime.strptime(row['Date'].strip(), '%d/%m/%Y').date()
                except ValueError:
                    continue

                try:
                    amount = float(row['Amount'].replace(',', ''))
                except ValueError:
                    continue

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

    def _is_already_posted(self, txn: BankTransaction, bank_code: str = "BC010") -> Tuple[bool, str]:
        """
        Check if transaction has already been posted to Opera 3.

        Checks multiple tables to prevent duplicates:
        1. atran (cashbook) - bank account transactions
        2. ptran (purchase ledger) - for supplier payments
        3. stran (sales ledger) - for customer receipts

        Args:
            txn: Transaction to check
            bank_code: Bank account code

        Returns:
            Tuple of (is_posted, reason) where reason explains where duplicate was found
        """
        amount_pence = int(txn.abs_amount * 100)

        def dates_equal(db_date, txn_date) -> bool:
            """Compare dates that may be strings or date objects."""
            if db_date is None or txn_date is None:
                return False
            # Convert both to string format for comparison
            if isinstance(db_date, str):
                db_str = db_date
            else:
                db_str = db_date.strftime('%Y-%m-%d') if hasattr(db_date, 'strftime') else str(db_date)
            if isinstance(txn_date, str):
                txn_str = txn_date
            else:
                txn_str = txn_date.strftime('%Y-%m-%d') if hasattr(txn_date, 'strftime') else str(txn_date)
            return db_str == txn_str

        try:
            # Check 1: Cashbook (atran) - amounts in PENCE
            # Note: Opera3Reader returns UPPERCASE keys and dates as strings
            atran_records = self.reader.read_table("atran")
            for record in atran_records:
                at_acnt = record.get('AT_ACNT', record.get('at_acnt', '')).strip()
                at_pstdate = record.get('AT_PSTDATE', record.get('at_pstdate'))
                at_value = record.get('AT_VALUE', record.get('at_value', 0))
                if (at_acnt == bank_code and
                    dates_equal(at_pstdate, txn.date) and
                    abs(abs(at_value) - amount_pence) < 1):
                    return True, "Already in cashbook (atran)"

            # Check 2: Purchase Ledger (ptran) - for supplier payments
            if txn.action == 'purchase_payment' and txn.matched_account:
                ptran_records = self.reader.read_table("ptran")
                for record in ptran_records:
                    pt_account = record.get('PT_ACCOUNT', record.get('pt_account', '')).strip()
                    pt_trdate = record.get('PT_TRDATE', record.get('pt_trdate'))
                    pt_trvalue = record.get('PT_TRVALUE', record.get('pt_trvalue', 0))
                    pt_trtype = record.get('PT_TRTYPE', record.get('pt_trtype', '')).strip()
                    if (pt_account == txn.matched_account and
                        dates_equal(pt_trdate, txn.date) and
                        abs(abs(pt_trvalue) - txn.abs_amount) < 0.01 and
                        pt_trtype == 'P'):
                        return True, f"Already in purchase ledger (ptran) for {txn.matched_account}"

            # Check 3: Sales Ledger (stran) - for customer receipts
            if txn.action == 'sales_receipt' and txn.matched_account:
                stran_records = self.reader.read_table("stran")
                for record in stran_records:
                    st_account = record.get('ST_ACCOUNT', record.get('st_account', '')).strip()
                    st_trdate = record.get('ST_TRDATE', record.get('st_trdate'))
                    st_trvalue = record.get('ST_TRVALUE', record.get('st_trvalue', 0))
                    st_trtype = record.get('ST_TRTYPE', record.get('st_trtype', '')).strip()
                    if (st_account == txn.matched_account and
                        dates_equal(st_trdate, txn.date) and
                        abs(abs(st_trvalue) - txn.abs_amount) < 0.01 and
                        st_trtype == 'R'):
                        return True, f"Already in sales ledger (stran) for {txn.matched_account}"

        except FileNotFoundError:
            # If table doesn't exist, can't be a duplicate
            pass
        except Exception as e:
            logger.warning(f"Error checking for duplicates: {e}")

        return False, ""

    def process_transactions(self, transactions: List[BankTransaction],
                            check_duplicates: bool = True,
                            bank_code: str = "BC010") -> None:
        """
        Process transactions: skip checks, matching, and duplicate detection.

        Args:
            transactions: List of transactions to process
            check_duplicates: Whether to check for already-posted transactions
            bank_code: Bank account code for duplicate checking
        """
        for txn in transactions:
            skip_reason = self._should_skip(txn.name, txn.subcategory)
            if skip_reason:
                txn.action = 'skip'
                txn.skip_reason = skip_reason
                continue

            self._match_transaction(txn, bank_code)

            # Check for duplicates after matching (including repeat entries)
            if check_duplicates and txn.action in ('sales_receipt', 'purchase_payment', 'repeat_entry'):
                is_posted, reason = self._is_already_posted(txn, bank_code)
                if is_posted:
                    txn.action = 'skip'
                    txn.skip_reason = reason

    def preview_file(self, filepath: str) -> MatchPreviewResult:
        """
        Preview what would be matched from a bank statement.

        Args:
            filepath: Path to CSV file

        Returns:
            MatchPreviewResult with details
        """
        result = MatchPreviewResult(filename=filepath)

        try:
            transactions = self.parse_csv(filepath)
            result.total_transactions = len(transactions)
            result.transactions = transactions
        except Exception as e:
            result.errors.append(f"Error parsing CSV: {str(e)}")
            return result

        self.process_transactions(transactions)

        for txn in transactions:
            if txn.action in ('sales_receipt', 'purchase_payment'):
                result.matched_transactions += 1

        result.skipped_transactions = result.total_transactions - result.matched_transactions

        return result

    def get_preview_summary(self, result: MatchPreviewResult) -> str:
        """
        Generate a human-readable preview summary.

        Args:
            result: MatchPreviewResult from preview_file()

        Returns:
            Formatted summary string
        """
        lines = [
            f"Bank Statement Match Preview (Opera 3): {result.filename}",
            f"=" * 60,
            f"Data Path: {self.data_path}",
            f"Total transactions: {result.total_transactions}",
            f"Matched: {result.matched_transactions}",
            f"Skipped: {result.skipped_transactions}",
            f"Match rate: {result.match_rate:.1f}%",
            "",
            "Matched Transactions:",
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
                "Skipped Transactions:",
                "-" * 60
            ])
            for txn in result.transactions:
                if txn.action == 'skip':
                    lines.append(
                        f"  {txn.date} | {txn.name[:25]:25} | {txn.amount:>10,.2f} | "
                        f"{txn.skip_reason}"
                    )

        return "\n".join(lines)

    def generate_audit_report(self, result: MatchPreviewResult) -> Dict[str, Any]:
        """
        Generate a comprehensive audit report of matching results.

        Args:
            result: MatchPreviewResult from preview_file()

        Returns:
            Dictionary containing audit report data
        """
        matched = []
        not_matched = []
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

            if txn.action in ('sales_receipt', 'purchase_payment'):
                txn_data.update({
                    "action": txn.action,
                    "matched_account": txn.matched_account,
                    "matched_name": txn.matched_name,
                    "match_score": round(txn.match_score * 100) if txn.match_score else 0
                })
                matched.append(txn_data)
            else:
                reason = txn.skip_reason or "Unknown"

                if "Subcategory" in reason:
                    category = "Excluded subcategory"
                elif "skip pattern" in reason:
                    category = "Excluded pattern"
                elif "No customer match" in reason:
                    category = "No customer match"
                elif "No supplier match" in reason:
                    category = "No supplier match"
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
                not_matched.append(txn_data)

                if category not in by_reason:
                    by_reason[category] = {"count": 0, "total_amount": 0}
                by_reason[category]["count"] += 1
                by_reason[category]["total_amount"] += txn.abs_amount

        total_matched_amount = sum(t["abs_amount"] for t in matched)
        total_not_matched_amount = sum(t["abs_amount"] for t in not_matched)

        return {
            "summary": {
                "filename": result.filename,
                "data_path": str(self.data_path),
                "total_transactions": result.total_transactions,
                "matched_count": len(matched),
                "matched_amount": round(total_matched_amount, 2),
                "not_matched_count": len(not_matched),
                "not_matched_amount": round(total_not_matched_amount, 2),
                "match_rate": round(result.match_rate, 1)
            },
            "matched": matched,
            "not_matched": not_matched,
            "by_category": by_reason,
            "errors": result.errors
        }

    def export_audit_to_csv(self, result: MatchPreviewResult, filepath: str) -> str:
        """
        Export audit report to CSV file.

        Args:
            result: MatchPreviewResult from preview_file()
            filepath: Path to save CSV file

        Returns:
            Path to saved file
        """
        report = self.generate_audit_report(result)

        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            writer.writerow([
                'Status', 'Row', 'Date', 'Type', 'Amount', 'Subcategory',
                'Memo Name', 'Memo Reference', 'Matched Account', 'Matched Name',
                'Match Score %', 'Category', 'Reason'
            ])

            for txn in report["matched"]:
                writer.writerow([
                    'Matched',
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

            for txn in report["not_matched"]:
                writer.writerow([
                    'Not Matched',
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


    def get_supplier_control_account(self, supplier_account: str) -> str:
        """
        Get the creditors control account for a specific supplier.

        Looks up the supplier's profile (pn_sprfl) and gets the control account
        from the profile (pc_crdctrl). If blank or not found, returns company default.

        Args:
            supplier_account: Supplier account code (e.g., 'H031')

        Returns:
            Control account code (e.g., 'CA030')
        """
        default_control = 'CA030'  # Standard creditors control

        try:
            # Get supplier's profile code
            supplier_records = self.reader.read_table("pname")
            profile_code = None
            for record in supplier_records:
                acct = record.get('pn_account', '').strip()
                if acct.upper() == supplier_account.upper():
                    profile_code = record.get('pn_sprfl', '').strip()
                    break

            if not profile_code:
                return default_control

            # Look up control account from profile
            try:
                profile_records = self.reader.read_table("pprfls")
                for record in profile_records:
                    code = record.get('pc_code', '').strip()
                    if code.upper() == profile_code.upper():
                        control = record.get('pc_crdctrl', '').strip()
                        if control:
                            return control
                        break
            except FileNotFoundError:
                logger.debug("Profile table (pprfls.dbf) not found")

            return default_control

        except Exception as e:
            logger.error(f"Error getting supplier control account: {e}")
            return default_control

    def get_customer_control_account(self, customer_account: str) -> str:
        """
        Get the debtors control account for a specific customer.

        Looks up the customer's profile (sn_sprfl) and gets the control account
        from the profile (sc_dbtctrl). If blank or not found, returns company default.

        Args:
            customer_account: Customer account code (e.g., 'A001')

        Returns:
            Control account code (e.g., 'BB020')
        """
        default_control = 'BB020'  # Standard debtors control

        try:
            # Get customer's profile code
            customer_records = self.reader.read_table("sname")
            profile_code = None
            for record in customer_records:
                acct = record.get('sn_account', '').strip()
                if acct.upper() == customer_account.upper():
                    profile_code = record.get('sn_sprfl', '').strip()
                    break

            if not profile_code:
                return default_control

            # Look up control account from profile
            try:
                profile_records = self.reader.read_table("sprfls")
                for record in profile_records:
                    code = record.get('sc_code', '').strip()
                    if code.upper() == profile_code.upper():
                        control = record.get('sc_dbtctrl', '').strip()
                        if control:
                            return control
                        break
            except FileNotFoundError:
                logger.debug("Profile table (sprfls.dbf) not found")

            return default_control

        except Exception as e:
            logger.error(f"Error getting customer control account: {e}")
            return default_control

    def validate_bank_account(self, bank_code: str) -> bool:
        """
        Validate that a bank account exists in Opera 3.

        Args:
            bank_code: Bank account code (e.g., 'BC010')

        Returns:
            True if bank account exists, False otherwise
        """
        try:
            bank_records = self.reader.read_table("nbank")
            for record in bank_records:
                code = record.get('nb_acnt', '').strip()
                if code.upper() == bank_code.upper():
                    return True
            return False
        except FileNotFoundError:
            logger.debug("Bank table (nbank.dbf) not found")
            return True  # Allow if we can't validate
        except Exception as e:
            logger.error(f"Error validating bank account: {e}")
            return True  # Allow if we can't validate

    def get_audit_report_for_approval(self, filepath: str, bank_code: str = "") -> Tuple[Opera3AuditReport, MatchPreviewResult]:
        """
        Generate an audit report for user approval before import.

        This is the first step in a two-step workflow:
        1. Call this to get the audit report and display to user
        2. User reviews and either approves or rejects
        3. If approved, call import_approved() to execute the import

        Args:
            filepath: Path to CSV file
            bank_code: Optional bank account code for the report header

        Returns:
            Tuple of (Opera3AuditReport, MatchPreviewResult)
        """
        # Run matching preview
        result = self.preview_file(filepath)

        # Generate audit report
        report = Opera3AuditReport(result, str(self.data_path), bank_code)

        return report, result

    def import_approved(
        self,
        result: MatchPreviewResult,
        bank_code: str = "BC010",
        validate_only: bool = False
    ) -> MatchPreviewResult:
        """
        Import approved transactions into Opera 3 DBF files.

        This is the second step in the two-step workflow:
        1. get_audit_report_for_approval() to preview
        2. User approves
        3. This method to execute the import

        WARNING: Direct DBF writes bypass Opera's application-level locking.
        Ensure Opera 3 is not running, or use with caution.

        Args:
            result: MatchPreviewResult from get_audit_report_for_approval()
            bank_code: Bank account code (e.g., 'BC010')
            validate_only: If True, only validate without importing

        Returns:
            Updated MatchPreviewResult with import status

        Raises:
            ImportError: If dbf package not available
            FileLockTimeout: If files are locked by another process
        """
        if not FOXPRO_IMPORT_AVAILABLE:
            raise ImportError(
                "Opera 3 import requires the 'dbf' package. "
                "Install with: pip install dbf"
            )

        # Initialize importer
        importer = Opera3FoxProImport(str(self.data_path))

        imported_count = 0
        failed_count = 0
        import_errors = []

        skipped_duplicates = 0

        for txn in result.transactions:
            if txn.action == 'bank_transfer':
                # Bank transfers not yet implemented for Opera 3
                failed_count += 1
                txn.action = 'skip'
                txn.skip_reason = "Bank transfers are not supported for Opera 3 (FoxPro). Please use Opera SQL SE or post the transfer manually in Opera."
                import_errors.append(txn.skip_reason)
                logger.warning(f"Row {txn.row_number}: Bank transfer skipped - not implemented for Opera 3")
                continue

            if txn.action not in ('sales_receipt', 'purchase_payment'):
                continue

            # Just-in-time duplicate check - catches entries that appeared since statement was processed
            try:
                acct_type = 'customer' if txn.action == 'sales_receipt' else 'supplier'
                dup_check = importer.check_duplicate_before_posting(
                    bank_account=bank_code,
                    transaction_date=txn.date,
                    amount_pounds=txn.abs_amount,
                    account_code=txn.matched_account,
                    account_type=acct_type
                )
                if dup_check['is_duplicate']:
                    skipped_duplicates += 1
                    txn.action = 'skip'
                    txn.skip_reason = f"Skipped - {dup_check['details']}"
                    logger.warning(f"Row {txn.row_number}: Pre-posting duplicate detected - {dup_check['details']}")
                    continue
            except Exception as dup_err:
                logger.warning(f"Row {txn.row_number}: Pre-posting duplicate check failed: {dup_err}")

            try:
                if txn.action == 'purchase_payment':
                    # Import supplier payment
                    import_result = importer.import_purchase_payment(
                        bank_account=bank_code,
                        supplier_account=txn.matched_account,
                        amount_pounds=txn.abs_amount,
                        reference=txn.reference or txn.name[:20],
                        post_date=txn.date,
                        input_by="IMPORT",
                        validate_only=validate_only
                    )
                else:
                    # Import customer receipt
                    import_result = importer.import_sales_receipt(
                        bank_account=bank_code,
                        customer_account=txn.matched_account,
                        amount_pounds=txn.abs_amount,
                        reference=txn.reference or txn.name[:20],
                        post_date=txn.date,
                        input_by="IMPORT",
                        validate_only=validate_only
                    )

                if import_result.success:
                    imported_count += 1
                    txn.skip_reason = f"Imported: {import_result.entry_number}"
                    logger.info(
                        f"Imported {txn.action}: {txn.matched_account} "
                        f"£{txn.abs_amount:.2f} -> {import_result.entry_number}"
                    )
                else:
                    failed_count += 1
                    txn.action = 'skip'
                    txn.skip_reason = f"Import failed: {'; '.join(import_result.errors)}"
                    import_errors.extend(import_result.errors)
                    logger.error(f"Failed to import: {import_result.errors}")

            except FileLockTimeout as e:
                failed_count += 1
                txn.action = 'skip'
                txn.skip_reason = f"Lock timeout: {str(e)}"
                import_errors.append(str(e))
                logger.error(f"Lock timeout during import: {e}")

            except Exception as e:
                failed_count += 1
                txn.action = 'skip'
                txn.skip_reason = f"Error: {str(e)}"
                import_errors.append(str(e))
                logger.error(f"Error importing transaction: {e}")

        # Update result counts
        result.matched_transactions = imported_count
        result.errors.extend(import_errors)

        logger.info(
            f"Import complete: {imported_count} imported, "
            f"{failed_count} failed out of {len(result.transactions)} total"
        )

        return result

    def import_interactive(
        self,
        filepath: str,
        bank_code: str = "BC010",
        auto_approve: bool = False
    ) -> Tuple[bool, MatchPreviewResult, str]:
        """
        Interactive import with audit report display and approval prompt.

        Combines the two-step workflow into a single interactive method.

        Args:
            filepath: Path to CSV file
            bank_code: Bank account code
            auto_approve: If True, skip approval prompt and import directly

        Returns:
            Tuple of (approved: bool, result: MatchPreviewResult, message: str)
        """
        # Step 1: Generate audit report
        report, result = self.get_audit_report_for_approval(filepath, bank_code)

        # Print report
        print(report.format_report())

        # Count importable transactions
        importable = sum(
            1 for t in result.transactions
            if t.action in ('sales_receipt', 'purchase_payment')
        )

        if importable == 0:
            return False, result, "No transactions to import"

        # Step 2: Get approval
        if not auto_approve:
            print(f"\n{importable} transaction(s) ready to import.")
            response = input("Proceed with import? (yes/no): ").strip().lower()
            if response not in ('yes', 'y'):
                return False, result, "Import cancelled by user"

        # Step 3: Execute import
        print("\nExecuting import...")
        result = self.import_approved(result, bank_code)

        imported = sum(
            1 for t in result.transactions
            if t.skip_reason and 'Imported:' in t.skip_reason
        )

        message = f"Import complete: {imported} transactions imported"
        if result.errors:
            message += f", {len(result.errors)} errors"

        return True, result, message

    def import_file(
        self,
        filepath: str,
        bank_code: str = "BC010",
        validate_only: bool = False,
        check_duplicates: bool = True
    ) -> MatchPreviewResult:
        """
        Import all matched transactions from a CSV file.

        This is a convenience method that combines preview and import.
        For more control, use get_audit_report_for_approval() and import_approved().

        Args:
            filepath: Path to CSV file
            bank_code: Bank account code (e.g., 'BC010')
            validate_only: If True, only validate without importing
            check_duplicates: Whether to check for already-posted transactions

        Returns:
            MatchPreviewResult with import status
        """
        # Parse and process
        result = MatchPreviewResult(filename=filepath)
        transactions = self.parse_csv(filepath)
        result.total_transactions = len(transactions)
        result.transactions = transactions

        # Process with duplicate checking
        self.process_transactions(transactions, check_duplicates, bank_code)

        # Count matched
        for txn in transactions:
            if txn.action in ('sales_receipt', 'purchase_payment'):
                result.matched_transactions += 1

        result.skipped_transactions = result.total_transactions - result.matched_transactions

        if validate_only:
            return result

        # Import approved transactions
        return self.import_approved(result, bank_code, validate_only=False)

    def save_audit_report(
        self,
        result: MatchPreviewResult,
        base_path: str,
        bank_code: str = ""
    ) -> Dict[str, str]:
        """
        Save audit report to files (text and JSON).

        Args:
            result: MatchPreviewResult to save
            base_path: Base path for output files (without extension)
            bank_code: Bank account code for report header

        Returns:
            Dictionary with paths to saved files
        """
        import json
        from datetime import datetime

        # Generate report
        report = Opera3AuditReport(result, str(self.data_path), bank_code)

        # Save text report
        text_path = f"{base_path}_audit.txt"
        with open(text_path, 'w') as f:
            f.write(report.format_report())

        # Save JSON report
        json_path = f"{base_path}_audit.json"
        with open(json_path, 'w') as f:
            json.dump(report.format_json(), f, indent=2, default=str)

        # Save CSV summary
        csv_path = f"{base_path}_audit.csv"
        self.export_audit_to_csv(result, csv_path)

        logger.info(f"Audit reports saved: {text_path}, {json_path}, {csv_path}")

        return {
            'text': text_path,
            'json': json_path,
            'csv': csv_path
        }

    def get_audit_report_text(self, result: MatchPreviewResult, bank_code: str = "") -> str:
        """
        Get audit report as plain text string.

        Args:
            result: MatchPreviewResult to report on
            bank_code: Bank account code for report header

        Returns:
            Formatted text report
        """
        report = Opera3AuditReport(result, str(self.data_path), bank_code)
        return report.format_report()

    def print_audit_report(self, filepath: str, bank_code: str = "") -> Opera3AuditReport:
        """
        Print the audit report to stdout for CLI usage.

        Args:
            filepath: Path to CSV file
            bank_code: Optional bank account code for the report header

        Returns:
            Opera3AuditReport instance
        """
        report, _ = self.get_audit_report_for_approval(filepath, bank_code)
        print(report.format_report())
        return report

    def get_matched_transactions_for_export(self, result: MatchPreviewResult) -> List[Dict[str, Any]]:
        """
        Get matched transactions in a format suitable for Opera 3 import.

        Returns data that can be used to create an Opera 3 import file
        or for manual entry guidance.

        Args:
            result: MatchPreviewResult from preview_file()

        Returns:
            List of dictionaries with transaction details for import
        """
        export_data = []

        for txn in result.transactions:
            if txn.action not in ('sales_receipt', 'purchase_payment'):
                continue

            # Get control account for this transaction
            if txn.match_type == 'supplier':
                control_account = self.get_supplier_control_account(txn.matched_account)
            else:
                control_account = self.get_customer_control_account(txn.matched_account)

            export_data.append({
                'row': txn.row_number,
                'date': txn.date.strftime('%d/%m/%Y'),
                'date_iso': txn.date.isoformat(),
                'amount': txn.amount,
                'abs_amount': txn.abs_amount,
                'type': 'RECEIPT' if txn.action == 'sales_receipt' else 'PAYMENT',
                'ledger': 'SALES' if txn.match_type == 'customer' else 'PURCHASE',
                'account_code': txn.matched_account,
                'account_name': txn.matched_name,
                'control_account': control_account,
                'reference': txn.reference,
                'match_score': txn.match_score,
                'memo_name': txn.name
            })

        return export_data


def list_opera3_companies(base_path: str = r"C:\Apps\O3 Server VFP") -> List[Dict[str, str]]:
    """
    List available Opera 3 companies.

    Args:
        base_path: Base path to Opera 3 installation

    Returns:
        List of company dictionaries with code, name, and data_path
    """
    try:
        system = Opera3System(base_path)
        return system.get_companies()
    except Exception as e:
        logger.error(f"Error listing Opera 3 companies: {e}")
        return []
