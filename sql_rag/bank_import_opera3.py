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
from sql_rag.bank_import import extract_payee_name, extract_payee_name_full
from sql_rag.opera3_foxpro import Opera3Reader, Opera3System

# Import alias manager (optional — graceful fallback if not available)
try:
    from sql_rag.bank_aliases import BankAliasManager
    ALIAS_AVAILABLE = True
except ImportError:
    ALIAS_AVAILABLE = False
    BankAliasManager = None

# Import the new FoxPro import module
try:
    from sql_rag.opera3_foxpro_import import Opera3FoxProImport, Opera3ImportResult, FileLockTimeout
    FOXPRO_IMPORT_AVAILABLE = True
except ImportError:
    FOXPRO_IMPORT_AVAILABLE = False
    Opera3FoxProImport = None
    Opera3ImportResult = None
    FileLockTimeout = None

# Import write provider for automatic agent routing
try:
    from sql_rag.opera3_write_provider import get_opera3_writer, Opera3AgentRequired
    WRITE_PROVIDER_AVAILABLE = True
except ImportError:
    WRITE_PROVIDER_AVAILABLE = False
    get_opera3_writer = None
    Opera3AgentRequired = None

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
    action: Optional[str] = None  # 'sales_receipt', 'purchase_payment', 'sales_refund', 'purchase_refund', 'skip', 'repeat_entry', 'bank_transfer'
    skip_reason: Optional[str] = None
    match_source: Optional[str] = None  # 'alias', 'fuzzy', 'bank_account_number', etc.

    # Bank transfer details (for inter-bank transfers)
    bank_transfer_details: Optional[Dict[str, Any]] = None  # dest_bank, reference, comment, cashbook_type

    # Repeat entry detection
    repeat_entry_ref: Optional[str] = None  # arhead.ae_entry reference
    repeat_entry_desc: Optional[str] = None  # Description from arhead
    repeat_entry_next_date: Optional[date] = None  # ae_nxtpost
    repeat_entry_posted: Optional[int] = None  # ae_posted
    repeat_entry_total: Optional[int] = None  # ae_topost (0=unlimited)
    repeat_entry_freq: Optional[str] = None  # ae_freq (D/W/M/Q/Y)
    repeat_entry_every: Optional[int] = None  # ae_every

    # Duplicate detection
    is_duplicate: bool = False

    # Refund detection
    refund_credit_note: Optional[str] = None  # Credit note reference
    refund_credit_amount: Optional[float] = None  # Credit note amount

    # Advanced nominal analysis (project/department)
    project_code: Optional[str] = None  # Project code for nominal entries
    department_code: Optional[str] = None  # Department code for nominal entries

    # VAT code for nominal entries (e.g., '1' for standard rate)
    vat_code: Optional[str] = None

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

    # No hardcoded skip patterns — all transactions are checked against Opera.
    # The already-posted check and customer/supplier matching handle everything.
    SKIP_PATTERNS = []
    SKIP_SUBCATEGORIES = []

    def __init__(self,
                 data_path: str,
                 min_match_score: float = 0.6,
                 learn_threshold: float = 0.8,
                 use_extended_fields: bool = True,
                 alias_manager=None):
        """
        Initialize Opera 3 bank statement matcher.

        Args:
            data_path: Path to Opera 3 company data folder (e.g., C:\\Apps\\O3 Server VFP\\COMPANY01)
            min_match_score: Minimum fuzzy match score (0-1) to consider a match
            learn_threshold: Minimum score (0-1) to save as alias for future (default 0.8)
            use_extended_fields: Use payee/search keys for matching (default True)
            alias_manager: Optional BankAliasManager for alias lookups
        """
        self.data_path = Path(data_path)
        self.min_match_score = min_match_score
        self.use_extended_fields = use_extended_fields
        self.alias_manager = alias_manager
        self.learn_threshold = learn_threshold

        # Initialize Opera 3 reader
        self.reader = Opera3Reader(str(self.data_path))

        # Initialize shared matcher
        self.matcher = BankMatcher(min_score=self.min_match_score)

        # Load master files
        self._load_master_files()

        # Cache for other bank accounts (for transfer detection)
        self._other_banks = None

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

        # Load customers from sname.dbf (exclude dormant accounts)
        try:
            customer_records = self.reader.read_table("sname")
            for record in customer_records:
                # Skip dormant customers — cannot post to them
                if record.get("SN_DORMANT") or record.get("sn_dormant"):
                    continue
                candidate = create_match_candidate_from_dict(record, is_supplier=False)
                if candidate.account and candidate.primary_name:
                    customers[candidate.account] = candidate
            logger.info(f"Loaded {len(customers)} customers from Opera 3")
        except FileNotFoundError:
            logger.warning("Customer master file (sname.dbf) not found")
        except Exception as e:
            logger.error(f"Error loading customers: {e}")

        # Load suppliers from pname.dbf (exclude dormant accounts)
        try:
            supplier_records = self.reader.read_table("pname")
            for record in supplier_records:
                # Skip dormant suppliers — cannot post to them
                if record.get("PN_DORMANT") or record.get("pn_dormant"):
                    continue
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
            # Step 1: Check alias table first (fast path for previously learned names)
            if self.alias_manager:
                alias_match = self.alias_manager.lookup_repeat_entry_alias(txn.name, bank_code)
                if alias_match:
                    entry_ref = alias_match['entry_ref']
                    entry_desc = alias_match['entry_desc']
                    use_count = alias_match.get('use_count', 1)

                    # Validate the entry still exists and is active in Opera 3
                    arhead_records = self.reader.read_table("arhead")
                    valid_entry = None
                    bank_code_upper = bank_code.strip().upper()
                    for h in (arhead_records or []):
                        ae_entry = str(h.get('AE_ENTRY', '')).strip()
                        ae_acnt = str(h.get('AE_ACNT', '')).strip().upper()
                        ae_posted = float(h.get('AE_POSTED', 0) or 0)
                        ae_topost = float(h.get('AE_TOPOST', 0) or 0)
                        if ae_entry == entry_ref and ae_acnt == bank_code_upper:
                            if ae_topost == 0 or ae_posted < ae_topost:
                                valid_entry = h
                                break

                    if valid_entry:
                        # Parse next post date for tolerance check
                        next_post_raw = valid_entry.get('AE_NXTPOST')
                        next_post_date = None
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

                        # Don't match if transaction date is too far before the next_post_date
                        from datetime import timedelta
                        tolerance_days = 10
                        if next_post_date and txn.date < (next_post_date - timedelta(days=tolerance_days)):
                            logger.debug(f"Alias found for '{txn.name}' but transaction date {txn.date} is more than {tolerance_days} days before next_post_date {next_post_date} - skipping")
                        else:
                            txn.action = 'repeat_entry'
                            txn.skip_reason = None
                            txn.repeat_entry_ref = entry_ref
                            txn.repeat_entry_desc = entry_desc or str(valid_entry.get('AE_DESC', '')).strip()
                            txn.repeat_entry_next_date = next_post_date
                            txn.repeat_entry_posted = int(valid_entry.get('AE_POSTED', 0) or 0)
                            txn.repeat_entry_total = int(valid_entry.get('AE_TOPOST', 0) or 0)

                            freq = str(valid_entry.get('AE_FREQ', '')).strip().upper()
                            every = int(valid_entry.get('AE_EVERY', 1) or 1)
                            txn.repeat_entry_freq = freq
                            txn.repeat_entry_every = every

                            freq_map = {'D': 'Daily', 'W': 'Weekly', 'M': 'Monthly', 'Q': 'Quarterly', 'Y': 'Yearly'}
                            freq_desc = freq_map.get(freq, freq)
                            if every > 1:
                                freq_desc = f"Every {every} {freq_desc.lower()}s"

                            logger.info(f"Repeat entry matched (alias, {use_count} uses): '{txn.name}' -> {entry_ref} ({txn.repeat_entry_desc}) - {freq_desc}")
                            return True
                    else:
                        logger.debug(f"Alias found for '{txn.name}' -> {entry_ref} but entry no longer active, falling through to amount/ref match")

            # Step 2: Amount in pence for comparison with arline.at_value
            amount_pence_abs = abs(int(txn.amount * 100))

            # Build search terms from transaction name, reference, and memo
            # (parity with SE version's SQL LIKE matching)
            search_terms = []
            for text in [txn.name, txn.reference, txn.memo]:
                if text and len(text.strip()) >= 3:
                    clean = text.strip().upper()
                    # Extract key words (at least 3 chars)
                    words = [w for w in clean.split() if len(w) >= 3]
                    search_terms.extend(words[:3])  # Limit to first 3 words
            search_terms = search_terms[:5]  # Limit to 5 terms total

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

            # Score each header+line pair by amount match and reference match
            # (parity with SE version which uses amount_match and ref_match columns)
            candidates = []
            for header in matching_headers:
                ae_entry = str(header.get('AE_ENTRY', '')).strip()
                ae_desc = str(header.get('AE_DESC', '')).strip().upper()

                for line in arline_records:
                    at_entry = str(line.get('AT_ENTRY', '')).strip()
                    at_acnt = str(line.get('AT_ACNT', '')).strip().upper()

                    if at_entry == ae_entry and at_acnt == bank_code_upper:
                        at_value = int(float(line.get('AT_VALUE', 0) or 0))
                        at_comment = str(line.get('AT_COMMENT', '')).strip().upper()

                        # Check amount match (10p tolerance)
                        amount_match = abs(abs(at_value) - amount_pence_abs) < 10

                        # Check reference keyword match against ae_desc and at_comment
                        ref_match = False
                        if search_terms:
                            combined_text = f"{ae_desc} {at_comment}"
                            for term in search_terms:
                                if term in combined_text:
                                    ref_match = True
                                    break

                        if amount_match or ref_match:
                            candidates.append({
                                'header': header,
                                'line': line,
                                'amount_match': amount_match,
                                'ref_match': ref_match,
                                'ae_entry': ae_entry,
                            })

            if not candidates:
                logger.debug(f"No repeat entry match found for amount={amount_pence_abs}p or refs={search_terms} on bank {bank_code}")
                return False

            # Sort: prefer amount matches, then reference matches
            candidates.sort(key=lambda c: (0 if c['amount_match'] else 1,))

            # Try candidates in order, checking date proximity
            from datetime import timedelta
            for cand in candidates:
                header = cand['header']
                line = cand['line']
                ae_entry = cand['ae_entry']
                match_type = "amount" if cand['amount_match'] else ("reference" if cand['ref_match'] else "unknown")

                # Parse next post date
                next_post_raw = header.get('AE_NXTPOST')
                next_post_date = None
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

                # Don't match if transaction date is too far before the next_post_date
                tolerance_days = 10
                if next_post_date and txn.date < (next_post_date - timedelta(days=tolerance_days)):
                    logger.debug(f"Repeat entry {match_type} match found but transaction date {txn.date} is more than {tolerance_days} days before next_post_date {next_post_date} - skipping")
                    continue

                if next_post_date:
                    date_diff = abs((txn.date - next_post_date).days)
                    if date_diff > 5 and not cand['ref_match']:
                        # For amount-only matches, enforce 5-day proximity
                        continue

                # Found matching repeat entry
                txn.action = 'repeat_entry'
                txn.skip_reason = None
                txn.repeat_entry_ref = ae_entry
                txn.repeat_entry_desc = str(header.get('AE_DESC', '')).strip() or str(line.get('AT_COMMENT', '')).strip()
                txn.repeat_entry_next_date = next_post_date
                txn.repeat_entry_posted = int(header.get('AE_POSTED', 0) or 0)
                txn.repeat_entry_total = int(header.get('AE_TOPOST', 0) or 0)
                txn.repeat_entry_freq = str(header.get('AE_FREQ', '')).strip().upper()
                txn.repeat_entry_every = int(header.get('AE_EVERY', 1) or 1)

                logger.info(f"Repeat entry matched ({match_type}): '{txn.name}' -> {txn.repeat_entry_ref} ({txn.repeat_entry_desc})")
                return True

        except Exception as e:
            logger.warning(f"Error checking repeat entries: {e}")

        return False

    def _load_other_bank_accounts(self, exclude_bank: str = None) -> List[Dict[str, str]]:
        """Load other Opera bank accounts for transfer detection."""
        if self._other_banks is not None:
            return self._other_banks
        try:
            banks = self.reader.read_table("nbank")
            self._other_banks = []
            for b in banks:
                code = (b.get('nb_acnt') or b.get('nk_acnt', '')).strip()
                if code and code != exclude_bank:
                    self._other_banks.append({
                        'code': code,
                        'sort_code': (b.get('nb_sort') or b.get('nk_sort', '')).strip().replace(' ', '').replace('-', ''),
                        'account_number': (b.get('nb_number') or b.get('nk_number', '')).strip().replace(' ', '').replace('-', ''),
                        'name': (b.get('nb_name') or b.get('nk_name', '')).strip(),
                    })
            return self._other_banks
        except Exception as e:
            logger.warning(f"Could not load bank accounts for transfer detection: {e}")
            self._other_banks = []
            return self._other_banks

    def _check_bank_transfer(self, txn: BankTransaction, bank_code: str = "") -> bool:
        """Check if transaction is a transfer to/from another Opera bank account."""
        other_banks = self._load_other_bank_accounts(exclude_bank=bank_code)
        if not other_banks:
            return False

        search_text = f"{txn.name} {txn.reference} {txn.memo}".upper().replace('-', '').replace(' ', '')
        for bank in other_banks:
            if bank['account_number'] and len(bank['account_number']) >= 6:
                if bank['account_number'] in search_text:
                    txn.action = 'bank_transfer'
                    txn.match_type = 'bank_transfer'
                    txn.matched_account = bank['code']
                    txn.matched_name = bank['name'] or bank['code']
                    txn.match_score = 1.0
                    txn.match_source = 'bank_account_number'
                    txn.bank_transfer_details = {'dest_bank': bank['code']}
                    return True
            if bank['sort_code'] and len(bank['sort_code']) >= 6:
                if bank['sort_code'] in search_text:
                    txn.action = 'bank_transfer'
                    txn.match_type = 'bank_transfer'
                    txn.matched_account = bank['code']
                    txn.matched_name = bank['name'] or bank['code']
                    txn.match_score = 0.9
                    txn.match_source = 'bank_sort_code'
                    txn.bank_transfer_details = {'dest_bank': bank['code']}
                    return True
        return False

    def _check_purchase_refund(self, txn: BankTransaction, supplier_code: str, amount: float) -> bool:
        """Check ptran for unallocated credit notes matching this receipt."""
        try:
            records = self.reader.query("ptran", filters={"pt_account": supplier_code})
            candidates = []
            for r in records:
                trtype = (r.get('pt_trtype', '') or '').strip().upper()
                trbal = float(r.get('pt_trbal', 0) or 0)
                if trtype in ('C', 'P') and trbal > 0:
                    candidates.append(r)
            if candidates:
                # Sort by closest match to amount
                candidates.sort(key=lambda r: abs(float(r.get('pt_trbal', 0) or 0) - amount))
                best = candidates[0]
                txn.action = 'purchase_refund'
                txn.match_type = 'supplier'
                txn.matched_account = supplier_code
                txn.skip_reason = None
                txn.refund_credit_note = str(best.get('pt_trref', '')).strip()
                txn.refund_credit_amount = abs(float(best.get('pt_trbal', 0) or 0))
                logger.debug(f"Purchase refund detected: {supplier_code} credit note {txn.refund_credit_note}")
                return True
        except Exception as e:
            logger.warning(f"Error checking purchase refund for {supplier_code}: {e}")
        return False

    def _check_customer_refund(self, txn: BankTransaction, customer_code: str, amount: float) -> bool:
        """Check stran for unallocated credit notes matching this payment."""
        try:
            records = self.reader.query("stran", filters={"st_account": customer_code})
            candidates = []
            for r in records:
                trtype = (r.get('st_trtype', '') or '').strip().upper()
                trbal = float(r.get('st_trbal', 0) or 0)
                if trtype in ('C', 'R') and trbal < 0:
                    candidates.append(r)
            if candidates:
                candidates.sort(key=lambda r: abs(abs(float(r.get('st_trbal', 0) or 0)) - amount))
                best = candidates[0]
                txn.action = 'sales_refund'
                txn.match_type = 'customer'
                txn.matched_account = customer_code
                txn.skip_reason = None
                txn.refund_credit_note = str(best.get('st_trref', '')).strip()
                txn.refund_credit_amount = abs(float(best.get('st_trbal', 0) or 0))
                logger.debug(f"Customer refund detected: {customer_code} credit note {txn.refund_credit_note}")
                return True
        except Exception as e:
            logger.warning(f"Error checking customer refund for {customer_code}: {e}")
        return False

    def _match_transaction(self, txn: BankTransaction, bank_code: str = "") -> None:
        """
        Match transaction to customer or supplier using shared matcher.

        Full parity with SQL SE version:
        - Repeat entry detection (checked first)
        - Bank transfer detection
        - Alias lookup (full name + clean name)
        - Fuzzy match with clean name fallback
        - Refund detection via credit note lookup
        - Score difference check for ambiguous matches
        """
        # Step 0: Check repeat entries first
        if self._check_repeat_entry(txn, bank_code):
            return

        # Step 0.5: Check bank transfers
        if self._check_bank_transfer(txn, bank_code):
            return

        # Determine expected ledger type based on transaction direction
        expected_type = 'C' if txn.is_receipt else 'S'

        # Extract clean payee name from AI extraction descriptions
        clean_name = extract_payee_name_full(txn.name)

        # Step 1: Check alias table first (fast path) — try both full and clean name
        if self.alias_manager:
            alias_account = self.alias_manager.lookup_alias(txn.name, expected_type)
            if not alias_account and clean_name != txn.name:
                alias_account = self.alias_manager.lookup_alias(clean_name, expected_type)
            if alias_account:
                if expected_type == 'C' and alias_account in self.matcher.customers:
                    candidate = self.matcher.customers[alias_account]
                    txn.match_type = 'customer'
                    txn.matched_account = alias_account
                    txn.matched_name = candidate.primary_name
                    txn.match_score = 1.0
                    txn.action = 'sales_receipt'
                    txn.match_source = 'alias'
                    return
                elif expected_type == 'S' and alias_account in self.matcher.suppliers:
                    candidate = self.matcher.suppliers[alias_account]
                    txn.match_type = 'supplier'
                    txn.matched_account = alias_account
                    txn.matched_name = candidate.primary_name
                    txn.match_score = 1.0
                    txn.action = 'purchase_payment'
                    txn.match_source = 'alias'
                    return

        # Step 2: Fuzzy match — try full name first, then clean name if no match
        match_name = txn.name
        cust_result = self.matcher.match_customer(match_name)
        supp_result = self.matcher.match_supplier(match_name)

        if clean_name and clean_name != txn.name and not cust_result.is_match and not supp_result.is_match:
            cust_clean = self.matcher.match_customer(clean_name)
            supp_clean = self.matcher.match_supplier(clean_name)
            if cust_clean.score > cust_result.score:
                cust_result = cust_clean
            if supp_clean.score > supp_result.score:
                supp_result = supp_clean
            match_name = clean_name

        # Handle ambiguous matches (both customer AND supplier match above threshold)
        if cust_result.is_match and supp_result.is_match:
            score_diff = abs(cust_result.score - supp_result.score)

            if score_diff < 0.15:
                if txn.is_receipt:
                    txn.matched_account = cust_result.account
                    txn.matched_name = cust_result.name
                    txn.match_score = cust_result.score
                    txn.action = 'sales_receipt'
                else:
                    txn.matched_account = supp_result.account
                    txn.matched_name = supp_result.name
                    txn.match_score = supp_result.score
                    txn.action = 'purchase_payment'
                txn.match_source = 'fuzzy_ambiguous'
                txn.skip_reason = f'Review: matches both customer ({cust_result.name}) and supplier ({supp_result.name})'
                return

            if txn.is_receipt:
                if cust_result.score > supp_result.score:
                    pass  # Fall through to receipt handling
                else:
                    txn.matched_account = cust_result.account
                    txn.matched_name = cust_result.name
                    txn.match_score = cust_result.score
                    txn.action = 'sales_receipt'
                    txn.match_source = 'fuzzy_review'
                    txn.skip_reason = f'Review: supplier score ({supp_result.score:.2f}) higher than customer ({cust_result.score:.2f})'
                    return
            else:
                if supp_result.score > cust_result.score:
                    pass  # Fall through to payment handling
                else:
                    if self._check_customer_refund(txn, cust_result.account, txn.abs_amount):
                        txn.matched_name = cust_result.name
                        txn.match_score = cust_result.score
                        txn.match_source = 'fuzzy'
                        return
                    txn.matched_account = supp_result.account
                    txn.matched_name = supp_result.name
                    txn.match_score = supp_result.score
                    txn.action = 'purchase_payment'
                    txn.match_source = 'fuzzy_review'
                    txn.skip_reason = f'Review: customer score ({cust_result.score:.2f}) higher than supplier ({supp_result.score:.2f})'
                    return

        # Determine best match based on transaction direction
        if txn.is_receipt:
            if cust_result.is_match:
                txn.match_type = 'customer'
                txn.matched_account = cust_result.account
                txn.matched_name = cust_result.name
                txn.match_score = cust_result.score
                txn.action = 'sales_receipt'

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
                if supp_result.is_match:
                    txn.skip_reason = f'Receipt matches supplier {supp_result.name} but not a customer — assign manually'
                else:
                    txn.skip_reason = f'No customer match found (best score: {cust_result.score:.2f})'
        else:
            if supp_result.is_match:
                txn.match_type = 'supplier'
                txn.matched_account = supp_result.account
                txn.matched_name = supp_result.name
                txn.match_score = supp_result.score
                txn.action = 'purchase_payment'

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
                if cust_result.is_match:
                    txn.skip_reason = f'Payment matches customer {cust_result.name} but not a supplier — assign manually'
                else:
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

    def _is_already_posted(self, txn: BankTransaction, bank_code: str = "") -> Tuple[bool, str]:
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
        amount_pence = int(round(txn.abs_amount * 100))

        def dates_within_tolerance(db_date, txn_date, tolerance_days=14) -> bool:
            """Compare dates with ±N day tolerance to catch transactions posted on different dates."""
            if db_date is None or txn_date is None:
                return False
            from datetime import date as date_type, timedelta
            # Convert db_date to a date object
            if isinstance(db_date, str):
                try:
                    db_dt = date_type.fromisoformat(db_date[:10])
                except (ValueError, TypeError):
                    return False
            elif hasattr(db_date, 'date'):
                db_dt = db_date.date()
            elif isinstance(db_date, date_type):
                db_dt = db_date
            else:
                return False
            # Convert txn_date
            if isinstance(txn_date, str):
                try:
                    txn_dt = date_type.fromisoformat(txn_date[:10])
                except (ValueError, TypeError):
                    return False
            elif hasattr(txn_date, 'date'):
                txn_dt = txn_date.date()
            elif isinstance(txn_date, date_type):
                txn_dt = txn_date
            else:
                return False
            return abs((db_dt - txn_dt).days) <= tolerance_days

        try:
            # Check 0: Bank transfer — at_type=8, same amount + date on this bank
            if txn.action == 'bank_transfer':
                atran_records_bt = self.reader.read_table("atran")
                for record in atran_records_bt:
                    at_acnt = record.get('AT_ACNT', record.get('at_acnt', '')).strip()
                    at_type = int(record.get('AT_TYPE', record.get('at_type', 0)) or 0)
                    at_pstdate = record.get('AT_PSTDATE', record.get('at_pstdate'))
                    at_value = record.get('AT_VALUE', record.get('at_value', 0))
                    if (at_acnt == bank_code and at_type == 8 and
                        dates_within_tolerance(at_pstdate, txn.date) and
                        abs(abs(at_value) - amount_pence) < 1):
                        txn.is_duplicate = True
                        return True, "Already in cashbook (bank transfer)"

            # Check 1: Cashbook — use aentry (header total) not atran (nominal splits)
            # First try description match, then amount + date only (catches Opera-entered transactions)
            txn_comment = (txn.name or '').strip()[:15].lower()
            aentry_records = self.reader.read_table("aentry")
            matched_entry_ref = None
            comment_match = False
            amount_date_match = False
            for record in aentry_records:
                ae_acnt = record.get('AE_ACNT', record.get('ae_acnt', '')).strip()
                ae_lstdate = record.get('AE_LSTDATE', record.get('ae_lstdate'))
                ae_value = record.get('AE_VALUE', record.get('ae_value', 0))
                ae_comment = record.get('AE_COMMENT', record.get('ae_comment', '')).strip()[:15].lower()
                ae_entry = record.get('AE_ENTRY', record.get('ae_entry', '')).strip()
                if (ae_acnt == bank_code and
                    dates_within_tolerance(ae_lstdate, txn.date) and
                    abs(abs(ae_value) - amount_pence) < 1):
                    matched_entry_ref = ae_entry
                    if ae_comment == txn_comment or not txn_comment:
                        comment_match = True
                        break
                    amount_date_match = True

            if comment_match and matched_entry_ref:
                txn.is_duplicate = True
                from sql_rag.bank_duplicates import DuplicateCandidate
                txn.duplicate_candidates.append(DuplicateCandidate(
                    table='aentry', record_id=matched_entry_ref,
                    match_type='fallback_comment', confidence=0.95,
                    details={'ae_entry': matched_entry_ref}
                ))
                return True, "Already in cashbook"

            if amount_date_match and matched_entry_ref:
                txn.is_duplicate = True
                from sql_rag.bank_duplicates import DuplicateCandidate
                txn.duplicate_candidates.append(DuplicateCandidate(
                    table='aentry', record_id=matched_entry_ref,
                    match_type='fallback_amount_date', confidence=0.9,
                    details={'ae_entry': matched_entry_ref}
                ))
                return True, "Already in cashbook (amount + date match)"

            # Check 2: Purchase Ledger (ptran) - for supplier payments
            if txn.action == 'purchase_payment' and txn.matched_account:
                ptran_records = self.reader.read_table("ptran")
                for record in ptran_records:
                    pt_account = record.get('PT_ACCOUNT', record.get('pt_account', '')).strip()
                    pt_trdate = record.get('PT_TRDATE', record.get('pt_trdate'))
                    pt_trvalue = record.get('PT_TRVALUE', record.get('pt_trvalue', 0))
                    pt_trtype = record.get('PT_TRTYPE', record.get('pt_trtype', '')).strip()
                    if (pt_account == txn.matched_account and
                        dates_within_tolerance(pt_trdate, txn.date) and
                        abs(abs(pt_trvalue) - txn.abs_amount) < 0.01 and
                        pt_trtype == 'P'):
                        txn.is_duplicate = True
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
                        dates_within_tolerance(st_trdate, txn.date) and
                        abs(abs(st_trvalue) - txn.abs_amount) < 0.01 and
                        st_trtype == 'R'):
                        txn.is_duplicate = True
                        return True, f"Already in sales ledger (stran) for {txn.matched_account}"

        except FileNotFoundError:
            # If table doesn't exist, can't be a duplicate
            pass
        except Exception as e:
            logger.warning(f"Error checking for duplicates: {e}")

        return False, ""

    def process_transactions(self, transactions: List[BankTransaction],
                            check_duplicates: bool = True,
                            bank_code: str = "") -> None:
        """
        Process transactions: skip checks, matching, and duplicate detection.

        Args:
            transactions: List of transactions to process
            check_duplicates: Whether to check for already-posted transactions
            bank_code: Bank account code for duplicate checking
        """
        for txn in transactions:
            # Check if should skip (name pattern match)
            skip_reason = self._should_skip(txn.name, txn.subcategory)

            if not skip_reason:
                # Match to customer/supplier
                self._match_transaction(txn, bank_code)
            else:
                txn.action = 'skip'
                txn.skip_reason = skip_reason
                logger.debug(f"MATCH_DEBUG: SKIPPED '{txn.name}' subcat='{txn.subcategory}' reason='{skip_reason}'")

            # Check if already posted — run for ALL transactions including skipped ones.
            # A GC payout or other "skipped" transaction may already be in Opera's cashbook.
            if check_duplicates:
                is_posted, posted_reason = self._is_already_posted(txn, bank_code)
                if is_posted:
                    txn.is_duplicate = True
                    if txn.action == 'repeat_entry':
                        txn.skip_reason = f'Already posted: {posted_reason}'
                    else:
                        txn.action = 'skip'
                        txn.skip_reason = f'Already posted: {posted_reason}'

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
        # Get company default from Opera config (nparm) — NEVER hardcode account codes
        try:
            from sql_rag.opera3_config import Opera3Config
            config = Opera3Config(self.reader.data_path)
            defaults = config.get_control_accounts()
            default_control = defaults.creditors_control
        except Exception as e:
            raise ValueError(
                f"Cannot determine creditors control account: Opera3Config failed ({e}). "
                "Control accounts vary by company — they must be read from Opera configuration."
            )

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
        # Get company default from Opera config (nparm) — NEVER hardcode account codes
        try:
            from sql_rag.opera3_config import Opera3Config
            config = Opera3Config(self.reader.data_path)
            defaults = config.get_control_accounts()
            default_control = defaults.debtors_control
        except Exception as e:
            raise ValueError(
                f"Cannot determine debtors control account: Opera3Config failed ({e}). "
                "Control accounts vary by company — they must be read from Opera configuration."
            )

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
        bank_code: str = "",
        validate_only: bool = False,
        auto_allocate: bool = False
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
        if not FOXPRO_IMPORT_AVAILABLE and not WRITE_PROVIDER_AVAILABLE:
            raise ImportError(
                "Opera 3 import requires either the Write Agent service or the 'dbf' package. "
                "Install with: pip install dbf"
            )

        # Initialize importer — automatically uses Write Agent if available
        if WRITE_PROVIDER_AVAILABLE:
            importer = get_opera3_writer(str(self.data_path))
        else:
            importer = Opera3FoxProImport(str(self.data_path))

        imported_count = 0
        failed_count = 0
        import_errors = []

        skipped_duplicates = 0

        for txn in result.transactions:
            if txn.action not in ('sales_receipt', 'purchase_payment', 'sales_refund', 'purchase_refund', 'bank_transfer', 'nominal_payment', 'nominal_receipt'):
                continue

            # Just-in-time duplicate check - catches entries that appeared since statement was processed
            try:
                acct_type = 'customer' if txn.action in ('sales_receipt', 'sales_refund') else 'supplier' if txn.action in ('purchase_payment', 'purchase_refund') else 'transfer' if txn.action == 'bank_transfer' else 'nominal'
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
                    import_result = importer.import_purchase_payment(
                        bank_account=bank_code,
                        supplier_account=txn.matched_account,
                        amount_pounds=txn.abs_amount,
                        reference=txn.reference or txn.name[:20],
                        post_date=txn.date,
                        input_by="IMPORT",
                        validate_only=validate_only
                    )
                elif txn.action == 'sales_refund':
                    import_result = importer.import_sales_refund(
                        bank_account=bank_code,
                        customer_account=txn.matched_account,
                        amount_pounds=txn.abs_amount,
                        reference=txn.reference or txn.name[:20],
                        post_date=txn.date,
                        input_by="BANK_IMP",
                        validate_only=validate_only
                    )
                elif txn.action == 'purchase_refund':
                    import_result = importer.import_purchase_refund(
                        bank_account=bank_code,
                        supplier_account=txn.matched_account,
                        amount_pounds=txn.abs_amount,
                        reference=txn.reference or txn.name[:20],
                        post_date=txn.date,
                        input_by="BANK_IMP",
                        validate_only=validate_only
                    )
                elif txn.action == 'bank_transfer':
                    details = txn.bank_transfer_details or {}
                    dest_bank = details.get('dest_bank', '')
                    if not dest_bank:
                        import_result = Opera3ImportResult(
                            success=False, records_processed=1, records_failed=1,
                            errors=["Bank transfer missing destination bank"]
                        )
                    else:
                        transfer_result = importer.import_bank_transfer(
                            source_bank=bank_code,
                            dest_bank=dest_bank,
                            amount_pounds=txn.abs_amount,
                            reference=txn.reference or txn.name[:20],
                            post_date=txn.date,
                            comment=txn.memo or '',
                            input_by="BANK_IMP"
                        )
                        # Convert dict result to Opera3ImportResult
                        if transfer_result.get('success'):
                            import_result = Opera3ImportResult(
                                success=True, records_processed=1, records_imported=1,
                                entry_number=transfer_result.get('source_entry', ''),
                                details=[transfer_result.get('message', '')]
                            )
                        else:
                            import_result = Opera3ImportResult(
                                success=False, records_processed=1, records_failed=1,
                                errors=[transfer_result.get('error', 'Unknown error')]
                            )
                elif txn.action in ('nominal_payment', 'nominal_receipt'):
                    is_receipt = txn.action == 'nominal_receipt'
                    import_result = importer.import_nominal_entry(
                        bank_account=bank_code,
                        nominal_account=txn.matched_account,
                        amount_pounds=txn.abs_amount,
                        reference=txn.reference or txn.name[:20],
                        post_date=txn.date,
                        description=txn.memo or '',
                        input_by="IMPORT",
                        is_receipt=is_receipt,
                        validate_only=validate_only,
                        project_code=txn.project_code or '',
                        department_code=txn.department_code or '',
                        vat_code=txn.vat_code or ''
                    )
                else:
                    # Import customer receipt (sales_receipt)
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

                    # Auto-allocate if enabled (matches SQL SE behaviour)
                    if auto_allocate and txn.action in ('sales_receipt', 'purchase_payment'):
                        account_code = txn.matched_account
                        txn_ref = txn.reference or txn.name[:20]

                        try:
                            if txn.action == 'sales_receipt':
                                alloc_result = importer.auto_allocate_receipt(
                                    customer_account=account_code,
                                    receipt_ref=txn_ref,
                                    receipt_amount=abs(txn.amount),
                                    allocation_date=txn.date,
                                    bank_account=bank_code,
                                    description=txn.memo or txn.name
                                )
                            else:  # purchase_payment
                                alloc_result = importer.auto_allocate_payment(
                                    supplier_account=account_code,
                                    payment_ref=txn_ref,
                                    payment_amount=abs(txn.amount),
                                    allocation_date=txn.date,
                                    bank_account=bank_code,
                                    description=txn.memo or txn.name
                                )

                            if alloc_result.get('success'):
                                txn.skip_reason += f" | Allocated: {alloc_result['message']}"
                        except Exception as alloc_err:
                            logger.warning(f"Auto-allocate failed for {txn.matched_account}: {alloc_err}")
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
        bank_code: str = "",
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
        bank_code: str = "",
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
