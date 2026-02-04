"""
Bank Statement Import Module for Opera 3 (FoxPro)

Processes bank statement CSV files and matches transactions against
Opera 3 customer/supplier master files stored in FoxPro DBF format.

Uses the shared matching module (bank_matching.py) for fuzzy matching logic.

Note: This module is read-only for matching/preview purposes.
Opera 3 imports should be done through Opera's standard import mechanisms.
"""

import csv
import re
import logging
from datetime import datetime, date
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from pathlib import Path

from sql_rag.bank_matching import (
    BankMatcher, MatchCandidate, MatchResult,
    create_match_candidate_from_dict
)
from sql_rag.opera3_foxpro import Opera3Reader, Opera3System

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
    action: Optional[str] = None  # 'sales_receipt', 'purchase_payment', 'skip'
    skip_reason: Optional[str] = None

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
    errors: List[str] = field(default_factory=list)
    transactions: List[BankTransaction] = field(default_factory=list)

    @property
    def match_rate(self) -> float:
        if self.total_transactions == 0:
            return 0.0
        return self.matched_transactions / self.total_transactions * 100


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

    def _match_transaction(self, txn: BankTransaction) -> None:
        """
        Match transaction to customer or supplier using shared matcher.

        Updates transaction with match results.
        """
        # Use shared matcher
        cust_result = self.matcher.match_customer(txn.name)
        supp_result = self.matcher.match_supplier(txn.name)

        # Skip if name matches both customer AND supplier above threshold
        if cust_result.is_match and supp_result.is_match:
            txn.action = 'skip'
            txn.skip_reason = f'Matches both customer ({cust_result.name}) and supplier ({supp_result.name}) - ambiguous'
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
            if supp_result.is_match:
                txn.match_type = 'supplier'
                txn.matched_account = supp_result.account
                txn.matched_name = supp_result.name
                txn.match_score = supp_result.score
                txn.action = 'purchase_payment'
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

    def process_transactions(self, transactions: List[BankTransaction]) -> None:
        """
        Process transactions: skip checks and matching.

        Args:
            transactions: List of transactions to process
        """
        for txn in transactions:
            skip_reason = self._should_skip(txn.name, txn.subcategory)
            if skip_reason:
                txn.action = 'skip'
                txn.skip_reason = skip_reason
                continue

            self._match_transaction(txn)

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
