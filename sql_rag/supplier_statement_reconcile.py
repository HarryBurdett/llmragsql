"""
Supplier Statement Reconciliation against Opera Purchase Ledger.

Reconciles extracted statement data against ptran and generates
smart responses following business rules.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Tuple
from enum import Enum

logger = logging.getLogger(__name__)


class MatchStatus(str, Enum):
    """Match status for statement lines."""
    MATCHED = "matched"           # Found in our records, amounts match
    AMOUNT_MISMATCH = "amount_mismatch"  # Found but amount differs
    NOT_IN_OPERA = "not_in_opera"        # On statement but not in our system
    NOT_ON_STATEMENT = "not_on_statement" # In our system but not on statement
    PAID = "paid"                 # We've paid this
    IN_OUR_FAVOUR = "in_our_favour"      # Discrepancy benefits us - stay quiet


@dataclass
class ReconciliationLine:
    """A reconciled line item."""
    statement_ref: Optional[str] = None
    statement_date: Optional[str] = None
    statement_amount: Optional[float] = None
    statement_type: Optional[str] = None  # INV, CN, PMT

    opera_ref: Optional[str] = None
    opera_date: Optional[str] = None
    opera_amount: Optional[float] = None
    opera_balance: Optional[float] = None
    opera_type: Optional[str] = None
    opera_due_date: Optional[str] = None

    match_status: MatchStatus = MatchStatus.NOT_IN_OPERA
    difference: float = 0.0
    query_required: bool = False
    query_text: Optional[str] = None
    notify_payment: bool = False
    payment_details: Optional[str] = None


@dataclass
class ReconciliationResult:
    """Complete reconciliation result."""
    supplier_code: Optional[str] = None
    supplier_name: str = ""
    supplier_found: bool = False

    statement_date: Optional[str] = None
    statement_balance: Optional[float] = None
    opera_balance: Optional[float] = None
    variance: float = 0.0

    is_old_statement: bool = False
    statement_age_days: int = 0

    lines: List[ReconciliationLine] = field(default_factory=list)

    # Summary counts
    matched_count: int = 0
    query_count: int = 0
    in_our_favour_count: int = 0
    payment_notifications: int = 0

    # Generated response
    response_text: Optional[str] = None
    queries: List[str] = field(default_factory=list)
    payment_info: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "supplier_code": self.supplier_code,
            "supplier_name": self.supplier_name,
            "supplier_found": self.supplier_found,
            "statement_date": self.statement_date,
            "statement_balance": self.statement_balance,
            "opera_balance": self.opera_balance,
            "variance": self.variance,
            "is_old_statement": self.is_old_statement,
            "statement_age_days": self.statement_age_days,
            "matched_count": self.matched_count,
            "query_count": self.query_count,
            "in_our_favour_count": self.in_our_favour_count,
            "payment_notifications": self.payment_notifications,
            "lines": [
                {
                    "statement_ref": l.statement_ref,
                    "statement_date": l.statement_date,
                    "statement_amount": l.statement_amount,
                    "statement_type": l.statement_type,
                    "opera_ref": l.opera_ref,
                    "opera_date": l.opera_date,
                    "opera_amount": l.opera_amount,
                    "opera_balance": l.opera_balance,
                    "opera_due_date": l.opera_due_date,
                    "match_status": l.match_status.value,
                    "difference": l.difference,
                    "query_required": l.query_required,
                    "query_text": l.query_text,
                    "notify_payment": l.notify_payment,
                    "payment_details": l.payment_details
                }
                for l in self.lines
            ],
            "queries": self.queries,
            "payment_info": self.payment_info,
            "response_text": self.response_text
        }


class SupplierStatementReconciler:
    """
    Reconciles supplier statements against Opera purchase ledger.

    Follows business rules:
    - Only query when NOT in our favour
    - Stay quiet when discrepancy benefits us
    - Always notify payments made
    - Flag old statements
    """

    def __init__(self, sql_connector, old_statement_threshold_days: int = 14):
        """
        Initialize reconciler.

        Args:
            sql_connector: SQL connector for Opera database
            old_statement_threshold_days: Days after which a statement is considered old
        """
        self.sql = sql_connector
        self.old_threshold_days = old_statement_threshold_days

    def find_supplier(self, supplier_name: str, account_ref: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Find supplier in Opera by name or account reference.

        Returns supplier record or None if not found.
        """
        # Try by account reference first (most reliable)
        if account_ref:
            query = f"""
                SELECT pn_account, pn_name, pn_currbal, pn_email, pn_teleno, pn_contact
                FROM pname WITH (NOLOCK)
                WHERE pn_account = '{account_ref}'
            """
            result = self.sql.execute_query(query)
            if result is not None and len(result) > 0:
                row = result.iloc[0]
                return {
                    "account": row.get("pn_account", "").strip(),
                    "name": row.get("pn_name", "").strip(),
                    "balance": float(row.get("pn_currbal", 0) or 0),
                    "email": row.get("pn_email", "").strip() if row.get("pn_email") else None,
                    "phone": row.get("pn_teleno", "").strip() if row.get("pn_teleno") else None,
                    "contact": row.get("pn_contact", "").strip() if row.get("pn_contact") else None
                }

        # Try by name match
        clean_name = supplier_name.replace("'", "''")
        query = f"""
            SELECT pn_account, pn_name, pn_currbal, pn_email, pn_teleno, pn_contact
            FROM pname WITH (NOLOCK)
            WHERE pn_name LIKE '%{clean_name}%'
               OR pn_name LIKE '%{clean_name.split()[0]}%'
        """
        result = self.sql.execute_query(query)
        if result is not None and len(result) > 0:
            row = result.iloc[0]
            return {
                "account": row.get("pn_account", "").strip(),
                "name": row.get("pn_name", "").strip(),
                "balance": float(row.get("pn_currbal", 0) or 0),
                "email": row.get("pn_email", "").strip() if row.get("pn_email") else None,
                "phone": row.get("pn_teleno", "").strip() if row.get("pn_teleno") else None,
                "contact": row.get("pn_contact", "").strip() if row.get("pn_contact") else None
            }

        return None

    def get_supplier_transactions(self, account_code: str) -> List[Dict[str, Any]]:
        """Get all transactions for a supplier from ptran."""
        query = f"""
            SELECT
                pt_unique,
                pt_account,
                pt_trdate,
                pt_trref,
                pt_trtype,
                pt_trvalue,
                pt_trbal,
                pt_dueday,
                pt_supref
            FROM ptran WITH (NOLOCK)
            WHERE pt_account = '{account_code}'
            ORDER BY pt_trdate DESC
        """
        result = self.sql.execute_query(query)

        transactions = []
        if result is not None and len(result) > 0:
            for _, row in result.iterrows():
                tr_type = str(row.get("pt_trtype", "")).strip()
                type_name = {"I": "Invoice", "C": "Credit Note", "P": "Payment", "F": "Refund"}.get(tr_type, tr_type)

                transactions.append({
                    "unique_id": row.get("pt_unique", ""),
                    "account": str(row.get("pt_account", "")).strip(),
                    "date": row.get("pt_trdate"),
                    "reference": str(row.get("pt_trref", "")).strip() if row.get("pt_trref") else "",
                    "type": type_name,
                    "type_code": tr_type,
                    "value": float(row.get("pt_trvalue", 0) or 0),
                    "balance": float(row.get("pt_trbal", 0) or 0),
                    "due_date": row.get("pt_dueday"),
                    "detail": str(row.get("pt_supref", "")).strip() if row.get("pt_supref") else ""
                })

        return transactions

    def get_recent_payments(self, account_code: str, days: int = 60) -> List[Dict[str, Any]]:
        """Get recent payments made to this supplier."""
        cutoff = datetime.now() - timedelta(days=days)
        cutoff_str = cutoff.strftime('%Y-%m-%d')

        query = f"""
            SELECT
                pt_trdate,
                pt_trref,
                pt_trvalue,
                pt_supref
            FROM ptran WITH (NOLOCK)
            WHERE pt_account = '{account_code}'
              AND pt_trtype = 'P'
              AND pt_trdate >= '{cutoff_str}'
            ORDER BY pt_trdate DESC
        """
        result = self.sql.execute_query(query)

        payments = []
        if result is not None and len(result) > 0:
            for _, row in result.iterrows():
                payments.append({
                    "date": row.get("pt_trdate"),
                    "reference": str(row.get("pt_trref", "")).strip() if row.get("pt_trref") else "",
                    "amount": abs(float(row.get("pt_trvalue", 0) or 0)),
                    "detail": str(row.get("pt_supref", "")).strip() if row.get("pt_supref") else ""
                })

        return payments

    def reconcile(
        self,
        statement_info: Dict[str, Any],
        statement_lines: List[Dict[str, Any]]
    ) -> ReconciliationResult:
        """
        Reconcile a supplier statement against Opera.

        Args:
            statement_info: Dict with supplier_name, account_reference, statement_date, closing_balance
            statement_lines: List of line items with date, reference, debit, credit, doc_type

        Returns:
            ReconciliationResult with match details and generated response
        """
        result = ReconciliationResult()
        result.supplier_name = statement_info.get("supplier_name", "Unknown")
        result.statement_date = statement_info.get("statement_date")
        result.statement_balance = statement_info.get("closing_balance")

        # Check statement age
        if result.statement_date:
            try:
                stmt_date = datetime.strptime(result.statement_date, "%Y-%m-%d")
                result.statement_age_days = (datetime.now() - stmt_date).days
                result.is_old_statement = result.statement_age_days > self.old_threshold_days
            except ValueError:
                pass

        # Find supplier in Opera
        supplier = self.find_supplier(
            statement_info.get("supplier_name", ""),
            statement_info.get("account_reference")
        )

        if not supplier:
            result.supplier_found = False
            result.response_text = self._generate_supplier_not_found_response(statement_info)
            return result

        result.supplier_found = True
        result.supplier_code = supplier["account"]
        result.supplier_name = supplier["name"]
        result.opera_balance = supplier["balance"]
        result.variance = (result.statement_balance or 0) - result.opera_balance

        # Get Opera transactions
        opera_txns = self.get_supplier_transactions(supplier["account"])
        opera_by_ref = {t["reference"]: t for t in opera_txns if t["reference"]}

        # Get recent payments
        recent_payments = self.get_recent_payments(supplier["account"])

        # Reconcile each statement line
        for line in statement_lines:
            recon_line = self._reconcile_line(line, opera_by_ref, opera_txns)
            result.lines.append(recon_line)

            if recon_line.match_status == MatchStatus.MATCHED:
                result.matched_count += 1
            elif recon_line.query_required:
                result.query_count += 1
                if recon_line.query_text:
                    result.queries.append(recon_line.query_text)
            elif recon_line.match_status == MatchStatus.IN_OUR_FAVOUR:
                result.in_our_favour_count += 1

        # Check for Opera items not on statement (potential payment notifications)
        # Only notify about payments made in the last 90 days to keep response focused
        cutoff_date = datetime.now() - timedelta(days=90)
        statement_refs = {l.get("reference", "").upper() for l in statement_lines if l.get("reference")}

        for txn in opera_txns:
            if txn["reference"] and txn["reference"].upper() not in statement_refs:
                if txn["type_code"] == "P" and txn["balance"] == 0:
                    txn_date = txn["date"]
                    if isinstance(txn_date, str):
                        try:
                            txn_date = datetime.strptime(txn_date[:10], "%Y-%m-%d")
                        except ValueError:
                            continue

                    # Only include recent payments
                    if txn_date and txn_date >= cutoff_date:
                        result.payment_notifications += 1
                        date_str = txn_date.strftime("%d/%m/%Y") if isinstance(txn_date, datetime) else str(txn_date)[:10]
                        result.payment_info.append(
                            f"Payment of {self._format_currency(abs(txn['value']))} on {date_str} (Ref: {txn['reference']})"
                        )

        # Generate response
        result.response_text = self._generate_response(result, statement_info, recent_payments)

        return result

    def _reconcile_line(
        self,
        statement_line: Dict[str, Any],
        opera_by_ref: Dict[str, Dict],
        all_opera_txns: List[Dict]
    ) -> ReconciliationLine:
        """Reconcile a single statement line against Opera records."""
        recon = ReconciliationLine()
        recon.statement_ref = statement_line.get("reference")
        recon.statement_date = statement_line.get("date")
        recon.statement_amount = statement_line.get("debit") or statement_line.get("credit")
        recon.statement_type = statement_line.get("doc_type")

        ref = (recon.statement_ref or "").upper()

        # Try to match by reference
        if ref and ref in {k.upper(): k for k in opera_by_ref}:
            # Find the actual key (case-insensitive)
            actual_key = next(k for k in opera_by_ref if k.upper() == ref)
            opera_txn = opera_by_ref[actual_key]

            recon.opera_ref = opera_txn["reference"]
            recon.opera_date = str(opera_txn["date"])[:10] if opera_txn["date"] else None
            recon.opera_amount = abs(opera_txn["value"])
            recon.opera_balance = opera_txn["balance"]
            recon.opera_type = opera_txn["type"]
            recon.opera_due_date = str(opera_txn["due_date"])[:10] if opera_txn["due_date"] else None

            # Check if amounts match
            stmt_amt = recon.statement_amount or 0
            opera_amt = recon.opera_amount or 0
            recon.difference = stmt_amt - opera_amt

            if abs(recon.difference) < 0.01:
                recon.match_status = MatchStatus.MATCHED
            elif recon.difference > 0:
                # Their amount is higher - query (not in our favour)
                recon.match_status = MatchStatus.AMOUNT_MISMATCH
                recon.query_required = True
                recon.query_text = (
                    f"Invoice {recon.statement_ref}: Your statement shows {self._format_currency(stmt_amt)}, "
                    f"our records show {self._format_currency(opera_amt)}. "
                    f"Please clarify the difference of {self._format_currency(recon.difference)}."
                )
            else:
                # Our amount is higher - in our favour, stay quiet
                recon.match_status = MatchStatus.IN_OUR_FAVOUR

            # Check if paid
            if opera_txn["balance"] == 0:
                recon.match_status = MatchStatus.PAID
                recon.notify_payment = True
                recon.payment_details = f"Invoice {recon.statement_ref} has been paid"

        else:
            # Not found by reference - try amount match
            stmt_amt = recon.statement_amount or 0

            # Look for invoices with matching amount
            matches = [t for t in all_opera_txns
                      if abs(abs(t["value"]) - stmt_amt) < 0.01 and t["type_code"] == "I"]

            if matches:
                # Found potential match by amount
                opera_txn = matches[0]
                recon.opera_ref = opera_txn["reference"]
                recon.opera_amount = abs(opera_txn["value"])
                recon.opera_balance = opera_txn["balance"]
                recon.match_status = MatchStatus.MATCHED
                logger.info(f"Matched {recon.statement_ref} to {opera_txn['reference']} by amount")
            else:
                # Not in our system - query
                recon.match_status = MatchStatus.NOT_IN_OPERA
                recon.query_required = True
                recon.query_text = (
                    f"Invoice {recon.statement_ref or 'unknown'} for {self._format_currency(stmt_amt)} "
                    f"dated {recon.statement_date}: We do not have this invoice on our system. "
                    f"Please send a copy."
                )

        return recon

    def _generate_response(
        self,
        result: ReconciliationResult,
        statement_info: Dict[str, Any],
        recent_payments: List[Dict]
    ) -> str:
        """Generate the response text for the supplier."""
        lines = []

        # Header
        lines.append(f"Subject: Statement Reconciliation - {result.supplier_name} - {result.statement_date}")
        lines.append("")
        lines.append("Dear Accounts Team,")
        lines.append("")
        lines.append(f"Thank you for your statement dated {result.statement_date}. Please find our reconciliation below.")
        lines.append("")

        # Old statement warning
        if result.is_old_statement:
            lines.append("=" * 50)
            lines.append("IMPORTANT: OLD STATEMENT")
            lines.append("=" * 50)
            lines.append(f"This statement is {result.statement_age_days} days old.")
            lines.append("Please send a current statement for accurate reconciliation.")
            lines.append("")

        # Balance comparison
        lines.append("ACCOUNT SUMMARY")
        lines.append("=" * 50)
        lines.append(f"Your statement balance:  {self._format_currency(result.statement_balance or 0)}")
        lines.append(f"Our records balance:     {self._format_currency(result.opera_balance or 0)}")
        if abs(result.variance) >= 0.01:
            lines.append(f"Variance:                {self._format_currency(result.variance)}")
        else:
            lines.append("Status: RECONCILED")
        lines.append("")

        # Line-by-line reconciliation table
        lines.append("LINE BY LINE RECONCILIATION")
        lines.append("=" * 80)
        lines.append("")
        lines.append(f"{'Your Ref':<15} {'Your Amount':>12}  {'Our Ref':<15} {'Our Amount':>12}  {'Status':<20}")
        lines.append("-" * 80)

        agreed_count = 0
        query_items = []

        for line in result.lines:
            your_ref = (line.statement_ref or '-')[:15]
            your_amt = self._format_currency(line.statement_amount or 0) if line.statement_amount else '-'
            our_ref = (line.opera_ref or '-')[:15]
            our_amt = self._format_currency(line.opera_amount or 0) if line.opera_amount else '-'

            if line.match_status == MatchStatus.MATCHED:
                status = "AGREED"
                agreed_count += 1
            elif line.match_status == MatchStatus.PAID:
                status = "PAID - AGREED"
                agreed_count += 1
            elif line.match_status == MatchStatus.IN_OUR_FAVOUR:
                status = "AGREED*"  # We don't query items in our favour
                agreed_count += 1
            elif line.match_status == MatchStatus.AMOUNT_MISMATCH:
                diff = line.difference
                status = f"DIFFERS BY {self._format_currency(abs(diff))}"
                query_items.append(line)
            elif line.match_status == MatchStatus.NOT_IN_OPERA:
                status = "NOT ON OUR RECORDS"
                our_ref = "-"
                our_amt = "-"
                query_items.append(line)
            else:
                status = line.match_status.value

            lines.append(f"{your_ref:<15} {your_amt:>12}  {our_ref:<15} {our_amt:>12}  {status:<20}")

        lines.append("-" * 80)
        lines.append(f"{'TOTALS':<15} {self._format_currency(result.statement_balance or 0):>12}  {'':<15} {self._format_currency(result.opera_balance or 0):>12}")
        lines.append("")

        # Summary
        total_lines = len(result.lines)
        lines.append(f"Reconciliation Summary: {agreed_count} of {total_lines} items agreed")
        if agreed_count == total_lines:
            lines.append("STATUS: FULLY RECONCILED")
        lines.append("")

        # Queries section - explain WHY figures don't match
        if query_items:
            lines.append("ITEMS REQUIRING CLARIFICATION")
            lines.append("=" * 80)
            lines.append("")
            for i, line in enumerate(query_items, 1):
                if line.match_status == MatchStatus.NOT_IN_OPERA:
                    lines.append(f"{i}. {line.statement_ref} - {self._format_currency(line.statement_amount or 0)}")
                    lines.append(f"   REASON: This invoice does not appear on our purchase ledger.")
                    lines.append(f"   ACTION: Please send a copy of this invoice so we can investigate.")
                    lines.append("")
                elif line.match_status == MatchStatus.AMOUNT_MISMATCH:
                    lines.append(f"{i}. {line.statement_ref} - Your amount: {self._format_currency(line.statement_amount or 0)}, "
                               f"Our amount: {self._format_currency(line.opera_amount or 0)}")
                    lines.append(f"   REASON: Amount difference of {self._format_currency(abs(line.difference))}")
                    lines.append(f"   ACTION: Please confirm the correct amount or provide supporting documentation.")
                    lines.append("")

        # Payment notifications
        if result.payment_info:
            lines.append("PAYMENTS NOT ON YOUR STATEMENT")
            lines.append("=" * 50)
            lines.append("We have made the following payment(s) that may not yet appear on your records:")
            for payment in result.payment_info:
                lines.append(f"- {payment}")
            lines.append("")

        # Recent payments (additional info)
        if recent_payments:
            lines.append("RECENT PAYMENT HISTORY")
            lines.append("=" * 50)
            for pmt in recent_payments[:5]:
                date_str = pmt["date"].strftime("%d/%m/%Y") if isinstance(pmt["date"], datetime) else str(pmt["date"])[:10]
                lines.append(f"- {date_str}: {self._format_currency(pmt['amount'])} (Ref: {pmt['reference']})")
            lines.append("")

        # Payment schedule for agreed outstanding invoices
        outstanding = [l for l in result.lines
                      if l.match_status in (MatchStatus.MATCHED, MatchStatus.IN_OUR_FAVOUR)
                      and l.opera_balance and l.opera_balance > 0]
        if outstanding:
            lines.append("PAYMENT SCHEDULE")
            lines.append("=" * 80)
            lines.append("")
            lines.append("The following agreed items are scheduled for payment:")
            lines.append("")
            lines.append(f"{'Invoice Ref':<20} {'Amount':>12}  {'Due Date':<12} {'Payment Date':<12}")
            lines.append("-" * 60)

            total_outstanding = 0.0
            for line in outstanding:
                total_outstanding += line.opera_balance or 0
                ref = (line.opera_ref or line.statement_ref or '-')[:20]
                amount = self._format_currency(line.opera_balance)
                due_date = line.opera_due_date[:10] if line.opera_due_date else '-'

                # Calculate payment date
                payment_date = self._calculate_suggested_payment_date(line.opera_due_date)

                lines.append(f"{ref:<20} {amount:>12}  {due_date:<12} {payment_date:<12}")

            lines.append("-" * 60)
            lines.append(f"{'TOTAL TO PAY':<20} {self._format_currency(total_outstanding):>12}")
            lines.append("")

            # Next payment run info
            next_payment_run = self._get_next_payment_run_date()
            lines.append(f"Our payment runs are processed weekly on Fridays.")
            lines.append(f"Next payment run: {next_payment_run}")
            lines.append("")

        # Footer
        if result.queries:
            lines.append("Please respond to the queries above at your earliest convenience.")
            lines.append("")

        lines.append("Regards,")
        lines.append("Accounts Department")

        return "\n".join(lines)

    def generate_acknowledgment(self, supplier_name: str, statement_date: str, sender_email: str) -> str:
        """
        Generate immediate acknowledgment when statement is received.

        This is sent immediately upon receipt, before reconciliation.
        """
        lines = []
        lines.append(f"Subject: Statement Received - {supplier_name}")
        lines.append("")
        lines.append("Dear Accounts Team,")
        lines.append("")
        lines.append(f"Thank you for your statement dated {statement_date}.")
        lines.append("")
        lines.append("We confirm receipt and will process this against our records.")
        lines.append("You will receive our full reconciliation response shortly.")
        lines.append("")
        lines.append("If you have any urgent queries in the meantime, please reply to this email.")
        lines.append("")
        lines.append("Regards,")
        lines.append("Accounts Department")
        lines.append("")
        lines.append("---")
        lines.append("This is an automated acknowledgment.")

        return "\n".join(lines)

    def _generate_supplier_not_found_response(self, statement_info: Dict[str, Any]) -> str:
        """Generate response when supplier not found in Opera."""
        lines = []
        lines.append(f"Subject: Statement Query - {statement_info.get('supplier_name', 'Unknown')}")
        lines.append("")
        lines.append("Dear Accounts Team,")
        lines.append("")
        lines.append(f"Thank you for your statement dated {statement_info.get('statement_date', 'unknown date')}.")
        lines.append("")
        lines.append("We have been unable to match this statement to a supplier account in our system.")
        lines.append("")
        lines.append("Please could you confirm:")
        lines.append("1. The correct account reference for our company")
        lines.append("2. Copies of the invoices listed on the statement")
        lines.append("")
        lines.append("This will help us process your statement promptly.")
        lines.append("")
        lines.append("Regards,")
        lines.append("Accounts Department")

        return "\n".join(lines)

    def _format_currency(self, amount: float) -> str:
        """Format amount as currency."""
        return f"£{amount:,.2f}"

    def _calculate_suggested_payment_date(self, due_date_str: Optional[str]) -> str:
        """
        Calculate suggested payment date based on due date.

        Business logic:
        - If due date is past: next payment run date
        - If due date is in future: the due date or next payment run (whichever is later)
        - If no due date: next payment run date
        """
        today = datetime.now().date()
        next_payment_run = self._get_next_payment_run_date_obj()

        if not due_date_str:
            return next_payment_run.strftime("%d/%m/%Y")

        try:
            # Parse due date (handle various formats)
            due_date = None
            for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
                try:
                    due_date = datetime.strptime(due_date_str[:10], fmt).date()
                    break
                except ValueError:
                    continue

            if not due_date:
                return next_payment_run.strftime("%d/%m/%Y")

            # If past due, suggest next payment run
            if due_date <= today:
                return next_payment_run.strftime("%d/%m/%Y")

            # If due in future, suggest the due date (or next run if sooner)
            # Payment runs are typically weekly on Fridays
            if due_date > next_payment_run:
                return due_date.strftime("%d/%m/%Y")
            else:
                return next_payment_run.strftime("%d/%m/%Y")

        except Exception:
            return next_payment_run.strftime("%d/%m/%Y")

    def _get_next_payment_run_date_obj(self) -> 'datetime.date':
        """Get the next payment run date (typically Friday)."""
        today = datetime.now().date()
        # Find next Friday (weekday 4)
        days_until_friday = (4 - today.weekday()) % 7
        if days_until_friday == 0 and datetime.now().hour >= 12:
            # If it's Friday afternoon, next run is next week
            days_until_friday = 7
        elif days_until_friday == 0:
            # Friday morning - today's run
            pass

        return today + timedelta(days=days_until_friday)

    def _get_next_payment_run_date(self) -> str:
        """Get the next payment run date as formatted string."""
        return self._get_next_payment_run_date_obj().strftime("%d/%m/%Y")
