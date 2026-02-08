"""
Bank Statement Reconciliation Module

Extracts transactions from bank statement PDFs/images using Claude Vision
and matches them against Opera's unreconciled cashbook entries for auto-reconciliation.
"""

import anthropic
import base64
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any
import logging

logger = logging.getLogger(__name__)


@dataclass
class StatementTransaction:
    """A transaction extracted from a bank statement."""
    date: datetime
    description: str
    amount: float  # Positive = money in, Negative = money out
    balance: Optional[float] = None
    transaction_type: Optional[str] = None  # DD, STO, Giro, Card, etc.
    reference: Optional[str] = None
    raw_text: Optional[str] = None


@dataclass
class ReconciliationMatch:
    """A match between a statement transaction and an Opera entry."""
    statement_txn: StatementTransaction
    opera_entry: Dict[str, Any]  # ae_entry, ae_date, ae_ref, value_pounds, etc.
    match_score: float  # 0-1 confidence score
    match_reasons: List[str] = field(default_factory=list)


@dataclass
class StatementInfo:
    """Metadata extracted from a bank statement."""
    bank_name: str
    account_number: str
    sort_code: Optional[str] = None
    statement_date: Optional[datetime] = None
    period_start: Optional[datetime] = None
    period_end: Optional[datetime] = None
    opening_balance: Optional[float] = None
    closing_balance: Optional[float] = None


class StatementReconciler:
    """
    Handles extraction and matching of bank statement transactions
    against Opera cashbook entries.
    """

    def __init__(self, sql_connector, anthropic_api_key: Optional[str] = None):
        """
        Initialize the reconciler.

        Args:
            sql_connector: SQLConnector instance for Opera database queries
            anthropic_api_key: Optional API key (uses ANTHROPIC_API_KEY env var if not provided)
        """
        self.sql_connector = sql_connector
        self.client = anthropic.Anthropic(api_key=anthropic_api_key) if anthropic_api_key else anthropic.Anthropic()

    def extract_transactions_from_pdf(self, pdf_path: str) -> Tuple[StatementInfo, List[StatementTransaction]]:
        """
        Extract transactions from a bank statement PDF using Claude Vision.

        Args:
            pdf_path: Path to the PDF file

        Returns:
            Tuple of (StatementInfo, list of StatementTransaction)
        """
        # Read and encode the PDF
        pdf_bytes = Path(pdf_path).read_bytes()
        pdf_base64 = base64.standard_b64encode(pdf_bytes).decode('utf-8')

        # Use Claude to extract transactions
        extraction_prompt = """Analyze this bank statement and extract ALL transactions.

Return a JSON object with this exact structure:
{
    "statement_info": {
        "bank_name": "e.g. Barclays",
        "account_number": "e.g. 90764205",
        "sort_code": "e.g. 20-96-89",
        "statement_date": "YYYY-MM-DD",
        "period_start": "YYYY-MM-DD",
        "period_end": "YYYY-MM-DD",
        "opening_balance": 18076.42,
        "closing_balance": 51574.97
    },
    "transactions": [
        {
            "date": "YYYY-MM-DD",
            "description": "Full description text",
            "money_out": 22.00,
            "money_in": null,
            "balance": 18054.42,
            "type": "DD|STO|Giro|Card|Transfer|Other",
            "reference": "Any reference number if present"
        }
    ]
}

Important:
- Extract EVERY transaction, don't skip any
- Use the year from the statement date for all transactions
- money_out and money_in should be numbers or null (not both populated)
- type should be: DD (Direct Debit), STO (Standing Order), Giro (Direct Credit/BACS), Card (Card payment), Transfer, or Other
- Include the full description exactly as shown
- Return ONLY valid JSON, no other text"""

        response = self.client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=8000,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "document",
                            "source": {
                                "type": "base64",
                                "media_type": "application/pdf",
                                "data": pdf_base64
                            }
                        },
                        {
                            "type": "text",
                            "text": extraction_prompt
                        }
                    ]
                }
            ]
        )

        # Parse the response
        response_text = response.content[0].text

        # Try to extract JSON from the response
        json_match = re.search(r'\{[\s\S]*\}', response_text)
        if not json_match:
            raise ValueError(f"Could not extract JSON from Claude response: {response_text[:500]}")

        data = json.loads(json_match.group())

        # Parse statement info
        info_data = data.get('statement_info', {})
        statement_info = StatementInfo(
            bank_name=info_data.get('bank_name', 'Unknown'),
            account_number=info_data.get('account_number', ''),
            sort_code=info_data.get('sort_code'),
            statement_date=self._parse_date(info_data.get('statement_date')),
            period_start=self._parse_date(info_data.get('period_start')),
            period_end=self._parse_date(info_data.get('period_end')),
            opening_balance=info_data.get('opening_balance'),
            closing_balance=info_data.get('closing_balance')
        )

        # Parse transactions
        transactions = []
        for txn_data in data.get('transactions', []):
            date = self._parse_date(txn_data.get('date'))
            if not date:
                continue

            money_out = txn_data.get('money_out')
            money_in = txn_data.get('money_in')

            # Calculate signed amount (positive = in, negative = out)
            if money_out and money_out > 0:
                amount = -money_out
            elif money_in and money_in > 0:
                amount = money_in
            else:
                continue  # Skip if no amount

            txn = StatementTransaction(
                date=date,
                description=txn_data.get('description', ''),
                amount=amount,
                balance=txn_data.get('balance'),
                transaction_type=txn_data.get('type'),
                reference=txn_data.get('reference'),
                raw_text=json.dumps(txn_data)
            )
            transactions.append(txn)

        logger.info(f"Extracted {len(transactions)} transactions from {pdf_path}")
        return statement_info, transactions

    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse a date string in various formats."""
        if not date_str:
            return None

        formats = ['%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%Y/%m/%d']
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        return None

    def get_unreconciled_entries(self, bank_code: str, date_from: Optional[datetime] = None,
                                  date_to: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Get unreconciled cashbook entries from Opera.

        Args:
            bank_code: The bank account code (e.g., 'BC010')
            date_from: Optional start date filter
            date_to: Optional end date filter

        Returns:
            List of unreconciled entry dictionaries
        """
        date_filter = ""
        if date_from:
            date_filter += f" AND ae_date >= '{date_from.strftime('%Y-%m-%d')}'"
        if date_to:
            date_filter += f" AND ae_date <= '{date_to.strftime('%Y-%m-%d')}'"

        query = f"""
            SELECT
                ae_entry,
                ae_date,
                ae_ref,
                ae_cbtype,
                ae_value / 100.0 as value_pounds,
                ae_detail,
                ae_reclnum,
                ae_statln
            FROM aentry WITH (NOLOCK)
            WHERE ae_bank = '{bank_code}'
              AND ae_reclnum = 0
              AND ae_complet = 1
              {date_filter}
            ORDER BY ae_date, ae_entry
        """

        df = self.sql_connector.execute_query(query)
        if df is None or df.empty:
            return []

        entries = []
        for _, row in df.iterrows():
            entries.append({
                'ae_entry': row['ae_entry'],
                'ae_date': row['ae_date'],
                'ae_ref': row['ae_ref'],
                'ae_cbtype': row['ae_cbtype'],
                'value_pounds': float(row['value_pounds']),
                'ae_detail': row['ae_detail'],
                'ae_reclnum': row['ae_reclnum'],
                'ae_statln': row['ae_statln']
            })

        return entries

    def match_transactions(self, statement_txns: List[StatementTransaction],
                          opera_entries: List[Dict[str, Any]],
                          date_tolerance_days: int = 3) -> Tuple[List[ReconciliationMatch],
                                                                   List[StatementTransaction],
                                                                   List[Dict[str, Any]]]:
        """
        Match statement transactions against Opera entries.

        Args:
            statement_txns: Transactions extracted from statement
            opera_entries: Unreconciled Opera entries
            date_tolerance_days: How many days difference to allow for date matching

        Returns:
            Tuple of (matches, unmatched_statement_txns, unmatched_opera_entries)
        """
        matches = []
        used_statement_indices = set()
        used_opera_indices = set()

        # First pass: exact amount + close date matches
        for i, stmt_txn in enumerate(statement_txns):
            if i in used_statement_indices:
                continue

            best_match = None
            best_score = 0
            best_opera_idx = None

            for j, opera_entry in enumerate(opera_entries):
                if j in used_opera_indices:
                    continue

                score, reasons = self._calculate_match_score(
                    stmt_txn, opera_entry, date_tolerance_days
                )

                if score > best_score and score >= 0.7:  # Minimum threshold
                    best_match = opera_entry
                    best_score = score
                    best_opera_idx = j
                    best_reasons = reasons

            if best_match:
                matches.append(ReconciliationMatch(
                    statement_txn=stmt_txn,
                    opera_entry=best_match,
                    match_score=best_score,
                    match_reasons=best_reasons
                ))
                used_statement_indices.add(i)
                used_opera_indices.add(best_opera_idx)

        # Collect unmatched items
        unmatched_statement = [txn for i, txn in enumerate(statement_txns)
                               if i not in used_statement_indices]
        unmatched_opera = [entry for i, entry in enumerate(opera_entries)
                          if i not in used_opera_indices]

        logger.info(f"Matched {len(matches)} transactions, "
                   f"{len(unmatched_statement)} statement txns unmatched, "
                   f"{len(unmatched_opera)} Opera entries unmatched")

        return matches, unmatched_statement, unmatched_opera

    def _calculate_match_score(self, stmt_txn: StatementTransaction,
                               opera_entry: Dict[str, Any],
                               date_tolerance_days: int) -> Tuple[float, List[str]]:
        """
        Calculate how well a statement transaction matches an Opera entry.

        Returns:
            Tuple of (score 0-1, list of reasons)
        """
        score = 0.0
        reasons = []

        # Amount match (most important) - must match exactly
        stmt_amount = round(stmt_txn.amount, 2)
        opera_amount = round(opera_entry['value_pounds'], 2)

        if abs(stmt_amount - opera_amount) < 0.01:
            score += 0.6
            reasons.append(f"Amount matches exactly: {stmt_amount}")
        else:
            # No match if amounts don't match
            return 0.0, []

        # Date match
        stmt_date = stmt_txn.date.date() if isinstance(stmt_txn.date, datetime) else stmt_txn.date
        opera_date = opera_entry['ae_date']
        if isinstance(opera_date, datetime):
            opera_date = opera_date.date()

        date_diff = abs((stmt_date - opera_date).days)

        if date_diff == 0:
            score += 0.3
            reasons.append("Date matches exactly")
        elif date_diff <= date_tolerance_days:
            date_score = 0.3 * (1 - date_diff / (date_tolerance_days + 1))
            score += date_score
            reasons.append(f"Date within {date_diff} days")
        else:
            # Date too far off, reduce score significantly
            score *= 0.5
            reasons.append(f"Date differs by {date_diff} days")

        # Reference/description similarity bonus
        desc_lower = stmt_txn.description.lower()
        opera_ref = (opera_entry.get('ae_ref') or '').lower()
        opera_detail = (opera_entry.get('ae_detail') or '').lower()

        # Check for common keywords
        if opera_ref and opera_ref in desc_lower:
            score += 0.1
            reasons.append(f"Reference '{opera_ref}' found in description")
        elif opera_detail:
            # Check if any significant words match
            opera_words = set(w for w in opera_detail.split() if len(w) > 3)
            desc_words = set(w for w in desc_lower.split() if len(w) > 3)
            common_words = opera_words & desc_words
            if common_words:
                score += 0.05
                reasons.append(f"Common words: {', '.join(list(common_words)[:3])}")

        return min(score, 1.0), reasons

    def reconcile_matches(self, bank_code: str, matches: List[ReconciliationMatch],
                         statement_balance: float, statement_date: datetime) -> Dict[str, Any]:
        """
        Mark matched entries as reconciled in Opera.

        Args:
            bank_code: The bank account code
            matches: List of confirmed matches to reconcile
            statement_balance: The closing balance from the statement
            statement_date: The date of the statement

        Returns:
            Dict with reconciliation results
        """
        if not matches:
            return {'success': False, 'message': 'No matches to reconcile', 'reconciled_count': 0}

        # Get the next reconciliation batch number
        batch_query = f"""
            SELECT ISNULL(MAX(ae_reclnum), 0) + 1 as next_batch
            FROM aentry WITH (NOLOCK)
            WHERE ae_bank = '{bank_code}'
        """
        batch_result = self.sql_connector.execute_query(batch_query)
        next_batch = int(batch_result.iloc[0]['next_batch']) if batch_result is not None else 1

        # Get the last statement line number
        line_query = f"""
            SELECT ISNULL(nk_lstrecl, 0) as last_line
            FROM nbank WITH (NOLOCK)
            WHERE nk_code = '{bank_code}'
        """
        line_result = self.sql_connector.execute_query(line_query)
        last_line = int(line_result.iloc[0]['last_line']) if line_result is not None else 0

        # Mark each matched entry as reconciled
        entry_ids = [m.opera_entry['ae_entry'] for m in matches]
        reconciled_count = 0

        for i, entry_id in enumerate(entry_ids):
            line_number = (i + 1) * 10  # 10, 20, 30, etc.

            update_query = f"""
                UPDATE aentry
                SET ae_reclnum = {next_batch},
                    ae_statln = {line_number},
                    ae_recdate = '{statement_date.strftime('%Y-%m-%d')}',
                    ae_recbal = {int(statement_balance * 100)}
                WHERE ae_entry = {entry_id}
                  AND ae_bank = '{bank_code}'
                  AND ae_reclnum = 0
            """

            result = self.sql_connector.execute_non_query(update_query)
            if result:
                reconciled_count += 1

        # Update nbank with new reconciled balance
        if reconciled_count > 0:
            nbank_update = f"""
                UPDATE nbank
                SET nk_recbal = {int(statement_balance * 100)},
                    nk_lstrecl = {next_batch},
                    nk_lststno = nk_lststno + 1,
                    nk_lststdt = '{statement_date.strftime('%Y-%m-%d')}'
                WHERE nk_code = '{bank_code}'
            """
            self.sql_connector.execute_non_query(nbank_update)

        return {
            'success': True,
            'message': f'Reconciled {reconciled_count} entries',
            'reconciled_count': reconciled_count,
            'batch_number': next_batch,
            'statement_balance': statement_balance
        }
