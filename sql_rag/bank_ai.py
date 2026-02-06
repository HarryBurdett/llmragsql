"""
AI-Assisted Bank Transaction Categorization

Provides AI-powered suggestions for:
- Matching unmatched transactions to accounts
- Suggesting nominal accounts for direct postings
- Detecting recurring transaction patterns
- Explaining match decisions

Uses the existing LLM infrastructure (supports Ollama, OpenAI, Anthropic, etc.)
"""

import json
import logging
import re
from dataclasses import dataclass
from datetime import date
from typing import List, Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class AISuggestion:
    """
    Represents an AI-generated suggestion for a transaction.
    """
    suggested_account: str
    confidence: float  # 0.0 - 1.0
    reason: str
    alternative_accounts: List[str] = None
    is_nominal_posting: bool = False  # True if suggesting direct nominal posting


@dataclass
class PatternDetection:
    """
    Represents a detected recurring transaction pattern.
    """
    pattern_name: str
    matching_transactions: List[int]  # Indices of matching transactions
    suggested_account: Optional[str]
    frequency: str  # 'weekly', 'monthly', 'quarterly', etc.
    confidence: float


class BankAICategorizer:
    """
    AI-assisted categorization for bank statement transactions.

    Uses LLM to:
    1. Suggest best match from multiple candidates
    2. Suggest nominal accounts for direct postings (bank charges, etc.)
    3. Detect recurring transaction patterns
    """

    def __init__(self, llm_interface, sql_connector=None):
        """
        Initialize categorizer with LLM interface.

        Args:
            llm_interface: LLMInterface instance (from sql_rag.llm)
            sql_connector: Optional SQLConnector for querying Opera data
        """
        self.llm = llm_interface
        self.sql = sql_connector
        self._pattern_cache: Dict[str, str] = {}

    def suggest_match(
        self,
        bank_name: str,
        amount: float,
        txn_date: date,
        reference: str,
        candidates: List[Dict[str, Any]],
        is_receipt: bool = True
    ) -> Optional[AISuggestion]:
        """
        Use LLM to suggest the best match from multiple candidates.

        Args:
            bank_name: Name from bank statement
            amount: Transaction amount
            txn_date: Transaction date
            reference: Transaction reference
            candidates: List of potential matches with 'account', 'name', 'score'
            is_receipt: True if receipt (customer), False if payment (supplier)

        Returns:
            AISuggestion with recommended account or None
        """
        if not candidates:
            return None

        # Format candidates for the prompt
        candidate_text = "\n".join([
            f"{i+1}. {c.get('account', 'N/A')} - {c.get('name', 'Unknown')} (Match score: {c.get('score', 0):.0%})"
            for i, c in enumerate(candidates[:10])  # Limit to top 10
        ])

        txn_type = "customer receipt" if is_receipt else "supplier payment"

        prompt = f"""You are a financial assistant helping to categorize bank transactions.

Bank Transaction Details:
- Name on statement: {bank_name}
- Amount: £{abs(amount):.2f}
- Date: {txn_date.strftime('%d/%m/%Y')}
- Reference: {reference or 'None'}
- Type: {txn_type}

Possible account matches:
{candidate_text}

Based on the transaction name and details, which account is the BEST match?
Consider:
- Name similarity (the statement name may be abbreviated or truncated)
- Transaction type (receipt = customer, payment = supplier)
- Common abbreviations in business names

Respond with ONLY a JSON object in this format:
{{"account": "ACCOUNT_CODE", "confidence": 0.85, "reason": "Brief explanation"}}

If none of the candidates are a good match, respond with:
{{"account": null, "confidence": 0, "reason": "No suitable match found"}}
"""

        try:
            response = self.llm.get_completion(prompt, temperature=0.1, max_tokens=200)
            return self._parse_suggestion_response(response, candidates)
        except Exception as e:
            logger.error(f"Error getting AI suggestion: {e}")
            return None

    def suggest_nominal_account(
        self,
        bank_name: str,
        amount: float,
        txn_date: date,
        reference: str,
        nominal_accounts: List[Dict[str, Any]]
    ) -> Optional[AISuggestion]:
        """
        Suggest a nominal account for direct posting.

        For transactions that don't match any customer/supplier (e.g., bank charges,
        subscriptions, utilities), suggest an appropriate nominal account.

        Args:
            bank_name: Name from bank statement
            amount: Transaction amount
            txn_date: Transaction date
            reference: Transaction reference
            nominal_accounts: List of nominal accounts with 'code', 'name', 'category'

        Returns:
            AISuggestion with recommended nominal account or None
        """
        # Format nominal accounts for the prompt
        accounts_text = "\n".join([
            f"- {a.get('code', 'N/A')}: {a.get('name', 'Unknown')} ({a.get('category', 'General')})"
            for a in nominal_accounts[:30]  # Limit to 30
        ])

        txn_type = "credit/receipt" if amount > 0 else "debit/payment"

        prompt = f"""You are a financial assistant helping to categorize bank transactions for direct nominal ledger posting.

Bank Transaction Details:
- Name on statement: {bank_name}
- Amount: £{abs(amount):.2f}
- Direction: {txn_type}
- Date: {txn_date.strftime('%d/%m/%Y')}
- Reference: {reference or 'None'}

This transaction does not match any customer or supplier, so it needs to be posted directly to a nominal account.

Available Nominal Accounts:
{accounts_text}

Based on the transaction description, which nominal account is most appropriate?
Consider:
- Common categories: Bank charges, Subscriptions, Utilities, Professional fees, etc.
- The transaction direction (debit vs credit)

Respond with ONLY a JSON object in this format:
{{"account": "NOMINAL_CODE", "confidence": 0.75, "reason": "Brief explanation"}}

If unsure, provide your best guess with a lower confidence score.
"""

        try:
            response = self.llm.get_completion(prompt, temperature=0.2, max_tokens=200)
            suggestion = self._parse_suggestion_response(response, nominal_accounts)
            if suggestion:
                suggestion.is_nominal_posting = True
            return suggestion
        except Exception as e:
            logger.error(f"Error getting nominal account suggestion: {e}")
            return None

    def detect_patterns(
        self,
        transactions: List[Dict[str, Any]]
    ) -> List[PatternDetection]:
        """
        Detect recurring transaction patterns.

        Identifies transactions that appear regularly (subscriptions,
        standing orders, etc.) and suggests categorizations.

        Args:
            transactions: List of transactions with 'name', 'amount', 'date'

        Returns:
            List of PatternDetection objects
        """
        if not transactions or len(transactions) < 3:
            return []

        # Group transactions by similar names
        groups = self._group_similar_transactions(transactions)

        patterns = []
        for group_name, group_txns in groups.items():
            if len(group_txns) < 2:
                continue

            # Analyze frequency
            frequency = self._detect_frequency(group_txns)
            if frequency:
                # Check if amounts are consistent
                amounts = [t['amount'] for t in group_txns]
                avg_amount = sum(amounts) / len(amounts)
                amount_variance = sum((a - avg_amount) ** 2 for a in amounts) / len(amounts)

                confidence = 0.8 if amount_variance < 1 else 0.6

                patterns.append(PatternDetection(
                    pattern_name=group_name,
                    matching_transactions=[t.get('index', i) for i, t in enumerate(group_txns)],
                    suggested_account=None,  # Could be enhanced to suggest accounts
                    frequency=frequency,
                    confidence=confidence
                ))

        return patterns

    def explain_match_decision(
        self,
        bank_name: str,
        matched_account: str,
        matched_name: str,
        match_score: float,
        algorithm_breakdown: Dict[str, float]
    ) -> str:
        """
        Generate a human-readable explanation for a match decision.

        Args:
            bank_name: Name from bank statement
            matched_account: Account that was matched
            matched_name: Name of the matched account
            match_score: Overall match score
            algorithm_breakdown: Dict of algorithm -> score

        Returns:
            Human-readable explanation
        """
        breakdown_text = "\n".join([
            f"- {algo}: {score:.0%}"
            for algo, score in algorithm_breakdown.items()
        ])

        prompt = f"""Explain why this bank transaction was matched to this account in simple terms:

Bank Statement Name: {bank_name}
Matched to: {matched_account} - {matched_name}
Overall Match Score: {match_score:.0%}

Algorithm Scores:
{breakdown_text}

Provide a brief (2-3 sentences) explanation of why this match was made.
Focus on what made the names similar (abbreviations, word order, etc.)
"""

        try:
            response = self.llm.get_completion(prompt, temperature=0.3, max_tokens=150)
            return response.strip()
        except Exception as e:
            logger.error(f"Error generating explanation: {e}")
            return f"Matched based on name similarity (score: {match_score:.0%})"

    def _parse_suggestion_response(
        self,
        response: str,
        candidates: List[Dict[str, Any]]
    ) -> Optional[AISuggestion]:
        """
        Parse LLM response into AISuggestion.
        """
        try:
            # Extract JSON from response (handle markdown code blocks)
            json_match = re.search(r'\{[^{}]*\}', response, re.DOTALL)
            if not json_match:
                logger.warning(f"No JSON found in response: {response[:200]}")
                return None

            data = json.loads(json_match.group())

            account = data.get('account')
            if not account:
                return None

            # Verify account exists in candidates
            account_upper = account.upper().strip()
            valid_accounts = {c.get('account', '').upper().strip() for c in candidates}
            valid_accounts.update({c.get('code', '').upper().strip() for c in candidates})

            if account_upper not in valid_accounts:
                # Try to find partial match
                for c in candidates:
                    if account_upper in c.get('account', '').upper() or account_upper in c.get('code', '').upper():
                        account = c.get('account') or c.get('code')
                        break
                else:
                    logger.warning(f"Suggested account {account} not in candidates")
                    return None

            confidence = float(data.get('confidence', 0.5))
            reason = data.get('reason', 'AI suggestion')

            return AISuggestion(
                suggested_account=account,
                confidence=min(1.0, max(0.0, confidence)),
                reason=reason,
                alternative_accounts=[]
            )

        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse JSON response: {e}")
            return None
        except Exception as e:
            logger.error(f"Error parsing suggestion response: {e}")
            return None

    def _group_similar_transactions(
        self,
        transactions: List[Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Group transactions by similar names.
        """
        groups: Dict[str, List[Dict[str, Any]]] = {}

        for i, txn in enumerate(transactions):
            name = txn.get('name', '').upper().strip()
            if not name:
                continue

            txn_copy = dict(txn)
            txn_copy['index'] = i

            # Simple grouping by first 10 characters
            # Could be enhanced with fuzzy matching
            key = name[:10]

            # Try to find existing similar group
            matched_key = None
            for existing_key in groups.keys():
                if existing_key.startswith(key[:5]) or key.startswith(existing_key[:5]):
                    matched_key = existing_key
                    break

            if matched_key:
                groups[matched_key].append(txn_copy)
            else:
                groups[key] = [txn_copy]

        return groups

    def _detect_frequency(
        self,
        transactions: List[Dict[str, Any]]
    ) -> Optional[str]:
        """
        Detect the frequency of transactions (weekly, monthly, etc.)
        """
        if len(transactions) < 2:
            return None

        dates = []
        for txn in transactions:
            txn_date = txn.get('date')
            if isinstance(txn_date, str):
                try:
                    from datetime import datetime
                    txn_date = datetime.strptime(txn_date, '%Y-%m-%d').date()
                except ValueError:
                    continue
            if txn_date:
                dates.append(txn_date)

        if len(dates) < 2:
            return None

        dates.sort()

        # Calculate average days between transactions
        gaps = [(dates[i+1] - dates[i]).days for i in range(len(dates) - 1)]
        avg_gap = sum(gaps) / len(gaps)

        # Determine frequency
        if 5 <= avg_gap <= 9:
            return 'weekly'
        elif 25 <= avg_gap <= 35:
            return 'monthly'
        elif 85 <= avg_gap <= 95:
            return 'quarterly'
        elif 360 <= avg_gap <= 370:
            return 'yearly'
        else:
            return None


def create_bank_ai_categorizer(config=None, sql_connector=None) -> Optional[BankAICategorizer]:
    """
    Factory function to create BankAICategorizer with configured LLM.

    Args:
        config: Configuration object (ConfigParser)
        sql_connector: Optional SQLConnector instance

    Returns:
        BankAICategorizer instance or None if LLM unavailable
    """
    try:
        from sql_rag.llm import create_llm_instance

        if config is None:
            import configparser
            from pathlib import Path

            config_path = Path(__file__).parent.parent / 'config.ini'
            if config_path.exists():
                config = configparser.ConfigParser()
                config.read(config_path)
            else:
                logger.warning("Config file not found, AI categorization unavailable")
                return None

        llm = create_llm_instance(config)

        if not llm.check_api_available():
            logger.warning("LLM not available, AI categorization disabled")
            return None

        return BankAICategorizer(llm, sql_connector)

    except Exception as e:
        logger.error(f"Failed to create AI categorizer: {e}")
        return None
