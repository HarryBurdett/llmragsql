"""
Bank Statement Reconciliation Module - Opera 3 (FoxPro) Version

Extracts transactions from bank statement PDFs/images using Google Gemini Vision
and matches them against Opera 3's unreconciled cashbook entries for auto-reconciliation.

This is the Opera 3 (FoxPro) version of statement_reconcile.py.
"""

import google.generativeai as genai
import base64
import configparser
import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any
import logging

logger = logging.getLogger(__name__)

# Default config file path
DEFAULT_CONFIG_PATH = Path(__file__).parent.parent / 'config.ini'

# Import shared dataclasses from SQL SE version
from sql_rag.statement_reconcile import (
    StatementTransaction,
    ReconciliationMatch,
    StatementInfo
)


class StatementReconcilerOpera3:
    """
    Handles extraction and matching of bank statement transactions
    against Opera 3 (FoxPro) cashbook entries.
    """

    def __init__(self, opera3_reader, config: Optional[configparser.ConfigParser] = None,
                 config_path: Optional[str] = None, gemini_api_key: Optional[str] = None):
        """
        Initialize the reconciler.

        Args:
            opera3_reader: Opera3Reader instance for FoxPro database access
            config: Optional ConfigParser instance with [gemini] section
            config_path: Optional path to config file (uses default if not provided)
            gemini_api_key: Optional API key override (falls back to config)
        """
        self.reader = opera3_reader

        # Load config if not provided
        if config is None:
            config = configparser.ConfigParser()
            cfg_path = config_path or DEFAULT_CONFIG_PATH
            if Path(cfg_path).exists():
                config.read(cfg_path)
                logger.info(f"Loaded config from {cfg_path}")
            else:
                logger.warning(f"Config file not found: {cfg_path}")

        self.config = config

        # Get API key: parameter > config > environment
        api_key = gemini_api_key
        if not api_key and config.has_section('gemini'):
            api_key = config.get('gemini', 'api_key', fallback='')
        if not api_key:
            api_key = os.environ.get('GEMINI_API_KEY', '')

        if not api_key:
            raise ValueError(
                "Gemini API key not found. Please set it in config.ini [gemini] section, "
                "or set GEMINI_API_KEY environment variable"
            )

        # Get model from config (with sensible default for Vision tasks)
        self.model_name = 'gemini-2.0-flash'
        if config.has_section('gemini'):
            configured_model = config.get('gemini', 'model', fallback='')
            if configured_model:
                self.model_name = configured_model

        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(self.model_name)
        logger.info(f"StatementReconcilerOpera3 initialized with Gemini model: {self.model_name}")

    def find_bank_code_from_statement(self, statement_info: StatementInfo) -> Optional[Dict[str, Any]]:
        """
        Find the Opera 3 bank account code from statement bank details.

        Matches by sort code and account number from the statement against
        the nbank table in Opera 3.

        Args:
            statement_info: Extracted statement info with bank details

        Returns:
            Dict with bank_code, description, and match_type, or None if not found
        """
        sort_code = statement_info.sort_code
        account_number = statement_info.account_number

        if not sort_code and not account_number:
            logger.warning("No sort code or account number in statement to match")
            return None

        # Normalize sort code (remove dashes/spaces)
        sort_code_clean = sort_code.replace('-', '').replace(' ', '') if sort_code else ''
        account_number_clean = account_number.replace(' ', '') if account_number else ''

        # Read nbank table from Opera 3
        try:
            nbank_records = self.reader.read_table('nbank')
        except Exception as e:
            logger.error(f"Failed to read nbank table: {e}")
            return None

        if not nbank_records:
            logger.warning("No bank accounts found in nbank")
            return None

        # Try to match
        for row in nbank_records:
            opera_sort = (row.get('nk_sort') or '').strip().replace('-', '').replace(' ', '')
            opera_acct = (row.get('nk_number') or '').strip().replace(' ', '')

            # Match by both sort code and account number (best match)
            if sort_code_clean and account_number_clean:
                if opera_sort == sort_code_clean and opera_acct == account_number_clean:
                    code = (row.get('nk_acnt') or '').strip()
                    logger.info(f"Matched bank account by sort code + account number: {code}")
                    return {
                        'bank_code': code,
                        'description': (row.get('nk_desc') or '').strip(),
                        'match_type': 'exact',
                        'sort_code': row.get('nk_sort', '').strip(),
                        'account_number': row.get('nk_number', '').strip()
                    }

            # Match by account number only (fallback)
            if account_number_clean and opera_acct == account_number_clean:
                code = (row.get('nk_acnt') or '').strip()
                logger.info(f"Matched bank account by account number: {code}")
                return {
                    'bank_code': code,
                    'description': (row.get('nk_desc') or '').strip(),
                    'match_type': 'account_number',
                    'sort_code': row.get('nk_sort', '').strip(),
                    'account_number': row.get('nk_number', '').strip()
                }

            # Match by sort code only (weaker match)
            if sort_code_clean and opera_sort == sort_code_clean:
                code = (row.get('nk_acnt') or '').strip()
                logger.info(f"Matched bank account by sort code: {code}")
                return {
                    'bank_code': code,
                    'description': (row.get('nk_desc') or '').strip(),
                    'match_type': 'sort_code',
                    'sort_code': row.get('nk_sort', '').strip(),
                    'account_number': row.get('nk_number', '').strip()
                }

        logger.warning(f"No matching bank account found for sort code '{sort_code}', account '{account_number}'")
        return None

    def check_reconciliation_in_progress(self, bank_acnt: str) -> Dict[str, Any]:
        """
        Check if there's a reconciliation in progress in Opera 3 that needs to be cleared.

        Checks for entries with ae_tmpstat populated (partial reconciliation) for the given bank.

        Args:
            bank_acnt: The Opera bank account code

        Returns:
            Dict with 'in_progress' (bool) and details if true
        """
        try:
            aentry_records = self.reader.read_table('aentry')
            if not aentry_records:
                return {'in_progress': False}

            bank_code_upper = bank_acnt.strip().upper()
            partial_count = 0
            for entry in aentry_records:
                ae_acnt = str(entry.get('AE_ACNT', entry.get('ae_acnt', ''))).strip().upper()
                if ae_acnt != bank_code_upper:
                    continue
                ae_tmpstat = entry.get('AE_TMPSTAT', entry.get('ae_tmpstat', 0))
                try:
                    tmpstat_val = int(float(ae_tmpstat or 0))
                except (ValueError, TypeError):
                    tmpstat_val = 0
                if tmpstat_val != 0:
                    partial_count += 1

            if partial_count > 0:
                logger.info(f"Bank {bank_acnt} has {partial_count} entries with ae_tmpstat set (will be cleared on reconciliation)")
                return {
                    'in_progress': True,
                    'partial_entries': partial_count,
                    'message': f"{partial_count} entries have partial reconciliation markers from Opera or a previous session. These will be cleared automatically when you reconcile."
                }

            return {'in_progress': False}

        except Exception as e:
            logger.warning(f"Error checking reconciliation in progress for {bank_acnt}: {e}")
            return {'in_progress': False}

    def validate_statement_bank(self, bank_acnt: str, statement_info: StatementInfo) -> Dict[str, Any]:
        """
        Validate that the statement's bank details match the selected Opera 3 bank account.

        Args:
            bank_acnt: The Opera bank account code selected by user
            statement_info: Extracted statement info with bank details

        Returns:
            Dict with 'valid' (bool), 'opera_bank' info, and 'error' if not valid
        """
        # Read nbank and find the selected account
        try:
            nbank_records = self.reader.query('nbank', filters={'nk_acnt': bank_acnt})
        except Exception as e:
            return {
                'valid': False,
                'error': f"Failed to read bank account: {e}",
                'opera_bank': None
            }

        if not nbank_records:
            return {
                'valid': False,
                'error': f"Bank account '{bank_acnt}' not found in Opera 3",
                'opera_bank': None
            }

        row = nbank_records[0]
        opera_bank = {
            'code': (row.get('nk_acnt') or '').strip(),
            'description': (row.get('nk_desc') or '').strip(),
            'sort_code': (row.get('nk_sort') or '').strip(),
            'account_number': (row.get('nk_number') or '').strip()
        }

        # Normalize values for comparison
        opera_sort = opera_bank['sort_code'].replace('-', '').replace(' ', '')
        opera_acct = opera_bank['account_number'].replace(' ', '')
        stmt_sort = (statement_info.sort_code or '').replace('-', '').replace(' ', '')
        stmt_acct = (statement_info.account_number or '').replace(' ', '')

        # Check if statement matches the selected account
        sort_match = not opera_sort or not stmt_sort or opera_sort == stmt_sort
        acct_match = not opera_acct or not stmt_acct or opera_acct == stmt_acct

        if sort_match and acct_match:
            match_type = 'exact' if (opera_sort and stmt_sort and opera_acct and stmt_acct) else 'partial'
            logger.info(f"Statement validated against bank {bank_acnt}: {match_type} match")
            return {
                'valid': True,
                'match_type': match_type,
                'opera_bank': opera_bank,
                'statement_sort_code': statement_info.sort_code,
                'statement_account': statement_info.account_number
            }

        # Mismatch - provide helpful error
        error_parts = []
        if opera_sort and stmt_sort and opera_sort != stmt_sort:
            error_parts.append(f"sort code (Opera: {opera_bank['sort_code']}, Statement: {statement_info.sort_code})")
        if opera_acct and stmt_acct and opera_acct != stmt_acct:
            error_parts.append(f"account number (Opera: {opera_bank['account_number']}, Statement: {statement_info.account_number})")

        actual_bank = self.find_bank_code_from_statement(statement_info)

        error_msg = f"Statement does not match selected bank account '{bank_acnt}'. Mismatch in: {', '.join(error_parts)}."
        if actual_bank:
            error_msg += f" This statement appears to be for account '{actual_bank['bank_code']}' ({actual_bank['description']})."

        logger.warning(error_msg)
        return {
            'valid': False,
            'error': error_msg,
            'opera_bank': opera_bank,
            'statement_sort_code': statement_info.sort_code,
            'statement_account': statement_info.account_number,
            'suggested_bank': actual_bank
        }

    def extract_statement_info_only(self, pdf_path: str) -> Optional[Dict]:
        """
        Lightweight extraction — gets only statement header info (balances, dates,
        bank details) without extracting individual transactions. Much faster than
        full extraction, suitable for scanning.

        Returns dict with opening_balance, closing_balance, period_start, period_end,
        bank_name, account_number, sort_code — or None on failure.
        """
        pdf_bytes = Path(pdf_path).read_bytes()

        # Check cache first
        from sql_rag.pdf_extraction_cache import get_extraction_cache
        cache = get_extraction_cache()
        pdf_hash = cache.hash_pdf(pdf_bytes)
        cached = cache.get(pdf_hash)
        if cached:
            info_data, _ = cached
            return info_data

        prompt = """Look at this bank statement PDF and extract the statement details.

Look carefully for the opening balance. It may be labelled as:
- "Balance brought forward" or "Opening balance" (traditional banks)
- "Money in/out" summary showing start balance (Monzo, Starling)
- The first line item showing a starting balance before any transactions
- A summary section at the top showing the account balance at start of period

If you still cannot find it, look at the VERY FIRST transaction chronologically,
take its running balance, and reverse the transaction to get the balance before it.

Return ONLY this JSON:
{
    "bank_name": "Bank name",
    "account_number": "Account number",
    "sort_code": "Sort code (e.g. 12-34-56)",
    "statement_date": "YYYY-MM-DD",
    "period_start": "YYYY-MM-DD",
    "period_end": "YYYY-MM-DD",
    "opening_balance": 12345.67,
    "closing_balance": 12345.67
}

IMPORTANT: Return actual values from this document, not examples. Return ONLY valid JSON."""

        try:
            file_part = {"mime_type": "application/pdf", "data": pdf_bytes}
            response = self.model.generate_content([file_part, prompt])
            json_match = re.search(r'\{[\s\S]*\}', response.text)
            if not json_match:
                return None
            info_data = json.loads(json_match.group())
            # Mark as info-only so full extraction knows to re-extract
            info_data['_info_only'] = True
            cache.put(pdf_hash, info_data, [])
            logger.info(f"Extracted statement info: open={info_data.get('opening_balance')}, close={info_data.get('closing_balance')}")
            return info_data
        except Exception as e:
            logger.warning(f"Statement info extraction failed: {e}")
            return None

    def extract_transactions_from_pdf(self, pdf_path: str) -> Tuple[StatementInfo, List[StatementTransaction]]:
        """
        Extract transactions from a bank statement PDF using Gemini Vision.
        Results are cached by PDF content hash to avoid redundant API calls.

        Args:
            pdf_path: Path to the PDF file

        Returns:
            Tuple of (StatementInfo, list of StatementTransaction)
        """
        # Read the PDF
        pdf_bytes = Path(pdf_path).read_bytes()

        # Check cache first
        from sql_rag.pdf_extraction_cache import get_extraction_cache
        cache = get_extraction_cache()
        pdf_hash = cache.hash_pdf(pdf_bytes)
        cached = cache.get(pdf_hash)
        if cached:
            info_data, raw_transactions = cached
            # Skip info-only cache entries (from lightweight scan extraction)
            if not info_data.get('_info_only'):
                return self._parse_extraction_result(info_data, raw_transactions, pdf_path)

        # Use Gemini to extract transactions
        extraction_prompt = """Analyze this bank statement PDF and extract ALL transactions and statement details.

CRITICAL INSTRUCTIONS:
1. Extract the ACTUAL values from this specific PDF document
2. Do NOT use example/placeholder values like "2024-01-01" or 1000.00
3. Look carefully at the statement header area for account details and balances

Return a JSON object with this structure:
{
    "statement_info": {
        "bank_name": "The bank name shown on the statement (e.g., NatWest, Barclays, HSBC, Lloyds)",
        "account_number": "The actual account number from the statement",
        "sort_code": "The actual sort code (e.g., 12-34-56)",
        "statement_date": "The date of the statement in YYYY-MM-DD format",
        "period_start": "The period FROM date in YYYY-MM-DD format (often shown as 'Statement period: DD Mon YYYY to DD Mon YYYY')",
        "period_end": "The period TO date in YYYY-MM-DD format",
        "opening_balance": "The OPENING/BROUGHT FORWARD balance as a decimal number (e.g., 12345.67)",
        "closing_balance": "The CLOSING/CARRIED FORWARD balance as a decimal number"
    },
    "transactions": [
        {
            "date": "YYYY-MM-DD",
            "description": "Full description text from statement",
            "money_out": null or amount,
            "money_in": null or amount,
            "balance": null or running balance,
            "type": "DD|STO|Giro|Card|Transfer|Other",
            "reference": "Any reference number if present"
        }
    ]
}

IMPORTANT EXTRACTION RULES:
- opening_balance: Look for "Balance brought forward", "Opening balance", "Previous balance", "Money in/out" summary start balance, or the balance at the very start of the period. For Monzo/fintech: check the summary section showing starting balance
- closing_balance: Look for "Balance carried forward", "Closing balance", end balance, or the balance at the very end of the period
- Extract EVERY transaction — do not skip any. Include ALL pages of the statement
- For UK bank statements, balances are typically in GBP (£) - extract just the number
- period_start and period_end: Usually shown near the top as "Statement period" or similar
- Extract EVERY transaction row, including DD (Direct Debit), STO (Standing Order), Giro credits, card payments
- Use the year from the statement period for transaction dates if only day/month shown
- money_out = payments/debits (money leaving the account)
- money_in = receipts/credits (money entering the account)
- Return ONLY valid JSON, no other text or explanation"""

        # Create the file part for Gemini
        file_part = {
            "mime_type": "application/pdf",
            "data": pdf_bytes
        }

        response = self.model.generate_content([file_part, extraction_prompt])

        # Parse the response
        response_text = response.text

        # Try to extract JSON from the response
        json_match = re.search(r'\{[\s\S]*\}', response_text)
        if not json_match:
            raise ValueError(f"Could not extract JSON from Gemini response: {response_text[:500]}")

        json_text = json_match.group()

        # Try to parse JSON, with fallback repair attempts
        try:
            data = json.loads(json_text)
        except json.JSONDecodeError as e:
            # Attempt to repair common JSON issues
            logger.warning(f"JSON parse error: {e}. Attempting repair...")
            repaired = self._repair_json(json_text)
            try:
                data = json.loads(repaired)
                logger.info("JSON repair successful")
            except json.JSONDecodeError as e2:
                raise ValueError(f"Could not parse JSON even after repair: {e2}. Original error: {e}")

        # Extract raw data for caching
        info_data = data.get('statement_info', {})
        raw_transactions = data.get('transactions', [])

        # Cache the raw extraction result for future use
        cache.put(pdf_hash, info_data, raw_transactions,
                  model_name=self.model_name, file_size=len(pdf_bytes))

        return self._parse_extraction_result(info_data, raw_transactions, pdf_path)

    def _parse_extraction_result(
        self,
        info_data: Dict[str, Any],
        raw_transactions: List[Dict[str, Any]],
        source_label: str = ''
    ) -> Tuple[StatementInfo, List[StatementTransaction]]:
        """
        Parse raw extraction data (from Gemini or cache) into StatementInfo and transactions.

        Args:
            info_data: Statement info dict from extraction
            raw_transactions: List of raw transaction dicts from extraction
            source_label: Label for logging (e.g. file path)

        Returns:
            Tuple of (StatementInfo, list of StatementTransaction)
        """
        # Parse balance values - Gemini may return them as strings (possibly with commas)
        def _safe_float(val):
            if val is None: return None
            if isinstance(val, (int, float)): return float(val)
            s = str(val).replace(',', '').replace('£', '').replace('$', '').strip()
            return float(s) if s else None

        opening_bal_raw = info_data.get('opening_balance')
        closing_bal_raw = info_data.get('closing_balance')
        opening_balance = _safe_float(opening_bal_raw)
        closing_balance = _safe_float(closing_bal_raw)

        # Validate closing balance using transaction chain from opening.
        if raw_transactions and opening_balance is not None:
            try:
                remaining = list(range(len(raw_transactions)))
                current_bal = opening_balance
                chain_used = set()
                for _ in range(len(raw_transactions)):
                    found = False
                    for idx in remaining:
                        if idx in chain_used:
                            continue
                        t = raw_transactions[idx]
                        mi = _safe_float(t.get('money_in')) or 0
                        mo = _safe_float(t.get('money_out')) or 0
                        txn_bal = _safe_float(t.get('balance'))
                        if txn_bal is None:
                            continue
                        expected = round(current_bal + mi - mo, 2)
                        if abs(expected - txn_bal) < 0.02:
                            current_bal = txn_bal
                            chain_used.add(idx)
                            found = True
                            break
                    if not found:
                        break
                if chain_used:
                    closing_balance = current_bal
            except Exception:
                pass

        statement_info = StatementInfo(
            bank_name=info_data.get('bank_name', 'Unknown'),
            account_number=info_data.get('account_number', ''),
            sort_code=info_data.get('sort_code'),
            statement_date=self._parse_date(info_data.get('statement_date')),
            period_start=self._parse_date(info_data.get('period_start')),
            period_end=self._parse_date(info_data.get('period_end')),
            opening_balance=opening_balance,
            closing_balance=closing_balance
        )

        # Parse transactions
        transactions = []
        for txn_data in raw_transactions:
            date = self._parse_date(txn_data.get('date'))
            if not date:
                continue

            # Parse money values - Gemini may return them as formatted strings with commas
            def _parse_amount(val):
                if val is None: return None
                if isinstance(val, (int, float)): return float(val)
                s = str(val).replace(',', '').replace('£', '').replace('$', '').strip()
                return float(s) if s else None
            money_out_raw = txn_data.get('money_out')
            money_in_raw = txn_data.get('money_in')
            money_out = _parse_amount(money_out_raw)
            money_in = _parse_amount(money_in_raw)

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
                balance=_parse_amount(txn_data.get('balance')),
                transaction_type=txn_data.get('type'),
                reference=txn_data.get('reference'),
                raw_text=json.dumps(txn_data)
            )
            transactions.append(txn)

        logger.info(f"Extracted {len(transactions)} transactions from {source_label}")
        return statement_info, transactions

    def _repair_json(self, json_text: str) -> str:
        """
        Attempt to repair common JSON issues from LLM responses.
        """
        import re

        text = json_text

        # Remove trailing commas before ] or }
        text = re.sub(r',(\s*[}\]])', r'\1', text)

        # Fix single quotes to double quotes (careful with apostrophes)
        # Only replace single quotes that look like JSON string delimiters
        text = re.sub(r"(?<=[{,:\[])\s*'([^']*?)'\s*(?=[,}\]:])", r'"\1"', text)

        # Remove any trailing content after the last }
        last_brace = text.rfind('}')
        if last_brace != -1:
            text = text[:last_brace + 1]

        # Try to fix truncated arrays - close any unclosed brackets
        open_braces = text.count('{') - text.count('}')
        open_brackets = text.count('[') - text.count(']')

        # If we have unclosed structures, try to close them
        if open_brackets > 0 or open_braces > 0:
            # Find the last complete transaction entry and truncate there
            # Look for the pattern of a complete object in the transactions array
            match = re.search(r'("transactions"\s*:\s*\[.*?)(\{[^{}]*\})\s*,?\s*(\{[^}]*$)', text, re.DOTALL)
            if match:
                # Truncate at the last complete object
                text = match.group(1) + match.group(2) + ']}'
            else:
                # Just close the brackets
                text += ']' * open_brackets + '}' * open_braces

        return text

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

    def get_unreconciled_entries(self, bank_acnt: str, date_from: Optional[datetime] = None,
                                  date_to: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Get unreconciled cashbook entries from Opera 3.

        Args:
            bank_acnt: The bank account code (e.g., 'BC010')
            date_from: Optional start date filter
            date_to: Optional end date filter

        Returns:
            List of unreconciled entry dictionaries
        """
        # Read aentry table
        try:
            all_entries = self.reader.query('aentry', filters={'ae_acnt': bank_acnt})
        except Exception as e:
            logger.error(f"Failed to read aentry table: {e}")
            return []

        entries = []
        for row in all_entries:
            # Filter for unreconciled and completed entries
            if row.get('ae_reclnum', 0) != 0:
                continue
            if row.get('ae_complet', 0) != 1:
                continue

            # Apply date filters
            ae_date = row.get('ae_lstdate')
            if ae_date:
                if isinstance(ae_date, str):
                    ae_date = self._parse_date(ae_date)
                if date_from and ae_date and ae_date < date_from:
                    continue
                if date_to and ae_date and ae_date > date_to:
                    continue

            # Convert value from pence to pounds
            value_pence = row.get('ae_value', 0) or 0
            value_pounds = value_pence / 100.0

            entries.append({
                'ae_entry': row.get('ae_entry'),
                'ae_date': ae_date,
                'ae_ref': (row.get('ae_entref') or '').strip(),
                'ae_cbtype': row.get('ae_cbtype'),
                'value_pounds': value_pounds,
                'ae_detail': (row.get('ae_comment') or '').strip(),
                'ae_reclnum': row.get('ae_reclnum', 0),
                'ae_statln': row.get('ae_statln', 0)
            })

        # Sort by date
        entries.sort(key=lambda x: (x['ae_date'] or datetime.min, x['ae_entry'] or 0))

        return entries

    def get_all_entries(self, bank_acnt: str, date_from: Optional[datetime] = None,
                        date_to: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Get ALL cashbook entries (both reconciled and unreconciled) from Opera 3.

        Args:
            bank_acnt: The bank account code (e.g., 'BC010')
            date_from: Optional start date filter
            date_to: Optional end date filter

        Returns:
            List of entry dictionaries with reconciliation status
        """
        # Read aentry table
        try:
            all_entries = self.reader.query('aentry', filters={'ae_acnt': bank_acnt})
        except Exception as e:
            logger.error(f"Failed to read aentry table: {e}")
            return []

        entries = []
        for row in all_entries:
            # Only completed entries
            if row.get('ae_complet', 0) != 1:
                continue

            # Apply date filters
            ae_date = row.get('ae_lstdate')
            if ae_date:
                if isinstance(ae_date, str):
                    ae_date = self._parse_date(ae_date)
                if date_from and ae_date and ae_date < date_from:
                    continue
                if date_to and ae_date and ae_date > date_to:
                    continue

            # Convert value from pence to pounds
            value_pence = row.get('ae_value', 0) or 0
            value_pounds = value_pence / 100.0

            reclnum = row.get('ae_reclnum', 0) or 0

            entries.append({
                'ae_entry': row.get('ae_entry'),
                'ae_date': ae_date,
                'ae_ref': (row.get('ae_entref') or '').strip(),
                'ae_cbtype': row.get('ae_cbtype'),
                'value_pounds': value_pounds,
                'ae_detail': (row.get('ae_comment') or '').strip(),
                'ae_reclnum': reclnum,
                'ae_statln': row.get('ae_statln', 0),
                'is_reconciled': reclnum > 0
            })

        # Sort by date
        entries.sort(key=lambda x: (x['ae_date'] or datetime.min, x['ae_entry'] or 0))

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
            score += 0.5
            reasons.append(f"Amount matches exactly: {stmt_amount}")
        else:
            # No match if amounts don't match
            return 0.0, []

        # Reference/description similarity — check BEFORE date so that
        # a strong reference match can compensate for date differences
        desc_lower = stmt_txn.description.lower()
        opera_ref = (opera_entry.get('ae_ref') or '').lower().strip()
        opera_detail = (opera_entry.get('ae_detail') or '').lower().strip()

        ref_matched = False
        # Check if Opera reference appears in statement description (or vice versa)
        if opera_ref and len(opera_ref) > 3:
            if opera_ref in desc_lower or desc_lower[:30] in opera_ref:
                score += 0.3
                reasons.append(f"Reference match: '{opera_ref.strip()}'")
                ref_matched = True
        # Also check if any significant portion of the Opera reference is in the description
        if not ref_matched and opera_ref:
            # Extract alphanumeric reference parts (e.g. "Y6A8JY7MX" from "CLOUDSIS-Y6A8JY7MX")
            import re as _re
            ref_parts = [p for p in _re.split(r'[\s\-/]+', opera_ref) if len(p) >= 4]
            for part in ref_parts:
                if part in desc_lower:
                    score += 0.3
                    reasons.append(f"Reference part '{part}' found in description")
                    ref_matched = True
                    break
        if not ref_matched and opera_detail and len(opera_detail) > 3:
            opera_words = set(w for w in opera_detail.split() if len(w) > 3)
            desc_words = set(w for w in desc_lower.split() if len(w) > 3)
            common_words = opera_words & desc_words
            if common_words:
                score += 0.1
                reasons.append(f"Common words: {', '.join(list(common_words)[:3])}")

        # Date match
        stmt_date = stmt_txn.date.date() if isinstance(stmt_txn.date, datetime) else stmt_txn.date
        opera_date = opera_entry['ae_date']
        if isinstance(opera_date, datetime):
            opera_date = opera_date.date()
        elif hasattr(opera_date, 'date'):
            opera_date = opera_date.date()

        date_diff = abs((stmt_date - opera_date).days)

        if date_diff == 0:
            score += 0.2
            reasons.append("Date matches exactly")
        elif date_diff <= date_tolerance_days:
            date_score = 0.2 * (1 - date_diff / (date_tolerance_days + 1))
            score += date_score
            reasons.append(f"Date within {date_diff} days")
        elif date_diff <= 14:
            # Extended tolerance — reduced score but still considered
            # (bank transfers, GC payouts can take several days)
            date_score = 0.1 * (1 - (date_diff - date_tolerance_days) / 14)
            score += max(date_score, 0)
            reasons.append(f"Date within {date_diff} days (extended tolerance)")
        else:
            # Date very far off — only match if reference is strong
            if ref_matched:
                reasons.append(f"Date differs by {date_diff} days (reference match overrides)")
            else:
                score *= 0.5
                reasons.append(f"Date differs by {date_diff} days")

        return min(score, 1.0), reasons

    def process_statement_unified(self, bank_acnt: str, pdf_path: str) -> Dict[str, Any]:
        """
        Unified statement processing: identifies transactions to import AND reconcile.

        Args:
            bank_acnt: The Opera bank account code (user-selected)
            pdf_path: Path to the PDF statement

        Returns:
            Dict with:
                - statement_info: Statement metadata
                - bank_validation: Validation of statement against selected bank account
                - to_import: Transactions not in Opera (need importing)
                - to_reconcile: Matches with unreconciled Opera entries
                - already_reconciled: Matches with already reconciled entries (info only)
                - balance_check: Verification of closing balance
        """
        # Extract transactions from PDF
        statement_info, statement_txns = self.extract_transactions_from_pdf(pdf_path)

        # Validate that statement matches the selected bank account
        bank_validation = self.validate_statement_bank(bank_acnt, statement_info)
        if not bank_validation['valid']:
            return {
                'success': False,
                'error': bank_validation['error'],
                'statement_info': statement_info,
                'bank_validation': bank_validation
            }

        # Get ALL Opera entries for the date range
        all_entries = self.get_all_entries(
            bank_acnt,
            date_from=statement_info.period_start,
            date_to=statement_info.period_end
        )

        # Separate into reconciled and unreconciled
        reconciled_entries = [e for e in all_entries if e['is_reconciled']]
        unreconciled_entries = [e for e in all_entries if not e['is_reconciled']]

        # Match statement transactions against ALL Opera entries
        used_statement_indices = set()
        to_reconcile = []  # Matches with unreconciled entries
        already_reconciled = []  # Matches with reconciled entries (info only)

        # First, match against unreconciled entries (these can be reconciled)
        for i, stmt_txn in enumerate(statement_txns):
            if i in used_statement_indices:
                continue

            best_match = None
            best_score = 0
            best_entry_idx = None

            for j, entry in enumerate(unreconciled_entries):
                score, reasons = self._calculate_match_score(stmt_txn, entry, date_tolerance_days=5)
                if score > best_score and score >= 0.7:
                    best_match = entry
                    best_score = score
                    best_entry_idx = j
                    best_reasons = reasons

            if best_match:
                to_reconcile.append({
                    'statement_txn': stmt_txn,
                    'opera_entry': best_match,
                    'match_score': best_score,
                    'match_reasons': best_reasons
                })
                used_statement_indices.add(i)
                unreconciled_entries[best_entry_idx]['_used'] = True

        # Then, match remaining against reconciled entries (for info/verification)
        for i, stmt_txn in enumerate(statement_txns):
            if i in used_statement_indices:
                continue

            best_match = None
            best_score = 0

            for entry in reconciled_entries:
                score, reasons = self._calculate_match_score(stmt_txn, entry, date_tolerance_days=5)
                if score > best_score and score >= 0.7:
                    best_match = entry
                    best_score = score
                    best_reasons = reasons

            if best_match:
                already_reconciled.append({
                    'statement_txn': stmt_txn,
                    'opera_entry': best_match,
                    'match_score': best_score,
                    'match_reasons': best_reasons
                })
                used_statement_indices.add(i)

        # Remaining statement transactions = need to be imported
        to_import = [txn for i, txn in enumerate(statement_txns) if i not in used_statement_indices]

        # Balance verification - get current bank balance from nbank
        current_balance = 0
        reconciled_balance = 0
        try:
            nbank_records = self.reader.query('nbank', filters={'nk_acnt': bank_acnt})
            if nbank_records:
                row = nbank_records[0]
                current_balance = (row.get('nk_curbal', 0) or 0) / 100.0
                reconciled_balance = (row.get('nk_recbal', 0) or 0) / 100.0
        except Exception as e:
            logger.warning(f"Could not read nbank balance: {e}")

        import_total = sum(txn.amount for txn in to_import)

        balance_check = {
            'statement_closing': statement_info.closing_balance,
            'statement_opening': statement_info.opening_balance,
            'opera_current_balance': current_balance,
            'opera_reconciled_balance': reconciled_balance,
            'import_total': import_total,
            'expected_after_import': current_balance + import_total,
            'variance': (statement_info.closing_balance or 0) - (current_balance + import_total) if statement_info.closing_balance else None
        }

        return {
            'success': True,
            'bank_code': bank_acnt,
            'bank_validation': bank_validation,
            'statement_info': statement_info,
            'summary': {
                'total_statement_txns': len(statement_txns),
                'to_import': len(to_import),
                'to_reconcile': len(to_reconcile),
                'already_reconciled': len(already_reconciled),
                'opera_entries_in_period': len(all_entries)
            },
            'to_import': to_import,
            'to_reconcile': to_reconcile,
            'already_reconciled': already_reconciled,
            'balance_check': balance_check
        }
