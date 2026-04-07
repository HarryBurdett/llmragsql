"""
Bank Statement Reconciliation Module

Extracts transactions from bank statement PDFs/images using Google Gemini Vision
and matches them against Opera's unreconciled cashbook entries for auto-reconciliation.
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

    def __init__(self, sql_connector, config: Optional[configparser.ConfigParser] = None,
                 config_path: Optional[str] = None, gemini_api_key: Optional[str] = None):
        """
        Initialize the reconciler.

        Args:
            sql_connector: SQLConnector instance for Opera database queries
            config: Optional ConfigParser instance with [gemini] section
            config_path: Optional path to config file (uses default if not provided)
            gemini_api_key: Optional API key override (falls back to config)
        """
        self.sql_connector = sql_connector

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
        # Set generous max_output_tokens to prevent silent truncation on large statements
        self.model = genai.GenerativeModel(
            self.model_name,
            generation_config=genai.types.GenerationConfig(
                max_output_tokens=32768,
            )
        )
        logger.info(f"StatementReconciler initialized with Gemini model: {self.model_name}")

    def find_bank_code_from_statement(self, statement_info: StatementInfo) -> Optional[Dict[str, Any]]:
        """
        Find the Opera bank account code from statement bank details.

        Matches by sort code and account number from the statement against
        the nbank table in Opera.

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
        if sort_code:
            sort_code_clean = sort_code.replace('-', '').replace(' ', '')
        else:
            sort_code_clean = ''

        # Normalize account number (remove spaces)
        if account_number:
            account_number_clean = account_number.replace(' ', '')
        else:
            account_number_clean = ''

        # Query nbank for matching accounts
        query = """
            SELECT
                RTRIM(nk_acnt) as code,
                RTRIM(nk_desc) as description,
                RTRIM(nk_sort) as sort_code,
                RTRIM(nk_number) as account_number
            FROM nbank WITH (NOLOCK)
            WHERE nk_petty = 0
        """
        df = self.sql_connector.execute_query(query)

        if df is None or df.empty:
            logger.warning("No bank accounts found in nbank")
            return None

        # Try to match
        for _, row in df.iterrows():
            opera_sort = (row['sort_code'] or '').replace('-', '').replace(' ', '')
            opera_acct = (row['account_number'] or '').replace(' ', '')

            # Match by both sort code and account number (best match)
            if sort_code_clean and account_number_clean:
                if opera_sort == sort_code_clean and opera_acct == account_number_clean:
                    logger.info(f"Matched bank account by sort code + account number: {row['code']}")
                    return {
                        'bank_code': row['code'],
                        'description': row['description'],
                        'match_type': 'exact',
                        'sort_code': row['sort_code'],
                        'account_number': row['account_number']
                    }

            # Match by account number only (fallback)
            if account_number_clean and opera_acct == account_number_clean:
                logger.info(f"Matched bank account by account number: {row['code']}")
                return {
                    'bank_code': row['code'],
                    'description': row['description'],
                    'match_type': 'account_number',
                    'sort_code': row['sort_code'],
                    'account_number': row['account_number']
                }

            # Match by sort code only (weaker match)
            if sort_code_clean and opera_sort == sort_code_clean:
                logger.info(f"Matched bank account by sort code: {row['code']}")
                return {
                    'bank_code': row['code'],
                    'description': row['description'],
                    'match_type': 'sort_code',
                    'sort_code': row['sort_code'],
                    'account_number': row['account_number']
                }

        logger.warning(f"No matching bank account found for sort code '{sort_code}', account '{account_number}'")
        return None

    def check_reconciliation_in_progress(self, bank_acnt: str) -> Dict[str, Any]:
        """
        Check if there's a reconciliation in progress in Opera that needs to be cleared.

        Checks for:
        - Entries with ae_tmpstat populated (partial reconciliation)
        - nbank with partial reconciliation markers

        Args:
            bank_acnt: The Opera bank account code

        Returns:
            Dict with 'in_progress' (bool) and details if true
        """
        # Check for entries with temporary statement markers
        query = f"""
            SELECT COUNT(*) as partial_count
            FROM aentry WITH (NOLOCK)
            WHERE ae_acnt = '{bank_acnt}'
              AND ae_tmpstat <> 0
              AND ae_tmpstat IS NOT NULL
        """
        df = self.sql_connector.execute_query(query)
        partial_count = int(df.iloc[0]['partial_count']) if df is not None and len(df) > 0 else 0

        if partial_count > 0:
            logger.info(f"Bank {bank_acnt} has {partial_count} entries with ae_tmpstat set (will be cleared on reconciliation)")
            return {
                'in_progress': True,
                'partial_entries': partial_count,
                'message': f"{partial_count} entries have partial reconciliation markers from Opera or a previous session. These will be cleared automatically when you reconcile."
            }

        return {'in_progress': False}

    def validate_statement_bank(self, bank_acnt: str, statement_info: StatementInfo) -> Dict[str, Any]:
        """
        Validate that the statement's bank details match the selected Opera bank account.

        Args:
            bank_acnt: The Opera bank account code selected by user
            statement_info: Extracted statement info with bank details

        Returns:
            Dict with 'valid' (bool), 'opera_bank' info, and 'error' if not valid
        """
        # Get the Opera bank account details
        query = f"""
            SELECT
                RTRIM(nk_acnt) as code,
                RTRIM(nk_desc) as description,
                RTRIM(nk_sort) as sort_code,
                RTRIM(nk_number) as account_number
            FROM nbank WITH (NOLOCK)
            WHERE nk_acnt = '{bank_acnt}'
        """
        df = self.sql_connector.execute_query(query)

        if df is None or df.empty:
            return {
                'valid': False,
                'error': f"Bank account '{bank_acnt}' not found in Opera",
                'opera_bank': None
            }

        opera_bank = df.iloc[0].to_dict()

        # Normalize values for comparison
        opera_sort = (opera_bank.get('sort_code') or '').replace('-', '').replace(' ', '')
        opera_acct = (opera_bank.get('account_number') or '').replace(' ', '')
        stmt_sort = (statement_info.sort_code or '').replace('-', '').replace(' ', '')
        stmt_acct = (statement_info.account_number or '').replace(' ', '')

        # Check if statement matches the selected account
        sort_match = not opera_sort or not stmt_sort or opera_sort == stmt_sort
        acct_match = not opera_acct or not stmt_acct or opera_acct == stmt_acct

        if sort_match and acct_match:
            # Either they match or Opera doesn't have the details stored
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
            error_parts.append(f"sort code (Opera: {opera_bank.get('sort_code')}, Statement: {statement_info.sort_code})")
        if opera_acct and stmt_acct and opera_acct != stmt_acct:
            error_parts.append(f"account number (Opera: {opera_bank.get('account_number')}, Statement: {statement_info.account_number})")

        # Try to find which account the statement actually belongs to
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

    def validate_statement_sequence(self, bank_acnt: str, statement_info: StatementInfo) -> Dict[str, Any]:
        """
        Validate that the statement is the next one in sequence by comparing opening balance
        to Opera's reconciled balance.

        Logic:
        - Opening balance = Reconciled balance → PROCESS (correct next statement)
        - Opening balance < Reconciled balance → SKIP (already processed/earlier statement)
        - Opening balance > Reconciled balance → PENDING (future statement, missing one in between)

        Args:
            bank_acnt: The Opera bank account code
            statement_info: Extracted statement info with opening balance

        Returns:
            Dict with 'status' (process/skip/pending), 'reconciled_balance', 'opening_balance',
            and 'missing_statement_balance' if pending
        """
        # Get current reconciled balance from nbank
        query = f"""
            SELECT nk_recbal / 100.0 as reconciled_balance,
                   nk_curbal / 100.0 as current_balance,
                   nk_lststno as last_statement_number
            FROM nbank WITH (NOLOCK)
            WHERE nk_acnt = '{bank_acnt}'
        """
        df = self.sql_connector.execute_query(query)

        if df is None or df.empty:
            return {
                'status': 'error',
                'error': f"Bank account '{bank_acnt}' not found in Opera"
            }

        reconciled_balance = float(df.iloc[0]['reconciled_balance']) if df.iloc[0]['reconciled_balance'] else 0.0
        current_balance = float(df.iloc[0]['current_balance']) if df.iloc[0]['current_balance'] else 0.0
        last_stmt_no = int(df.iloc[0]['last_statement_number']) if df.iloc[0]['last_statement_number'] else 0

        opening_balance = statement_info.opening_balance

        # If opening balance wasn't extracted, can't validate sequence — proceed anyway
        if opening_balance is None:
            logger.warning(f"Statement opening balance not extracted — skipping sequence validation")
            return {
                'status': 'process',
                'reconciled_balance': reconciled_balance,
                'opening_balance': None,
                'current_balance': current_balance,
                'last_statement_number': last_stmt_no,
                'warning': 'Opening balance not available — sequence not validated'
            }

        # Compare with small tolerance for floating point
        tolerance = 0.01
        balance_diff = abs(opening_balance - reconciled_balance)

        if balance_diff <= tolerance:
            # Opening balance matches reconciled balance - this is the next statement
            logger.info(f"Statement sequence valid: opening £{opening_balance:.2f} = reconciled £{reconciled_balance:.2f}")
            return {
                'status': 'process',
                'reconciled_balance': reconciled_balance,
                'opening_balance': opening_balance,
                'current_balance': current_balance,
                'last_statement_number': last_stmt_no
            }
        elif opening_balance < reconciled_balance - tolerance:
            # Opening balance is less than reconciled - this is an earlier/already processed statement
            logger.info(f"Statement skipped: opening £{opening_balance:.2f} < reconciled £{reconciled_balance:.2f} (already processed)")
            return {
                'status': 'skip',
                'reason': 'already_processed',
                'reconciled_balance': reconciled_balance,
                'opening_balance': opening_balance,
                'message': f"Statement already processed or earlier than current position. Opening balance £{opening_balance:.2f} is less than reconciled balance £{reconciled_balance:.2f}."
            }
        else:
            # Opening balance is greater than reconciled - missing statement in between
            logger.warning(f"Statement pending: opening £{opening_balance:.2f} > reconciled £{reconciled_balance:.2f} (missing statement)")
            return {
                'status': 'pending',
                'reason': 'missing_statement',
                'reconciled_balance': reconciled_balance,
                'opening_balance': opening_balance,
                'missing_statement_balance': reconciled_balance,
                'message': f"Cannot process this statement yet. Missing statement with opening balance £{reconciled_balance:.2f}. Please send the missing statement to continue."
            }

    def get_bank_reconciliation_status(self, bank_acnt: str) -> Dict[str, Any]:
        """
        Get the current reconciliation status for a bank account from Opera.

        Returns information about the last reconciled statement including:
        - Reconciled balance (should be opening balance of next statement)
        - Last statement number
        - Last reconciliation date
        - Current book balance

        Args:
            bank_acnt: The Opera bank account code

        Returns:
            Dict with reconciliation status info
        """
        # Log the database being queried for debugging
        try:
            db_name = self.sql_connector.config['database'].get('database', 'unknown')
        except:
            db_name = 'unknown'
        logger.info(f"get_bank_reconciliation_status: Querying bank '{bank_acnt}' from database '{db_name}'")

        query = f"""
            SELECT
                nk_acnt as bank_code,
                nk_desc as bank_name,
                nk_recbal / 100.0 as reconciled_balance,
                nk_lststno as last_statement_number
            FROM nbank WITH (NOLOCK)
            WHERE nk_acnt = '{bank_acnt}'
        """
        df = self.sql_connector.execute_query(query)

        if df is None or df.empty:
            return {
                'success': False,
                'error': f"Bank account '{bank_acnt}' not found in Opera"
            }

        # Query actual unreconciled entries to derive current balance
        unrec_query = f"""
            SELECT ISNULL(SUM(ae_value), 0) / 100.0 as total
            FROM aentry WITH (NOLOCK)
            WHERE ae_acnt = '{bank_acnt}' AND (ae_reclnum = 0 OR ae_reclnum IS NULL)
        """
        unrec_df = self.sql_connector.execute_query(unrec_query)
        unreconciled_total = float(unrec_df.iloc[0]['total']) if unrec_df is not None and len(unrec_df) > 0 else 0.0

        row = df.iloc[0]
        reconciled_balance = float(row['reconciled_balance']) if row['reconciled_balance'] else 0.0
        # Derive current balance from reconciled + actual unreconciled entries
        # This avoids reliance on nk_curbal which may have historical discrepancies
        current_balance = reconciled_balance + unreconciled_total

        result = {
            'success': True,
            'bank_code': row['bank_code'],
            'bank_name': row['bank_name'],
            'reconciled_balance': reconciled_balance,
            'current_balance': current_balance,
            'last_statement_number': int(row['last_statement_number']) if row['last_statement_number'] else 0
        }
        logger.info(f"get_bank_reconciliation_status: Returning status for '{bank_acnt}': stmt#{result['last_statement_number']}, recbal={result['reconciled_balance']}, curbal={result['current_balance']}")
        return result

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

        prompt = """Extract the statement details from this bank statement PDF.

Bank statements vary in layout — apply this logic:

1. ACCOUNT DETAILS: Find bank name, account number, and sort code wherever they appear (header, sidebar, footer, or any page).

2. DATES: Find the statement date or period dates. May be labelled "Statement period", "From/To", or similar.

3. OPENING BALANCE: Apply in order:
   a) If explicitly labelled ("Balance brought forward", "Opening balance", "Previous balance") — use it
   b) If there is a summary section showing starting balance — use it
   c) If NEITHER of the above, you MUST calculate it from the first transaction:
      - Find the first transaction's running balance, money_in, and money_out
      - opening = running_balance - money_in + money_out
      - Example: if first transaction balance is £5000, money_out is £100, opening = £5000 + £100 = £5100
      - The running balance AFTER a transaction is NOT the opening balance — you must reverse the transaction
   CRITICAL: Do NOT use the first running balance as the opening balance unless it is explicitly labelled as such

4. CLOSING BALANCE: Apply in order:
   a) If labelled ("Balance carried forward", "Closing balance") — use it
   b) Otherwise use the running balance on the very last transaction on the last page

Return ONLY this JSON:
{
    "bank_name": "Bank name",
    "account_number": "Account number",
    "sort_code": "Sort code",
    "statement_date": "YYYY-MM-DD",
    "period_start": "YYYY-MM-DD",
    "period_end": "YYYY-MM-DD",
    "opening_balance": 12345.67,
    "closing_balance": 12345.67
}

IMPORTANT: Return actual values from this document, not examples. Amounts as numbers without currency symbols. Return ONLY valid JSON."""

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
        logger.info(f"extract_transactions_from_pdf: hash={pdf_hash[:16]}, cache_path={cache.db_path}, cached={'HIT' if cached else 'MISS'}")
        if cached:
            info_data, raw_transactions = cached
            logger.info(f"extract_transactions_from_pdf: info_only={info_data.get('_info_only', False)}, raw_txns={len(raw_transactions)}")
            # Skip info-only cache entries (from lightweight scan extraction)
            if info_data.get('_info_only'):
                pass  # Re-extract with full transactions
            elif len(raw_transactions) < 5 and len(pdf_bytes) > 50000:
                # Suspiciously low count for a large PDF — likely a truncated extraction
                logger.warning(f"Cache has only {len(raw_transactions)} transactions for {len(pdf_bytes)} byte PDF — invalidating and re-extracting")
                cache.delete(pdf_hash)
            else:
                return self._parse_extraction_result(info_data, raw_transactions, pdf_path)

        # Use Gemini to extract transactions
        extraction_prompt = """You are extracting data from a bank statement PDF. Process ALL pages.

STEP 1 — IDENTIFY STATEMENT FORMAT:
Scan the entire document first. Bank statements vary widely:
- Some have a summary section with balances at the top
- Some start directly with transactions
- Some have account details in a header, others in a sidebar or footer
- Some label opening/closing balances explicitly, others do not

STEP 2 — EXTRACT ACCOUNT DETAILS:
Find these wherever they appear in the document:
- Bank name (logo, header, or watermark)
- Account number and sort code
- Statement date or period dates

STEP 3 — EXTRACT EVERY TRANSACTION:
Go through every page systematically and extract every transaction row.
Each transaction has: date, description, amount (in or out), and possibly a running balance.
Do NOT stop after the first page. Continue until no more transactions remain.

STEP 4 — DETERMINE OPENING AND CLOSING BALANCES:
Apply this logic in order:
a) If labelled (e.g. "Balance brought forward", "Opening balance") — use the labelled value
b) If the first transaction has a running balance — reverse it: opening = balance - money_in + money_out
c) For closing: if labelled ("Balance carried forward", "Closing balance") — use it. Otherwise use the running balance on the very last transaction.

Return this JSON structure:
{
    "statement_info": {
        "bank_name": "Bank name",
        "account_number": "Account number",
        "sort_code": "Sort code",
        "statement_date": "YYYY-MM-DD",
        "period_start": "YYYY-MM-DD",
        "period_end": "YYYY-MM-DD",
        "opening_balance": 12345.67,
        "closing_balance": 12345.67
    },
    "transactions": [
        {
            "date": "YYYY-MM-DD",
            "description": "Full description text",
            "money_out": null,
            "money_in": null,
            "balance": null,
            "type": "DD|STO|Giro|Card|FP|BGC|Transfer|BACS|CHQ|Other",
            "reference": null
        }
    ]
}

RULES:
- Extract ACTUAL values from this document — never use placeholder values
- money_out = payments/debits leaving the account, money_in = receipts/credits entering
- Amounts as numbers without currency symbols (e.g. 1234.56 not £1,234.56)
- Use the year from the statement period if transaction dates show only day/month
- Include running balance for each transaction if shown on the statement
- Return ONLY valid JSON — no other text
- CRITICAL: Extract EVERY transaction from EVERY page. Do not truncate or summarise."""

        # Create the file part for Gemini
        file_part = {
            "mime_type": "application/pdf",
            "data": pdf_bytes
        }

        # Attempt extraction with truncation detection and retry
        max_attempts = 2
        data = None
        for attempt in range(max_attempts):
            response = self.model.generate_content([file_part, extraction_prompt])

            # Check for truncation via finish_reason
            finish_reason = None
            try:
                if response.candidates:
                    finish_reason = response.candidates[0].finish_reason
                    # finish_reason 1 = STOP (normal), 2 = MAX_TOKENS (truncated)
                    if finish_reason == 2:
                        logger.warning(f"Gemini response truncated (MAX_TOKENS) on attempt {attempt+1}")
            except Exception:
                pass

            # Parse the response
            response_text = response.text
            logger.info(f"Gemini extraction response: {len(response_text)} chars, finish_reason={finish_reason}")

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

            # Check if we got a suspiciously low number of transactions (likely truncated)
            raw_txns = data.get('transactions', [])
            if finish_reason == 2 or (len(raw_txns) < 5 and attempt < max_attempts - 1):
                logger.warning(f"Only {len(raw_txns)} transactions extracted (attempt {attempt+1}) — retrying with explicit instruction")
                extraction_prompt = f"""The previous extraction attempt only returned {len(raw_txns)} transactions.
This bank statement has MANY MORE transactions than that across multiple pages.

CRITICAL: You MUST extract EVERY SINGLE transaction from ALL pages of this PDF.
Go through page by page systematically. Do not stop after the first page.
A typical business bank statement has 20-100+ transactions.

{extraction_prompt}"""
                continue

            break

        if data is None:
            raise ValueError("Failed to extract transactions from PDF after all attempts")

        # Parse the raw JSON data
        info_data = data.get('statement_info', {})
        raw_transactions = data.get('transactions', [])
        logger.info(f"Extracted {len(raw_transactions)} raw transactions from PDF")

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
        # Debug logging for extracted statement info
        logger.info(f"Parsing statement_info: {json.dumps(info_data, indent=2, default=str)}")

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

        # Verify opening balance by calculating from first transaction.
        # Many banks (e.g. Monzo) don't show an explicit opening balance —
        # the AI may incorrectly use the first running balance instead of
        # reversing it. Always recalculate: opening = first_balance - money_in + money_out
        if raw_transactions and len(raw_transactions) > 0:
            first_txn = raw_transactions[0]
            first_bal = _safe_float(first_txn.get('balance'))
            first_in = abs(_safe_float(first_txn.get('money_in')) or 0)
            first_out = abs(_safe_float(first_txn.get('money_out')) or 0)
            if first_bal is not None and (first_in > 0 or first_out > 0):
                calculated_opening = round(first_bal - first_in + first_out, 2)
                if opening_balance is not None and abs(opening_balance - calculated_opening) > 0.01:
                    logger.info(
                        f"Opening balance corrected: AI extracted £{opening_balance:,.2f}, "
                        f"calculated from first transaction £{calculated_opening:,.2f} "
                        f"(first_bal={first_bal}, in={first_in}, out={first_out})"
                    )
                    opening_balance = calculated_opening
                    info_data['opening_balance'] = calculated_opening
                elif opening_balance is None:
                    opening_balance = calculated_opening
                    info_data['opening_balance'] = calculated_opening
                    logger.info(f"Opening balance calculated from first transaction: £{calculated_opening:,.2f}")

        # Validate opening and closing balances using the transaction chain.
        # Walk from opening balance, finding each next transaction where
        # current + (money_in - money_out) = transaction.balance.
        # This determines the correct order AND excludes phantom transactions
        # from other accounts (e.g. Monzo savings on a current account statement).
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
                        mi = abs(_safe_float(t.get('money_in')) or 0)
                        mo = abs(_safe_float(t.get('money_out')) or 0)
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
                    excluded = len(raw_transactions) - len(chain_used)
                    if excluded > 0:
                        logger.info(f"Balance chain: {len(chain_used)}/{len(raw_transactions)} transactions chained, "
                                    f"{excluded} excluded (wrong account?), closing=£{closing_balance:,.2f}")
                    else:
                        logger.info(f"Balance chain: all {len(chain_used)} transactions chained, closing=£{closing_balance:,.2f}")
            except Exception as chain_err:
                logger.warning(f"Balance chain validation failed: {chain_err}")

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

        logger.info(f"Parsed StatementInfo - opening_balance: {statement_info.opening_balance}, closing_balance: {statement_info.closing_balance}, period: {statement_info.period_start} to {statement_info.period_end}")

        # Parse transactions
        transactions = []
        logger.info(f"Parsing {len(raw_transactions)} raw transactions")

        # Log first few transactions for debugging
        if raw_transactions:
            sample_count = min(3, len(raw_transactions))
            logger.info(f"Sample of first {sample_count} raw transactions: {json.dumps(raw_transactions[:sample_count], indent=2, default=str)}")

        skipped_no_date = 0
        skipped_no_amount = 0

        for i, txn_data in enumerate(raw_transactions):
            date = self._parse_date(txn_data.get('date'))
            if not date:
                skipped_no_date += 1
                logger.debug(f"Transaction {i} skipped - no valid date: {txn_data.get('date')}")
                continue

            # Parse money values - Gemini may return them as formatted strings with commas
            def _parse_amount(val):
                if val is None:
                    return None
                if isinstance(val, (int, float)):
                    return float(val)
                s = str(val).replace(',', '').replace('£', '').replace('$', '').strip()
                return float(s) if s else None

            money_out_raw = txn_data.get('money_out')
            money_in_raw = txn_data.get('money_in')
            money_out = _parse_amount(money_out_raw)
            money_in = _parse_amount(money_in_raw)

            # Skip balance-only lines (not real transactions)
            desc_lower = (txn_data.get('description') or '').strip().lower()
            if desc_lower in ('start balance', 'end balance', 'opening balance', 'closing balance',
                              'balance brought forward', 'balance carried forward',
                              'previous balance', 'balance b/f', 'balance c/f'):
                logger.debug(f"Transaction {i} skipped - balance line: {desc_lower}")
                continue

            # Calculate signed amount (positive = in, negative = out)
            # AI may return money_out as positive or negative — use abs()
            if money_out is not None and abs(money_out) > 0:
                amount = -abs(money_out)
            elif money_in is not None and abs(money_in) > 0:
                amount = abs(money_in)
            else:
                skipped_no_amount += 1
                logger.debug(f"Transaction {i} skipped - no amount: money_out={money_out}, money_in={money_in}")
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

        # Keep transactions in PDF order — statement line numbers should match the statement layout

        logger.info(f"Parsed {len(transactions)} transactions from {source_label} (skipped: {skipped_no_date} no date, {skipped_no_amount} no amount)")
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
        Get unreconciled cashbook entries from Opera.

        Args:
            bank_acnt: The bank account code (e.g., 'BC010')
            date_from: Optional start date filter
            date_to: Optional end date filter

        Returns:
            List of unreconciled entry dictionaries
        """
        date_filter = ""
        if date_from:
            date_filter += f" AND ae_lstdate >= '{date_from.strftime('%Y-%m-%d')}'"
        if date_to:
            date_filter += f" AND ae_lstdate <= '{date_to.strftime('%Y-%m-%d')}'"

        query = f"""
            SELECT
                ae_entry,
                ae_lstdate,
                ae_entref,
                ae_cbtype,
                ae_value / 100.0 as value_pounds,
                ae_comment,
                ae_reclnum,
                ae_statln
            FROM aentry WITH (NOLOCK)
            WHERE ae_acnt = '{bank_acnt}'
              AND ae_reclnum = 0
              AND ae_complet = 1
              {date_filter}
            ORDER BY ae_lstdate, ae_entry
        """

        df = self.sql_connector.execute_query(query)
        if df is None or df.empty:
            return []

        entries = []
        for _, row in df.iterrows():
            entries.append({
                'ae_entry': row['ae_entry'],
                'ae_date': row['ae_lstdate'],
                'ae_ref': row['ae_entref'],
                'ae_cbtype': row['ae_cbtype'],
                'value_pounds': float(row['value_pounds']),
                'ae_detail': row['ae_comment'],
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

    def reconcile_matches(self, bank_acnt: str, matches: List[ReconciliationMatch],
                         statement_balance: float, statement_date: datetime) -> Dict[str, Any]:
        """
        Mark matched entries as reconciled in Opera.

        Args:
            bank_acnt: The bank account code
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
            WHERE ae_acnt = '{bank_acnt}'
        """
        batch_result = self.sql_connector.execute_query(batch_query)
        next_batch = int(batch_result.iloc[0]['next_batch']) if batch_result is not None else 1

        # Get the last statement line number
        line_query = f"""
            SELECT ISNULL(nk_lstrecl, 0) as last_line
            FROM nbank WITH (NOLOCK)
            WHERE nk_acnt = '{bank_acnt}'
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
                  AND ae_acnt = '{bank_acnt}'
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
                WHERE nk_acnt = '{bank_acnt}'
            """
            self.sql_connector.execute_non_query(nbank_update)

        return {
            'success': True,
            'message': f'Reconciled {reconciled_count} entries',
            'reconciled_count': reconciled_count,
            'batch_number': next_batch,
            'statement_balance': statement_balance
        }

    def get_all_entries(self, bank_acnt: str, date_from: Optional[datetime] = None,
                        date_to: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Get ALL cashbook entries (both reconciled and unreconciled) from Opera.
        Used to identify which statement transactions need importing vs reconciling.

        Args:
            bank_acnt: The bank account code (e.g., 'BC010')
            date_from: Optional start date filter
            date_to: Optional end date filter

        Returns:
            List of entry dictionaries with reconciliation status
        """
        date_filter = ""
        if date_from:
            date_filter += f" AND ae_lstdate >= '{date_from.strftime('%Y-%m-%d')}'"
        if date_to:
            date_filter += f" AND ae_lstdate <= '{date_to.strftime('%Y-%m-%d')}'"

        query = f"""
            SELECT
                ae_entry,
                ae_lstdate,
                ae_entref,
                ae_cbtype,
                ae_value / 100.0 as value_pounds,
                ae_comment,
                ae_reclnum,
                ae_statln,
                ae_complet
            FROM aentry WITH (NOLOCK)
            WHERE ae_acnt = '{bank_acnt}'
              AND ae_complet = 1
              {date_filter}
            ORDER BY ae_lstdate, ae_entry
        """

        df = self.sql_connector.execute_query(query)
        if df is None or df.empty:
            return []

        entries = []
        for _, row in df.iterrows():
            entries.append({
                'ae_entry': row['ae_entry'],
                'ae_date': row['ae_lstdate'],
                'ae_ref': row['ae_entref'],
                'ae_cbtype': row['ae_cbtype'],
                'value_pounds': float(row['value_pounds']),
                'ae_detail': row['ae_comment'],
                'ae_reclnum': row['ae_reclnum'],
                'ae_statln': row['ae_statln'],
                'is_reconciled': row['ae_reclnum'] > 0
            })

        return entries

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
                # Mark this entry as used
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

        # Balance verification
        opera_total = sum(e['value_pounds'] for e in all_entries)
        import_total = sum(txn.amount for txn in to_import)
        expected_balance = opera_total + import_total

        # Get current bank balance from nbank
        balance_query = f"""
            SELECT nk_curbal / 100.0 as current_balance,
                   nk_recbal / 100.0 as reconciled_balance
            FROM nbank WITH (NOLOCK)
            WHERE nk_acnt = '{bank_acnt}'
        """
        balance_result = self.sql_connector.execute_query(balance_query)
        current_balance = float(balance_result.iloc[0]['current_balance']) if balance_result is not None and len(balance_result) > 0 else 0
        reconciled_balance = float(balance_result.iloc[0]['reconciled_balance']) if balance_result is not None and len(balance_result) > 0 else 0

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
            'bank_validation': bank_validation,  # Validation that statement matches selected bank
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
