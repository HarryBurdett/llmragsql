"""
Supplier Statement Extraction using Google Gemini Vision.

Extracts transaction data from supplier statements (PDF or text).
Includes cross-reference verification against Opera supplier data,
balance chain validation, and extraction caching.
"""

import base64
import hashlib
import json
import logging
import os
import re
import sqlite3
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any

import google.generativeai as genai

logger = logging.getLogger(__name__)

# Default cache database location (next to other SQLite DBs)
DEFAULT_CACHE_DB = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'supplier_extraction_cache.db')


@dataclass
class SupplierStatementInfo:
    """Information about the supplier statement."""
    supplier_name: str
    account_reference: Optional[str] = None
    statement_date: Optional[str] = None
    period_start: Optional[str] = None
    period_end: Optional[str] = None
    opening_balance: Optional[float] = None
    closing_balance: Optional[float] = None
    currency: str = "GBP"
    contact_email: Optional[str] = None
    contact_phone: Optional[str] = None


@dataclass
class SupplierStatementLine:
    """A single line from a supplier statement."""
    date: str
    reference: Optional[str] = None
    description: Optional[str] = None
    debit: Optional[float] = None  # Invoices (amounts we owe)
    credit: Optional[float] = None  # Payments/credit notes (reducing what we owe)
    balance: Optional[float] = None
    doc_type: Optional[str] = None  # INV, CN, PMT, etc.


class SupplierExtractionCache:
    """
    SQLite-backed cache for supplier statement extraction results.

    Uses MD5 hash of file contents as cache key to avoid re-extracting
    the same document via Gemini.
    """

    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize the extraction cache.

        Args:
            db_path: Path to SQLite database. Defaults to per-company or
                     supplier_extraction_cache.db in the project root.
        """
        self.db_path = db_path or self._resolve_db_path()
        self._init_db()

    @staticmethod
    def _resolve_db_path() -> str:
        """Resolve database path, using per-company path if available."""
        try:
            from sql_rag.company_data import get_current_db_path
            path = get_current_db_path("supplier_extraction_cache.db")
            if path is not None:
                return str(path)
        except ImportError:
            pass
        return DEFAULT_CACHE_DB

    def _init_db(self):
        """Create cache table if it doesn't exist."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS supplier_extraction_cache (
                    file_hash TEXT UNIQUE,
                    extracted_data TEXT NOT NULL,
                    created_at DATETIME NOT NULL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_supplier_cache_created_at
                ON supplier_extraction_cache(created_at)
            """)

    @staticmethod
    def _hash_file(file_path: str) -> str:
        """Compute MD5 hash of file contents."""
        content = Path(file_path).read_bytes()
        return hashlib.md5(content).hexdigest()

    @staticmethod
    def _hash_bytes(data: bytes) -> str:
        """Compute MD5 hash of raw bytes."""
        return hashlib.md5(data).hexdigest()

    def get_cached(self, file_path: str) -> Optional[Tuple[SupplierStatementInfo, List[SupplierStatementLine]]]:
        """
        Look up cached extraction result for a file.

        Args:
            file_path: Path to the file to look up.

        Returns:
            Tuple of (SupplierStatementInfo, list of SupplierStatementLine) or None if not cached.
        """
        try:
            file_hash = self._hash_file(file_path)
            return self._get_by_hash(file_hash)
        except Exception as e:
            logger.warning(f"Supplier extraction cache lookup error for file: {e}")
            return None

    def get_cached_bytes(self, data: bytes) -> Optional[Tuple[SupplierStatementInfo, List[SupplierStatementLine]]]:
        """
        Look up cached extraction result for raw bytes.

        Args:
            data: Raw file bytes to look up.

        Returns:
            Tuple of (SupplierStatementInfo, list of SupplierStatementLine) or None if not cached.
        """
        try:
            file_hash = self._hash_bytes(data)
            return self._get_by_hash(file_hash)
        except Exception as e:
            logger.warning(f"Supplier extraction cache lookup error for bytes: {e}")
            return None

    def _get_by_hash(self, file_hash: str) -> Optional[Tuple[SupplierStatementInfo, List[SupplierStatementLine]]]:
        """
        Look up cached extraction result by hash.

        Args:
            file_hash: MD5 hash of the file contents.

        Returns:
            Tuple of (SupplierStatementInfo, list of SupplierStatementLine) or None if not cached.
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT extracted_data FROM supplier_extraction_cache WHERE file_hash = ?",
                (file_hash,)
            ).fetchone()
            if row:
                data = json.loads(row['extracted_data'])
                info = self._dict_to_info(data.get('statement_info', {}))
                lines = [self._dict_to_line(ld) for ld in data.get('lines', [])]
                logger.info(f"Supplier extraction cache HIT for hash={file_hash[:12]}... ({len(lines)} lines)")
                return info, lines
        return None

    def save_cache(self, file_path: str, extracted_data: Tuple[SupplierStatementInfo, List[SupplierStatementLine]]) -> None:
        """
        Save extraction result to cache, keyed by file contents hash.

        Args:
            file_path: Path to the source file.
            extracted_data: Tuple of (SupplierStatementInfo, list of SupplierStatementLine).
        """
        try:
            file_hash = self._hash_file(file_path)
            self._save_by_hash(file_hash, extracted_data)
        except Exception as e:
            logger.warning(f"Supplier extraction cache save error for file: {e}")

    def save_cache_bytes(self, data: bytes, extracted_data: Tuple[SupplierStatementInfo, List[SupplierStatementLine]]) -> None:
        """
        Save extraction result to cache, keyed by raw bytes hash.

        Args:
            data: Raw file bytes.
            extracted_data: Tuple of (SupplierStatementInfo, list of SupplierStatementLine).
        """
        try:
            file_hash = self._hash_bytes(data)
            self._save_by_hash(file_hash, extracted_data)
        except Exception as e:
            logger.warning(f"Supplier extraction cache save error for bytes: {e}")

    def _save_by_hash(self, file_hash: str, extracted_data: Tuple[SupplierStatementInfo, List[SupplierStatementLine]]) -> None:
        """
        Save extraction result to cache by hash.

        Args:
            file_hash: MD5 hash of the file contents.
            extracted_data: Tuple of (SupplierStatementInfo, list of SupplierStatementLine).
        """
        info, lines = extracted_data
        payload = {
            "statement_info": {
                "supplier_name": info.supplier_name,
                "account_reference": info.account_reference,
                "statement_date": info.statement_date,
                "period_start": info.period_start,
                "period_end": info.period_end,
                "opening_balance": info.opening_balance,
                "closing_balance": info.closing_balance,
                "currency": info.currency,
                "contact_email": info.contact_email,
                "contact_phone": info.contact_phone,
            },
            "lines": [
                {
                    "date": line.date,
                    "reference": line.reference,
                    "description": line.description,
                    "debit": line.debit,
                    "credit": line.credit,
                    "balance": line.balance,
                    "doc_type": line.doc_type,
                }
                for line in lines
            ],
        }
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """INSERT OR REPLACE INTO supplier_extraction_cache
                   (file_hash, extracted_data, created_at)
                   VALUES (?, ?, ?)""",
                (file_hash, json.dumps(payload, default=str), datetime.utcnow().isoformat()),
            )
            logger.info(f"Supplier extraction cache STORE for hash={file_hash[:12]}... ({len(lines)} lines)")

    def clear_cache(self) -> None:
        """Clear all cached extraction results."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM supplier_extraction_cache")
            logger.info("Supplier extraction cache cleared")

    @staticmethod
    def _dict_to_info(d: Dict[str, Any]) -> SupplierStatementInfo:
        """Convert a dictionary to a SupplierStatementInfo instance."""
        return SupplierStatementInfo(
            supplier_name=d.get('supplier_name', 'Unknown Supplier'),
            account_reference=d.get('account_reference'),
            statement_date=d.get('statement_date'),
            period_start=d.get('period_start'),
            period_end=d.get('period_end'),
            opening_balance=d.get('opening_balance'),
            closing_balance=d.get('closing_balance'),
            currency=d.get('currency', 'GBP'),
            contact_email=d.get('contact_email'),
            contact_phone=d.get('contact_phone'),
        )

    @staticmethod
    def _dict_to_line(d: Dict[str, Any]) -> SupplierStatementLine:
        """Convert a dictionary to a SupplierStatementLine instance."""
        return SupplierStatementLine(
            date=d.get('date', ''),
            reference=d.get('reference'),
            description=d.get('description'),
            debit=d.get('debit'),
            credit=d.get('credit'),
            balance=d.get('balance'),
            doc_type=d.get('doc_type'),
        )


class SupplierStatementExtractor:
    """
    Extract supplier statement data using Google Gemini Vision API.

    Handles both PDF attachments and text-based statements.
    Includes cross-reference verification against Opera supplier data,
    balance chain validation, and extraction caching.
    """

    def __init__(self, api_key: str, model: str = "gemini-2.0-flash"):
        """
        Initialize the extractor.

        Args:
            api_key: Google Gemini API key
            model: Gemini model to use (should support vision for PDFs)
        """
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(model)
        self._cache = SupplierExtractionCache()

    def extract_from_pdf(self, pdf_path: str) -> Tuple[SupplierStatementInfo, List[SupplierStatementLine]]:
        """
        Extract statement data from a PDF file using Gemini Vision.

        Checks the extraction cache first. If cached, returns the cached result
        without calling Gemini. Otherwise extracts via Gemini and caches the result.

        Args:
            pdf_path: Path to the PDF file

        Returns:
            Tuple of (SupplierStatementInfo, list of SupplierStatementLine)
        """
        # Check cache first
        cached = self._cache.get_cached(pdf_path)
        if cached is not None:
            return cached

        # Read and encode the PDF
        pdf_bytes = Path(pdf_path).read_bytes()
        result = self._extract_with_vision(pdf_bytes, "application/pdf")

        # Cache the result keyed by file contents
        self._cache.save_cache(pdf_path, result)

        return result

    def extract_from_pdf_bytes(self, pdf_bytes: bytes) -> Tuple[SupplierStatementInfo, List[SupplierStatementLine]]:
        """
        Extract statement data from PDF bytes.

        Checks the extraction cache first. If cached, returns the cached result
        without calling Gemini. Otherwise extracts via Gemini and caches the result.

        Args:
            pdf_bytes: Raw PDF content

        Returns:
            Tuple of (SupplierStatementInfo, list of SupplierStatementLine)
        """
        # Check cache first
        cached = self._cache.get_cached_bytes(pdf_bytes)
        if cached is not None:
            return cached

        result = self._extract_with_vision(pdf_bytes, "application/pdf")

        # Cache the result keyed by bytes hash
        self._cache.save_cache_bytes(pdf_bytes, result)

        return result

    def extract_from_text(self, text: str, sender_email: Optional[str] = None) -> Tuple[SupplierStatementInfo, List[SupplierStatementLine]]:
        """
        Extract statement data from plain text (email body).

        Text-based extractions are not cached because the same text content
        is unlikely to be re-processed, and text extraction is fast.

        Args:
            text: The statement text content
            sender_email: Optional sender email for supplier identification

        Returns:
            Tuple of (SupplierStatementInfo, list of SupplierStatementLine)
        """
        extraction_prompt = self._get_text_extraction_prompt(sender_email)
        full_prompt = f"{extraction_prompt}\n\nStatement text:\n```\n{text}\n```"

        response = self.model.generate_content(full_prompt)
        return self._parse_response(response.text)

    def verify_supplier(self, extracted_info: SupplierStatementInfo, sql_connector) -> Dict[str, Any]:
        """
        Cross-reference the extracted supplier against Opera pname table.

        Tries to match the supplier by account reference first (exact match),
        then falls back to fuzzy name matching using difflib.SequenceMatcher.

        Args:
            extracted_info: SupplierStatementInfo from extraction.
            sql_connector: SQLConnector instance connected to Opera database.

        Returns:
            Dict with keys:
                - matched (bool): Whether a supplier was found.
                - supplier_code (str): The matched pn_account, or empty string.
                - confidence (float): Match confidence 0.0-1.0.
                - method (str): 'exact_account', 'fuzzy_name', or 'not_found'.
        """
        from difflib import SequenceMatcher

        # Strategy 1: Exact match on account reference
        if extracted_info.account_reference:
            account_ref = extracted_info.account_reference.strip()
            if account_ref:
                try:
                    df = sql_connector.execute_query(
                        "SELECT pn_account, RTRIM(pn_name) as pn_name "
                        "FROM pname WITH (NOLOCK) "
                        "WHERE RTRIM(pn_account) = ?",
                        [account_ref]
                    )
                    if len(df) > 0:
                        supplier_code = df.iloc[0]['pn_account'].strip()
                        logger.info(
                            f"Supplier verified by exact account reference: "
                            f"'{account_ref}' -> '{supplier_code}'"
                        )
                        return {
                            "matched": True,
                            "supplier_code": supplier_code,
                            "confidence": 1.0,
                            "method": "exact_account",
                        }
                except Exception as e:
                    logger.warning(f"Error querying pname by account reference: {e}")

        # Strategy 2: Fuzzy name match
        extracted_name = (extracted_info.supplier_name or "").strip().upper()
        if not extracted_name:
            return {
                "matched": False,
                "supplier_code": "",
                "confidence": 0.0,
                "method": "not_found",
            }

        try:
            df = sql_connector.execute_query(
                "SELECT pn_account, RTRIM(pn_name) as pn_name FROM pname WITH (NOLOCK)"
            )
        except Exception as e:
            logger.warning(f"Error querying pname for fuzzy match: {e}")
            return {
                "matched": False,
                "supplier_code": "",
                "confidence": 0.0,
                "method": "not_found",
            }

        best_score = 0.0
        best_account = ""
        for _, row in df.iterrows():
            opera_name = (row['pn_name'] or "").strip().upper()
            if not opera_name:
                continue
            score = SequenceMatcher(None, extracted_name, opera_name).ratio()
            if score > best_score:
                best_score = score
                best_account = row['pn_account'].strip()

        # Threshold: 0.7 is a reasonable cutoff for fuzzy supplier name matching
        if best_score >= 0.7:
            logger.info(
                f"Supplier verified by fuzzy name match: "
                f"'{extracted_info.supplier_name}' -> '{best_account}' (score={best_score:.3f})"
            )
            return {
                "matched": True,
                "supplier_code": best_account,
                "confidence": round(best_score, 3),
                "method": "fuzzy_name",
            }

        logger.info(
            f"Supplier not matched: '{extracted_info.supplier_name}' "
            f"(best score={best_score:.3f}, best account='{best_account}')"
        )
        return {
            "matched": False,
            "supplier_code": "",
            "confidence": round(best_score, 3),
            "method": "not_found",
        }

    def validate_balance_chain(
        self,
        info: SupplierStatementInfo,
        transactions: List[SupplierStatementLine],
    ) -> Dict[str, Any]:
        """
        Validate that the balance chain is internally consistent.

        Walks from the opening balance through each transaction line,
        applying debits (increase balance owed) and credits (decrease balance owed),
        then compares the calculated closing balance to the extracted closing balance.

        Args:
            info: SupplierStatementInfo with opening_balance and closing_balance.
            transactions: List of SupplierStatementLine with debit/credit amounts.

        Returns:
            Dict with keys:
                - valid (bool): True if calculated closing matches extracted closing within tolerance.
                - calculated_closing (float): Balance computed by walking the chain.
                - extracted_closing (float): Closing balance from the extracted statement info.
                - discrepancy (float): Absolute difference between calculated and extracted.
        """
        opening = info.opening_balance if info.opening_balance is not None else 0.0
        extracted_closing = info.closing_balance if info.closing_balance is not None else 0.0

        running = opening
        for txn in transactions:
            debit_amount = txn.debit if txn.debit is not None else 0.0
            credit_amount = txn.credit if txn.credit is not None else 0.0
            # Debits increase the balance owed (invoices), credits decrease it (payments/CNs)
            running = running + debit_amount - credit_amount

        # Round to avoid floating-point precision issues
        calculated_closing = round(running, 2)
        extracted_closing = round(extracted_closing, 2)
        discrepancy = round(abs(calculated_closing - extracted_closing), 2)

        # Tolerance of 0.01 to handle rounding differences
        valid = discrepancy <= 0.01

        if valid:
            logger.info(
                f"Balance chain valid: opening={opening:.2f}, "
                f"calculated_closing={calculated_closing:.2f}, "
                f"extracted_closing={extracted_closing:.2f}"
            )
        else:
            logger.warning(
                f"Balance chain INVALID: opening={opening:.2f}, "
                f"calculated_closing={calculated_closing:.2f}, "
                f"extracted_closing={extracted_closing:.2f}, "
                f"discrepancy={discrepancy:.2f}"
            )

        return {
            "valid": valid,
            "calculated_closing": calculated_closing,
            "extracted_closing": extracted_closing,
            "discrepancy": discrepancy,
        }

    def _extract_with_vision(self, file_bytes: bytes, mime_type: str) -> Tuple[SupplierStatementInfo, List[SupplierStatementLine]]:
        """
        Extract using Gemini Vision API.
        """
        extraction_prompt = self._get_pdf_extraction_prompt()

        # Create the file part for Gemini
        file_part = {
            "mime_type": mime_type,
            "data": file_bytes
        }

        response = self.model.generate_content([file_part, extraction_prompt])
        return self._parse_response(response.text)

    def _get_pdf_extraction_prompt(self) -> str:
        """Get the extraction prompt for PDF statements."""
        return """Analyze this supplier statement and extract ALL line items.

Return a JSON object with this exact structure:
{
    "statement_info": {
        "supplier_name": "Full supplier/company name",
        "account_reference": "Our account number with this supplier",
        "statement_date": "YYYY-MM-DD",
        "period_start": "YYYY-MM-DD or null",
        "period_end": "YYYY-MM-DD or null",
        "opening_balance": 1234.56,
        "closing_balance": 5678.90,
        "currency": "GBP",
        "contact_email": "email if shown",
        "contact_phone": "phone if shown"
    },
    "lines": [
        {
            "date": "YYYY-MM-DD",
            "reference": "Invoice/CN/payment reference",
            "description": "Description or details",
            "debit": 100.00,
            "credit": null,
            "balance": 1234.56,
            "doc_type": "INV"
        }
    ]
}

Important rules:
- Extract EVERY line item, don't skip any
- debit = amounts we owe (invoices) - positive number or null
- credit = amounts reducing what we owe (payments, credit notes) - positive number or null
- doc_type should be: INV (invoice), CN (credit note), PMT (payment), ADJ (adjustment), or null if unclear
- Use YYYY-MM-DD format for all dates
- Include the full reference exactly as shown
- If opening/closing balance not explicit, calculate from the lines
- Return ONLY valid JSON, no other text"""

    def _get_text_extraction_prompt(self, sender_email: Optional[str] = None) -> str:
        """Get the extraction prompt for text-based statements."""
        extra_context = f"\nThe statement was sent from: {sender_email}" if sender_email else ""

        return f"""Analyze this supplier statement text and extract ALL line items.{extra_context}

Return a JSON object with this exact structure:
{{
    "statement_info": {{
        "supplier_name": "Full supplier/company name",
        "account_reference": "Our account number with this supplier",
        "statement_date": "YYYY-MM-DD",
        "period_start": "YYYY-MM-DD or null",
        "period_end": "YYYY-MM-DD or null",
        "opening_balance": 1234.56,
        "closing_balance": 5678.90,
        "currency": "GBP",
        "contact_email": "email if shown",
        "contact_phone": "phone if shown"
    }},
    "lines": [
        {{
            "date": "YYYY-MM-DD",
            "reference": "Invoice/CN/payment reference",
            "description": "Description or details",
            "debit": 100.00,
            "credit": null,
            "balance": 1234.56,
            "doc_type": "INV"
        }}
    ]
}}

Important rules:
- Extract EVERY line item, don't skip any
- debit = amounts we owe (invoices) - positive number or null
- credit = amounts reducing what we owe (payments, credit notes) - positive number or null
- doc_type should be: INV (invoice), CN (credit note), PMT (payment), ADJ (adjustment), or null if unclear
- Use YYYY-MM-DD format for all dates
- Include the full reference exactly as shown
- If balances not explicit, calculate from lines
- Return ONLY valid JSON, no other text"""

    def _parse_response(self, response_text: str) -> Tuple[SupplierStatementInfo, List[SupplierStatementLine]]:
        """
        Parse Gemini's response into structured data.
        """
        # Try to extract JSON from the response
        json_match = re.search(r'\{[\s\S]*\}', response_text)
        if not json_match:
            raise ValueError(f"Could not extract JSON from response: {response_text[:500]}")

        try:
            data = json.loads(json_match.group())
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in response: {e}")

        # Parse statement info
        info_data = data.get('statement_info', {})
        statement_info = SupplierStatementInfo(
            supplier_name=info_data.get('supplier_name', 'Unknown Supplier'),
            account_reference=info_data.get('account_reference'),
            statement_date=info_data.get('statement_date'),
            period_start=info_data.get('period_start'),
            period_end=info_data.get('period_end'),
            opening_balance=info_data.get('opening_balance'),
            closing_balance=info_data.get('closing_balance'),
            currency=info_data.get('currency', 'GBP'),
            contact_email=info_data.get('contact_email'),
            contact_phone=info_data.get('contact_phone')
        )

        # Parse statement lines
        lines = []
        for line_data in data.get('lines', []):
            line = SupplierStatementLine(
                date=line_data.get('date', ''),
                reference=line_data.get('reference'),
                description=line_data.get('description'),
                debit=line_data.get('debit'),
                credit=line_data.get('credit'),
                balance=line_data.get('balance'),
                doc_type=line_data.get('doc_type')
            )
            lines.append(line)

        logger.info(f"Extracted statement from {statement_info.supplier_name}: {len(lines)} lines")
        return statement_info, lines

    def to_dict(self, info: SupplierStatementInfo, lines: List[SupplierStatementLine]) -> Dict[str, Any]:
        """
        Convert extraction results to dictionary for JSON serialization.
        """
        return {
            "statement_info": {
                "supplier_name": info.supplier_name,
                "account_reference": info.account_reference,
                "statement_date": info.statement_date,
                "period_start": info.period_start,
                "period_end": info.period_end,
                "opening_balance": info.opening_balance,
                "closing_balance": info.closing_balance,
                "currency": info.currency,
                "contact_email": info.contact_email,
                "contact_phone": info.contact_phone
            },
            "lines": [
                {
                    "date": line.date,
                    "reference": line.reference,
                    "description": line.description,
                    "debit": line.debit,
                    "credit": line.credit,
                    "balance": line.balance,
                    "doc_type": line.doc_type
                }
                for line in lines
            ],
            "summary": {
                "total_lines": len(lines),
                "total_debits": sum(l.debit or 0 for l in lines),
                "total_credits": sum(l.credit or 0 for l in lines)
            }
        }
