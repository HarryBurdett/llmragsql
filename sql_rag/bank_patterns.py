"""
Bank Import Pattern Learning Module

Learns from user import decisions to automatically suggest account assignments
for future imports. Stores patterns in SQLite and uses fuzzy matching to find
similar transactions.

Key features:
- Learns transaction type, account, and VAT settings from user choices
- Normalizes descriptions for better matching
- Tracks usage frequency for confidence scoring
- Supports company-specific patterns
"""

import sqlite3
import re
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

# Default database path (used when no per-company path is provided)
DB_PATH = Path(__file__).parent.parent / "bank_patterns.db"


@dataclass
class PatternMatch:
    """Result of a pattern match lookup"""
    description_pattern: str
    transaction_type: str  # SI, PI, SC, PC, NP, NR, BT
    account_code: str
    account_name: Optional[str]
    ledger_type: str  # C (customer), S (supplier), N (nominal)
    vat_code: Optional[str]
    nominal_code: Optional[str]
    times_used: int
    last_used: str
    confidence: float  # 0.0 to 1.0 based on times_used and recency
    match_type: str  # 'exact', 'normalized', 'fuzzy'


class BankPatternLearner:
    """
    Learns and applies patterns from bank import history.

    Stores successful import decisions and uses them to auto-suggest
    account assignments for new imports.
    """

    def __init__(self, company_code: str, db_path: Optional[Path] = None):
        self.company_code = company_code
        self.db_path = db_path or self._resolve_db_path()
        self._init_db()

    @staticmethod
    def _resolve_db_path() -> Path:
        """Resolve database path, using per-company path if available."""
        try:
            from sql_rag.company_data import get_current_db_path
            path = get_current_db_path("bank_patterns.db")
            if path is not None:
                return path
        except ImportError:
            pass
        return DB_PATH

    def _init_db(self):
        """Initialize the database and create tables if needed"""
        conn = sqlite3.connect(str(self.db_path))
        try:
            cursor = conn.cursor()

            # Main patterns table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bank_import_patterns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_code TEXT NOT NULL,
                    description_raw TEXT NOT NULL,
                    description_normalized TEXT NOT NULL,
                    transaction_type TEXT NOT NULL,
                    account_code TEXT NOT NULL,
                    account_name TEXT,
                    ledger_type TEXT NOT NULL,
                    vat_code TEXT,
                    nominal_code TEXT,
                    net_amount_typical REAL,
                    times_used INTEGER DEFAULT 1,
                    first_used TEXT NOT NULL,
                    last_used TEXT NOT NULL,
                    UNIQUE(company_code, description_normalized)
                )
            """)

            # Index for fast lookups
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_patterns_company_desc
                ON bank_import_patterns(company_code, description_normalized)
            """)

            # Keywords table for pattern-based matching
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bank_import_keywords (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_code TEXT NOT NULL,
                    keyword TEXT NOT NULL,
                    transaction_type TEXT NOT NULL,
                    account_code TEXT NOT NULL,
                    account_name TEXT,
                    ledger_type TEXT NOT NULL,
                    vat_code TEXT,
                    nominal_code TEXT,
                    priority INTEGER DEFAULT 0,
                    UNIQUE(company_code, keyword)
                )
            """)

            conn.commit()
        finally:
            conn.close()

    @staticmethod
    def normalize_description(description: str) -> str:
        """
        Normalize a bank description for matching.

        - Convert to uppercase
        - Remove common prefixes (DD, BACS, FP, etc.)
        - Remove dates and reference numbers
        - Remove extra whitespace
        - Keep core identifying text
        """
        if not description:
            return ""

        # Uppercase
        text = description.upper()

        # Remove common bank prefixes
        prefixes = [
            r'^DD\s+',
            r'^DIRECT DEBIT\s+',
            r'^BACS\s+',
            r'^FASTER PAYMENT\s+',
            r'^FP\s+',
            r'^FPI\s+',
            r'^FPO\s+',
            r'^BGC\s+',
            r'^BANK GIRO CREDIT\s+',
            r'^CHQ\s+',
            r'^CHEQUE\s+',
            r'^TFR\s+',
            r'^TRANSFER\s+',
            r'^S/O\s+',
            r'^STANDING ORDER\s+',
            r'^POS\s+',
            r'^CARD\s+',
            r'^VISA\s+',
            r'^MASTERCARD\s+',
        ]
        for prefix in prefixes:
            text = re.sub(prefix, '', text, flags=re.IGNORECASE)

        # Remove reference numbers (various formats)
        text = re.sub(r'\b[A-Z]{2,3}\d{6,}\b', '', text)  # XX123456
        text = re.sub(r'\b\d{6,}\b', '', text)  # Long numbers
        text = re.sub(r'\bREF[:\s]*\S+', '', text, flags=re.IGNORECASE)

        # Remove dates in various formats
        text = re.sub(r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b', '', text)
        text = re.sub(r'\b\d{1,2}\s+(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s*\d{0,4}\b', '', text, flags=re.IGNORECASE)

        # Remove common suffixes
        text = re.sub(r'\bLTD\.?\b', '', text, flags=re.IGNORECASE)
        text = re.sub(r'\bLIMITED\b', '', text, flags=re.IGNORECASE)
        text = re.sub(r'\bPLC\.?\b', '', text, flags=re.IGNORECASE)
        text = re.sub(r'\b& CO\.?\b', '', text, flags=re.IGNORECASE)

        # Clean up whitespace
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    def learn_pattern(
        self,
        description: str,
        transaction_type: str,
        account_code: str,
        account_name: Optional[str],
        ledger_type: str,
        vat_code: Optional[str] = None,
        nominal_code: Optional[str] = None,
        net_amount: Optional[float] = None
    ) -> bool:
        """
        Learn a pattern from a user's import decision.

        If the pattern already exists, update the usage count and last_used date.
        If new, create a new pattern entry.

        Returns True if successful.
        """
        normalized = self.normalize_description(description)
        if not normalized:
            return False

        now = datetime.now().isoformat()

        conn = sqlite3.connect(str(self.db_path))
        try:
            cursor = conn.cursor()

            # Try to update existing pattern
            cursor.execute("""
                UPDATE bank_import_patterns
                SET transaction_type = ?,
                    account_code = ?,
                    account_name = ?,
                    ledger_type = ?,
                    vat_code = ?,
                    nominal_code = ?,
                    net_amount_typical = COALESCE(?, net_amount_typical),
                    times_used = times_used + 1,
                    last_used = ?
                WHERE company_code = ? AND description_normalized = ?
            """, (
                transaction_type, account_code, account_name, ledger_type,
                vat_code, nominal_code, net_amount, now,
                self.company_code, normalized
            ))

            if cursor.rowcount == 0:
                # Insert new pattern
                cursor.execute("""
                    INSERT INTO bank_import_patterns (
                        company_code, description_raw, description_normalized,
                        transaction_type, account_code, account_name, ledger_type,
                        vat_code, nominal_code, net_amount_typical,
                        times_used, first_used, last_used
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                """, (
                    self.company_code, description, normalized,
                    transaction_type, account_code, account_name, ledger_type,
                    vat_code, nominal_code, net_amount, now, now
                ))

            conn.commit()
            logger.info(f"Learned pattern: '{normalized}' -> {transaction_type}/{account_code}")
            return True

        except Exception as e:
            logger.error(f"Error learning pattern: {e}")
            return False
        finally:
            conn.close()

    def find_pattern(self, description: str) -> Optional[PatternMatch]:
        """
        Find a matching pattern for a transaction description.

        Tries in order:
        1. Exact normalized match
        2. Fuzzy match on normalized description
        3. Keyword match

        Returns PatternMatch if found, None otherwise.
        """
        normalized = self.normalize_description(description)
        if not normalized:
            return None

        conn = sqlite3.connect(str(self.db_path))
        try:
            cursor = conn.cursor()

            # 1. Try exact normalized match
            cursor.execute("""
                SELECT description_normalized, transaction_type, account_code,
                       account_name, ledger_type, vat_code, nominal_code,
                       times_used, last_used
                FROM bank_import_patterns
                WHERE company_code = ? AND description_normalized = ?
            """, (self.company_code, normalized))

            row = cursor.fetchone()
            if row:
                return PatternMatch(
                    description_pattern=row[0],
                    transaction_type=row[1],
                    account_code=row[2],
                    account_name=row[3],
                    ledger_type=row[4],
                    vat_code=row[5],
                    nominal_code=row[6],
                    times_used=row[7],
                    last_used=row[8],
                    confidence=self._calculate_confidence(row[7], row[8]),
                    match_type='exact'
                )

            # 2. Try fuzzy match - look for patterns that contain key words
            words = normalized.split()
            if words:
                # Try matching on first significant word (usually company name)
                main_word = words[0]
                if len(main_word) >= 3:
                    cursor.execute("""
                        SELECT description_normalized, transaction_type, account_code,
                               account_name, ledger_type, vat_code, nominal_code,
                               times_used, last_used
                        FROM bank_import_patterns
                        WHERE company_code = ? AND description_normalized LIKE ?
                        ORDER BY times_used DESC
                        LIMIT 1
                    """, (self.company_code, f"%{main_word}%"))

                    row = cursor.fetchone()
                    if row:
                        return PatternMatch(
                            description_pattern=row[0],
                            transaction_type=row[1],
                            account_code=row[2],
                            account_name=row[3],
                            ledger_type=row[4],
                            vat_code=row[5],
                            nominal_code=row[6],
                            times_used=row[7],
                            last_used=row[8],
                            confidence=self._calculate_confidence(row[7], row[8]) * 0.8,  # Lower confidence for fuzzy
                            match_type='fuzzy'
                        )

            # 3. Try keyword match
            cursor.execute("""
                SELECT keyword, transaction_type, account_code, account_name,
                       ledger_type, vat_code, nominal_code, priority
                FROM bank_import_keywords
                WHERE company_code = ? AND ? LIKE '%' || keyword || '%'
                ORDER BY priority DESC, LENGTH(keyword) DESC
                LIMIT 1
            """, (self.company_code, normalized))

            row = cursor.fetchone()
            if row:
                return PatternMatch(
                    description_pattern=row[0],
                    transaction_type=row[1],
                    account_code=row[2],
                    account_name=row[3],
                    ledger_type=row[4],
                    vat_code=row[5],
                    nominal_code=row[6],
                    times_used=1,
                    last_used='',
                    confidence=0.6,  # Keyword matches have moderate confidence
                    match_type='keyword'
                )

            return None

        finally:
            conn.close()

    def find_patterns_bulk(self, descriptions: List[str]) -> Dict[str, Optional[PatternMatch]]:
        """
        Find patterns for multiple descriptions efficiently.

        Returns a dict mapping description -> PatternMatch (or None)
        """
        results = {}
        for desc in descriptions:
            results[desc] = self.find_pattern(desc)
        return results

    def _calculate_confidence(self, times_used: int, last_used: str) -> float:
        """
        Calculate confidence score based on usage frequency and recency.

        - More uses = higher confidence (up to 0.9)
        - Recent use = small boost
        - Base confidence starts at 0.5 for first use
        """
        # Base confidence from usage count (diminishing returns)
        if times_used <= 1:
            base = 0.5
        elif times_used <= 3:
            base = 0.7
        elif times_used <= 10:
            base = 0.8
        else:
            base = 0.9

        # Recency boost (up to 0.1)
        try:
            last = datetime.fromisoformat(last_used)
            days_ago = (datetime.now() - last).days
            if days_ago <= 7:
                recency_boost = 0.1
            elif days_ago <= 30:
                recency_boost = 0.05
            else:
                recency_boost = 0
        except:
            recency_boost = 0

        return min(1.0, base + recency_boost)

    def add_keyword(
        self,
        keyword: str,
        transaction_type: str,
        account_code: str,
        account_name: Optional[str],
        ledger_type: str,
        vat_code: Optional[str] = None,
        nominal_code: Optional[str] = None,
        priority: int = 0
    ) -> bool:
        """
        Add a keyword-based matching rule.

        Keywords are checked against normalized descriptions.
        Higher priority keywords are matched first.
        """
        keyword = keyword.upper().strip()
        if not keyword:
            return False

        conn = sqlite3.connect(str(self.db_path))
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO bank_import_keywords (
                    company_code, keyword, transaction_type, account_code,
                    account_name, ledger_type, vat_code, nominal_code, priority
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                self.company_code, keyword, transaction_type, account_code,
                account_name, ledger_type, vat_code, nominal_code, priority
            ))
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding keyword: {e}")
            return False
        finally:
            conn.close()

    def get_all_patterns(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get all patterns for this company, ordered by usage"""
        conn = sqlite3.connect(str(self.db_path))
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT description_normalized, transaction_type, account_code,
                       account_name, ledger_type, vat_code, nominal_code,
                       times_used, last_used
                FROM bank_import_patterns
                WHERE company_code = ?
                ORDER BY times_used DESC, last_used DESC
                LIMIT ?
            """, (self.company_code, limit))

            return [
                {
                    'description': row[0],
                    'transaction_type': row[1],
                    'account_code': row[2],
                    'account_name': row[3],
                    'ledger_type': row[4],
                    'vat_code': row[5],
                    'nominal_code': row[6],
                    'times_used': row[7],
                    'last_used': row[8]
                }
                for row in cursor.fetchall()
            ]
        finally:
            conn.close()

    def delete_pattern(self, description_normalized: str) -> bool:
        """Delete a specific pattern"""
        conn = sqlite3.connect(str(self.db_path))
        try:
            cursor = conn.cursor()
            cursor.execute("""
                DELETE FROM bank_import_patterns
                WHERE company_code = ? AND description_normalized = ?
            """, (self.company_code, description_normalized))
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()

    def clear_all_patterns(self) -> int:
        """Clear all patterns for this company. Returns count deleted."""
        conn = sqlite3.connect(str(self.db_path))
        try:
            cursor = conn.cursor()
            cursor.execute("""
                DELETE FROM bank_import_patterns WHERE company_code = ?
            """, (self.company_code,))
            count = cursor.rowcount
            conn.commit()
            return count
        finally:
            conn.close()


def apply_patterns_to_transactions(
    transactions: List[Dict[str, Any]],
    company_code: str
) -> List[Dict[str, Any]]:
    """
    Apply learned patterns to a list of transactions.

    Adds suggestion fields to each transaction:
    - suggested_type: Transaction type suggestion
    - suggested_account: Account code suggestion
    - suggested_account_name: Account name
    - suggested_ledger_type: C/S/N
    - suggested_vat_code: VAT code (for nominals)
    - suggested_nominal_code: Nominal code (for nominals)
    - suggestion_confidence: 0.0 to 1.0
    - suggestion_source: 'pattern', 'keyword', or None

    Returns the modified transactions list.
    """
    learner = BankPatternLearner(company_code)

    for txn in transactions:
        description = txn.get('memo') or txn.get('name') or txn.get('description', '')
        pattern = learner.find_pattern(description)

        if pattern:
            txn['suggested_type'] = pattern.transaction_type
            txn['suggested_account'] = pattern.account_code
            txn['suggested_account_name'] = pattern.account_name
            txn['suggested_ledger_type'] = pattern.ledger_type
            txn['suggested_vat_code'] = pattern.vat_code
            txn['suggested_nominal_code'] = pattern.nominal_code
            txn['suggestion_confidence'] = pattern.confidence
            txn['suggestion_source'] = pattern.match_type
        else:
            txn['suggested_type'] = None
            txn['suggested_account'] = None
            txn['suggested_account_name'] = None
            txn['suggested_ledger_type'] = None
            txn['suggested_vat_code'] = None
            txn['suggested_nominal_code'] = None
            txn['suggestion_confidence'] = 0.0
            txn['suggestion_source'] = None

    return transactions


def learn_from_import(
    transactions: List[Dict[str, Any]],
    company_code: str
) -> int:
    """
    Learn patterns from a completed import.

    Call this after a successful import to save user decisions.

    Each transaction should have:
    - memo/name/description: The bank description
    - transaction_type: SI, PI, SC, PC, NP, NR, BT
    - account_code: The selected account
    - account_name: Account name (optional)
    - ledger_type: C, S, or N
    - vat_code: VAT code (for nominals)
    - nominal_code: Nominal code (for nominals)
    - net_amount: Net amount (for nominals)

    Returns the number of patterns learned.
    """
    learner = BankPatternLearner(company_code)
    learned = 0

    for txn in transactions:
        description = txn.get('memo') or txn.get('name') or txn.get('description', '')
        if not description:
            continue

        transaction_type = txn.get('transaction_type')
        account_code = txn.get('account_code') or txn.get('manual_account')
        ledger_type = txn.get('ledger_type') or txn.get('manual_ledger_type')

        if not transaction_type or not account_code or not ledger_type:
            continue

        success = learner.learn_pattern(
            description=description,
            transaction_type=transaction_type,
            account_code=account_code,
            account_name=txn.get('account_name') or txn.get('matched_name'),
            ledger_type=ledger_type,
            vat_code=txn.get('vat_code'),
            nominal_code=txn.get('nominal_code'),
            net_amount=txn.get('net_amount')
        )

        if success:
            learned += 1

    logger.info(f"Learned {learned} patterns from import")
    return learned
