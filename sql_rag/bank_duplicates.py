"""
Enhanced Duplicate Detection for Bank Statement Import

Provides multiple strategies for detecting duplicate transactions:
1. Fingerprint matching (definitive - using at_refer field)
2. Exact matching (date + amount + account)
3. Fuzzy amount matching (within tolerance)
4. Reference-based matching
5. Cross-period matching (same transaction, different posting date)
6. FIT ID matching (OFX unique transaction IDs)

The fingerprint system uses the at_refer field in atran to store
import markers, preventing re-import of the same transaction.
"""

import hashlib
import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import List, Optional, Dict, Any, Tuple

logger = logging.getLogger(__name__)


@dataclass
class DuplicateCandidate:
    """
    Represents a potential duplicate transaction found in the database.
    """
    table: str  # 'atran', 'ptran', 'stran'
    record_id: str  # Unique record identifier
    match_type: str  # 'fingerprint', 'exact', 'fuzzy_amount', 'reference', 'cross_period', 'fit_id'
    confidence: float  # 0.0 - 1.0 (1.0 = definitive match)
    details: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_definitive(self) -> bool:
        """True if this is a definitive match (fingerprint)"""
        return self.match_type == 'fingerprint' or self.confidence >= 1.0


def generate_import_fingerprint(name: str, amount: float, txn_date: date) -> str:
    """
    Generate unique fingerprint for a bank transaction.

    Format: BKIMP:{hash8}:{YYYYMMDD}
    - BKIMP: Prefix identifying bank import origin
    - hash8: 8-character MD5 hash of: name|amount|date
    - YYYYMMDD: Import date (when imported, not transaction date)

    Example: BKIMP:A7F3B2C1:20260206

    Args:
        name: Transaction name/payee
        amount: Transaction amount
        txn_date: Transaction date

    Returns:
        Fingerprint string
    """
    data = f"{name}|{amount}|{txn_date.isoformat()}"
    hash8 = hashlib.md5(data.encode()).hexdigest()[:8].upper()
    import_date = date.today().strftime('%Y%m%d')
    return f"BKIMP:{hash8}:{import_date}"


def extract_hash_from_fingerprint(fingerprint: str) -> Optional[str]:
    """
    Extract the hash portion from a fingerprint.

    Args:
        fingerprint: Full fingerprint (e.g., "BKIMP:A7F3B2C1:20260206")

    Returns:
        Hash portion (e.g., "A7F3B2C1") or None if invalid
    """
    if not fingerprint or not fingerprint.startswith('BKIMP:'):
        return None

    parts = fingerprint.split(':')
    if len(parts) >= 2:
        return parts[1]
    return None


class EnhancedDuplicateDetector:
    """
    Enhanced duplicate detection with multiple matching strategies.

    Uses fingerprinting as the primary detection method, with
    fallback strategies for transactions imported before fingerprinting
    was implemented.
    """

    def __init__(self, sql_connector):
        """
        Initialize detector with SQL connector.

        Args:
            sql_connector: SQLConnector instance for database queries
        """
        self.sql = sql_connector

    def find_duplicates(
        self,
        name: str,
        amount: float,
        txn_date: date,
        account: Optional[str] = None,
        bank_code: Optional[str] = None,
        fit_id: str = "",
        reference: str = ""
    ) -> List[DuplicateCandidate]:
        """
        Find potential duplicate transactions using all strategies.

        Args:
            name: Transaction name/payee
            amount: Transaction amount (positive or negative)
            txn_date: Transaction date
            account: Optional matched Opera account code
            bank_code: Optional bank account code
            fit_id: Optional FIT ID (OFX unique transaction ID)
            reference: Optional transaction reference

        Returns:
            List of DuplicateCandidate sorted by confidence (highest first)
        """
        candidates = []

        # Strategy 0: Fingerprint match (highest priority - definitive)
        candidates.extend(self._fingerprint_match(name, amount, txn_date))

        # Only continue with other strategies if no fingerprint match
        if not any(c.match_type == 'fingerprint' for c in candidates):
            # Strategy 1: FIT ID match (if available - from OFX)
            if fit_id:
                candidates.extend(self._fit_id_match(fit_id))

            # Strategy 2: Exact match (date + amount + account)
            if account:
                candidates.extend(self._exact_match(amount, txn_date, account, bank_code))

            # Strategy 3: Fuzzy amount (within tolerance)
            if account:
                candidates.extend(self._fuzzy_amount_match(amount, txn_date, account, tolerance=0.05))

            # Strategy 4: Reference-based (ignore date/amount)
            if reference and account:
                candidates.extend(self._reference_match(reference, account))

            # Strategy 5: Cross-period (same transaction, different posting date)
            if account:
                candidates.extend(self._cross_period_match(amount, txn_date, account, days=7))

        # Remove duplicates and sort by confidence
        seen = set()
        unique_candidates = []
        for c in sorted(candidates, key=lambda x: x.confidence, reverse=True):
            key = (c.table, c.record_id)
            if key not in seen:
                seen.add(key)
                unique_candidates.append(c)

        return unique_candidates

    def _fingerprint_match(self, name: str, amount: float, txn_date: date) -> List[DuplicateCandidate]:
        """
        Check for fingerprint in at_refer field.

        This is the definitive match - if fingerprint exists, transaction
        was previously imported.
        """
        candidates = []

        fingerprint = generate_import_fingerprint(name, amount, txn_date)
        hash_part = extract_hash_from_fingerprint(fingerprint)

        if not hash_part:
            return candidates

        # Check atran table
        try:
            query = f"""
                SELECT at_unique, at_date, at_value, at_refer, at_acnt
                FROM atran WITH (NOLOCK)
                WHERE at_refer LIKE 'BKIMP:{hash_part}%'
            """
            df = self.sql.execute_query(query)

            if not df.empty:
                for _, row in df.iterrows():
                    import_date = ""
                    if row.get('at_refer'):
                        parts = row['at_refer'].split(':')
                        if len(parts) >= 3:
                            import_date = parts[2]

                    candidates.append(DuplicateCandidate(
                        table='atran',
                        record_id=str(row.get('at_unique', '')).strip(),
                        match_type='fingerprint',
                        confidence=1.0,  # Definitive match
                        details={
                            'fingerprint': fingerprint,
                            'imported_on': import_date,
                            'at_date': str(row.get('at_date', '')),
                            'at_value': row.get('at_value', 0),
                            'at_acnt': str(row.get('at_acnt', '')).strip()
                        }
                    ))
        except Exception as e:
            logger.warning(f"Error checking atran fingerprint: {e}")

        # Check stran table (st_tref field)
        try:
            query = f"""
                SELECT st_unique, st_trdate, st_trvalue, st_tref, st_account
                FROM stran WITH (NOLOCK)
                WHERE st_tref LIKE 'BKIMP:{hash_part}%'
            """
            df = self.sql.execute_query(query)

            if not df.empty:
                for _, row in df.iterrows():
                    candidates.append(DuplicateCandidate(
                        table='stran',
                        record_id=str(row.get('st_unique', '')).strip(),
                        match_type='fingerprint',
                        confidence=1.0,
                        details={
                            'fingerprint': fingerprint,
                            'st_trdate': str(row.get('st_trdate', '')),
                            'st_trvalue': row.get('st_trvalue', 0),
                            'st_account': str(row.get('st_account', '')).strip()
                        }
                    ))
        except Exception as e:
            logger.warning(f"Error checking stran fingerprint: {e}")

        # Check ptran table (pt_tref field)
        try:
            query = f"""
                SELECT pt_unique, pt_trdate, pt_trvalue, pt_tref, pt_account
                FROM ptran WITH (NOLOCK)
                WHERE pt_tref LIKE 'BKIMP:{hash_part}%'
            """
            df = self.sql.execute_query(query)

            if not df.empty:
                for _, row in df.iterrows():
                    candidates.append(DuplicateCandidate(
                        table='ptran',
                        record_id=str(row.get('pt_unique', '')).strip(),
                        match_type='fingerprint',
                        confidence=1.0,
                        details={
                            'fingerprint': fingerprint,
                            'pt_trdate': str(row.get('pt_trdate', '')),
                            'pt_trvalue': row.get('pt_trvalue', 0),
                            'pt_account': str(row.get('pt_account', '')).strip()
                        }
                    ))
        except Exception as e:
            logger.warning(f"Error checking ptran fingerprint: {e}")

        return candidates

    def _fit_id_match(self, fit_id: str) -> List[DuplicateCandidate]:
        """
        Check for OFX FIT ID match.

        FIT ID is the bank's unique transaction identifier,
        stored in at_refer or similar field.
        """
        candidates = []

        if not fit_id:
            return candidates

        try:
            # Check if FIT ID is stored in at_refer (for non-fingerprint imports)
            query = f"""
                SELECT at_unique, at_date, at_value, at_refer, at_acnt
                FROM atran WITH (NOLOCK)
                WHERE at_refer = '{fit_id.replace("'", "''")}'
                   OR at_refer LIKE '%{fit_id.replace("'", "''")}%'
            """
            df = self.sql.execute_query(query)

            if not df.empty:
                for _, row in df.iterrows():
                    candidates.append(DuplicateCandidate(
                        table='atran',
                        record_id=str(row.get('at_unique', '')).strip(),
                        match_type='fit_id',
                        confidence=0.95,  # High confidence for FIT ID match
                        details={
                            'fit_id': fit_id,
                            'at_refer': row.get('at_refer', ''),
                            'at_date': str(row.get('at_date', '')),
                            'at_value': row.get('at_value', 0)
                        }
                    ))
        except Exception as e:
            logger.warning(f"Error checking FIT ID: {e}")

        return candidates

    def _exact_match(
        self,
        amount: float,
        txn_date: date,
        account: str,
        bank_code: Optional[str] = None
    ) -> List[DuplicateCandidate]:
        """
        Check for exact match on date, amount, and account.
        """
        candidates = []
        date_str = txn_date.strftime('%Y-%m-%d')
        abs_amount = abs(amount)

        # Check cashbook (atran) - amounts in PENCE
        if bank_code:
            try:
                amount_pence = int(abs_amount * 100)
                query = f"""
                    SELECT at_unique, at_date, at_value, at_refer, at_acnt
                    FROM atran WITH (NOLOCK)
                    WHERE at_acnt = '{bank_code}'
                    AND at_pstdate = '{date_str}'
                    AND ABS(ABS(at_value) - {amount_pence}) < 1
                """
                df = self.sql.execute_query(query)

                if not df.empty:
                    for _, row in df.iterrows():
                        candidates.append(DuplicateCandidate(
                            table='atran',
                            record_id=str(row.get('at_unique', '')).strip(),
                            match_type='exact',
                            confidence=0.90,
                            details={
                                'matched_on': 'date+amount+bank',
                                'at_date': str(row.get('at_date', '')),
                                'at_value_pence': row.get('at_value', 0)
                            }
                        ))
            except Exception as e:
                logger.warning(f"Error checking atran exact match: {e}")

        # Determine if we should check stran or ptran based on amount sign
        if amount > 0:
            # Receipt - check stran
            try:
                query = f"""
                    SELECT st_unique, st_trdate, st_trvalue, st_tref, st_account
                    FROM stran WITH (NOLOCK)
                    WHERE RTRIM(st_account) = '{account}'
                    AND st_trdate = '{date_str}'
                    AND ABS(ABS(st_trvalue) - {abs_amount}) < 0.01
                    AND st_trtype = 'R'
                """
                df = self.sql.execute_query(query)

                if not df.empty:
                    for _, row in df.iterrows():
                        candidates.append(DuplicateCandidate(
                            table='stran',
                            record_id=str(row.get('st_unique', '')).strip(),
                            match_type='exact',
                            confidence=0.90,
                            details={
                                'matched_on': 'date+amount+customer',
                                'st_trdate': str(row.get('st_trdate', '')),
                                'st_trvalue': row.get('st_trvalue', 0)
                            }
                        ))
            except Exception as e:
                logger.warning(f"Error checking stran exact match: {e}")
        else:
            # Payment - check ptran
            try:
                query = f"""
                    SELECT pt_unique, pt_trdate, pt_trvalue, pt_tref, pt_account
                    FROM ptran WITH (NOLOCK)
                    WHERE RTRIM(pt_account) = '{account}'
                    AND pt_trdate = '{date_str}'
                    AND ABS(ABS(pt_trvalue) - {abs_amount}) < 0.01
                    AND pt_trtype = 'P'
                """
                df = self.sql.execute_query(query)

                if not df.empty:
                    for _, row in df.iterrows():
                        candidates.append(DuplicateCandidate(
                            table='ptran',
                            record_id=str(row.get('pt_unique', '')).strip(),
                            match_type='exact',
                            confidence=0.90,
                            details={
                                'matched_on': 'date+amount+supplier',
                                'pt_trdate': str(row.get('pt_trdate', '')),
                                'pt_trvalue': row.get('pt_trvalue', 0)
                            }
                        ))
            except Exception as e:
                logger.warning(f"Error checking ptran exact match: {e}")

        return candidates

    def _fuzzy_amount_match(
        self,
        amount: float,
        txn_date: date,
        account: str,
        tolerance: float = 0.05
    ) -> List[DuplicateCandidate]:
        """
        Check for fuzzy amount match (within tolerance).

        Useful for catching transactions where fees were added/removed.
        """
        candidates = []
        date_str = txn_date.strftime('%Y-%m-%d')
        abs_amount = abs(amount)
        tolerance_amount = abs_amount * tolerance

        if amount > 0:
            # Receipt - check stran
            try:
                query = f"""
                    SELECT st_unique, st_trdate, st_trvalue, st_account
                    FROM stran WITH (NOLOCK)
                    WHERE RTRIM(st_account) = '{account}'
                    AND st_trdate = '{date_str}'
                    AND ABS(ABS(st_trvalue) - {abs_amount}) <= {tolerance_amount}
                    AND ABS(ABS(st_trvalue) - {abs_amount}) > 0.01
                    AND st_trtype = 'R'
                """
                df = self.sql.execute_query(query)

                if not df.empty:
                    for _, row in df.iterrows():
                        diff = abs(abs(row.get('st_trvalue', 0)) - abs_amount)
                        diff_pct = diff / abs_amount if abs_amount > 0 else 0
                        confidence = 0.7 - (diff_pct * 2)  # Lower confidence for larger diff

                        candidates.append(DuplicateCandidate(
                            table='stran',
                            record_id=str(row.get('st_unique', '')).strip(),
                            match_type='fuzzy_amount',
                            confidence=max(0.5, confidence),
                            details={
                                'matched_on': 'date+fuzzy_amount+customer',
                                'amount_diff': round(diff, 2),
                                'diff_pct': round(diff_pct * 100, 1),
                                'st_trvalue': row.get('st_trvalue', 0)
                            }
                        ))
            except Exception as e:
                logger.warning(f"Error checking stran fuzzy match: {e}")
        else:
            # Payment - check ptran
            try:
                query = f"""
                    SELECT pt_unique, pt_trdate, pt_trvalue, pt_account
                    FROM ptran WITH (NOLOCK)
                    WHERE RTRIM(pt_account) = '{account}'
                    AND pt_trdate = '{date_str}'
                    AND ABS(ABS(pt_trvalue) - {abs_amount}) <= {tolerance_amount}
                    AND ABS(ABS(pt_trvalue) - {abs_amount}) > 0.01
                    AND pt_trtype = 'P'
                """
                df = self.sql.execute_query(query)

                if not df.empty:
                    for _, row in df.iterrows():
                        diff = abs(abs(row.get('pt_trvalue', 0)) - abs_amount)
                        diff_pct = diff / abs_amount if abs_amount > 0 else 0
                        confidence = 0.7 - (diff_pct * 2)

                        candidates.append(DuplicateCandidate(
                            table='ptran',
                            record_id=str(row.get('pt_unique', '')).strip(),
                            match_type='fuzzy_amount',
                            confidence=max(0.5, confidence),
                            details={
                                'matched_on': 'date+fuzzy_amount+supplier',
                                'amount_diff': round(diff, 2),
                                'diff_pct': round(diff_pct * 100, 1),
                                'pt_trvalue': row.get('pt_trvalue', 0)
                            }
                        ))
            except Exception as e:
                logger.warning(f"Error checking ptran fuzzy match: {e}")

        return candidates

    def _reference_match(self, reference: str, account: str) -> List[DuplicateCandidate]:
        """
        Check for reference-based match (ignoring date/amount).

        Useful for catching transactions with same reference
        but posted on different dates.
        """
        candidates = []

        if not reference or len(reference) < 3:
            return candidates

        # Escape single quotes
        ref_escaped = reference.replace("'", "''")

        # Check stran
        try:
            query = f"""
                SELECT TOP 5 st_unique, st_trdate, st_trvalue, st_ref, st_account
                FROM stran WITH (NOLOCK)
                WHERE RTRIM(st_account) = '{account}'
                AND st_ref LIKE '%{ref_escaped}%'
                ORDER BY st_trdate DESC
            """
            df = self.sql.execute_query(query)

            if not df.empty:
                for _, row in df.iterrows():
                    candidates.append(DuplicateCandidate(
                        table='stran',
                        record_id=str(row.get('st_unique', '')).strip(),
                        match_type='reference',
                        confidence=0.6,  # Medium confidence
                        details={
                            'matched_on': 'reference',
                            'reference': reference,
                            'st_ref': row.get('st_ref', ''),
                            'st_trdate': str(row.get('st_trdate', '')),
                            'st_trvalue': row.get('st_trvalue', 0)
                        }
                    ))
        except Exception as e:
            logger.warning(f"Error checking stran reference match: {e}")

        # Check ptran
        try:
            query = f"""
                SELECT TOP 5 pt_unique, pt_trdate, pt_trvalue, pt_ref, pt_account
                FROM ptran WITH (NOLOCK)
                WHERE RTRIM(pt_account) = '{account}'
                AND pt_ref LIKE '%{ref_escaped}%'
                ORDER BY pt_trdate DESC
            """
            df = self.sql.execute_query(query)

            if not df.empty:
                for _, row in df.iterrows():
                    candidates.append(DuplicateCandidate(
                        table='ptran',
                        record_id=str(row.get('pt_unique', '')).strip(),
                        match_type='reference',
                        confidence=0.6,
                        details={
                            'matched_on': 'reference',
                            'reference': reference,
                            'pt_ref': row.get('pt_ref', ''),
                            'pt_trdate': str(row.get('pt_trdate', '')),
                            'pt_trvalue': row.get('pt_trvalue', 0)
                        }
                    ))
        except Exception as e:
            logger.warning(f"Error checking ptran reference match: {e}")

        return candidates

    def _cross_period_match(
        self,
        amount: float,
        txn_date: date,
        account: str,
        days: int = 7
    ) -> List[DuplicateCandidate]:
        """
        Check for same transaction posted on a different date.

        Useful for catching transactions where the bank date
        differs from the posting date.
        """
        candidates = []
        abs_amount = abs(amount)

        # Date range
        start_date = (txn_date - timedelta(days=days)).strftime('%Y-%m-%d')
        end_date = (txn_date + timedelta(days=days)).strftime('%Y-%m-%d')

        if amount > 0:
            # Receipt - check stran
            try:
                query = f"""
                    SELECT st_unique, st_trdate, st_trvalue, st_account
                    FROM stran WITH (NOLOCK)
                    WHERE RTRIM(st_account) = '{account}'
                    AND st_trdate BETWEEN '{start_date}' AND '{end_date}'
                    AND st_trdate != '{txn_date.strftime('%Y-%m-%d')}'
                    AND ABS(ABS(st_trvalue) - {abs_amount}) < 0.01
                    AND st_trtype = 'R'
                """
                df = self.sql.execute_query(query)

                if not df.empty:
                    for _, row in df.iterrows():
                        posted_date = row.get('st_trdate')
                        if hasattr(posted_date, 'date'):
                            posted_date = posted_date.date()

                        days_diff = abs((posted_date - txn_date).days) if posted_date else days
                        confidence = 0.75 - (days_diff * 0.05)  # Lower confidence for larger date diff

                        candidates.append(DuplicateCandidate(
                            table='stran',
                            record_id=str(row.get('st_unique', '')).strip(),
                            match_type='cross_period',
                            confidence=max(0.5, confidence),
                            details={
                                'matched_on': 'amount+customer+nearby_date',
                                'days_diff': days_diff,
                                'st_trdate': str(posted_date),
                                'txn_date': txn_date.strftime('%Y-%m-%d'),
                                'st_trvalue': row.get('st_trvalue', 0)
                            }
                        ))
            except Exception as e:
                logger.warning(f"Error checking stran cross-period match: {e}")
        else:
            # Payment - check ptran
            try:
                query = f"""
                    SELECT pt_unique, pt_trdate, pt_trvalue, pt_account
                    FROM ptran WITH (NOLOCK)
                    WHERE RTRIM(pt_account) = '{account}'
                    AND pt_trdate BETWEEN '{start_date}' AND '{end_date}'
                    AND pt_trdate != '{txn_date.strftime('%Y-%m-%d')}'
                    AND ABS(ABS(pt_trvalue) - {abs_amount}) < 0.01
                    AND pt_trtype = 'P'
                """
                df = self.sql.execute_query(query)

                if not df.empty:
                    for _, row in df.iterrows():
                        posted_date = row.get('pt_trdate')
                        if hasattr(posted_date, 'date'):
                            posted_date = posted_date.date()

                        days_diff = abs((posted_date - txn_date).days) if posted_date else days
                        confidence = 0.75 - (days_diff * 0.05)

                        candidates.append(DuplicateCandidate(
                            table='ptran',
                            record_id=str(row.get('pt_unique', '')).strip(),
                            match_type='cross_period',
                            confidence=max(0.5, confidence),
                            details={
                                'matched_on': 'amount+supplier+nearby_date',
                                'days_diff': days_diff,
                                'pt_trdate': str(posted_date),
                                'txn_date': txn_date.strftime('%Y-%m-%d'),
                                'pt_trvalue': row.get('pt_trvalue', 0)
                            }
                        ))
            except Exception as e:
                logger.warning(f"Error checking ptran cross-period match: {e}")

        return candidates

    def is_already_imported(
        self,
        name: str,
        amount: float,
        txn_date: date
    ) -> Tuple[bool, str]:
        """
        Quick check if transaction was previously imported (fingerprint check only).

        Args:
            name: Transaction name
            amount: Transaction amount
            txn_date: Transaction date

        Returns:
            Tuple of (is_duplicate, reason)
        """
        candidates = self._fingerprint_match(name, amount, txn_date)

        if candidates:
            c = candidates[0]
            return True, f"Already imported: {c.table}.{c.record_id}"

        return False, ""

    def check_batch(
        self,
        transactions: List[Dict[str, Any]],
        bank_code: Optional[str] = None
    ) -> Dict[int, List[DuplicateCandidate]]:
        """
        Check multiple transactions for duplicates.

        Args:
            transactions: List of dicts with keys: name, amount, date, account (optional)
            bank_code: Optional bank account code

        Returns:
            Dict mapping transaction index to list of duplicate candidates
        """
        results = {}

        for i, txn in enumerate(transactions):
            candidates = self.find_duplicates(
                name=txn.get('name', ''),
                amount=txn.get('amount', 0),
                txn_date=txn.get('date', date.today()),
                account=txn.get('account'),
                bank_code=bank_code,
                fit_id=txn.get('fit_id', ''),
                reference=txn.get('reference', '')
            )

            if candidates:
                results[i] = candidates

        return results
