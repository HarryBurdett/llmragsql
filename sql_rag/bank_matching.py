"""
Shared Bank Statement Matching Module

Provides fuzzy name matching logic for bank statement imports.
Used by both Opera SQL SE and Opera 3 import modules.

Features:
- Token-based matching (word order independence)
- Abbreviation normalization (LTD -> LIMITED, etc.)
- Word containment scoring (for truncated bank names)
- Prefix matching
- Multi-field matching (primary name, payee, search keys)
"""

import re
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Optional, List, Dict, Tuple, Any


@dataclass
class MatchCandidate:
    """
    Represents a customer or supplier for matching purposes.

    Contains all available fields that can be used for matching:
    - Primary name (pn_name/sn_name)
    - Payee name (pn_payee) - for suppliers only
    - Search keys (pn_key1-4/sn_key1-4)
    - Bank details for potential BACS matching
    """
    account: str
    primary_name: str
    payee_name: Optional[str] = None
    search_keys: List[str] = field(default_factory=list)
    bank_account: Optional[str] = None
    bank_sort: Optional[str] = None
    vendor_ref: Optional[str] = None  # sn_vendor - customer's reference for us

    def get_all_match_names(self) -> List[Tuple[str, str]]:
        """
        Get all names that can be used for matching.

        Returns:
            List of tuples (name, source) where source indicates origin
        """
        names = [(self.primary_name, 'primary')]

        if self.payee_name and self.payee_name.strip():
            names.append((self.payee_name, 'payee'))

        for i, key in enumerate(self.search_keys, 1):
            if key and key.strip():
                names.append((key, f'key{i}'))

        return names


@dataclass
class MatchResult:
    """Result of a matching operation"""
    account: Optional[str] = None
    name: Optional[str] = None
    score: float = 0.0
    source: str = ''  # 'primary', 'payee', 'key1', etc.

    @property
    def is_match(self) -> bool:
        return self.account is not None and self.score > 0


class BankMatcher:
    """
    Fuzzy name matching for bank statement imports.

    Provides matching against customer/supplier master data using multiple
    algorithms for robust matching of bank statement names.

    Usage:
        matcher = BankMatcher(min_score=0.6)
        matcher.load_suppliers({'H031': MatchCandidate(account='H031', primary_name='Harrowden IT')})
        matcher.load_customers({'C001': MatchCandidate(account='C001', primary_name='ABC Ltd')})

        result = matcher.match_supplier('HARROWDEN')
        if result.is_match:
            print(f"Matched: {result.account} - {result.name} ({result.score:.0%})")
    """

    # Common abbreviations for name normalization
    ABBREVIATIONS = {
        'LTD': 'LIMITED',
        'CO': 'COMPANY',
        'CORP': 'CORPORATION',
        'INC': 'INCORPORATED',
        'INTL': 'INTERNATIONAL',
        'INT': 'INTERNATIONAL',
        'MGMT': 'MANAGEMENT',
        'MGT': 'MANAGEMENT',
        'SVCS': 'SERVICES',
        'SVC': 'SERVICE',
        'SERV': 'SERVICES',
        'TECH': 'TECHNOLOGY',
        'TECHS': 'TECHNOLOGIES',
        'ASSOC': 'ASSOCIATES',
        'ASSOCS': 'ASSOCIATES',
        'BROS': 'BROTHERS',
        'MFG': 'MANUFACTURING',
        'DIST': 'DISTRIBUTION',
        'DISTRIB': 'DISTRIBUTION',
        'GOVT': 'GOVERNMENT',
        'NATL': 'NATIONAL',
        'ENGR': 'ENGINEERING',
        'ENG': 'ENGINEERING',
        'ELEC': 'ELECTRICAL',
        'ELECT': 'ELECTRICAL',
        'COMMS': 'COMMUNICATIONS',
        'COMM': 'COMMUNICATIONS',
        'UK': 'UNITED KINGDOM',
        'GRP': 'GROUP',
        'HLDGS': 'HOLDINGS',
        'ACCT': 'ACCOUNT',
        'ACCTS': 'ACCOUNTS',
        'ADMIN': 'ADMINISTRATION',
        'ADV': 'ADVERTISING',
        'ADVTG': 'ADVERTISING',
        'CONS': 'CONSULTING',
        'CONSULT': 'CONSULTING',
    }

    # Stopwords to ignore in token matching
    STOPWORDS = {'THE', 'AND', 'OF', 'FOR', 'A', 'AN', 'IN', 'ON', 'AT', 'TO', 'BY'}

    def __init__(self, min_score: float = 0.6):
        """
        Initialize the matcher.

        Args:
            min_score: Minimum score (0-1) to consider a match valid
        """
        self.min_score = min_score
        self._suppliers: Dict[str, MatchCandidate] = {}
        self._customers: Dict[str, MatchCandidate] = {}

    def load_suppliers(self, suppliers: Dict[str, MatchCandidate]) -> None:
        """
        Load supplier data for matching.

        Args:
            suppliers: Dictionary of account_code -> MatchCandidate
        """
        self._suppliers = suppliers

    def load_customers(self, customers: Dict[str, MatchCandidate]) -> None:
        """
        Load customer data for matching.

        Args:
            customers: Dictionary of account_code -> MatchCandidate
        """
        self._customers = customers

    @property
    def suppliers(self) -> Dict[str, MatchCandidate]:
        """Get loaded suppliers"""
        return self._suppliers

    @property
    def customers(self) -> Dict[str, MatchCandidate]:
        """Get loaded customers"""
        return self._customers

    def normalize_name(self, name: str) -> str:
        """
        Normalize a name for matching.

        - Convert to uppercase
        - Remove punctuation
        - Expand abbreviations
        - Remove extra whitespace

        Args:
            name: Raw name string

        Returns:
            Normalized name
        """
        if not name:
            return ""

        # Uppercase
        normalized = name.upper()

        # Remove common punctuation but keep spaces
        normalized = re.sub(r'[^\w\s]', ' ', normalized)

        # Expand abbreviations
        words = normalized.split()
        expanded_words = []
        for word in words:
            expanded_words.append(self.ABBREVIATIONS.get(word, word))

        # Rejoin and normalize whitespace
        normalized = ' '.join(expanded_words)
        normalized = re.sub(r'\s+', ' ', normalized).strip()

        return normalized

    def _get_significant_tokens(self, name: str) -> set:
        """
        Get significant tokens from a name (excluding stopwords).

        Args:
            name: Normalized name string

        Returns:
            Set of significant tokens
        """
        if not name:
            return set()

        tokens = set(name.upper().split())
        # Remove stopwords and short tokens
        significant = {t for t in tokens if t not in self.STOPWORDS and len(t) > 1}
        return significant

    def _token_match(self, name: str, candidate: str) -> float:
        """
        Token-based matching for word order independence.

        Handles cases like "SYSTEMS CLOUD" matching "CLOUD SYSTEMS".

        Args:
            name: Bank statement name (normalized)
            candidate: Candidate name (normalized)

        Returns:
            Score from 0 to 1
        """
        if not name or not candidate:
            return 0.0

        name_tokens = self._get_significant_tokens(name)
        candidate_tokens = self._get_significant_tokens(candidate)

        if not name_tokens or not candidate_tokens:
            return 0.0

        # Jaccard similarity: intersection / union
        intersection = name_tokens & candidate_tokens
        union = name_tokens | candidate_tokens

        if not union:
            return 0.0

        return len(intersection) / len(union)

    def _word_containment_score(self, name: str, candidate: str) -> float:
        """
        Check if all significant words from bank name appear in candidate.

        Useful for matching truncated names like "HARROWDEN IT" against
        "Harrowden IT (Kintyre) Limited".

        Args:
            name: Bank statement name (normalized)
            candidate: Candidate name (normalized)

        Returns:
            Score from 0 to 1 (1.0 if all words from name are in candidate)
        """
        if not name or not candidate:
            return 0.0

        name_tokens = self._get_significant_tokens(name)
        candidate_tokens = self._get_significant_tokens(candidate)

        if not name_tokens:
            return 0.0

        # How many of the bank name's tokens are in the candidate?
        matches = sum(1 for t in name_tokens if t in candidate_tokens)
        return matches / len(name_tokens)

    def _prefix_match_score(self, name: str, candidate: str) -> float:
        """
        Check for prefix matching (bank names are often truncated).

        Args:
            name: Bank statement name (uppercase)
            candidate: Candidate name (uppercase)

        Returns:
            Score from 0 to 1
        """
        if not name or not candidate:
            return 0.0

        name_upper = name.upper()
        candidate_upper = candidate.upper()

        # Check if one is a prefix of the other
        if candidate_upper.startswith(name_upper) or name_upper.startswith(candidate_upper):
            min_len = min(len(name_upper), len(candidate_upper))
            max_len = max(len(name_upper), len(candidate_upper))
            base_score = min_len / max_len
            # Boost for prefix matches
            return min(1.0, base_score + 0.3)

        return 0.0

    def calculate_match_score(self, bank_name: str, candidate_name: str) -> float:
        """
        Calculate a combined match score using multiple algorithms.

        Combines:
        - SequenceMatcher ratio (character-level similarity)
        - Token match score (word order independence)
        - Word containment score (truncated names)
        - Prefix match boost

        Args:
            bank_name: Name from bank statement
            candidate_name: Name from Opera master file

        Returns:
            Combined score from 0 to 1
        """
        if not bank_name or not candidate_name:
            return 0.0

        # Normalize both names
        norm_bank = self.normalize_name(bank_name)
        norm_candidate = self.normalize_name(candidate_name)

        if not norm_bank or not norm_candidate:
            return 0.0

        # Calculate individual scores
        seq_score = SequenceMatcher(None, norm_bank, norm_candidate).ratio()
        token_score = self._token_match(norm_bank, norm_candidate)
        containment_score = self._word_containment_score(norm_bank, norm_candidate)
        prefix_score = self._prefix_match_score(bank_name, candidate_name)

        # Weighted combination
        # Priority: containment (catches truncated names) > token > sequence
        # Prefix score is added as a bonus
        combined = (
            seq_score * 0.25 +
            token_score * 0.35 +
            containment_score * 0.40
        )

        # Boost with prefix match if significant
        if prefix_score > 0.5:
            combined = max(combined, prefix_score)

        # Perfect containment with high token match is a strong signal
        if containment_score >= 0.9 and token_score >= 0.5:
            combined = max(combined, 0.85)

        return min(1.0, combined)

    def _match_against_candidates(self, name: str, candidates: Dict[str, MatchCandidate]) -> MatchResult:
        """
        Match a name against a set of candidates.

        Args:
            name: Name to match
            candidates: Dict of account -> MatchCandidate

        Returns:
            MatchResult with best match details
        """
        if not name or not candidates:
            return MatchResult()

        best_result = MatchResult()

        for account, candidate in candidates.items():
            # Try all available names for this candidate
            for candidate_name, source in candidate.get_all_match_names():
                if not candidate_name:
                    continue

                score = self.calculate_match_score(name, candidate_name)

                # Slight preference for primary name matches
                if source != 'primary' and score > 0:
                    score = score * 0.95

                if score > best_result.score:
                    best_result = MatchResult(
                        account=account,
                        name=candidate.primary_name,  # Always return primary name
                        score=score,
                        source=source
                    )

        return best_result

    def match_supplier(self, name: str) -> MatchResult:
        """
        Match a name against suppliers.

        Args:
            name: Name from bank statement

        Returns:
            MatchResult with best match (check is_match and score >= min_score)
        """
        result = self._match_against_candidates(name, self._suppliers)
        if result.score < self.min_score:
            return MatchResult(score=result.score)  # Return score but no match
        return result

    def match_customer(self, name: str) -> MatchResult:
        """
        Match a name against customers.

        Args:
            name: Name from bank statement

        Returns:
            MatchResult with best match (check is_match and score >= min_score)
        """
        result = self._match_against_candidates(name, self._customers)
        if result.score < self.min_score:
            return MatchResult(score=result.score)  # Return score but no match
        return result

    def match_both(self, name: str) -> Tuple[MatchResult, MatchResult]:
        """
        Match a name against both customers and suppliers.

        Args:
            name: Name from bank statement

        Returns:
            Tuple of (customer_result, supplier_result)
        """
        return self.match_customer(name), self.match_supplier(name)

    def get_best_matches(self, name: str, candidates: Dict[str, MatchCandidate], top_n: int = 5) -> List[MatchResult]:
        """
        Get top N matches for a name (useful for debugging/review).

        Args:
            name: Name to match
            candidates: Dict of account -> MatchCandidate
            top_n: Number of top matches to return

        Returns:
            List of MatchResult sorted by score descending
        """
        if not name or not candidates:
            return []

        results = []
        for account, candidate in candidates.items():
            for candidate_name, source in candidate.get_all_match_names():
                if not candidate_name:
                    continue

                score = self.calculate_match_score(name, candidate_name)
                if score > 0:
                    results.append(MatchResult(
                        account=account,
                        name=candidate.primary_name,
                        score=score,
                        source=source
                    ))

        # Sort by score descending and return top N
        results.sort(key=lambda r: r.score, reverse=True)
        return results[:top_n]


def create_match_candidate_from_dict(data: Dict[str, Any], is_supplier: bool = True) -> MatchCandidate:
    """
    Helper function to create MatchCandidate from a dictionary.

    Handles both Opera SQL SE query results and Opera 3 DBF records.

    Args:
        data: Dictionary with account data
        is_supplier: True for supplier, False for customer

    Returns:
        MatchCandidate object
    """
    # Determine field prefixes based on type
    if is_supplier:
        account_field = 'pn_account'
        name_field = 'pn_name'
        payee_field = 'pn_payee'
        key_prefix = 'pn_key'
        bank_ac_field = 'pn_bankac'
        bank_sort_field = 'pn_banksor'
        vendor_field = None
    else:
        account_field = 'sn_account'
        name_field = 'sn_name'
        payee_field = None
        key_prefix = 'sn_key'
        bank_ac_field = 'sn_bankac'
        bank_sort_field = 'sn_banksor'
        vendor_field = 'sn_vendor'

    # Extract account and name (handle both uppercase and lowercase field names)
    account = (data.get(account_field) or data.get(account_field.upper()) or
               data.get('account') or '').strip()
    name = (data.get(name_field) or data.get(name_field.upper()) or
            data.get('name') or '').strip()

    # Extract payee name (suppliers only)
    payee_name = None
    if payee_field:
        payee_name = (data.get(payee_field) or data.get(payee_field.upper()) or
                      data.get('payee') or '').strip() or None

    # Extract search keys
    search_keys = []
    for i in range(1, 5):
        key_field = f'{key_prefix}{i}'
        key = (data.get(key_field) or data.get(key_field.upper()) or
               data.get(f'key{i}') or '').strip()
        if key:
            search_keys.append(key)

    # Extract bank details
    bank_account = (data.get(bank_ac_field) or data.get(bank_ac_field.upper()) or
                    data.get('bank_account') or '').strip() or None
    bank_sort = (data.get(bank_sort_field) or data.get(bank_sort_field.upper()) or
                 data.get('bank_sort') or '').strip() or None

    # Extract vendor ref (customers only)
    vendor_ref = None
    if vendor_field:
        vendor_ref = (data.get(vendor_field) or data.get(vendor_field.upper()) or
                      data.get('vendor_ref') or '').strip() or None

    return MatchCandidate(
        account=account,
        primary_name=name,
        payee_name=payee_name,
        search_keys=search_keys,
        bank_account=bank_account,
        bank_sort=bank_sort,
        vendor_ref=vendor_ref
    )
