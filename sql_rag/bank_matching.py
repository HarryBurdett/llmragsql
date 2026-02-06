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
- Phonetic matching (Metaphone) - handles pronunciation variations
- Levenshtein distance - handles typos
- N-gram similarity - handles partial matches and typos
"""

import re
import logging
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Optional, List, Dict, Tuple, Any

logger = logging.getLogger(__name__)

# Try to import optional dependencies for enhanced matching
try:
    from metaphone import doublemetaphone
    METAPHONE_AVAILABLE = True
except ImportError:
    METAPHONE_AVAILABLE = False
    logger.debug("metaphone not available - phonetic matching disabled")

try:
    from Levenshtein import ratio as levenshtein_ratio
    LEVENSHTEIN_AVAILABLE = True
except ImportError:
    LEVENSHTEIN_AVAILABLE = False
    logger.debug("python-Levenshtein not available - using fallback")


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


def _levenshtein_ratio_fallback(s1: str, s2: str) -> float:
    """
    Fallback Levenshtein ratio calculation when python-Levenshtein is not available.

    Uses dynamic programming to calculate edit distance.
    """
    if not s1 or not s2:
        return 0.0 if s1 != s2 else 1.0

    len1, len2 = len(s1), len(s2)

    # Create distance matrix
    d = [[0] * (len2 + 1) for _ in range(len1 + 1)]

    for i in range(len1 + 1):
        d[i][0] = i
    for j in range(len2 + 1):
        d[0][j] = j

    for i in range(1, len1 + 1):
        for j in range(1, len2 + 1):
            cost = 0 if s1[i - 1] == s2[j - 1] else 1
            d[i][j] = min(
                d[i - 1][j] + 1,      # deletion
                d[i][j - 1] + 1,      # insertion
                d[i - 1][j - 1] + cost  # substitution
            )

    # Convert edit distance to ratio (1.0 = identical)
    max_len = max(len1, len2)
    return 1.0 - (d[len1][len2] / max_len) if max_len > 0 else 1.0


class EnhancedBankMatcher(BankMatcher):
    """
    Enhanced fuzzy name matching with additional algorithms.

    Extends BankMatcher with:
    - Phonetic matching (Metaphone) - handles pronunciation variations
    - Levenshtein distance - handles typos
    - N-gram similarity - handles partial matches

    The enhanced algorithms are weighted and combined with the base
    algorithms for improved matching accuracy.

    Usage:
        matcher = EnhancedBankMatcher(min_score=0.6)
        matcher.load_suppliers(suppliers)
        result = matcher.match_supplier('HAROWDEN')  # typo handled
    """

    def __init__(
        self,
        min_score: float = 0.6,
        use_phonetic: bool = True,
        use_levenshtein: bool = True,
        use_ngram: bool = True,
        weights: Optional[Dict[str, float]] = None
    ):
        """
        Initialize enhanced matcher.

        Args:
            min_score: Minimum score to consider a match
            use_phonetic: Enable phonetic matching
            use_levenshtein: Enable Levenshtein distance
            use_ngram: Enable n-gram similarity
            weights: Optional custom weights for algorithms
        """
        super().__init__(min_score)

        self.use_phonetic = use_phonetic and METAPHONE_AVAILABLE
        self.use_levenshtein = use_levenshtein
        self.use_ngram = use_ngram

        # Default weights (must sum to 1.0)
        self.weights = weights or {
            'base': 0.50,       # Original BankMatcher algorithm
            'phonetic': 0.20,   # Metaphone phonetic matching
            'levenshtein': 0.15,  # Levenshtein ratio
            'ngram': 0.15       # N-gram similarity
        }

        # Phonetic cache for performance
        self._phonetic_cache: Dict[str, Tuple[str, str]] = {}

    def _get_phonetic(self, name: str) -> Tuple[str, str]:
        """
        Get Metaphone phonetic encoding for a name.

        Returns tuple of (primary, secondary) encodings.
        """
        if not METAPHONE_AVAILABLE or not name:
            return ('', '')

        name_upper = name.upper()
        if name_upper in self._phonetic_cache:
            return self._phonetic_cache[name_upper]

        codes = doublemetaphone(name_upper)
        self._phonetic_cache[name_upper] = codes
        return codes

    def _phonetic_match(self, name1: str, name2: str) -> float:
        """
        Calculate phonetic similarity using Double Metaphone.

        Returns score from 0 to 1.
        """
        if not METAPHONE_AVAILABLE or not name1 or not name2:
            return 0.0

        # Get phonetic codes for both names
        codes1 = self._get_phonetic(name1)
        codes2 = self._get_phonetic(name2)

        # Compare primary codes first
        if codes1[0] and codes2[0] and codes1[0] == codes2[0]:
            return 1.0

        # Compare secondary codes
        if codes1[1] and codes2[1] and codes1[1] == codes2[1]:
            return 0.9

        # Cross-compare primary with secondary
        if codes1[0] and codes2[1] and codes1[0] == codes2[1]:
            return 0.8
        if codes1[1] and codes2[0] and codes1[1] == codes2[0]:
            return 0.8

        # Partial match on tokens
        tokens1 = name1.upper().split()
        tokens2 = name2.upper().split()

        if not tokens1 or not tokens2:
            return 0.0

        matches = 0
        total = max(len(tokens1), len(tokens2))

        for t1 in tokens1:
            t1_codes = self._get_phonetic(t1)
            for t2 in tokens2:
                t2_codes = self._get_phonetic(t2)
                if t1_codes[0] and t2_codes[0] and t1_codes[0] == t2_codes[0]:
                    matches += 1
                    break

        return matches / total if total > 0 else 0.0

    def _levenshtein_match(self, name1: str, name2: str) -> float:
        """
        Calculate Levenshtein ratio between two names.

        Returns score from 0 to 1 (1 = identical).
        """
        if not name1 or not name2:
            return 0.0

        n1 = name1.lower()
        n2 = name2.lower()

        if LEVENSHTEIN_AVAILABLE:
            return levenshtein_ratio(n1, n2)
        else:
            return _levenshtein_ratio_fallback(n1, n2)

    def _ngram_similarity(self, name1: str, name2: str, n: int = 3) -> float:
        """
        Calculate n-gram similarity between two names.

        N-grams are overlapping substrings of length n.
        Useful for handling typos and partial matches.

        Args:
            name1: First name
            name2: Second name
            n: N-gram size (default 3 for trigrams)

        Returns:
            Jaccard similarity of n-gram sets (0 to 1)
        """
        if not name1 or not name2:
            return 0.0

        def get_ngrams(s: str) -> set:
            s = s.lower().replace(' ', '')
            if len(s) < n:
                return {s} if s else set()
            return {s[i:i+n] for i in range(len(s) - n + 1)}

        ngrams1 = get_ngrams(name1)
        ngrams2 = get_ngrams(name2)

        if not ngrams1 or not ngrams2:
            return 0.0

        intersection = ngrams1 & ngrams2
        union = ngrams1 | ngrams2

        return len(intersection) / len(union) if union else 0.0

    def calculate_match_score(self, bank_name: str, candidate_name: str) -> float:
        """
        Calculate enhanced match score using multiple algorithms.

        Combines base algorithm with phonetic, Levenshtein, and n-gram scores.

        Args:
            bank_name: Name from bank statement
            candidate_name: Name from Opera master file

        Returns:
            Combined score from 0 to 1
        """
        if not bank_name or not candidate_name:
            return 0.0

        # Get base score from parent class
        base_score = super().calculate_match_score(bank_name, candidate_name)

        # If base score is already very high, use it
        if base_score >= 0.95:
            return base_score

        # Calculate additional scores
        scores = {'base': base_score}

        if self.use_phonetic:
            scores['phonetic'] = self._phonetic_match(bank_name, candidate_name)

        if self.use_levenshtein:
            scores['levenshtein'] = self._levenshtein_match(bank_name, candidate_name)

        if self.use_ngram:
            scores['ngram'] = self._ngram_similarity(bank_name, candidate_name, n=3)

        # Weighted combination
        weighted_score = 0.0
        total_weight = 0.0

        for algo, weight in self.weights.items():
            if algo in scores:
                weighted_score += scores[algo] * weight
                total_weight += weight

        # Normalize if not all algorithms contributed
        if total_weight > 0:
            weighted_score /= total_weight
            weighted_score *= sum(self.weights.values())

        # Use the higher of base score and weighted score
        # This ensures we don't make matching worse
        final_score = max(base_score, weighted_score)

        # Boost if multiple algorithms agree on high score
        high_scores = sum(1 for s in scores.values() if s >= 0.7)
        if high_scores >= 3:
            final_score = max(final_score, 0.85)
        elif high_scores >= 2:
            final_score = max(final_score, final_score * 1.1)

        return min(1.0, final_score)

    def get_match_breakdown(self, bank_name: str, candidate_name: str) -> Dict[str, float]:
        """
        Get detailed breakdown of match scores by algorithm.

        Useful for debugging and understanding why matches succeed/fail.

        Args:
            bank_name: Name from bank statement
            candidate_name: Name from Opera master file

        Returns:
            Dict of algorithm -> score
        """
        breakdown = {
            'base': super().calculate_match_score(bank_name, candidate_name),
        }

        if self.use_phonetic:
            breakdown['phonetic'] = self._phonetic_match(bank_name, candidate_name)

        if self.use_levenshtein:
            breakdown['levenshtein'] = self._levenshtein_match(bank_name, candidate_name)

        if self.use_ngram:
            breakdown['ngram'] = self._ngram_similarity(bank_name, candidate_name)

        breakdown['combined'] = self.calculate_match_score(bank_name, candidate_name)

        return breakdown
