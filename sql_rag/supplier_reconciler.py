"""
Supplier Reconciliation Engine

Compares two lists of outstanding transactions:
  - Their side: transactions from a supplier's statement (invoices, credits)
  - Our side: outstanding transactions from Opera's ptran table (pt_trbal <> 0)

Matching is by reference only — case-insensitive, stripped of whitespace and
artefacts like *OVERDUE*.  No fuzzy or amount-based matching.

Mathematical guarantee:
    difference == theirs_only_net - ours_only_net + amount_diffs_net

If math_checks_out is False there is a bug in this module.

Opera 3 Parity:
    This module is pure logic — no database access. It works identically for
    both Opera SQL SE and Opera 3. The data provider (which queries ptran/pname)
    is responsible for the database layer. See supplier_config.py and
    background.py for the data access points that need Opera 3 equivalents.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional


# ---------------------------------------------------------------------------
# Input types
# ---------------------------------------------------------------------------

@dataclass
class TheirItem:
    """One line from the supplier's statement."""
    reference: str
    debit: float   # invoice / increases what we owe — always positive
    credit: float  # payment / credit — always positive
    detail: Optional[str] = None   # free-text description, date, etc.

    @property
    def net(self) -> float:
        """Positive = we owe them money."""
        return self.debit - self.credit


@dataclass
class OurItem:
    """One outstanding transaction from Opera ptran (pt_trbal <> 0)."""
    reference: str              # pt_trref
    balance: float              # pt_trbal — positive = invoice, negative = credit
    detail: Optional[str] = None


# ---------------------------------------------------------------------------
# Output types
# ---------------------------------------------------------------------------

@dataclass
class AgreedItem:
    """Reference exists on both sides.  Amounts may differ slightly."""
    reference: str
    their_amount: float          # their net for this reference
    our_amount: float            # our balance for this reference (sum if dupes)
    amount_difference: float     # their_amount - our_amount
    their_detail: Optional[str] = None
    our_details: List[str] = field(default_factory=list)


@dataclass
class UnmatchedItem:
    """Reference appears on one side only."""
    reference: str
    amount: float               # signed: positive = we owe, negative = they owe
    detail: Optional[str] = None


@dataclass
class ReconciliationResult:
    their_balance: float
    our_balance: float
    difference: float

    agreed: List[AgreedItem]
    theirs_only: List[UnmatchedItem]
    ours_only: List[UnmatchedItem]

    # Components of the difference — used to verify the math
    theirs_only_net: float
    ours_only_net: float
    amount_diffs_net: float

    math_checks_out: bool


# ---------------------------------------------------------------------------
# Reference cleaning
# ---------------------------------------------------------------------------

_OVERDUE_RE = re.compile(r'\*OVERDUE\*', re.IGNORECASE)
_STAR_RE = re.compile(r'\*')


def clean_reference(ref: str) -> str:
    """
    Normalise a reference for comparison.

    Strips:
      - *OVERDUE* (case-insensitive)
      - Remaining asterisks
      - Leading/trailing whitespace

    Returns the lower-cased result.
    """
    if not ref:
        return ''
    result = _OVERDUE_RE.sub('', ref)
    result = _STAR_RE.sub('', result)
    return result.strip().lower()


# ---------------------------------------------------------------------------
# Main reconciliation function
# ---------------------------------------------------------------------------

_AMOUNT_TOLERANCE = 0.01   # £0.01


def reconcile(
    their_items: List[TheirItem],
    our_items: List[OurItem],
) -> ReconciliationResult:
    """
    Reconcile a supplier statement against Opera purchase ledger balances.

    Parameters
    ----------
    their_items:
        Transactions from the supplier's statement.
    our_items:
        Outstanding Opera ptran rows (pt_trbal <> 0) for this supplier.

    Returns
    -------
    ReconciliationResult
        Three-way split: agreed, theirs_only, ours_only plus the math check.
    """

    # --- Group their items by clean ref, sum debits/credits -------------
    # Duplicate statement lines with the same reference are merged so that
    # each reference is matched exactly once against Opera.
    their_grouped: Dict[str, List[TheirItem]] = {}
    theirs_only_empty: List[UnmatchedItem] = []

    for item in their_items:
        key = clean_reference(item.reference)
        if key == '':
            # Empty reference — always unmatched
            theirs_only_empty.append(
                UnmatchedItem(
                    reference=item.reference,
                    amount=item.net,
                    detail=item.detail,
                )
            )
            continue
        their_grouped.setdefault(key, []).append(item)

    # --- Group Opera items by clean ref, sum balances -------------------
    # Empty references go straight to ours_only (cannot be matched)
    our_grouped: Dict[str, List[OurItem]] = {}
    ours_only_empty: List[UnmatchedItem] = []

    for item in our_items:
        key = clean_reference(item.reference)
        if key == '':
            ours_only_empty.append(
                UnmatchedItem(
                    reference=item.reference,
                    amount=item.balance,
                    detail=item.detail,
                )
            )
            continue
        our_grouped.setdefault(key, []).append(item)

    # --- Walk their grouped items ----------------------------------------
    agreed: List[AgreedItem] = []
    theirs_only: List[UnmatchedItem] = list(theirs_only_empty)

    # Track which of our refs have been matched
    matched_our_refs: set = set()

    for key, their_group in their_grouped.items():
        # Sum all statement lines for this reference
        their_net = sum(i.net for i in their_group)
        # Use the original reference from the first line
        their_ref = their_group[0].reference
        their_detail = their_group[0].detail if len(their_group) == 1 else None

        if key in our_grouped:
            # Matched — build AgreedItem
            our_group = our_grouped[key]
            our_total = sum(o.balance for o in our_group)
            diff = round(their_net - our_total, 6)

            agreed.append(AgreedItem(
                reference=their_ref,
                their_amount=their_net,
                our_amount=our_total,
                amount_difference=diff,
                their_detail=their_detail,
                our_details=[o.detail for o in our_group if o.detail],
            ))
            matched_our_refs.add(key)
        else:
            # On their statement, not in Opera
            theirs_only.append(
                UnmatchedItem(
                    reference=their_ref,
                    amount=their_net,
                    detail=their_detail,
                )
            )

    # --- Ours only ------------------------------------------------------
    ours_only: List[UnmatchedItem] = list(ours_only_empty)

    for key, group in our_grouped.items():
        if key not in matched_our_refs:
            our_total = sum(o.balance for o in group)
            # Use the reference from the first item in the group
            ours_only.append(
                UnmatchedItem(
                    reference=group[0].reference,
                    amount=our_total,
                    detail=group[0].detail if len(group) == 1 else None,
                )
            )

    # --- Compute balances -----------------------------------------------
    their_balance = sum(i.net for i in their_items)
    our_balance = sum(i.balance for i in our_items)
    difference = round(their_balance - our_balance, 6)

    theirs_only_net = round(sum(i.amount for i in theirs_only), 6)
    ours_only_net = round(sum(i.amount for i in ours_only), 6)
    amount_diffs_net = round(sum(a.amount_difference for a in agreed), 6)

    # --- Math check -------------------------------------------------------
    # difference = theirs_only_net - ours_only_net + amount_diffs_net
    computed = round(theirs_only_net - ours_only_net + amount_diffs_net, 6)
    math_checks_out = abs(computed - difference) <= _AMOUNT_TOLERANCE

    return ReconciliationResult(
        their_balance=their_balance,
        our_balance=our_balance,
        difference=difference,
        agreed=agreed,
        theirs_only=theirs_only,
        ours_only=ours_only,
        theirs_only_net=theirs_only_net,
        ours_only_net=ours_only_net,
        amount_diffs_net=amount_diffs_net,
        math_checks_out=math_checks_out,
    )
