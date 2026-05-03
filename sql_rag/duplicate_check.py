"""Single source of truth for bank-import duplicate detection.

Replaces six scattered implementations across analyse-time and
post-time, on Opera SE and Opera 3. See
docs/superpowers/specs/2026-05-03-duplicate-check-consolidation-design.md.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

__all__ = [
    "DuplicateMatchKind",
    "DuplicateCheckResult",
    "DataSource",
    "ACTION_TYPE_MAP",
    "check_for_duplicate",
]


class DuplicateMatchKind(Enum):
    """What the duplicate check found.

    Caller contract:
      NONE: safe to post — no matching entry in cashbook OR ledger.
      CASHBOOK_DUPLICATE: an aentry of the matching at_type and signed
        amount exists. Do NOT post — the transaction is already in
        Opera's cashbook.
      LEDGER_ALLOCATION_TARGET: no cashbook entry, but a credit-note-
        type stran/ptran row of the matching refund type exists. The
        bank line is NOT a duplicate — it's the missing payment for
        an existing credit note. POST it; optionally auto-allocate.
    """
    NONE = "none"
    CASHBOOK_DUPLICATE = "cashbook_duplicate"
    LEDGER_ALLOCATION_TARGET = "ledger_allocation_target"


@dataclass(frozen=True)
class DuplicateCheckResult:
    """Result of check_for_duplicate.

    Invariants:
      - kind=NONE: matched_table and matched_entry are None.
      - kind=CASHBOOK_DUPLICATE: matched_table='aentry', matched_entry
        is the ae_entry string.
      - kind=LEDGER_ALLOCATION_TARGET: matched_table is 'stran' or
        'ptran', matched_entry is the unique key (st_trref/pt_trref).
      - reason is always a non-empty human-readable string.
    """
    kind: DuplicateMatchKind
    matched_table: Optional[str]
    matched_entry: Optional[str]
    reason: str

    @property
    def is_duplicate(self) -> bool:
        """Convenience for callers that only care about the cashbook
        question (the existing _is_already_posted contract).
        """
        return self.kind is DuplicateMatchKind.CASHBOOK_DUPLICATE


# Mapping: bank-import action → Opera posting types
# ----------------------------------------------------------------
# Source: apps/core/docs/opera_knowledge_base.md "Cashbook Transaction
# Types (at_type)" plus the central KB business-rules. Locking this
# here as a single mapping prevents the per-callsite drift that
# bit us multiple times in production.
ACTION_TYPE_MAP: Dict[str, Dict[str, Any]] = {
    'sales_receipt':    {'at_type': 4, 'st_trtype': 'R'},
    'sales_refund':     {'at_type': 3, 'st_trtype': 'F'},
    'purchase_payment': {'at_type': 5, 'pt_trtype': 'P'},
    'purchase_refund':  {'at_type': 6, 'pt_trtype': 'F'},
    'nominal_payment':  {'at_type': 1},
    'nominal_receipt':  {'at_type': 2},
    'bank_transfer':    {'at_type': 8},
}


@runtime_checkable
class DataSource(Protocol):
    """Protocol for the data lookups the function needs."""

    def find_aentry_by_signed_value(
        self,
        bank_code: str,
        date_from: date,
        date_to: date,
        signed_pence: int,
        expected_at_type: int,
        exclude_entry_numbers: Optional[List[str]],
    ) -> List[Dict[str, Any]]:
        """Return aentry rows for this bank in the date window where
        ae_value matches signed_pence (within ±1p) AND at_type =
        expected_at_type. Excludes any ae_entry in exclude_entry_numbers.
        Each row is a dict with at minimum 'ae_entry'.
        """
        ...

    def find_stran_by_signed_value(
        self,
        account_code: str,
        date_from: date,
        date_to: date,
        signed_pounds: float,
        st_trtype: str,
    ) -> List[Dict[str, Any]]:
        """Return stran rows for this customer in the date window where
        st_trvalue matches signed_pounds (within £0.01) AND st_trtype =
        the given character.
        """
        ...

    def find_ptran_by_signed_value(
        self,
        account_code: str,
        date_from: date,
        date_to: date,
        signed_pounds: float,
        pt_trtype: str,
    ) -> List[Dict[str, Any]]:
        """Return ptran rows for this supplier in the date window where
        pt_trvalue matches signed_pounds (within £0.01) AND pt_trtype =
        the given character.
        """
        ...


from datetime import timedelta


def _signed_pence(signed_pounds: float) -> int:
    return int(round(signed_pounds * 100))


def check_for_duplicate(
    *,
    data_source: DataSource,
    bank_code: str,
    transaction_date: date,
    signed_amount_pounds: float,
    action: str,
    account_code: Optional[str],
    description: str,
    reference: str,
    date_tolerance_days: int = 14,
    exclude_entry_numbers: Optional[List[str]] = None,
) -> DuplicateCheckResult:
    """Single source of truth for the bank-import duplicate decision.

    Order:
      1. Cashbook (atran/aentry) check, type-aware AND sign-aware. The
         cashbook is authoritative — if the bank entry is already there
         with the correct at_type and signed amount, it's a duplicate.
      2. Ledger advisory check (refunds only): if no cashbook duplicate,
         look for a matching credit-note-type stran/ptran row. If found,
         return LEDGER_ALLOCATION_TARGET — the caller should POST and
         optionally auto-allocate.
      3. Otherwise: NONE.

    Sign-aware throughout: a +£X receipt is never a duplicate of a -£X
    refund. Type-aware: the cashbook check filters by the at_type for
    the action, so matching magnitudes of different type don't collide.

    Raises ValueError if action is not in ACTION_TYPE_MAP.
    """
    if action not in ACTION_TYPE_MAP:
        raise ValueError(
            f"action {action!r} is not in ACTION_TYPE_MAP — "
            f"add it explicitly, do not let unknown actions silently match."
        )

    type_map = ACTION_TYPE_MAP[action]
    expected_at_type = type_map['at_type']

    date_from = transaction_date - timedelta(days=date_tolerance_days)
    date_to = transaction_date + timedelta(days=date_tolerance_days)
    signed_pence = _signed_pence(signed_amount_pounds)

    # 1. Cashbook check — authoritative
    aentry_rows = data_source.find_aentry_by_signed_value(
        bank_code,
        date_from,
        date_to,
        signed_pence,
        expected_at_type,
        list(exclude_entry_numbers or []),
    )
    if aentry_rows:
        row = aentry_rows[0]
        return DuplicateCheckResult(
            kind=DuplicateMatchKind.CASHBOOK_DUPLICATE,
            matched_table='aentry',
            matched_entry=str(row.get('ae_entry', '')).strip() or None,
            reason=(
                f"cashbook entry {row.get('ae_entry')} already posted "
                f"(at_type={expected_at_type}, ae_value~={signed_pence}p) "
                f"on {bank_code} in window {date_from}..{date_to}"
            ),
        )

    # 2. Ledger advisory (refund actions only) — task 3 implements
    # 3. NONE
    return DuplicateCheckResult(
        kind=DuplicateMatchKind.NONE,
        matched_table=None,
        matched_entry=None,
        reason=(
            f"no cashbook match for {action} on {bank_code} "
            f"({signed_amount_pounds:+.2f}, {transaction_date.isoformat()})"
        ),
    )
