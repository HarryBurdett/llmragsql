# tests/test_duplicate_check_regression.py
"""Regression tests pinning each historical bug we fixed during the
2026-05-03 audit. These run against a fixture DataSource — no live
Opera dependency.

Each test reproduces a specific class of bug that was bitten in
production. If any of these fail in the future, that regression will
be caught at PR time.
"""
from __future__ import annotations

from datetime import date

from sql_rag.duplicate_check import (
    check_for_duplicate,
    DuplicateMatchKind,
)


class _FixtureDS:
    def __init__(self, aentry=None, stran=None, ptran=None):
        self._aentry = aentry or []
        self._stran = stran or []
        self._ptran = ptran or []

    def find_aentry_by_signed_value(self, bank_code, date_from, date_to,
                                     signed_pence, expected_at_type,
                                     exclude_entry_numbers):
        excluded = set(exclude_entry_numbers or [])
        return [
            r for r in self._aentry
            if r.get('at_type') == expected_at_type
            and abs(r.get('ae_value', 0) - signed_pence) < 1
            and r.get('ae_entry') not in excluded
        ]

    def find_stran_by_signed_value(self, account_code, date_from, date_to,
                                    signed_pounds, st_trtype):
        return [
            r for r in self._stran
            if r.get('st_trtype') == st_trtype
            and abs(r.get('st_trvalue', 0) - signed_pounds) < 0.01
        ]

    def find_ptran_by_signed_value(self, account_code, date_from, date_to,
                                    signed_pounds, pt_trtype):
        return [
            r for r in self._ptran
            if r.get('pt_trtype') == pt_trtype
            and abs(r.get('pt_trvalue', 0) - signed_pounds) < 0.01
        ]


def test_regression_cloudsis_p051_refund_vs_misposted_receipt():
    """Cloudsis BB005, 2026-04-16:
    - existing aentry R100000407 has at_type=4 (sales_receipt) value=+£198 — misposted
    - bank line is -£198 sales_refund
    Sign-blind ABS-on-ABS matched these falsely. The fix: signed +
    type-aware. Refund must NOT match the misposted receipt.
    """
    ds = _FixtureDS(
        aentry=[
            {'ae_entry': 'R100000407', 'ae_value': 19800, 'at_type': 4},
        ],
    )
    result = check_for_duplicate(
        data_source=ds,
        bank_code="BB005",
        transaction_date=date(2026, 4, 16),
        signed_amount_pounds=-198.00,
        action="sales_refund",
        account_code="P051",
        description="P Flannery",
        reference="Refund",
    )
    assert result.kind is DuplicateMatchKind.NONE, (
        f"Refund must NOT match a +£198 receipt — got {result}"
    )


def test_regression_multi_occurrence_two_lime_purchases_both_post():
    """Two identical -£6.99 Lime card purchases on different dates;
    Opera has only ONE matching aentry. The second bank line must
    NOT see the same Opera entry as a duplicate.
    """
    existing_entry = {'ae_entry': 'P100008190', 'ae_value': -699,
                      'at_type': 5}
    ds = _FixtureDS(aentry=[existing_entry])

    # First line: matches the entry → duplicate
    result1 = check_for_duplicate(
        data_source=ds,
        bank_code="BB005",
        transaction_date=date(2026, 4, 17),
        signed_amount_pounds=-6.99,
        action="purchase_payment",
        account_code="LIME",
        description="Lime", reference="",
    )
    assert result1.kind is DuplicateMatchKind.CASHBOOK_DUPLICATE
    assert result1.matched_entry == 'P100008190'

    # Second line: caller passes the consumed entry in exclude list
    result2 = check_for_duplicate(
        data_source=ds,
        bank_code="BB005",
        transaction_date=date(2026, 4, 19),
        signed_amount_pounds=-6.99,
        action="purchase_payment",
        account_code="LIME",
        description="Lime", reference="",
        exclude_entry_numbers=['P100008190'],
    )
    assert result2.kind is DuplicateMatchKind.NONE


def test_regression_unallocated_credit_note_is_allocation_target_not_duplicate():
    """The Cloudsis case: P051 has an unallocated -£198 stran credit
    note (type='F'). The bank line is a -£198 sales_refund. The credit
    note is the *target* the new refund should allocate to — it is NOT
    a duplicate.
    """
    ds = _FixtureDS(
        aentry=[],  # no cashbook entry
        stran=[{'st_trref': 'CN_P051_198', 'st_trvalue': -198.00,
                'st_trtype': 'F'}],
    )
    result = check_for_duplicate(
        data_source=ds,
        bank_code="BB005",
        transaction_date=date(2026, 4, 16),
        signed_amount_pounds=-198.00,
        action="sales_refund",
        account_code="P051",
        description="P Flannery", reference="Refund",
    )
    assert result.kind is DuplicateMatchKind.LEDGER_ALLOCATION_TARGET
    assert result.matched_entry == 'CN_P051_198'
    # CRITICAL: NOT a duplicate — caller must POST.
    assert not result.is_duplicate


def test_regression_action_type_map_includes_pt_trtype_for_purchase_refund():
    """Direct lock-in of the historical pt_ref vs pt_trtype confusion.
    If anyone changes ACTION_TYPE_MAP['purchase_refund']['pt_trtype']
    to something other than 'F', this test fails.
    """
    from sql_rag.duplicate_check import ACTION_TYPE_MAP
    assert ACTION_TYPE_MAP['purchase_refund']['pt_trtype'] == 'F'
    assert ACTION_TYPE_MAP['sales_refund']['st_trtype'] == 'F'
