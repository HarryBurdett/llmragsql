"""Unit tests for sql_rag.duplicate_check — single-source-of-truth
duplicate detection for bank reconcile."""
from __future__ import annotations

from sql_rag.duplicate_check import (
    DuplicateCheckResult,
    DuplicateMatchKind,
    ACTION_TYPE_MAP,
    DataSource,
)


def test_match_kind_enum_has_required_values():
    expected = {'NONE', 'CASHBOOK_DUPLICATE', 'LEDGER_ALLOCATION_TARGET'}
    actual = {k.name for k in DuplicateMatchKind}
    assert actual == expected


def test_match_kind_value_strings_are_stable():
    """Lock the .value strings — they appear in logs and audit fields."""
    assert DuplicateMatchKind.NONE.value == "none"
    assert DuplicateMatchKind.CASHBOOK_DUPLICATE.value == "cashbook_duplicate"
    assert DuplicateMatchKind.LEDGER_ALLOCATION_TARGET.value == "ledger_allocation_target"


def test_result_dataclass_fields():
    r = DuplicateCheckResult(
        kind=DuplicateMatchKind.NONE,
        matched_table=None,
        matched_entry=None,
        reason="no match",
    )
    assert r.kind is DuplicateMatchKind.NONE
    assert r.matched_table is None
    assert r.matched_entry is None
    assert r.reason == "no match"


def test_action_type_map_covers_all_actions():
    """Every action the bank-import flow can produce must have a mapping."""
    required_actions = {
        'sales_receipt', 'sales_refund',
        'purchase_payment', 'purchase_refund',
        'nominal_payment', 'nominal_receipt',
        'bank_transfer',
    }
    assert required_actions.issubset(set(ACTION_TYPE_MAP.keys()))


def test_action_type_map_has_correct_at_types():
    """The at_type values must match Opera's cashbook conventions
    (CLAUDE.md / opera_knowledge_base.md):
      1=Nominal Pmt, 2=Nominal Rcpt, 3=Sales Refund,
      4=Sales Receipt, 5=Purchase Pmt, 6=Purchase Refund, 8=Bank Transfer
    """
    assert ACTION_TYPE_MAP['nominal_payment']['at_type'] == 1
    assert ACTION_TYPE_MAP['nominal_receipt']['at_type'] == 2
    assert ACTION_TYPE_MAP['sales_refund']['at_type'] == 3
    assert ACTION_TYPE_MAP['sales_receipt']['at_type'] == 4
    assert ACTION_TYPE_MAP['purchase_payment']['at_type'] == 5
    assert ACTION_TYPE_MAP['purchase_refund']['at_type'] == 6
    assert ACTION_TYPE_MAP['bank_transfer']['at_type'] == 8


def test_action_type_map_has_correct_ledger_types():
    """Ledger types per central KB:
      sales_receipt   → stran 'R'
      sales_refund    → stran 'F'
      purchase_payment → ptran 'P'
      purchase_refund → ptran 'F'
      nominal_*, bank_transfer → no ledger row
    """
    assert ACTION_TYPE_MAP['sales_receipt']['st_trtype'] == 'R'
    assert ACTION_TYPE_MAP['sales_refund']['st_trtype'] == 'F'
    assert ACTION_TYPE_MAP['purchase_payment']['pt_trtype'] == 'P'
    assert ACTION_TYPE_MAP['purchase_refund']['pt_trtype'] == 'F'
    # Nominal and bank-transfer actions have no ledger type
    assert ACTION_TYPE_MAP['nominal_payment'].get('st_trtype') is None
    assert ACTION_TYPE_MAP['nominal_payment'].get('pt_trtype') is None
    assert ACTION_TYPE_MAP['bank_transfer'].get('st_trtype') is None
    assert ACTION_TYPE_MAP['bank_transfer'].get('pt_trtype') is None


def test_datasource_protocol_signatures_pinned():
    import inspect
    sig = inspect.signature(DataSource.find_aentry_by_signed_value)
    params = list(sig.parameters)
    assert params == [
        'self', 'bank_code', 'date_from', 'date_to',
        'signed_pence', 'expected_at_type', 'exclude_entry_numbers',
    ], f"find_aentry_by_signed_value signature drifted: {params}"

    sig = inspect.signature(DataSource.find_stran_by_signed_value)
    params = list(sig.parameters)
    assert params == [
        'self', 'account_code', 'date_from', 'date_to',
        'signed_pounds', 'st_trtype',
    ], f"find_stran_by_signed_value signature drifted: {params}"

    sig = inspect.signature(DataSource.find_ptran_by_signed_value)
    params = list(sig.parameters)
    assert params == [
        'self', 'account_code', 'date_from', 'date_to',
        'signed_pounds', 'pt_trtype',
    ], f"find_ptran_by_signed_value signature drifted: {params}"

    class _Good:
        def find_aentry_by_signed_value(self, bank_code, date_from, date_to,
                                         signed_pence, expected_at_type,
                                         exclude_entry_numbers): return []
        def find_stran_by_signed_value(self, account_code, date_from, date_to,
                                        signed_pounds, st_trtype): return []
        def find_ptran_by_signed_value(self, account_code, date_from, date_to,
                                        signed_pounds, pt_trtype): return []
    class _Bad:
        def find_aentry_by_signed_value(self, *a, **kw): return []
        # missing the other two
    assert isinstance(_Good(), DataSource)
    assert not isinstance(_Bad(), DataSource)
