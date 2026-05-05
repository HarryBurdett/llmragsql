"""Opera 3 parity tests for the type-blind already-posted fallback.

The SE side has _is_already_posted_typeblind (sql_rag/bank_import.py:1530)
which catches the HISCOX-class scenario: a direct debit whose statement
description suggests action=purchase_payment (at_type=5) but Opera holds
the entry as at_type=1 (nominal payment to the supplier's NL account).
Without the fallback the operator double-posts.

Opera 3 was missing this fallback (cross-cutting audit F1). This test
pins the fixed Opera 3 implementation.
"""
from datetime import date
from unittest.mock import MagicMock


class _FakeReader:
    def __init__(self, tables):
        self._tables = tables
    def read_table(self, name):
        return self._tables.get(name, [])


def _make_txn(name='HISCOX', amount=-32.66, txn_date=None, action=None):
    from sql_rag.bank_import_opera3 import BankTransaction
    return BankTransaction(
        row_number=1,
        date=txn_date or date(2026, 4, 1),
        amount=amount,
        subcategory='',
        memo='',
        name=name,
        reference='',
        action=action,
    )


def _make_importer(reader):
    from sql_rag.bank_import_opera3 import BankStatementMatcherOpera3
    importer = BankStatementMatcherOpera3.__new__(BankStatementMatcherOpera3)
    importer.reader = reader
    importer.bank_code = 'BB005'
    return importer


def test_o3_typeblind_falls_back_when_action_unset():
    """When txn.action is None, the type-aware path can't run.
    Opera 3 must fall through to the type-blind fallback (parity with SE)."""
    reader = _FakeReader({
        'aentry': [{
            'ae_acnt': 'BB005', 'ae_entry': 'P100000754',
            'ae_cbtype': 'P1', 'ae_reclnum': 0, 'ae_remove': False,
        }],
        'atran': [{
            'at_acnt': 'BB005', 'at_entry': 'P100000754',
            'at_cbtype': 'P1', 'at_value': -3266,
            'at_pstdate': date(2026, 4, 1), 'at_type': 5,
        }],
    })
    importer = _make_importer(reader)
    txn = _make_txn(action=None)

    is_posted, reason = importer._is_already_posted(txn, bank_code='BB005')

    assert is_posted, (
        "Opera 3 must fall through to type-blind when action is unset "
        "(parity with SE)."
    )
    assert txn.is_duplicate is True
    assert 'P100000754' in reason
    assert 'type-blind' in reason


def test_o3_typeblind_runs_after_type_aware_misses():
    """When action IS set BUT type-aware finds no match (HISCOX scenario:
    action=purchase_payment / at_type=5, but Opera holds it as at_type=1),
    Opera 3 must run the type-blind fallback as a safety net."""
    from unittest.mock import patch
    from sql_rag.duplicate_check import DuplicateCheckResult, DuplicateMatchKind

    reader = _FakeReader({
        'aentry': [{
            'ae_acnt': 'BB005', 'ae_entry': 'P100000754',
            'ae_cbtype': 'P1', 'ae_reclnum': 0, 'ae_remove': False,
        }],
        'atran': [{
            'at_acnt': 'BB005', 'at_entry': 'P100000754',
            'at_cbtype': 'P1', 'at_value': -3266,
            'at_pstdate': date(2026, 4, 1), 'at_type': 1,  # not 5
        }],
    })
    importer = _make_importer(reader)
    txn = _make_txn(action='purchase_payment')

    with patch('sql_rag.duplicate_check.check_for_duplicate') as mock_check:
        mock_check.return_value = DuplicateCheckResult(
            kind=DuplicateMatchKind.NONE,
            matched_table=None,
            matched_entry=None,
            reason='no match',
        )
        is_posted, reason = importer._is_already_posted(txn, bank_code='BB005')

    assert is_posted, "Type-blind fallback must catch the HISCOX scenario"
    assert txn.is_duplicate is True
    assert 'P100000754' in reason


def test_o3_typeblind_skips_correction_pair_matched():
    """An aentry with ae_remove=True must NOT be a candidate for the
    type-blind fallback (open-items rule)."""
    reader = _FakeReader({
        'aentry': [{
            'ae_acnt': 'BB005', 'ae_entry': 'P100000755',
            'ae_cbtype': 'P1', 'ae_reclnum': 0, 'ae_remove': True,  # matched out
        }],
        'atran': [{
            'at_acnt': 'BB005', 'at_entry': 'P100000755',
            'at_cbtype': 'P1', 'at_value': -19800,
            'at_pstdate': date(2026, 4, 16), 'at_type': 5,
        }],
    })
    importer = _make_importer(reader)
    txn = _make_txn(name='P Flannery refund', amount=-198.00,
                    txn_date=date(2026, 4, 16), action=None)

    is_posted, reason = importer._is_already_posted(txn, bank_code='BB005')
    assert is_posted is False
    assert txn.is_duplicate is False


def test_o3_typeblind_skips_reconciled():
    """An aentry with ae_reclnum>0 must NOT be a candidate (open-items rule)."""
    reader = _FakeReader({
        'aentry': [{
            'ae_acnt': 'BB005', 'ae_entry': 'P100000700',
            'ae_cbtype': 'P1', 'ae_reclnum': 2682, 'ae_remove': False,
        }],
        'atran': [{
            'at_acnt': 'BB005', 'at_entry': 'P100000700',
            'at_cbtype': 'P1', 'at_value': -3266,
            'at_pstdate': date(2026, 4, 1), 'at_type': 5,
        }],
    })
    importer = _make_importer(reader)
    txn = _make_txn(action=None)

    is_posted, reason = importer._is_already_posted(txn, bank_code='BB005')
    assert is_posted is False


def test_o3_typeblind_sign_aware():
    """A -£32.66 statement line must NOT match a +£32.66 atran row."""
    reader = _FakeReader({
        'aentry': [{
            'ae_acnt': 'BB005', 'ae_entry': 'R100000200',
            'ae_cbtype': 'R1', 'ae_reclnum': 0, 'ae_remove': False,
        }],
        'atran': [{
            'at_acnt': 'BB005', 'at_entry': 'R100000200',
            'at_cbtype': 'R1', 'at_value': 3266,  # POSITIVE
            'at_pstdate': date(2026, 4, 1), 'at_type': 4,
        }],
    })
    importer = _make_importer(reader)
    txn = _make_txn(action=None)  # txn.amount = -32.66 (negative)

    is_posted, reason = importer._is_already_posted(txn, bank_code='BB005')
    assert is_posted is False, (
        "Sign-blind match would falsely flag a +£32.66 receipt as the "
        "duplicate of a -£32.66 payment."
    )


def test_o3_typeblind_date_window_strict():
    """Outside ±7 days → no match."""
    reader = _FakeReader({
        'aentry': [{
            'ae_acnt': 'BB005', 'ae_entry': 'P100000754',
            'ae_cbtype': 'P1', 'ae_reclnum': 0, 'ae_remove': False,
        }],
        'atran': [{
            'at_acnt': 'BB005', 'at_entry': 'P100000754',
            'at_cbtype': 'P1', 'at_value': -3266,
            'at_pstdate': date(2026, 3, 1),  # 31 days before txn.date
            'at_type': 5,
        }],
    })
    importer = _make_importer(reader)
    txn = _make_txn(action=None)  # txn.date = 2026-04-01

    is_posted, _ = importer._is_already_posted(txn, bank_code='BB005')
    assert is_posted is False
