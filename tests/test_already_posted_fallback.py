"""Pin the contract for `_is_already_posted` fallback when no action is set.

Regression introduced in commit 856d5ad (3 May 2026) — `_is_already_posted`
now early-returns when `txn.action` is unset/empty, skipping the duplicate
check entirely. Before that refactor, the function did a type-blind atran
lookup as one of its strategies, catching transactions that exist in Opera
even when the customer/supplier matcher couldn't classify them.

User-visible impact: scanning a bank statement against Opera shows
transactions as "unmatched" (needing posting) when they're actually
already in Opera atran — operators get asked to post duplicate entries.

This test pins the post-fix contract: when `txn.action` is unset, the
function MUST still detect a matching atran row on the same bank, same
signed amount, within the date tolerance, and flag `txn.is_duplicate=True`.
"""

from datetime import date
from unittest.mock import MagicMock

import pytest


def _make_txn(name: str, amount: float, txn_date: date, action=None, is_receipt=False):
    """Build a minimal stand-in for BankTransaction with the attributes
    `_is_already_posted` reads."""
    txn = MagicMock()
    txn.name = name
    txn.amount = amount
    txn.date = txn_date
    txn.action = action
    txn.is_receipt = is_receipt
    txn.matched_account = None
    txn.reference = ''
    txn.is_duplicate = False
    txn.skip_reason = None
    return txn


def _build_importer_with_fake_sql(rows):
    """Build a BankStatementImport whose sql_connector returns the given rows.

    `rows` is a list of dicts; the fake DataFrame supports `.empty`, `.iloc[0]`,
    and dict-style access on the row.
    """
    from sql_rag.bank_import import BankStatementImport

    if rows:
        fake_df = MagicMock()
        fake_df.empty = False
        first_row = MagicMock()
        # Make the row support dict-style access AND `in` checks
        first_row.__getitem__.side_effect = lambda k: rows[0][k]
        first_row.__contains__.side_effect = lambda k: k in rows[0]
        fake_df.iloc = MagicMock()
        fake_df.iloc.__getitem__ = MagicMock(return_value=first_row)
    else:
        fake_df = MagicMock()
        fake_df.empty = True

    fake_sql = MagicMock()
    fake_sql.execute_query.return_value = fake_df

    importer = BankStatementImport.__new__(BankStatementImport)
    importer.sql_connector = fake_sql
    importer.bank_code = 'BB005'
    return importer, fake_sql


def test_unset_action_still_falls_back_to_type_blind_atran_lookup():
    """When `txn.action` is unset, the duplicate check must NOT be skipped.

    Falls back to a type-blind atran lookup so transactions that exist in
    Opera but couldn't be classified by the customer/supplier matcher are
    still flagged as already-posted.
    """
    importer, fake_sql = _build_importer_with_fake_sql([
        {
            'ae_entry': 'P100000754',
            'ae_cbtype': 'P1',
            'at_value': -3266,
            'at_pstdate': date(2026, 4, 1),
            'at_type': 5,
        }
    ])

    txn = _make_txn(
        name='HISCOX UNDERWRITIN (Direct Debit)',
        amount=-32.66,
        txn_date=date(2026, 4, 1),
        action=None,
    )

    is_posted, reason = importer._is_already_posted(txn)

    assert is_posted, (
        "When action is unset but a matching atran row exists, the function "
        "must flag the transaction as already posted (type-blind fallback). "
        "Without this, scan shows the row as 'unmatched/needs posting'."
    )
    assert txn.is_duplicate is True
    assert 'P100000754' in reason
    # Confirm the SQL filtered by signed amount (-3266 pence), not abs
    sql_call = fake_sql.execute_query.call_args[0][0]
    assert 'at_value = -3266' in sql_call


def test_unset_action_with_no_matching_atran_returns_false():
    """If nothing matches in Opera, the transaction is genuinely new."""
    importer, _ = _build_importer_with_fake_sql([])

    txn = _make_txn(
        name='Brand New Supplier Ltd',
        amount=-100.00,
        txn_date=date(2026, 4, 1),
        action=None,
    )

    is_posted, _ = importer._is_already_posted(txn)
    assert is_posted is False
    assert txn.is_duplicate is False


def test_set_action_falls_back_when_type_aware_finds_nothing():
    """When action IS set BUT the type-aware check finds no match, the
    type-blind fallback runs as a safety net.

    Real-world example: HISCOX statement transaction with action=
    purchase_payment (at_type=5). Opera has the entry posted as a
    nominal_payment (at_type=1) to HISCOX's NL account. Type-aware
    check filters by at_type=5 → finds nothing. But the entry IS in
    Opera — type-blind fallback catches it by amount+bank+date.
    """
    from unittest.mock import patch
    from sql_rag.duplicate_check import DuplicateCheckResult, DuplicateMatchKind

    importer, fake_sql = _build_importer_with_fake_sql([
        {
            'ae_entry': 'P100000754',
            'ae_cbtype': 'P1',
            'at_value': -3266,
            'at_pstdate': date(2026, 4, 1),
            'at_type': 1,  # nominal_payment — DIFFERENT from action's at_type=5
        }
    ])

    txn = _make_txn(
        name='HISCOX',
        amount=-32.66,
        txn_date=date(2026, 4, 1),
        action='purchase_payment',  # action IS set, but Opera has at_type=1
    )

    # Stub the type-aware path to return NONE (no match found via at_type=5)
    with patch('sql_rag.duplicate_check.check_for_duplicate') as mock_check:
        mock_check.return_value = DuplicateCheckResult(
            kind=DuplicateMatchKind.NONE,
            matched_table=None,
            matched_entry=None,
            reason='no match',
        )
        is_posted, reason = importer._is_already_posted(txn)

    assert is_posted, (
        "Type-aware found no match (action=purchase_payment but Opera entry "
        "is at_type=1) — type-blind fallback must catch it as a safety net."
    )
    assert txn.is_duplicate is True
    assert 'P100000754' in reason
    # Confirm the type-blind SQL ran
    type_blind_calls = [
        c for c in fake_sql.execute_query.call_args_list
        if c[0] and 'at_value = -3266' in c[0][0]
    ]
    assert type_blind_calls, "Type-blind fallback SQL must run after type-aware misses"


def test_set_action_with_type_aware_match_does_not_run_fallback():
    """When type-aware finds a match, the type-blind fallback must NOT also
    run. Avoid redundant SQL and avoid weakening the type-aware protection.
    """
    from unittest.mock import patch
    from sql_rag.duplicate_check import DuplicateCheckResult, DuplicateMatchKind

    importer, fake_sql = _build_importer_with_fake_sql([
        {
            'ae_entry': 'P100000754',
            'ae_cbtype': 'P1',
            'at_value': -3266,
            'at_pstdate': date(2026, 4, 1),
            'at_type': 5,
        }
    ])

    txn = _make_txn(
        name='HISCOX',
        amount=-32.66,
        txn_date=date(2026, 4, 1),
        action='purchase_payment',
    )

    with patch('sql_rag.duplicate_check.check_for_duplicate') as mock_check:
        mock_check.return_value = DuplicateCheckResult(
            kind=DuplicateMatchKind.CASHBOOK_DUPLICATE,
            matched_table='aentry',
            matched_entry='P100000754',
            reason='Already in Opera as P100000754',
        )
        is_posted, _ = importer._is_already_posted(txn)

    assert is_posted, "Type-aware match must still flag as duplicate"
    type_blind_calls = [
        c for c in fake_sql.execute_query.call_args_list
        if c[0] and 'at_value = -3266' in c[0][0]
    ]
    assert not type_blind_calls, (
        "When type-aware found a match, type-blind fallback must NOT also "
        "run (avoids redundant SQL and unintended behaviour)."
    )
