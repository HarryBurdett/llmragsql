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


def test_set_action_does_not_fall_back():
    """When action IS set, the fallback must NOT run — the type-aware
    `check_for_duplicate` path is the correct one. Pinning this prevents
    the fallback from weakening the existing type-specific protection.
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
        name='HISCOX',
        amount=-32.66,
        txn_date=date(2026, 4, 1),
        action='purchase_payment',
    )

    # We don't care what the type-aware check returns here — only that
    # the type-blind fallback SQL is NOT issued (action is set, so it
    # shouldn't fire). The type-blind path uses `at_value = -3266` in its
    # SQL; assert that string never appears in any execute_query call.
    try:
        importer._is_already_posted(txn)
    except Exception:
        # The real `check_for_duplicate` may raise for various reasons in
        # this stubbed environment — that's fine. We only care about which
        # path was taken.
        pass

    type_blind_calls = [
        c for c in fake_sql.execute_query.call_args_list
        if c[0] and 'at_value = -3266' in c[0][0]
    ]
    assert not type_blind_calls, (
        "When action is set, the type-blind fallback SQL must NOT run; "
        "the function should delegate to check_for_duplicate instead."
    )
