"""Tests for the bank-import outcome builder."""
import pytest

from apps.bank_reconcile.logic.import_outcomes import (
    build_import_outcomes,
    _classify_skip_reason,
)


def _txn(row, amount=10.0, action='purchase_payment', **extra):
    return {
        'row': row,
        'amount': amount,
        'date': '2026-06-01',
        'description': f'Row {row}',
        'action': action,
        **extra,
    }


def test_all_imported_clean_path():
    r = build_import_outcomes(imported=[_txn(1), _txn(2)])
    assert r['success'] is True
    assert r['summary'] == 'all_posted'
    assert r['counts'] == {'posted': 2, 'held': 0, 'failed': 0, 'total': 2}
    assert all(o['status'] == 'posted' for o in r['outcomes'])


def test_all_already_posted_is_success_not_failure():
    """The 12-row case from the user report — every row is a duplicate.

    Today's UI screams 'Import Failed'. Verify the new shape reports
    success=True and summary='all_already_posted'.
    """
    rows = [_txn(i, reason='cashbook entry already posted on BC010') for i in range(1, 13)]
    r = build_import_outcomes(already_posted=rows)
    assert r['success'] is True
    assert r['summary'] == 'all_already_posted'
    assert r['counts'] == {'posted': 0, 'held': 12, 'failed': 0, 'total': 12}
    assert all(o['sub_status'] == 'already_posted' for o in r['outcomes'])


def test_partial_post_with_blocked_period():
    """3 posted, 3 held for blocked period — green banner with 'see below'."""
    r = build_import_outcomes(
        imported=[_txn(i) for i in (1, 2, 3)],
        period_blocked=[_txn(i, reason='Period 04/2026 is blocked') for i in (4, 5, 6)],
    )
    assert r['success'] is True
    assert r['summary'] == 'partial'
    assert r['counts']['posted'] == 3
    assert r['counts']['held'] == 3
    assert r['counts']['failed'] == 0
    period_rows = [o for o in r['outcomes'] if o['status'] == 'held']
    assert all(o['sub_status'] == 'period_blocked' for o in period_rows)


def test_any_failure_makes_it_red():
    r = build_import_outcomes(
        imported=[_txn(1)],
        errors=[_txn(2, error='DB constraint violation', sub_status='db_error')],
    )
    assert r['success'] is False
    assert r['summary'] == 'failed'
    assert r['counts'] == {'posted': 1, 'held': 0, 'failed': 1, 'total': 2}


def test_nothing_to_import():
    r = build_import_outcomes()
    assert r['success'] is True
    assert r['summary'] == 'nothing_to_import'
    assert r['counts']['total'] == 0


def test_outcomes_carry_opera_entry_ref_when_posted():
    r = build_import_outcomes(
        imported=[_txn(1, entry_number='P100008306')],
    )
    assert r['outcomes'][0]['opera_entry_ref'] == 'P100008306'


def test_outcomes_carry_opera_entry_ref_for_already_posted():
    """The duplicate-check identifies the existing Opera entry — surface it."""
    r = build_import_outcomes(
        already_posted=[_txn(1, opera_entry_ref='P100008306', reason='duplicate')],
    )
    assert r['outcomes'][0]['opera_entry_ref'] == 'P100008306'
    assert r['outcomes'][0]['sub_status'] == 'already_posted'


def test_skipped_with_period_reason_classified_as_period_blocked():
    r = build_import_outcomes(
        skipped=[_txn(1, reason='Period 03/2026 blocked')],
    )
    assert r['outcomes'][0]['sub_status'] == 'period_blocked'


def test_skipped_with_unknown_reason_falls_back_to_other():
    r = build_import_outcomes(skipped=[_txn(1, reason='something funky')])
    assert r['outcomes'][0]['sub_status'] == 'other'


def test_outcomes_are_deterministically_ordered():
    """Stable sort so client-side diffing / snapshot tests don't flake."""
    r = build_import_outcomes(
        imported=[_txn(3)],
        already_posted=[_txn(1)],
        errors=[_txn(2, error='boom')],
    )
    rows = [o['row'] for o in r['outcomes']]
    assert rows == [1, 2, 3]


@pytest.mark.parametrize('reason,expected', [
    ('Cashbook entry already posted on BC010', 'already_posted'),
    ('Duplicate of entry P100008306', 'already_posted'),
    ('Period 03/2026 is blocked', 'period_blocked'),
    ('Period closed', 'period_blocked'),
    ('Missing required field', 'incomplete'),
    ('Could not find matching customer', 'unmatched'),
    ('User ignored', 'user_ignored'),
    ('', 'other'),
    (None, 'other'),
])
def test_classify_skip_reason(reason, expected):
    assert _classify_skip_reason(reason) == expected
