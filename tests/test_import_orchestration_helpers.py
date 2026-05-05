"""Behavioural tests for the F9 import-orchestration helpers.

Audit cross-cutting F9: the bank-statement import handlers
(import_bank_statement_from_email and import_bank_statement_from_pdf)
shared a near-identical sequence of phases. Tests pin every helper
extracted to apps.bank_reconcile.logic.import_orchestration:

  - check_statement_period_overlap
  - convert_to_bank_transactions
  - apply_date_overrides
  - apply_manual_overrides
  - validate_transaction_periods
  - find_unprocessed_repeat_entries
"""
from __future__ import annotations

from datetime import date as _date
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from apps.bank_reconcile.logic.import_orchestration import (
    POSTING_ACTIONS,
    VALID_TRANSACTION_TYPE_OVERRIDES,
    apply_date_overrides,
    apply_manual_overrides,
    check_statement_period_overlap,
    convert_to_bank_transactions,
    find_unprocessed_repeat_entries,
    validate_transaction_periods,
)


# ====================================================================
# check_statement_period_overlap
# ====================================================================


def test_overlap_skipped_when_flag_set():
    storage = MagicMock()
    err, resume = check_statement_period_overlap(
        email_storage=storage,
        bank_code='BC010',
        period_start='2026-04-01', period_end='2026-04-30',
        stmt_transactions=[], filename='stmt.pdf',
        resume_import_id=None,
        skip_overlap_check=True,
    )
    assert err is None
    assert resume is None
    storage.check_period_overlap.assert_not_called()


def test_overlap_skipped_when_storage_none():
    err, resume = check_statement_period_overlap(
        email_storage=None,
        bank_code='BC010',
        period_start='2026-04-01', period_end='2026-04-30',
        stmt_transactions=[], filename='stmt.pdf',
        resume_import_id=42,
        skip_overlap_check=False,
    )
    assert err is None
    assert resume == 42  # passed through unchanged


def test_overlap_falls_back_to_txn_dates_when_period_missing():
    storage = MagicMock()
    storage.check_period_overlap.return_value = None
    txns = [
        SimpleNamespace(date=_date(2026, 4, 5)),
        SimpleNamespace(date=_date(2026, 4, 27)),
    ]
    check_statement_period_overlap(
        email_storage=storage,
        bank_code='BC010',
        period_start=None, period_end=None,
        stmt_transactions=txns, filename='stmt.pdf',
        resume_import_id=None,
        skip_overlap_check=False,
    )
    args = storage.check_period_overlap.call_args.kwargs
    assert args['period_start'] == '2026-04-05'
    assert args['period_end'] == '2026-04-27'


def test_overlap_no_dates_returns_pass_through():
    storage = MagicMock()
    err, resume = check_statement_period_overlap(
        email_storage=storage,
        bank_code='BC010',
        period_start=None, period_end=None,
        stmt_transactions=[], filename='stmt.pdf',
        resume_import_id=None,
        skip_overlap_check=False,
    )
    assert err is None
    assert resume is None
    storage.check_period_overlap.assert_not_called()


def test_overlap_same_filename_resumes_existing_id():
    storage = MagicMock()
    storage.check_period_overlap.return_value = {
        'import_id': 99,
        'filename': 'stmt.pdf',
        'period_start': '2026-04-01',
        'period_end': '2026-04-30',
        'import_date': '2026-04-30 10:00:00',
    }
    err, resume = check_statement_period_overlap(
        email_storage=storage,
        bank_code='BC010',
        period_start='2026-04-01', period_end='2026-04-30',
        stmt_transactions=[], filename='stmt.pdf',
        resume_import_id=None,
        skip_overlap_check=False,
    )
    assert err is None
    assert resume == 99  # auto-resume same-filename


def test_overlap_different_filename_returns_overlap_error():
    storage = MagicMock()
    storage.check_period_overlap.return_value = {
        'import_id': 99,
        'filename': 'old_stmt.pdf',
        'period_start': '2026-04-01',
        'period_end': '2026-04-30',
        'import_date': '2026-04-30 10:00:00',
    }
    err, resume = check_statement_period_overlap(
        email_storage=storage,
        bank_code='BC010',
        period_start='2026-04-15', period_end='2026-05-15',
        stmt_transactions=[], filename='new_stmt.pdf',
        resume_import_id=None,
        skip_overlap_check=False,
    )
    assert err is not None
    assert err['success'] is False
    assert err['overlap_warning'] is True
    assert err['overlap_details']['existing_import_id'] == 99
    assert resume is None


# ====================================================================
# convert_to_bank_transactions
# ====================================================================


def _stmt_txn(d, amount, ttype='', desc='', ref=''):
    return SimpleNamespace(
        date=d, amount=amount,
        transaction_type=ttype,
        description=desc,
        reference=ref,
    )


def test_convert_indexes_rows_starting_at_1():
    txns = convert_to_bank_transactions([
        _stmt_txn(_date(2026, 4, 1), 100.0, desc='A'),
        _stmt_txn(_date(2026, 4, 2), -50.0, desc='B'),
    ])
    assert len(txns) == 2
    assert txns[0].row_number == 1
    assert txns[1].row_number == 2


def test_convert_maps_fields():
    txn = convert_to_bank_transactions([
        _stmt_txn(_date(2026, 4, 1), 100.0, ttype='credit', desc='ACME', ref='R1'),
    ])[0]
    assert txn.date == _date(2026, 4, 1)
    assert txn.amount == 100.0
    assert txn.subcategory == 'credit'
    assert txn.memo == 'ACME'
    assert txn.name == 'ACME'
    assert txn.reference == 'R1'
    assert txn.fit_id == ''


def test_convert_handles_none_text_fields():
    """None description/reference/transaction_type → empty strings."""
    bad = SimpleNamespace(
        date=_date(2026, 4, 1), amount=10.0,
        transaction_type=None, description=None, reference=None,
    )
    txn = convert_to_bank_transactions([bad])[0]
    assert txn.subcategory == ''
    assert txn.memo == ''
    assert txn.reference == ''


# ====================================================================
# apply_date_overrides
# ====================================================================


def _bt(row, action='nominal_payment', amount=-100.0):
    """Lightweight stand-in for BankTransaction (avoids the import)."""
    return SimpleNamespace(
        row_number=row,
        date=_date(2026, 4, 1),
        amount=amount,
        action=action,
        is_duplicate=False,
        manual_account=None,
        manual_ledger_type=None,
        cbtype=None,
        bank_transfer_details=None,
        project_code=None,
        department_code=None,
        vat_code=None,
        name=f'txn{row}',
    )


def test_apply_date_overrides_changes_matching_rows_only():
    txns = [_bt(1), _bt(2), _bt(3)]
    apply_date_overrides(txns, [{'row': 2, 'date': '2026-05-15'}])
    assert txns[0].date == _date(2026, 4, 1)
    assert txns[1].date == _date(2026, 5, 15)
    assert txns[1].original_date == _date(2026, 4, 1)
    assert txns[2].date == _date(2026, 4, 1)


def test_apply_date_overrides_empty_no_change():
    txns = [_bt(1)]
    apply_date_overrides(txns, [])
    assert txns[0].date == _date(2026, 4, 1)


# ====================================================================
# apply_manual_overrides
# ====================================================================


def test_apply_manual_override_account_and_ledger_type():
    txns = [_bt(1)]
    apply_manual_overrides(txns, [{
        'row': 1, 'account': 'A001', 'ledger_type': 'C',
        'transaction_type': 'sales_receipt',
    }])
    assert txns[0].manual_account == 'A001'
    assert txns[0].manual_ledger_type == 'C'
    assert txns[0].action == 'sales_receipt'


def test_apply_manual_override_cbtype():
    txns = [_bt(1)]
    apply_manual_overrides(txns, [{'row': 1, 'cbtype': 'P1'}])
    assert txns[0].cbtype == 'P1'


def test_apply_manual_override_invalid_transaction_type_falls_through():
    """If transaction_type is bogus, ledger_type→action mapping
    still fires."""
    txns = [_bt(1)]
    apply_manual_overrides(txns, [{
        'row': 1, 'transaction_type': 'made_up_action',
        'ledger_type': 'C',
    }])
    assert txns[0].action == 'sales_receipt'


def test_apply_manual_override_supplier_payment():
    txns = [_bt(1)]
    apply_manual_overrides(txns, [{'row': 1, 'ledger_type': 'S'}])
    assert txns[0].action == 'purchase_payment'


def test_apply_manual_override_nominal_sign_dependent():
    """Negative → nominal_payment, positive → nominal_receipt."""
    out_neg = [_bt(1, amount=-100.0)]
    apply_manual_overrides(out_neg, [{'row': 1, 'ledger_type': 'N'}])
    assert out_neg[0].action == 'nominal_payment'

    out_pos = [_bt(1, amount=100.0)]
    apply_manual_overrides(out_pos, [{'row': 1, 'ledger_type': 'N'}])
    assert out_pos[0].action == 'nominal_receipt'


def test_apply_manual_override_bank_transfer_details_attached():
    txns = [_bt(1)]
    bt_details = {'dest_bank': 'BC020', 'reference': 'TF1'}
    apply_manual_overrides(txns, [{
        'row': 1,
        'transaction_type': 'bank_transfer',
        'bank_transfer_details': bt_details,
    }])
    assert txns[0].action == 'bank_transfer'
    assert txns[0].bank_transfer_details == bt_details


def test_apply_manual_override_defer_action_supported():
    """Harmonisation: both SE handlers now accept 'defer' (pre-F9
    only from-pdf accepted it)."""
    assert 'defer' in VALID_TRANSACTION_TYPE_OVERRIDES
    txns = [_bt(1)]
    apply_manual_overrides(txns, [{'row': 1, 'transaction_type': 'defer'}])
    assert txns[0].action == 'defer'


def test_apply_manual_override_project_dept_vat_codes():
    txns = [_bt(1)]
    apply_manual_overrides(txns, [{
        'row': 1, 'project_code': 'P1',
        'department_code': 'D1', 'vat_code': 'V1',
    }])
    assert txns[0].project_code == 'P1'
    assert txns[0].department_code == 'D1'
    assert txns[0].vat_code == 'V1'


def test_apply_manual_override_skips_unmatched_row():
    txns = [_bt(1), _bt(2)]
    apply_manual_overrides(txns, [{'row': 1, 'cbtype': 'P1'}])
    assert txns[0].cbtype == 'P1'
    assert txns[1].cbtype is None


# ====================================================================
# validate_transaction_periods
# ====================================================================


def _mk_period_result(is_valid, error_message='blocked', year=2026, period=4):
    return SimpleNamespace(
        is_valid=is_valid,
        error_message=error_message,
        year=year, period=period,
    )


def test_validate_periods_skips_non_posting_actions():
    txns = [_bt(1, action='repeat_entry'), _bt(2, action='sales_receipt')]

    def get_ledger(action): return 'SL'
    def vp(conn, d, lt): return _mk_period_result(True)

    out = validate_transaction_periods(
        transactions=txns, selected_rows=None,
        sql_connector=None,
        get_ledger_type_for_transaction=get_ledger,
        validate_posting_period=vp,
    )
    assert out == []


def test_validate_periods_skips_duplicates():
    txn = _bt(1, action='sales_receipt')
    txn.is_duplicate = True
    txns = [txn]

    def get_ledger(action): return 'SL'
    def vp(conn, d, lt): return _mk_period_result(False)  # would fail if checked

    out = validate_transaction_periods(
        transactions=txns, selected_rows=None,
        sql_connector=None,
        get_ledger_type_for_transaction=get_ledger,
        validate_posting_period=vp,
    )
    assert out == []  # duplicate skipped


def test_validate_periods_filters_to_selected_rows():
    txns = [
        _bt(1, action='sales_receipt'),
        _bt(2, action='sales_receipt'),
        _bt(3, action='sales_receipt'),
    ]

    def get_ledger(action): return 'SL'
    calls = []
    def vp(conn, d, lt):
        calls.append(lt)
        return _mk_period_result(True)

    validate_transaction_periods(
        transactions=txns, selected_rows={2},
        sql_connector=None,
        get_ledger_type_for_transaction=get_ledger,
        validate_posting_period=vp,
    )
    assert len(calls) == 1


def test_validate_periods_returns_violation_dicts():
    txns = [_bt(1, action='sales_receipt')]

    def get_ledger(action): return 'SL'
    def vp(conn, d, lt): return _mk_period_result(False, error_message='blocked')

    out = validate_transaction_periods(
        transactions=txns, selected_rows=None,
        sql_connector=None,
        get_ledger_type_for_transaction=get_ledger,
        validate_posting_period=vp,
    )
    assert len(out) == 1
    v = out[0]
    assert v['row'] == 1
    assert v['ledger_type'] == 'SL'
    assert v['ledger_name'] == 'Sales Ledger'
    assert v['error'] == 'blocked'


def test_posting_actions_constant_includes_all_cashbook_types():
    expected = {
        'sales_receipt', 'purchase_payment',
        'sales_refund', 'purchase_refund',
        'nominal_payment', 'nominal_receipt',
        'bank_transfer',
    }
    assert set(POSTING_ACTIONS) == expected


# ====================================================================
# find_unprocessed_repeat_entries
# ====================================================================


def test_repeat_entries_period_blocked_skipped():
    txn = _bt(1, action='repeat_entry')
    txn.period_valid = False
    out = find_unprocessed_repeat_entries([txn])
    assert out == []


def test_repeat_entries_open_period_returned():
    txn = _bt(1, action='repeat_entry')
    txn.period_valid = True
    txn.repeat_entry_ref = 'RE001'
    txn.repeat_entry_desc = 'Monthly fee'
    out = find_unprocessed_repeat_entries([txn])
    assert len(out) == 1
    assert out[0]['entry_ref'] == 'RE001'
    assert out[0]['entry_desc'] == 'Monthly fee'


def test_repeat_entries_default_period_valid_true():
    """If period_valid attr is missing, default to True (treat as
    open). Pre-F9 behaviour: getattr(..., True)."""
    txn = SimpleNamespace(
        row_number=1, action='repeat_entry',
        date=_date(2026, 4, 1), amount=10.0, name='x',
    )
    out = find_unprocessed_repeat_entries([txn])
    assert len(out) == 1


def test_repeat_entries_non_repeat_action_skipped():
    txn = _bt(1, action='sales_receipt')
    txn.period_valid = True
    out = find_unprocessed_repeat_entries([txn])
    assert out == []
