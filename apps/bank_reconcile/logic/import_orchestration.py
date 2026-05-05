"""Pure helpers for the bank-statement import handlers.

Audit cross-cutting F9: the two SE import handlers
(import_bank_statement_from_email at 841 lines and
import_bank_statement_from_pdf at 765 lines) share most of their
shape — extract from PDF, period-overlap guard, BankTransaction
creation, date+manual override merge, period validation,
repeat-entry guard, posting loop, audit row.

This module extracts the pure phases that don't depend on FastAPI
request/response objects:
  - check_statement_period_overlap(...)
  - convert_to_bank_transactions(...)
  - apply_date_overrides(...)
  - apply_manual_overrides(...)
  - validate_transaction_periods(...)
  - find_unprocessed_repeat_entries(...)

The handlers retain the FastAPI-specific glue (request body parse,
import lock, response shape) but delegate all transaction-shaping
to these helpers.
"""
from __future__ import annotations

from datetime import datetime as _datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple


# Valid transaction actions a manual override may set on a row.
# Union of what both SE import handlers accepted pre-F9 (from-pdf
# included 'defer'; from-email did not — harmonised here).
VALID_TRANSACTION_TYPE_OVERRIDES = (
    'sales_receipt', 'purchase_payment',
    'sales_refund', 'purchase_refund',
    'nominal_payment', 'nominal_receipt',
    'bank_transfer', 'defer',
)


# Posting actions (subset of overrides) that need period validation.
POSTING_ACTIONS = (
    'sales_receipt', 'purchase_payment',
    'sales_refund', 'purchase_refund',
    'nominal_payment', 'nominal_receipt',
    'bank_transfer',
)


# ====================================================================
# Phase: statement period overlap guard
# ====================================================================


def check_statement_period_overlap(
    *,
    email_storage,
    bank_code: str,
    period_start: Optional[str],
    period_end: Optional[str],
    stmt_transactions: Iterable,
    filename: Optional[str],
    resume_import_id: Optional[int],
    skip_overlap_check: bool,
) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
    """Detect a statement that overlaps with a previously imported one.

    Returns (overlap_error, resume_import_id).
      - If `skip_overlap_check` or `email_storage` is None: returns
        (None, resume_import_id) — caller continues normally.
      - If no overlap found: returns (None, resume_import_id).
      - If overlap on SAME filename: returns (None, overlap.import_id)
        — caller should resume the existing import_id.
      - If overlap on DIFFERENT filename: returns (overlap_error, None)
        — caller should return overlap_error to the client.

    The dates are passed in pre-isoformatted (the call sites differ
    in how they get period_start/period_end out of statement_info /
    stmt_transactions).
    """
    if skip_overlap_check or not email_storage:
        return None, resume_import_id

    # Fall back to first/last transaction dates if period not explicitly
    # available (matches both handlers' pre-F9 behaviour).
    eff_start = period_start
    eff_end = period_end
    if not eff_start and stmt_transactions:
        dates = [st.date for st in stmt_transactions if getattr(st, 'date', None)]
        if dates:
            eff_start = min(dates).isoformat() if hasattr(min(dates), 'isoformat') else str(min(dates))
            eff_end = max(dates).isoformat() if hasattr(max(dates), 'isoformat') else str(max(dates))

    if not (eff_start and eff_end):
        return None, resume_import_id

    overlap = email_storage.check_period_overlap(
        bank_code=bank_code,
        period_start=eff_start,
        period_end=eff_end,
        exclude_import_id=resume_import_id,
    )
    if not overlap:
        return None, resume_import_id

    # Same-filename re-import is a continuation, not a conflict —
    # operator went back to add missed lines, accumulate them on the
    # existing import record.
    if (overlap.get('filename') or '').strip() == (filename or '').strip():
        return None, overlap['import_id']

    return {
        "success": False,
        "overlap_warning": True,
        "error": "Statement period overlaps with a previously imported statement",
        "overlap_details": {
            "existing_import_id": overlap['import_id'],
            "existing_filename": overlap['filename'],
            "existing_period": f"{overlap['period_start']} to {overlap['period_end']}",
            "existing_import_date": overlap['import_date'],
            "new_period": f"{eff_start} to {eff_end}",
        },
    }, None


# ====================================================================
# Phase: BankTransaction creation
# ====================================================================


def convert_to_bank_transactions(stmt_transactions: Iterable) -> List:
    """Convert a list of StatementTransaction (from
    StatementReconciler.extract_transactions_from_pdf) into the
    BankTransaction shape that BankStatementImport.process_transactions
    expects."""
    from sql_rag.bank_import import BankTransaction
    transactions: List = []
    for i, st in enumerate(stmt_transactions, start=1):
        transactions.append(BankTransaction(
            row_number=i,
            date=st.date,
            amount=st.amount,
            subcategory=st.transaction_type or '',
            memo=st.description or '',
            name=st.description or '',
            reference=st.reference or '',
            fit_id='',
        ))
    return transactions


# ====================================================================
# Phase: apply date overrides
# ====================================================================


def apply_date_overrides(transactions: List, date_overrides: Iterable[Dict[str, Any]]) -> None:
    """Mutate each txn whose row_number is in date_overrides to use
    the override date. Stashes the original on `original_date`."""
    date_override_map = {d['row']: d['date'] for d in date_overrides}
    for txn in transactions:
        if txn.row_number in date_override_map:
            new_date_str = date_override_map[txn.row_number]
            txn.original_date = txn.date
            txn.date = _datetime.strptime(new_date_str, '%Y-%m-%d').date()


# ====================================================================
# Phase: apply manual overrides
# ====================================================================


def apply_manual_overrides(transactions: List, overrides: Iterable[Dict[str, Any]]) -> None:
    """Apply each manual override row to the matching transaction.

    Per-row override fields supported:
      - account / ledger_type     : manual_account/manual_ledger_type
      - cbtype                    : cashbook type override
      - transaction_type          : sets txn.action (must be in
                                     VALID_TRANSACTION_TYPE_OVERRIDES)
      - bank_transfer_details     : payload for bank_transfer action
      - project_code/department_code/vat_code : nominal-entry codes

    If transaction_type is not one of the explicit values, the legacy
    ledger_type → action mapping fires instead:
      C → sales_receipt, S → purchase_payment, N → nominal_*

    For nominal, the sign of txn.amount picks _payment vs _receipt.
    """
    override_map = {o['row']: o for o in overrides}
    for txn in transactions:
        if txn.row_number not in override_map:
            continue
        override = override_map[txn.row_number]
        if override.get('account'):
            txn.manual_account = override.get('account')
            txn.manual_ledger_type = override.get('ledger_type')

        if override.get('cbtype'):
            txn.cbtype = override.get('cbtype')

        transaction_type = override.get('transaction_type')
        if transaction_type and transaction_type in VALID_TRANSACTION_TYPE_OVERRIDES:
            txn.action = transaction_type
            if transaction_type == 'bank_transfer':
                txn.bank_transfer_details = override.get('bank_transfer_details', {})
        elif override.get('ledger_type') == 'C':
            txn.action = 'sales_receipt'
        elif override.get('ledger_type') == 'S':
            txn.action = 'purchase_payment'
        elif override.get('ledger_type') == 'N':
            txn.action = 'nominal_payment' if txn.amount < 0 else 'nominal_receipt'

        if override.get('project_code'):
            txn.project_code = override['project_code']
        if override.get('department_code'):
            txn.department_code = override['department_code']
        if override.get('vat_code'):
            txn.vat_code = override['vat_code']


# ====================================================================
# Phase: validate posting periods
# ====================================================================


def validate_transaction_periods(
    *,
    transactions: List,
    selected_rows: Optional[Iterable[int]],
    sql_connector,
    get_ledger_type_for_transaction,
    validate_posting_period,
) -> List[Dict[str, Any]]:
    """For every selected, non-duplicate, posting-action transaction,
    check that the post date falls in an open ledger period.

    `get_ledger_type_for_transaction` and `validate_posting_period` are
    injected to keep this module decoupled from the import package
    layout.

    Returns a list of period-violation dicts; empty when all pass.
    """
    selected = set(selected_rows) if selected_rows is not None else None
    ledger_names = {'SL': 'Sales Ledger', 'PL': 'Purchase Ledger', 'NL': 'Nominal Ledger'}
    violations: List[Dict[str, Any]] = []

    for txn in transactions:
        if selected is not None and txn.row_number not in selected:
            continue
        if txn.action not in POSTING_ACTIONS:
            continue
        if getattr(txn, 'is_duplicate', False):
            continue

        ledger_type = get_ledger_type_for_transaction(txn.action)
        period_result = validate_posting_period(sql_connector, txn.date, ledger_type)
        if period_result.is_valid:
            continue

        violations.append({
            "row": txn.row_number,
            "date": txn.date.isoformat(),
            "name": txn.name,
            "amount": txn.amount,
            "action": txn.action,
            "ledger_type": ledger_type,
            "ledger_name": ledger_names.get(ledger_type, ledger_type),
            "error": period_result.error_message,
            "year": period_result.year,
            "period": period_result.period,
        })
    return violations


# ====================================================================
# Phase: detect unprocessed repeat entries (must run Opera's routine first)
# ====================================================================


def find_unprocessed_repeat_entries(transactions: List) -> List[Dict[str, Any]]:
    """Find any txn whose action is 'repeat_entry' AND whose
    period_valid flag (set by upstream period gating) is True.

    These rows are surfaced to the operator as a hard error — they
    must run Opera's Repeat Entries routine before this import can
    proceed.

    Period-blocked repeat entries are silently skipped (they can't
    be posted anyway).
    """
    out: List[Dict[str, Any]] = []
    for txn in transactions:
        if txn.action != 'repeat_entry':
            continue
        if not getattr(txn, 'period_valid', True):
            continue
        out.append({
            "row": txn.row_number,
            "name": txn.name,
            "amount": txn.amount,
            "date": txn.date.isoformat(),
            "entry_ref": getattr(txn, 'repeat_entry_ref', None),
            "entry_desc": getattr(txn, 'repeat_entry_desc', None),
        })
    return out
