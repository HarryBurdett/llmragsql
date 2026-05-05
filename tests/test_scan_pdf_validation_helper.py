"""Behavioural tests for the F9 scan-PDF validation helper.

Audit cross-cutting F9: the per-attachment validation block was
duplicated across four scan endpoints. This module is the new
single source of truth — tests pin every branch:
  - cache hit (with all fields)
  - cache miss + extract_on_miss=False (defer)
  - cache miss + extract success
  - cache miss + rate-limit
  - cache miss + extraction error
  - cache miss + generic exception
  - account match (all three sub-cases)
  - account mismatch (sort+account, account-only)
  - chain complete via reconciled opening
  - chain complete via opening below reconciled
  - chain not complete
  - orchestrator: extraction failure short-circuits
  - orchestrator: account mismatch short-circuits chain check
  - orchestrator: full happy path
"""
from __future__ import annotations

import datetime
from unittest.mock import MagicMock, patch

import pytest

from apps.bank_reconcile.logic.scan_pdf_validation import (
    StatementInfoData,
    AccountMatchResult,
    ChainCheckResult,
    StatementValidationVerdict,
    get_statement_info,
    check_account_match,
    check_chain_complete,
    validate_pdf_for_scan,
    _info_to_attachment_updates,
)


# ====================================================================
# Phase 1 — get_statement_info
# ====================================================================


class _FakeCache:
    """Minimal PDFExtractionCache stand-in."""
    def __init__(self, hit_data=None):
        self.hit_data = hit_data
        self.hashed = []
        self.gotten = []

    def hash_pdf(self, content):
        self.hashed.append(content)
        return f"hash_of_{len(content)}"

    def get(self, pdf_hash):
        self.gotten.append(pdf_hash)
        return self.hit_data


def test_cache_hit_returns_normalised_info_data():
    cache = _FakeCache(hit_data=({
        'opening_balance': 1234.56,
        'closing_balance': 5678.90,
        'period_start': '2026-04-01',
        'period_end': '2026-04-30',
        'bank_name': 'Barclays',
        'account_number': '12345678',
        'sort_code': '20-00-00',
    }, 'meta'))

    out = get_statement_info(
        content_bytes=b'pdf bytes',
        filename='stmt.pdf',
        cache=cache,
        sql_connector=None,
        company_settings={},
        config=None,
        extract_on_miss=True,
    )

    assert out.extraction_status == 'cached'
    assert out.opening_balance == 1234.56
    assert out.closing_balance == 5678.90
    assert out.period_start == '2026-04-01'
    assert out.bank_name == 'Barclays'
    assert out.account_number == '12345678'
    assert out.sort_code == '20-00-00'
    assert out.extraction_failure_reason is None


def test_cache_hit_handles_none_balances():
    cache = _FakeCache(hit_data=({
        'opening_balance': None,
        'closing_balance': None,
        'period_start': None,
        'period_end': None,
        'bank_name': None,
        'account_number': None,
        'sort_code': None,
    }, None))

    out = get_statement_info(
        content_bytes=b'x', filename='x.pdf', cache=cache,
        sql_connector=None, company_settings={}, config=None,
        extract_on_miss=True,
    )
    assert out.opening_balance is None
    assert out.closing_balance is None
    assert out.extraction_status == 'cached'


def test_cache_miss_with_extract_on_miss_false_defers():
    cache = _FakeCache(hit_data=None)
    out = get_statement_info(
        content_bytes=b'pdf', filename='stmt.pdf', cache=cache,
        sql_connector=None, company_settings={}, config=None,
        extract_on_miss=False,
    )
    assert out.extraction_status == 'pending_extraction'
    assert out.opening_balance is None
    assert out.extraction_failure_reason is None


def test_cache_miss_with_extract_runs_reconciler():
    """Cache miss + extract_on_miss=True → calls StatementReconciler
    and maps fields back."""
    cache = _FakeCache(hit_data=None)

    fake_stmt_info = MagicMock()
    fake_stmt_info.opening_balance = 100.0
    fake_stmt_info.closing_balance = 200.0
    fake_stmt_info.period_start = datetime.date(2026, 4, 1)
    fake_stmt_info.period_end = datetime.date(2026, 4, 30)
    fake_stmt_info.bank_name = 'Barclays'
    fake_stmt_info.account_number = '12345678'
    fake_stmt_info.sort_code = '20-00-00'

    fake_reconciler = MagicMock()
    fake_reconciler.extract_transactions_from_pdf.return_value = (fake_stmt_info, [])

    with patch('sql_rag.statement_reconcile.StatementReconciler', return_value=fake_reconciler):
        out = get_statement_info(
            content_bytes=b'pdf bytes',
            filename='stmt.pdf',
            cache=cache,
            sql_connector=MagicMock(),
            company_settings={'gemini_api_key': 'KEY'},
            config=None,
            extract_on_miss=True,
        )

    assert out.extraction_status == 'extracted'
    assert out.opening_balance == 100.0
    assert out.closing_balance == 200.0
    assert out.period_start == '2026-04-01'
    assert out.period_end == '2026-04-30'
    assert out.bank_name == 'Barclays'


def test_cache_miss_rate_limit_returns_pending_extraction():
    """RateLimitExhaustedError → pending_extraction with
    extraction_failure_reason='rate_limit'."""
    cache = _FakeCache(hit_data=None)

    from sql_rag.statement_reconcile import RateLimitExhaustedError
    fake_reconciler = MagicMock()
    fake_reconciler.extract_transactions_from_pdf.side_effect = (
        RateLimitExhaustedError("quota exhausted")
    )

    with patch('sql_rag.statement_reconcile.StatementReconciler', return_value=fake_reconciler):
        out = get_statement_info(
            content_bytes=b'pdf', filename='stmt.pdf', cache=cache,
            sql_connector=MagicMock(), company_settings={}, config=None,
            extract_on_miss=True,
        )
    assert out.extraction_status == 'pending_extraction'
    assert out.extraction_failure_reason == 'rate_limit'


def test_cache_miss_extraction_error_returns_failed():
    """ExtractionFailedError → failed with reason 'extraction_error'."""
    cache = _FakeCache(hit_data=None)

    from sql_rag.statement_reconcile import ExtractionFailedError
    fake_reconciler = MagicMock()
    fake_reconciler.extract_transactions_from_pdf.side_effect = (
        ExtractionFailedError("bad PDF")
    )

    with patch('sql_rag.statement_reconcile.StatementReconciler', return_value=fake_reconciler):
        out = get_statement_info(
            content_bytes=b'pdf', filename='stmt.pdf', cache=cache,
            sql_connector=MagicMock(), company_settings={}, config=None,
            extract_on_miss=True,
        )
    assert out.extraction_status == 'failed'
    assert out.extraction_failure_reason == 'extraction_error'


def test_cache_miss_generic_exception_returns_failed():
    """Any other Exception → failed with reason 'extraction_error'."""
    cache = _FakeCache(hit_data=None)

    fake_reconciler = MagicMock()
    fake_reconciler.extract_transactions_from_pdf.side_effect = (
        RuntimeError("network down")
    )

    with patch('sql_rag.statement_reconcile.StatementReconciler', return_value=fake_reconciler):
        out = get_statement_info(
            content_bytes=b'pdf', filename='stmt.pdf', cache=cache,
            sql_connector=MagicMock(), company_settings={}, config=None,
            extract_on_miss=True,
        )
    assert out.extraction_status == 'failed'
    assert out.extraction_failure_reason == 'extraction_error'


# ====================================================================
# Phase 2 — check_account_match
# ====================================================================


def _info(sort=None, account=None):
    return StatementInfoData(sort_code=sort, account_number=account)


def test_account_match_full_pair_matches():
    out = check_account_match(
        info=_info(sort='20-00-00', account='12345678'),
        opera_sort_code='200000',
        opera_account_number='12345678',
        filename='x.pdf',
    )
    assert out.matches is True
    assert out.skip_reason is None


def test_account_match_full_pair_mismatched_sort():
    out = check_account_match(
        info=_info(sort='20-00-00', account='12345678'),
        opera_sort_code='123456',
        opera_account_number='12345678',
        filename='x.pdf',
    )
    assert out.matches is False
    assert out.validation_status == 'wrong_account'
    assert 'wrong bank account' in out.skip_reason


def test_account_match_account_only_matches():
    out = check_account_match(
        info=_info(account='12345678'),  # no sort code on stmt
        opera_sort_code='200000',
        opera_account_number='12345678',
        filename='x.pdf',
    )
    assert out.matches is True


def test_account_match_account_only_mismatch():
    out = check_account_match(
        info=_info(account='12345678'),
        opera_sort_code='200000',
        opera_account_number='99999999',
        filename='x.pdf',
    )
    assert out.matches is False
    assert out.validation_status == 'wrong_account'
    assert 'wrong account number' in out.skip_reason


def test_account_match_missing_sides_assumes_match():
    """When either side has no usable identifier, default to match."""
    out = check_account_match(
        info=_info(),  # no sort, no account
        opera_sort_code=None,
        opera_account_number=None,
        filename='x.pdf',
    )
    assert out.matches is True


def test_account_match_normalises_dashes_and_spaces():
    out = check_account_match(
        info=_info(sort='20 - 00 - 00', account='1234 5678'),
        opera_sort_code='200000',
        opera_account_number='12345678',
        filename='x.pdf',
    )
    assert out.matches is True


# ====================================================================
# Phase 3 — check_chain_complete
# ====================================================================


def test_chain_no_opening_balance():
    out = check_chain_complete(
        opening_balance=None, closing_balance=100.0,
        effective_reconciled_balance=50.0, fallback_reconciled_balance=50.0,
        bank_rec_openings=set(), filename='x.pdf',
    )
    assert out.chain_complete is False
    assert out.reason_kind is None


def test_chain_closing_matches_reconciled_opening():
    out = check_chain_complete(
        opening_balance=900.0, closing_balance=1000.0,
        effective_reconciled_balance=2000.0, fallback_reconciled_balance=2000.0,
        bank_rec_openings={1000.00},
        filename='x.pdf',
    )
    assert out.chain_complete is True
    assert out.reason_kind == 'closing_matches_reconciled_opening'
    assert 'closing matches reconciled' in out.skip_reason


def test_chain_opening_below_reconciled():
    out = check_chain_complete(
        opening_balance=400.0, closing_balance=500.0,
        effective_reconciled_balance=1000.0, fallback_reconciled_balance=900.0,
        bank_rec_openings=set(),
        filename='x.pdf',
    )
    assert out.chain_complete is True
    assert out.reason_kind == 'opening_below_reconciled'
    assert 'opening' in out.skip_reason and '£400.00' in out.skip_reason


def test_chain_opening_at_reconciled_is_not_complete():
    """Penny tolerance: opening must be more than 0.01 below."""
    out = check_chain_complete(
        opening_balance=1000.00, closing_balance=1100.00,
        effective_reconciled_balance=1000.00, fallback_reconciled_balance=1000.00,
        bank_rec_openings=set(),
        filename='x.pdf',
    )
    assert out.chain_complete is False


def test_chain_falls_back_to_fallback_balance_when_effective_missing():
    out = check_chain_complete(
        opening_balance=100.0, closing_balance=200.0,
        effective_reconciled_balance=None, fallback_reconciled_balance=500.0,
        bank_rec_openings=set(),
        filename='x.pdf',
    )
    assert out.chain_complete is True
    assert out.reason_kind == 'opening_below_reconciled'


def test_chain_match_takes_priority_over_below_reconciled():
    """If both branches fire, the chain-match path wins (no audit row)."""
    out = check_chain_complete(
        opening_balance=400.0, closing_balance=1000.0,
        effective_reconciled_balance=1000.0, fallback_reconciled_balance=1000.0,
        bank_rec_openings={1000.00},
        filename='x.pdf',
    )
    assert out.chain_complete is True
    assert out.reason_kind == 'closing_matches_reconciled_opening'


# ====================================================================
# Phase 4 — _info_to_attachment_updates
# ====================================================================


def test_attachment_updates_cached_includes_extraction_status():
    """Cache hit sets extraction_status='cached' for UI consistency
    across all four scan endpoints (post-F9 harmonisation)."""
    info = StatementInfoData(
        opening_balance=100.0, closing_balance=200.0,
        period_start='2026-04-01', extraction_status='cached',
    )
    upd = _info_to_attachment_updates(info)
    assert upd['extraction_status'] == 'cached'
    assert upd['period_start'] == '2026-04-01'
    assert upd['opening_balance'] == 100.0
    assert upd['closing_balance'] == 200.0


def test_attachment_updates_extracted_includes_status():
    info = StatementInfoData(
        opening_balance=100.0, closing_balance=200.0,
        extraction_status='extracted',
    )
    upd = _info_to_attachment_updates(info)
    assert upd['extraction_status'] == 'extracted'
    assert 'status' not in upd  # only set on pending_extraction


def test_attachment_updates_pending_sets_status_too():
    info = StatementInfoData(extraction_status='pending_extraction')
    upd = _info_to_attachment_updates(info)
    assert upd['extraction_status'] == 'pending_extraction'
    assert upd['status'] == 'pending_extraction'


def test_attachment_updates_failed_sets_failure_reason():
    info = StatementInfoData(
        extraction_status='failed',
        extraction_failure_reason='extraction_error',
    )
    upd = _info_to_attachment_updates(info)
    assert upd['extraction_status'] == 'failed'
    assert upd['extraction_failure_reason'] == 'extraction_error'


def test_attachment_updates_skips_opening_when_none():
    """When opening_balance is None, no key set — preserves the
    handler's `if opening_bal_raw is not None:` guard."""
    info = StatementInfoData(opening_balance=None, extraction_status='cached')
    upd = _info_to_attachment_updates(info)
    assert 'opening_balance' not in upd


# ====================================================================
# Orchestrator — validate_pdf_for_scan
# ====================================================================


def _hit_cache(opening=None, closing=None, sort='200000', account='12345678'):
    return _FakeCache(hit_data=({
        'opening_balance': opening,
        'closing_balance': closing,
        'period_start': '2026-04-01',
        'period_end': '2026-04-30',
        'bank_name': 'Barclays',
        'account_number': account,
        'sort_code': sort,
    }, None))


def test_validate_happy_path_cache_hit_account_match_chain_open():
    cache = _hit_cache(opening=2000.0, closing=2500.0)
    verdict = validate_pdf_for_scan(
        content_bytes=b'pdf', filename='stmt.pdf', cache=cache,
        sql_connector=None, company_settings={}, config=None,
        extract_on_miss=True,
        opera_sort_code='200000', opera_account_number='12345678',
        effective_reconciled_balance=1500.0, fallback_reconciled_balance=1500.0,
        bank_rec_openings=set(),
    )
    assert verdict.is_valid is True
    assert verdict.validation_status is None
    assert verdict.statement_opening_balance == 2000.0
    assert verdict.info_updates['bank_name'] == 'Barclays'


def test_validate_extraction_pending_short_circuits_to_valid():
    """Original behaviour: extraction failure does NOT drop the
    statement; downstream code handles the pending state."""
    cache = _FakeCache(hit_data=None)  # miss
    verdict = validate_pdf_for_scan(
        content_bytes=b'pdf', filename='stmt.pdf', cache=cache,
        sql_connector=None, company_settings={}, config=None,
        extract_on_miss=False,  # defer
        opera_sort_code='200000', opera_account_number='12345678',
        effective_reconciled_balance=None, fallback_reconciled_balance=None,
        bank_rec_openings=set(),
    )
    assert verdict.is_valid is True  # not dropped
    assert verdict.info_updates['extraction_status'] == 'pending_extraction'
    assert verdict.info_updates['status'] == 'pending_extraction'


def test_validate_account_mismatch_drops_statement():
    cache = _hit_cache(opening=2000.0, closing=2500.0, account='99999999')
    verdict = validate_pdf_for_scan(
        content_bytes=b'pdf', filename='stmt.pdf', cache=cache,
        sql_connector=None, company_settings={}, config=None,
        extract_on_miss=True,
        opera_sort_code='200000', opera_account_number='12345678',
        effective_reconciled_balance=None, fallback_reconciled_balance=None,
        bank_rec_openings=set(),
    )
    assert verdict.is_valid is False
    assert verdict.validation_status == 'wrong_account'
    assert verdict.record_already_processed is False


def test_validate_chain_match_does_not_record_audit_row():
    """Closing matches reconciled opening → drop, but DO NOT write
    audit row (handler behaviour preserved)."""
    cache = _hit_cache(opening=900.0, closing=1000.0)
    verdict = validate_pdf_for_scan(
        content_bytes=b'pdf', filename='stmt.pdf', cache=cache,
        sql_connector=None, company_settings={}, config=None,
        extract_on_miss=True,
        opera_sort_code='200000', opera_account_number='12345678',
        effective_reconciled_balance=2000.0, fallback_reconciled_balance=2000.0,
        bank_rec_openings={1000.00},
    )
    assert verdict.is_valid is False
    assert verdict.validation_status == 'already_processed'
    assert verdict.record_already_processed is False


def test_validate_below_reconciled_records_audit_row():
    """Opening below reconciled → drop AND record audit row."""
    cache = _hit_cache(opening=400.0, closing=500.0)
    verdict = validate_pdf_for_scan(
        content_bytes=b'pdf', filename='stmt.pdf', cache=cache,
        sql_connector=None, company_settings={}, config=None,
        extract_on_miss=True,
        opera_sort_code='200000', opera_account_number='12345678',
        effective_reconciled_balance=1000.0, fallback_reconciled_balance=1000.0,
        bank_rec_openings=set(),
    )
    assert verdict.is_valid is False
    assert verdict.validation_status == 'already_processed'
    assert verdict.record_already_processed is True


# ====================================================================
# Opera 3 mode: opening_unblocks_chain + audit_row_on_chain_match
# ====================================================================


def test_o3_no_account_match_when_sort_account_none():
    """Opera 3 mode: pass opera_sort_code=None and opera_account_number=
    None to skip account match. Statement with mismatched account in
    PDF still passes if chain check is fine."""
    cache = _hit_cache(opening=2000.0, closing=2500.0, account='99999999')
    verdict = validate_pdf_for_scan(
        content_bytes=b'pdf', filename='stmt.pdf', cache=cache,
        sql_connector=None, company_settings={}, config=None,
        extract_on_miss=True,
        opera_sort_code=None, opera_account_number=None,
        effective_reconciled_balance=1500.0, fallback_reconciled_balance=1500.0,
        bank_rec_openings=set(),
    )
    assert verdict.is_valid is True


def test_o3_opening_unblocks_chain_lets_statement_through():
    """Opera 3 sequential gating: when opening matches an imported-
    pending closing, the opening_below_reconciled branch is suppressed."""
    cache = _hit_cache(opening=400.0, closing=500.0)
    verdict = validate_pdf_for_scan(
        content_bytes=b'pdf', filename='stmt.pdf', cache=cache,
        sql_connector=None, company_settings={}, config=None,
        extract_on_miss=True,
        opera_sort_code=None, opera_account_number=None,
        effective_reconciled_balance=1000.0, fallback_reconciled_balance=1000.0,
        bank_rec_openings=set(),
        opening_unblocks_chain=lambda opening: opening == 400.0,
    )
    assert verdict.is_valid is True


def test_o3_opening_unblocks_chain_does_not_override_chain_match():
    """The chain-match branch (closing == reconciled opening) is NOT
    suppressed by opening_unblocks_chain."""
    cache = _hit_cache(opening=400.0, closing=1000.0)
    verdict = validate_pdf_for_scan(
        content_bytes=b'pdf', filename='stmt.pdf', cache=cache,
        sql_connector=None, company_settings={}, config=None,
        extract_on_miss=True,
        opera_sort_code=None, opera_account_number=None,
        effective_reconciled_balance=1000.0, fallback_reconciled_balance=1000.0,
        bank_rec_openings={1000.00},
        opening_unblocks_chain=lambda opening: True,
    )
    assert verdict.is_valid is False
    assert verdict.validation_status == 'already_processed'


def test_o3_audit_row_on_chain_match_records_for_both_branches():
    """Opera 3 wants the audit row written for BOTH chain-match AND
    below-reconciled branches."""
    # Branch A: chain match
    cache = _hit_cache(opening=900.0, closing=1000.0)
    verdict_a = validate_pdf_for_scan(
        content_bytes=b'pdf', filename='stmt.pdf', cache=cache,
        sql_connector=None, company_settings={}, config=None,
        extract_on_miss=True,
        opera_sort_code=None, opera_account_number=None,
        effective_reconciled_balance=2000.0, fallback_reconciled_balance=2000.0,
        bank_rec_openings={1000.00},
        audit_row_on_chain_match=True,
    )
    assert verdict_a.is_valid is False
    assert verdict_a.record_already_processed is True

    # Branch B: opening below reconciled
    cache_b = _hit_cache(opening=400.0, closing=500.0)
    verdict_b = validate_pdf_for_scan(
        content_bytes=b'pdf', filename='stmt.pdf', cache=cache_b,
        sql_connector=None, company_settings={}, config=None,
        extract_on_miss=True,
        opera_sort_code=None, opera_account_number=None,
        effective_reconciled_balance=1000.0, fallback_reconciled_balance=1000.0,
        bank_rec_openings=set(),
        audit_row_on_chain_match=True,
    )
    assert verdict_b.is_valid is False
    assert verdict_b.record_already_processed is True


def test_check_chain_with_opening_unblocks_chain_directly():
    """Direct unit test for the new chain-check parameter."""
    out = check_chain_complete(
        opening_balance=400.0, closing_balance=500.0,
        effective_reconciled_balance=1000.0, fallback_reconciled_balance=1000.0,
        bank_rec_openings=set(),
        filename='x.pdf',
        opening_unblocks_chain=lambda opening: opening == 400.0,
    )
    assert out.chain_complete is False  # would have been True without callback
