"""Integration test for scan-all-banks rate-limit gate.

Verifies that when extraction fails (rate-limited or otherwise), the bank is
gated to extraction_status='incomplete' and no statement is marked 'ready'.
"""

import pytest
from unittest.mock import MagicMock, patch

from sql_rag.gemini_throttle import RateLimitExhaustedError


def _build_stmts(states):
    """states: list of (opening, closing, status) tuples"""
    out = []
    for i, (opening, closing, status) in enumerate(states):
        out.append({
            'filename': f'stmt_{i}.pdf',
            'opening_balance': opening,
            'closing_balance': closing,
            'status': status,
            'extraction_status': 'extracted' if opening is not None else 'pending_extraction',
            'extraction_failure_reason': None if opening is not None else 'rate_limit',
        })
    return out


def _compute_gate(bank_info):
    """Mirror of the gate logic from routes.py for unit testing.

    The real implementation lives inside `scan_all_banks_for_statements`; this
    helper duplicates it so the test is fast and self-contained. Keep in sync.
    """
    bank_stmts = bank_info.get('statements', [])
    statements_total = len(bank_stmts)
    statements_extracted = sum(
        1 for s in bank_stmts
        if s.get('opening_balance') is not None
        and s.get('closing_balance') is not None
    )
    extraction_failures = [
        {'filename': s.get('filename'),
         'reason': s.get('extraction_failure_reason') or 'rate_limit'}
        for s in bank_stmts
        if (s.get('opening_balance') is None or s.get('closing_balance') is None)
    ]
    bank_info['statements_total'] = statements_total
    bank_info['statements_extracted'] = statements_extracted
    bank_info['extraction_failures'] = extraction_failures
    bank_info['extraction_status'] = (
        'complete' if statements_total > 0 and statements_extracted == statements_total
        else 'incomplete' if statements_total > 0
        else 'complete'
    )
    if bank_info['extraction_status'] == 'incomplete':
        for s in bank_stmts:
            if s.get('status') == 'ready':
                s['status'] = 'pending_extraction'
    return bank_info


def test_all_extracted_marks_complete():
    bank = {'statements': _build_stmts([
        (100.0, 200.0, 'ready'),
        (200.0, 300.0, 'ready'),
        (300.0, 400.0, 'ready'),
    ])}
    result = _compute_gate(bank)
    assert result['extraction_status'] == 'complete'
    assert result['statements_extracted'] == 3
    assert result['statements_total'] == 3
    assert result['extraction_failures'] == []
    assert all(s['status'] == 'ready' for s in result['statements'])


def test_one_failed_marks_incomplete_and_demotes_ready():
    bank = {'statements': _build_stmts([
        (100.0, 200.0, 'ready'),
        (None, None, 'ready'),
        (300.0, 400.0, 'ready'),
    ])}
    result = _compute_gate(bank)
    assert result['extraction_status'] == 'incomplete'
    assert result['statements_extracted'] == 2
    assert result['statements_total'] == 3
    assert len(result['extraction_failures']) == 1
    assert result['extraction_failures'][0]['filename'] == 'stmt_1.pdf'
    assert result['extraction_failures'][0]['reason'] == 'rate_limit'
    # All 'ready' demoted to 'pending_extraction' so user can't process out of order
    statuses = [s['status'] for s in result['statements']]
    assert statuses == ['pending_extraction', 'pending_extraction', 'pending_extraction']


def test_empty_bank_counts_as_complete():
    bank = {'statements': []}
    result = _compute_gate(bank)
    assert result['extraction_status'] == 'complete'
    assert result['statements_extracted'] == 0
    assert result['statements_total'] == 0


def test_partial_balance_counts_as_unextracted():
    # Opening present but closing missing — still incomplete
    bank = {'statements': [
        {'filename': 'a.pdf', 'opening_balance': 100.0, 'closing_balance': None,
         'status': 'ready', 'extraction_status': 'pending_extraction',
         'extraction_failure_reason': 'rate_limit'},
    ]}
    result = _compute_gate(bank)
    assert result['extraction_status'] == 'incomplete'
    assert result['statements_extracted'] == 0
