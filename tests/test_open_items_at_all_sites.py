"""Pin: open-items rule (ae_reclnum=0 AND ae_remove=0) is applied at every
candidate-fetch / unreconciled-balance-derivation site across the bank-rec
workflow, on both Opera SE and Opera 3.

This catches regressions of the kind the £198 P Flannery scenario revealed
(2026-05-04) — and the same class of regression at additional sites
discovered in the 2026-05-05 audit:

  - StatementReconciler.get_unreconciled_entries (SE + Opera 3)
  - StatementReconciler.get_all_entries (SE + Opera 3)
  - SE unreconciled-difference query at statement_reconcile.py:540
  - Opera 3 unreconciled-difference at opera3_data_provider.py:1561
  - Opera 3 match-statement endpoint (apps/bank_reconcile/api/routes.py:15575)

Spec: docs/superpowers/specs/2026-05-04-bank-rec-open-items-filter-design.md
"""
import inspect


def test_se_get_unreconciled_entries_uses_open_items_rule():
    """SE get_unreconciled_entries SQL must include ae_remove=0
    (or reference OPEN_FOR_REC_SQL)."""
    from sql_rag.statement_reconcile import StatementReconciler

    src = inspect.getsource(StatementReconciler.get_unreconciled_entries)
    assert ('ae_remove = 0' in src or 'OPEN_FOR_REC_SQL' in src), (
        'StatementReconciler.get_unreconciled_entries must apply the '
        'open-items rule (ae_remove=0) — re-introduces the £198 P Flannery '
        'bug otherwise.'
    )


def test_se_get_all_entries_uses_open_items_rule():
    """SE get_all_entries does NOT filter by ae_reclnum (returns reconciled
    + unreconciled) but MUST exclude ae_remove=True entries."""
    from sql_rag.statement_reconcile import StatementReconciler

    src = inspect.getsource(StatementReconciler.get_all_entries)
    assert 'ae_remove = 0' in src, (
        'StatementReconciler.get_all_entries must include ae_remove=0 — '
        'correction-pair-matched entries should not appear at all.'
    )


def test_se_unreconciled_difference_query_uses_open_items_rule():
    """The SE unreconciled-difference SUM query at statement_reconcile.py
    must filter by ae_remove=0."""
    src = open(
        '/Users/maccb/llmragsql/sql_rag/statement_reconcile.py',
        'r',
        encoding='utf-8',
    ).read()
    # The unrec_query string must include the ae_remove guard.
    # Search for the SELECT-SUM block.
    lower_src = src.lower()
    idx = lower_src.find('isnull(sum(ae_value), 0) / 100.0 as total')
    assert idx > 0, 'unreconciled-difference query not found'
    # The next 400 chars should contain ae_remove
    snippet = src[idx:idx + 400]
    assert 'ae_remove = 0' in snippet, (
        'unreconciled-difference query must include ae_remove=0 — '
        'correction-paired entries should not inflate the unreconciled total.'
    )


def test_o3_get_unreconciled_entries_uses_open_items_rule():
    """Opera 3 get_unreconciled_entries must call is_open_for_rec()."""
    from sql_rag.statement_reconcile_opera3 import StatementReconcilerOpera3

    src = inspect.getsource(StatementReconcilerOpera3.get_unreconciled_entries)
    assert 'is_open_for_rec' in src, (
        'Opera 3 get_unreconciled_entries must use is_open_for_rec()'
    )


def test_o3_get_all_entries_excludes_ae_remove():
    """Opera 3 get_all_entries does not filter ae_reclnum (returns both)
    but MUST exclude ae_remove=True."""
    from sql_rag.statement_reconcile_opera3 import StatementReconcilerOpera3

    src = inspect.getsource(StatementReconcilerOpera3.get_all_entries)
    assert 'ae_remove' in src, (
        'Opera 3 get_all_entries must reference ae_remove'
    )


def test_o3_data_provider_unreconciled_uses_open_items_rule():
    """opera3_data_provider.get_bank_reconciliation_status's
    unreconciled-loop must call is_open_for_rec()."""
    src = open(
        '/Users/maccb/llmragsql/sql_rag/opera3_data_provider.py',
        'r',
        encoding='utf-8',
    ).read()
    assert 'is_open_for_rec' in src, (
        'opera3_data_provider unreconciled-balance derivation must apply '
        'the open-items rule via is_open_for_rec()'
    )


def test_o3_match_statement_endpoint_uses_open_items_rule():
    """The Opera 3 /api/opera3/bank-reconciliation/match-statement endpoint
    in routes.py must apply is_open_for_rec() when building its candidate
    pool from aentry."""
    src = open(
        '/Users/maccb/llmragsql/apps/bank_reconcile/api/routes.py',
        'r',
        encoding='utf-8',
    ).read()
    # Find the Opera 3 match-statement candidate-fetch block — characterised
    # by the for-row in aentry_records loop after Opera3Reader.read_table.
    idx = src.find("aentry_records = reader.read_table('aentry')")
    assert idx > 0, 'Opera 3 match-statement candidate fetch not found'
    snippet = src[idx:idx + 1500]
    assert 'is_open_for_rec' in snippet, (
        'Opera 3 match-statement endpoint must call is_open_for_rec() on '
        'each aentry row before considering it a match candidate.'
    )


def test_central_kb_matcher_period_bound_documents_ae_remove():
    """The central KB business-rule doc for matcher-period-bound must
    document the ae_remove=0 predicate (cross-check 2026-05-05 audit
    finding)."""
    import os
    path = os.path.expanduser(
        '~/opera-knowledge-ref/packages/opera-knowledge/business-rules/'
        'matcher-period-bound.md'
    )
    if not os.path.exists(path):
        # If the central KB isn't checked out alongside, skip — the local
        # repo's tests don't depend on it being present.
        import pytest
        pytest.skip('central KB not present at expected path')
    text = open(path, 'r', encoding='utf-8').read()
    assert 'ae_remove = 0' in text, (
        'matcher-period-bound.md candidate_pool snippet must include '
        'ae_remove = 0 alongside ae_reclnum = 0.'
    )
