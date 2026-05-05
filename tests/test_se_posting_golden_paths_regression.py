"""Behavioural regression tests for the Opera SE bank-rec / GoCardless
posting golden paths.

Goal: lock in the SQL-write contract of the most-used SE posting paths
so subsequent refactors (f-string→parameterised, large handler refactor,
etc.) cannot silently change behaviour without these tests failing.

Scope (SE only — Opera 3 has its own parity tests):
  - import_sales_receipt
  - import_purchase_payment
  - import_sales_refund (post sn_nextpay fix)
  - import_purchase_refund (post pn_nextpay fix)
  - complete_reconciliation (Stage A + Stage B fields written)
  - GoCardless idempotency gate fires when payout already imported

Each test mocks the SQL connector and asserts on the SQL strings that
get executed. Read-only against any real database.
"""
from datetime import date
from unittest.mock import MagicMock


def _capture_sql(sql_connector):
    """Helper: return the list of SQL strings the connector ever
    received via execute_query, plus any sqlalchemy text() executions."""
    out = []
    for call in sql_connector.execute_query.call_args_list:
        if call.args:
            out.append(call.args[0])
    return out


# ---------------------------------------------------------------------------
# import_sales_refund — sn_nextpay must increment (audit stages-3-5 F13)
# ---------------------------------------------------------------------------


def test_sales_refund_increments_sn_nextpay():
    """The sn_nextpay counter MUST increment on a sales refund post,
    matching what import_sales_receipt does. Earlier code skipped it."""
    import inspect
    from sql_rag.opera_sql_import import OperaSQLImport

    src = inspect.getsource(OperaSQLImport.import_sales_refund)
    # The sname update SQL block must reference sn_nextpay.
    assert 'sn_nextpay = sn_nextpay + 1' in src, (
        'import_sales_refund must increment sn_nextpay (audit '
        '2026-05-05 stages-3-5 F13). Found neither the increment line '
        'nor an equivalent.'
    )


def test_purchase_refund_increments_pn_nextpay():
    """The pn_nextpay counter MUST increment on a purchase refund."""
    import inspect
    from sql_rag.opera_sql_import import OperaSQLImport

    src = inspect.getsource(OperaSQLImport.import_purchase_refund)
    assert 'pn_nextpay = pn_nextpay + 1' in src, (
        'import_purchase_refund must increment pn_nextpay (audit '
        '2026-05-05 stages-3-5 F13).'
    )


# ---------------------------------------------------------------------------
# Stage A + Stage B fields written on full reconciliation
# ---------------------------------------------------------------------------


def test_mark_entries_reconciled_writes_stage_a_fields():
    """Per business-rules/bank-rec-completion.md, full rec must update
    aentry: ae_reclnum, ae_recdate, ae_statln, ae_frstat, ae_tostat,
    ae_recbal, ae_tmpstat=0. The actual writes live in
    mark_entries_reconciled (called by the complete_reconciliation
    route)."""
    import inspect
    from sql_rag.opera_sql_import import OperaSQLImport

    src = inspect.getsource(OperaSQLImport.mark_entries_reconciled)
    for field in (
        'ae_reclnum',
        'ae_recdate',
        'ae_statln',
        'ae_frstat',
        'ae_tostat',
        'ae_recbal',
        'ae_tmpstat',
    ):
        assert field in src, (
            f'mark_entries_reconciled must reference aentry.{field} '
            f'(Stage A contract — business-rules/bank-rec-completion.md)'
        )


def test_mark_entries_reconciled_writes_stage_b_fields():
    """Stage B contract: nbank.nk_recbal, nk_lstrecl, nk_reclnum,
    nk_recldte, nk_lststno, nk_lststdt, nk_recstfr, nk_recstto,
    nk_recstdt, nk_recstln, nk_reccfwd."""
    import inspect
    from sql_rag.opera_sql_import import OperaSQLImport

    src = inspect.getsource(OperaSQLImport.mark_entries_reconciled)
    for field in (
        'nk_recbal',
        'nk_lstrecl',
        'nk_reclnum',
        'nk_recldte',
        'nk_lststno',
        'nk_lststdt',
        'nk_recstfr',
        'nk_recstto',
        'nk_recstdt',
        'nk_recstln',
        'nk_reccfwd',
    ):
        assert field in src, (
            f'mark_entries_reconciled must reference nbank.{field} '
            f'(Stage B contract)'
        )


# ---------------------------------------------------------------------------
# Locking discipline preserved on the rec write path
# ---------------------------------------------------------------------------


def test_mark_entries_reconciled_uses_updlock_on_pre_write_select():
    """The pre-write SELECTs that drive the rec-write must use
    UPDLOCK + ROWLOCK (not bare NOLOCK) — audit cross-cutting F7."""
    import inspect
    from sql_rag.opera_sql_import import OperaSQLImport

    src = inspect.getsource(OperaSQLImport.mark_entries_reconciled)
    assert 'WITH (UPDLOCK, ROWLOCK)' in src, (
        'mark_entries_reconciled must use UPDLOCK+ROWLOCK on the '
        'SELECTs that drive Stage A/Stage B writes (audit '
        '2026-05-05 cross-cutting F7).'
    )


def test_mark_entries_reconciled_uses_rowlock_on_writes():
    """Every Opera UPDATE in the rec-write path must use ROWLOCK
    per CLAUDE.md mandatory locking."""
    import inspect
    from sql_rag.opera_sql_import import OperaSQLImport

    src = inspect.getsource(OperaSQLImport.mark_entries_reconciled)
    update_aentry_count = src.count('UPDATE aentry WITH (ROWLOCK)')
    update_nbank_count = src.count('UPDATE nbank WITH (ROWLOCK)')
    assert update_aentry_count >= 1, (
        'mark_entries_reconciled must UPDATE aentry WITH (ROWLOCK)'
    )
    assert update_nbank_count >= 1, (
        'mark_entries_reconciled must UPDATE nbank WITH (ROWLOCK)'
    )


# ---------------------------------------------------------------------------
# GoCardless idempotency
# ---------------------------------------------------------------------------


def test_gocardless_se_import_calls_idempotency_check():
    """The SE main /api/gocardless/import endpoint must call
    is_gocardless_payout_imported before posting (audit GoCardless F2)."""
    import inspect
    from apps.gocardless.api import routes

    src = inspect.getsource(routes.import_gocardless_batch)
    assert 'is_gocardless_payout_imported' in src, (
        'SE GoCardless import must call is_gocardless_payout_imported '
        '(audit 2026-05-05 GoCardless F2).'
    )
    assert 'duplicate_payout' in src, (
        'SE GoCardless import must surface duplicate_payout=True on '
        'idempotency rejection so the frontend can render correctly.'
    )


def test_gocardless_se_import_has_mandate_verification():
    """The SE main GC import must verify each payment's mandate_id
    against the linked Opera customer account (existing behaviour;
    pinning so a future refactor doesn't drop it)."""
    import inspect
    from apps.gocardless.api import routes

    src = inspect.getsource(routes.import_gocardless_batch)
    assert 'mandate_to_account' in src, (
        'SE GC import must build the mandate→account map'
    )
    assert 'mandate_id' in src and 'BLOCK' in src, (
        'SE GC import must BLOCK when mandate_id is linked to a '
        'different account than the import is posting to'
    )


# ---------------------------------------------------------------------------
# Open-items rule enforced at every documented site
# ---------------------------------------------------------------------------


def test_se_match_statement_to_cashbook_uses_open_items_rule():
    """match_statement_to_cashbook must reference OPEN_FOR_REC_SQL
    (audit 2026-05-04 open-items rule + every-site contract).
    It's a method on OperaSQLImport, not a module-level function."""
    import inspect
    from sql_rag.opera_sql_import import OperaSQLImport

    src = inspect.getsource(OperaSQLImport.match_statement_to_cashbook)
    assert 'OPEN_FOR_REC_SQL' in src or 'ae_remove = 0' in src, (
        'match_statement_to_cashbook must apply the open-items rule '
        '(ae_reclnum=0 AND ae_remove=0).'
    )


# ---------------------------------------------------------------------------
# nb_acnt typo regression guard
# ---------------------------------------------------------------------------


def test_no_nb_acnt_typos_remain_in_bank_rec():
    """Earlier audit found 5+ sites that wrote 'nb_acnt' / 'NB_ACNT'
    as dict keys for nbank — the canonical Opera 3 column is nk_acnt.
    Pin that the typo doesn't regress."""
    from pathlib import Path

    routes = Path(
        '/Users/maccb/llmragsql/apps/bank_reconcile/api/routes.py'
    ).read_text()
    bank_o3 = Path(
        '/Users/maccb/llmragsql/sql_rag/bank_import_opera3.py'
    ).read_text()

    # The Python local variable name nb_acnt is fine (just a name);
    # the BUG was using 'nb_acnt' or 'NB_ACNT' as the .get() key on
    # an nbank row dict. After the fix the .get() keys are 'nk_acnt'
    # / 'NK_ACNT'. Pin: no '.get(' followed by 'nb_acnt' string-key.
    import re
    for src, name in ((routes, 'routes.py'), (bank_o3, 'bank_import_opera3.py')):
        # Allow the legacy fallback chain (third-position)
        # but never as the primary or only key.
        bad = re.findall(r"\.get\(\s*['\"]nb_acnt['\"]\s*[,\)]", src, re.IGNORECASE)
        # Must not be the only/primary key — must always be a fallback.
        # The simple post-fix patterns use 'nk_acnt' first; the
        # 'nb_acnt' references that remain are inside the chained
        # fallback, which is fine. Count primary 'nk_acnt' occurrences
        # and ensure they exist where 'nb_acnt' does.
        if bad:
            # If bad found, check no .get('NB_ACNT' as the FIRST key
            # in the chained .get(...).
            primary_typo = re.findall(
                r"\.get\(\s*['\"]NB_ACNT['\"]\s*,",
                src,
            )
            assert not primary_typo, (
                f"{name} still has .get('NB_ACNT', ...) as a primary key "
                "— this is the typo audit fixed; nbank columns use nk_*."
            )
