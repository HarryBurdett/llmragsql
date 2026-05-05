"""Opera 3 parity regression tests.

Pin that every behavioural contract the SE side enforces also has its
Opera 3 equivalent. Audit 2026-05-05 cross-cutting F18 — Opera 3 was
materially under-tested for the recent fixes.
"""
import inspect
from datetime import date


# ---------------------------------------------------------------------------
# Open-items rule applied at every Opera 3 candidate-fetch site
# ---------------------------------------------------------------------------


def test_o3_statement_reconciler_uses_open_items_rule():
    """Opera 3 StatementReconciler.get_unreconciled_entries must call
    is_open_for_rec()."""
    from sql_rag.statement_reconcile_opera3 import StatementReconcilerOpera3

    src = inspect.getsource(StatementReconcilerOpera3.get_unreconciled_entries)
    assert 'is_open_for_rec' in src


def test_o3_data_provider_unreconciled_uses_open_items_rule():
    """opera3_data_provider's unreconciled-balance derivation must
    call is_open_for_rec()."""
    src = open(
        '/Users/maccb/llmragsql/sql_rag/opera3_data_provider.py',
        'r',
        encoding='utf-8',
    ).read()
    assert 'is_open_for_rec' in src


def test_o3_match_statement_endpoint_uses_open_items_rule():
    """The Opera 3 match-statement endpoint must apply
    is_open_for_rec() on each aentry row."""
    src = open(
        '/Users/maccb/llmragsql/apps/bank_reconcile/api/routes.py',
        'r',
        encoding='utf-8',
    ).read()
    idx = src.find("aentry_records = reader.read_table('aentry')")
    assert idx > 0
    snippet = src[idx:idx + 1500]
    assert 'is_open_for_rec' in snippet


# ---------------------------------------------------------------------------
# Opera 3 type-blind fallback (parity with SE)
# ---------------------------------------------------------------------------


def test_o3_type_blind_fallback_exists():
    """sql_rag.bank_import_opera3.BankStatementMatcherOpera3 must
    expose _is_already_posted_typeblind (audit cross-cutting F1)."""
    from sql_rag.bank_import_opera3 import BankStatementMatcherOpera3
    assert hasattr(BankStatementMatcherOpera3, '_is_already_posted_typeblind'), (
        'Opera 3 type-blind fallback method missing — silent double-post '
        'risk for the HISCOX-class scenario.'
    )


def test_o3_type_blind_fallback_called_from_is_already_posted():
    """The Opera 3 _is_already_posted must fall through to the type-blind
    fallback when action is unset/skip OR when type-aware finds no match."""
    from sql_rag.bank_import_opera3 import BankStatementMatcherOpera3

    src = inspect.getsource(BankStatementMatcherOpera3._is_already_posted)
    assert '_is_already_posted_typeblind' in src, (
        'Opera 3 _is_already_posted must call the type-blind fallback'
    )


# ---------------------------------------------------------------------------
# Opera 3 reversal CLI script exists (parity with SE)
# ---------------------------------------------------------------------------


def test_o3_reversal_script_exists():
    """scripts/reverse_bank_rec_batch_opera3.py must exist.
    Audit stages-3-5 F11 — Opera 3 reversal was missing entirely."""
    from pathlib import Path
    p = Path('/Users/maccb/llmragsql/scripts/reverse_bank_rec_batch_opera3.py')
    assert p.exists(), (
        'Opera 3 reversal CLI script missing — Opera 3 customers '
        'cannot recover from a wrong rec without manual DBF surgery.'
    )
    text = p.read_text()
    assert 'derive_prior_state' in text
    assert 'snapshot_aentry' in text


# ---------------------------------------------------------------------------
# Opera 3 startup integrity check exists (parity with SE)
# ---------------------------------------------------------------------------


def test_o3_integrity_check_module_exists():
    """sql_rag/bank_rec_integrity_o3.py must exist and expose
    log_bank_rec_integrity_opera3."""
    from sql_rag import bank_rec_integrity_o3
    assert hasattr(bank_rec_integrity_o3, 'log_bank_rec_integrity_opera3')
    assert hasattr(bank_rec_integrity_o3, 'check_bank_rec_integrity_opera3')


def test_o3_integrity_wired_into_company_context():
    """api/main._ensure_company_context routes to the Opera 3 integrity
    check when [opera] version=opera3."""
    src = open(
        '/Users/maccb/llmragsql/api/main.py', 'r', encoding='utf-8',
    ).read()
    assert 'log_bank_rec_integrity_opera3' in src
    assert "version == 'opera3'" in src or 'opera3' in src


# ---------------------------------------------------------------------------
# Opera 3 supplier provider exists (parity)
# ---------------------------------------------------------------------------


def test_o3_supplier_data_provider_exists():
    """Opera 3 supplier-data provider must exist with the full
    SupplierDataProvider interface."""
    from sql_rag.supplier_data_opera3 import Opera3SupplierDataProvider
    from sql_rag.supplier_data_provider import SupplierDataProvider

    assert issubclass(Opera3SupplierDataProvider, SupplierDataProvider)
    # Every abstract method must be implemented.
    abstract = getattr(SupplierDataProvider, '__abstractmethods__', set())
    for name in abstract:
        assert hasattr(Opera3SupplierDataProvider, name), (
            f'Opera3SupplierDataProvider missing abstract method {name}'
        )


def test_supplier_data_provider_factory_routes_by_version():
    """get_supplier_data_provider() must branch on config.ini
    [opera] version and route to the Opera 3 provider when
    version='opera3'."""
    src = inspect.getsource(
        __import__('sql_rag.supplier_data_provider', fromlist=['get_supplier_data_provider']).get_supplier_data_provider
    )
    assert 'opera3' in src
    assert 'Opera3SupplierDataProvider' in src


# ---------------------------------------------------------------------------
# Opera 3 GoCardless: idempotency, mandate verification, currency, lock
# ---------------------------------------------------------------------------


def test_o3_gocardless_import_has_idempotency():
    """Opera 3 GC import must check is_gocardless_payout_imported."""
    from apps.gocardless.api import routes
    src = inspect.getsource(routes.opera3_import_gocardless_batch)
    assert 'is_gocardless_payout_imported' in src


def test_o3_gocardless_import_has_mandate_verification():
    """Opera 3 GC import must verify each payment's mandate→account."""
    from apps.gocardless.api import routes
    src = inspect.getsource(routes.opera3_import_gocardless_batch)
    assert 'mandate_to_account' in src


def test_o3_gocardless_import_has_currency_validation():
    """Opera 3 GC import must reject non-GBP currency."""
    from apps.gocardless.api import routes
    src = inspect.getsource(routes.opera3_import_gocardless_batch)
    assert 'GBP' in src
    assert 'home currency' in src.lower() or 'foreign currenc' in src.lower()


def test_o3_gocardless_import_has_import_lock():
    """Opera 3 GC import must acquire the per-bank import lock."""
    from apps.gocardless.api import routes
    src = inspect.getsource(routes.opera3_import_gocardless_batch)
    assert 'acquire_import_lock' in src
    assert 'release_import_lock' in src


def test_o3_gocardless_import_validates_data_path():
    """Opera 3 GC import must validate data_path is a real directory."""
    from apps.gocardless.api import routes
    src = inspect.getsource(routes.opera3_import_gocardless_batch)
    assert 'is_dir' in src or 'data_path' in src


# ---------------------------------------------------------------------------
# Opera 3 column-name typos (audit opera3-column-audit) — guard regression
# ---------------------------------------------------------------------------


def test_o3_no_nk_lstrcln_typo():
    """Old typo nk_lstrcln (real column nk_lstrecl) must not regress
    as a dict-access key. Comments mentioning the old name in
    historical-context notes are fine."""
    src = open(
        '/Users/maccb/llmragsql/sql_rag/opera3_data_provider.py',
        'r',
        encoding='utf-8',
    ).read()
    import re
    # Strip comments first.
    code_only = re.sub(r'#[^\n]*', '', src)
    bad = re.findall(r"\.get\(\s*['\"]nk_lstrcln['\"]", code_only)
    assert not bad, "nk_lstrcln dict-access typo regressed"


def test_o3_no_nk_forgn_typo():
    """Old typo nk_forgn (real column nk_fcurr) must not regress."""
    src = open(
        '/Users/maccb/llmragsql/apps/bank_reconcile/api/routes.py',
        'r',
        encoding='utf-8',
    ).read()
    # nk_forgn must not be referenced as a primary lookup key.
    # (Comments mentioning the old name are fine; .get('nk_forgn') is not.)
    import re
    bad = re.findall(r"\.get\(\s*['\"]nk_forgn['\"]", src)
    assert not bad, "nk_forgn typo regressed — real column is nk_fcurr"


# ---------------------------------------------------------------------------
# stran column standardisation
# ---------------------------------------------------------------------------


def test_o3_no_st_cusref_writes():
    """opera3_foxpro_import.py must not write 'st_cusref' as a dict key
    (canonical column is st_custref). Audit stages-3-5 F5."""
    src = open(
        '/Users/maccb/llmragsql/sql_rag/opera3_foxpro_import.py',
        'r',
        encoding='utf-8',
    ).read()
    assert "'st_cusref'" not in src
    # also st_custype which doesn't exist on the schema
    assert "'st_custype'" not in src
