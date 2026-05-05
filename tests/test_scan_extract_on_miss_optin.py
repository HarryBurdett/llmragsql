"""Pin the F8 audit fix: scan endpoints take an opt-in extract_on_miss
flag.

Audit cross-cutting F8: the four scan endpoints
  GET /api/bank-import/scan-folder
  GET /api/bank-import/scan-emails
  GET /api/bank-import/scan-all-banks
  GET /api/opera3/bank-import/scan-emails
each call StatementReconciler.extract_transactions_from_pdf inline on
a cache miss. That blocks the HTTP response on a Gemini round-trip
per PDF — a 30-statement scan can hang for minutes.

The fix is an opt-in `extract_on_miss=False` query parameter (default
True so the tested SE behaviour is preserved). When set to False, the
cache-miss block marks the statement `pending_extraction` instead of
calling Gemini synchronously.

After the F9 refactor the cache-miss handling moved into the shared
helper apps.bank_reconcile.logic.scan_pdf_validation.get_statement_info.
The handlers pass extract_on_miss through to validate_pdf_for_scan.

These tests source-inspect:
  - The handlers expose the query parameter with default True
  - The handlers thread `extract_on_miss=extract_on_miss` into the
    helper (or, for scan_folder which still inlines, retain the
    `elif not extract_on_miss` branch)
  - The helper itself contains the deferral logic
"""
import inspect


def _src(handler):
    return inspect.getsource(handler)


def _src_threads_flag_to_helper_or_inline(src):
    """Either the handler delegates to the F9 helper threading the
    flag through, or it still has the inline `elif not extract_on_miss`
    branch (scan_folder kept inline for now)."""
    return (
        'extract_on_miss=extract_on_miss' in src
        or 'elif not extract_on_miss' in src
    )


def test_scan_folder_has_extract_on_miss_flag():
    from apps.bank_reconcile.api import routes
    src = _src(routes.scan_folder_for_bank_statements)
    assert 'extract_on_miss' in src
    assert 'extract_on_miss: bool = Query(True' in src
    assert _src_threads_flag_to_helper_or_inline(src)


def test_scan_emails_has_extract_on_miss_flag():
    from apps.bank_reconcile.api import routes
    src = _src(routes.scan_emails_for_bank_statements)
    assert 'extract_on_miss' in src
    assert 'extract_on_miss: bool = Query(True' in src
    assert _src_threads_flag_to_helper_or_inline(src)


def test_scan_all_banks_has_extract_on_miss_flag():
    from apps.bank_reconcile.api import routes
    src = _src(routes.scan_all_banks_for_statements)
    assert 'extract_on_miss' in src
    assert 'extract_on_miss: bool = Query(True' in src
    assert _src_threads_flag_to_helper_or_inline(src)


def test_opera3_scan_emails_has_extract_on_miss_flag():
    from apps.bank_reconcile.api import routes
    src = _src(routes.opera3_scan_emails_for_bank_statements)
    assert 'extract_on_miss' in src
    assert 'extract_on_miss: bool = Query(True' in src
    assert _src_threads_flag_to_helper_or_inline(src)


def test_helper_module_implements_deferral_logic():
    """The F9 helper that the handlers delegate to MUST honour
    extract_on_miss=False by returning pending_extraction without
    calling Gemini."""
    from apps.bank_reconcile.logic.scan_pdf_validation import get_statement_info
    src = inspect.getsource(get_statement_info)
    assert 'extract_on_miss' in src
    assert "'pending_extraction'" in src


def test_default_preserves_inline_extraction_for_se_compat():
    """The user explicitly flagged that the SE bank-rec routine is fully
    tested and must not regress. The fix MUST default to True (i.e.
    inline extraction stays the default) so existing callers see
    identical behaviour."""
    from apps.bank_reconcile.api import routes
    for h in (
        routes.scan_folder_for_bank_statements,
        routes.scan_emails_for_bank_statements,
        routes.scan_all_banks_for_statements,
        routes.opera3_scan_emails_for_bank_statements,
    ):
        sig = inspect.signature(h)
        param = sig.parameters['extract_on_miss']
        # Default is a fastapi.Query(True, ...). The Query object's
        # `.default` is True.
        default_val = getattr(param.default, 'default', param.default)
        assert default_val is True, (
            f"{h.__name__}: extract_on_miss must default to True to preserve SE behaviour"
        )
