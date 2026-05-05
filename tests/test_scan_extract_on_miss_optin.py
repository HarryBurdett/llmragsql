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

These tests source-inspect the four handlers to pin:
  1. The query parameter exists and defaults to True.
  2. The cache-miss block respects the flag (`elif not extract_on_miss`).
"""
import inspect


def _src(handler):
    return inspect.getsource(handler)


def test_scan_folder_has_extract_on_miss_flag():
    from apps.bank_reconcile.api import routes
    src = _src(routes.scan_folder_for_bank_statements)
    assert 'extract_on_miss' in src
    assert 'extract_on_miss: bool = Query(True' in src
    assert 'elif not extract_on_miss' in src


def test_scan_emails_has_extract_on_miss_flag():
    from apps.bank_reconcile.api import routes
    src = _src(routes.scan_emails_for_bank_statements)
    assert 'extract_on_miss' in src
    assert 'extract_on_miss: bool = Query(True' in src
    assert 'elif not extract_on_miss' in src


def test_scan_all_banks_has_extract_on_miss_flag():
    from apps.bank_reconcile.api import routes
    src = _src(routes.scan_all_banks_for_statements)
    assert 'extract_on_miss' in src
    assert 'extract_on_miss: bool = Query(True' in src
    assert 'elif not extract_on_miss' in src


def test_opera3_scan_emails_has_extract_on_miss_flag():
    from apps.bank_reconcile.api import routes
    src = _src(routes.opera3_scan_emails_for_bank_statements)
    assert 'extract_on_miss' in src
    assert 'extract_on_miss: bool = Query(True' in src
    assert 'elif not extract_on_miss' in src


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
