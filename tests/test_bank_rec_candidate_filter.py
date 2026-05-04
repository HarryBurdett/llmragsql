"""Contract test: every site that fetches Opera atran/aentry candidates
for bank-rec matching MUST apply the open-items filter.

If a future code change adds a new candidate-fetcher without the filter,
this test fails loudly. Add the new function to FETCHERS and ensure its
source contains both ae_reclnum and ae_remove references.
"""

import inspect
import re

import sql_rag.bank_import as _bank_import
import sql_rag.duplicate_check_se as _se
import sql_rag.duplicate_check_o3 as _o3
import sql_rag.opera_sql_import as _opera


# (function, kind, label) — kind is 'sql' or 'inmem'
FETCHERS = [
    (_bank_import.BankStatementImport._is_already_posted_typeblind,
     'sql', 'bank_import._is_already_posted_typeblind'),
    (_se.OperaSEDataSource.find_aentry_by_signed_value,
     'sql', 'duplicate_check_se.OperaSEDataSource.find_aentry_by_signed_value'),
    (_opera.OperaSQLImport.match_statement_to_cashbook,
     'sql', 'opera_sql_import.OperaSQLImport.match_statement_to_cashbook'),
    (_o3.Opera3DataSource.find_aentry_by_signed_value,
     'inmem', 'duplicate_check_o3.Opera3DataSource.find_aentry_by_signed_value'),
]


def _strip_docstring(src: str) -> str:
    """Strip the leading triple-quoted docstring after the def line so
    historical-bug notes in docstrings don't trigger false positives."""
    return re.sub(
        r'(def [^\n]*?:\s*\n\s*)""".*?"""',
        r'\1',
        src,
        count=1,
        flags=re.DOTALL,
    )


def test_every_sql_fetcher_applies_open_items_rule():
    for fn, kind, label in FETCHERS:
        if kind != 'sql':
            continue
        src = _strip_docstring(inspect.getsource(fn))
        assert 'ae_reclnum = 0' in src or 'OPEN_FOR_REC_SQL' in src, (
            f"{label} must apply ae_reclnum=0 (use OPEN_FOR_REC_SQL)"
        )
        assert 'ae_remove = 0' in src or 'OPEN_FOR_REC_SQL' in src, (
            f"{label} must apply ae_remove=0 (use OPEN_FOR_REC_SQL)"
        )


def test_every_inmem_fetcher_uses_is_open_for_rec():
    for fn, kind, label in FETCHERS:
        if kind != 'inmem':
            continue
        src = _strip_docstring(inspect.getsource(fn))
        assert 'is_open_for_rec' in src, (
            f"{label} must call is_open_for_rec from opera_open_items"
        )


def test_open_items_module_is_imported_at_call_sites():
    """All four fetchers should reference opera_open_items somehow,
    either via import or via the SQL fragment."""
    for fn, _kind, label in FETCHERS:
        src = inspect.getsource(fn)
        has_import = (
            'opera_open_items' in src
            or 'OPEN_FOR_REC_SQL' in src
            or 'is_open_for_rec' in src
        )
        assert has_import, f"{label} must reference sql_rag.opera_open_items"
