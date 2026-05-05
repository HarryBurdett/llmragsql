"""Pin that the SQL-input validators are wired into the highest-risk
Opera-writing route handlers.

Audit cross-cutting F5: ~250 f-string SQL sites in route handlers.
The boundary-level validator (sql_rag/sql_input_validator.py) is the
defence — these tests source-inspect the critical handlers to ensure
they call the validator before any SQL is built.

Future deep-parameterisation (a separate hardening sprint) will
remove the f-string usage entirely; until then these tests pin the
input gate as the security guarantee.
"""
import inspect


def test_unreconcile_validates_bank_code_and_entries():
    from apps.bank_reconcile.api import routes
    src = inspect.getsource(routes.unreconcile_entries)
    assert 'validate_bank_code(bank_code)' in src
    assert 'validate_entry_number' in src


def test_mark_entries_reconciled_validates_inputs():
    from apps.bank_reconcile.api import routes
    src = inspect.getsource(routes.mark_entries_reconciled)
    assert 'validate_bank_code(bank_code)' in src


def test_complete_reconciliation_validates_inputs():
    from apps.bank_reconcile.api import routes
    src = inspect.getsource(routes.complete_reconciliation)
    assert 'validate_bank_code(bank_code)' in src
    assert 'validate_entry_number' in src


def test_opera3_complete_reconciliation_validates_inputs():
    from apps.bank_reconcile.api import routes
    src = inspect.getsource(routes.opera3_complete_reconciliation)
    assert 'validate_bank_code(bank_code)' in src


def test_se_gocardless_import_validates_inputs():
    from apps.gocardless.api import routes
    src = inspect.getsource(routes.import_gocardless_batch)
    assert 'validate_bank_code(bank_code)' in src


def test_opera3_gocardless_import_validates_inputs():
    from apps.gocardless.api import routes
    src = inspect.getsource(routes.opera3_import_gocardless_batch)
    assert 'validate_bank_code(bank_code)' in src
