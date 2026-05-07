"""Tests for the Phase B ports and local adapters.

Pin the contracts:
  - Each port is a runtime-checkable Protocol
  - Each local adapter satisfies its port
  - The factory returns the right adapter for the env config
  - to_records() normalises results across adapter shapes
  - Adapters degrade gracefully when state is unset (test env)
"""
from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

from apps.core.adapters.factory import (
    get_auth,
    get_email_storage,
    get_email_sync,
    get_opera3_reader,
    get_opera3_writer,
    get_opera_sql,
    get_smtp,
    list_adapter_selection,
)
from apps.core.ports import (
    AuthPort,
    EmailStoragePort,
    EmailSyncPort,
    Opera3ReaderPort,
    Opera3WriterPort,
    OperaSQLPort,
    SMTPPort,
    to_records,
)


# =====================================================================
# Port satisfaction (runtime-check)
# =====================================================================


def test_local_opera_sql_satisfies_port():
    adapter = get_opera_sql()
    assert isinstance(adapter, OperaSQLPort)


def test_local_email_storage_satisfies_port():
    adapter = get_email_storage()
    assert isinstance(adapter, EmailStoragePort)


def test_local_opera3_reader_satisfies_port():
    adapter = get_opera3_reader()
    assert isinstance(adapter, Opera3ReaderPort)


def test_local_opera3_writer_satisfies_port():
    adapter = get_opera3_writer()
    assert isinstance(adapter, Opera3WriterPort)


def test_local_email_sync_satisfies_port():
    adapter = get_email_sync()
    assert isinstance(adapter, EmailSyncPort)


def test_local_smtp_satisfies_port():
    adapter = get_smtp()
    assert isinstance(adapter, SMTPPort)


def test_local_auth_satisfies_port():
    adapter = get_auth()
    assert isinstance(adapter, AuthPort)


def test_local_company_context_satisfies_port():
    from apps.core.adapters.factory import get_company_context
    from apps.core.ports import CompanyContextPort
    adapter = get_company_context()
    assert isinstance(adapter, CompanyContextPort)


def test_local_company_context_returns_none_when_unset(monkeypatch):
    """No company context active → all getters return None."""
    import api.main  # noqa: F401
    from apps.core.adapters.factory import get_company_context
    with patch('apps.core.state.current_company', None):
        with patch('apps.core.state.active_system_id', None):
            with patch('api.main.current_company', None):
                with patch('api.main.active_system_id', None):
                    adapter = get_company_context()
                    assert adapter.get_company() is None
                    assert adapter.get_active_system_id() is None


# =====================================================================
# Factory selection
# =====================================================================


def test_factory_default_returns_local_adapters():
    """No env vars set → all ports return Local* adapters."""
    selection = list_adapter_selection()
    assert selection['opera_sql'] == 'LocalOperaSQLAdapter'
    assert selection['email_storage'] == 'LocalEmailStorageAdapter'
    assert selection['opera3_reader'] == 'LocalOpera3ReaderAdapter'
    assert selection['opera3_writer'] == 'LocalOpera3WriterAdapter'
    assert selection['email_sync'] == 'LocalEmailSyncAdapter'
    assert selection['smtp'] == 'LocalSMTPAdapter'
    assert selection['auth'] == 'LocalAuthAdapter'


def test_factory_writer_always_local_per_directive():
    """Per the SAM-readiness brief: 'the write agent stays as is.'
    There is no SAM/HTTP-alternative adapter for Opera3Writer —
    it's always the local HTTP-to-Windows-agent client."""
    with patch.dict(os.environ, {'SAM_ENABLED': 'true', 'CORE_OPERA3_URL': 'http://x'}):
        adapter = get_opera3_writer()
        assert type(adapter).__name__ == 'LocalOpera3WriterAdapter'


# =====================================================================
# OperaSQL adapter behaviour
# =====================================================================


def test_local_opera_sql_raises_when_no_connector():
    """If neither apps.core.state nor api.main has a connector,
    execute_query raises with a clear error."""
    import api.main  # noqa: F401  ensure module is imported before patch
    from apps.core.adapters.local.opera_sql import LocalOperaSQLAdapter
    with patch('apps.core.state.sql_connector', None):
        with patch('api.main.sql_connector', None):
            adapter = LocalOperaSQLAdapter()
            with pytest.raises(RuntimeError, match='No SQL connector available'):
                adapter.execute_query("SELECT 1")


def test_local_opera_sql_uses_state_connector_when_present():
    """state.sql_connector takes priority over api.main.sql_connector."""
    from apps.core.adapters.local.opera_sql import LocalOperaSQLAdapter

    fake_connector = MagicMock()
    fake_connector.execute_query.return_value = "result"

    with patch('apps.core.state.sql_connector', fake_connector):
        adapter = LocalOperaSQLAdapter()
        result = adapter.execute_query("SELECT 1", {'k': 'v'})
        assert result == "result"
        fake_connector.execute_query.assert_called_once_with("SELECT 1", {'k': 'v'})


def test_local_opera_sql_falls_back_to_api_main():
    """When state has no connector, fall back to api.main."""
    import api.main  # noqa: F401
    from apps.core.adapters.local.opera_sql import LocalOperaSQLAdapter
    fake = MagicMock()
    fake.execute_query.return_value = "from_main"
    with patch('apps.core.state.sql_connector', None):
        with patch('api.main.sql_connector', fake):
            adapter = LocalOperaSQLAdapter()
            result = adapter.execute_query("SELECT 1")
            assert result == "from_main"


def test_local_opera_sql_bool_is_false_when_no_connector():
    """bool(adapter) preserves the `if not sql_connector:` pattern
    that route handlers use to fail fast."""
    import api.main  # noqa: F401
    from apps.core.adapters.local.opera_sql import LocalOperaSQLAdapter
    with patch('apps.core.state.sql_connector', None):
        with patch('api.main.sql_connector', None):
            adapter = LocalOperaSQLAdapter()
            assert bool(adapter) is False
            assert not adapter   # idiomatic check used in route handlers


def test_local_opera_sql_bool_is_true_when_connector_present():
    from apps.core.adapters.local.opera_sql import LocalOperaSQLAdapter
    fake = MagicMock()
    with patch('apps.core.state.sql_connector', fake):
        adapter = LocalOperaSQLAdapter()
        assert bool(adapter) is True


def test_local_email_storage_bool_reflects_underlying_storage():
    import api.main  # noqa: F401
    from apps.core.adapters.local.email_storage import LocalEmailStorageAdapter
    fake = MagicMock()
    with patch('apps.core.state.email_storage', fake):
        adapter = LocalEmailStorageAdapter()
        assert bool(adapter) is True
    with patch('apps.core.state.email_storage', None):
        with patch('api.main.email_storage', None):
            adapter = LocalEmailStorageAdapter()
            assert bool(adapter) is False


# =====================================================================
# to_records normalisation
# =====================================================================


def test_to_records_handles_none():
    assert to_records(None) == []


def test_to_records_handles_list_of_dicts():
    rows = [{'a': 1}, {'a': 2}]
    assert to_records(rows) == rows


def test_to_records_handles_dataframe_like():
    """Anything with .to_dict('records') is treated as a DataFrame."""
    df = MagicMock()
    df.to_dict.return_value = [{'a': 1, 'b': 2}]
    result = to_records(df)
    df.to_dict.assert_called_once_with('records')
    assert result == [{'a': 1, 'b': 2}]


# =====================================================================
# EmailStorage adapter behaviour
# =====================================================================


def test_local_email_storage_returns_empty_when_no_storage():
    """Graceful degrade — no storage means empty results, not crash."""
    import api.main  # noqa: F401
    from apps.core.adapters.local.email_storage import LocalEmailStorageAdapter
    with patch('apps.core.state.email_storage', None):
        with patch('api.main.email_storage', None):
            adapter = LocalEmailStorageAdapter()
            assert adapter.get_emails() == {'emails': [], 'total': 0}
            assert adapter.get_email_by_id(1) is None
            assert adapter.get_reconciled_filenames() == set()


def test_local_email_storage_forwards_to_state():
    """When state.email_storage is set, calls forward to it."""
    from apps.core.adapters.local.email_storage import LocalEmailStorageAdapter

    fake_storage = MagicMock()
    fake_storage.get_email_by_id.return_value = {'id': 42}

    with patch('apps.core.state.email_storage', fake_storage):
        adapter = LocalEmailStorageAdapter()
        result = adapter.get_email_by_id(42)
        assert result == {'id': 42}
        fake_storage.get_email_by_id.assert_called_once_with(42)


def test_local_email_storage_record_audit_row():
    """record_bank_statement_import forwards every kwarg."""
    from apps.core.adapters.local.email_storage import LocalEmailStorageAdapter

    fake = MagicMock()
    fake.record_bank_statement_import.return_value = 99

    with patch('apps.core.state.email_storage', fake):
        adapter = LocalEmailStorageAdapter()
        result = adapter.record_bank_statement_import(
            bank_code='BC010',
            filename='stmt.pdf',
            transactions_imported=5,
            source='email',
            target_system='opera_se',
            email_id=1,
            attachment_id='att1',
            total_receipts=100.0,
            total_payments=50.0,
            imported_by='harry',
        )
        assert result == 99
        fake.record_bank_statement_import.assert_called_once()
        kwargs = fake.record_bank_statement_import.call_args.kwargs
        assert kwargs['bank_code'] == 'BC010'
        assert kwargs['transactions_imported'] == 5


# =====================================================================
# SMTP adapter
# =====================================================================


def test_local_smtp_rejects_empty_body():
    from apps.core.adapters.local.smtp import LocalSMTPAdapter
    adapter = LocalSMTPAdapter(server='smtp.example.com')
    result = adapter.send(to_address='x@y.com', subject='test')
    assert result['success'] is False
    assert 'body' in result['error']


def test_local_smtp_rejects_no_server(monkeypatch):
    """Explicitly clear EMAIL_SMTP_SERVER — other tests may have set
    it via the env-config loader's config.ini fallback."""
    monkeypatch.delenv('EMAIL_SMTP_SERVER', raising=False)
    from apps.core.adapters.local.smtp import LocalSMTPAdapter
    adapter = LocalSMTPAdapter(server='')
    result = adapter.send(
        to_address='x@y.com', subject='test', body_plain='hi',
    )
    assert result['success'] is False
    assert 'EMAIL_SMTP_SERVER' in result['error']


# =====================================================================
# Opera3Writer adapter
# =====================================================================


def test_local_opera3_writer_is_available_handles_import_error():
    """If sql_rag.opera3_write_provider isn't available, return False."""
    from apps.core.adapters.local.opera3_writer import LocalOpera3WriterAdapter
    adapter = LocalOpera3WriterAdapter()
    # Whether available or not, this should never raise
    result = adapter.is_available()
    assert isinstance(result, bool)


# =====================================================================
# Opera3Reader adapter
# =====================================================================


def test_local_opera3_reader_no_data_path_raises_clearly():
    from apps.core.adapters.local.opera3_reader import LocalOpera3ReaderAdapter
    adapter = LocalOpera3ReaderAdapter(data_path='')
    with patch.dict(os.environ, {'OPERA3_DATA_PATH': ''}, clear=False):
        # Force re-read of env var
        adapter._data_path = ''
        with pytest.raises(RuntimeError, match='OPERA3_DATA_PATH'):
            adapter.read_table('nbank')


# =====================================================================
# list_adapter_selection
# =====================================================================


def test_list_adapter_selection_includes_all_ports():
    selection = list_adapter_selection()
    expected_ports = {
        'opera_sql', 'email_storage', 'opera3_reader',
        'opera3_writer', 'email_sync', 'smtp', 'auth',
        'company_context',
    }
    assert set(selection.keys()) == expected_ports
    # All values are non-empty strings (adapter class names)
    for port, adapter in selection.items():
        assert isinstance(adapter, str)
        assert adapter, f"Empty adapter for port {port}"


def test_factory_modules_are_lazy_imports():
    """The factory imports adapter modules lazily so a missing
    optional dependency in one adapter doesn't break others."""
    import inspect
    import apps.core.adapters.factory as factory_mod
    src = inspect.getsource(factory_mod)
    # Each get_* function imports inside the function, not at the top
    for fn_name in ('get_opera_sql', 'get_email_storage', 'get_opera3_reader',
                    'get_opera3_writer', 'get_email_sync', 'get_smtp', 'get_auth'):
        # Find the def line
        idx = src.find(f"def {fn_name}(")
        assert idx >= 0, f"{fn_name} not found"
        body_start = src.find(':', idx)
        body_end = src.find('\ndef ', body_start)
        if body_end < 0:
            body_end = len(src)
        body = src[body_start:body_end]
        assert 'from apps.core.adapters.local' in body, (
            f"{fn_name} should lazy-import its adapter"
        )
