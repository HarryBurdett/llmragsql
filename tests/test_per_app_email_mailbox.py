"""Pin the per-app email mailbox contract.

A customer's MS Graph (or IMAP) credentials are central — one set per
customer for all apps. The **mailbox identity** that each app reads
from / sends as is per-app:

    bank-reconcile -> EMAIL_MAILBOX=banking@customer.com
    gocardless     -> EMAIL_MAILBOX=payments@customer.com
    suppliers      -> EMAIL_MAILBOX=ap@customer.com

Or (single-mailbox install):

    EMAIL_MAILBOX=accounts@customer.com  (same for every app)

This test file pins:

  - `EMAIL_MAILBOX` is documented in the env-var contract
  - The `/api/system/connection-info` endpoint surfaces the active
    mailbox + its source
  - The endpoint surfaces the active email provider (microsoft / imap)
    and the central MS Graph credential indicators
  - Secrets are still never returned (only `*_configured` booleans)
  - The frontend SystemConnectionPanel accepts an `appLabel` prop and
    renders the mailbox section
  - Every Settings page passes its app label
  - The per-app SAM docs document `EMAIL_MAILBOX`
"""
from __future__ import annotations

import asyncio
import inspect
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


REPO = Path(__file__).resolve().parent.parent


# =====================================================================
# Documentation contract
# =====================================================================


def test_env_var_contract_documents_email_mailbox():
    p = REPO / 'docs' / 'sam-migration' / 'env-var-contract.md'
    src = p.read_text(encoding='utf-8')
    assert 'EMAIL_MAILBOX' in src, (
        "env-var-contract.md must document EMAIL_MAILBOX"
    )
    assert 'EMAIL_MICROSOFT_TENANT_ID' in src
    assert 'EMAIL_MICROSOFT_CLIENT_ID' in src
    assert 'EMAIL_MICROSOFT_CLIENT_SECRET' in src
    assert 'EMAIL_PROVIDER' in src


def test_handover_doc_explains_per_app_mailbox():
    p = REPO / 'docs' / 'sam-migration' / 'sam-team-handover.md'
    src = p.read_text(encoding='utf-8')
    assert 'EMAIL_MAILBOX' in src, (
        "handover doc must call out EMAIL_MAILBOX as per-app config"
    )
    # Mention of the central vs per-app split
    assert 'central' in src.lower()


@pytest.mark.parametrize('app_doc', [
    'bank-reconcile.md',
    'gocardless.md',
    'suppliers.md',
])
def test_each_app_doc_documents_email_mailbox(app_doc):
    p = REPO / 'docs' / 'sam-migration' / 'apps' / app_doc
    src = p.read_text(encoding='utf-8')
    assert 'EMAIL_MAILBOX' in src, (
        f"{app_doc} must document EMAIL_MAILBOX as a required env var"
    )


# =====================================================================
# Endpoint contract
# =====================================================================


@pytest.fixture
def fake_request():
    class _Req:
        def __init__(self):
            self.state = SimpleNamespace(
                user={'username': 'test', 'is_admin': True}
            )
    return _Req()


def test_endpoint_returns_email_mailbox_section():
    import api.main as main_mod
    src = inspect.getsource(main_mod.get_system_connection_info)
    assert '"email_mailbox"' in src, (
        "connection-info must return an email_mailbox section so the "
        "panel can show which inbox THIS app reads from"
    )
    assert '"email_provider"' in src, (
        "connection-info must return an email_provider section so the "
        "panel can show whether MS Graph or IMAP is active"
    )


def test_endpoint_reports_email_mailbox_from_env(fake_request, monkeypatch):
    """When EMAIL_MAILBOX is set, the endpoint reports it and labels
    the source correctly."""
    from apps.core.env_config import reload_config

    monkeypatch.setenv('EMAIL_MAILBOX', 'banking@customer.com')
    monkeypatch.setenv('CONFIG_INI_PATH', '/nonexistent.ini')
    reload_config()

    from api.main import get_system_connection_info
    response = asyncio.run(get_system_connection_info(fake_request))

    assert response['email_mailbox']['mailbox'] == 'banking@customer.com'
    assert 'EMAIL_MAILBOX' in response['email_mailbox']['source']


def test_endpoint_falls_back_to_imap_username_when_mailbox_unset(
    fake_request, monkeypatch,
):
    """Single-inbox legacy installs use EMAIL_IMAP_USERNAME. The
    endpoint should surface that fallback explicitly."""
    from apps.core.env_config import reload_config

    monkeypatch.delenv('EMAIL_MAILBOX', raising=False)
    monkeypatch.setenv('EMAIL_IMAP_USERNAME', 'accounts@customer.com')
    monkeypatch.setenv('CONFIG_INI_PATH', '/nonexistent.ini')
    reload_config()

    from api.main import get_system_connection_info
    response = asyncio.run(get_system_connection_info(fake_request))

    assert response['email_mailbox']['mailbox'] == 'accounts@customer.com'
    assert 'fallback' in response['email_mailbox']['source'].lower()


def test_endpoint_reports_microsoft_provider_with_indicators(
    fake_request, monkeypatch,
):
    """When EMAIL_PROVIDER=microsoft, indicators report whether the
    central Graph creds are populated. NEVER the values themselves."""
    from apps.core.env_config import reload_config

    secret_tenant = 'tenant-uuid-secret-xyz-12345'
    secret_client = 'client-uuid-secret-abc-67890'
    secret_secret = 'graph-client-secret-def-extremely-private'

    monkeypatch.setenv('EMAIL_PROVIDER', 'microsoft')
    monkeypatch.setenv('EMAIL_MICROSOFT_TENANT_ID', secret_tenant)
    monkeypatch.setenv('EMAIL_MICROSOFT_CLIENT_ID', secret_client)
    monkeypatch.setenv('EMAIL_MICROSOFT_CLIENT_SECRET', secret_secret)
    monkeypatch.setenv('CONFIG_INI_PATH', '/nonexistent.ini')
    reload_config()

    from api.main import get_system_connection_info
    response = asyncio.run(get_system_connection_info(fake_request))
    body = json.dumps(response)

    # Provider reported
    assert response['email_provider']['provider'] == 'microsoft'

    # Central creds: indicators true, values absent
    assert response['email_provider']['microsoft_tenant_id_configured'] is True
    assert response['email_provider']['microsoft_client_id_configured'] is True
    assert response['email_provider']['microsoft_client_secret_configured'] is True

    assert secret_tenant not in body, "leaked EMAIL_MICROSOFT_TENANT_ID"
    assert secret_client not in body, "leaked EMAIL_MICROSOFT_CLIENT_ID"
    assert secret_secret not in body, "leaked EMAIL_MICROSOFT_CLIENT_SECRET"


def test_endpoint_reports_microsoft_indicators_false_when_unset(
    fake_request, monkeypatch,
):
    from apps.core.env_config import reload_config

    monkeypatch.delenv('EMAIL_MICROSOFT_TENANT_ID', raising=False)
    monkeypatch.delenv('EMAIL_MICROSOFT_CLIENT_ID', raising=False)
    monkeypatch.delenv('EMAIL_MICROSOFT_CLIENT_SECRET', raising=False)
    monkeypatch.setenv('CONFIG_INI_PATH', '/nonexistent.ini')
    reload_config()

    from api.main import get_system_connection_info
    response = asyncio.run(get_system_connection_info(fake_request))

    assert response['email_provider']['microsoft_tenant_id_configured'] is False
    assert response['email_provider']['microsoft_client_id_configured'] is False
    assert response['email_provider']['microsoft_client_secret_configured'] is False


def test_smtp_from_address_falls_back_to_mailbox(fake_request, monkeypatch):
    """Most single-mailbox installs leave EMAIL_FROM_ADDRESS unset and
    let it default to EMAIL_MAILBOX. Pin that fallback."""
    from apps.core.env_config import reload_config

    monkeypatch.delenv('EMAIL_FROM_ADDRESS', raising=False)
    monkeypatch.setenv('EMAIL_MAILBOX', 'payments@customer.com')
    monkeypatch.setenv('CONFIG_INI_PATH', '/nonexistent.ini')
    reload_config()

    from api.main import get_system_connection_info
    response = asyncio.run(get_system_connection_info(fake_request))

    assert response['email_smtp']['from_address'] == 'payments@customer.com'
    assert 'fallback' in response['email_smtp']['from_address_source'].lower()


# =====================================================================
# Frontend component contract
# =====================================================================


def test_panel_accepts_app_label_prop():
    p = REPO / 'frontend' / 'src' / 'components' / 'SystemConnectionPanel.tsx'
    src = p.read_text(encoding='utf-8')
    assert 'appLabel' in src, (
        "SystemConnectionPanel must accept an appLabel prop so it can "
        "show 'Mailbox (Bank Reconciliation)' / '(GoCardless)' / "
        "'(Suppliers)' next to the active mailbox"
    )


def test_panel_renders_mailbox_section():
    p = REPO / 'frontend' / 'src' / 'components' / 'SystemConnectionPanel.tsx'
    src = p.read_text(encoding='utf-8')
    assert 'email_mailbox' in src, (
        "panel must bind to data.email_mailbox so the operator sees "
        "which inbox this app instance is reading"
    )


def test_panel_renders_email_provider_section():
    p = REPO / 'frontend' / 'src' / 'components' / 'SystemConnectionPanel.tsx'
    src = p.read_text(encoding='utf-8')
    assert 'email_provider' in src
    # MS Graph indicators rendered when active
    assert 'microsoft_tenant_id_configured' in src
    assert 'microsoft_client_id_configured' in src
    assert 'microsoft_client_secret_configured' in src


def test_panel_does_not_leak_microsoft_secrets():
    """MS Graph creds get *_configured indicators only — values are
    never bound."""
    import re
    p = REPO / 'frontend' / 'src' / 'components' / 'SystemConnectionPanel.tsx'
    src = p.read_text(encoding='utf-8')

    # `.microsoft_client_secret` (without _configured) must not appear
    bad = re.compile(r"\.microsoft_(tenant_id|client_id|client_secret)(?!_configured)\b")
    matches = bad.findall(src)
    assert not matches, (
        f"panel binds to a Graph secret field directly; use the "
        f"_configured indicator instead. Found: {matches}"
    )


@pytest.mark.parametrize('settings_page,expected_label', [
    ('CashbookOptions.tsx', 'Bank Reconciliation'),
    ('GoCardlessSettings.tsx', 'GoCardless'),
    ('SupplierSettings.tsx', 'Suppliers'),
])
def test_each_settings_page_passes_app_label(settings_page, expected_label):
    """Each app's Settings page must render the panel WITH its app
    label so the mailbox section is unambiguous."""
    p = REPO / 'frontend' / 'src' / 'pages' / settings_page
    src = p.read_text(encoding='utf-8')
    assert f'appLabel="{expected_label}"' in src, (
        f'{settings_page} must render <SystemConnectionPanel '
        f'appLabel="{expected_label}" />'
    )
