"""Pin the System Connection panel: backend endpoint + frontend wire-up.

The panel is a read-only diagnostic display showing the centralised
parameters this app is wired up to. Critical contracts:

  - The endpoint exists and returns the standardised shape
  - SECRETS ARE NEVER RETURNED — passwords, API keys, tokens are
    only reported as `*_configured: bool` indicators
  - Each per-app Settings page imports + renders the panel
"""
from __future__ import annotations

import inspect
import os
from pathlib import Path
from unittest.mock import patch

import pytest


REPO = Path(__file__).resolve().parent.parent


# =====================================================================
# Endpoint contract
# =====================================================================


def test_endpoint_registered_in_api_main():
    import api.main as main_mod
    src = inspect.getsource(main_mod)
    assert '/api/system/connection-info' in src
    assert 'def get_system_connection_info' in src


def test_endpoint_requires_authentication():
    """The endpoint reads the user from request.state and 401s when
    unset. Defence-in-depth even though middleware should already
    enforce auth."""
    import api.main as main_mod
    src = inspect.getsource(main_mod.get_system_connection_info)
    assert "401" in src
    assert "Authentication required" in src


def test_endpoint_response_does_not_serialise_passwords():
    """Source-inspection contract: the endpoint must NEVER include
    a literal dict-key like `"password":` (a returned value) in
    its response. The configparser key name `cfg.get('db', 'password')`
    is allowed (it's a lookup, not a returned value).

    Pattern: forbidden = `"<secret>":` followed by anything except
    `_configured` (which is the explicit indicator key).
    """
    import re
    import api.main as main_mod
    src = inspect.getsource(main_mod.get_system_connection_info)

    # Find every dict-literal key. A returned secret would look like:
    #   "password": some_expression,
    # Allowed:
    #   "password_configured": _has(...)
    #   _has('database', 'password')   <- inside a function call, not a dict key
    forbidden_dict_keys = ['password', 'api_key', 'access_token', 'webhook_secret']
    for secret in forbidden_dict_keys:
        # Match `"<secret>":` or `'<secret>':` at the START of a dict
        # entry (i.e. preceded by whitespace or comma). NOT the
        # `_configured` form (which is a different key entirely).
        pat = re.compile(rf"""[\s,({{]['"]({re.escape(secret)})['"]\s*:""")
        matches = pat.findall(src)
        # Filter out matches that are actually `<secret>_configured`
        # — they share the substring but are different keys.
        # The regex anchors to the secret name exactly so this isn't
        # an issue, but a defensive sanity check follows in the
        # runtime test below.
        assert not matches, (
            f"connection-info endpoint must NOT return secret field "
            f"'{secret}' as a response key. Use '{secret}_configured': "
            f"<bool> instead. Found: {matches}"
        )


def test_endpoint_response_includes_required_sections():
    """The frontend SystemConnectionPanel relies on these top-level
    keys; pin the contract."""
    import api.main as main_mod
    src = inspect.getsource(main_mod.get_system_connection_info)
    expected_keys = [
        '"active_company"',
        '"opera_sql"',
        '"opera3"',
        '"email_imap"',
        '"email_smtp"',
        '"ai_provider"',
        '"deployment"',
    ]
    for k in expected_keys:
        assert k in src, f"connection-info missing required section {k}"


def test_endpoint_response_includes_configured_indicators():
    """Pin: each secret has a corresponding *_configured boolean."""
    import api.main as main_mod
    src = inspect.getsource(main_mod.get_system_connection_info)
    expected_indicators = [
        'password_configured',          # Opera SQL
        'gemini_configured',            # AI provider
    ]
    for ind in expected_indicators:
        assert ind in src, f"endpoint missing {ind} indicator"


# =====================================================================
# Endpoint runtime behaviour (call it directly with test env)
# =====================================================================


@pytest.fixture
def fake_request():
    """Build a minimal Request-like object that has the user set."""
    from types import SimpleNamespace

    class _Req:
        def __init__(self):
            self.state = SimpleNamespace(user={'username': 'test', 'is_admin': True})
    return _Req()


def test_endpoint_returns_no_secret_values_at_runtime(fake_request, monkeypatch):
    """Set known secret values via env vars, call the endpoint,
    assert NONE of the secret values appear in the response.

    This is the strongest guarantee — even if a future refactor
    accidentally added a secret-bearing field, this test catches
    the leak by string-searching the JSON response.
    """
    import asyncio
    import json
    from apps.core.env_config import reload_config

    secret_password = 'super_secret_password_xyz'
    secret_api_key = 'sk-abc123-secret-gemini-key'
    secret_smtp_pass = 'smtp-password-very-secret'

    monkeypatch.setenv('DATABASE_PASSWORD', secret_password)
    monkeypatch.setenv('GEMINI_API_KEY', secret_api_key)
    monkeypatch.setenv('EMAIL_SMTP_PASSWORD', secret_smtp_pass)
    monkeypatch.setenv('CONFIG_INI_PATH', '/nonexistent.ini')
    reload_config()

    from api.main import get_system_connection_info
    response = asyncio.run(get_system_connection_info(fake_request))
    body = json.dumps(response)

    assert secret_password not in body, "endpoint leaked DATABASE_PASSWORD"
    assert secret_api_key not in body, "endpoint leaked GEMINI_API_KEY"
    assert secret_smtp_pass not in body, "endpoint leaked EMAIL_SMTP_PASSWORD"

    # The *_configured indicators must report True for these
    assert response['opera_sql']['password_configured'] is True
    assert response['ai_provider']['gemini_configured'] is True
    assert response['email_smtp']['password_configured'] is True


def test_endpoint_reports_configured_false_when_secrets_unset(fake_request, monkeypatch):
    import asyncio
    from apps.core.env_config import reload_config

    monkeypatch.delenv('DATABASE_PASSWORD', raising=False)
    monkeypatch.delenv('GEMINI_API_KEY', raising=False)
    monkeypatch.delenv('EMAIL_SMTP_PASSWORD', raising=False)
    monkeypatch.setenv('CONFIG_INI_PATH', '/nonexistent.ini')
    reload_config()

    from api.main import get_system_connection_info
    response = asyncio.run(get_system_connection_info(fake_request))

    assert response['opera_sql']['password_configured'] is False
    assert response['ai_provider']['gemini_configured'] is False
    assert response['email_smtp']['password_configured'] is False


# =====================================================================
# Frontend component + per-app wire-up
# =====================================================================


def test_system_connection_panel_component_exists():
    p = REPO / 'frontend' / 'src' / 'components' / 'SystemConnectionPanel.tsx'
    assert p.exists(), 'SystemConnectionPanel.tsx must exist'


def test_panel_calls_correct_endpoint():
    p = REPO / 'frontend' / 'src' / 'components' / 'SystemConnectionPanel.tsx'
    src = p.read_text(encoding='utf-8')
    assert '/api/system/connection-info' in src


def test_panel_renders_no_secret_fields():
    """Pin: panel does not display any field name that would imply
    a raw secret value. Only `_configured` indicators are allowed.

    Forbidden bindings end with the secret name and a non-_configured
    suffix (i.e. `data.opera_sql.password ` or `data.opera_sql.password)`,
    NOT `data.opera_sql.password_configured`).
    """
    import re
    p = REPO / 'frontend' / 'src' / 'components' / 'SystemConnectionPanel.tsx'
    src = p.read_text(encoding='utf-8')

    # Look for `<scope>.<secret>` not followed by `_configured`.
    # Pattern: word boundary, secret name, NOT followed by `_configured`.
    forbidden_secrets = ['password', 'api_key', 'access_token', 'webhook_secret']
    for secret in forbidden_secrets:
        pat = re.compile(
            rf"\b{re.escape(secret)}(?!_configured)\b"
        )
        # Skip false-positive: comments / string literals describing
        # the secret are fine. The dangerous pattern is field access
        # like `data.opera_sql.password`. So restrict to that shape:
        access_pat = re.compile(
            rf"\.{re.escape(secret)}(?!_configured)\b"
        )
        matches = access_pat.findall(src)
        assert not matches, (
            f"panel binds to forbidden secret field ending in "
            f".{secret} (use .{secret}_configured instead). "
            f"Found {len(matches)} occurrences."
        )

    # Indicators ARE present
    assert 'password_configured' in src
    assert 'gemini_configured' in src


@pytest.mark.parametrize('settings_page', [
    'CashbookOptions.tsx',
    'GoCardlessSettings.tsx',
    'SupplierSettings.tsx',
])
def test_each_settings_page_imports_panel(settings_page):
    p = REPO / 'frontend' / 'src' / 'pages' / settings_page
    src = p.read_text(encoding='utf-8')
    assert "from '../components/SystemConnectionPanel'" in src, (
        f"{settings_page} must import SystemConnectionPanel"
    )


@pytest.mark.parametrize('settings_page', [
    'CashbookOptions.tsx',
    'GoCardlessSettings.tsx',
    'SupplierSettings.tsx',
])
def test_each_settings_page_renders_panel(settings_page):
    p = REPO / 'frontend' / 'src' / 'pages' / settings_page
    src = p.read_text(encoding='utf-8')
    assert '<SystemConnectionPanel' in src, (
        f"{settings_page} must render <SystemConnectionPanel />"
    )
