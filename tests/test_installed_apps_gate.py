"""Pin the INSTALLED_APPS gating contract.

Phase A — SAM-readiness. When `INSTALLED_APPS` env var lists a
subset of app names, only those routers register. This is the
mechanism that makes per-app containers possible: the gocardless
container has INSTALLED_APPS=gocardless, so it physically can't
respond to /api/bank-import/*.

Default behaviour (unset, empty, "*", "all") = all routers
register, matching monolithic deployment.
"""
from __future__ import annotations

import inspect

import pytest


def test_should_install_returns_true_when_installed_apps_unset():
    """Default: empty set = install everything."""
    from api.main import _should_install, _installed_apps
    if _installed_apps:
        pytest.skip("Test environment has INSTALLED_APPS set")
    assert _should_install('bank_reconcile') is True
    assert _should_install('gocardless') is True
    assert _should_install('suppliers') is True


def test_routers_module_imports_clean():
    """Smoke: the gating logic in api/main.py runs without error."""
    from api.main import _should_install, _installed_apps
    assert callable(_should_install)
    assert isinstance(_installed_apps, set)


def test_healthz_endpoint_registered():
    """The /healthz endpoint must register regardless of INSTALLED_APPS
    so docker-compose healthchecks and SAM readiness probes always work.
    """
    from api.main import app
    paths = [r.path for r in app.routes if hasattr(r, 'path')]
    assert '/healthz' in paths


def test_main_module_uses_installed_apps_env_var():
    """Source-inspection: the gating reads INSTALLED_APPS from env."""
    import api.main
    src = inspect.getsource(api.main)
    assert "os.environ.get('INSTALLED_APPS'" in src or 'INSTALLED_APPS' in src
    assert "_should_install" in src


def test_known_app_names_are_gated():
    """Each known app name has a corresponding gate."""
    import api.main
    src = inspect.getsource(api.main)
    expected_gates = [
        "_should_install('bank_reconcile')",
        "_should_install('gocardless')",
        "_should_install('suppliers')",
        "_should_install('balance_check')",
    ]
    for gate in expected_gates:
        assert gate in src, f"missing gate: {gate}"


def test_special_values_default_to_all():
    """INSTALLED_APPS="*" or "all" → empty set = "install everything".

    Tested via _installed_apps_raw parsing logic by source inspection
    (we can't easily mutate env vars after module import).
    """
    import api.main
    src = inspect.getsource(api.main)
    assert "in ('', '*', 'all')" in src
