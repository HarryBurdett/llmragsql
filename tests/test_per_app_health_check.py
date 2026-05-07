"""Pin the per-app Health Check feature.

Each app exposes `GET /api/{app}/health-check` returning a
standardised HealthCheckResult. The frontend has a top-level
"Diagnostics > Health Check" menu item per app. SAM (Phase C)
will fan out across these endpoints.

These tests pin:
  - The shared HealthCheckResult dataclass shape
  - Each app's endpoint exists + returns the standardised shape
    (via source-inspection — runtime would need an Opera connection)
  - The menu has Diagnostics > Health Check in each app
  - App.tsx registers the per-app routes with the right appFilter
"""
from __future__ import annotations

import inspect
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent


# =====================================================================
# Shared HealthCheckResult shape
# =====================================================================


def test_health_check_result_dataclass_has_required_fields():
    from apps.core.health_check import HealthCheckResult, HealthCheckItem
    r = HealthCheckResult(app='test', healthy=True, summary='ok')
    assert hasattr(r, 'app')
    assert hasattr(r, 'healthy')
    assert hasattr(r, 'summary')
    assert hasattr(r, 'checks')
    assert hasattr(r, 'metadata')

    item = HealthCheckItem(name='x', description='y', passed=True)
    assert hasattr(item, 'severity')
    assert hasattr(item, 'orphans')
    assert hasattr(item, 'orphan_count')


def test_to_response_dict_returns_standardised_shape():
    from apps.core.health_check import HealthCheckResult, HealthCheckItem
    r = HealthCheckResult(
        app='gocardless',
        healthy=False,
        summary='1 orphan',
        checks=[
            HealthCheckItem(
                name='Settings bank code',
                description='ok',
                passed=False,
                total_checked=1,
                orphan_count=1,
                orphans=[{'bank_code': 'BC999'}],
                severity='error',
            ),
        ],
        metadata={'checked_at': '2026-05-07T10:00:00Z'},
    )
    out = r.to_response_dict()
    # Top-level keys
    assert set(out.keys()) >= {'app', 'healthy', 'summary', 'checks', 'metadata'}
    # Per-check keys
    assert set(out['checks'][0].keys()) == {
        'name', 'description', 'passed', 'total_checked',
        'orphan_count', 'orphans', 'severity',
    }
    assert out['app'] == 'gocardless'
    assert out['healthy'] is False
    assert out['checks'][0]['orphans'] == [{'bank_code': 'BC999'}]


def test_derive_overall_healthy():
    from apps.core.health_check import (
        HealthCheckItem, derive_overall_healthy,
    )
    # All passing
    assert derive_overall_healthy([
        HealthCheckItem('a', 'a', True, severity='warning'),
    ]) is True
    # Warning failure does NOT fail overall
    assert derive_overall_healthy([
        HealthCheckItem('a', 'a', False, severity='warning'),
    ]) is True
    # Error failure DOES fail overall
    assert derive_overall_healthy([
        HealthCheckItem('a', 'a', False, severity='error'),
    ]) is False


# =====================================================================
# Endpoint registration (source inspection — avoids needing Opera)
# =====================================================================


def test_bank_reconcile_health_check_endpoint_registered():
    from apps.bank_reconcile.api import routes
    src = inspect.getsource(routes)
    assert '/api/bank-import/health-check' in src
    assert 'bank_reconcile_health_check' in src


def test_gocardless_health_check_endpoint_registered():
    from apps.gocardless.api import routes
    src = inspect.getsource(routes)
    assert '/api/gocardless/health-check' in src
    assert 'gocardless_health_check' in src


def test_suppliers_health_check_endpoint_registered():
    from apps.suppliers.api import routes
    src = inspect.getsource(routes)
    assert '/api/suppliers/health-check' in src
    assert 'suppliers_health_check' in src


def test_endpoints_use_run_health_check_from_logic_module():
    """Each endpoint must call run_health_check() from its app's
    logic.health_check module — confirms the boundary is right."""
    from apps.bank_reconcile.api import routes as br
    from apps.gocardless.api import routes as gc
    from apps.suppliers.api import routes as sup

    assert 'from apps.bank_reconcile.logic.health_check import run_health_check' in inspect.getsource(br)
    assert 'from apps.gocardless.logic.health_check import run_health_check' in inspect.getsource(gc)
    assert 'from apps.suppliers.logic.health_check import run_health_check' in inspect.getsource(sup)


# =====================================================================
# run_health_check signatures
# =====================================================================


def test_bank_reconcile_run_health_check_signature():
    from apps.bank_reconcile.logic.health_check import run_health_check
    sig = inspect.signature(run_health_check)
    assert 'opera_sql' in sig.parameters
    assert 'company_db_paths' in sig.parameters


def test_gocardless_run_health_check_signature():
    from apps.gocardless.logic.health_check import run_health_check
    sig = inspect.signature(run_health_check)
    assert 'opera_sql' in sig.parameters
    assert 'gocardless_db_path' in sig.parameters
    assert 'settings' in sig.parameters


def test_suppliers_run_health_check_signature():
    from apps.suppliers.logic.health_check import run_health_check
    sig = inspect.signature(run_health_check)
    assert 'opera_sql' in sig.parameters
    assert 'supplier_db_path' in sig.parameters


# =====================================================================
# Behaviour: graceful degrade when DBs unavailable
# =====================================================================


class _StubOperaSQL:
    """Returns canned rows for known schema queries."""
    def __init__(self, codes_by_table: dict[str, list[str]] | None = None):
        self.codes_by_table = codes_by_table or {
            'nbank': ['BC010', 'BC020'],
            'sname': ['CUST001'],
            'pname': ['SUPP001'],
            'nacnt': ['1000', '2000'],
        }
        self.queries: list[str] = []

    def execute_query(self, sql, params=None):
        self.queries.append(sql)
        # Match the table name in "FROM <table> WITH (NOLOCK)"
        for table, codes in self.codes_by_table.items():
            if f"FROM {table} " in sql:
                return [{'code': c} for c in codes]
        return []

    def __bool__(self):
        return True


def test_bank_reconcile_health_check_graceful_when_no_dbs():
    from apps.bank_reconcile.logic.health_check import run_health_check
    opera = _StubOperaSQL()
    result = run_health_check(opera, company_db_paths={
        'bank_aliases.db': None,
        'bank_patterns.db': None,
        'email_data.db': None,
    })
    # Should report info-severity skipped checks, not crash
    assert result.app == 'bank_reconcile'
    skipped = [c for c in result.checks if c.severity == 'info']
    assert len(skipped) >= 2
    # Overall healthy because no error-severity failures
    assert result.healthy is True


def test_bank_reconcile_handles_pre_migration_aliases_db(tmp_path):
    """Regression: a real bank_aliases.db without the bank_code
    column (pre-Phase-6 migration) must NOT cause an error-severity
    check failure. The health check should:
      - tolerate the missing column (build SELECT dynamically)
      - return info-severity 'skipped' for the bank-code check
      - still report customer/supplier orphans correctly

    Caused by user-reported bug: 'Could not read bank_aliases.db:
    no such column: bank_code'.
    """
    import sqlite3
    from apps.bank_reconcile.logic.health_check import run_health_check

    # Build a pre-migration schema (no bank_code column)
    db_path = tmp_path / 'bank_aliases.db'
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE bank_import_aliases (
            id INTEGER PRIMARY KEY,
            bank_name TEXT NOT NULL,
            account_code TEXT,
            ledger_type TEXT,
            active INTEGER DEFAULT 1
        )
    """)
    conn.execute(
        "INSERT INTO bank_import_aliases (bank_name, account_code, ledger_type) "
        "VALUES (?, ?, ?)",
        ('TESCO', 'CUST001', 'C'),
    )
    conn.commit()
    conn.close()

    opera = _StubOperaSQL(codes_by_table={
        'nbank': ['BC010'],
        'sname': ['CUST001'],
        'pname': ['SUPP001'],
        'nacnt': ['1000'],
    })

    result = run_health_check(opera, company_db_paths={
        'bank_aliases.db': str(db_path),
        'bank_patterns.db': None,
        'email_data.db': None,
    })

    # The 'Bank aliases' top-level read should NOT fail with an
    # error severity any more.
    error_aliases = [
        c for c in result.checks
        if c.name == 'Bank aliases' and c.severity == 'error'
    ]
    assert not error_aliases, (
        f"Pre-migration aliases DB triggered an error: "
        f"{[c.description for c in error_aliases]}"
    )

    # The bank-code check should either be skipped (info severity)
    # if the migration didn't run, or pass cleanly (warning severity,
    # 0 orphans) if it did. Either way, no failure.
    bank_code_check = [c for c in result.checks if c.name == 'Alias bank codes']
    assert len(bank_code_check) == 1
    assert bank_code_check[0].passed is True
    assert bank_code_check[0].orphan_count == 0

    # Customer/supplier checks should still run normally (the row
    # has valid customer 'CUST001' so passes).
    cust_check = [c for c in result.checks if c.name == 'Alias customer codes']
    assert len(cust_check) == 1
    assert cust_check[0].passed is True
    assert cust_check[0].total_checked == 1

    # Overall: healthy=True (no error-severity failures)
    assert result.healthy is True


def test_gocardless_health_check_graceful_when_no_dbs():
    from apps.gocardless.logic.health_check import run_health_check
    opera = _StubOperaSQL()
    result = run_health_check(opera, gocardless_db_path=None, settings=None)
    assert result.app == 'gocardless'
    assert result.healthy is True


def test_suppliers_health_check_graceful_when_no_dbs():
    from apps.suppliers.logic.health_check import run_health_check
    opera = _StubOperaSQL()
    result = run_health_check(opera, supplier_db_path=None)
    assert result.app == 'suppliers'
    assert result.healthy is True


def test_health_check_fails_when_opera_returns_no_codes():
    """If Opera returns nothing, the connection check fires and
    overall report fails."""
    from apps.bank_reconcile.logic.health_check import run_health_check
    empty_opera = _StubOperaSQL(codes_by_table={
        'nbank': [], 'sname': [], 'pname': [], 'nacnt': [],
    })
    result = run_health_check(empty_opera, company_db_paths={
        'bank_aliases.db': None,
        'bank_patterns.db': None,
        'email_data.db': None,
    })
    assert result.healthy is False
    assert any(c.severity == 'error' and not c.passed for c in result.checks)


def test_gocardless_health_check_orphan_settings_fails():
    """Settings reference a bank code that doesn't exist → error
    severity → overall healthy=False."""
    from apps.gocardless.logic.health_check import run_health_check
    opera = _StubOperaSQL(codes_by_table={
        'nbank': ['BC010'],
        'sname': ['CUST1'],
        'nacnt': ['1000'],
    })
    result = run_health_check(
        opera,
        gocardless_db_path=None,
        settings={'bank_code': 'BCBOGUS', 'fees_nominal_account': '1000'},
    )
    assert result.healthy is False
    failed_settings = [c for c in result.checks if c.name == 'Settings bank code']
    assert len(failed_settings) == 1
    assert failed_settings[0].passed is False
    assert failed_settings[0].orphan_count == 1


# =====================================================================
# Frontend menu + routes
# =====================================================================


@pytest.fixture(scope='module')
def layout_src() -> str:
    return (REPO / 'frontend' / 'src' / 'components' / 'Layout.tsx').read_text(encoding='utf-8')


@pytest.fixture(scope='module')
def app_src() -> str:
    return (REPO / 'frontend' / 'src' / 'App.tsx').read_text(encoding='utf-8')


def test_each_app_menu_has_diagnostics_health_check(layout_src):
    """Every per-app menu has a Diagnostics section with Health Check."""
    # Cashbook
    assert "/cashbook/health-check" in layout_src
    # GoCardless
    assert "/cashbook/gocardless-health-check" in layout_src
    # Suppliers
    assert "/supplier/health-check" in layout_src
    # All under Diagnostics heading
    diagnostics_count = layout_src.count("heading: 'Diagnostics'")
    assert diagnostics_count >= 3, (
        f"Expected ≥3 Diagnostics sections (one per app); found {diagnostics_count}"
    )


def test_app_tsx_registers_health_check_routes(app_src):
    assert '/cashbook/health-check' in app_src
    assert 'appFilter="bank_reconcile"' in app_src

    assert '/cashbook/gocardless-health-check' in app_src
    assert 'appFilter="gocardless"' in app_src

    assert '/supplier/health-check' in app_src
    assert 'appFilter="suppliers"' in app_src


def test_app_tsx_imports_health_check_component(app_src):
    assert "import { HealthCheck } from './pages/HealthCheck'" in app_src


# =====================================================================
# UI page exists and follows expected shape
# =====================================================================


def test_health_check_page_exists():
    page = REPO / 'frontend' / 'src' / 'pages' / 'HealthCheck.tsx'
    assert page.exists()
    src = page.read_text(encoding='utf-8')
    # Component name + props
    assert 'export function HealthCheck(' in src
    assert 'appFilter' in src
    # Endpoint mapping
    assert '/api/bank-import/health-check' in src
    assert '/api/gocardless/health-check' in src
    assert '/api/suppliers/health-check' in src
