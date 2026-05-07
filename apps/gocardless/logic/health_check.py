"""GoCardless data-integrity health checks.

Used by `GET /api/gocardless/health-check` and the Opera 3 mirror.
Verifies the GoCardless app's own data still references valid
Opera codes:

  - Customer codes in gocardless_payments.db → exist in Opera sname?
  - Bank code in saved settings → exists in Opera nbank?
  - Fees nominal account → exists in Opera nacnt?
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from apps.core.health_check import (
    MAX_ORPHANS_RETURNED,
    HealthCheckItem,
    HealthCheckResult,
    derive_overall_healthy,
    summarise,
)
from apps.core.ports import to_records

logger = logging.getLogger(__name__)


APP_NAME = 'gocardless'


def run_health_check(opera_sql, *, gocardless_db_path: str | None,
                     settings: dict[str, Any] | None = None) -> HealthCheckResult:
    """Run all GoCardless health checks.

    Args:
        opera_sql: An OperaSQLPort adapter for read-only Opera queries
        gocardless_db_path: Resolved path to gocardless_payments.db
                            (None if not yet provisioned)
        settings: Per-company GoCardless settings (bank_code,
                  fees_nominal_account, etc.). None = skip the
                  settings-related checks.
    """
    checks: list[HealthCheckItem] = []

    valid_bank_codes = _fetch_valid_codes(opera_sql, table='nbank', col='nk_acnt')
    valid_customer_codes = _fetch_valid_codes(opera_sql, table='sname', col='sn_account')
    valid_nominal_codes = _fetch_valid_codes(opera_sql, table='nacnt', col='na_acnt')

    # ---- Settings: bank_code + fees_nominal_account ----
    if settings is not None:
        checks.append(_check_settings_bank_code(settings, valid_bank_codes))
        checks.append(_check_settings_fees_account(settings, valid_nominal_codes))
    else:
        checks.append(HealthCheckItem(
            name='GoCardless settings',
            description='Skipped — no GoCardless settings configured for this company',
            passed=True,
            severity='info',
        ))

    # ---- Payment history customer codes ----
    if gocardless_db_path and Path(gocardless_db_path).exists():
        checks.append(_check_payment_customer_codes(gocardless_db_path, valid_customer_codes))
    else:
        checks.append(HealthCheckItem(
            name='Payment history',
            description='Skipped — gocardless_payments.db not yet created',
            passed=True,
            severity='info',
        ))

    # Sanity
    if not valid_bank_codes:
        checks.append(HealthCheckItem(
            name='Opera connection',
            description='Opera returned no bank codes — connection or schema broken',
            passed=False,
            severity='error',
        ))

    return HealthCheckResult(
        app=APP_NAME,
        healthy=derive_overall_healthy(checks),
        summary=summarise(APP_NAME, checks),
        checks=checks,
        metadata={
            'checked_at': datetime.utcnow().isoformat() + 'Z',
            'opera_bank_count': len(valid_bank_codes),
            'opera_customer_count': len(valid_customer_codes),
        },
    )


def _check_settings_bank_code(
    settings: dict[str, Any], valid_bank_codes: set[str],
) -> HealthCheckItem:
    bc = (settings.get('bank_code') or '').strip()
    if not bc:
        return HealthCheckItem(
            name='Settings bank code',
            description='No bank account configured in GoCardless settings',
            passed=True,
            severity='info',
        )
    if bc in valid_bank_codes:
        return HealthCheckItem(
            name='Settings bank code',
            description=f"Bank code '{bc}' exists in Opera nbank",
            passed=True,
            total_checked=1,
            severity='warning',
        )
    return HealthCheckItem(
        name='Settings bank code',
        description=f"Bank code '{bc}' from GoCardless settings does NOT exist in Opera nbank",
        passed=False,
        total_checked=1,
        orphan_count=1,
        orphans=[{'bank_code': bc, 'reason': 'not in Opera nbank'}],
        severity='error',
    )


def _check_settings_fees_account(
    settings: dict[str, Any], valid_nominal_codes: set[str],
) -> HealthCheckItem:
    acct = (settings.get('fees_nominal_account') or '').strip()
    if not acct:
        return HealthCheckItem(
            name='Settings fees account',
            description='No fees nominal account configured (fees won\'t auto-post)',
            passed=True,
            severity='info',
        )
    if acct in valid_nominal_codes:
        return HealthCheckItem(
            name='Settings fees account',
            description=f"Fees account '{acct}' exists in Opera nacnt",
            passed=True,
            total_checked=1,
            severity='warning',
        )
    return HealthCheckItem(
        name='Settings fees account',
        description=f"Fees account '{acct}' does NOT exist in Opera nacnt",
        passed=False,
        total_checked=1,
        orphan_count=1,
        orphans=[{'account_code': acct, 'reason': 'not in Opera nacnt'}],
        severity='error',
    )


def _check_payment_customer_codes(
    db_path: str, valid_customer_codes: set[str],
) -> HealthCheckItem:
    """Inspect the GoCardless app's local DB for orphan Opera customer
    references.

    The actual schema (sql_rag/gocardless_payments.py) uses two
    tables:
      - gocardless_mandates.opera_account
      - gocardless_payment_requests.opera_account

    Both reference Opera customers (sname.sn_account). We union the
    distinct codes from both and check each against Opera.
    """
    # Discover which expected tables actually exist (schema may
    # have evolved across versions / installations).
    try:
        tables = _sqlite_tables(db_path)
    except Exception as e:
        return HealthCheckItem(
            name='Payment history customers',
            description=f'Skipped — could not read GoCardless DB: {e}',
            passed=True,
            severity='info',
        )

    # Pull opera_account codes from each table that exists.
    referenced: set[str] = set()
    sources_inspected = 0
    for table in ('gocardless_mandates', 'gocardless_payment_requests'):
        if table not in tables:
            continue
        try:
            rows = _read_sqlite(
                db_path,
                f"SELECT DISTINCT opera_account FROM {table} "
                "WHERE opera_account IS NOT NULL AND opera_account != ''"
            )
            sources_inspected += 1
            for r in rows:
                code = (r.get('opera_account') or '').strip()
                if code:
                    referenced.add(code)
        except Exception as e:
            logger.debug(f"Skipping {table}: {e}")

    if sources_inspected == 0:
        return HealthCheckItem(
            name='Payment history customers',
            description=(
                'Skipped — no GoCardless tables present yet '
                '(no mandates or payment requests recorded)'
            ),
            passed=True,
            severity='info',
        )

    if not referenced:
        return HealthCheckItem(
            name='Payment history customers',
            description='No customer references in GoCardless data — nothing to check',
            passed=True,
            severity='info',
        )

    orphans: list[dict[str, Any]] = []
    orphan_total = 0
    for code in sorted(referenced):
        if code not in valid_customer_codes:
            orphan_total += 1
            if len(orphans) < MAX_ORPHANS_RETURNED:
                orphans.append({
                    'opera_account': code,
                    'reason': f"customer '{code}' from GoCardless data not in Opera sname",
                })

    return HealthCheckItem(
        name='Payment history customers',
        description=(
            'Opera customer codes referenced in GoCardless mandates / '
            'payment requests must exist in Opera sname'
        ),
        passed=orphan_total == 0,
        total_checked=len(referenced),
        orphan_count=orphan_total,
        orphans=orphans,
        severity='warning',
    )


# =====================================================================
# SQL helpers (shared shape with bank_reconcile.health_check)
# =====================================================================


def _fetch_valid_codes(opera_sql, *, table: str, col: str) -> set[str]:
    try:
        result = opera_sql.execute_query(
            f"SELECT RTRIM({col}) AS code FROM {table} WITH (NOLOCK)"
        )
        rows = to_records(result)
        return {(r.get('code') or '').strip() for r in rows if r.get('code')}
    except Exception as e:
        logger.warning(f"Could not fetch {col} from {table}: {e}")
        return set()


def _read_sqlite(path: str, sql: str) -> list[dict[str, Any]]:
    conn = sqlite3.connect(path)
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(sql)
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _sqlite_tables(path: str) -> set[str]:
    """Return the set of table names in the SQLite at `path`.
    Used so the health check can adapt to whichever schema version
    is present (no hard dependency on a specific table existing)."""
    conn = sqlite3.connect(path)
    try:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        return {row[0] for row in cur.fetchall()}
    finally:
        conn.close()
