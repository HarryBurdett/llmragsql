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
    try:
        # The exact column name varies by schema version. Try both.
        rows = _read_sqlite(
            db_path,
            "SELECT DISTINCT customer_code FROM gocardless_payments "
            "WHERE customer_code IS NOT NULL AND customer_code != ''"
        )
    except Exception:
        try:
            rows = _read_sqlite(
                db_path,
                "SELECT DISTINCT account_code AS customer_code FROM gocardless_payments "
                "WHERE account_code IS NOT NULL AND account_code != ''"
            )
        except Exception as e:
            return HealthCheckItem(
                name='Payment history customers',
                description=f'Skipped — could not read gocardless_payments: {e}',
                passed=True,
                severity='info',
            )

    if not rows:
        return HealthCheckItem(
            name='Payment history customers',
            description='No payment history yet — nothing to check',
            passed=True,
            severity='info',
        )

    orphans: list[dict[str, Any]] = []
    orphan_total = 0
    for r in rows:
        code = (r.get('customer_code') or '').strip()
        if code and code not in valid_customer_codes:
            orphan_total += 1
            if len(orphans) < MAX_ORPHANS_RETURNED:
                orphans.append({
                    'customer_code': code,
                    'reason': f"customer '{code}' from payment history not in Opera sname",
                })

    return HealthCheckItem(
        name='Payment history customers',
        description='Customer codes in payment history must exist in Opera sname',
        passed=orphan_total == 0,
        total_checked=len(rows),
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
