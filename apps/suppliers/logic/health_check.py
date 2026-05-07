"""Suppliers data-integrity health checks.

Used by `GET /api/suppliers/health-check`. Verifies the suppliers
app's own data still references valid Opera codes:

  - Supplier codes in supplier_statements.db → exist in Opera pname?
"""
from __future__ import annotations

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


APP_NAME = 'suppliers'


def run_health_check(opera_sql, *, supplier_db_path: str | None) -> HealthCheckResult:
    """Run all suppliers health checks.

    Args:
        opera_sql: An OperaSQLPort adapter
        supplier_db_path: Path to supplier_statements.db (None if
                          not yet created)
    """
    checks: list[HealthCheckItem] = []

    valid_supplier_codes = _fetch_valid_codes(opera_sql, table='pname', col='pn_account')

    # ---- Supplier codes referenced in our local DB ----
    if supplier_db_path and Path(supplier_db_path).exists():
        checks.append(_check_supplier_codes(supplier_db_path, valid_supplier_codes))
    else:
        checks.append(HealthCheckItem(
            name='Supplier statement history',
            description='Skipped — supplier_statements.db not yet created',
            passed=True,
            severity='info',
        ))

    # Sanity
    if not valid_supplier_codes:
        checks.append(HealthCheckItem(
            name='Opera connection',
            description='Opera returned no supplier codes — connection or schema broken',
            passed=False,
            severity='error',
        ))
    else:
        checks.append(HealthCheckItem(
            name='Opera connection',
            description=f'Opera returned {len(valid_supplier_codes)} suppliers',
            passed=True,
            severity='info',
        ))

    return HealthCheckResult(
        app=APP_NAME,
        healthy=derive_overall_healthy(checks),
        summary=summarise(APP_NAME, checks),
        checks=checks,
        metadata={
            'checked_at': datetime.utcnow().isoformat() + 'Z',
            'opera_supplier_count': len(valid_supplier_codes),
        },
    )


def _check_supplier_codes(
    db_path: str, valid_supplier_codes: set[str],
) -> HealthCheckItem:
    """Inspect supplier_statements.db for orphan supplier codes.

    Tries multiple schema shapes since the table layout has
    evolved over time."""
    rows: list[dict[str, Any]] = []
    candidates = [
        "SELECT DISTINCT supplier_code FROM supplier_statements WHERE supplier_code IS NOT NULL",
        "SELECT DISTINCT pn_account AS supplier_code FROM supplier_statements WHERE pn_account IS NOT NULL",
        "SELECT DISTINCT supplier_code FROM supplier_config WHERE supplier_code IS NOT NULL",
    ]
    for sql in candidates:
        try:
            rows = _read_sqlite(db_path, sql)
            if rows:
                break
        except Exception:
            continue

    if not rows:
        return HealthCheckItem(
            name='Supplier statement history',
            description='No supplier statement history yet — nothing to check',
            passed=True,
            severity='info',
        )

    orphans: list[dict[str, Any]] = []
    orphan_total = 0
    for r in rows:
        code = (r.get('supplier_code') or '').strip()
        if code and code not in valid_supplier_codes:
            orphan_total += 1
            if len(orphans) < MAX_ORPHANS_RETURNED:
                orphans.append({
                    'supplier_code': code,
                    'reason': f"supplier '{code}' from local data not in Opera pname",
                })

    return HealthCheckItem(
        name='Supplier statement history',
        description='Supplier codes referenced in local data must exist in Opera pname',
        passed=orphan_total == 0,
        total_checked=len(rows),
        orphan_count=orphan_total,
        orphans=orphans,
        severity='warning',
    )


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
