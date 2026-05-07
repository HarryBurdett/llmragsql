"""Bank-reconcile data-integrity health checks.

Used by `GET /api/bank-import/health-check` and `GET
/api/opera3/bank-import/health-check`. Verifies the bank-rec app's
own data still references valid Opera codes:

  - Bank codes in bank_aliases.db → exist in Opera nbank?
  - Customer codes (ledger_type='C') in aliases → exist in sname?
  - Supplier codes (ledger_type='S') in aliases → exist in pname?
  - Bank codes in bank_statement_imports history → exist in nbank?
  - Pattern accounts in bank_patterns.db → exist in sname/pname/nacnt?

Especially useful immediately after an Opera 3 → Opera SE upgrade,
to confirm Opera's migration preserved the codes our learned data
references.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
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


APP_NAME = 'bank_reconcile'


def run_health_check(opera_sql, *, company_db_paths: dict[str, str | None]) -> HealthCheckResult:
    """Run all bank-reconcile health checks.

    Args:
        opera_sql: An OperaSQLPort adapter (used for read-only
                   queries against nbank, sname, pname, nacnt)
        company_db_paths: Dict mapping db_name → resolved path,
                          for company-scoped SQLite lookup. Pass
                          None for any DB that isn't available
                          (graceful degrade — the check just
                          reports total_checked=0).

    Returns the standardised HealthCheckResult.
    """
    checks: list[HealthCheckItem] = []

    # Pull the universe of valid codes from Opera once
    valid_bank_codes = _fetch_valid_codes(opera_sql, table='nbank', col='nk_acnt')
    valid_customer_codes = _fetch_valid_codes(opera_sql, table='sname', col='sn_account')
    valid_supplier_codes = _fetch_valid_codes(opera_sql, table='pname', col='pn_account')
    valid_nominal_codes = _fetch_valid_codes(opera_sql, table='nacnt', col='na_acnt')

    # ---- bank_aliases.db checks ----
    aliases_path = company_db_paths.get('bank_aliases.db')
    if aliases_path:
        checks.extend(_check_bank_aliases(
            aliases_path,
            valid_bank_codes=valid_bank_codes,
            valid_customer_codes=valid_customer_codes,
            valid_supplier_codes=valid_supplier_codes,
        ))
    else:
        checks.append(HealthCheckItem(
            name='Bank aliases',
            description='Skipped — bank_aliases.db not available for this company',
            passed=True,
            severity='info',
        ))

    # ---- bank_patterns.db checks ----
    patterns_path = company_db_paths.get('bank_patterns.db')
    if patterns_path:
        checks.extend(_check_bank_patterns(
            patterns_path,
            valid_customer_codes=valid_customer_codes,
            valid_supplier_codes=valid_supplier_codes,
            valid_nominal_codes=valid_nominal_codes,
        ))
    else:
        checks.append(HealthCheckItem(
            name='Pattern learning',
            description='Skipped — bank_patterns.db not available for this company',
            passed=True,
            severity='info',
        ))

    # ---- bank_statement_imports historical bank codes ----
    email_path = company_db_paths.get('email_data.db')
    if email_path:
        checks.append(_check_audit_bank_codes(email_path, valid_bank_codes))

    # ---- Opera nominal completeness sanity check ----
    checks.append(_check_opera_codes_present(valid_bank_codes, valid_customer_codes,
                                              valid_supplier_codes, valid_nominal_codes))

    return HealthCheckResult(
        app=APP_NAME,
        healthy=derive_overall_healthy(checks),
        summary=summarise(APP_NAME, checks),
        checks=checks,
        metadata={
            'checked_at': datetime.utcnow().isoformat() + 'Z',
            'opera_bank_count': len(valid_bank_codes),
            'opera_customer_count': len(valid_customer_codes),
            'opera_supplier_count': len(valid_supplier_codes),
            'opera_nominal_count': len(valid_nominal_codes),
        },
    )


# =====================================================================
# Individual checks
# =====================================================================


def _check_bank_aliases(
    aliases_path: str,
    *,
    valid_bank_codes: set[str],
    valid_customer_codes: set[str],
    valid_supplier_codes: set[str],
) -> list[HealthCheckItem]:
    """Inspect bank_aliases.db for orphan code references."""
    items: list[HealthCheckItem] = []

    # Trigger any pending schema migrations before reading. The
    # health check reads SQLite directly (not via BankAliasManager),
    # so any column added by a migration won't exist on a DB that
    # hasn't been touched since the migration shipped. Instantiating
    # BankAliasManager is idempotent and runs the migration if needed.
    try:
        from sql_rag.bank_aliases import BankAliasManager
        BankAliasManager(db_path=aliases_path)
    except Exception as e:
        logger.debug(f"Could not pre-migrate bank_aliases.db: {e}")
        # Continue — the column-tolerant query below will still work
        # even if the migration didn't run.

    # Discover which columns actually exist (defensive: pre-migration
    # DBs lack 'bank_code'; future migrations may add more).
    try:
        cols = _sqlite_columns(aliases_path, 'bank_import_aliases')
    except Exception as e:
        items.append(HealthCheckItem(
            name='Bank aliases',
            description=f'Could not read bank_aliases.db: {e}',
            passed=False,
            severity='error',
        ))
        return items

    # Build SELECT with only the columns we have. account_code +
    # ledger_type + bank_name are required for the orphan checks;
    # bank_code is optional (post-migration).
    required = {'bank_name', 'account_code', 'ledger_type'}
    missing_required = required - cols
    if missing_required:
        items.append(HealthCheckItem(
            name='Bank aliases',
            description=f'bank_import_aliases missing required columns: {sorted(missing_required)}',
            passed=False,
            severity='error',
        ))
        return items

    has_bank_code = 'bank_code' in cols
    select_cols = ['bank_name', 'account_code', 'ledger_type']
    if has_bank_code:
        select_cols.append('bank_code')
    try:
        rows = _read_sqlite(
            aliases_path,
            f"SELECT {', '.join(select_cols)} FROM bank_import_aliases",
        )
    except Exception as e:
        items.append(HealthCheckItem(
            name='Bank aliases',
            description=f'Could not read bank_aliases.db: {e}',
            passed=False,
            severity='error',
        ))
        return items

    if not rows:
        items.append(HealthCheckItem(
            name='Bank aliases',
            description='No aliases learned yet — nothing to check',
            passed=True,
            severity='info',
        ))
        return items

    # Bank-code orphans (only meaningful post-migration when the
    # column exists; pre-migration DBs default-imply 'no bank scope').
    if has_bank_code:
        bank_orphans: list[dict[str, Any]] = []
        for r in rows:
            bc = (r.get('bank_code') or '').strip()
            if bc and bc not in valid_bank_codes:
                if len(bank_orphans) < MAX_ORPHANS_RETURNED:
                    bank_orphans.append({
                        'bank_name': r.get('bank_name'),
                        'bank_code': bc,
                        'reason': f"bank_code '{bc}' not in Opera nbank",
                    })
        bank_orphan_total = sum(
            1 for r in rows
            if (r.get('bank_code') or '').strip()
            and (r.get('bank_code') or '').strip() not in valid_bank_codes
        )
        items.append(HealthCheckItem(
            name='Alias bank codes',
            description='Bank codes used in alias rows must exist in Opera nbank',
            passed=bank_orphan_total == 0,
            total_checked=len(rows),
            orphan_count=bank_orphan_total,
            orphans=bank_orphans,
            severity='warning',
        ))
    else:
        items.append(HealthCheckItem(
            name='Alias bank codes',
            description=(
                'Skipped — bank_aliases.db is pre-migration (no bank_code '
                'column). Migration runs on next BankAliasManager use.'
            ),
            passed=True,
            severity='info',
        ))

    # Customer-code orphans (ledger_type 'C')
    cust_rows = [r for r in rows if (r.get('ledger_type') or '').upper() == 'C']
    cust_orphans: list[dict[str, Any]] = []
    cust_orphan_total = 0
    for r in cust_rows:
        ac = (r.get('account_code') or '').strip()
        if ac and ac not in valid_customer_codes:
            cust_orphan_total += 1
            if len(cust_orphans) < MAX_ORPHANS_RETURNED:
                cust_orphans.append({
                    'bank_name': r.get('bank_name'),
                    'account_code': ac,
                    'reason': f"customer '{ac}' not in Opera sname",
                })
    items.append(HealthCheckItem(
        name='Alias customer codes',
        description='Customer codes (ledger_type C) in aliases must exist in Opera sname',
        passed=cust_orphan_total == 0,
        total_checked=len(cust_rows),
        orphan_count=cust_orphan_total,
        orphans=cust_orphans,
        severity='warning',
    ))

    # Supplier-code orphans (ledger_type 'S')
    sup_rows = [r for r in rows if (r.get('ledger_type') or '').upper() == 'S']
    sup_orphans: list[dict[str, Any]] = []
    sup_orphan_total = 0
    for r in sup_rows:
        ac = (r.get('account_code') or '').strip()
        if ac and ac not in valid_supplier_codes:
            sup_orphan_total += 1
            if len(sup_orphans) < MAX_ORPHANS_RETURNED:
                sup_orphans.append({
                    'bank_name': r.get('bank_name'),
                    'account_code': ac,
                    'reason': f"supplier '{ac}' not in Opera pname",
                })
    items.append(HealthCheckItem(
        name='Alias supplier codes',
        description='Supplier codes (ledger_type S) in aliases must exist in Opera pname',
        passed=sup_orphan_total == 0,
        total_checked=len(sup_rows),
        orphan_count=sup_orphan_total,
        orphans=sup_orphans,
        severity='warning',
    ))

    return items


def _check_bank_patterns(
    patterns_path: str,
    *,
    valid_customer_codes: set[str],
    valid_supplier_codes: set[str],
    valid_nominal_codes: set[str],
) -> list[HealthCheckItem]:
    """Inspect bank_patterns.db for orphan account references."""
    try:
        rows = _read_sqlite(
            patterns_path,
            "SELECT account_code, ledger_type FROM bank_import_patterns "
            "WHERE account_code IS NOT NULL AND account_code != ''"
        )
    except Exception as e:
        logger.debug(f"Pattern table not present or unreadable: {e}")
        return [HealthCheckItem(
            name='Pattern learning',
            description='No learned patterns to check (or table missing — fine for new installs)',
            passed=True,
            severity='info',
        )]

    if not rows:
        return [HealthCheckItem(
            name='Pattern learning',
            description='No patterns learned yet — nothing to check',
            passed=True,
            severity='info',
        )]

    orphans: list[dict[str, Any]] = []
    orphan_total = 0
    for r in rows:
        code = (r.get('account_code') or '').strip()
        ledger = (r.get('ledger_type') or '').upper()
        if not code:
            continue
        valid = (
            (ledger == 'C' and code in valid_customer_codes) or
            (ledger == 'S' and code in valid_supplier_codes) or
            (ledger == 'N' and code in valid_nominal_codes) or
            (not ledger and (
                code in valid_customer_codes
                or code in valid_supplier_codes
                or code in valid_nominal_codes
            ))
        )
        if not valid:
            orphan_total += 1
            if len(orphans) < MAX_ORPHANS_RETURNED:
                orphans.append({
                    'account_code': code,
                    'ledger_type': ledger or '(unset)',
                    'reason': 'Account code not found in any Opera ledger',
                })

    return [HealthCheckItem(
        name='Pattern learning',
        description='Learned-pattern account codes must exist in Opera ledgers',
        passed=orphan_total == 0,
        total_checked=len(rows),
        orphan_count=orphan_total,
        orphans=orphans,
        severity='warning',
    )]


def _check_audit_bank_codes(
    email_path: str,
    valid_bank_codes: set[str],
) -> HealthCheckItem:
    """Bank codes in bank_statement_imports history should still
    exist post-upgrade (otherwise the dedup logic can't match new
    statements against history)."""
    try:
        rows = _read_sqlite(
            email_path,
            "SELECT DISTINCT bank_code FROM bank_statement_imports WHERE bank_code IS NOT NULL"
        )
    except Exception as e:
        logger.debug(f"Could not read bank_statement_imports: {e}")
        return HealthCheckItem(
            name='Statement import history',
            description=f'Skipped — could not read bank_statement_imports ({e})',
            passed=True,
            severity='info',
        )

    if not rows:
        return HealthCheckItem(
            name='Statement import history',
            description='No statement-import history yet — nothing to check',
            passed=True,
            severity='info',
        )

    orphans: list[dict[str, Any]] = []
    for r in rows:
        bc = (r.get('bank_code') or '').strip()
        if bc and bc not in valid_bank_codes:
            if len(orphans) < MAX_ORPHANS_RETURNED:
                orphans.append({
                    'bank_code': bc,
                    'reason': f"bank_code '{bc}' from import history not in current Opera nbank",
                })

    return HealthCheckItem(
        name='Statement import history',
        description='Bank codes in import history must still exist in Opera (for dedup)',
        passed=len(orphans) == 0,
        total_checked=len(rows),
        orphan_count=len(orphans),
        orphans=orphans,
        severity='warning',
    )


def _check_opera_codes_present(
    valid_bank_codes: set[str],
    valid_customer_codes: set[str],
    valid_supplier_codes: set[str],
    valid_nominal_codes: set[str],
) -> HealthCheckItem:
    """Sanity check that Opera even returned any codes — catches
    a fundamentally broken connection."""
    if not valid_bank_codes and not valid_customer_codes:
        return HealthCheckItem(
            name='Opera connection',
            description='Opera returned no bank or customer codes — connection or schema broken',
            passed=False,
            severity='error',
        )
    return HealthCheckItem(
        name='Opera connection',
        description=(
            f'Opera returned {len(valid_bank_codes)} banks, '
            f'{len(valid_customer_codes)} customers, '
            f'{len(valid_supplier_codes)} suppliers, '
            f'{len(valid_nominal_codes)} nominal accounts'
        ),
        passed=True,
        severity='info',
    )


# =====================================================================
# SQL helpers
# =====================================================================


def _fetch_valid_codes(opera_sql, *, table: str, col: str) -> set[str]:
    """Pull the set of valid codes from an Opera table.

    Uses NOLOCK per CLAUDE.md locking rules. Returns an empty set
    on any error so the calling check can report 'Opera connection
    broken' instead of crashing.
    """
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
    """Read SQLite with sqlite3 row dicts. Always returns a list."""
    conn = sqlite3.connect(path)
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(sql)
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _sqlite_columns(path: str, table: str) -> set[str]:
    """Return the set of column names for `table` in the SQLite at
    `path`. Used to build SELECTs that tolerate pre-migration schemas."""
    conn = sqlite3.connect(path)
    try:
        cur = conn.execute(f"PRAGMA table_info({table})")
        return {row[1] for row in cur.fetchall()}
    finally:
        conn.close()
