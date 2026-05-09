/**
 * Suppliers data-integrity health check.
 *
 * Faithful port of `apps/suppliers/logic/health_check.py`. Verifies
 * the suppliers app's local data still references valid Opera codes:
 *   - Supplier codes in our app DB → exist in Opera pname?
 *
 * Storage difference (not a behavioural amendment): the Python
 * version reads supplier_statements.db SQLite; we read the per-app
 * MSSQL tables (supplier_statements / supplier_config) provisioned
 * by the suppliers migration.
 */
import type { Knex } from 'knex';

const APP_NAME = 'suppliers';
const MAX_ORPHANS_RETURNED = 50;

export interface HealthCheckItem {
  name: string;
  description: string;
  passed: boolean;
  total_checked?: number;
  orphan_count?: number;
  orphans?: Array<Record<string, unknown>>;
  severity: 'info' | 'warning' | 'error';
}

export interface HealthCheckResult {
  app: string;
  healthy: boolean;
  summary: string;
  checks: HealthCheckItem[];
  metadata: Record<string, unknown>;
}

function deriveOverallHealthy(checks: HealthCheckItem[]): boolean {
  return checks.every((c) => c.passed || c.severity !== 'error');
}

function summarise(app: string, checks: HealthCheckItem[]): string {
  const errors = checks.filter((c) => !c.passed && c.severity === 'error').length;
  const warnings = checks.filter(
    (c) => !c.passed && c.severity === 'warning',
  ).length;
  if (errors === 0 && warnings === 0) {
    return `${app}: all checks passed`;
  }
  return `${app}: ${errors} error(s), ${warnings} warning(s)`;
}

async function fetchValidSupplierCodes(operaDb: Knex): Promise<Set<string>> {
  try {
    const rows = (await operaDb.raw(
      'SELECT RTRIM(pn_account) AS code FROM pname WITH (NOLOCK)',
    )) as unknown as Array<{ code: string | null }> | { rows: Array<{ code: string | null }> };
    const list = Array.isArray(rows)
      ? rows
      : Array.isArray((rows as any)?.rows)
        ? (rows as any).rows
        : [];
    const out = new Set<string>();
    for (const r of list) {
      const code = (r?.code ?? '').toString().trim();
      if (code) out.add(code);
    }
    return out;
  } catch {
    return new Set();
  }
}

async function fetchLocalSupplierCodes(
  appDb: Knex | null | undefined,
): Promise<{ rows: Array<{ supplier_code: string }>; source: 'statements' | 'config' | 'none' }> {
  if (!appDb) return { rows: [], source: 'none' };
  try {
    const rows = (await appDb('supplier_statements')
      .whereNotNull('supplier_code')
      .distinct('supplier_code')
      .select('supplier_code')) as unknown as Array<{
      supplier_code: string | null;
    }>;
    const cleaned = (rows ?? [])
      .map((r) => ({ supplier_code: (r.supplier_code ?? '').trim() }))
      .filter((r) => r.supplier_code);
    if (cleaned.length > 0) {
      return { rows: cleaned, source: 'statements' };
    }
  } catch {
    // table may not exist yet
  }
  try {
    const rows = (await appDb('supplier_config')
      .whereNotNull('supplier_code')
      .distinct('supplier_code')
      .select('supplier_code')) as unknown as Array<{
      supplier_code: string | null;
    }>;
    const cleaned = (rows ?? [])
      .map((r) => ({ supplier_code: (r.supplier_code ?? '').trim() }))
      .filter((r) => r.supplier_code);
    return { rows: cleaned, source: 'config' };
  } catch {
    return { rows: [], source: 'none' };
  }
}

export async function runSuppliersHealthCheck(opts: {
  operaDb: Knex;
  appDb?: Knex | null;
}): Promise<HealthCheckResult> {
  const checks: HealthCheckItem[] = [];

  const validCodes = await fetchValidSupplierCodes(opts.operaDb);

  // ---- Supplier codes in local data ----
  const local = await fetchLocalSupplierCodes(opts.appDb ?? null);
  if (local.source === 'none' || local.rows.length === 0) {
    checks.push({
      name: 'Supplier statement history',
      description:
        local.source === 'none'
          ? 'Skipped — no local supplier tables provisioned'
          : 'No supplier statement history yet — nothing to check',
      passed: true,
      severity: 'info',
    });
  } else {
    const orphans: Array<Record<string, unknown>> = [];
    let orphanTotal = 0;
    for (const row of local.rows) {
      const code = row.supplier_code;
      if (code && !validCodes.has(code)) {
        orphanTotal += 1;
        if (orphans.length < MAX_ORPHANS_RETURNED) {
          orphans.push({
            supplier_code: code,
            reason: `supplier '${code}' from local data not in Opera pname`,
          });
        }
      }
    }
    checks.push({
      name: 'Supplier statement history',
      description:
        'Supplier codes referenced in local data must exist in Opera pname',
      passed: orphanTotal === 0,
      total_checked: local.rows.length,
      orphan_count: orphanTotal,
      orphans,
      severity: 'warning',
    });
  }

  // ---- Opera connection sanity ----
  if (validCodes.size === 0) {
    checks.push({
      name: 'Opera connection',
      description:
        'Opera returned no supplier codes — connection or schema broken',
      passed: false,
      severity: 'error',
    });
  } else {
    checks.push({
      name: 'Opera connection',
      description: `Opera returned ${validCodes.size} suppliers`,
      passed: true,
      severity: 'info',
    });
  }

  return {
    app: APP_NAME,
    healthy: deriveOverallHealthy(checks),
    summary: summarise(APP_NAME, checks),
    checks,
    metadata: {
      checked_at: new Date().toISOString(),
      opera_supplier_count: validCodes.size,
    },
  };
}
