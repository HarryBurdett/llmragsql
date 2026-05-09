/**
 * Supplier security — alerts and change-detection scan.
 *
 * Faithful ports of:
 *   - list_security_alerts        (routes.py:1574)
 *   - verify_security_alert       (routes.py:1621)
 *   - list_security_audit_log     (routes.py:1657)
 *   - scan_supplier_changes       (routes.py:1706)
 *   - get_flagged_emails          (routes.py:1934 — stub: depends on
 *                                  email_storage tables not in scope here)
 *
 * The scan compares the current `pname` snapshot against the latest
 * `supplier_change_audit` row per (supplier, field) — anything that
 * differs gets a new audit row and (for bank fields) drives an email
 * alert to the security recipients list. First-time observations are
 * recorded as a `scan_baseline` row and auto-verified so they don't
 * appear as alerts.
 */
import type { Knex } from 'knex';

const SENSITIVE_FIELDS = ['pn_bankac', 'pn_banksor', 'pn_email'] as const;
const BANK_FIELDS = new Set(['pn_bankac', 'pn_banksor']);

export interface SupplierSnapshot {
  account: string;
  name: string;
  pn_bankac: string;
  pn_banksor: string;
  pn_email: string;
}

export interface OperaPnameProvider {
  /** Reads pname or pname.dbf — engine-agnostic. */
  snapshot(): Promise<SupplierSnapshot[]>;
  /** Returns supplier display name keyed by code, or {} if not available. */
  resolveNames(codes: string[]): Promise<Record<string, string>>;
}

export interface SecurityEmailSender {
  send(opts: {
    to: string;
    subject: string;
    body: string;
  }): Promise<{ success: boolean; error?: string }>;
}

interface AlertRow {
  id: number;
  supplier_code: string;
  changed_field: string | null;
  old_value: string | null;
  new_value: string | null;
  changed_by: string | null;
  changed_at: string | Date | null;
  verified: number | boolean | null;
}

export interface SecurityAlert {
  id: number;
  supplier_code: string;
  supplier_name: string;
  field: string;
  old_value: string;
  new_value: string;
  changed_by: string;
  changed_at: string;
  verified: boolean;
}

function toIso(value: string | Date | null | undefined): string {
  if (!value) return '';
  if (value instanceof Date) return value.toISOString();
  return String(value);
}

function mapAlert(
  row: AlertRow,
  nameMap: Record<string, string>,
): SecurityAlert {
  return {
    id: Number(row.id),
    supplier_code: row.supplier_code,
    supplier_name: nameMap[row.supplier_code] ?? row.supplier_code,
    field: row.changed_field ?? '',
    old_value: row.old_value ?? '',
    new_value: row.new_value ?? '',
    changed_by: row.changed_by ?? '',
    changed_at: toIso(row.changed_at),
    verified: !!row.verified,
  };
}

// ---------------------------------------------------------------------
// list alerts (unverified)
// ---------------------------------------------------------------------

export interface AlertsResponse {
  success: boolean;
  alerts: SecurityAlert[];
  count: number;
  error?: string;
}

export async function listSecurityAlerts(
  appDb: Knex,
  pnameProvider: OperaPnameProvider,
): Promise<AlertsResponse> {
  try {
    const rows = (await appDb('supplier_change_audit')
      .where({ verified: false })
      .orderBy('changed_at', 'desc')) as unknown as AlertRow[];
    const codes = Array.from(
      new Set(rows.map((r) => r.supplier_code).filter(Boolean)),
    );
    const nameMap = await pnameProvider.resolveNames(codes);
    return {
      success: true,
      alerts: rows.map((r) => mapAlert(r, nameMap)),
      count: rows.length,
    };
  } catch (err: any) {
    return {
      success: false,
      alerts: [],
      count: 0,
      error: err?.message ?? String(err),
    };
  }
}

// ---------------------------------------------------------------------
// verify alert
// ---------------------------------------------------------------------

export interface VerifyAlertResponse {
  success: boolean;
  message?: string;
  error?: string;
}

export async function verifySecurityAlert(
  appDb: Knex,
  alertId: number,
  verifiedBy: string,
): Promise<VerifyAlertResponse> {
  if (!Number.isFinite(alertId) || alertId <= 0) {
    return { success: false, error: 'Invalid alert_id' };
  }
  try {
    const updated = await appDb('supplier_change_audit')
      .where({ id: alertId })
      .update({
        verified: true,
        verified_by: verifiedBy,
        verified_at: appDb.fn.now(),
      });
    if (!updated) return { success: false, error: 'Alert not found' };
    return { success: true, message: 'Alert verified' };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}

// ---------------------------------------------------------------------
// audit log (everything in window, regardless of verified)
// ---------------------------------------------------------------------

export interface AuditResponse {
  success: boolean;
  entries: SecurityAlert[];
  count: number;
  error?: string;
}

export async function listSecurityAuditLog(
  appDb: Knex,
  pnameProvider: OperaPnameProvider,
  days = 90,
): Promise<AuditResponse> {
  try {
    const cutoff = new Date(
      Date.now() - days * 24 * 60 * 60 * 1000,
    ).toISOString();
    const rows = (await appDb('supplier_change_audit')
      .where('changed_at', '>=', cutoff)
      .orderBy('changed_at', 'desc')) as unknown as AlertRow[];
    const codes = Array.from(
      new Set(rows.map((r) => r.supplier_code).filter(Boolean)),
    );
    const nameMap = await pnameProvider.resolveNames(codes);
    return {
      success: true,
      entries: rows.map((r) => mapAlert(r, nameMap)),
      count: rows.length,
    };
  } catch (err: any) {
    return {
      success: false,
      entries: [],
      count: 0,
      error: err?.message ?? String(err),
    };
  }
}

// ---------------------------------------------------------------------
// scan supplier changes
// ---------------------------------------------------------------------

export interface BankChange {
  account: string;
  name: string;
  field: string;
  old: string;
  new: string;
}

export interface ScanResponse {
  success: boolean;
  changes_detected: number;
  alerts_sent: number;
  bank_changes: BankChange[];
  error?: string;
}

interface LastKnownRow {
  supplier_code: string;
  changed_field: string | null;
  new_value: string | null;
}

async function loadLastKnown(
  appDb: Knex,
): Promise<Map<string, string>> {
  const subquery = appDb('supplier_change_audit')
    .select('supplier_code', 'changed_field')
    .max({ id: 'id' })
    .groupBy('supplier_code', 'changed_field');
  // Knex doesn't have a portable way to express "row whose id is the
  // max in its group" without a subquery; emulate by iterating the
  // grouped maxes and resolving to rows.
  const groups = (await subquery) as unknown as Array<{
    supplier_code: string;
    changed_field: string | null;
    id: number | null;
  }>;
  const ids = groups.map((g) => g.id).filter((n): n is number => !!n);
  if (ids.length === 0) return new Map();
  const rows = (await appDb('supplier_change_audit')
    .whereIn('id', ids)
    .select(
      'supplier_code',
      'changed_field',
      'new_value',
    )) as unknown as LastKnownRow[];
  const map = new Map<string, string>();
  for (const row of rows) {
    if (!row.supplier_code || !row.changed_field) continue;
    map.set(`${row.supplier_code}:${row.changed_field}`, row.new_value ?? '');
  }
  return map;
}

async function loadAutomationSetting(
  appDb: Knex,
  key: string,
): Promise<string | null> {
  try {
    const row = (await appDb('supplier_automation_settings')
      .where({ key })
      .first()) as { value?: string } | undefined;
    return row?.value ?? null;
  } catch {
    return null;
  }
}

export async function scanSupplierChanges(
  appDb: Knex,
  pnameProvider: OperaPnameProvider,
  email: SecurityEmailSender,
): Promise<ScanResponse> {
  try {
    const suppliers = await pnameProvider.snapshot();
    if (suppliers.length === 0) {
      return {
        success: true,
        changes_detected: 0,
        alerts_sent: 0,
        bank_changes: [],
      };
    }

    const lastKnown = await loadLastKnown(appDb);
    let changesDetected = 0;
    const bankChanges: BankChange[] = [];

    for (const supplier of suppliers) {
      const account = (supplier.account ?? '').trim();
      if (!account) continue;
      for (const field of SENSITIVE_FIELDS) {
        const current = (supplier[field] ?? '').toString().trim();
        const key = `${account}:${field}`;
        const previous = lastKnown.get(key);

        if (previous !== undefined) {
          if (current !== previous) {
            await appDb('supplier_change_audit').insert({
              supplier_code: account,
              changed_field: field,
              old_value: previous,
              new_value: current,
              changed_by: 'scan',
              changed_at: appDb.fn.now(),
              verified: false,
            });
            changesDetected += 1;
            if (BANK_FIELDS.has(field)) {
              bankChanges.push({
                account,
                name: supplier.name || account,
                field,
                old: previous,
                new: current,
              });
            }
          }
        } else if (current) {
          // First-time observation — write a baseline row already
          // verified so it doesn't appear as an alert.
          await appDb('supplier_change_audit').insert({
            supplier_code: account,
            changed_field: field,
            old_value: '',
            new_value: current,
            changed_by: 'scan_baseline',
            changed_at: appDb.fn.now(),
            verified: true,
            verified_by: 'scan_baseline',
            verified_at: appDb.fn.now(),
          });
        }
      }
    }

    let alertsSent = 0;
    if (bankChanges.length > 0) {
      const recipientsRaw = await loadAutomationSetting(
        appDb,
        'security_alert_recipients',
      );
      const recipients = (recipientsRaw ?? '')
        .split(',')
        .map((s) => s.trim())
        .filter((s) => s.length > 0);
      if (recipients.length > 0) {
        const lines: string[] = [
          'SECURITY ALERT: Supplier Bank Detail Changes Detected',
          '',
          `Scan time: ${new Date().toISOString()}`,
          `Changes detected: ${bankChanges.length}`,
          '-'.repeat(60),
        ];
        for (const c of bankChanges) {
          lines.push('');
          lines.push(`Supplier: ${c.name} (${c.account})`);
          lines.push(`  Field: ${c.field}`);
          lines.push(`  Old value: ${c.old || '(empty)'}`);
          lines.push(`  New value: ${c.new || '(empty)'}`);
        }
        lines.push('');
        lines.push('-'.repeat(60));
        lines.push('Please verify these changes are legitimate.');
        const body = lines.join('\n');
        const subject = `SECURITY ALERT: ${bankChanges.length} Supplier Bank Detail Change(s)`;
        for (const recipient of recipients) {
          const r = await email.send({ to: recipient, subject, body });
          if (r.success) alertsSent += 1;
        }
      }
    }

    return {
      success: true,
      changes_detected: changesDetected,
      alerts_sent: alertsSent,
      bank_changes: bankChanges,
    };
  } catch (err: any) {
    return {
      success: false,
      changes_detected: 0,
      alerts_sent: 0,
      bank_changes: [],
      error: err?.message ?? String(err),
    };
  }
}
