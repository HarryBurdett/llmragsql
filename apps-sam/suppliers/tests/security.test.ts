import { describe, it, expect, vi } from 'vitest';
import {
  listSecurityAlerts,
  verifySecurityAlert,
  listSecurityAuditLog,
  scanSupplierChanges,
  type OperaPnameProvider,
  type SecurityEmailSender,
  type SupplierSnapshot,
} from '../src/services/security.js';

interface AuditRow {
  id: number;
  supplier_code: string;
  changed_field: string;
  old_value: string;
  new_value: string;
  changed_by: string;
  changed_at: string;
  verified: boolean;
  verified_by?: string | null;
  verified_at?: string | null;
}

interface State {
  audit: AuditRow[];
  settings: Record<string, string>;
  nextId: number;
}

function makeAppDb(state: State): any {
  function tableBuilder(table: string) {
    let mode: 'list' | 'verify-update' | 'count' | 'group' | 'where-in' = 'list';
    let verifiedFilter: boolean | null = null;
    let cutoff: string | null = null;
    let idFilter: number | null = null;
    let idsFilter: number[] | null = null;
    let groupCols: string[] = [];
    let aggMaxOf: string | null = null;
    let keyFilter: string | null = null;

    const builder: any = {
      where: (cond: any, op?: any, val?: any) => {
        if (typeof cond === 'object') {
          if ('verified' in cond) verifiedFilter = cond.verified;
          if ('id' in cond) idFilter = cond.id;
          if ('key' in cond) keyFilter = cond.key;
        }
        if (typeof cond === 'string' && cond === 'changed_at' && op === '>=') {
          cutoff = val;
        }
        return builder;
      },
      whereIn: (col: string, vals: number[]) => {
        if (col === 'id') idsFilter = vals;
        return builder;
      },
      orderBy: () => builder,
      groupBy: (...cols: string[]) => {
        groupCols = cols;
        mode = 'group';
        return builder;
      },
      max: (alias: any) => {
        aggMaxOf = typeof alias === 'object' ? Object.values(alias)[0] : alias;
        return builder;
      },
      select: () => builder,
      first: async () => {
        if (table === 'supplier_automation_settings' && keyFilter) {
          const v = state.settings[keyFilter];
          return v !== undefined ? { value: v } : undefined;
        }
        return undefined;
      },
      update: async (payload: any) => {
        if (table === 'supplier_change_audit' && idFilter !== null) {
          const idx = state.audit.findIndex((a) => a.id === idFilter);
          if (idx < 0) return 0;
          state.audit[idx] = { ...state.audit[idx]!, ...payload };
          return 1;
        }
        return 0;
      },
      insert: async (payload: any) => {
        if (table === 'supplier_change_audit') {
          const id = state.nextId++;
          state.audit.push({
            id,
            supplier_code: payload.supplier_code,
            changed_field: payload.changed_field,
            old_value: payload.old_value ?? '',
            new_value: payload.new_value ?? '',
            changed_by: payload.changed_by,
            changed_at: new Date().toISOString(),
            verified: !!payload.verified,
            verified_by: payload.verified_by ?? null,
            verified_at: payload.verified_at ?? null,
          });
          return [id];
        }
        return [1];
      },
      then: async (resolve: any) => {
        if (table !== 'supplier_change_audit') return resolve([]);
        if (mode === 'group' && groupCols.length > 0 && aggMaxOf === 'id') {
          const seen = new Map<string, number>();
          for (const r of state.audit) {
            const key = `${r.supplier_code}:${r.changed_field}`;
            const cur = seen.get(key) ?? 0;
            if (r.id > cur) seen.set(key, r.id);
          }
          const out = Array.from(seen.entries()).map(([key, id]) => {
            const parts = key.split(':');
            return {
              supplier_code: parts[0]!,
              changed_field: parts[1]!,
              id,
            };
          });
          return resolve(out);
        }
        if (idsFilter && idsFilter.length > 0) {
          const out = state.audit
            .filter((r) => idsFilter!.includes(r.id))
            .map((r) => ({
              supplier_code: r.supplier_code,
              changed_field: r.changed_field,
              new_value: r.new_value,
            }));
          return resolve(out);
        }
        if (verifiedFilter !== null) {
          return resolve(state.audit.filter((r) => r.verified === verifiedFilter));
        }
        if (cutoff !== null) {
          return resolve(state.audit.filter((r) => r.changed_at >= cutoff!));
        }
        return resolve(state.audit);
      },
    };
    return builder;
  }
  const db: any = (table: string) => tableBuilder(table);
  db.fn = { now: () => '__NOW__' };
  return db;
}

const pnameProvider: OperaPnameProvider = {
  snapshot: async () => [],
  resolveNames: async (codes) => {
    const out: Record<string, string> = {};
    for (const c of codes) out[c] = `Acme ${c}`;
    return out;
  },
};

// ---------------------------------------------------------------------
// listSecurityAlerts
// ---------------------------------------------------------------------

describe('listSecurityAlerts', () => {
  it('returns unverified alerts only and resolves names', async () => {
    const state: State = {
      audit: [
        {
          id: 1,
          supplier_code: 'A001',
          changed_field: 'pn_bankac',
          old_value: '11111111',
          new_value: '22222222',
          changed_by: 'scan',
          changed_at: '2026-04-15T10:00:00Z',
          verified: false,
        },
        {
          id: 2,
          supplier_code: 'A002',
          changed_field: 'pn_email',
          old_value: 'a@x.com',
          new_value: 'b@x.com',
          changed_by: 'scan',
          changed_at: '2026-04-14T10:00:00Z',
          verified: true,
        },
      ],
      settings: {},
      nextId: 3,
    };
    const result = await listSecurityAlerts(makeAppDb(state), pnameProvider);
    expect(result.success).toBe(true);
    expect(result.count).toBe(1);
    expect(result.alerts[0]?.id).toBe(1);
    expect(result.alerts[0]?.supplier_name).toBe('Acme A001');
  });
});

// ---------------------------------------------------------------------
// verifySecurityAlert
// ---------------------------------------------------------------------

describe('verifySecurityAlert', () => {
  it('marks an alert verified', async () => {
    const state: State = {
      audit: [
        {
          id: 1,
          supplier_code: 'A001',
          changed_field: 'pn_bankac',
          old_value: '',
          new_value: '22222222',
          changed_by: 'scan',
          changed_at: '2026-04-15T10:00:00Z',
          verified: false,
        },
      ],
      settings: {},
      nextId: 2,
    };
    const result = await verifySecurityAlert(makeAppDb(state), 1, 'admin');
    expect(result.success).toBe(true);
    expect(state.audit[0]?.verified).toBe(true);
    expect(state.audit[0]?.verified_by).toBe('admin');
  });

  it('returns error on missing id', async () => {
    const result = await verifySecurityAlert(
      makeAppDb({ audit: [], settings: {}, nextId: 1 }),
      999,
      'admin',
    );
    expect(result.success).toBe(false);
  });
});

// ---------------------------------------------------------------------
// listSecurityAuditLog
// ---------------------------------------------------------------------

describe('listSecurityAuditLog', () => {
  it('lists entries within window', async () => {
    const recent = new Date().toISOString();
    const old = new Date(
      Date.now() - 365 * 24 * 60 * 60 * 1000,
    ).toISOString();
    const state: State = {
      audit: [
        {
          id: 1,
          supplier_code: 'A',
          changed_field: 'pn_bankac',
          old_value: '',
          new_value: 'new',
          changed_by: 'scan',
          changed_at: recent,
          verified: false,
        },
        {
          id: 2,
          supplier_code: 'B',
          changed_field: 'pn_email',
          old_value: '',
          new_value: 'new',
          changed_by: 'scan',
          changed_at: old,
          verified: true,
        },
      ],
      settings: {},
      nextId: 3,
    };
    const result = await listSecurityAuditLog(
      makeAppDb(state),
      pnameProvider,
      90,
    );
    expect(result.count).toBe(1);
    expect(result.entries[0]?.id).toBe(1);
  });
});

// ---------------------------------------------------------------------
// scanSupplierChanges
// ---------------------------------------------------------------------

describe('scanSupplierChanges', () => {
  it('records baseline rows for first-time observations and auto-verifies them', async () => {
    const state: State = {
      audit: [],
      settings: {},
      nextId: 1,
    };
    const provider: OperaPnameProvider = {
      snapshot: async () =>
        [
          {
            account: 'A001',
            name: 'Acme',
            pn_bankac: '12345678',
            pn_banksor: '20-00-00',
            pn_email: 'ap@acme.com',
          },
        ] satisfies SupplierSnapshot[],
      resolveNames: pnameProvider.resolveNames,
    };
    const email: SecurityEmailSender = { send: vi.fn() };
    const result = await scanSupplierChanges(makeAppDb(state), provider, email);
    expect(result.success).toBe(true);
    expect(result.changes_detected).toBe(0);
    expect(state.audit.length).toBe(3);
    expect(state.audit.every((a) => a.verified)).toBe(true);
    expect(email.send).not.toHaveBeenCalled();
  });

  it('detects bank-detail change and emails recipients', async () => {
    const state: State = {
      audit: [
        {
          id: 1,
          supplier_code: 'A001',
          changed_field: 'pn_bankac',
          old_value: '',
          new_value: '11111111',
          changed_by: 'scan_baseline',
          changed_at: '2026-04-01T00:00:00Z',
          verified: true,
        },
        {
          id: 2,
          supplier_code: 'A001',
          changed_field: 'pn_banksor',
          old_value: '',
          new_value: '20-00-00',
          changed_by: 'scan_baseline',
          changed_at: '2026-04-01T00:00:00Z',
          verified: true,
        },
        {
          id: 3,
          supplier_code: 'A001',
          changed_field: 'pn_email',
          old_value: '',
          new_value: 'ap@acme.com',
          changed_by: 'scan_baseline',
          changed_at: '2026-04-01T00:00:00Z',
          verified: true,
        },
      ],
      settings: { security_alert_recipients: 'security@example.com' },
      nextId: 4,
    };
    const provider: OperaPnameProvider = {
      snapshot: async () => [
        {
          account: 'A001',
          name: 'Acme',
          pn_bankac: '99999999', // changed
          pn_banksor: '20-00-00',
          pn_email: 'ap@acme.com',
        },
      ],
      resolveNames: pnameProvider.resolveNames,
    };
    const send = vi.fn().mockResolvedValue({ success: true });
    const email: SecurityEmailSender = { send };
    const result = await scanSupplierChanges(makeAppDb(state), provider, email);
    expect(result.success).toBe(true);
    expect(result.changes_detected).toBe(1);
    expect(result.bank_changes.length).toBe(1);
    expect(result.alerts_sent).toBe(1);
    expect(send).toHaveBeenCalledTimes(1);
  });

  it('skips email send when no security recipients configured', async () => {
    const state: State = {
      audit: [
        {
          id: 1,
          supplier_code: 'A001',
          changed_field: 'pn_bankac',
          old_value: '',
          new_value: '11111111',
          changed_by: 'scan_baseline',
          changed_at: '2026-04-01T00:00:00Z',
          verified: true,
        },
      ],
      settings: {},
      nextId: 2,
    };
    const provider: OperaPnameProvider = {
      snapshot: async () => [
        {
          account: 'A001',
          name: 'Acme',
          pn_bankac: '99999999',
          pn_banksor: '',
          pn_email: '',
        },
      ],
      resolveNames: pnameProvider.resolveNames,
    };
    const send = vi.fn();
    const email: SecurityEmailSender = { send };
    const result = await scanSupplierChanges(makeAppDb(state), provider, email);
    expect(result.alerts_sent).toBe(0);
    expect(send).not.toHaveBeenCalled();
    expect(result.changes_detected).toBe(1);
  });

  it('records nothing when supplier list is empty', async () => {
    const state: State = { audit: [], settings: {}, nextId: 1 };
    const provider: OperaPnameProvider = {
      snapshot: async () => [],
      resolveNames: pnameProvider.resolveNames,
    };
    const email: SecurityEmailSender = { send: vi.fn() };
    const result = await scanSupplierChanges(makeAppDb(state), provider, email);
    expect(result.success).toBe(true);
    expect(result.changes_detected).toBe(0);
    expect(state.audit.length).toBe(0);
  });
});
