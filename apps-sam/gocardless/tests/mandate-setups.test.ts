import { describe, it, expect } from 'vitest';
import {
  listMandateSetups,
  cancelMandateSetup,
} from '../src/services/mandate-setups.js';

interface SetupRow {
  id: number;
  opera_account: string;
  opera_name: string;
  customer_email: string;
  billing_request_id: string;
  billing_request_flow_id: string;
  authorisation_url: string;
  mandate_id: string;
  gocardless_customer_id: string;
  status: string;
  status_detail: string;
  email_sent_at: string | null;
  mandate_active_at: string | null;
  created_at: string;
  updated_at: string;
}

interface MockState {
  rows: SetupRow[];
}

function makeAppDb(state: MockState): any {
  const db: any = (table: string) => {
    if (table !== 'mandate_setup_requests') {
      throw new Error(`Unexpected table: ${table}`);
    }
    let conds: Record<string, unknown> = {};
    let order: { col: string; dir: 'asc' | 'desc' } | null = null;
    const builder: any = {
      where: (cond: Record<string, unknown>) => {
        Object.assign(conds, cond);
        return builder;
      },
      orderBy: (col: string, dir: 'asc' | 'desc' = 'asc') => {
        order = { col, dir };
        return builder;
      },
      first: () => {
        const found = state.rows.find((r) =>
          Object.entries(conds).every(([k, v]) => (r as any)[k] === v),
        );
        return Promise.resolve(found);
      },
      update: (data: Record<string, unknown>) => {
        let count = 0;
        for (const r of state.rows) {
          if (
            Object.entries(conds).every(([k, v]) => (r as any)[k] === v)
          ) {
            Object.assign(r, data);
            count++;
          }
        }
        return Promise.resolve(count);
      },
      then: (cb: (rows: SetupRow[]) => unknown) => {
        let rows = state.rows.filter((r) =>
          Object.entries(conds).every(([k, v]) => (r as any)[k] === v),
        );
        if (order) {
          rows = [...rows].sort((a, b) => {
            const cmp = String((a as any)[order!.col]).localeCompare(
              String((b as any)[order!.col]),
            );
            return order!.dir === 'desc' ? -cmp : cmp;
          });
        }
        return Promise.resolve(cb(rows));
      },
    };
    return builder;
  };
  db.fn = { now: () => new Date() };
  return db;
}

function emptySetup(over: Partial<SetupRow> = {}): SetupRow {
  return {
    id: 1,
    opera_account: 'CUST01',
    opera_name: 'Acme Ltd',
    customer_email: 'a@a.com',
    billing_request_id: 'BR1',
    billing_request_flow_id: '',
    authorisation_url: '',
    mandate_id: '',
    gocardless_customer_id: '',
    status: 'pending',
    status_detail: '',
    email_sent_at: null,
    mandate_active_at: null,
    created_at: '2026-04-15',
    updated_at: '2026-04-15',
    ...over,
  };
}

describe('listMandateSetups', () => {
  it('returns rows in id-desc order', async () => {
    const state: MockState = {
      rows: [
        emptySetup({ id: 1 }),
        emptySetup({ id: 2 }),
        emptySetup({ id: 3 }),
      ],
    };
    const result = await listMandateSetups(makeAppDb(state));
    expect(result.success).toBe(true);
    expect(result.setups[0]?.id).toBe(3);
  });

  it('counts pending (excludes completed/failed/cancelled)', async () => {
    const state: MockState = {
      rows: [
        emptySetup({ id: 1, status: 'pending' }),
        emptySetup({ id: 2, status: 'completed' }),
        emptySetup({ id: 3, status: 'cancelled' }),
        emptySetup({ id: 4, status: 'pending' }),
        emptySetup({ id: 5, status: 'failed' }),
      ],
    };
    const result = await listMandateSetups(makeAppDb(state));
    expect(result.pending_count).toBe(2);
  });
});

describe('cancelMandateSetup', () => {
  it('cancels a pending setup', async () => {
    const state: MockState = {
      rows: [emptySetup({ id: 1, status: 'pending' })],
    };
    const result = await cancelMandateSetup(makeAppDb(state), 1);
    expect(result.success).toBe(true);
    expect(result.message).toMatch(/Acme Ltd/);
    expect(state.rows[0]?.status).toBe('cancelled');
    expect(state.rows[0]?.status_detail).toBe('Cancelled by user');
  });

  it('refuses to cancel a completed setup', async () => {
    const state: MockState = {
      rows: [emptySetup({ id: 1, status: 'completed' })],
    };
    const result = await cancelMandateSetup(makeAppDb(state), 1);
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/already completed/);
  });

  it('returns 404 when setup not found', async () => {
    const state: MockState = { rows: [] };
    const result = await cancelMandateSetup(makeAppDb(state), 999);
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/not found/);
  });

  it('rejects bad setup_id', async () => {
    const state: MockState = { rows: [] };
    const result = await cancelMandateSetup(makeAppDb(state), 0);
    expect(result.success).toBe(false);
  });

  it('falls back to opera_account when opera_name empty', async () => {
    const state: MockState = {
      rows: [emptySetup({ id: 1, opera_name: '', opera_account: 'CUST_X' })],
    };
    const result = await cancelMandateSetup(makeAppDb(state), 1);
    expect(result.message).toMatch(/CUST_X/);
  });
});
