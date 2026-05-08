import { describe, it, expect } from 'vitest';
import { listSubscriptions } from '../src/services/subscriptions.js';

interface SubRow {
  id: number;
  subscription_id: string;
  mandate_id: string;
  opera_account: string;
  amount: number;
  frequency: string;
  status: string;
  created_at: string;
  updated_at: string;
}

interface MandateRow {
  opera_account: string;
  opera_name: string;
}

interface MockState {
  subs: SubRow[];
  mandates: MandateRow[];
}

function makeAppDb(state: MockState): any {
  const db: any = (table: string) => {
    let conds: Record<string, unknown> = {};
    let inCol: string | null = null;
    let inVals: unknown[] | null = null;
    let limitN = Infinity;
    let order: { col: string; dir: 'asc' | 'desc' } | null = null;
    if (table === 'gocardless_subscriptions') {
      const builder: any = {
        where: (cond: Record<string, unknown>) => {
          Object.assign(conds, cond);
          return builder;
        },
        orderBy: (col: string, dir: 'asc' | 'desc' = 'asc') => {
          order = { col, dir };
          return builder;
        },
        limit: (n: number) => {
          limitN = n;
          return builder;
        },
        then: (cb: (rows: SubRow[]) => unknown) => {
          let rows = state.subs.filter((r) =>
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
          return Promise.resolve(cb(rows.slice(0, limitN)));
        },
      };
      return builder;
    }
    if (table === 'gocardless_mandates') {
      const builder: any = {
        whereIn: (col: string, vals: unknown[]) => {
          inCol = col;
          inVals = vals;
          return builder;
        },
        select: (..._cols: string[]) => {
          const rows = state.mandates.filter(
            (m) =>
              !inCol || (inVals && inVals.includes((m as any)[inCol!])),
          );
          return Promise.resolve(rows);
        },
      };
      return builder;
    }
    throw new Error(`Unexpected table: ${table}`);
  };
  db.fn = { now: () => new Date() };
  return db;
}

function emptySub(over: Partial<SubRow> = {}): SubRow {
  return {
    id: 1,
    subscription_id: 'SUB_X',
    mandate_id: 'MD_X',
    opera_account: 'CUST01',
    amount: 100,
    frequency: 'M',
    status: 'active',
    created_at: '2026-04-15T10:00:00Z',
    updated_at: '2026-04-15T10:00:00Z',
    ...over,
  };
}

describe('listSubscriptions', () => {
  it('returns subscriptions in created_at desc order', async () => {
    const state: MockState = {
      subs: [
        emptySub({ id: 1, created_at: '2026-04-10T10:00:00Z' }),
        emptySub({ id: 2, created_at: '2026-04-15T10:00:00Z' }),
      ],
      mandates: [],
    };
    const result = await listSubscriptions(makeAppDb(state));
    expect(result.success).toBe(true);
    expect(result.count).toBe(2);
    expect(result.subscriptions[0]?.id).toBe(2);
  });

  it('filters by status', async () => {
    const state: MockState = {
      subs: [
        emptySub({ id: 1, status: 'active' }),
        emptySub({ id: 2, status: 'cancelled' }),
      ],
      mandates: [],
    };
    const result = await listSubscriptions(makeAppDb(state), {
      status: 'cancelled',
    });
    expect(result.count).toBe(1);
  });

  it('enriches with customer_name from mandates', async () => {
    const state: MockState = {
      subs: [emptySub({ opera_account: 'CUST01' })],
      mandates: [{ opera_account: 'CUST01', opera_name: 'Acme Ltd' }],
    };
    const result = await listSubscriptions(makeAppDb(state));
    expect(result.subscriptions[0]?.customer_name).toBe('Acme Ltd');
  });

  it('falls back to opera_account when no mandate', async () => {
    const state: MockState = {
      subs: [emptySub({ opera_account: 'CUST99' })],
      mandates: [],
    };
    const result = await listSubscriptions(makeAppDb(state));
    expect(result.subscriptions[0]?.customer_name).toBe('CUST99');
  });

  it('respects limit', async () => {
    const state: MockState = {
      subs: Array.from({ length: 5 }, (_, i) =>
        emptySub({ id: i + 1, created_at: `2026-04-${10 + i}` }),
      ),
      mandates: [],
    };
    const result = await listSubscriptions(makeAppDb(state), { limit: 2 });
    expect(result.count).toBe(2);
  });
});
