import { describe, it, expect } from 'vitest';
import {
  listSubscriptions,
  getSubscription,
  pauseSubscription,
  resumeSubscription,
  cancelSubscription,
  updateSubscriptionDetails,
  linkSubscriptionToDocument,
  unlinkSubscriptionFromDocument,
  updateSubscriptionStatus,
  syncSubscriptionFromOpera,
  type RemoteSubscriptionResult,
} from '../src/services/subscriptions.js';

// ---------------------------------------------------------------------
// Mock app DB — covers the Knex shapes used by subscriptions.ts.
// ---------------------------------------------------------------------

interface SubRow {
  id: number;
  subscription_id: string;
  mandate_id: string;
  opera_account: string | null;
  opera_name: string | null;
  source_doc: string | null;
  amount_pence: number;
  currency: string;
  interval_unit: string;
  interval_count: number;
  day_of_month: number | null;
  name: string | null;
  status: string;
  start_date: string | null;
  end_date: string | null;
  created_at: string;
  updated_at: string;
  synced_at: string | null;
}

interface MandateRow {
  opera_account: string;
  opera_name: string;
}

interface SubDocRow {
  subscription_id: string;
  source_doc: string;
  added_at: string;
}

interface MockState {
  subs: SubRow[];
  mandates: MandateRow[];
  docs: SubDocRow[];
}

function makeAppDb(state: MockState): any {
  function table(name: string) {
    if (name === 'gocardless_subscriptions') {
      return makeSubsBuilder(state);
    }
    if (name === 'gocardless_mandates') {
      return makeMandateBuilder(state);
    }
    if (name === 'gocardless_subscription_documents') {
      return makeDocsBuilder(state);
    }
    throw new Error(`Unexpected table: ${name}`);
  }
  table.fn = { now: () => '__NOW__' };
  return table;
}

function applyConds(rows: SubRow[], conds: Record<string, unknown>): SubRow[] {
  return rows.filter((r) =>
    Object.entries(conds).every(([k, v]) => (r as any)[k] === v),
  );
}

function makeSubsBuilder(state: MockState): any {
  let rows: SubRow[] = [...state.subs];
  let conds: Record<string, unknown> = {};
  let notConds: Record<string, unknown> = {};
  let order: { col: keyof SubRow; dir: 'asc' | 'desc' } | null = null;
  let limitN = Infinity;
  const builder: any = {
    where: (cond: Record<string, unknown>) => {
      Object.assign(conds, cond);
      return builder;
    },
    whereNot: (cond: Record<string, unknown>) => {
      Object.assign(notConds, cond);
      return builder;
    },
    orderBy: (col: keyof SubRow, dir: 'asc' | 'desc' = 'asc') => {
      order = { col, dir };
      return builder;
    },
    limit: (n: number) => {
      limitN = n;
      return builder;
    },
    first: async () => {
      let result = applyConds(rows, conds);
      result = result.filter((r) =>
        Object.entries(notConds).every(([k, v]) => (r as any)[k] !== v),
      );
      return result[0];
    },
    update: async (patch: Record<string, unknown>) => {
      let count = 0;
      for (const r of state.subs) {
        if (Object.entries(conds).every(([k, v]) => (r as any)[k] === v)) {
          Object.assign(r, patch);
          count += 1;
        }
      }
      return count;
    },
    insert: async (row: SubRow) => {
      state.subs.push(row);
      return [state.subs.length];
    },
    then: (cb: (rows: SubRow[]) => unknown) => {
      let result = applyConds(rows, conds);
      result = result.filter((r) =>
        Object.entries(notConds).every(([k, v]) => (r as any)[k] !== v),
      );
      if (order) {
        const o = order;
        result = [...result].sort((a, b) => {
          const av = String(a[o.col]);
          const bv = String(b[o.col]);
          const cmp = av.localeCompare(bv);
          return o.dir === 'desc' ? -cmp : cmp;
        });
      }
      return Promise.resolve(cb(result.slice(0, limitN)));
    },
  };
  return builder;
}

function makeMandateBuilder(state: MockState): any {
  let inCol: string | null = null;
  let inVals: unknown[] | null = null;
  const builder: any = {
    whereIn: (col: string, vals: unknown[]) => {
      inCol = col;
      inVals = vals;
      return builder;
    },
    select: async (..._cols: string[]) => {
      return state.mandates.filter(
        (m) => !inCol || (inVals && inVals.includes((m as any)[inCol!])),
      );
    },
  };
  return builder;
}

function makeDocsBuilder(state: MockState): any {
  let conds: Record<string, unknown> = {};
  let inCol: string | null = null;
  let inVals: unknown[] | null = null;
  let order: { col: keyof SubDocRow; dir: 'asc' | 'desc' } | null = null;
  const builder: any = {
    where: (cond: Record<string, unknown>) => {
      Object.assign(conds, cond);
      return builder;
    },
    whereIn: (col: string, vals: unknown[]) => {
      inCol = col;
      inVals = vals;
      return builder;
    },
    orderBy: (col: keyof SubDocRow, dir: 'asc' | 'desc' = 'asc') => {
      order = { col, dir };
      return builder;
    },
    select: async (..._cols: string[]) => {
      return state.docs.filter(
        (d) =>
          Object.entries(conds).every(([k, v]) => (d as any)[k] === v) &&
          (!inCol || (inVals && inVals.includes((d as any)[inCol!]))),
      );
    },
    insert: async (row: { subscription_id: string; source_doc: string }) => {
      state.docs.push({
        subscription_id: row.subscription_id,
        source_doc: row.source_doc,
        added_at: new Date().toISOString(),
      });
      return [state.docs.length];
    },
    delete: async () => {
      const before = state.docs.length;
      state.docs = state.docs.filter(
        (d) =>
          !(
            Object.entries(conds).every(([k, v]) => (d as any)[k] === v) &&
            (!inCol || (inVals && inVals.includes((d as any)[inCol!])))
          ),
      );
      return before - state.docs.length;
    },
    then: (cb: (rows: SubDocRow[]) => unknown) => {
      let result = state.docs.filter(
        (d) =>
          Object.entries(conds).every(([k, v]) => (d as any)[k] === v) &&
          (!inCol || (inVals && inVals.includes((d as any)[inCol!]))),
      );
      if (order) {
        const o = order;
        result = [...result].sort((a, b) => {
          const cmp = String(a[o.col]).localeCompare(String(b[o.col]));
          return o.dir === 'desc' ? -cmp : cmp;
        });
      }
      return Promise.resolve(cb(result));
    },
  };
  return builder;
}

function emptySub(over: Partial<SubRow> = {}): SubRow {
  return {
    id: 1,
    subscription_id: 'SUB_X',
    mandate_id: 'MD_X',
    opera_account: 'CUST01',
    opera_name: '',
    source_doc: null,
    amount_pence: 10000,
    currency: 'GBP',
    interval_unit: 'monthly',
    interval_count: 1,
    day_of_month: null,
    name: null,
    status: 'active',
    start_date: null,
    end_date: null,
    created_at: '2026-04-15T10:00:00Z',
    updated_at: '2026-04-15T10:00:00Z',
    synced_at: null,
    ...over,
  };
}

// ---------------------------------------------------------------------
// listSubscriptions
// ---------------------------------------------------------------------

describe('listSubscriptions', () => {
  it('returns subscriptions in created_at desc order', async () => {
    const state: MockState = {
      subs: [
        emptySub({ id: 1, subscription_id: 'A', created_at: '2026-04-10T10:00:00Z' }),
        emptySub({ id: 2, subscription_id: 'B', created_at: '2026-04-15T10:00:00Z' }),
      ],
      mandates: [],
      docs: [],
    };
    const result = await listSubscriptions(makeAppDb(state));
    expect(result.success).toBe(true);
    expect(result.count).toBe(2);
    expect(result.subscriptions[0]?.id).toBe(2);
  });

  it('filters by status', async () => {
    const state: MockState = {
      subs: [
        emptySub({ id: 1, subscription_id: 'A', status: 'active' }),
        emptySub({ id: 2, subscription_id: 'B', status: 'paused' }),
      ],
      mandates: [],
      docs: [],
    };
    const result = await listSubscriptions(makeAppDb(state), { status: 'paused' });
    expect(result.count).toBe(1);
    expect(result.subscriptions[0]?.status).toBe('paused');
  });

  it('excludes cancelled subscriptions by default', async () => {
    const state: MockState = {
      subs: [
        emptySub({ id: 1, subscription_id: 'A', status: 'active' }),
        emptySub({ id: 2, subscription_id: 'B', status: 'cancelled' }),
      ],
      mandates: [],
      docs: [],
    };
    const result = await listSubscriptions(makeAppDb(state));
    expect(result.count).toBe(1);
    expect(result.subscriptions[0]?.status).toBe('active');
  });

  it('includeCancelled=true returns cancelled rows', async () => {
    const state: MockState = {
      subs: [
        emptySub({ id: 1, subscription_id: 'A', status: 'active' }),
        emptySub({ id: 2, subscription_id: 'B', status: 'cancelled' }),
      ],
      mandates: [],
      docs: [],
    };
    const result = await listSubscriptions(makeAppDb(state), {
      includeCancelled: true,
    });
    expect(result.count).toBe(2);
  });

  it('enriches with customer_name from opera_name when present', async () => {
    const state: MockState = {
      subs: [emptySub({ opera_name: 'Acme Ltd' })],
      mandates: [],
      docs: [],
    };
    const result = await listSubscriptions(makeAppDb(state));
    expect(result.subscriptions[0]?.customer_name).toBe('Acme Ltd');
  });

  it('falls back to mandate name lookup when opera_name empty', async () => {
    const state: MockState = {
      subs: [emptySub({ opera_account: 'CUST01', opera_name: '' })],
      mandates: [{ opera_account: 'CUST01', opera_name: 'Beta Co' }],
      docs: [],
    };
    const result = await listSubscriptions(makeAppDb(state));
    expect(result.subscriptions[0]?.customer_name).toBe('Beta Co');
  });

  it('falls back to opera_account when no mandate name', async () => {
    const state: MockState = {
      subs: [emptySub({ opera_account: 'CUST99', opera_name: '' })],
      mandates: [],
      docs: [],
    };
    const result = await listSubscriptions(makeAppDb(state));
    expect(result.subscriptions[0]?.customer_name).toBe('CUST99');
  });

  it('attaches source_docs from junction table', async () => {
    const state: MockState = {
      subs: [emptySub({ subscription_id: 'SUB1' })],
      mandates: [],
      docs: [
        { subscription_id: 'SUB1', source_doc: 'INV001', added_at: '2026-04-01' },
        { subscription_id: 'SUB1', source_doc: 'INV002', added_at: '2026-04-02' },
      ],
    };
    const result = await listSubscriptions(makeAppDb(state));
    expect(result.subscriptions[0]?.source_docs).toEqual(['INV001', 'INV002']);
  });

  it('formats amount_pence to amount_formatted with thousand separators', async () => {
    const state: MockState = {
      subs: [emptySub({ amount_pence: 1234567 })], // £12,345.67
      mandates: [],
      docs: [],
    };
    const result = await listSubscriptions(makeAppDb(state));
    expect(result.subscriptions[0]?.amount_pounds).toBeCloseTo(12345.67, 2);
    expect(result.subscriptions[0]?.amount_formatted).toBe('£12,345.67');
  });

  it('derives "Quarterly" from monthly/3', async () => {
    const state: MockState = {
      subs: [emptySub({ interval_unit: 'monthly', interval_count: 3 })],
      mandates: [],
      docs: [],
    };
    const result = await listSubscriptions(makeAppDb(state));
    expect(result.subscriptions[0]?.frequency).toBe('Quarterly');
  });

  it('derives "Annual" from yearly/1 and "Weekly" from weekly/1', async () => {
    const state: MockState = {
      subs: [
        emptySub({ id: 1, subscription_id: 'A', interval_unit: 'yearly' }),
        emptySub({ id: 2, subscription_id: 'B', interval_unit: 'weekly' }),
      ],
      mandates: [],
      docs: [],
    };
    const result = await listSubscriptions(makeAppDb(state));
    const map = Object.fromEntries(
      result.subscriptions.map((s) => [s.subscription_id, s.frequency]),
    );
    expect(map.A).toBe('Annual');
    expect(map.B).toBe('Weekly');
  });

  it('respects limit', async () => {
    const state: MockState = {
      subs: Array.from({ length: 5 }, (_, i) =>
        emptySub({
          id: i + 1,
          subscription_id: `S${i}`,
          created_at: `2026-04-${10 + i}T00:00:00Z`,
        }),
      ),
      mandates: [],
      docs: [],
    };
    const result = await listSubscriptions(makeAppDb(state), { limit: 2 });
    expect(result.count).toBe(2);
  });
});

// ---------------------------------------------------------------------
// getSubscription
// ---------------------------------------------------------------------

describe('getSubscription', () => {
  it('returns 404-style error when not found', async () => {
    const state: MockState = { subs: [], mandates: [], docs: [] };
    const result = await getSubscription(makeAppDb(state), 'MISSING');
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/not found/);
  });

  it('returns the subscription with attached source_docs', async () => {
    const state: MockState = {
      subs: [emptySub({ subscription_id: 'SUB1' })],
      mandates: [{ opera_account: 'CUST01', opera_name: 'Acme Ltd' }],
      docs: [
        { subscription_id: 'SUB1', source_doc: 'D1', added_at: '2026-04-01' },
      ],
    };
    const result = await getSubscription(makeAppDb(state), 'SUB1');
    expect(result.success).toBe(true);
    expect(result.subscription?.subscription_id).toBe('SUB1');
    expect(result.subscription?.source_docs).toEqual(['D1']);
    expect(result.subscription?.customer_name).toBe('Acme Ltd');
  });

  it('rejects empty subscription_id', async () => {
    const state: MockState = { subs: [], mandates: [], docs: [] };
    const result = await getSubscription(makeAppDb(state), '');
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/required/);
  });
});

// ---------------------------------------------------------------------
// updateSubscriptionStatus + lifecycle (pause/resume/cancel)
// ---------------------------------------------------------------------

describe('updateSubscriptionStatus', () => {
  it('updates the local row and returns true', async () => {
    const state: MockState = {
      subs: [emptySub({ subscription_id: 'SUB1', status: 'active' })],
      mandates: [],
      docs: [],
    };
    const ok = await updateSubscriptionStatus(makeAppDb(state), 'SUB1', 'paused');
    expect(ok).toBe(true);
    expect(state.subs[0]?.status).toBe('paused');
  });

  it('returns false when subscription missing', async () => {
    const state: MockState = { subs: [], mandates: [], docs: [] };
    const ok = await updateSubscriptionStatus(makeAppDb(state), 'MISSING', 'x');
    expect(ok).toBe(false);
  });
});

function ok(status: string): RemoteSubscriptionResult {
  return { success: true, subscription: { id: 'SUB1', status } };
}

describe('pauseSubscription', () => {
  it('calls remote then mirrors status locally', async () => {
    const state: MockState = {
      subs: [emptySub({ subscription_id: 'SUB1', status: 'active' })],
      mandates: [],
      docs: [],
    };
    let called = '';
    const remote = async (id: string) => {
      called = id;
      return ok('paused');
    };
    const result = await pauseSubscription(makeAppDb(state), 'SUB1', remote);
    expect(result.success).toBe(true);
    expect(called).toBe('SUB1');
    expect(result.subscription?.status).toBe('paused');
    expect(state.subs[0]?.status).toBe('paused');
  });

  it('falls back to "paused" when remote omits status', async () => {
    const state: MockState = {
      subs: [emptySub({ subscription_id: 'SUB1', status: 'active' })],
      mandates: [],
      docs: [],
    };
    const result = await pauseSubscription(makeAppDb(state), 'SUB1', async () => ({
      success: true,
      subscription: {},
    }));
    expect(result.success).toBe(true);
    expect(result.subscription?.status).toBe('paused');
  });

  it('does NOT update local on remote failure', async () => {
    const state: MockState = {
      subs: [emptySub({ subscription_id: 'SUB1', status: 'active' })],
      mandates: [],
      docs: [],
    };
    const result = await pauseSubscription(makeAppDb(state), 'SUB1', async () => ({
      success: false,
      error: 'GoCardless API down',
    }));
    expect(result.success).toBe(false);
    expect(state.subs[0]?.status).toBe('active');
  });

  it('rejects empty id without calling remote', async () => {
    const state: MockState = { subs: [], mandates: [], docs: [] };
    let called = false;
    const result = await pauseSubscription(makeAppDb(state), '', async () => {
      called = true;
      return ok('paused');
    });
    expect(result.success).toBe(false);
    expect(called).toBe(false);
  });
});

describe('resumeSubscription', () => {
  it('mirrors active status to local', async () => {
    const state: MockState = {
      subs: [emptySub({ subscription_id: 'SUB1', status: 'paused' })],
      mandates: [],
      docs: [],
    };
    const result = await resumeSubscription(
      makeAppDb(state),
      'SUB1',
      async () => ok('active'),
    );
    expect(result.success).toBe(true);
    expect(state.subs[0]?.status).toBe('active');
  });
});

describe('cancelSubscription', () => {
  it('mirrors cancelled status to local', async () => {
    const state: MockState = {
      subs: [emptySub({ subscription_id: 'SUB1', status: 'active' })],
      mandates: [],
      docs: [],
    };
    const result = await cancelSubscription(
      makeAppDb(state),
      'SUB1',
      async () => ok('cancelled'),
    );
    expect(result.success).toBe(true);
    expect(state.subs[0]?.status).toBe('cancelled');
  });
});

// ---------------------------------------------------------------------
// updateSubscriptionDetails (PUT)
// ---------------------------------------------------------------------

describe('updateSubscriptionDetails', () => {
  it('passes name and amountPence to remote and mirrors locally', async () => {
    const state: MockState = {
      subs: [
        emptySub({
          subscription_id: 'SUB1',
          amount_pence: 10000,
          name: 'Old',
          status: 'active',
        }),
      ],
      mandates: [],
      docs: [],
    };
    const captured: any = {};
    const result = await updateSubscriptionDetails(
      makeAppDb(state),
      'SUB1',
      { name: 'New', amountPence: 25000 },
      async (id, opts) => {
        captured.id = id;
        captured.opts = opts;
        return { success: true, subscription: { id, status: 'active' } };
      },
    );
    expect(captured.id).toBe('SUB1');
    expect(captured.opts).toEqual({ name: 'New', amountPence: 25000 });
    expect(result.success).toBe(true);
    expect(state.subs[0]?.name).toBe('New');
    expect(state.subs[0]?.amount_pence).toBe(25000);
  });

  it('does not update local fields the caller did not send', async () => {
    const state: MockState = {
      subs: [
        emptySub({
          subscription_id: 'SUB1',
          amount_pence: 10000,
          name: 'KeepMe',
        }),
      ],
      mandates: [],
      docs: [],
    };
    await updateSubscriptionDetails(
      makeAppDb(state),
      'SUB1',
      { amountPence: 25000 },
      async (id) => ({ success: true, subscription: { id, status: 'active' } }),
    );
    expect(state.subs[0]?.name).toBe('KeepMe');
    expect(state.subs[0]?.amount_pence).toBe(25000);
  });

  it('returns error and skips local update when remote fails', async () => {
    const state: MockState = {
      subs: [emptySub({ subscription_id: 'SUB1', amount_pence: 10000 })],
      mandates: [],
      docs: [],
    };
    const result = await updateSubscriptionDetails(
      makeAppDb(state),
      'SUB1',
      { amountPence: 99999 },
      async () => ({ success: false, error: 'invalid' }),
    );
    expect(result.success).toBe(false);
    expect(state.subs[0]?.amount_pence).toBe(10000);
  });
});

// ---------------------------------------------------------------------
// Link / unlink
// ---------------------------------------------------------------------

describe('linkSubscriptionToDocument', () => {
  it('inserts the link', async () => {
    const state: MockState = {
      subs: [emptySub({ subscription_id: 'SUB1' })],
      mandates: [],
      docs: [],
    };
    const result = await linkSubscriptionToDocument(makeAppDb(state), {
      subscriptionId: 'SUB1',
      sourceDoc: 'INV001',
    });
    expect(result.success).toBe(true);
    expect(state.docs).toHaveLength(1);
    expect(state.docs[0]?.source_doc).toBe('INV001');
    expect(result.subscription?.source_docs).toEqual(['INV001']);
  });

  it('refuses when doc already linked to a different subscription', async () => {
    const state: MockState = {
      subs: [emptySub({ subscription_id: 'SUB1' })],
      mandates: [],
      docs: [
        { subscription_id: 'OTHER', source_doc: 'INV001', added_at: '2026-04-01' },
      ],
    };
    const result = await linkSubscriptionToDocument(makeAppDb(state), {
      subscriptionId: 'SUB1',
      sourceDoc: 'INV001',
    });
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/already linked to subscription OTHER/);
  });

  it('refuses when the same doc is already linked to this subscription', async () => {
    const state: MockState = {
      subs: [emptySub({ subscription_id: 'SUB1' })],
      mandates: [],
      docs: [
        { subscription_id: 'SUB1', source_doc: 'INV001', added_at: '2026-04-01' },
      ],
    };
    const result = await linkSubscriptionToDocument(makeAppDb(state), {
      subscriptionId: 'SUB1',
      sourceDoc: 'INV001',
    });
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/already linked/);
  });

  it('refuses when subscription is not present locally', async () => {
    const state: MockState = { subs: [], mandates: [], docs: [] };
    const result = await linkSubscriptionToDocument(makeAppDb(state), {
      subscriptionId: 'MISSING',
      sourceDoc: 'INV001',
    });
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/not found locally/);
  });

  it('rejects empty inputs', async () => {
    const state: MockState = { subs: [], mandates: [], docs: [] };
    const result = await linkSubscriptionToDocument(makeAppDb(state), {
      subscriptionId: '',
      sourceDoc: 'X',
    });
    expect(result.success).toBe(false);
  });
});

describe('syncSubscriptionFromOpera', () => {
  it('returns 404-style error when subscription missing', async () => {
    const state: MockState = { subs: [], mandates: [], docs: [] };
    const result = await syncSubscriptionFromOpera(
      makeAppDb(state),
      'MISSING',
      async () => ({ lineNettPence: 0, lineVatPence: 0 }),
      async () => ({ success: true }),
    );
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/not found/);
  });

  it('refuses when subscription has no linked docs', async () => {
    const state: MockState = {
      subs: [emptySub({ subscription_id: 'SUB1' })],
      mandates: [],
      docs: [],
    };
    const result = await syncSubscriptionFromOpera(
      makeAppDb(state),
      'SUB1',
      async () => ({ lineNettPence: 0, lineVatPence: 0 }),
      async () => ({ success: true }),
    );
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/not linked/);
  });

  it('refuses when Opera read returns zero amounts', async () => {
    const state: MockState = {
      subs: [emptySub({ subscription_id: 'SUB1' })],
      mandates: [],
      docs: [
        { subscription_id: 'SUB1', source_doc: 'INV1', added_at: '2026-04-01' },
      ],
    };
    const result = await syncSubscriptionFromOpera(
      makeAppDb(state),
      'SUB1',
      async () => ({ lineNettPence: 0, lineVatPence: 0 }),
      async () => ({ success: true }),
    );
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/no lines/);
  });

  it('returns "no change needed" when amounts already match', async () => {
    const state: MockState = {
      subs: [emptySub({ subscription_id: 'SUB1', amount_pence: 12000 })],
      mandates: [],
      docs: [
        { subscription_id: 'SUB1', source_doc: 'INV1', added_at: '2026-04-01' },
      ],
    };
    let remoteCalled = false;
    const result = await syncSubscriptionFromOpera(
      makeAppDb(state),
      'SUB1',
      async () => ({ lineNettPence: 10000, lineVatPence: 2000 }),
      async () => {
        remoteCalled = true;
        return { success: true };
      },
    );
    expect(result.success).toBe(true);
    expect(result.message).toMatch(/already match/);
    expect(remoteCalled).toBe(false);
  });

  it('pushes new total to remote and mirrors locally on amount change', async () => {
    const state: MockState = {
      subs: [emptySub({ subscription_id: 'SUB1', amount_pence: 10000 })],
      mandates: [],
      docs: [
        { subscription_id: 'SUB1', source_doc: 'INV1', added_at: '2026-04-01' },
        { subscription_id: 'SUB1', source_doc: 'INV2', added_at: '2026-04-02' },
      ],
    };
    let remoteAmount = 0;
    const result = await syncSubscriptionFromOpera(
      makeAppDb(state),
      'SUB1',
      async () => ({ lineNettPence: 12500, lineVatPence: 2500 }),
      async (_id, amount) => {
        remoteAmount = amount;
        return { success: true, subscription: { amount: amount } };
      },
    );
    expect(result.success).toBe(true);
    expect(remoteAmount).toBe(15000);
    expect(result.old_amount_pence).toBe(10000);
    expect(result.new_amount_pence).toBe(15000);
    expect(result.old_amount_formatted).toBe('£100.00');
    expect(result.new_amount_formatted).toBe('£150.00');
    expect(state.subs[0]?.amount_pence).toBe(15000);
  });

  it('does NOT update local when remote update fails', async () => {
    const state: MockState = {
      subs: [emptySub({ subscription_id: 'SUB1', amount_pence: 10000 })],
      mandates: [],
      docs: [
        { subscription_id: 'SUB1', source_doc: 'INV1', added_at: '2026-04-01' },
      ],
    };
    const result = await syncSubscriptionFromOpera(
      makeAppDb(state),
      'SUB1',
      async () => ({ lineNettPence: 12500, lineVatPence: 2500 }),
      async () => ({ success: false, error: 'GC API down' }),
    );
    expect(result.success).toBe(false);
    expect(state.subs[0]?.amount_pence).toBe(10000);
  });

  it('rejects empty subscription id', async () => {
    const state: MockState = { subs: [], mandates: [], docs: [] };
    const result = await syncSubscriptionFromOpera(
      makeAppDb(state),
      '',
      async () => ({ lineNettPence: 1000, lineVatPence: 100 }),
      async () => ({ success: true }),
    );
    expect(result.success).toBe(false);
  });
});

describe('unlinkSubscriptionFromDocument', () => {
  it('removes a specific document link', async () => {
    const state: MockState = {
      subs: [emptySub({ subscription_id: 'SUB1' })],
      mandates: [],
      docs: [
        { subscription_id: 'SUB1', source_doc: 'A', added_at: '2026-04-01' },
        { subscription_id: 'SUB1', source_doc: 'B', added_at: '2026-04-02' },
      ],
    };
    const result = await unlinkSubscriptionFromDocument(makeAppDb(state), {
      subscriptionId: 'SUB1',
      sourceDoc: 'A',
    });
    expect(result.success).toBe(true);
    expect(state.docs.map((d) => d.source_doc)).toEqual(['B']);
  });

  it('removes ALL links when sourceDoc is omitted', async () => {
    const state: MockState = {
      subs: [emptySub({ subscription_id: 'SUB1' })],
      mandates: [],
      docs: [
        { subscription_id: 'SUB1', source_doc: 'A', added_at: '2026-04-01' },
        { subscription_id: 'SUB1', source_doc: 'B', added_at: '2026-04-02' },
        { subscription_id: 'OTHER', source_doc: 'C', added_at: '2026-04-03' },
      ],
    };
    const result = await unlinkSubscriptionFromDocument(makeAppDb(state), {
      subscriptionId: 'SUB1',
    });
    expect(result.success).toBe(true);
    expect(state.docs.map((d) => d.source_doc)).toEqual(['C']);
  });

  it('returns error when specific doc was not linked', async () => {
    const state: MockState = {
      subs: [emptySub({ subscription_id: 'SUB1' })],
      mandates: [],
      docs: [],
    };
    const result = await unlinkSubscriptionFromDocument(makeAppDb(state), {
      subscriptionId: 'SUB1',
      sourceDoc: 'A',
    });
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/not linked/);
  });

  it('returns 404-style error when subscription missing', async () => {
    const state: MockState = { subs: [], mandates: [], docs: [] };
    const result = await unlinkSubscriptionFromDocument(makeAppDb(state), {
      subscriptionId: 'MISSING',
    });
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/not found/);
  });
});
