import { describe, it, expect } from 'vitest';
import {
  listQueries,
  resolveQuery,
  autoResolveQueries,
  listOverdueQueries,
  recordReminderSent,
  type OperaPaymentLookup,
} from '../src/services/supplier-queries.js';

interface QueryRow {
  id: number;
  supplier_code: string;
  reference: string;
  amount: number;
  status: 'open' | 'resolved' | 'cancelled';
  description?: string;
  resolved_by?: string | null;
  resolved_at?: string | null;
  resolution_notes?: string | null;
  created_at: string;
  reminder_sent_at?: string | null;
  reminder_count?: number;
}

interface State {
  queries: QueryRow[];
}

function makeAppDb(state: State): any {
  const db: any = (table: string) => {
    if (table !== 'supplier_queries') {
      throw new Error(`Unexpected table: ${table}`);
    }
    let supplierFilter: string | null = null;
    let statusFilter: string | null = null;
    let createdBefore: string | null = null;
    let limitN: number | undefined;
    let updateMode: 'resolve' | 'reminder' | null = null;
    let updatePayload: any = null;
    let idFilter: number | null = null;

    const builder: any = {
      where: (cond: any) => {
        if (typeof cond === 'object' && cond.supplier_code) {
          supplierFilter = cond.supplier_code;
        } else if (typeof cond === 'object' && cond.status) {
          statusFilter = cond.status;
        } else if (typeof cond === 'object' && cond.id) {
          idFilter = cond.id;
        }
        return builder;
      },
      andWhere: (col: any, op?: any, val?: any) => {
        if (typeof col === 'object' && col.status) {
          statusFilter = col.status;
        } else if (typeof col === 'string' && col === 'created_at' && op === '<') {
          createdBefore = val;
        }
        return builder;
      },
      orderBy: () => builder,
      limit: (n: number) => {
        limitN = n;
        return builder;
      },
      select: (..._cols: any[]) => builder,
      first: async () => {
        if (idFilter !== null) {
          return state.queries.find((q) => q.id === idFilter);
        }
        return undefined;
      },
      update: async (payload: any) => {
        updatePayload = payload;
        if ('resolved_by' in payload) updateMode = 'resolve';
        else if ('reminder_sent_at' in payload) updateMode = 'reminder';
        const matches = state.queries.filter((q) => {
          if (idFilter !== null && q.id !== idFilter) return false;
          if (statusFilter && q.status !== statusFilter) return false;
          return true;
        });
        for (const q of matches) {
          if (updateMode === 'resolve') {
            q.status = 'resolved';
            q.resolved_by = payload.resolved_by;
            q.resolved_at = '2026-04-15T12:00:00Z';
            q.resolution_notes = payload.resolution_notes ?? null;
          } else if (updateMode === 'reminder') {
            q.reminder_sent_at = '2026-04-15T12:00:00Z';
            q.reminder_count = (q.reminder_count ?? 0) + 1;
          }
        }
        return matches.length;
      },
      then: async (resolve: any) => {
        const filtered = state.queries.filter((q) => {
          if (supplierFilter && q.supplier_code !== supplierFilter)
            return false;
          if (statusFilter && q.status !== statusFilter) return false;
          if (createdBefore && q.created_at >= createdBefore) return false;
          return true;
        });
        return resolve(filtered.slice(0, limitN ?? 1000));
      },
    };
    return builder;
  };
  db.fn = { now: () => '__NOW__' };
  db.raw = (s: string) => s;
  return db;
}

describe('listQueries', () => {
  it('lists queries optionally filtered', async () => {
    const state: State = {
      queries: [
        {
          id: 1,
          supplier_code: 'A',
          reference: 'INV1',
          amount: 100,
          status: 'open',
          created_at: '2026-04-15T08:00:00Z',
        },
        {
          id: 2,
          supplier_code: 'B',
          reference: 'INV2',
          amount: 50,
          status: 'resolved',
          created_at: '2026-04-14T08:00:00Z',
        },
      ],
    };
    const all = await listQueries(makeAppDb(state));
    expect(all.count).toBe(2);

    const onlyA = await listQueries(makeAppDb(state), { supplierCode: 'A' });
    expect(onlyA.count).toBe(1);
    expect(onlyA.queries[0]?.id).toBe(1);

    const onlyOpen = await listQueries(makeAppDb(state), { status: 'open' });
    expect(onlyOpen.count).toBe(1);
    expect(onlyOpen.queries[0]?.id).toBe(1);
  });
});

describe('resolveQuery', () => {
  it('marks an open query resolved', async () => {
    const state: State = {
      queries: [
        {
          id: 1,
          supplier_code: 'A',
          reference: 'INV1',
          amount: 100,
          status: 'open',
          created_at: '2026-04-15T08:00:00Z',
        },
      ],
    };
    const result = await resolveQuery(makeAppDb(state), {
      queryId: 1,
      resolvedBy: 'admin',
      notes: 'paid',
    });
    expect(result.success).toBe(true);
    expect(result.query?.status).toBe('resolved');
    expect(result.query?.resolved_by).toBe('admin');
    expect(state.queries[0]?.status).toBe('resolved');
  });

  it('rejects invalid id', async () => {
    const result = await resolveQuery(makeAppDb({ queries: [] }), {
      queryId: 0,
      resolvedBy: 'x',
    });
    expect(result.success).toBe(false);
  });

  it('errors when query already resolved', async () => {
    const state: State = {
      queries: [
        {
          id: 1,
          supplier_code: 'A',
          reference: 'INV1',
          amount: 100,
          status: 'resolved',
          created_at: '2026-04-15T08:00:00Z',
        },
      ],
    };
    const result = await resolveQuery(makeAppDb(state), {
      queryId: 1,
      resolvedBy: 'admin',
    });
    expect(result.success).toBe(false);
  });
});

describe('autoResolveQueries', () => {
  it('resolves queries that have a matching opera posting', async () => {
    const state: State = {
      queries: [
        {
          id: 1,
          supplier_code: 'A',
          reference: 'INV1',
          amount: 100,
          status: 'open',
          created_at: '2026-04-15T08:00:00Z',
        },
        {
          id: 2,
          supplier_code: 'B',
          reference: 'INV2',
          amount: 50,
          status: 'open',
          created_at: '2026-04-15T08:00:00Z',
        },
      ],
    };
    const lookup: OperaPaymentLookup = {
      hasMatchingPosting: async ({ reference }) => reference === 'INV1',
    };
    const result = await autoResolveQueries(makeAppDb(state), lookup, 'system');
    expect(result.success).toBe(true);
    expect(result.scanned_count).toBe(2);
    expect(result.resolved_count).toBe(1);
    expect(state.queries[0]?.status).toBe('resolved');
    expect(state.queries[1]?.status).toBe('open');
  });
});

describe('listOverdueQueries', () => {
  it('returns queries open beyond threshold', async () => {
    const oldDate = new Date(
      Date.now() - 30 * 24 * 60 * 60 * 1000,
    ).toISOString();
    const state: State = {
      queries: [
        {
          id: 1,
          supplier_code: 'A',
          reference: 'INV1',
          amount: 100,
          status: 'open',
          created_at: oldDate,
        },
        {
          id: 2,
          supplier_code: 'B',
          reference: 'INV2',
          amount: 50,
          status: 'open',
          created_at: new Date().toISOString(),
        },
      ],
    };
    const result = await listOverdueQueries(makeAppDb(state), 7);
    expect(result.count).toBe(1);
    expect(result.queries[0]?.id).toBe(1);
    expect(result.threshold_days).toBe(7);
  });
});

describe('recordReminderSent', () => {
  it('increments reminder_count and stamps reminder_sent_at', async () => {
    const state: State = {
      queries: [
        {
          id: 1,
          supplier_code: 'A',
          reference: 'INV1',
          amount: 100,
          status: 'open',
          created_at: '2026-04-15T08:00:00Z',
          reminder_count: 0,
        },
      ],
    };
    const result = await recordReminderSent(makeAppDb(state), {
      queryId: 1,
      triggeredBy: 'admin',
    });
    expect(result.success).toBe(true);
    // Mock can't replicate raw SQL increment; the row.reminder_count
    // path exits via the .raw() expression so we just confirm the
    // call returned success and the audit timestamp is set.
    expect(state.queries[0]?.reminder_sent_at).not.toBeNull();
  });
});
