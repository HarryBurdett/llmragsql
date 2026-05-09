import { describe, it, expect } from 'vitest';
import {
  getStatementQueue,
  getStatementsDashboard,
  getStatementHistory,
} from '../src/services/statement-queue.js';

interface Row {
  id: number;
  supplier_code: string;
  statement_date: string | null;
  received_date: string | null;
  status: string;
  sender_email?: string | null;
  opening_balance: number;
  closing_balance: number;
  currency?: string;
  error_message?: string | null;
  approved_by?: string | null;
  approved_at?: string | null;
  sent_at?: string | null;
}

interface State {
  statements: Row[];
  lines: Array<{ id: number; statement_id: number; status: string }>;
  queries: Array<{
    id: number;
    status: string;
    created_at: string;
  }>;
  overrides: Array<{ id: number; override_type: string }>;
}

function makeAppDb(state: State): any {
  const db: any = (table: string) => {
    if (
      table !== 'supplier_statements' &&
      table !== 'supplier_statements as ss' &&
      table !== 'supplier_queries' &&
      table !== 'supplier_overrides'
    ) {
      throw new Error(`Unexpected table: ${table}`);
    }
    const isQueriesTable = table === 'supplier_queries';
    const isOverridesTable = table === 'supplier_overrides';

    let lhs: 'statements' | 'queries' | 'overrides' = 'statements';
    if (isQueriesTable) lhs = 'queries';
    if (isOverridesTable) lhs = 'overrides';

    let statusFilter: string[] | null = null;
    let supplierFilter: string | null = null;
    let createdBefore: string | null = null;
    let groupByStatus = false;
    let limitN: number | undefined;
    let mode: 'queue' | 'dashboard' | 'history' | 'count' = 'queue';
    let overrideTypeFilter: string | null = null;
    let firstMode = false;

    const builder: any = {
      leftJoin: () => builder,
      select: (...args: any[]) => {
        if (args.some((a) => typeof a === 'object' && a && 'sql' in a && /COUNT\(\*\)/i.test(String(a.sql)))) {
          mode = 'count';
        }
        // history mode includes approved_at
        if (args.some((a) => typeof a === 'string' && a.includes('approved_at'))) {
          mode = 'history';
        }
        return builder;
      },
      count: (col: string) => {
        groupByStatus = true;
        return builder;
      },
      whereIn: (col: string, vals: string[]) => {
        statusFilter = vals;
        return builder;
      },
      where: (cond: any) => {
        if (typeof cond === 'object' && cond.status) {
          statusFilter = [cond.status];
        } else if (typeof cond === 'object' && cond.override_type) {
          overrideTypeFilter = cond.override_type;
        } else if (typeof cond === 'object' && cond.supplier_code) {
          supplierFilter = cond.supplier_code;
        }
        return builder;
      },
      andWhere: (col: any, op?: any, val?: any) => {
        if (typeof col === 'string' && col === 'created_at' && op === '<') {
          createdBefore = val;
        } else if (typeof col === 'string' && col === 'ss.supplier_code') {
          // Two-arg signature: andWhere('ss.supplier_code', value)
          supplierFilter = op as string;
        } else if (typeof col === 'object' && col?.status) {
          statusFilter = [col.status];
        }
        return builder;
      },
      groupBy: () => builder,
      orderBy: () => builder,
      limit: (n: number) => {
        limitN = n;
        return builder;
      },
      first: async () => {
        firstMode = true;
        if (lhs === 'queries' && createdBefore) {
          const total = state.queries.filter(
            (q) => q.status === 'open' && q.created_at < createdBefore!,
          ).length;
          return { total };
        }
        if (lhs === 'overrides') {
          const total = state.overrides.filter(
            (o) => o.override_type === overrideTypeFilter,
          ).length;
          return { total };
        }
        return undefined;
      },
      then: async (resolve: any) => {
        const result = await runQuery();
        return resolve(result);
      },
    };

    async function runQuery() {
      void firstMode;
      if (lhs === 'queries' && groupByStatus) {
        const counts = new Map<string, number>();
        for (const q of state.queries) {
          counts.set(q.status, (counts.get(q.status) ?? 0) + 1);
        }
        return Array.from(counts.entries()).map(([s, total]) => ({
          status: s,
          total,
        }));
      }
      if (lhs === 'overrides' && groupByStatus) {
        return [];
      }
      if (lhs === 'statements' && groupByStatus) {
        const counts = new Map<string, number>();
        for (const s of state.statements) {
          counts.set(s.status, (counts.get(s.status) ?? 0) + 1);
        }
        return Array.from(counts.entries()).map(([s, total]) => ({
          status: s,
          total,
        }));
      }
      const filtered = state.statements.filter((s) => {
        if (statusFilter && !statusFilter.includes(s.status)) return false;
        if (supplierFilter && s.supplier_code !== supplierFilter) return false;
        return true;
      });
      const out = filtered
        .map((s) => {
          const lines = state.lines.filter((l) => l.statement_id === s.id);
          return {
            ...s,
            line_count: lines.length,
            matched_count: lines.filter((l) => l.status === 'Agreed').length,
            query_count: lines.filter((l) => l.status === 'Query').length,
          };
        })
        .slice(0, limitN ?? 1000);
      return out;
    }

    return builder;
  };
  db.fn = { now: () => '__NOW__' };
  db.raw = (s: string, p?: unknown[]) => ({ sql: s, bindings: p });
  return db;
}

describe('getStatementQueue', () => {
  it('lists received + processing statements with line stats', async () => {
    const state: State = {
      statements: [
        {
          id: 1,
          supplier_code: 'SUP001',
          statement_date: '2026-04-01',
          received_date: '2026-04-15T10:00:00Z',
          status: 'received',
          sender_email: 'ap@sup1.com',
          opening_balance: 100,
          closing_balance: 250,
          currency: 'GBP',
          error_message: null,
        },
        {
          id: 2,
          supplier_code: 'SUP001',
          statement_date: '2026-03-01',
          received_date: '2026-03-15T10:00:00Z',
          status: 'approved',
          opening_balance: 0,
          closing_balance: 100,
        },
      ],
      lines: [
        { id: 10, statement_id: 1, status: 'Agreed' },
        { id: 11, statement_id: 1, status: 'Query' },
        { id: 12, statement_id: 1, status: 'Pending' },
      ],
      queries: [],
      overrides: [],
    };
    const result = await getStatementQueue(makeAppDb(state));
    expect(result.success).toBe(true);
    expect(result.count).toBe(1);
    const item = result.statements[0];
    expect(item?.id).toBe(1);
    expect(item?.line_count).toBe(3);
    expect(item?.matched_count).toBe(1);
    expect(item?.query_count).toBe(1);
  });

  it('returns empty when no statements', async () => {
    const result = await getStatementQueue(
      makeAppDb({
        statements: [],
        lines: [],
        queries: [],
        overrides: [],
      }),
    );
    expect(result.success).toBe(true);
    expect(result.count).toBe(0);
  });
});

describe('getStatementsDashboard', () => {
  it('counts statements by status group', async () => {
    const state: State = {
      statements: [
        {
          id: 1,
          supplier_code: 'A',
          statement_date: null,
          received_date: null,
          status: 'received',
          opening_balance: 0,
          closing_balance: 0,
        },
        {
          id: 2,
          supplier_code: 'B',
          statement_date: null,
          received_date: null,
          status: 'processing',
          opening_balance: 0,
          closing_balance: 0,
        },
        {
          id: 3,
          supplier_code: 'C',
          statement_date: null,
          received_date: null,
          status: 'approved',
          opening_balance: 0,
          closing_balance: 0,
        },
        {
          id: 4,
          supplier_code: 'D',
          statement_date: null,
          received_date: null,
          status: 'sent',
          opening_balance: 0,
          closing_balance: 0,
        },
      ],
      lines: [],
      queries: [
        {
          id: 1,
          status: 'open',
          created_at: new Date().toISOString(),
        },
        {
          id: 2,
          status: 'resolved',
          created_at: new Date().toISOString(),
        },
        {
          id: 3,
          status: 'open',
          created_at: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString(),
        },
      ],
      overrides: [{ id: 1, override_type: 'dispute' }],
    };
    const result = await getStatementsDashboard(makeAppDb(state));
    expect(result.success).toBe(true);
    expect(result.counts.pending).toBe(1);
    expect(result.counts.processing).toBe(1);
    expect(result.counts.approved).toBe(2);
    expect(result.counts.total_open_queries).toBe(2);
    expect(result.counts.overdue_queries).toBe(1);
    expect(result.counts.total_disputes).toBe(1);
  });
});

describe('getStatementHistory', () => {
  it('lists approved/sent statements only', async () => {
    const state: State = {
      statements: [
        {
          id: 1,
          supplier_code: 'A',
          statement_date: '2026-04-01',
          received_date: '2026-04-15T10:00:00Z',
          status: 'received',
          opening_balance: 0,
          closing_balance: 0,
        },
        {
          id: 2,
          supplier_code: 'A',
          statement_date: '2026-03-01',
          received_date: '2026-03-15T10:00:00Z',
          status: 'sent',
          approved_by: 'admin',
          approved_at: '2026-03-20T08:00:00Z',
          sent_at: '2026-03-21T08:00:00Z',
          opening_balance: 0,
          closing_balance: 100,
        },
      ],
      lines: [],
      queries: [],
      overrides: [],
    };
    const result = await getStatementHistory(makeAppDb(state));
    expect(result.success).toBe(true);
    expect(result.count).toBe(1);
    expect(result.statements[0]?.id).toBe(2);
    expect(result.statements[0]?.approved_by).toBe('admin');
  });

  it('filters by supplier_code when supplied', async () => {
    const state: State = {
      statements: [
        {
          id: 1,
          supplier_code: 'A',
          statement_date: null,
          received_date: null,
          status: 'sent',
          opening_balance: 0,
          closing_balance: 0,
        },
        {
          id: 2,
          supplier_code: 'B',
          statement_date: null,
          received_date: null,
          status: 'sent',
          opening_balance: 0,
          closing_balance: 0,
        },
      ],
      lines: [],
      queries: [],
      overrides: [],
    };
    const result = await getStatementHistory(makeAppDb(state), {
      supplierCode: 'A',
    });
    expect(result.count).toBe(1);
    expect(result.statements[0]?.supplier_code).toBe('A');
  });
});
