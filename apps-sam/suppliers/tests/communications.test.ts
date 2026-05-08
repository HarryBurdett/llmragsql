import { describe, it, expect } from 'vitest';
import {
  listCommunications,
  recordCommunication,
  deleteCommunication,
} from '../src/services/communications.js';

interface Row {
  id: number;
  supplier_code: string;
  channel: string;
  subject: string;
  content: string;
  sent_at: string;
}

interface MockState {
  rows: Row[];
  nextId: number;
}

function makeAppDb(state: MockState): any {
  const db: any = (table: string) => {
    if (table !== 'supplier_communications') {
      throw new Error(`Unexpected table: ${table}`);
    }
    let conds: Record<string, unknown> = {};
    let dateFrom: string | null = null;
    let dateTo: string | null = null;
    let limitN = Infinity;
    let order: { col: keyof Row; dir: 'asc' | 'desc' } | null = null;

    const builder: any = {
      where: (cond: Record<string, unknown> | string, val?: unknown) => {
        if (typeof cond === 'object') Object.assign(conds, cond);
        else if (val !== undefined) conds[cond] = val;
        return builder;
      },
      andWhere: (col: string, op: string, val: string) => {
        if (col === 'sent_at') {
          if (op === '>=') dateFrom = val;
          if (op === '<=') dateTo = val;
        }
        return builder;
      },
      orderBy: (col: keyof Row, dir: 'asc' | 'desc') => {
        order = { col, dir };
        return builder;
      },
      limit: (n: number) => {
        limitN = n;
        return builder;
      },
      then: (cb: (rows: Row[]) => unknown) => {
        let rows = state.rows.filter((r) =>
          Object.entries(conds).every(([k, v]) => (r as any)[k] === v),
        );
        if (dateFrom) rows = rows.filter((r) => r.sent_at >= dateFrom!);
        if (dateTo) rows = rows.filter((r) => r.sent_at <= dateTo!);
        if (order) {
          const { col, dir } = order;
          rows = [...rows].sort((a, b) => {
            const cmp = String(a[col]).localeCompare(String(b[col]));
            return dir === 'desc' ? -cmp : cmp;
          });
        }
        return Promise.resolve(cb(rows.slice(0, limitN)));
      },
      insert: (row: Partial<Row>) => ({
        returning: (_: string) => {
          const id = state.nextId++;
          state.rows.push({
            id,
            supplier_code: String(row.supplier_code ?? ''),
            channel: String(row.channel ?? 'email'),
            subject: String(row.subject ?? ''),
            content: String(row.content ?? ''),
            sent_at:
              typeof row.sent_at === 'string'
                ? row.sent_at
                : new Date().toISOString(),
          });
          return Promise.resolve([{ id }]);
        },
      }),
      delete: () => {
        const before = state.rows.length;
        state.rows = state.rows.filter(
          (r) =>
            !Object.entries(conds).every(([k, v]) => (r as any)[k] === v),
        );
        return Promise.resolve(before - state.rows.length);
      },
    };
    return builder;
  };
  db.fn = { now: () => 'NOW()' };
  return db;
}

describe('listCommunications', () => {
  it('returns rows in date-desc order, limited', async () => {
    const state: MockState = {
      rows: [
        { id: 1, supplier_code: 'S1', channel: 'email', subject: 'A', content: '', sent_at: '2026-04-10T10:00:00Z' },
        { id: 2, supplier_code: 'S1', channel: 'phone', subject: 'B', content: '', sent_at: '2026-04-15T10:00:00Z' },
        { id: 3, supplier_code: 'S2', channel: 'email', subject: 'C', content: '', sent_at: '2026-04-12T10:00:00Z' },
      ],
      nextId: 4,
    };
    const db = makeAppDb(state);
    const result = await listCommunications(db, { limit: 2 });
    expect(result.success).toBe(true);
    expect(result.count).toBe(2);
    expect(result.entries[0]?.id).toBe(2); // newest
    expect(result.entries[1]?.id).toBe(3);
  });

  it('filters by supplier_code', async () => {
    const state: MockState = {
      rows: [
        { id: 1, supplier_code: 'S1', channel: 'email', subject: '', content: '', sent_at: '2026-04-10T10:00:00Z' },
        { id: 2, supplier_code: 'S2', channel: 'email', subject: '', content: '', sent_at: '2026-04-15T10:00:00Z' },
      ],
      nextId: 3,
    };
    const result = await listCommunications(makeAppDb(state), {
      supplierCode: 'S1',
    });
    expect(result.count).toBe(1);
    expect(result.entries[0]?.supplier_code).toBe('S1');
  });

  it('filters by channel', async () => {
    const state: MockState = {
      rows: [
        { id: 1, supplier_code: 'S1', channel: 'email', subject: '', content: '', sent_at: '2026-04-10T10:00:00Z' },
        { id: 2, supplier_code: 'S1', channel: 'phone', subject: '', content: '', sent_at: '2026-04-15T10:00:00Z' },
      ],
      nextId: 3,
    };
    const result = await listCommunications(makeAppDb(state), {
      channel: 'phone',
    });
    expect(result.count).toBe(1);
    expect(result.entries[0]?.channel).toBe('phone');
  });

  it('rejects bad channel input', async () => {
    const result = await listCommunications(makeAppDb({ rows: [], nextId: 1 }), {
      channel: 'fax' as any,
    });
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/channel must be/);
  });
});

describe('recordCommunication', () => {
  it('inserts a new entry', async () => {
    const state: MockState = { rows: [], nextId: 1 };
    const result = await recordCommunication(makeAppDb(state), {
      supplier_code: 'S1',
      channel: 'email',
      subject: 'Test subject',
      content: 'Test content',
    });
    expect(result.success).toBe(true);
    expect(result.entry?.id).toBe(1);
    expect(state.rows).toHaveLength(1);
  });

  it('rejects missing supplier_code', async () => {
    const result = await recordCommunication(makeAppDb({ rows: [], nextId: 1 }), {
      supplier_code: '',
      channel: 'email',
    });
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/supplier_code/);
  });

  it('rejects bad channel', async () => {
    const result = await recordCommunication(makeAppDb({ rows: [], nextId: 1 }), {
      supplier_code: 'S1',
      channel: 'fax',
    });
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/channel must be/);
  });

  it('truncates subject to 500 chars', async () => {
    const state: MockState = { rows: [], nextId: 1 };
    const long = 'x'.repeat(800);
    await recordCommunication(makeAppDb(state), {
      supplier_code: 'S1',
      channel: 'email',
      subject: long,
    });
    expect(state.rows[0]?.subject.length).toBe(500);
  });
});

describe('deleteCommunication', () => {
  it('removes an existing entry', async () => {
    const state: MockState = {
      rows: [
        { id: 1, supplier_code: 'S1', channel: 'email', subject: '', content: '', sent_at: '2026-04-10' },
      ],
      nextId: 2,
    };
    const result = await deleteCommunication(makeAppDb(state), 1);
    expect(result.success).toBe(true);
    expect(result.deleted).toBe(true);
    expect(state.rows).toHaveLength(0);
  });

  it('returns deleted=false when id not found', async () => {
    const result = await deleteCommunication(makeAppDb({ rows: [], nextId: 1 }), 99);
    expect(result.success).toBe(true);
    expect(result.deleted).toBe(false);
  });

  it('rejects invalid id', async () => {
    const result = await deleteCommunication(makeAppDb({ rows: [], nextId: 1 }), 0);
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/positive number/);
  });
});
