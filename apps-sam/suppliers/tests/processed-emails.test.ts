import { describe, it, expect } from 'vitest';
import {
  isEmailProcessed,
  recordProcessedEmail,
  listProcessedEmails,
} from '../src/services/processed-emails.js';

interface Row {
  id: number;
  message_id: string;
  supplier_code: string | null;
  subject: string | null;
  processed_at: string;
}

interface MockState {
  rows: Row[];
  nextId: number;
}

function makeAppDb(state: MockState): any {
  const db: any = (table: string) => {
    if (table !== 'processed_emails') {
      throw new Error(`Unexpected table: ${table}`);
    }
    let conds: Record<string, unknown> = {};
    let dateFrom: string | null = null;
    let dateTo: string | null = null;
    let limitN = Infinity;
    let order: { col: keyof Row; dir: 'asc' | 'desc' } | null = null;
    const builder: any = {
      where: (cond: Record<string, unknown>) => {
        Object.assign(conds, cond);
        return builder;
      },
      andWhere: (col: string, op: string, val: string) => {
        if (col === 'processed_at') {
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
      first: () => {
        const found = state.rows.find((r) =>
          Object.entries(conds).every(([k, v]) => (r as any)[k] === v),
        );
        return Promise.resolve(found);
      },
      then: (cb: (rows: Row[]) => unknown) => {
        let rows = state.rows.filter((r) =>
          Object.entries(conds).every(([k, v]) => (r as any)[k] === v),
        );
        if (dateFrom) rows = rows.filter((r) => r.processed_at >= dateFrom!);
        if (dateTo) rows = rows.filter((r) => r.processed_at <= dateTo!);
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
            message_id: String(row.message_id ?? ''),
            supplier_code: (row.supplier_code as string) ?? null,
            subject: (row.subject as string) ?? null,
            processed_at: new Date().toISOString(),
          });
          return Promise.resolve([{ id }]);
        },
      }),
    };
    return builder;
  };
  db.fn = { now: () => new Date() };
  return db;
}

describe('isEmailProcessed', () => {
  it('returns true when row exists', async () => {
    const state: MockState = {
      rows: [
        { id: 1, message_id: 'MSG_X', supplier_code: 'S1', subject: '', processed_at: '2026-04-15' },
      ],
      nextId: 2,
    };
    expect(await isEmailProcessed(makeAppDb(state), 'MSG_X')).toBe(true);
  });

  it('returns false when row missing', async () => {
    expect(
      await isEmailProcessed(makeAppDb({ rows: [], nextId: 1 }), 'MSG_Y'),
    ).toBe(false);
  });

  it('returns false on empty input', async () => {
    expect(
      await isEmailProcessed(makeAppDb({ rows: [], nextId: 1 }), ''),
    ).toBe(false);
  });
});

describe('recordProcessedEmail', () => {
  it('inserts a new row', async () => {
    const state: MockState = { rows: [], nextId: 1 };
    const result = await recordProcessedEmail(makeAppDb(state), {
      message_id: 'MSG_X',
      supplier_code: 'S1',
      subject: 'Statement Apr 26',
    });
    expect(result.success).toBe(true);
    expect(result.duplicate).toBe(false);
    expect(state.rows).toHaveLength(1);
  });

  it('returns duplicate=true when message_id already exists', async () => {
    const state: MockState = {
      rows: [
        { id: 7, message_id: 'MSG_X', supplier_code: 'S1', subject: 'old', processed_at: '2026-04-10' },
      ],
      nextId: 8,
    };
    const result = await recordProcessedEmail(makeAppDb(state), {
      message_id: 'MSG_X',
      supplier_code: 'S1',
      subject: 'new',
    });
    expect(result.success).toBe(true);
    expect(result.duplicate).toBe(true);
    expect(result.entry?.id).toBe(7);
    expect(state.rows).toHaveLength(1); // not duplicated
  });

  it('rejects empty message_id', async () => {
    const result = await recordProcessedEmail(
      makeAppDb({ rows: [], nextId: 1 }),
      { message_id: '' },
    );
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/message_id/);
  });

  it('truncates supplier_code to 32, subject to 500', async () => {
    const state: MockState = { rows: [], nextId: 1 };
    await recordProcessedEmail(makeAppDb(state), {
      message_id: 'MSG_X',
      supplier_code: 'X'.repeat(50),
      subject: 'S'.repeat(800),
    });
    expect(state.rows[0]?.supplier_code?.length).toBe(32);
    expect(state.rows[0]?.subject?.length).toBe(500);
  });
});

describe('listProcessedEmails', () => {
  it('returns rows in date-desc order', async () => {
    const state: MockState = {
      rows: [
        { id: 1, message_id: 'A', supplier_code: 'S1', subject: '', processed_at: '2026-04-10T10:00:00Z' },
        { id: 2, message_id: 'B', supplier_code: 'S1', subject: '', processed_at: '2026-04-15T10:00:00Z' },
        { id: 3, message_id: 'C', supplier_code: 'S2', subject: '', processed_at: '2026-04-12T10:00:00Z' },
      ],
      nextId: 4,
    };
    const result = await listProcessedEmails(makeAppDb(state));
    expect(result.success).toBe(true);
    expect(result.entries[0]?.id).toBe(2); // newest first
  });

  it('filters by supplier_code', async () => {
    const state: MockState = {
      rows: [
        { id: 1, message_id: 'A', supplier_code: 'S1', subject: '', processed_at: '2026-04-10' },
        { id: 2, message_id: 'B', supplier_code: 'S2', subject: '', processed_at: '2026-04-15' },
      ],
      nextId: 3,
    };
    const result = await listProcessedEmails(makeAppDb(state), {
      supplierCode: 'S1',
    });
    expect(result.count).toBe(1);
    expect(result.entries[0]?.supplier_code).toBe('S1');
  });

  it('respects limit', async () => {
    const state: MockState = {
      rows: Array.from({ length: 5 }, (_, i) => ({
        id: i + 1,
        message_id: `M${i}`,
        supplier_code: 'S1',
        subject: '',
        processed_at: `2026-04-${String(10 + i).padStart(2, '0')}`,
      })),
      nextId: 6,
    };
    const result = await listProcessedEmails(makeAppDb(state), { limit: 2 });
    expect(result.count).toBe(2);
  });
});
