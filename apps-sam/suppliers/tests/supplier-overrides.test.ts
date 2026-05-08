import { describe, it, expect } from 'vitest';
import {
  listOverrides,
  recordOverride,
  deleteOverride,
} from '../src/services/supplier-overrides.js';

interface Row {
  id: number;
  statement_id: number;
  line_id: number | null;
  override_type: string;
  reason: string;
  created_at: string;
}

interface MockState {
  rows: Row[];
  nextId: number;
}

function makeAppDb(state: MockState): any {
  const db: any = (table: string) => {
    if (table !== 'supplier_overrides') {
      throw new Error(`Unexpected table: ${table}`);
    }
    let conds: Partial<Row> = {};
    let order: { col: string; dir: 'asc' | 'desc' } | null = null;
    const builder: any = {
      where: (cond: Partial<Row>) => {
        Object.assign(conds, cond);
        return builder;
      },
      orderBy: (col: string, dir: 'asc' | 'desc' = 'asc') => {
        order = { col, dir };
        return builder;
      },
      then: (cb: (rows: Row[]) => unknown) => {
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
      delete: () => {
        const before = state.rows.length;
        state.rows = state.rows.filter(
          (r) =>
            !Object.entries(conds).every(([k, v]) => (r as any)[k] === v),
        );
        return Promise.resolve(before - state.rows.length);
      },
      insert: (row: Partial<Row>) => ({
        returning: (_: string) => {
          const id = state.nextId++;
          state.rows.push({
            id,
            statement_id: Number(row.statement_id ?? 0),
            line_id: row.line_id == null ? null : Number(row.line_id),
            override_type: String(row.override_type ?? 'accept'),
            reason: String(row.reason ?? ''),
            created_at: new Date().toISOString(),
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

describe('listOverrides', () => {
  it('returns rows for a statement in date-desc order', async () => {
    const state: MockState = {
      rows: [
        { id: 1, statement_id: 100, line_id: null, override_type: 'accept', reason: '', created_at: '2026-04-10T10:00:00Z' },
        { id: 2, statement_id: 100, line_id: 5, override_type: 'dispute', reason: 'wrong amount', created_at: '2026-04-15T10:00:00Z' },
        { id: 3, statement_id: 200, line_id: null, override_type: 'reject', reason: '', created_at: '2026-04-12T10:00:00Z' },
      ],
      nextId: 4,
    };
    const result = await listOverrides(makeAppDb(state), 100);
    expect(result.success).toBe(true);
    expect(result.count).toBe(2);
    expect(result.entries[0]?.id).toBe(2); // newest first
  });

  it('rejects bad statement_id', async () => {
    const result = await listOverrides(
      makeAppDb({ rows: [], nextId: 1 }),
      0,
    );
    expect(result.success).toBe(false);
  });

  it('coerces unknown override_type to accept', async () => {
    const state: MockState = {
      rows: [
        { id: 1, statement_id: 100, line_id: null, override_type: 'weird', reason: '', created_at: '2026-04-10' },
      ],
      nextId: 2,
    };
    const result = await listOverrides(makeAppDb(state), 100);
    expect(result.entries[0]?.override_type).toBe('accept');
  });
});

describe('recordOverride', () => {
  it('records an accept override at statement level', async () => {
    const state: MockState = { rows: [], nextId: 1 };
    const result = await recordOverride(makeAppDb(state), {
      statement_id: 100,
      override_type: 'accept',
      reason: 'Difference within tolerance',
    });
    expect(result.success).toBe(true);
    expect(result.entry?.id).toBe(1);
    expect(state.rows[0]?.line_id).toBeNull();
  });

  it('records a dispute override on a specific line', async () => {
    const state: MockState = { rows: [], nextId: 1 };
    await recordOverride(makeAppDb(state), {
      statement_id: 100,
      line_id: 5,
      override_type: 'dispute',
      reason: '',
    });
    expect(state.rows[0]?.line_id).toBe(5);
    expect(state.rows[0]?.override_type).toBe('dispute');
  });

  it('rejects bad statement_id', async () => {
    const result = await recordOverride(makeAppDb({ rows: [], nextId: 1 }), {
      statement_id: 0,
      override_type: 'accept',
    });
    expect(result.success).toBe(false);
  });

  it('rejects bad override_type', async () => {
    const result = await recordOverride(makeAppDb({ rows: [], nextId: 1 }), {
      statement_id: 100,
      override_type: 'weird',
    });
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/override_type/);
  });

  it('rejects bad line_id', async () => {
    const result = await recordOverride(makeAppDb({ rows: [], nextId: 1 }), {
      statement_id: 100,
      line_id: 0,
      override_type: 'accept',
    });
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/line_id/);
  });
});

describe('deleteOverride', () => {
  it('removes an existing entry', async () => {
    const state: MockState = {
      rows: [
        { id: 1, statement_id: 100, line_id: null, override_type: 'accept', reason: '', created_at: '2026-04-10' },
      ],
      nextId: 2,
    };
    const result = await deleteOverride(makeAppDb(state), 1);
    expect(result.success).toBe(true);
    expect(result.deleted).toBe(true);
    expect(state.rows).toHaveLength(0);
  });

  it('returns deleted=false when id not found', async () => {
    const result = await deleteOverride(
      makeAppDb({ rows: [], nextId: 1 }),
      99,
    );
    expect(result.success).toBe(true);
    expect(result.deleted).toBe(false);
  });

  it('rejects invalid id', async () => {
    const result = await deleteOverride(
      makeAppDb({ rows: [], nextId: 1 }),
      0,
    );
    expect(result.success).toBe(false);
  });
});
