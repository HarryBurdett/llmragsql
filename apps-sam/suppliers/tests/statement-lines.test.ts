import { describe, it, expect } from 'vitest';
import {
  listStatementLines,
  addStatementLines,
  updateStatementLineMatch,
  deleteStatementLines,
  listOperaOnlyItems,
} from '../src/services/statement-lines.js';

interface LineRow {
  id: number;
  statement_id: number;
  line_date: string | null;
  reference: string | null;
  description: string | null;
  amount: number;
  matched_opera_ref: string | null;
  match_status: string;
}

interface OperaOnlyRow {
  id: number;
  statement_id: number;
  reference: string | null;
  amount: number | null;
  reason: string | null;
}

interface MockState {
  lines: LineRow[];
  operaOnly: OperaOnlyRow[];
  nextId: number;
  nextOpId: number;
}

function makeAppDb(state: MockState): any {
  const db: any = (table: string) => {
    let conds: Partial<LineRow & OperaOnlyRow> = {};
    let order: Array<{ col: string; dir: 'asc' | 'desc' }> = [];
    if (table === 'statement_lines') {
      const builder: any = {
        where: (cond: Partial<LineRow>) => {
          conds = { ...conds, ...cond };
          return builder;
        },
        orderBy: (col: string, dir: 'asc' | 'desc' = 'asc') => {
          order.push({ col, dir });
          return builder;
        },
        then: (cb: (rows: LineRow[]) => unknown) => {
          let rows = state.lines.filter((r) =>
            Object.entries(conds).every(([k, v]) => (r as any)[k] === v),
          );
          for (const o of [...order].reverse()) {
            rows = [...rows].sort((a, b) => {
              const cmp = String((a as any)[o.col]).localeCompare(
                String((b as any)[o.col]),
              );
              return o.dir === 'desc' ? -cmp : cmp;
            });
          }
          return Promise.resolve(cb(rows));
        },
        update: (data: Partial<LineRow>) => {
          let count = 0;
          for (const r of state.lines) {
            if (
              Object.entries(conds).every(([k, v]) => (r as any)[k] === v)
            ) {
              Object.assign(r, data);
              count++;
            }
          }
          return Promise.resolve(count);
        },
        delete: () => {
          const before = state.lines.length;
          state.lines = state.lines.filter(
            (r) =>
              !Object.entries(conds).every(([k, v]) => (r as any)[k] === v),
          );
          return Promise.resolve(before - state.lines.length);
        },
        insert: (row: Partial<LineRow>) => ({
          returning: (_: string) => {
            const id = state.nextId++;
            state.lines.push({
              id,
              statement_id: Number(row.statement_id ?? 0),
              line_date: (row.line_date as string) ?? null,
              reference: (row.reference as string) ?? null,
              description: (row.description as string) ?? null,
              amount: Number(row.amount ?? 0),
              matched_opera_ref: (row.matched_opera_ref as string) ?? null,
              match_status: String(row.match_status ?? 'unmatched'),
            });
            return Promise.resolve([{ id }]);
          },
        }),
      };
      return builder;
    }
    if (table === 'statement_opera_only') {
      const builder: any = {
        where: (cond: Partial<OperaOnlyRow>) => {
          conds = { ...conds, ...cond };
          return builder;
        },
        orderBy: (col: string, dir: 'asc' | 'desc' = 'asc') => {
          order.push({ col, dir });
          return builder;
        },
        then: (cb: (rows: OperaOnlyRow[]) => unknown) => {
          const rows = state.operaOnly.filter((r) =>
            Object.entries(conds).every(([k, v]) => (r as any)[k] === v),
          );
          return Promise.resolve(cb(rows));
        },
      };
      return builder;
    }
    throw new Error(`Unexpected table: ${table}`);
  };
  db.fn = { now: () => new Date() };
  return db;
}

describe('listStatementLines', () => {
  it('returns lines + aggregates for a statement', async () => {
    const state: MockState = {
      lines: [
        { id: 1, statement_id: 100, line_date: '2026-04-10', reference: 'INV001', description: '', amount: 500, matched_opera_ref: null, match_status: 'matched' },
        { id: 2, statement_id: 100, line_date: '2026-04-12', reference: 'INV002', description: '', amount: 250, matched_opera_ref: null, match_status: 'unmatched' },
        { id: 3, statement_id: 100, line_date: '2026-04-15', reference: 'INV003', description: '', amount: 100, matched_opera_ref: null, match_status: 'disputed' },
        { id: 4, statement_id: 200, line_date: '2026-04-20', reference: 'OTHER', description: '', amount: 999, matched_opera_ref: null, match_status: 'matched' },
      ],
      operaOnly: [],
      nextId: 5,
      nextOpId: 1,
    };
    const result = await listStatementLines(makeAppDb(state), 100);
    expect(result.success).toBe(true);
    expect(result.count).toBe(3);
    expect(result.total_amount).toBe(850);
    expect(result.matched_count).toBe(1);
    expect(result.unmatched_count).toBe(1);
    expect(result.disputed_count).toBe(1);
  });

  it('rejects bad statement_id', async () => {
    const result = await listStatementLines(
      makeAppDb({ lines: [], operaOnly: [], nextId: 1, nextOpId: 1 }),
      0,
    );
    expect(result.success).toBe(false);
  });

  it('returns empty list when no lines', async () => {
    const result = await listStatementLines(
      makeAppDb({ lines: [], operaOnly: [], nextId: 1, nextOpId: 1 }),
      100,
    );
    expect(result.success).toBe(true);
    expect(result.count).toBe(0);
    expect(result.total_amount).toBe(0);
  });

  it('coerces unknown match_status to unmatched', async () => {
    const state: MockState = {
      lines: [
        { id: 1, statement_id: 100, line_date: '2026-04-10', reference: '', description: '', amount: 100, matched_opera_ref: null, match_status: 'weird' },
      ],
      operaOnly: [],
      nextId: 2,
      nextOpId: 1,
    };
    const result = await listStatementLines(makeAppDb(state), 100);
    expect(result.lines[0]?.match_status).toBe('unmatched');
  });
});

describe('addStatementLines', () => {
  it('bulk-inserts lines and returns ids', async () => {
    const state: MockState = { lines: [], operaOnly: [], nextId: 1, nextOpId: 1 };
    const result = await addStatementLines(makeAppDb(state), 100, [
      { reference: 'INV001', amount: 500 },
      { reference: 'INV002', amount: 250, match_status: 'matched' },
    ]);
    expect(result.success).toBe(true);
    expect(result.inserted).toBe(2);
    expect(result.ids).toEqual([1, 2]);
    expect(state.lines).toHaveLength(2);
    expect(state.lines[0]?.match_status).toBe('unmatched');
    expect(state.lines[1]?.match_status).toBe('matched');
  });

  it('returns inserted=0 for empty array', async () => {
    const result = await addStatementLines(
      makeAppDb({ lines: [], operaOnly: [], nextId: 1, nextOpId: 1 }),
      100,
      [],
    );
    expect(result.success).toBe(true);
    expect(result.inserted).toBe(0);
  });

  it('rejects bad statement_id', async () => {
    const result = await addStatementLines(
      makeAppDb({ lines: [], operaOnly: [], nextId: 1, nextOpId: 1 }),
      0,
      [{ amount: 100 }],
    );
    expect(result.success).toBe(false);
  });

  it('rejects bad match_status', async () => {
    const result = await addStatementLines(
      makeAppDb({ lines: [], operaOnly: [], nextId: 1, nextOpId: 1 }),
      100,
      [{ amount: 100, match_status: 'weird' as any }],
    );
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/match_status/);
  });

  it('rejects non-numeric amount', async () => {
    const result = await addStatementLines(
      makeAppDb({ lines: [], operaOnly: [], nextId: 1, nextOpId: 1 }),
      100,
      [{ amount: NaN }],
    );
    expect(result.success).toBe(false);
  });

  it('truncates reference + description to schema limits', async () => {
    const state: MockState = { lines: [], operaOnly: [], nextId: 1, nextOpId: 1 };
    await addStatementLines(makeAppDb(state), 100, [
      {
        reference: 'X'.repeat(200),
        description: 'D'.repeat(800),
        amount: 1,
      },
    ]);
    expect(state.lines[0]?.reference?.length).toBe(100);
    expect(state.lines[0]?.description?.length).toBe(500);
  });
});

describe('updateStatementLineMatch', () => {
  it('updates match_status', async () => {
    const state: MockState = {
      lines: [
        { id: 1, statement_id: 100, line_date: null, reference: '', description: '', amount: 100, matched_opera_ref: null, match_status: 'unmatched' },
      ],
      operaOnly: [],
      nextId: 2,
      nextOpId: 1,
    };
    const result = await updateStatementLineMatch(makeAppDb(state), 1, {
      match_status: 'matched',
      matched_opera_ref: 'PT_REF',
    });
    expect(result.success).toBe(true);
    expect(result.updated).toBe(true);
    expect(state.lines[0]?.match_status).toBe('matched');
    expect(state.lines[0]?.matched_opera_ref).toBe('PT_REF');
  });

  it('rejects bad match_status', async () => {
    const result = await updateStatementLineMatch(
      makeAppDb({ lines: [], operaOnly: [], nextId: 1, nextOpId: 1 }),
      1,
      { match_status: 'weird' as any },
    );
    expect(result.success).toBe(false);
  });

  it('rejects empty update', async () => {
    const result = await updateStatementLineMatch(
      makeAppDb({ lines: [], operaOnly: [], nextId: 1, nextOpId: 1 }),
      1,
      {},
    );
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/No fields to update/);
  });
});

describe('deleteStatementLines', () => {
  it('removes all lines for a statement', async () => {
    const state: MockState = {
      lines: [
        { id: 1, statement_id: 100, line_date: null, reference: '', description: '', amount: 1, matched_opera_ref: null, match_status: 'matched' },
        { id: 2, statement_id: 100, line_date: null, reference: '', description: '', amount: 2, matched_opera_ref: null, match_status: 'matched' },
        { id: 3, statement_id: 200, line_date: null, reference: '', description: '', amount: 3, matched_opera_ref: null, match_status: 'matched' },
      ],
      operaOnly: [],
      nextId: 4,
      nextOpId: 1,
    };
    const result = await deleteStatementLines(makeAppDb(state), 100);
    expect(result.success).toBe(true);
    expect(result.deleted).toBe(2);
    expect(state.lines).toHaveLength(1);
    expect(state.lines[0]?.statement_id).toBe(200);
  });
});

describe('listOperaOnlyItems', () => {
  it('returns items + total for a statement', async () => {
    const state: MockState = {
      lines: [],
      operaOnly: [
        { id: 1, statement_id: 100, reference: 'INV001', amount: 500, reason: 'Missing in Opera' },
        { id: 2, statement_id: 100, reference: 'INV002', amount: 250, reason: '' },
        { id: 3, statement_id: 200, reference: 'OTHER', amount: 9, reason: '' },
      ],
      nextId: 1,
      nextOpId: 4,
    };
    const result = await listOperaOnlyItems(makeAppDb(state), 100);
    expect(result.success).toBe(true);
    expect(result.count).toBe(2);
    expect(result.total_amount).toBe(750);
  });
});
