import { describe, it, expect } from 'vitest';
import {
  listChangeAudit,
  recordChange,
  recordChangeIfDifferent,
} from '../src/services/change-audit.js';

interface Row {
  id: number;
  supplier_code: string;
  changed_field: string;
  old_value: string;
  new_value: string;
  changed_by: string;
  changed_at: string;
}

interface MockState {
  rows: Row[];
  nextId: number;
}

function makeAppDb(state: MockState): any {
  const db: any = (table: string) => {
    if (table !== 'supplier_change_audit') {
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
        if (col === 'changed_at') {
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
        if (dateFrom) rows = rows.filter((r) => r.changed_at >= dateFrom!);
        if (dateTo) rows = rows.filter((r) => r.changed_at <= dateTo!);
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
            changed_field: String(row.changed_field ?? ''),
            old_value: String(row.old_value ?? ''),
            new_value: String(row.new_value ?? ''),
            changed_by: String(row.changed_by ?? ''),
            changed_at: new Date().toISOString(),
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

describe('listChangeAudit', () => {
  it('returns rows in changed_at desc order, limited', async () => {
    const state: MockState = {
      rows: [
        { id: 1, supplier_code: 'S1', changed_field: 'auto_process', old_value: 'false', new_value: 'true', changed_by: 'admin', changed_at: '2026-04-10T10:00:00Z' },
        { id: 2, supplier_code: 'S1', changed_field: 'frequency', old_value: 'M', new_value: 'W', changed_by: 'admin', changed_at: '2026-04-15T10:00:00Z' },
      ],
      nextId: 3,
    };
    const result = await listChangeAudit(makeAppDb(state));
    expect(result.success).toBe(true);
    expect(result.count).toBe(2);
    expect(result.entries[0]?.id).toBe(2); // newer first
  });

  it('filters by supplier_code', async () => {
    const state: MockState = {
      rows: [
        { id: 1, supplier_code: 'S1', changed_field: 'x', old_value: '', new_value: '', changed_by: '', changed_at: '2026-04-10' },
        { id: 2, supplier_code: 'S2', changed_field: 'x', old_value: '', new_value: '', changed_by: '', changed_at: '2026-04-15' },
      ],
      nextId: 3,
    };
    const result = await listChangeAudit(makeAppDb(state), {
      supplierCode: 'S1',
    });
    expect(result.count).toBe(1);
    expect(result.entries[0]?.supplier_code).toBe('S1');
  });

  it('filters by changed_field', async () => {
    const state: MockState = {
      rows: [
        { id: 1, supplier_code: 'S1', changed_field: 'auto_process', old_value: 'false', new_value: 'true', changed_by: '', changed_at: '2026-04-10' },
        { id: 2, supplier_code: 'S1', changed_field: 'frequency', old_value: 'M', new_value: 'W', changed_by: '', changed_at: '2026-04-15' },
      ],
      nextId: 3,
    };
    const result = await listChangeAudit(makeAppDb(state), {
      changedField: 'frequency',
    });
    expect(result.count).toBe(1);
    expect(result.entries[0]?.changed_field).toBe('frequency');
  });
});

describe('recordChange', () => {
  it('records a primitive change', async () => {
    const state: MockState = { rows: [], nextId: 1 };
    const result = await recordChange(makeAppDb(state), {
      supplier_code: 'S1',
      changed_field: 'auto_process',
      old_value: false,
      new_value: true,
      changed_by: 'admin',
    });
    expect(result.success).toBe(true);
    expect(result.entry?.old_value).toBe('false');
    expect(result.entry?.new_value).toBe('true');
  });

  it('JSON-stringifies object values', async () => {
    const state: MockState = { rows: [], nextId: 1 };
    await recordChange(makeAppDb(state), {
      supplier_code: 'S1',
      changed_field: 'matching_rules',
      old_value: { mode: 'strict' },
      new_value: { mode: 'fuzzy', threshold: 0.8 },
    });
    expect(state.rows[0]?.old_value).toBe('{"mode":"strict"}');
    expect(state.rows[0]?.new_value).toBe(
      '{"mode":"fuzzy","threshold":0.8}',
    );
  });

  it('handles null/undefined as empty string', async () => {
    const state: MockState = { rows: [], nextId: 1 };
    await recordChange(makeAppDb(state), {
      supplier_code: 'S1',
      changed_field: 'notes',
      old_value: null,
      new_value: undefined,
    });
    expect(state.rows[0]?.old_value).toBe('');
    expect(state.rows[0]?.new_value).toBe('');
  });

  it('rejects missing supplier_code', async () => {
    const result = await recordChange(makeAppDb({ rows: [], nextId: 1 }), {
      supplier_code: '',
      changed_field: 'x',
    });
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/supplier_code/);
  });

  it('rejects missing changed_field', async () => {
    const result = await recordChange(makeAppDb({ rows: [], nextId: 1 }), {
      supplier_code: 'S1',
      changed_field: '',
    });
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/changed_field/);
  });
});

describe('recordChangeIfDifferent', () => {
  it('skips no-op writes', async () => {
    const state: MockState = { rows: [], nextId: 1 };
    const result = await recordChangeIfDifferent(makeAppDb(state), {
      supplier_code: 'S1',
      changed_field: 'auto_process',
      old_value: true,
      new_value: true,
    });
    expect(result.success).toBe(true);
    expect(result.entry).toBeUndefined();
    expect(state.rows).toHaveLength(0);
  });

  it('records when values differ', async () => {
    const state: MockState = { rows: [], nextId: 1 };
    const result = await recordChangeIfDifferent(makeAppDb(state), {
      supplier_code: 'S1',
      changed_field: 'auto_process',
      old_value: false,
      new_value: true,
    });
    expect(result.success).toBe(true);
    expect(result.entry).toBeDefined();
    expect(state.rows).toHaveLength(1);
  });

  it('detects deep differences in object values', async () => {
    const state: MockState = { rows: [], nextId: 1 };
    await recordChangeIfDifferent(makeAppDb(state), {
      supplier_code: 'S1',
      changed_field: 'rules',
      old_value: { x: 1, y: 2 },
      new_value: { x: 1, y: 3 },
    });
    expect(state.rows).toHaveLength(1);
  });

  it('treats equivalent objects as no-op', async () => {
    const state: MockState = { rows: [], nextId: 1 };
    await recordChangeIfDifferent(makeAppDb(state), {
      supplier_code: 'S1',
      changed_field: 'rules',
      old_value: { x: 1, y: 2 },
      new_value: { x: 1, y: 2 },
    });
    expect(state.rows).toHaveLength(0);
  });
});
