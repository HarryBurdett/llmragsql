import { describe, it, expect } from 'vitest';
import {
  listRemittanceLog,
  recordRemittance,
} from '../src/services/remittance-log.js';

interface MockState {
  rows: Array<Record<string, unknown> & { id: number }>;
  nextId: number;
}

function makeAppDb(state: MockState): any {
  const db: any = (table: string) => {
    if (table !== 'supplier_remittance_log') {
      throw new Error(`Unexpected table: ${table}`);
    }
    let filters: Record<string, unknown> = {};
    let limitN = Infinity;
    let dateFrom: string | null = null;
    let dateTo: string | null = null;
    const builder: any = {
      where: (col: Record<string, unknown> | string, val?: unknown) => {
        if (typeof col === 'object') Object.assign(filters, col);
        else if (val !== undefined) filters[col] = val;
        return builder;
      },
      andWhere: (col: string, op: string, val: string) => {
        if (col === 'sent_at') {
          if (op === '>=') dateFrom = val;
          if (op === '<=') dateTo = val;
        }
        return builder;
      },
      orderBy: () => builder,
      limit: (n: number) => {
        limitN = n;
        return builder;
      },
      then: (cb: (rows: unknown[]) => unknown) => {
        let rows = state.rows.filter((r) =>
          Object.keys(filters).every((k) => r[k] === filters[k]),
        );
        if (dateFrom) {
          rows = rows.filter((r) => String(r.sent_at ?? '') >= dateFrom!);
        }
        if (dateTo) {
          rows = rows.filter((r) => String(r.sent_at ?? '') <= dateTo!);
        }
        return Promise.resolve(cb(rows.slice(0, limitN)));
      },
      insert: (row: Record<string, unknown>) => {
        const id = state.nextId++;
        state.rows.push({ id, ...row, sent_at: new Date() } as any);
        return {
          returning: () => Promise.resolve([{ id }]),
        };
      },
    };
    return builder;
  };
  return db;
}

describe('listRemittanceLog', () => {
  it('returns rows with total_amount aggregated', async () => {
    const state: MockState = {
      rows: [
        {
          id: 1,
          supplier_code: 'SUPP001',
          to_address: 'a@x.com',
          subject: 'Remit 1',
          amount: 1500,
          sent_at: '2026-04-15T10:00:00Z',
        } as any,
        {
          id: 2,
          supplier_code: 'SUPP001',
          to_address: 'a@x.com',
          subject: 'Remit 2',
          amount: 750,
          sent_at: '2026-04-16T10:00:00Z',
        } as any,
      ],
      nextId: 3,
    };
    const db = makeAppDb(state);
    const result = await listRemittanceLog(db);
    expect(result.success).toBe(true);
    expect(result.count).toBe(2);
    expect(result.total_amount).toBe(2250);
  });

  it('filters by supplier_code', async () => {
    const state: MockState = {
      rows: [
        {
          id: 1,
          supplier_code: 'SUPP001',
          to_address: 'a@x.com',
          subject: '',
          amount: 100,
          sent_at: '2026-04-15T10:00:00Z',
        } as any,
        {
          id: 2,
          supplier_code: 'SUPP002',
          to_address: 'b@x.com',
          subject: '',
          amount: 200,
          sent_at: '2026-04-15T10:00:00Z',
        } as any,
      ],
      nextId: 3,
    };
    const db = makeAppDb(state);
    const result = await listRemittanceLog(db, { supplierCode: 'SUPP001' });
    expect(result.count).toBe(1);
    expect(result.entries[0]?.supplier_code).toBe('SUPP001');
  });

  it('respects limit', async () => {
    const state: MockState = {
      rows: Array.from({ length: 5 }, (_, i) => ({
        id: i + 1,
        supplier_code: 'SUPP001',
        to_address: 'a@x.com',
        subject: '',
        amount: 100,
        sent_at: `2026-04-${String(15 + i).padStart(2, '0')}T10:00:00Z`,
      } as any)),
      nextId: 6,
    };
    const db = makeAppDb(state);
    const result = await listRemittanceLog(db, { limit: 3 });
    expect(result.count).toBe(3);
  });
});

describe('recordRemittance', () => {
  it('rejects missing supplier_code', async () => {
    const db = makeAppDb({ rows: [], nextId: 1 });
    const result = await recordRemittance(db, {
      supplier_code: '',
      to_address: 'a@b.com',
      subject: 's',
      amount: 100,
    });
    expect(result.success).toBe(false);
  });

  it('rejects missing to_address', async () => {
    const db = makeAppDb({ rows: [], nextId: 1 });
    const result = await recordRemittance(db, {
      supplier_code: 'SUPP001',
      to_address: '',
      subject: 's',
      amount: 100,
    });
    expect(result.success).toBe(false);
  });

  it('rejects non-numeric amount', async () => {
    const db = makeAppDb({ rows: [], nextId: 1 });
    const result = await recordRemittance(db, {
      supplier_code: 'SUPP001',
      to_address: 'a@b.com',
      subject: 's',
      amount: NaN,
    });
    expect(result.success).toBe(false);
  });

  it('records a valid remittance', async () => {
    const state: MockState = { rows: [], nextId: 1 };
    const db = makeAppDb(state);
    const result = await recordRemittance(db, {
      supplier_code: 'SUPP001',
      to_address: 'ap@acme.com',
      subject: 'Remittance for April',
      amount: 1500,
    });
    expect(result.success).toBe(true);
    expect(result.entry?.id).toBe(1);
    expect(state.rows).toHaveLength(1);
  });
});
