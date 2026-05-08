import { describe, it, expect } from 'vitest';
import {
  listStatements,
  getStatement,
} from '../src/services/supplier-statements.js';

interface MockState {
  statements: Array<Record<string, unknown> & { id: number }>;
  lines: Array<Record<string, unknown> & { id: number; statement_id: number }>;
  operaOnly: Array<Record<string, unknown> & { id: number; statement_id: number }>;
}

function makeAppDb(state: MockState): any {
  const db: any = (table: string) => {
    let data: Array<Record<string, unknown>>;
    if (table === 'supplier_statements') data = state.statements;
    else if (table === 'statement_lines') data = state.lines;
    else if (table === 'statement_opera_only') data = state.operaOnly;
    else throw new Error(`Unexpected table: ${table}`);

    let filters: Record<string, unknown> = {};
    let limitN = Infinity;
    const builder: any = {
      where: (col: Record<string, unknown> | string, val?: unknown) => {
        if (typeof col === 'object') Object.assign(filters, col);
        else if (val !== undefined) filters[col] = val;
        return builder;
      },
      andWhere: () => builder,
      orderBy: () => builder,
      limit: (n: number) => {
        limitN = n;
        return builder;
      },
      first: async () => data.find((r) =>
        Object.keys(filters).every((k) => r[k] === filters[k]),
      ),
      then: (cb: (rows: unknown[]) => unknown) => {
        const filtered = data.filter((r) =>
          Object.keys(filters).every((k) => r[k] === filters[k]),
        );
        return Promise.resolve(cb(filtered.slice(0, limitN)));
      },
    };
    return builder;
  };
  return db;
}

describe('listStatements', () => {
  it('returns all statements when no filters', async () => {
    const db = makeAppDb({
      statements: [
        {
          id: 1,
          supplier_code: 'SUPP001',
          statement_date: '2026-04-15',
          opening_balance: 1000,
          closing_balance: 1500,
          source: 'email',
          source_ref: 'msg-123',
          pdf_path: '/path/to/pdf',
          imported_at: '2026-04-15T10:00:00Z',
        },
      ],
      lines: [],
      operaOnly: [],
    });
    const result = await listStatements(db);
    expect(result.success).toBe(true);
    expect(result.count).toBe(1);
    expect(result.statements[0]?.supplier_code).toBe('SUPP001');
  });

  it('filters by supplier_code', async () => {
    const db = makeAppDb({
      statements: [
        {
          id: 1,
          supplier_code: 'SUPP001',
          statement_date: '2026-04-15',
          opening_balance: 0,
          closing_balance: 0,
          source: '',
          source_ref: '',
          pdf_path: '',
          imported_at: '2026-04-15T10:00:00Z',
        },
        {
          id: 2,
          supplier_code: 'SUPP002',
          statement_date: '2026-04-15',
          opening_balance: 0,
          closing_balance: 0,
          source: '',
          source_ref: '',
          pdf_path: '',
          imported_at: '2026-04-15T10:00:00Z',
        },
      ],
      lines: [],
      operaOnly: [],
    });
    const result = await listStatements(db, { supplierCode: 'SUPP001' });
    expect(result.count).toBe(1);
    expect(result.statements[0]?.supplier_code).toBe('SUPP001');
  });

  it('respects limit', async () => {
    const db = makeAppDb({
      statements: [
        { id: 1, supplier_code: 'SUPP001', imported_at: '2026-04-15T10:00:00Z' } as any,
        { id: 2, supplier_code: 'SUPP001', imported_at: '2026-04-16T10:00:00Z' } as any,
        { id: 3, supplier_code: 'SUPP001', imported_at: '2026-04-17T10:00:00Z' } as any,
      ],
      lines: [],
      operaOnly: [],
    });
    const result = await listStatements(db, { limit: 2 });
    expect(result.count).toBe(2);
  });
});

describe('getStatement', () => {
  it('returns header + lines + opera-only', async () => {
    const db = makeAppDb({
      statements: [
        {
          id: 1,
          supplier_code: 'SUPP001',
          statement_date: '2026-04-15',
          opening_balance: 1000,
          closing_balance: 1500,
          source: 'email',
          source_ref: 'msg-123',
          pdf_path: '',
          imported_at: '2026-04-15T10:00:00Z',
        },
      ],
      lines: [
        {
          id: 10,
          statement_id: 1,
          line_date: '2026-04-10',
          reference: 'INV001',
          description: 'Invoice 001',
          amount: 500,
          matched_opera_ref: 'PT00001',
          match_status: 'matched',
        } as any,
        {
          id: 11,
          statement_id: 1,
          line_date: '2026-04-12',
          reference: 'INV002',
          description: 'Invoice 002',
          amount: 1000,
          matched_opera_ref: '',
          match_status: 'unmatched',
        } as any,
      ],
      operaOnly: [
        {
          id: 100,
          statement_id: 1,
          reference: 'PT99999',
          amount: 250,
          reason: 'In Opera ptran but not on statement',
        } as any,
      ],
    });
    const result = await getStatement(db, 1);
    expect(result.success).toBe(true);
    expect(result.statement?.header.supplier_code).toBe('SUPP001');
    expect(result.statement?.lines).toHaveLength(2);
    expect(result.statement?.opera_only).toHaveLength(1);
    expect(result.statement?.lines[0]?.match_status).toBe('matched');
  });

  it('returns 404-style response when not found', async () => {
    const db = makeAppDb({ statements: [], lines: [], operaOnly: [] });
    const result = await getStatement(db, 999);
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/not found/);
  });

  it('rejects invalid id', async () => {
    const db = makeAppDb({ statements: [], lines: [], operaOnly: [] });
    const result = await getStatement(db, 0);
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/Invalid/);
  });
});
