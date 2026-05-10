import { describe, it, expect } from 'vitest';
import { persistExtractedStatement } from '../src/services/extract-statement.js';

interface AppState {
  statements: Array<{
    id: number;
    supplier_code: string;
    statement_date: string;
    opening_balance: number;
    closing_balance: number;
    source: string;
    source_ref: string;
    pdf_path: string;
  }>;
  lines: Array<{
    id: number;
    statement_id: number;
    line_date: string;
    reference: string;
    description: string;
    amount: number;
    matched_opera_ref: string;
    match_status: string;
  }>;
  nextStatementId: number;
  nextLineId: number;
}

function makeAppDb(state: AppState): any {
  const fn = { now: () => new Date() };
  const txMode = (table: string) => {
    if (table === 'supplier_statements') {
      let conds: any = {};
      const builder: any = {
        where: (c: any) => {
          conds = { ...conds, ...c };
          return builder;
        },
        first: () =>
          Promise.resolve(
            state.statements.find((r) =>
              Object.entries(conds).every(([k, v]) => (r as any)[k] === v),
            ),
          ),
        update: (row: any) => {
          for (const r of state.statements) {
            if (
              Object.entries(conds).every(
                ([k, v]) => (r as any)[k] === v,
              )
            ) {
              Object.assign(r, row);
            }
          }
          return Promise.resolve(1);
        },
        insert: (row: any) => ({
          returning: (col: string) => {
            const id = state.nextStatementId++;
            state.statements.push({
              id,
              supplier_code: row.supplier_code,
              statement_date: row.statement_date,
              opening_balance: row.opening_balance,
              closing_balance: row.closing_balance,
              source: row.source,
              source_ref: row.source_ref,
              pdf_path: row.pdf_path ?? '',
            });
            return Promise.resolve([{ [col]: id }]);
          },
        }),
        delete: () => {
          const before = state.statements.length;
          state.statements = state.statements.filter(
            (r) =>
              !Object.entries(conds).every(([k, v]) => (r as any)[k] === v),
          );
          return Promise.resolve(before - state.statements.length);
        },
      };
      return builder;
    }
    if (table === 'statement_lines') {
      let conds: any = {};
      const builder: any = {
        where: (c: any) => {
          conds = { ...conds, ...c };
          return builder;
        },
        delete: () => {
          const before = state.lines.length;
          state.lines = state.lines.filter(
            (r) =>
              !Object.entries(conds).every(([k, v]) => (r as any)[k] === v),
          );
          return Promise.resolve(before - state.lines.length);
        },
        insert: (rows: any) => {
          const arr = Array.isArray(rows) ? rows : [rows];
          for (const r of arr) {
            state.lines.push({
              id: state.nextLineId++,
              statement_id: r.statement_id,
              line_date: r.line_date,
              reference: r.reference ?? '',
              description: r.description ?? '',
              amount: Number(r.amount ?? 0),
              matched_opera_ref: r.matched_opera_ref ?? '',
              match_status: r.match_status ?? 'unmatched',
            });
          }
          return Promise.resolve([]);
        },
      };
      return builder;
    }
    throw new Error(`Unexpected table: ${table}`);
  };
  const db: any = (table: string) => txMode(table);
  db.fn = fn;
  db.transaction = async (fn2: (trx: any) => Promise<unknown>) => {
    const trx: any = (table: string) => txMode(table);
    trx.fn = db.fn;
    return fn2(trx);
  };
  return db;
}

describe('persistExtractedStatement', () => {
  it('rejects missing required fields', async () => {
    const db = makeAppDb({
      statements: [],
      lines: [],
      nextStatementId: 1,
      nextLineId: 1,
    });
    const r = await persistExtractedStatement(db, {
      supplierCode: '',
      statementDate: '2026-04-15',
      openingBalance: 0,
      closingBalance: 100,
      source: 'manual',
      sourceRef: 'X',
      lines: [],
    });
    expect(r.success).toBe(false);
    expect(r.error).toMatch(/supplier_code/);
  });

  it('inserts a new statement with lines', async () => {
    const state = {
      statements: [],
      lines: [],
      nextStatementId: 100,
      nextLineId: 200,
    };
    const r = await persistExtractedStatement(makeAppDb(state), {
      supplierCode: 'W034',
      statementDate: '2026-04-15',
      openingBalance: 1000,
      closingBalance: 1500,
      source: 'email',
      sourceRef: 'EMAIL_X|ATT_Y',
      lines: [
        {
          line_date: '2026-04-10',
          reference: 'INV1',
          description: 'Item',
          amount: 250,
        },
        {
          line_date: '2026-04-12',
          reference: 'INV2',
          description: 'Item2',
          amount: 250,
        },
      ],
    });
    expect(r.success).toBe(true);
    expect(r.statement_id).toBe(100);
    expect(r.inserted_lines).toBe(2);
    expect(r.updated).toBe(false);
    expect(state.statements).toHaveLength(1);
    expect(state.lines).toHaveLength(2);
  });

  it('replaces an existing statement under the same source_ref', async () => {
    const state: AppState = {
      statements: [
        {
          id: 99,
          supplier_code: 'OLD',
          statement_date: '2025-01-01',
          opening_balance: 0,
          closing_balance: 100,
          source: 'email',
          source_ref: 'KEY1',
          pdf_path: '',
        },
      ],
      lines: [
        {
          id: 1,
          statement_id: 99,
          line_date: '2025-01-01',
          reference: 'OLD',
          description: '',
          amount: 100,
          matched_opera_ref: '',
          match_status: 'unmatched',
        },
      ],
      nextStatementId: 100,
      nextLineId: 2,
    };
    const r = await persistExtractedStatement(makeAppDb(state), {
      supplierCode: 'NEW',
      statementDate: '2026-04-15',
      openingBalance: 0,
      closingBalance: 200,
      source: 'email',
      sourceRef: 'KEY1',
      lines: [
        {
          line_date: '2026-04-15',
          reference: 'NEW',
          description: '',
          amount: 200,
        },
      ],
    });
    expect(r.success).toBe(true);
    expect(r.updated).toBe(true);
    expect(r.statement_id).toBe(99);
    expect(state.statements[0]?.supplier_code).toBe('NEW');
    expect(state.lines).toHaveLength(1);
    expect(state.lines[0]?.reference).toBe('NEW');
  });
});
