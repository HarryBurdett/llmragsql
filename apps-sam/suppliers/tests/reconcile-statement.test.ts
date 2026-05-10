import { describe, it, expect } from 'vitest';
import { reconcileStatement } from '../src/services/reconcile-statement.js';

interface AppState {
  statements: Array<{
    id: number;
    supplier_code: string;
    statement_date: string;
    opening_balance: number;
    closing_balance: number;
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
}

interface OperaPtran {
  pt_account: string;
  pt_trref: string;
  pt_supref: string;
  pt_trdate: string;
  pt_trvalue: number;
  pt_trbal: number;
}

function makeAppDb(state: AppState): any {
  const tx = (table: string) => {
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
      };
      return builder;
    }
    if (table === 'statement_lines') {
      let conds: any = {};
      let order: string | null = null;
      const builder: any = {
        where: (c: any) => {
          conds = { ...conds, ...c };
          return builder;
        },
        orderBy: (col: string) => {
          order = col;
          return builder;
        },
        update: (data: any) => {
          for (const r of state.lines) {
            if (
              Object.entries(conds).every(
                ([k, v]) => (r as any)[k] === v,
              )
            ) {
              Object.assign(r, data);
            }
          }
          return Promise.resolve(1);
        },
        then: (resolve: any) => {
          let rows = state.lines.filter((r) =>
            Object.entries(conds).every(
              ([k, v]) => (r as any)[k] === v,
            ),
          );
          if (order) {
            rows = [...rows].sort((a, b) =>
              String((a as any)[order!]).localeCompare(
                String((b as any)[order!]),
              ),
            );
          }
          resolve(rows);
        },
      };
      return builder;
    }
    throw new Error(`Unexpected: ${table}`);
  };
  const db: any = (table: string) => tx(table);
  db.transaction = async (fn: any) => {
    const trx: any = (table: string) => tx(table);
    return fn(trx);
  };
  return db;
}

function makeOperaDb(ptran: OperaPtran[]): any {
  return {
    raw: (sql: string, params?: unknown[]) => {
      if (sql.includes('FROM ptran')) {
        const acct = String((params ?? [])[0] ?? '').trim();
        const matches = ptran.filter((p) => p.pt_account.trim() === acct);
        return Promise.resolve(
          matches.map((p) => ({
            pt_trref: p.pt_trref,
            pt_supref: p.pt_supref,
            pt_trdate: p.pt_trdate,
            pt_trvalue: p.pt_trvalue,
            pt_trbal: p.pt_trbal,
          })),
        );
      }
      return Promise.resolve([]);
    },
  };
}

describe('reconcileStatement', () => {
  it('rejects missing statement', async () => {
    const r = await reconcileStatement(
      makeAppDb({ statements: [], lines: [] }),
      makeOperaDb([]),
      { statementId: 999 },
    );
    expect(r.success).toBe(false);
    expect(r.error).toMatch(/not found/);
  });

  it('matches by reference and reports variance', async () => {
    const state: AppState = {
      statements: [
        {
          id: 1,
          supplier_code: 'W034',
          statement_date: '2026-04-30',
          opening_balance: 0,
          closing_balance: 500,
        },
      ],
      lines: [
        {
          id: 10,
          statement_id: 1,
          line_date: '2026-04-15',
          reference: 'INV001',
          description: '',
          amount: 200,
          matched_opera_ref: '',
          match_status: 'unmatched',
        },
        {
          id: 11,
          statement_id: 1,
          line_date: '2026-04-20',
          reference: 'INV002',
          description: '',
          amount: 200,
          matched_opera_ref: '',
          match_status: 'unmatched',
        },
        {
          id: 12,
          statement_id: 1,
          line_date: '2026-04-25',
          reference: 'INV003',
          description: '',
          amount: 100,
          matched_opera_ref: '',
          match_status: 'unmatched',
        },
      ],
    };
    const ptran: OperaPtran[] = [
      {
        pt_account: 'W034',
        pt_trref: 'INV001',
        pt_supref: '',
        pt_trdate: '2026-04-15',
        pt_trvalue: 200,
        pt_trbal: 0,
      },
      {
        pt_account: 'W034',
        pt_trref: 'INV002',
        pt_supref: '',
        pt_trdate: '2026-04-20',
        pt_trvalue: 200,
        pt_trbal: 0,
      },
      // INV003 missing in Opera — will show as missing
    ];
    const r = await reconcileStatement(
      makeAppDb(state),
      makeOperaDb(ptran),
      { statementId: 1 },
    );
    expect(r.success).toBe(true);
    expect(r.summary?.matched_count).toBe(2);
    expect(r.summary?.missing_count).toBe(1);
    expect(r.summary?.matched_total).toBe(400);
    expect(r.summary?.missing_total).toBe(100);
  });

  it('falls back to amount + ±7 day match when reference is empty', async () => {
    const state: AppState = {
      statements: [
        {
          id: 2,
          supplier_code: 'W050',
          statement_date: '2026-04-30',
          opening_balance: 0,
          closing_balance: 75,
        },
      ],
      lines: [
        {
          id: 20,
          statement_id: 2,
          line_date: '2026-04-10',
          reference: '',
          description: 'Just a description',
          amount: 75,
          matched_opera_ref: '',
          match_status: 'unmatched',
        },
      ],
    };
    const ptran: OperaPtran[] = [
      {
        pt_account: 'W050',
        pt_trref: 'PI777',
        pt_supref: '',
        pt_trdate: '2026-04-12',
        pt_trvalue: 75,
        pt_trbal: 0,
      },
    ];
    const r = await reconcileStatement(
      makeAppDb(state),
      makeOperaDb(ptran),
      { statementId: 2 },
    );
    expect(r.success).toBe(true);
    expect(r.summary?.matched_count).toBe(1);
    expect(r.lines?.[0]?.matched_opera_ref).toBe('PI777');
  });
});
