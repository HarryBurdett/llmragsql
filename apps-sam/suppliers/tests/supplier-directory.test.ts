import { describe, it, expect } from 'vitest';
import { listSupplierDirectory } from '../src/services/supplier-directory.js';

interface OperaRow {
  pn_account: string;
  pn_name: string;
  pn_email: string | null;
  pn_teleno: string | null;
  pn_contact: string | null;
  pn_currbal: number;
  pn_dormant: number;
}

interface StmtRow {
  supplier_code: string;
  imported_at: string;
}

interface SenderRow {
  supplier_code: string;
}

interface MockState {
  pname: OperaRow[];
  statements: StmtRow[];
  senders: SenderRow[];
}

function makeOperaDb(state: MockState): any {
  const db: any = (table: string) => {
    if (table !== 'pname') throw new Error(`Unexpected table: ${table}`);
    let conds: Array<{ col: string; op: string; val: any }> = [];
    let likes: Array<{ col: string; pattern: string }> = [];
    let limitN = Infinity;
    let inOrGroup = false;
    let orPatterns: Array<{ col: string; pattern: string }> = [];
    const builder: any = {
      where: (col: any, op?: any, val?: any) => {
        if (typeof col === 'string') {
          if (op === 'like' && typeof val === 'string') {
            if (inOrGroup) {
              orPatterns.push({ col, pattern: val });
            } else {
              likes.push({ col, pattern: val });
            }
          } else if (op === '<>') {
            conds.push({ col, op: '<>', val });
          } else if (op === 0) {
            conds.push({ col, op: '=', val: 0 });
          } else {
            conds.push({ col, op: '=', val: op });
          }
        } else {
          for (const [k, v] of Object.entries(col)) {
            conds.push({ col: k, op: '=', val: v });
          }
        }
        return builder;
      },
      orWhere: (col: any, op: any, val: any) => {
        if (typeof col === 'string' && op === 'like' && typeof val === 'string') {
          orPatterns.push({ col, pattern: val });
        }
        return builder;
      },
      andWhere: (cb: any, op?: any, val?: any) => {
        if (typeof cb === 'function') {
          inOrGroup = true;
          cb(builder);
          inOrGroup = false;
        } else {
          builder.where(cb, op, val);
        }
        return builder;
      },
      orderBy: () => builder,
      limit: (n: number) => {
        limitN = n;
        return builder;
      },
      select: async (..._cols: any[]) => {
        const filtered = state.pname.filter((r) => {
          for (const c of conds) {
            const lhs = (r as any)[c.col];
            if (c.op === '=' && lhs !== c.val) return false;
            if (c.op === '<>' && lhs === c.val) return false;
          }
          for (const l of likes) {
            const lhs = ((r as any)[l.col] ?? '').toString().toUpperCase();
            const pat = l.pattern.replace(/%/g, '').toUpperCase();
            if (!lhs.includes(pat)) return false;
          }
          if (orPatterns.length > 0) {
            const matched = orPatterns.some((l) => {
              const lhs = ((r as any)[l.col] ?? '').toString().toUpperCase();
              const pat = l.pattern.replace(/%/g, '').toUpperCase();
              return lhs.includes(pat);
            });
            if (!matched) return false;
          }
          return true;
        });
        const slice = filtered.slice(0, limitN);
        return slice.map((r) => ({
          account: r.pn_account,
          name: r.pn_name,
          email: r.pn_email,
          phone: r.pn_teleno,
          contact: r.pn_contact,
          balance: r.pn_currbal,
        }));
      },
    };
    return builder;
  };
  db.raw = (s: string) => s;
  return db;
}

function makeAppDb(state: MockState): any {
  const db: any = (table: string) => {
    let inCol: string | null = null;
    let inVals: any[] = [];
    const builder: any = {
      whereIn: (col: string, vals: any[]) => {
        inCol = col;
        inVals = vals;
        return builder;
      },
      groupBy: () => builder,
      select: async (..._cols: any[]) => {
        if (table === 'supplier_statements') {
          const filtered = inCol
            ? state.statements.filter((r) => inVals.includes((r as any)[inCol!]))
            : state.statements;
          // Group by supplier_code
          const map = new Map<string, { count: number; last: string }>();
          for (const r of filtered) {
            const m = map.get(r.supplier_code);
            if (!m) {
              map.set(r.supplier_code, { count: 1, last: r.imported_at });
            } else {
              m.count++;
              if (r.imported_at > m.last) m.last = r.imported_at;
            }
          }
          return Array.from(map.entries()).map(([k, v]) => ({
            supplier_code: k,
            statement_count: v.count,
            last_statement: v.last,
          }));
        }
        if (table === 'supplier_approved_emails') {
          const filtered = inCol
            ? state.senders.filter((r) => inVals.includes((r as any)[inCol!]))
            : state.senders;
          const map = new Map<string, number>();
          for (const r of filtered) {
            map.set(r.supplier_code, (map.get(r.supplier_code) ?? 0) + 1);
          }
          return Array.from(map.entries()).map(([k, v]) => ({
            supplier_code: k,
            sender_count: v,
          }));
        }
        throw new Error(`Unexpected table: ${table}`);
      },
    };
    return builder;
  };
  db.raw = (s: string) => s;
  db.fn = { now: () => '__NOW__' };
  return db;
}

function row(over: Partial<OperaRow> = {}): OperaRow {
  return {
    pn_account: 'SUPP01',
    pn_name: 'Acme Suppliers',
    pn_email: 'a@a.com',
    pn_teleno: '01234',
    pn_contact: 'John',
    pn_currbal: 1000,
    pn_dormant: 0,
    ...over,
  };
}

describe('listSupplierDirectory', () => {
  it('lists active suppliers with non-zero balance when no search supplied', async () => {
    const state: MockState = {
      pname: [
        row({ pn_account: 'SUPP01', pn_currbal: 1000 }),
        row({ pn_account: 'SUPP02', pn_currbal: 0 }), // excluded
        row({ pn_account: 'SUPP03', pn_currbal: -500 }), // included (creditor)
      ],
      statements: [],
      senders: [],
    };
    const result = await listSupplierDirectory(
      makeOperaDb(state),
      makeAppDb(state),
    );
    expect(result.success).toBe(true);
    expect(result.count).toBe(2);
  });

  it('search mode matches name OR account (LIKE %term%)', async () => {
    const state: MockState = {
      pname: [
        row({ pn_account: 'SUPP01', pn_name: 'Acme Suppliers' }),
        row({ pn_account: 'SUPP02', pn_name: 'Beta Trading' }),
        row({ pn_account: 'ACME99', pn_name: 'Different Co' }),
      ],
      statements: [],
      senders: [],
    };
    const result = await listSupplierDirectory(
      makeOperaDb(state),
      makeAppDb(state),
      { search: 'Acme' },
    );
    // Both rows where 'Acme' is in name OR account
    expect(result.count).toBe(2);
    const accounts = result.suppliers.map((s) => s.account).sort();
    expect(accounts).toEqual(['ACME99', 'SUPP01']);
  });

  it('excludes dormant suppliers', async () => {
    const state: MockState = {
      pname: [
        row({ pn_account: 'SUPP01' }),
        row({ pn_account: 'SUPP02', pn_dormant: 1 }), // excluded
      ],
      statements: [],
      senders: [],
    };
    const result = await listSupplierDirectory(
      makeOperaDb(state),
      makeAppDb(state),
    );
    expect(result.count).toBe(1);
    expect(result.suppliers[0]?.account).toBe('SUPP01');
  });

  it('attaches statement_count and last_statement from app DB', async () => {
    const state: MockState = {
      pname: [row({ pn_account: 'SUPP01' })],
      statements: [
        { supplier_code: 'SUPP01', imported_at: '2026-04-01' },
        { supplier_code: 'SUPP01', imported_at: '2026-05-15' },
        { supplier_code: 'SUPP01', imported_at: '2026-03-10' },
      ],
      senders: [],
    };
    const result = await listSupplierDirectory(
      makeOperaDb(state),
      makeAppDb(state),
    );
    expect(result.suppliers[0]?.statement_count).toBe(3);
    expect(result.suppliers[0]?.last_statement).toBe('2026-05-15');
  });

  it('attaches approved_senders count', async () => {
    const state: MockState = {
      pname: [row({ pn_account: 'SUPP01' })],
      statements: [],
      senders: [
        { supplier_code: 'SUPP01' },
        { supplier_code: 'SUPP01' },
      ],
    };
    const result = await listSupplierDirectory(
      makeOperaDb(state),
      makeAppDb(state),
    );
    expect(result.suppliers[0]?.approved_senders).toBe(2);
  });

  it('zero counts when supplier has no statements / senders', async () => {
    const state: MockState = {
      pname: [row({ pn_account: 'SUPP01' })],
      statements: [{ supplier_code: 'OTHER', imported_at: '2026-04-01' }],
      senders: [{ supplier_code: 'OTHER' }],
    };
    const result = await listSupplierDirectory(
      makeOperaDb(state),
      makeAppDb(state),
    );
    expect(result.suppliers[0]?.statement_count).toBe(0);
    expect(result.suppliers[0]?.approved_senders).toBe(0);
  });

  it('works without an appDb (statement counts default to 0)', async () => {
    const state: MockState = {
      pname: [row({ pn_account: 'SUPP01' })],
      statements: [],
      senders: [],
    };
    const result = await listSupplierDirectory(makeOperaDb(state), null);
    expect(result.success).toBe(true);
    expect(result.suppliers[0]?.statement_count).toBe(0);
    expect(result.suppliers[0]?.approved_senders).toBe(0);
  });

  it('reports DB error gracefully', async () => {
    const operaDb: any = (_t: string) => {
      const builder: any = {
        where: () => builder,
        andWhere: () => builder,
        orWhere: () => builder,
        orderBy: () => builder,
        limit: () => builder,
        select: () => Promise.reject(new Error('DB unavailable')),
      };
      return builder;
    };
    operaDb.raw = (s: string) => s;
    const result = await listSupplierDirectory(operaDb, null);
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/DB unavailable/);
  });
});
