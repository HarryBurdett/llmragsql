import { describe, it, expect, vi } from 'vitest';
import {
  getCreditorsDashboard,
  getCreditorsReport,
  searchCreditors,
  getCreditorsSupplier,
  getCreditorsSupplierTransactions,
  getStatementPdf,
  previewStatementResponse,
  sendUpdatedStatementStatus,
  extractStatementFromText,
  processStatementEmail,
  reconcileStatementByEmail,
  listReconciliations,
  getFlaggedEmails,
  getSupplierAccountByCode,
  getFirstSupplierAccount,
  type LlmService,
} from '../src/services/misc-endpoints.js';

function makeOperaDb(rawHandler: (sql: string, params: any[]) => Promise<any>): any {
  const db: any = (_table: string) => ({
    where: () => ({}),
  });
  db.raw = rawHandler;
  return db;
}

function makeAppDb(handlers: Record<string, any>): any {
  function tableBuilder(table: string) {
    let idFilter: number | null = null;
    let codeFilter: string | null = null;
    let mode: string | null = null;
    let createdBefore: string | null = null;
    let statusList: string[] | null = null;
    const builder: any = {
      where: (cond: any) => {
        if (typeof cond === 'object') {
          if (cond.id) idFilter = cond.id;
          if (cond.statement_id) idFilter = cond.statement_id;
          if (cond.changed_field) mode = cond.changed_field;
          if (cond.source) codeFilter = String(cond.source_ref ?? cond.source);
          if (cond.source_ref) codeFilter = cond.source_ref;
        }
        return builder;
      },
      whereIn: (col: string, vals: string[]) => {
        if (col === 'ss.status') statusList = vals;
        return builder;
      },
      andWhere: (col: any, op?: any, val?: any) => {
        if (col === 'created_at' && op === '<') createdBefore = val;
        return builder;
      },
      andWhereNot: () => builder,
      orderBy: () => builder,
      limit: () => builder,
      groupBy: () => builder,
      leftJoin: () => builder,
      select: () => builder,
      first: async () => {
        if (table === 'supplier_statements' && idFilter !== null) {
          return handlers.supplier_statement?.(idFilter);
        }
        if (table === 'supplier_statements') {
          return handlers.supplier_statement_email?.(codeFilter);
        }
        return undefined;
      },
      insert: async () => [1],
      update: async () => 1,
      then: async (resolve: any) => {
        if (table === 'statement_lines' && idFilter !== null) {
          return resolve(handlers.statement_lines?.(idFilter) ?? []);
        }
        if (table === 'supplier_change_audit' && mode === 'pn_email') {
          return resolve(handlers.email_flags?.() ?? []);
        }
        if (
          table === 'supplier_statements as ss' &&
          (statusList || createdBefore !== null)
        ) {
          return resolve(handlers.reconciliations?.() ?? []);
        }
        if (table === 'supplier_communications') {
          return resolve(handlers.communications?.() ?? []);
        }
        return resolve([]);
      },
    };
    return builder;
  }
  const db: any = (table: string) => tableBuilder(table);
  db.fn = { now: () => '__NOW__' };
  db.raw = (s: string) => s;
  return db;
}

describe('getCreditorsDashboard', () => {
  it('aggregates counts + totals + overdue', async () => {
    const db = makeOperaDb(async (sql) => {
      if (sql.includes('total_outstanding')) {
        return [{ total_suppliers: 5, total_outstanding: 1000 }];
      }
      if (sql.includes('overdue')) return [{ overdue: 2 }];
      return [];
    });
    const r = await getCreditorsDashboard(db);
    expect(r.success).toBe(true);
    expect(r.total_suppliers).toBe(5);
    expect(r.overdue_count).toBe(2);
  });
});

describe('getCreditorsReport / searchCreditors', () => {
  it('returns list', async () => {
    const db = makeOperaDb(async () => [
      { account: 'A001', name: 'Acme', current_balance: 100, credit_limit: 1000, contact_email: 'a@a.com' },
    ]);
    const r = await getCreditorsReport(db);
    expect(r.suppliers.length).toBe(1);
  });

  it('searchCreditors short query returns empty', async () => {
    const r = await searchCreditors(makeOperaDb(async () => []), 'a');
    expect(r.suppliers).toEqual([]);
  });
});

describe('getCreditorsSupplier / Transactions', () => {
  it('returns supplier when present', async () => {
    const db = makeOperaDb(async () => [
      { account: 'A001', name: 'Acme', current_balance: 100, credit_limit: 1000, contact_email: '' },
    ]);
    const r = await getCreditorsSupplier(db, 'A001');
    expect(r.success).toBe(true);
    expect(r.supplier?.account).toBe('A001');
  });

  it('returns transactions', async () => {
    const db = makeOperaDb(async () => [
      { date: '2026-04-15', reference: 'INV1', type: 'I', value: 100, balance: 100, comment: '' },
    ]);
    const r = await getCreditorsSupplierTransactions(db, 'A001');
    expect(r.transactions.length).toBe(1);
  });

  it('errors when supplier not found', async () => {
    const db = makeOperaDb(async () => []);
    const r = await getCreditorsSupplier(db, 'GHOST');
    expect(r.success).toBe(false);
  });
});

describe('getStatementPdf', () => {
  it('errors when no pdf_path', async () => {
    const r = await getStatementPdf(
      makeAppDb({
        supplier_statement: () => ({ pdf_path: null }),
      }),
      1,
    );
    expect(r.success).toBe(false);
  });
  it('returns path when present', async () => {
    const r = await getStatementPdf(
      makeAppDb({
        supplier_statement: () => ({ pdf_path: '/x/y.pdf' }),
      }),
      1,
    );
    expect(r.success).toBe(true);
    expect(r.filename).toBe('y.pdf');
  });
});

describe('previewStatementResponse', () => {
  it('503 when llm missing', async () => {
    const r = await previewStatementResponse(
      makeAppDb({
        supplier_statement: () => ({
          supplier_code: 'A001',
          statement_date: '2026-04-15',
          closing_balance: 100,
        }),
        statement_lines: () => [],
      }),
      null,
      1,
    );
    expect(r.success).toBe(false);
  });
  it('returns body when llm provided', async () => {
    const llm: LlmService = {
      chat() {
        async function* gen(): AsyncIterable<unknown> {
          yield 'Drafted body';
        }
        return gen();
      },
    };
    const r = await previewStatementResponse(
      makeAppDb({
        supplier_statement: () => ({
          supplier_code: 'A001',
          statement_date: '2026-04-15',
          closing_balance: 100,
        }),
        statement_lines: () => [],
      }),
      llm,
      1,
    );
    expect(r.success).toBe(true);
    expect(r.body).toBe('Drafted body');
  });
});

describe('sendUpdatedStatementStatus', () => {
  it('updates and logs comm', async () => {
    const r = await sendUpdatedStatementStatus(
      makeAppDb({}),
      1,
      'received',
      'admin',
    );
    expect(r.success).toBe(true);
  });
});

describe('extractStatementFromText', () => {
  it('503 when llm missing', async () => {
    const r = await extractStatementFromText(null, 'text');
    expect(r.success).toBe(false);
  });
  it('parses JSON output', async () => {
    const llm: LlmService = {
      chat() {
        async function* gen(): AsyncIterable<unknown> {
          yield JSON.stringify({
            supplier_code: 'A001',
            statement_date: '2026-04-15',
            opening_balance: 100,
            closing_balance: 200,
            currency: 'GBP',
            lines: [],
          });
        }
        return gen();
      },
    };
    const r = await extractStatementFromText(llm, 'text');
    expect(r.success).toBe(true);
    expect(r.extraction?.supplier_code).toBe('A001');
  });
});

describe('processStatementEmail', () => {
  it('rejects invalid email_id', async () => {
    const r = await processStatementEmail(makeAppDb({}), 0);
    expect(r.success).toBe(false);
  });
});

describe('reconcileStatementByEmail', () => {
  it('rejects invalid email_id', async () => {
    const r = await reconcileStatementByEmail(makeAppDb({}), 0);
    expect(r.success).toBe(false);
  });
});

describe('listReconciliations', () => {
  it('returns mapped rows', async () => {
    const r = await listReconciliations(
      makeAppDb({
        reconciliations: () => [
          {
            id: 1,
            supplier_code: 'A001',
            statement_date: '2026-04-15',
            status: 'reconciled',
            approved_at: null,
            matched_count: 5,
            query_count: 1,
          },
        ],
      }),
    );
    expect(r.reconciliations.length).toBe(1);
  });
});

describe('getFlaggedEmails', () => {
  it('returns audit rows', async () => {
    const r = await getFlaggedEmails(
      makeAppDb({
        email_flags: () => [
          {
            id: 1,
            supplier_code: 'A001',
            new_value: 'new@a.com',
            changed_by: 'scan',
            changed_at: '2026-04-15T00:00:00Z',
          },
        ],
      }),
    );
    expect(r.flags.length).toBe(1);
    expect(r.flags[0]?.email_address).toBe('new@a.com');
  });
});

describe('getSupplierAccountByCode / getFirstSupplierAccount', () => {
  it('returns first supplier', async () => {
    const db = makeOperaDb(async () => [
      { account: 'A001', name: 'Acme', email: '', phone: '', address: '' },
    ]);
    const r = await getFirstSupplierAccount(db);
    expect(r.success).toBe(true);
    expect(r.supplier?.account).toBe('A001');
  });
  it('errors when none', async () => {
    const db = makeOperaDb(async () => []);
    const r = await getSupplierAccountByCode(db, 'GHOST');
    expect(r.success).toBe(false);
  });
});
