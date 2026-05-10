import { describe, it, expect } from 'vitest';
import { postCashbookEntry } from '../src/services/cashbook-poster.js';

/**
 * Validation/branching tests for the unified cashbook poster.
 *
 * The full transactional write path is exercised against a real Opera
 * SQL Server in the integration harness. These unit tests pin the
 * pre-transaction guard rails: amount sign, bank-code injection guard,
 * cbtype category mismatch, missing partner, validate_only.
 *
 * The four kinds (sales_receipt / sales_refund / purchase_payment /
 * purchase_refund) share validation logic so the same gates apply to
 * each — we test one happy-path validate_only per kind.
 */

interface OperaState {
  customers: Record<string, { sn_name: string; sn_region?: string | null }>;
  suppliers: Record<string, { pn_name: string }>;
  atypeRows: Array<{
    ay_cbtype: string;
    ay_desc: string;
    ay_type: 'R' | 'P' | 'T';
    ay_batched: number;
  }>;
  /** Override for getControlAccounts defaults */
  defaultDebtors?: string;
  defaultCreditors?: string;
}

function makeOperaDb(state: OperaState): any {
  // Builder used when getControlAccounts does db('sprfls').select(...).first()
  const builderFor = (table: string): any => {
    const builder: any = {
      _table: table,
      select: () => builder,
      first: () => {
        if (table === 'sprfls') {
          return Promise.resolve(
            state.defaultDebtors
              ? { debtors_control: state.defaultDebtors }
              : null,
          );
        }
        if (table === 'pprfls') {
          return Promise.resolve(
            state.defaultCreditors
              ? { creditors_control: state.defaultCreditors }
              : null,
          );
        }
        if (table === 'nparm') {
          return Promise.resolve({
            debtors_control: state.defaultDebtors ?? null,
            creditors_control: state.defaultCreditors ?? null,
          });
        }
        return Promise.resolve(null);
      },
    };
    return builder;
  };
  const db: any = (table: string) => builderFor(table);
  db.raw = (sql: string, params?: unknown[]) => {
      // sname customer lookup
      if (sql.includes('FROM sname') && sql.includes('sn_name')) {
        const acct = String((params ?? [])[0] ?? '');
        const c = state.customers[acct];
        return Promise.resolve(
          c
            ? [
                {
                  sn_name: c.sn_name,
                  sn_region: c.sn_region ?? null,
                  sn_terrtry: null,
                  sn_custype: null,
                },
              ]
            : [],
        );
      }
      // pname supplier lookup
      if (sql.includes('FROM pname') && sql.includes('pn_name')) {
        const acct = String((params ?? [])[0] ?? '');
        const s = state.suppliers[acct];
        return Promise.resolve(s ? [{ pn_name: s.pn_name }] : []);
      }
      // atype default-cbtype lookup
      if (
        sql.includes('atype') &&
        sql.includes('ay_batched = 0') &&
        sql.includes('ORDER BY ay_cbtype')
      ) {
        const cat = String((params ?? [])[0] ?? '');
        const m = state.atypeRows.find(
          (r) => r.ay_type === cat && r.ay_batched === 0,
        );
        return Promise.resolve(m ? [{ ay_cbtype: m.ay_cbtype }] : []);
      }
      // atype specific cbtype
      if (sql.includes('atype') && sql.includes('RTRIM(ay_cbtype) = ?')) {
        const code = String((params ?? [])[0] ?? '');
        const m = state.atypeRows.find((r) => r.ay_cbtype.trim() === code);
        return Promise.resolve(m ? [m] : []);
      }
      // sprfls/pprfls/nparm fallback control account lookups (return empty defaults)
      if (sql.includes('sprfls')) {
        return Promise.resolve(
          state.defaultDebtors
            ? [{ debtors_control: state.defaultDebtors }]
            : [],
        );
      }
      if (sql.includes('pprfls')) {
        return Promise.resolve(
          state.defaultCreditors
            ? [{ creditors_control: state.defaultCreditors }]
            : [],
        );
      }
      if (sql.includes('nparm')) {
        return Promise.resolve([
          {
            debtors_control: state.defaultDebtors ?? null,
            creditors_control: state.defaultCreditors ?? null,
          },
        ]);
      }
      return Promise.resolve([]);
    };
  db.transaction = async () => {
    throw new Error('Test should not reach transaction phase');
  };
  return db;
}

describe('postCashbookEntry — validation', () => {
  it('rejects negative amount', async () => {
    const db = makeOperaDb({
      customers: {},
      suppliers: {},
      atypeRows: [],
    });
    const r = await postCashbookEntry(db, {
      kind: 'sales_receipt',
      bankAccount: 'BC010',
      partnerAccount: 'CUST01',
      amountPounds: -10,
      reference: 'X',
      postDate: '2026-04-15',
    });
    expect(r.success).toBe(false);
    expect(r.errors?.[0]).toMatch(/positive/i);
  });

  it('rejects bad bank code', async () => {
    const db = makeOperaDb({
      customers: {},
      suppliers: {},
      atypeRows: [],
    });
    const r = await postCashbookEntry(db, {
      kind: 'sales_receipt',
      bankAccount: "BC';--",
      partnerAccount: 'CUST01',
      amountPounds: 10,
      reference: 'X',
      postDate: '2026-04-15',
    });
    expect(r.success).toBe(false);
    expect(r.errors?.[0]).toMatch(/bank/);
  });

  it('rejects when no Receipt cbtype configured', async () => {
    const db = makeOperaDb({
      customers: { CUST01: { sn_name: 'Acme' } },
      suppliers: {},
      atypeRows: [],
      defaultDebtors: '1100',
      defaultCreditors: '2100',
    });
    const r = await postCashbookEntry(db, {
      kind: 'sales_receipt',
      bankAccount: 'BC010',
      partnerAccount: 'CUST01',
      amountPounds: 100,
      reference: 'INV1',
      postDate: '2026-04-15',
    });
    expect(r.success).toBe(false);
    expect(r.errors?.[0]).toMatch(/Receipt/);
  });

  it('rejects when supplied cbtype has wrong category', async () => {
    const db = makeOperaDb({
      customers: { CUST01: { sn_name: 'Acme' } },
      suppliers: {},
      atypeRows: [
        { ay_cbtype: 'NP', ay_desc: 'Nominal Payment', ay_type: 'P', ay_batched: 0 },
      ],
      defaultDebtors: '1100',
      defaultCreditors: '2100',
    });
    const r = await postCashbookEntry(db, {
      kind: 'sales_receipt',
      bankAccount: 'BC010',
      partnerAccount: 'CUST01',
      amountPounds: 100,
      reference: 'INV1',
      postDate: '2026-04-15',
      cbtype: 'NP',
    });
    expect(r.success).toBe(false);
    expect(r.errors?.[0]).toMatch(/'R' \(Receipt\) is required/);
  });

  it('rejects missing customer for sales_receipt', async () => {
    const db = makeOperaDb({
      customers: {},
      suppliers: {},
      atypeRows: [
        { ay_cbtype: 'NR', ay_desc: 'Receipt', ay_type: 'R', ay_batched: 0 },
      ],
      defaultDebtors: '1100',
      defaultCreditors: '2100',
    });
    const r = await postCashbookEntry(db, {
      kind: 'sales_receipt',
      bankAccount: 'BC010',
      partnerAccount: 'GHOST',
      amountPounds: 100,
      reference: 'INV1',
      postDate: '2026-04-15',
    });
    expect(r.success).toBe(false);
    expect(r.errors?.[0]).toMatch(/Customer account 'GHOST' not found/);
  });

  it('rejects missing supplier for purchase_payment', async () => {
    const db = makeOperaDb({
      customers: {},
      suppliers: {},
      atypeRows: [
        { ay_cbtype: 'NP', ay_desc: 'Payment', ay_type: 'P', ay_batched: 0 },
      ],
      defaultDebtors: '1100',
      defaultCreditors: '2100',
    });
    const r = await postCashbookEntry(db, {
      kind: 'purchase_payment',
      bankAccount: 'BC010',
      partnerAccount: 'GHOST',
      amountPounds: 100,
      reference: 'BACS1',
      postDate: '2026-04-15',
    });
    expect(r.success).toBe(false);
    expect(r.errors?.[0]).toMatch(/Supplier account 'GHOST' not found/);
  });

  it.each([
    ['sales_receipt', 'R', 'CUST'] as const,
    ['sales_refund', 'P', 'CUST'] as const,
    ['purchase_payment', 'P', 'SUPP'] as const,
    ['purchase_refund', 'R', 'SUPP'] as const,
  ])('returns validate_only for %s', async (kind, cat, who) => {
    const db = makeOperaDb({
      customers: { [who]: { sn_name: 'Customer' } },
      suppliers: { [who]: { pn_name: 'Supplier' } },
      atypeRows: [
        {
          ay_cbtype: cat === 'R' ? 'NR' : 'NP',
          ay_desc: 'X',
          ay_type: cat,
          ay_batched: 0,
        },
      ],
      defaultDebtors: '1100',
      defaultCreditors: '2100',
    });
    const r = await postCashbookEntry(db, {
      kind,
      bankAccount: 'BC010',
      partnerAccount: who,
      amountPounds: 100,
      reference: 'X',
      postDate: '2026-04-15',
      validateOnly: true,
    });
    expect(r.success).toBe(true);
    expect(r.warnings?.[0]).toMatch(/Validation passed/);
  });
});
