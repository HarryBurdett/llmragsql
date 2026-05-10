import { describe, it, expect } from 'vitest';
import { importGocardlessBatch } from '../src/services/import-batch.js';

/**
 * Validation/branching tests for `importGocardlessBatch`.
 *
 * The full Opera write flow is exercised against a real Opera SQL Server
 * in the integration harness. These unit tests pin the
 * pre-transaction guard rails: payload validation, currency check,
 * fees-nominal requirement, customer existence, period rejection,
 * validate_only short-circuit. The actual transactional writes are
 * not exercised here — that lives in the integration suite.
 */

interface OperaState {
  /** sname rows keyed by sn_account */
  customers: Record<
    string,
    {
      sn_name: string;
      sn_region?: string | null;
      sn_terrtry?: string | null;
      sn_custype?: string | null;
    }
  >;
  /** Optional override of the home-currency lookup */
  homeCurrencyCode?: string;
  homeCurrencyDescription?: string;
  /** Period decision overrides */
  periodCanPost?: boolean;
  periodErrorMessage?: string;
  /** Atype rows for cbtype lookup */
  atypeRows?: Array<{
    ay_cbtype: string;
    ay_desc: string;
    ay_type: 'R' | 'P' | 'T';
    ay_batched: number;
  }>;
}

function makeOperaDb(state: OperaState): any {
  return {
    raw: (sql: string, params?: unknown[]) => {
      // Home-currency lookup
      if (sql.includes('FROM seqco') || sql.includes('co_basecurr')) {
        return Promise.resolve([
          {
            code: state.homeCurrencyCode ?? 'GBP',
            description: state.homeCurrencyDescription ?? 'Pound Sterling',
          },
        ]);
      }
      // Atype lookup (auto-detect cbtype)
      if (
        typeof sql === 'string' &&
        sql.includes('atype') &&
        sql.includes("ay_type = 'R'") &&
        sql.includes('ay_batched = 1')
      ) {
        const matches = (state.atypeRows ?? []).filter(
          (r) => r.ay_type === 'R' && r.ay_batched === 1,
        );
        return Promise.resolve(matches.length > 0 ? [matches[0]] : []);
      }
      // Atype lookup (specific cbtype validation)
      if (
        typeof sql === 'string' &&
        sql.includes('atype') &&
        sql.includes('RTRIM(ay_cbtype) = ?')
      ) {
        const code = String((params ?? [])[0] ?? '');
        const found = (state.atypeRows ?? []).find(
          (r) => r.ay_cbtype.trim() === code,
        );
        return Promise.resolve(found ? [found] : []);
      }
      // Customer lookup
      if (
        typeof sql === 'string' &&
        sql.includes('FROM sname') &&
        sql.includes('sn_name')
      ) {
        const acct = String((params ?? [])[0] ?? '');
        const c = state.customers[acct];
        return Promise.resolve(
          c
            ? [
                {
                  sn_name: c.sn_name,
                  sn_region: c.sn_region ?? null,
                  sn_terrtry: c.sn_terrtry ?? null,
                  sn_custype: c.sn_custype ?? null,
                },
              ]
            : [],
        );
      }
      // Period decision: nclndd lookup
      if (sql.includes('nclndd')) {
        return Promise.resolve([{ ncd_period: 4, ncd_year: 2026 }]);
      }
      // nparm current-period info
      if (sql.includes('nparm')) {
        return Promise.resolve([
          { np_year: 2026, np_perno: 4, np_periods: 12 },
        ]);
      }
      // seqco OPA / RT
      if (sql.includes('seqco')) {
        return Promise.resolve([{ co_opanl: 1, co_rtupdnl: 1 }]);
      }
      return Promise.resolve([]);
    },
    transaction: async (_fn: any) => {
      // Tests in this file should never reach the transaction; if they
      // do, fail loudly so we know the validation gate didn't trigger.
      throw new Error('Test should not reach transaction phase');
    },
  };
}

describe('importGocardlessBatch validation', () => {
  it('rejects empty payments', async () => {
    const db = makeOperaDb({ customers: {} });
    const result = await importGocardlessBatch(db, {
      bankAccount: 'BC010',
      payments: [],
      postDate: '2026-04-15',
    });
    expect(result.success).toBe(false);
    expect(result.errors?.[0]).toMatch(/No payments/);
  });

  it('rejects foreign currency', async () => {
    const db = makeOperaDb({
      customers: {},
      homeCurrencyCode: 'GBP',
      homeCurrencyDescription: 'Pound Sterling',
    });
    const result = await importGocardlessBatch(db, {
      bankAccount: 'BC010',
      payments: [{ customer_account: 'CUST01', amount: 100 }],
      postDate: '2026-04-15',
      currency: 'EUR',
    });
    expect(result.success).toBe(false);
    expect(result.errors?.[0]).toMatch(/EUR.*GBP/);
  });

  it('rejects fees > 0 with no fees nominal account', async () => {
    const db = makeOperaDb({ customers: {} });
    const result = await importGocardlessBatch(db, {
      bankAccount: 'BC010',
      payments: [{ customer_account: 'CUST01', amount: 100 }],
      postDate: '2026-04-15',
      goCardlessFees: 5,
    });
    expect(result.success).toBe(false);
    expect(result.errors?.[0]).toMatch(/fees_nominal_account/);
  });

  it('rejects bad bank code with input-validation error', async () => {
    const db = makeOperaDb({ customers: {} });
    const result = await importGocardlessBatch(db, {
      bankAccount: "BC';--",
      payments: [{ customer_account: 'CUST01', amount: 100 }],
      postDate: '2026-04-15',
    });
    expect(result.success).toBe(false);
    expect(result.errors?.[0]).toMatch(/bank/);
  });

  it('rejects missing customer', async () => {
    const db = makeOperaDb({
      customers: {},
      atypeRows: [
        {
          ay_cbtype: 'R1',
          ay_desc: 'GoCardless Receipt',
          ay_type: 'R',
          ay_batched: 1,
        },
      ],
    });
    const result = await importGocardlessBatch(db, {
      bankAccount: 'BC010',
      payments: [{ customer_account: 'GHOST', amount: 100 }],
      postDate: '2026-04-15',
    });
    expect(result.success).toBe(false);
    expect(result.errors?.[0]).toMatch(/Customer account 'GHOST' not found/);
  });

  it('rejects when no batched receipt cbtype is configured', async () => {
    const db = makeOperaDb({
      customers: {
        CUST01: { sn_name: 'Acme Ltd' },
      },
      atypeRows: [],
    });
    const result = await importGocardlessBatch(db, {
      bankAccount: 'BC010',
      payments: [{ customer_account: 'CUST01', amount: 100 }],
      postDate: '2026-04-15',
    });
    expect(result.success).toBe(false);
    expect(result.errors?.[0]).toMatch(/No batched Receipt type codes/);
  });

  it('rejects when explicit cbtype is not type R', async () => {
    const db = makeOperaDb({
      customers: {
        CUST01: { sn_name: 'Acme Ltd' },
      },
      atypeRows: [
        {
          ay_cbtype: 'NP',
          ay_desc: 'Nominal Payment',
          ay_type: 'P',
          ay_batched: 0,
        },
      ],
    });
    const result = await importGocardlessBatch(db, {
      bankAccount: 'BC010',
      payments: [{ customer_account: 'CUST01', amount: 100 }],
      postDate: '2026-04-15',
      cbtype: 'NP',
    });
    expect(result.success).toBe(false);
    expect(result.errors?.[0]).toMatch(/'R' \(Receipt\) is required/);
  });

  it('returns validate_only summary without writing', async () => {
    const db = makeOperaDb({
      customers: {
        CUST01: { sn_name: 'Acme Ltd' },
        CUST02: { sn_name: 'Other Ltd' },
      },
      atypeRows: [
        {
          ay_cbtype: 'R1',
          ay_desc: 'GoCardless Receipt',
          ay_type: 'R',
          ay_batched: 1,
        },
      ],
    });
    const result = await importGocardlessBatch(db, {
      bankAccount: 'BC010',
      payments: [
        { customer_account: 'CUST01', amount: 100 },
        { customer_account: 'CUST02', amount: 50 },
      ],
      postDate: '2026-04-15',
      validateOnly: true,
    });
    expect(result.success).toBe(true);
    expect(result.records_imported).toBe(2);
    expect(result.warnings?.[0]).toMatch(/Validation passed for 2 payments/);
  });

  it('flags duplicate customer+amount in validate_only warnings', async () => {
    const db = makeOperaDb({
      customers: {
        CUST01: { sn_name: 'Acme Ltd' },
      },
      atypeRows: [
        {
          ay_cbtype: 'R1',
          ay_desc: 'GoCardless Receipt',
          ay_type: 'R',
          ay_batched: 1,
        },
      ],
    });
    const result = await importGocardlessBatch(db, {
      bankAccount: 'BC010',
      payments: [
        { customer_account: 'CUST01', amount: 100 },
        { customer_account: 'CUST01', amount: 100 },
      ],
      postDate: '2026-04-15',
      validateOnly: true,
    });
    expect(result.success).toBe(true);
    const dupWarning = (result.warnings ?? []).find(
      (w) => typeof w === 'string' && w.includes('Duplicate'),
    );
    expect(dupWarning).toBeDefined();
  });
});
