/**
 * Structural tests for reconcileCreditors.
 *
 * These verify the response shape matches the Python contract — the
 * frontend depends on specific keys at specific levels. Behavioural
 * parity (matching strategies, sign conventions) is harder to test
 * without a real Opera SQL fixture; this file pins the shape and
 * the easy-to-isolate behaviours.
 */
import { describe, it, expect } from 'vitest';
import { reconcileCreditors } from '../src/services/reconcile-creditors.js';

/**
 * Mock Knex that supports both the chainable builder API
 * (db(table).select().first(), .where()) AND the raw SQL API (db.raw(sql)).
 *
 * We canned-respond by inspecting the SQL string for keywords.
 */
function makeMockKnex(canned: {
  sprfls?: { debtors_control: string };
  pprfls?: { creditors_control: string };
  nparm?: { debtors_control: string; creditors_control: string };
  ptranOutstanding?: { transaction_count: number; total_outstanding: number };
  ptranBreakdown?: Array<{ type: string; count: number; total: number }>;
  pnameMaster?: { supplier_count: number; total_balance: number };
  ptranVariance?: unknown[];
  pnomlPending?: unknown[];
  pnomlSummary?: Array<{ status: string; count: number; total: number }>;
  ntranMaxYear?: { current_year: number };
  nacntControl?: Array<{
    na_acnt: string;
    na_desc: string;
    na_ytddr: number;
    na_ytdcr: number;
    na_prydr: number;
    na_prycr: number;
  }>;
  ntranCurrentByAcct?: { debits: number; credits: number; net: number };
  ntranByYear?: unknown[];
  // Variance analysis support
  ntranTransactions?: unknown[];
  pnameNames?: unknown[];
  ptranTransactions?: unknown[];
  // Aged + top
  agedRows?: unknown[];
  topSuppliers?: unknown[];
}): any {
  const db: any = (table: string) => {
    const filters: Record<string, unknown> = {};
    const builder: any = {
      _table: table,
      select: (..._cols: unknown[]) => builder,
      with: (..._args: unknown[]) => builder,
      where: (col: string | Record<string, unknown>, op?: any, val?: unknown) => {
        if (typeof col === 'object') Object.assign(filters, col);
        else if (val !== undefined) filters[col] = { op, val };
        else filters[col] = op;
        return builder;
      },
      andWhere: (col: string, op?: any, val?: unknown) => {
        if (val !== undefined) filters[col] = { op, val };
        else filters[col] = op;
        return builder;
      },
      andWhereNot: (col: string, val: unknown) => {
        filters[`!${col}`] = val;
        return builder;
      },
      whereRaw: (..._args: unknown[]) => builder,
      andWhereRaw: (..._args: unknown[]) => builder,
      first: async () => {
        if (table === 'sprfls') return canned.sprfls ?? null;
        if (table === 'pprfls') return canned.pprfls ?? null;
        if (table === 'nparm') return canned.nparm ?? null;
        return null;
      },
    };
    return builder;
  };
  db.raw = async (sql: string, _bindings?: unknown[]) => {
    // Identify by SQL keywords
    if (sql.includes('SUM(pt_trbal)') && sql.includes('COUNT(*)') && !sql.includes('GROUP BY')) {
      return [canned.ptranOutstanding ?? { transaction_count: 0, total_outstanding: 0 }];
    }
    if (sql.includes('GROUP BY pt_trtype')) {
      return canned.ptranBreakdown ?? [];
    }
    if (sql.includes('SUM(pn_currbal)')) {
      return [canned.pnameMaster ?? { supplier_count: 0, total_balance: 0 }];
    }
    if (sql.includes('LEFT JOIN') && sql.includes('pname m WITH')) {
      return canned.ptranVariance ?? [];
    }
    if (sql.includes('FROM pnoml WITH (NOLOCK)') && sql.includes('px_done <> ')) {
      return canned.pnomlPending ?? [];
    }
    if (sql.includes("CASE WHEN px_done = 'Y'")) {
      return canned.pnomlSummary ?? [];
    }
    if (sql.includes('MAX(nt_year)')) {
      return [canned.ntranMaxYear ?? { current_year: 2026 }];
    }
    if (sql.includes('SELECT na_acnt, na_desc')) {
      return canned.nacntControl ?? [];
    }
    if (sql.includes('SUM(nt_value) AS net') && sql.includes('GROUP BY nt_year')) {
      return canned.ntranByYear ?? [];
    }
    if (sql.includes('SUM(nt_value) AS net') && sql.includes('AND nt_year = ?')) {
      return [canned.ntranCurrentByAcct ?? { debits: 0, credits: 0, net: 0 }];
    }
    if (sql.includes('FROM ntran') && sql.includes('nt_cmnt')) {
      return canned.ntranTransactions ?? [];
    }
    if (sql.includes('FROM pname') && sql.includes('pn_account') && sql.includes('pn_name')) {
      return canned.pnameNames ?? [];
    }
    if (sql.includes('FROM ptran') && sql.includes('pt_trref')) {
      return canned.ptranTransactions ?? [];
    }
    if (sql.includes('age_band')) {
      return canned.agedRows ?? [];
    }
    if (sql.includes('TOP 10') && sql.includes('pn_account')) {
      return canned.topSuppliers ?? [];
    }
    return [];
  };
  return db;
}

describe('reconcileCreditors', () => {
  it('returns the expected response shape', async () => {
    const db = makeMockKnex({
      sprfls: { debtors_control: '1100' },
      pprfls: { creditors_control: '2100' },
      ptranOutstanding: { transaction_count: 5, total_outstanding: 12345.67 },
      ptranBreakdown: [{ type: 'I', count: 5, total: 12345.67 }],
      pnameMaster: { supplier_count: 3, total_balance: 12345.67 },
      ptranVariance: [],
      pnomlPending: [],
      pnomlSummary: [
        { status: 'Posted', count: 5, total: 12345.67 },
        { status: 'Pending', count: 0, total: 0 },
      ],
      ntranMaxYear: { current_year: 2026 },
      nacntControl: [
        {
          na_acnt: '2100',
          na_desc: 'Trade Creditors Control',
          na_ytddr: 0,
          na_ytdcr: 0,
          na_prydr: 0,
          na_prycr: 0,
        },
      ],
      ntranCurrentByAcct: { debits: 0, credits: 12345.67, net: -12345.67 },
      ntranByYear: [],
      agedRows: [],
      topSuppliers: [],
    });

    const result = await reconcileCreditors(db);

    expect(result.success).toBe(true);
    expect(result.purchase_ledger.source).toBe('ptran (Purchase Ledger Transactions)');
    expect(result.purchase_ledger.total_outstanding).toBe(12345.67);
    expect(result.purchase_ledger.transaction_count).toBe(5);
    expect(result.nominal_ledger.control_accounts).toHaveLength(1);
    expect(result.nominal_ledger.control_accounts[0]?.account).toBe('2100');
    // Creditors negate ntran for comparison; net = -12345.67 → comparison balance = 12345.67
    expect(result.nominal_ledger.control_accounts[0]?.closing_balance).toBe(12345.67);
    expect(result.variance.reconciled).toBe(true);
    expect(result.status).toBe('RECONCILED');
    expect(result.control_account_used).toBe('2100');
  });

  it('reports UNRECONCILED with variance details when PL ≠ NL', async () => {
    const db = makeMockKnex({
      sprfls: { debtors_control: '1100' },
      pprfls: { creditors_control: '2100' },
      ptranOutstanding: { transaction_count: 5, total_outstanding: 12345.67 },
      ptranBreakdown: [],
      pnameMaster: { supplier_count: 3, total_balance: 12345.67 },
      ptranVariance: [],
      pnomlPending: [],
      pnomlSummary: [
        { status: 'Posted', count: 4, total: 10000 },
        { status: 'Pending', count: 1, total: 2345.67 },
      ],
      ntranMaxYear: { current_year: 2026 },
      nacntControl: [
        {
          na_acnt: '2100',
          na_desc: 'Trade Creditors Control',
          na_ytddr: 0,
          na_ytdcr: 0,
          na_prydr: 0,
          na_prycr: 0,
        },
      ],
      // ntran shows £100 less than PL — creditors negate so net = -12245.67 → 12245.67
      ntranCurrentByAcct: { debits: 0, credits: 12245.67, net: -12245.67 },
      ntranByYear: [],
      agedRows: [],
      topSuppliers: [],
    });

    const result = await reconcileCreditors(db);

    expect(result.variance.reconciled).toBe(false);
    expect(result.status).toBe('UNRECONCILED');
    expect(result.variance.amount).toBe(100); // PL > NL by £100
    expect(result.message).toMatch(/MORE/);
  });

  it('returns success=false when control accounts cannot be determined', async () => {
    // Mock db that throws on every config-table query — getControlAccounts will
    // exhaust all fallbacks and throw the "not found" error.
    const db: any = () => {
      const builder: any = {
        select: () => builder,
        first: async () => {
          throw new Error('connection refused');
        },
      };
      return builder;
    };
    db.raw = (sql: string) => {
      throw new Error('connection refused');
    };

    const result = await reconcileCreditors(db);
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/Debtors control account not found|Creditors control account not found|connection refused/);
  });
});
