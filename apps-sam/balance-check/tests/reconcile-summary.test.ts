/**
 * Tests for the reconcile summary service.
 *
 * Mirrors the behavioural assertions from the Python implementation:
 *   - All 4 checks run independently (one failure doesn't break others)
 *   - Variance comparisons are exact to the penny (no tolerance)
 *   - Output structure matches the frontend contract
 *   - NL creditors total is negated; NL VAT total is negated
 *
 * The service takes a Knex pool. We mock it with a chainable builder
 * that returns canned rows per table.
 */
import { describe, it, expect, beforeEach } from 'vitest';
import { reconcileSummary } from '../src/services/reconcile-summary.js';
import type { ReconcileCheck } from '../src/types.js';

interface MockResponses {
  // Per-table canned `.first()` responses. Some tables are queried
  // multiple times with different filters — use a function to vary.
  sprfls?: { debtors_control: string };
  pprfls?: { creditors_control: string };
  nparm?: { debtors_control: string; creditors_control: string };
  ntran?: (filters: Record<string, unknown>) => Record<string, number | null>;
  stran?: { total: number };
  sname?: { total: number };
  ptran?: { total: number };
  pname?: { total: number };
  nbank?: Array<{ nk_acnt: string; nk_curbal: number }>;
  nvat?: (filters: Record<string, unknown>) => { total: number | null };
  ztax?: Array<{ tx_nominal: string; tx_trantyp: string }>;
}

/**
 * Build a minimal Knex-shaped mock that returns canned results.
 *
 * We support the subset of Knex APIs the service uses:
 *   db(table).select(raw).first()
 *   db(table).select(raw).where(...).andWhere(...).first()
 *   db(table).select(...) (returns full array)
 *   db(table).distinct(...).where(...).andWhereNot(...)
 *   db.raw(sql)
 */
function makeMockKnex(responses: MockResponses): any {
  const db: any = (table: string) => {
    const filters: Record<string, unknown> = {};
    const builder: any = {
      _table: table,
      select: (..._cols: unknown[]) => builder,
      distinct: (..._cols: unknown[]) => builder,
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
      whereRaw: (_sql: string, _bindings?: unknown[]) => builder,
      andWhereRaw: (_sql: string, _bindings?: unknown[]) => builder,
      first: async () => {
        // Return canned data based on table
        if (table === 'sprfls') return responses.sprfls ?? null;
        if (table === 'pprfls') return responses.pprfls ?? null;
        if (table === 'nparm') return responses.nparm ?? null;
        if (table === 'ntran') {
          if (typeof responses.ntran === 'function') {
            return responses.ntran(filters);
          }
          return null;
        }
        if (table === 'stran') return responses.stran ?? { total: 0 };
        if (table === 'sname') return responses.sname ?? { total: 0 };
        if (table === 'ptran') return responses.ptran ?? { total: 0 };
        if (table === 'pname') return responses.pname ?? { total: 0 };
        if (table === 'nvat') {
          if (typeof responses.nvat === 'function') {
            return responses.nvat(filters);
          }
          return null;
        }
        return null;
      },
      then: async (cb: (rows: unknown[]) => unknown) => {
        // Promise-style for builders without .first() (e.g. nbank.select())
        if (table === 'nbank') return cb(responses.nbank ?? []);
        if (table === 'ztax') return cb(responses.ztax ?? []);
        return cb([]);
      },
    };
    return builder;
  };
  db.raw = (sql: string) => sql;
  return db;
}

describe('reconcileSummary', () => {
  beforeEach(() => {
    // Each test creates its own mock with fresh state
  });

  it('returns success=true when all checks reconcile', async () => {
    const db = makeMockKnex({
      sprfls: { debtors_control: '1100' },
      pprfls: { creditors_control: '2100' },
      // ntran responses keyed by which check is asking
      ntran: (filters) => {
        // First call: SELECT MAX(nt_year) — fiscal current year
        // We can identify it because there are no where filters.
        if (Object.keys(filters).length === 0) {
          return { current_year: 2026 };
        }
        // Debtors NL: filtered by nt_acnt='1100' AND nt_year
        if (filters.nt_acnt === '1100') return { total: 5000 };
        // Creditors NL: nt_acnt='2100' (returned as -3000 → negated to 3000)
        if (filters.nt_acnt === '2100') return { total: -3000 };
        // Bank NL: nt_acnt='BC010'
        if (filters.nt_acnt === 'BC010') return { total: 10000 };
        // VAT NL — by nominal account on calendar year
        return { total: 0 };
      },
      stran: { total: 5000 },
      sname: { total: 5000 },
      ptran: { total: 3000 },
      pname: { total: 3000 },
      nbank: [{ nk_acnt: 'BC010', nk_curbal: 1000000 }], // 10000 in pence
      nvat: () => ({ total: 0 }),
      ztax: [],
    });

    const result = await reconcileSummary(db);

    expect(result.success).toBe(true);
    expect(result.checks).toHaveLength(4);
    expect(result.checks.map((c) => c.name)).toEqual([
      'Debtors',
      'Creditors',
      'Cashbook',
      'VAT',
    ]);
    expect(result.total_checks).toBe(4);
  });

  it('marks a check as not-reconciled when the variance is non-zero', async () => {
    const db = makeMockKnex({
      sprfls: { debtors_control: '1100' },
      pprfls: { creditors_control: '2100' },
      ntran: (filters) => {
        if (Object.keys(filters).length === 0) return { current_year: 2026 };
        if (filters.nt_acnt === '1100') return { total: 4999 }; // mismatch
        if (filters.nt_acnt === '2100') return { total: -3000 };
        return { total: 0 };
      },
      stran: { total: 5000 },
      sname: { total: 5000 },
      ptran: { total: 3000 },
      pname: { total: 3000 },
      nbank: [],
      nvat: () => ({ total: 0 }),
      ztax: [],
    });

    const result = await reconcileSummary(db);
    const debtors = result.checks.find((c) => c.name === 'Debtors')!;

    expect(debtors.reconciled).toBe(false);
    expect(debtors.variances).toBeDefined();
    const slVsNl = debtors.variances!.find((v) => v.label === 'SL vs NL')!;
    expect(slVsNl.value).toBe(1);
    expect(slVsNl.ok).toBe(false);
  });

  it('returns error on a check when its query throws — other checks still run', async () => {
    const db = makeMockKnex({
      sprfls: { debtors_control: '1100' },
      pprfls: { creditors_control: '2100' },
      ntran: (filters) => {
        if (Object.keys(filters).length === 0) return { current_year: 2026 };
        if (filters.nt_acnt === '1100') {
          throw new Error('simulated query failure');
        }
        if (filters.nt_acnt === '2100') return { total: -3000 };
        return { total: 0 };
      },
      stran: { total: 5000 },
      sname: { total: 5000 },
      ptran: { total: 3000 },
      pname: { total: 3000 },
      nbank: [],
      nvat: () => ({ total: 0 }),
      ztax: [],
    });

    const result = await reconcileSummary(db);
    const debtors = result.checks.find((c) => c.name === 'Debtors')!;
    const creditors = result.checks.find((c) => c.name === 'Creditors')!;

    expect(debtors.reconciled).toBe(false);
    expect(debtors.error).toMatch(/simulated query failure/);
    // Creditors still ran
    expect(creditors.reconciled).toBe(true);
  });

  it('negates the NL creditors total — Python: nl_creditors = -float(...)', async () => {
    const db = makeMockKnex({
      sprfls: { debtors_control: '1100' },
      pprfls: { creditors_control: '2100' },
      ntran: (filters) => {
        if (Object.keys(filters).length === 0) return { current_year: 2026 };
        if (filters.nt_acnt === '2100') return { total: -3000 };
        return { total: 0 };
      },
      stran: { total: 0 },
      sname: { total: 0 },
      ptran: { total: 3000 },
      pname: { total: 3000 },
      nbank: [],
      nvat: () => ({ total: 0 }),
      ztax: [],
    });

    const result = await reconcileSummary(db);
    const creditors = result.checks.find((c) => c.name === 'Creditors')!;

    expect(creditors.reconciled).toBe(true);
    const nominalDetail = creditors.details!.find((d) =>
      d.label.startsWith('Nominal'),
    )!;
    // -(-3000) = 3000
    expect(nominalDetail.value).toBe(3000);
  });

  it('converts nbank.nk_curbal from pence to pounds', async () => {
    const db = makeMockKnex({
      sprfls: { debtors_control: '1100' },
      pprfls: { creditors_control: '2100' },
      ntran: (filters) => {
        if (Object.keys(filters).length === 0) return { current_year: 2026 };
        if (filters.nt_acnt === 'BC010') return { total: 12345.67 };
        return { total: 0 };
      },
      stran: { total: 0 },
      sname: { total: 0 },
      ptran: { total: 0 },
      pname: { total: 0 },
      nbank: [{ nk_acnt: 'BC010', nk_curbal: 1234567 }], // 12,345.67 in pence
      nvat: () => ({ total: 0 }),
      ztax: [],
    });

    const result = await reconcileSummary(db);
    const cashbook = result.checks.find((c) => c.name === 'Cashbook')!;

    expect(cashbook.reconciled).toBe(true);
    const bankMaster = cashbook.details!.find(
      (d) => d.label === 'Bank Master (nbank)',
    )!;
    expect(bankMaster.value).toBe(12345.67);
  });

  it('returns reconciliation_date in YYYY-MM-DD HH:MM:SS format', async () => {
    const db = makeMockKnex({
      sprfls: { debtors_control: '1100' },
      pprfls: { creditors_control: '2100' },
      ntran: (filters) => {
        if (Object.keys(filters).length === 0) return { current_year: 2026 };
        return { total: 0 };
      },
      stran: { total: 0 },
      sname: { total: 0 },
      ptran: { total: 0 },
      pname: { total: 0 },
      nbank: [],
      nvat: () => ({ total: 0 }),
      ztax: [],
    });

    const result = await reconcileSummary(db);
    expect(result.reconciliation_date).toMatch(
      /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/,
    );
  });

  it('checks every variance to the penny — NO tolerance for finance', async () => {
    const db = makeMockKnex({
      sprfls: { debtors_control: '1100' },
      pprfls: { creditors_control: '2100' },
      ntran: (filters) => {
        if (Object.keys(filters).length === 0) return { current_year: 2026 };
        if (filters.nt_acnt === '1100') return { total: 5000.01 }; // 1p mismatch
        if (filters.nt_acnt === '2100') return { total: -3000 };
        return { total: 0 };
      },
      stran: { total: 5000 },
      sname: { total: 5000 },
      ptran: { total: 3000 },
      pname: { total: 3000 },
      nbank: [],
      nvat: () => ({ total: 0 }),
      ztax: [],
    });

    const result = await reconcileSummary(db);
    const debtors = result.checks.find((c) => c.name === 'Debtors')!;
    const slVsNl = debtors.variances!.find((v) => v.label === 'SL vs NL')!;
    // 1p variance is enough to fail — finance system, no tolerance.
    expect(slVsNl.value).toBe(0.01);
    expect(slVsNl.ok).toBe(false);
    expect(debtors.reconciled).toBe(false);
  });

  it('counts passed/failed checks correctly', async () => {
    const db = makeMockKnex({
      sprfls: { debtors_control: '1100' },
      pprfls: { creditors_control: '2100' },
      ntran: (filters) => {
        if (Object.keys(filters).length === 0) return { current_year: 2026 };
        if (filters.nt_acnt === '1100') return { total: 4999 }; // fail
        if (filters.nt_acnt === '2100') return { total: -3000 }; // pass
        return { total: 0 };
      },
      stran: { total: 5000 },
      sname: { total: 5000 },
      ptran: { total: 3000 },
      pname: { total: 3000 },
      nbank: [],
      nvat: () => ({ total: 0 }),
      ztax: [],
    });

    const result = await reconcileSummary(db);
    expect(result.total_checks).toBe(4);
    // Debtors fails, Creditors passes, Cashbook passes (empty), VAT passes (empty)
    expect(result.failed_checks).toBe(1);
    expect(result.passed_checks).toBe(3);
    expect(result.all_reconciled).toBe(false);
  });
});

describe('check structure matches the frontend contract', () => {
  it('returns the same icons as Python: users, building, book, receipt', async () => {
    const db = makeMockKnex({
      sprfls: { debtors_control: '1100' },
      pprfls: { creditors_control: '2100' },
      ntran: () => ({ total: 0 }),
      stran: { total: 0 },
      sname: { total: 0 },
      ptran: { total: 0 },
      pname: { total: 0 },
      nbank: [],
      nvat: () => ({ total: 0 }),
      ztax: [],
    });

    const result = await reconcileSummary(db);
    const expectedIcons: ReconcileCheck['icon'][] = ['users', 'building', 'book', 'receipt'];
    const actualIcons = result.checks.map((c) => c.icon);
    expect(actualIcons).toEqual(expectedIcons);
  });
});
