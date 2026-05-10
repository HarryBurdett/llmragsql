import { describe, it, expect } from 'vitest';
import {
  getPeriodForDate,
  getCurrentPeriodInfo,
  getPeriodStatus,
  isOpenPeriodAccountingEnabled,
  validatePostingPeriod,
  getPeriodPostingDecision,
  getLedgerTypeForTransaction,
} from '../src/opera/period-validation.js';

interface RawHandler {
  match: (sql: string) => boolean;
  rows: unknown[] | (() => unknown[]) | (() => Promise<unknown[]>);
  throwOn?: boolean;
}

function makeMockOperaDb(handlers: RawHandler[]) {
  const calls: Array<{ sql: string; params: unknown[] }> = [];
  const db: any = {
    raw: (sql: string, params?: unknown[]) => {
      calls.push({ sql, params: params ?? [] });
      const handler = handlers.find((h) => h.match(sql));
      if (!handler) {
        return Promise.reject(new Error(`No mock handler for SQL: ${sql.slice(0, 80)}`));
      }
      if (handler.throwOn) {
        return Promise.reject(new Error('mock throw'));
      }
      const rows =
        typeof handler.rows === 'function' ? handler.rows() : handler.rows;
      return Promise.resolve(rows);
    },
  };
  return { db, calls };
}

describe('getPeriodForDate', () => {
  it('parses YYYY-MM-DD and returns period/year from nclndd', async () => {
    const { db } = makeMockOperaDb([
      {
        match: (sql) => sql.includes('FROM nclndd'),
        rows: [{ ncd_period: 4, ncd_year: 2026 }],
      },
    ]);
    const result = await getPeriodForDate(db, '2026-04-15');
    expect(result).toEqual({ period: 4, year: 2026 });
  });

  it('falls back to calendar month when nclndd has no row', async () => {
    const { db } = makeMockOperaDb([
      { match: (sql) => sql.includes('FROM nclndd'), rows: [] },
    ]);
    const result = await getPeriodForDate(db, '2026-07-15');
    expect(result).toEqual({ period: 7, year: 2026 });
  });

  it('falls back to calendar month when nclndd query throws', async () => {
    const { db } = makeMockOperaDb([
      { match: (sql) => sql.includes('FROM nclndd'), rows: [], throwOn: true },
    ]);
    const result = await getPeriodForDate(db, '2026-12-01');
    expect(result).toEqual({ period: 12, year: 2026 });
  });

  it('rejects malformed dates', async () => {
    const { db } = makeMockOperaDb([]);
    await expect(getPeriodForDate(db, '15-04-2026')).rejects.toThrow(
      /Invalid date format/,
    );
  });
});

describe('getCurrentPeriodInfo', () => {
  it('returns nparm row', async () => {
    const { db } = makeMockOperaDb([
      {
        match: (sql) => sql.includes('FROM nparm'),
        rows: [{ np_year: 2026, np_perno: 4, np_periods: 12 }],
      },
    ]);
    const result = await getCurrentPeriodInfo(db);
    expect(result).toEqual({ np_year: 2026, np_perno: 4, np_periods: 12 });
  });

  it('returns nulls when nparm empty', async () => {
    const { db } = makeMockOperaDb([
      { match: (sql) => sql.includes('FROM nparm'), rows: [] },
    ]);
    const result = await getCurrentPeriodInfo(db);
    expect(result).toEqual({ np_year: null, np_perno: null, np_periods: 12 });
  });
});

describe('getPeriodStatus', () => {
  it('returns nlstat for NL', async () => {
    const { db, calls } = makeMockOperaDb([
      {
        match: (sql) => sql.includes('ncd_nlstat'),
        rows: [{ period_status: 0 }],
      },
    ]);
    const result = await getPeriodStatus(db, 2026, 4, 'NL');
    expect(result).toBe(0);
    expect(calls[0]?.sql).toContain('ncd_nlstat');
  });

  it('returns slstat for SL', async () => {
    const { db, calls } = makeMockOperaDb([
      {
        match: (sql) => sql.includes('ncd_slstat'),
        rows: [{ period_status: 1 }],
      },
    ]);
    const result = await getPeriodStatus(db, 2026, 4, 'SL');
    expect(result).toBe(1);
    expect(calls[0]?.sql).toContain('ncd_slstat');
  });

  it('returns null when no row found', async () => {
    const { db } = makeMockOperaDb([
      { match: (sql) => sql.includes('ncd_nlstat'), rows: [] },
    ]);
    const result = await getPeriodStatus(db, 2026, 4, 'NL');
    expect(result).toBeNull();
  });
});

describe('isOpenPeriodAccountingEnabled', () => {
  it('returns true when seqco.co_opanl is truthy', async () => {
    const { db } = makeMockOperaDb([
      {
        match: (sql) => sql.includes('seqco'),
        rows: [{ co_opanl: 1 }],
      },
    ]);
    expect(await isOpenPeriodAccountingEnabled(db)).toBe(true);
  });

  it('falls back to nparm.np_opawarn when seqco missing', async () => {
    const { db } = makeMockOperaDb([
      { match: (sql) => sql.includes('seqco'), rows: [], throwOn: true },
      {
        match: (sql) => sql.includes('np_opawarn'),
        rows: [{ np_opawarn: 1 }],
      },
    ]);
    expect(await isOpenPeriodAccountingEnabled(db)).toBe(true);
  });

  it('defaults to disabled when both queries fail', async () => {
    const { db } = makeMockOperaDb([
      { match: (sql) => sql.includes('seqco'), rows: [], throwOn: true },
      { match: (sql) => sql.includes('np_opawarn'), rows: [], throwOn: true },
    ]);
    expect(await isOpenPeriodAccountingEnabled(db)).toBe(false);
  });
});

describe('validatePostingPeriod', () => {
  it('OPA on, NL open: returns valid', async () => {
    const { db } = makeMockOperaDb([
      {
        match: (sql) => sql.includes('FROM nclndd') && !sql.includes('ncd_nlstat'),
        rows: [{ ncd_period: 4, ncd_year: 2026 }],
      },
      { match: (sql) => sql.includes('seqco'), rows: [{ co_opanl: 1 }] },
      {
        match: (sql) => sql.includes('ncd_nlstat'),
        rows: [{ period_status: 0 }],
      },
    ]);
    const result = await validatePostingPeriod(db, '2026-04-15', 'NL');
    expect(result.is_valid).toBe(true);
    expect(result.period).toBe(4);
    expect(result.year).toBe(2026);
    expect(result.open_period_accounting).toBe(true);
  });

  it('OPA on, NL blocked: returns invalid with NL message', async () => {
    const { db } = makeMockOperaDb([
      {
        match: (sql) => sql.includes('FROM nclndd') && !sql.includes('ncd_nlstat'),
        rows: [{ ncd_period: 4, ncd_year: 2026 }],
      },
      { match: (sql) => sql.includes('seqco'), rows: [{ co_opanl: 1 }] },
      {
        match: (sql) => sql.includes('ncd_nlstat'),
        rows: [{ period_status: 1 }],
      },
    ]);
    const result = await validatePostingPeriod(db, '2026-04-15', 'SL');
    expect(result.is_valid).toBe(false);
    expect(result.error_message).toMatch(/Nominal Ledger is blocked/);
  });

  it('OPA on, NL open + SL closed: returns invalid with SL message', async () => {
    const { db } = makeMockOperaDb([
      {
        match: (sql) =>
          sql.includes('FROM nclndd') &&
          !sql.includes('ncd_nlstat') &&
          !sql.includes('ncd_slstat'),
        rows: [{ ncd_period: 4, ncd_year: 2026 }],
      },
      { match: (sql) => sql.includes('seqco'), rows: [{ co_opanl: 1 }] },
      {
        match: (sql) => sql.includes('ncd_nlstat'),
        rows: [{ period_status: 0 }],
      },
      {
        match: (sql) => sql.includes('ncd_slstat'),
        rows: [{ period_status: 2 }],
      },
    ]);
    const result = await validatePostingPeriod(db, '2026-04-15', 'SL');
    expect(result.is_valid).toBe(false);
    expect(result.error_message).toMatch(/Sales Ledger is closed/);
  });

  it('OPA off, posting to current period: valid', async () => {
    const { db } = makeMockOperaDb([
      {
        match: (sql) => sql.includes('FROM nclndd') && !sql.includes('ncd_nlstat'),
        rows: [{ ncd_period: 4, ncd_year: 2026 }],
      },
      { match: (sql) => sql.includes('seqco'), rows: [{ co_opanl: 0 }] },
      {
        match: (sql) => sql.includes('FROM nparm'),
        rows: [{ np_year: 2026, np_perno: 4, np_periods: 12 }],
      },
    ]);
    const result = await validatePostingPeriod(db, '2026-04-15', 'NL');
    expect(result.is_valid).toBe(true);
    expect(result.open_period_accounting).toBe(false);
  });

  it('OPA off, posting to non-current period: invalid', async () => {
    const { db } = makeMockOperaDb([
      {
        match: (sql) => sql.includes('FROM nclndd') && !sql.includes('ncd_nlstat'),
        rows: [{ ncd_period: 3, ncd_year: 2026 }],
      },
      { match: (sql) => sql.includes('seqco'), rows: [{ co_opanl: 0 }] },
      {
        match: (sql) => sql.includes('FROM nparm'),
        rows: [{ np_year: 2026, np_perno: 4, np_periods: 12 }],
      },
    ]);
    const result = await validatePostingPeriod(db, '2026-03-15', 'NL');
    expect(result.is_valid).toBe(false);
    expect(result.error_message).toMatch(/Period 3\/2026 is blocked/);
    expect(result.error_message).toMatch(/Current period is 4\/2026/);
  });
});

/**
 * getPeriodPostingDecision — port of `get_period_posting_decision`
 * (sql_rag/opera_config.py:848-1037). The Python tests in
 * tests/test_opera_config.py cover these branches; we mirror them.
 */
describe('getPeriodPostingDecision', () => {
  function setupHandlers(opts: {
    txnPeriod?: number;
    txnYear?: number;
    currentPeriod?: number | null;
    currentYear?: number | null;
    opa?: 0 | 1;
    rt?: 0 | 1;
    nlStatus?: 0 | 1 | 2 | null;
    slStatus?: 0 | 1 | 2 | null;
  }): RawHandler[] {
    return [
      {
        match: (sql) => sql.includes('FROM nclndd') && !sql.includes('ncd_nlstat') && !sql.includes('ncd_slstat'),
        rows: [
          {
            ncd_period: opts.txnPeriod ?? 4,
            ncd_year: opts.txnYear ?? 2026,
          },
        ],
      },
      {
        match: (sql) => sql.includes('FROM nparm'),
        rows: [
          {
            np_year: opts.currentYear ?? 2026,
            np_perno: opts.currentPeriod ?? 4,
            np_periods: 12,
          },
        ],
      },
      {
        match: (sql) => sql.includes('seqco') && sql.includes('co_opanl'),
        rows: [{ co_opanl: opts.opa ?? 1 }],
      },
      {
        match: (sql) => sql.includes('seqco') && sql.includes('co_rtupdnl'),
        rows: [{ co_rtupdnl: opts.rt ?? 1 }],
      },
      // ncd status lookup for NL
      {
        match: (sql) => sql.includes('ncd_nlstat'),
        rows:
          opts.nlStatus === null
            ? []
            : [{ period_status: opts.nlStatus ?? 0 }],
      },
      // ncd status lookup for SL
      {
        match: (sql) => sql.includes('ncd_slstat'),
        rows:
          opts.slStatus === null
            ? []
            : [{ period_status: opts.slStatus ?? 0 }],
      },
    ];
  }

  it('OPA off + RT on + current period → posts to NL with done=Y', async () => {
    const { db } = makeMockOperaDb(
      setupHandlers({ opa: 0, txnPeriod: 4, currentPeriod: 4 }),
    );
    const decision = await getPeriodPostingDecision(db, '2026-04-15', 'SL');
    expect(decision.can_post).toBe(true);
    expect(decision.post_to_nominal).toBe(true);
    expect(decision.post_to_transfer_file).toBe(true);
    expect(decision.transfer_file_done_flag).toBe('Y');
  });

  it('OPA off + non-current period → REJECT', async () => {
    const { db } = makeMockOperaDb(
      setupHandlers({ opa: 0, txnPeriod: 3, currentPeriod: 4 }),
    );
    const decision = await getPeriodPostingDecision(db, '2026-03-15', 'SL');
    expect(decision.can_post).toBe(false);
    expect(decision.error_message).toMatch(/Open Period Accounting is disabled/);
  });

  it('OPA on + NL closed → REJECT (NL is master gatekeeper)', async () => {
    const { db } = makeMockOperaDb(
      setupHandlers({ opa: 1, nlStatus: 2 }),
    );
    const decision = await getPeriodPostingDecision(db, '2026-04-15', 'SL');
    expect(decision.can_post).toBe(false);
    expect(decision.error_message).toMatch(/closed for NL/);
  });

  it('OPA on + sub-ledger blocked → REJECT', async () => {
    const { db } = makeMockOperaDb(
      setupHandlers({ opa: 1, nlStatus: 0, slStatus: 1 }),
    );
    const decision = await getPeriodPostingDecision(db, '2026-04-15', 'SL');
    expect(decision.can_post).toBe(false);
    expect(decision.error_message).toMatch(/blocked for SL/);
  });

  it('OPA on + RT off → transfer-file only with done=" "', async () => {
    const { db } = makeMockOperaDb(
      setupHandlers({ opa: 1, rt: 0, nlStatus: 0, slStatus: 0 }),
    );
    const decision = await getPeriodPostingDecision(db, '2026-04-15', 'SL');
    expect(decision.can_post).toBe(true);
    expect(decision.post_to_nominal).toBe(false);
    expect(decision.post_to_transfer_file).toBe(true);
    expect(decision.transfer_file_done_flag).toBe(' ');
  });

  it('OPA on + RT on + backdated within same year → still posts to NL', async () => {
    const { db } = makeMockOperaDb(
      setupHandlers({
        opa: 1,
        rt: 1,
        nlStatus: 0,
        slStatus: 0,
        txnPeriod: 2,
        currentPeriod: 5,
        txnYear: 2026,
        currentYear: 2026,
      }),
    );
    const decision = await getPeriodPostingDecision(db, '2026-02-15', 'SL');
    expect(decision.can_post).toBe(true);
    expect(decision.post_to_nominal).toBe(true);
    expect(decision.transfer_file_done_flag).toBe('Y');
  });

  it('Prior financial year → REJECT', async () => {
    const { db } = makeMockOperaDb(
      setupHandlers({
        opa: 1,
        rt: 1,
        nlStatus: 0,
        slStatus: 0,
        txnYear: 2025,
        currentYear: 2026,
        txnPeriod: 4,
        currentPeriod: 4,
      }),
    );
    const decision = await getPeriodPostingDecision(db, '2025-04-15', 'SL');
    expect(decision.can_post).toBe(false);
    expect(decision.error_message).toMatch(/prior financial year/);
  });
});

describe('getLedgerTypeForTransaction', () => {
  it.each([
    ['sales_receipt', 'SL'],
    ['sales_refund', 'SL'],
    ['sales_invoice', 'SL'],
    ['purchase_payment', 'PL'],
    ['purchase_refund', 'PL'],
    ['purchase_invoice', 'PL'],
    ['nominal_payment', 'NL'],
    ['nominal_receipt', 'NL'],
    ['bank_transfer', 'NL'],
    ['unknown_thing', 'NL'],
  ])('%s → %s', (txn, expected) => {
    expect(getLedgerTypeForTransaction(txn)).toBe(expected);
  });
});
