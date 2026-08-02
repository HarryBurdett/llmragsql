import { describe, it, expect } from 'vitest';
import { getCashflowForecast } from '../src/services/forecast.js';

interface MockState {
  nbank: Array<{
    nk_acnt: string;
    nk_desc: string;
    nk_curbal: number; // pence
    nk_fcurr?: string;
  }>;
  stran: Array<{
    st_account: string;
    st_trtype: string;
    st_trdate: string;
    st_dueday?: string | null;
    st_trbal: number;
    st_trvalue?: number;
    sn_terms?: number;
    sn_dormant?: number;
  }>;
  ptran: Array<{
    pt_account: string;
    pt_trtype: string;
    pt_trdate: string;
    pt_dueday?: string | null;
    pt_trbal: number;
    pt_trvalue?: number;
    pn_terms?: number;
    pn_dormant?: number;
  }>;
  /** Recurring entries: arhead joined with arline */
  recurring: Array<{
    ae_entry: string;
    ae_nxtpost: string;
    ae_freq: 'D' | 'W' | 'M' | 'Q' | 'A';
    ae_every: number;
    ae_posted: number;
    ae_topost: number;
    at_value: number; // pence, signed
  }>;
  /** Historical receipts by calendar month (1..12) → £ total */
  histReceipts?: Record<number, number>;
  /** Historical payments by calendar month → £ total */
  histPayments?: Record<number, number>;
}

function makeOperaDb(state: MockState): any {
  return {
    raw: (sql: string) => {
      // nbank
      if (sql.includes('FROM nbank')) {
        return Promise.resolve(
          state.nbank.map((b) => ({
            code: b.nk_acnt.trim(),
            description: b.nk_desc,
            balance_pence: b.nk_curbal,
            fcurr: b.nk_fcurr ?? '',
          })),
        );
      }
      // stran debtors (outstanding sales invoices)
      if (sql.includes('FROM stran') && sql.includes("st.st_trtype = 'I'")) {
        return Promise.resolve(
          state.stran
            .filter((s) => s.st_trtype === 'I' && s.st_trbal > 0 && (s.sn_dormant ?? 0) === 0)
            .map((s) => ({
              account: s.st_account,
              st_trdate: s.st_trdate,
              st_dueday: s.st_dueday ?? null,
              trbal: s.st_trbal,
              terms: s.sn_terms ?? 0,
            })),
        );
      }
      // ptran creditors (outstanding purchase invoices)
      if (sql.includes('FROM ptran') && sql.includes("pt.pt_trtype = 'I'")) {
        return Promise.resolve(
          state.ptran
            .filter((p) => p.pt_trtype === 'I' && p.pt_trbal > 0 && (p.pn_dormant ?? 0) === 0)
            .map((p) => ({
              account: p.pt_account,
              pt_trdate: p.pt_trdate,
              pt_dueday: p.pt_dueday ?? null,
              trbal: p.pt_trbal,
              terms: p.pn_terms ?? 0,
            })),
        );
      }
      // Recurring entries
      if (sql.includes('FROM arhead')) {
        return Promise.resolve(
          state.recurring.map((r) => ({
            entry: r.ae_entry,
            ae_nxtpost: r.ae_nxtpost,
            freq: r.ae_freq,
            every_n: r.ae_every,
            posted: r.ae_posted,
            topost: r.ae_topost,
            value_pence: r.at_value,
          })),
        );
      }
      // Historical receipts
      if (sql.includes('FROM stran') && sql.includes("st_trtype = 'R'")) {
        const map = state.histReceipts ?? {};
        return Promise.resolve(
          Object.entries(map).map(([m, total]) => ({ m: Number(m), total })),
        );
      }
      // Historical payments
      if (sql.includes('FROM ptran') && sql.includes("pt_trtype = 'P'")) {
        const map = state.histPayments ?? {};
        return Promise.resolve(
          Object.entries(map).map(([m, total]) => ({ m: Number(m), total })),
        );
      }
      return Promise.resolve([]);
    },
  };
}

describe('getCashflowForecast', () => {
  it('returns zero opening balance when no banks', async () => {
    const r = await getCashflowForecast(
      makeOperaDb({
        nbank: [],
        stran: [],
        ptran: [],
        recurring: [],
      }),
      { asOfDate: '2026-05-13', monthsAhead: 3 },
    );
    expect(r.success).toBe(true);
    expect(r.current_position.bank_total).toBe(0);
    expect(r.monthly_forecast).toHaveLength(3);
    expect(r.totals.opening_balance).toBe(0);
  });

  it('excludes foreign currency bank accounts from opening balance', async () => {
    const r = await getCashflowForecast(
      makeOperaDb({
        nbank: [
          { nk_acnt: 'BC010', nk_desc: 'GBP Main', nk_curbal: 100000 }, // £1000
          { nk_acnt: 'BC020', nk_desc: 'USD', nk_curbal: 50000, nk_fcurr: 'USD' },
        ],
        stran: [],
        ptran: [],
        recurring: [],
      }),
      { asOfDate: '2026-05-13', monthsAhead: 1 },
    );
    expect(r.current_position.bank_total).toBe(1000);
    expect(r.current_position.bank_accounts).toHaveLength(1);
    expect(r.current_position.bank_accounts[0]?.code).toBe('BC010');
  });

  it('buckets outstanding sales invoices by expected payment date', async () => {
    const r = await getCashflowForecast(
      makeOperaDb({
        nbank: [{ nk_acnt: 'BC010', nk_desc: 'Main', nk_curbal: 0 }],
        stran: [
          {
            st_account: 'CUST01',
            st_trtype: 'I',
            st_trdate: '2026-05-01',
            st_trbal: 500,
            sn_terms: 30, // due ~2026-05-31
          },
          {
            st_account: 'CUST02',
            st_trtype: 'I',
            st_trdate: '2026-06-01',
            st_trbal: 1000,
            sn_terms: 30, // due ~2026-07-01
          },
        ],
        ptran: [],
        recurring: [],
      }),
      { asOfDate: '2026-05-13', monthsAhead: 4 },
    );
    expect(r.success).toBe(true);
    // CUST01 due in May, CUST02 due in July
    const may = r.monthly_forecast.find((m) => m.month === '2026-05');
    const jul = r.monthly_forecast.find((m) => m.month === '2026-07');
    expect(may?.sources.commitments_in).toBe(500);
    expect(jul?.sources.commitments_in).toBe(1000);
    expect(r.current_position.debtors_outstanding).toBe(1500);
  });

  it('treats overdue invoices as collectable this month', async () => {
    const r = await getCashflowForecast(
      makeOperaDb({
        nbank: [{ nk_acnt: 'BC010', nk_desc: 'Main', nk_curbal: 0 }],
        stran: [
          {
            st_account: 'OVERDUE',
            st_trtype: 'I',
            st_trdate: '2026-02-01', // 3+ months overdue at as-of 2026-05-13
            st_trbal: 750,
            sn_terms: 30,
          },
        ],
        ptran: [],
        recurring: [],
      }),
      { asOfDate: '2026-05-13', monthsAhead: 3 },
    );
    const may = r.monthly_forecast.find((m) => m.month === '2026-05');
    expect(may?.sources.commitments_in).toBe(750);
  });

  it('prefers st_dueday over computed due date when present', async () => {
    const r = await getCashflowForecast(
      makeOperaDb({
        nbank: [{ nk_acnt: 'BC010', nk_desc: 'Main', nk_curbal: 0 }],
        stran: [
          {
            st_account: 'CUST01',
            st_trtype: 'I',
            st_trdate: '2026-05-01',
            // sn_terms would compute due ~2026-05-31, but operator
            // has set st_dueday explicitly to 2026-08-15
            st_dueday: '2026-08-15',
            st_trbal: 400,
            sn_terms: 30,
          },
        ],
        ptran: [],
        recurring: [],
      }),
      { asOfDate: '2026-05-13', monthsAhead: 5 },
    );
    const aug = r.monthly_forecast.find((m) => m.month === '2026-08');
    expect(aug?.sources.commitments_in).toBe(400);
  });

  it('buckets purchase commitments separately', async () => {
    const r = await getCashflowForecast(
      makeOperaDb({
        nbank: [{ nk_acnt: 'BC010', nk_desc: 'Main', nk_curbal: 100000 }], // £1000 (pence)
        stran: [],
        ptran: [
          {
            pt_account: 'SUPP01',
            pt_trtype: 'I',
            pt_trdate: '2026-05-01',
            pt_trbal: 300,
            pn_terms: 30,
          },
        ],
        recurring: [],
      }),
      { asOfDate: '2026-05-13', monthsAhead: 2 },
    );
    const may = r.monthly_forecast.find((m) => m.month === '2026-05');
    expect(may?.sources.commitments_out).toBe(300);
    expect(r.current_position.creditors_outstanding).toBe(300);
    expect(r.current_position.net_working_capital).toBe(700); // 1000 + 0 - 300
  });

  it('projects monthly recurring entries across the horizon', async () => {
    const r = await getCashflowForecast(
      makeOperaDb({
        nbank: [{ nk_acnt: 'BC010', nk_desc: 'Main', nk_curbal: 100000 }], // £1000
        stran: [],
        ptran: [],
        recurring: [
          {
            ae_entry: 'PR001',
            ae_nxtpost: '2026-06-01',
            ae_freq: 'M',
            ae_every: 1,
            ae_posted: 0,
            ae_topost: 12,
            at_value: -50000, // -£500/month (payment)
          },
          {
            ae_entry: 'PR002',
            ae_nxtpost: '2026-06-15',
            ae_freq: 'M',
            ae_every: 1,
            ae_posted: 0,
            ae_topost: 12,
            at_value: 20000, // +£200/month (receipt)
          },
        ],
      }),
      { asOfDate: '2026-05-13', monthsAhead: 4 },
    );
    const jun = r.monthly_forecast.find((m) => m.month === '2026-06');
    const jul = r.monthly_forecast.find((m) => m.month === '2026-07');
    expect(jun?.sources.recurring_out).toBe(500);
    expect(jun?.sources.recurring_in).toBe(200);
    expect(jul?.sources.recurring_out).toBe(500);
    expect(jul?.sources.recurring_in).toBe(200);
  });

  it('respects remaining-post limit (topost - posted)', async () => {
    const r = await getCashflowForecast(
      makeOperaDb({
        nbank: [{ nk_acnt: 'BC010', nk_desc: 'Main', nk_curbal: 0 }],
        stran: [],
        ptran: [],
        recurring: [
          {
            ae_entry: 'PR_LTD',
            ae_nxtpost: '2026-06-01',
            ae_freq: 'M',
            ae_every: 1,
            ae_posted: 10,
            ae_topost: 12, // only 2 left
            at_value: -10000,
          },
        ],
      }),
      { asOfDate: '2026-05-13', monthsAhead: 6 },
    );
    // We should see exactly 2 occurrences (June + July, then it stops)
    const totalRecurringOut = r.monthly_forecast.reduce(
      (s, m) => s + m.sources.recurring_out,
      0,
    );
    expect(totalRecurringOut).toBe(200); // 2 × £100
  });

  it('uses historical averages for months 3+ but not 1-2', async () => {
    const r = await getCashflowForecast(
      makeOperaDb({
        nbank: [{ nk_acnt: 'BC010', nk_desc: 'Main', nk_curbal: 0 }],
        stran: [],
        ptran: [],
        recurring: [],
        // August historic receipts £5000, August payments £2000
        histReceipts: { 8: 5000 },
        histPayments: { 8: 2000 },
      }),
      { asOfDate: '2026-05-13', monthsAhead: 6 },
    );
    const may = r.monthly_forecast.find((m) => m.month === '2026-05');
    const aug = r.monthly_forecast.find((m) => m.month === '2026-08');
    // Month 1 (May) should NOT use historicals
    expect(may?.sources.historical_in).toBe(0);
    expect(may?.sources.historical_out).toBe(0);
    // Month 4 (Aug) IS month ≥ 3, so historicals apply
    expect(aug?.sources.historical_in).toBe(5000);
    expect(aug?.sources.historical_out).toBe(2000);
  });

  it('computes running balance and identifies the lowest month', async () => {
    const r = await getCashflowForecast(
      makeOperaDb({
        nbank: [{ nk_acnt: 'BC010', nk_desc: 'Main', nk_curbal: 100000 }], // £1000 open
        stran: [],
        ptran: [],
        recurring: [
          {
            ae_entry: 'PR001',
            ae_nxtpost: '2026-06-01',
            ae_freq: 'M',
            ae_every: 1,
            ae_posted: 0,
            ae_topost: 12,
            at_value: -30000, // -£300/month
          },
        ],
      }),
      { asOfDate: '2026-05-13', monthsAhead: 4 },
    );
    // Opening £1000, then 3 × -300 = balance £100 at month 4
    expect(r.totals.opening_balance).toBe(1000);
    const last = r.monthly_forecast[r.monthly_forecast.length - 1];
    expect(last?.running_balance).toBe(100);
    expect(r.totals.lowest_balance).toBe(100);
    expect(r.totals.lowest_balance_month).toBe(last?.label);
  });

  it('respects monthsAhead option (capped 1..24)', async () => {
    const r3 = await getCashflowForecast(
      makeOperaDb({ nbank: [], stran: [], ptran: [], recurring: [] }),
      { asOfDate: '2026-05-13', monthsAhead: 3 },
    );
    expect(r3.monthly_forecast).toHaveLength(3);
    const rOverflow = await getCashflowForecast(
      makeOperaDb({ nbank: [], stran: [], ptran: [], recurring: [] }),
      { asOfDate: '2026-05-13', monthsAhead: 99 },
    );
    expect(rOverflow.monthly_forecast).toHaveLength(24);
  });
});
