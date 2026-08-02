/**
 * Cashflow forecast — read-only forward view against Opera SE.
 *
 * Combines three signals to project the next 12 months:
 *
 *   1. Current bank position (nbank.nk_curbal) — opening balance.
 *      Foreign-currency accounts are excluded (consistent with the
 *      bank-transfer port, which also rejects them).
 *
 *   2. Known commitments (next 60 days):
 *      - Outstanding sales invoices: stran where st_trtype='I' AND
 *        st_trbal > 0. Expected receipt date = st_dueday if set on the
 *        invoice; otherwise st_trdate + DEFAULT_TERMS_DAYS (30).
 *      - Outstanding purchase invoices: ptran where pt_trtype='I' AND
 *        pt_trbal > 0. Expected payment date = pt_dueday if set;
 *        otherwise pt_trdate + DEFAULT_TERMS_DAYS (30).
 *      Customer/supplier terms live in separate `sterms`/`pterms`
 *      tables in Opera; we trust st_dueday/pt_dueday (which Opera
 *      populates from terms at invoice time) rather than re-joining.
 *
 *   3. Recurring cashbook entries (arhead joined to arline):
 *      - ae_nxtpost gives the next scheduled post date
 *      - ae_topost / ae_posted tell us how many remain
 *      - ae_freq drives the cadence (D / W / M / Q / A)
 *      - at_value (in pence) on arline → signed cash movement
 *      We project occurrences within the 12-month horizon.
 *
 *   4. Historical averages (used for months without commitments):
 *      - Receipts: stran where st_trtype='R' over the last 12 months,
 *        averaged by calendar month.
 *      - Payments: ptran where pt_trtype='P' over the last 12 months,
 *        averaged by calendar month.
 *
 * The forecast walks month-by-month adding commitments first
 * (high-confidence buckets), then topping up with averages where
 * gaps exist. Running balance = opening + cumulative net.
 *
 * Locking: NOLOCK on every read (CLAUDE.md mandate).
 *
 * NOTE: This service is intentionally simple. A future iteration could
 * add: VAT quarterly bumps from nvat, payroll from whist, scenario
 * adjustments, and per-customer payment-history modelling.
 */
import type { Knex } from 'knex';

// ---------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------

export interface CashflowForecastOptions {
  /** As-of date for the forecast (defaults to today). YYYY-MM-DD. */
  asOfDate?: string;
  /** Months forward to project (default 12, range 1..24). */
  monthsAhead?: number;
}

export interface BankPosition {
  code: string;
  description: string;
  balance: number;
}

export interface MonthlyForecast {
  /** YYYY-MM */
  month: string;
  /** Display label "Mar 2026" */
  label: string;
  expected_receipts: number;
  expected_payments: number;
  net_cashflow: number;
  running_balance: number;
  /** What drove the numbers — for transparency */
  sources: {
    commitments_in: number;
    commitments_out: number;
    recurring_in: number;
    recurring_out: number;
    historical_in: number;
    historical_out: number;
  };
}

export interface CashflowForecastResponse {
  success: boolean;
  as_of_date: string;
  current_position: {
    bank_total: number;
    bank_accounts: BankPosition[];
    debtors_outstanding: number;
    creditors_outstanding: number;
    net_working_capital: number;
  };
  monthly_forecast: MonthlyForecast[];
  totals: {
    total_receipts: number;
    total_payments: number;
    net_position: number;
    opening_balance: number;
    closing_balance: number;
    lowest_balance: number;
    lowest_balance_month: string | null;
  };
  assumptions: string[];
  error?: string;
}

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

function pad2(n: number): string {
  return n < 10 ? `0${n}` : String(n);
}

function ymKey(year: number, month: number): string {
  return `${year}-${pad2(month)}`;
}

function monthLabel(year: number, month: number): string {
  const names = [
    'Jan',
    'Feb',
    'Mar',
    'Apr',
    'May',
    'Jun',
    'Jul',
    'Aug',
    'Sep',
    'Oct',
    'Nov',
    'Dec',
  ];
  return `${names[month - 1]} ${year}`;
}

function parseDateOrToday(input: string | undefined): Date {
  if (!input) return new Date();
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(input.trim());
  if (!m) return new Date();
  return new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
}

function addMonths(d: Date, n: number): Date {
  const r = new Date(d.getTime());
  const target = r.getUTCMonth() + n;
  r.setUTCDate(1);
  r.setUTCMonth(target);
  return r;
}

function addDays(d: Date, n: number): Date {
  const r = new Date(d.getTime());
  r.setUTCDate(r.getUTCDate() + n);
  return r;
}

function r2(v: number): number {
  return Math.round(v * 100) / 100;
}

function parseDb(v: unknown): Date | null {
  if (v === null || v === undefined || v === '') return null;
  if (v instanceof Date) return v;
  const s = String(v).slice(0, 10);
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(s);
  if (!m) return null;
  return new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
}

function diffMonths(from: Date, to: Date): number {
  return (
    (to.getUTCFullYear() - from.getUTCFullYear()) * 12 +
    (to.getUTCMonth() - from.getUTCMonth())
  );
}

// ---------------------------------------------------------------------
// Frequency expansion for arhead
// ---------------------------------------------------------------------

/**
 * Generate the set of (year, month) buckets a recurring entry will
 * post in, between `firstPostDate` and `horizon`. Capped by
 * `remaining` postings.
 *
 * Opera's ae_freq codes (per opera_knowledge_base.md):
 *   D = Daily, W = Weekly, M = Monthly, Q = Quarterly, A = Annual
 */
function* recurringOccurrences(
  firstPostDate: Date,
  freq: string,
  every: number,
  remaining: number,
  horizon: Date,
): Generator<Date> {
  const step = Math.max(1, Math.round(every || 1));
  let date = new Date(firstPostDate.getTime());
  let count = 0;
  const maxIterations = 365 * 2; // safety net
  for (let i = 0; i < maxIterations; i++) {
    if (date > horizon) return;
    if (remaining > 0 && count >= remaining) return;
    yield new Date(date.getTime());
    count++;
    switch ((freq || 'M').toUpperCase()) {
      case 'D':
        date = addDays(date, step);
        break;
      case 'W':
        date = addDays(date, 7 * step);
        break;
      case 'M':
        date = addMonths(date, step);
        break;
      case 'Q':
        date = addMonths(date, 3 * step);
        break;
      case 'A':
      case 'Y':
        date = addMonths(date, 12 * step);
        break;
      default:
        date = addMonths(date, step);
    }
  }
}

// ---------------------------------------------------------------------
// Main service
// ---------------------------------------------------------------------

export async function getCashflowForecast(
  operaDb: Knex,
  opts: CashflowForecastOptions = {},
): Promise<CashflowForecastResponse> {
  const asOf = parseDateOrToday(opts.asOfDate);
  const asOfStr = asOf.toISOString().slice(0, 10);
  const monthsAhead = Math.max(1, Math.min(24, Number(opts.monthsAhead ?? 12)));
  const horizon = addMonths(asOf, monthsAhead);
  const assumptions: string[] = [];

  // -------------------------------------------------------------------
  // 1. Current bank position (nbank, exclude foreign currency accounts)
  // -------------------------------------------------------------------
  let bankAccounts: BankPosition[] = [];
  let bankTotal = 0;
  try {
    const rows = (await operaDb.raw(
      `SELECT
          RTRIM(nk_acnt) AS code,
          RTRIM(ISNULL(nk_desc, '')) AS description,
          ISNULL(nk_curbal, 0) AS balance_pence,
          RTRIM(ISNULL(nk_fcurr, '')) AS fcurr
         FROM nbank WITH (NOLOCK)
         ORDER BY nk_acnt`,
    )) as Array<{
      code: string;
      description: string;
      balance_pence: number;
      fcurr: string;
    }>;
    for (const r of rows ?? []) {
      if ((r.fcurr ?? '').trim()) continue; // skip foreign
      const balance = Number(r.balance_pence ?? 0) / 100;
      bankAccounts.push({
        code: r.code,
        description: r.description,
        balance: r2(balance),
      });
      bankTotal += balance;
    }
  } catch (e: any) {
    return {
      success: false,
      as_of_date: asOfStr,
      current_position: {
        bank_total: 0,
        bank_accounts: [],
        debtors_outstanding: 0,
        creditors_outstanding: 0,
        net_working_capital: 0,
      },
      monthly_forecast: [],
      totals: {
        total_receipts: 0,
        total_payments: 0,
        net_position: 0,
        opening_balance: 0,
        closing_balance: 0,
        lowest_balance: 0,
        lowest_balance_month: null,
      },
      assumptions: [],
      error: `Bank position read failed: ${e?.message ?? String(e)}`,
    };
  }

  // -------------------------------------------------------------------
  // 2. Outstanding debtors (sales invoices unpaid) — bucket by expected
  //    receipt date.
  // -------------------------------------------------------------------
  const commitmentsIn = new Map<string, number>(); // YYYY-MM → £
  let debtorsOutstanding = 0;
  const DEFAULT_TERMS_DAYS = 30;
  try {
    const rows = (await operaDb.raw(
      `SELECT
          RTRIM(st.st_account) AS account,
          st.st_trdate,
          st.st_dueday,
          ISNULL(st.st_trbal, 0) AS trbal
         FROM stran st WITH (NOLOCK)
         LEFT JOIN sname s WITH (NOLOCK)
           ON RTRIM(st.st_account) = RTRIM(s.sn_account)
         WHERE st.st_trtype = 'I'
           AND st.st_trbal > 0
           AND ISNULL(s.sn_dormant, 0) = 0`,
    )) as Array<{
      account: string;
      st_trdate: string | Date;
      st_dueday: string | Date | null;
      trbal: number;
    }>;
    for (const r of rows ?? []) {
      const trbal = Number(r.trbal ?? 0);
      if (trbal <= 0) continue;
      debtorsOutstanding += trbal;
      const trDate = parseDb(r.st_trdate);
      const dueDate = parseDb(r.st_dueday);
      const expectedDate =
        dueDate ?? (trDate ? addDays(trDate, DEFAULT_TERMS_DAYS) : null);
      if (!expectedDate) continue;
      if (expectedDate < asOf) {
        // Overdue — assume payment within current month
        const key = ymKey(asOf.getUTCFullYear(), asOf.getUTCMonth() + 1);
        commitmentsIn.set(key, (commitmentsIn.get(key) ?? 0) + trbal);
        continue;
      }
      if (expectedDate > horizon) continue;
      const key = ymKey(
        expectedDate.getUTCFullYear(),
        expectedDate.getUTCMonth() + 1,
      );
      commitmentsIn.set(key, (commitmentsIn.get(key) ?? 0) + trbal);
    }
  } catch (e: any) {
    assumptions.push(
      `Debtors lookup failed (${e?.message ?? String(e)}) — falling back to historical averages only`,
    );
  }

  // -------------------------------------------------------------------
  // 3. Outstanding creditors (purchase invoices unpaid)
  // -------------------------------------------------------------------
  const commitmentsOut = new Map<string, number>();
  let creditorsOutstanding = 0;
  try {
    const rows = (await operaDb.raw(
      `SELECT
          RTRIM(pt.pt_account) AS account,
          pt.pt_trdate,
          pt.pt_dueday,
          ISNULL(pt.pt_trbal, 0) AS trbal
         FROM ptran pt WITH (NOLOCK)
         LEFT JOIN pname p WITH (NOLOCK)
           ON RTRIM(pt.pt_account) = RTRIM(p.pn_account)
         WHERE pt.pt_trtype = 'I'
           AND pt.pt_trbal > 0
           AND ISNULL(p.pn_dormant, 0) = 0`,
    )) as Array<{
      account: string;
      pt_trdate: string | Date;
      pt_dueday: string | Date | null;
      trbal: number;
    }>;
    for (const r of rows ?? []) {
      const trbal = Number(r.trbal ?? 0);
      if (trbal <= 0) continue;
      creditorsOutstanding += trbal;
      const trDate = parseDb(r.pt_trdate);
      const dueDate = parseDb(r.pt_dueday);
      const expectedDate =
        dueDate ?? (trDate ? addDays(trDate, DEFAULT_TERMS_DAYS) : null);
      if (!expectedDate) continue;
      if (expectedDate < asOf) {
        const key = ymKey(asOf.getUTCFullYear(), asOf.getUTCMonth() + 1);
        commitmentsOut.set(key, (commitmentsOut.get(key) ?? 0) + trbal);
        continue;
      }
      if (expectedDate > horizon) continue;
      const key = ymKey(
        expectedDate.getUTCFullYear(),
        expectedDate.getUTCMonth() + 1,
      );
      commitmentsOut.set(key, (commitmentsOut.get(key) ?? 0) + trbal);
    }
  } catch (e: any) {
    assumptions.push(
      `Creditors lookup failed (${e?.message ?? String(e)}) — falling back to historical averages only`,
    );
  }

  // -------------------------------------------------------------------
  // 4. Recurring entries (arhead + arline) — scheduled cashbook events
  //    in the horizon window.
  // -------------------------------------------------------------------
  const recurringIn = new Map<string, number>();
  const recurringOut = new Map<string, number>();
  try {
    const rows = (await operaDb.raw(
      `SELECT
          RTRIM(ae.ae_entry) AS entry,
          ae.ae_nxtpost,
          RTRIM(ISNULL(ae.ae_freq, 'M')) AS freq,
          ISNULL(ae.ae_every, 1) AS every_n,
          ISNULL(ae.ae_posted, 0) AS posted,
          ISNULL(ae.ae_topost, 0) AS topost,
          ISNULL(at.at_value, 0) AS value_pence
         FROM arhead ae WITH (NOLOCK)
         LEFT JOIN arline at WITH (NOLOCK)
           ON RTRIM(ae.ae_entry) = RTRIM(at.at_entry)
         WHERE ae.ae_nxtpost IS NOT NULL`,
    )) as Array<{
      entry: string;
      ae_nxtpost: string | Date;
      freq: string;
      every_n: number;
      posted: number;
      topost: number;
      value_pence: number;
    }>;
    for (const r of rows ?? []) {
      const first = parseDb(r.ae_nxtpost);
      if (!first) continue;
      const remaining = Math.max(
        0,
        Number(r.topost ?? 0) - Number(r.posted ?? 0),
      );
      // value_pence is the line's signed amount in pence (Opera's
      // convention: payments negative, receipts positive)
      const amount = Number(r.value_pence ?? 0) / 100;
      if (amount === 0) continue;
      for (const occ of recurringOccurrences(
        first,
        r.freq,
        Number(r.every_n ?? 1),
        remaining,
        horizon,
      )) {
        if (occ < asOf) continue;
        const key = ymKey(occ.getUTCFullYear(), occ.getUTCMonth() + 1);
        if (amount >= 0) {
          recurringIn.set(key, (recurringIn.get(key) ?? 0) + amount);
        } else {
          recurringOut.set(key, (recurringOut.get(key) ?? 0) + Math.abs(amount));
        }
      }
    }
  } catch (e: any) {
    assumptions.push(
      `Recurring entries lookup failed (${e?.message ?? String(e)}) — recurring postings excluded from forecast`,
    );
  }

  // -------------------------------------------------------------------
  // 5. Historical averages — receipts and payments by calendar month
  //    over the last 12 months.
  // -------------------------------------------------------------------
  const historicalReceipts = new Map<number, number>(); // month (1..12) → avg £
  const historicalPayments = new Map<number, number>();
  try {
    const rows = (await operaDb.raw(
      `SELECT MONTH(st_trdate) AS m,
              SUM(ABS(ISNULL(st_trvalue, 0))) AS total
         FROM stran WITH (NOLOCK)
         WHERE st_trtype = 'R'
           AND st_trdate >= DATEADD(YEAR, -1, GETDATE())
         GROUP BY MONTH(st_trdate)`,
    )) as Array<{ m: number; total: number }>;
    for (const r of rows ?? []) {
      const m = Number(r.m ?? 0);
      if (m < 1 || m > 12) continue;
      historicalReceipts.set(m, Number(r.total ?? 0));
    }
  } catch (e: any) {
    assumptions.push(
      `Historical receipts lookup failed (${e?.message ?? String(e)})`,
    );
  }
  try {
    const rows = (await operaDb.raw(
      `SELECT MONTH(pt_trdate) AS m,
              SUM(ABS(ISNULL(pt_trvalue, 0))) AS total
         FROM ptran WITH (NOLOCK)
         WHERE pt_trtype = 'P'
           AND pt_trdate >= DATEADD(YEAR, -1, GETDATE())
         GROUP BY MONTH(pt_trdate)`,
    )) as Array<{ m: number; total: number }>;
    for (const r of rows ?? []) {
      const m = Number(r.m ?? 0);
      if (m < 1 || m > 12) continue;
      historicalPayments.set(m, Number(r.total ?? 0));
    }
  } catch (e: any) {
    assumptions.push(
      `Historical payments lookup failed (${e?.message ?? String(e)})`,
    );
  }

  // -------------------------------------------------------------------
  // 6. Walk forward month-by-month
  // -------------------------------------------------------------------
  const monthly: MonthlyForecast[] = [];
  let runningBalance = bankTotal;
  let totalReceipts = 0;
  let totalPayments = 0;
  let lowestBalance = runningBalance;
  let lowestBalanceMonth: string | null = null;

  // First bucket = current month (so overdue items show at as-of-month)
  const startYear = asOf.getUTCFullYear();
  const startMonthIdx = asOf.getUTCMonth(); // 0-based
  for (let i = 0; i < monthsAhead; i++) {
    const cursor = addMonths(
      new Date(Date.UTC(startYear, startMonthIdx, 1)),
      i,
    );
    const y = cursor.getUTCFullYear();
    const m = cursor.getUTCMonth() + 1;
    const key = ymKey(y, m);

    const cIn = commitmentsIn.get(key) ?? 0;
    const cOut = commitmentsOut.get(key) ?? 0;
    const rIn = recurringIn.get(key) ?? 0;
    const rOut = recurringOut.get(key) ?? 0;

    // Use historical averages ONLY when commitments + recurring are
    // light for that bucket — typical for months beyond the
    // commitments horizon (~60 days). This avoids double-counting
    // the same invoice in both buckets.
    const monthsAhead_i = diffMonths(asOf, cursor);
    const useHistory = monthsAhead_i >= 2;
    const hIn = useHistory ? (historicalReceipts.get(m) ?? 0) : 0;
    const hOut = useHistory ? (historicalPayments.get(m) ?? 0) : 0;

    const receipts = cIn + rIn + hIn;
    const payments = cOut + rOut + hOut;
    const net = receipts - payments;
    runningBalance += net;
    totalReceipts += receipts;
    totalPayments += payments;
    if (runningBalance < lowestBalance) {
      lowestBalance = runningBalance;
      lowestBalanceMonth = monthLabel(y, m);
    }

    monthly.push({
      month: key,
      label: monthLabel(y, m),
      expected_receipts: r2(receipts),
      expected_payments: r2(payments),
      net_cashflow: r2(net),
      running_balance: r2(runningBalance),
      sources: {
        commitments_in: r2(cIn),
        commitments_out: r2(cOut),
        recurring_in: r2(rIn),
        recurring_out: r2(rOut),
        historical_in: r2(hIn),
        historical_out: r2(hOut),
      },
    });
  }

  // -------------------------------------------------------------------
  // 7. Standing assumptions banner
  // -------------------------------------------------------------------
  assumptions.unshift(
    'Months 1–2 use known commitments (outstanding invoices) + scheduled recurring entries.',
    'Months 3+ blend known commitments with 12-month historical averages by calendar month.',
    'Expected payment dates use st_dueday/pt_dueday when set on the invoice; otherwise trdate + 30 days.',
    'Foreign-currency bank accounts are excluded from the opening balance.',
  );

  return {
    success: true,
    as_of_date: asOfStr,
    current_position: {
      bank_total: r2(bankTotal),
      bank_accounts: bankAccounts,
      debtors_outstanding: r2(debtorsOutstanding),
      creditors_outstanding: r2(creditorsOutstanding),
      net_working_capital: r2(
        bankTotal + debtorsOutstanding - creditorsOutstanding,
      ),
    },
    monthly_forecast: monthly,
    totals: {
      total_receipts: r2(totalReceipts),
      total_payments: r2(totalPayments),
      net_position: r2(totalReceipts - totalPayments),
      opening_balance: r2(bankTotal),
      closing_balance: r2(runningBalance),
      lowest_balance: r2(lowestBalance),
      lowest_balance_month: lowestBalanceMonth,
    },
    assumptions,
  };
}
