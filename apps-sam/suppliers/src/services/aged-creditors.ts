/**
 * Aged creditors — summary, trend, and per-supplier detail.
 *
 * Faithful port of `routes_aged.py` (apps/suppliers/api/routes_aged.py):
 *   - aged_creditors_summary  → GET /api/creditors/aged
 *   - aged_creditors_trend    → GET /api/creditors/aged/trend
 *   - aged_creditors_detail   → GET /api/creditors/aged/{account}
 *
 * Frontend consumers:
 *   - SupplierDashboard.tsx     calls /api/creditors/aged
 *   - SupplierAgedCreditors.tsx calls all three
 *
 * Aging method (matches legacy):
 *   - "days" mode (default): fixed-day buckets from pparm.pp_percday
 *     (or 30 if not set). Buckets: <p / <2p / <3p / 3p+ days.
 *   - "months" mode: calendar-month buckets (0 / 1 / 2 / 3+ months
 *     from transaction date).
 *
 * Detail endpoint uses a fixed 30/60/90/120+ scheme (legacy
 * `_classify_aging_bucket`).
 *
 * Knex query builder throughout — driver-agnostic so this works on
 * Opera SE (MSSQL) and Opera 3 (FoxPro via the Write Agent) without
 * change.
 */
import type { Knex } from 'knex';

type AgingBucket =
  | 'days_90'
  | 'days_60'
  | 'days_30'
  | 'current'
  | 'days_120_plus';

interface SummaryBuckets {
  days_90: number;
  days_60: number;
  days_30: number;
  current: number;
  total: number;
  unallocated: number;
}

interface TrendBuckets extends SummaryBuckets {
  days_120_plus: number;
}

function emptySummaryBuckets(): SummaryBuckets {
  return {
    days_90: 0,
    days_60: 0,
    days_30: 0,
    current: 0,
    total: 0,
    unallocated: 0,
  };
}

function emptyTrendBuckets(): TrendBuckets {
  return {
    days_90: 0,
    days_60: 0,
    days_30: 0,
    current: 0,
    days_120_plus: 0,
    total: 0,
    unallocated: 0,
  };
}

function parseDate(v: unknown): Date | null {
  if (!v) return null;
  if (v instanceof Date) return Number.isNaN(v.getTime()) ? null : v;
  if (typeof v !== 'string') return null;
  const s = v.trim().slice(0, 10);
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (m) {
    const d = new Date(`${s}T00:00:00Z`);
    return Number.isNaN(d.getTime()) ? null : d;
  }
  // dd/mm/yyyy fallback
  const m2 = s.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (m2) {
    const d = new Date(`${m2[3]}-${m2[2]}-${m2[1]}T00:00:00Z`);
    return Number.isNaN(d.getTime()) ? null : d;
  }
  return null;
}

function daysBetween(a: Date, b: Date): number {
  const ms = b.getTime() - a.getTime();
  return Math.floor(ms / 86_400_000);
}

function lastDayOfMonth(year: number, month: number): Date {
  // month is 1-based here
  const next = new Date(Date.UTC(month === 12 ? year + 1 : year, month === 12 ? 0 : month, 1));
  return new Date(next.getTime() - 86_400_000);
}

function classifyAging(daysOld: number): AgingBucket {
  if (daysOld <= 30) return 'current';
  if (daysOld <= 60) return 'days_30';
  if (daysOld <= 90) return 'days_60';
  if (daysOld <= 120) return 'days_90';
  return 'days_120_plus';
}

function round2(n: number): number {
  return Math.round(n * 100) / 100;
}

// ---------------------------------------------------------------------
// SUMMARY  GET /api/creditors/aged
// ---------------------------------------------------------------------

export interface AgedSupplier {
  account: string;
  name: string;
  days_90: number;
  days_60: number;
  days_30: number;
  current: number;
  balance: number;
  unallocated: number;
  currency: string;
  fc_rate: number;
  fc_balance: number;
}

export interface AgedSummaryResponse {
  success: boolean;
  summary: SummaryBuckets;
  suppliers: AgedSupplier[];
  period_mode: 'days' | 'months';
  period_label: string;
  column_labels: Record<string, string>;
  error?: string;
}

export async function getAgedCreditorsSummary(
  operaDb: Knex,
): Promise<AgedSummaryResponse> {
  try {
    const today = new Date();
    today.setUTCHours(0, 0, 0, 0);

    let periodLabel = '30 Days';
    let periodMode: 'days' | 'months' = 'days';
    let periodDays = 30;
    try {
      const pparm = (await operaDb('pparm')
        .select('pp_period', 'pp_percday')
        .first()) as { pp_period: string | null; pp_percday: number | string | null } | undefined;
      if (pparm) {
        const pp = (pparm.pp_period ?? '').toString().trim();
        const pd = Number(pparm.pp_percday ?? 0) || 0;
        if (pp) {
          periodLabel = pp;
          if (pp.toLowerCase().includes('month')) periodMode = 'months';
          else if (pd > 0) periodDays = Math.floor(pd);
        }
      }
    } catch {
      // pparm not available — use defaults
    }

    const rows = (await operaDb({ p: 'ptran' })
      .innerJoin({ n: 'pname' }, 'n.pn_account', 'p.pt_account')
      .select(
        operaDb.raw('RTRIM(p.pt_account) AS account'),
        operaDb.raw('RTRIM(n.pn_name) AS name'),
        'n.pn_currbal',
        'p.pt_trbal',
        'p.pt_trdate',
        'p.pt_dueday',
        operaDb.raw('RTRIM(p.pt_trtype) AS pt_trtype'),
        operaDb.raw('RTRIM(p.pt_paid) AS pt_paid',),
        operaDb.raw("RTRIM(ISNULL(p.pt_fcurr, '')) AS pt_fcurr"),
        operaDb.raw('ISNULL(p.pt_fcrate, 0) AS pt_fcrate'),
        operaDb.raw('ISNULL(p.pt_fcbal, 0) AS pt_fcbal'),
        operaDb.raw('ISNULL(p.pt_fcdec, 0) AS pt_fcdec'),
      )
      .whereRaw('p.pt_trbal <> 0')
      .andWhereRaw('n.pn_currbal <> 0')
      .orderBy('p.pt_account')) as unknown as Array<{
      account: string;
      name: string;
      pn_currbal: number | string | null;
      pt_trbal: number | string | null;
      pt_trdate: Date | string | null;
      pt_dueday: number | string | null;
      pt_trtype: string;
      pt_paid: string;
      pt_fcurr: string;
      pt_fcrate: number | string | null;
      pt_fcbal: number | string | null;
      pt_fcdec: number | string | null;
    }>;

    if (!rows.length) {
      return {
        success: true,
        summary: emptySummaryBuckets(),
        suppliers: [],
        period_mode: periodMode,
        period_label: periodLabel,
        column_labels: {
          days_90: '90+ Days',
          days_60: '60 Days',
          days_30: '30 Days',
          current: 'Current',
        },
      };
    }

    const supplierData = new Map<string, AgedSupplier>();
    const summary = emptySummaryBuckets();

    for (const r of rows) {
      const account = (r.account ?? '').trim();
      const name = (r.name ?? '').trim();
      const balance = Number(r.pt_trbal ?? 0);
      const currbal = Number(r.pn_currbal ?? 0);
      const ageDate = parseDate(r.pt_trdate) ?? today;

      let bucket: AgingBucket;
      if (periodMode === 'months') {
        const monthsOld =
          (today.getUTCFullYear() - ageDate.getUTCFullYear()) * 12 +
          (today.getUTCMonth() - ageDate.getUTCMonth());
        if (monthsOld <= 0) bucket = 'current';
        else if (monthsOld === 1) bucket = 'days_30';
        else if (monthsOld === 2) bucket = 'days_60';
        else bucket = 'days_90';
      } else {
        const daysOld = Math.max(0, daysBetween(ageDate, today));
        if (daysOld < periodDays) bucket = 'current';
        else if (daysOld < periodDays * 2) bucket = 'days_30';
        else if (daysOld < periodDays * 3) bucket = 'days_60';
        else bucket = 'days_90';
      }

      const fcCurr = (r.pt_fcurr ?? '').trim();
      const fcRate = Number(r.pt_fcrate ?? 0);
      let fcBal = Number(r.pt_fcbal ?? 0);
      const fcDec = Math.floor(Number(r.pt_fcdec ?? 0));
      if (fcDec > 0 && fcBal !== 0) fcBal = fcBal / Math.pow(10, fcDec);

      let s = supplierData.get(account);
      if (!s) {
        s = {
          account,
          name,
          days_90: 0,
          days_60: 0,
          days_30: 0,
          current: 0,
          balance: currbal,
          unallocated: 0,
          currency: fcCurr || 'GBP',
          fc_rate: fcCurr ? fcRate : 0,
          fc_balance: 0,
        };
        supplierData.set(account, s);
      }
      // Increment the bucket. The bucket key is one of the four
      // summary keys for this path.
      const sAsRec = s as unknown as Record<string, number>;
      sAsRec[bucket] = (sAsRec[bucket] ?? 0) + balance;
      if (fcCurr) s.fc_balance += fcBal;
    }

    const suppliers: AgedSupplier[] = [];
    for (const s of supplierData.values()) {
      const agingSum = s.days_90 + s.days_60 + s.days_30 + s.current;
      s.unallocated = round2(s.balance - agingSum);
      s.days_90 = round2(s.days_90);
      s.days_60 = round2(s.days_60);
      s.days_30 = round2(s.days_30);
      s.current = round2(s.current);
      s.balance = round2(s.balance);
      s.fc_balance = round2(s.fc_balance);
      suppliers.push(s);
      summary.days_90 += s.days_90;
      summary.days_60 += s.days_60;
      summary.days_30 += s.days_30;
      summary.current += s.current;
      summary.total += s.balance;
      summary.unallocated += s.unallocated;
    }
    summary.days_90 = round2(summary.days_90);
    summary.days_60 = round2(summary.days_60);
    summary.days_30 = round2(summary.days_30);
    summary.current = round2(summary.current);
    summary.total = round2(summary.total);
    summary.unallocated = round2(summary.unallocated);

    suppliers.sort((a, b) => Math.abs(b.balance) - Math.abs(a.balance));

    const columnLabels: Record<string, string> =
      periodMode === 'months'
        ? {
            days_90: '3 Months+',
            days_60: '2 Months',
            days_30: '1 Month',
            current: 'Current',
          }
        : {
            days_90: `${periodDays * 3}+ Days`,
            days_60: `${periodDays * 2} Days`,
            days_30: `${periodDays} Days`,
            current: 'Current',
          };

    return {
      success: true,
      summary,
      suppliers,
      period_mode: periodMode,
      period_label: periodLabel,
      column_labels: columnLabels,
    };
  } catch (err: any) {
    return {
      success: false,
      summary: emptySummaryBuckets(),
      suppliers: [],
      period_mode: 'days',
      period_label: '30 Days',
      column_labels: {},
      error: err?.message ?? String(err),
    };
  }
}

// ---------------------------------------------------------------------
// TREND  GET /api/creditors/aged/trend
// ---------------------------------------------------------------------

export interface AgedTrendPoint extends TrendBuckets {
  month: string;
}

export interface AgedTrendResponse {
  success: boolean;
  trend: AgedTrendPoint[];
  error?: string;
}

export async function getAgedCreditorsTrend(
  operaDb: Knex,
  months: number = 6,
): Promise<AgedTrendResponse> {
  try {
    const m = Math.min(24, Math.max(1, Math.floor(months)));
    const today = new Date();
    today.setUTCHours(0, 0, 0, 0);

    const monthEnds: Date[] = [];
    for (let i = 0; i < m; i++) {
      let mm = today.getUTCMonth() + 1 - i;
      let yy = today.getUTCFullYear();
      while (mm <= 0) {
        mm += 12;
        yy -= 1;
      }
      monthEnds.push(lastDayOfMonth(yy, mm));
    }
    monthEnds.reverse();

    const earliest = new Date(monthEnds[0]!.getTime() - 365 * 86_400_000);

    const rows = (await operaDb('ptran')
      .select('pt_trdate', 'pt_trvalue', 'pt_trbal', 'pt_trtype')
      .whereRaw('pt_trbal <> 0')
      .andWhere('pt_trdate', '>=', earliest.toISOString().slice(0, 10))) as unknown as Array<{
      pt_trdate: Date | string | null;
      pt_trvalue: number | string | null;
      pt_trbal: number | string | null;
      pt_trtype: string | null;
    }>;

    const trend: AgedTrendPoint[] = [];
    for (const monthEnd of monthEnds) {
      const buckets = emptyTrendBuckets();
      for (const r of rows ?? []) {
        const trDate = parseDate(r.pt_trdate);
        const balance = Number(r.pt_trbal ?? 0);
        if (!trDate || balance === 0) continue;
        if (trDate > monthEnd) continue;
        const daysOld = Math.max(0, daysBetween(trDate, monthEnd));
        const bucket = classifyAging(daysOld);
        const bAsRec = buckets as unknown as Record<string, number>;
        bAsRec[bucket] = (bAsRec[bucket] ?? 0) + balance;
        buckets.total += balance;
      }
      const ymd = monthEnd.toISOString().slice(0, 7);
      trend.push({
        month: ymd,
        current: round2(buckets.current),
        days_30: round2(buckets.days_30),
        days_60: round2(buckets.days_60),
        days_90: round2(buckets.days_90),
        days_120_plus: round2(buckets.days_120_plus),
        total: round2(buckets.total),
        unallocated: 0,
      });
    }
    return { success: true, trend };
  } catch (err: any) {
    return {
      success: false,
      trend: [],
      error: err?.message ?? String(err),
    };
  }
}

// ---------------------------------------------------------------------
// DETAIL  GET /api/creditors/aged/:account
// ---------------------------------------------------------------------

export interface AgedDetailRow {
  ref: string;
  date: string;
  amount: number;
  days_old: number;
}

export interface AgedDetailResponse {
  success: boolean;
  supplier?: { account: string; name: string };
  aging?: Record<AgingBucket, AgedDetailRow[]>;
  totals?: TrendBuckets;
  error?: string;
}

export async function getAgedCreditorsDetail(
  operaDb: Knex,
  account: string,
): Promise<AgedDetailResponse> {
  const acct = (account ?? '').trim();
  if (!acct) return { success: false, error: 'account required' };
  try {
    const today = new Date();
    today.setUTCHours(0, 0, 0, 0);

    const nameRow = (await operaDb('pname')
      .select(operaDb.raw('RTRIM(pn_name) AS name'))
      .where('pn_account', acct)
      .first()) as { name: string | null } | undefined;
    if (!nameRow?.name) {
      return {
        success: false,
        error: `Supplier account '${acct}' not found`,
      };
    }
    const supplierName = nameRow.name.trim();

    const rows = (await operaDb('ptran')
      .select(
        operaDb.raw('RTRIM(pt_trref) AS ref'),
        'pt_trdate',
        operaDb.raw('pt_trbal AS amount'),
        'pt_trtype',
      )
      .where('pt_account', acct)
      .andWhereRaw('pt_trbal <> 0')
      .orderBy('pt_trdate')) as unknown as Array<{
      ref: string;
      pt_trdate: Date | string | null;
      amount: number | string | null;
      pt_trtype: string | null;
    }>;

    const aging: Record<AgingBucket, AgedDetailRow[]> = {
      current: [],
      days_30: [],
      days_60: [],
      days_90: [],
      days_120_plus: [],
    };
    const totals = emptyTrendBuckets();

    for (const r of rows ?? []) {
      const trDate = parseDate(r.pt_trdate);
      if (!trDate) continue;
      const amount = Number(r.amount ?? 0);
      const daysOld = Math.max(0, daysBetween(trDate, today));
      const bucket = classifyAging(daysOld);
      aging[bucket].push({
        ref: (r.ref ?? '').trim(),
        date: trDate.toISOString().slice(0, 10),
        amount: round2(amount),
        days_old: daysOld,
      });
      const tAsRec = totals as unknown as Record<string, number>;
      tAsRec[bucket] = (tAsRec[bucket] ?? 0) + amount;
      totals.total += amount;
    }
    totals.current = round2(totals.current);
    totals.days_30 = round2(totals.days_30);
    totals.days_60 = round2(totals.days_60);
    totals.days_90 = round2(totals.days_90);
    totals.days_120_plus = round2(totals.days_120_plus);
    totals.total = round2(totals.total);

    return {
      success: true,
      supplier: { account: acct, name: supplierName },
      aging,
      totals,
    };
  } catch (err: any) {
    return {
      success: false,
      error: err?.message ?? String(err),
    };
  }
}
