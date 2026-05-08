/**
 * Pure helpers for the VAT reconciliation route handler.
 *
 * Faithful port of `apps/balance_check/logic/vat_reconcile.py`.
 *
 * Phases (matches Python exactly):
 *   1. Quarter detection (most-recent zvtran/nvat date → calendar quarter)
 *   2. VAT-codes-with-rates fetch (ztax + date-based rate selection) —
 *      now lives in @sqlrag/sam-shared so gocardless can reuse it
 *   3. VAT-by-code aggregation, repeated four times:
 *        - zvtran uncommitted output (va_done=0, va_vattype='S')
 *        - zvtran uncommitted input  (va_done=0, va_vattype='P')
 *        - nvat   committed   output (nv_vattype='S')
 *        - nvat   committed   input  (nv_vattype='P')
 *   4. NL movement summary across VAT accounts (ntran + nacnt desc)
 *   5. Variance computation + reporting (stays inline in the endpoint)
 */
import type { Knex } from 'knex';
import {
  fetchVatCodesWithRates as sharedFetchVatCodesWithRates,
} from '@sqlrag/sam-shared';

function r2(n: number): number {
  return Math.round(n * 100) / 100;
}

// ====================================================================
// Phase 1 — quarter detection
// ====================================================================

export interface QuarterInfo {
  current_quarter: string;
  quarter_start: string;
  quarter_end: string;
  quarters: Array<{
    name: string;
    start: string;
    end: string;
    is_current: boolean;
  }>;
}

function dateToYmd(year: number, month0: number, day: number): string {
  // month0 is 0-indexed (Date convention)
  const mo = String(month0 + 1).padStart(2, '0');
  const da = String(day).padStart(2, '0');
  return `${year}-${mo}-${da}`;
}

/**
 * Calculate VAT quarter start/end dates based on standard UK calendar
 * quarters. Returns current quarter dates plus the 3 previous quarters
 * for reference. Faithful port of `get_vat_quarter_dates()`.
 */
export function getVatQuarterDates(referenceDate: Date | null = null): QuarterInfo {
  const ref = referenceDate ?? new Date();
  const year = ref.getFullYear();
  const month = ref.getMonth() + 1; // 1-indexed for the conditionals below

  let currentQStart: string;
  let currentQEnd: string;
  let quarterName: string;
  let quarterNum: number;

  if (month <= 3) {
    currentQStart = dateToYmd(year, 0, 1);
    currentQEnd = dateToYmd(year, 2, 31);
    quarterName = `Q1 ${year}`;
    quarterNum = 1;
  } else if (month <= 6) {
    currentQStart = dateToYmd(year, 3, 1);
    currentQEnd = dateToYmd(year, 5, 30);
    quarterName = `Q2 ${year}`;
    quarterNum = 2;
  } else if (month <= 9) {
    currentQStart = dateToYmd(year, 6, 1);
    currentQEnd = dateToYmd(year, 8, 30);
    quarterName = `Q3 ${year}`;
    quarterNum = 3;
  } else {
    currentQStart = dateToYmd(year, 9, 1);
    currentQEnd = dateToYmd(year, 11, 31);
    quarterName = `Q4 ${year}`;
    quarterNum = 4;
  }

  const quarters: QuarterInfo['quarters'] = [];
  for (let i = 0; i < 4; i++) {
    let qNum = quarterNum - i;
    let qYear = year;
    while (qNum <= 0) {
      qNum += 4;
      qYear -= 1;
    }
    let qStart: string;
    let qEnd: string;
    if (qNum === 1) {
      qStart = dateToYmd(qYear, 0, 1);
      qEnd = dateToYmd(qYear, 2, 31);
    } else if (qNum === 2) {
      qStart = dateToYmd(qYear, 3, 1);
      qEnd = dateToYmd(qYear, 5, 30);
    } else if (qNum === 3) {
      qStart = dateToYmd(qYear, 6, 1);
      qEnd = dateToYmd(qYear, 8, 30);
    } else {
      qStart = dateToYmd(qYear, 9, 1);
      qEnd = dateToYmd(qYear, 11, 31);
    }
    quarters.push({
      name: `Q${qNum} ${qYear}`,
      start: qStart,
      end: qEnd,
      is_current: i === 0,
    });
  }

  return {
    current_quarter: quarterName,
    quarter_start: currentQStart,
    quarter_end: currentQEnd,
    quarters,
  };
}

// ====================================================================
// Phase 2 — VAT codes + applicable rate
// (Implementation moved to @sqlrag/sam-shared for reuse by gocardless.
//  This module re-exports under the original snake_case shape that
//  the rest of the balance-check code depends on.)
// ====================================================================

export interface VatCode {
  code: string;
  description: string;
  rate: number;
  type: string;
  nominal_account: string;
}

export interface VatCodesResult {
  vat_codes: VatCode[];
  output_nominal_accounts: Set<string>;
  input_nominal_accounts: Set<string>;
}

/**
 * Read ztax and compute the applicable rate for `refDate`.
 * Thin wrapper over the shared helper that maps to balance-check's
 * existing snake_case shape so the rest of the module is unchanged.
 */
export async function fetchVatCodesWithRates(
  db: Knex,
  refDate: Date,
): Promise<VatCodesResult> {
  const result = await sharedFetchVatCodesWithRates(db, refDate);
  return {
    vat_codes: result.vatCodes,
    output_nominal_accounts: result.outputNominalAccounts,
    input_nominal_accounts: result.inputNominalAccounts,
  };
}

// ====================================================================
// Phase 3 — VAT-by-code aggregation (zvtran uncommitted | nvat committed)
// ====================================================================

export interface VatAggregate {
  total_vat: number;
  by_code: Array<{
    vat_code: string;
    transaction_count: number;
    vat_amount: number;
    net_amount?: number;
  }>;
}

/**
 * Aggregate uncommitted (va_done=0) VAT transactions from zvtran for a
 * date range. Faithful port of `fetch_zvtran_aggregate`.
 */
export async function fetchZvtranAggregate(
  db: Knex,
  opts: {
    vattype: 'S' | 'P';
    quarterStart: string;
    quarterEnd: string;
    includeNet?: boolean;
  },
): Promise<VatAggregate> {
  const includeNet = opts.includeNet ?? true;
  const sql = `
    SELECT
      va_anvat AS vat_code,
      COUNT(*) AS transaction_count,
      SUM(va_vatval) AS vat_amount,
      SUM(va_trvalue) AS net_amount
    FROM zvtran WITH (NOLOCK)
    WHERE va_vattype = ?
      AND va_done = 0
      AND va_taxdate >= ?
      AND va_taxdate <= ?
    GROUP BY va_anvat
    ORDER BY va_anvat
  `;
  const rows = (await db.raw(sql, [
    opts.vattype,
    opts.quarterStart,
    opts.quarterEnd,
  ])) as unknown as Array<{
    vat_code: string | null;
    transaction_count: number | null;
    vat_amount: number | null;
    net_amount: number | null;
  }>;

  let total = 0;
  const byCode: VatAggregate['by_code'] = [];
  for (const row of Array.isArray(rows) ? rows : []) {
    const vatAmount = Number(row.vat_amount ?? 0);
    total += vatAmount;
    const item: VatAggregate['by_code'][number] = {
      vat_code: row.vat_code ? String(row.vat_code).trim() : '',
      transaction_count: Number(row.transaction_count ?? 0),
      vat_amount: r2(vatAmount),
    };
    if (includeNet) {
      item.net_amount = r2(Number(row.net_amount ?? 0));
    }
    byCode.push(item);
  }

  return { total_vat: total, by_code: byCode };
}

/**
 * Aggregate committed VAT transactions from nvat for a date range.
 * Faithful port of `fetch_nvat_aggregate`.
 */
export async function fetchNvatAggregate(
  db: Knex,
  opts: {
    vattype: 'S' | 'P';
    periodStart: string;
    periodEnd: string;
  },
): Promise<VatAggregate> {
  const sql = `
    SELECT
      nv_vatcode AS vat_code,
      COUNT(*) AS transaction_count,
      SUM(nv_vatval) AS vat_amount
    FROM nvat WITH (NOLOCK)
    WHERE nv_vattype = ?
      AND nv_date >= ?
      AND nv_date <= ?
    GROUP BY nv_vatcode
    ORDER BY nv_vatcode
  `;
  const rows = (await db.raw(sql, [
    opts.vattype,
    opts.periodStart,
    opts.periodEnd,
  ])) as unknown as Array<{
    vat_code: string | null;
    transaction_count: number | null;
    vat_amount: number | null;
  }>;

  let total = 0;
  const byCode: VatAggregate['by_code'] = [];
  for (const row of Array.isArray(rows) ? rows : []) {
    const vatAmount = Number(row.vat_amount ?? 0);
    total += vatAmount;
    byCode.push({
      vat_code: row.vat_code ? String(row.vat_code).trim() : '',
      transaction_count: Number(row.transaction_count ?? 0),
      vat_amount: r2(vatAmount),
    });
  }

  return { total_vat: total, by_code: byCode };
}

// ====================================================================
// Phase 4 — per-account NL movement summary
// ====================================================================

export interface NlVatMovement {
  account: string;
  description: string;
  type: 'Output' | 'Input' | 'Mixed';
  debits: number;
  credits: number;
  net: number;
  transaction_count: number;
}

export interface NlMovementResult {
  accounts: NlVatMovement[];
  output_total: number;
  input_total: number;
}

/**
 * For each VAT-related NL account, fetch ntran movement and summarise.
 * Faithful port of `fetch_nl_vat_movements`.
 */
export async function fetchNlVatMovements(
  db: Knex,
  opts: {
    outputNominalAccounts: Set<string>;
    inputNominalAccounts: Set<string>;
    periodStart: string;
    periodEnd: string;
  },
): Promise<NlMovementResult> {
  const allAccounts = new Set<string>([
    ...opts.outputNominalAccounts,
    ...opts.inputNominalAccounts,
  ]);
  const movements: NlVatMovement[] = [];
  let outputTotal = 0;
  let inputTotal = 0;

  for (const acnt of allAccounts) {
    const ntranSql = `
      SELECT
        SUM(CASE WHEN nt_value > 0 THEN nt_value ELSE 0 END) AS debits,
        SUM(CASE WHEN nt_value < 0 THEN ABS(nt_value) ELSE 0 END) AS credits,
        SUM(nt_value) AS net,
        COUNT(*) AS transaction_count
      FROM ntran WITH (NOLOCK)
      WHERE nt_acnt = ?
        AND nt_entr >= ?
        AND nt_entr <= ?
    `;
    const rows = (await db.raw(ntranSql, [acnt, opts.periodStart, opts.periodEnd])) as unknown as Array<{
      debits: number | null;
      credits: number | null;
      net: number | null;
      transaction_count: number | null;
    }>;
    const row = Array.isArray(rows) && rows.length > 0 ? rows[0] : null;
    if (!row) continue;

    const debits = Number(row.debits ?? 0);
    const credits = Number(row.credits ?? 0);
    const net = Number(row.net ?? 0);
    const txnCount = Number(row.transaction_count ?? 0);
    if (txnCount <= 0) continue;

    const isOutput = opts.outputNominalAccounts.has(acnt);
    const isInput = opts.inputNominalAccounts.has(acnt);

    // Description from nacnt
    const nacntSql = `SELECT RTRIM(na_desc) AS description FROM nacnt WITH (NOLOCK) WHERE na_acnt = ?`;
    const descRows = (await db.raw(nacntSql, [acnt])) as unknown as Array<{ description: string | null }>;
    const description = descRows?.[0]?.description ?? '';

    movements.push({
      account: acnt,
      description,
      type: isOutput ? 'Output' : isInput ? 'Input' : 'Mixed',
      debits: r2(debits),
      credits: r2(credits),
      net: r2(net),
      transaction_count: txnCount,
    });

    if (isOutput) outputTotal += credits;
    if (isInput) inputTotal += debits;
  }

  return {
    accounts: movements,
    output_total: outputTotal,
    input_total: inputTotal,
  };
}
