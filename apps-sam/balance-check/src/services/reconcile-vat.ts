/**
 * VAT reconciliation — faithful port of `reconcile_vat()` from
 * `apps/balance_check/api/routes.py`.
 *
 * Reconciles VAT accounts. Compares VAT liability in NL to VAT
 * transactions across:
 *   - Quarterly VAT (uncommitted from zvtran + committed from nvat)
 *   - YTD VAT (nvat totals)
 *   - NL VAT accounts (ntran + nacnt)
 *
 * Read-only against Opera SQL.
 */
import type { Knex } from 'knex';
import {
  getVatQuarterDates,
  fetchVatCodesWithRates,
  fetchZvtranAggregate,
  fetchNvatAggregate,
  fetchNlVatMovements,
  type QuarterInfo,
  type VatCode,
  type VatAggregate,
  type NlVatMovement,
} from './vat-helpers.js';

function r2(n: number): number {
  return Math.round(n * 100) / 100;
}

function formatNow(): string {
  const now = new Date();
  const pad = (n: number, w = 2) => String(n).padStart(w, '0');
  return (
    `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())} ` +
    `${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`
  );
}

function formatGbp(n: number): string {
  return n.toLocaleString('en-GB', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

interface NominalAccountSummary {
  account: string;
  description: string;
  type: 'Output' | 'Input' | 'Mixed';
  brought_forward: number;
  current_year_debits: number;
  current_year_credits: number;
  current_year_net: number;
  closing_balance: number;
}

interface VatBankTransaction {
  date: string;
  amount: number;
  reference: string;
  account: string;
}

export interface ReconcileVatResponse {
  success: boolean;
  reconciliation_date?: string;
  quarter_info?: QuarterInfo;
  vat_codes?: VatCode[];
  current_quarter?: {
    output_vat: { source: string; total_vat: number; by_code: VatAggregate['by_code']; quarter: string };
    input_vat: { source: string; total_vat: number; by_code: VatAggregate['by_code']; quarter: string };
    uncommitted: {
      source: string;
      quarter: string;
      period_start: string;
      period_end: string;
      output_vat: { total: number; by_code: VatAggregate['by_code'] };
      input_vat: { total: number; by_code: VatAggregate['by_code'] };
      net_liability: number;
      description: string;
    };
    nominal_movements: {
      source: string;
      quarter: string;
      period_start: string;
      period_end: string;
      accounts: NlVatMovement[];
      output_vat_total: number;
      input_vat_total: number;
      net_movement: number;
    };
  };
  year_to_date?: {
    output_vat: { source: string; total_vat: number; by_code: VatAggregate['by_code']; current_year: number };
    input_vat: { source: string; total_vat: number; by_code: VatAggregate['by_code']; current_year: number };
    nominal_accounts: {
      source: string;
      accounts: NominalAccountSummary[];
      total_balance: number;
      current_year: number;
    };
  };
  vat_balance?: {
    total_uncommitted_vat: number;
    total_uncommitted_output: number;
    total_uncommitted_input: number;
    vat_account_balance: number;
    vat_bank_transactions: VatBankTransaction[];
    vat_bank_total: number;
    expected_balance: number;
    balance_variance: number;
    reconciled: boolean;
  };
  variance?: {
    quarter: {
      uncommitted_output: number;
      uncommitted_input: number;
      uncommitted_net: number;
      nl_output_movement: number;
      nl_input_movement: number;
      nl_net_movement: number;
      variance_amount: number;
      variance_absolute: number;
      reconciled: boolean;
    };
    year_to_date: {
      nvat_output_total: number;
      nvat_input_total: number;
      nvat_net_liability: number;
      nominal_ledger_balance: number;
      variance_amount: number;
      variance_absolute: number;
      reconciled: boolean;
    };
  };
  status?: 'RECONCILED' | 'VARIANCE' | 'UNRECONCILED';
  message?: string;
  error?: string;
}

export async function reconcileVat(db: Knex): Promise<ReconcileVatResponse> {
  try {
    // Find the most recent uncommitted VAT transaction date to determine the relevant quarter.
    // Handles cases where the database has data from a different year than the current calendar date.
    const recentVatRows = (await db.raw(`
      SELECT MAX(va_taxdate) AS most_recent_date
      FROM zvtran WITH (NOLOCK)
      WHERE va_done = 0
    `)) as unknown as Array<{ most_recent_date: Date | string | null }>;

    let referenceDate: Date | null = null;
    const recentDate = recentVatRows?.[0]?.most_recent_date;
    if (recentDate) {
      referenceDate = recentDate instanceof Date ? recentDate : new Date(recentDate);
      if (Number.isNaN(referenceDate.getTime())) referenceDate = null;
    }

    let quarterInfo = getVatQuarterDates(referenceDate);

    // Get current calendar year from ntran (using YEAR(nt_entr) — calendar, not fiscal).
    const cyRows = (await db.raw(
      'SELECT MAX(YEAR(nt_entr)) AS current_year FROM ntran WITH (NOLOCK)',
    )) as unknown as Array<{ current_year: number | null }>;
    const currentYear = Number(cyRows?.[0]?.current_year ?? new Date().getFullYear());

    // If no uncommitted VAT, fall back to nvat for most recent date
    if (referenceDate === null) {
      const nvatRows = (await db.raw(`
        SELECT MAX(nv_date) AS most_recent_date
        FROM nvat WITH (NOLOCK)
      `)) as unknown as Array<{ most_recent_date: Date | string | null }>;
      const nvatDate = nvatRows?.[0]?.most_recent_date;
      if (nvatDate) {
        referenceDate = nvatDate instanceof Date ? nvatDate : new Date(nvatDate);
        if (!Number.isNaN(referenceDate.getTime())) {
          quarterInfo = getVatQuarterDates(referenceDate);
        }
      }
    }

    // VAT codes + applicable rate (today's date as reference)
    const vatResult = await fetchVatCodesWithRates(db, new Date());
    const vatCodes = vatResult.vat_codes;
    const outputNominalAccounts = vatResult.output_nominal_accounts;
    const inputNominalAccounts = vatResult.input_nominal_accounts;
    const allVatNominals = new Set<string>([...outputNominalAccounts, ...inputNominalAccounts]);

    const quarterStart = quarterInfo.quarter_start;
    const quarterEnd = quarterInfo.quarter_end;

    // ========== CURRENT QUARTER — Uncommitted VAT (zvtran) ==========
    const uncOut = await fetchZvtranAggregate(db, {
      vattype: 'S',
      quarterStart,
      quarterEnd,
    });
    const uncommittedOutputTotal = uncOut.total_vat;
    const uncommittedOutputByCode = uncOut.by_code;

    const uncIn = await fetchZvtranAggregate(db, {
      vattype: 'P',
      quarterStart,
      quarterEnd,
    });
    const uncommittedInputTotal = uncIn.total_vat;
    const uncommittedInputByCode = uncIn.by_code;

    const uncommittedNet = uncommittedOutputTotal - uncommittedInputTotal;

    // ========== CURRENT QUARTER — NL Movements ==========
    const nlMovementResult = await fetchNlVatMovements(db, {
      outputNominalAccounts,
      inputNominalAccounts,
      periodStart: quarterStart,
      periodEnd: quarterEnd,
    });
    const quarterNlMovements = nlMovementResult.accounts;
    const quarterNlOutputTotal = nlMovementResult.output_total;
    const quarterNlInputTotal = nlMovementResult.input_total;

    // ========== CURRENT QUARTER — nvat transactions ==========
    const qOut = await fetchNvatAggregate(db, {
      vattype: 'S',
      periodStart: quarterStart,
      periodEnd: quarterEnd,
    });
    const quarterOutputTotal = qOut.total_vat;
    const quarterOutputByCode = qOut.by_code;

    const qIn = await fetchNvatAggregate(db, {
      vattype: 'P',
      periodStart: quarterStart,
      periodEnd: quarterEnd,
    });
    const quarterInputTotal = qIn.total_vat;
    const quarterInputByCode = qIn.by_code;

    // ========== YEAR TO DATE — nvat totals ==========
    const ytdOutputRows = (await db.raw(
      `
      SELECT nv_vatcode AS vat_code, COUNT(*) AS transaction_count, SUM(nv_vatval) AS vat_amount
      FROM nvat WITH (NOLOCK)
      WHERE nv_vattype = 'S' AND YEAR(nv_date) = ?
      GROUP BY nv_vatcode
      ORDER BY nv_vatcode
      `,
      [currentYear],
    )) as unknown as Array<{ vat_code: string | null; transaction_count: number | null; vat_amount: number | null }>;
    let ytdOutputTotal = 0;
    const ytdOutputByCode: VatAggregate['by_code'] = [];
    for (const row of Array.isArray(ytdOutputRows) ? ytdOutputRows : []) {
      const va = Number(row.vat_amount ?? 0);
      ytdOutputTotal += va;
      ytdOutputByCode.push({
        vat_code: row.vat_code ? String(row.vat_code).trim() : '',
        transaction_count: Number(row.transaction_count ?? 0),
        vat_amount: r2(va),
      });
    }

    const ytdInputRows = (await db.raw(
      `
      SELECT nv_vatcode AS vat_code, COUNT(*) AS transaction_count, SUM(nv_vatval) AS vat_amount
      FROM nvat WITH (NOLOCK)
      WHERE nv_vattype = 'P' AND YEAR(nv_date) = ?
      GROUP BY nv_vatcode
      ORDER BY nv_vatcode
      `,
      [currentYear],
    )) as unknown as Array<{ vat_code: string | null; transaction_count: number | null; vat_amount: number | null }>;
    let ytdInputTotal = 0;
    const ytdInputByCode: VatAggregate['by_code'] = [];
    for (const row of Array.isArray(ytdInputRows) ? ytdInputRows : []) {
      const va = Number(row.vat_amount ?? 0);
      ytdInputTotal += va;
      ytdInputByCode.push({
        vat_code: row.vat_code ? String(row.vat_code).trim() : '',
        transaction_count: Number(row.transaction_count ?? 0),
        vat_amount: r2(va),
      });
    }

    // ========== YEAR TO DATE — Nominal accounts ==========
    const nominalAccounts: NominalAccountSummary[] = [];
    let nlTotal = 0;

    for (const acnt of allVatNominals) {
      const nacntRows = (await db.raw(
        `SELECT na_acnt, RTRIM(na_desc) AS description, na_ytddr, na_ytdcr, na_prydr, na_prycr
         FROM nacnt WITH (NOLOCK) WHERE na_acnt = ?`,
        [acnt],
      )) as unknown as Array<{
        na_acnt: string | null;
        description: string | null;
        na_ytddr: number | null;
        na_ytdcr: number | null;
        na_prydr: number | null;
        na_prycr: number | null;
      }>;
      if (!Array.isArray(nacntRows) || nacntRows.length === 0) continue;
      const acc = nacntRows[0]!;

      const pryDr = Number(acc.na_prydr ?? 0);
      const pryCr = Number(acc.na_prycr ?? 0);
      const bfBalance = pryCr - pryDr;

      const ntranRows = (await db.raw(
        `
        SELECT
          SUM(CASE WHEN nt_value > 0 THEN nt_value ELSE 0 END) AS debits,
          SUM(CASE WHEN nt_value < 0 THEN ABS(nt_value) ELSE 0 END) AS credits,
          SUM(nt_value) AS net
        FROM ntran WITH (NOLOCK)
        WHERE nt_acnt = ? AND YEAR(nt_entr) = ?
        `,
        [acnt, currentYear],
      )) as unknown as Array<{ debits: number | null; credits: number | null; net: number | null }>;
      const cyDr = Number(ntranRows?.[0]?.debits ?? 0);
      const cyCr = Number(ntranRows?.[0]?.credits ?? 0);
      const cyNet = Number(ntranRows?.[0]?.net ?? 0);

      // VAT liability is typically a credit balance (negative) → negate for closing
      const closingBalance = -cyNet;

      const isOutput = outputNominalAccounts.has(acnt);
      const isInput = inputNominalAccounts.has(acnt);

      nominalAccounts.push({
        account: acnt,
        description: acc.description ?? '',
        type: isOutput ? 'Output' : isInput ? 'Input' : 'Mixed',
        brought_forward: r2(bfBalance),
        current_year_debits: r2(cyDr),
        current_year_credits: r2(cyCr),
        current_year_net: r2(cyNet),
        closing_balance: r2(closingBalance),
      });

      nlTotal += closingBalance;
    }

    // ========== VAT ACCOUNT BALANCE & BANK TRANSACTIONS ==========
    const totalUncRows = (await db.raw(`
      SELECT
        SUM(CASE WHEN va_vattype = 'S' THEN va_vatval ELSE 0 END) AS output_total,
        SUM(CASE WHEN va_vattype = 'P' THEN va_vatval ELSE 0 END) AS input_total
      FROM zvtran WITH (NOLOCK)
      WHERE va_done = 0
    `)) as unknown as Array<{ output_total: number | null; input_total: number | null }>;

    const totalUncommittedOutput = Number(totalUncRows?.[0]?.output_total ?? 0);
    const totalUncommittedInput = Number(totalUncRows?.[0]?.input_total ?? 0);
    const totalUncommittedNet = totalUncommittedOutput - totalUncommittedInput;

    // VAT nominal account balances
    let vatAccountBalance = 0;
    for (const acnt of allVatNominals) {
      const balanceRows = (await db.raw(
        'SELECT SUM(nt_value) AS balance FROM ntran WITH (NOLOCK) WHERE nt_acnt = ?',
        [acnt],
      )) as unknown as Array<{ balance: number | null }>;
      const bal = balanceRows?.[0]?.balance;
      if (bal !== null && bal !== undefined) {
        vatAccountBalance += Number(bal);
      }
    }

    // Bank transactions on VAT accounts (VAT payments to HMRC / refunds)
    const vatBankTransactions: VatBankTransaction[] = [];
    let vatBankTotal = 0;
    for (const acnt of allVatNominals) {
      const bankRows = (await db.raw(
        `
        SELECT ax_date AS trans_date, ax_value AS amount, ax_tref AS reference, ax_source AS source
        FROM anoml WITH (NOLOCK)
        WHERE ax_nacnt = ? AND ax_source = 'A'
        ORDER BY ax_date DESC
        `,
        [acnt],
      )) as unknown as Array<{
        trans_date: Date | string | null;
        amount: number | null;
        reference: string | null;
        source: string | null;
      }>;
      for (const row of Array.isArray(bankRows) ? bankRows : []) {
        const amount = Number(row.amount ?? 0);
        vatBankTotal += amount;
        vatBankTransactions.push({
          date: row.trans_date ? String(row.trans_date) : '',
          amount: r2(amount),
          reference: (row.reference ?? '').trim(),
          account: acnt,
        });
      }
    }

    const adjustedBalance = -vatAccountBalance; // Negate because VAT liability is a credit
    const expectedBalance = totalUncommittedNet - vatBankTotal;
    const balanceVariance = adjustedBalance - expectedBalance;

    // ========== VARIANCE CALCULATION ==========
    const quarterVariance = uncommittedNet - (quarterNlOutputTotal - quarterNlInputTotal);
    const quarterVarianceAbs = Math.abs(quarterVariance);

    const ytdNetVat = ytdOutputTotal - ytdInputTotal;
    const ytdVariance = ytdNetVat - nlTotal;
    const ytdVarianceAbs = Math.abs(ytdVariance);

    const reconciled = quarterVarianceAbs < 0.005;
    const status: 'RECONCILED' | 'VARIANCE' = reconciled ? 'RECONCILED' : 'VARIANCE';
    let message: string;
    if (reconciled) {
      message = `${quarterInfo.current_quarter}: Uncommitted VAT (£${formatGbp(uncommittedNet)}) reconciles to NL movements`;
    } else if (quarterVariance > 0) {
      message = `${quarterInfo.current_quarter}: Uncommitted VAT (£${formatGbp(uncommittedNet)}) is £${formatGbp(quarterVarianceAbs)} MORE than NL movements`;
    } else {
      message = `${quarterInfo.current_quarter}: Uncommitted VAT (£${formatGbp(uncommittedNet)}) is £${formatGbp(quarterVarianceAbs)} LESS than NL movements`;
    }

    return {
      success: true,
      reconciliation_date: formatNow(),
      quarter_info: quarterInfo,
      vat_codes: vatCodes,
      current_quarter: {
        output_vat: {
          source: 'nvat (VAT Transactions - Sales/Output)',
          total_vat: r2(quarterOutputTotal),
          by_code: quarterOutputByCode,
          quarter: quarterInfo.current_quarter,
        },
        input_vat: {
          source: 'nvat (VAT Transactions - Purchase/Input)',
          total_vat: r2(quarterInputTotal),
          by_code: quarterInputByCode,
          quarter: quarterInfo.current_quarter,
        },
        uncommitted: {
          source: 'zvtran (VAT Return Transactions - va_done=0)',
          quarter: quarterInfo.current_quarter,
          period_start: quarterStart,
          period_end: quarterEnd,
          output_vat: { total: r2(uncommittedOutputTotal), by_code: uncommittedOutputByCode },
          input_vat: { total: r2(uncommittedInputTotal), by_code: uncommittedInputByCode },
          net_liability: r2(uncommittedNet),
          description: 'VAT transactions not yet submitted in a VAT return',
        },
        nominal_movements: {
          source: 'ntran (Nominal Ledger)',
          quarter: quarterInfo.current_quarter,
          period_start: quarterStart,
          period_end: quarterEnd,
          accounts: quarterNlMovements,
          output_vat_total: r2(quarterNlOutputTotal),
          input_vat_total: r2(quarterNlInputTotal),
          net_movement: r2(quarterNlOutputTotal - quarterNlInputTotal),
        },
      },
      year_to_date: {
        output_vat: {
          source: 'nvat (VAT Transactions - Sales/Output)',
          total_vat: r2(ytdOutputTotal),
          by_code: ytdOutputByCode,
          current_year: currentYear,
        },
        input_vat: {
          source: 'nvat (VAT Transactions - Purchase/Input)',
          total_vat: r2(ytdInputTotal),
          by_code: ytdInputByCode,
          current_year: currentYear,
        },
        nominal_accounts: {
          source: 'ntran (Nominal Ledger)',
          accounts: nominalAccounts,
          total_balance: r2(nlTotal),
          current_year: currentYear,
        },
      },
      vat_balance: {
        total_uncommitted_vat: r2(totalUncommittedNet),
        total_uncommitted_output: r2(totalUncommittedOutput),
        total_uncommitted_input: r2(totalUncommittedInput),
        vat_account_balance: r2(adjustedBalance),
        vat_bank_transactions: vatBankTransactions.slice(0, 10),
        vat_bank_total: r2(vatBankTotal),
        expected_balance: r2(expectedBalance),
        balance_variance: r2(balanceVariance),
        reconciled: Math.abs(balanceVariance) < 0.005,
      },
      variance: {
        quarter: {
          uncommitted_output: r2(uncommittedOutputTotal),
          uncommitted_input: r2(uncommittedInputTotal),
          uncommitted_net: r2(uncommittedNet),
          nl_output_movement: r2(quarterNlOutputTotal),
          nl_input_movement: r2(quarterNlInputTotal),
          nl_net_movement: r2(quarterNlOutputTotal - quarterNlInputTotal),
          variance_amount: r2(quarterVariance),
          variance_absolute: r2(quarterVarianceAbs),
          reconciled: quarterVarianceAbs < 0.005,
        },
        year_to_date: {
          nvat_output_total: r2(ytdOutputTotal),
          nvat_input_total: r2(ytdInputTotal),
          nvat_net_liability: r2(ytdNetVat),
          nominal_ledger_balance: r2(nlTotal),
          variance_amount: r2(ytdVariance),
          variance_absolute: r2(ytdVarianceAbs),
          reconciled: ytdVarianceAbs < 0.005,
        },
      },
      status,
      message,
    };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}
