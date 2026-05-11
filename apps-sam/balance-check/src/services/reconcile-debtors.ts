/**
 * Debtors reconciliation — faithful port of `reconcile_debtors()`
 * from `apps/balance_check/api/routes.py`.
 *
 * Reconciles Sales Ledger (stran) to Debtors Control Account (NL).
 * Compares outstanding balances in stran with the control account in
 * nacnt/ntran.
 *
 * Read-only against Opera SQL.
 *
 * NOTE on parity:
 *   The Python `reconcile_debtors` and `reconcile_creditors` have
 *   structurally similar but not identical variance-analysis sections.
 *   We use the shared `analyseVariance` helper, which produces the
 *   creditors-shape (`summary` nesting). This is a MINOR shape
 *   divergence from Python debtors, which has flat top-level fields
 *   (`value_diff_total`, `nl_only_total`, `sl_only_total`).
 *   See docs/sam-rewrite/progress.md for the follow-up to align
 *   the debtors response shape exactly.
 */
import type { Knex } from 'knex';
import { getControlAccounts } from '@sqlrag/sam-shared';
import {
  DEBTORS,
  fetchOutstanding,
  fetchBreakdownByType,
  fetchMasterTotals,
  fetchMasterTxnVariance,
  fetchTransferFilePending,
  fetchTransferFileSummary,
  type BreakdownRow,
  type MasterTxnVarianceRow,
  type TransferFilePendingRow,
} from './sub-ledger-reconcile.js';
import {
  fetchControlAccountDetails,
  type ControlAccountDetail,
} from './control-account-details.js';
import { analyseVariance, type VarianceAnalysisResult } from './variance-analysis.js';

/**
 * Translate the shared variance-analysis result (creditors-shape with
 * `pl_*` keys) into the debtors-shape with `sl_*` keys + flat
 * top-level totals matching the legacy Python `reconcile_debtors`
 * response (apps/balance_check/api/routes.py).
 *
 * Legacy debtors fields:
 *   value_diff_total, value_diff_count
 *   nl_only_total, nl_only_count
 *   sl_only_total, sl_only_count
 *   small_balance_count
 *   nl_total_check, sl_total_check
 *   items[], note
 */
function adaptForDebtors(v: VarianceAnalysisResult | undefined): Record<string, unknown> | undefined {
  if (!v) return undefined;
  return {
    items: v.items,
    count: v.count,
    value_diff_count: v.value_diff_count,
    value_diff_total: v.summary.value_differences.total,
    nl_only_count: v.nl_only_count,
    nl_only_total: v.summary.nl_only.total,
    sl_only_count: v.pl_only_count,
    sl_only_total: v.summary.pl_only.total,
    small_balance_count: v.small_balance_count,
    nl_total_check: v.nl_total_check,
    sl_total_check: v.pl_total_check,
    // Keep the nested `summary` for backwards compatibility with any
    // caller that has already migrated to the SAM-port shape.
    summary: {
      nl_only: v.summary.nl_only,
      sl_only: v.summary.pl_only,
      value_differences: v.summary.value_differences,
    },
  };
}

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

export interface AgedAnalysisRow {
  age_band: string;
  count: number;
  total: number;
}

export interface TopCustomer {
  account: string;
  name: string;
  invoice_count: number;
  outstanding: number;
}

export interface ReconcileDebtorsResponse {
  success: boolean;
  reconciliation_date: string;
  sales_ledger: {
    source: string;
    total_outstanding: number;
    transaction_count: number;
    breakdown_by_type: BreakdownRow[];
    transfer_file: {
      source: string;
      posted_to_nl: { count: number; total: number };
      pending_transfer: {
        count: number;
        total: number;
        transactions: TransferFilePendingRow[];
      };
    };
    customer_master_check: {
      source: string;
      total: number;
      customer_count: number;
      matches_stran: boolean;
      balance_variance: number;
      customers_with_balance_issues: MasterTxnVarianceRow[];
      total_customers_with_issues: number;
    };
  };
  nominal_ledger: {
    source: string;
    control_accounts: ControlAccountDetail[];
    total_balance: number;
    current_year: number;
  };
  variance: {
    amount: number;
    absolute: number;
    sales_ledger_total: number;
    transfer_file_posted: number;
    transfer_file_pending: number;
    nominal_ledger_total: number;
    posted_variance: number;
    posted_variance_abs: number;
    reconciled: boolean;
    has_pending_transfers: boolean;
  };
  status: 'RECONCILED' | 'UNRECONCILED';
  message?: string;
  details: unknown[];
  control_account_used: string;
  /**
   * Debtors-shape variance analysis: flat top-level `sl_*` /
   * `value_diff_*` / `nl_only_*` keys per legacy
   * `reconcile_debtors`. The nested `summary.{nl_only,sl_only,
   * value_differences}` block is included for backwards-compat.
   */
  variance_analysis?: Record<string, unknown>;
  aged_analysis?: AgedAnalysisRow[];
  top_customers?: TopCustomer[];
  error?: string;
}

export async function reconcileDebtors(db: Knex): Promise<ReconcileDebtorsResponse> {
  try {
    const controlAccounts = await getControlAccounts(db);
    const debtorsControl = controlAccounts.debtorsControl;

    // ========== SALES LEDGER (stran) — phases extracted to helpers ==========
    const outstanding = await fetchOutstanding(db, DEBTORS);
    const slTotal = outstanding.total_outstanding;
    const slCount = outstanding.transaction_count;

    const typeNames: Record<string, string> = {
      I: 'Invoices',
      C: 'Credit Notes',
      R: 'Receipts',
      B: 'Brought Forward',
    };
    const slByType = await fetchBreakdownByType(db, DEBTORS, typeNames);

    const master = await fetchMasterTotals(db, DEBTORS);
    const snameTotal = master.total_balance;
    const snameCount = master.count;

    const customerBalanceIssues = await fetchMasterTxnVariance(db, DEBTORS);

    // ========== TRANSFER FILE (snoml) — phases extracted to helpers ==========
    const pendingTransactions = await fetchTransferFilePending(db, DEBTORS);
    const transferSummary = await fetchTransferFileSummary(db, DEBTORS);
    const slPostedCount = transferSummary.posted.count;
    const slPostedTotal = transferSummary.posted.total;
    const slPendingCount = transferSummary.pending.count;
    const slPendingTotal = transferSummary.pending.total;

    // ========== NOMINAL LEDGER (nacnt/ntran) ==========
    const nlResult = await fetchControlAccountDetails(
      db,
      debtorsControl,
      '%Debtor%Control%',
      '%Trade%Debtor%',
      false, // do NOT negate ntran (debtors keep positive sign)
    );
    const nlTotal = nlResult.nlTotal;
    const currentYear = nlResult.currentYear;
    const nlDetails = nlResult.details;

    // ========== VARIANCE CALCULATION ==========
    const variance = slTotal - nlTotal;
    const varianceAbs = Math.abs(variance);
    const variancePosted = slPostedTotal - nlTotal;
    const variancePostedAbs = Math.abs(variancePosted);

    // ========== VARIANCE ANALYSIS ==========
    // Debtors: pull NL across ALL years (matches Python comment), and
    // run analysis even when reconciled.
    const varianceAnalysis = await analyseVariance(db, {
      side: 'debtors',
      controlAccountCodes: nlResult.controlAccountCodes,
      currentYear,
      varianceAmount: variance,
      filterNlByCurrentYear: false,
      alwaysRun: true,
    });

    // ========== AGED ANALYSIS ==========
    const agedSql = `
      SELECT
        CASE
          WHEN DATEDIFF(day, st_trdate, GETDATE()) <= 30 THEN 'Current (0-30 days)'
          WHEN DATEDIFF(day, st_trdate, GETDATE()) <= 60 THEN '31-60 days'
          WHEN DATEDIFF(day, st_trdate, GETDATE()) <= 90 THEN '61-90 days'
          ELSE 'Over 90 days'
        END AS age_band,
        COUNT(*) AS count,
        SUM(st_trbal) AS total
      FROM stran
      WHERE st_trbal <> 0
        AND RTRIM(st_account) IN (SELECT RTRIM(sn_account) FROM sname)
      GROUP BY CASE
        WHEN DATEDIFF(day, st_trdate, GETDATE()) <= 30 THEN 'Current (0-30 days)'
        WHEN DATEDIFF(day, st_trdate, GETDATE()) <= 60 THEN '31-60 days'
        WHEN DATEDIFF(day, st_trdate, GETDATE()) <= 90 THEN '61-90 days'
        ELSE 'Over 90 days'
      END
      ORDER BY MIN(DATEDIFF(day, st_trdate, GETDATE()))
    `;
    let agedRows: Array<{
      age_band: string;
      count: number | null;
      total: number | null;
    }> = [];
    try {
      const result = (await db.raw(agedSql)) as unknown as typeof agedRows;
      agedRows = Array.isArray(result) ? result : [];
    } catch {
      agedRows = [];
    }
    const agedAnalysis: AgedAnalysisRow[] = agedRows.map((row) => ({
      age_band: row.age_band,
      count: Number(row.count ?? 0),
      total: r2(Number(row.total ?? 0)),
    }));

    // ========== TOP CUSTOMERS ==========
    const topCustomersSql = `
      SELECT TOP 10
        RTRIM(s.sn_account) AS account,
        RTRIM(s.sn_name) AS customer_name,
        COUNT(*) AS invoice_count,
        SUM(st.st_trbal) AS outstanding
      FROM stran WITH (NOLOCK) st
      JOIN sname WITH (NOLOCK) s ON st.st_account = s.sn_account
      WHERE st.st_trbal <> 0
      GROUP BY s.sn_account, s.sn_name
      ORDER BY SUM(st.st_trbal) DESC
    `;
    let topRows: Array<{
      account: string | null;
      customer_name: string | null;
      invoice_count: number | null;
      outstanding: number | null;
    }> = [];
    try {
      const result = (await db.raw(topCustomersSql)) as unknown as typeof topRows;
      topRows = Array.isArray(result) ? result : [];
    } catch {
      topRows = [];
    }
    const topCustomers: TopCustomer[] = topRows.map((row) => ({
      account: row.account ?? '',
      name: row.customer_name ?? '',
      invoice_count: Number(row.invoice_count ?? 0),
      outstanding: r2(Number(row.outstanding ?? 0)),
    }));

    const reconciled = varianceAbs < 0.005;
    const status: 'RECONCILED' | 'UNRECONCILED' = reconciled ? 'RECONCILED' : 'UNRECONCILED';
    let message: string;
    if (reconciled) {
      if (slPendingCount > 0) {
        message =
          `Sales Ledger reconciles to Nominal Ledger. ${slPendingCount} transactions ` +
          `(£${Math.abs(slPendingTotal).toLocaleString('en-GB', {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
          })}) in transfer file pending.`;
      } else {
        message = 'Sales Ledger reconciles to Nominal Ledger Debtors Control';
      }
    } else if (variance > 0) {
      message = `Sales Ledger is £${varianceAbs.toLocaleString('en-GB', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      })} MORE than Nominal Ledger Control`;
    } else {
      message = `Sales Ledger is £${varianceAbs.toLocaleString('en-GB', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      })} LESS than Nominal Ledger Control`;
    }

    return {
      success: true,
      reconciliation_date: formatNow(),
      sales_ledger: {
        source: 'stran (Sales Ledger Transactions)',
        total_outstanding: r2(slTotal),
        transaction_count: slCount,
        breakdown_by_type: slByType,
        transfer_file: {
          source: 'snoml (Sales to Nominal Transfer File)',
          posted_to_nl: { count: slPostedCount, total: r2(slPostedTotal) },
          pending_transfer: {
            count: slPendingCount,
            total: r2(slPendingTotal),
            transactions: pendingTransactions,
          },
        },
        customer_master_check: {
          source: 'sname (Customer Master)',
          total: r2(snameTotal),
          customer_count: snameCount,
          matches_stran: Math.abs(slTotal - snameTotal) < 0.01,
          balance_variance: r2(slTotal - snameTotal),
          customers_with_balance_issues: customerBalanceIssues.slice(0, 20),
          total_customers_with_issues: customerBalanceIssues.length,
        },
      },
      nominal_ledger: {
        source: `ntran (Nominal Ledger - ${currentYear} only)`,
        control_accounts: nlDetails,
        total_balance: r2(nlTotal),
        current_year: currentYear,
      },
      variance: {
        amount: r2(variance),
        absolute: r2(varianceAbs),
        sales_ledger_total: r2(slTotal),
        transfer_file_posted: r2(slPostedTotal),
        transfer_file_pending: r2(slPendingTotal),
        nominal_ledger_total: r2(nlTotal),
        posted_variance: r2(variancePosted),
        posted_variance_abs: r2(variancePostedAbs),
        reconciled,
        has_pending_transfers: slPendingCount > 0,
      },
      status,
      message,
      details: [],
      control_account_used: debtorsControl,
      variance_analysis: adaptForDebtors(varianceAnalysis),
      aged_analysis: agedAnalysis,
      top_customers: topCustomers,
    };
  } catch (err: any) {
    return {
      success: false,
      error: err?.message ?? String(err),
      reconciliation_date: formatNow(),
      sales_ledger: {} as any,
      nominal_ledger: {} as any,
      variance: {} as any,
      status: 'UNRECONCILED',
      details: [],
      control_account_used: '',
    };
  }
}
