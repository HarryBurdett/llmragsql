/**
 * Creditors reconciliation — faithful port of `reconcile_creditors()`
 * from `apps/balance_check/api/routes.py`.
 *
 * Reconciles Purchase Ledger (ptran) to Creditors Control Account (NL).
 * Compares outstanding balances in ptran with the control account in
 * nacnt/ntran.
 *
 * Read-only against Opera SQL.
 */
import type { Knex } from 'knex';
import { getControlAccounts } from '@sqlrag/sam-shared';
import {
  CREDITORS,
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

export interface TopSupplier {
  account: string;
  name: string;
  invoice_count: number;
  outstanding: number;
}

export interface ReconcileCreditorsResponse {
  success: boolean;
  reconciliation_date: string;
  purchase_ledger: {
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
    supplier_master_check: {
      source: string;
      total: number;
      supplier_count: number;
      matches_ptran: boolean;
      balance_variance: number;
      suppliers_with_balance_issues: MasterTxnVarianceRow[];
      total_suppliers_with_issues: number;
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
    purchase_ledger_total: number;
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
  variance_analysis?: VarianceAnalysisResult;
  aged_analysis?: AgedAnalysisRow[];
  top_suppliers?: TopSupplier[];
  error?: string;
}

export async function reconcileCreditors(
  db: Knex,
): Promise<ReconcileCreditorsResponse> {
  try {
    const controlAccounts = await getControlAccounts(db);
    const creditorsControl = controlAccounts.creditorsControl;

    // ========== PURCHASE LEDGER (ptran) — phases extracted to helpers ==========
    const outstanding = await fetchOutstanding(db, CREDITORS);
    const plTotal = outstanding.total_outstanding;
    const plCount = outstanding.transaction_count;

    const typeNames: Record<string, string> = {
      I: 'Invoices',
      C: 'Credit Notes',
      P: 'Payments',
      B: 'Brought Forward',
    };
    const plByType = await fetchBreakdownByType(db, CREDITORS, typeNames);

    const master = await fetchMasterTotals(db, CREDITORS);
    const pnameTotal = master.total_balance;
    const pnameCount = master.count;

    const supplierBalanceIssues = await fetchMasterTxnVariance(db, CREDITORS);

    // ========== TRANSFER FILE (pnoml) — phases extracted to helpers ==========
    const pendingTransactions = await fetchTransferFilePending(db, CREDITORS);
    const transferSummary = await fetchTransferFileSummary(db, CREDITORS);
    const postedCount = transferSummary.posted.count;
    const postedTotal = transferSummary.posted.total;
    const pendingCount = transferSummary.pending.count;
    const pendingTotal = transferSummary.pending.total;

    // ========== NOMINAL LEDGER (nacnt/ntran) ==========
    const nlResult = await fetchControlAccountDetails(
      db,
      creditorsControl,
      '%Creditor%Control%',
      '%Trade%Creditor%',
      true, // negate ntran for comparison with ptran (creditors convention)
    );
    const nlTotal = nlResult.nlTotal;
    const currentYear = nlResult.currentYear;
    const nlDetails = nlResult.details;

    // ========== VARIANCE CALCULATION ==========
    const variance = plTotal - nlTotal;
    const varianceAbs = Math.abs(variance);
    const variancePosted = postedTotal - nlTotal;
    const variancePostedAbs = Math.abs(variancePosted);

    // ========== VARIANCE ANALYSIS ==========
    // Creditors: filter NL transactions by current year (matches Python).
    const varianceAnalysis = await analyseVariance(db, {
      side: 'creditors',
      controlAccountCodes: nlResult.controlAccountCodes,
      currentYear,
      varianceAmount: variance,
      filterNlByCurrentYear: true,
      alwaysRun: false,
    });

    // ========== AGED ANALYSIS ==========
    const agedSql = `
      SELECT
        CASE
          WHEN DATEDIFF(day, pt_trdate, GETDATE()) <= 30 THEN 'Current (0-30 days)'
          WHEN DATEDIFF(day, pt_trdate, GETDATE()) <= 60 THEN '31-60 days'
          WHEN DATEDIFF(day, pt_trdate, GETDATE()) <= 90 THEN '61-90 days'
          ELSE 'Over 90 days'
        END AS age_band,
        COUNT(*) AS count,
        SUM(pt_trbal) AS total
      FROM ptran
      WHERE pt_trbal <> 0
        AND RTRIM(pt_account) IN (SELECT RTRIM(pn_account) FROM pname)
      GROUP BY CASE
        WHEN DATEDIFF(day, pt_trdate, GETDATE()) <= 30 THEN 'Current (0-30 days)'
        WHEN DATEDIFF(day, pt_trdate, GETDATE()) <= 60 THEN '31-60 days'
        WHEN DATEDIFF(day, pt_trdate, GETDATE()) <= 90 THEN '61-90 days'
        ELSE 'Over 90 days'
      END
      ORDER BY MIN(DATEDIFF(day, pt_trdate, GETDATE()))
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

    // ========== TOP SUPPLIERS ==========
    const topSuppliersSql = `
      SELECT TOP 10
        RTRIM(p.pn_account) AS account,
        RTRIM(p.pn_name) AS supplier_name,
        COUNT(*) AS invoice_count,
        SUM(pt.pt_trbal) AS outstanding
      FROM ptran WITH (NOLOCK) pt
      JOIN pname WITH (NOLOCK) p ON pt.pt_account = p.pn_account
      WHERE pt.pt_trbal <> 0
      GROUP BY p.pn_account, p.pn_name
      ORDER BY SUM(pt.pt_trbal) DESC
    `;
    let topRows: Array<{
      account: string | null;
      supplier_name: string | null;
      invoice_count: number | null;
      outstanding: number | null;
    }> = [];
    try {
      const result = (await db.raw(topSuppliersSql)) as unknown as typeof topRows;
      topRows = Array.isArray(result) ? result : [];
    } catch {
      topRows = [];
    }
    const topSuppliers: TopSupplier[] = topRows.map((row) => ({
      account: row.account ?? '',
      name: row.supplier_name ?? '',
      invoice_count: Number(row.invoice_count ?? 0),
      outstanding: r2(Number(row.outstanding ?? 0)),
    }));

    const reconciled = varianceAbs < 0.005;
    const status: 'RECONCILED' | 'UNRECONCILED' = reconciled ? 'RECONCILED' : 'UNRECONCILED';
    let message: string;
    if (reconciled) {
      if (pendingCount > 0) {
        message =
          `Purchase Ledger reconciles to Nominal Ledger. ${pendingCount} transactions ` +
          `(£${Math.abs(pendingTotal).toLocaleString('en-GB', {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
          })}) in transfer file pending.`;
      } else {
        message = 'Purchase Ledger reconciles to Nominal Ledger Creditors Control';
      }
    } else if (variance > 0) {
      message = `Purchase Ledger is £${varianceAbs.toLocaleString('en-GB', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      })} MORE than Nominal Ledger Control`;
    } else {
      message = `Purchase Ledger is £${varianceAbs.toLocaleString('en-GB', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      })} LESS than Nominal Ledger Control`;
    }

    return {
      success: true,
      reconciliation_date: formatNow(),
      purchase_ledger: {
        source: 'ptran (Purchase Ledger Transactions)',
        total_outstanding: r2(plTotal),
        transaction_count: plCount,
        breakdown_by_type: plByType,
        transfer_file: {
          source: 'pnoml (Purchase to Nominal Transfer File)',
          posted_to_nl: { count: postedCount, total: r2(postedTotal) },
          pending_transfer: {
            count: pendingCount,
            total: r2(pendingTotal),
            transactions: pendingTransactions,
          },
        },
        supplier_master_check: {
          source: 'pname (Supplier Master)',
          total: r2(pnameTotal),
          supplier_count: pnameCount,
          matches_ptran: Math.abs(plTotal - pnameTotal) < 0.01,
          balance_variance: r2(plTotal - pnameTotal),
          suppliers_with_balance_issues: supplierBalanceIssues.slice(0, 20),
          total_suppliers_with_issues: supplierBalanceIssues.length,
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
        purchase_ledger_total: r2(plTotal),
        transfer_file_posted: r2(postedTotal),
        transfer_file_pending: r2(pendingTotal),
        nominal_ledger_total: r2(nlTotal),
        posted_variance: r2(variancePosted),
        posted_variance_abs: r2(variancePostedAbs),
        reconciled,
        has_pending_transfers: pendingCount > 0,
      },
      status,
      message,
      details: [],
      control_account_used: creditorsControl,
      variance_analysis: varianceAnalysis,
      aged_analysis: agedAnalysis,
      top_suppliers: topSuppliers,
    };
  } catch (err: any) {
    return {
      success: false,
      error: err?.message ?? String(err),
      reconciliation_date: formatNow(),
      purchase_ledger: {} as any,
      nominal_ledger: {} as any,
      variance: {} as any,
      status: 'UNRECONCILED',
      details: [],
      control_account_used: '',
    };
  }
}
