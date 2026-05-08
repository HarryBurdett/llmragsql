/**
 * Trial Balance check — faithful port of `reconcile_trial_balance()`
 * from `apps/balance_check/api/routes.py`.
 *
 * Verifies the nominal ledger as a whole balances (debits = credits).
 * Returns all nominal accounts with brought-forward, current-year
 * movements, and closing balances. Read-only against Opera SQL.
 */
import type { Knex } from 'knex';

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

const TYPE_NAMES: Record<string, string> = {
  A: 'Asset',
  L: 'Liability',
  E: 'Expense',
  I: 'Income',
  C: 'Capital',
  P: 'P&L',
  B: 'Balance Sheet',
};

export interface TrialBalanceAccount {
  account: string;
  description: string;
  type: string;
  type_name: string;
  bf_balance: number;
  current_debits: number;
  current_credits: number;
  current_net: number;
  closing_balance: number;
}

export interface TrialBalanceSummary {
  brought_forward: {
    debits: number;
    credits: number;
    variance: number;
    balanced: boolean;
  };
  current_year: {
    debits: number;
    credits: number;
    variance: number;
    balanced: boolean;
  };
  closing: {
    debits: number;
    credits: number;
    variance: number;
    balanced: boolean;
  };
  account_count: number;
}

export interface TrialBalanceResponse {
  success: boolean;
  reconciliation_date: string;
  summary?: TrialBalanceSummary;
  accounts: TrialBalanceAccount[];
  status: 'BALANCED' | 'UNBALANCED';
  message: string;
  current_year?: number;
  error?: string;
}

export async function reconcileTrialBalance(db: Knex): Promise<TrialBalanceResponse> {
  try {
    // Get current fiscal year
    const cyResult = (await db.raw(
      'SELECT MAX(nt_year) AS current_year FROM ntran WITH (NOLOCK)',
    )) as unknown as Array<{ current_year: number | null }>;
    const currentYear = Number(cyResult?.[0]?.current_year ?? new Date().getFullYear());

    // Get all nominal accounts with balances
    const accountsSql = `
      SELECT
        n.na_acnt AS account,
        RTRIM(n.na_desc) AS description,
        n.na_type AS type,
        n.na_prydr AS prior_debits,
        n.na_prycr AS prior_credits,
        n.na_ytddr AS ytd_debits,
        n.na_ytdcr AS ytd_credits,
        COALESCE(t.current_debits, 0) AS current_debits,
        COALESCE(t.current_credits, 0) AS current_credits,
        COALESCE(t.current_net, 0) AS current_net
      FROM nacnt n WITH (NOLOCK)
      LEFT JOIN (
        SELECT
          nt_acnt,
          SUM(CASE WHEN nt_value > 0 THEN nt_value ELSE 0 END) AS current_debits,
          SUM(CASE WHEN nt_value < 0 THEN ABS(nt_value) ELSE 0 END) AS current_credits,
          SUM(nt_value) AS current_net
        FROM ntran WITH (NOLOCK)
        WHERE nt_year = ?
        GROUP BY nt_acnt
      ) t ON n.na_acnt = t.nt_acnt
      WHERE n.na_ytddr <> 0 OR n.na_ytdcr <> 0 OR n.na_prydr <> 0 OR n.na_prycr <> 0
         OR COALESCE(t.current_debits, 0) <> 0 OR COALESCE(t.current_credits, 0) <> 0
      ORDER BY n.na_acnt
    `;
    const rows = (await db.raw(accountsSql, [currentYear])) as unknown as Array<{
      account: string | null;
      description: string | null;
      type: string | null;
      prior_debits: number | null;
      prior_credits: number | null;
      ytd_debits: number | null;
      ytd_credits: number | null;
      current_debits: number | null;
      current_credits: number | null;
      current_net: number | null;
    }>;

    let totalBfDebits = 0;
    let totalBfCredits = 0;
    let totalCurrentDebits = 0;
    let totalCurrentCredits = 0;
    let totalClosingDebits = 0;
    let totalClosingCredits = 0;

    const accounts: TrialBalanceAccount[] = [];
    for (const row of Array.isArray(rows) ? rows : []) {
      const priorDr = Number(row.prior_debits ?? 0);
      const priorCr = Number(row.prior_credits ?? 0);
      const currentDr = Number(row.current_debits ?? 0);
      const currentCr = Number(row.current_credits ?? 0);

      // B/F balance (prior year net)
      const bfBalance = priorDr - priorCr;
      const currentNet = currentDr - currentCr;
      const closingBalance = bfBalance + currentNet;

      if (bfBalance > 0) totalBfDebits += bfBalance;
      else totalBfCredits += Math.abs(bfBalance);

      totalCurrentDebits += currentDr;
      totalCurrentCredits += currentCr;

      if (closingBalance > 0) totalClosingDebits += closingBalance;
      else totalClosingCredits += Math.abs(closingBalance);

      const accountType = row.type ? String(row.type).trim() : '';

      accounts.push({
        account: row.account ? String(row.account).trim() : '',
        description: row.description ?? '',
        type: accountType,
        type_name: TYPE_NAMES[accountType] ?? accountType,
        bf_balance: r2(bfBalance),
        current_debits: r2(currentDr),
        current_credits: r2(currentCr),
        current_net: r2(currentNet),
        closing_balance: r2(closingBalance),
      });
    }

    // Variances (should be zero for a balanced trial balance)
    const bfVariance = Math.abs(totalBfDebits - totalBfCredits);
    const currentVariance = Math.abs(totalCurrentDebits - totalCurrentCredits);
    const closingVariance = Math.abs(totalClosingDebits - totalClosingCredits);

    const summary: TrialBalanceSummary = {
      brought_forward: {
        debits: r2(totalBfDebits),
        credits: r2(totalBfCredits),
        variance: r2(bfVariance),
        balanced: bfVariance < 0.005,
      },
      current_year: {
        debits: r2(totalCurrentDebits),
        credits: r2(totalCurrentCredits),
        variance: r2(currentVariance),
        balanced: currentVariance < 0.005,
      },
      closing: {
        debits: r2(totalClosingDebits),
        credits: r2(totalClosingCredits),
        variance: r2(closingVariance),
        balanced: closingVariance < 0.005,
      },
      account_count: accounts.length,
    };

    const allBalanced =
      bfVariance < 0.005 && currentVariance < 0.005 && closingVariance < 0.005;

    let status: 'BALANCED' | 'UNBALANCED';
    let message: string;
    if (allBalanced) {
      status = 'BALANCED';
      message = `Trial Balance is correct. ${accounts.length} accounts with matching debits and credits.`;
    } else {
      status = 'UNBALANCED';
      const variances: string[] = [];
      if (bfVariance >= 1.0) {
        variances.push(
          `B/F: £${bfVariance.toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
        );
      }
      if (currentVariance >= 1.0) {
        variances.push(
          `Current: £${currentVariance.toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
        );
      }
      if (closingVariance >= 1.0) {
        variances.push(
          `Closing: £${closingVariance.toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
        );
      }
      message = `Trial Balance has variance: ${variances.join(', ')}`;
    }

    return {
      success: true,
      reconciliation_date: formatNow(),
      summary,
      accounts,
      status,
      message,
      current_year: currentYear,
    };
  } catch (err: any) {
    return {
      success: false,
      reconciliation_date: formatNow(),
      accounts: [],
      status: 'UNBALANCED',
      message: '',
      error: err?.message ?? String(err),
    };
  }
}
