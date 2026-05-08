/**
 * Helpers for fetching nominal-ledger control-account details + transactions.
 *
 * Faithful port of the inline logic from `apps/balance_check/api/routes.py`
 * inside `reconcile_creditors` and `reconcile_debtors` — extracted here
 * because both endpoints use identical NL-side logic, only differing by
 * sign convention (creditors negate ntran totals, debtors don't).
 */
import type { Knex } from 'knex';

function r2(n: number): number {
  return Math.round(n * 100) / 100;
}

export interface NtranByYearRow {
  year: number;
  debits: number;
  credits: number;
  net: number;
}

export interface ControlAccountDetail {
  account: string;
  description: string;
  brought_forward: number;
  current_year: number;
  current_year_debits: number;
  current_year_credits: number;
  current_year_net: number;
  closing_balance: number;
  ntran_by_year: NtranByYearRow[];
}

export interface ControlAccountDetailsResult {
  details: ControlAccountDetail[];
  /** Total balance across all control accounts, applying the side's sign convention. */
  nlTotal: number;
  currentYear: number;
  controlAccountCodes: string[];
}

/**
 * Fetch control-account details with ntran year breakdown and balance.
 *
 * @param db - Knex pool against the per-company Opera SE database
 * @param controlAccountCode - The configured control account code (e.g. "2100" for creditors)
 * @param descriptionLike - The LIKE pattern for finding additional matching accounts (e.g. "%Creditor%Control%")
 * @param descriptionLike2 - A second LIKE pattern (e.g. "%Trade%Creditor%")
 * @param negateForBalance - If true, the balance is negated for comparison with the sub-ledger
 *                           (creditors: yes, debtors: no)
 */
export async function fetchControlAccountDetails(
  db: Knex,
  controlAccountCode: string,
  descriptionLike: string,
  descriptionLike2: string,
  negateForBalance: boolean,
): Promise<ControlAccountDetailsResult> {
  // Find control accounts — match on description patterns OR exact code
  const controlAccountSql = `
    SELECT na_acnt, na_desc, na_ytddr, na_ytdcr, na_prydr, na_prycr
    FROM nacnt WITH (NOLOCK)
    WHERE na_desc LIKE ?
       OR na_desc LIKE ?
       OR na_acnt = ?
    ORDER BY na_acnt
  `;
  const controlRows = (await db.raw(controlAccountSql, [
    descriptionLike,
    descriptionLike2,
    controlAccountCode,
  ])) as unknown as Array<{
    na_acnt: string | null;
    na_desc: string | null;
    na_ytddr: number | null;
    na_ytdcr: number | null;
    na_prydr: number | null;
    na_prycr: number | null;
  }>;

  // Get current fiscal year from ntran
  const cyResult = (await db.raw(
    'SELECT MAX(nt_year) AS current_year FROM ntran WITH (NOLOCK)',
  )) as unknown as Array<{ current_year: number | null }>;
  const currentYear = Number(cyResult?.[0]?.current_year ?? new Date().getFullYear());

  let nlTotal = 0;
  const details: ControlAccountDetail[] = [];

  for (const acc of Array.isArray(controlRows) ? controlRows : []) {
    const acnt = acc.na_acnt ? String(acc.na_acnt).trim() : '';

    const pryDr = Number(acc.na_prydr ?? 0);
    const pryCr = Number(acc.na_prycr ?? 0);

    // Prior year B/F follows the same sign convention as the side:
    //   creditors (negateForBalance=true): pry_cr - pry_dr (credit balance)
    //   debtors (negateForBalance=false): pry_dr - pry_cr (debit balance)
    // Matches the Python implementation in reconcile_creditors / reconcile_debtors.
    const bfBalance = negateForBalance ? pryCr - pryDr : pryDr - pryCr;

    // Get current year transactions from ntran
    const ntranCurrentSql = `
      SELECT
        SUM(CASE WHEN nt_value > 0 THEN nt_value ELSE 0 END) AS debits,
        SUM(CASE WHEN nt_value < 0 THEN ABS(nt_value) ELSE 0 END) AS credits,
        SUM(nt_value) AS net
      FROM ntran WITH (NOLOCK)
      WHERE nt_acnt = ? AND nt_year = ?
    `;
    const currentRows = (await db.raw(ntranCurrentSql, [acnt, currentYear])) as unknown as Array<{
      debits: number | null;
      credits: number | null;
      net: number | null;
    }>;
    const cyDr = Number(currentRows?.[0]?.debits ?? 0);
    const cyCr = Number(currentRows?.[0]?.credits ?? 0);
    const cyNet = Number(currentRows?.[0]?.net ?? 0);

    const currentYearBalance = negateForBalance ? -cyNet : cyNet;

    // Get all years for reference
    const ntranSql = `
      SELECT
        nt_year,
        SUM(CASE WHEN nt_value > 0 THEN nt_value ELSE 0 END) AS debits,
        SUM(CASE WHEN nt_value < 0 THEN ABS(nt_value) ELSE 0 END) AS credits,
        SUM(nt_value) AS net
      FROM ntran WITH (NOLOCK)
      WHERE nt_acnt = ?
      GROUP BY nt_year
      ORDER BY nt_year
    `;
    const yearRows = (await db.raw(ntranSql, [acnt])) as unknown as Array<{
      nt_year: number | null;
      debits: number | null;
      credits: number | null;
      net: number | null;
    }>;
    const ntranByYear: NtranByYearRow[] = (Array.isArray(yearRows) ? yearRows : []).map((r) => ({
      year: Number(r.nt_year ?? 0),
      debits: r2(Number(r.debits ?? 0)),
      credits: r2(Number(r.credits ?? 0)),
      net: r2(Number(r.net ?? 0)),
    }));

    details.push({
      account: acnt,
      description: acc.na_desc ? String(acc.na_desc).trim() : '',
      brought_forward: r2(bfBalance),
      current_year: currentYear,
      current_year_debits: r2(cyDr),
      current_year_credits: r2(cyCr),
      current_year_net: r2(cyNet),
      closing_balance: r2(currentYearBalance),
      ntran_by_year: ntranByYear,
    });

    nlTotal += currentYearBalance;
  }

  const controlAccountCodes = details.length > 0
    ? details.map((d) => d.account)
    : [controlAccountCode];

  return {
    details,
    nlTotal,
    currentYear,
    controlAccountCodes,
  };
}
