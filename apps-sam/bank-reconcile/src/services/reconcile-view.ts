/**
 * Bank reconcile view — port of `reconcile_bank`
 * (apps/bank_reconcile/api/routes.py:320-705).
 *
 * Pulls the data needed to display the reconciliation page for one
 * Opera bank. Compares three independent sources to detect drift:
 *
 *   1. Cashbook (atran current-year movements + nacnt prior-year B/F)
 *   2. Bank master (nbank.nk_curbal in pence)
 *   3. Nominal Ledger (ntran current-year net signed)
 *
 * Plus the transfer-file backlog (anoml ax_done <> 'Y') so operators
 * can see what's still queued for NL posting.
 *
 * Read-only — purely a query against Opera. Uses NOLOCK on every read
 * (mandatory per CLAUDE.md to avoid blocking concurrent writers).
 */
import type { Knex } from 'knex';
import { validateBankCode, SqlInputValidationError } from '@sqlrag/sam-shared';

export interface ReconcileBankView {
  success: boolean;
  reconciliation_date: string;
  bank_code: string;
  bank_account: {
    code: string;
    description: string;
    sort_code: string;
    account_number: string;
  };
  cashbook: {
    source: string;
    current_year: number;
    current_year_entries: number;
    current_year_transactions: number;
    current_year_receipts: number;
    current_year_payments: number;
    current_year_movements: number;
    prior_year_bf: number;
    expected_closing: number;
    all_time_entries: number;
    all_time_net: number;
    transfer_file: {
      source: string;
      posted_to_nl: { count: number; total: number };
      pending_transfer: {
        count: number;
        total: number;
        transactions: Array<{
          nominal_account: string;
          source: string;
          source_desc: string;
          date: string;
          value: number;
          reference: string;
          comment: string;
        }>;
      };
    };
  };
  bank_master: {
    source: string;
    balance_pence: number;
    balance_pounds: number;
  };
  nominal_ledger: {
    source: string;
    account: string;
    description: string;
    current_year?: number;
    brought_forward?: number;
    current_year_debits?: number;
    current_year_credits?: number;
    current_year_net?: number;
    closing_balance?: number;
    total_balance: number;
  };
  variance: {
    cashbook_vs_bank_master: VarianceRow;
    bank_master_vs_nominal: VarianceRow;
    cashbook_vs_nominal: VarianceRow;
    summary: {
      current_year: number;
      cashbook_movements: number;
      prior_year_bf: number;
      cashbook_expected_closing: number;
      bank_master_balance: number;
      nominal_ledger_balance: number;
      transfer_file_pending: number;
      all_reconciled: boolean;
      has_pending_transfers: boolean;
    };
  };
  status: 'RECONCILED' | 'UNRECONCILED';
  message: string;
  details: unknown[];
  error?: string;
}

interface VarianceRow {
  description: string;
  amount: number;
  absolute: number;
  reconciled: boolean;
  cashbook_expected?: number;
  bank_master?: number;
  nominal_ledger?: number;
}

const SOURCE_DESC: Record<string, string> = {
  P: 'Purchase',
  S: 'Sales',
  A: 'Cashbook',
  J: 'Journal',
};

function nowStr(): string {
  return new Date().toISOString().replace('T', ' ').slice(0, 19);
}

function num(v: unknown): number {
  if (v === null || v === undefined) return 0;
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function int(v: unknown): number {
  return Math.trunc(num(v));
}

function rstrip(s: unknown): string {
  return typeof s === 'string' ? s.trim() : '';
}

function r2(v: number): number {
  return Math.round(v * 100) / 100;
}

export async function getReconcileBankView(
  operaDb: Knex,
  bankCode: string,
): Promise<ReconcileBankView | { success: false; error: string }> {
  // SQL-injection guard — the 14 raw queries below all bind via ? but
  // we still validate the boundary as defence in depth.
  let code: string;
  try {
    code = validateBankCode(bankCode);
  } catch (e) {
    if (e instanceof SqlInputValidationError) {
      return { success: false, error: e.message };
    }
    throw e;
  }

  try {
    // -- Bank master record
    const bankRows = (await operaDb.raw(
      `SELECT TOP 1
          RTRIM(nk_acnt) AS code,
          RTRIM(ISNULL(nk_desc, '')) AS description,
          RTRIM(ISNULL(nk_sort, '')) AS sort_code,
          RTRIM(ISNULL(nk_number, '')) AS account_number,
          ISNULL(nk_curbal, 0) AS balance_pence
         FROM nbank WITH (NOLOCK)
         WHERE RTRIM(nk_acnt) = ?`,
      [code],
    )) as Array<{
      code: string;
      description: string;
      sort_code: string;
      account_number: string;
      balance_pence: number;
    }>;
    if (!Array.isArray(bankRows) || bankRows.length === 0) {
      return { success: false, error: `Bank account ${code} not found` };
    }
    const bank = bankRows[0]!;

    // -- Current-year discriminator
    const cyRows = (await operaDb.raw(
      `SELECT MAX(nt_year) AS current_year FROM ntran WITH (NOLOCK)`,
    )) as Array<{ current_year: number | null }>;
    const currentYear =
      Array.isArray(cyRows) && cyRows.length > 0 && cyRows[0]?.current_year
        ? Number(cyRows[0].current_year)
        : new Date().getFullYear();

    // -- Cashbook current-year movements (atran in pence)
    const cbCyRows = (await operaDb.raw(
      `SELECT
          COUNT(DISTINCT at_entry) AS entry_count,
          COUNT(*) AS txn_count,
          SUM(CASE WHEN at_value > 0 THEN at_value ELSE 0 END) AS receipts_pence,
          SUM(CASE WHEN at_value < 0 THEN ABS(at_value) ELSE 0 END) AS payments_pence,
          SUM(at_value) AS net_pence
         FROM atran WITH (NOLOCK)
         WHERE at_acnt = ?
           AND YEAR(at_pstdate) = ?`,
      [code, currentYear],
    )) as Array<{
      entry_count: number | null;
      txn_count: number | null;
      receipts_pence: number | null;
      payments_pence: number | null;
      net_pence: number | null;
    }>;
    const cbCy = cbCyRows[0] ?? {
      entry_count: 0,
      txn_count: 0,
      receipts_pence: 0,
      payments_pence: 0,
      net_pence: 0,
    };
    const cbCyReceipts = num(cbCy.receipts_pence) / 100;
    const cbCyPayments = num(cbCy.payments_pence) / 100;
    const cbCyMovements = num(cbCy.net_pence) / 100;

    // -- All-time totals
    const cbAllRows = (await operaDb.raw(
      `SELECT
          COUNT(DISTINCT at_entry) AS entry_count,
          SUM(at_value) AS net_pence
         FROM atran WITH (NOLOCK)
         WHERE at_acnt = ?`,
      [code],
    )) as Array<{ entry_count: number | null; net_pence: number | null }>;
    const cbAllCount = int(cbAllRows[0]?.entry_count);
    const cbAllNet = num(cbAllRows[0]?.net_pence) / 100;

    // -- Bank master balance
    const nbankCurbalPence = num(bank.balance_pence);
    const nbankCurbalPounds = nbankCurbalPence / 100;

    // -- Nominal ledger account details
    const nacntRows = (await operaDb.raw(
      `SELECT
          RTRIM(na_acnt) AS account,
          RTRIM(ISNULL(na_desc, '')) AS description,
          ISNULL(na_ytddr, 0) AS na_ytddr,
          ISNULL(na_ytdcr, 0) AS na_ytdcr,
          ISNULL(na_prydr, 0) AS na_prydr,
          ISNULL(na_prycr, 0) AS na_prycr
         FROM nacnt WITH (NOLOCK)
         WHERE na_acnt = ?`,
      [code],
    )) as Array<{
      description: string | null;
      na_prydr: number;
      na_prycr: number;
    }>;
    let bfBalance = 0;
    let nlTotal = 0;
    let nlDetails: ReconcileBankView['nominal_ledger'];
    if (Array.isArray(nacntRows) && nacntRows.length > 0) {
      const acc = nacntRows[0]!;
      const pryDr = num(acc.na_prydr);
      const pryCr = num(acc.na_prycr);
      bfBalance = pryDr - pryCr;

      const ntranRows = (await operaDb.raw(
        `SELECT
            SUM(CASE WHEN nt_value > 0 THEN nt_value ELSE 0 END) AS debits,
            SUM(CASE WHEN nt_value < 0 THEN ABS(nt_value) ELSE 0 END) AS credits,
            SUM(nt_value) AS net
           FROM ntran WITH (NOLOCK)
           WHERE nt_acnt = ? AND nt_year = ?`,
        [code, currentYear],
      )) as Array<{
        debits: number | null;
        credits: number | null;
        net: number | null;
      }>;
      const cyDr = num(ntranRows[0]?.debits);
      const cyCr = num(ntranRows[0]?.credits);
      const cyNet = num(ntranRows[0]?.net);

      nlTotal = cyNet;
      nlDetails = {
        source: 'ntran (Nominal Ledger)',
        account: code,
        description: rstrip(acc.description),
        current_year: currentYear,
        brought_forward: r2(bfBalance),
        current_year_debits: r2(cyDr),
        current_year_credits: r2(cyCr),
        current_year_net: r2(cyNet),
        closing_balance: r2(cyNet),
        total_balance: r2(nlTotal),
      };
    } else {
      nlDetails = {
        source: 'ntran (Nominal Ledger)',
        account: code,
        description: 'Account not found in nacnt',
        total_balance: 0,
      };
    }

    const cbExpectedClosing = cbCyMovements + bfBalance;

    // -- anoml summary (posted vs pending)
    const anomlSummary = (await operaDb.raw(
      `SELECT
          CASE WHEN ax_done = 'Y' THEN 'Posted' ELSE 'Pending' END AS status,
          COUNT(*) AS cnt,
          SUM(ax_value) AS total
         FROM anoml WITH (NOLOCK)
         WHERE ax_nacnt = ?
         GROUP BY CASE WHEN ax_done = 'Y' THEN 'Posted' ELSE 'Pending' END`,
      [code],
    )) as Array<{ status: string; cnt: number; total: number | null }>;
    let postedCount = 0;
    let postedTotal = 0;
    let pendingCount = 0;
    let pendingTotal = 0;
    for (const row of anomlSummary ?? []) {
      if (row.status === 'Posted') {
        postedCount = int(row.cnt);
        postedTotal = num(row.total);
      } else {
        pendingCount = int(row.cnt);
        pendingTotal = num(row.total);
      }
    }

    // -- anoml pending detail
    const anomlPending = (await operaDb.raw(
      `SELECT
          RTRIM(ISNULL(ax_nacnt, '')) AS nominal_account,
          RTRIM(ISNULL(ax_source, '')) AS source,
          ax_date AS date,
          ax_value AS value,
          RTRIM(ISNULL(ax_tref, '')) AS reference,
          RTRIM(ISNULL(ax_comment, '')) AS comment
         FROM anoml WITH (NOLOCK)
         WHERE ax_nacnt = ? AND (ax_done <> 'Y' OR ax_done IS NULL)
         ORDER BY ax_date DESC`,
      [code],
    )) as Array<{
      nominal_account: string;
      source: string;
      date: string | Date | null;
      value: number;
      reference: string;
      comment: string;
    }>;
    const pendingTransactions = (anomlPending ?? []).map((r) => {
      const dateRaw = r.date;
      const date =
        dateRaw instanceof Date
          ? dateRaw.toISOString().slice(0, 10)
          : typeof dateRaw === 'string'
            ? dateRaw.slice(0, 10)
            : '';
      const src = rstrip(r.source);
      return {
        nominal_account: rstrip(r.nominal_account),
        source: src,
        source_desc: SOURCE_DESC[src] ?? src,
        date,
        value: r2(num(r.value)),
        reference: rstrip(r.reference),
        comment: rstrip(r.comment),
      };
    });

    // -- Variance maths (matches Python exactly)
    const varianceCbNbank = cbExpectedClosing - nbankCurbalPounds;
    const varianceCbNbankAbs = Math.abs(varianceCbNbank);
    const varianceNbankNl = nbankCurbalPounds - nlTotal;
    const varianceNbankNlAbs = Math.abs(varianceNbankNl);
    const varianceCbNl = cbExpectedClosing - nlTotal;
    const varianceCbNlAbs = Math.abs(varianceCbNl);
    const allReconciled = varianceCbNbankAbs < 0.005 && varianceNbankNlAbs < 0.005;

    let status: 'RECONCILED' | 'UNRECONCILED';
    let message: string;
    if (allReconciled) {
      status = 'RECONCILED';
      message =
        pendingCount > 0
          ? `Bank ${code} reconciles across all sources. ${pendingCount} entries (£${Math.abs(pendingTotal).toFixed(2)}) in transfer file pending.`
          : `Bank ${code} fully reconciles: Cashbook = Bank Master = Nominal Ledger`;
    } else {
      status = 'UNRECONCILED';
      const issues: string[] = [];
      if (varianceCbNlAbs >= 0.005) {
        issues.push(
          varianceCbNl > 0
            ? `Cashbook £${varianceCbNlAbs.toFixed(2)} MORE than NL`
            : `Cashbook £${varianceCbNlAbs.toFixed(2)} LESS than NL`,
        );
      }
      if (varianceCbNbankAbs >= 0.005) {
        issues.push(
          varianceCbNbank > 0
            ? `Cashbook £${varianceCbNbankAbs.toFixed(2)} MORE than Bank Master`
            : `Cashbook £${varianceCbNbankAbs.toFixed(2)} LESS than Bank Master`,
        );
      }
      if (varianceNbankNlAbs >= 0.005) {
        issues.push(
          varianceNbankNl > 0
            ? `Bank Master £${varianceNbankNlAbs.toFixed(2)} MORE than NL`
            : `Bank Master £${varianceNbankNlAbs.toFixed(2)} LESS than NL`,
        );
      }
      message = issues.length ? issues.join('; ') : 'Variance detected';
    }

    return {
      success: true,
      reconciliation_date: nowStr(),
      bank_code: code,
      bank_account: {
        code: rstrip(bank.code),
        description: rstrip(bank.description),
        sort_code: rstrip(bank.sort_code),
        account_number: rstrip(bank.account_number),
      },
      cashbook: {
        source: 'atran (Cashbook Transactions)',
        current_year: currentYear,
        current_year_entries: int(cbCy.entry_count),
        current_year_transactions: int(cbCy.txn_count),
        current_year_receipts: r2(cbCyReceipts),
        current_year_payments: r2(cbCyPayments),
        current_year_movements: r2(cbCyMovements),
        prior_year_bf: r2(bfBalance),
        expected_closing: r2(cbExpectedClosing),
        all_time_entries: cbAllCount,
        all_time_net: r2(cbAllNet),
        transfer_file: {
          source: 'anoml (Cashbook to Nominal Transfer File)',
          posted_to_nl: { count: postedCount, total: r2(postedTotal) },
          pending_transfer: {
            count: pendingCount,
            total: r2(pendingTotal),
            transactions: pendingTransactions,
          },
        },
      },
      bank_master: {
        source: 'nbank.nk_curbal (Bank Master Balance)',
        balance_pence: Math.round(nbankCurbalPence),
        balance_pounds: r2(nbankCurbalPounds),
      },
      nominal_ledger: nlDetails,
      variance: {
        cashbook_vs_bank_master: {
          description: 'atran movements + B/F vs nbank.nk_curbal',
          cashbook_expected: r2(cbExpectedClosing),
          bank_master: r2(nbankCurbalPounds),
          amount: r2(varianceCbNbank),
          absolute: r2(varianceCbNbankAbs),
          reconciled: varianceCbNbankAbs < 0.005,
        },
        bank_master_vs_nominal: {
          description: 'nbank.nk_curbal vs ntran current year',
          bank_master: r2(nbankCurbalPounds),
          nominal_ledger: r2(nlTotal),
          amount: r2(varianceNbankNl),
          absolute: r2(varianceNbankNlAbs),
          reconciled: varianceNbankNlAbs < 0.005,
        },
        cashbook_vs_nominal: {
          description: 'atran expected vs ntran',
          cashbook_expected: r2(cbExpectedClosing),
          nominal_ledger: r2(nlTotal),
          amount: r2(varianceCbNl),
          absolute: r2(varianceCbNlAbs),
          reconciled: varianceCbNlAbs < 0.005,
        },
        summary: {
          current_year: currentYear,
          cashbook_movements: r2(cbCyMovements),
          prior_year_bf: r2(bfBalance),
          cashbook_expected_closing: r2(cbExpectedClosing),
          bank_master_balance: r2(nbankCurbalPounds),
          nominal_ledger_balance: r2(nlTotal),
          transfer_file_pending: r2(pendingTotal),
          all_reconciled: allReconciled,
          has_pending_transfers: pendingCount > 0,
        },
      },
      status,
      message,
      details: [],
    };
  } catch (e: any) {
    return { success: false, error: e?.message ?? String(e) };
  }
}
