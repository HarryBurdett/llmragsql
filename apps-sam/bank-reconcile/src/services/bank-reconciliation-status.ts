/**
 * Bank reconciliation status + history queries.
 *
 * Faithful ports of:
 *   - get_bank_reconciliation_status (routes.py:711)
 *   - get_unreconciled_entries_for_bank (routes.py:10921)
 *   - get_statement_transactions (routes.py:10800)
 *
 * Used by the bank-reconciliation UI to render "where are we?" state
 * across all bank accounts and to display previously imported
 * statement transactions for review.
 */
import type { Knex } from 'knex';

export interface BankReconciliationStatusEntry {
  bank_code: string;
  description: string;
  reconciled_balance: number | null;
  current_balance: number | null;
  unreconciled_count: number;
  unreconciled_total: number;
  last_reconciled: string | null;
}

export async function getBankReconciliationStatus(
  operaDb: Knex,
): Promise<{ success: boolean; banks: BankReconciliationStatusEntry[]; error?: string }> {
  try {
    const rows = (await operaDb.raw(
      `SELECT
         RTRIM(nk_acnt) AS bank_code,
         RTRIM(nk_desc) AS description,
         nk_recbal / 100.0 AS reconciled_balance,
         nk_curbal / 100.0 AS current_balance
       FROM nbank WITH (NOLOCK)
       ORDER BY nk_acnt`,
    )) as unknown as Array<{
      bank_code: string;
      description: string;
      reconciled_balance: number | null;
      current_balance: number | null;
    }>;

    const banks: BankReconciliationStatusEntry[] = [];
    for (const r of rows) {
      const code = (r.bank_code ?? '').toString().trim();
      const unrecRows = (await operaDb.raw(
        `SELECT COUNT(*) AS cnt, SUM(at_value) / 100.0 AS total
         FROM atran WITH (NOLOCK)
         WHERE at_acnt = ? AND (at_statln IS NULL OR at_statln = 0)`,
        [code],
      )) as unknown as Array<{ cnt: number | null; total: number | null }>;
      banks.push({
        bank_code: code,
        description: (r.description ?? '').toString().trim(),
        reconciled_balance: r.reconciled_balance,
        current_balance: r.current_balance,
        unreconciled_count: Number(unrecRows[0]?.cnt ?? 0),
        unreconciled_total: Number(unrecRows[0]?.total ?? 0),
        last_reconciled: null,
      });
    }
    return { success: true, banks };
  } catch (err: any) {
    return { success: false, banks: [], error: err?.message ?? String(err) };
  }
}

export interface UnreconciledEntry {
  bank_code: string;
  date: string;
  reference: string;
  amount: number;
  comment: string;
  entry_number: string;
}

export async function getUnreconciledEntriesForBank(
  operaDb: Knex,
  bankCode: string | null,
): Promise<{ success: boolean; entries: UnreconciledEntry[]; error?: string }> {
  try {
    let query = `
      SELECT
        RTRIM(at_acnt) AS bank_code,
        at_pstdate AS date,
        RTRIM(ae_entref) AS reference,
        at_value / 100.0 AS amount,
        RTRIM(at_comment) AS comment,
        RTRIM(at_entry) AS entry_number
      FROM atran WITH (NOLOCK)
      JOIN aentry WITH (NOLOCK) ON at_acnt = ae_acnt AND at_cntr = ae_cntr
        AND at_cbtype = ae_cbtype AND at_entry = ae_entry
      WHERE (at_statln IS NULL OR at_statln = 0)
    `;
    const params: (string | number)[] = [];
    if (bankCode) {
      query += ' AND at_acnt = ?';
      params.push(bankCode);
    }
    query += ' ORDER BY at_pstdate DESC';
    const rows = (await operaDb.raw(query, params)) as unknown as Array<{
      bank_code: string;
      date: Date | string | null;
      reference: string | null;
      amount: number | null;
      comment: string | null;
      entry_number: string | null;
    }>;
    return {
      success: true,
      entries: rows.map((r) => ({
        bank_code: (r.bank_code ?? '').toString().trim(),
        date:
          r.date instanceof Date
            ? r.date.toISOString().slice(0, 10)
            : String(r.date ?? '').slice(0, 10),
        reference: (r.reference ?? '').toString().trim(),
        amount: Number(r.amount ?? 0),
        comment: (r.comment ?? '').toString().trim(),
        entry_number: (r.entry_number ?? '').toString().trim(),
      })),
    };
  } catch (err: any) {
    return { success: false, entries: [], error: err?.message ?? String(err) };
  }
}

export interface StatementTransaction {
  line_number: number;
  date: string | null;
  name: string | null;
  memo: string | null;
  amount: number;
  type: string;
  matched_account: string | null;
  matched_name: string | null;
  reconciled: boolean;
}

export async function getStatementTransactionsForImport(
  appDb: Knex,
  importId: number,
): Promise<{
  success: boolean;
  transactions: StatementTransaction[];
  error?: string;
}> {
  if (!Number.isFinite(importId) || importId <= 0) {
    return { success: false, transactions: [], error: 'invalid import_id' };
  }
  try {
    const rows = (await appDb('bank_statement_transactions')
      .where({ import_id: importId })
      .orderBy('line_number', 'asc')) as unknown as Array<{
      line_number: number;
      transaction_date: string | Date | null;
      name: string | null;
      memo: string | null;
      amount: number | string | null;
      transaction_type: string | null;
      matched_account: string | null;
      matched_name: string | null;
      reconciled: number | boolean | null;
    }>;
    return {
      success: true,
      transactions: rows.map((r) => ({
        line_number: Number(r.line_number),
        date:
          r.transaction_date instanceof Date
            ? r.transaction_date.toISOString().slice(0, 10)
            : r.transaction_date
            ? String(r.transaction_date).slice(0, 10)
            : null,
        name: r.name,
        memo: r.memo,
        amount: Number(r.amount ?? 0),
        type: r.transaction_type ?? 'credit',
        matched_account: r.matched_account,
        matched_name: r.matched_name,
        reconciled: !!r.reconciled,
      })),
    };
  } catch (err: any) {
    return { success: false, transactions: [], error: err?.message ?? String(err) };
  }
}
