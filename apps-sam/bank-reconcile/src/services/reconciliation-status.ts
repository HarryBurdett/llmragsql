/**
 * Bank reconciliation status + unreconciled entries.
 *
 * Faithful port of:
 *   OperaSQLImport.get_unreconciled_entries
 *   OperaSQLImport.get_reconciliation_status
 *
 * Both used by the GET /api/reconcile/bank/:bank_code/* endpoints.
 * Read-only against Opera SQL with NOLOCK.
 */
import type { Knex } from 'knex';

// =====================================================================
// get_unreconciled_entries
// =====================================================================

export interface UnreconciledEntry {
  ae_entry: string;
  value_pounds: number;
  ae_lstdate: string;
  ae_cbtype: string;
  ae_entref: string;
  ae_comment: string;
  ae_complet: number;
  is_complete: boolean;
}

export interface UnreconciledEntriesResponse {
  success: boolean;
  bank_code: string;
  count: number;
  entries: UnreconciledEntry[];
  error?: string;
}

function dateToYmd(d: Date | string | null): string {
  if (!d) return '';
  if (d instanceof Date) {
    if (Number.isNaN(d.getTime())) return '';
    const yr = d.getFullYear();
    const mo = String(d.getMonth() + 1).padStart(2, '0');
    const da = String(d.getDate()).padStart(2, '0');
    return `${yr}-${mo}-${da}`;
  }
  return String(d).slice(0, 10);
}

export async function getUnreconciledEntries(
  operaDb: Knex,
  bankCode: string,
  includeIncomplete = false,
): Promise<UnreconciledEntriesResponse> {
  try {
    const completeFilter = includeIncomplete ? '' : 'AND ae_complet = 1';
    const sql = `
      SELECT ae_entry, ae_value/100.0 as value_pounds, ae_lstdate,
             ae_cbtype, ae_entref, ae_comment, ae_complet
      FROM aentry WITH (NOLOCK)
      WHERE ae_acnt = ?
        AND ae_reclnum = 0
        ${completeFilter}
      ORDER BY ae_lstdate, ae_entry
    `;
    const rows = (await operaDb.raw(sql, [bankCode])) as unknown as Array<{
      ae_entry: string | null;
      value_pounds: number | null;
      ae_lstdate: Date | string | null;
      ae_cbtype: string | null;
      ae_entref: string | null;
      ae_comment: string | null;
      ae_complet: number | null;
    }>;

    const entries: UnreconciledEntry[] = (Array.isArray(rows) ? rows : []).map((r) => ({
      ae_entry: String(r.ae_entry ?? '').trim(),
      value_pounds: Number(r.value_pounds ?? 0),
      ae_lstdate: dateToYmd(r.ae_lstdate),
      ae_cbtype: (r.ae_cbtype ?? '').trim(),
      ae_entref: (r.ae_entref ?? '').trim(),
      ae_comment: (r.ae_comment ?? '').trim(),
      ae_complet: Number(r.ae_complet ?? 0),
      is_complete: Number(r.ae_complet ?? 0) !== 0,
    }));

    return {
      success: true,
      bank_code: bankCode,
      count: entries.length,
      entries,
    };
  } catch (err: any) {
    return {
      success: false,
      bank_code: bankCode,
      count: 0,
      entries: [],
      error: err?.message ?? String(err),
    };
  }
}

// =====================================================================
// get_reconciliation_status
// =====================================================================

export interface ReconciliationStatus {
  success: boolean;
  bank_account?: string;
  reconciled_balance?: number;
  current_balance?: number;
  unreconciled_difference?: number;
  unreconciled_count?: number;
  unreconciled_total?: number;
  last_rec_line?: number;
  last_stmt_no?: number | null;
  last_stmt_date?: string | null;
  last_rec_date?: string | null;
  rec_cfwd_balance?: number;
  error?: string;
}

export async function getReconciliationStatus(
  operaDb: Knex,
  bankCode: string,
): Promise<ReconciliationStatus> {
  try {
    const nbankRows = (await operaDb.raw(
      `
      SELECT nk_recbal/100.0 as reconciled_balance,
             nk_curbal/100.0 as current_balance,
             nk_lstrecl as last_rec_line,
             nk_lststno as last_stmt_no,
             nk_lststdt as last_stmt_date,
             nk_recldte as last_rec_date,
             nk_reccfwd/100.0 as rec_cfwd_balance
      FROM nbank WITH (NOLOCK)
      WHERE nk_acnt = ?
      `,
      [bankCode],
    )) as unknown as Array<{
      reconciled_balance: number | null;
      current_balance: number | null;
      last_rec_line: number | null;
      last_stmt_no: number | null;
      last_stmt_date: Date | string | null;
      last_rec_date: Date | string | null;
      rec_cfwd_balance: number | null;
    }>;

    if (!Array.isArray(nbankRows) || nbankRows.length === 0) {
      return { success: false, error: `Bank account ${bankCode} not found` };
    }
    const nbank = nbankRows[0]!;

    const unrecRows = (await operaDb.raw(
      `
      SELECT COUNT(*) as count, COALESCE(SUM(ae_value), 0)/100.0 as total
      FROM aentry WITH (NOLOCK)
      WHERE ae_acnt = ?
        AND ae_reclnum = 0
        AND ae_complet = 1
      `,
      [bankCode],
    )) as unknown as Array<{ count: number | null; total: number | null }>;
    const unrec = (Array.isArray(unrecRows) && unrecRows.length > 0)
      ? unrecRows[0]!
      : { count: 0, total: 0 };

    const reconciledBalance = Number(nbank.reconciled_balance ?? 0);
    const unreconciledTotal = Number(unrec.total ?? 0);
    // Derive current balance from reconciled + actual unreconciled entries
    // (matches Python: avoids reliance on nk_curbal which may have history)
    const currentBalance = reconciledBalance + unreconciledTotal;

    return {
      success: true,
      bank_account: bankCode,
      reconciled_balance: reconciledBalance,
      current_balance: currentBalance,
      unreconciled_difference: unreconciledTotal,
      unreconciled_count: Number(unrec.count ?? 0),
      unreconciled_total: unreconciledTotal,
      last_rec_line: Number(nbank.last_rec_line ?? 0),
      last_stmt_no: nbank.last_stmt_no !== null ? Number(nbank.last_stmt_no) : null,
      last_stmt_date: dateToYmd(nbank.last_stmt_date) || null,
      last_rec_date: dateToYmd(nbank.last_rec_date) || null,
      rec_cfwd_balance: Number(nbank.rec_cfwd_balance ?? 0),
    };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}
