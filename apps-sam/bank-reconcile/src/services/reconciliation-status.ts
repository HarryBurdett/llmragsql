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
  // Partial-reconciliation / sequential gating fields (legacy parity).
  reconciliation_in_progress?: boolean;
  reconciliation_in_progress_message?: string | null;
  partial_entries?: number;
  sequential_gating?: boolean;
  sequential_gating_self?: boolean;
  error?: string;
}

/**
 * Normalise a filename for case + whitespace-insensitive comparison.
 * Mirrors the legacy `_norm_fn` helper that lets a stored
 * "Statement 17-APR-26 AC X  Y.pdf" (double space) match an inbound
 * "Statement 17-APR-26 AC X Y.pdf" (single space).
 */
function normFilename(fn: string | null | undefined): string {
  if (!fn) return '';
  return fn.split(/\s+/).filter(Boolean).join(' ').trim().toLowerCase();
}

export async function getReconciliationStatus(
  operaDb: Knex,
  bankCode: string,
  appDb: Knex | null = null,
  currentFilename: string | null = null,
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
    const currentBalance = reconciledBalance + unreconciledTotal;

    // Partial-reconciliation check — ae_tmpstat is non-zero when Opera
    // has half-reconciled entries waiting for the user to finish a
    // deferred row. Faithful port of
    // `StatementReconciler.check_reconciliation_in_progress`
    // (sql_rag/statement_reconcile.py:266-299).
    let partialEntries = 0;
    let inProgressMessage: string | null = null;
    let sequentialGating = false;
    let sequentialGatingSelf = false;
    try {
      const partialRows = (await operaDb.raw(
        `
        SELECT COUNT(*) AS partial_count
        FROM aentry WITH (NOLOCK)
        WHERE ae_acnt = ?
          AND ae_tmpstat <> 0
          AND ae_tmpstat IS NOT NULL
        `,
        [bankCode],
      )) as unknown as Array<{ partial_count: number | string | null }>;
      partialEntries = Number(partialRows?.[0]?.partial_count ?? 0);
    } catch {
      partialEntries = 0;
    }

    if (partialEntries > 0) {
      // Default message — pre-gating.
      inProgressMessage =
        `${partialEntries} entries have partial reconciliation markers from ` +
        `Opera or a previous session. These will be cleared automatically ` +
        `when you reconcile.`;

      // Sequential gating: differentiate the message based on whether
      // the user is processing the deferred-row statement itself
      // (sequential_gating_self) vs a subsequent statement in the
      // chain. Faithful port of routes.py:743-797.
      if (appDb) {
        try {
          const pendingRows = (await appDb('bank_statement_imports')
            .distinct('filename')
            .where('bank_code', bankCode)
            .andWhere(function notReconciled(this: Knex.QueryBuilder) {
              this.where('is_reconciled', 0).orWhereNull('is_reconciled');
            })
            .andWhere(function notArchived(this: Knex.QueryBuilder) {
              this.whereNotIn('target_system', [
                'archived',
                'deleted',
                'retained',
              ]).orWhereNull('target_system');
            })
            .whereNotNull('filename')) as unknown as Array<{ filename: string | null }>;
          const pendingFiles = (pendingRows ?? [])
            .map((r) => (r.filename ?? '').toString())
            .filter((f) => f.length > 0);
          if (pendingFiles.length > 0) {
            const names = pendingFiles.slice(0, 2).join(', ');
            const more =
              pendingFiles.length > 2 ? ` (+${pendingFiles.length - 2} more)` : '';
            const curNorm = normFilename(currentFilename);
            const isSelf = !!(
              curNorm && pendingFiles.some((p) => normFilename(p) === curNorm)
            );
            if (isSelf) {
              inProgressMessage =
                `This statement has ${partialEntries} partial reconciliation ` +
                `markers from a previous session and is awaiting a ` +
                `deferred-row resolution. Resolve the deferred row, then ` +
                `reconcile — the markers will clear automatically.`;
            } else {
              inProgressMessage =
                `This statement cannot be fully reconciled until ` +
                `statement ${names}${more} is completed (it's awaiting a ` +
                `deferred-row resolution). You can still process and ` +
                `import this statement to keep Opera up to date — ` +
                `reconciliation will run once the prior statement is done.`;
            }
            sequentialGating = true;
            sequentialGatingSelf = isSelf;
          }
        } catch {
          // best-effort — sequential-gating message is advisory
        }
      }
    }

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
      reconciliation_in_progress: partialEntries > 0,
      reconciliation_in_progress_message: inProgressMessage,
      partial_entries: partialEntries,
      sequential_gating: sequentialGating,
      sequential_gating_self: sequentialGatingSelf,
    };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}
