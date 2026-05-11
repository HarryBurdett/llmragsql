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

export interface StaleReconciledStatement {
  import_id: number;
  filename: string | null;
  statement_date: string | null;
  closing_balance: number;
}

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
  // SAM-enhancement: Opera-restore detection. When Opera SQL is rolled
  // back to an earlier backup, SAM's `bank_statement_imports` history
  // can disagree with Opera's reconciled balance (`nbank.nk_recbal`).
  // This block surfaces the divergence so the user (or a recovery
  // endpoint) can re-process the affected statements.
  opera_divergence_detected?: boolean;
  opera_divergence_message?: string | null;
  stale_reconciled_statements?: StaleReconciledStatement[];
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

    // ============================================================
    // SAM enhancement — Opera divergence / restore detection
    // ============================================================
    // SAM's `bank_statement_imports` records the closing balance of
    // every statement it has reconciled. Opera's `nk_recbal` should
    // never drop BELOW the highest closing balance SAM has marked as
    // reconciled. If it does, Opera was rolled back to an earlier
    // backup (or someone unreconciled directly in Opera Cashbook),
    // and the SAM history is now stale. Surface the affected rows so
    // the user can re-process them.
    let operaDivergenceDetected = false;
    let operaDivergenceMessage: string | null = null;
    let staleStatements: StaleReconciledStatement[] = [];
    if (appDb) {
      try {
        const rows = (await appDb('bank_statement_imports')
          .select(
            'id',
            'filename',
            'statement_date',
            'closing_balance',
          )
          .where('bank_code', bankCode)
          .andWhere('is_reconciled', 1)
          .andWhere('closing_balance', '>', reconciledBalance + 0.005)
          .orderBy('closing_balance', 'asc')) as unknown as Array<{
          id: number;
          filename: string | null;
          statement_date: Date | string | null;
          closing_balance: number | string | null;
        }>;
        if (rows.length > 0) {
          staleStatements = rows.map((r) => ({
            import_id: Number(r.id),
            filename: r.filename,
            statement_date: dateToYmd(r.statement_date) || null,
            closing_balance: Number(r.closing_balance ?? 0),
          }));
          operaDivergenceDetected = true;
          const names = staleStatements
            .map((s) => s.filename || `import_id=${s.import_id}`)
            .slice(0, 3)
            .join(', ');
          const more =
            staleStatements.length > 3
              ? ` (+${staleStatements.length - 3} more)`
              : '';
          operaDivergenceMessage =
            `Opera's reconciled balance (£${reconciledBalance.toFixed(2)}) is ` +
            `lower than the closing balance of ${staleStatements.length} ` +
            `statement(s) SAM has marked as reconciled: ${names}${more}. ` +
            `This usually means Opera was restored from a backup or those ` +
            `reconciliations were undone directly in Opera Cashbook. ` +
            `Use the recovery endpoint to mark these statements ` +
            `unreconciled so they can be re-processed.`;
        }
      } catch {
        // detection is advisory — never block the status response
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
      opera_divergence_detected: operaDivergenceDetected,
      opera_divergence_message: operaDivergenceMessage,
      stale_reconciled_statements: staleStatements,
    };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}

// =====================================================================
// Recovery — clear stale `is_reconciled` flags after Opera restore
// =====================================================================

export interface OperaDivergenceRecoveryResult {
  success: boolean;
  cleared: number;
  cleared_imports?: StaleReconciledStatement[];
  error?: string;
}

/**
 * Mark every `bank_statement_imports` row whose closing balance is
 * higher than Opera's current `nk_recbal` as un-reconciled, so they
 * can be re-imported. Called by the user after confirming an Opera
 * restore has put SAM and Opera out of sync.
 *
 * Returns the rows that were cleared so the caller can list them in
 * the UI.
 */
export async function recoverFromOperaDivergence(
  operaDb: Knex,
  appDb: Knex,
  bankCode: string,
): Promise<OperaDivergenceRecoveryResult> {
  try {
    const nbank = (await operaDb('nbank')
      .select(operaDb.raw('nk_recbal / 100.0 AS reconciled_balance'))
      .where('nk_acnt', bankCode)
      .first()) as { reconciled_balance: number | string | null } | undefined;
    if (!nbank) {
      return { success: false, cleared: 0, error: `Bank ${bankCode} not found in nbank` };
    }
    const reconciledBalance = Number(nbank.reconciled_balance ?? 0);

    const stale = (await appDb('bank_statement_imports')
      .select('id', 'filename', 'statement_date', 'closing_balance')
      .where('bank_code', bankCode)
      .andWhere('is_reconciled', 1)
      .andWhere('closing_balance', '>', reconciledBalance + 0.005)) as unknown as Array<{
      id: number;
      filename: string | null;
      statement_date: Date | string | null;
      closing_balance: number | string | null;
    }>;
    if (stale.length === 0) {
      return { success: true, cleared: 0, cleared_imports: [] };
    }

    const ids = stale.map((s) => s.id);
    const cleared = Number(
      await appDb('bank_statement_imports')
        .whereIn('id', ids)
        .update({
          is_reconciled: 0,
          reconciled_count: 0,
          reconciled_at: null,
          reconciled_by: null,
        }),
    );

    return {
      success: true,
      cleared,
      cleared_imports: stale.map((s) => ({
        import_id: Number(s.id),
        filename: s.filename,
        statement_date: dateToYmd(s.statement_date) || null,
        closing_balance: Number(s.closing_balance ?? 0),
      })),
    };
  } catch (err: any) {
    return { success: false, cleared: 0, error: err?.message ?? String(err) };
  }
}
