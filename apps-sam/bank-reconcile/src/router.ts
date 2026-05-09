/**
 * Express router for the bank-reconcile plugin.
 *
 * Foundation endpoints + first batch of read-only ports. Many more
 * endpoints to come — bank-reconcile is the largest app at 127 routes.
 */
import { Router, type Request, type Response } from 'express';
import type { AppContext } from './app-context.js';
import { listBanks } from './services/banks.js';
import { runHealthCheck } from './services/health-check.js';
import {
  listOrphanTmpstat,
  clearOrphanTmpstat,
} from './services/orphan-tmpstat.js';
import {
  getUnreconciledEntries,
  getReconciliationStatus,
} from './services/reconciliation-status.js';
import {
  ignoreTransaction,
  listIgnoredTransactions,
  unignoreTransactionById,
  unignoreTransactionByMatch,
} from './services/ignored-transactions.js';
import {
  markStatementReconciled,
  listImportedStatements,
} from './services/statement-files.js';
import {
  getRecurringEntriesMode,
  setRecurringEntriesMode,
} from './services/settings.js';
import { listCashbookTypes } from './services/cashbook-types.js';
import {
  getMatchConfig,
  updateMatchConfig,
} from './services/match-config.js';
import {
  detectFormat,
  supportedFormats,
} from './services/format-detect.js';
import { detectBankFromContent } from './services/detect-bank.js';
import { recordDuplicateOverride } from './services/duplicate-override.js';
import {
  saveImportDraft,
  loadImportDraft,
  deleteImportDraft,
} from './services/bank-import-drafts.js';
import {
  getCustomersForDropdown,
  getSuppliersForDropdown,
} from './services/account-dropdowns.js';
import { unreconcileEntries } from './services/unreconcile.js';
import {
  listImportHistory,
  deleteImportRecord,
  clearImportHistory,
} from './services/import-history.js';
import {
  getFolderSettings,
  saveFolderSettings,
} from './services/folder-settings.js';
import { validateStatementForReconciliation } from './services/validate-statement.js';
import {
  markEntriesReconciled,
  type ReconcileEntryInput,
} from './services/mark-reconciled.js';
import {
  recordCorrection,
  listCorrections,
} from './services/alias-corrections.js';
import { completeBatch } from './services/complete-batch.js';
import { persistImportDecisions } from './services/persist-decisions.js';
import {
  confirmStatementMatches,
  type ConfirmMatchInput,
} from './services/confirm-matches.js';
import {
  updateRepeatEntryDate,
  listRepeatEntries,
} from './services/repeat-entries.js';

export function createRouter(ctx: AppContext): Router {
  const router = Router();

  function getAppDb(req: Request, res: Response): import('knex').Knex | null {
    if (!ctx.db.app) {
      res.status(503).json({
        success: false,
        error: 'bank-reconcile per-app database not provisioned for this tenant.',
      });
      return null;
    }
    return ctx.db.app;
  }

  function getOperaDb(req: Request, res: Response): import('knex').Knex | null {
    const company = req.operaCompany;
    if (!company) {
      res.status(400).json({
        success: false,
        error: 'No Opera company in context. SAM should set X-Opera-Company.',
      });
      return null;
    }
    const db = ctx.db.getCompanyDb(company);
    if (!db) {
      res.status(503).json({
        success: false,
        error: `Opera SQL connection not available for company ${company}.`,
      });
      return null;
    }
    return db;
  }

  /**
   * GET /api/bank-reconcile/status — plugin liveness.
   */
  router.get('/api/bank-reconcile/status', (_req, res) => {
    res.json({
      success: true,
      app: 'bank-reconcile',
      tenant_id: ctx.tenantId,
      opera_type: ctx.operaType,
      message: 'Foundation in place. Endpoint port in progress.',
    });
  });

  /**
   * GET /api/reconcile/banks — list of bank accounts.
   *
   * Faithful port of `get_bank_accounts` (line 280).
   */
  router.get('/api/reconcile/banks', async (req, res) => {
    const operaDb = getOperaDb(req, res);
    if (!operaDb) return;
    try {
      const result = await listBanks(operaDb);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('List banks failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/bank-import/health-check — data-integrity health check.
   *
   * Faithful port of `bank_import_health_check`.
   */
  router.get('/api/bank-import/health-check', async (req, res) => {
    const operaDb = getOperaDb(req, res);
    if (!operaDb) return;
    try {
      const result = await runHealthCheck({ operaDb, appDb: ctx.db.app });
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Health check failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/reconcile/bank/:bank_code/orphan-tmpstat — list orphaned
   * partial reconcile reservations on a bank. Read-only.
   */
  router.get('/api/reconcile/bank/:bank_code/orphan-tmpstat', async (req, res) => {
    const operaDb = getOperaDb(req, res);
    if (!operaDb) return;
    try {
      const bankCode = String(req.params.bank_code ?? '').trim();
      if (!bankCode) {
        res.status(400).json({ success: false, error: 'Missing bank_code' });
        return;
      }
      const result = await listOrphanTmpstat(operaDb, bankCode);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('List orphan tmpstat failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * POST /api/reconcile/bank/:bank_code/clear-orphan-tmpstat —
   * clear orphan tmpstat reservations. Optional body
   * `{ entry_numbers: [...] }` restricts to specific entries.
   *
   * Faithful port of `clear_orphan_tmpstat`. Uses ROWLOCK on a narrow
   * UPDATE per CLAUDE.md locking rules.
   */
  router.post('/api/reconcile/bank/:bank_code/clear-orphan-tmpstat', async (req, res) => {
    const operaDb = getOperaDb(req, res);
    if (!operaDb) return;
    try {
      const bankCode = String(req.params.bank_code ?? '').trim();
      if (!bankCode) {
        res.status(400).json({ success: false, error: 'Missing bank_code' });
        return;
      }
      const body = (req.body ?? {}) as Record<string, unknown>;
      const entryNumbers = Array.isArray(body.entry_numbers)
        ? (body.entry_numbers as string[])
        : undefined;
      const result = await clearOrphanTmpstat(operaDb, bankCode, entryNumbers);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Clear orphan tmpstat failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/reconcile/bank/:bank_code/unreconciled — list unreconciled
   * cashbook entries for a bank account. Faithful port of
   * `get_unreconciled_entries` (line 818).
   *
   * Query: ?include_incomplete=true to include batches with ae_complet=0
   * (not yet posted to NL).
   */
  router.get('/api/reconcile/bank/:bank_code/unreconciled', async (req, res) => {
    const operaDb = getOperaDb(req, res);
    if (!operaDb) return;
    try {
      const bankCode = String(req.params.bank_code ?? '').trim();
      if (!bankCode) {
        res.status(400).json({ success: false, error: 'Missing bank_code' });
        return;
      }
      const includeIncomplete = req.query.include_incomplete === 'true';
      const result = await getUnreconciledEntries(operaDb, bankCode, includeIncomplete);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Get unreconciled entries failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/reconcile/bank/:bank_code/status — current reconciliation
   * status (balances + last reconcile info). Faithful port of
   * `get_reconciliation_status` (the OperaSQLImport method, not the
   * full route handler with sequential-gating logic).
   */
  router.get('/api/reconcile/bank/:bank_code/status', async (req, res) => {
    const operaDb = getOperaDb(req, res);
    if (!operaDb) return;
    try {
      const bankCode = String(req.params.bank_code ?? '').trim();
      if (!bankCode) {
        res.status(400).json({ success: false, error: 'Missing bank_code' });
        return;
      }
      const result = await getReconciliationStatus(operaDb, bankCode);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Get reconciliation status failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * POST /api/reconcile/bank/:bank_code/ignore-transaction
   *
   * Mark a bank statement line as "already in Opera, ignore for reconcile".
   * Faithful port of `ignore_bank_transaction`.
   */
  router.post('/api/reconcile/bank/:bank_code/ignore-transaction', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const bankCode = String(req.params.bank_code ?? '').trim();
      const q = req.query;
      const tx = String(q.transaction_date ?? '').trim();
      const amt = q.amount !== undefined ? Number(q.amount) : NaN;
      if (!bankCode || !tx || Number.isNaN(amt)) {
        res.status(400).json({
          success: false,
          error: 'bank_code, transaction_date, and amount are required',
        });
        return;
      }
      const result = await ignoreTransaction(appDb, {
        bankCode,
        transactionDate: tx,
        amount: amt,
        description: typeof q.description === 'string' ? q.description : null,
        reference: typeof q.reference === 'string' ? q.reference : null,
        reason: typeof q.reason === 'string' ? q.reason : null,
        ignoredBy: 'API',
      });
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Ignore transaction failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/reconcile/bank/:bank_code/ignored-transactions
   *
   * List the ignored transactions for a bank account. Faithful port of
   * `get_ignored_transactions`.
   */
  router.get('/api/reconcile/bank/:bank_code/ignored-transactions', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const bankCode = String(req.params.bank_code ?? '').trim();
      const limit = req.query.limit ? Number(req.query.limit) : 100;
      const result = await listIgnoredTransactions(appDb, bankCode, limit);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('List ignored transactions failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * DELETE /api/reconcile/bank/ignored-transaction/:record_id
   *
   * Remove an ignored-transaction record by id. Faithful port of
   * `unignore_transaction`.
   */
  router.delete('/api/reconcile/bank/ignored-transaction/:record_id', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const recordId = Number(req.params.record_id);
      if (!Number.isFinite(recordId)) {
        res.status(400).json({ success: false, error: 'Invalid record_id' });
        return;
      }
      const result = await unignoreTransactionById(appDb, recordId);
      if (!result.success && result.error === 'Record not found') {
        res.status(404).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Unignore transaction failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * DELETE /api/reconcile/bank/:bank_code/unignore-transaction
   *
   * Remove an ignored transaction by matching bank+date+amount.
   * Faithful port of `unignore_transaction_by_match`.
   */
  router.delete('/api/reconcile/bank/:bank_code/unignore-transaction', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const bankCode = String(req.params.bank_code ?? '').trim();
      const tx = String(req.query.transaction_date ?? '').trim();
      const amt = req.query.amount !== undefined ? Number(req.query.amount) : NaN;
      if (!bankCode || !tx || Number.isNaN(amt)) {
        res.status(400).json({
          success: false,
          error: 'bank_code, transaction_date, and amount are required',
        });
        return;
      }
      const result = await unignoreTransactionByMatch(appDb, bankCode, tx, amt);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Unignore (by match) failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * POST /api/statement-files/mark-reconciled
   *
   * Mark a statement file as reconciled. Faithful port of
   * `mark_statement_reconciled`.
   */
  router.post('/api/statement-files/mark-reconciled', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const filename = String(req.query.filename ?? '').trim();
      const bankCode = typeof req.query.bank_code === 'string' ? req.query.bank_code : null;
      const reconciledCount = req.query.reconciled_count
        ? Number(req.query.reconciled_count)
        : 0;
      if (!filename) {
        res.status(400).json({ success: false, error: 'filename is required' });
        return;
      }
      const result = await markStatementReconciled(appDb, {
        filename,
        bankCode,
        reconciledCount,
      });
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Mark statement reconciled failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/statement-files/imported-for-reconciliation
   *
   * List imported bank statements pending reconciliation.
   * Faithful port (without the Opera-side cross-check yet — queued for
   * a future session per progress.md).
   */
  router.get('/api/statement-files/imported-for-reconciliation', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const bankCode = typeof req.query.bank_code === 'string' ? req.query.bank_code : null;
      const limit = req.query.limit ? Number(req.query.limit) : 200;
      const includeReconciled = req.query.include_reconciled === 'true';
      const result = await listImportedStatements(appDb, {
        bankCode,
        limit,
        includeReconciled,
      });
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('List imported statements failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/recurring-entries/config
   *
   * Read recurring-entries processing mode ('process' or 'warn').
   * Faithful port of `get_recurring_entries_config` (api/main.py:10290).
   */
  router.get('/api/recurring-entries/config', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const result = await getRecurringEntriesMode(appDb);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Get recurring-entries mode failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * PUT /api/recurring-entries/config?mode=process|warn
   *
   * Update recurring-entries processing mode. Faithful port of
   * `update_recurring_entries_config`.
   */
  router.put('/api/recurring-entries/config', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const mode = String(req.query.mode ?? '').trim();
      const result = await setRecurringEntriesMode(appDb, mode);
      if (!result.success) {
        res.status(400).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Set recurring-entries mode failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/bank-import/cashbook-types?category=R|P|T
   *
   * Returns the configured Opera cashbook entry types from `atype`,
   * optionally filtered by category. Faithful port of
   * `get_cashbook_types` (apps/bank_reconcile/api/routes.py:3009-3040).
   */
  router.get('/api/bank-import/cashbook-types', async (req: Request, res: Response) => {
    const operaDb = getOperaDb(req, res);
    if (!operaDb) return;
    try {
      const category =
        typeof req.query.category === 'string' && req.query.category.trim()
          ? req.query.category.trim()
          : null;
      const result = await listCashbookTypes(operaDb, category);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Cashbook types fetch failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/bank-import/config
   *
   * Returns the bank-import matching thresholds (min_match_score,
   * learn_threshold, ambiguity_threshold, use_phonetic, use_levenshtein,
   * use_ngram). If no row exists, returns hard-coded defaults — same
   * fallback as `get_match_config` in routes.py:3046-3088.
   */
  router.get('/api/bank-import/config', async (req: Request, res: Response) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const result = await getMatchConfig(appDb);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Match config fetch failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * PUT /api/bank-import/config?min_match_score=...&learn_threshold=...
   *
   * Update the bank-import matching thresholds. Faithful port of
   * `update_match_config` (routes.py:3094-3134). All numeric thresholds
   * are clamped to [0,1] (matches the FastAPI `ge=0.0, le=1.0` validator).
   */
  router.put('/api/bank-import/config', async (req: Request, res: Response) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const q = req.query;
      const result = await updateMatchConfig(appDb, {
        min_match_score:
          q.min_match_score !== undefined ? Number(q.min_match_score) : 0.6,
        learn_threshold:
          q.learn_threshold !== undefined ? Number(q.learn_threshold) : 0.8,
        ambiguity_threshold:
          q.ambiguity_threshold !== undefined ? Number(q.ambiguity_threshold) : 0.15,
        use_phonetic: q.use_phonetic !== undefined ? q.use_phonetic === 'true' : true,
        use_levenshtein:
          q.use_levenshtein !== undefined ? q.use_levenshtein === 'true' : true,
        use_ngram: q.use_ngram !== undefined ? q.use_ngram === 'true' : true,
      });
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Match config update failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * POST /api/bank-import/detect-format
   *
   * Detect the format of a bank-statement file. Faithful port of
   * `detect_file_format` (apps/bank_reconcile/api/routes.py:2337-2363).
   *
   * SAM port note: the Python endpoint took a server-side `filepath`
   * and read the file from disk. Under SAM the plugin doesn't see
   * the user's file system — the frontend uploads the file content
   * (or the email-ingest service produces it). Accept the content in
   * the JSON body instead. This is the only difference; the parser
   * sniffing logic is unchanged.
   *
   * Body: { content: string, filename?: string }
   * Returns: { success, format: 'CSV'|'OFX'|'QIF'|'MT940'|null,
   *            supported_formats: string[] }
   */
  router.post('/api/bank-import/detect-format', async (req: Request, res: Response) => {
    try {
      const body = (req.body ?? {}) as { content?: string; filename?: string };
      const content = String(body.content ?? '');
      const filename = String(body.filename ?? '');
      if (!content) {
        res.status(400).json({ success: false, error: 'content is required' });
        return;
      }
      const format = detectFormat(content, filename);
      res.json({
        success: true,
        format,
        supported_formats: supportedFormats,
      });
    } catch (err: any) {
      ctx.logger.error('Detect format failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * POST /api/bank-import/detect-bank
   *
   * Detect which Opera bank account a bank-statement file belongs to.
   * Faithful port of `detect_bank_from_file`
   * (apps/bank_reconcile/api/routes.py:2369-2490).
   *
   * Two extraction strategies on the first 30 lines:
   *   1. regex: sort code (XX-XX-XX) + 8-digit account number
   *   2. CSV header scan + 'Account' field "20-96-89 90764205"
   *
   * Once extracted, both sides are normalised (whitespace + dashes
   * stripped) before comparing against Opera nbank.
   *
   * Body: { content: string }
   * Returns:
   *   - detected=true:  bank_code + bank_description + sort_code + account_number
   *   - detected=false: available_banks for manual selection
   */
  router.post('/api/bank-import/detect-bank', async (req: Request, res: Response) => {
    const operaDb = getOperaDb(req, res);
    if (!operaDb) return;
    try {
      const body = (req.body ?? {}) as { content?: string };
      const content = String(body.content ?? '');
      if (!content) {
        res.status(400).json({ success: false, error: 'content is required' });
        return;
      }
      const detected = await detectBankFromContent(operaDb, content);
      if (detected.bank_code) {
        const banks = await listBanks(operaDb);
        const info = banks.banks?.find((b) => b.account_code === detected.bank_code);
        res.json({
          success: true,
          detected: true,
          bank_code: detected.bank_code,
          bank_description: info?.description ?? detected.bank_code,
          sort_code: info?.sort_code ?? detected.sort_code ?? '',
          account_number: info?.account_number ?? detected.account_number ?? '',
          message: `Detected bank account: ${detected.bank_code}`,
        });
      } else {
        const banks = await listBanks(operaDb);
        const found =
          detected.sort_code && detected.account_number
            ? ` Found: ${detected.sort_code} ${detected.account_number}`
            : '';
        res.json({
          success: true,
          detected: false,
          bank_code: null,
          message: `Could not detect bank account from file.${found} Please select manually.`,
          available_banks: banks.banks ?? [],
        });
      }
    } catch (err: any) {
      ctx.logger.error('Detect bank failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * POST /api/bank-import/duplicate-override
   *
   * Record a user's decision to import a transaction despite it being
   * flagged as a possible duplicate. Faithful port of
   * `override_duplicate` (apps/bank_reconcile/api/routes.py:2961-3003).
   *
   * Query params:
   *   - transaction_hash: hash of the transaction
   *   - reason: free-text explanation
   *   - user_code: (optional) operator code from req.user.appRole etc.
   *
   * Upsert semantics — re-overriding the same hash updates the reason
   * and timestamp.
   */
  router.post(
    '/api/bank-import/duplicate-override',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const transactionHash = String(req.query.transaction_hash ?? '').trim();
        const reason = String(req.query.reason ?? '').trim();
        const userCode = req.user?.userId ?? null;
        if (!transactionHash) {
          res.status(400).json({
            success: false,
            error: 'transaction_hash is required',
          });
          return;
        }
        if (!reason) {
          res.status(400).json({ success: false, error: 'reason is required' });
          return;
        }
        const result = await recordDuplicateOverride(appDb, {
          transactionHash,
          reason,
          userCode,
        });
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Duplicate override failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/bank-import/draft
   *
   * Save (upsert) a work-in-progress bank statement import. Faithful
   * port of `save_bank_import_draft` (routes.py:3297-3327).
   *
   * Body: { bank_code, source, filename, preview_data, user_edits,
   *         email_id?, attachment_id?, pdf_hash?, target_system? }
   */
  router.post('/api/bank-import/draft', async (req: Request, res: Response) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const body = (req.body ?? {}) as Record<string, unknown>;
      const result = await saveImportDraft(appDb, {
        bankCode: String(body.bank_code ?? ''),
        source: String(body.source ?? ''),
        filename: String(body.filename ?? ''),
        previewData: body.preview_data ?? {},
        userEdits: body.user_edits ?? {},
        emailId: body.email_id as number | string | null | undefined,
        attachmentId: (body.attachment_id as string | null | undefined) ?? null,
        pdfHash: (body.pdf_hash as string | null | undefined) ?? null,
        targetSystem: (body.target_system as string | undefined) ?? 'opera_se',
      });
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Save bank import draft failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/bank-import/draft
   *
   * Load a previously-saved draft. Faithful port of
   * `load_bank_import_draft` (routes.py:3333-3371). Optional filters
   * are applied only when explicitly provided (`null` means "no filter
   * on this column" — same as Python's `if x is not None` guards).
   */
  router.get('/api/bank-import/draft', async (req: Request, res: Response) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const q = req.query;
      const bankCode = String(q.bank_code ?? '').trim();
      const source = String(q.source ?? '').trim();
      if (!bankCode || !source) {
        res.status(400).json({
          success: false,
          error: 'bank_code and source are required',
        });
        return;
      }
      const result = await loadImportDraft(appDb, {
        bankCode,
        source,
        emailId:
          q.email_id !== undefined && q.email_id !== ''
            ? String(q.email_id)
            : undefined,
        attachmentId:
          q.attachment_id !== undefined ? String(q.attachment_id) : undefined,
        pdfHash:
          q.pdf_hash !== undefined ? String(q.pdf_hash) : undefined,
        filename:
          q.filename !== undefined ? String(q.filename) : undefined,
      });
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Load bank import draft failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * DELETE /api/bank-import/draft
   *
   * Delete a saved draft (after import completion or manual clear).
   * Same identifying-key shape as load.
   */
  router.delete(
    '/api/bank-import/draft',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const q = req.query;
        const bankCode = String(q.bank_code ?? '').trim();
        const source = String(q.source ?? '').trim();
        if (!bankCode || !source) {
          res.status(400).json({
            success: false,
            error: 'bank_code and source are required',
          });
          return;
        }
        const result = await deleteImportDraft(appDb, {
          bankCode,
          source,
          emailId:
            q.email_id !== undefined && q.email_id !== ''
              ? String(q.email_id)
              : undefined,
          attachmentId:
            q.attachment_id !== undefined ? String(q.attachment_id) : undefined,
          pdfHash:
            q.pdf_hash !== undefined ? String(q.pdf_hash) : undefined,
          filename: q.filename !== undefined ? String(q.filename) : undefined,
        });
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Delete bank import draft failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * GET /api/bank-import/accounts/customers
   *
   * Customer accounts for the import-UI manual override dropdown.
   * Faithful port of get_customers_for_dropdown
   * (apps/bank_reconcile/api/routes.py:4767-4805).
   *
   * Adds the dormant + stopped filters per CLAUDE.md "cannot post to
   * dormant accounts" — the original Python missed these on the
   * dropdown but enforces them on actual posting; this prevents the
   * operator from picking an account they couldn't post to anyway.
   */
  router.get(
    '/api/bank-import/accounts/customers',
    async (req: Request, res: Response) => {
      const operaDb = getOperaDb(req, res);
      if (!operaDb) return;
      try {
        const result = await getCustomersForDropdown(operaDb);
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Customers dropdown failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * GET /api/bank-import/accounts/suppliers
   *
   * Supplier accounts for the import-UI manual override dropdown.
   * Faithful port of get_suppliers_for_dropdown
   * (apps/bank_reconcile/api/routes.py:4811-4849).
   *
   * Same dormant+stopped filtering as the customer dropdown.
   */
  router.get(
    '/api/bank-import/accounts/suppliers',
    async (req: Request, res: Response) => {
      const operaDb = getOperaDb(req, res);
      if (!operaDb) return;
      try {
        const result = await getSuppliersForDropdown(operaDb);
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Suppliers dropdown failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/reconcile/bank/:bank_code/unreconcile
   *
   * Reverse a previously-reconciled batch. Faithful port of
   * `unreconcile_entries` (apps/bank_reconcile/api/routes.py:981-1143).
   *
   * Body: array of entry numbers to unreconcile.
   *
   * Resets every per-aentry rec field, recalculates nbank.nk_recbal,
   * and walks back to the prior batch state to update nbank's last-rec
   * fields. Bank-level lock + ROWLOCK on writes per CLAUDE.md.
   *
   * SQL injection guards: bank_code + every entry number validated at
   * the boundary via @sqlrag/sam-shared validators.
   */
  router.post(
    '/api/reconcile/bank/:bank_code/unreconcile',
    async (req: Request, res: Response) => {
      const operaDb = getOperaDb(req, res);
      if (!operaDb) return;
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const bankCode = String(req.params.bank_code ?? '');
        const body = req.body as
          | string[]
          | { entry_numbers?: string[] }
          | null;
        const entryNumbers = Array.isArray(body)
          ? body
          : Array.isArray(body?.entry_numbers)
            ? body.entry_numbers
            : null;
        if (!entryNumbers) {
          res.status(400).json({
            success: false,
            error: 'Body must be an array of entry numbers',
          });
          return;
        }
        const result = await unreconcileEntries(appDb, operaDb, {
          bankCode,
          entryNumbers,
        });
        if (!result.success) {
          res.status(400).json(result);
          return;
        }
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Unreconcile failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/reconcile/bank/:bank_code/mark-reconciled
   *
   * Mark cashbook entries as reconciled (full or partial).
   * Faithful port of mark_entries_reconciled (apps/bank_reconcile/api/
   * routes.py:897-975) + the underlying OperaSQLImport method.
   *
   * Body:
   *   {
   *     entries: [{entry_number, statement_line}, ...],
   *     statement_number: number,
   *     statement_date?:    'YYYY-MM-DD',
   *     reconciliation_date?: 'YYYY-MM-DD',
   *     partial?: boolean,
   *     closing_balance?: number  // pounds, used for nk_reccfwd in partial mode
   *   }
   *
   * Bank-level lock + UPDLOCK on nbank/aentry reads, ROWLOCK on
   * writes, single transaction.
   */
  router.post(
    '/api/reconcile/bank/:bank_code/mark-reconciled',
    async (req: Request, res: Response) => {
      const operaDb = getOperaDb(req, res);
      if (!operaDb) return;
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const bankCode = String(req.params.bank_code ?? '');
        const body = (req.body ?? {}) as {
          entries?: ReconcileEntryInput[];
          statement_number?: number;
          statement_date?: string;
          reconciliation_date?: string;
          partial?: boolean;
          closing_balance?: number;
        };
        const result = await markEntriesReconciled(appDb, operaDb, {
          bankCode,
          entries: Array.isArray(body.entries) ? body.entries : [],
          statementNumber: Number(body.statement_number ?? 0),
          statementDate: body.statement_date ?? null,
          reconciliationDate: body.reconciliation_date ?? null,
          partial: !!body.partial,
          closingBalance:
            body.closing_balance !== undefined ? Number(body.closing_balance) : null,
        });
        if (!result.success) {
          res.status(400).json(result);
          return;
        }
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Mark reconciled failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/bank-import/correction
   *
   * Record an operator correction to the bank-name → Opera-account
   * matching. Faithful port of record_correction (routes.py:2845-2895)
   * + BankAliasManager.record_correction (bank_aliases.py:728-790).
   *
   * Three side-effects in one transaction:
   *   1. Audit row in alias_corrections
   *   2. Upsert positive alias in bank_import_aliases (confidence=1.0)
   *   3. INSERT-OR-IGNORE negative example in negative_aliases so
   *      future matches avoid the bad mapping
   *
   * Query params:
   *   - bank_name        (required)
   *   - wrong_account    (required)
   *   - correct_account  (required)
   *   - ledger_type      (required: 'S' supplier | 'C' customer)
   *   - account_name     (optional — currently informational)
   */
  router.post(
    '/api/bank-import/correction',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const q = req.query;
        const result = await recordCorrection(appDb, {
          bank_name: String(q.bank_name ?? ''),
          wrong_account: String(q.wrong_account ?? ''),
          correct_account: String(q.correct_account ?? ''),
          ledger_type: String(q.ledger_type ?? ''),
          account_name: typeof q.account_name === 'string' ? q.account_name : null,
          corrected_by: req.user?.userId ?? 'USER',
        });
        if (!result.success) {
          res.status(400).json(result);
          return;
        }
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Record correction failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * GET /api/bank-import/corrections
   *
   * List recorded alias corrections (audit trail UI).
   */
  router.get(
    '/api/bank-import/corrections',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const result = await listCorrections(appDb, {
          bankName:
            typeof req.query.bank_name === 'string' ? req.query.bank_name : null,
          correctAccount:
            typeof req.query.correct_account === 'string'
              ? req.query.correct_account
              : null,
          limit: req.query.limit ? Number(req.query.limit) : undefined,
        });
        if (!result.success) {
          res.status(400).json(result);
          return;
        }
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('List corrections failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/reconcile/bank/:bank_code/complete-batch/:entry_number
   *
   * Complete an incomplete cashbook batch by posting to the nominal
   * ledger. Faithful port of complete_batch (routes.py:849-891) +
   * OperaSQLImport.complete_batch_posting (opera_sql_import.py
   * :8809-9019).
   *
   * Reads unposted anoml records (ax_done='N') for the entry,
   * creates the corresponding ntran rows + updates nacnt/nhist/
   * nbank, marks anoml ax_done='Y', and sets ae_complet=1. All in
   * a single transaction with bank-level lock.
   *
   * SQL injection guards: bank_code + entry_number validated at
   * the route boundary.
   */
  router.post(
    '/api/reconcile/bank/:bank_code/complete-batch/:entry_number',
    async (req: Request, res: Response) => {
      const operaDb = getOperaDb(req, res);
      if (!operaDb) return;
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const result = await completeBatch(appDb, operaDb, {
          bankCode: String(req.params.bank_code ?? ''),
          entryNumber: String(req.params.entry_number ?? ''),
        });
        if (!result.success) {
          res.status(400).json(result);
          return;
        }
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Complete batch failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/bank-import/persist-decisions
   *
   * Persist defer / partial-rec decisions for a bank statement WITHOUT
   * requiring the user to click the green Import button. Faithful
   * port of persist_bank_import_decisions (routes.py:3406-3565).
   *
   * Body:
   *   {
   *     bank_code, filename,
   *     source: 'pdf'|'email',
   *     statement_info: { opening_balance?, closing_balance?,
   *                        statement_date?, period_start?, period_end?,
   *                        account_number?, sort_code? },
   *     deferred_transactions: [{date, amount, description}],
   *     imported_by?: string
   *   }
   *
   * Behaviour:
   *   - Idempotent UPSERT of bank_statement_imports row
   *   - Replaces the bank+period defer set in deferred_transactions
   *     (period bounds optional — full-bank clear if omitted)
   */
  router.post(
    '/api/bank-import/persist-decisions',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const body = (req.body ?? {}) as {
          bank_code?: string;
          filename?: string;
          source?: string;
          statement_info?: any;
          deferred_transactions?: any[];
          imported_by?: string;
        };
        const result = await persistImportDecisions(appDb, {
          bankCode: String(body.bank_code ?? ''),
          filename: String(body.filename ?? ''),
          source: String(body.source ?? 'pdf'),
          statementInfo: body.statement_info ?? null,
          deferredTransactions: Array.isArray(body.deferred_transactions)
            ? body.deferred_transactions
            : [],
          importedBy: body.imported_by ?? req.user?.userId ?? 'admin',
        });
        if (!result.success) {
          res.status(400).json(result);
          return;
        }
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Persist decisions failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/reconcile/bank/:bank_code/confirm-matches
   *
   * Confirm matched transactions and reconcile them. Faithful port of
   * confirm_statement_matches (routes.py:1935-2035). Thin wrapper
   * around mark-reconciled that:
   *   - reads the next statement_number from nbank.nk_lststno + 1
   *     (per CLAUDE.md never use MAX+1 — comes from Opera's stored
   *     counter)
   *   - assigns statement_line numbers in 10s (Opera convention)
   *   - delegates the actual write to markEntriesReconciled (which
   *     has the bank lock + UPDLOCK + ROWLOCK + transaction)
   *
   * Body:
   *   {
   *     matches: [{ ae_entry } | { opera_entry: { ae_entry }}, ...],
   *     statement_balance: number  (pounds — flows to nk_reccfwd),
   *     statement_date: 'YYYY-MM-DD'
   *   }
   */
  router.post(
    '/api/reconcile/bank/:bank_code/confirm-matches',
    async (req: Request, res: Response) => {
      const operaDb = getOperaDb(req, res);
      if (!operaDb) return;
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const body = (req.body ?? {}) as {
          matches?: ConfirmMatchInput[];
          statement_balance?: number;
          statement_date?: string;
        };
        const result = await confirmStatementMatches(appDb, operaDb, {
          bankCode: String(req.params.bank_code ?? ''),
          matches: Array.isArray(body.matches) ? body.matches : [],
          statementBalance: Number(body.statement_balance ?? 0),
          statementDate: String(body.statement_date ?? ''),
        });
        if (!result.success) {
          res.status(400).json(result);
          return;
        }
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Confirm matches failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * GET /api/reconcile/bank/:bank_code/scan-emails (legacy/deprecated)
   *
   * Returns the deprecated-redirect payload pointing callers at the
   * real /api/bank-import/scan-emails endpoint. Faithful port of
   * scan_emails_for_statements_legacy (routes.py:2041-2062). Older
   * frontend builds still bind to this URL — we preserve the URL
   * shape but make it explicit that the data is empty.
   */
  router.get(
    '/api/reconcile/bank/:bank_code/scan-emails',
    (_req: Request, res: Response) => {
      res.json({
        success: false,
        deprecated: true,
        redirect_to: '/api/bank-import/scan-emails',
        message:
          "This endpoint is deprecated — use /api/bank-import/scan-emails " +
          "instead. The legacy URL preserved an empty placeholder; that's " +
          'been removed to stop callers silently receiving zero results.',
        statements_found: [],
      });
    },
  );

  /**
   * GET /api/bank-import/repeat-entries?bank_code=...
   *
   * List active repeat entries for a bank — debug + UI listing.
   * Faithful port of list_repeat_entries (routes.py:5425-5495).
   * Joins arhead + arline so each entry includes its first line's
   * amount/account/cbtype/comment for display purposes.
   */
  router.get(
    '/api/bank-import/repeat-entries',
    async (req: Request, res: Response) => {
      const operaDb = getOperaDb(req, res);
      if (!operaDb) return;
      try {
        const result = await listRepeatEntries(
          operaDb,
          String(req.query.bank_code ?? ''),
        );
        if (!result.success) {
          res.status(400).json(result);
          return;
        }
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('List repeat entries failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/bank-import/update-repeat-entry-date
   *
   * Update ae_nxtpost on an arhead row so the operator can sync a
   * repeat entry's next posting date with the actual bank
   * transaction date. Faithful port of update_repeat_entry_date
   * (routes.py:5320-5419).
   *
   * Bank-level lock + ROWLOCK on the UPDATE per CLAUDE.md.
   *
   * Optional alias save: when statement_name is supplied, upsert a
   * row in repeat_entry_aliases (per-app DB) so future imports
   * auto-match this bank statement description to this repeat entry.
   *
   * Query params:
   *   - bank_code, entry_ref, new_date (YYYY-MM-DD)
   *   - statement_name (optional)
   */
  router.post(
    '/api/bank-import/update-repeat-entry-date',
    async (req: Request, res: Response) => {
      const operaDb = getOperaDb(req, res);
      if (!operaDb) return;
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const q = req.query;
        const result = await updateRepeatEntryDate(appDb, operaDb, {
          bankCode: String(q.bank_code ?? ''),
          entryRef: String(q.entry_ref ?? ''),
          newDate: String(q.new_date ?? ''),
          statementName:
            typeof q.statement_name === 'string' ? q.statement_name : null,
        });
        if (!result.success) {
          res.status(400).json(result);
          return;
        }
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Update repeat entry date failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * GET /api/bank-import/import-history
   *
   * List bank statement import audit rows. Faithful port of
   * get_bank_statement_import_history (apps/bank_reconcile/api/
   * routes.py:9967-9997).
   *
   * Query params:
   *   - bank_code (optional)
   *   - from_date / to_date (statement_date range, optional)
   *   - limit (default 50)
   * Filters target_system='opera_se' to match the Python wrapper —
   * the legacy variant on /api/bank-import/email-import-history below
   * mirrors that without the filter for backwards compatibility.
   */
  router.get(
    '/api/bank-import/import-history',
    async (req: Request, res: Response) => {
      const appDb = ctx.db.app;
      if (!appDb) {
        res.status(503).json({
          success: false,
          error: 'bank-reconcile per-app database not provisioned for this tenant.',
        });
        return;
      }
      try {
        const result = await listImportHistory(appDb, {
          bankCode:
            typeof req.query.bank_code === 'string'
              ? req.query.bank_code
              : null,
          fromDate:
            typeof req.query.from_date === 'string'
              ? req.query.from_date
              : null,
          toDate:
            typeof req.query.to_date === 'string'
              ? req.query.to_date
              : null,
          limit: req.query.limit ? Number(req.query.limit) : 50,
        });
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('List import history failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * GET /api/bank-import/email-import-history
   *
   * Legacy alias for /api/bank-import/import-history. Same shape but
   * the response key is `history` (Python wrapper kept that name for
   * backwards compatibility — see routes.py:10171-10192). Filters
   * default-target_system NOT applied; matches Python's exact
   * behaviour.
   */
  router.get(
    '/api/bank-import/email-import-history',
    async (req: Request, res: Response) => {
      const appDb = ctx.db.app;
      if (!appDb) {
        res.status(503).json({
          success: false,
          error: 'bank-reconcile per-app database not provisioned for this tenant.',
        });
        return;
      }
      try {
        const result = await listImportHistory(appDb, {
          bankCode:
            typeof req.query.bank_code === 'string'
              ? req.query.bank_code
              : null,
          limit: req.query.limit ? Number(req.query.limit) : 50,
          targetSystem: null, // legacy: no target_system filter
        });
        if (!result.success) {
          res.status(500).json(result);
          return;
        }
        res.json({
          success: true,
          history: result.imports,
          count: result.count,
        });
      } catch (err: any) {
        ctx.logger.error('List email import history failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * DELETE /api/bank-import/import-history/:record_id
   *
   * Delete a single import audit row so the statement can be
   * re-imported. Faithful port of delete_bank_statement_import_record
   * (apps/bank_reconcile/api/routes.py:10104-10131). Does NOT touch
   * Opera — only the local audit row.
   */
  router.delete(
    '/api/bank-import/import-history/:record_id',
    async (req: Request, res: Response) => {
      const appDb = ctx.db.app;
      if (!appDb) {
        res.status(503).json({
          success: false,
          error: 'bank-reconcile per-app database not provisioned for this tenant.',
        });
        return;
      }
      try {
        const id = Number(req.params.record_id);
        const result = await deleteImportRecord(appDb, id);
        if (!result.success) {
          res
            .status(/not found/i.test(result.error ?? '') ? 404 : 400)
            .json(result);
          return;
        }
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Delete import record failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * DELETE /api/bank-import/import-history
   *
   * Bulk-clear import audit rows by optional bank_code + date range.
   * Faithful port of clear_bank_statement_import_history
   * (apps/bank_reconcile/api/routes.py:10137-10165). Returns the
   * deleted count.
   */
  router.delete(
    '/api/bank-import/import-history',
    async (req: Request, res: Response) => {
      const appDb = ctx.db.app;
      if (!appDb) {
        res.status(503).json({
          success: false,
          error: 'bank-reconcile per-app database not provisioned for this tenant.',
        });
        return;
      }
      try {
        const result = await clearImportHistory(appDb, {
          bankCode:
            typeof req.query.bank_code === 'string'
              ? req.query.bank_code
              : null,
          fromDate:
            typeof req.query.from_date === 'string'
              ? req.query.from_date
              : null,
          toDate:
            typeof req.query.to_date === 'string'
              ? req.query.to_date
              : null,
        });
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Clear import history failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * GET /api/bank-import/folder-settings
   *
   * Read the per-tenant bank-statement folder paths used by both the
   * file-system scanner and the email-archiver. Faithful port of
   * get_bank_import_folder_settings (apps/bank_reconcile/api/
   * routes.py:5501-5516). Always returns success=true so the UI
   * loads even when the row is missing.
   */
  router.get(
    '/api/bank-import/folder-settings',
    async (_req: Request, res: Response) => {
      const appDb = ctx.db.app;
      if (!appDb) {
        res.status(503).json({
          success: false,
          error: 'bank-reconcile per-app database not provisioned for this tenant.',
        });
        return;
      }
      try {
        const result = await getFolderSettings(appDb);
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Get folder settings failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/bank-import/folder-settings
   *
   * Update the bank-statement folder paths. Faithful port of
   * save_bank_import_folder_settings (apps/bank_reconcile/api/
   * routes.py:5522-5535). Empty strings are valid (clears the
   * setting).
   */
  router.post(
    '/api/bank-import/folder-settings',
    async (req: Request, res: Response) => {
      const appDb = ctx.db.app;
      if (!appDb) {
        res.status(503).json({
          success: false,
          error: 'bank-reconcile per-app database not provisioned for this tenant.',
        });
        return;
      }
      try {
        const body = (req.body ?? {}) as {
          base_folder?: string | null;
          archive_folder?: string | null;
        };
        const result = await saveFolderSettings(appDb, {
          base_folder: body.base_folder ?? '',
          archive_folder: body.archive_folder ?? '',
        });
        if (!result.success) {
          res.status(500).json(result);
          return;
        }
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Save folder settings failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/bank-reconciliation/validate-statement
   *
   * Validate that a bank statement is ready for reconciliation by
   * comparing its opening balance to Opera's `nbank.nk_recbal`.
   * Faithful port of validate_statement_for_reconciliation
   * (apps/bank_reconcile/api/routes.py:10198-10238).
   *
   * Query params:
   *   - bank_code (required)
   *   - opening_balance (required, pounds)
   *   - closing_balance (required, pounds)
   *   - statement_number (optional)
   *   - statement_date (required, YYYY-MM-DD)
   */
  router.post(
    '/api/bank-reconciliation/validate-statement',
    async (req: Request, res: Response) => {
      const operaDb = getOperaDb(req, res);
      if (!operaDb) return;
      try {
        const bankCode = String(req.query.bank_code ?? '').trim();
        const openingBalance = Number(req.query.opening_balance);
        const closingBalance = Number(req.query.closing_balance);
        const statementNumber =
          req.query.statement_number !== undefined &&
          req.query.statement_number !== ''
            ? Number(req.query.statement_number)
            : null;
        const statementDate =
          typeof req.query.statement_date === 'string'
            ? req.query.statement_date
            : null;
        const result = await validateStatementForReconciliation(operaDb, {
          bankAccount: bankCode,
          openingBalance,
          closingBalance,
          statementNumber,
          statementDate,
        });
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Validate statement failed', err);
        res.status(500).json({ valid: false, error_message: err?.message ?? String(err) });
      }
    },
  );

  // Many more endpoints to port from apps/bank_reconcile/api/routes.py
  // (127 routes total). Future-session priorities:
  //   - GET  /api/reconcile/bank/{bank_code} — full reconcile (~600 LOC)
  //   - GET  /api/reconcile/bank/{bank_code}/status
  //   - GET  /api/reconcile/bank/{bank_code}/unreconciled
  //   - POST /api/reconcile/bank/{bank_code}/mark-reconciled
  //   - POST /api/bank-import/scan-emails (via SAM email service)
  //   - POST /api/bank-import/preview-from-pdf (Gemini extraction)
  //   - POST /api/bank-import/import (the big posting flow)
  //   - ~120 more

  return router;
}
