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
