/**
 * Express router for the suppliers plugin.
 *
 * In development — being completed in TypeScript directly per the
 * user's direction. The Python suppliers app is incomplete; new
 * features added here are the source of truth going forward.
 */
import { Router, type Request, type Response } from 'express';
import type { AppContext } from './app-context.js';
import { listSuppliers, getSupplier } from './services/supplier-list.js';
import { runSuppliersHealthCheck } from './services/health-check.js';
import {
  getGlobalSupplierSettings,
  updateGlobalSupplierSettings,
} from './services/global-settings.js';
import { getAgedDebtSummary, getAgedDebtBySupplier } from './services/aged-debt.js';
import {
  listContacts,
  addContact,
  deleteContact,
} from './services/contacts.js';
import {
  listApprovedEmails,
  approveEmail,
  revokeEmail,
} from './services/approved-emails.js';
import {
  getSupplierConfig,
  saveSupplierConfig,
} from './services/supplier-config.js';
import {
  listStatements,
  getStatement,
} from './services/supplier-statements.js';
import {
  getAutomationConfig,
  saveAutomationConfig,
} from './services/automation-config.js';
import {
  getOnboardingState,
  listOnboardingStates,
  updateOnboardingState,
  type OnboardingStage,
} from './services/onboarding.js';
import {
  listRemittanceLog,
  recordRemittance,
} from './services/remittance-log.js';
import {
  listCommunications,
  recordCommunication,
  deleteCommunication,
  type CommunicationChannel,
} from './services/communications.js';
import { listChangeAudit, recordChange } from './services/change-audit.js';
import {
  listStatementLines,
  addStatementLines,
  updateStatementLineMatch,
  deleteStatementLines,
  listOperaOnlyItems,
  type MatchStatus,
  type NewStatementLine,
} from './services/statement-lines.js';
import {
  isEmailProcessed,
  recordProcessedEmail,
  listProcessedEmails,
} from './services/processed-emails.js';
import {
  listOverrides,
  recordOverride,
  deleteOverride,
} from './services/supplier-overrides.js';

export function createRouter(ctx: AppContext): Router {
  const router = Router();

  function getAppDb(req: Request, res: Response): import('knex').Knex | null {
    if (!ctx.db.app) {
      res.status(503).json({
        success: false,
        error: 'suppliers per-app database not provisioned for this tenant.',
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

  /** GET /api/suppliers/status — plugin liveness. */
  router.get('/api/suppliers/status', (_req, res) => {
    res.json({
      success: true,
      app: 'suppliers',
      tenant_id: ctx.tenantId,
      opera_type: ctx.operaType,
      message: 'In development. See docs/sam-rewrite/progress.md for status.',
    });
  });

  /**
   * GET /api/suppliers/settings
   *
   * Read global supplier-automation settings (per-tenant). Faithful
   * port of get_supplier_settings (apps/suppliers/api/routes.py
   * :2167-2210). Returns the full known-key set with defaults applied
   * to any unset values.
   */
  router.get('/api/suppliers/settings', async (_req, res) => {
    const appDb = ctx.db.app;
    if (!appDb) {
      res.status(503).json({
        success: false,
        error: 'suppliers per-app database not provisioned for this tenant.',
      });
      return;
    }
    try {
      const result = await getGlobalSupplierSettings(appDb);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Get supplier settings failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * POST /api/suppliers/settings
   *
   * Update global supplier-automation settings. Faithful port of
   * update_supplier_settings (apps/suppliers/api/routes.py
   * :2213-2278). Validates that follow_up_reminder_days exceeds
   * query_response_days (loads the missing value from the existing
   * settings when only one of the pair is supplied). Unknown keys
   * are skipped silently.
   *
   * Body: arbitrary key/value object; values are coerced to strings.
   */
  router.post('/api/suppliers/settings', async (req, res) => {
    const appDb = ctx.db.app;
    if (!appDb) {
      res.status(503).json({
        success: false,
        error: 'suppliers per-app database not provisioned for this tenant.',
      });
      return;
    }
    try {
      const body = (req.body ?? {}) as Record<string, unknown>;
      const result = await updateGlobalSupplierSettings(appDb, body as any);
      if (!result.success) {
        res.status(400).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Update supplier settings failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/suppliers/health-check
   *
   * Per-app data-integrity health check. Verifies supplier codes
   * referenced in our local data still exist in Opera pname.
   * Faithful port of `apps/suppliers/api/routes.py:135-156`.
   */
  router.get('/api/suppliers/health-check', async (req, res) => {
    const operaDb = getOperaDb(req, res);
    if (!operaDb) return;
    try {
      const result = await runSuppliersHealthCheck({
        operaDb,
        appDb: ctx.db.app ?? null,
      });
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Suppliers health-check failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /** GET /api/suppliers — list active suppliers from Opera pname. */
  router.get('/api/suppliers', async (req, res) => {
    const operaDb = getOperaDb(req, res);
    if (!operaDb) return;
    try {
      const includeDormant = req.query.include_dormant === 'true';
      const result = await listSuppliers(operaDb, { includeDormant });
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('List suppliers failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/suppliers/aged-debt — aged-bucket totals across all
   * active suppliers (Current 0-30 / 31-60 / 61-90 / Over 90).
   */
  router.get('/api/suppliers/aged-debt', async (req, res) => {
    const operaDb = getOperaDb(req, res);
    if (!operaDb) return;
    try {
      const result = await getAgedDebtSummary(operaDb);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Aged-debt summary failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/suppliers/aged-debt/by-supplier — per-supplier rows
   * with bucket breakdown. Suppliers ordered by total descending.
   */
  router.get('/api/suppliers/aged-debt/by-supplier', async (req, res) => {
    const operaDb = getOperaDb(req, res);
    if (!operaDb) return;
    try {
      const result = await getAgedDebtBySupplier(operaDb);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Aged-debt by-supplier failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/suppliers/:code/contacts — list extended contacts for
   * a supplier. Mounted before /api/suppliers/:code so the more
   * specific path matches first.
   */
  router.get('/api/suppliers/:code/contacts', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const code = String(req.params.code ?? '').trim();
      if (!code) {
        res.status(400).json({ success: false, error: 'Missing supplier code' });
        return;
      }
      const result = await listContacts(appDb, code);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('List contacts failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * POST /api/suppliers/:code/contacts — add an extended contact.
   *
   * Body: { contact_email: string, contact_name?: string, contact_role?: string }
   */
  router.post('/api/suppliers/:code/contacts', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const code = String(req.params.code ?? '').trim();
      if (!code) {
        res.status(400).json({ success: false, error: 'Missing supplier code' });
        return;
      }
      const body = (req.body ?? {}) as Record<string, unknown>;
      const result = await addContact(appDb, {
        supplier_code: code,
        contact_email: typeof body.contact_email === 'string' ? body.contact_email : '',
        contact_name: typeof body.contact_name === 'string' ? body.contact_name : '',
        contact_role: typeof body.contact_role === 'string' ? body.contact_role : '',
      });
      if (!result.success) {
        res.status(400).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Add contact failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * DELETE /api/suppliers/contacts/:contact_id — delete a contact by id.
   *
   * Mounted on /api/suppliers/contacts/:contact_id (NOT /:code/contacts/:id)
   * because the contact id is globally unique and the supplier code
   * isn't needed for the delete.
   */
  router.delete('/api/suppliers/contacts/:contact_id', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const id = Number(req.params.contact_id);
      if (!Number.isFinite(id)) {
        res.status(400).json({ success: false, error: 'Invalid contact_id' });
        return;
      }
      const result = await deleteContact(appDb, id);
      if (!result.success && result.error === 'Contact not found') {
        res.status(404).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Delete contact failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/suppliers/:code/approved-emails — list approved senders
   * for a supplier.
   */
  router.get('/api/suppliers/:code/approved-emails', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const code = String(req.params.code ?? '').trim();
      if (!code) {
        res.status(400).json({ success: false, error: 'Missing supplier code' });
        return;
      }
      const result = await listApprovedEmails(appDb, code);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('List approved emails failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * POST /api/suppliers/:code/approved-emails — approve a sender
   * for a supplier. Idempotent — re-approving returns the existing
   * record.
   *
   * Body: { email_address: string }
   */
  router.post('/api/suppliers/:code/approved-emails', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const code = String(req.params.code ?? '').trim();
      const body = (req.body ?? {}) as Record<string, unknown>;
      const email =
        typeof body.email_address === 'string' ? body.email_address.trim() : '';
      const result = await approveEmail(appDb, {
        supplier_code: code,
        email_address: email,
      });
      if (!result.success) {
        res.status(400).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Approve email failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * DELETE /api/suppliers/approved-emails/:record_id — revoke an
   * approved-email record by id.
   */
  router.delete('/api/suppliers/approved-emails/:record_id', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const id = Number(req.params.record_id);
      if (!Number.isFinite(id)) {
        res.status(400).json({ success: false, error: 'Invalid record_id' });
        return;
      }
      const result = await revokeEmail(appDb, id);
      if (!result.success && result.error?.includes('not found')) {
        res.status(404).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Revoke email failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/suppliers/:code/config — per-supplier configuration.
   * Returns merged-with-defaults config dict.
   */
  router.get('/api/suppliers/:code/config', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const code = String(req.params.code ?? '').trim();
      const result = await getSupplierConfig(appDb, code);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Get supplier config failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * PUT /api/suppliers/:code/config — replace per-supplier config.
   * Body: the full config JSON object.
   */
  router.put('/api/suppliers/:code/config', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const code = String(req.params.code ?? '').trim();
      const body = req.body;
      if (
        !body ||
        typeof body !== 'object' ||
        Array.isArray(body)
      ) {
        res
          .status(400)
          .json({ success: false, error: 'Request body must be a JSON object' });
        return;
      }
      const result = await saveSupplierConfig(appDb, {
        supplier_code: code,
        config: body as Record<string, unknown>,
      });
      if (!result.success) {
        res.status(400).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Save supplier config failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/suppliers/statements — list supplier statements with
   * optional supplier_code / from_date / to_date filters.
   *
   * Mounted at /api/suppliers/statements (not /:code/statements) so
   * the catch-all /:code route doesn't shadow it.
   */
  router.get('/api/suppliers/statements', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const supplierCode =
        typeof req.query.supplier_code === 'string' ? req.query.supplier_code : null;
      const fromDate =
        typeof req.query.from_date === 'string' ? req.query.from_date : null;
      const toDate =
        typeof req.query.to_date === 'string' ? req.query.to_date : null;
      const limit = req.query.limit ? Number(req.query.limit) : 100;
      const result = await listStatements(appDb, {
        supplierCode,
        fromDate,
        toDate,
        limit,
      });
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('List statements failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/suppliers/statements/:id — full statement detail
   * (header + lines + opera-only items).
   */
  router.get('/api/suppliers/statements/:id', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const id = Number(req.params.id);
      if (!Number.isFinite(id)) {
        res.status(400).json({ success: false, error: 'Invalid statement id' });
        return;
      }
      const result = await getStatement(appDb, id);
      if (!result.success) {
        res.status(404).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Get statement failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/suppliers/:code/automation — per-supplier automation
   * config (auto_process, frequency, matching_rules).
   */
  router.get('/api/suppliers/:code/automation', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const code = String(req.params.code ?? '').trim();
      const result = await getAutomationConfig(appDb, code);
      if (!result.success) {
        res.status(400).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Get automation-config failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * PUT /api/suppliers/:code/automation — partial-merge update of
   * automation config. Body fields are optional; only provided fields
   * overwrite. matching_rules MUST be a JSON object if present.
   */
  router.put('/api/suppliers/:code/automation', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const code = String(req.params.code ?? '').trim();
      const body = (req.body ?? {}) as Record<string, unknown>;
      const result = await saveAutomationConfig(appDb, {
        supplier_code: code,
        auto_process:
          typeof body.auto_process === 'boolean' ? body.auto_process : undefined,
        frequency: typeof body.frequency === 'string' ? body.frequency : undefined,
        matching_rules:
          body.matching_rules &&
          typeof body.matching_rules === 'object' &&
          !Array.isArray(body.matching_rules)
            ? (body.matching_rules as Record<string, unknown>)
            : undefined,
      });
      if (!result.success) {
        res.status(400).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Save automation-config failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/suppliers/onboarding — list onboarding states across
   * all suppliers, optionally filtered by stage.
   *
   * Mounted at /api/suppliers/onboarding (not /:code/onboarding) so
   * the catch-all /:code route doesn't shadow it.
   */
  router.get('/api/suppliers/onboarding', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const stage =
        typeof req.query.stage === 'string'
          ? (req.query.stage as OnboardingStage)
          : undefined;
      const result = await listOnboardingStates(appDb, { stage });
      if (!result.success) {
        res.status(400).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('List onboarding states failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/suppliers/:code/onboarding — get onboarding state for
   * one supplier (returns defaults when no row exists).
   */
  router.get('/api/suppliers/:code/onboarding', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const code = String(req.params.code ?? '').trim();
      const result = await getOnboardingState(appDb, code);
      if (!result.success) {
        res.status(400).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Get onboarding state failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * PUT /api/suppliers/:code/onboarding — update onboarding state
   * for one supplier (partial-merge of stage and notes).
   */
  router.put('/api/suppliers/:code/onboarding', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const code = String(req.params.code ?? '').trim();
      const body = (req.body ?? {}) as Record<string, unknown>;
      const result = await updateOnboardingState(appDb, {
        supplier_code: code,
        stage: typeof body.stage === 'string' ? body.stage : undefined,
        notes: typeof body.notes === 'string' ? body.notes : undefined,
      });
      if (!result.success) {
        res.status(400).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Update onboarding state failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/suppliers/remittance — list remittance log entries.
   * Optional filters: supplier_code, from_date, to_date.
   *
   * Mounted before /api/suppliers/:code so the static path matches first.
   */
  router.get('/api/suppliers/remittance', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const supplierCode =
        typeof req.query.supplier_code === 'string'
          ? req.query.supplier_code
          : null;
      const fromDate =
        typeof req.query.from_date === 'string' ? req.query.from_date : null;
      const toDate =
        typeof req.query.to_date === 'string' ? req.query.to_date : null;
      const limit = req.query.limit ? Number(req.query.limit) : 100;
      const result = await listRemittanceLog(appDb, {
        supplierCode,
        fromDate,
        toDate,
        limit,
      });
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('List remittance log failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * POST /api/suppliers/:code/remittance — record a remittance send.
   * Body: { to_address, subject, amount }
   */
  router.post('/api/suppliers/:code/remittance', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const code = String(req.params.code ?? '').trim();
      const body = (req.body ?? {}) as Record<string, unknown>;
      const result = await recordRemittance(appDb, {
        supplier_code: code,
        to_address: typeof body.to_address === 'string' ? body.to_address : '',
        subject: typeof body.subject === 'string' ? body.subject : '',
        amount: typeof body.amount === 'number' ? body.amount : NaN,
      });
      if (!result.success) {
        res.status(400).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Record remittance failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/suppliers/communications
   *
   * List entries from supplier_communications, optionally filtered by
   * supplier_code, channel ('email'|'phone'|'portal'), and date range.
   */
  router.get('/api/suppliers/communications', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const result = await listCommunications(appDb, {
        supplierCode:
          typeof req.query.supplier_code === 'string'
            ? req.query.supplier_code
            : null,
        channel:
          typeof req.query.channel === 'string'
            ? (req.query.channel as CommunicationChannel)
            : null,
        fromDate:
          typeof req.query.from_date === 'string'
            ? req.query.from_date
            : null,
        toDate:
          typeof req.query.to_date === 'string' ? req.query.to_date : null,
        limit: req.query.limit ? Number(req.query.limit) : undefined,
      });
      if (!result.success) {
        res.status(400).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('List communications failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * POST /api/suppliers/:code/communications
   *
   * Record a new entry in supplier_communications. Body:
   *   { channel: 'email'|'phone'|'portal', subject?: string,
   *     content?: string, sent_at?: string }
   */
  router.post('/api/suppliers/:code/communications', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const code = String(req.params.code ?? '').trim();
      const body = (req.body ?? {}) as {
        channel?: string;
        subject?: string;
        content?: string;
        sent_at?: string;
      };
      const result = await recordCommunication(appDb, {
        supplier_code: code,
        channel: String(body.channel ?? ''),
        subject: body.subject,
        content: body.content,
        sent_at: body.sent_at,
      });
      if (!result.success) {
        res.status(400).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Record communication failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * DELETE /api/suppliers/communications/:id
   *
   * Remove a single entry from supplier_communications by id.
   */
  router.delete('/api/suppliers/communications/:id', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const id = Number(req.params.id);
      const result = await deleteCommunication(appDb, id);
      if (!result.success) {
        res.status(400).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Delete communication failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/suppliers/change-audit
   *
   * List entries from supplier_change_audit, optionally filtered by
   * supplier_code, changed_field, and date range.
   */
  router.get('/api/suppliers/change-audit', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const result = await listChangeAudit(appDb, {
        supplierCode:
          typeof req.query.supplier_code === 'string'
            ? req.query.supplier_code
            : null,
        changedField:
          typeof req.query.changed_field === 'string'
            ? req.query.changed_field
            : null,
        fromDate:
          typeof req.query.from_date === 'string' ? req.query.from_date : null,
        toDate:
          typeof req.query.to_date === 'string' ? req.query.to_date : null,
        limit: req.query.limit ? Number(req.query.limit) : undefined,
      });
      if (!result.success) {
        res.status(400).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('List change-audit failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * POST /api/suppliers/:code/change-audit
   *
   * Manually record a change-audit entry. Body:
   *   { changed_field: string, old_value?: any, new_value?: any,
   *     changed_by?: string }
   *
   * NB: most write services (automation-config, onboarding, etc.)
   * SHOULD call recordChange()/recordChangeIfDifferent() internally.
   * This endpoint exists for ad-hoc manual annotations.
   */
  router.post('/api/suppliers/:code/change-audit', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const code = String(req.params.code ?? '').trim();
      const body = (req.body ?? {}) as {
        changed_field?: string;
        old_value?: unknown;
        new_value?: unknown;
        changed_by?: string;
      };
      const result = await recordChange(appDb, {
        supplier_code: code,
        changed_field: String(body.changed_field ?? ''),
        old_value: body.old_value,
        new_value: body.new_value,
        changed_by: body.changed_by ?? req.user?.userId,
      });
      if (!result.success) {
        res.status(400).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Record change-audit failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/suppliers/statements/:id/lines
   *
   * List the line items for a statement. Returns the lines plus
   * aggregates (total_amount, matched_count, unmatched_count,
   * disputed_count) so the UI can render a summary.
   */
  router.get('/api/suppliers/statements/:id/lines', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const id = Number(req.params.id);
      const result = await listStatementLines(appDb, id);
      if (!result.success) {
        res.status(400).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('List statement lines failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * POST /api/suppliers/statements/:id/lines
   *
   * Bulk-insert lines for a statement (used by extract-statement when
   * AI extracts items from PDF). Body: { lines: NewStatementLine[] }.
   */
  router.post('/api/suppliers/statements/:id/lines', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const id = Number(req.params.id);
      const body = (req.body ?? {}) as { lines?: NewStatementLine[] };
      const result = await addStatementLines(appDb, id, body.lines ?? []);
      if (!result.success) {
        res.status(400).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Add statement lines failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * PATCH /api/suppliers/statement-lines/:line_id
   *
   * Update match_status / matched_opera_ref on a single line. Used by
   * the matching pipeline and by manual operator overrides.
   * Body: { match_status?: 'matched'|'unmatched'|'disputed',
   *         matched_opera_ref?: string|null }
   */
  router.patch('/api/suppliers/statement-lines/:line_id', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const lineId = Number(req.params.line_id);
      const body = (req.body ?? {}) as {
        match_status?: MatchStatus;
        matched_opera_ref?: string | null;
      };
      const result = await updateStatementLineMatch(appDb, lineId, body);
      if (!result.success) {
        res.status(400).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Update statement line failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * DELETE /api/suppliers/statements/:id/lines
   *
   * Remove all lines for a statement (used when re-extracting).
   */
  router.delete('/api/suppliers/statements/:id/lines', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const id = Number(req.params.id);
      const result = await deleteStatementLines(appDb, id);
      if (!result.success) {
        res.status(400).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Delete statement lines failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/suppliers/statements/:id/opera-only
   *
   * List statement_opera_only items — lines that appear in the
   * supplier statement but have no corresponding Opera ptran row
   * (typically missing invoices the supplier is asking us about).
   */
  router.get('/api/suppliers/statements/:id/opera-only', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const id = Number(req.params.id);
      const result = await listOperaOnlyItems(appDb, id);
      if (!result.success) {
        res.status(400).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('List opera-only items failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/suppliers/processed-emails
   *
   * List processed-email dedup entries (audit trail of which emails
   * have been extracted into supplier statements).
   */
  router.get('/api/suppliers/processed-emails', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const result = await listProcessedEmails(appDb, {
        supplierCode:
          typeof req.query.supplier_code === 'string'
            ? req.query.supplier_code
            : null,
        fromDate:
          typeof req.query.from_date === 'string' ? req.query.from_date : null,
        toDate:
          typeof req.query.to_date === 'string' ? req.query.to_date : null,
        limit: req.query.limit ? Number(req.query.limit) : undefined,
      });
      if (!result.success) {
        res.status(400).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('List processed-emails failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * POST /api/suppliers/processed-emails
   *
   * Record a Graph message as processed (called by the scan-emails
   * flow after a successful statement extraction). Body:
   *   { message_id: string, supplier_code?: string, subject?: string }
   *
   * Idempotent — a duplicate message_id returns success=true with
   * duplicate=true.
   */
  router.post('/api/suppliers/processed-emails', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const body = (req.body ?? {}) as {
        message_id?: string;
        supplier_code?: string;
        subject?: string;
      };
      const result = await recordProcessedEmail(appDb, {
        message_id: String(body.message_id ?? ''),
        supplier_code: body.supplier_code,
        subject: body.subject,
      });
      if (!result.success) {
        res.status(400).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Record processed-email failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/suppliers/processed-emails/:message_id/exists
   *
   * Fast existence check used by the scan-emails dedup gate.
   * Returns { exists: boolean }.
   */
  router.get(
    '/api/suppliers/processed-emails/:message_id/exists',
    async (req, res) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const messageId = String(req.params.message_id ?? '').trim();
        const exists = await isEmailProcessed(appDb, messageId);
        res.json({ success: true, message_id: messageId, exists });
      } catch (err: any) {
        ctx.logger.error('Check processed-email failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * GET /api/suppliers/statements/:id/overrides
   *
   * List operator overrides (accept / reject / dispute) recorded
   * against a statement.
   */
  router.get('/api/suppliers/statements/:id/overrides', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const id = Number(req.params.id);
      const result = await listOverrides(appDb, id);
      if (!result.success) {
        res.status(400).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('List overrides failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * POST /api/suppliers/statements/:id/overrides
   *
   * Record a new override for a statement (statement-level) or a
   * specific line (line_id provided). Body:
   *   { line_id?: number, override_type: 'accept'|'reject'|'dispute',
   *     reason?: string }
   */
  router.post('/api/suppliers/statements/:id/overrides', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const id = Number(req.params.id);
      const body = (req.body ?? {}) as {
        line_id?: number;
        override_type?: string;
        reason?: string;
      };
      const result = await recordOverride(appDb, {
        statement_id: id,
        line_id: body.line_id ?? null,
        override_type: String(body.override_type ?? ''),
        reason: body.reason,
      });
      if (!result.success) {
        res.status(400).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Record override failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * DELETE /api/suppliers/overrides/:id
   *
   * Remove a previously-recorded override.
   */
  router.delete('/api/suppliers/overrides/:id', async (req, res) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const id = Number(req.params.id);
      const result = await deleteOverride(appDb, id);
      if (!result.success) {
        res.status(400).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Delete override failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/suppliers/:code — single supplier detail. Mounted LAST
   * so specific paths above (status, aged-debt, contacts, etc.)
   * match first.
   */
  router.get('/api/suppliers/:code', async (req, res) => {
    const operaDb = getOperaDb(req, res);
    if (!operaDb) return;
    try {
      const code = String(req.params.code ?? '').trim();
      if (!code) {
        res.status(400).json({ success: false, error: 'Missing supplier code' });
        return;
      }
      const result = await getSupplier(operaDb, code);
      if (!result.success) {
        res.status(404).json(result);
        return;
      }
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Get supplier failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  // Future endpoints (designed during the TS port, not translated):
  //   POST /api/suppliers/scan-emails        — scan SAM mailbox
  //   POST /api/suppliers/extract-statement  — Gemini extract line items
  //   POST /api/suppliers/reconcile          — reconcile statement vs ptran
  //   GET  /api/suppliers/:code/contacts     — extended contacts
  //   POST /api/suppliers/onboard            — onboarding flow
  //   POST /api/suppliers/remittance         — generate + send remittance

  return router;
}
