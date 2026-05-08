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
