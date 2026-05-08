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

export function createRouter(ctx: AppContext): Router {
  const router = Router();

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
   * GET /api/suppliers/:code — single supplier detail. Mounted LAST
   * so specific paths above (status, aged-debt, etc.) match first.
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
