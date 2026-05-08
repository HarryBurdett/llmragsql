/**
 * Express router for balance-check.
 *
 * Mounts the reconciliation endpoints. Each handler reads the per-request
 * company from `req.operaCompany` (set by SAM's `resolveCompany`
 * middleware) and resolves the matching Knex pool from the AppContext.
 *
 * NB: per CLAUDE.md, all endpoints here are read-only against Opera SQL.
 * No table writes, no schema changes.
 */
import { Router, type Request, type Response } from 'express';
import type { AppContext } from './app-context.js';
import { reconcileSummary } from './services/reconcile-summary.js';

export function createRouter(ctx: AppContext): Router {
  const router = Router();

  /**
   * GET /api/reconcile/summary
   *
   * Faithful port of `reconcile_summary()` in
   * `apps/balance_check/api/routes.py`.
   *
   * Returns a quick at-a-glance reconciliation status across the four
   * checks (debtors, creditors, cashbook, VAT). Each check independently
   * succeeds or fails — a failure in one doesn't break the others.
   */
  router.get('/api/reconcile/summary', async (req: Request, res: Response) => {
    const company = req.operaCompany;
    if (!company) {
      res.status(400).json({
        success: false,
        error: 'No Opera company in context. SAM should set X-Opera-Company.',
      });
      return;
    }

    const db = ctx.db.getCompanyDb(company);
    if (!db) {
      res.status(503).json({
        success: false,
        error: `Opera SQL connection not available for company ${company}.`,
      });
      return;
    }

    try {
      const summary = await reconcileSummary(db);
      res.json(summary);
    } catch (err: any) {
      ctx.logger.error('Reconciliation summary failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  // Future endpoints (port next):
  //   GET /api/reconcile/creditors
  //   GET /api/reconcile/debtors
  //   GET /api/reconcile/vat
  //   GET /api/reconcile/cashbook
  //   GET /api/reconcile/trial-balance
  //   GET /api/reconcile/vat/diagnostic
  //   GET /api/reconcile/vat/variance-drilldown

  return router;
}
