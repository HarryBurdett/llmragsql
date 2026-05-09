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
import { reconcileCreditors } from './services/reconcile-creditors.js';
import { reconcileDebtors } from './services/reconcile-debtors.js';
import { reconcileTrialBalance } from './services/reconcile-trial-balance.js';
import { vatDiagnostic } from './services/vat-diagnostic.js';
import { reconcileVat } from './services/reconcile-vat.js';
import { vatVarianceDrilldown } from './services/vat-variance-drilldown.js';

export function createRouter(ctx: AppContext): Router {
  const router = Router();

  // Opera-3 mirror routes — see bank-reconcile/router.ts for the
  // rationale. /api/opera3/reconcile/* resolves to the same handlers
  // as the canonical /api/reconcile/* routes; ctx.db.getCompanyDb()
  // returns the right (FoxPro/Knex) connection for opera-3 tenants.
  router.use((req, _res, next) => {
    if (req.url.startsWith('/api/opera3/')) {
      req.url = '/api/' + req.url.slice('/api/opera3/'.length);
      (req as unknown as { operaMirror?: boolean }).operaMirror = true;
    }
    next();
  });

  /** Resolve the per-company Opera pool, with consistent error handling. */
  function resolveCompanyDb(req: Request, res: Response): import('knex').Knex | null {
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
   * GET /api/reconcile/summary
   *
   * Faithful port of `reconcile_summary()` in the Python codebase.
   * At-a-glance reconciliation status across debtors, creditors,
   * cashbook, VAT — each runs independently.
   */
  router.get('/api/reconcile/summary', async (req: Request, res: Response) => {
    const db = resolveCompanyDb(req, res);
    if (!db) return;
    try {
      const result = await reconcileSummary(db);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Reconciliation summary failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/reconcile/creditors
   *
   * Faithful port of `reconcile_creditors()` in the Python codebase.
   * Reconciles Purchase Ledger to Creditors Control Account, including
   * variance analysis with NL ↔ PL transaction matching.
   */
  router.get('/api/reconcile/creditors', async (req: Request, res: Response) => {
    const db = resolveCompanyDb(req, res);
    if (!db) return;
    try {
      const result = await reconcileCreditors(db);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Creditors reconciliation failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/reconcile/debtors
   *
   * Faithful port of `reconcile_debtors()` in the Python codebase.
   * Reconciles Sales Ledger to Debtors Control Account, including
   * variance analysis with NL ↔ SL transaction matching.
   */
  router.get('/api/reconcile/debtors', async (req: Request, res: Response) => {
    const db = resolveCompanyDb(req, res);
    if (!db) return;
    try {
      const result = await reconcileDebtors(db);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Debtors reconciliation failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/reconcile/trial-balance
   *
   * Faithful port of `reconcile_trial_balance()`. Verifies the nominal
   * ledger as a whole balances (debits = credits). Returns all nominal
   * accounts with B/F, current movements, and closing balances.
   */
  router.get('/api/reconcile/trial-balance', async (req: Request, res: Response) => {
    const db = resolveCompanyDb(req, res);
    if (!db) return;
    try {
      const result = await reconcileTrialBalance(db);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Trial balance check failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/reconcile/vat/diagnostic
   *
   * Faithful port of `vat_diagnostic()`. Reports row counts and date
   * ranges for the VAT tables (zvtran, nvat, ztax, ntran) — used to
   * confirm data availability before running the main VAT reconcile.
   */
  router.get('/api/reconcile/vat/diagnostic', async (req: Request, res: Response) => {
    const db = resolveCompanyDb(req, res);
    if (!db) return;
    try {
      const result = await vatDiagnostic(db);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('VAT diagnostic failed', err);
      res.status(500).json({ error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/reconcile/vat
   *
   * Faithful port of `reconcile_vat()`. Reconciles VAT accounts —
   * compares VAT liability in NL to VAT transactions across quarter
   * (uncommitted zvtran + committed nvat) and YTD (nvat totals).
   */
  router.get('/api/reconcile/vat', async (req: Request, res: Response) => {
    const db = resolveCompanyDb(req, res);
    if (!db) return;
    try {
      const result = await reconcileVat(db);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('VAT reconciliation failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/reconcile/vat/variance-drilldown
   *
   * Faithful port of `vat_variance_drilldown()`. Drill-down report
   * showing uncommitted VAT by period, NL movements by period,
   * largest transactions, and a variance summary with explanations.
   */
  router.get('/api/reconcile/vat/variance-drilldown', async (req: Request, res: Response) => {
    const db = resolveCompanyDb(req, res);
    if (!db) return;
    try {
      const result = await vatVarianceDrilldown(db);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('VAT variance drilldown failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  return router;
}
