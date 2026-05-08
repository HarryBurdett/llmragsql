/**
 * Express router for the GoCardless plugin.
 *
 * Mounts foundational endpoints. Many more remain to be ported from
 * the Python implementation — see docs/sam-rewrite/progress.md.
 */
import { Router, type Request, type Response } from 'express';
import type { AppContext } from './app-context.js';
import {
  loadSettings,
  saveSettings,
  maskSettingsForResponse,
  mergeSettingsUpdate,
  type GoCardlessSettings,
} from './services/settings.js';
import { runHealthCheck } from './services/health-check.js';
import {
  getBatchTypes,
  getNominalAccounts,
  getPaymentTypes,
  getVatCodes,
  getBankAccounts,
  getImportConfig,
  getSetupStatus,
} from './services/lookups.js';
import { getImportHistory } from './services/import-history.js';

export function createRouter(ctx: AppContext): Router {
  const router = Router();

  function getAppDb(req: Request, res: Response): import('knex').Knex | null {
    if (!ctx.db.app) {
      res.status(503).json({
        success: false,
        error: 'GoCardless per-app database not provisioned for this tenant.',
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
   * GET /api/gocardless/settings
   *
   * Returns the GoCardless settings dict with secrets masked. Faithful
   * port of `get_gocardless_settings` in the Python codebase.
   */
  router.get('/api/gocardless/settings', async (_req: Request, res: Response) => {
    const appDb = getAppDb(_req, res);
    if (!appDb) return;
    try {
      const settings = await loadSettings(appDb);
      const masked = maskSettingsForResponse(settings);
      res.json({ success: true, settings: masked });
    } catch (err: any) {
      ctx.logger.error('Failed to load GoCardless settings', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * POST /api/gocardless/settings
   *
   * Merges request body into existing settings and saves. Faithful port
   * of `save_gocardless_settings` — preserves api_access_token and
   * partner_client_secret if not explicitly provided.
   */
  router.post('/api/gocardless/settings', async (req: Request, res: Response) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const body = (req.body ?? {}) as Record<string, unknown>;
      const existing = await loadSettings(appDb);
      const merged: GoCardlessSettings = mergeSettingsUpdate(existing, body);
      const ok = await saveSettings(appDb, merged);
      if (ok) {
        res.json({ success: true, message: 'Settings saved' });
      } else {
        res.status(500).json({ success: false, error: 'Failed to save settings' });
      }
    } catch (err: any) {
      ctx.logger.error('Failed to save GoCardless settings', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/gocardless/health-check
   *
   * Per-app data-integrity health check. Faithful port of
   * `gocardless_health_check`.
   */
  router.get('/api/gocardless/health-check', async (req: Request, res: Response) => {
    const operaDb = getOperaDb(req, res);
    if (!operaDb) return;
    try {
      const appDb = ctx.db.app;
      let settings: GoCardlessSettings | null = null;
      if (appDb) {
        try {
          settings = await loadSettings(appDb);
        } catch (err) {
          ctx.logger.debug('GoCardless settings not loadable', err);
        }
      }
      const result = await runHealthCheck({
        operaDb,
        appDb,
        settings,
      });
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('GoCardless health-check failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/gocardless/setup-status
   *
   * Reports whether GoCardless is configured (api_access_token > 10 chars).
   * Used by the launcher to decide whether to redirect to signup.
   */
  router.get('/api/gocardless/setup-status', async (_req: Request, res: Response) => {
    try {
      const result = await getSetupStatus(ctx.db.app);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Setup status failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/gocardless/batch-types
   *
   * Returns the available batched receipt types from Opera (atype where
   * ay_type='R' AND ay_batched=1). Recommends the first one with
   * 'gocardless' in its description.
   */
  router.get('/api/gocardless/batch-types', async (req: Request, res: Response) => {
    const operaDb = getOperaDb(req, res);
    if (!operaDb) return;
    try {
      const result = await getBatchTypes(operaDb);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Batch types fetch failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/gocardless/nominal-accounts
   *
   * Returns the nominal accounts dropdown list from nacnt (excluding
   * Z-prefixed system accounts).
   */
  router.get('/api/gocardless/nominal-accounts', async (req: Request, res: Response) => {
    const operaDb = getOperaDb(req, res);
    if (!operaDb) return;
    try {
      const result = await getNominalAccounts(operaDb);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Nominal accounts fetch failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/gocardless/payment-types
   *
   * Returns nominal payment types (atype where ay_type='P' AND not batched).
   */
  router.get('/api/gocardless/payment-types', async (req: Request, res: Response) => {
    const operaDb = getOperaDb(req, res);
    if (!operaDb) return;
    try {
      const result = await getPaymentTypes(operaDb);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Payment types fetch failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/gocardless/vat-codes
   *
   * Returns the VAT codes from ztax with applicable rates for the given
   * date. Used for the fees-VAT split.
   */
  router.get('/api/gocardless/vat-codes', async (req: Request, res: Response) => {
    const operaDb = getOperaDb(req, res);
    if (!operaDb) return;
    try {
      const asOfDate =
        typeof req.query.as_of_date === 'string' ? req.query.as_of_date : null;
      const result = await getVatCodes(operaDb, asOfDate);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('VAT codes fetch failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/gocardless/bank-accounts
   *
   * Returns Opera bank accounts for dropdown selection.
   */
  router.get('/api/gocardless/bank-accounts', async (req: Request, res: Response) => {
    const operaDb = getOperaDb(req, res);
    if (!operaDb) return;
    try {
      const result = await getBankAccounts(operaDb);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Bank accounts fetch failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/gocardless/import-config
   *
   * Consolidated endpoint returning batch_types + nominal_accounts +
   * vat_codes in a single response. Faithful port of
   * `get_gocardless_import_config`.
   */
  router.get('/api/gocardless/import-config', async (req: Request, res: Response) => {
    const operaDb = getOperaDb(req, res);
    if (!operaDb) return;
    try {
      const asOfDate =
        typeof req.query.as_of_date === 'string' ? req.query.as_of_date : null;
      const result = await getImportConfig(operaDb, asOfDate);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Import config fetch failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/gocardless/import-history
   *
   * Past GoCardless batches imported to Opera. Faithful port of
   * `get_gocardless_import_history`. Enriches payment records with
   * Opera customer names (sname) and GC mandate customer names.
   */
  router.get('/api/gocardless/import-history', async (req: Request, res: Response) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    // Opera DB is optional — if missing the history is returned without
    // Opera-name enrichment.
    let operaDb: import('knex').Knex | null = null;
    const company = req.operaCompany;
    if (company) {
      operaDb = ctx.db.getCompanyDb(company);
    }
    try {
      const limit = req.query.limit ? Number(req.query.limit) : 50;
      const fromDate = typeof req.query.from_date === 'string' ? req.query.from_date : null;
      const toDate = typeof req.query.to_date === 'string' ? req.query.to_date : null;
      const result = await getImportHistory(appDb, operaDb, {
        limit,
        fromDate,
        toDate,
      });
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Import history fetch failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  // Many more endpoints to port from apps/gocardless/api/routes.py:
  //   /api/gocardless/setup-status       — wizard / onboarding state
  //   /api/gocardless/scan-emails        — IMAP/Graph scan via SAM email service
  //   /api/gocardless/preview-batch      — match payments to Opera customers
  //   /api/gocardless/import             — post sales receipts to Opera
  //   /api/gocardless/api-payouts        — query GoCardless API directly
  //   /api/gocardless/import-history     — view past imports
  //   /api/gocardless/remittance/*       — generate / send remittance emails
  //   /api/gocardless/partner/*          — partner portal flows
  //   /api/gocardless/update-subscription-tags
  //   /api/gocardless/nominal-accounts   — list of valid Opera nominal codes
  //   /api/gocardless/vat-codes          — list of VAT codes for fees split
  //   ... 100+ endpoints. Each ports independently.

  return router;
}
