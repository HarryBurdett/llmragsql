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
import { skipPayout } from './services/skip-payout.js';
import { createClientFromSettings } from './services/gocardless-api.js';
import { searchReceipts } from './services/receipt-search.js';
import {
  clearImportHistory,
  deleteImportRecord,
} from './services/import-history-delete.js';
import { updateSubscriptionTags } from './services/subscription-tags.js';
import { getPaymentStats } from './services/payment-stats.js';
import {
  matchCustomersWithDuplicateCheck,
  type PaymentInput,
} from './services/match-customers.js';
import {
  revalidateBatches,
  type BatchInput,
} from './services/revalidate-batches.js';
import {
  getPartnerConfig,
  getLatestPartnerSignup,
  getAllMerchantSignups,
  partnerAdminAuth,
  setPartnerAdminPassword,
  updateMerchantAppUrl,
  activateMerchant,
  deployToken,
  initiatePartnerSignup,
  handlePartnerCallback,
  partnerCallbackHtml,
} from './services/partner.js';
import { archiveGocardlessEmail } from './services/archive-email.js';
import {
  listPaymentRequests,
  getPaymentRequest,
  cancelPaymentRequest,
  syncPaymentStatuses,
} from './services/payment-requests.js';
import {
  listSubscriptions,
  getSubscription,
  pauseSubscription,
  resumeSubscription,
  cancelSubscription,
  updateSubscriptionDetails,
  linkSubscriptionToDocument,
  unlinkSubscriptionFromDocument,
  syncSubscriptionFromOpera,
  syncSubscriptionsFromGocardless,
} from './services/subscriptions.js';
import {
  listMandates,
  listUnlinkedMandates,
  cancelMandate,
  unlinkMandate,
  linkMandate,
  syncMandatesFromGocardless,
  type OperaGcCustomer,
} from './services/mandates.js';
import {
  listMandateSetups,
  cancelMandateSetup,
} from './services/mandate-setups.js';
import { getEligibleCustomers } from './services/eligible-customers.js';
import { getCustomerEmail } from './services/customer-email.js';
import { getRepeatDocuments } from './services/repeat-documents.js';
import { getCollectableInvoices } from './services/collectable-invoices.js';
import {
  requestPayment,
  requestBulkPayments,
  type OperaSnapshot,
  type RequestPaymentInput,
} from './services/request-payment.js';
import {
  validatePostingPeriod,
  getCurrentPeriodInfo,
} from '@sqlrag/sam-shared';

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

  /**
   * POST /api/gocardless/skip-payout
   *
   * Record a payout to history without importing — used for foreign-currency,
   * already-manually-entered, or duplicate payouts. Faithful port of
   * `skip_gocardless_payout`.
   */
  router.post('/api/gocardless/skip-payout', async (req: Request, res: Response) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const q = req.query;
      const body = (req.body ?? null) as Array<Record<string, unknown>> | null;
      const result = await skipPayout(appDb, {
        payoutId: String(q.payout_id ?? ''),
        bankReference: String(q.bank_reference ?? ''),
        grossAmount: Number(q.gross_amount ?? 0),
        currency: typeof q.currency === 'string' ? q.currency : 'GBP',
        paymentCount: q.payment_count ? Number(q.payment_count) : 0,
        reason: typeof q.reason === 'string' ? q.reason : 'manual',
        fxAmount: q.fx_amount ? Number(q.fx_amount) : null,
        payments: Array.isArray(body) ? (body as any) : undefined,
      });
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Skip payout failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * POST /api/gocardless/test-api
   *
   * Test the saved GoCardless API token by hitting GET /creditors.
   * Faithful port of `test_gocardless_api`.
   */
  router.post('/api/gocardless/test-api', async (req: Request, res: Response) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const settings = await loadSettings(appDb);
      const client = createClientFromSettings(settings);
      if (!client) {
        res.json({ success: false, error: 'No API access token configured' });
        return;
      }
      const result = await client.testConnection();
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('GoCardless test-api failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * POST /api/gocardless/archive-email
   *
   * Mark a GoCardless email as already-in-Opera and (when SAM's email
   * service exposes the capability) move it to an archive folder.
   * Faithful port of archive_gocardless_email (routes.py:3503-3574).
   *
   * Query params:
   *   - email_id (required)
   *   - archive_folder (default 'Archive/GoCardless')
   *
   * NB: SAM's emailIngest service doesn't currently expose moveEmail.
   * The DB tracking happens regardless; the move reports
   * 'provider_not_available' until that capability lands. The
   * tracking row alone is enough to keep the email out of future
   * scans (which is the primary purpose).
   */
  router.post(
    '/api/gocardless/archive-email',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const emailId = Number(req.query.email_id);
        const archiveFolder = String(
          req.query.archive_folder ?? 'Archive/GoCardless',
        );
        const result = await archiveGocardlessEmail(
          appDb,
          { emailId, archiveFolder },
          ctx.emailIngest ?? null,
        );
        if (!result.success) {
          res.status(400).json(result);
          return;
        }
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Archive email failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * GET /api/gocardless/test-data
   *
   * Returns a hard-coded sample GoCardless payout dataset (the
   * Intsys-extracted figures from the gocardless.png screenshot used
   * during development). Faithful port of get_gocardless_test_data
   * (apps/gocardless/api/routes.py:191-224).
   *
   * Used by the frontend dev playground when the user wants to
   * exercise the matching/import UI without a real payout email.
   */
  router.get('/api/gocardless/test-data', async (_req: Request, res: Response) => {
    res.json({
      success: true,
      payment_count: 18,
      gross_amount: 29869.8,
      gocardless_fees: -118.31,
      vat_on_fees: -19.73,
      net_amount: 29751.49,
      bank_reference: 'INTSYSUKLTD-KN3CMJ',
      payments: [
        { customer_name: 'Deep Blue Restaurantes Ltd', description: 'Intsys INV26362,26363', amount: 7380.0, invoice_refs: ['INV26362', 'INV26363'] },
        { customer_name: 'Medimpex UK Ltd', description: 'Intsys INV26365', amount: 1530.0, invoice_refs: ['INV26365'] },
        { customer_name: 'The Prospect Trust', description: 'Intsys INV', amount: 3000.0, invoice_refs: [] },
        { customer_name: 'SMCP UK Limited', description: 'Intsys INV26374,26375', amount: 1320.0, invoice_refs: ['INV26374', 'INV26375'] },
        { customer_name: 'Vectair Systems Limited', description: 'Intsys INV26378', amount: 8398.8, invoice_refs: ['INV26378'] },
        { customer_name: 'Jackson Lifts', description: 'Intsys Opera 3 Support', amount: 123.0, invoice_refs: [] },
        { customer_name: 'Vectair Systems Limited', description: 'Opera SE Toolkit', amount: 109.2, invoice_refs: [] },
        { customer_name: 'A WARNE & CO LTD', description: 'Intsys Data Connector', amount: 168.0, invoice_refs: [] },
        { customer_name: 'Physique Management Ltd', description: 'Intsys Pegasus Support', amount: 551.4, invoice_refs: [] },
        { customer_name: 'Ormiston Wire Ltd', description: 'Intsys Opera 3 Support', amount: 90.0, invoice_refs: [] },
        { customer_name: 'Totality GCS Ltd', description: 'Intsys Pegasus Support', amount: 240.0, invoice_refs: [] },
        { customer_name: 'Red Band Chemical Co Ltd T/A Lindsay & Gilmour', description: 'Intsys Pegasus Upgrade Plan', amount: 74.4, invoice_refs: [] },
        { customer_name: 'P Flannery Plant Hire (Oval) Ltd', description: 'Intsys Pegasus Upgrade Plan', amount: 78.0, invoice_refs: [] },
        { customer_name: 'Harro Foods Limited', description: 'Intsys Opera 3 Sales Website', amount: 5607.0, invoice_refs: [] },
        { customer_name: 'Physique Management Ltd', description: 'Intsys Data Connector', amount: 168.0, invoice_refs: [] },
        { customer_name: 'Nisbets Limited', description: 'Intsys Opera 3 Licence Subs', amount: 540.0, invoice_refs: [] },
        { customer_name: 'Vectair Systems Limited', description: 'Intsys Pegasus WEBLINK', amount: 192.0, invoice_refs: [] },
        { customer_name: 'ST Astier Limited', description: 'Intsys CIS Support', amount: 300.0, invoice_refs: [] },
      ],
    });
  });

  /**
   * GET /api/gocardless/api-payouts
   *
   * Fetch payouts directly from the GoCardless REST API. Faithful slim
   * port of `get_gocardless_api_payouts` (apps/gocardless/api/routes.py
   * lines 1952-1989). Query params:
   *   - status:    payout status filter (default 'paid')
   *   - limit:     number of payouts (default 20)
   *   - days_back: lookback window (default settings.payout_lookback_days
   *                or 30)
   *
   * NB: the Python version then enriches each payout with full payment
   * details, dedupes against Opera + import history, and applies
   * period-closed filtering. That enrichment depends on
   * OperaSQLImport.get_home_currency, get_payout_with_payments,
   * email_storage.is_gocardless_payout_imported, and _is_period_closed
   * — none of which are ported yet. This endpoint returns the raw
   * payouts array; the enrichment is added in a later session once
   * the helper services land.
   */
  router.get('/api/gocardless/api-payouts', async (req: Request, res: Response) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const settings = await loadSettings(appDb);
      const accessToken = settings.api_access_token ?? '';
      if (!accessToken) {
        res.json({
          success: false,
          error:
            'No API access token configured. Go to Settings to add your GoCardless API credentials.',
        });
        return;
      }

      const status =
        typeof req.query.status === 'string' ? req.query.status : 'paid';
      const limit = req.query.limit ? Number(req.query.limit) : 20;
      const daysBackOverride = req.query.days_back
        ? Number(req.query.days_back)
        : NaN;
      const daysBack = Number.isFinite(daysBackOverride)
        ? daysBackOverride
        : Number(settings.payout_lookback_days ?? 30);

      // Compute YYYY-MM-DD created_at_gte (mirrors Python's
      // (datetime.now() - timedelta(days=days_back)).date()).
      const cutoff = new Date(Date.now() - daysBack * 24 * 60 * 60 * 1000);
      const createdAtGte = cutoff.toISOString().slice(0, 10);

      const client = createClientFromSettings(settings);
      if (!client) {
        res.json({ success: false, error: 'No API access token configured' });
        return;
      }

      const result = await client.getPayouts({
        status,
        limit,
        createdAtGte,
      });

      if (!result.success) {
        res.json(result);
        return;
      }

      res.json({
        success: true,
        payouts: result.payouts,
        before: result.before,
        days_back: daysBack,
        status,
        limit,
      });
    } catch (err: any) {
      ctx.logger.error('GoCardless api-payouts failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/gocardless/receipt-search
   *
   * Search GoCardless receipts by customer + date range. Faithful
   * port of `search_gocardless_receipts`. Reads from app DB import
   * history, flattens payments_json, enriches with Opera names.
   */
  router.get('/api/gocardless/receipt-search', async (req: Request, res: Response) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    let operaDb: import('knex').Knex | null = null;
    const company = req.operaCompany;
    if (company) {
      operaDb = ctx.db.getCompanyDb(company);
    }
    try {
      const customer =
        typeof req.query.customer === 'string' ? req.query.customer : null;
      const fromDate =
        typeof req.query.from_date === 'string' ? req.query.from_date : null;
      const toDate =
        typeof req.query.to_date === 'string' ? req.query.to_date : null;
      const limit = req.query.limit ? Number(req.query.limit) : 200;
      const result = await searchReceipts(appDb, operaDb, {
        customer,
        fromDate,
        toDate,
        limit,
      });
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Receipt search failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * DELETE /api/gocardless/import-history
   *
   * Bulk-delete import history records within an optional date range.
   * If no dates supplied, clears ALL records — caller responsible for
   * confirmation. Faithful port of `clear_gocardless_import_history`.
   */
  router.delete('/api/gocardless/import-history', async (req: Request, res: Response) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const fromDate =
        typeof req.query.from_date === 'string' ? req.query.from_date : null;
      const toDate =
        typeof req.query.to_date === 'string' ? req.query.to_date : null;
      const result = await clearImportHistory(appDb, { fromDate, toDate });
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Clear import history failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * DELETE /api/gocardless/import-history/:record_id
   *
   * Delete a single import record so the payout can be re-imported.
   * Does NOT touch Opera. Faithful port of
   * `delete_gocardless_import_record`.
   */
  router.delete(
    '/api/gocardless/import-history/:record_id',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const id = Number(req.params.record_id);
        if (!Number.isFinite(id)) {
          res.status(400).json({ success: false, error: 'Invalid record_id' });
          return;
        }
        const result = await deleteImportRecord(appDb, id);
        if (!result.success && result.error === 'Record not found') {
          res.status(404).json(result);
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
   * POST /api/gocardless/match-customers
   *
   * Match a list of GoCardless payments to Opera customer accounts.
   * Faithful port of `match_gocardless_customers` (apps/gocardless/api/
   * routes.py:497-575). Strategy priority:
   *   0. metadata.opera_account → exact account if customer exists
   *   1. mandate_id → linked Opera account
   *   2. gocardless_customer_id → linked Opera account
   *   3. customer_name → mandate names (normalised, exact then contains)
   *   4. customer_name → Opera sname.sn_name (normalised, exact then contains)
   *
   * After matching, scans Opera cashbook (atran at_type=1) for receipts
   * with the same value (1p tolerance) and tags possible_duplicate=true.
   *
   * Body: array of payment objects with customer_name, description,
   * amount, mandate_id, customer_id, metadata, gc_payment_id.
   */
  router.post(
    '/api/gocardless/match-customers',
    async (req: Request, res: Response) => {
      const operaDb = getOperaDb(req, res);
      if (!operaDb) return;
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const body = req.body as PaymentInput[] | { payments?: PaymentInput[] };
        const payments = Array.isArray(body)
          ? body
          : Array.isArray(body?.payments)
            ? body.payments
            : null;
        if (!payments) {
          res.status(400).json({
            success: false,
            error: 'Body must be an array of payments',
          });
          return;
        }
        const settings = await loadSettings(appDb);
        const result = await matchCustomersWithDuplicateCheck(
          appDb,
          operaDb,
          payments,
          { defaultBatchType: settings.default_batch_type ?? null },
        );
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Match customers failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/gocardless/revalidate-batches
   *
   * Refresh validation status for previously-fetched batches without
   * re-hitting the GoCardless API. Faithful port of
   * revalidate_gocardless_batches (routes.py:2530-2702). Per batch:
   *   - parse payment_date
   *   - detect foreign currency vs Opera home currency
   *   - run validatePostingPeriod (SL ledger)
   *   - duplicate scan against atran/aentry:
   *       foreign currency → ref-only (suffix LIKE)
   *       GBP             → ref + amount (£1 tolerance), then amount
   *                         alone within 14 days (1p tolerance)
   *
   * Body: array of batch objects (originals preserved through the
   * pipeline like Python's **batch spread).
   */
  router.post(
    '/api/gocardless/revalidate-batches',
    async (req: Request, res: Response) => {
      const operaDb = getOperaDb(req, res);
      if (!operaDb) return;
      try {
        const body = req.body as BatchInput[] | { batches?: BatchInput[] };
        const batches = Array.isArray(body)
          ? body
          : Array.isArray(body?.batches)
            ? body.batches
            : null;
        if (!batches) {
          res.status(400).json({
            success: false,
            error: 'Body must be an array of batches',
          });
          return;
        }
        const result = await revalidateBatches(operaDb, batches);
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Revalidate batches failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * GET /api/gocardless/mandates
   *
   * List all GoCardless mandates linked to Opera customers. Faithful
   * port of list_gocardless_mandates (routes.py:6404-6425). Filters
   * out __UNLINKED__ rows when a linked version of the same
   * mandate_id exists. Sorted alphabetically by opera_name.
   */
  router.get(
    '/api/gocardless/mandates',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const result = await listMandates(appDb, {
          status:
            typeof req.query.status === 'string' ? req.query.status : null,
          operaAccount:
            typeof req.query.opera_account === 'string'
              ? req.query.opera_account
              : null,
        });
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('List mandates failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * GET /api/gocardless/mandates/unlinked
   *
   * List GoCardless mandates synced from the API but not yet linked to
   * an Opera customer (opera_account='__UNLINKED__'). Faithful port of
   * list_unlinked_gocardless_mandates (routes.py:6428-6447).
   */
  router.get(
    '/api/gocardless/mandates/unlinked',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const result = await listUnlinkedMandates(appDb);
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('List unlinked mandates failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/gocardless/mandates/sync
   *
   * Pull every active mandate from the GoCardless API and upsert
   * the local row, auto-linking to GC-tagged Opera customers
   * (`sn_analsys = 'GC'`) by normalised name match. Faithful port
   * of sync_gocardless_mandates (apps/gocardless/api/routes.py
   * :6450-6654). Returns counters for synced / new / updated /
   * auto_linked + a human-readable message.
   *
   * NB: must be defined before /mandates/:mandate_id paths so
   * Express doesn't mis-route 'sync' as a path parameter.
   */
  router.post(
    '/api/gocardless/mandates/sync',
    async (req: Request, res: Response) => {
      const operaDb = getOperaDb(req, res);
      if (!operaDb) return;
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const settings = await loadSettings(appDb);
        const client = createClientFromSettings(settings);
        if (!client) {
          res.status(400).json({
            success: false,
            error: 'No API access token configured',
          });
          return;
        }
        // Fetch GC-tagged Opera customers for auto-match
        const operaCustomers: OperaGcCustomer[] = (await operaDb('sname')
          .whereRaw("LTRIM(RTRIM(UPPER(sn_analsys))) = 'GC'")
          .select('sn_account', 'sn_name', 'sn_email')) as unknown as Array<any>;
        const customers: OperaGcCustomer[] = operaCustomers.map((r: any) => ({
          account: String(r.sn_account ?? '').trim(),
          name: String(r.sn_name ?? '').trim(),
          email: r.sn_email ? String(r.sn_email).trim() : null,
        }));
        const fetchPage = async (cursor: string | null) => {
          const r = await client.listMandates({
            status: 'active',
            limit: 100,
            cursor: cursor ?? undefined,
          });
          if (!r.success) throw new Error(r.error ?? 'Mandate list failed');
          return { mandates: r.mandates as any[], after: r.after };
        };
        const customerCache = new Map<string, any | null>();
        const fetchCustomer = async (customerId: string) => {
          if (customerCache.has(customerId)) {
            return customerCache.get(customerId) ?? null;
          }
          const r = await client.getCustomer(customerId);
          const cust =
            r.success && r.customer
              ? {
                  company_name:
                    (r.customer as any).company_name ?? undefined,
                  given_name: (r.customer as any).given_name ?? undefined,
                  family_name: (r.customer as any).family_name ?? undefined,
                  email: (r.customer as any).email ?? undefined,
                }
              : null;
          customerCache.set(customerId, cust);
          return cust;
        };
        const result = await syncMandatesFromGocardless(
          appDb,
          fetchPage,
          fetchCustomer,
          customers,
        );
        if (!result.success) {
          res.status(400).json(result);
          return;
        }
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Sync mandates failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/gocardless/mandates/link
   *
   * Link a GoCardless mandate to an Opera customer. Faithful port of
   * link_gocardless_mandate (apps/gocardless/api/routes.py
   * :6657-6792). Pipeline:
   *   1. Best-effort GoCardless API verify (mandate.status, scheme,
   *      linked customer.email) when an access token is configured.
   *      Falls back to defaults on any API error — operator workflow
   *      should not be blocked by transient API issues.
   *   2. Local upsert of (opera_account, mandate_id), with
   *      __UNLINKED__ placeholder cleanup.
   *   3. Re-link confirmation guard: when the same mandate currently
   *      points at a different non-__UNLINKED__ account, the call
   *      returns 409 + needs_confirm=true unless confirm=true.
   *   4. Opera write: ROWLOCK update of sname.sn_analsys = 'GC' on
   *      the new account; clears 'GC' on the old account when re-
   *      linking. Failures are reported per-side in the response
   *      (matches Python's "log + continue" behaviour).
   */
  router.post(
    '/api/gocardless/mandates/link',
    async (req: Request, res: Response) => {
      const operaDb = getOperaDb(req, res);
      if (!operaDb) return;
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const body = (req.body ?? {}) as {
          opera_account?: string;
          mandate_id?: string;
          opera_name?: string | null;
          confirm?: boolean;
        };
        const operaAccount = String(body.opera_account ?? '').trim();
        const mandateId = String(body.mandate_id ?? '').trim();
        const operaName =
          typeof body.opera_name === 'string' ? body.opera_name : null;
        const confirm = !!body.confirm;
        if (!operaAccount || !mandateId) {
          res.status(400).json({
            success: false,
            error: 'opera_account and mandate_id are required',
          });
          return;
        }
        // 1. Best-effort GoCardless verify
        let mandateStatus = 'active';
        let scheme = 'bacs';
        let customerId: string | null = null;
        let email: string | null = null;
        const settings = await loadSettings(appDb);
        const client = createClientFromSettings(settings);
        if (client) {
          const m = await client.getMandate(mandateId);
          if (m.success && m.mandate) {
            const md = m.mandate as Record<string, any>;
            mandateStatus = (md.status as string | undefined) ?? 'active';
            scheme = (md.scheme as string | undefined) ?? 'bacs';
            customerId =
              (md.links?.customer as string | undefined | null) ?? null;
            if (customerId) {
              const c = await client.getCustomer(customerId);
              if (c.success && c.customer) {
                const cd = c.customer as Record<string, any>;
                email = (cd.email as string | undefined) ?? null;
              }
            }
          }
        }
        // 2. Local upsert
        const linkResult = await linkMandate(appDb, {
          operaAccount,
          mandateId,
          operaName,
          gocardlessCustomerId: customerId,
          mandateStatus,
          scheme,
          email,
          confirm,
        });
        if (!linkResult.success && linkResult.needsConfirm) {
          res.status(409).json({
            success: false,
            needs_confirm: true,
            error: linkResult.error,
            old_account: linkResult.oldOperaAccount,
          });
          return;
        }
        if (!linkResult.success) {
          res.status(400).json(linkResult);
          return;
        }
        // 3. Opera sn_analsys flag move
        const gcFlag: Record<string, unknown> = {};
        try {
          if (linkResult.oldOperaAccount) {
            const removed = await operaDb('sname')
              .whereRaw('LTRIM(RTRIM(sn_account)) = ?', [
                linkResult.oldOperaAccount,
              ])
              .andWhereRaw("LTRIM(RTRIM(UPPER(sn_analsys))) = 'GC'")
              .update({ sn_analsys: '' });
            gcFlag.gc_removed_from = linkResult.oldOperaAccount;
            gcFlag.gc_removed_rows = Number(removed);
          }
          const set = await operaDb('sname')
            .whereRaw('LTRIM(RTRIM(sn_account)) = ?', [operaAccount])
            .andWhereRaw(
              "(sn_analsys IS NULL OR LTRIM(RTRIM(sn_analsys)) = '' OR LTRIM(RTRIM(UPPER(sn_analsys))) != 'GC')",
            )
            .update({ sn_analsys: 'GC' });
          gcFlag.gc_set_on = operaAccount;
          gcFlag.gc_set_rows = Number(set);
        } catch (sqlErr: any) {
          gcFlag.gc_error = sqlErr?.message ?? String(sqlErr);
          ctx.logger.warn?.('sn_analsys flag move failed', sqlErr);
        }
        res.json({
          success: true,
          message: linkResult.message,
          mandate: linkResult.mandate,
          gc_flag: gcFlag,
        });
      } catch (err: any) {
        ctx.logger.error('Link mandate failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * GET /api/gocardless/eligible-customers
   *
   * Customers eligible for GoCardless: union of customers with
   * sn_analsys='GC' (operator-flagged) + customers with a linked
   * mandate. Faithful port of get_gocardless_eligible_customers
   * (routes.py:7551-7635). Each row reports has_mandate +
   * mandate_id + mandate_status so the UI can show "needs setup"
   * vs "already mandated" status.
   *
   * Adds dormant + stopped filter per CLAUDE.md (the original
   * Python missed these).
   */
  router.get(
    '/api/gocardless/eligible-customers',
    async (req: Request, res: Response) => {
      const operaDb = getOperaDb(req, res);
      if (!operaDb) return;
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const result = await getEligibleCustomers(appDb, operaDb);
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Eligible customers failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/gocardless/request-payment
   *
   * Request a single Direct Debit payment from a customer via the
   * customer's existing GoCardless mandate. Faithful port of
   * request_gocardless_payment (apps/gocardless/api/routes.py
   * :8249-8435). Pipeline:
   *   1. Duplicate-invoice guard against active payment_requests
   *   2. Active mandate lookup (gocardless_mandates)
   *   3. Opera read: invoice total (from stran.st_trbal) +
   *      unallocated-credit safety check
   *   4. POST /payments via the GoCardless client
   *   5. Persist to gocardless_payment_requests + return enriched
   *      response with customer_name + estimated_arrival
   */
  router.post(
    '/api/gocardless/request-payment',
    async (req: Request, res: Response) => {
      const operaDb = getOperaDb(req, res);
      if (!operaDb) return;
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const settings = await loadSettings(appDb);
        const client = createClientFromSettings(settings);
        if (!client) {
          res.status(400).json({
            success: false,
            error: 'GoCardless API not configured',
          });
          return;
        }
        const body = (req.body ?? {}) as Partial<RequestPaymentInput> & {
          opera_account?: string;
          amount?: number | string | null;
          charge_date?: string | null;
        };
        const input: RequestPaymentInput = {
          operaAccount: String(body.opera_account ?? body.operaAccount ?? ''),
          invoices: Array.isArray(body.invoices) ? body.invoices.map(String) : [],
          amountPence:
            body.amount === undefined || body.amount === null
              ? body.amountPence ?? null
              : Number(body.amount),
          chargeDate:
            typeof body.charge_date === 'string'
              ? body.charge_date
              : (body.chargeDate ?? null),
          description: body.description ?? null,
        };
        const readOpera = async (
          operaAccount: string,
          invoices: string[],
        ): Promise<OperaSnapshot> => {
          let invoiceTotalPounds: number | null = null;
          if (invoices.length > 0) {
            const totalRow = await operaDb('stran')
              .where({ st_account: operaAccount })
              .whereIn('st_trref', invoices)
              .sum<{ total: number | string | null }>({ total: 'st_trbal' })
              .first();
            const total = totalRow?.total;
            if (total !== null && total !== undefined && Number(total) !== 0) {
              invoiceTotalPounds = Number(total);
            }
          }
          const creditRow = await operaDb('stran')
            .where({ st_account: operaAccount })
            .andWhere('st_trbal', '<', 0)
            .sum<{ total: number | string | null }>({ total: 'st_trbal' })
            .first();
          const credit = Number(creditRow?.total ?? 0);
          return {
            invoiceTotalPounds,
            unallocatedCreditPounds: Math.abs(credit),
          };
        };
        const createRemote = async (input2: {
          amountPence: number;
          mandateId: string;
          description: string;
          chargeDate: string | null;
          metadata: Record<string, string>;
        }) =>
          client.createPayment({
            amountPence: input2.amountPence,
            mandateId: input2.mandateId,
            description: input2.description,
            chargeDate: input2.chargeDate,
            metadata: input2.metadata,
          });
        const result = await requestPayment(
          appDb,
          input,
          { request_statement_reference: settings.request_statement_reference ?? '' },
          readOpera,
          createRemote,
        );
        if (!result.success) {
          res.status(400).json(result);
          return;
        }
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Request payment failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/gocardless/payment-requests/bulk
   *
   * Request multiple payments in one shot. Each row is run through
   * the same pipeline as /request-payment; failures are reported
   * per-row and don't abort the batch. Faithful port of
   * request_bulk_payments (apps/gocardless/api/routes.py:8438-8486).
   *
   * Accepts either { requests: [...] } or a bare array body.
   */
  router.post(
    '/api/gocardless/payment-requests/bulk',
    async (req: Request, res: Response) => {
      const operaDb = getOperaDb(req, res);
      if (!operaDb) return;
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const settings = await loadSettings(appDb);
        const client = createClientFromSettings(settings);
        if (!client) {
          res.status(400).json({
            success: false,
            error: 'GoCardless API not configured',
          });
          return;
        }
        const raw = req.body;
        const body = Array.isArray(raw)
          ? raw
          : Array.isArray(raw?.requests)
            ? raw.requests
            : [];
        const inputs: RequestPaymentInput[] = body.map((r: any) => ({
          operaAccount: String(r.opera_account ?? r.operaAccount ?? ''),
          invoices: Array.isArray(r.invoices) ? r.invoices.map(String) : [],
          amountPence:
            r.amount === undefined || r.amount === null
              ? r.amountPence ?? null
              : Number(r.amount),
          chargeDate:
            typeof r.charge_date === 'string' ? r.charge_date : (r.chargeDate ?? null),
          description: r.description ?? null,
        }));
        const readOpera = async (
          operaAccount: string,
          invoices: string[],
        ): Promise<OperaSnapshot> => {
          let invoiceTotalPounds: number | null = null;
          if (invoices.length > 0) {
            const totalRow = await operaDb('stran')
              .where({ st_account: operaAccount })
              .whereIn('st_trref', invoices)
              .sum<{ total: number | string | null }>({ total: 'st_trbal' })
              .first();
            const total = totalRow?.total;
            if (total !== null && total !== undefined && Number(total) !== 0) {
              invoiceTotalPounds = Number(total);
            }
          }
          const creditRow = await operaDb('stran')
            .where({ st_account: operaAccount })
            .andWhere('st_trbal', '<', 0)
            .sum<{ total: number | string | null }>({ total: 'st_trbal' })
            .first();
          const credit = Number(creditRow?.total ?? 0);
          return {
            invoiceTotalPounds,
            unallocatedCreditPounds: Math.abs(credit),
          };
        };
        const createRemote = async (input2: {
          amountPence: number;
          mandateId: string;
          description: string;
          chargeDate: string | null;
          metadata: Record<string, string>;
        }) =>
          client.createPayment({
            amountPence: input2.amountPence,
            mandateId: input2.mandateId,
            description: input2.description,
            chargeDate: input2.chargeDate,
            metadata: input2.metadata,
          });
        const result = await requestBulkPayments(
          appDb,
          inputs,
          { request_statement_reference: settings.request_statement_reference ?? '' },
          readOpera,
          createRemote,
        );
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Request bulk payments failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * GET /api/gocardless/collectable-invoices
   *
   * List outstanding sales-ledger invoices that can be collected via
   * GoCardless Direct Debit. Faithful port of get_collectable_invoices
   * (apps/gocardless/api/routes.py:7721-7894). Decorates each invoice
   * with mandate status, days-overdue, and whether a payment request
   * already covers it.
   *
   * Query params:
   *   - overdue_only (default false)
   *   - min_amount (default 0)
   */
  router.get(
    '/api/gocardless/collectable-invoices',
    async (req: Request, res: Response) => {
      const operaDb = getOperaDb(req, res);
      if (!operaDb) return;
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const overdueOnly =
          req.query.overdue_only === 'true' || req.query.overdue_only === '1';
        const minAmount = req.query.min_amount
          ? Number(req.query.min_amount)
          : 0;
        const result = await getCollectableInvoices(operaDb, appDb, {
          overdueOnly,
          minAmount,
        });
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Get collectable invoices failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * GET /api/gocardless/repeat-documents
   *
   * List Opera repeat documents (ih_docstat='U') suitable for
   * GoCardless subscriptions, cross-referenced with the per-app
   * mandates + subscriptions tables. Faithful port of
   * get_gocardless_repeat_documents
   * (apps/gocardless/api/routes.py:8619-8785).
   *
   * Query params:
   *   - require_mandate (default true): when true, only show docs
   *     for customers with an active mandate. Set false to show all
   *     active customers (used by the link-existing-sub UI).
   */
  router.get(
    '/api/gocardless/repeat-documents',
    async (req: Request, res: Response) => {
      const operaDb = getOperaDb(req, res);
      if (!operaDb) return;
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const settings = await loadSettings(appDb);
        const requireMandateRaw = req.query.require_mandate;
        const requireMandate =
          requireMandateRaw === undefined
            ? true
            : !(requireMandateRaw === 'false' || requireMandateRaw === '0');
        const result = await getRepeatDocuments(operaDb, appDb, {
          requireMandate,
          subscriptionTag: settings.subscription_tag ?? 'SUB',
        });
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Get repeat documents failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * GET /api/gocardless/customer-email/:account
   *
   * Look up the customer email + name + contact from sname for the
   * given account. Used by the mandate-setup form to pre-fill
   * customer details. Faithful port of get_customer_email_for_mandate
   * (apps/gocardless/api/routes.py:7189-7217).
   *
   * Returns success=true with empty fields if the account is not
   * found — the form still loads, the operator just types the email
   * manually.
   */
  router.get(
    '/api/gocardless/customer-email/:account',
    async (req: Request, res: Response) => {
      const operaDb = getOperaDb(req, res);
      if (!operaDb) return;
      try {
        const result = await getCustomerEmail(
          operaDb,
          String(req.params.account ?? ''),
        );
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Get customer email failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * GET /api/gocardless/mandates/pending-setups
   *
   * List all mandate setup requests with current status. Faithful
   * port of list_pending_mandate_setups (routes.py:7054-7067).
   * Returns pending_count for the dashboard "X to chase up" widget.
   */
  router.get(
    '/api/gocardless/mandates/pending-setups',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const result = await listMandateSetups(appDb);
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('List mandate setups failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/gocardless/mandates/cancel-setup/:setup_id
   *
   * Cancel a pending mandate setup request. Faithful port of
   * cancel_mandate_setup (routes.py:7220-7244). Refuses cancellation
   * when status is already final (completed/failed/cancelled).
   */
  router.post(
    '/api/gocardless/mandates/cancel-setup/:setup_id',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const id = Number(req.params.setup_id);
        const result = await cancelMandateSetup(appDb, id);
        if (!result.success) {
          res
            .status(result.error === 'Setup request not found' ? 404 : 400)
            .json(result);
          return;
        }
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Cancel mandate setup failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/gocardless/mandates/:mandate_id/cancel
   *
   * Cancel a mandate via GoCardless API and update the local
   * mandate_status. Faithful port of cancel_gocardless_mandate
   * (routes.py:6795-6830). Local update only proceeds if the remote
   * cancel succeeds (or returns "already cancelled").
   */
  router.post(
    '/api/gocardless/mandates/:mandate_id/cancel',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const settings = await loadSettings(appDb);
        const client = createClientFromSettings(settings);
        if (!client) {
          res.status(400).json({
            success: false,
            error: 'GoCardless not configured',
          });
          return;
        }
        const cancelRemote = async (id: string) => client.cancelMandate(id);
        const result = await cancelMandate(
          appDb,
          String(req.params.mandate_id ?? ''),
          cancelRemote,
        );
        if (!result.success) {
          res
            .status(result.error === 'Mandate not found' ? 404 : 400)
            .json(result);
          return;
        }
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Cancel mandate failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * DELETE /api/gocardless/mandates/:mandate_id
   *
   * Unlink a mandate from its Opera customer (sets opera_account to
   * '__UNLINKED__' rather than deleting the row — mandate-level
   * history matters for audit). Faithful port of
   * unlink_gocardless_mandate (routes.py:6833-6849). Does NOT cancel
   * the mandate in GoCardless — operator must call /cancel for that.
   */
  router.delete(
    '/api/gocardless/mandates/:mandate_id',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const result = await unlinkMandate(
          appDb,
          String(req.params.mandate_id ?? ''),
        );
        if (!result.success) {
          res
            .status(result.error === 'Mandate not found' ? 404 : 400)
            .json(result);
          return;
        }
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Unlink mandate failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * GET /api/gocardless/subscriptions
   *
   * List GoCardless subscriptions stored in the per-app DB. SAM-side
   * read endpoint; the Python source has only the Opera 3 variant
   * which adds Opera-side mismatch detection (deferred until full
   * Opera SE ihead/itran reads land).
   *
   * Filters: status, opera_account. Default limit 200. Each row
   * enriched with customer_name from the matching mandate.
   */
  router.get(
    '/api/gocardless/subscriptions',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const result = await listSubscriptions(appDb, {
          status:
            typeof req.query.status === 'string' ? req.query.status : null,
          operaAccount:
            typeof req.query.opera_account === 'string'
              ? req.query.opera_account
              : null,
          limit: req.query.limit ? Number(req.query.limit) : undefined,
        });
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('List subscriptions failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/gocardless/subscriptions/link
   *
   * Link an Opera repeat document (ih_doc) to a GoCardless subscription.
   * Faithful port of link_subscription_to_document
   * (apps/gocardless/api/routes.py:8788-8832). Multiple docs per
   * subscription supported; rejects when the doc is already linked
   * to a different subscription.
   *
   * NB: this route MUST be defined before /subscriptions/:id so Express
   * doesn't mis-route 'link' as a path parameter.
   */
  router.post(
    '/api/gocardless/subscriptions/link',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const body = (req.body ?? {}) as {
          subscription_id?: string;
          source_doc?: string;
        };
        const result = await linkSubscriptionToDocument(appDb, {
          subscriptionId: String(body.subscription_id ?? ''),
          sourceDoc: String(body.source_doc ?? ''),
        });
        if (!result.success) {
          const isMissing =
            typeof result.error === 'string' &&
            /not found locally/i.test(result.error);
          res.status(isMissing ? 404 : 400).json(result);
          return;
        }
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Link subscription failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/gocardless/subscriptions/unlink
   *
   * Remove the link between a subscription and an Opera repeat document.
   * Faithful port of unlink_subscription_from_document
   * (apps/gocardless/api/routes.py:8835-8874). When source_doc is
   * omitted, all document links for the subscription are removed.
   */
  router.post(
    '/api/gocardless/subscriptions/unlink',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const body = (req.body ?? {}) as {
          subscription_id?: string;
          source_doc?: string;
        };
        const result = await unlinkSubscriptionFromDocument(appDb, {
          subscriptionId: String(body.subscription_id ?? ''),
          sourceDoc:
            typeof body.source_doc === 'string' && body.source_doc
              ? body.source_doc
              : null,
        });
        if (!result.success) {
          const isMissing =
            typeof result.error === 'string' && /not found/i.test(result.error);
          res.status(isMissing ? 404 : 400).json(result);
          return;
        }
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Unlink subscription failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/gocardless/subscriptions/sync
   *
   * Pull every subscription from the GoCardless API and upsert into
   * the local DB. Faithful port of sync_gocardless_subscriptions
   * (apps/gocardless/api/routes.py:9375-9500). Resolves
   * mandate -> {opera_account, opera_name} via the local mandates
   * table first, falling back to the GoCardless mandate + customer
   * APIs when the local link doesn't carry a name.
   *
   * NB: must be defined before /subscriptions/:id so Express doesn't
   * mis-route 'sync' as a path parameter.
   */
  router.post(
    '/api/gocardless/subscriptions/sync',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const settings = await loadSettings(appDb);
        const client = createClientFromSettings(settings);
        if (!client) {
          res.status(400).json({
            success: false,
            error: 'GoCardless API not configured',
          });
          return;
        }
        const fetchPage = async (cursor: string | null) => {
          const r = await client.listSubscriptions({
            limit: 100,
            cursor: cursor ?? undefined,
          });
          if (!r.success) {
            throw new Error(r.error ?? 'Subscription list failed');
          }
          return { subscriptions: r.subscriptions as any[], after: r.after };
        };
        const customerCache = new Map<string, string | null>();
        const mandateCustomerCache = new Map<string, string | null>();
        const resolveAccount = async (mandateId: string) => {
          let customerId = mandateCustomerCache.get(mandateId);
          if (customerId === undefined) {
            const m = await client.getMandate(mandateId);
            customerId =
              m.success && m.mandate
                ? ((m.mandate as Record<string, any>).links
                    ?.customer as string | undefined) ?? null
                : null;
            mandateCustomerCache.set(mandateId, customerId);
          }
          if (!customerId) return { opera_account: null, opera_name: null };
          let name = customerCache.get(customerId);
          if (name === undefined) {
            const c = await client.getCustomer(customerId);
            if (c.success && c.customer) {
              const cd = c.customer as Record<string, any>;
              const company = (cd.company_name as string | undefined) ?? '';
              const given = (cd.given_name as string | undefined) ?? '';
              const family = (cd.family_name as string | undefined) ?? '';
              name = company || `${given} ${family}`.trim() || null;
            } else {
              name = null;
            }
            customerCache.set(customerId, name);
          }
          return { opera_account: null, opera_name: name ?? null };
        };
        const result = await syncSubscriptionsFromGocardless(
          appDb,
          fetchPage,
          { resolveAccount },
        );
        if (!result.success) {
          res.status(400).json(result);
          return;
        }
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Sync subscriptions failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * GET /api/gocardless/subscriptions/:subscription_id
   *
   * Read a single subscription with its linked source_docs and Opera
   * customer name enrichment. Faithful port of
   * get_gocardless_subscription (apps/gocardless/api/routes.py:9157-9169).
   */
  router.get(
    '/api/gocardless/subscriptions/:subscription_id',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const result = await getSubscription(
          appDb,
          String(req.params.subscription_id ?? ''),
        );
        if (!result.success) {
          const isMissing =
            typeof result.error === 'string' && /not found/i.test(result.error);
          res.status(isMissing ? 404 : 400).json(result);
          return;
        }
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Get subscription failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * PUT /api/gocardless/subscriptions/:subscription_id
   *
   * Update name / amount on a GoCardless subscription, then mirror the
   * change locally. Faithful port of update_gocardless_subscription
   * (apps/gocardless/api/routes.py:9248-9291). Returns the fresh
   * subscription with source_docs.
   */
  router.put(
    '/api/gocardless/subscriptions/:subscription_id',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const settings = await loadSettings(appDb);
        const client = createClientFromSettings(settings);
        if (!client) {
          res.status(400).json({
            success: false,
            error: 'GoCardless API not configured',
          });
          return;
        }
        const body = (req.body ?? {}) as {
          name?: string | null;
          amount_pence?: number | string | null;
        };
        const subscriptionId = String(req.params.subscription_id ?? '');
        const amount =
          body.amount_pence === undefined || body.amount_pence === null
            ? null
            : Number(body.amount_pence);
        const remote = async (
          id: string,
          opts: { name?: string | null; amountPence?: number | null },
        ) =>
          client.updateSubscription(id, {
            name: opts.name ?? null,
            amountPence: opts.amountPence ?? null,
          });
        const result = await updateSubscriptionDetails(
          appDb,
          subscriptionId,
          {
            name: typeof body.name === 'string' ? body.name : null,
            amountPence: Number.isFinite(amount as number) ? (amount as number) : null,
          },
          remote,
        );
        if (!result.success) {
          res.status(400).json(result);
          return;
        }
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Update subscription failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/gocardless/subscriptions/:subscription_id/pause
   *
   * Pause an active subscription via GoCardless API + mirror locally.
   * Faithful port of pause_gocardless_subscription
   * (apps/gocardless/api/routes.py:9294-9318).
   */
  router.post(
    '/api/gocardless/subscriptions/:subscription_id/pause',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const settings = await loadSettings(appDb);
        const client = createClientFromSettings(settings);
        if (!client) {
          res.status(400).json({
            success: false,
            error: 'GoCardless API not configured',
          });
          return;
        }
        const result = await pauseSubscription(
          appDb,
          String(req.params.subscription_id ?? ''),
          (id) => client.pauseSubscription(id),
        );
        if (!result.success) {
          const isMissing =
            typeof result.error === 'string' && /not found/i.test(result.error);
          res.status(isMissing ? 404 : 400).json(result);
          return;
        }
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Pause subscription failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/gocardless/subscriptions/:subscription_id/resume
   *
   * Resume a paused subscription. Faithful port of
   * resume_gocardless_subscription (apps/gocardless/api/routes.py
   * :9321-9345).
   */
  router.post(
    '/api/gocardless/subscriptions/:subscription_id/resume',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const settings = await loadSettings(appDb);
        const client = createClientFromSettings(settings);
        if (!client) {
          res.status(400).json({
            success: false,
            error: 'GoCardless API not configured',
          });
          return;
        }
        const result = await resumeSubscription(
          appDb,
          String(req.params.subscription_id ?? ''),
          (id) => client.resumeSubscription(id),
        );
        if (!result.success) {
          const isMissing =
            typeof result.error === 'string' && /not found/i.test(result.error);
          res.status(isMissing ? 404 : 400).json(result);
          return;
        }
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Resume subscription failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/gocardless/subscriptions/:subscription_id/sync-from-opera
   *
   * Re-derive the subscription amount from its linked Opera repeat
   * documents and push the new total to GoCardless. Faithful port of
   * sync_subscription_from_opera (apps/gocardless/api/routes.py
   * :9172-9245). Reads itran (in pence) for all linked source_docs:
   *   amount_pence = SUM(it_exvat) + SUM(it_vatval)
   *
   * Skips remote+local update when the new amount matches the existing
   * one. Returns old/new amounts when an update happens.
   */
  router.post(
    '/api/gocardless/subscriptions/:subscription_id/sync-from-opera',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      const operaDb = getOperaDb(req, res);
      if (!operaDb) return;
      try {
        const settings = await loadSettings(appDb);
        const client = createClientFromSettings(settings);
        if (!client) {
          res.status(400).json({
            success: false,
            error: 'GoCardless API not configured',
          });
          return;
        }
        const subscriptionId = String(req.params.subscription_id ?? '');
        // Read itran totals (in pence) for the linked repeat docs.
        const readOperaDocAmount = async (sourceDocs: string[]) => {
          const row = await operaDb('itran')
            .whereIn('it_doc', sourceDocs)
            .select(
              operaDb.raw('COALESCE(SUM(it_exvat), 0) AS line_nett'),
              operaDb.raw('COALESCE(SUM(it_vatval), 0) AS line_vat'),
            )
            .first<{ line_nett: number | string | null; line_vat: number | string | null }>();
          return {
            lineNettPence: Number(row?.line_nett ?? 0),
            lineVatPence: Number(row?.line_vat ?? 0),
          };
        };
        const updateRemote = async (id: string, amountPence: number) =>
          client.updateSubscription(id, { amountPence });
        const result = await syncSubscriptionFromOpera(
          appDb,
          subscriptionId,
          readOperaDocAmount,
          updateRemote,
        );
        if (!result.success) {
          const isMissing =
            typeof result.error === 'string' && /not found/i.test(result.error);
          res.status(isMissing ? 404 : 400).json(result);
          return;
        }
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Sync subscription from Opera failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/gocardless/subscriptions/:subscription_id/cancel
   *
   * Cancel a subscription (cannot be undone in GoCardless). Faithful
   * port of cancel_gocardless_subscription (apps/gocardless/api/
   * routes.py:9348-9372).
   */
  router.post(
    '/api/gocardless/subscriptions/:subscription_id/cancel',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const settings = await loadSettings(appDb);
        const client = createClientFromSettings(settings);
        if (!client) {
          res.status(400).json({
            success: false,
            error: 'GoCardless API not configured',
          });
          return;
        }
        const result = await cancelSubscription(
          appDb,
          String(req.params.subscription_id ?? ''),
          (id) => client.cancelSubscription(id),
        );
        if (!result.success) {
          const isMissing =
            typeof result.error === 'string' && /not found/i.test(result.error);
          res.status(isMissing ? 404 : 400).json(result);
          return;
        }
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Cancel subscription failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * GET /api/gocardless/payment-requests
   *
   * List payment requests, optionally filtered by status + opera_account.
   * Faithful port of list_payment_requests (routes.py:8217-8246).
   * Each row enriched with customer_name from the mandate.
   */
  router.get(
    '/api/gocardless/payment-requests',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const result = await listPaymentRequests(appDb, {
          status:
            typeof req.query.status === 'string' ? req.query.status : null,
          operaAccount:
            typeof req.query.opera_account === 'string'
              ? req.query.opera_account
              : null,
          limit: req.query.limit ? Number(req.query.limit) : undefined,
        });
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('List payment requests failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/gocardless/payment-requests/sync
   *
   * Poll the GoCardless API for status updates on all pending payment
   * requests and update local rows. Faithful port of
   * sync_payment_statuses (apps/gocardless/api/routes.py:8556-8616).
   *
   * Per-payment failures are logged + skipped — never fails the whole
   * sync run. Returns counts so the UI can show "synced X / total Y".
   */
  router.post(
    '/api/gocardless/payment-requests/sync',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const settings = await loadSettings(appDb);
        const client = createClientFromSettings(settings);
        if (!client) {
          res.status(400).json({
            success: false,
            error: 'GoCardless API not configured',
          });
          return;
        }
        const syncRemote = async (paymentId: string) =>
          client.getPayment(paymentId);
        const result = await syncPaymentStatuses(appDb, syncRemote);
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Sync payment statuses failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * GET /api/gocardless/payment-requests/:request_id
   *
   * Single-payment-request detail. Faithful port of get_payment_request
   * (routes.py:8489-8506). Includes customer_name from the linked
   * mandate when available.
   */
  router.get(
    '/api/gocardless/payment-requests/:request_id',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const id = Number(req.params.request_id);
        const result = await getPaymentRequest(appDb, id);
        if (!result.success) {
          res
            .status(result.error === 'Payment request not found' ? 404 : 400)
            .json(result);
          return;
        }
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Get payment request failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/gocardless/payment-requests/:request_id/cancel
   *
   * Cancel a pending payment request. Faithful port of
   * cancel_payment_request (routes.py:8509-8553).
   *   - Refuses cancellation when status isn't pending/pending_*
   *   - Best-effort GoCardless API cancel via the saved access token
   *     — failure is reported as remote_warning but local cancel
   *     proceeds (matches Python's "log + continue")
   *   - Local row marked status='cancelled' with error_message
   */
  router.post(
    '/api/gocardless/payment-requests/:request_id/cancel',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const id = Number(req.params.request_id);
        const settings = await loadSettings(appDb);
        const client = createClientFromSettings(settings);
        const cancelRemote = client
          ? async (paymentId: string) => {
              const r = await client.cancelPayment(paymentId);
              return { success: r.success, error: r.error };
            }
          : undefined;
        const result = await cancelPaymentRequest(appDb, id, cancelRemote);
        if (!result.success) {
          res
            .status(result.error === 'Payment request not found' ? 404 : 400)
            .json(result);
          return;
        }
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Cancel payment request failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * GET /api/gocardless/payment-requests/stats
   *
   * Dashboard statistics for the GoCardless payments-DB. Faithful port
   * of `get_gocardless_payment_stats` (routes.py:6271-6280) which calls
   * `GoCardlessPaymentsDB.get_statistics()`. Returns active-mandate
   * count, pending count + amount, month-to-date paid-out, and 30-day
   * failed count — flat shape merged onto {success}.
   */
  router.get(
    '/api/gocardless/payment-requests/stats',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const result = await getPaymentStats(appDb);
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Payment stats failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * GET /api/gocardless/validate-date?post_date=YYYY-MM-DD
   *
   * Validate that a posting date is allowed in Opera, based on Open
   * Period Accounting / nclndd / nparm. Faithful port of
   * `validate_gocardless_date` (apps/gocardless/api/routes.py:578-618).
   *
   * Returns:
   *   - valid:                bool
   *   - error:                string when invalid
   *   - year/period:          mapped from nclndd
   *   - current_year/current_period: from nparm
   *   - open_period_accounting: bool
   */
  router.get('/api/gocardless/validate-date', async (req: Request, res: Response) => {
    const operaDb = getOperaDb(req, res);
    if (!operaDb) return;
    try {
      const postDate = String(req.query.post_date ?? '').trim();
      if (!postDate) {
        res.json({
          success: false,
          valid: false,
          error: 'post_date is required',
        });
        return;
      }
      let result;
      try {
        result = await validatePostingPeriod(operaDb, postDate, 'SL');
      } catch (parseErr: any) {
        res.json({
          success: false,
          valid: false,
          error: parseErr?.message ?? String(parseErr),
        });
        return;
      }
      const current = await getCurrentPeriodInfo(operaDb);
      res.json({
        success: true,
        valid: result.is_valid,
        error: result.is_valid ? null : result.error_message,
        year: result.year,
        period: result.period,
        current_year: current.np_year,
        current_period: current.np_perno,
        open_period_accounting: result.open_period_accounting,
      });
    } catch (err: any) {
      ctx.logger.error('Validate-date failed', err);
      res.status(500).json({
        success: false,
        valid: false,
        error: err?.message ?? String(err),
      });
    }
  });

  /**
   * POST /api/gocardless/update-subscription-tags
   *
   * Preview or apply ih_analsys tag updates to Opera repeat documents
   * matching the configured frequency filters. Faithful port of
   * `update_subscription_tags`.
   *
   * Body:
   *   - mode: 'preview' (default) or 'apply'
   *   - overwrite: bool — if true, also update docs whose ih_analsys
   *                differs from the configured tag
   */
  router.post(
    '/api/gocardless/update-subscription-tags',
    async (req: Request, res: Response) => {
      const operaDb = getOperaDb(req, res);
      if (!operaDb) return;
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const settings = await loadSettings(appDb);
        const body = (req.body ?? {}) as Record<string, unknown>;
        const result = await updateSubscriptionTags(
          operaDb,
          {
            subscription_tag: settings.subscription_tag ?? '',
            subscription_frequencies: settings.subscription_frequencies ?? [],
          },
          {
            mode: body.mode === 'apply' ? 'apply' : 'preview',
            overwrite: !!body.overwrite,
          },
        );
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Update subscription tags failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/gocardless/partner/initiate-signup
   *
   * Start the OAuth Connect flow for a new merchant. Faithful port of
   * initiate_gocardless_partner_signup (routes.py:1153-1219). Inserts
   * a pending row in gocardless_partner_signups and returns the
   * GoCardless authorisation URL the merchant should be redirected
   * to. State token is stored in status_detail for CSRF validation
   * on /partner/callback.
   *
   * Body: { company_name, company_email }
   */
  router.post(
    '/api/gocardless/partner/initiate-signup',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const body = (req.body ?? {}) as {
          company_name?: string;
          company_email?: string;
        };
        const proto = (req.headers['x-forwarded-proto'] as string) ?? req.protocol;
        const host = (req.headers['x-forwarded-host'] as string) ?? req.get('host') ?? '';
        const baseUrl = host ? `${proto}://${host}` : '';
        const result = await initiatePartnerSignup(appDb, {
          companyName: String(body.company_name ?? ''),
          companyEmail: String(body.company_email ?? ''),
          baseUrl,
        });
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Initiate partner signup failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * GET /api/gocardless/partner/callback
   *
   * OAuth redirect target — GoCardless sends the merchant's browser
   * here after they complete signup. Faithful port of
   * gocardless_partner_callback (routes.py:1222-1319).
   *
   * Validates the state token (CSRF), exchanges the auth code for a
   * merchant access token, fetches the creditor info, and stores
   * everything against the latest signup row. Returns HTML (not JSON)
   * because the merchant's browser hits this URL — the partner-portal
   * UI polls /signup-status to detect completion.
   */
  router.get(
    '/api/gocardless/partner/callback',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const proto = (req.headers['x-forwarded-proto'] as string) ?? req.protocol;
        const host = (req.headers['x-forwarded-host'] as string) ?? req.get('host') ?? '';
        const baseUrl = host ? `${proto}://${host}` : '';
        const result = await handlePartnerCallback(appDb, {
          code: typeof req.query.code === 'string' ? req.query.code : null,
          state: typeof req.query.state === 'string' ? req.query.state : null,
          error: typeof req.query.error === 'string' ? req.query.error : null,
          baseUrl,
        });
        const html = partnerCallbackHtml(result);
        res.setHeader('Content-Type', 'text/html; charset=utf-8');
        res.status(200).send(html);
      } catch (err: any) {
        ctx.logger.error('Partner callback failed', err);
        const html = partnerCallbackHtml({
          ok: false,
          title: 'Connection Failed',
          message: `Something went wrong: ${err?.message ?? String(err)}`,
        });
        res.setHeader('Content-Type', 'text/html; charset=utf-8');
        res.status(500).send(html);
      }
    },
  );

  /**
   * GET /api/gocardless/partner/config
   *
   * Probe whether GoCardless Partner credentials are configured.
   * Faithful port of get_gocardless_partner_config (routes.py:1487-1501).
   * Constructs a redirect_uri fallback from request origin when no
   * explicit partner_redirect_uri is configured.
   */
  router.get(
    '/api/gocardless/partner/config',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        // Mirror Python's `request.base_url`: protocol://host (+ port)
        const proto = (req.headers['x-forwarded-proto'] as string) ?? req.protocol;
        const host = (req.headers['x-forwarded-host'] as string) ?? req.get('host') ?? '';
        const baseUrl = host ? `${proto}://${host}` : '';
        const result = await getPartnerConfig(appDb, { baseUrl });
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Partner config failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * GET /api/gocardless/partner/signup-status
   *
   * Latest partner signup record (token redacted). Faithful port of
   * get_gocardless_partner_signup_status (routes.py:1322-1339).
   */
  router.get(
    '/api/gocardless/partner/signup-status',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const result = await getLatestPartnerSignup(appDb);
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Partner signup-status failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * GET /api/gocardless/partner/merchants?status=...
   *
   * All merchants onboarded via the partner signup flow.
   * Faithful port of list_gocardless_partner_merchants
   * (routes.py:1504-1522). Tokens NEVER returned.
   */
  router.get(
    '/api/gocardless/partner/merchants',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const status =
          typeof req.query.status === 'string' && req.query.status.trim()
            ? req.query.status.trim()
            : null;
        const result = await getAllMerchantSignups(appDb, { status });
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Partner merchants failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/gocardless/partner/admin-auth
   *
   * Validate admin password gate for the partner signup app config.
   * Faithful port of gocardless_partner_admin_auth (routes.py:1342-1354).
   * Returns first_time=true when no password is set yet (allow operator
   * to define one).
   */
  router.post(
    '/api/gocardless/partner/admin-auth',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const body = (req.body ?? {}) as { password?: string };
        const result = await partnerAdminAuth(appDb, String(body.password ?? ''));
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Partner admin auth failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * PUT /api/gocardless/partner/admin-password
   *
   * Set or change the partner admin password. Faithful port of
   * update_gocardless_partner_admin_password (routes.py:1357-1369).
   * Minimum 4 chars (matches Python's check).
   */
  router.put(
    '/api/gocardless/partner/admin-password',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const body = (req.body ?? {}) as { password?: string };
        const result = await setPartnerAdminPassword(
          appDb,
          String(body.password ?? ''),
        );
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Partner admin-password failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * PUT /api/gocardless/partner/merchant-app-url
   *
   * Save the deployment URL for a merchant. Faithful port of
   * set_merchant_app_url (routes.py:1372-1388). Strips trailing slash.
   */
  router.put(
    '/api/gocardless/partner/merchant-app-url',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const body = (req.body ?? {}) as { signup_id?: number; app_url?: string };
        const signupId = Number(body.signup_id ?? 0);
        const result = await updateMerchantAppUrl(appDb, {
          signupId,
          appUrl: String(body.app_url ?? ''),
        });
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Set merchant app URL failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * POST /api/gocardless/partner/activate-merchant
   *
   * Push a merchant's GoCardless access token to their app.
   * Faithful port of activate_gocardless_merchant (routes.py:1391-1463).
   *
   * Local-host (localhost / 127.0.0.1 / 0.0.0.0) → write directly to
   * our own settings.api_access_token. Otherwise PUT the token via
   * fetch to {app_url}/api/gocardless/deploy-token (15s timeout). On
   * success, marks signup status='activated'.
   */
  router.post(
    '/api/gocardless/partner/activate-merchant',
    async (req: Request, res: Response) => {
      const appDb = getAppDb(req, res);
      if (!appDb) return;
      try {
        const body = (req.body ?? {}) as { signup_id?: number };
        const signupId = Number(body.signup_id ?? 0);
        const result = await activateMerchant(appDb, { signupId });
        res.json(result);
      } catch (err: any) {
        ctx.logger.error('Activate merchant failed', err);
        res.status(500).json({ success: false, error: err?.message ?? String(err) });
      }
    },
  );

  /**
   * PUT /api/gocardless/deploy-token
   *
   * Receive a GoCardless access token (the activate-merchant flow's
   * remote target). Faithful port of deploy_gocardless_token
   * (routes.py:1466-1484). Saves to settings.api_access_token.
   */
  router.put('/api/gocardless/deploy-token', async (req: Request, res: Response) => {
    const appDb = getAppDb(req, res);
    if (!appDb) return;
    try {
      const body = (req.body ?? {}) as {
        access_token?: string;
        company_name?: string;
      };
      const result = await deployToken(appDb, body);
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Deploy token failed', err);
      res.status(500).json({ success: false, error: err?.message ?? String(err) });
    }
  });

  // Many more endpoints to port from apps/gocardless/api/routes.py:
  //   /api/gocardless/scan-emails        — IMAP/Graph scan via SAM email service
  //   /api/gocardless/preview-batch      — match payments to Opera customers
  //   /api/gocardless/import             — post sales receipts to Opera
  //   /api/gocardless/remittance/*       — generate / send remittance emails
  //   /api/gocardless/partner/*          — partner portal flows
  //   /api/gocardless/update-subscription-tags
  //   /api/gocardless/nominal-accounts   — list of valid Opera nominal codes
  //   /api/gocardless/vat-codes          — list of VAT codes for fees split
  //   ... 100+ endpoints. Each ports independently.

  return router;
}
