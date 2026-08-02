/**
 * Express router for the cashflow plugin.
 *
 * Exposes a single read-only endpoint that returns the forward
 * cashflow forecast for the active Opera company.
 */
import { Router, type Request, type Response } from 'express';
import type { AppContext } from './app-context.js';
import { getCashflowForecast } from './services/forecast.js';

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

  /**
   * GET /api/cashflow/forecast
   *
   * Query params:
   *   as_of_date  YYYY-MM-DD — defaults to today
   *   months      1..24      — defaults to 12
   *
   * Returns:
   *   {
   *     success, as_of_date,
   *     current_position: { bank_total, bank_accounts, debtors_outstanding, creditors_outstanding, net_working_capital },
   *     monthly_forecast: [ { month, label, expected_receipts, expected_payments, net_cashflow, running_balance, sources } ],
   *     totals: { total_receipts, total_payments, net_position, opening_balance, closing_balance, lowest_balance, lowest_balance_month },
   *     assumptions: [ ... ]
   *   }
   */
  router.get('/api/cashflow/forecast', async (req: Request, res: Response) => {
    const operaDb = getOperaDb(req, res);
    if (!operaDb) return;
    try {
      const q = (req.query ?? {}) as Record<string, string | undefined>;
      const result = await getCashflowForecast(operaDb, {
        ...(q.as_of_date ? { asOfDate: q.as_of_date } : {}),
        ...(q.months !== undefined ? { monthsAhead: Number(q.months) } : {}),
      });
      res.json(result);
    } catch (err: any) {
      ctx.logger.error('Cashflow forecast failed', err);
      res
        .status(500)
        .json({ success: false, error: err?.message ?? String(err) });
    }
  });

  /**
   * GET /api/cashflow/health-check
   *
   * Lightweight ping — confirms the per-tenant Opera connection is
   * reachable and the key tables are queryable.
   */
  router.get('/api/cashflow/health-check', async (req: Request, res: Response) => {
    const operaDb = getOperaDb(req, res);
    if (!operaDb) return;
    try {
      const rows = (await operaDb.raw(
        `SELECT COUNT(*) AS n FROM nbank WITH (NOLOCK)`,
      )) as Array<{ n: number }>;
      const banks = Number(rows?.[0]?.n ?? 0);
      res.json({
        success: true,
        app: 'cashflow',
        opera_company: req.operaCompany,
        opera_type: ctx.operaType,
        bank_account_count: banks,
      });
    } catch (err: any) {
      res.status(500).json({
        success: false,
        error: err?.message ?? String(err),
      });
    }
  });

  return router;
}
