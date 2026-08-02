/**
 * cashflow — SAM plugin entry point.
 *
 * Forward cashflow forecast against Opera SE. Read-only.
 *
 * The forecast combines three signals:
 *   1. Known commitments (next 60 days) — outstanding debtors/creditors
 *      bucketed by expected payment date (st_trdate + sn_terms, or
 *      pt_trdate + pn_terms; falling back to st_dueday / pt_dueday).
 *   2. Recurring entries (arhead / arline) — scheduled cashbook
 *      postings via ae_nxtpost + ae_freq.
 *   3. Historical averages (months 3-12) — 12-month average receipts
 *      and payments by calendar month.
 *
 * Opening position is the sum of nbank.nk_curbal across non-foreign
 * bank accounts (in pence; converted to pounds in the response).
 */
import { createRouter } from './router.js';
import type { AppContext, AppBackendFactory } from './app-context.js';

const factory: AppBackendFactory = (ctx: AppContext) => {
  ctx.logger.info(`cashflow plugin loaded for tenant ${ctx.tenantId}`);
  return createRouter(ctx);
};

export default factory;
