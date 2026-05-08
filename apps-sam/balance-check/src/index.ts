/**
 * balance-check — SAM plugin entry point.
 *
 * Faithful TypeScript port of the Python `apps/balance_check/` app.
 * No business-logic amendments — only language change.
 *
 * SAM loads plugins via `import()` and calls the default export with
 * an AppContext. We return an Express Router that SAM mounts under
 * `/api/apps/balance-check/*`.
 *
 * See `~/opera-knowledge-ref/docs/plugin-authoring.md` for the full
 * plugin contract.
 */
import { createRouter } from './router.js';
import type { AppContext, AppBackendFactory } from './app-context.js';

/**
 * Default export — the factory function SAM calls.
 *
 * Receives the per-tenant AppContext and returns the Express Router
 * that handles all balance-check endpoints.
 */
const factory: AppBackendFactory = (ctx: AppContext) => {
  ctx.logger.info(`balance-check plugin loaded for tenant ${ctx.tenantId}`);
  return createRouter(ctx);
};

export default factory;
