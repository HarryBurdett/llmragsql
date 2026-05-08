/**
 * suppliers — SAM plugin entry point.
 *
 * The Python suppliers app is incomplete. Per the user's direction,
 * suppliers is finished in TypeScript directly (rather than completing
 * Python first then porting). Foundation + supplier listing in place;
 * feature work continues.
 *
 * SAM loads plugins via `import()` and calls the default export with
 * an AppContext.
 */
import { createRouter } from './router.js';
import type { AppContext, AppBackendFactory } from './app-context.js';

const factory: AppBackendFactory = (ctx: AppContext) => {
  ctx.logger.info(`suppliers plugin loaded for tenant ${ctx.tenantId} (DEV)`);
  return createRouter(ctx);
};

export default factory;
