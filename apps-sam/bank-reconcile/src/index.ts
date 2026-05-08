/**
 * bank-reconcile — SAM plugin entry point.
 *
 * Foundation in place; endpoints are being ported in stages from the
 * Python `apps/bank_reconcile/` app. See docs/sam-rewrite/progress.md
 * for status.
 *
 * SAM loads plugins via `import()` and calls the default export with
 * an AppContext. We return an Express Router that SAM mounts under
 * `/api/apps/bank-reconcile/*`.
 */
import { Router } from 'express';
import type { AppContext, AppBackendFactory } from './app-context.js';

const factory: AppBackendFactory = (ctx: AppContext) => {
  ctx.logger.info(`bank-reconcile plugin loaded for tenant ${ctx.tenantId}`);

  const router = Router();

  // Foundation endpoint: confirms the plugin is alive and a tenant
  // context resolves. More endpoints will be mounted as the port
  // progresses.
  router.get('/api/bank-reconcile/status', (_req, res) => {
    res.json({
      success: true,
      app: 'bank-reconcile',
      tenant_id: ctx.tenantId,
      opera_type: ctx.operaType,
      message: 'Foundation in place. Endpoint port in progress — see docs/sam-rewrite/progress.md',
    });
  });

  // Endpoints to port next:
  //   GET  /api/bank-import/scan-emails       — scan SAM mailbox for statements
  //   GET  /api/bank-import/scan-folder       — scan local folder for PDFs
  //   GET  /api/bank-import/scan-all-banks
  //   POST /api/bank-import/preview-from-email
  //   POST /api/bank-import/preview-from-pdf
  //   POST /api/bank-import/import            — post to Opera (the BIG one)
  //   GET  /api/reconcile/bank/:code/list
  //   POST /api/reconcile/bank/:code/reconcile
  //   GET  /api/repeat-entries
  //   POST /api/repeat-entries/process
  //   ... ~80+ more

  return router;
};

export default factory;
