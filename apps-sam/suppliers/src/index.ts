/**
 * suppliers — SAM plugin entry point.
 *
 * The Python suppliers app is incomplete. Per the user's direction,
 * suppliers is finished in TypeScript directly (rather than completing
 * Python first then porting). Foundation in place; feature work
 * proceeds in subsequent sessions.
 *
 * SAM loads plugins via `import()` and calls the default export with
 * an AppContext.
 */
import { Router } from 'express';
import type { AppContext, AppBackendFactory } from './app-context.js';

const factory: AppBackendFactory = (ctx: AppContext) => {
  ctx.logger.info(`suppliers plugin loaded for tenant ${ctx.tenantId} (DEV)`);

  const router = Router();

  // Foundation endpoint: confirms the plugin is alive.
  router.get('/api/suppliers/status', (_req, res) => {
    res.json({
      success: true,
      app: 'suppliers',
      tenant_id: ctx.tenantId,
      opera_type: ctx.operaType,
      message:
        'In development. Being completed in TypeScript directly. ' +
        'See docs/sam-rewrite/progress.md for status.',
    });
  });

  // Endpoints to add (target shape — design driven by the existing
  // Python skeleton + frontend wireframes):
  //   GET  /api/suppliers/                   — list of suppliers + reconciliation status
  //   POST /api/suppliers/scan-emails        — scan SAM mailbox for statements
  //   POST /api/suppliers/extract-statement  — AI-extract line items from PDF
  //   POST /api/suppliers/reconcile          — reconcile statement vs ptran
  //   GET  /api/suppliers/:code/contacts     — supplier contacts
  //   POST /api/suppliers/onboard            — supplier onboarding flow
  //   POST /api/suppliers/remittance         — generate + send remittance email
  //   POST /api/suppliers/aged               — aged debt analysis

  return router;
};

export default factory;
