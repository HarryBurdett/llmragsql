/**
 * Standalone test runner for the four SAM plugins.
 *
 * Loads each plugin's default-export factory with a real `ctx` containing:
 *  - Live Opera SE connection (from systems.json)
 *  - Per-plugin SQLite database (at ~/.local/sam-test/<plugin>.db)
 *  - Stubs for email-ingest / LLM / graph (see stubs.ts)
 *
 * Mounts each plugin's Express router on the standard URL prefix the
 * plugins were built for (`/api/...`), then listens on port 3001.
 *
 * The legacy Python backend continues to run on port 8000 in parallel;
 * the frontend can toggle between the two via a config flag.
 *
 * Run with:
 *    cd apps-sam/standalone-runner
 *    npm run dev
 */
import express from 'express';
import cors from 'cors';
import type { Knex } from 'knex';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

import { readOperaSEConfig } from './config.js';
import {
  createOperaSystemDb,
  createOperaCompanyDb,
  discoverCompanies,
} from './opera-db.js';
import { createPluginAppDb, createStubSamDb } from './app-db.js';
import {
  stubLogger,
  stubEmailIngest,
  stubLlm,
  stubEmail,
  stubGraph,
} from './stubs.js';

// Plugin factories. The plugins are workspaces; the runtime imports
// resolve to dist/index.js by way of each package.json's "main" field.
import balanceCheckFactory from '@sqlrag/balance-check';
import bankReconcileFactory from '@sqlrag/bank-reconcile';
import gocardlessFactory from '@sqlrag/gocardless';
import suppliersFactory from '@sqlrag/suppliers';

const PORT = parseInt(process.env.PORT ?? '3001', 10);
const APPS_SAM_ROOT = join(
  dirname(fileURLToPath(import.meta.url)),
  '..',
  '..',
);

interface PluginRegistration {
  appId: string;
  /** package name like '@sqlrag/balance-check' */
  packageName: string;
  /** name of the directory under apps-sam/ */
  dirName: string;
  factory: (ctx: any) => Promise<any> | any;
  /** if false, no per-app DB is set up (balance-check is read-only) */
  hasPerAppDb: boolean;
}

const PLUGINS: PluginRegistration[] = [
  { appId: 'balance-check', packageName: '@sqlrag/balance-check', dirName: 'balance-check', factory: balanceCheckFactory as any, hasPerAppDb: false },
  { appId: 'bank-reconcile', packageName: '@sqlrag/bank-reconcile', dirName: 'bank-reconcile', factory: bankReconcileFactory as any, hasPerAppDb: true },
  { appId: 'gocardless', packageName: '@sqlrag/gocardless', dirName: 'gocardless', factory: gocardlessFactory as any, hasPerAppDb: true },
  { appId: 'suppliers', packageName: '@sqlrag/suppliers', dirName: 'suppliers', factory: suppliersFactory as any, hasPerAppDb: true },
];

async function main(): Promise<void> {
  console.log('=== SAM standalone test runner starting ===');
  console.log(`Port: ${PORT}`);
  console.log('');

  // -----------------------------------------------------------------
  // 1. Opera SE connections (shared across all plugins)
  // -----------------------------------------------------------------
  const operaCfg = readOperaSEConfig();
  console.log(`Opera SE: ${operaCfg.systemName} @ ${operaCfg.server}:${operaCfg.port}`);
  console.log(`Default company DB: ${operaCfg.defaultDatabase}`);

  const operaSystemDb = createOperaSystemDb(operaCfg);

  // Try to discover companies; fall back to known mapping if seqco
  // schema doesn't match.
  let companyDbNameByCode: Record<string, string>;
  try {
    companyDbNameByCode = await discoverCompanies(operaSystemDb);
    if (Object.keys(companyDbNameByCode).length === 0) {
      throw new Error('seqco returned no rows');
    }
    console.log(`Discovered companies:`, companyDbNameByCode);
  } catch (err) {
    console.warn(`[startup] Company discovery failed (${err}); using fallback mapping.`);
    // Fallback: just expose the default DB under a generic 'default' code.
    companyDbNameByCode = { default: operaCfg.defaultDatabase };
  }

  // Memoised per-company Knex clients.
  const companyDbCache = new Map<string, Knex>();
  const getCompanyDb = (code: string): Knex | null => {
    const key = code.toUpperCase();
    let db = companyDbCache.get(key);
    if (db) return db;
    const dbName = companyDbNameByCode[key] ?? companyDbNameByCode[code] ?? null;
    if (!dbName) {
      console.warn(`[getCompanyDb] no DB mapping for company code: ${code}`);
      return null;
    }
    db = createOperaCompanyDb(operaCfg, dbName);
    companyDbCache.set(key, db);
    return db;
  };

  const samDb = createStubSamDb();

  // -----------------------------------------------------------------
  // 2. Express app
  // -----------------------------------------------------------------
  const app = express();
  app.use(cors({ origin: true, credentials: true }));
  app.use(express.json({ limit: '50mb' }));
  app.use(express.urlencoded({ extended: true, limit: '50mb' }));

  // Company resolver — sets req.operaCompany from header / query.
  app.use((req, _res, next) => {
    const code = (req.headers['x-opera-company'] as string)
      ?? (req.query.opera_company as string)
      ?? (req.query.company as string)
      ?? 'I'; // default to Intsys per systems.json
    (req as any).operaCompany = code;
    // Stub user so plugins that check req.user don't crash.
    (req as any).user = {
      userId: 'test-user',
      email: 'harry@intsysuk.com',
      role: 'sam-admin',
      userType: 'sam-admin',
      tenantId: 'intsys-test',
      permissions: ['*'],
    };
    next();
  });

  // Health endpoint.
  app.get('/api/health', (_req, res) => {
    res.json({
      ok: true,
      runner: 'standalone',
      opera: { system: operaCfg.systemName, server: operaCfg.server },
      companies: Object.keys(companyDbNameByCode),
      plugins: PLUGINS.map((p) => p.appId),
    });
  });

  // -----------------------------------------------------------------
  // Legacy-shape stubs — endpoints the existing React frontend expects
  // before it'll show the main pages. These are NOT what SAM provides
  // in production (SAM has its own auth + tenant model); they exist
  // only so the standalone runner can be tested using the existing
  // frontend without standing up the whole legacy auth stack.
  // -----------------------------------------------------------------

  // /api/systems — the company/system list shown in the login dropdown.
  app.get('/api/systems', (_req, res) => {
    const systems = Object.entries(companyDbNameByCode)
      .filter(([code]) => code.length === 1) // skip lowercase friendly aliases
      .map(([code, dbName]) => ({
        id: code.toLowerCase(),
        name: `Opera SE — ${dbName}`,
        is_default: code === 'I',
        database: {
          type: 'mssql',
          server: operaCfg.server,
          port: String(operaCfg.port),
          database: dbName,
          username: operaCfg.username,
        },
        opera: { version: 'sql_se' },
      }));
    res.json({ systems });
  });

  // /api/auth/login — accept any credentials and return a fake JWT.
  // The runner is intentionally unauthenticated; anything goes.
  // Response shape matches frontend/src/context/AuthContext.tsx expectations.
  app.post('/api/auth/login', (req, res) => {
    const username = (req.body?.username as string) ?? 'harry';
    res.json({
      success: true,
      token: 'standalone-runner-stub-jwt',
      user: {
        id: 1,
        username,
        display_name: 'Harry Burdett',
        email: 'harry@intsysuk.com',
        role: 'admin',
      },
      permissions: ['*'],
      license: {
        id: 1,
        valid: true,
        expires: '2099-12-31',
        apps: ['*'],
      },
    });
  });

  // /api/auth/me — confirms the current user
  app.get('/api/auth/me', (_req, res) => {
    res.json({
      username: 'harry',
      email: 'harry@intsysuk.com',
      role: 'admin',
    });
  });

  app.post('/api/auth/logout', (_req, res) => {
    res.json({ success: true });
  });

  // /api/companies — list of Opera companies the user can switch between.
  // Drives the company switcher in the legacy frontend's header.
  app.get('/api/companies', (_req, res) => {
    const FRIENDLY: Record<string, string> = {
      I: 'Intsys UK Ltd',
      C: 'Cloudsis Limited',
      K: 'Crakd.ai',
      Z: 'Orion Vehicles Leasing',
    };
    const companies = Object.keys(companyDbNameByCode)
      .filter((c) => c.length === 1)
      .map((code) => ({
        code,
        name: FRIENDLY[code] ?? code,
      }));
    res.json({ companies });
  });

  // /api/auth/switch-company — the legacy switches active company via this
  app.post('/api/auth/switch-company', (req, res) => {
    const company = (req.body?.company as string) ?? 'I';
    res.json({ success: true, company });
  });

  // /api/auth/active-company — confirm current company
  app.get('/api/auth/active-company', (_req, res) => {
    res.json({ company: 'I' });
  });

  // /api/licenses — stub: always valid in test mode
  app.get('/api/licenses', (_req, res) => {
    res.json({
      valid: true,
      licenses: [{ app: 'all', valid: true, expires: '2099-12-31' }],
    });
  });

  // /api/systems/:id/activate — frontend POSTs this when a system is selected
  app.post('/api/systems/:id/activate', (req, res) => {
    res.json({ success: true, active: req.params.id });
  });

  // /api/auth/user-default-company — pre-fill company dropdown based on username
  app.get('/api/auth/user-default-company', (req, res) => {
    res.json({
      username: req.query.username ?? 'harry',
      default_company: 'I',
      name: 'Intsys UK Ltd',
    });
  });

  // /api/companies/list — fuller company list used by Login.tsx
  app.get('/api/companies/list', (_req, res) => {
    const FRIENDLY: Record<string, string> = {
      I: 'Intsys UK Ltd',
      C: 'Cloudsis Limited',
      K: 'Crakd.ai',
      Z: 'Orion Vehicles Leasing',
    };
    const companies = Object.keys(companyDbNameByCode)
      .filter((c) => c.length === 1)
      .map((code) => ({
        code,
        name: FRIENDLY[code] ?? code,
        is_default: code === 'I',
      }));
    res.json({ companies });
  });

  // Force-clear-session — legacy endpoint the frontend hits on errors.
  app.post('/api/auth/force-clear-session', (_req, res) => {
    res.json({ success: true });
  });

  // -----------------------------------------------------------------
  // 3. Load each plugin and mount its router
  // -----------------------------------------------------------------
  for (const plugin of PLUGINS) {
    console.log(`[plugin ${plugin.appId}] loading...`);

    let appDb: Knex | null = null;
    if (plugin.hasPerAppDb) {
      const migrationsDir = join(APPS_SAM_ROOT, plugin.dirName, 'db', 'migrations');
      appDb = await createPluginAppDb({
        pluginId: plugin.appId,
        migrationsDir,
      });
    }

    const ctx = {
      appId: plugin.appId,
      tenantId: 'intsys-test',
      config: {},
      operaType: 'opera-se' as const,
      db: {
        sam: samDb,
        app: appDb,
        operaSystem: operaSystemDb,
        getCompanyDb,
      },
      logger: stubLogger,
      email: stubEmail,
      llm: stubLlm,
      emailIngest: stubEmailIngest,
      graph: stubGraph,
    };

    let router;
    try {
      router = await plugin.factory(ctx);
    } catch (err) {
      console.error(`[plugin ${plugin.appId}] factory failed:`, err);
      continue;
    }

    // Plugins return their router with all routes registered under
    // /api/... — mount at root so the URLs line up exactly with the
    // legacy Python (and the existing frontend).
    app.use(router);
    console.log(`[plugin ${plugin.appId}] mounted`);
  }

  // -----------------------------------------------------------------
  // 4. Listen
  // -----------------------------------------------------------------
  app.listen(PORT, () => {
    console.log('');
    console.log(`==============================================`);
    console.log(`Standalone test runner listening on port ${PORT}`);
    console.log(`==============================================`);
    console.log(`Health:  curl http://localhost:${PORT}/api/health`);
    console.log(`Switch the frontend at port 5173 to use this backend`);
    console.log(`by adding ?test=1 to the URL (after the frontend toggle is`);
    console.log(`wired in).`);
    console.log('');
  });
}

main().catch((err) => {
  console.error('[fatal] runner crashed during startup:', err);
  process.exit(1);
});
