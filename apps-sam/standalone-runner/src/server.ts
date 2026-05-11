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
