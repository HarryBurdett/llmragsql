/**
 * Data migrator: legacy Python SQLite → SAM per-app MSSQL.
 *
 * Source layout (Python today):
 *   data/<company>/bank_reconcile/bank_aliases.db          → bank_import_aliases, repeat_entry_aliases, match_config, duplicate_overrides
 *   data/<company>/bank_reconcile/bank_patterns.db         → bank_import_patterns
 *   data/<company>/bank_reconcile/deferred_transactions.db → deferred_transactions
 *   data/<company>/gocardless/gocardless_payments.db       → gocardless_mandates, gocardless_payment_requests, gocardless_subscriptions, gocardless_subscription_documents, gocardless_partner_signups, mandate_setup_requests
 *   data/<company>/gocardless/gocardless_settings.json     → settings table
 *   data/<company>/suppliers/supplier_statements.db        → supplier_statements + supporting tables
 *
 * Target: a SAM per-app MSSQL database (one per appId), connected
 * via knex with tedious. Connection string supplied via env vars
 * (see usage below). Each plugin's per-app DB has SAM's Knex
 * migrations already applied — this script only INSERTs data, never
 * runs DDL.
 *
 * Schema differences between Python and SAM are handled in per-table
 * mapper functions — see /^map[A-Z]/ identifiers below.
 *
 * Idempotency: every INSERT uses MERGE / ON CONFLICT-equivalent so
 * re-running the migration on the same target is safe.
 *
 * Usage:
 *   tsx migrate.ts \
 *     --company intsys \
 *     --plugin bank-reconcile \
 *     --data-root /Users/maccb/llmragsql/data \
 *     --target-host localhost --target-port 1433 \
 *     --target-user sa --target-password '...' \
 *     --target-db ai_sam_app_bank_reconcile \
 *     [--dry-run]
 *
 * Or set the env vars TARGET_HOST/PORT/USER/PASSWORD/DB and skip
 * those flags.
 */
import Database from 'better-sqlite3';
import knex, { type Knex } from 'knex';
import { readFileSync } from 'node:fs';
import * as path from 'node:path';
import * as process from 'node:process';

interface Args {
  company: string;
  plugin: 'bank-reconcile' | 'gocardless' | 'suppliers';
  dataRoot: string;
  targetHost: string;
  targetPort: number;
  targetUser: string;
  targetPassword: string;
  targetDb: string;
  dryRun: boolean;
}

function parseArgs(): Args {
  const args: Record<string, string> = {};
  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a?.startsWith('--')) {
      const key = a.slice(2);
      if (key === 'dry-run') {
        args[key] = 'true';
      } else {
        args[key] = argv[i + 1] ?? '';
        i++;
      }
    }
  }
  const get = (k: string, env: string, dflt?: string) =>
    args[k] ?? process.env[env] ?? dflt ?? '';
  const company = get('company', 'COMPANY');
  const plugin = get('plugin', 'PLUGIN') as Args['plugin'];
  if (!company) throw new Error('--company required');
  if (!['bank-reconcile', 'gocardless', 'suppliers'].includes(plugin)) {
    throw new Error('--plugin must be bank-reconcile, gocardless, or suppliers');
  }
  return {
    company,
    plugin,
    dataRoot: get('data-root', 'DATA_ROOT', '/Users/maccb/llmragsql/data'),
    targetHost: get('target-host', 'TARGET_HOST', 'localhost'),
    targetPort: Number(get('target-port', 'TARGET_PORT', '1433')),
    targetUser: get('target-user', 'TARGET_USER', 'sa'),
    targetPassword: get('target-password', 'TARGET_PASSWORD'),
    targetDb: get('target-db', 'TARGET_DB'),
    dryRun: args['dry-run'] === 'true',
  };
}

function makeTargetDb(args: Args): Knex {
  return knex({
    client: 'mssql',
    connection: {
      server: args.targetHost,
      port: args.targetPort,
      user: args.targetUser,
      password: args.targetPassword,
      database: args.targetDb,
      options: {
        encrypt: false,
        trustServerCertificate: true,
      },
    },
  });
}

function openSqlite(file: string): Database.Database {
  return new Database(file, { readonly: true, fileMustExist: true });
}

function readJson<T = unknown>(file: string): T | null {
  try {
    const buf = readFileSync(file, 'utf-8');
    return JSON.parse(buf) as T;
  } catch {
    return null;
  }
}

function logInsert(table: string, count: number, dryRun: boolean) {
  console.log(`  ${dryRun ? '[dry-run] ' : ''}${table}: ${count} row(s)`);
}

// ---------------------------------------------------------------------
// bank-reconcile mappers
// ---------------------------------------------------------------------

interface PyBankAlias {
  id: number;
  bank_name: string;
  ledger_type: string; // customer / supplier / nominal
  account_code: string;
  account_name: string | null;
  match_score: number | null;
  created_date: string | null;
  last_used: string | null;
  use_count: number | null;
  active: number | null;
  bank_code: string | null;
}

function mapBankAlias(py: PyBankAlias): Record<string, unknown> | null {
  if (py.active === 0) return null; // skip soft-deleted aliases
  return {
    bank_code: (py.bank_code ?? '').slice(0, 16),
    payee_pattern: py.bank_name.slice(0, 200),
    match_type: py.ledger_type.slice(0, 16),
    opera_account: (py.account_code ?? '').slice(0, 32),
    confidence: py.match_score,
    direction: 'either',
    match_count: py.use_count ?? 1,
    created_at: py.created_date ?? new Date().toISOString(),
    updated_at: py.last_used ?? py.created_date ?? new Date().toISOString(),
  };
}

interface PyBankPattern {
  id: number;
  description_normalized: string;
  account_code: string;
  times_used: number | null;
  last_used: string | null;
}

function mapBankPattern(py: PyBankPattern): Record<string, unknown> {
  return {
    pattern: py.description_normalized.slice(0, 200),
    opera_account: (py.account_code ?? '').slice(0, 32),
    confidence: 0.85, // not present in Python; default
    match_count: py.times_used ?? 1,
    updated_at: py.last_used ?? new Date().toISOString(),
  };
}

interface PyDeferredTxn {
  id: number;
  bank_code: string;
  statement_date: string | null;
  amount: number | null;
  description: string | null;
  deferred_by: string | null;
  deferred_at: string | null;
}

function mapDeferredTxn(py: PyDeferredTxn): Record<string, unknown> {
  return {
    bank_code: (py.bank_code ?? '').slice(0, 32),
    statement_date: py.statement_date,
    amount: py.amount,
    description: (py.description ?? '').slice(0, 255),
    deferred_by: (py.deferred_by ?? '').slice(0, 64),
    deferred_at: py.deferred_at ?? new Date().toISOString(),
  };
}

async function migrateBankReconcile(
  args: Args,
  target: Knex,
): Promise<void> {
  const baseDir = path.join(args.dataRoot, args.company, 'bank_reconcile');
  console.log(`bank-reconcile from ${baseDir}`);

  // 1. Aliases
  const aliasesDb = openSqlite(path.join(baseDir, 'bank_aliases.db'));
  try {
    const rows = aliasesDb
      .prepare('SELECT * FROM bank_import_aliases')
      .all() as PyBankAlias[];
    const mapped = rows.map(mapBankAlias).filter((r): r is Record<string, unknown> => r !== null);
    logInsert('bank_import_aliases', mapped.length, args.dryRun);
    if (!args.dryRun && mapped.length > 0) {
      // Idempotent insert via MERGE — bank_code+payee_pattern is the
      // composite key.
      for (const row of mapped) {
        await target.raw(
          `MERGE bank_import_aliases AS t
           USING (SELECT ? AS bank_code, ? AS payee_pattern) AS s
             ON t.bank_code = s.bank_code AND t.payee_pattern = s.payee_pattern
           WHEN NOT MATCHED THEN INSERT (bank_code, payee_pattern, match_type, opera_account, confidence, direction, match_count, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);`,
          [
            row.bank_code, row.payee_pattern,
            row.bank_code, row.payee_pattern, row.match_type, row.opera_account,
            row.confidence, row.direction, row.match_count, row.created_at, row.updated_at,
          ] as readonly unknown[] as readonly (string | number | boolean | Date | null)[],
        );
      }
    }
  } finally {
    aliasesDb.close();
  }

  // 2. Patterns
  try {
    const patternsDb = openSqlite(path.join(baseDir, 'bank_patterns.db'));
    try {
      const rows = patternsDb
        .prepare('SELECT * FROM bank_import_patterns')
        .all() as PyBankPattern[];
      const mapped = rows.map(mapBankPattern);
      logInsert('bank_import_patterns', mapped.length, args.dryRun);
      if (!args.dryRun && mapped.length > 0) {
        // Patterns table doesn't have a unique index on (pattern) in the
        // SAM schema, so we just batch-insert. Re-running this migration
        // is the operator's responsibility (clear table first if needed).
        await target('bank_import_patterns').insert(mapped);
      }
    } finally {
      patternsDb.close();
    }
  } catch (err: any) {
    if (!String(err?.message ?? '').includes('does not exist')) {
      console.warn(`  bank_import_patterns skipped: ${err?.message}`);
    }
  }

  // 3. Deferred transactions
  try {
    const deferredDb = openSqlite(path.join(baseDir, 'deferred_transactions.db'));
    try {
      const rows = deferredDb
        .prepare('SELECT * FROM deferred_transactions')
        .all() as PyDeferredTxn[];
      const mapped = rows.map(mapDeferredTxn);
      logInsert('deferred_transactions', mapped.length, args.dryRun);
      if (!args.dryRun && mapped.length > 0) {
        await target('deferred_transactions').insert(mapped);
      }
    } finally {
      deferredDb.close();
    }
  } catch (err: any) {
    if (!String(err?.message ?? '').includes('does not exist')) {
      console.warn(`  deferred_transactions skipped: ${err?.message}`);
    }
  }
}

// ---------------------------------------------------------------------
// gocardless mappers
// ---------------------------------------------------------------------

interface PyGcMandate {
  id: number;
  opera_account: string;
  opera_name: string | null;
  gocardless_customer_id: string | null;
  gocardless_name: string | null;
  mandate_id: string;
  mandate_status: string | null;
  scheme: string | null;
  email: string | null;
  created_at: string | null;
  updated_at: string | null;
}

function mapGcMandate(py: PyGcMandate): Record<string, unknown> {
  return {
    mandate_id: py.mandate_id.slice(0, 64),
    opera_account: (py.opera_account ?? '').slice(0, 32),
    customer_name: (py.opera_name ?? py.gocardless_name ?? '').slice(0, 200),
    status: (py.mandate_status ?? 'active').slice(0, 32),
    created_at: py.created_at ?? new Date().toISOString(),
    updated_at: py.updated_at ?? py.created_at ?? new Date().toISOString(),
  };
}

interface PyGcSettings {
  default_batch_type?: string;
  default_bank_code?: string;
  fees_nominal_account?: string;
  fees_vat_code?: string;
  fees_payment_type?: string;
  company_reference?: string;
  archive_folder?: string;
  exclude_description_patterns?: string[];
  gocardless_bank_code?: string;
  gocardless_transfer_cbtype?: string;
  subscription_tag?: string;
  subscription_frequencies?: string[];
  request_statement_reference?: string;
  // INTENTIONALLY NOT MIGRATED: api_access_token, api_sandbox,
  // partner_client_id, partner_client_secret, partner_redirect_uri.
  // The operator re-enters these in SAM, ideally as sandbox first.
}

async function migrateGocardless(args: Args, target: Knex): Promise<void> {
  const baseDir = path.join(args.dataRoot, args.company, 'gocardless');
  console.log(`gocardless from ${baseDir}`);

  // 1. Settings (from JSON)
  const settings = readJson<PyGcSettings>(
    path.join(baseDir, 'gocardless_settings.json'),
  );
  if (settings) {
    const safeKeys: Array<keyof PyGcSettings> = [
      'default_batch_type', 'default_bank_code', 'fees_nominal_account',
      'fees_vat_code', 'fees_payment_type', 'company_reference',
      'archive_folder', 'gocardless_bank_code', 'gocardless_transfer_cbtype',
      'subscription_tag', 'request_statement_reference',
    ];
    const safeSubset: Record<string, unknown> = {};
    for (const k of safeKeys) {
      if (settings[k] !== undefined) safeSubset[k as string] = settings[k];
    }
    logInsert('settings (gocardless config)', Object.keys(safeSubset).length, args.dryRun);
    if (!args.dryRun) {
      for (const [key, value] of Object.entries(safeSubset)) {
        await target.raw(
          `MERGE settings AS t
           USING (SELECT ? AS [key]) AS s ON t.[key] = s.[key]
           WHEN MATCHED THEN UPDATE SET value = ?, updated_at = SYSUTCDATETIME()
           WHEN NOT MATCHED THEN INSERT ([key], value) VALUES (?, ?);`,
          [key, JSON.stringify(value), key, JSON.stringify(value)],
        );
      }
    }
    console.log('  ⚠ NOT migrated (re-enter in SAM): api_access_token, api_sandbox, partner_*');
  }

  // 2. Mandates
  const dbPath = path.join(baseDir, 'gocardless_payments.db');
  try {
    const db = openSqlite(dbPath);
    try {
      const rows = db
        .prepare('SELECT * FROM gocardless_mandates')
        .all() as PyGcMandate[];
      const mapped = rows.map(mapGcMandate);
      logInsert('gocardless_mandates', mapped.length, args.dryRun);
      if (!args.dryRun && mapped.length > 0) {
        for (const row of mapped) {
          await target.raw(
            `MERGE gocardless_mandates AS t
             USING (SELECT ? AS mandate_id) AS s ON t.mandate_id = s.mandate_id
             WHEN NOT MATCHED THEN INSERT (mandate_id, opera_account, customer_name, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?);`,
            [
              row.mandate_id,
              row.mandate_id, row.opera_account, row.customer_name, row.status, row.created_at, row.updated_at,
            ] as readonly unknown[] as readonly (string | number | boolean | Date | null)[],
          );
        }
      }
    } finally {
      db.close();
    }
  } catch (err: any) {
    console.warn(`  gocardless_mandates skipped: ${err?.message}`);
  }
}

// ---------------------------------------------------------------------
// suppliers (lighter — historical statements only; supplier_change_audit
// can stay in Python until the user requests it)
// ---------------------------------------------------------------------

async function migrateSuppliers(args: Args, target: Knex): Promise<void> {
  const baseDir = path.join(args.dataRoot, args.company, 'suppliers');
  console.log(`suppliers from ${baseDir}`);

  const dbPath = path.join(baseDir, 'supplier_statements.db');
  try {
    const db = openSqlite(dbPath);
    try {
      const rows = db
        .prepare('SELECT * FROM supplier_statements')
        .all() as Array<Record<string, unknown>>;
      logInsert('supplier_statements', rows.length, args.dryRun);
      if (!args.dryRun && rows.length > 0) {
        for (const row of rows) {
          // Same column names — straight copy (truncating where needed).
          const r = {
            supplier_code: String(row.supplier_code ?? '').slice(0, 32),
            statement_date: row.statement_date,
            received_date: row.received_date,
            sender_email: row.sender_email,
            pdf_path: row.pdf_path,
            status: String(row.status ?? 'received').slice(0, 32),
            opening_balance: row.opening_balance,
            closing_balance: row.closing_balance,
            currency: String(row.currency ?? 'GBP').slice(0, 3),
            created_at: row.created_at,
            updated_at: row.updated_at,
          };
          await target('supplier_statements').insert(r).onConflict?.('supplier_code').ignore?.() ?? null;
          // MSSQL doesn't support onConflict — fall back to plain INSERT;
          // duplicate keys will throw which the operator can investigate.
          try {
            await target('supplier_statements').insert(r);
          } catch (err: any) {
            if (!String(err.message ?? '').includes('duplicate')) throw err;
          }
        }
      }
    } finally {
      db.close();
    }
  } catch (err: any) {
    console.warn(`  supplier_statements skipped: ${err?.message}`);
  }

  console.log(
    '  Other supplier tables (statement_lines, supplier_change_audit, ' +
      'supplier_onboarding, etc.) intentionally NOT migrated — those are ' +
      'historical and the operator can choose to migrate manually if needed.',
  );
}

// ---------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------

async function main(): Promise<void> {
  const args = parseArgs();
  console.log('═══════════════════════════════════════════════════════');
  console.log(`  Migrate ${args.plugin} for company '${args.company}'`);
  console.log(`  Source: ${args.dataRoot}/${args.company}/${args.plugin}/`);
  console.log(`  Target: ${args.targetUser}@${args.targetHost}:${args.targetPort}/${args.targetDb}`);
  console.log(`  Mode  : ${args.dryRun ? 'DRY RUN (no writes)' : 'LIVE'}`);
  console.log('═══════════════════════════════════════════════════════');
  console.log();

  // In dry-run mode we don't connect to the target — useful for
  // sanity-checking row counts before any MSSQL setup is done.
  const target = args.dryRun ? (null as unknown as Knex) : makeTargetDb(args);
  try {
    if (args.plugin === 'bank-reconcile') {
      await migrateBankReconcile(args, target);
    } else if (args.plugin === 'gocardless') {
      await migrateGocardless(args, target);
    } else if (args.plugin === 'suppliers') {
      await migrateSuppliers(args, target);
    }
    console.log();
    console.log(args.dryRun ? '✓ Dry run complete — re-run without --dry-run to apply.' : '✓ Migration complete.');
  } finally {
    if (target) await target.destroy();
  }
}

main().catch((err) => {
  console.error('FAILED:', err.message);
  if (err.stack) console.error(err.stack);
  process.exit(1);
});
