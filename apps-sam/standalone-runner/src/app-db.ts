/**
 * Per-plugin SQLite Knex client for the standalone test runner.
 *
 * Each plugin gets its own SQLite file at ~/.local/sam-test/<plugin>.db,
 * with its own migrations applied. This mirrors SAM's per-app MSSQL DB
 * concept but using SQLite for zero-infrastructure local testing.
 *
 * The plugin's own `db/migrations/` directory is used to set up the
 * schema on first boot — knex.migrate.latest() runs everything.
 */
import knex, { type Knex } from 'knex';
import { mkdirSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';

const TEST_DATA_DIR = join(homedir(), '.local', 'sam-test');

export interface PluginAppDbOptions {
  /** Plugin id — used as the SQLite filename: <pluginId>.db */
  pluginId: string;
  /** Absolute path to the plugin's `db/migrations/` directory */
  migrationsDir: string;
}

/** Create the test data dir if missing. Called once at startup. */
export function ensureTestDataDir(): string {
  mkdirSync(TEST_DATA_DIR, { recursive: true });
  return TEST_DATA_DIR;
}

/**
 * Open or create a SQLite database for one plugin, run its migrations,
 * and return the connected Knex client.
 */
export async function createPluginAppDb(opts: PluginAppDbOptions): Promise<Knex> {
  ensureTestDataDir();
  const filename = join(TEST_DATA_DIR, `${opts.pluginId}.db`);

  const db = knex({
    client: 'better-sqlite3',
    connection: { filename },
    useNullAsDefault: true,
    pool: { min: 1, max: 1 },
    migrations: {
      directory: opts.migrationsDir,
      extension: 'ts',
      loadExtensions: ['.ts', '.js'],
    },
  });

  // Run migrations to apply any new schema.
  try {
    const [batchNo, applied] = await db.migrate.latest();
    if (applied.length > 0) {
      console.log(`[app-db ${opts.pluginId}] ran ${applied.length} migration(s) (batch ${batchNo}):`, applied);
    } else {
      console.log(`[app-db ${opts.pluginId}] schema up to date (${filename})`);
    }
  } catch (err) {
    console.error(`[app-db ${opts.pluginId}] migrate.latest() failed:`, err);
    throw err;
  }

  return db;
}

/**
 * SAM's own database. The standalone runner doesn't have a real SAM DB,
 * but plugins occasionally read from it (rare). We provide an in-memory
 * SQLite that's effectively empty — plugins that depend on SAM DB data
 * will degrade gracefully or skip those code paths in the test runner.
 */
export function createStubSamDb(): Knex {
  return knex({
    client: 'better-sqlite3',
    connection: { filename: ':memory:' },
    useNullAsDefault: true,
    pool: { min: 1, max: 1 },
  });
}
