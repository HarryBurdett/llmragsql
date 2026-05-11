/**
 * Knex client factories for Opera SE in the standalone test runner.
 *
 * Two flavours:
 *  - `createOperaSystemDb()` — connection to Opera3SESystem (the system
 *    database holding `seqco` etc.)
 *  - `createOperaCompanyDb(databaseName)` — connection to a specific
 *    company database (e.g. Opera3SECompany00I for Intsys)
 *
 * Plus `discoverCompanies(systemDb)` which queries seqco to map
 * Opera company codes to database names. Used to satisfy SAM's
 * `ctx.db.getCompanyDb(code)` contract.
 */
import knex, { type Knex } from 'knex';
import type { OperaSEConfig } from './config.js';

function baseConnection(cfg: OperaSEConfig, databaseName: string) {
  // Match SAM's working config (packages/backend/src/opera/pool.ts):
  // host (not server), encrypt: false, trustServerCertificate: true.
  // Opera SE typically isn't TLS-configured so encrypt: false is required.
  return {
    client: 'mssql' as const,
    connection: {
      host: cfg.server,
      port: cfg.port,
      user: cfg.username,
      password: cfg.password,
      database: databaseName,
      options: {
        encrypt: false,
        trustServerCertificate: true,
      },
    },
    pool: { min: 0, max: 5 },
  };
}

export function createOperaSystemDb(cfg: OperaSEConfig): Knex {
  return knex(baseConnection(cfg, 'Opera3SESystem'));
}

export function createOperaCompanyDb(cfg: OperaSEConfig, databaseName: string): Knex {
  return knex(baseConnection(cfg, databaseName));
}

/**
 * Query `seqco` (on Opera3SESystem) to discover Opera companies.
 * Returns a map of company code → database name.
 *
 * Opera's convention: each company has a single-char code in `seqco.co_code`
 * and lives in a database named `Opera3SECompany00<CODE>`.
 *
 * Example data (verified against live system 2026-05-11):
 *   I → Opera3SECompany00I (Intsys UK Ltd)
 *   C → Opera3SECompany00C (Cloudsis Limited)
 *   K → Opera3SECompany00K (Crakd.ai)
 *   Z → Opera3SECompany00Z (Orion Vehicles Leasing)
 *
 * Also exposes lowercase aliases (e.g. 'intsys' → ..I, 'cloudsis' → ..C)
 * so callers can pass friendly company names.
 */
export async function discoverCompanies(
  systemDb: Knex,
): Promise<Record<string, string>> {
  const result: Record<string, string> = {};
  const FRIENDLY: Record<string, string> = {
    INTSYS: 'I',
    CLOUDSIS: 'C',
    CRAKD: 'K',
    ORION: 'Z',
  };

  // knex.raw on mssql returns the rows array directly (no [rows, fields] wrapper).
  const rows = await systemDb.raw(
    `SELECT co_code, RTRIM(co_name) AS co_name FROM seqco ORDER BY co_code`,
  ) as Array<{ co_code: string; co_name: string }>;

  for (const row of rows) {
    const code = (row.co_code ?? '').trim().toUpperCase();
    if (!code) continue;
    const dbName = `Opera3SECompany00${code}`;
    result[code] = dbName;
    // Lowercase friendly alias if we have a mapping.
    for (const [friendly, mapped] of Object.entries(FRIENDLY)) {
      if (mapped === code) {
        result[friendly.toLowerCase()] = dbName;
      }
    }
  }
  return result;
}
