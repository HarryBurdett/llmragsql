import { readFileSync } from 'node:fs';
import knex from 'knex';

const recs = JSON.parse(readFileSync('/Users/maccb/llmragsql/systems.json', 'utf-8'));
const d = recs.find((r: any) => r.is_default).database;
const db = knex({
  client: 'mssql',
  connection: {
    host: d.server,
    port: parseInt(d.port, 10),
    user: d.username,
    password: d.password,
    database: 'Opera3SECompany00C',
    options: { encrypt: false, trustServerCertificate: true },
  },
  pool: { min: 0, max: 5 },
});

async function main() {
  for (const tbl of ['zvtran', 'nvat', 'ntran']) {
    const cols = (await db.raw(
      `SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=? ORDER BY ORDINAL_POSITION`,
      [tbl],
    )) as Array<{ COLUMN_NAME: string; DATA_TYPE: string }>;
    console.log(`\n=== ${tbl} (${cols.length} columns) ===`);
    console.log(cols.map((c) => `${c.COLUMN_NAME}:${c.DATA_TYPE}`).join('  '));
  }
  await db.destroy();
}
main().catch((e) => {
  console.error(e);
  process.exit(1);
});
