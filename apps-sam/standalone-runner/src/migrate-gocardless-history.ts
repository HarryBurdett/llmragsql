/**
 * Migrate legacy gocardless_imports rows from each company's
 * `data/<co>/core/email_data.db` into the SAM gocardless plugin's
 * per-plugin SQLite at ~/.local/sam-test/gocardless.db.
 *
 * The legacy schema uses different column names than the SAM port:
 *   legacy.gocardless_fees → sam.fees_amount
 *   legacy.import_date     → sam.imported_at  (also seeds payment_date)
 * `customer_name` has no SAM equivalent — dropped.
 *
 * Intsys ids stay as-is; cloudsis offset by +10000 to avoid collisions.
 */
import Database from 'better-sqlite3';
import { existsSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';

const LEGACY = '/Users/maccb/llmragsql/data';
const TARGET = join(homedir(), '.local', 'sam-test');
const COMPANIES = ['intsys', 'cloudsis'] as const;
type Company = (typeof COMPANIES)[number];

function offsetForCompany(c: Company): number { return c === 'intsys' ? 0 : 10000; }

interface LegacyRow {
  id: number;
  email_id: number | null;
  payout_id: string | null;
  source: string | null;
  bank_reference: string | null;
  gross_amount: number | null;
  net_amount: number | null;
  gocardless_fees: number | null;
  vat_on_fees: number | null;
  payment_count: number | null;
  payments_json: string | null;
  target_system: string | null;
  batch_ref: string | null;
  import_date: string | null;
  imported_by: string | null;
  fx_amount: number | null;
  post_date: string | null;
}

function migrate(): void {
  console.log('--- gocardless_imports → suppliers gocardless.db ---');
  const dst = new Database(`${TARGET}/gocardless.db`);
  try {
    const stmt = dst.prepare(`
      INSERT OR IGNORE INTO gocardless_imports
        (id, email_id, payout_id, source, bank_reference,
         gross_amount, net_amount, fees_amount, vat_on_fees,
         payment_count, payments_json, target_system, batch_ref,
         imported_at, imported_by, fx_amount, post_date, payment_date)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    `);
    let total = 0;
    for (const c of COMPANIES) {
      const src = `${LEGACY}/${c}/core/email_data.db`;
      if (!existsSync(src)) { console.log(`  (no email_data.db for ${c})`); continue; }
      const sdb = new Database(src, { readonly: true });
      try {
        const hasTable = sdb.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='gocardless_imports'").get();
        if (!hasTable) { console.log(`  (no gocardless_imports in ${c})`); continue; }
        const rows = sdb.prepare('SELECT * FROM gocardless_imports').all() as LegacyRow[];
        const offset = offsetForCompany(c);
        let copied = 0;
        for (const r of rows) {
          const newId = r.id + offset;
          const importedAt = r.import_date ?? null;
          const paymentDate = r.post_date ?? (importedAt ? importedAt.slice(0, 10) : null);
          const result = stmt.run(
            newId, r.email_id, r.payout_id, r.source ?? 'email', r.bank_reference,
            r.gross_amount, r.net_amount, r.gocardless_fees, r.vat_on_fees,
            r.payment_count, r.payments_json, r.target_system, r.batch_ref,
            importedAt, r.imported_by, r.fx_amount, r.post_date, paymentDate,
          );
          if (result.changes > 0) copied++;
        }
        console.log(`  ${c}: copied ${copied} of ${rows.length} rows`);
        total += copied;
      } finally { sdb.close(); }
    }
    console.log(`Done — ${total} total rows migrated.`);
  } finally { dst.close(); }
}

migrate();
