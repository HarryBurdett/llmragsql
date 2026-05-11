/**
 * Migrate accumulated legacy Python app data from the legacy SQLite
 * files into the standalone runner's per-plugin SQLite databases.
 *
 * Strategy: for tables where the legacy + target schemas match closely
 * (gocardless mandates, gocardless subscriptions, supplier statements),
 * do a direct copy of the intersecting columns with INSERT OR IGNORE.
 *
 * For tables with fundamentally different schemas (bank-reconcile
 * aliases/patterns), this migration script skips them. Those need
 * a proper schema-remapping pass that's beyond the scope of "get
 * useful data in for testing today". Harry's smoke tests can still
 * exercise those plugins against live Opera data without the learned
 * state.
 *
 * Idempotency: INSERT OR IGNORE — re-running is safe.
 *
 * Usage:
 *   tsx migrate-legacy-sqlite.ts
 *   tsx migrate-legacy-sqlite.ts intsys     # only intsys
 */
import Database from 'better-sqlite3';
import { existsSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';

const LEGACY_ROOT = '/Users/maccb/llmragsql/data';
const TARGET_ROOT = join(homedir(), '.local', 'sam-test');
const COMPANIES = ['intsys', 'cloudsis'] as const;
type Company = (typeof COMPANIES)[number];

/** company → opera company code ('I' / 'C') */
function companyCode(c: Company): string {
  return c === 'intsys' ? 'I' : 'C';
}

interface CopySpec {
  description: string;
  legacyFile: string;
  legacyTable: string;
  targetFile: string;
  targetTable: string;
  /** Columns to copy (intersection of legacy + target). */
  columns: string[];
  /** Optional values to inject on every row (e.g. company_code). */
  injectColumns?: Record<string, unknown>;
}

function makeSpecs(company: Company): CopySpec[] {
  const cc = companyCode(company);
  return [
    {
      description: `gocardless mandates (${company})`,
      legacyFile: `${LEGACY_ROOT}/${company}/gocardless/gocardless_payments.db`,
      legacyTable: 'gocardless_mandates',
      targetFile: `${TARGET_ROOT}/gocardless.db`,
      targetTable: 'gocardless_mandates',
      columns: [
        'opera_account', 'opera_name', 'gocardless_customer_id',
        'mandate_id', 'mandate_status', 'scheme', 'email',
        'created_at', 'updated_at', 'gocardless_name',
      ],
    },
    {
      description: `gocardless subscriptions (${company})`,
      legacyFile: `${LEGACY_ROOT}/${company}/gocardless/gocardless_payments.db`,
      legacyTable: 'gocardless_subscriptions',
      targetFile: `${TARGET_ROOT}/gocardless.db`,
      targetTable: 'gocardless_subscriptions',
      columns: [
        'subscription_id', 'mandate_id', 'opera_account', 'opera_name',
        'source_doc', 'amount_pence', 'currency', 'interval_unit',
        'interval_count', 'day_of_month', 'name', 'status',
        'start_date', 'end_date', 'created_at', 'updated_at', 'synced_at',
      ],
    },
    {
      description: `gocardless payment_requests (${company})`,
      legacyFile: `${LEGACY_ROOT}/${company}/gocardless/gocardless_payments.db`,
      legacyTable: 'gocardless_payment_requests',
      targetFile: `${TARGET_ROOT}/gocardless.db`,
      targetTable: 'gocardless_payment_requests',
      columns: [
        'payment_id', 'mandate_id', 'opera_account', 'amount_pence',
        'currency', 'charge_date', 'status', 'payout_id',
        'invoice_refs', 'opera_receipt_ref', 'error_message',
        'created_at', 'updated_at',
      ],
    },
    {
      description: `supplier_statements (${company})`,
      legacyFile: `${LEGACY_ROOT}/${company}/suppliers/supplier_statements.db`,
      legacyTable: 'supplier_statements',
      targetFile: `${TARGET_ROOT}/suppliers.db`,
      targetTable: 'supplier_statements',
      columns: [
        'supplier_code', 'statement_date', 'received_date', 'sender_email',
        'pdf_path', 'status', 'opening_balance', 'closing_balance',
        'currency', 'acknowledged_at', 'processed_at', 'approved_by',
        'approved_at', 'sent_at', 'error_message', 'response_text',
      ],
    },
    {
      description: `supplier_change_audit (${company})`,
      legacyFile: `${LEGACY_ROOT}/${company}/suppliers/supplier_statements.db`,
      legacyTable: 'supplier_change_audit',
      targetFile: `${TARGET_ROOT}/suppliers.db`,
      targetTable: 'supplier_change_audit',
      columns: [
        'supplier_code', 'changed_by', 'changed_at',
        'verified', 'verified_by', 'verified_at',
      ],
    },
    {
      description: `supplier_communications (${company})`,
      legacyFile: `${LEGACY_ROOT}/${company}/suppliers/supplier_statements.db`,
      legacyTable: 'supplier_communications',
      targetFile: `${TARGET_ROOT}/suppliers.db`,
      targetTable: 'supplier_communications',
      columns: [
        'supplier_code', 'subject', 'content', 'sent_at',
      ],
    },
  ];
}

function tableExists(db: Database.Database, name: string): boolean {
  const r = db.prepare('SELECT 1 FROM sqlite_master WHERE type=? AND name=? LIMIT 1').get('table', name);
  return Boolean(r);
}

function existingColumns(db: Database.Database, table: string): Set<string> {
  const rows = db.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>;
  return new Set(rows.map((r) => r.name));
}

function copyOne(spec: CopySpec): { copied: number; skipped: number; errors: number; total: number } {
  if (!existsSync(spec.legacyFile)) {
    console.log(`  [skip] ${spec.description} — legacy file missing`);
    return { copied: 0, skipped: 0, errors: 0, total: 0 };
  }
  if (!existsSync(spec.targetFile)) {
    console.log(`  [skip] ${spec.description} — target plugin DB missing (start the runner once first)`);
    return { copied: 0, skipped: 0, errors: 0, total: 0 };
  }

  const src = new Database(spec.legacyFile, { readonly: true });
  const dst = new Database(spec.targetFile);

  try {
    if (!tableExists(src, spec.legacyTable) || !tableExists(dst, spec.targetTable)) {
      console.log(`  [skip] ${spec.description} — source or target table missing`);
      return { copied: 0, skipped: 0, errors: 0, total: 0 };
    }

    // Only copy columns that exist in BOTH legacy and target.
    const legacyCols = existingColumns(src, spec.legacyTable);
    const targetCols = existingColumns(dst, spec.targetTable);
    const cols = spec.columns.filter((c) => legacyCols.has(c) && targetCols.has(c));
    if (cols.length === 0) {
      console.log(`  [skip] ${spec.description} — no shared columns`);
      return { copied: 0, skipped: 0, errors: 0, total: 0 };
    }

    // Optional inject columns must also be valid target cols
    const injectCols = Object.entries(spec.injectColumns ?? {})
      .filter(([k]) => targetCols.has(k));

    const allTargetCols = [...cols, ...injectCols.map(([k]) => k)];
    const placeholders = allTargetCols.map(() => '?').join(',');
    const insertSql = `INSERT OR IGNORE INTO ${spec.targetTable} (${allTargetCols.join(',')}) VALUES (${placeholders})`;
    const stmt = dst.prepare(insertSql);

    const rows = src.prepare(`SELECT ${cols.join(',')} FROM ${spec.legacyTable}`).all() as Array<Record<string, unknown>>;
    let copied = 0, errors = 0;
    for (const r of rows) {
      const values = [
        ...cols.map((c) => r[c] ?? null),
        ...injectCols.map(([, v]) => v),
      ];
      try {
        const result = stmt.run(...values);
        if (result.changes > 0) copied++;
      } catch (err) {
        errors++;
        if (errors <= 2) {
          console.warn(`    [warn] row insert failed: ${(err as Error).message.slice(0, 200)}`);
        }
      }
    }
    return { copied, skipped: rows.length - copied - errors, errors, total: rows.length };
  } finally {
    src.close();
    dst.close();
  }
}

async function main(): Promise<void> {
  const targetCompany = process.argv[2] as Company | undefined;
  const companies = targetCompany ? [targetCompany] : COMPANIES;

  console.log('=== Legacy SQLite → standalone-runner SQLite migration ===');
  console.log(`Companies: ${companies.join(', ')}`);
  console.log('');
  console.log('Note: bank-reconcile aliases and patterns are NOT migrated by');
  console.log('this script — their schemas have evolved between legacy and');
  console.log('target and need a proper remap that\'s not yet implemented.');
  console.log('Plugins will still work; they\'ll just start with empty learned state.');
  console.log('');

  let totalCopied = 0, totalErrors = 0;
  for (const company of companies) {
    console.log(`--- ${company} ---`);
    for (const spec of makeSpecs(company as Company)) {
      const r = copyOne(spec);
      const status = r.total === 0
        ? '(nothing to do)'
        : `copied=${r.copied}/${r.total}${r.errors > 0 ? `, errors=${r.errors}` : ''}`;
      console.log(`  ${spec.description}: ${status}`);
      totalCopied += r.copied;
      totalErrors += r.errors;
    }
    console.log('');
  }
  console.log(`=== Total: ${totalCopied} rows copied across all combinations${totalErrors > 0 ? `, ${totalErrors} errors` : ''} ===`);
}

main().catch((err) => {
  console.error('Migration crashed:', err);
  process.exit(1);
});
