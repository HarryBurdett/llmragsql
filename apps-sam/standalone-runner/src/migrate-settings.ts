/**
 * Migrate plugin SETTINGS from legacy Python to standalone-runner SQLite.
 *
 * Handles the non-trivial transforms:
 *  - bank-reconcile folder_settings — pulled from core/company_settings.json
 *  - suppliers supplier_config — legacy column-per-field → new JSON blob
 *  - suppliers supplier_automation_settings — legacy key/value carry-over
 *  - suppliers supplier_approved_emails — direct column copy
 *
 * Idempotent. Re-running is safe.
 *
 * Usage: tsx migrate-settings.ts
 */
import Database from 'better-sqlite3';
import { existsSync, readFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';

const LEGACY = '/Users/maccb/llmragsql/data';
const TARGET = join(homedir(), '.local', 'sam-test');
const COMPANIES = ['intsys', 'cloudsis'];

function upsertSettingsRow(db: Database.Database, key: string, value: string): void {
  const now = new Date().toISOString();
  db.prepare(`
    INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)
    ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
  `).run(key, value, now);
}

function migrateBankReconcileFolders(): void {
  console.log('--- bank-reconcile: folder_settings ---');
  // Take the intsys folder paths (default system). cloudsis can be swapped later
  // by re-running this script with --company cloudsis if needed.
  const src = `${LEGACY}/intsys/core/company_settings.json`;
  if (!existsSync(src)) {
    console.log('  [skip] no legacy company_settings.json');
    return;
  }
  const cfg = JSON.parse(readFileSync(src, 'utf-8')) as Record<string, unknown>;
  const value = JSON.stringify({
    base_folder: cfg.bank_statements_base_folder ?? '',
    archive_folder: cfg.bank_statements_archive_folder ?? '',
  });
  const db = new Database(`${TARGET}/bank-reconcile.db`);
  try {
    upsertSettingsRow(db, 'folder_settings', value);
    console.log(`  set folder_settings: base=${cfg.bank_statements_base_folder ?? '(none)'}`);
  } finally {
    db.close();
  }
}

function migrateSupplierAutomationSettings(): void {
  console.log('--- suppliers: supplier_automation_settings (global key/value) ---');
  const dst = new Database(`${TARGET}/suppliers.db`);
  try {
    let copied = 0;
    for (const c of COMPANIES) {
      const src = `${LEGACY}/${c}/suppliers/supplier_statements.db`;
      if (!existsSync(src)) continue;
      const srcDb = new Database(src, { readonly: true });
      try {
        const exists = srcDb.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='supplier_automation_config'").get();
        if (!exists) continue;
        const rows = srcDb.prepare("SELECT key, value FROM supplier_automation_config").all() as Array<{key: string; value: string}>;
        for (const r of rows) {
          if (!r.key) continue;
          const namespaced = `${c}/${r.key}`;
          upsertSettingsRow(dst, namespaced, r.value ?? '');
          copied++;
        }
      } finally { srcDb.close(); }
    }
    console.log(`  copied ${copied} automation settings (namespaced as company/key)`);
  } finally { dst.close(); }
}

function migrateSupplierApprovedEmails(): void {
  console.log('--- suppliers: supplier_approved_emails ---');
  const dst = new Database(`${TARGET}/suppliers.db`);
  try {
    const insertStmt = dst.prepare(`
      INSERT OR IGNORE INTO supplier_approved_emails (supplier_code, email_address, approved_at)
      VALUES (?, ?, ?)
    `);
    let copied = 0;
    for (const c of COMPANIES) {
      const src = `${LEGACY}/${c}/suppliers/supplier_statements.db`;
      if (!existsSync(src)) continue;
      const srcDb = new Database(src, { readonly: true });
      try {
        const exists = srcDb.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='supplier_approved_emails'").get();
        if (!exists) continue;
        const rows = srcDb.prepare("SELECT supplier_code, email_address, added_at AS approved_at FROM supplier_approved_emails").all() as any[];
        for (const r of rows) {
          if (!r.supplier_code || !r.email_address) continue;
          const result = insertStmt.run(r.supplier_code, r.email_address, r.approved_at ?? new Date().toISOString());
          if (result.changes > 0) copied++;
        }
      } finally { srcDb.close(); }
    }
    console.log(`  copied ${copied} approved emails`);
  } finally { dst.close(); }
}

function migrateSupplierConfig(): void {
  console.log('--- suppliers: supplier_config (legacy columns → JSON blob) ---');
  const dst = new Database(`${TARGET}/suppliers.db`);
  try {
    const insertStmt = dst.prepare(`
      INSERT INTO supplier_config (supplier_code, config_json, updated_at)
      VALUES (?, ?, ?)
      ON CONFLICT(supplier_code) DO UPDATE SET config_json=excluded.config_json, updated_at=excluded.updated_at
    `);
    let copied = 0;
    for (const c of COMPANIES) {
      const src = `${LEGACY}/${c}/suppliers/supplier_statements.db`;
      if (!existsSync(src)) continue;
      const srcDb = new Database(src, { readonly: true });
      try {
        const exists = srcDb.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='supplier_config'").get();
        if (!exists) continue;
        const rows = srcDb.prepare("SELECT * FROM supplier_config").all() as Array<Record<string, unknown>>;
        for (const r of rows) {
          const supplierCode = r.account_code as string | null;
          if (!supplierCode) continue;
          const configBlob = JSON.stringify({
            name: r.name ?? '',
            balance: r.balance ?? 0,
            payment_terms_days: r.payment_terms_days ?? 30,
            payment_method: r.payment_method ?? '',
            reconciliation_active: Boolean(r.reconciliation_active),
            auto_respond: Boolean(r.auto_respond),
            never_communicate: Boolean(r.never_communicate),
            statements_contact_position: r.statements_contact_position ?? '',
            last_synced: r.last_synced ?? null,
            last_statement_date: r.last_statement_date ?? null,
            source_company: c,
          });
          try {
            // upsert by supplier_code — but if already exists from a previous run (intsys),
            // skip rather than overwrite with cloudsis blob.
            const existing = dst.prepare("SELECT 1 FROM supplier_config WHERE supplier_code=?").get(supplierCode);
            if (existing) continue;
            insertStmt.run(supplierCode, configBlob, r.updated_at ?? new Date().toISOString());
            copied++;
          } catch (e) {
            console.warn('  warn:', supplierCode, (e as Error).message.slice(0, 80));
          }
        }
      } finally { srcDb.close(); }
    }
    console.log(`  copied ${copied} supplier_config rows (intsys first; cloudsis fills gaps)`);
  } finally { dst.close(); }
}

function main(): void {
  console.log('=== Settings + missing data migration ===\n');
  migrateBankReconcileFolders();
  console.log('');
  migrateSupplierAutomationSettings();
  console.log('');
  migrateSupplierApprovedEmails();
  console.log('');
  migrateSupplierConfig();
  console.log('');
  console.log('=== Done ===');
}

main();
