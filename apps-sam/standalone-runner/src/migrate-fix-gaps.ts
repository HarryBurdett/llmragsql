/**
 * Fix the gaps identified by the audit:
 *   - bank-reconcile.extraction_cache (missing)
 *   - suppliers.supplier_automation_settings (rows went to wrong table)
 *   - suppliers.statement_lines (75 failed FK — need to preserve statement IDs)
 *   - suppliers.statement_opera_only (missing)
 *
 * Approach for statement_lines FK problem:
 *   The legacy supplier_statements.id values were re-numbered by SQLite's
 *   autoincrement when we INSERT'd them. So legacy statement_lines.statement_id
 *   no longer points anywhere. Fix: delete the migrated supplier_statements +
 *   statement_lines rows, re-migrate supplier_statements with explicit IDs
 *   (intsys keeps legacy id; cloudsis offset by 10000 to avoid collisions),
 *   then statement_lines uses the same legacy id (offset for cloudsis).
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

// ============================================================
// 1. bank-reconcile.extraction_cache
// ============================================================
function migrateBankReconcileExtractionCache(): void {
  console.log('--- bank-reconcile: extraction_cache ---');
  const dst = new Database(`${TARGET}/bank-reconcile.db`);
  try {
    let total = 0;
    for (const c of COMPANIES) {
      const src = `${LEGACY}/${c}/bank_reconcile/bank_aliases.db`;
      if (!existsSync(src)) continue;
      // extraction_cache lives in a separate file in legacy
      const cachePath = `${LEGACY}/${c}/bank_reconcile/pdf_extraction_cache.db`;
      if (!existsSync(cachePath)) {
        console.log(`  (no pdf_extraction_cache.db for ${c})`);
        continue;
      }
      const sdb = new Database(cachePath, { readonly: true });
      try {
        const tables = sdb.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").all() as Array<{name: string}>;
        if (tables.length === 0) continue;
        // Try common table names: extraction_cache, pdf_cache, cache
        const tableName = tables[0].name;
        const cols = sdb.prepare(`PRAGMA table_info(${tableName})`).all() as Array<{name: string}>;
        const colNames = cols.map(c => c.name);
        const rows = sdb.prepare(`SELECT * FROM ${tableName}`).all() as Array<Record<string, unknown>>;
        // Target schema: id, content_hash, extraction_json, created_at
        const stmt = dst.prepare(`
          INSERT OR IGNORE INTO extraction_cache (content_hash, extraction_json, created_at)
          VALUES (?,?,?)
        `);
        for (const r of rows) {
          // Find hash-like and json-like columns
          const hash = r.content_hash ?? r.pdf_hash ?? r.hash ?? null;
          const json = r.extraction_json ?? r.extraction ?? r.data ?? r.result ?? null;
          const ts = r.created_at ?? r.cached_at ?? new Date().toISOString();
          if (!hash) continue;
          const result = stmt.run(String(hash), typeof json === 'string' ? json : JSON.stringify(json), String(ts));
          if (result.changes > 0) total++;
        }
      } finally { sdb.close(); }
    }
    console.log(`  copied ${total} extraction_cache rows`);
  } finally { dst.close(); }
}

// ============================================================
// 2. suppliers.supplier_automation_settings — move rows from settings
// ============================================================
function fixSupplierAutomationSettings(): void {
  console.log('--- suppliers: move automation rows from settings → supplier_automation_settings ---');
  const dst = new Database(`${TARGET}/suppliers.db`);
  try {
    // We previously put 'company/key' rows into the `settings` table.
    // They actually belong in `supplier_automation_settings`. Move them.
    const rows = dst.prepare("SELECT key, value, updated_at FROM settings WHERE key LIKE '%/%'").all() as Array<{key: string; value: string; updated_at: string}>;
    if (rows.length === 0) {
      console.log('  (no namespaced settings rows to move)');
      return;
    }
    const stmt = dst.prepare(`
      INSERT OR IGNORE INTO supplier_automation_settings (key, value, updated_at)
      VALUES (?,?,?)
    `);
    let moved = 0;
    for (const r of rows) {
      const result = stmt.run(r.key, r.value, r.updated_at);
      if (result.changes > 0) moved++;
    }
    // Now delete from the wrong table
    const deleted = dst.prepare("DELETE FROM settings WHERE key LIKE '%/%'").run();
    console.log(`  moved ${moved} rows; deleted ${deleted.changes} from settings table`);
  } finally { dst.close(); }
}

// ============================================================
// 3. supplier_statements + statement_lines + statement_opera_only — fix IDs
// ============================================================
function migrateSupplierStatementsWithIds(): void {
  console.log('--- suppliers: re-migrate supplier_statements + statement_lines (preserve IDs) ---');
  const dst = new Database(`${TARGET}/suppliers.db`);
  try {
    // Wipe existing rows so we can re-insert with explicit IDs
    dst.prepare('DELETE FROM statement_lines').run();
    dst.prepare('DELETE FROM statement_opera_only').run();
    dst.prepare('DELETE FROM supplier_statements').run();
    console.log('  cleared statement_lines, statement_opera_only, supplier_statements');

    let stmtCopied = 0, linesCopied = 0, opOnlyCopied = 0, errors = 0;

    for (const c of COMPANIES) {
      const offset = offsetForCompany(c);
      const src = `${LEGACY}/${c}/suppliers/supplier_statements.db`;
      if (!existsSync(src)) continue;
      const sdb = new Database(src, { readonly: true });
      try {
        // supplier_statements — query only columns that exist in this legacy DB
        const cols = sdb.prepare('PRAGMA table_info(supplier_statements)').all() as Array<{name: string}>;
        const colNames = new Set(cols.map(c => c.name));
        const wantedCols = [
          'id', 'supplier_code', 'statement_date', 'received_date', 'sender_email',
          'pdf_path', 'status', 'opening_balance', 'closing_balance', 'currency',
          'acknowledged_at', 'processed_at', 'approved_by', 'approved_at', 'sent_at',
          'error_message', 'response_text',
        ];
        const useCols = wantedCols.filter(c => colNames.has(c));
        const stmts = sdb.prepare(`SELECT ${useCols.join(',')} FROM supplier_statements`).all() as Array<Record<string, unknown>>;
        const stmtInsert = dst.prepare(`
          INSERT OR IGNORE INTO supplier_statements
            (id, supplier_code, statement_date, received_date, sender_email,
             pdf_path, status, opening_balance, closing_balance, currency,
             acknowledged_at, processed_at, approved_by, approved_at, sent_at,
             error_message, response_text)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        `);
        for (const s of stmts) {
          const newId = (s.id as number) + offset;
          try {
            const r = stmtInsert.run(
              newId,
              s.supplier_code ?? null,
              s.statement_date ?? null,
              s.received_date ?? null,
              s.sender_email ?? null,
              s.pdf_path ?? null,
              s.status ?? null,
              s.opening_balance ?? null,
              s.closing_balance ?? null,
              s.currency ?? null,
              s.acknowledged_at ?? null,
              s.processed_at ?? null,
              s.approved_by ?? null,
              s.approved_at ?? null,
              s.sent_at ?? null,
              s.error_message ?? null,
              s.response_text ?? null,
            );
            if (r.changes > 0) stmtCopied++;
          } catch (e) { errors++; if (errors < 3) console.warn('    stmt err:', (e as Error).message); }
        }

        // statement_lines using offset statement_id
        const lines = sdb.prepare(`
          SELECT statement_id, line_date, reference, description, debit, credit, match_status
          FROM statement_lines
        `).all() as Array<Record<string, unknown>>;
        const linesInsert = dst.prepare(`
          INSERT OR IGNORE INTO statement_lines
            (statement_id, line_date, reference, description, amount, match_status, status)
          VALUES (?,?,?,?,?,?,?)
        `);
        for (const ln of lines) {
          const debit = Number(ln.debit ?? 0);
          const credit = Number(ln.credit ?? 0);
          const amount = debit - credit;
          try {
            const r = linesInsert.run(
              (ln.statement_id as number) + offset,
              ln.line_date, ln.reference, ln.description, amount,
              ln.match_status ?? 'unmatched', ln.match_status ?? 'open',
            );
            if (r.changes > 0) linesCopied++;
          } catch (e) { errors++; if (errors < 3) console.warn('    line err:', (e as Error).message); }
        }

        // statement_opera_only (column mapping)
        if (sdb.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='statement_opera_only'").get()) {
          const ops = sdb.prepare(`
            SELECT statement_id, reference, amount
            FROM statement_opera_only
          `).all() as Array<Record<string, unknown>>;
          const opInsert = dst.prepare(`
            INSERT OR IGNORE INTO statement_opera_only (statement_id, reference, amount, reason)
            VALUES (?,?,?,?)
          `);
          for (const o of ops) {
            try {
              const r = opInsert.run(
                (o.statement_id as number) + offset,
                o.reference, o.amount, 'in_opera_not_on_statement',
              );
              if (r.changes > 0) opOnlyCopied++;
            } catch (e) { errors++; }
          }
        }
      } finally { sdb.close(); }
    }

    console.log(`  supplier_statements: copied ${stmtCopied}`);
    console.log(`  statement_lines: copied ${linesCopied}`);
    console.log(`  statement_opera_only: copied ${opOnlyCopied}`);
    if (errors > 0) console.log(`  errors: ${errors}`);
  } finally { dst.close(); }
}

function main(): void {
  console.log('=== Fix audit gaps ===\n');
  migrateBankReconcileExtractionCache();
  console.log('');
  fixSupplierAutomationSettings();
  console.log('');
  migrateSupplierStatementsWithIds();
  console.log('\n=== Done ===');
}

main();
