/**
 * Runner-only migration — bring legacy `bank_statement_imports` +
 * `bank_statement_transactions` rows into SAM's bank-reconcile.db so
 * the user's existing test scenario (statement 72 with 18 orphaned
 * `posted_entry_number` references after the Opera restore) can
 * exercise the SAM-side per-line orphan detection.
 *
 * Production note: this migration is NOT needed in real SAM
 * deployments. There every bank statement is imported through the
 * SAM plugin which writes per-line tracking to SAM's bank-reconcile
 * DB natively. This script only bridges the test environment where
 * earlier flows ran through the legacy Python backend (port 8000)
 * and wrote to email_data.db.
 *
 * Idempotent — uses (bank_code, filename, statement_date) as a
 * dedupe key on the SAM side so re-running doesn't duplicate.
 */
import Database from 'better-sqlite3';
import { existsSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';

const LEGACY = '/Users/maccb/llmragsql/data';
const TARGET = join(homedir(), '.local', 'sam-test');
const COMPANIES = ['intsys', 'cloudsis'] as const;
type Company = (typeof COMPANIES)[number];

function offsetForCompany(c: Company): number {
  return c === 'intsys' ? 0 : 50000;
}

interface LegacyImport {
  id: number;
  bank_code: string;
  filename: string | null;
  opening_balance: number | null;
  closing_balance: number | null;
  is_reconciled: number;
  reconciled_date: string | null;
  reconciled_count: number | null;
  statement_date: string | null;
  account_number: string | null;
  sort_code: string | null;
  period_start: string | null;
  period_end: string | null;
  import_date: string;
  imported_by: string | null;
  target_system: string;
  total_receipts: number | null;
  total_payments: number | null;
  transactions_imported: number | null;
}

interface LegacyTransaction {
  id: number;
  import_id: number;
  line_number: number;
  date: string;
  description: string | null;
  amount: number;
  balance: number | null;
  transaction_type: string | null;
  reference: string | null;
  matched_entry: string | null;
  match_confidence: number | null;
  match_type: string | null;
  is_reconciled: number;
  posted_entry_number: string | null;
  posted_at: string | null;
}

function migrate(): void {
  const dst = new Database(`${TARGET}/bank-reconcile.db`);
  try {
    let imports_copied = 0;
    let txns_copied = 0;
    for (const c of COMPANIES) {
      const src = `${LEGACY}/${c}/core/email_data.db`;
      if (!existsSync(src)) {
        console.log(`  (no email_data.db for ${c})`);
        continue;
      }
      const sdb = new Database(src, { readonly: true });
      try {
        const legacyImports = sdb
          .prepare(
            `SELECT id, bank_code, filename, opening_balance, closing_balance,
                    is_reconciled, reconciled_date, reconciled_count,
                    statement_date, account_number, sort_code, period_start,
                    period_end, import_date, imported_by, target_system,
                    total_receipts, total_payments, transactions_imported
             FROM bank_statement_imports
             WHERE bank_code IS NOT NULL`,
          )
          .all() as LegacyImport[];

        const offset = offsetForCompany(c);
        const insertImport = dst.prepare(`
          INSERT OR IGNORE INTO bank_statement_imports
            (id, bank_code, statement_date, opening_balance, closing_balance,
             source, source_ref, imported_by, imported_at,
             is_reconciled, reconciled_count, target_system, reconciled_at,
             filename, transactions_imported, total_receipts, total_payments,
             account_number, sort_code, period_start, period_end)
          VALUES (?,?,?,?,?, ?,?,?,?, ?,?,?,?, ?,?,?,?, ?,?,?,?)
        `);

        const idMap = new Map<number, number>();
        for (const r of legacyImports) {
          const newId = r.id + offset;
          insertImport.run(
            newId,
            r.bank_code,
            r.statement_date,
            r.opening_balance,
            r.closing_balance,
            'email', // assume legacy ones were email-sourced
            r.filename ?? null,
            r.imported_by ?? null,
            r.import_date,
            r.is_reconciled ?? 0,
            r.reconciled_count ?? 0,
            r.target_system ?? 'opera_se',
            r.reconciled_date,
            r.filename,
            r.transactions_imported ?? 0,
            r.total_receipts ?? 0,
            r.total_payments ?? 0,
            r.account_number,
            r.sort_code,
            r.period_start,
            r.period_end,
          );
          idMap.set(r.id, newId);
          imports_copied++;
        }

        const legacyTxns = sdb
          .prepare(
            `SELECT id, import_id, line_number, date, description, amount,
                    balance, transaction_type, reference, matched_entry,
                    match_confidence, match_type, is_reconciled,
                    posted_entry_number, posted_at
             FROM bank_statement_transactions`,
          )
          .all() as LegacyTransaction[];
        const insertTxn = dst.prepare(`
          INSERT OR IGNORE INTO bank_statement_transactions
            (id, import_id, line_number, post_date, description, amount,
             balance, transaction_type, reference, matched_entry,
             match_confidence, match_type, is_reconciled,
             posted_entry_number, posted_at)
          VALUES (?,?,?,?,?,?, ?,?,?,?, ?,?,?, ?,?)
        `);

        for (const t of legacyTxns) {
          const newImportId = idMap.get(t.import_id);
          if (!newImportId) continue; // orphan transaction — skip
          const newId = t.id + offset;
          insertTxn.run(
            newId,
            newImportId,
            t.line_number,
            t.date,
            t.description,
            t.amount,
            t.balance,
            t.transaction_type,
            t.reference,
            t.matched_entry,
            t.match_confidence,
            t.match_type,
            t.is_reconciled,
            t.posted_entry_number,
            t.posted_at,
          );
          txns_copied++;
        }
        console.log(`  ${c}: ${legacyImports.length} imports, ${legacyTxns.length} transactions read; running total ${imports_copied}/${txns_copied}`);
      } finally {
        sdb.close();
      }
    }
    console.log(`Done — ${imports_copied} bank_statement_imports + ${txns_copied} bank_statement_transactions copied.`);
  } finally {
    dst.close();
  }
}

migrate();
