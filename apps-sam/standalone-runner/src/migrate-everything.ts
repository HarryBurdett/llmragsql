/**
 * Comprehensive sweep — migrate every legacy table that hasn't been touched yet.
 *
 * Handles the schema mappings that the earlier `migrate-legacy-sqlite.ts` skipped:
 *   - bank-reconcile aliases / patterns / deferred (legacy → new schema)
 *   - gocardless partner_signups / subscription_documents / mandate_setup_requests
 *   - suppliers onboarding / statement_lines / statement_opera_only /
 *     processed_emails / supplier_automation_config
 *
 * All inserts are INSERT OR IGNORE / upsert — re-running is safe.
 */
import Database from 'better-sqlite3';
import { existsSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';

const LEGACY = '/Users/maccb/llmragsql/data';
const TARGET = join(homedir(), '.local', 'sam-test');
const COMPANIES = ['intsys', 'cloudsis'] as const;
type Company = (typeof COMPANIES)[number];
function companyCode(c: Company): string { return c === 'intsys' ? 'I' : 'C'; }

function withSrc<T>(path: string, fn: (db: Database.Database) => T): T | null {
  if (!existsSync(path)) return null;
  const db = new Database(path, { readonly: true });
  try { return fn(db); } finally { db.close(); }
}

function tableExists(db: Database.Database, name: string): boolean {
  return Boolean(db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(name));
}

interface CopyResult { copied: number; total: number; errors: number }

function copyMapped(
  targetPath: string,
  targetTable: string,
  perCompany: (company: Company, srcDb: Database.Database) => Array<Record<string, unknown>>,
  insertSql: string,
  insertArgsFromMapped: (row: Record<string, unknown>) => unknown[],
): CopyResult {
  const dst = new Database(targetPath);
  let copied = 0, total = 0, errors = 0;
  try {
    if (!tableExists(dst, targetTable)) {
      console.log('  [skip] target table missing: ' + targetTable);
      return { copied, total, errors };
    }
    const stmt = dst.prepare(insertSql);
    for (const c of COMPANIES) {
      const src = LEGACY + '/' + c + '/' + (
        targetPath.endsWith('bank-reconcile.db') ? 'bank_reconcile' :
        targetPath.endsWith('gocardless.db') ? 'gocardless' :
        targetPath.endsWith('suppliers.db') ? 'suppliers' : ''
      );
      // Try each *.db file in the legacy sub-folder
      const possibles = [
        src + '/bank_aliases.db',
        src + '/bank_patterns.db',
        src + '/deferred_transactions.db',
        src + '/gocardless_payments.db',
        src + '/supplier_statements.db',
      ];
      for (const p of possibles) {
        if (!existsSync(p)) continue;
        const rows = withSrc(p, (sdb) => {
          try { return perCompany(c, sdb); } catch { return []; }
        }) ?? [];
        for (const r of rows) {
          total++;
          try {
            const result = stmt.run(...insertArgsFromMapped(r));
            if (result.changes > 0) copied++;
          } catch (err) {
            errors++;
            if (errors < 3) console.warn('    insert err:', (err as Error).message.slice(0, 100));
          }
        }
      }
    }
  } finally { dst.close(); }
  return { copied, total, errors };
}

function migrateBankAliases(): CopyResult {
  return copyMapped(
    TARGET + '/bank-reconcile.db',
    'bank_import_aliases',
    (c, sdb) => {
      if (!tableExists(sdb, 'bank_import_aliases')) return [];
      const rows = sdb.prepare(`
        SELECT bank_name, ledger_type, account_code, match_score,
               created_date, last_used, use_count, bank_code
        FROM bank_import_aliases
      `).all() as Array<Record<string, unknown>>;
      return rows.map(r => ({ ...r, _company: c }));
    },
    `INSERT OR IGNORE INTO bank_import_aliases
       (bank_code, payee_pattern, match_type, opera_account, confidence,
        match_count, direction, created_at, updated_at)
       VALUES (?,?,?,?,?,?,?,?,?)`,
    (r) => [
      r.bank_code ?? null,
      r.bank_name ?? '',
      'fuzzy',
      r.account_code ?? '',
      r.match_score ?? 0.8,
      r.use_count ?? 0,
      r.ledger_type ?? null,
      r.created_date ?? new Date().toISOString(),
      r.last_used ?? new Date().toISOString(),
    ],
  );
}

function migrateBankPatterns(): CopyResult {
  return copyMapped(
    TARGET + '/bank-reconcile.db',
    'bank_import_patterns',
    (_c, sdb) => {
      if (!tableExists(sdb, 'bank_import_patterns')) return [];
      return sdb.prepare(`
        SELECT description_normalized, account_code, times_used, last_used
        FROM bank_import_patterns
      `).all() as Array<Record<string, unknown>>;
    },
    `INSERT OR IGNORE INTO bank_import_patterns
       (pattern, opera_account, confidence, match_count, updated_at)
       VALUES (?,?,?,?,?)`,
    (r) => [
      r.description_normalized ?? '',
      r.account_code ?? '',
      0.8,
      r.times_used ?? 0,
      r.last_used ?? new Date().toISOString(),
    ],
  );
}

function migrateDeferredTransactions(): CopyResult {
  return copyMapped(
    TARGET + '/bank-reconcile.db',
    'deferred_transactions',
    (_c, sdb) => {
      if (!tableExists(sdb, 'deferred_transactions')) return [];
      return sdb.prepare('SELECT * FROM deferred_transactions').all() as Array<Record<string, unknown>>;
    },
    `INSERT OR IGNORE INTO deferred_transactions
       (bank_code, statement_date, amount, description, deferred_by, deferred_at)
       VALUES (?,?,?,?,?,?)`,
    (r) => [
      r.bank_code ?? null,
      r.statement_date ?? r.tx_date ?? null,
      r.amount ?? 0,
      r.description ?? '',
      r.deferred_by ?? r.user_code ?? 'system',
      r.deferred_at ?? r.created_at ?? new Date().toISOString(),
    ],
  );
}

function migrateGocardlessPartnerSignups(): CopyResult {
  return copyMapped(
    TARGET + '/gocardless.db',
    'gocardless_partner_signups',
    (_c, sdb) => {
      if (!tableExists(sdb, 'gocardless_partner_signups')) return [];
      return sdb.prepare('SELECT * FROM gocardless_partner_signups').all() as Array<Record<string, unknown>>;
    },
    `INSERT OR IGNORE INTO gocardless_partner_signups
       (company_name, company_email, billing_request_id, billing_request_flow_id,
        authorisation_url, status, status_detail, access_token_obtained,
        merchant_access_token, merchant_organisation_id, merchant_creditor_name,
        merchant_app_url, partner_referral_id, created_at, completed_at, updated_at)
       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
    (r) => [
      r.company_name, r.company_email, r.billing_request_id, r.billing_request_flow_id,
      r.authorisation_url, r.status, r.status_detail, r.access_token_obtained,
      r.merchant_access_token, r.merchant_organisation_id, r.merchant_creditor_name,
      r.merchant_app_url, r.partner_referral_id, r.created_at, r.completed_at, r.updated_at,
    ],
  );
}

function migrateGocardlessSubscriptionDocuments(): CopyResult {
  return copyMapped(
    TARGET + '/gocardless.db',
    'gocardless_subscription_documents',
    (_c, sdb) => {
      if (!tableExists(sdb, 'gocardless_subscription_documents')) return [];
      return sdb.prepare('SELECT subscription_id, source_doc, added_at FROM gocardless_subscription_documents').all() as Array<Record<string, unknown>>;
    },
    `INSERT OR IGNORE INTO gocardless_subscription_documents
       (subscription_id, source_doc, added_at) VALUES (?,?,?)`,
    (r) => [r.subscription_id, r.source_doc, r.added_at],
  );
}

function migrateMandateSetupRequests(): CopyResult {
  return copyMapped(
    TARGET + '/gocardless.db',
    'mandate_setup_requests',
    (_c, sdb) => {
      if (!tableExists(sdb, 'mandate_setup_requests')) return [];
      return sdb.prepare('SELECT * FROM mandate_setup_requests').all() as Array<Record<string, unknown>>;
    },
    `INSERT OR IGNORE INTO mandate_setup_requests
       (opera_account, opera_name, customer_email, billing_request_id, billing_request_flow_id,
        authorisation_url, mandate_id, gocardless_customer_id, status, status_detail,
        email_sent_at, mandate_active_at, created_at, updated_at)
       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
    (r) => [
      r.opera_account, r.opera_name, r.customer_email, r.billing_request_id,
      r.billing_request_flow_id, r.authorisation_url, r.mandate_id, r.gocardless_customer_id,
      r.status, r.status_detail, r.email_sent_at, r.mandate_active_at, r.created_at, r.updated_at,
    ],
  );
}

function migrateSupplierOnboarding(): CopyResult {
  return copyMapped(
    TARGET + '/suppliers.db',
    'supplier_onboarding',
    (_c, sdb) => {
      if (!tableExists(sdb, 'supplier_onboarding')) return [];
      return sdb.prepare('SELECT supplier_code, category, priority, notes, completed_at, detected_at FROM supplier_onboarding').all() as Array<Record<string, unknown>>;
    },
    `INSERT OR IGNORE INTO supplier_onboarding (supplier_code, stage, notes, updated_at)
       VALUES (?,?,?,?)`,
    (r) => [
      r.supplier_code,
      r.completed_at ? 'completed' : (r.category as string ?? 'detected'),
      r.notes ?? null,
      r.completed_at ?? r.detected_at ?? new Date().toISOString(),
    ],
  );
}

function migrateProcessedEmails(): CopyResult {
  return copyMapped(
    TARGET + '/suppliers.db',
    'processed_emails',
    (_c, sdb) => {
      if (!tableExists(sdb, 'processed_emails')) return [];
      return sdb.prepare('SELECT email_id AS message_id, processed_at FROM processed_emails').all() as Array<Record<string, unknown>>;
    },
    `INSERT OR IGNORE INTO processed_emails (message_id, supplier_code, subject, processed_at)
       VALUES (?,?,?,?)`,
    (r) => [
      r.message_id ?? '',
      null, // supplier_code unknown from legacy
      '', // subject unknown
      r.processed_at ?? new Date().toISOString(),
    ],
  );
}

function migrateStatementLines(): CopyResult {
  return copyMapped(
    TARGET + '/suppliers.db',
    'statement_lines',
    (_c, sdb) => {
      if (!tableExists(sdb, 'statement_lines')) return [];
      return sdb.prepare(`
        SELECT statement_id, line_date, reference, description,
               debit, credit, match_status
        FROM statement_lines
      `).all() as Array<Record<string, unknown>>;
    },
    `INSERT OR IGNORE INTO statement_lines
       (statement_id, line_date, reference, description, amount, match_status, status)
       VALUES (?,?,?,?,?,?,?)`,
    (r) => {
      // Legacy has debit/credit; new has single signed amount
      const debit = Number(r.debit ?? 0);
      const credit = Number(r.credit ?? 0);
      const amount = debit - credit; // debit positive in payable context
      return [
        r.statement_id ?? null,
        r.line_date ?? null,
        r.reference ?? '',
        r.description ?? '',
        amount,
        r.match_status ?? 'unmatched',
        r.match_status ?? 'open',
      ];
    },
  );
}

function migrateSupplierAutomationConfigPerSupplier(): CopyResult {
  // Legacy is GLOBAL key/value (already handled by previous migrate-settings.ts run
  // copying to supplier_automation_settings). The per-supplier table in new
  // is a different concept (per-supplier auto-process toggles, frequencies, etc.)
  // which the legacy doesn't have one-to-one. We skip this for now — supplier_config
  // already captures the per-supplier reconciliation_active / auto_respond fields.
  console.log('  [skip] supplier_automation_config — different concept; per-supplier toggles');
  console.log('         live in supplier_config.config_json instead (already migrated)');
  return { copied: 0, total: 0, errors: 0 };
}

function main(): void {
  console.log('=== Comprehensive sweep — fill remaining tables ===\n');

  const sections: Array<[string, () => CopyResult]> = [
    ['bank-reconcile: bank_import_aliases', migrateBankAliases],
    ['bank-reconcile: bank_import_patterns', migrateBankPatterns],
    ['bank-reconcile: deferred_transactions', migrateDeferredTransactions],
    ['gocardless: partner_signups', migrateGocardlessPartnerSignups],
    ['gocardless: subscription_documents', migrateGocardlessSubscriptionDocuments],
    ['gocardless: mandate_setup_requests', migrateMandateSetupRequests],
    ['suppliers: supplier_onboarding', migrateSupplierOnboarding],
    ['suppliers: processed_emails', migrateProcessedEmails],
    ['suppliers: statement_lines', migrateStatementLines],
    ['suppliers: supplier_automation_config (per-supplier)', migrateSupplierAutomationConfigPerSupplier],
  ];

  let grand = 0, grandErrors = 0;
  for (const [label, fn] of sections) {
    console.log('--- ' + label + ' ---');
    const r = fn();
    if (r.total > 0) {
      console.log('  copied ' + r.copied + '/' + r.total + (r.errors > 0 ? ', errors=' + r.errors : ''));
    }
    grand += r.copied;
    grandErrors += r.errors;
    console.log('');
  }
  console.log('=== Total: ' + grand + ' rows copied' + (grandErrors > 0 ? ', ' + grandErrors + ' errors' : '') + ' ===');
}

main();
