/**
 * Initial schema for the suppliers per-app database.
 *
 * Mirrors the Python SQLite tables in
 * data/{company}/suppliers/supplier_statements.db plus the extraction
 * cache from supplier_extraction_cache.db.
 *
 * The suppliers app is incomplete in Python. Schema is included
 * for the parts that ARE used; new features added during the TS
 * port will append migrations 002, 003, etc.
 */
import type { Knex } from 'knex';

export async function up(knex: Knex): Promise<void> {
  // Settings — one row per tenant
  await knex.schema.createTable('settings', (table) => {
    table.increments('id').primary();
    table.string('key', 64).notNullable().unique();
    table.text('value');
    table.timestamp('updated_at').defaultTo(knex.fn.now());
  });

  // Supplier statements — header row per imported statement
  await knex.schema.createTable('supplier_statements', (table) => {
    table.increments('id').primary();
    table.string('supplier_code', 32).notNullable();
    table.date('statement_date');
    table.decimal('opening_balance', 12, 2);
    table.decimal('closing_balance', 12, 2);
    table.string('source', 16); // 'email' | 'file'
    table.string('source_ref', 500);
    table.string('pdf_path', 1000);
    table.timestamp('imported_at').defaultTo(knex.fn.now());
    table.index('supplier_code');
    table.index('statement_date');
  });

  // Statement lines — one row per extracted invoice / credit note line
  await knex.schema.createTable('statement_lines', (table) => {
    table.increments('id').primary();
    table.integer('statement_id').notNullable();
    table.date('line_date');
    table.string('reference', 100);
    table.string('description', 500);
    table.decimal('amount', 12, 2);
    table.string('matched_opera_ref', 64);
    table.string('match_status', 16); // matched / unmatched / disputed
    table.foreign('statement_id').references('supplier_statements.id').onDelete('CASCADE');
    table.index('statement_id');
  });

  // Statement-only items (in supplier statement, not in Opera ptran)
  await knex.schema.createTable('statement_opera_only', (table) => {
    table.increments('id').primary();
    table.integer('statement_id').notNullable();
    table.string('reference', 100);
    table.decimal('amount', 12, 2);
    table.text('reason');
    table.foreign('statement_id').references('supplier_statements.id').onDelete('CASCADE');
  });

  // Processed emails (de-dupe — don't re-extract the same statement email)
  await knex.schema.createTable('processed_emails', (table) => {
    table.increments('id').primary();
    table.string('message_id', 200).notNullable().unique();
    table.string('supplier_code', 32);
    table.string('subject', 500);
    table.timestamp('processed_at').defaultTo(knex.fn.now());
  });

  // Per-supplier configuration
  await knex.schema.createTable('supplier_config', (table) => {
    table.increments('id').primary();
    table.string('supplier_code', 32).notNullable().unique();
    table.text('config_json'); // arbitrary per-supplier config
    table.timestamp('updated_at').defaultTo(knex.fn.now());
  });

  // Per-supplier automation rules
  await knex.schema.createTable('supplier_automation_config', (table) => {
    table.increments('id').primary();
    table.string('supplier_code', 32).notNullable().unique();
    table.boolean('auto_process').defaultTo(false);
    table.string('frequency', 16); // weekly / monthly
    table.text('matching_rules_json');
    table.timestamp('updated_at').defaultTo(knex.fn.now());
  });

  // Override flags — per-supplier per-statement disputes / accept
  await knex.schema.createTable('supplier_overrides', (table) => {
    table.increments('id').primary();
    table.integer('statement_id').notNullable();
    table.integer('line_id');
    table.string('override_type', 16); // accept / reject / dispute
    table.text('reason');
    table.timestamp('created_at').defaultTo(knex.fn.now());
  });

  // Extended supplier contacts (over and above pname)
  await knex.schema.createTable('supplier_contacts_ext', (table) => {
    table.increments('id').primary();
    table.string('supplier_code', 32).notNullable();
    table.string('contact_email', 200);
    table.string('contact_name', 200);
    table.string('contact_role', 100);
    table.timestamp('updated_at').defaultTo(knex.fn.now());
    table.index('supplier_code');
  });

  // Onboarding flow state
  await knex.schema.createTable('supplier_onboarding', (table) => {
    table.increments('id').primary();
    table.string('supplier_code', 32).notNullable().unique();
    table.string('stage', 32);
    table.text('notes');
    table.timestamp('updated_at').defaultTo(knex.fn.now());
  });

  // Approved sender emails — only accept statements from these
  await knex.schema.createTable('supplier_approved_emails', (table) => {
    table.increments('id').primary();
    table.string('supplier_code', 32).notNullable();
    table.string('email_address', 200).notNullable();
    table.timestamp('approved_at').defaultTo(knex.fn.now());
    table.index(['supplier_code', 'email_address']);
  });

  // Remittance log — audit trail of remittance emails sent
  await knex.schema.createTable('supplier_remittance_log', (table) => {
    table.increments('id').primary();
    table.string('supplier_code', 32).notNullable();
    table.string('to_address', 200);
    table.string('subject', 500);
    table.decimal('amount', 12, 2);
    table.timestamp('sent_at').defaultTo(knex.fn.now());
    table.index('supplier_code');
  });

  // Audit trail of supplier-master changes
  await knex.schema.createTable('supplier_change_audit', (table) => {
    table.increments('id').primary();
    table.string('supplier_code', 32).notNullable();
    table.string('changed_field', 64);
    table.text('old_value');
    table.text('new_value');
    table.string('changed_by', 64);
    table.timestamp('changed_at').defaultTo(knex.fn.now());
  });

  // Communications log
  await knex.schema.createTable('supplier_communications', (table) => {
    table.increments('id').primary();
    table.string('supplier_code', 32).notNullable();
    table.string('channel', 16); // email / phone / portal
    table.string('subject', 500);
    table.text('content');
    table.timestamp('sent_at').defaultTo(knex.fn.now());
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists('supplier_communications');
  await knex.schema.dropTableIfExists('supplier_change_audit');
  await knex.schema.dropTableIfExists('supplier_remittance_log');
  await knex.schema.dropTableIfExists('supplier_approved_emails');
  await knex.schema.dropTableIfExists('supplier_onboarding');
  await knex.schema.dropTableIfExists('supplier_contacts_ext');
  await knex.schema.dropTableIfExists('supplier_overrides');
  await knex.schema.dropTableIfExists('supplier_automation_config');
  await knex.schema.dropTableIfExists('supplier_config');
  await knex.schema.dropTableIfExists('processed_emails');
  await knex.schema.dropTableIfExists('statement_opera_only');
  await knex.schema.dropTableIfExists('statement_lines');
  await knex.schema.dropTableIfExists('supplier_statements');
  await knex.schema.dropTableIfExists('settings');
}
