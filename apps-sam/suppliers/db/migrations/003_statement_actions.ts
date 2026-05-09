/**
 * Migration 003 — fields needed for the statement-action endpoints
 * (acknowledge / approve / process / edit-response / bulk-approve).
 *
 * Mirrors columns the Python supplier_statements.db gains over time
 * via runtime ALTER TABLE in `_run_migrations`:
 *   - supplier_statements.response_text   (operator-edited body)
 *   - supplier_statements.response_subject (operator-edited subject)
 *   - supplier_statements.email_pdf_path   (PDF attached on approve)
 *   - supplier_contacts_ext.is_statement_contact
 *   - supplier_contacts_ext.never_communicate (per-contact opt-out)
 *
 * Adds a key/value `supplier_automation_config` shape that the
 * acknowledge endpoint relies on (acknowledgment_template,
 * acknowledgment_delay_minutes, response_subject_template). The
 * existing per-supplier table (migration 001) is for per-supplier
 * rules — this is for tenant-wide config keys.
 */
import type { Knex } from 'knex';

export async function up(knex: Knex): Promise<void> {
  if (!(await knex.schema.hasColumn('supplier_statements', 'response_text'))) {
    await knex.schema.alterTable('supplier_statements', (table) => {
      table.text('response_text');
      table.string('response_subject', 500);
      table.string('email_pdf_path', 1000);
    });
  }

  if (
    !(await knex.schema.hasColumn(
      'supplier_contacts_ext',
      'is_statement_contact',
    ))
  ) {
    await knex.schema.alterTable('supplier_contacts_ext', (table) => {
      table.boolean('is_statement_contact').defaultTo(false);
      table.boolean('never_communicate').defaultTo(false);
    });
  }

  await knex.schema.createTable('supplier_automation_settings', (table) => {
    table.increments('id').primary();
    table.string('key', 64).notNullable().unique();
    table.text('value');
    table.timestamp('updated_at').defaultTo(knex.fn.now());
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists('supplier_automation_settings');

  if (
    await knex.schema.hasColumn(
      'supplier_contacts_ext',
      'is_statement_contact',
    )
  ) {
    await knex.schema.alterTable('supplier_contacts_ext', (table) => {
      table.dropColumn('never_communicate');
      table.dropColumn('is_statement_contact');
    });
  }

  if (await knex.schema.hasColumn('supplier_statements', 'response_text')) {
    await knex.schema.alterTable('supplier_statements', (table) => {
      table.dropColumn('email_pdf_path');
      table.dropColumn('response_subject');
      table.dropColumn('response_text');
    });
  }
}
