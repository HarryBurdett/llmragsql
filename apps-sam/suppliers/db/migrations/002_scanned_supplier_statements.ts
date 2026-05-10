/**
 * scanned_supplier_statements — capture-side store for the SAM
 * email-ingest handler that watches inbox for supplier statement
 * attachments. Pattern matches bank-reconcile and gocardless.
 */
import type { Knex } from 'knex';

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable('scanned_supplier_statements', (table) => {
    table.increments('id').primary();
    table.string('email_id', 256).notNullable();
    table.string('attachment_id', 256).notNullable();
    table.string('filename', 500).notNullable();
    table.integer('size_bytes').defaultTo(0);
    table.string('content_type', 128);
    table.timestamp('received_at').notNullable();
    table.string('from_address', 256);
    table.string('subject', 500);
    table.string('detected_supplier_code', 32);
    table.string('statement_date', 16);
    table.timestamp('captured_at').defaultTo(knex.fn.now());
    table.unique(['email_id', 'attachment_id']);
    table.index(['received_at']);
    table.index(['detected_supplier_code']);
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists('scanned_supplier_statements');
}
