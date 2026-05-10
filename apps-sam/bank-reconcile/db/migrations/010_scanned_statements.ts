/**
 * scanned_statements — capture-side store for the SAM email-ingest
 * handler that watches inbox for bank-statement attachments.
 *
 * The handler (registered in plugin index when `manifest.consumes['email-ingest'] = true`)
 * inserts a row here for every detected statement attachment. The
 * `/api/bank-import/scan-emails` endpoint reads from this table.
 *
 * Faithful equivalent of the per-company SQLite cache used by Python's
 * `email_storage.get_emails(...)` filtered through
 * `is_bank_statement_attachment`. Decoupling capture from query keeps
 * the endpoint fast (no inbox round-trip per scan).
 */
import type { Knex } from 'knex';

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable('scanned_statements', (table) => {
    table.increments('id').primary();
    table.string('email_id', 256).notNullable();
    table.string('attachment_id', 256).notNullable();
    table.string('filename', 500).notNullable();
    table.integer('size_bytes').defaultTo(0);
    table.string('content_type', 128);
    table.timestamp('received_at').notNullable();
    table.string('from_address', 256);
    table.string('subject', 500);
    /** Resolved bank_code (nbank.nk_acnt) once detected; nullable */
    table.string('detected_bank', 32);
    /** Statement date parsed from filename/subject when detectable */
    table.string('statement_date', 16);
    /** Numeric sort key derived from filename/subject */
    table.integer('sort_key');
    table.timestamp('captured_at').defaultTo(knex.fn.now());
    table.unique(['email_id', 'attachment_id']);
    table.index(['received_at']);
    table.index(['detected_bank']);
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists('scanned_statements');
}
