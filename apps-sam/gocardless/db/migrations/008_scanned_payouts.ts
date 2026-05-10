/**
 * scanned_payouts — capture-side store for the SAM email-ingest
 * handler that watches inbox for GoCardless payout notifications.
 *
 * The handler inserts a row here for every detected payout-style
 * email. The /api/gocardless/scan-emails endpoint reads from this
 * table.
 */
import type { Knex } from 'knex';

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable('scanned_payouts', (table) => {
    table.increments('id').primary();
    table.string('email_id', 256).notNullable();
    table.string('attachment_id', 256);
    table.string('filename', 500);
    table.integer('size_bytes').defaultTo(0);
    table.string('content_type', 128);
    table.timestamp('received_at').notNullable();
    table.string('from_address', 256);
    table.string('subject', 500);
    table.string('detected_payout_id', 64);
    table.decimal('detected_amount', 18, 2);
    table.string('detected_currency', 8);
    table.timestamp('captured_at').defaultTo(knex.fn.now());
    table.unique(['email_id']);
    table.index(['received_at']);
    table.index(['detected_payout_id']);
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists('scanned_payouts');
}
