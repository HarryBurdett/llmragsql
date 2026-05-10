/**
 * Import-locks table for GoCardless.
 *
 * Faithful port of the per-company `import_locks.db` SQLite store used
 * by `sql_rag/import_lock.py`. Bank-level mutex prevents two GoCardless
 * imports from running against the same Opera bank account
 * simultaneously (which would corrupt nbank.nk_recbal).
 *
 * Note: this is per-app (gocardless) and DOES NOT coordinate with the
 * bank-reconcile app's import_locks. The Python implementation also
 * had this gap (separate SQLite files in separate folders) and relied
 * on operators not running both flows at once. A cross-app coordinator
 * is on the SAM roadmap; for now the per-app lock catches the most
 * common case (double-clicking the GC import button).
 */
import type { Knex } from 'knex';

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable('import_locks', (table) => {
    table.increments('id').primary();
    table.string('bank_code', 16).notNullable().unique();
    table.string('locked_by', 64).defaultTo('unknown');
    table.string('endpoint', 64).defaultTo('unknown');
    table.text('description').defaultTo('');
    table.timestamp('locked_at').defaultTo(knex.fn.now());
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists('import_locks');
}
