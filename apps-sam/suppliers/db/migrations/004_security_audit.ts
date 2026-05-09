/**
 * Migration 004 — verified columns on supplier_change_audit so the
 * security alert workflow can mark reviewed entries as cleared.
 *
 * Mirrors the runtime ALTER TABLE in Python's `_run_migrations`:
 *   - verified           (boolean, default false)
 *   - verified_by        (string)
 *   - verified_at        (timestamp)
 *
 * The Python code calls the field `field_name` while the existing TS
 * uses `changed_field` — the route layer maps between them, so no
 * column rename here.
 */
import type { Knex } from 'knex';

export async function up(knex: Knex): Promise<void> {
  if (!(await knex.schema.hasColumn('supplier_change_audit', 'verified'))) {
    await knex.schema.alterTable('supplier_change_audit', (table) => {
      table.boolean('verified').defaultTo(false).index();
      table.string('verified_by', 64);
      table.timestamp('verified_at');
    });
  }
}

export async function down(knex: Knex): Promise<void> {
  if (await knex.schema.hasColumn('supplier_change_audit', 'verified')) {
    await knex.schema.alterTable('supplier_change_audit', (table) => {
      table.dropColumn('verified_at');
      table.dropColumn('verified_by');
      table.dropColumn('verified');
    });
  }
}
