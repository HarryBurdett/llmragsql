/**
 * Bank-level import lock — prevents concurrent GoCardless imports
 * targeting the same Opera bank account.
 *
 * Faithful port of `sql_rag/import_lock.py` (the GC use). Stores
 * locks in the per-app SQLite/MSSQL DB's `import_locks` table
 * (provisioned by migration 007_import_locks.ts).
 *
 * Cross-app coordination caveat: the bank-reconcile app has its own
 * `import_locks` table in its per-app DB. The two are NOT linked, so
 * running a GC import and a bank-reconcile import against the same
 * Opera bank simultaneously is technically possible. That same gap
 * existed in the Python source (separate per-folder SQLite files).
 */
import type { Knex } from 'knex';

export const LOCK_EXPIRY_SECONDS = 300; // 5 minutes — same as Python

export interface ImportLockOptions {
  locked_by?: string;
  endpoint?: string;
  description?: string;
}

async function cleanupStaleLocks(appDb: Knex): Promise<number> {
  const cutoff = new Date(Date.now() - LOCK_EXPIRY_SECONDS * 1000);
  return Number(
    await appDb('import_locks').where('locked_at', '<', cutoff).delete(),
  );
}

export async function acquireImportLock(
  appDb: Knex,
  bankCode: string,
  opts: ImportLockOptions = {},
): Promise<boolean> {
  const code = (bankCode ?? '').trim();
  if (!code) return false;

  await cleanupStaleLocks(appDb);

  const existing = await appDb('import_locks')
    .where({ bank_code: code })
    .first();
  if (existing) return false;

  try {
    await appDb('import_locks').insert({
      bank_code: code,
      locked_at: appDb.fn.now(),
      locked_by: opts.locked_by ?? 'unknown',
      endpoint: opts.endpoint ?? 'unknown',
      description: opts.description ?? '',
    });
    return true;
  } catch {
    return false;
  }
}

export async function releaseImportLock(
  appDb: Knex,
  bankCode: string,
): Promise<void> {
  const code = (bankCode ?? '').trim();
  if (!code) return;
  await appDb('import_locks').where({ bank_code: code }).delete();
}

export class ImportLockError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ImportLockError';
  }
}

export async function withImportLock<T>(
  appDb: Knex,
  bankCode: string,
  opts: ImportLockOptions,
  fn: () => Promise<T>,
): Promise<T> {
  const acquired = await acquireImportLock(appDb, bankCode, opts);
  if (!acquired) {
    throw new ImportLockError(
      `Bank account ${bankCode} is currently being imported by another user. ` +
        'Please wait for the current import to complete before starting another.',
    );
  }
  try {
    return await fn();
  } finally {
    await releaseImportLock(appDb, bankCode);
  }
}
