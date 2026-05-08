/**
 * Repeat-entry maintenance for the bank-import flow.
 *
 * Faithful port of:
 *   - update_repeat_entry_date (apps/bank_reconcile/api/routes.py:5320-5419)
 *
 * Updates ae_nxtpost on arhead so the operator can sync a repeat
 * entry's next posting date with the actual bank transaction date,
 * then run Opera's "Repeat Entries" routine to post.
 *
 * Optional alias-save: when statement_name is supplied, save a
 * repeat-entry alias in `repeat_entry_aliases` (per-app DB) so future
 * imports auto-match this bank statement description to this repeat
 * entry. Best-effort — alias-save failure doesn't fail the whole
 * operation.
 *
 * SQL injection guard: bank_code + entry_ref validated at the route
 * boundary via the shared validators.
 */
import type { Knex } from 'knex';
import {
  validateBankCode,
  validateEntryNumber,
  SqlInputValidationError,
} from '@sqlrag/sam-shared';
import { withImportLock, ImportLockError } from './import-lock.js';

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

export interface UpdateRepeatEntryDateInput {
  bankCode: string;
  entryRef: string;
  newDate: string;
  /** Optional bank-statement description to record as alias. */
  statementName?: string | null;
}

export interface UpdateRepeatEntryDateResponse {
  success: boolean;
  message?: string;
  entry_ref?: string;
  old_date?: string | null;
  new_date?: string;
  alias_saved?: boolean;
  error?: string;
}

export async function updateRepeatEntryDate(
  appDb: Knex,
  operaDb: Knex,
  input: UpdateRepeatEntryDateInput,
): Promise<UpdateRepeatEntryDateResponse> {
  let bankCode: string;
  let entryRef: string;
  try {
    bankCode = validateBankCode(input.bankCode);
    entryRef = validateEntryNumber(input.entryRef);
  } catch (e) {
    if (e instanceof SqlInputValidationError) {
      return { success: false, error: e.message };
    }
    throw e;
  }

  const newDate = (input.newDate ?? '').trim();
  if (!DATE_RE.test(newDate)) {
    return {
      success: false,
      error: `Invalid date format: ${newDate}. Expected YYYY-MM-DD`,
    };
  }

  try {
    return await withImportLock(
      appDb,
      bankCode,
      { locked_by: 'api', endpoint: 'update-repeat-entry-date' },
      async () => {
        // Verify the entry exists
        const verifyRows = (await operaDb.raw(
          `SELECT ae_entry, ae_desc, ae_nxtpost
           FROM arhead WITH (NOLOCK)
           WHERE RTRIM(ae_entry) = ?
             AND RTRIM(ae_acnt) = ?`,
          [entryRef, bankCode],
        )) as unknown as Array<{
          ae_entry: string;
          ae_desc: string | null;
          ae_nxtpost: Date | string | null;
        }>;

        const existing = Array.isArray(verifyRows) ? verifyRows[0] : undefined;
        if (!existing) {
          return {
            success: false,
            error: `Repeat entry '${entryRef}' not found for bank '${bankCode}'`,
          };
        }

        const oldDate =
          existing.ae_nxtpost instanceof Date
            ? existing.ae_nxtpost.toISOString().slice(0, 10)
            : existing.ae_nxtpost
              ? String(existing.ae_nxtpost).slice(0, 10)
              : null;
        const description = (existing.ae_desc ?? '').toString().trim();

        // UPDATE arhead with audit fields
        const result = (await operaDb.raw(
          `UPDATE arhead WITH (ROWLOCK)
           SET ae_nxtpost = ?,
               sq_amdate = CONVERT(varchar(10), GETDATE(), 23),
               sq_amtime = CONVERT(varchar(8), GETDATE(), 108),
               sq_amuser = 'BANKIMP'
           WHERE RTRIM(ae_entry) = ?
             AND RTRIM(ae_acnt) = ?`,
          [newDate, entryRef, bankCode],
        )) as unknown as { rowCount?: number } | Array<{ rowCount?: number }>;
        const rowsAffected =
          typeof result === 'object' && result !== null
            ? Array.isArray(result)
              ? Number(result[0]?.rowCount ?? 0)
              : Number(result.rowCount ?? 0)
            : 0;
        if (rowsAffected === 0) {
          return {
            success: false,
            error: 'No rows updated - entry may have been modified',
          };
        }

        // Best-effort alias save
        let aliasSaved = false;
        const statementName = (input.statementName ?? '').trim();
        if (statementName) {
          try {
            const existingAlias = (await appDb('repeat_entry_aliases')
              .where({ bank_code: bankCode, memo_pattern: statementName })
              .first()) as { id: number } | undefined;
            if (existingAlias) {
              await appDb('repeat_entry_aliases')
                .where({ id: existingAlias.id })
                .update({
                  opera_repeat_ref: entryRef,
                  // description not in this table — keep schema simple
                });
              aliasSaved = true;
            } else {
              await appDb('repeat_entry_aliases').insert({
                bank_code: bankCode,
                memo_pattern: statementName,
                opera_repeat_ref: entryRef,
              });
              aliasSaved = true;
            }
          } catch {
            // best-effort — alias save failure doesn't fail the operation
          }
        }

        return {
          success: true,
          message: `Updated '${description}' next posting date to ${newDate}`,
          entry_ref: entryRef,
          old_date: oldDate,
          new_date: newDate,
          alias_saved: aliasSaved,
        };
      },
    );
  } catch (err: any) {
    if (err instanceof ImportLockError) {
      return { success: false, error: err.message };
    }
    return { success: false, error: err?.message ?? String(err) };
  }
}
