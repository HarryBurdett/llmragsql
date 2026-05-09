/**
 * Bank-import / import-from-pdf — orchestration shell.
 *
 * Faithful port of the route-level orchestration from
 * `import_bank_statement_from_pdf` (apps/bank_reconcile/api/routes.py:4031-4787)
 * with the heavy lifting (PDF extraction, transaction matching, the
 * actual aentry/atran/sname/pname/ntran writes, auto-allocate, and
 * auto-reconcile) delegated to discrete executor adapters.
 *
 * Why split this up?
 *   - PDF extraction needs `ctx.llm` (Claude vision) — not yet wired.
 *   - The 750-line posting body has many seams that the SAM team will
 *     fill against the unified Knex client. Carving the contract now
 *     lets the frontend wire while the executor is built independently.
 *   - Keeping the orchestration shell deterministic means the route
 *     can run validations + audit-row writes today without ctx.llm.
 *
 * Validations performed here (Python parity):
 *   - bank_code exists in nbank
 *   - file path provided + non-empty
 *   - import-lock acquired/released around the executor
 *   - import history row written on success
 *
 * Everything between extraction and posting is an executor seam.
 */
import type { Knex } from 'knex';
import {
  validateBankCode,
  SqlInputValidationError,
} from '@sqlrag/sam-shared';

export interface PdfExtractionResult {
  bank_name: string | null;
  account_number: string | null;
  sort_code: string | null;
  statement_date: string | null;
  period_start: string | null;
  period_end: string | null;
  opening_balance: number | null;
  closing_balance: number | null;
  transactions: Array<{
    date: string | null;
    name: string | null;
    memo: string | null;
    amount: number;
    type: 'credit' | 'debit' | string;
    line_number?: number;
  }>;
}

export interface PdfExtractor {
  /**
   * Read a PDF (or PDF bytes) and return extracted statement +
   * transactions. Implementation will use ctx.llm when wired.
   */
  extractFromPdf(opts: {
    filePath?: string;
    bytes?: Uint8Array;
    filename?: string;
  }): Promise<PdfExtractionResult>;
}

export interface ImportPostingExecutor {
  postBankImport(opts: {
    operaDb: Knex;
    bankCode: string;
    statementInfo: PdfExtractionResult;
    transactions: PdfExtractionResult['transactions'];
    overrides: unknown[];
    selectedRows: number[] | null;
    autoAllocate: boolean;
    autoReconcile: boolean;
  }): Promise<{
    success: boolean;
    records_imported: number;
    records_failed: number;
    skipped_count: number;
    errors: string[];
    warnings: string[];
    import_id?: number | null;
  }>;
}

export interface ImportLockAdapter {
  acquire(key: string, locker: string): Promise<boolean>;
  release(key: string): Promise<void>;
}

export interface PeriodOverlapChecker {
  checkOverlap(opts: {
    bankCode: string;
    periodStart: string | null;
    periodEnd: string | null;
    filename: string;
    resumeImportId: number | null;
    skipOverlapCheck: boolean;
  }): Promise<{
    overlapError?: { success: false; error: string } | null;
    resumeImportId: number | null;
  }>;
}

export interface ImportFromPdfInput {
  filePath: string;
  bankCode: string;
  filename?: string;
  autoAllocate?: boolean;
  autoReconcile?: boolean;
  resumeImportId?: number | null;
  overrides?: unknown[];
  selectedRows?: number[] | null;
  dateOverrides?: unknown[];
  rejectedRefundRows?: number[];
  skipOverlapCheck?: boolean;
}

export interface ImportFromPdfResponse {
  success: boolean;
  message?: string;
  records_imported?: number;
  records_failed?: number;
  skipped_count?: number;
  warnings?: string[];
  errors?: string[];
  error?: string;
  resume_import_id?: number | null;
  import_id?: number | null;
}

async function bankExists(operaDb: Knex, bankCode: string): Promise<boolean> {
  try {
    const row = (await operaDb('nbank')
      .whereRaw('RTRIM(nk_acnt) = ?', [bankCode])
      .select('nk_acnt')
      .first()) as { nk_acnt?: string } | undefined;
    return !!row;
  } catch {
    return false;
  }
}

export async function importBankStatementFromPdf(
  operaDb: Knex,
  appDb: Knex,
  input: ImportFromPdfInput,
  extractor: PdfExtractor,
  executor: ImportPostingExecutor,
  importLock: ImportLockAdapter,
  overlapChecker: PeriodOverlapChecker,
): Promise<ImportFromPdfResponse> {
  let bankCode: string;
  try {
    bankCode = validateBankCode(input.bankCode);
  } catch (e) {
    if (e instanceof SqlInputValidationError) {
      return { success: false, error: e.message };
    }
    return { success: false, error: (e as Error)?.message ?? String(e) };
  }

  if (!input.filePath || !input.filePath.trim()) {
    return { success: false, error: 'file_path is required' };
  }

  if (!(await bankExists(operaDb, bankCode))) {
    return {
      success: false,
      error: `Bank account '${bankCode}' not found in Opera.`,
    };
  }

  let extracted: PdfExtractionResult;
  try {
    extracted = await extractor.extractFromPdf({
      filePath: input.filePath,
      filename: input.filename,
    });
  } catch (e) {
    return {
      success: false,
      error: `PDF extraction failed: ${(e as Error)?.message ?? String(e)}`,
    };
  }

  if (!extracted || !extracted.transactions) {
    return {
      success: false,
      error: 'Failed to extract statement information from PDF',
    };
  }

  const overlap = await overlapChecker.checkOverlap({
    bankCode,
    periodStart: extracted.period_start,
    periodEnd: extracted.period_end,
    filename: input.filename ?? input.filePath.split('/').pop() ?? '',
    resumeImportId: input.resumeImportId ?? null,
    skipOverlapCheck: !!input.skipOverlapCheck,
  });
  if (overlap.overlapError) {
    return {
      ...overlap.overlapError,
      resume_import_id: overlap.resumeImportId,
    };
  }

  const lockKey = `bank-import:${bankCode}`;
  const acquired = await importLock.acquire(lockKey, 'import-from-pdf');
  if (!acquired) {
    return {
      success: false,
      error: `Bank account ${bankCode} is currently being imported by another user. Please wait for the current import to complete.`,
    };
  }

  try {
    const result = await executor.postBankImport({
      operaDb,
      bankCode,
      statementInfo: extracted,
      transactions: extracted.transactions,
      overrides: input.overrides ?? [],
      selectedRows: input.selectedRows ?? null,
      autoAllocate: !!input.autoAllocate,
      autoReconcile: !!input.autoReconcile,
    });

    if (result.success) {
      try {
        await appDb('bank_statement_imports').insert({
          bank_code: bankCode,
          source: 'file',
          source_ref: input.filename ?? input.filePath,
          opening_balance: extracted.opening_balance,
          closing_balance: extracted.closing_balance,
          imported_at: appDb.fn.now(),
          import_status: 'imported',
          records_imported: result.records_imported,
        });
      } catch {
        // history write failure is non-fatal
      }
      return {
        success: true,
        message: `Imported ${result.records_imported} transactions`,
        records_imported: result.records_imported,
        records_failed: result.records_failed,
        skipped_count: result.skipped_count,
        warnings: result.warnings,
        import_id: result.import_id ?? null,
        resume_import_id: overlap.resumeImportId,
      };
    }
    return {
      success: false,
      error: result.errors.join('; ') || 'Import failed',
      errors: result.errors,
      warnings: result.warnings,
      resume_import_id: overlap.resumeImportId,
    };
  } finally {
    try {
      await importLock.release(lockKey);
    } catch {
      // best-effort
    }
  }
}
