/**
 * Extract a supplier statement from already-extracted line data.
 *
 * The actual AI extraction (Gemini) lives in a separate service that
 * reads the PDF and returns a structured payload. This service takes
 * that payload and persists:
 *
 *   1. A row in `supplier_statements` (header)
 *   2. One row per line in `statement_lines`
 *
 * Idempotent against `source_ref` — re-extracting the same email/file
 * updates rather than duplicates. The full extraction pipeline (PDF
 * download via SAM emailIngest, AI extraction, persisting) is the
 * `extractAndPersistStatement` orchestrator below.
 */
import type { Knex } from 'knex';

export interface ExtractedLine {
  line_date: string; // YYYY-MM-DD
  reference?: string;
  description?: string;
  amount: number;
  /** Optional best-guess match against ptran */
  matched_opera_ref?: string;
}

export interface PersistStatementInput {
  supplierCode: string;
  statementDate: string; // YYYY-MM-DD
  openingBalance: number;
  closingBalance: number;
  source: 'email' | 'file' | 'manual';
  sourceRef: string; // unique ref — usually email_id|attachment_id or file path
  pdfPath?: string;
  lines: ExtractedLine[];
}

export interface PersistStatementResult {
  success: boolean;
  statement_id?: number;
  inserted_lines?: number;
  updated?: boolean;
  error?: string;
}

export async function persistExtractedStatement(
  appDb: Knex,
  input: PersistStatementInput,
): Promise<PersistStatementResult> {
  const supplierCode = (input.supplierCode ?? '').trim();
  if (!supplierCode) {
    return { success: false, error: 'supplier_code required' };
  }
  if (!input.statementDate) {
    return { success: false, error: 'statement_date required' };
  }
  if (!input.sourceRef) {
    return { success: false, error: 'source_ref required for idempotency' };
  }

  try {
    let updated = false;
    let statementId = 0;

    await appDb.transaction(async (trx) => {
      // Idempotency: replace existing statement under same source_ref
      const existing = await trx('supplier_statements')
        .where({ source_ref: input.sourceRef })
        .first();

      if (existing) {
        statementId = Number((existing as any).id);
        await trx('supplier_statements')
          .where({ id: statementId })
          .update({
            supplier_code: supplierCode,
            statement_date: input.statementDate,
            opening_balance: input.openingBalance,
            closing_balance: input.closingBalance,
            source: input.source,
            pdf_path: input.pdfPath ?? '',
            imported_at: trx.fn.now(),
          });
        await trx('statement_lines').where({ statement_id: statementId }).delete();
        updated = true;
      } else {
        const inserted = await trx('supplier_statements')
          .insert({
            supplier_code: supplierCode,
            statement_date: input.statementDate,
            opening_balance: input.openingBalance,
            closing_balance: input.closingBalance,
            source: input.source,
            source_ref: input.sourceRef,
            pdf_path: input.pdfPath ?? '',
            imported_at: trx.fn.now(),
          })
          .returning('id');
        statementId =
          typeof inserted[0] === 'object' && inserted[0] !== null
            ? Number((inserted[0] as { id: number }).id)
            : Number(inserted[0]);
      }

      if (Array.isArray(input.lines) && input.lines.length > 0) {
        const rows = input.lines.map((l) => ({
          statement_id: statementId,
          line_date: l.line_date,
          reference: (l.reference ?? '').slice(0, 64),
          description: (l.description ?? '').slice(0, 200),
          amount: Number(l.amount ?? 0),
          matched_opera_ref: (l.matched_opera_ref ?? '').slice(0, 64),
          match_status: (l.matched_opera_ref ?? '').trim()
            ? 'matched'
            : 'unmatched',
        }));
        // Chunk to avoid SQL Server's 2100 parameter limit
        const chunkSize = 100;
        for (let i = 0; i < rows.length; i += chunkSize) {
          await trx('statement_lines').insert(rows.slice(i, i + chunkSize));
        }
      }
    });

    return {
      success: true,
      statement_id: statementId,
      inserted_lines: input.lines?.length ?? 0,
      updated,
    };
  } catch (e: any) {
    return { success: false, error: e?.message ?? String(e) };
  }
}
