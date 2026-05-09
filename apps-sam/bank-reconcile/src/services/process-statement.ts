/**
 * Process a bank statement — extract + match in one pass.
 *
 * Faithful port of `process_bank_statement`
 * (apps/bank_reconcile/api/routes.py:1370-1645) and
 * `process_statement_unified` (1719+, which is the same flow with a
 * different response shape used by the unified import UI).
 *
 * Pipeline:
 *   1. Extract statement + transactions via ctx.llm
 *   2. Validate bank match (already done by preview-from-pdf)
 *   3. Run duplicate detection on each transaction (uses the
 *      existing six-strategy `findDuplicates` from
 *      duplicate-detection.ts)
 *   4. Suggest customer/supplier accounts for unmatched lines
 *      (uses the existing suggestAccountForTransaction)
 *
 * The Python implementation does a much more thorough match
 * (alias lookup, refund detection, repeat-entry check, bank-transfer
 * detection). Those are independent ports of `_match_transaction` —
 * the SAM team can layer them as the UI needs them. This port
 * delivers the deterministic core: extract → flag duplicates →
 * suggest accounts.
 */
import type { Knex } from 'knex';
import { findDuplicates } from './duplicate-detection.js';
import {
  previewBankImportFromPdf,
  type LlmService,
  type PreviewResponse,
} from './preview-from-pdf.js';
import {
  suggestAccountForTransaction,
  type TransactionType,
} from './suggest-account.js';

export interface ProcessTransaction {
  date: string | null;
  name: string | null;
  memo: string | null;
  amount: number;
  type: string;
  balance?: number | null;
  line_number?: number;
  is_duplicate: boolean;
  duplicate_reason: string | null;
  suggested_account: {
    code: string;
    name: string;
    score: number;
    match_type: string;
  } | null;
  ledger_type: 'C' | 'S' | null;
  action: TransactionType | 'skip';
}

export interface ProcessStatementResponse extends PreviewResponse {
  matched_transactions?: ProcessTransaction[];
  matched_count?: number;
  duplicate_count?: number;
}

function inferAction(amount: number): TransactionType {
  return amount > 0 ? 'sales_receipt' : 'purchase_payment';
}

export async function processStatement(
  operaDb: Knex,
  llm: LlmService,
  input: {
    filePath?: string;
    pdfBytes?: Uint8Array;
    bankCode: string;
  },
): Promise<ProcessStatementResponse> {
  const preview = await previewBankImportFromPdf(operaDb, llm, input);
  if (!preview.success || !preview.transactions) {
    return preview;
  }

  const matched: ProcessTransaction[] = [];
  let duplicateCount = 0;
  let matchedCount = 0;

  for (const txn of preview.transactions) {
    const action = inferAction(txn.amount);

    // Duplicate detection
    const candidates = await findDuplicates(operaDb, {
      name: txn.name ?? '',
      amount: txn.amount,
      date: txn.date ?? new Date().toISOString().slice(0, 10),
      bank_code: input.bankCode,
    });
    const top = candidates.find((c) => c.confidence >= 0.85);
    const isDup = !!top;
    if (isDup) duplicateCount += 1;

    // Account suggestion
    let suggestedAccount: ProcessTransaction['suggested_account'] = null;
    let ledgerType: 'C' | 'S' | null = null;
    if (!isDup && txn.name) {
      const sug = await suggestAccountForTransaction(
        operaDb,
        txn.name,
        action,
        1,
      );
      if (sug.success && sug.suggestions.length > 0) {
        const first = sug.suggestions[0]!;
        suggestedAccount = {
          code: first.code,
          name: first.name,
          score: first.score,
          match_type: first.match_type,
        };
        ledgerType = sug.ledger_type ?? null;
        if (first.score >= 60) matchedCount += 1;
      }
    }

    matched.push({
      date: txn.date,
      name: txn.name,
      memo: txn.memo,
      amount: txn.amount,
      type: txn.type,
      balance: txn.balance ?? null,
      line_number: txn.line_number,
      is_duplicate: isDup,
      duplicate_reason: top
        ? `${top.table}.${top.record_id} (${top.match_type})`
        : null,
      suggested_account: suggestedAccount,
      ledger_type: ledgerType,
      action: isDup ? 'skip' : action,
    });
  }

  return {
    ...preview,
    matched_transactions: matched,
    matched_count: matchedCount,
    duplicate_count: duplicateCount,
  };
}
