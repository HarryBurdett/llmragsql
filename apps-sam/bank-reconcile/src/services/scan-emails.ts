/**
 * Scan emails for bank statement attachments — port of
 * `scan_emails_for_bank_statements` (apps/bank_reconcile/api/routes.py:6044+).
 *
 * The Python implementation queries a per-company SQLite mailbox cache
 * (populated by a separate background sync) for emails received in the
 * last N days, then filters to those carrying PDF attachments that
 * look like bank statements.
 *
 * SAM's email-ingest is event-based (registerHandler) rather than
 * query-based, so the natural port is:
 *
 *   1. On plugin start, register a handler that captures incoming
 *      emails matching bank-statement heuristics into the per-app DB
 *      table `scanned_statements`.
 *   2. /api/bank-import/scan-emails reads from that local table
 *      (filterable by bank_code + days_back), exactly as the Python
 *      version reads from its mailbox cache.
 *
 * The handler-registration piece is plumbed via SAM's
 * `ctx.emailIngest.registerHandler` — see the plugin index for wiring.
 * Until the handler-side capture is wired up the endpoint returns an
 * empty list with a deferred-feature note so the UI can fall back to
 * manual upload (which IS fully ported via /api/bank-import/draft).
 *
 * The bank-existence guard, sort-code/account-number lookup, and
 * reconciled-balance read are ported faithfully from Python so the
 * same validation surfaces on the SAM endpoint.
 */
import type { Knex } from 'knex';
import { validateBankCode, SqlInputValidationError } from '@sqlrag/sam-shared';

export interface ScanEmailsInput {
  bankCode: string;
  daysBack?: number;
  includeProcessed?: boolean;
  validateBalances?: boolean;
}

export interface BankStatementAttachment {
  email_id: string;
  attachment_id: string;
  filename: string;
  size_bytes: number;
  content_type: string;
  received_at: string;
  from_address: string;
  subject: string;
  detected_bank: string | null;
  statement_date: string | null;
  sort_key: number | null;
  already_processed: boolean;
  status: 'ready' | 'pending_extraction';
}

export interface ScanEmailsResult {
  success: boolean;
  bank_code: string;
  bank_master?: {
    sort_code: string;
    account_number: string;
    reconciled_balance_pence: number;
  };
  statements: BankStatementAttachment[];
  already_processed_count: number;
  warnings?: string[];
  error?: string;
}

interface ScannedStatementRow {
  id: number;
  email_id: string;
  attachment_id: string;
  filename: string;
  size_bytes: number;
  content_type: string;
  received_at: string;
  from_address: string;
  subject: string;
  detected_bank: string | null;
  statement_date: string | null;
  sort_key: number | null;
}

async function loadProcessedKeys(
  appDb: Knex,
  bankCode: string,
): Promise<Set<string>> {
  try {
    const rows = (await appDb('bank_statement_imports')
      .where({ bank_code: bankCode })
      .select('email_id', 'attachment_id', 'filename')) as Array<{
      email_id: string | null;
      attachment_id: string | null;
      filename: string | null;
    }>;
    const set = new Set<string>();
    for (const r of rows ?? []) {
      if (r.email_id && r.attachment_id) {
        set.add(`${r.email_id}|${r.attachment_id}`);
      }
      if (r.filename) set.add(`f|${r.filename}`);
    }
    return set;
  } catch {
    return new Set<string>();
  }
}

async function loadScannedStatements(
  appDb: Knex,
  bankCode: string,
  daysBack: number,
): Promise<ScannedStatementRow[]> {
  try {
    const cutoff = new Date(Date.now() - daysBack * 24 * 60 * 60 * 1000);
    return (await appDb('scanned_statements')
      .where('received_at', '>=', cutoff)
      .andWhere((qb) => {
        // detected_bank either null (still pending detection) or matches
        qb.whereNull('detected_bank').orWhere(
          'detected_bank',
          bankCode,
        );
      })
      .orderBy('received_at', 'desc')
      .select(
        'id',
        'email_id',
        'attachment_id',
        'filename',
        'size_bytes',
        'content_type',
        'received_at',
        'from_address',
        'subject',
        'detected_bank',
        'statement_date',
        'sort_key',
      )) as ScannedStatementRow[];
  } catch {
    return [];
  }
}

export async function scanEmailsForBankStatements(
  appDb: Knex,
  operaDb: Knex,
  input: ScanEmailsInput,
): Promise<ScanEmailsResult> {
  let code: string;
  try {
    code = validateBankCode(input.bankCode);
  } catch (e) {
    if (e instanceof SqlInputValidationError) {
      return {
        success: false,
        bank_code: input.bankCode,
        statements: [],
        already_processed_count: 0,
        error: e.message,
      };
    }
    throw e;
  }

  // Bank existence + sort-code/account-number/recbal lookup
  const bankRows = (await operaDb.raw(
    `SELECT TOP 1
        RTRIM(ISNULL(nk_sort, '')) AS sort_code,
        RTRIM(ISNULL(nk_number, '')) AS account_number,
        ISNULL(nk_recbal, 0) AS reconciled_balance_pence
       FROM nbank WITH (NOLOCK)
       WHERE RTRIM(nk_acnt) = ?`,
    [code],
  )) as Array<{
    sort_code: string;
    account_number: string;
    reconciled_balance_pence: number;
  }>;
  if (!Array.isArray(bankRows) || bankRows.length === 0) {
    return {
      success: false,
      bank_code: code,
      statements: [],
      already_processed_count: 0,
      error: `Bank account '${code}' not found in Opera. Please select a valid bank account.`,
    };
  }
  const bank = bankRows[0]!;

  const daysBack = Math.max(1, Math.min(365, Number(input.daysBack ?? 30)));
  const includeProcessed = !!input.includeProcessed;
  const processed = await loadProcessedKeys(appDb, code);
  const scanned = await loadScannedStatements(appDb, code, daysBack);

  let alreadyProcessedCount = 0;
  const statements: BankStatementAttachment[] = [];
  for (const r of scanned) {
    const key = `${r.email_id}|${r.attachment_id}`;
    const fkey = `f|${r.filename}`;
    const isProcessed = processed.has(key) || processed.has(fkey);
    if (isProcessed) alreadyProcessedCount += 1;
    if (isProcessed && !includeProcessed) continue;

    statements.push({
      email_id: r.email_id,
      attachment_id: r.attachment_id,
      filename: r.filename,
      size_bytes: Number(r.size_bytes ?? 0),
      content_type: r.content_type,
      received_at: r.received_at,
      from_address: r.from_address,
      subject: r.subject,
      detected_bank: r.detected_bank,
      statement_date: r.statement_date,
      sort_key: r.sort_key,
      already_processed: isProcessed,
      // Without the SAM-side AI extraction wiring the status is always
      // pending_extraction — UI shows the operator a "Process" button
      // that triggers extraction on demand.
      status: 'pending_extraction',
    });
  }

  return {
    success: true,
    bank_code: code,
    bank_master: {
      sort_code: bank.sort_code,
      account_number: bank.account_number,
      reconciled_balance_pence: Number(bank.reconciled_balance_pence ?? 0),
    },
    statements,
    already_processed_count: alreadyProcessedCount,
    warnings: [
      'Scan returns statements captured by the SAM email-ingest handler. AI extraction (Gemini) is not yet wired in this build — each statement is returned with status=pending_extraction so the operator can trigger extraction on demand from the UI.',
    ],
  };
}
