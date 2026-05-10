/**
 * Scan emails for supplier statements.
 *
 * Same architectural shape as the bank-reconcile and gocardless ports:
 * SAM email-ingest captures supplier-statement-style emails into a
 * per-app table (`scanned_supplier_statements`); this endpoint reads
 * from it.
 *
 * Filters out statements already in `supplier_statements` (idempotency).
 * The matched supplier is stored when detected from the sender email
 * (see suppliers/src/services/approved-emails.ts).
 */
import type { Knex } from 'knex';

export interface ScanSupplierStatementsInput {
  daysBack?: number;
  includeProcessed?: boolean;
  supplierCode?: string;
}

export interface SupplierStatementCandidate {
  email_id: string;
  attachment_id: string;
  filename: string;
  size_bytes: number;
  content_type: string | null;
  received_at: string;
  from_address: string;
  subject: string;
  detected_supplier_code: string | null;
  statement_date: string | null;
  already_processed: boolean;
  status: 'ready' | 'pending_extraction';
}

export interface ScanSupplierStatementsResult {
  success: boolean;
  candidates: SupplierStatementCandidate[];
  already_processed_count: number;
  warnings?: string[];
  error?: string;
}

interface ScannedRow {
  id: number;
  email_id: string;
  attachment_id: string;
  filename: string;
  size_bytes: number;
  content_type: string | null;
  received_at: string;
  from_address: string;
  subject: string;
  detected_supplier_code: string | null;
  statement_date: string | null;
}

async function loadProcessedKeys(appDb: Knex): Promise<Set<string>> {
  try {
    const rows = (await appDb('supplier_statements')
      .select('source_ref')) as Array<{ source_ref: string | null }>;
    const set = new Set<string>();
    for (const r of rows ?? []) {
      if (r.source_ref) set.add(r.source_ref);
    }
    return set;
  } catch {
    return new Set<string>();
  }
}

export async function scanEmailsForSupplierStatements(
  appDb: Knex,
  input: ScanSupplierStatementsInput = {},
): Promise<ScanSupplierStatementsResult> {
  const daysBack = Math.max(1, Math.min(365, Number(input.daysBack ?? 30)));
  const includeProcessed = !!input.includeProcessed;
  const cutoff = new Date(Date.now() - daysBack * 24 * 60 * 60 * 1000);
  const supplierFilter = (input.supplierCode ?? '').trim();

  let scanned: ScannedRow[] = [];
  try {
    let q = appDb('scanned_supplier_statements')
      .where('received_at', '>=', cutoff)
      .orderBy('received_at', 'desc');
    if (supplierFilter) {
      q = q.andWhere('detected_supplier_code', supplierFilter);
    }
    scanned = (await q.select(
      'id',
      'email_id',
      'attachment_id',
      'filename',
      'size_bytes',
      'content_type',
      'received_at',
      'from_address',
      'subject',
      'detected_supplier_code',
      'statement_date',
    )) as ScannedRow[];
  } catch {
    scanned = [];
  }

  const processed = await loadProcessedKeys(appDb);
  let alreadyProcessedCount = 0;
  const candidates: SupplierStatementCandidate[] = [];
  for (const r of scanned) {
    const key = `${r.email_id}|${r.attachment_id}`;
    const isProcessed = processed.has(key);
    if (isProcessed) alreadyProcessedCount += 1;
    if (isProcessed && !includeProcessed) continue;
    candidates.push({
      email_id: r.email_id,
      attachment_id: r.attachment_id,
      filename: r.filename,
      size_bytes: Number(r.size_bytes ?? 0),
      content_type: r.content_type,
      received_at: r.received_at,
      from_address: r.from_address,
      subject: r.subject,
      detected_supplier_code: r.detected_supplier_code,
      statement_date: r.statement_date,
      already_processed: isProcessed,
      status: 'pending_extraction',
    });
  }

  return {
    success: true,
    candidates,
    already_processed_count: alreadyProcessedCount,
    warnings: [
      'Scan returns supplier statement emails captured by the SAM email-ingest handler. AI extraction (Gemini) of the PDF is not yet wired in this build — each candidate is returned with status=pending_extraction so the operator can trigger extraction on demand.',
    ],
  };
}
