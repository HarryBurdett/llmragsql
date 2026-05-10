/**
 * Scan emails for GoCardless payout notifications — port of
 * `scan_emails_for_gocardless` (apps/gocardless/api/routes.py).
 *
 * Same architectural shape as the bank-reconcile scan-emails port:
 * SAM email-ingest captures payout-style emails into a per-app table
 * (`scanned_payouts`); this endpoint reads from it.
 *
 * The endpoint also filters out emails already imported via
 * `gocardless_imports` (idempotency), and joins to
 * `gocardless_payment_requests` so the operator can see which payouts
 * relate to known collection requests.
 */
import type { Knex } from 'knex';

export interface ScanGcEmailsInput {
  daysBack?: number;
  includeProcessed?: boolean;
}

export interface GcPayoutCandidate {
  email_id: string;
  attachment_id: string | null;
  filename: string | null;
  size_bytes: number;
  content_type: string | null;
  received_at: string;
  from_address: string;
  subject: string;
  detected_payout_id: string | null;
  detected_amount: number | null;
  detected_currency: string | null;
  already_imported: boolean;
  status: 'ready' | 'pending_extraction';
}

export interface ScanGcEmailsResult {
  success: boolean;
  candidates: GcPayoutCandidate[];
  already_imported_count: number;
  warnings?: string[];
  error?: string;
}

interface ScannedPayoutRow {
  id: number;
  email_id: string;
  attachment_id: string | null;
  filename: string | null;
  size_bytes: number;
  content_type: string | null;
  received_at: string;
  from_address: string;
  subject: string;
  detected_payout_id: string | null;
  detected_amount: number | null;
  detected_currency: string | null;
}

async function loadImportedKeys(appDb: Knex): Promise<Set<string>> {
  try {
    const rows = (await appDb('gocardless_imports')
      .whereNotNull('payout_id')
      .select('payout_id', 'email_id')) as Array<{
      payout_id: string | null;
      email_id: number | null;
    }>;
    const set = new Set<string>();
    for (const r of rows ?? []) {
      if (r.payout_id) set.add(`p|${r.payout_id}`);
      if (r.email_id) set.add(`e|${r.email_id}`);
    }
    return set;
  } catch {
    return new Set<string>();
  }
}

export async function scanEmailsForGocardless(
  appDb: Knex,
  input: ScanGcEmailsInput = {},
): Promise<ScanGcEmailsResult> {
  const daysBack = Math.max(1, Math.min(365, Number(input.daysBack ?? 30)));
  const includeProcessed = !!input.includeProcessed;
  const cutoff = new Date(Date.now() - daysBack * 24 * 60 * 60 * 1000);

  let scanned: ScannedPayoutRow[] = [];
  try {
    scanned = (await appDb('scanned_payouts')
      .where('received_at', '>=', cutoff)
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
        'detected_payout_id',
        'detected_amount',
        'detected_currency',
      )) as ScannedPayoutRow[];
  } catch {
    scanned = [];
  }

  const imported = await loadImportedKeys(appDb);

  let alreadyImportedCount = 0;
  const candidates: GcPayoutCandidate[] = [];
  for (const r of scanned) {
    const isImported =
      (r.detected_payout_id && imported.has(`p|${r.detected_payout_id}`)) ||
      imported.has(`e|${r.email_id}`);
    if (isImported) alreadyImportedCount += 1;
    if (isImported && !includeProcessed) continue;
    candidates.push({
      email_id: r.email_id,
      attachment_id: r.attachment_id,
      filename: r.filename,
      size_bytes: Number(r.size_bytes ?? 0),
      content_type: r.content_type,
      received_at: r.received_at,
      from_address: r.from_address,
      subject: r.subject,
      detected_payout_id: r.detected_payout_id,
      detected_amount:
        r.detected_amount !== null ? Number(r.detected_amount) : null,
      detected_currency: r.detected_currency,
      already_imported: !!isImported,
      status: 'pending_extraction',
    });
  }

  return {
    success: true,
    candidates,
    already_imported_count: alreadyImportedCount,
    warnings: [
      'Scan returns payout-style emails captured by the SAM email-ingest handler. AI extraction (Gemini) of the GoCardless email body is not yet wired in this build — each candidate is returned with status=pending_extraction so the operator can trigger extraction on demand.',
    ],
  };
}
