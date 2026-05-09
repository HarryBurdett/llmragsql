/**
 * Supplier statement state-transition actions.
 *
 * Faithful ports of:
 *   - process_supplier_statement       (routes.py:4012)
 *   - acknowledge_supplier_statement   (routes.py:2611)
 *   - approve_supplier_statement       (routes.py:2450)
 *   - edit_statement_response          (routes.py:2783)
 *   - bulk_approve_statements          (routes.py:2834)
 *
 * Email send is delegated via `EmailSender` so tests don't hit the
 * SMTP relay; the route layer wraps `ctx.email.send`. The supplier
 * pname lookup runs against `operaDb`. Policy gates
 * (`never_communicate`) are enforced consistently across all
 * outbound paths.
 */
import type { Knex } from 'knex';

export interface EmailSender {
  send(opts: {
    to: string;
    subject: string;
    body: string;
    pdfPath?: string | null;
  }): Promise<{ success: boolean; error?: string }>;
}

export interface OperaSupplierLookup {
  /** Returns supplier display name, or the code if lookup fails. */
  resolveName(supplierCode: string): Promise<string>;
}

export interface PtranLine {
  pt_unique: string;
  pt_trref: string | null;
  pt_supref: string | null;
  pt_trtype: string | null;
  pt_trvalue: number;
  pt_trbal: number;
  pt_trdate: Date | string | null;
}

export interface PtranLookup {
  forSupplier(supplierCode: string): Promise<PtranLine[]>;
}

interface StatementRow {
  id: number;
  supplier_code: string;
  status: string;
  statement_date: string | Date | null;
  received_date: string | Date | null;
  sender_email: string | null;
  acknowledged_at: string | Date | null;
  response_text: string | null;
  response_subject: string | null;
  email_pdf_path: string | null;
}

interface LineRow {
  id: number;
  statement_id: number;
  reference: string | null;
  amount: number | null;
  status: string | null;
  match_status: string | null;
  matched_opera_ref: string | null;
}

async function getStatement(
  appDb: Knex,
  statementId: number,
): Promise<StatementRow | null> {
  const row = (await appDb('supplier_statements')
    .where({ id: statementId })
    .first()) as StatementRow | undefined;
  return row ?? null;
}

async function isCommunicationAllowed(
  appDb: Knex,
  supplierCode: string,
): Promise<{ allowed: boolean; reason?: string }> {
  const row = (await appDb('supplier_contacts_ext')
    .where({ supplier_code: supplierCode, never_communicate: true })
    .first()) as { id?: number } | undefined;
  if (row) {
    return {
      allowed: false,
      reason: `Supplier ${supplierCode} is marked never_communicate — outbound email blocked.`,
    };
  }
  return { allowed: true };
}

async function resolveContactEmail(
  appDb: Knex,
  supplierCode: string,
  fallback: string | null,
): Promise<string | null> {
  const contact = (await appDb('supplier_contacts_ext')
    .where({ supplier_code: supplierCode, is_statement_contact: true })
    .whereNotNull('contact_email')
    .first()) as { contact_email?: string } | undefined;
  if (contact?.contact_email) return contact.contact_email;
  return fallback ?? null;
}

async function loadAutomationSetting(
  appDb: Knex,
  key: string,
): Promise<string | null> {
  const row = (await appDb('supplier_automation_settings')
    .where({ key })
    .first()) as { value?: string } | undefined;
  return row?.value ?? null;
}

// ---------------------------------------------------------------------
// process — reconcile statement lines against ptran
// ---------------------------------------------------------------------

export interface ProcessResponse {
  success: boolean;
  matched: number;
  query: number;
  unmatched: number;
  status?: string;
  error?: string;
}

export async function processStatement(
  appDb: Knex,
  statementId: number,
  ptranLookup: PtranLookup,
): Promise<ProcessResponse> {
  const stmt = await getStatement(appDb, statementId);
  if (!stmt) return { success: false, matched: 0, query: 0, unmatched: 0, error: 'Statement not found' };
  if (!['received', 'error'].includes(stmt.status)) {
    return {
      success: false,
      matched: 0,
      query: 0,
      unmatched: 0,
      error: `Statement cannot be processed from status '${stmt.status}'`,
    };
  }

  await appDb('supplier_statements')
    .where({ id: statementId })
    .update({ status: 'processing', updated_at: appDb.fn.now() });

  const ptran = await ptranLookup.forSupplier(stmt.supplier_code);
  const lines = (await appDb('statement_lines')
    .where({ statement_id: statementId })) as unknown as LineRow[];

  let matched = 0;
  let query = 0;
  let unmatched = 0;

  for (const line of lines) {
    const ref = (line.reference ?? '').trim();
    const lower = ref.toLowerCase();
    let matchStatus: 'matched' | 'unmatched' | 'query' = 'unmatched';
    let matchedRef: string | null = null;
    if (ref) {
      const hit = ptran.find(
        (p) =>
          (p.pt_trref ?? '').toLowerCase().includes(lower) ||
          (p.pt_supref ?? '').toLowerCase().includes(lower),
      );
      if (hit) {
        matchStatus = 'matched';
        matchedRef = hit.pt_unique;
      }
    }
    if (matchStatus === 'unmatched' && Number(line.amount ?? 0) > 0) {
      matchStatus = 'query';
    }
    if (matchStatus === 'matched') matched += 1;
    else if (matchStatus === 'query') query += 1;
    else unmatched += 1;

    await appDb('statement_lines')
      .where({ id: line.id })
      .update({
        match_status: matchStatus,
        status: matchStatus === 'matched' ? 'Agreed' : matchStatus === 'query' ? 'Query' : 'Pending',
        matched_opera_ref: matchedRef,
      });
  }

  // Statement transitions to 'queued' after processing.
  await appDb('supplier_statements')
    .where({ id: statementId })
    .update({
      status: 'queued',
      processed_at: appDb.fn.now(),
      updated_at: appDb.fn.now(),
    });

  return {
    success: true,
    matched,
    query,
    unmatched,
    status: 'queued',
  };
}

// ---------------------------------------------------------------------
// acknowledge — send acknowledgement email and mark statement
// ---------------------------------------------------------------------

const DEFAULT_ACK_TEMPLATE =
  'Thank you for sending your statement dated {date}. We have received it and are currently processing.';

export interface AcknowledgeResponse {
  success: boolean;
  message?: string;
  email_sent?: boolean;
  recipient?: string | null;
  subject?: string;
  body?: string;
  email_error?: string;
  earliest_send_at?: string;
  policy_blocked?: boolean;
  error?: string;
}

export async function acknowledgeStatement(
  appDb: Knex,
  email: EmailSender,
  supplierLookup: OperaSupplierLookup,
  statementId: number,
): Promise<AcknowledgeResponse> {
  const stmt = await getStatement(appDb, statementId);
  if (!stmt) return { success: false, error: 'Statement not found' };
  if (stmt.acknowledged_at) {
    return { success: false, error: 'Statement has already been acknowledged' };
  }

  const delayRaw = await loadAutomationSetting(
    appDb,
    'acknowledgment_delay_minutes',
  );
  const delayMinutes = delayRaw ? Number(delayRaw) : 0;
  if (delayMinutes > 0 && stmt.received_date) {
    const received =
      stmt.received_date instanceof Date
        ? stmt.received_date
        : new Date(String(stmt.received_date));
    if (!Number.isNaN(received.getTime())) {
      const earliest = new Date(received.getTime() + delayMinutes * 60_000);
      if (Date.now() < earliest.getTime()) {
        return {
          success: false,
          error: `Acknowledgment delayed. Earliest send time: ${earliest.toISOString()}`,
          earliest_send_at: earliest.toISOString(),
        };
      }
    }
  }

  const policy = await isCommunicationAllowed(appDb, stmt.supplier_code);
  if (!policy.allowed) {
    return { success: false, error: policy.reason, policy_blocked: true };
  }

  const template =
    (await loadAutomationSetting(appDb, 'acknowledgment_template')) ??
    DEFAULT_ACK_TEMPLATE;
  const dateLabel =
    stmt.statement_date instanceof Date
      ? stmt.statement_date.toISOString().slice(0, 10)
      : String(stmt.statement_date ?? 'N/A');
  const body = template.replaceAll('{date}', dateLabel);

  const supplierName = await supplierLookup.resolveName(stmt.supplier_code);
  const subject = `Statement Received - ${supplierName} - ${dateLabel}`;

  const recipient = await resolveContactEmail(
    appDb,
    stmt.supplier_code,
    stmt.sender_email,
  );
  if (!recipient) {
    return {
      success: false,
      error: 'No contact email found for this supplier',
    };
  }

  const sendResult = await email.send({ to: recipient, subject, body });
  const nowIso = new Date().toISOString();

  await appDb('supplier_statements')
    .where({ id: statementId })
    .update({
      status: 'acknowledged',
      acknowledged_at: nowIso,
      updated_at: appDb.fn.now(),
    });

  await appDb('supplier_communications').insert({
    supplier_code: stmt.supplier_code,
    channel: 'email',
    subject,
    content: body,
    sent_at: appDb.fn.now(),
  });

  const out: AcknowledgeResponse = {
    success: true,
    message:
      'Statement acknowledged' + (sendResult.success ? ' and email sent' : ''),
    email_sent: sendResult.success,
    recipient,
    subject,
    body,
  };
  if (!sendResult.success && sendResult.error) {
    out.email_error = sendResult.error;
  }
  return out;
}

// ---------------------------------------------------------------------
// approve — send the response email and mark statement
// ---------------------------------------------------------------------

export interface ApproveInput {
  approvedBy: string;
  body?: string | null;
  subject?: string | null;
}

export interface ApproveResponse extends AcknowledgeResponse {}

export async function approveStatement(
  appDb: Knex,
  email: EmailSender,
  supplierLookup: OperaSupplierLookup,
  statementId: number,
  input: ApproveInput,
): Promise<ApproveResponse> {
  const stmt = await getStatement(appDb, statementId);
  if (!stmt) return { success: false, error: 'Statement not found' };
  if (
    !['reconciled', 'acknowledged', 'queued', 'received'].includes(stmt.status)
  ) {
    return {
      success: false,
      error: `Cannot approve statement from status '${stmt.status}'`,
    };
  }

  const policy = await isCommunicationAllowed(appDb, stmt.supplier_code);
  if (!policy.allowed) {
    return { success: false, error: policy.reason, policy_blocked: true };
  }

  const supplierName = await supplierLookup.resolveName(stmt.supplier_code);
  const dateLabel =
    stmt.statement_date instanceof Date
      ? stmt.statement_date.toISOString().slice(0, 10)
      : String(stmt.statement_date ?? '');

  const body =
    input.body ??
    stmt.response_text ??
    `Statement reconciliation complete for ${supplierName}.`;

  const queryCountRow = (await appDb('statement_lines')
    .where({ statement_id: statementId, match_status: 'query' })
    .count<{ total: number | string }[]>('id as total')
    .first()) as { total: number | string } | undefined;
  const hasQueries = Number(queryCountRow?.total ?? 0) > 0;

  const subject =
    input.subject ??
    stmt.response_subject ??
    `Statement Reconciled - ${supplierName} - ${dateLabel}${
      hasQueries ? ' (queries to discuss)' : ''
    }`;

  const recipient = await resolveContactEmail(
    appDb,
    stmt.supplier_code,
    stmt.sender_email,
  );
  let sendResult: { success: boolean; error?: string } = {
    success: false,
    error: 'No recipient email',
  };
  if (recipient) {
    sendResult = await email.send({
      to: recipient,
      subject,
      body,
      pdfPath: stmt.email_pdf_path,
    });
  }

  const nowIso = new Date().toISOString();
  await appDb('supplier_statements')
    .where({ id: statementId })
    .update({
      status: sendResult.success ? 'sent' : 'approved',
      approved_by: input.approvedBy,
      approved_at: nowIso,
      sent_at: sendResult.success ? nowIso : null,
      updated_at: appDb.fn.now(),
    });

  await appDb('supplier_communications').insert({
    supplier_code: stmt.supplier_code,
    channel: 'email',
    subject,
    content: body,
    sent_at: appDb.fn.now(),
  });

  const out: ApproveResponse = {
    success: true,
    message:
      'Statement approved' + (sendResult.success ? ' and email sent' : ''),
    email_sent: sendResult.success,
    recipient,
    subject,
    body,
  };
  if (!sendResult.success && sendResult.error) {
    out.email_error = sendResult.error;
  }
  return out;
}

// ---------------------------------------------------------------------
// edit-response — store operator-edited response text
// ---------------------------------------------------------------------

export interface EditResponseInput {
  responseText: string;
  responseSubject?: string | null;
}

export interface EditResponseResponse {
  success: boolean;
  message?: string;
  statement?: StatementRow;
  error?: string;
}

export async function editStatementResponse(
  appDb: Knex,
  statementId: number,
  input: EditResponseInput,
): Promise<EditResponseResponse> {
  const stmt = await getStatement(appDb, statementId);
  if (!stmt) return { success: false, error: 'Statement not found' };
  await appDb('supplier_statements')
    .where({ id: statementId })
    .update({
      response_text: input.responseText,
      response_subject: input.responseSubject ?? null,
      updated_at: appDb.fn.now(),
    });
  const updated = await getStatement(appDb, statementId);
  return {
    success: true,
    message: 'Response text updated',
    statement: updated ?? undefined,
  };
}

// ---------------------------------------------------------------------
// bulk-approve — fan-out approve over a list of statement ids
// ---------------------------------------------------------------------

export interface BulkApproveInput {
  statementIds: number[];
  approvedBy: string;
}

export interface BulkApproveResponse {
  success: boolean;
  approved: number;
  failed: number;
  results: Array<{
    statement_id: number;
    success: boolean;
    error?: string;
  }>;
}

export async function bulkApproveStatements(
  appDb: Knex,
  email: EmailSender,
  supplierLookup: OperaSupplierLookup,
  input: BulkApproveInput,
): Promise<BulkApproveResponse> {
  const results: BulkApproveResponse['results'] = [];
  let approved = 0;
  let failed = 0;
  for (const id of input.statementIds) {
    const r = await approveStatement(appDb, email, supplierLookup, id, {
      approvedBy: input.approvedBy,
    });
    if (r.success) {
      approved += 1;
      results.push({ statement_id: id, success: true });
    } else {
      failed += 1;
      results.push({ statement_id: id, success: false, error: r.error });
    }
  }
  return { success: true, approved, failed, results };
}
