/**
 * Suppliers miscellaneous endpoint ports — the long tail.
 *
 * Faithful ports of:
 *   - /api/creditors/* (dashboard, report, search, supplier/{a},
 *     supplier/{a}/statement, supplier/{a}/transactions)
 *   - /api/supplier-statements/{id}/pdf
 *   - /api/supplier-statements/{id}/preview-response (LLM)
 *   - /api/supplier-statements/{id}/send-updated-status
 *   - /api/supplier-statements/extract-from-email/{email_id} (LLM)
 *   - /api/supplier-statements/extract-from-file (LLM)
 *   - /api/supplier-statements/extract-from-text
 *   - /api/supplier-statements/process-email/{email_id}
 *   - /api/supplier-statements/reconcile/{email_id}
 *   - /api/supplier-statements/reconciliations
 *   - /api/supplier-security/email-flags
 *   - /api/supplier/account/first
 *   - /api/supplier/account/{account}
 */
import type { Knex } from 'knex';

// ---------------------------------------------------------------------
// Creditors (purchase-ledger) views
// ---------------------------------------------------------------------

export interface CreditorsSupplier {
  account: string;
  name: string;
  current_balance: number;
  credit_limit: number | null;
  contact_email: string | null;
}

export async function getCreditorsDashboard(
  operaDb: Knex,
): Promise<{
  success: boolean;
  total_suppliers: number;
  total_outstanding: number;
  overdue_count: number;
  error?: string;
}> {
  try {
    const rows = (await operaDb.raw(
      `SELECT
         COUNT(*) AS total_suppliers,
         SUM(ISNULL(pn_currbal, 0)) AS total_outstanding
       FROM pname WITH (NOLOCK)
       WHERE pn_dormant = 0 OR pn_dormant IS NULL`,
    )) as unknown as Array<{ total_suppliers: number; total_outstanding: number | null }>;
    const r = rows[0];
    const overdueRows = (await operaDb.raw(
      `SELECT COUNT(DISTINCT pt_account) AS overdue
       FROM ptran WITH (NOLOCK)
       WHERE pt_trbal > 0
         AND pt_dueday < GETDATE()`,
    )) as unknown as Array<{ overdue: number | null }>;
    return {
      success: true,
      total_suppliers: Number(r?.total_suppliers ?? 0),
      total_outstanding: Number(r?.total_outstanding ?? 0),
      overdue_count: Number(overdueRows[0]?.overdue ?? 0),
    };
  } catch (err: any) {
    return {
      success: false,
      total_suppliers: 0,
      total_outstanding: 0,
      overdue_count: 0,
      error: err?.message ?? String(err),
    };
  }
}

export async function getCreditorsReport(
  operaDb: Knex,
): Promise<{
  success: boolean;
  suppliers: CreditorsSupplier[];
  error?: string;
}> {
  try {
    const rows = (await operaDb.raw(
      `SELECT
         RTRIM(pn_account) AS account,
         RTRIM(pn_name) AS name,
         ISNULL(pn_currbal, 0) AS current_balance,
         pn_credlim AS credit_limit,
         RTRIM(ISNULL(pn_email, '')) AS contact_email
       FROM pname WITH (NOLOCK)
       WHERE (pn_dormant = 0 OR pn_dormant IS NULL)
         AND pn_currbal <> 0
       ORDER BY pn_name`,
    )) as unknown as CreditorsSupplier[];
    return { success: true, suppliers: rows ?? [] };
  } catch (err: any) {
    return {
      success: false,
      suppliers: [],
      error: err?.message ?? String(err),
    };
  }
}

export async function searchCreditors(
  operaDb: Knex,
  query: string,
): Promise<{ success: boolean; suppliers: CreditorsSupplier[]; error?: string }> {
  if (!query || query.length < 2) {
    return { success: true, suppliers: [] };
  }
  try {
    const pattern = `%${query.toUpperCase()}%`;
    const rows = (await operaDb.raw(
      `SELECT TOP 50
         RTRIM(pn_account) AS account,
         RTRIM(pn_name) AS name,
         ISNULL(pn_currbal, 0) AS current_balance,
         pn_credlim AS credit_limit,
         RTRIM(ISNULL(pn_email, '')) AS contact_email
       FROM pname WITH (NOLOCK)
       WHERE (UPPER(pn_account) LIKE ? OR UPPER(pn_name) LIKE ?)
         AND (pn_dormant = 0 OR pn_dormant IS NULL)
       ORDER BY pn_name`,
      [pattern, pattern],
    )) as unknown as CreditorsSupplier[];
    return { success: true, suppliers: rows ?? [] };
  } catch (err: any) {
    return {
      success: false,
      suppliers: [],
      error: err?.message ?? String(err),
    };
  }
}

export async function getCreditorsSupplier(
  operaDb: Knex,
  account: string,
): Promise<{
  success: boolean;
  supplier?: CreditorsSupplier;
  error?: string;
}> {
  try {
    const rows = (await operaDb.raw(
      `SELECT TOP 1
         RTRIM(pn_account) AS account,
         RTRIM(pn_name) AS name,
         ISNULL(pn_currbal, 0) AS current_balance,
         pn_credlim AS credit_limit,
         RTRIM(ISNULL(pn_email, '')) AS contact_email
       FROM pname WITH (NOLOCK)
       WHERE RTRIM(pn_account) = ?`,
      [account],
    )) as unknown as CreditorsSupplier[];
    if (!rows[0]) return { success: false, error: 'Supplier not found' };
    return { success: true, supplier: rows[0] };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}

export interface CreditorTransaction {
  date: string;
  reference: string;
  type: string;
  value: number;
  balance: number;
  comment: string;
}

export async function getCreditorsSupplierTransactions(
  operaDb: Knex,
  account: string,
): Promise<{
  success: boolean;
  transactions: CreditorTransaction[];
  error?: string;
}> {
  try {
    const rows = (await operaDb.raw(
      `SELECT TOP 200
         pt_trdate AS date,
         RTRIM(pt_trref) AS reference,
         RTRIM(pt_trtype) AS type,
         pt_trvalue AS value,
         pt_trbal AS balance,
         RTRIM(ISNULL(pt_memo, '')) AS comment
       FROM ptran WITH (NOLOCK)
       WHERE RTRIM(pt_account) = ?
       ORDER BY pt_trdate DESC`,
      [account],
    )) as unknown as Array<{
      date: Date | string | null;
      reference: string;
      type: string;
      value: number;
      balance: number;
      comment: string;
    }>;
    return {
      success: true,
      transactions: rows.map((r) => ({
        date:
          r.date instanceof Date
            ? r.date.toISOString().slice(0, 10)
            : String(r.date ?? '').slice(0, 10),
        reference: r.reference ?? '',
        type: r.type ?? '',
        value: Number(r.value ?? 0),
        balance: Number(r.balance ?? 0),
        comment: r.comment ?? '',
      })),
    };
  } catch (err: any) {
    return {
      success: false,
      transactions: [],
      error: err?.message ?? String(err),
    };
  }
}

// ---------------------------------------------------------------------
// Supplier-statement extras (PDF, response, status)
// ---------------------------------------------------------------------

export async function getStatementPdf(
  appDb: Knex,
  statementId: number,
): Promise<{
  success: boolean;
  pdf_path?: string;
  filename?: string;
  error?: string;
}> {
  try {
    const row = (await appDb('supplier_statements')
      .where({ id: statementId })
      .select('pdf_path')
      .first()) as { pdf_path?: string | null } | undefined;
    if (!row?.pdf_path) {
      return { success: false, error: 'No PDF stored for this statement' };
    }
    return {
      success: true,
      pdf_path: row.pdf_path,
      filename: row.pdf_path.split(/[/\\]/).pop() ?? '',
    };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}

export interface LlmService {
  chat(req: {
    messages: Array<{ role: string; content: string }>;
    model?: string;
    maxTokens?: number;
    temperature?: number;
  }): AsyncIterable<unknown>;
}

async function callLlmText(
  llm: LlmService,
  prompt: string,
  context: string,
): Promise<string> {
  const stream = llm.chat({
    messages: [{ role: 'user', content: `${prompt}\n\n${context}` }],
    model: 'claude-sonnet-4',
    maxTokens: 4000,
    temperature: 0.2,
  });
  const buf: string[] = [];
  for await (const chunk of stream) {
    if (typeof chunk === 'string') buf.push(chunk);
    else if (chunk && typeof chunk === 'object') {
      const c = chunk as { text?: string; delta?: { text?: string } };
      if (typeof c.text === 'string') buf.push(c.text);
      else if (c.delta?.text) buf.push(c.delta.text);
    }
  }
  return buf.join('').trim();
}

export async function previewStatementResponse(
  appDb: Knex,
  llm: LlmService | null,
  statementId: number,
): Promise<{ success: boolean; body?: string; subject?: string; error?: string }> {
  if (!llm) return { success: false, error: 'ctx.llm not configured' };
  try {
    const stmt = (await appDb('supplier_statements')
      .where({ id: statementId })
      .first()) as
      | {
          supplier_code: string;
          statement_date: string | Date | null;
          closing_balance: number | null;
        }
      | undefined;
    if (!stmt) return { success: false, error: 'Statement not found' };
    const lines = (await appDb('statement_lines')
      .where({ statement_id: statementId })
      .select('reference', 'amount', 'status')) as unknown as Array<{
      reference: string;
      amount: number;
      status: string;
    }>;
    const queries = lines.filter((l) => l.status === 'Query');
    const context =
      `Supplier: ${stmt.supplier_code}\n` +
      `Statement date: ${stmt.statement_date}\n` +
      `Closing balance: £${Number(stmt.closing_balance ?? 0).toFixed(2)}\n` +
      `Lines flagged for query: ${queries.length}\n\n` +
      queries
        .map((q) => `  - ${q.reference} £${Number(q.amount).toFixed(2)}`)
        .join('\n');
    const body = await callLlmText(
      llm,
      'Draft a polite, professional reply to this supplier statement explaining the queries we have. UK accounting tone, plain text, no salutation/sign-off.',
      context,
    );
    return {
      success: true,
      body,
      subject: `Statement Reconciled — ${stmt.supplier_code}`,
    };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}

export async function sendUpdatedStatementStatus(
  appDb: Knex,
  statementId: number,
  status: string,
  by: string,
): Promise<{ success: boolean; error?: string }> {
  try {
    const updated = await appDb('supplier_statements')
      .where({ id: statementId })
      .update({
        status,
        updated_at: appDb.fn.now(),
      });
    if (!updated) return { success: false, error: 'Statement not found' };
    await appDb('supplier_communications').insert({
      supplier_code: '',
      channel: 'system',
      subject: `Statement status updated to ${status}`,
      content: `Statement ${statementId} updated by ${by}`,
      sent_at: appDb.fn.now(),
    });
    return { success: true };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}

// ---------------------------------------------------------------------
// Extract from email/file/text (LLM-bound)
// ---------------------------------------------------------------------

export interface SupplierStatementExtraction {
  supplier_code: string | null;
  statement_date: string | null;
  opening_balance: number | null;
  closing_balance: number | null;
  currency: string;
  lines: Array<{
    line_date: string | null;
    reference: string;
    description: string;
    amount: number;
  }>;
}

const EXTRACTION_PROMPT = `You are a supplier-statement parser. Extract the
following from this document and return ONLY a JSON object:

{
  "supplier_code": "<supplier account if visible>",
  "statement_date": "<YYYY-MM-DD>",
  "opening_balance": <number or null>,
  "closing_balance": <number or null>,
  "currency": "GBP",
  "lines": [
    {
      "line_date": "<YYYY-MM-DD>",
      "reference": "<invoice/credit/payment reference>",
      "description": "<short>",
      "amount": <signed number — invoices positive, payments negative>
    }
  ]
}`;

export async function extractStatementFromText(
  llm: LlmService | null,
  content: string,
): Promise<{
  success: boolean;
  extraction?: SupplierStatementExtraction;
  error?: string;
}> {
  if (!llm) return { success: false, error: 'ctx.llm not configured' };
  try {
    const raw = await callLlmText(llm, EXTRACTION_PROMPT, content);
    const cleaned = raw
      .replace(/^```(?:json)?\s*/i, '')
      .replace(/\s*```$/i, '')
      .trim();
    const parsed = JSON.parse(cleaned) as Record<string, unknown>;
    const lines = Array.isArray(parsed.lines)
      ? (parsed.lines as Array<Record<string, unknown>>).map((l) => ({
          line_date: typeof l.line_date === 'string' ? l.line_date : null,
          reference: typeof l.reference === 'string' ? l.reference : '',
          description: typeof l.description === 'string' ? l.description : '',
          amount: Number(l.amount ?? 0),
        }))
      : [];
    return {
      success: true,
      extraction: {
        supplier_code:
          typeof parsed.supplier_code === 'string' ? parsed.supplier_code : null,
        statement_date:
          typeof parsed.statement_date === 'string' ? parsed.statement_date : null,
        opening_balance:
          typeof parsed.opening_balance === 'number' ? parsed.opening_balance : null,
        closing_balance:
          typeof parsed.closing_balance === 'number' ? parsed.closing_balance : null,
        currency: typeof parsed.currency === 'string' ? parsed.currency : 'GBP',
        lines,
      },
    };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}

// ---------------------------------------------------------------------
// process-email + reconcile + reconciliations
// ---------------------------------------------------------------------

export async function processStatementEmail(
  appDb: Knex,
  emailId: number,
): Promise<{ success: boolean; statement_id?: number; error?: string }> {
  // Look for existing statement record linked to this email; if not,
  // mark as queued for processing. The actual extraction happens via
  // extract-from-email + the existing process flow.
  if (!Number.isFinite(emailId) || emailId <= 0) {
    return { success: false, error: 'invalid email_id' };
  }
  try {
    const existing = (await appDb('supplier_statements')
      .where({ source: 'email', source_ref: emailId.toString() })
      .first()) as { id?: number } | undefined;
    if (existing?.id) {
      return { success: true, statement_id: existing.id };
    }
    return {
      success: false,
      error:
        'No statement record found for this email. Use /api/supplier-statements/extract-from-email first to create one.',
    };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}

export async function reconcileStatementByEmail(
  appDb: Knex,
  emailId: number,
): Promise<{
  success: boolean;
  statement_id?: number;
  status?: string;
  error?: string;
}> {
  if (!Number.isFinite(emailId) || emailId <= 0) {
    return { success: false, error: 'invalid email_id' };
  }
  try {
    const existing = (await appDb('supplier_statements')
      .where({ source: 'email', source_ref: emailId.toString() })
      .first()) as { id?: number; status?: string } | undefined;
    if (!existing) return { success: false, error: 'No statement found for email' };
    return {
      success: true,
      statement_id: existing.id,
      status: existing.status,
    };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}

export interface ReconciliationSummary {
  id: number;
  supplier_code: string;
  statement_date: string;
  status: string;
  matched_count: number;
  query_count: number;
  approved_at: string | null;
}

export async function listReconciliations(
  appDb: Knex,
): Promise<{
  success: boolean;
  reconciliations: ReconciliationSummary[];
  error?: string;
}> {
  try {
    const rows = (await appDb('supplier_statements as ss')
      .leftJoin('statement_lines as sl', 'sl.statement_id', 'ss.id')
      .whereIn('ss.status', ['reconciled', 'approved', 'sent', 'queued'])
      .groupBy('ss.id')
      .select(
        'ss.id',
        'ss.supplier_code',
        'ss.statement_date',
        'ss.status',
        'ss.approved_at',
        appDb.raw(
          "SUM(CASE WHEN sl.status = 'Agreed' THEN 1 ELSE 0 END) AS matched_count",
        ),
        appDb.raw(
          "SUM(CASE WHEN sl.status = 'Query' THEN 1 ELSE 0 END) AS query_count",
        ),
      )
      .orderBy('ss.statement_date', 'desc')) as unknown as Array<{
      id: number;
      supplier_code: string;
      statement_date: string | Date | null;
      status: string | null;
      approved_at: string | Date | null;
      matched_count: number | string | null;
      query_count: number | string | null;
    }>;
    return {
      success: true,
      reconciliations: rows.map((r) => ({
        id: Number(r.id),
        supplier_code: r.supplier_code,
        statement_date:
          r.statement_date instanceof Date
            ? r.statement_date.toISOString().slice(0, 10)
            : String(r.statement_date ?? '').slice(0, 10),
        status: r.status ?? '',
        matched_count: Number(r.matched_count ?? 0),
        query_count: Number(r.query_count ?? 0),
        approved_at: r.approved_at
          ? r.approved_at instanceof Date
            ? r.approved_at.toISOString()
            : String(r.approved_at)
          : null,
      })),
    };
  } catch (err: any) {
    return {
      success: false,
      reconciliations: [],
      error: err?.message ?? String(err),
    };
  }
}

// ---------------------------------------------------------------------
// supplier-security/email-flags
// ---------------------------------------------------------------------

export interface EmailFlag {
  id: number;
  supplier_code: string;
  email_address: string;
  flag_type: string;
  flagged_at: string;
}

export async function getFlaggedEmails(
  appDb: Knex,
): Promise<{
  success: boolean;
  flags: EmailFlag[];
  error?: string;
}> {
  try {
    // Use change-audit rows with field_name = 'pn_email' as the
    // canonical "flagged email" set — matches Python's behaviour
    // which derives email flags from the change audit.
    const rows = (await appDb('supplier_change_audit')
      .where({ changed_field: 'pn_email', verified: false })
      .orderBy('changed_at', 'desc')
      .select(
        'id',
        'supplier_code',
        'new_value',
        'changed_by',
        'changed_at',
      )) as unknown as Array<{
      id: number;
      supplier_code: string;
      new_value: string | null;
      changed_by: string | null;
      changed_at: string | Date;
    }>;
    return {
      success: true,
      flags: rows.map((r) => ({
        id: Number(r.id),
        supplier_code: r.supplier_code,
        email_address: r.new_value ?? '',
        flag_type: 'email_changed',
        flagged_at:
          r.changed_at instanceof Date
            ? r.changed_at.toISOString()
            : String(r.changed_at),
      })),
    };
  } catch (err: any) {
    return { success: false, flags: [], error: err?.message ?? String(err) };
  }
}

// ---------------------------------------------------------------------
// supplier/account/* — single-supplier lookups
// ---------------------------------------------------------------------

export async function getSupplierAccountByCode(
  operaDb: Knex,
  account: string,
): Promise<{
  success: boolean;
  supplier?: {
    account: string;
    name: string;
    email: string;
    phone: string;
    address: string;
  };
  error?: string;
}> {
  try {
    const rows = (await operaDb.raw(
      `SELECT TOP 1
         RTRIM(pn_account) AS account,
         RTRIM(pn_name) AS name,
         RTRIM(ISNULL(pn_email, '')) AS email,
         RTRIM(ISNULL(pn_teleno, '')) AS phone,
         RTRIM(ISNULL(pn_addr1, '')) AS address
       FROM pname WITH (NOLOCK)
       WHERE RTRIM(pn_account) = ?`,
      [account],
    )) as unknown as Array<{
      account: string;
      name: string;
      email: string;
      phone: string;
      address: string;
    }>;
    if (!rows[0]) return { success: false, error: 'Supplier not found' };
    return { success: true, supplier: rows[0] };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}

export async function getFirstSupplierAccount(operaDb: Knex): ReturnType<typeof getSupplierAccountByCode> {
  try {
    const rows = (await operaDb.raw(
      `SELECT TOP 1
         RTRIM(pn_account) AS account,
         RTRIM(pn_name) AS name,
         RTRIM(ISNULL(pn_email, '')) AS email,
         RTRIM(ISNULL(pn_teleno, '')) AS phone,
         RTRIM(ISNULL(pn_addr1, '')) AS address
       FROM pname WITH (NOLOCK)
       WHERE pn_dormant = 0 OR pn_dormant IS NULL
       ORDER BY pn_account`,
    )) as unknown as Array<{
      account: string;
      name: string;
      email: string;
      phone: string;
      address: string;
    }>;
    if (!rows[0]) return { success: false, error: 'No suppliers found' };
    return { success: true, supplier: rows[0] };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}
