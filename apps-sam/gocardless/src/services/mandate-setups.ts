/**
 * Mandate setup-request operations.
 *
 * Faithful ports of:
 *   - list_pending_mandate_setups (apps/gocardless/api/routes.py:7054-7067)
 *   - cancel_mandate_setup        (routes.py:7220-7244)
 *
 * (The check-setups poll endpoint is a separate larger port — depends
 * on `client.get_billing_request` + `client.get_mandate` + auto-link
 * logic. This service is the read + cancel half.)
 *
 * Stored in `mandate_setup_requests` (per-app DB).
 */
import type { Knex } from 'knex';

const FINAL_STATUSES = new Set(['completed', 'failed', 'cancelled']);

export interface MandateSetup {
  id: number;
  opera_account: string;
  opera_name: string;
  customer_email: string;
  billing_request_id: string;
  billing_request_flow_id: string;
  authorisation_url: string;
  mandate_id: string;
  gocardless_customer_id: string;
  status: string;
  status_detail: string;
  email_sent_at: string | null;
  mandate_active_at: string | null;
  created_at: string;
  updated_at: string;
}

function dateToIso(d: Date | string | null): string | null {
  if (!d) return null;
  if (d instanceof Date) {
    if (Number.isNaN(d.getTime())) return null;
    return d.toISOString();
  }
  return String(d);
}

interface SetupRow {
  id: number;
  opera_account: string | null;
  opera_name: string | null;
  customer_email: string | null;
  billing_request_id: string | null;
  billing_request_flow_id: string | null;
  authorisation_url: string | null;
  mandate_id: string | null;
  gocardless_customer_id: string | null;
  status: string | null;
  status_detail: string | null;
  email_sent_at: Date | string | null;
  mandate_active_at: Date | string | null;
  created_at: Date | string | null;
  updated_at: Date | string | null;
}

function rowToSetup(r: SetupRow): MandateSetup {
  return {
    id: r.id,
    opera_account: r.opera_account ?? '',
    opera_name: r.opera_name ?? '',
    customer_email: r.customer_email ?? '',
    billing_request_id: r.billing_request_id ?? '',
    billing_request_flow_id: r.billing_request_flow_id ?? '',
    authorisation_url: r.authorisation_url ?? '',
    mandate_id: r.mandate_id ?? '',
    gocardless_customer_id: r.gocardless_customer_id ?? '',
    status: r.status ?? 'pending',
    status_detail: r.status_detail ?? '',
    email_sent_at: dateToIso(r.email_sent_at),
    mandate_active_at: dateToIso(r.mandate_active_at),
    created_at: dateToIso(r.created_at) ?? '',
    updated_at: dateToIso(r.updated_at) ?? '',
  };
}

// ---------------------------------------------------------------------
// list pending setups
// ---------------------------------------------------------------------

export interface ListMandateSetupsResponse {
  success: boolean;
  setups: MandateSetup[];
  pending_count: number;
  error?: string;
}

export async function listMandateSetups(
  appDb: Knex,
): Promise<ListMandateSetupsResponse> {
  try {
    const rows = (await appDb('mandate_setup_requests').orderBy(
      'id',
      'desc',
    )) as unknown as SetupRow[];
    const setups = rows.map(rowToSetup);
    const pendingCount = setups.filter(
      (s) => !FINAL_STATUSES.has(s.status),
    ).length;
    return { success: true, setups, pending_count: pendingCount };
  } catch (err: any) {
    return {
      success: false,
      setups: [],
      pending_count: 0,
      error: err?.message ?? String(err),
    };
  }
}

// ---------------------------------------------------------------------
// create a setup — orchestrate billing-request + email
// ---------------------------------------------------------------------

export interface CreateMandateSetupInput {
  operaAccount: string;
  operaName?: string | null;
  customerEmail: string;
  emailSubject?: string | null;
  emailBodyHtml?: string | null;
  /** Falls back to 'Our Company' (matches Python). */
  companyName?: string | null;
}

export interface CreateMandateSetupResponse {
  success: boolean;
  message?: string;
  setup?: MandateSetup;
  email_sent?: boolean;
  email_error?: string | null;
  authorisation_url?: string;
  error?: string;
}

export interface CreateMandateSetupRemote {
  /**
   * Create a billing request + return its id (and a customer link if any).
   */
  createBillingRequest: (opts: {
    customerEmail: string;
    customerName: string | null;
    metadata: Record<string, string>;
  }) => Promise<{ success: boolean; id?: string; error?: string }>;
  /**
   * Create a flow → returns the hosted authorisation URL + flow id.
   */
  createBillingRequestFlow: (
    billingRequestId: string,
  ) => Promise<{
    success: boolean;
    flowId?: string;
    authorisationUrl?: string;
    error?: string;
  }>;
}

export interface CreateMandateSetupEmailSender {
  (opts: {
    to: string;
    subject: string;
    bodyHtml: string;
  }): Promise<{ success: boolean; error?: string | null }>;
}

const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

function defaultEmailSubject(companyName: string): string {
  return `Set Up Your Direct Debit — ${companyName}`;
}

function defaultEmailBody(
  customerName: string,
  authUrl: string,
  companyName: string,
): string {
  // HTML matches Python's exactly — same heading, button styling, footer.
  return `
    <html>
    <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
      <h2 style="color: #333;">Direct Debit Setup</h2>
      <p>Dear ${customerName || 'Customer'},</p>
      <p>We would like to invite you to set up a Direct Debit with us for convenient automated payment processing.</p>
      <p>Please click the button below to securely set up your Direct Debit mandate through GoCardless:</p>
      <p style="text-align: center; margin: 30px 0;">
        <a href="${authUrl}"
           style="display: inline-block; padding: 14px 28px; background-color: #1a73e8; color: white;
                  text-decoration: none; border-radius: 6px; font-weight: bold; font-size: 16px;">
          Set Up Direct Debit
        </a>
      </p>
      <p>This process is quick, secure, and protected by the <a href="https://www.directdebit.co.uk/direct-debit-guarantee/">Direct Debit Guarantee</a>.</p>
      <p>If you have any questions, please don't hesitate to contact us.</p>
      <p>Kind regards,<br>${companyName}</p>
      <hr style="border: none; border-top: 1px solid #eee; margin-top: 30px;">
      <p style="font-size: 11px; color: #999;">
        If the button above doesn't work, copy and paste this link into your browser:<br>
        <a href="${authUrl}" style="color: #999;">${authUrl}</a>
      </p>
    </body>
    </html>
  `;
}

/**
 * Faithful port of create_mandate_setup
 * (apps/gocardless/api/routes.py:6852-7051). Pipeline:
 *   1. Validate inputs (account, email).
 *   2. Create billing request via remote callback.
 *   3. Create billing-request-flow → authorisation URL.
 *   4. Insert a tracking row (status='pending').
 *   5. Best-effort email dispatch via injected sender. On success
 *      mark status='email_sent' + email_sent_at; on failure leave
 *      status='pending' with status_detail.
 */
export async function createMandateSetup(
  appDb: Knex,
  input: CreateMandateSetupInput,
  remote: CreateMandateSetupRemote,
  sendEmail?: CreateMandateSetupEmailSender,
): Promise<CreateMandateSetupResponse> {
  const operaAccount = (input.operaAccount ?? '').trim();
  const operaName = (input.operaName ?? '').trim();
  const customerEmail = (input.customerEmail ?? '').trim();
  const companyName = (input.companyName ?? '').trim() || 'Our Company';

  if (!operaAccount) {
    return { success: false, error: 'Opera customer account is required' };
  }
  if (!customerEmail) {
    return { success: false, error: 'Customer email address is required' };
  }
  if (!EMAIL_REGEX.test(customerEmail)) {
    return { success: false, error: 'Invalid email address format' };
  }

  // 1. Billing request
  const metadata: Record<string, string> = { opera_account: operaAccount };
  if (operaName) metadata.opera_name = operaName.slice(0, 50);
  const brq = await remote.createBillingRequest({
    customerEmail,
    customerName: operaName || null,
    metadata,
  });
  if (!brq.success || !brq.id) {
    return {
      success: false,
      error: brq.error ?? 'Failed to create billing request in GoCardless',
    };
  }

  // 2. Flow → auth URL
  const flow = await remote.createBillingRequestFlow(brq.id);
  if (!flow.success || !flow.authorisationUrl) {
    return {
      success: false,
      error:
        flow.error ?? 'Failed to generate authorisation URL from GoCardless',
    };
  }

  // 3. Persist tracking row
  let setupId: number;
  try {
    const ids = await appDb('mandate_setup_requests')
      .insert({
        opera_account: operaAccount,
        opera_name: operaName || null,
        customer_email: customerEmail,
        billing_request_id: brq.id,
        billing_request_flow_id: flow.flowId ?? null,
        authorisation_url: flow.authorisationUrl,
        status: 'pending',
      })
      .returning('id');
    const inserted = Array.isArray(ids) && ids.length > 0 ? ids[0] : null;
    setupId =
      typeof inserted === 'number'
        ? inserted
        : typeof (inserted as any)?.id === 'number'
          ? (inserted as any).id
          : 0;
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }

  // 4. Email dispatch (best-effort)
  let emailSent = false;
  let emailError: string | null = null;
  if (sendEmail) {
    try {
      const subject =
        (input.emailSubject ?? '').trim() || defaultEmailSubject(companyName);
      const bodyTemplate =
        (input.emailBodyHtml ?? '').trim() ||
        defaultEmailBody(operaName, flow.authorisationUrl, companyName);
      const body = bodyTemplate.includes('{authorisation_url}')
        ? bodyTemplate.replaceAll('{authorisation_url}', flow.authorisationUrl)
        : bodyTemplate;
      const sendResult = await sendEmail({
        to: customerEmail,
        subject,
        bodyHtml: body,
      });
      if (sendResult.success) {
        emailSent = true;
      } else {
        emailError = sendResult.error ?? 'Email send failed';
      }
    } catch (err: any) {
      emailError = err?.message ?? String(err);
    }
  } else {
    emailError = 'No email sender configured';
  }

  // 5. Update setup status based on email outcome
  try {
    if (emailSent) {
      await appDb('mandate_setup_requests')
        .where({ id: setupId })
        .update({
          status: 'email_sent',
          email_sent_at: appDb.fn.now(),
          updated_at: appDb.fn.now(),
        });
    } else {
      await appDb('mandate_setup_requests')
        .where({ id: setupId })
        .update({
          status: 'pending',
          status_detail: `Email not sent: ${emailError}`,
          updated_at: appDb.fn.now(),
        });
    }
  } catch {
    // best-effort
  }

  // 6. Return enriched setup record
  const fresh = (await appDb('mandate_setup_requests')
    .where({ id: setupId })
    .first()) as unknown as SetupRow | undefined;

  return {
    success: true,
    message: `Mandate setup initiated for ${operaName || operaAccount}`,
    setup: fresh ? rowToSetup(fresh) : undefined,
    email_sent: emailSent,
    email_error: emailError,
    authorisation_url: flow.authorisationUrl,
  };
}

// ---------------------------------------------------------------------
// cancel a setup
// ---------------------------------------------------------------------

export interface CancelSetupResponse {
  success: boolean;
  message?: string;
  error?: string;
}

export async function cancelMandateSetup(
  appDb: Knex,
  setupId: number,
): Promise<CancelSetupResponse> {
  if (!Number.isFinite(setupId) || setupId <= 0) {
    return { success: false, error: 'setup_id must be a positive number' };
  }
  try {
    const row = (await appDb('mandate_setup_requests')
      .where({ id: setupId })
      .first()) as
      | { id: number; status: string | null; opera_account: string | null; opera_name: string | null }
      | undefined;
    if (!row) {
      return { success: false, error: 'Setup request not found' };
    }
    const status = (row.status ?? '').trim();
    if (FINAL_STATUSES.has(status)) {
      return {
        success: false,
        error: `Cannot cancel — setup is already ${status}`,
      };
    }

    await appDb('mandate_setup_requests').where({ id: setupId }).update({
      status: 'cancelled',
      status_detail: 'Cancelled by user',
      updated_at: appDb.fn.now(),
    });

    const display = (row.opera_name ?? '').trim() || (row.opera_account ?? '').trim();
    return {
      success: true,
      message: `Mandate setup for ${display} cancelled`,
    };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}
