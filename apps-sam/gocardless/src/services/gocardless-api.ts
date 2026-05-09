/**
 * Minimal GoCardless REST API client.
 *
 * Faithful port of the bits of `sql_rag/gocardless_api.py` we need
 * for test-api. Other client methods (get_payouts, create_payment,
 * etc.) ported as needed in subsequent sessions.
 *
 * Uses native fetch (Node 18+) to avoid the axios dependency in the
 * Python version.
 */

const SANDBOX_URL = 'https://api-sandbox.gocardless.com';
const LIVE_URL = 'https://api.gocardless.com';

export interface GoCardlessClientOptions {
  accessToken: string;
  sandbox?: boolean;
}

export interface TestConnectionResult {
  success: boolean;
  message?: string;
  organisation?: string;
  environment?: 'sandbox' | 'live';
  error?: string;
}

export class GoCardlessClient {
  private accessToken: string;
  private baseUrl: string;
  private environment: 'sandbox' | 'live';

  constructor(opts: GoCardlessClientOptions) {
    this.accessToken = opts.accessToken;
    this.environment = opts.sandbox ? 'sandbox' : 'live';
    this.baseUrl = opts.sandbox ? SANDBOX_URL : LIVE_URL;
  }

  private async request(method: string, path: string, body?: unknown): Promise<Response> {
    return fetch(`${this.baseUrl}${path}`, {
      method,
      headers: {
        Authorization: `Bearer ${this.accessToken}`,
        'GoCardless-Version': '2015-07-06',
        Accept: 'application/json',
        'Content-Type': 'application/json',
      },
      body: body ? JSON.stringify(body) : undefined,
    });
  }

  /**
   * GET /payouts — list payouts with optional filters.
   *
   * Faithful port of the get_payouts call in
   * sql_rag/gocardless_api.py. Returns the raw payouts array plus
   * the cursor `before` from GoCardless's pagination metadata.
   *
   * NB: this returns raw GoCardless objects. The matching/import
   * pipeline that joins payouts → payments → mandates → customers
   * lives elsewhere (and isn't ported in this session).
   */
  async getPayouts(opts: {
    status?: string;
    limit?: number;
    createdAtGte?: string; // ISO date 'YYYY-MM-DD'
    before?: string;
  } = {}): Promise<{
    success: boolean;
    payouts: Array<Record<string, unknown>>;
    before: string | null;
    error?: string;
  }> {
    try {
      const params = new URLSearchParams();
      if (opts.status) params.set('status', opts.status);
      if (opts.limit) params.set('limit', String(opts.limit));
      if (opts.createdAtGte) params.set('created_at[gte]', opts.createdAtGte);
      if (opts.before) params.set('before', opts.before);
      const path = `/payouts${params.toString() ? `?${params.toString()}` : ''}`;

      const res = await this.request('GET', path);
      if (res.status === 401) {
        return {
          success: false,
          payouts: [],
          before: null,
          error: 'Invalid GoCardless API token (401 Unauthorized)',
        };
      }
      if (!res.ok) {
        const text = await res.text().catch(() => '');
        return {
          success: false,
          payouts: [],
          before: null,
          error: `GoCardless API returned ${res.status}: ${text.slice(0, 200)}`,
        };
      }
      const data = (await res.json()) as {
        payouts?: Array<Record<string, unknown>>;
        meta?: { cursors?: { before?: string | null } };
      };
      const payouts = Array.isArray(data.payouts) ? data.payouts : [];
      const before = data.meta?.cursors?.before ?? null;
      return { success: true, payouts, before };
    } catch (err: any) {
      return {
        success: false,
        payouts: [],
        before: null,
        error: `Network error: ${err?.message ?? String(err)}`,
      };
    }
  }

  /**
   * POST /payments/:id/actions/cancel — cancel a pending payment.
   *
   * Faithful port of the cancel_payment call used by
   * cancel_payment_request (apps/gocardless/api/routes.py:8509-8553).
   * Returns a uniform shape rather than throwing so callers can fall
   * back to local-only cancellation if the API call fails (matches
   * Python's "log and continue" behaviour).
   */
  async cancelPayment(
    paymentId: string,
  ): Promise<{ success: boolean; data?: Record<string, unknown>; error?: string }> {
    if (!paymentId) return { success: false, error: 'paymentId required' };
    try {
      const res = await this.request(
        'POST',
        `/payments/${encodeURIComponent(paymentId)}/actions/cancel`,
        {},
      );
      if (res.status === 401) {
        return {
          success: false,
          error: 'Invalid GoCardless API token (401 Unauthorized)',
        };
      }
      if (!res.ok) {
        const text = await res.text().catch(() => '');
        return {
          success: false,
          error: `GoCardless API returned ${res.status}: ${text.slice(0, 200)}`,
        };
      }
      const data = (await res.json()) as Record<string, unknown>;
      return { success: true, data };
    } catch (err: any) {
      return {
        success: false,
        error: `Network error: ${err?.message ?? String(err)}`,
      };
    }
  }

  /**
   * GET /payments/:id — fetch a single payment's current status.
   *
   * Used by payment-requests/sync to reconcile local state with
   * GoCardless's view of each pending payment.
   */
  async getPayment(
    paymentId: string,
  ): Promise<{
    success: boolean;
    payment?: {
      id?: string;
      status?: string;
      charge_date?: string;
      amount?: number;
      [k: string]: unknown;
    };
    error?: string;
  }> {
    if (!paymentId) return { success: false, error: 'paymentId required' };
    try {
      const res = await this.request(
        'GET',
        `/payments/${encodeURIComponent(paymentId)}`,
      );
      if (res.status === 401) {
        return {
          success: false,
          error: 'Invalid GoCardless API token (401 Unauthorized)',
        };
      }
      if (!res.ok) {
        const text = await res.text().catch(() => '');
        return {
          success: false,
          error: `GoCardless API returned ${res.status}: ${text.slice(0, 200)}`,
        };
      }
      const data = (await res.json()) as { payments?: Record<string, unknown> };
      return { success: true, payment: data.payments ?? {} };
    } catch (err: any) {
      return {
        success: false,
        error: `Network error: ${err?.message ?? String(err)}`,
      };
    }
  }

  /**
   * GET /billing_requests/:id — fetch a billing request's current state.
   *
   * Used by the check-setups poll endpoint to detect when a customer
   * has completed their authorisation and a mandate has been minted.
   */
  async getBillingRequest(
    billingRequestId: string,
  ): Promise<{
    success: boolean;
    billingRequest?: Record<string, unknown>;
    error?: string;
  }> {
    if (!billingRequestId) {
      return { success: false, error: 'billingRequestId required' };
    }
    try {
      const res = await this.request(
        'GET',
        `/billing_requests/${encodeURIComponent(billingRequestId)}`,
      );
      if (res.status === 401) {
        return {
          success: false,
          error: 'Invalid GoCardless API token (401 Unauthorized)',
        };
      }
      if (!res.ok) {
        const text = await res.text().catch(() => '');
        return {
          success: false,
          error: `GoCardless API returned ${res.status}: ${text.slice(0, 200)}`,
        };
      }
      const data = (await res.json()) as {
        billing_requests?: Record<string, unknown>;
      };
      return { success: true, billingRequest: data.billing_requests ?? {} };
    } catch (err: any) {
      return {
        success: false,
        error: `Network error: ${err?.message ?? String(err)}`,
      };
    }
  }

  /**
   * POST /billing_requests — create a new billing request.
   *
   * Faithful port of GoCardlessClient.create_billing_request — used
   * by the mandate-setup flow to generate a hosted authorisation
   * URL the customer can use to sign the Direct Debit.
   */
  async createBillingRequest(opts: {
    customerEmail: string;
    customerName?: string | null;
    description?: string | null;
    metadata?: Record<string, string> | null;
  }): Promise<{
    success: boolean;
    billingRequest?: Record<string, unknown>;
    error?: string;
  }> {
    if (!opts.customerEmail) {
      return { success: false, error: 'customerEmail required' };
    }
    const customer: Record<string, unknown> = { email: opts.customerEmail };
    if (opts.customerName) customer.given_name = opts.customerName;
    const body: Record<string, unknown> = {
      billing_requests: {
        mandate_request: { scheme: 'bacs' },
        links: {},
        resources: { customer },
      },
    };
    if (opts.metadata) {
      (body.billing_requests as any).metadata = opts.metadata;
    }
    try {
      const res = await this.request('POST', '/billing_requests', body);
      if (res.status === 401) {
        return {
          success: false,
          error: 'Invalid GoCardless API token (401 Unauthorized)',
        };
      }
      if (!res.ok) {
        const text = await res.text().catch(() => '');
        return {
          success: false,
          error: `GoCardless API returned ${res.status}: ${text.slice(0, 200)}`,
        };
      }
      const data = (await res.json()) as {
        billing_requests?: Record<string, unknown>;
      };
      return { success: true, billingRequest: data.billing_requests ?? {} };
    } catch (err: any) {
      return {
        success: false,
        error: `Network error: ${err?.message ?? String(err)}`,
      };
    }
  }

  /**
   * POST /billing_request_flows — create the hosted-payment-pages flow
   * URL for an existing billing request. Returns `{authorisation_url}`.
   */
  async createBillingRequestFlow(opts: {
    billingRequestId: string;
    redirectUri?: string | null;
    exitUri?: string | null;
  }): Promise<{
    success: boolean;
    flow?: Record<string, unknown>;
    error?: string;
  }> {
    if (!opts.billingRequestId) {
      return { success: false, error: 'billingRequestId required' };
    }
    const flow: Record<string, unknown> = {
      links: { billing_request: opts.billingRequestId },
    };
    if (opts.redirectUri) flow.redirect_uri = opts.redirectUri;
    if (opts.exitUri) flow.exit_uri = opts.exitUri;
    try {
      const res = await this.request(
        'POST',
        '/billing_request_flows',
        { billing_request_flows: flow },
      );
      if (res.status === 401) {
        return {
          success: false,
          error: 'Invalid GoCardless API token (401 Unauthorized)',
        };
      }
      if (!res.ok) {
        const text = await res.text().catch(() => '');
        return {
          success: false,
          error: `GoCardless API returned ${res.status}: ${text.slice(0, 200)}`,
        };
      }
      const data = (await res.json()) as {
        billing_request_flows?: Record<string, unknown>;
      };
      return { success: true, flow: data.billing_request_flows ?? {} };
    } catch (err: any) {
      return {
        success: false,
        error: `Network error: ${err?.message ?? String(err)}`,
      };
    }
  }

  /**
   * GET /mandates — list mandates.
   *
   * Faithful port of GoCardlessClient.list_mandates. Returns a page
   * of raw mandate objects + the next-page cursor. Used by the
   * mandate-sync flow to walk every mandate in the GoCardless org.
   */
  async listMandates(opts: {
    customerId?: string;
    status?: string;
    limit?: number;
    cursor?: string;
  } = {}): Promise<{
    success: boolean;
    mandates: Array<Record<string, unknown>>;
    after: string | null;
    error?: string;
  }> {
    try {
      const params = new URLSearchParams();
      if (opts.customerId) params.set('customer', opts.customerId);
      if (opts.status) params.set('status', opts.status);
      params.set('limit', String(Math.min(opts.limit ?? 100, 500)));
      if (opts.cursor) params.set('after', opts.cursor);
      const path = `/mandates?${params.toString()}`;
      const res = await this.request('GET', path);
      if (res.status === 401) {
        return {
          success: false,
          mandates: [],
          after: null,
          error: 'Invalid GoCardless API token (401 Unauthorized)',
        };
      }
      if (!res.ok) {
        const text = await res.text().catch(() => '');
        return {
          success: false,
          mandates: [],
          after: null,
          error: `GoCardless API returned ${res.status}: ${text.slice(0, 200)}`,
        };
      }
      const data = (await res.json()) as {
        mandates?: Array<Record<string, unknown>>;
        meta?: { cursors?: { after?: string | null } };
      };
      return {
        success: true,
        mandates: Array.isArray(data.mandates) ? data.mandates : [],
        after: data.meta?.cursors?.after ?? null,
      };
    } catch (err: any) {
      return {
        success: false,
        mandates: [],
        after: null,
        error: `Network error: ${err?.message ?? String(err)}`,
      };
    }
  }

  /**
   * GET /mandates/:id — fetch a mandate's current state.
   *
   * Faithful port of GoCardlessClient.get_mandate
   * (sql_rag/gocardless_api.py). Returns the raw GoCardless mandate
   * object. 404s and errors are surfaced as success=false with a
   * friendly message; callers can fall back to local data.
   */
  async getMandate(
    mandateId: string,
  ): Promise<{ success: boolean; mandate?: Record<string, unknown>; error?: string }> {
    if (!mandateId) return { success: false, error: 'mandateId required' };
    try {
      const res = await this.request(
        'GET',
        `/mandates/${encodeURIComponent(mandateId)}`,
      );
      if (res.status === 401) {
        return {
          success: false,
          error: 'Invalid GoCardless API token (401 Unauthorized)',
        };
      }
      if (!res.ok) {
        const text = await res.text().catch(() => '');
        return {
          success: false,
          error: `GoCardless API returned ${res.status}: ${text.slice(0, 200)}`,
        };
      }
      const data = (await res.json()) as { mandates?: Record<string, unknown> };
      return { success: true, mandate: data.mandates ?? {} };
    } catch (err: any) {
      return {
        success: false,
        error: `Network error: ${err?.message ?? String(err)}`,
      };
    }
  }

  /**
   * GET /customers/:id — fetch a customer's contact details.
   *
   * Faithful port of GoCardlessClient.get_customer. Used during
   * mandate-link to harvest the customer email.
   */
  async getCustomer(
    customerId: string,
  ): Promise<{ success: boolean; customer?: Record<string, unknown>; error?: string }> {
    if (!customerId) return { success: false, error: 'customerId required' };
    try {
      const res = await this.request(
        'GET',
        `/customers/${encodeURIComponent(customerId)}`,
      );
      if (res.status === 401) {
        return {
          success: false,
          error: 'Invalid GoCardless API token (401 Unauthorized)',
        };
      }
      if (!res.ok) {
        const text = await res.text().catch(() => '');
        return {
          success: false,
          error: `GoCardless API returned ${res.status}: ${text.slice(0, 200)}`,
        };
      }
      const data = (await res.json()) as { customers?: Record<string, unknown> };
      return { success: true, customer: data.customers ?? {} };
    } catch (err: any) {
      return {
        success: false,
        error: `Network error: ${err?.message ?? String(err)}`,
      };
    }
  }

  /**
   * POST /mandates/:id/actions/cancel — cancel a mandate.
   *
   * Faithful port of the cancel call wrapped by
   * cancel_gocardless_mandate (apps/gocardless/api/routes.py
   * :6795-6830). Returns uniform shape so the wrapping service can
   * detect "already cancelled" responses gracefully (Python's source
   * treats them as success too).
   */
  async cancelMandate(
    mandateId: string,
  ): Promise<{ success: boolean; status?: string; error?: string; alreadyCancelled?: boolean }> {
    if (!mandateId) return { success: false, error: 'mandateId required' };
    try {
      const res = await this.request(
        'POST',
        `/mandates/${encodeURIComponent(mandateId)}/actions/cancel`,
        {},
      );
      if (res.status === 401) {
        return {
          success: false,
          error: 'Invalid GoCardless API token (401 Unauthorized)',
        };
      }
      if (!res.ok) {
        const text = await res.text().catch(() => '');
        if (
          text.toLowerCase().includes('already') &&
          text.toLowerCase().includes('cancel')
        ) {
          return { success: true, status: 'cancelled', alreadyCancelled: true };
        }
        return {
          success: false,
          error: `GoCardless API error: ${text.slice(0, 200) || `${res.status}`}`,
        };
      }
      const data = (await res.json()) as { mandates?: { status?: string } };
      return { success: true, status: data.mandates?.status ?? 'cancelled' };
    } catch (err: any) {
      return {
        success: false,
        error: `Network error: ${err?.message ?? String(err)}`,
      };
    }
  }

  /**
   * POST /payments — create a new payment against a mandate.
   *
   * Faithful port of GoCardlessClient.create_payment
   * (sql_rag/gocardless_api.py:329-380). Uniform `{success, payment?,
   * error?}` shape so callers can compose without exception-handling.
   */
  async createPayment(opts: {
    amountPence: number;
    mandateId: string;
    description?: string | null;
    chargeDate?: string | null;
    currency?: string;
    metadata?: Record<string, string> | null;
    reference?: string | null;
    retryIfPossible?: boolean;
  }): Promise<{
    success: boolean;
    payment?: Record<string, unknown>;
    error?: string;
  }> {
    if (!opts.mandateId) return { success: false, error: 'mandateId required' };
    const body: Record<string, unknown> = {
      amount: opts.amountPence,
      currency: opts.currency ?? 'GBP',
      links: { mandate: opts.mandateId },
      retry_if_possible: opts.retryIfPossible !== false,
    };
    if (opts.description) body.description = opts.description;
    if (opts.chargeDate) body.charge_date = opts.chargeDate;
    if (opts.reference) body.reference = opts.reference;
    if (opts.metadata) body.metadata = opts.metadata;
    try {
      const res = await this.request('POST', '/payments', { payments: body });
      if (res.status === 401) {
        return {
          success: false,
          error: 'Invalid GoCardless API token (401 Unauthorized)',
        };
      }
      if (!res.ok) {
        const text = await res.text().catch(() => '');
        return {
          success: false,
          error: `GoCardless API returned ${res.status}: ${text.slice(0, 200)}`,
        };
      }
      const data = (await res.json()) as { payments?: Record<string, unknown> };
      return { success: true, payment: data.payments ?? {} };
    } catch (err: any) {
      return {
        success: false,
        error: `Network error: ${err?.message ?? String(err)}`,
      };
    }
  }

  /**
   * GET /subscriptions — list subscriptions.
   *
   * Faithful port of GoCardlessClient.list_subscriptions
   * (sql_rag/gocardless_api.py:487-527). Returns the page of raw
   * subscription objects plus the next-page cursor for pagination.
   */
  async listSubscriptions(opts: {
    mandateId?: string;
    customerId?: string;
    status?: string;
    limit?: number;
    cursor?: string;
  } = {}): Promise<{
    success: boolean;
    subscriptions: Array<Record<string, unknown>>;
    after: string | null;
    error?: string;
  }> {
    try {
      const params = new URLSearchParams();
      if (opts.mandateId) params.set('mandate', opts.mandateId);
      if (opts.customerId) params.set('customer', opts.customerId);
      if (opts.status) params.set('status', opts.status);
      params.set('limit', String(Math.min(opts.limit ?? 100, 500)));
      if (opts.cursor) params.set('after', opts.cursor);
      const path = `/subscriptions?${params.toString()}`;
      const res = await this.request('GET', path);
      if (res.status === 401) {
        return {
          success: false,
          subscriptions: [],
          after: null,
          error: 'Invalid GoCardless API token (401 Unauthorized)',
        };
      }
      if (!res.ok) {
        const text = await res.text().catch(() => '');
        return {
          success: false,
          subscriptions: [],
          after: null,
          error: `GoCardless API returned ${res.status}: ${text.slice(0, 200)}`,
        };
      }
      const data = (await res.json()) as {
        subscriptions?: Array<Record<string, unknown>>;
        meta?: { cursors?: { after?: string | null } };
      };
      return {
        success: true,
        subscriptions: Array.isArray(data.subscriptions) ? data.subscriptions : [],
        after: data.meta?.cursors?.after ?? null,
      };
    } catch (err: any) {
      return {
        success: false,
        subscriptions: [],
        after: null,
        error: `Network error: ${err?.message ?? String(err)}`,
      };
    }
  }

  /**
   * GET /subscriptions/:id — fetch a subscription's current state.
   *
   * Faithful port of `GoCardlessClient.get_subscription`
   * (sql_rag/gocardless_api.py:529-532). Returns the raw GoCardless
   * subscription object inside `{success, subscription}`.
   */
  async getSubscription(
    subscriptionId: string,
  ): Promise<{
    success: boolean;
    subscription?: Record<string, unknown>;
    error?: string;
  }> {
    if (!subscriptionId) return { success: false, error: 'subscriptionId required' };
    try {
      const res = await this.request(
        'GET',
        `/subscriptions/${encodeURIComponent(subscriptionId)}`,
      );
      if (res.status === 401) {
        return {
          success: false,
          error: 'Invalid GoCardless API token (401 Unauthorized)',
        };
      }
      if (!res.ok) {
        const text = await res.text().catch(() => '');
        return {
          success: false,
          error: `GoCardless API returned ${res.status}: ${text.slice(0, 200)}`,
        };
      }
      const data = (await res.json()) as {
        subscriptions?: Record<string, unknown>;
      };
      return { success: true, subscription: data.subscriptions ?? {} };
    } catch (err: any) {
      return {
        success: false,
        error: `Network error: ${err?.message ?? String(err)}`,
      };
    }
  }

  /**
   * PUT /subscriptions/:id — update name / amount / metadata.
   *
   * Faithful port of `GoCardlessClient.update_subscription`
   * (sql_rag/gocardless_api.py:534-564). Only fields that are non-null/
   * non-undefined are sent to GoCardless (matches Python's `is not None`
   * gate).
   */
  async updateSubscription(
    subscriptionId: string,
    opts: {
      name?: string | null;
      amountPence?: number | null;
      metadata?: Record<string, string> | null;
    } = {},
  ): Promise<{
    success: boolean;
    subscription?: Record<string, unknown>;
    error?: string;
  }> {
    if (!subscriptionId)
      return { success: false, error: 'subscriptionId required' };
    const subData: Record<string, unknown> = {};
    if (opts.name !== undefined && opts.name !== null) subData.name = opts.name;
    if (opts.amountPence !== undefined && opts.amountPence !== null)
      subData.amount = opts.amountPence;
    if (opts.metadata !== undefined && opts.metadata !== null)
      subData.metadata = opts.metadata;
    try {
      const res = await this.request(
        'PUT',
        `/subscriptions/${encodeURIComponent(subscriptionId)}`,
        { subscriptions: subData },
      );
      if (res.status === 401) {
        return {
          success: false,
          error: 'Invalid GoCardless API token (401 Unauthorized)',
        };
      }
      if (!res.ok) {
        const text = await res.text().catch(() => '');
        return {
          success: false,
          error: `GoCardless API returned ${res.status}: ${text.slice(0, 200)}`,
        };
      }
      const data = (await res.json()) as {
        subscriptions?: Record<string, unknown>;
      };
      return { success: true, subscription: data.subscriptions ?? {} };
    } catch (err: any) {
      return {
        success: false,
        error: `Network error: ${err?.message ?? String(err)}`,
      };
    }
  }

  /**
   * POST /subscriptions/:id/actions/pause — pause an active subscription.
   *
   * Faithful port of `GoCardlessClient.pause_subscription`.
   */
  async pauseSubscription(
    subscriptionId: string,
  ): Promise<{
    success: boolean;
    subscription?: Record<string, unknown>;
    error?: string;
  }> {
    return this._subscriptionAction(subscriptionId, 'pause');
  }

  /**
   * POST /subscriptions/:id/actions/resume — resume a paused subscription.
   *
   * Faithful port of `GoCardlessClient.resume_subscription`.
   */
  async resumeSubscription(
    subscriptionId: string,
  ): Promise<{
    success: boolean;
    subscription?: Record<string, unknown>;
    error?: string;
  }> {
    return this._subscriptionAction(subscriptionId, 'resume');
  }

  /**
   * POST /subscriptions/:id/actions/cancel — cancel a subscription.
   *
   * Faithful port of `GoCardlessClient.cancel_subscription`. Cannot
   * be undone (per GoCardless API).
   */
  async cancelSubscription(
    subscriptionId: string,
  ): Promise<{
    success: boolean;
    subscription?: Record<string, unknown>;
    error?: string;
  }> {
    return this._subscriptionAction(subscriptionId, 'cancel');
  }

  private async _subscriptionAction(
    subscriptionId: string,
    action: 'pause' | 'resume' | 'cancel',
  ): Promise<{
    success: boolean;
    subscription?: Record<string, unknown>;
    error?: string;
  }> {
    if (!subscriptionId)
      return { success: false, error: 'subscriptionId required' };
    try {
      const res = await this.request(
        'POST',
        `/subscriptions/${encodeURIComponent(subscriptionId)}/actions/${action}`,
        {},
      );
      if (res.status === 401) {
        return {
          success: false,
          error: 'Invalid GoCardless API token (401 Unauthorized)',
        };
      }
      if (!res.ok) {
        const text = await res.text().catch(() => '');
        return {
          success: false,
          error: `GoCardless API returned ${res.status}: ${text.slice(0, 200)}`,
        };
      }
      const data = (await res.json()) as {
        subscriptions?: Record<string, unknown>;
      };
      return { success: true, subscription: data.subscriptions ?? {} };
    } catch (err: any) {
      return {
        success: false,
        error: `Network error: ${err?.message ?? String(err)}`,
      };
    }
  }

  /**
   * Test the API token by hitting GET /creditors.
   *
   * Returns success + organisation name on a 200, or a friendly error
   * message on auth failure / network error.
   */
  async testConnection(): Promise<TestConnectionResult> {
    try {
      const res = await this.request('GET', '/creditors?limit=1');

      if (res.status === 401) {
        return {
          success: false,
          error: 'Invalid GoCardless API token (401 Unauthorized)',
          environment: this.environment,
        };
      }
      if (!res.ok) {
        const text = await res.text().catch(() => '');
        return {
          success: false,
          error: `GoCardless API returned ${res.status}: ${text.slice(0, 200)}`,
          environment: this.environment,
        };
      }

      const data = (await res.json()) as { creditors?: Array<{ name?: string }> };
      const orgName = data.creditors?.[0]?.name ?? '(no creditors found)';

      return {
        success: true,
        message: `Connected to GoCardless ${this.environment}`,
        organisation: orgName,
        environment: this.environment,
      };
    } catch (err: any) {
      return {
        success: false,
        error: `Network error: ${err?.message ?? String(err)}`,
        environment: this.environment,
      };
    }
  }
}

/**
 * Create a client from saved settings, or return null if no token.
 */
export function createClientFromSettings(settings: {
  api_access_token?: string;
  api_sandbox?: boolean;
}): GoCardlessClient | null {
  if (!settings.api_access_token) return null;
  return new GoCardlessClient({
    accessToken: settings.api_access_token,
    sandbox: !!settings.api_sandbox,
  });
}

// =====================================================================
// GoCardlessPartnerClient — OAuth Connect for merchant onboarding
// =====================================================================

/**
 * GoCardless Partner / Connect OAuth client.
 *
 * Faithful port of `GoCardlessPartnerClient`
 * (sql_rag/gocardless_api.py:855-1001).
 *
 * Used by the partner-portal flow to:
 *   1. Generate an authorisation URL for a new merchant
 *   2. Exchange the returned code for a merchant access token
 *   3. Fetch the merchant's creditor info to verify the token works
 *
 * NB: per MEMORY.md, sandbox=true is the default for safety. Live
 * mode must be explicitly opted in.
 */
const PARTNER_SANDBOX_CONNECT_URL = 'https://connect-sandbox.gocardless.com';
const PARTNER_LIVE_CONNECT_URL = 'https://connect.gocardless.com';
const PARTNER_SANDBOX_API_URL = 'https://api-sandbox.gocardless.com';
const PARTNER_LIVE_API_URL = 'https://api.gocardless.com';

export interface GoCardlessPartnerClientOptions {
  clientId: string;
  clientSecret: string;
  sandbox?: boolean;
  /** Override fetch — primarily for tests. */
  fetchImpl?: typeof fetch;
}

export interface AuthorisationUrlOptions {
  redirectUri: string;
  scope?: string;
  prefillEmail?: string | null;
  prefillCompanyName?: string | null;
  state?: string | null;
}

export interface ExchangeCodeResult {
  access_token: string;
  token_type?: string;
  scope?: string;
  organisation_id?: string;
}

export interface ExchangeCodeResponse {
  success: boolean;
  data?: ExchangeCodeResult;
  error?: string;
}

export interface OrganisationInfo {
  id?: string;
  name?: string;
  [k: string]: unknown;
}

export interface OrganisationInfoResponse {
  success: boolean;
  organisation?: OrganisationInfo;
  error?: string;
}

export class GoCardlessPartnerClient {
  private clientId: string;
  private clientSecret: string;
  private sandbox: boolean;
  private connectUrl: string;
  private apiUrl: string;
  private fetchImpl: typeof fetch;

  constructor(opts: GoCardlessPartnerClientOptions) {
    this.clientId = opts.clientId;
    this.clientSecret = opts.clientSecret;
    this.sandbox = opts.sandbox ?? true;
    this.connectUrl = this.sandbox
      ? PARTNER_SANDBOX_CONNECT_URL
      : PARTNER_LIVE_CONNECT_URL;
    this.apiUrl = this.sandbox ? PARTNER_SANDBOX_API_URL : PARTNER_LIVE_API_URL;
    this.fetchImpl = opts.fetchImpl ?? fetch;
  }

  /**
   * Generate the OAuth consent URL the merchant visits to authorise
   * our app. Uses GoCardless OAuth Connect bracketed-prefill keys
   * (`prefill[email]`, `prefill[company_name]`).
   */
  getAuthorisationUrl(opts: AuthorisationUrlOptions): string {
    const params = new URLSearchParams();
    params.set('response_type', 'code');
    params.set('client_id', this.clientId);
    params.set('scope', opts.scope ?? 'read_write');
    params.set('redirect_uri', opts.redirectUri);
    params.set('access_type', 'offline');
    if (opts.prefillEmail) params.set('prefill[email]', opts.prefillEmail);
    if (opts.prefillCompanyName)
      params.set('prefill[company_name]', opts.prefillCompanyName);
    if (opts.state) params.set('state', opts.state);
    return `${this.connectUrl}/oauth/authorize?${params.toString()}`;
  }

  /**
   * Exchange the authorisation code for a merchant access token.
   * The redirect_uri MUST match the one used in getAuthorisationUrl.
   */
  async exchangeAuthorisationCode(
    code: string,
    redirectUri: string,
  ): Promise<ExchangeCodeResponse> {
    const url = `${this.connectUrl}/oauth/access_token`;
    try {
      const ac = new AbortController();
      const timer = setTimeout(() => ac.abort(), 30000);
      let res: Response;
      try {
        res = await this.fetchImpl(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            grant_type: 'authorization_code',
            client_id: this.clientId,
            client_secret: this.clientSecret,
            code,
            redirect_uri: redirectUri,
          }),
          signal: ac.signal,
        });
      } finally {
        clearTimeout(timer);
      }
      if (res.status !== 200) {
        let msg = await res.text().catch(() => '');
        try {
          const data = JSON.parse(msg) as {
            error?: string;
            error_description?: string;
          };
          msg = data.error_description ?? data.error ?? msg;
        } catch {
          // keep raw text
        }
        return {
          success: false,
          error: `Token exchange failed (${res.status}): ${msg.slice(0, 200)}`,
        };
      }
      const data = (await res.json()) as ExchangeCodeResult;
      return { success: true, data };
    } catch (err: any) {
      return {
        success: false,
        error: `Token exchange request failed: ${err?.message ?? String(err)}`,
      };
    }
  }

  /**
   * Verify the merchant token by fetching the first creditor.
   * Returns the creditor (or `{}` when none) — same shape as Python.
   */
  async getOrganisationInfo(accessToken: string): Promise<OrganisationInfoResponse> {
    const url = `${this.apiUrl}/creditors`;
    try {
      const res = await this.fetchImpl(url, {
        method: 'GET',
        headers: {
          Authorization: `Bearer ${accessToken}`,
          'GoCardless-Version': '2015-07-06',
          'Content-Type': 'application/json',
        },
      });
      if (res.status !== 200) {
        return {
          success: false,
          error: `Failed to get organisation info: ${res.status}`,
        };
      }
      const data = (await res.json()) as { creditors?: OrganisationInfo[] };
      const creditors = Array.isArray(data.creditors) ? data.creditors : [];
      return { success: true, organisation: creditors[0] ?? {} };
    } catch (err: any) {
      return {
        success: false,
        error: `Organisation info request failed: ${err?.message ?? String(err)}`,
      };
    }
  }
}

/**
 * Build a GoCardlessPartnerClient from saved settings, or return null
 * when partner credentials aren't configured.
 */
export function createPartnerClientFromSettings(
  settings: {
    partner_client_id?: string;
    partner_client_secret?: string;
    api_sandbox?: boolean;
  },
  fetchImpl?: typeof fetch,
): GoCardlessPartnerClient | null {
  const clientId = (settings.partner_client_id ?? '').trim();
  const clientSecret = (settings.partner_client_secret ?? '').trim();
  if (!clientId || !clientSecret) return null;
  return new GoCardlessPartnerClient({
    clientId,
    clientSecret,
    sandbox: !!settings.api_sandbox,
    fetchImpl,
  });
}
