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
