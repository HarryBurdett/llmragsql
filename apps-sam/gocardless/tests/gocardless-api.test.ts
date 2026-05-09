import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { GoCardlessClient, createClientFromSettings } from '../src/services/gocardless-api.js';

describe('GoCardlessClient', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn());
  });
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('uses sandbox base URL when sandbox=true', async () => {
    (global.fetch as any).mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ creditors: [{ name: 'Test Org' }] }),
    });

    const client = new GoCardlessClient({ accessToken: 'sandbox_token', sandbox: true });
    const result = await client.testConnection();

    expect(result.success).toBe(true);
    expect(result.organisation).toBe('Test Org');
    expect(result.environment).toBe('sandbox');

    const fetchCall = (global.fetch as any).mock.calls[0];
    expect(fetchCall[0]).toMatch(/api-sandbox\.gocardless\.com/);
  });

  it('uses live URL when sandbox=false', async () => {
    (global.fetch as any).mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ creditors: [{ name: 'Live Org' }] }),
    });

    const client = new GoCardlessClient({ accessToken: 'live_token', sandbox: false });
    const result = await client.testConnection();

    expect(result.environment).toBe('live');
    const fetchCall = (global.fetch as any).mock.calls[0];
    expect(fetchCall[0]).toMatch(/^https:\/\/api\.gocardless\.com/);
    // Authorization header set
    expect(fetchCall[1].headers.Authorization).toBe('Bearer live_token');
  });

  it('returns success=false on 401', async () => {
    (global.fetch as any).mockResolvedValue({
      ok: false,
      status: 401,
      text: async () => 'Unauthorized',
    });

    const client = new GoCardlessClient({ accessToken: 'bad_token', sandbox: true });
    const result = await client.testConnection();

    expect(result.success).toBe(false);
    expect(result.error).toMatch(/Invalid GoCardless API token/);
  });

  it('returns success=false on other HTTP errors', async () => {
    (global.fetch as any).mockResolvedValue({
      ok: false,
      status: 500,
      text: async () => 'Internal Server Error',
    });

    const client = new GoCardlessClient({ accessToken: 'token', sandbox: true });
    const result = await client.testConnection();

    expect(result.success).toBe(false);
    expect(result.error).toMatch(/500/);
  });

  it('handles network errors', async () => {
    (global.fetch as any).mockRejectedValue(new Error('ENOTFOUND'));

    const client = new GoCardlessClient({ accessToken: 'token', sandbox: true });
    const result = await client.testConnection();

    expect(result.success).toBe(false);
    expect(result.error).toMatch(/ENOTFOUND/);
  });

  it('reports "no creditors found" when API returns empty list', async () => {
    (global.fetch as any).mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ creditors: [] }),
    });

    const client = new GoCardlessClient({ accessToken: 'token', sandbox: true });
    const result = await client.testConnection();

    expect(result.success).toBe(true);
    expect(result.organisation).toBe('(no creditors found)');
  });
});

describe('GoCardlessClient.getPayouts', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn());
  });
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('returns payouts and before-cursor on success', async () => {
    (global.fetch as any).mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({
        payouts: [
          { id: 'PO0001', amount: 1000, currency: 'GBP', status: 'paid' },
          { id: 'PO0002', amount: 2500, currency: 'GBP', status: 'paid' },
        ],
        meta: { cursors: { before: 'CUR_ABC' } },
      }),
    });

    const client = new GoCardlessClient({ accessToken: 'token', sandbox: true });
    const result = await client.getPayouts({
      status: 'paid',
      limit: 10,
      createdAtGte: '2026-04-01',
    });

    expect(result.success).toBe(true);
    expect(result.payouts).toHaveLength(2);
    expect(result.before).toBe('CUR_ABC');

    const fetchCall = (global.fetch as any).mock.calls[0];
    const url = fetchCall[0] as string;
    expect(url).toMatch(/\/payouts\?/);
    expect(url).toContain('status=paid');
    expect(url).toContain('limit=10');
    // GoCardless filter syntax — bracketed key URL-encoded
    expect(url).toMatch(/created_at(\[gte\]|%5Bgte%5D)=2026-04-01/);
  });

  it('omits query params when not provided', async () => {
    (global.fetch as any).mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ payouts: [], meta: { cursors: { before: null } } }),
    });

    const client = new GoCardlessClient({ accessToken: 'token', sandbox: true });
    const result = await client.getPayouts();

    expect(result.success).toBe(true);
    expect(result.payouts).toEqual([]);
    expect(result.before).toBeNull();

    const url = (global.fetch as any).mock.calls[0][0] as string;
    expect(url).toMatch(/\/payouts$/);
  });

  it('passes through before cursor for pagination', async () => {
    (global.fetch as any).mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ payouts: [], meta: { cursors: { before: null } } }),
    });

    const client = new GoCardlessClient({ accessToken: 'token', sandbox: true });
    await client.getPayouts({ before: 'PAGE_2' });

    const url = (global.fetch as any).mock.calls[0][0] as string;
    expect(url).toContain('before=PAGE_2');
  });

  it('returns success=false on 401', async () => {
    (global.fetch as any).mockResolvedValue({
      ok: false,
      status: 401,
      text: async () => 'Unauthorized',
    });

    const client = new GoCardlessClient({ accessToken: 'bad', sandbox: true });
    const result = await client.getPayouts();

    expect(result.success).toBe(false);
    expect(result.error).toMatch(/Invalid GoCardless API token/);
    expect(result.payouts).toEqual([]);
    expect(result.before).toBeNull();
  });

  it('returns success=false on other HTTP errors', async () => {
    (global.fetch as any).mockResolvedValue({
      ok: false,
      status: 500,
      text: async () => 'Server crashed',
    });

    const client = new GoCardlessClient({ accessToken: 'token', sandbox: true });
    const result = await client.getPayouts();

    expect(result.success).toBe(false);
    expect(result.error).toMatch(/500/);
    expect(result.error).toContain('Server crashed');
  });

  it('handles network errors', async () => {
    (global.fetch as any).mockRejectedValue(new Error('ECONNREFUSED'));

    const client = new GoCardlessClient({ accessToken: 'token', sandbox: true });
    const result = await client.getPayouts();

    expect(result.success).toBe(false);
    expect(result.error).toMatch(/ECONNREFUSED/);
  });

  it('handles missing payouts array gracefully', async () => {
    (global.fetch as any).mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ meta: { cursors: {} } }),
    });

    const client = new GoCardlessClient({ accessToken: 'token', sandbox: true });
    const result = await client.getPayouts();

    expect(result.success).toBe(true);
    expect(result.payouts).toEqual([]);
    expect(result.before).toBeNull();
  });
});

describe('createClientFromSettings', () => {
  it('returns null when no token', () => {
    expect(createClientFromSettings({})).toBeNull();
    expect(createClientFromSettings({ api_access_token: '' })).toBeNull();
  });

  it('returns client when token present', () => {
    const client = createClientFromSettings({
      api_access_token: 'sandbox_token',
      api_sandbox: true,
    });
    expect(client).not.toBeNull();
  });
});

describe('GoCardlessClient.getSubscription', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn());
  });
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('returns the subscription on 200', async () => {
    (global.fetch as any).mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ subscriptions: { id: 'SB1', status: 'active' } }),
    });
    const client = new GoCardlessClient({ accessToken: 'token', sandbox: true });
    const result = await client.getSubscription('SB1');
    expect(result.success).toBe(true);
    expect(result.subscription?.id).toBe('SB1');
    const url = (global.fetch as any).mock.calls[0][0] as string;
    expect(url).toMatch(/\/subscriptions\/SB1$/);
    expect((global.fetch as any).mock.calls[0][1].method).toBe('GET');
  });

  it('returns 401 error on bad token', async () => {
    (global.fetch as any).mockResolvedValue({
      ok: false,
      status: 401,
      text: async () => 'Unauthorized',
    });
    const client = new GoCardlessClient({ accessToken: 'bad', sandbox: true });
    const result = await client.getSubscription('SB1');
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/401/);
  });

  it('refuses empty subscription id', async () => {
    const client = new GoCardlessClient({ accessToken: 'token', sandbox: true });
    const result = await client.getSubscription('');
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/subscriptionId required/);
  });
});

describe('GoCardlessClient.updateSubscription', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn());
  });
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('PUTs subscriptions wrapper with only provided fields', async () => {
    (global.fetch as any).mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({
        subscriptions: { id: 'SB1', amount: 5000, status: 'active' },
      }),
    });
    const client = new GoCardlessClient({ accessToken: 'token', sandbox: true });
    const result = await client.updateSubscription('SB1', { amountPence: 5000 });
    expect(result.success).toBe(true);
    expect(result.subscription?.amount).toBe(5000);
    const fetchCall = (global.fetch as any).mock.calls[0];
    expect(fetchCall[1].method).toBe('PUT');
    const body = JSON.parse(fetchCall[1].body);
    expect(body).toEqual({ subscriptions: { amount: 5000 } });
  });

  it('passes name and metadata through', async () => {
    (global.fetch as any).mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ subscriptions: { id: 'SB1' } }),
    });
    const client = new GoCardlessClient({ accessToken: 'token', sandbox: true });
    await client.updateSubscription('SB1', {
      name: 'New Name',
      metadata: { foo: 'bar' },
    });
    const body = JSON.parse((global.fetch as any).mock.calls[0][1].body);
    expect(body.subscriptions.name).toBe('New Name');
    expect(body.subscriptions.metadata).toEqual({ foo: 'bar' });
    expect(body.subscriptions.amount).toBeUndefined();
  });

  it('returns error message on HTTP failure', async () => {
    (global.fetch as any).mockResolvedValue({
      ok: false,
      status: 422,
      text: async () => 'invalid amount',
    });
    const client = new GoCardlessClient({ accessToken: 'token', sandbox: true });
    const result = await client.updateSubscription('SB1', { amountPence: -1 });
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/422/);
    expect(result.error).toContain('invalid amount');
  });

  it('refuses empty subscription id', async () => {
    const client = new GoCardlessClient({ accessToken: 'token', sandbox: true });
    const result = await client.updateSubscription('', { name: 'X' });
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/subscriptionId required/);
  });
});

describe('GoCardlessClient.pauseSubscription / resumeSubscription / cancelSubscription', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn());
  });
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('pauseSubscription POSTs to /actions/pause', async () => {
    (global.fetch as any).mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ subscriptions: { id: 'SB1', status: 'paused' } }),
    });
    const client = new GoCardlessClient({ accessToken: 'token', sandbox: true });
    const result = await client.pauseSubscription('SB1');
    expect(result.success).toBe(true);
    expect(result.subscription?.status).toBe('paused');
    const fetchCall = (global.fetch as any).mock.calls[0];
    expect(fetchCall[0]).toMatch(/\/subscriptions\/SB1\/actions\/pause$/);
    expect(fetchCall[1].method).toBe('POST');
  });

  it('resumeSubscription POSTs to /actions/resume', async () => {
    (global.fetch as any).mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ subscriptions: { id: 'SB1', status: 'active' } }),
    });
    const client = new GoCardlessClient({ accessToken: 'token', sandbox: true });
    const result = await client.resumeSubscription('SB1');
    expect(result.success).toBe(true);
    expect(result.subscription?.status).toBe('active');
    const url = (global.fetch as any).mock.calls[0][0] as string;
    expect(url).toMatch(/\/actions\/resume$/);
  });

  it('cancelSubscription POSTs to /actions/cancel', async () => {
    (global.fetch as any).mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ subscriptions: { id: 'SB1', status: 'cancelled' } }),
    });
    const client = new GoCardlessClient({ accessToken: 'token', sandbox: true });
    const result = await client.cancelSubscription('SB1');
    expect(result.success).toBe(true);
    expect(result.subscription?.status).toBe('cancelled');
    const url = (global.fetch as any).mock.calls[0][0] as string;
    expect(url).toMatch(/\/actions\/cancel$/);
  });

  it('reports HTTP errors uniformly', async () => {
    (global.fetch as any).mockResolvedValue({
      ok: false,
      status: 422,
      text: async () => 'cannot pause',
    });
    const client = new GoCardlessClient({ accessToken: 'token', sandbox: true });
    const result = await client.pauseSubscription('SB1');
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/422/);
  });

  it('refuses empty subscription id', async () => {
    const client = new GoCardlessClient({ accessToken: 'token', sandbox: true });
    expect((await client.pauseSubscription('')).success).toBe(false);
    expect((await client.resumeSubscription('')).success).toBe(false);
    expect((await client.cancelSubscription('')).success).toBe(false);
  });
});
