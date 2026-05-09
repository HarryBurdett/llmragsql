/**
 * GoCardless subscription read + lifecycle services.
 *
 * Faithful port of:
 *   - list_subscriptions / get_subscription
 *     (sql_rag/gocardless_payments.py:958-1075)
 *   - update_subscription_status
 *     (sql_rag/gocardless_payments.py:1077-1092)
 *   - add_subscription_document / remove_subscription_document /
 *     get_subscriptions_by_source_doc
 *     (sql_rag/gocardless_payments.py:1107-1174)
 *   - pause/resume/cancel/update routes
 *     (apps/gocardless/api/routes.py:9157-9372)
 *   - link / unlink routes
 *     (apps/gocardless/api/routes.py:8788-8874)
 *
 * Reads from the per-app DB's aligned `gocardless_subscriptions` and
 * `gocardless_subscription_documents` tables (migration 007).
 *
 * The pause/resume/cancel/update lifecycle wrappers take a `remote`
 * callback so callers can wire the GoCardless API client (or a stub
 * for tests). Mirrors the existing pattern used by `cancelMandate`.
 */
import type { Knex } from 'knex';

// ---------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------

export interface Subscription {
  id: number;
  subscription_id: string;
  mandate_id: string;
  opera_account: string;
  opera_name: string;
  source_doc: string;
  source_docs: string[];
  amount_pence: number;
  amount_pounds: number;
  amount_formatted: string;
  currency: string;
  interval_unit: string;
  interval_count: number;
  frequency: string;
  day_of_month: number | null;
  name: string;
  status: string;
  start_date: string;
  end_date: string;
  created_at: string;
  updated_at: string;
  synced_at: string;
  /** Used by listSubscriptions for back-compat with the dashboard. */
  customer_name: string;
}

export interface ListSubscriptionsOptions {
  status?: string | null;
  operaAccount?: string | null;
  /** Mirrors Python's include_cancelled — defaults to false. */
  includeCancelled?: boolean;
  limit?: number;
}

export interface ListSubscriptionsResponse {
  success: boolean;
  subscriptions: Subscription[];
  count: number;
  error?: string;
}

export interface GetSubscriptionResponse {
  success: boolean;
  subscription?: Subscription;
  error?: string;
}

export interface SubscriptionLifecycleResponse {
  success: boolean;
  subscription?: Subscription;
  message?: string;
  error?: string;
}

export interface RemoteSubscriptionResult {
  success: boolean;
  subscription?: Record<string, unknown>;
  error?: string;
}

interface SubscriptionRow {
  id: number;
  subscription_id: string | null;
  mandate_id: string | null;
  opera_account: string | null;
  opera_name: string | null;
  source_doc: string | null;
  amount_pence: number | string | null;
  currency: string | null;
  interval_unit: string | null;
  interval_count: number | string | null;
  day_of_month: number | string | null;
  name: string | null;
  status: string | null;
  start_date: string | null;
  end_date: string | null;
  created_at: Date | string | null;
  updated_at: Date | string | null;
  synced_at: Date | string | null;
}

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

function dateToIso(d: Date | string | null): string {
  if (!d) return '';
  if (d instanceof Date) {
    if (Number.isNaN(d.getTime())) return '';
    return d.toISOString();
  }
  return String(d);
}

function formatPounds(pence: number): string {
  const pounds = pence / 100;
  // Match Python's `f"£{x:,.2f}"`: thousands sep + 2dp.
  return `£${pounds.toLocaleString('en-GB', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

function frequencyLabel(unit: string, count: number): string {
  const u = (unit ?? 'monthly').toLowerCase();
  const c = count || 1;
  if (u === 'weekly' && c === 1) return 'Weekly';
  if (u === 'monthly' && c === 1) return 'Monthly';
  if (u === 'monthly' && c === 3) return 'Quarterly';
  if (u === 'yearly' && c === 1) return 'Annual';
  return `Every ${c} ${u}`;
}

function rowToSubscription(
  row: SubscriptionRow,
  sourceDocs: string[],
  mandateNameLookup: Map<string, string>,
): Subscription {
  const operaAccount = (row.opera_account ?? '').trim();
  const operaName = (row.opera_name ?? '').trim();
  const intervalUnit = (row.interval_unit ?? 'monthly').toLowerCase();
  const intervalCount = Number(row.interval_count ?? 1) || 1;
  const amountPence = Math.round(Number(row.amount_pence ?? 0));
  const dayOfMonth =
    row.day_of_month === null || row.day_of_month === undefined
      ? null
      : Number(row.day_of_month);
  // Python returns null for empty values; we normalise to '' for
  // string fields that are guaranteed non-null in the response shape.
  const customerName =
    operaName || mandateNameLookup.get(operaAccount) || operaAccount;
  return {
    id: row.id,
    subscription_id: row.subscription_id ?? '',
    mandate_id: row.mandate_id ?? '',
    opera_account: operaAccount,
    opera_name: operaName,
    source_doc: row.source_doc ?? '',
    source_docs: sourceDocs,
    amount_pence: amountPence,
    amount_pounds: amountPence / 100,
    amount_formatted: formatPounds(amountPence),
    currency: row.currency ?? 'GBP',
    interval_unit: intervalUnit,
    interval_count: intervalCount,
    frequency: frequencyLabel(intervalUnit, intervalCount),
    day_of_month: Number.isFinite(dayOfMonth as number) ? (dayOfMonth as number) : null,
    name: row.name ?? '',
    status: row.status ?? '',
    start_date: row.start_date ?? '',
    end_date: row.end_date ?? '',
    created_at: dateToIso(row.created_at),
    updated_at: dateToIso(row.updated_at),
    synced_at: dateToIso(row.synced_at),
    customer_name: customerName,
  };
}

async function fetchSourceDocs(
  appDb: Knex,
  subscriptionIds: string[],
): Promise<Map<string, string[]>> {
  const map = new Map<string, string[]>();
  if (subscriptionIds.length === 0) return map;
  const rows = (await appDb('gocardless_subscription_documents')
    .whereIn('subscription_id', subscriptionIds)
    .orderBy('added_at', 'asc')
    .select('subscription_id', 'source_doc')) as unknown as Array<{
    subscription_id: string | null;
    source_doc: string | null;
  }>;
  for (const r of rows ?? []) {
    const sid = (r.subscription_id ?? '').trim();
    const doc = (r.source_doc ?? '').trim();
    if (!sid || !doc) continue;
    const arr = map.get(sid) ?? [];
    arr.push(doc);
    map.set(sid, arr);
  }
  return map;
}

async function fetchMandateNames(
  appDb: Knex,
  operaAccounts: string[],
): Promise<Map<string, string>> {
  const map = new Map<string, string>();
  if (operaAccounts.length === 0) return map;
  try {
    const rows = (await appDb('gocardless_mandates')
      .whereIn('opera_account', operaAccounts)
      .select('opera_account', 'opera_name')) as unknown as Array<{
      opera_account: string | null;
      opera_name: string | null;
    }>;
    for (const r of rows ?? []) {
      const acct = (r.opera_account ?? '').trim();
      if (acct) map.set(acct, (r.opera_name ?? '').trim());
    }
  } catch {
    // best-effort — mandate table may not exist in some test fixtures
  }
  return map;
}

// ---------------------------------------------------------------------
// listSubscriptions
// ---------------------------------------------------------------------

export async function listSubscriptions(
  appDb: Knex,
  opts: ListSubscriptionsOptions = {},
): Promise<ListSubscriptionsResponse> {
  try {
    let query = appDb('gocardless_subscriptions').orderBy('created_at', 'desc');
    if (opts.limit !== undefined) {
      query = query.limit(opts.limit);
    }
    if (opts.status) {
      query = query.where({ status: opts.status });
    } else if (!opts.includeCancelled) {
      query = query.whereNot({ status: 'cancelled' });
    }
    if (opts.operaAccount) {
      query = query.where({ opera_account: opts.operaAccount });
    }
    const rows = (await query) as unknown as SubscriptionRow[];

    const subIds = rows
      .map((r) => (r.subscription_id ?? '').trim())
      .filter(Boolean);
    const accounts = Array.from(
      new Set(rows.map((r) => (r.opera_account ?? '').trim()).filter(Boolean)),
    );
    const [docsBySub, mandateNames] = await Promise.all([
      fetchSourceDocs(appDb, subIds),
      fetchMandateNames(appDb, accounts),
    ]);

    const subscriptions = rows.map((r) =>
      rowToSubscription(
        r,
        docsBySub.get((r.subscription_id ?? '').trim()) ?? [],
        mandateNames,
      ),
    );
    return { success: true, subscriptions, count: subscriptions.length };
  } catch (err: any) {
    return {
      success: false,
      subscriptions: [],
      count: 0,
      error: err?.message ?? String(err),
    };
  }
}

// ---------------------------------------------------------------------
// getSubscription — single subscription with source_docs
// ---------------------------------------------------------------------

export async function getSubscription(
  appDb: Knex,
  subscriptionId: string,
): Promise<GetSubscriptionResponse> {
  const id = (subscriptionId ?? '').trim();
  if (!id) return { success: false, error: 'subscription_id is required' };
  try {
    const row = (await appDb('gocardless_subscriptions')
      .where({ subscription_id: id })
      .first()) as unknown as SubscriptionRow | undefined;
    if (!row) {
      return { success: false, error: `Subscription ${id} not found` };
    }
    const account = (row.opera_account ?? '').trim();
    const [docsBySub, mandateNames] = await Promise.all([
      fetchSourceDocs(appDb, [id]),
      fetchMandateNames(appDb, account ? [account] : []),
    ]);
    const subscription = rowToSubscription(
      row,
      docsBySub.get(id) ?? [],
      mandateNames,
    );
    return { success: true, subscription };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}

// ---------------------------------------------------------------------
// Status updates (local-only)
// ---------------------------------------------------------------------

export async function updateSubscriptionStatus(
  appDb: Knex,
  subscriptionId: string,
  status: string,
): Promise<boolean> {
  const id = (subscriptionId ?? '').trim();
  if (!id || !status) return false;
  const updated = await appDb('gocardless_subscriptions')
    .where({ subscription_id: id })
    .update({ status, updated_at: appDb.fn.now() });
  return Number(updated) > 0;
}

// ---------------------------------------------------------------------
// Lifecycle wrappers (pause / resume / cancel / update)
// ---------------------------------------------------------------------

async function runLifecycleAction(
  appDb: Knex,
  subscriptionId: string,
  fallbackStatus: string,
  remote: (id: string) => Promise<RemoteSubscriptionResult>,
): Promise<SubscriptionLifecycleResponse> {
  const id = (subscriptionId ?? '').trim();
  if (!id) return { success: false, error: 'subscription_id is required' };
  const r = await remote(id);
  if (!r.success) {
    return { success: false, error: r.error ?? 'Remote action failed' };
  }
  const remoteStatus =
    typeof r.subscription?.status === 'string' && r.subscription.status
      ? (r.subscription.status as string)
      : fallbackStatus;
  await updateSubscriptionStatus(appDb, id, remoteStatus);
  const fresh = await getSubscription(appDb, id);
  if (!fresh.success) {
    return { success: false, error: fresh.error };
  }
  return { success: true, subscription: fresh.subscription };
}

export async function pauseSubscription(
  appDb: Knex,
  subscriptionId: string,
  remote: (id: string) => Promise<RemoteSubscriptionResult>,
): Promise<SubscriptionLifecycleResponse> {
  return runLifecycleAction(appDb, subscriptionId, 'paused', remote);
}

export async function resumeSubscription(
  appDb: Knex,
  subscriptionId: string,
  remote: (id: string) => Promise<RemoteSubscriptionResult>,
): Promise<SubscriptionLifecycleResponse> {
  return runLifecycleAction(appDb, subscriptionId, 'active', remote);
}

export async function cancelSubscription(
  appDb: Knex,
  subscriptionId: string,
  remote: (id: string) => Promise<RemoteSubscriptionResult>,
): Promise<SubscriptionLifecycleResponse> {
  return runLifecycleAction(appDb, subscriptionId, 'cancelled', remote);
}

export interface UpdateSubscriptionInput {
  name?: string | null;
  amountPence?: number | null;
}

/**
 * PUT /subscriptions/:id — push name/amount to GoCardless, then mirror
 * the result locally. Faithful port of update_gocardless_subscription
 * (apps/gocardless/api/routes.py:9248-9291).
 *
 * Local update only changes columns the caller actually sent (or the
 * status mirrored from the remote response). If the local row is
 * absent the remote call is still performed (matches Python's "no-op
 * silently when local missing" semantics).
 */
export async function updateSubscriptionDetails(
  appDb: Knex,
  subscriptionId: string,
  input: UpdateSubscriptionInput,
  remote: (
    id: string,
    opts: UpdateSubscriptionInput,
  ) => Promise<RemoteSubscriptionResult>,
): Promise<SubscriptionLifecycleResponse> {
  const id = (subscriptionId ?? '').trim();
  if (!id) return { success: false, error: 'subscription_id is required' };
  const r = await remote(id, input);
  if (!r.success) {
    return { success: false, error: r.error ?? 'Remote update failed' };
  }
  const local = (await appDb('gocardless_subscriptions')
    .where({ subscription_id: id })
    .first()) as unknown as SubscriptionRow | undefined;
  if (local) {
    const patch: Record<string, unknown> = { updated_at: appDb.fn.now() };
    if (typeof input.name === 'string' && input.name) {
      patch.name = input.name;
    }
    if (typeof input.amountPence === 'number' && Number.isFinite(input.amountPence)) {
      patch.amount_pence = Math.round(input.amountPence);
    }
    if (typeof r.subscription?.status === 'string' && r.subscription.status) {
      patch.status = r.subscription.status;
    }
    await appDb('gocardless_subscriptions')
      .where({ subscription_id: id })
      .update(patch);
  }
  const fresh = await getSubscription(appDb, id);
  if (!fresh.success) {
    return { success: false, error: fresh.error };
  }
  return { success: true, subscription: fresh.subscription };
}

// ---------------------------------------------------------------------
// sync-from-opera — read itran totals for linked docs, push to GC
// ---------------------------------------------------------------------

export interface OperaDocAmount {
  /** Sum of `it_exvat` across the linked itran lines, in pence. */
  lineNettPence: number;
  /** Sum of `it_vatval` across the linked itran lines, in pence. */
  lineVatPence: number;
}

export interface SyncSubscriptionFromOperaResponse {
  success: boolean;
  message?: string;
  old_amount_pence?: number;
  new_amount_pence?: number;
  old_amount_formatted?: string;
  new_amount_formatted?: string;
  subscription?: Subscription;
  error?: string;
}

/**
 * Faithful port of sync_subscription_from_opera
 * (apps/gocardless/api/routes.py:9172-9245).
 *
 * The Opera read and the GoCardless update are injected so this
 * function stays unit-testable. The HTTP layer wires:
 *   readOperaDocAmount = sum(it_exvat) + sum(it_vatval) FROM itran
 *                        WHERE it_doc IN (...)
 *   updateRemote       = GoCardlessClient.updateSubscription(id, {amountPence})
 */
export async function syncSubscriptionFromOpera(
  appDb: Knex,
  subscriptionId: string,
  readOperaDocAmount: (sourceDocs: string[]) => Promise<OperaDocAmount>,
  updateRemote: (id: string, amountPence: number) => Promise<RemoteSubscriptionResult>,
): Promise<SyncSubscriptionFromOperaResponse> {
  const id = (subscriptionId ?? '').trim();
  if (!id) return { success: false, error: 'subscription_id is required' };

  const local = await getSubscription(appDb, id);
  if (!local.success || !local.subscription) {
    return { success: false, error: local.error ?? `Subscription ${id} not found` };
  }
  const sourceDocs = local.subscription.source_docs ?? [];
  if (sourceDocs.length === 0) {
    return {
      success: false,
      error: 'Subscription is not linked to any Opera documents',
    };
  }

  let opera: OperaDocAmount;
  try {
    opera = await readOperaDocAmount(sourceDocs);
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
  if ((opera.lineNettPence ?? 0) === 0 && (opera.lineVatPence ?? 0) === 0) {
    return {
      success: false,
      error: 'Opera documents not found or have no lines',
    };
  }

  const newAmountPence = Math.round(
    (opera.lineNettPence ?? 0) + (opera.lineVatPence ?? 0),
  );
  const oldAmountPence = local.subscription.amount_pence;
  if (newAmountPence === oldAmountPence) {
    return { success: true, message: 'No change needed — amounts already match' };
  }

  const remote = await updateRemote(id, newAmountPence);
  if (!remote.success) {
    return { success: false, error: remote.error ?? 'Remote update failed' };
  }

  await appDb('gocardless_subscriptions')
    .where({ subscription_id: id })
    .update({
      amount_pence: newAmountPence,
      updated_at: appDb.fn.now(),
    });

  const fresh = await getSubscription(appDb, id);
  return {
    success: true,
    old_amount_pence: oldAmountPence,
    new_amount_pence: newAmountPence,
    old_amount_formatted: formatPounds(oldAmountPence),
    new_amount_formatted: formatPounds(newAmountPence),
    subscription: fresh.subscription,
  };
}

// ---------------------------------------------------------------------
// Subscription <-> Opera repeat-document linking
// ---------------------------------------------------------------------

export interface LinkSubscriptionInput {
  subscriptionId: string;
  sourceDoc: string;
}

export async function linkSubscriptionToDocument(
  appDb: Knex,
  input: LinkSubscriptionInput,
): Promise<SubscriptionLifecycleResponse> {
  const subId = (input.subscriptionId ?? '').trim();
  const doc = (input.sourceDoc ?? '').trim();
  if (!subId || !doc) {
    return {
      success: false,
      error: 'subscription_id and source_doc are required',
    };
  }
  // 1. The subscription itself must exist locally
  const sub = (await appDb('gocardless_subscriptions')
    .where({ subscription_id: subId })
    .first()) as unknown as SubscriptionRow | undefined;
  if (!sub) {
    return {
      success: false,
      error: `Subscription ${subId} not found locally. Sync first.`,
    };
  }
  // 2. The doc must not already be linked to a *different* subscription
  const existing = (await appDb('gocardless_subscription_documents')
    .where({ source_doc: doc })
    .select('subscription_id')) as unknown as Array<{
    subscription_id: string | null;
  }>;
  for (const row of existing ?? []) {
    const linked = (row.subscription_id ?? '').trim();
    if (linked && linked !== subId) {
      return {
        success: false,
        error: `Document ${doc} already linked to subscription ${linked}`,
      };
    }
  }
  // 3. Insert (subscription_id, source_doc) — duplicate-safe.
  const already = existing.some(
    (row) => (row.subscription_id ?? '').trim() === subId,
  );
  if (already) {
    return {
      success: false,
      error: `Document ${doc} is already linked to this subscription`,
    };
  }
  try {
    await appDb('gocardless_subscription_documents').insert({
      subscription_id: subId,
      source_doc: doc,
      added_at: appDb.fn.now(),
    });
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
  const fresh = await getSubscription(appDb, subId);
  if (!fresh.success) {
    return { success: false, error: fresh.error };
  }
  return { success: true, subscription: fresh.subscription };
}

export interface UnlinkSubscriptionInput {
  subscriptionId: string;
  sourceDoc?: string | null;
}

export async function unlinkSubscriptionFromDocument(
  appDb: Knex,
  input: UnlinkSubscriptionInput,
): Promise<SubscriptionLifecycleResponse> {
  const subId = (input.subscriptionId ?? '').trim();
  if (!subId) {
    return { success: false, error: 'subscription_id is required' };
  }
  const sub = (await appDb('gocardless_subscriptions')
    .where({ subscription_id: subId })
    .first()) as unknown as SubscriptionRow | undefined;
  if (!sub) {
    return { success: false, error: `Subscription ${subId} not found` };
  }
  const doc = (input.sourceDoc ?? '').trim();
  if (doc) {
    const removed = await appDb('gocardless_subscription_documents')
      .where({ subscription_id: subId, source_doc: doc })
      .delete();
    if (!Number(removed)) {
      return {
        success: false,
        error: `Document ${doc} is not linked to this subscription`,
      };
    }
  } else {
    await appDb('gocardless_subscription_documents')
      .where({ subscription_id: subId })
      .delete();
  }
  const fresh = await getSubscription(appDb, subId);
  if (!fresh.success) {
    return { success: false, error: fresh.error };
  }
  return { success: true, subscription: fresh.subscription };
}
