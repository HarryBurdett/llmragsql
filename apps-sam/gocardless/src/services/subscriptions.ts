/**
 * GoCardless subscription listing.
 *
 * Reads from the per-app DB's `gocardless_subscriptions` table and
 * enriches each row with customer_name from the matching mandate.
 *
 * Greenfield SAM-app endpoint. The Python source has only the Opera 3
 * variant (`opera3_list_subscriptions` in routes.py:5613) which
 * additionally reads ihead/itran from the FoxPro DB to detect
 * GC-vs-Opera mismatches. The Opera-side mismatch detection requires
 * the full Opera SE schema for ihead/itran which lands as part of the
 * upcoming `subscription-mismatch` endpoint port.
 *
 * For now this is a clean SE list with name enrichment — useful for
 * the dashboard and for the partial mismatch report (which can be
 * computed by the frontend against the GC-only data).
 */
import type { Knex } from 'knex';

export interface Subscription {
  id: number;
  subscription_id: string;
  mandate_id: string;
  opera_account: string;
  amount: number;
  frequency: string;
  status: string;
  created_at: string;
  updated_at: string;
  customer_name: string;
}

export interface ListSubscriptionsOptions {
  status?: string | null;
  operaAccount?: string | null;
  limit?: number;
}

export interface ListSubscriptionsResponse {
  success: boolean;
  subscriptions: Subscription[];
  count: number;
  error?: string;
}

function dateToIso(d: Date | string | null): string {
  if (!d) return '';
  if (d instanceof Date) {
    if (Number.isNaN(d.getTime())) return '';
    return d.toISOString();
  }
  return String(d);
}

export async function listSubscriptions(
  appDb: Knex,
  opts: ListSubscriptionsOptions = {},
): Promise<ListSubscriptionsResponse> {
  try {
    const limit = opts.limit ?? 200;
    let query = appDb('gocardless_subscriptions')
      .orderBy('created_at', 'desc')
      .limit(limit);
    if (opts.status) {
      query = query.where({ status: opts.status });
    }
    if (opts.operaAccount) {
      query = query.where({ opera_account: opts.operaAccount });
    }

    const rows = (await query) as unknown as Array<{
      id: number;
      subscription_id: string | null;
      mandate_id: string | null;
      opera_account: string | null;
      amount: number | string | null;
      frequency: string | null;
      status: string | null;
      created_at: Date | string | null;
      updated_at: Date | string | null;
    }>;

    const accounts = Array.from(
      new Set(rows.map((r) => (r.opera_account ?? '').trim()).filter(Boolean)),
    );
    let mandateNames = new Map<string, string>();
    if (accounts.length > 0) {
      try {
        const mandates = (await appDb('gocardless_mandates')
          .whereIn('opera_account', accounts)
          .select('opera_account', 'opera_name')) as unknown as Array<{
          opera_account: string | null;
          opera_name: string | null;
        }>;
        for (const m of mandates ?? []) {
          const acct = (m.opera_account ?? '').trim();
          if (acct) mandateNames.set(acct, (m.opera_name ?? '').trim());
        }
      } catch {
        // best-effort
        mandateNames = new Map();
      }
    }

    const subscriptions: Subscription[] = rows.map((r) => {
      const acct = (r.opera_account ?? '').trim();
      return {
        id: r.id,
        subscription_id: r.subscription_id ?? '',
        mandate_id: r.mandate_id ?? '',
        opera_account: acct,
        amount: Number(r.amount ?? 0),
        frequency: r.frequency ?? '',
        status: r.status ?? '',
        created_at: dateToIso(r.created_at),
        updated_at: dateToIso(r.updated_at),
        customer_name: mandateNames.get(acct) || acct,
      };
    });

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
