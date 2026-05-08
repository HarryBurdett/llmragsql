/**
 * GoCardless mandate listing.
 *
 * Faithful port of:
 *   - list_gocardless_mandates       (apps/gocardless/api/routes.py:6404-6425)
 *   - list_unlinked_gocardless_mandates (routes.py:6428-6447)
 *
 * Reads the per-app DB's `gocardless_mandates` table.
 * `opera_account = '__UNLINKED__'` is the sentinel for mandates synced
 * from the GoCardless API but not yet linked to an Opera customer —
 * they appear in /unlinked endpoint for manual linking.
 *
 * The main /mandates list filters out __UNLINKED__ entries when there's
 * a linked version of the same mandate_id (deduplication for the case
 * where a sync creates an unlinked row and a later operator action
 * links it without removing the placeholder).
 */
import type { Knex } from 'knex';

export interface Mandate {
  id: number;
  mandate_id: string;
  opera_account: string;
  opera_name: string;
  gocardless_name: string;
  gocardless_customer_id: string;
  mandate_status: string;
  scheme: string;
  email: string;
  created_at: string;
  updated_at: string;
}

export interface ListMandatesOptions {
  status?: string | null;
  operaAccount?: string | null;
}

export interface ListMandatesResponse {
  success: boolean;
  mandates: Mandate[];
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

interface MandateRow {
  id: number;
  mandate_id: string | null;
  opera_account: string | null;
  opera_name: string | null;
  gocardless_name: string | null;
  gocardless_customer_id: string | null;
  mandate_status: string | null;
  scheme: string | null;
  email: string | null;
  created_at: Date | string | null;
  updated_at: Date | string | null;
}

function rowToMandate(r: MandateRow): Mandate {
  return {
    id: r.id,
    mandate_id: r.mandate_id ?? '',
    opera_account: r.opera_account ?? '',
    opera_name: r.opera_name ?? '',
    gocardless_name: r.gocardless_name ?? '',
    gocardless_customer_id: r.gocardless_customer_id ?? '',
    mandate_status: r.mandate_status ?? '',
    scheme: r.scheme ?? '',
    email: r.email ?? '',
    created_at: dateToIso(r.created_at),
    updated_at: dateToIso(r.updated_at),
  };
}

export async function listMandates(
  appDb: Knex,
  opts: ListMandatesOptions = {},
): Promise<ListMandatesResponse> {
  try {
    let query = appDb('gocardless_mandates');
    if (opts.status) {
      query = query.where({ mandate_status: opts.status });
    }
    if (opts.operaAccount) {
      query = query.where({ opera_account: opts.operaAccount });
    }
    const rows = (await query) as unknown as MandateRow[];

    // Dedup __UNLINKED__ entries when a linked version of the same
    // mandate_id exists. Same as Python.
    const linkedIds = new Set<string>();
    for (const r of rows) {
      if (r.opera_account && r.opera_account !== '__UNLINKED__') {
        linkedIds.add((r.mandate_id ?? '').trim());
      }
    }
    const filtered = rows.filter(
      (r) =>
        r.opera_account !== '__UNLINKED__' ||
        !linkedIds.has((r.mandate_id ?? '').trim()),
    );

    // Sort by opera_name (case-insensitive)
    filtered.sort((a, b) =>
      (a.opera_name ?? '').toLowerCase().localeCompare(
        (b.opera_name ?? '').toLowerCase(),
      ),
    );

    const mandates = filtered.map(rowToMandate);
    return { success: true, mandates, count: mandates.length };
  } catch (err: any) {
    return {
      success: false,
      mandates: [],
      count: 0,
      error: err?.message ?? String(err),
    };
  }
}

// ---------------------------------------------------------------------
// cancel — GoCardless API + local status update
// ---------------------------------------------------------------------

export interface CancelMandateResponse {
  success: boolean;
  message?: string;
  status?: string;
  error?: string;
}

export async function cancelMandate(
  appDb: Knex,
  mandateId: string,
  cancelRemote?: (
    id: string,
  ) => Promise<{ success: boolean; status?: string; error?: string; alreadyCancelled?: boolean }>,
): Promise<CancelMandateResponse> {
  const id = (mandateId ?? '').trim();
  if (!id) return { success: false, error: 'mandate_id is required' };

  // 1. Try GoCardless API cancel (when client passed). On failure
  //    don't update local — caller must retry.
  let gcStatus = 'cancelled';
  if (cancelRemote) {
    const r = await cancelRemote(id);
    if (!r.success) {
      return { success: false, error: r.error ?? 'Remote cancel failed' };
    }
    gcStatus = r.status ?? 'cancelled';
  }

  // 2. Update local mandate_status
  try {
    const updated = await appDb('gocardless_mandates')
      .where({ mandate_id: id })
      .update({
        mandate_status: gcStatus,
        updated_at: appDb.fn.now(),
      });
    if (!Number(updated)) {
      return { success: false, error: 'Mandate not found' };
    }
    return {
      success: true,
      message: `Mandate ${id} cancelled`,
      status: gcStatus,
    };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}

// ---------------------------------------------------------------------
// unlink — local-only, removes Opera linking
// ---------------------------------------------------------------------

export interface UnlinkMandateResponse {
  success: boolean;
  message?: string;
  error?: string;
}

export async function unlinkMandate(
  appDb: Knex,
  mandateId: string,
): Promise<UnlinkMandateResponse> {
  const id = (mandateId ?? '').trim();
  if (!id) return { success: false, error: 'mandate_id is required' };
  try {
    // Set opera_account to __UNLINKED__ to preserve the row's
    // existence (so future syncs don't try to re-create it). Don't
    // delete — mandate-level history matters for audit.
    const updated = await appDb('gocardless_mandates')
      .where({ mandate_id: id })
      .andWhere('opera_account', '!=', '__UNLINKED__')
      .update({
        opera_account: '__UNLINKED__',
        updated_at: appDb.fn.now(),
      });
    if (!Number(updated)) {
      return { success: false, error: 'Mandate not found' };
    }
    return { success: true, message: `Mandate ${id} unlinked` };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}

export async function listUnlinkedMandates(
  appDb: Knex,
): Promise<ListMandatesResponse> {
  try {
    const rows = (await appDb('gocardless_mandates').where({
      opera_account: '__UNLINKED__',
    })) as unknown as MandateRow[];

    rows.sort((a, b) =>
      (a.opera_name ?? '').toLowerCase().localeCompare(
        (b.opera_name ?? '').toLowerCase(),
      ),
    );

    const mandates = rows.map(rowToMandate);
    return { success: true, mandates, count: mandates.length };
  } catch (err: any) {
    return {
      success: false,
      mandates: [],
      count: 0,
      error: err?.message ?? String(err),
    };
  }
}
