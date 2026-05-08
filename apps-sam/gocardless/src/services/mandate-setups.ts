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
