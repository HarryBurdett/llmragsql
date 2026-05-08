/**
 * GoCardless partner-portal helpers.
 *
 * Faithful port of the partner endpoints in
 * `apps/gocardless/api/routes.py:1322-1522` and the supporting
 * GoCardlessPaymentsDB methods in `sql_rag/gocardless_payments.py`.
 *
 * Three read endpoints + admin-auth helpers:
 *   - GET  /api/gocardless/partner/config           (partner-configured probe)
 *   - GET  /api/gocardless/partner/signup-status    (latest signup, token stripped)
 *   - GET  /api/gocardless/partner/merchants        (all signups, tokens stripped)
 *   - POST /api/gocardless/partner/admin-auth       (admin-password gate)
 *   - PUT  /api/gocardless/partner/admin-password   (set/change admin password)
 *
 * Token redaction: `merchant_access_token` is NEVER returned to the
 * frontend — the response gets a `has_token: bool` instead.
 */
import type { Knex } from 'knex';
import {
  loadSettings,
  saveSettings,
  type GoCardlessSettings,
} from './settings.js';

// ---------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------

export interface PartnerConfigResponse {
  success: boolean;
  partner_configured: boolean;
  partner_sandbox: boolean;
  redirect_uri: string;
  error?: string;
}

export interface PartnerSignup {
  id: number;
  company_name: string | null;
  company_email: string | null;
  billing_request_id: string | null;
  billing_request_flow_id: string | null;
  authorisation_url: string | null;
  status: string;
  status_detail: string | null;
  access_token_obtained: boolean;
  merchant_organisation_id: string | null;
  merchant_creditor_name: string | null;
  merchant_app_url: string | null;
  partner_referral_id: string | null;
  created_at: string;
  completed_at: string | null;
  updated_at: string | null;
  /** True when merchant_access_token is set; the token itself is never returned. */
  has_token: boolean;
}

export interface SignupStatusResponse {
  success: boolean;
  signup: PartnerSignup | null;
  error?: string;
}

export interface MerchantsResponse {
  success: boolean;
  merchants: PartnerSignup[];
  error?: string;
}

export interface AdminAuthResponse {
  success: boolean;
  first_time?: boolean;
  error?: string;
}

export interface AdminPasswordResponse {
  success: boolean;
  error?: string;
}

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

interface SignupRow {
  id: number;
  company_name: string | null;
  company_email: string | null;
  billing_request_id: string | null;
  billing_request_flow_id: string | null;
  authorisation_url: string | null;
  status: string | null;
  status_detail: string | null;
  access_token_obtained: number | boolean | null;
  merchant_access_token: string | null;
  merchant_organisation_id: string | null;
  merchant_creditor_name: string | null;
  merchant_app_url: string | null;
  partner_referral_id: string | null;
  created_at: Date | string;
  completed_at: Date | string | null;
  updated_at: Date | string | null;
}

function toIso(d: Date | string | null | undefined): string | null {
  if (!d) return null;
  if (d instanceof Date) return d.toISOString();
  return String(d);
}

function rowToSignup(row: SignupRow): PartnerSignup {
  return {
    id: row.id,
    company_name: row.company_name ?? null,
    company_email: row.company_email ?? null,
    billing_request_id: row.billing_request_id ?? null,
    billing_request_flow_id: row.billing_request_flow_id ?? null,
    authorisation_url: row.authorisation_url ?? null,
    status: row.status ?? 'pending',
    status_detail: row.status_detail ?? null,
    access_token_obtained: !!row.access_token_obtained,
    merchant_organisation_id: row.merchant_organisation_id ?? null,
    merchant_creditor_name: row.merchant_creditor_name ?? null,
    merchant_app_url: row.merchant_app_url ?? null,
    partner_referral_id: row.partner_referral_id ?? null,
    created_at: toIso(row.created_at) ?? '',
    completed_at: toIso(row.completed_at),
    updated_at: toIso(row.updated_at),
    has_token: !!(row.merchant_access_token && row.merchant_access_token.length > 0),
  };
}

// ---------------------------------------------------------------------
// GET /api/gocardless/partner/config
// ---------------------------------------------------------------------

export interface GetPartnerConfigOptions {
  /**
   * Origin/base URL the request came from, used to build the redirect_uri
   * fallback when no explicit `partner_redirect_uri` is configured.
   * Mirrors Python's `request.base_url` usage.
   */
  baseUrl?: string;
}

export async function getPartnerConfig(
  appDb: Knex,
  opts: GetPartnerConfigOptions = {},
): Promise<PartnerConfigResponse> {
  try {
    const settings = await loadSettings(appDb);
    const hasPartner = !!(
      settings.partner_client_id && settings.partner_client_secret
    );
    let redirectUri = (settings.partner_redirect_uri ?? '').trim();
    if (!redirectUri) {
      const base = (opts.baseUrl ?? '').replace(/\/+$/, '');
      redirectUri = `${base}/api/gocardless/partner/callback`;
    }
    return {
      success: true,
      partner_configured: hasPartner,
      partner_sandbox: !!settings.api_sandbox,
      redirect_uri: redirectUri,
    };
  } catch (err: any) {
    return {
      success: false,
      partner_configured: false,
      partner_sandbox: false,
      redirect_uri: '',
      error: err?.message ?? String(err),
    };
  }
}

// ---------------------------------------------------------------------
// GET /api/gocardless/partner/signup-status
// ---------------------------------------------------------------------

export async function getLatestPartnerSignup(
  appDb: Knex,
): Promise<SignupStatusResponse> {
  try {
    const row = (await appDb('gocardless_partner_signups')
      .orderBy('id', 'desc')
      .first()) as SignupRow | undefined;
    if (!row) {
      return { success: true, signup: null };
    }
    return { success: true, signup: rowToSignup(row) };
  } catch (err: any) {
    return { success: false, signup: null, error: err?.message ?? String(err) };
  }
}

// ---------------------------------------------------------------------
// GET /api/gocardless/partner/merchants
// ---------------------------------------------------------------------

export async function getAllMerchantSignups(
  appDb: Knex,
  opts: { status?: string | null } = {},
): Promise<MerchantsResponse> {
  try {
    let query = appDb('gocardless_partner_signups').orderBy('id', 'desc');
    if (opts.status) {
      query = query.where({ status: opts.status });
    }
    const rows = (await query) as unknown as SignupRow[];
    return { success: true, merchants: rows.map(rowToSignup) };
  } catch (err: any) {
    return { success: false, merchants: [], error: err?.message ?? String(err) };
  }
}

// ---------------------------------------------------------------------
// POST /api/gocardless/partner/admin-auth
// ---------------------------------------------------------------------

export async function partnerAdminAuth(
  appDb: Knex,
  password: string,
): Promise<AdminAuthResponse> {
  try {
    const settings = await loadSettings(appDb);
    const stored = (
      (settings as GoCardlessSettings & { partner_admin_password?: string })
        .partner_admin_password ?? ''
    ).trim();
    if (!stored) {
      // First-time access — allow so the operator can set a password
      return { success: true, first_time: true };
    }
    if ((password ?? '').trim() === stored) {
      return { success: true };
    }
    return { success: false, error: 'Incorrect password' };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}

// ---------------------------------------------------------------------
// PUT /api/gocardless/partner/admin-password
// ---------------------------------------------------------------------

export async function setPartnerAdminPassword(
  appDb: Knex,
  newPassword: string,
): Promise<AdminPasswordResponse> {
  const trimmed = (newPassword ?? '').trim();
  if (!trimmed || trimmed.length < 4) {
    return { success: false, error: 'Password must be at least 4 characters' };
  }
  try {
    const settings = await loadSettings(appDb);
    const merged = {
      ...settings,
      partner_admin_password: trimmed,
    } as GoCardlessSettings & { partner_admin_password: string };
    const ok = await saveSettings(appDb, merged);
    if (!ok) return { success: false, error: 'Failed to save' };
    return { success: true };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}
