/**
 * Payment-request listing for the GoCardless dashboard.
 *
 * Faithful port of `list_payment_requests`
 * (apps/gocardless/api/routes.py:8217-8246).
 *
 * Reads from the per-app DB's `gocardless_payment_requests` table
 * and enriches each row with customer_name from the matching mandate.
 *
 * Filters: status, opera_account. Default limit 100.
 */
import type { Knex } from 'knex';

export interface PaymentRequest {
  id: number;
  payment_id: string;
  mandate_id: string;
  opera_account: string;
  amount: number; // pounds (DECIMAL in schema)
  amount_pence: number | null;
  currency: string;
  status: string;
  reference: string;
  charge_date: string;
  payout_id: string;
  invoice_refs: string;
  opera_receipt_ref: string;
  error_message: string;
  created_at: string;
  updated_at: string;
  customer_name: string;
}

export interface ListPaymentRequestsOptions {
  status?: string | null;
  operaAccount?: string | null;
  limit?: number;
}

export interface ListPaymentRequestsResponse {
  success: boolean;
  requests: PaymentRequest[];
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

export async function listPaymentRequests(
  appDb: Knex,
  opts: ListPaymentRequestsOptions = {},
): Promise<ListPaymentRequestsResponse> {
  try {
    const limit = opts.limit ?? 100;
    let query = appDb('gocardless_payment_requests')
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
      payment_id: string | null;
      mandate_id: string | null;
      opera_account: string | null;
      amount: number | string | null;
      amount_pence: number | null;
      currency: string | null;
      status: string | null;
      reference: string | null;
      charge_date: Date | string | null;
      payout_id: string | null;
      invoice_refs: string | null;
      opera_receipt_ref: string | null;
      error_message: string | null;
      created_at: Date | string | null;
      updated_at: Date | string | null;
    }>;

    // Enrich with customer name from mandates (one-shot lookup)
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
        // Best-effort enrichment
        mandateNames = new Map();
      }
    }

    const requests: PaymentRequest[] = rows.map((r) => {
      const acct = (r.opera_account ?? '').trim();
      const customerName = mandateNames.get(acct) || acct;
      return {
        id: r.id,
        payment_id: r.payment_id ?? '',
        mandate_id: r.mandate_id ?? '',
        opera_account: acct,
        amount: Number(r.amount ?? 0),
        amount_pence: r.amount_pence != null ? Number(r.amount_pence) : null,
        currency: r.currency ?? 'GBP',
        status: r.status ?? '',
        reference: r.reference ?? '',
        charge_date: dateToIso(r.charge_date),
        payout_id: r.payout_id ?? '',
        invoice_refs: r.invoice_refs ?? '',
        opera_receipt_ref: r.opera_receipt_ref ?? '',
        error_message: r.error_message ?? '',
        created_at: dateToIso(r.created_at),
        updated_at: dateToIso(r.updated_at),
        customer_name: customerName,
      };
    });

    return { success: true, requests, count: requests.length };
  } catch (err: any) {
    return {
      success: false,
      requests: [],
      count: 0,
      error: err?.message ?? String(err),
    };
  }
}
