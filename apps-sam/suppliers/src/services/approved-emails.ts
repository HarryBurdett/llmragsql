/**
 * Per-supplier approved-sender email management.
 *
 * Statements only get auto-processed if the sending email address
 * matches an approved entry — protects against spoofed statements.
 *
 * Stored in `supplier_approved_emails` (per-app DB, provisioned by
 * migration 001). Greenfield TS port.
 */
import type { Knex } from 'knex';

export interface ApprovedEmail {
  id: number;
  supplier_code: string;
  email_address: string;
  approved_at: string;
}

function dateToIso(d: Date | string | null): string {
  if (!d) return '';
  if (d instanceof Date) {
    if (Number.isNaN(d.getTime())) return '';
    return d.toISOString();
  }
  return String(d);
}

// ---------------------------------------------------------------------
// list
// ---------------------------------------------------------------------

export interface ListApprovedEmailsResponse {
  success: boolean;
  emails: ApprovedEmail[];
  count: number;
  error?: string;
}

export async function listApprovedEmails(
  appDb: Knex,
  supplierCode: string,
): Promise<ListApprovedEmailsResponse> {
  try {
    const rows = (await appDb('supplier_approved_emails')
      .where({ supplier_code: supplierCode })
      .orderBy('approved_at', 'desc')) as unknown as Array<{
      id: number;
      supplier_code: string;
      email_address: string;
      approved_at: Date | string;
    }>;

    const emails: ApprovedEmail[] = rows.map((r) => ({
      id: r.id,
      supplier_code: r.supplier_code,
      email_address: r.email_address,
      approved_at: dateToIso(r.approved_at),
    }));

    return { success: true, emails, count: emails.length };
  } catch (err: any) {
    return {
      success: false,
      emails: [],
      count: 0,
      error: err?.message ?? String(err),
    };
  }
}

// ---------------------------------------------------------------------
// approve
// ---------------------------------------------------------------------

export interface ApproveEmailInput {
  supplier_code: string;
  email_address: string;
}

export interface ApproveEmailResponse {
  success: boolean;
  approved?: ApprovedEmail;
  message?: string;
  error?: string;
}

export async function approveEmail(
  appDb: Knex,
  input: ApproveEmailInput,
): Promise<ApproveEmailResponse> {
  if (!input.supplier_code || !input.email_address) {
    return {
      success: false,
      error: 'supplier_code and email_address are required',
    };
  }
  if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(input.email_address)) {
    return { success: false, error: 'email_address is not a valid email' };
  }

  // Idempotent — if already approved, return the existing row.
  try {
    const existing = (await appDb('supplier_approved_emails')
      .where({
        supplier_code: input.supplier_code,
        email_address: input.email_address,
      })
      .first()) as
      | { id: number; supplier_code: string; email_address: string; approved_at: Date | string }
      | undefined;

    if (existing) {
      return {
        success: true,
        message: 'Email already approved',
        approved: {
          id: existing.id,
          supplier_code: existing.supplier_code,
          email_address: existing.email_address,
          approved_at: dateToIso(existing.approved_at),
        },
      };
    }

    const inserted = await appDb('supplier_approved_emails')
      .insert({
        supplier_code: input.supplier_code,
        email_address: input.email_address,
      })
      .returning('id');

    const id =
      Array.isArray(inserted) && inserted.length > 0
        ? typeof inserted[0] === 'object'
          ? (inserted[0] as { id: number }).id
          : Number(inserted[0])
        : 0;

    return {
      success: true,
      approved: {
        id,
        supplier_code: input.supplier_code,
        email_address: input.email_address,
        approved_at: new Date().toISOString(),
      },
    };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}

// ---------------------------------------------------------------------
// revoke
// ---------------------------------------------------------------------

export interface RevokeEmailResponse {
  success: boolean;
  message?: string;
  error?: string;
}

export async function revokeEmail(
  appDb: Knex,
  recordId: number,
): Promise<RevokeEmailResponse> {
  try {
    const deleted = await appDb('supplier_approved_emails')
      .where({ id: recordId })
      .delete();
    if (deleted > 0) {
      return { success: true, message: 'Approved email revoked' };
    }
    return { success: false, error: 'Approved-email record not found' };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}

/**
 * Convenience: check whether a sender is approved for a supplier.
 * Used by the inbox-scanning logic before auto-processing a statement.
 */
export async function isEmailApproved(
  appDb: Knex,
  supplierCode: string,
  emailAddress: string,
): Promise<boolean> {
  try {
    const row = await appDb('supplier_approved_emails')
      .where({
        supplier_code: supplierCode,
        email_address: emailAddress,
      })
      .first();
    return !!row;
  } catch {
    return false;
  }
}
