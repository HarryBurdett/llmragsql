/**
 * Extended supplier contacts management.
 *
 * Stored in the per-app database (`supplier_contacts_ext`) — these
 * extend whatever Opera pname holds with role-tagged contacts
 * (e.g. "Accounts Payable", "Statement Sender", "Approver").
 *
 * Greenfield TS port — Python suppliers app didn't reach this feature.
 */
import type { Knex } from 'knex';

export interface SupplierContact {
  id: number;
  supplier_code: string;
  contact_email: string;
  contact_name: string;
  contact_role: string;
  updated_at: string;
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

export interface ListContactsResponse {
  success: boolean;
  contacts: SupplierContact[];
  count: number;
  error?: string;
}

export async function listContacts(
  appDb: Knex,
  supplierCode: string,
): Promise<ListContactsResponse> {
  try {
    const rows = (await appDb('supplier_contacts_ext')
      .where({ supplier_code: supplierCode })
      .orderBy('contact_role', 'asc')) as unknown as Array<{
      id: number;
      supplier_code: string;
      contact_email: string | null;
      contact_name: string | null;
      contact_role: string | null;
      updated_at: Date | string;
    }>;

    const contacts: SupplierContact[] = rows.map((r) => ({
      id: r.id,
      supplier_code: r.supplier_code,
      contact_email: r.contact_email ?? '',
      contact_name: r.contact_name ?? '',
      contact_role: r.contact_role ?? '',
      updated_at: dateToIso(r.updated_at),
    }));

    return { success: true, contacts, count: contacts.length };
  } catch (err: any) {
    return {
      success: false,
      contacts: [],
      count: 0,
      error: err?.message ?? String(err),
    };
  }
}

// ---------------------------------------------------------------------
// add
// ---------------------------------------------------------------------

export interface AddContactInput {
  supplier_code: string;
  contact_email: string;
  contact_name?: string;
  contact_role?: string;
}

export interface AddContactResponse {
  success: boolean;
  contact?: SupplierContact;
  error?: string;
}

export async function addContact(
  appDb: Knex,
  input: AddContactInput,
): Promise<AddContactResponse> {
  if (!input.supplier_code || !input.contact_email) {
    return {
      success: false,
      error: 'supplier_code and contact_email are required',
    };
  }
  // Basic email shape check — not authoritative, just a guard
  if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(input.contact_email)) {
    return { success: false, error: 'contact_email is not a valid email address' };
  }

  try {
    const inserted = await appDb('supplier_contacts_ext')
      .insert({
        supplier_code: input.supplier_code,
        contact_email: input.contact_email,
        contact_name: input.contact_name ?? '',
        contact_role: input.contact_role ?? '',
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
      contact: {
        id,
        supplier_code: input.supplier_code,
        contact_email: input.contact_email,
        contact_name: input.contact_name ?? '',
        contact_role: input.contact_role ?? '',
        updated_at: new Date().toISOString(),
      },
    };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}

// ---------------------------------------------------------------------
// delete
// ---------------------------------------------------------------------

export interface DeleteContactResponse {
  success: boolean;
  message?: string;
  error?: string;
}

export async function deleteContact(
  appDb: Knex,
  contactId: number,
): Promise<DeleteContactResponse> {
  try {
    const deleted = await appDb('supplier_contacts_ext').where({ id: contactId }).delete();
    if (deleted > 0) {
      return { success: true, message: 'Contact deleted' };
    }
    return { success: false, error: 'Contact not found' };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}
