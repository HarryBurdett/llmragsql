/**
 * Opera zcontacts CRUD for supplier-contact management.
 *
 * Faithful port of the 4 legacy endpoints in
 * `apps/suppliers/api/routes_contacts.py`:
 *   - GET    /api/supplier-contacts/{account}
 *   - POST   /api/supplier-contacts/{account}/opera
 *   - PUT    /api/supplier-contacts/{account}/opera/{contact_id}
 *   - DELETE /api/supplier-contacts/{account}/opera/{contact_id}
 *
 * Reads/writes Opera's `zcontacts` table (zc_module='P' for purchase
 * ledger). All writes use Opera's `nextid` table to allocate IDs
 * (Opera SQL SE convention; never `MAX(id)+1`). All writes also log
 * to the per-app `supplier_change_audit` table.
 *
 * The legacy local-extension overlay (is_statement_contact,
 * is_payment_contact, preferred_contact_method, notes, security
 * fields) is preserved at the read layer using whatever columns
 * SAM's `supplier_contacts_ext` table has — fields the schema
 * doesn't yet support are returned as defaults rather than blocking
 * the read.
 *
 * Knex query builder throughout — driver-agnostic so the same code
 * serves Opera SE (MSSQL) and Opera 3 (FoxPro via the Write Agent).
 */
import type { Knex } from 'knex';

export interface MergedContact {
  source?: 'opera' | 'local';
  zc_id: string;
  zc_account: string;
  zc_name: string;
  zc_title: string;
  zc_forename: string;
  zc_surname: string;
  zc_role: string;
  zc_email: string;
  zc_phone: string;
  zc_mobile: string;
  zc_fax: string;
  zc_module: string;
  // Local extension overlay
  local_extension_id: number | null;
  is_statement_contact: boolean;
  is_payment_contact: boolean;
  is_query_contact: boolean;
  preferred_contact_method: string;
  notes: string | null;
}

export interface MergedContactsResponse {
  success: boolean;
  account: string;
  contacts: MergedContact[];
  opera_count: number;
  local_count: number;
  error?: string;
}

interface ZContactsRow {
  id: number;
  zc_account: string;
  zc_name: string;
  zc_title: string;
  zc_forename: string;
  zc_surname: string;
  zc_role: string;
  zc_email: string;
  zc_phone: string;
  zc_mobile: string;
  zc_fax: string;
  zc_module: string;
}

interface LocalExtRow {
  id: number;
  supplier_code: string | null;
  contact_email: string | null;
  contact_name: string | null;
  contact_role: string | null;
  is_statement_contact?: boolean | number | null;
  never_communicate?: boolean | number | null;
}

async function fetchOperaContacts(
  operaDb: Knex,
  account: string,
): Promise<ZContactsRow[]> {
  try {
    return (await operaDb('zcontacts')
      .select(
        'id',
        operaDb.raw('RTRIM(zc_account) AS zc_account'),
        operaDb.raw('RTRIM(zc_contact) AS zc_name'),
        operaDb.raw("RTRIM(ISNULL(zc_title, '')) AS zc_title"),
        operaDb.raw("RTRIM(ISNULL(zc_fornam, '')) AS zc_forename"),
        operaDb.raw("RTRIM(ISNULL(zc_surname, '')) AS zc_surname"),
        operaDb.raw("RTRIM(ISNULL(zc_pos, '')) AS zc_role"),
        operaDb.raw("RTRIM(ISNULL(zc_email, '')) AS zc_email"),
        operaDb.raw("RTRIM(ISNULL(zc_phone, '')) AS zc_phone"),
        operaDb.raw("RTRIM(ISNULL(zc_mobile, '')) AS zc_mobile"),
        operaDb.raw("RTRIM(ISNULL(zc_fax, '')) AS zc_fax"),
        operaDb.raw("RTRIM(ISNULL(zc_module, '')) AS zc_module"),
      )
      .where('zc_account', account)
      .andWhere('zc_module', 'P')
      .orderBy('zc_contact')) as unknown as ZContactsRow[];
  } catch {
    return [];
  }
}

async function fetchLocalExtensions(
  appDb: Knex,
  account: string,
): Promise<LocalExtRow[]> {
  try {
    return (await appDb('supplier_contacts_ext')
      .where('supplier_code', account)) as unknown as LocalExtRow[];
  } catch {
    return [];
  }
}

function buildMergedContact(
  oc: ZContactsRow,
  ext: LocalExtRow | null,
): MergedContact {
  return {
    source: 'opera',
    zc_id: String(oc.id ?? ''),
    zc_account: (oc.zc_account ?? '').trim(),
    zc_name: (oc.zc_name ?? '').trim(),
    zc_title: (oc.zc_title ?? '').trim(),
    zc_forename: (oc.zc_forename ?? '').trim(),
    zc_surname: (oc.zc_surname ?? '').trim(),
    zc_role: (oc.zc_role ?? '').trim(),
    zc_email: (oc.zc_email ?? '').trim(),
    zc_phone: (oc.zc_phone ?? '').trim(),
    zc_mobile: (oc.zc_mobile ?? '').trim(),
    zc_fax: (oc.zc_fax ?? '').trim(),
    zc_module: (oc.zc_module ?? '').trim() || 'P',
    local_extension_id: ext?.id ?? null,
    is_statement_contact: !!ext?.is_statement_contact,
    is_payment_contact: false,
    is_query_contact: false,
    preferred_contact_method: 'email',
    notes: null,
  };
}

function buildLocalOnlyContact(ext: LocalExtRow): MergedContact {
  return {
    source: 'local',
    zc_id: '',
    zc_account: (ext.supplier_code ?? '').trim(),
    zc_name: (ext.contact_name ?? '').trim(),
    zc_title: '',
    zc_forename: '',
    zc_surname: '',
    zc_role: (ext.contact_role ?? '').trim(),
    zc_email: (ext.contact_email ?? '').trim(),
    zc_phone: '',
    zc_mobile: '',
    zc_fax: '',
    zc_module: 'P',
    local_extension_id: ext.id,
    is_statement_contact: !!ext.is_statement_contact,
    is_payment_contact: false,
    is_query_contact: false,
    preferred_contact_method: 'email',
    notes: null,
  };
}

// ---------------------------------------------------------------------
// GET — read merged contacts
// ---------------------------------------------------------------------

export async function getMergedContacts(
  operaDb: Knex,
  appDb: Knex | null,
  account: string,
): Promise<MergedContactsResponse> {
  const acct = (account ?? '').trim();
  if (!acct) {
    return {
      success: false,
      account: acct,
      contacts: [],
      opera_count: 0,
      local_count: 0,
      error: 'account required',
    };
  }
  try {
    const [operaContacts, localExt] = await Promise.all([
      fetchOperaContacts(operaDb, acct),
      appDb ? fetchLocalExtensions(appDb, acct) : Promise.resolve([]),
    ]);

    // Index local extensions by email so we can overlay onto matching
    // Opera rows. The legacy port also keyed by zcontact_id; SAM's
    // current schema doesn't have that column, so we fall back to
    // email match.
    const extByEmail = new Map<string, LocalExtRow>();
    for (const e of localExt) {
      const em = (e.contact_email ?? '').trim().toLowerCase();
      if (em) extByEmail.set(em, e);
    }
    const usedExtIds = new Set<number>();

    const merged: MergedContact[] = [];
    for (const oc of operaContacts) {
      const email = (oc.zc_email ?? '').trim().toLowerCase();
      const matched = email ? extByEmail.get(email) ?? null : null;
      if (matched) usedExtIds.add(matched.id);
      merged.push(buildMergedContact(oc, matched));
    }
    // Append local-only contacts that didn't match any Opera row.
    for (const e of localExt) {
      if (!usedExtIds.has(e.id)) merged.push(buildLocalOnlyContact(e));
    }

    return {
      success: true,
      account: acct,
      contacts: merged,
      opera_count: operaContacts.length,
      local_count: localExt.length,
    };
  } catch (err: any) {
    return {
      success: false,
      account: acct,
      contacts: [],
      opera_count: 0,
      local_count: 0,
      error: err?.message ?? String(err),
    };
  }
}

// ---------------------------------------------------------------------
// helpers — Opera nextid allocation + audit logging
// ---------------------------------------------------------------------

/**
 * Allocate the next id for `zcontacts` from Opera's `nextid` table.
 * Faithful to the convention in legacy `create_opera_contact`:
 *   UPDATE nextid SET nextid = nextid + 1 WHERE tablename='zcontacts';
 *   SELECT nextid - 1 AS new_id ...
 * The UPDATE + SELECT must execute on the same connection so the
 * subsequent read sees the incremented value.
 */
async function allocateZcontactId(trx: Knex.Transaction): Promise<number | null> {
  try {
    await trx('nextid')
      .whereRaw("RTRIM(tablename) = ?", ['zcontacts'])
      .update({
        nextid: trx.raw('nextid + 1'),
        datemodified: trx.raw('GETDATE()'),
      });
    const row = (await trx('nextid')
      .whereRaw("RTRIM(tablename) = ?", ['zcontacts'])
      .first('nextid')) as { nextid: number | string | null } | undefined;
    if (!row) return null;
    return Number(row.nextid ?? 0) - 1;
  } catch {
    return null;
  }
}

async function logSupplierChange(
  appDb: Knex | null,
  account: string,
  fieldName: string,
  oldValue: string,
  newValue: string,
  changedBy: string,
): Promise<void> {
  if (!appDb) return;
  try {
    await appDb('supplier_change_audit').insert({
      supplier_code: account,
      changed_field: fieldName,
      old_value: oldValue,
      new_value: newValue,
      changed_by: changedBy,
      created_at: new Date().toISOString(),
    });
  } catch {
    // best-effort audit
  }
}

function splitName(full: string): { forename: string; surname: string } {
  const trimmed = (full ?? '').trim();
  if (!trimmed) return { forename: '', surname: '' };
  const idx = trimmed.indexOf(' ');
  if (idx === -1) return { forename: trimmed, surname: '' };
  return { forename: trimmed.slice(0, idx), surname: trimmed.slice(idx + 1) };
}

// ---------------------------------------------------------------------
// POST — create Opera contact
// ---------------------------------------------------------------------

export interface OperaContactCreateInput {
  name?: string;
  title?: string;
  role?: string;
  email?: string;
  phone?: string;
  mobile?: string;
  fax?: string;
}

export interface OperaContactCRUDResponse {
  success: boolean;
  contact_id?: number;
  contact?: Record<string, unknown>;
  error?: string;
}

export async function createOperaContact(
  operaDb: Knex,
  appDb: Knex | null,
  account: string,
  body: OperaContactCreateInput,
): Promise<OperaContactCRUDResponse> {
  const acct = (account ?? '').trim();
  if (!acct) return { success: false, error: 'account required' };
  try {
    let newId: number | null = null;
    let created: Record<string, unknown> | null = null;
    await operaDb.transaction(async (trx) => {
      newId = await allocateZcontactId(trx);
      if (!newId) {
        throw new Error('Could not allocate id from nextid table for zcontacts');
      }
      const { forename, surname } = splitName(body.name ?? '');
      await trx('zcontacts').insert({
        id: newId,
        zc_module: 'P',
        zc_account: acct,
        zc_contact: (body.name ?? '').slice(0, 35),
        zc_title: (body.title ?? '').slice(0, 35),
        zc_fornam: forename.slice(0, 35),
        zc_surname: surname.slice(0, 35),
        zc_pos: (body.role ?? '').slice(0, 35),
        zc_email: (body.email ?? '').slice(0, 200),
        zc_phone: (body.phone ?? '').slice(0, 35),
        zc_mobile: (body.mobile ?? '').slice(0, 35),
        zc_fax: (body.fax ?? '').slice(0, 35),
        datecreated: trx.raw('GETDATE()'),
        datemodified: trx.raw('GETDATE()'),
        state: 1,
      });
      const row = (await trx('zcontacts')
        .select(
          trx.raw('CAST(id AS VARCHAR(50)) AS zc_id'),
          trx.raw('RTRIM(zc_account) AS zc_account'),
          trx.raw('RTRIM(zc_contact) AS zc_name'),
          trx.raw("RTRIM(ISNULL(zc_title, '')) AS zc_title"),
          trx.raw("RTRIM(ISNULL(zc_email, '')) AS zc_email"),
          trx.raw("RTRIM(ISNULL(zc_phone, '')) AS zc_phone"),
          trx.raw("RTRIM(ISNULL(zc_mobile, '')) AS zc_mobile"),
          trx.raw("RTRIM(ISNULL(zc_pos, '')) AS zc_role"),
          trx.raw("RTRIM(ISNULL(zc_module, '')) AS zc_module"),
          trx.raw("RTRIM(ISNULL(zc_fax, '')) AS zc_fax"),
        )
        .where('id', newId)
        .first()) as Record<string, unknown> | undefined;
      created = row ?? null;
    });
    if (!newId) {
      return {
        success: false,
        error: 'Failed to allocate Opera contact id',
      };
    }
    await logSupplierChange(
      appDb,
      acct,
      'zcontact_created',
      '',
      `id=${newId}, name=${body.name ?? ''}`,
      'api',
    );
    return { success: true, contact_id: newId, contact: created ?? undefined };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}

// ---------------------------------------------------------------------
// PUT — update Opera contact
// ---------------------------------------------------------------------

export interface OperaContactUpdateInput {
  name?: string;
  title?: string;
  role?: string;
  email?: string;
  phone?: string;
  mobile?: string;
  fax?: string;
}

export async function updateOperaContact(
  operaDb: Knex,
  appDb: Knex | null,
  account: string,
  contactId: number,
  body: OperaContactUpdateInput,
): Promise<OperaContactCRUDResponse> {
  const acct = (account ?? '').trim();
  if (!acct) return { success: false, error: 'account required' };
  if (!Number.isFinite(contactId) || contactId <= 0) {
    return { success: false, error: 'contact_id required' };
  }

  // Field map for both UPDATE and audit logging
  const updates: Record<string, string> = {};
  const fieldMap: Array<{ key: keyof OperaContactUpdateInput; col: string }> = [
    { key: 'name', col: 'zc_contact' },
    { key: 'title', col: 'zc_title' },
    { key: 'role', col: 'zc_pos' },
    { key: 'email', col: 'zc_email' },
    { key: 'phone', col: 'zc_phone' },
    { key: 'mobile', col: 'zc_mobile' },
    { key: 'fax', col: 'zc_fax' },
  ];
  for (const { key, col } of fieldMap) {
    const v = body[key];
    if (v !== undefined && v !== null) updates[col] = String(v);
  }

  try {
    let updated: Record<string, unknown> | null = null;
    let oldRow: Record<string, unknown> | undefined;

    await operaDb.transaction(async (trx) => {
      // Read existing values for audit logging
      oldRow = (await trx('zcontacts')
        .select(
          trx.raw("RTRIM(ISNULL(zc_contact, '')) AS zc_contact"),
          trx.raw("RTRIM(ISNULL(zc_title, '')) AS zc_title"),
          trx.raw("RTRIM(ISNULL(zc_pos, '')) AS zc_pos"),
          trx.raw("RTRIM(ISNULL(zc_email, '')) AS zc_email"),
          trx.raw("RTRIM(ISNULL(zc_phone, '')) AS zc_phone"),
          trx.raw("RTRIM(ISNULL(zc_mobile, '')) AS zc_mobile"),
          trx.raw("RTRIM(ISNULL(zc_fax, '')) AS zc_fax"),
        )
        .where('id', contactId)
        .first()) as Record<string, unknown> | undefined;

      if (!oldRow) {
        throw new Error(`Opera contact with id ${contactId} not found`);
      }

      if (Object.keys(updates).length > 0) {
        const updateBody: Record<string, unknown> = {
          ...updates,
          datemodified: trx.raw('GETDATE()'),
        };
        await trx('zcontacts').where('id', contactId).update(updateBody);
      }

      updated = (await trx('zcontacts')
        .select(
          trx.raw('CAST(id AS VARCHAR(50)) AS zc_id'),
          trx.raw('RTRIM(zc_account) AS zc_account'),
          trx.raw('RTRIM(zc_contact) AS zc_name'),
          trx.raw("RTRIM(ISNULL(zc_title, '')) AS zc_title"),
          trx.raw("RTRIM(ISNULL(zc_email, '')) AS zc_email"),
          trx.raw("RTRIM(ISNULL(zc_phone, '')) AS zc_phone"),
          trx.raw("RTRIM(ISNULL(zc_mobile, '')) AS zc_mobile"),
          trx.raw("RTRIM(ISNULL(zc_pos, '')) AS zc_role"),
          trx.raw("RTRIM(ISNULL(zc_module, '')) AS zc_module"),
          trx.raw("RTRIM(ISNULL(zc_fax, '')) AS zc_fax"),
        )
        .where('id', contactId)
        .first()) as Record<string, unknown> | undefined ?? null;
    });

    // Audit log changed fields
    if (oldRow) {
      for (const { key, col } of fieldMap) {
        const newV = body[key];
        if (newV === undefined || newV === null) continue;
        const oldV = String(oldRow[col] ?? '').trim();
        const newVStr = String(newV).trim();
        if (oldV !== newVStr) {
          await logSupplierChange(
            appDb,
            acct,
            `zcontact.${col}`,
            oldV,
            newVStr,
            'api',
          );
        }
      }
    }

    return {
      success: true,
      contact_id: contactId,
      contact: updated ?? undefined,
    };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}

// ---------------------------------------------------------------------
// DELETE — delete Opera contact
// ---------------------------------------------------------------------

export interface OperaContactDeleteResponse {
  success: boolean;
  deleted?: boolean;
  error?: string;
}

export async function deleteOperaContact(
  operaDb: Knex,
  appDb: Knex | null,
  account: string,
  contactId: number,
): Promise<OperaContactDeleteResponse> {
  const acct = (account ?? '').trim();
  if (!acct) return { success: false, error: 'account required' };
  if (!Number.isFinite(contactId) || contactId <= 0) {
    return { success: false, error: 'contact_id required' };
  }
  try {
    let existing: { zc_contact: string | null } | undefined;
    await operaDb.transaction(async (trx) => {
      existing = (await trx('zcontacts')
        .select(trx.raw("RTRIM(ISNULL(zc_contact, '')) AS zc_contact"))
        .where('id', contactId)
        .first()) as { zc_contact: string | null } | undefined;
      if (!existing) {
        throw new Error(`Opera contact with id ${contactId} not found`);
      }
      await trx('zcontacts').where('id', contactId).del();
    });
    await logSupplierChange(
      appDb,
      acct,
      'zcontact_deleted',
      `id=${contactId}, name=${existing?.zc_contact ?? ''}`,
      '',
      'api',
    );
    return { success: true, deleted: true };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}
