/**
 * Supplier listing — read Opera pname for the dropdown / supplier picker.
 *
 * Read-only against Opera. NO writes to pname (third-party system).
 *
 * The Python suppliers app is incomplete; this TS implementation is
 * the source of truth for new supplier features going forward.
 */
import type { Knex } from 'knex';

export interface SupplierSummary {
  account: string;
  name: string;
  current_balance: number;
  dormant: boolean;
  email: string;
  phone: string;
}

export interface SuppliersListResponse {
  success: boolean;
  suppliers: SupplierSummary[];
  count: number;
  error?: string;
}

/**
 * List active suppliers from Opera pname.
 *
 * Excludes dormant accounts by default (per CLAUDE.md: "Dormant accounts
 * excluded from matching — pn_dormant = 0 filter on supplier queries").
 */
export async function listSuppliers(
  operaDb: Knex,
  opts: { includeDormant?: boolean } = {},
): Promise<SuppliersListResponse> {
  try {
    const includeDormant = opts.includeDormant ?? false;
    const dormantClause = includeDormant ? '' : 'WHERE pn_dormant = 0';

    const sql = `
      SELECT
        RTRIM(pn_account) AS account,
        RTRIM(pn_name) AS name,
        pn_currbal AS current_balance,
        pn_dormant AS dormant,
        RTRIM(ISNULL(pn_emailadd, '')) AS email,
        RTRIM(ISNULL(pn_telno, '')) AS phone
      FROM pname WITH (NOLOCK)
      ${dormantClause}
      ORDER BY pn_account
    `;

    const rows = (await operaDb.raw(sql)) as unknown as Array<{
      account: string | null;
      name: string | null;
      current_balance: number | null;
      dormant: number | null;
      email: string | null;
      phone: string | null;
    }>;

    const suppliers: SupplierSummary[] = (Array.isArray(rows) ? rows : []).map((r) => ({
      account: (r.account ?? '').trim(),
      name: (r.name ?? '').trim(),
      current_balance: Number(r.current_balance ?? 0),
      dormant: Number(r.dormant ?? 0) !== 0,
      email: (r.email ?? '').trim(),
      phone: (r.phone ?? '').trim(),
    }));

    return { success: true, suppliers, count: suppliers.length };
  } catch (err: any) {
    return {
      success: false,
      suppliers: [],
      count: 0,
      error: err?.message ?? String(err),
    };
  }
}

/**
 * Get a single supplier with detail — used for the supplier-page header.
 */
export async function getSupplier(
  operaDb: Knex,
  supplierCode: string,
): Promise<{ success: boolean; supplier?: SupplierSummary & { address: string }; error?: string }> {
  try {
    const sql = `
      SELECT
        RTRIM(pn_account) AS account,
        RTRIM(pn_name) AS name,
        pn_currbal AS current_balance,
        pn_dormant AS dormant,
        RTRIM(ISNULL(pn_emailadd, '')) AS email,
        RTRIM(ISNULL(pn_telno, '')) AS phone,
        RTRIM(ISNULL(pn_addr1, '')) + ', ' +
          RTRIM(ISNULL(pn_addr2, '')) + ', ' +
          RTRIM(ISNULL(pn_addr3, '')) + ', ' +
          RTRIM(ISNULL(pn_addr4, '')) + ', ' +
          RTRIM(ISNULL(pn_postcode, '')) AS address
      FROM pname WITH (NOLOCK)
      WHERE RTRIM(pn_account) = ?
    `;
    const rows = (await operaDb.raw(sql, [supplierCode])) as unknown as Array<{
      account: string | null;
      name: string | null;
      current_balance: number | null;
      dormant: number | null;
      email: string | null;
      phone: string | null;
      address: string | null;
    }>;

    if (!Array.isArray(rows) || rows.length === 0) {
      return { success: false, error: `Supplier '${supplierCode}' not found` };
    }
    const r = rows[0]!;
    return {
      success: true,
      supplier: {
        account: (r.account ?? '').trim(),
        name: (r.name ?? '').trim(),
        current_balance: Number(r.current_balance ?? 0),
        dormant: Number(r.dormant ?? 0) !== 0,
        email: (r.email ?? '').trim(),
        phone: (r.phone ?? '').trim(),
        address: (r.address ?? '').replace(/(, )+$/, '').replace(/(, ){2,}/g, ', '),
      },
    };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}
