/**
 * Supplier directory listing.
 *
 * Faithful port of list_supplier_directory
 * (apps/suppliers/api/routes.py:2285-2371). Returns suppliers from
 * Opera pname enriched with statement automation info from the
 * per-app DB (statement_count, last_statement, approved_senders).
 *
 * Two modes:
 *   - search supplied → match account or name (LIKE %search%, top 100)
 *   - no search       → only suppliers with non-zero balance (top 500)
 *
 * Both modes order by pn_name asc and exclude dormant suppliers (per
 * CLAUDE.md mandate — Python omits the dormant filter; we add it
 * for parity with the rest of the SAM port).
 */
import type { Knex } from 'knex';

export interface SupplierDirectoryEntry {
  account: string;
  name: string;
  email: string | null;
  phone: string | null;
  contact: string | null;
  balance: number;
  statement_count: number;
  last_statement: string | null;
  approved_senders: number;
}

export interface SupplierDirectoryOptions {
  search?: string | null;
}

export interface SupplierDirectoryResponse {
  success: boolean;
  suppliers: SupplierDirectoryEntry[];
  count: number;
  error?: string;
}

interface OperaRow {
  account: string | null;
  name: string | null;
  email: string | null;
  phone: string | null;
  contact: string | null;
  balance: number | string | null;
}

function trim(s: string | null | undefined): string {
  return (s ?? '').trim();
}

export async function listSupplierDirectory(
  operaDb: Knex,
  appDb: Knex | null | undefined,
  opts: SupplierDirectoryOptions = {},
): Promise<SupplierDirectoryResponse> {
  const search = trim(opts.search ?? '');
  try {
    let q = operaDb('pname')
      .where('pn_dormant', 0)
      .orderBy('pn_name', 'asc');
    if (search) {
      const pattern = `%${search}%`;
      q = q
        .andWhere((qb) => {
          qb.where('pn_name', 'like', pattern).orWhere('pn_account', 'like', pattern);
        })
        .limit(100);
    } else {
      q = q.andWhere('pn_currbal', '<>', 0).limit(500);
    }
    const rows = (await q.select(
      operaDb.raw('RTRIM(pn_account) AS account'),
      operaDb.raw('RTRIM(pn_name) AS name'),
      operaDb.raw('RTRIM(pn_email) AS email'),
      operaDb.raw('RTRIM(pn_teleno) AS phone'),
      operaDb.raw('RTRIM(pn_contact) AS contact'),
      operaDb.raw('pn_currbal AS balance'),
    )) as unknown as OperaRow[];

    const accounts = (rows ?? [])
      .map((r) => trim(r.account))
      .filter(Boolean);

    const stmtMap = new Map<string, { count: number; last: string | null }>();
    const senderMap = new Map<string, number>();

    if (appDb && accounts.length > 0) {
      try {
        const stmtRows = (await appDb('supplier_statements')
          .whereIn('supplier_code', accounts)
          .groupBy('supplier_code')
          .select(
            'supplier_code',
            appDb.raw('COUNT(*) AS statement_count'),
            appDb.raw('MAX(imported_at) AS last_statement'),
          )) as unknown as Array<{
          supplier_code: string | null;
          statement_count: number | string | null;
          last_statement: string | Date | null;
        }>;
        for (const r of stmtRows ?? []) {
          const code = trim(r.supplier_code);
          if (!code) continue;
          const last =
            r.last_statement instanceof Date
              ? r.last_statement.toISOString()
              : r.last_statement
                ? String(r.last_statement)
                : null;
          stmtMap.set(code, {
            count: Number(r.statement_count ?? 0),
            last,
          });
        }
      } catch {
        // best-effort
      }
      try {
        const senderRows = (await appDb('supplier_approved_emails')
          .whereIn('supplier_code', accounts)
          .groupBy('supplier_code')
          .select(
            'supplier_code',
            appDb.raw('COUNT(*) AS sender_count'),
          )) as unknown as Array<{
          supplier_code: string | null;
          sender_count: number | string | null;
        }>;
        for (const r of senderRows ?? []) {
          const code = trim(r.supplier_code);
          if (!code) continue;
          senderMap.set(code, Number(r.sender_count ?? 0));
        }
      } catch {
        // best-effort
      }
    }

    const suppliers: SupplierDirectoryEntry[] = (rows ?? []).map((r) => {
      const account = trim(r.account);
      const stmtInfo = stmtMap.get(account);
      return {
        account,
        name: trim(r.name),
        email: trim(r.email) || null,
        phone: trim(r.phone) || null,
        contact: trim(r.contact) || null,
        balance: Number(r.balance ?? 0),
        statement_count: stmtInfo?.count ?? 0,
        last_statement: stmtInfo?.last ?? null,
        approved_senders: senderMap.get(account) ?? 0,
      };
    });

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
