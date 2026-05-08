/**
 * VAT diagnostic — faithful port of `vat_diagnostic()` from
 * `apps/balance_check/api/routes.py`.
 *
 * Diagnostic endpoint to check VAT table data availability. Each table
 * check is independent — if one query fails, the others still run.
 */
import type { Knex } from 'knex';

export interface VatDiagnosticResponse {
  tables: Record<string, Record<string, unknown> | { error: string }>;
  error?: string;
}

export async function vatDiagnostic(db: Knex): Promise<VatDiagnosticResponse> {
  const result: VatDiagnosticResponse = { tables: {} };

  try {
    // Check zvtran
    try {
      const zvtranSql = `
        SELECT
          COUNT(*) AS total_rows,
          SUM(CASE WHEN va_done = 0 THEN 1 ELSE 0 END) AS uncommitted,
          SUM(CASE WHEN va_done = 1 THEN 1 ELSE 0 END) AS committed,
          MIN(va_taxdate) AS min_date,
          MAX(va_taxdate) AS max_date,
          SUM(va_vatval) AS total_vat
        FROM zvtran WITH (NOLOCK)
      `;
      const rows = (await db.raw(zvtranSql)) as unknown as Array<Record<string, unknown>>;
      const first = Array.isArray(rows) && rows.length > 0 ? rows[0] : null;
      result.tables.zvtran = first ?? { error: 'no data' };
    } catch (err: any) {
      result.tables.zvtran = { error: err?.message ?? String(err) };
    }

    // Check nvat
    try {
      const nvatSql = `
        SELECT
          COUNT(*) AS total_rows,
          MIN(nv_date) AS min_date,
          MAX(nv_date) AS max_date,
          SUM(nv_vatval) AS total_vat,
          COUNT(DISTINCT nv_vattype) AS vat_types
        FROM nvat WITH (NOLOCK)
      `;
      const rows = (await db.raw(nvatSql)) as unknown as Array<Record<string, unknown>>;
      const first = Array.isArray(rows) && rows.length > 0 ? rows[0] : null;
      result.tables.nvat = first ?? { error: 'no data' };
    } catch (err: any) {
      result.tables.nvat = { error: err?.message ?? String(err) };
    }

    // Check ztax (VAT codes)
    try {
      const ztaxSql = `
        SELECT COUNT(*) AS total_codes
        FROM ztax WITH (NOLOCK)
        WHERE tx_ctrytyp = 'H'
      `;
      const rows = (await db.raw(ztaxSql)) as unknown as Array<Record<string, unknown>>;
      const first = Array.isArray(rows) && rows.length > 0 ? rows[0] : null;
      result.tables.ztax = first ?? { error: 'no data' };
    } catch (err: any) {
      result.tables.ztax = { error: err?.message ?? String(err) };
    }

    // Check ntran for current year
    try {
      const ntranSql = `
        SELECT
          MAX(nt_year) AS current_year,
          COUNT(*) AS total_rows
        FROM ntran WITH (NOLOCK)
      `;
      const rows = (await db.raw(ntranSql)) as unknown as Array<Record<string, unknown>>;
      const first = Array.isArray(rows) && rows.length > 0 ? rows[0] : null;
      result.tables.ntran = first ?? { error: 'no data' };
    } catch (err: any) {
      result.tables.ntran = { error: err?.message ?? String(err) };
    }

    return result;
  } catch (err: any) {
    return { tables: result.tables, error: err?.message ?? String(err) };
  }
}
