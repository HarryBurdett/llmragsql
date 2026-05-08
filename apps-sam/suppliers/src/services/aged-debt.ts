/**
 * Per-supplier aged-debt analysis.
 *
 * Bucketed view of outstanding ptran balances split into:
 *   Current (0-30 days), 31-60, 61-90, Over 90 days.
 *
 * Mirrors the SQL pattern used by balance-check's creditors-aged
 * analysis but presented at the supplier level.
 *
 * Read-only against Opera SQL (NOLOCK).
 */
import type { Knex } from 'knex';

function r2(n: number): number {
  return Math.round(n * 100) / 100;
}

export interface AgedBucket {
  age_band: string;
  count: number;
  total: number;
}

export interface SupplierAgedRow {
  account: string;
  name: string;
  current_0_30: number;
  days_31_60: number;
  days_61_90: number;
  over_90: number;
  total: number;
}

export interface AgedDebtSummaryResponse {
  success: boolean;
  buckets: AgedBucket[];
  total: number;
  count: number;
  error?: string;
}

/**
 * Aggregate aged buckets across ALL active suppliers' ptran rows.
 */
export async function getAgedDebtSummary(operaDb: Knex): Promise<AgedDebtSummaryResponse> {
  try {
    const sql = `
      SELECT
        CASE
          WHEN DATEDIFF(day, pt_trdate, GETDATE()) <= 30 THEN 'Current (0-30 days)'
          WHEN DATEDIFF(day, pt_trdate, GETDATE()) <= 60 THEN '31-60 days'
          WHEN DATEDIFF(day, pt_trdate, GETDATE()) <= 90 THEN '61-90 days'
          ELSE 'Over 90 days'
        END AS age_band,
        COUNT(*) AS count,
        SUM(pt_trbal) AS total
      FROM ptran WITH (NOLOCK)
      WHERE pt_trbal <> 0
        AND RTRIM(pt_account) IN (
          SELECT RTRIM(pn_account) FROM pname WITH (NOLOCK) WHERE pn_dormant = 0
        )
      GROUP BY CASE
        WHEN DATEDIFF(day, pt_trdate, GETDATE()) <= 30 THEN 'Current (0-30 days)'
        WHEN DATEDIFF(day, pt_trdate, GETDATE()) <= 60 THEN '31-60 days'
        WHEN DATEDIFF(day, pt_trdate, GETDATE()) <= 90 THEN '61-90 days'
        ELSE 'Over 90 days'
      END
      ORDER BY MIN(DATEDIFF(day, pt_trdate, GETDATE()))
    `;

    const rows = (await operaDb.raw(sql)) as unknown as Array<{
      age_band: string | null;
      count: number | null;
      total: number | null;
    }>;

    const buckets: AgedBucket[] = (Array.isArray(rows) ? rows : []).map((r) => ({
      age_band: r.age_band ?? '',
      count: Number(r.count ?? 0),
      total: r2(Number(r.total ?? 0)),
    }));

    const total = buckets.reduce((sum, b) => sum + b.total, 0);
    const count = buckets.reduce((sum, b) => sum + b.count, 0);

    return { success: true, buckets, total: r2(total), count };
  } catch (err: any) {
    return {
      success: false,
      buckets: [],
      total: 0,
      count: 0,
      error: err?.message ?? String(err),
    };
  }
}

export interface SupplierAgedResponse {
  success: boolean;
  suppliers: SupplierAgedRow[];
  count: number;
  error?: string;
}

/**
 * Per-supplier aged-debt: each supplier with their bucket totals.
 */
export async function getAgedDebtBySupplier(operaDb: Knex): Promise<SupplierAgedResponse> {
  try {
    const sql = `
      SELECT
        RTRIM(p.pn_account) AS account,
        RTRIM(p.pn_name) AS name,
        SUM(CASE WHEN DATEDIFF(day, t.pt_trdate, GETDATE()) <= 30 THEN t.pt_trbal ELSE 0 END) AS current_0_30,
        SUM(CASE WHEN DATEDIFF(day, t.pt_trdate, GETDATE()) BETWEEN 31 AND 60 THEN t.pt_trbal ELSE 0 END) AS days_31_60,
        SUM(CASE WHEN DATEDIFF(day, t.pt_trdate, GETDATE()) BETWEEN 61 AND 90 THEN t.pt_trbal ELSE 0 END) AS days_61_90,
        SUM(CASE WHEN DATEDIFF(day, t.pt_trdate, GETDATE()) > 90 THEN t.pt_trbal ELSE 0 END) AS over_90,
        SUM(t.pt_trbal) AS total
      FROM ptran t WITH (NOLOCK)
      JOIN pname p WITH (NOLOCK) ON RTRIM(p.pn_account) = RTRIM(t.pt_account)
      WHERE t.pt_trbal <> 0
        AND p.pn_dormant = 0
      GROUP BY p.pn_account, p.pn_name
      HAVING SUM(t.pt_trbal) <> 0
      ORDER BY SUM(t.pt_trbal) DESC
    `;

    const rows = (await operaDb.raw(sql)) as unknown as Array<{
      account: string | null;
      name: string | null;
      current_0_30: number | null;
      days_31_60: number | null;
      days_61_90: number | null;
      over_90: number | null;
      total: number | null;
    }>;

    const suppliers: SupplierAgedRow[] = (Array.isArray(rows) ? rows : []).map((r) => ({
      account: (r.account ?? '').trim(),
      name: (r.name ?? '').trim(),
      current_0_30: r2(Number(r.current_0_30 ?? 0)),
      days_31_60: r2(Number(r.days_31_60 ?? 0)),
      days_61_90: r2(Number(r.days_61_90 ?? 0)),
      over_90: r2(Number(r.over_90 ?? 0)),
      total: r2(Number(r.total ?? 0)),
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
