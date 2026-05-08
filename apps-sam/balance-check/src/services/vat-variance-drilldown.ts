/**
 * VAT variance drill-down — faithful port of `vat_variance_drilldown()`
 * from `apps/balance_check/api/routes.py`.
 *
 * Drill-down to identify causes of VAT variance between zvtran and the
 * nominal ledger. Shows transactions that don't reconcile.
 */
import type { Knex } from 'knex';

function r2(n: number): number {
  return Math.round(n * 100) / 100;
}

function formatGbp(n: number): string {
  return n.toLocaleString('en-GB', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

interface UncommittedByPeriodRow {
  year: number;
  month: number;
  type: string;
  count: number;
  total: number;
}

interface NominalByPeriodRow {
  account: string;
  year: number;
  period: number;
  count: number;
  total: number;
}

interface LargestUncommittedRow {
  date: string;
  type: string;
  vat_amount: number;
  net_amount: number;
  vat_code: string;
}

interface LargestNlEntryRow {
  date: string;
  value: number;
  reference: string;
  type: string;
  year: number;
  period: number;
}

export interface VatVarianceDrilldownResponse {
  success: boolean;
  vat_nominal_accounts?: string[];
  analysis?: {
    uncommitted_by_period: UncommittedByPeriodRow[];
    nominal_by_period: NominalByPeriodRow[];
    largest_uncommitted: LargestUncommittedRow[];
    [accountKey: string]: unknown;
  };
  summary?: {
    uncommitted_vat: { output: number; input: number; net: number; record_count: number };
    nominal_balance: { total: number; record_count: number };
    variance: number;
    variance_explanation: string[];
  };
  error?: string;
}

export async function vatVarianceDrilldown(db: Knex): Promise<VatVarianceDrilldownResponse> {
  try {
    // Get VAT nominal accounts from ztax
    const ztaxRows = (await db.raw(`
      SELECT DISTINCT tx_nominal
      FROM ztax WITH (NOLOCK)
      WHERE tx_ctrytyp = 'H'
        AND tx_nominal IS NOT NULL
        AND RTRIM(tx_nominal) != ''
    `)) as unknown as Array<{ tx_nominal: string | null }>;

    const vatNominals: string[] = [];
    for (const row of Array.isArray(ztaxRows) ? ztaxRows : []) {
      if (row.tx_nominal) vatNominals.push(String(row.tx_nominal).trim());
    }

    // 1. Uncommitted VAT totals by year/month
    const zvtranByPeriodRows = (await db.raw(`
      SELECT
        YEAR(va_taxdate) AS year,
        MONTH(va_taxdate) AS month,
        va_vattype AS vat_type,
        COUNT(*) AS transaction_count,
        SUM(va_vatval) AS vat_total
      FROM zvtran WITH (NOLOCK)
      WHERE va_done = 0
      GROUP BY YEAR(va_taxdate), MONTH(va_taxdate), va_vattype
      ORDER BY year DESC, month DESC, va_vattype
    `)) as unknown as Array<{
      year: number | null;
      month: number | null;
      vat_type: string | null;
      transaction_count: number | null;
      vat_total: number | null;
    }>;

    const uncommittedByPeriod: UncommittedByPeriodRow[] = (Array.isArray(zvtranByPeriodRows)
      ? zvtranByPeriodRows
      : []
    ).map((r) => ({
      year: Number(r.year ?? 0),
      month: Number(r.month ?? 0),
      type: (r.vat_type ?? '').trim(),
      count: Number(r.transaction_count ?? 0),
      total: r2(Number(r.vat_total ?? 0)),
    }));

    // 2. Nominal ledger VAT movements by year/period
    const nominalByPeriod: NominalByPeriodRow[] = [];
    for (const acnt of vatNominals) {
      const nlRows = (await db.raw(
        `
        SELECT nt_year AS year, nt_period AS period, COUNT(*) AS transaction_count, SUM(nt_value) AS total_value
        FROM ntran WITH (NOLOCK)
        WHERE nt_acnt = ?
        GROUP BY nt_year, nt_period
        ORDER BY nt_year DESC, nt_period DESC
        `,
        [acnt],
      )) as unknown as Array<{
        year: number | null;
        period: number | null;
        transaction_count: number | null;
        total_value: number | null;
      }>;
      for (const row of Array.isArray(nlRows) ? nlRows : []) {
        nominalByPeriod.push({
          account: acnt,
          year: Number(row.year ?? 0),
          period: Number(row.period ?? 0),
          count: Number(row.transaction_count ?? 0),
          total: r2(Number(row.total_value ?? 0)),
        });
      }
    }

    // 3. Largest uncommitted VAT transactions (top 50 returned, query top 100)
    const largestRows = (await db.raw(`
      SELECT TOP 100
        va_taxdate, va_vattype, va_vatval, va_trvalue, va_anvat
      FROM zvtran WITH (NOLOCK)
      WHERE va_done = 0
      ORDER BY ABS(va_vatval) DESC
    `)) as unknown as Array<{
      va_taxdate: Date | string | null;
      va_vattype: string | null;
      va_vatval: number | null;
      va_trvalue: number | null;
      va_anvat: string | null;
    }>;

    const largestUncommitted: LargestUncommittedRow[] = (Array.isArray(largestRows) ? largestRows : [])
      .slice(0, 50)
      .map((r) => ({
        date: r.va_taxdate ? String(r.va_taxdate) : '',
        type: (r.va_vattype ?? '').trim(),
        vat_amount: r2(Number(r.va_vatval ?? 0)),
        net_amount: r2(Number(r.va_trvalue ?? 0)),
        vat_code: (r.va_anvat ?? '').trim(),
      }));

    // 4. NL entries on VAT accounts — largest transactions (first 2 accounts only)
    const accountSpecificEntries: Record<string, LargestNlEntryRow[]> = {};
    for (const acnt of vatNominals.slice(0, 2)) {
      const nlEntryRows = (await db.raw(
        `
        SELECT TOP 50 nt_entr AS post_date, nt_value, nt_trnref, nt_posttyp, nt_year, nt_period
        FROM ntran WITH (NOLOCK)
        WHERE nt_acnt = ?
        ORDER BY ABS(nt_value) DESC
        `,
        [acnt],
      )) as unknown as Array<{
        post_date: Date | string | null;
        nt_value: number | null;
        nt_trnref: string | null;
        nt_posttyp: string | null;
        nt_year: number | null;
        nt_period: number | null;
      }>;

      const entries: LargestNlEntryRow[] = (Array.isArray(nlEntryRows) ? nlEntryRows : []).map((r) => ({
        date: r.post_date ? String(r.post_date) : '',
        value: r2(Number(r.nt_value ?? 0)),
        reference: ((r.nt_trnref ?? '').trim()).slice(0, 40),
        type: (r.nt_posttyp ?? '').trim(),
        year: Number(r.nt_year ?? 0),
        period: Number(r.nt_period ?? 0),
      }));

      accountSpecificEntries[`largest_nl_entries_${acnt}`] = entries;
    }

    // 5. Summary comparison
    const totalUncRows = (await db.raw(`
      SELECT
        SUM(CASE WHEN va_vattype = 'S' THEN va_vatval ELSE 0 END) AS output_total,
        SUM(CASE WHEN va_vattype = 'P' THEN va_vatval ELSE 0 END) AS input_total,
        COUNT(*) AS total_records
      FROM zvtran WITH (NOLOCK)
      WHERE va_done = 0
    `)) as unknown as Array<{
      output_total: number | null;
      input_total: number | null;
      total_records: number | null;
    }>;

    const uncommittedOutput = Number(totalUncRows?.[0]?.output_total ?? 0);
    const uncommittedInput = Number(totalUncRows?.[0]?.input_total ?? 0);
    const uncommittedNet = uncommittedOutput - uncommittedInput;
    const uncommittedCount = Number(totalUncRows?.[0]?.total_records ?? 0);

    let nlTotal = 0;
    let nlCount = 0;
    for (const acnt of vatNominals) {
      const sumRows = (await db.raw(
        'SELECT SUM(nt_value) AS total, COUNT(*) AS cnt FROM ntran WITH (NOLOCK) WHERE nt_acnt = ?',
        [acnt],
      )) as unknown as Array<{ total: number | null; cnt: number | null }>;
      const r = sumRows?.[0];
      if (r) {
        nlTotal += Number(r.total ?? 0);
        nlCount += Number(r.cnt ?? 0);
      }
    }

    const nlBalance = -nlTotal; // VAT liability is typically credit
    const variance = uncommittedNet - nlBalance;
    const varianceExplanation: string[] = [];
    if (Math.abs(variance) > 1) {
      if (variance > 0) {
        varianceExplanation.push(
          `Uncommitted VAT is £${formatGbp(variance)} MORE than nominal balance`,
        );
        varianceExplanation.push(
          'Possible causes: VAT transactions not posted to nominal, or nominal entries reversed',
        );
      } else {
        varianceExplanation.push(
          `Uncommitted VAT is £${formatGbp(Math.abs(variance))} LESS than nominal balance`,
        );
        varianceExplanation.push(
          'Possible causes: Nominal entries without zvtran records, or VAT returns processed but marked done',
        );
      }
    }

    return {
      success: true,
      vat_nominal_accounts: vatNominals,
      analysis: {
        uncommitted_by_period: uncommittedByPeriod,
        nominal_by_period: nominalByPeriod,
        largest_uncommitted: largestUncommitted,
        ...accountSpecificEntries,
      },
      summary: {
        uncommitted_vat: {
          output: r2(uncommittedOutput),
          input: r2(uncommittedInput),
          net: r2(uncommittedNet),
          record_count: uncommittedCount,
        },
        nominal_balance: {
          total: r2(nlBalance),
          record_count: nlCount,
        },
        variance: r2(variance),
        variance_explanation: varianceExplanation,
      },
    };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}
