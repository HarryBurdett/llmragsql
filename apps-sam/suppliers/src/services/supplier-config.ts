/**
 * Per-supplier configuration JSON.
 *
 * Holds arbitrary supplier-specific settings (matching tolerances,
 * statement-template hints, etc.). One JSON blob per supplier in
 * the per-app `supplier_config` table.
 *
 * Greenfield TS port — Python suppliers app didn't reach this.
 */
import type { Knex } from 'knex';

export interface SupplierConfigResponse {
  success: boolean;
  supplier_code?: string;
  config?: Record<string, unknown>;
  updated_at?: string;
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

const DEFAULT_CONFIG: Record<string, unknown> = {
  // Matching tolerance for statement vs ptran (in pence)
  match_tolerance_pence: 1,
  // Whether to auto-process when an approved sender matches
  auto_process: false,
  // Reminder cadence (days) for unanswered statement queries
  reminder_days: 7,
  // Optional statement-template hint for the AI extractor
  statement_template_hint: '',
};

export async function getSupplierConfig(
  appDb: Knex,
  supplierCode: string,
): Promise<SupplierConfigResponse> {
  if (!supplierCode) {
    return { success: false, error: 'supplier_code is required' };
  }
  try {
    const row = (await appDb('supplier_config')
      .where({ supplier_code: supplierCode })
      .first()) as
      | { supplier_code: string; config_json: string | null; updated_at: Date | string }
      | undefined;

    if (!row) {
      return {
        success: true,
        supplier_code: supplierCode,
        config: { ...DEFAULT_CONFIG },
        updated_at: '',
      };
    }

    let parsed: Record<string, unknown> = {};
    if (row.config_json) {
      try {
        const obj = JSON.parse(row.config_json);
        if (obj && typeof obj === 'object' && !Array.isArray(obj)) {
          parsed = obj as Record<string, unknown>;
        }
      } catch {
        // Stored value isn't valid JSON — return defaults
      }
    }

    return {
      success: true,
      supplier_code: supplierCode,
      // Merge stored over defaults so a future field is exposed even if
      // older rows don't have it.
      config: { ...DEFAULT_CONFIG, ...parsed },
      updated_at: dateToIso(row.updated_at),
    };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}

export interface SaveSupplierConfigInput {
  supplier_code: string;
  config: Record<string, unknown>;
}

/**
 * Replace (or insert) the per-supplier config blob.
 *
 * Whole-document replacement. If you want partial-merge semantics,
 * read first, merge in the caller, then save.
 */
export async function saveSupplierConfig(
  appDb: Knex,
  input: SaveSupplierConfigInput,
): Promise<SupplierConfigResponse> {
  if (!input.supplier_code) {
    return { success: false, error: 'supplier_code is required' };
  }
  if (
    !input.config ||
    typeof input.config !== 'object' ||
    Array.isArray(input.config)
  ) {
    return { success: false, error: 'config must be a JSON object' };
  }

  try {
    const value = JSON.stringify(input.config);
    const existing = await appDb('supplier_config')
      .where({ supplier_code: input.supplier_code })
      .first();

    if (existing) {
      await appDb('supplier_config')
        .where({ supplier_code: input.supplier_code })
        .update({ config_json: value, updated_at: appDb.fn.now() });
    } else {
      await appDb('supplier_config').insert({
        supplier_code: input.supplier_code,
        config_json: value,
      });
    }

    return {
      success: true,
      supplier_code: input.supplier_code,
      config: input.config,
      updated_at: new Date().toISOString(),
    };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}
