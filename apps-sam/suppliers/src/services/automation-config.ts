/**
 * Per-supplier automation config.
 *
 * Controls whether incoming statements are auto-processed (without
 * operator review), the expected statement frequency, and the
 * matching-rules JSON used by the AI extractor.
 *
 * Stored in `supplier_automation_config` (per-app DB).
 * Greenfield TS port.
 */
import type { Knex } from 'knex';

export type StatementFrequency = 'weekly' | 'monthly' | 'quarterly' | 'on_demand';

export interface AutomationConfig {
  supplier_code: string;
  auto_process: boolean;
  frequency: StatementFrequency;
  matching_rules: Record<string, unknown>;
  updated_at: string;
}

const VALID_FREQUENCIES: ReadonlySet<StatementFrequency> = new Set([
  'weekly',
  'monthly',
  'quarterly',
  'on_demand',
]);

function dateToIso(d: Date | string | null): string {
  if (!d) return '';
  if (d instanceof Date) {
    if (Number.isNaN(d.getTime())) return '';
    return d.toISOString();
  }
  return String(d);
}

const DEFAULT_CONFIG: Omit<AutomationConfig, 'supplier_code' | 'updated_at'> = {
  auto_process: false,
  frequency: 'on_demand',
  matching_rules: {},
};

// ---------------------------------------------------------------------
// get
// ---------------------------------------------------------------------

export interface GetAutomationConfigResponse {
  success: boolean;
  config?: AutomationConfig;
  error?: string;
}

export async function getAutomationConfig(
  appDb: Knex,
  supplierCode: string,
): Promise<GetAutomationConfigResponse> {
  if (!supplierCode) {
    return { success: false, error: 'supplier_code is required' };
  }

  try {
    const row = (await appDb('supplier_automation_config')
      .where({ supplier_code: supplierCode })
      .first()) as
      | {
          supplier_code: string;
          auto_process: boolean | number | null;
          frequency: string | null;
          matching_rules_json: string | null;
          updated_at: Date | string;
        }
      | undefined;

    if (!row) {
      return {
        success: true,
        config: {
          supplier_code: supplierCode,
          ...DEFAULT_CONFIG,
          updated_at: '',
        },
      };
    }

    let matchingRules: Record<string, unknown> = {};
    if (row.matching_rules_json) {
      try {
        const parsed = JSON.parse(row.matching_rules_json);
        if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
          matchingRules = parsed as Record<string, unknown>;
        }
      } catch {
        // Stored value isn't valid JSON — leave as default empty
      }
    }

    const freq = (row.frequency ?? 'on_demand') as StatementFrequency;
    const validFreq: StatementFrequency = VALID_FREQUENCIES.has(freq) ? freq : 'on_demand';

    return {
      success: true,
      config: {
        supplier_code: row.supplier_code,
        auto_process: Boolean(row.auto_process),
        frequency: validFreq,
        matching_rules: matchingRules,
        updated_at: dateToIso(row.updated_at),
      },
    };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}

// ---------------------------------------------------------------------
// save
// ---------------------------------------------------------------------

export interface SaveAutomationConfigInput {
  supplier_code: string;
  auto_process?: boolean;
  frequency?: string;
  matching_rules?: Record<string, unknown>;
}

export interface SaveAutomationConfigResponse {
  success: boolean;
  config?: AutomationConfig;
  error?: string;
}

export async function saveAutomationConfig(
  appDb: Knex,
  input: SaveAutomationConfigInput,
): Promise<SaveAutomationConfigResponse> {
  if (!input.supplier_code) {
    return { success: false, error: 'supplier_code is required' };
  }

  if (
    input.frequency !== undefined &&
    !VALID_FREQUENCIES.has(input.frequency as StatementFrequency)
  ) {
    return {
      success: false,
      error: `frequency must be one of: ${[...VALID_FREQUENCIES].join(', ')}`,
    };
  }

  if (
    input.matching_rules !== undefined &&
    (typeof input.matching_rules !== 'object' || Array.isArray(input.matching_rules))
  ) {
    return { success: false, error: 'matching_rules must be a JSON object' };
  }

  try {
    // Read existing to support partial-merge semantics
    const existing = (await appDb('supplier_automation_config')
      .where({ supplier_code: input.supplier_code })
      .first()) as
      | {
          supplier_code: string;
          auto_process: boolean | number | null;
          frequency: string | null;
          matching_rules_json: string | null;
        }
      | undefined;

    let existingRules: Record<string, unknown> = {};
    if (existing?.matching_rules_json) {
      try {
        const parsed = JSON.parse(existing.matching_rules_json);
        if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
          existingRules = parsed as Record<string, unknown>;
        }
      } catch {
        // ignore
      }
    }

    const merged: AutomationConfig = {
      supplier_code: input.supplier_code,
      auto_process:
        input.auto_process ?? Boolean(existing?.auto_process ?? DEFAULT_CONFIG.auto_process),
      frequency:
        (input.frequency as StatementFrequency | undefined) ??
        ((existing?.frequency as StatementFrequency | undefined) ?? DEFAULT_CONFIG.frequency),
      matching_rules: input.matching_rules ?? existingRules,
      updated_at: new Date().toISOString(),
    };

    const dbRow = {
      supplier_code: merged.supplier_code,
      auto_process: merged.auto_process,
      frequency: merged.frequency,
      matching_rules_json: JSON.stringify(merged.matching_rules),
    };

    if (existing) {
      await appDb('supplier_automation_config')
        .where({ supplier_code: input.supplier_code })
        .update({ ...dbRow, updated_at: appDb.fn.now() });
    } else {
      await appDb('supplier_automation_config').insert(dbRow);
    }

    return { success: true, config: merged };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}
