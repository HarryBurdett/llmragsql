/**
 * Supplier onboarding state.
 *
 * Each supplier passes through stages as the operator sets up
 * their statement-processing workflow:
 *   1. discovered      — first statement received
 *   2. configured      — automation rules + approved senders set
 *   3. testing         — running auto-process in dry-run mode
 *   4. live            — fully automated
 *   5. paused          — temporarily disabled
 *
 * Stored in `supplier_onboarding` (one row per supplier).
 * Greenfield TS port.
 */
import type { Knex } from 'knex';

export type OnboardingStage =
  | 'discovered'
  | 'configured'
  | 'testing'
  | 'live'
  | 'paused';

const VALID_STAGES: ReadonlySet<OnboardingStage> = new Set([
  'discovered',
  'configured',
  'testing',
  'live',
  'paused',
]);

export interface OnboardingState {
  supplier_code: string;
  stage: OnboardingStage;
  notes: string;
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

const DEFAULT_STATE: Omit<OnboardingState, 'supplier_code' | 'updated_at'> = {
  stage: 'discovered',
  notes: '',
};

// ---------------------------------------------------------------------
// get
// ---------------------------------------------------------------------

export interface GetOnboardingResponse {
  success: boolean;
  state?: OnboardingState;
  error?: string;
}

export async function getOnboardingState(
  appDb: Knex,
  supplierCode: string,
): Promise<GetOnboardingResponse> {
  if (!supplierCode) {
    return { success: false, error: 'supplier_code is required' };
  }

  try {
    const row = (await appDb('supplier_onboarding')
      .where({ supplier_code: supplierCode })
      .first()) as
      | {
          supplier_code: string;
          stage: string | null;
          notes: string | null;
          updated_at: Date | string;
        }
      | undefined;

    if (!row) {
      return {
        success: true,
        state: {
          supplier_code: supplierCode,
          ...DEFAULT_STATE,
          updated_at: '',
        },
      };
    }

    const stage = (row.stage ?? 'discovered') as OnboardingStage;
    const validStage: OnboardingStage = VALID_STAGES.has(stage) ? stage : 'discovered';

    return {
      success: true,
      state: {
        supplier_code: row.supplier_code,
        stage: validStage,
        notes: row.notes ?? '',
        updated_at: dateToIso(row.updated_at),
      },
    };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}

// ---------------------------------------------------------------------
// list (across all suppliers)
// ---------------------------------------------------------------------

export interface ListOnboardingOptions {
  stage?: OnboardingStage;
}

export interface ListOnboardingResponse {
  success: boolean;
  states: OnboardingState[];
  count: number;
  error?: string;
}

export async function listOnboardingStates(
  appDb: Knex,
  opts: ListOnboardingOptions = {},
): Promise<ListOnboardingResponse> {
  if (opts.stage !== undefined && !VALID_STAGES.has(opts.stage)) {
    return {
      success: false,
      states: [],
      count: 0,
      error: `stage must be one of: ${[...VALID_STAGES].join(', ')}`,
    };
  }

  try {
    let query = appDb('supplier_onboarding').orderBy('supplier_code', 'asc');
    if (opts.stage) {
      query = query.where({ stage: opts.stage });
    }

    const rows = (await query) as unknown as Array<{
      supplier_code: string;
      stage: string | null;
      notes: string | null;
      updated_at: Date | string;
    }>;

    const states: OnboardingState[] = rows.map((r) => {
      const stage = (r.stage ?? 'discovered') as OnboardingStage;
      const validStage: OnboardingStage = VALID_STAGES.has(stage) ? stage : 'discovered';
      return {
        supplier_code: r.supplier_code,
        stage: validStage,
        notes: r.notes ?? '',
        updated_at: dateToIso(r.updated_at),
      };
    });

    return { success: true, states, count: states.length };
  } catch (err: any) {
    return {
      success: false,
      states: [],
      count: 0,
      error: err?.message ?? String(err),
    };
  }
}

// ---------------------------------------------------------------------
// update
// ---------------------------------------------------------------------

export interface UpdateOnboardingInput {
  supplier_code: string;
  stage?: string;
  notes?: string;
}

export async function updateOnboardingState(
  appDb: Knex,
  input: UpdateOnboardingInput,
): Promise<GetOnboardingResponse> {
  if (!input.supplier_code) {
    return { success: false, error: 'supplier_code is required' };
  }
  if (input.stage !== undefined && !VALID_STAGES.has(input.stage as OnboardingStage)) {
    return {
      success: false,
      error: `stage must be one of: ${[...VALID_STAGES].join(', ')}`,
    };
  }

  try {
    const existing = (await appDb('supplier_onboarding')
      .where({ supplier_code: input.supplier_code })
      .first()) as
      | { supplier_code: string; stage: string | null; notes: string | null }
      | undefined;

    const merged: OnboardingState = {
      supplier_code: input.supplier_code,
      stage:
        (input.stage as OnboardingStage | undefined) ??
        ((existing?.stage as OnboardingStage | undefined) ?? DEFAULT_STATE.stage),
      notes: input.notes ?? existing?.notes ?? DEFAULT_STATE.notes,
      updated_at: new Date().toISOString(),
    };

    if (existing) {
      await appDb('supplier_onboarding')
        .where({ supplier_code: input.supplier_code })
        .update({
          stage: merged.stage,
          notes: merged.notes,
          updated_at: appDb.fn.now(),
        });
    } else {
      await appDb('supplier_onboarding').insert({
        supplier_code: merged.supplier_code,
        stage: merged.stage,
        notes: merged.notes,
      });
    }

    return { success: true, state: merged };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}
