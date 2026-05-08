/**
 * Per-statement override flags — operator decisions on individual
 * statement lines (accept / reject / dispute) recorded with reasons.
 *
 * Greenfield TS work. The supplier_overrides table exists in
 * migration 001; this service is the missing CRUD wrapper.
 *
 * Used by the reconciliation UI to capture operator decisions that
 * differ from the AI/auto-matched defaults — e.g. accepting a
 * payment-difference, rejecting an invoice that the supplier claims
 * is outstanding but is in fact unpaid by design, or flagging a
 * dispute for the next supplier conversation.
 *
 * Stored in `supplier_overrides` (per-app DB).
 */
import type { Knex } from 'knex';

export type OverrideType = 'accept' | 'reject' | 'dispute';

const VALID_TYPES: ReadonlySet<OverrideType> = new Set([
  'accept',
  'reject',
  'dispute',
]);

export interface OverrideEntry {
  id: number;
  statement_id: number;
  line_id: number | null;
  override_type: OverrideType;
  reason: string;
  created_at: string;
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
// list overrides for a statement
// ---------------------------------------------------------------------

export interface ListOverridesResponse {
  success: boolean;
  entries: OverrideEntry[];
  count: number;
  error?: string;
}

export async function listOverrides(
  appDb: Knex,
  statementId: number,
): Promise<ListOverridesResponse> {
  if (!Number.isFinite(statementId) || statementId <= 0) {
    return {
      success: false,
      entries: [],
      count: 0,
      error: 'statement_id is required (positive number)',
    };
  }
  try {
    const rows = (await appDb('supplier_overrides')
      .where({ statement_id: statementId })
      .orderBy('created_at', 'desc')) as unknown as Array<{
      id: number;
      statement_id: number;
      line_id: number | null;
      override_type: string | null;
      reason: string | null;
      created_at: Date | string;
    }>;
    const entries: OverrideEntry[] = rows.map((r) => {
      const t = (r.override_type ?? 'accept') as OverrideType;
      return {
        id: r.id,
        statement_id: r.statement_id,
        line_id: r.line_id ?? null,
        override_type: VALID_TYPES.has(t) ? t : 'accept',
        reason: r.reason ?? '',
        created_at: dateToIso(r.created_at),
      };
    });
    return { success: true, entries, count: entries.length };
  } catch (err: any) {
    return {
      success: false,
      entries: [],
      count: 0,
      error: err?.message ?? String(err),
    };
  }
}

// ---------------------------------------------------------------------
// record a new override
// ---------------------------------------------------------------------

export interface RecordOverrideInput {
  statement_id: number;
  line_id?: number | null;
  override_type: string;
  reason?: string;
}

export interface RecordOverrideResponse {
  success: boolean;
  entry?: OverrideEntry;
  error?: string;
}

export async function recordOverride(
  appDb: Knex,
  input: RecordOverrideInput,
): Promise<RecordOverrideResponse> {
  if (!Number.isFinite(input.statement_id) || input.statement_id <= 0) {
    return { success: false, error: 'statement_id is required (positive)' };
  }
  const ot = (input.override_type ?? '').trim() as OverrideType;
  if (!VALID_TYPES.has(ot)) {
    return {
      success: false,
      error: `override_type must be one of: ${[...VALID_TYPES].join(', ')}`,
    };
  }
  const lineId =
    input.line_id !== undefined && input.line_id !== null
      ? Number(input.line_id)
      : null;
  if (lineId !== null && (!Number.isFinite(lineId) || lineId <= 0)) {
    return { success: false, error: 'line_id must be a positive number' };
  }
  try {
    const inserted = await appDb('supplier_overrides')
      .insert({
        statement_id: input.statement_id,
        line_id: lineId,
        override_type: ot,
        reason: input.reason ?? '',
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
      entry: {
        id,
        statement_id: input.statement_id,
        line_id: lineId,
        override_type: ot,
        reason: input.reason ?? '',
        created_at: new Date().toISOString(),
      },
    };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}

// ---------------------------------------------------------------------
// delete a single override
// ---------------------------------------------------------------------

export interface DeleteOverrideResponse {
  success: boolean;
  deleted?: boolean;
  error?: string;
}

export async function deleteOverride(
  appDb: Knex,
  id: number,
): Promise<DeleteOverrideResponse> {
  if (!Number.isFinite(id) || id <= 0) {
    return { success: false, error: 'id is required (positive number)' };
  }
  try {
    const removed = await appDb('supplier_overrides').where({ id }).delete();
    return { success: true, deleted: Number(removed) > 0 };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}
