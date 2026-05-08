/**
 * Statement-line CRUD for the supplier statement reconciliation flow.
 *
 * Greenfield TS work. Used by:
 *   - extract-statement (writes line items extracted from PDF)
 *   - reconcile         (updates match_status as lines match Opera ptran)
 *   - statement detail UI (lists lines)
 *   - manual edits      (operator overrides match_status)
 *
 * The schema (statement_lines + statement_opera_only) already exists
 * via migration 001. This service exposes the CRUD primitives.
 *
 * Match status values:
 *   - 'unmatched'  initial state after extraction
 *   - 'matched'    matched to an Opera ptran row
 *   - 'disputed'   matched but operator flagged a discrepancy
 *
 * Statement-only items (`statement_opera_only`) are lines that appear
 * in the supplier statement but have no corresponding Opera ptran row
 * — typically missing invoices the supplier is asking us about.
 */
import type { Knex } from 'knex';

export type MatchStatus = 'matched' | 'unmatched' | 'disputed';

const VALID_STATUSES: ReadonlySet<MatchStatus> = new Set([
  'matched',
  'unmatched',
  'disputed',
]);

export interface StatementLine {
  id: number;
  statement_id: number;
  line_date: string;
  reference: string;
  description: string;
  amount: number;
  matched_opera_ref: string;
  match_status: MatchStatus;
}

export interface StatementOperaOnly {
  id: number;
  statement_id: number;
  reference: string;
  amount: number;
  reason: string;
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
// list lines for a statement
// ---------------------------------------------------------------------

export interface ListStatementLinesResponse {
  success: boolean;
  lines: StatementLine[];
  count: number;
  total_amount: number;
  matched_count: number;
  unmatched_count: number;
  disputed_count: number;
  error?: string;
}

function r2(n: number): number {
  return Math.round(n * 100) / 100;
}

export async function listStatementLines(
  appDb: Knex,
  statementId: number,
): Promise<ListStatementLinesResponse> {
  if (!Number.isFinite(statementId) || statementId <= 0) {
    return {
      success: false,
      lines: [],
      count: 0,
      total_amount: 0,
      matched_count: 0,
      unmatched_count: 0,
      disputed_count: 0,
      error: 'statement_id is required (positive number)',
    };
  }
  try {
    const rows = (await appDb('statement_lines')
      .where({ statement_id: statementId })
      .orderBy('line_date', 'asc')
      .orderBy('id', 'asc')) as unknown as Array<{
      id: number;
      statement_id: number;
      line_date: Date | string | null;
      reference: string | null;
      description: string | null;
      amount: number | null;
      matched_opera_ref: string | null;
      match_status: string | null;
    }>;

    const lines: StatementLine[] = rows.map((r) => {
      const ms = (r.match_status ?? 'unmatched') as MatchStatus;
      return {
        id: r.id,
        statement_id: r.statement_id,
        line_date: dateToIso(r.line_date),
        reference: r.reference ?? '',
        description: r.description ?? '',
        amount: Number(r.amount ?? 0),
        matched_opera_ref: r.matched_opera_ref ?? '',
        match_status: VALID_STATUSES.has(ms) ? ms : 'unmatched',
      };
    });

    const total = lines.reduce((s, l) => s + l.amount, 0);
    const matched = lines.filter((l) => l.match_status === 'matched').length;
    const unmatched = lines.filter((l) => l.match_status === 'unmatched').length;
    const disputed = lines.filter((l) => l.match_status === 'disputed').length;
    return {
      success: true,
      lines,
      count: lines.length,
      total_amount: r2(total),
      matched_count: matched,
      unmatched_count: unmatched,
      disputed_count: disputed,
    };
  } catch (err: any) {
    return {
      success: false,
      lines: [],
      count: 0,
      total_amount: 0,
      matched_count: 0,
      unmatched_count: 0,
      disputed_count: 0,
      error: err?.message ?? String(err),
    };
  }
}

// ---------------------------------------------------------------------
// add lines (bulk insert from extraction)
// ---------------------------------------------------------------------

export interface NewStatementLine {
  line_date?: string | null;
  reference?: string;
  description?: string;
  amount: number;
  matched_opera_ref?: string;
  match_status?: MatchStatus;
}

export interface AddStatementLinesResponse {
  success: boolean;
  inserted: number;
  ids?: number[];
  error?: string;
}

export async function addStatementLines(
  appDb: Knex,
  statementId: number,
  lines: NewStatementLine[],
): Promise<AddStatementLinesResponse> {
  if (!Number.isFinite(statementId) || statementId <= 0) {
    return {
      success: false,
      inserted: 0,
      error: 'statement_id is required (positive number)',
    };
  }
  if (!Array.isArray(lines) || lines.length === 0) {
    return { success: true, inserted: 0, ids: [] };
  }
  // Validate match_status values up-front; refuse on bad input rather
  // than silently coercing.
  for (const l of lines) {
    if (l.match_status && !VALID_STATUSES.has(l.match_status)) {
      return {
        success: false,
        inserted: 0,
        error: `match_status must be one of: ${[...VALID_STATUSES].join(', ')}`,
      };
    }
    if (!Number.isFinite(l.amount)) {
      return {
        success: false,
        inserted: 0,
        error: 'each line.amount must be a finite number',
      };
    }
  }

  try {
    const ids: number[] = [];
    for (const l of lines) {
      const inserted = await appDb('statement_lines')
        .insert({
          statement_id: statementId,
          line_date: l.line_date ?? null,
          reference: (l.reference ?? '').slice(0, 100),
          description: (l.description ?? '').slice(0, 500),
          amount: l.amount,
          matched_opera_ref: (l.matched_opera_ref ?? '').slice(0, 64) || null,
          match_status: l.match_status ?? 'unmatched',
        })
        .returning('id');
      const id =
        Array.isArray(inserted) && inserted.length > 0
          ? typeof inserted[0] === 'object'
            ? (inserted[0] as { id: number }).id
            : Number(inserted[0])
          : 0;
      ids.push(id);
    }
    return { success: true, inserted: ids.length, ids };
  } catch (err: any) {
    return { success: false, inserted: 0, error: err?.message ?? String(err) };
  }
}

// ---------------------------------------------------------------------
// update match_status / matched_opera_ref
// ---------------------------------------------------------------------

export interface UpdateLineMatchInput {
  matched_opera_ref?: string | null;
  match_status?: MatchStatus;
}

export interface UpdateLineMatchResponse {
  success: boolean;
  updated?: boolean;
  error?: string;
}

export async function updateStatementLineMatch(
  appDb: Knex,
  lineId: number,
  input: UpdateLineMatchInput,
): Promise<UpdateLineMatchResponse> {
  if (!Number.isFinite(lineId) || lineId <= 0) {
    return { success: false, error: 'line_id is required (positive number)' };
  }
  if (input.match_status && !VALID_STATUSES.has(input.match_status)) {
    return {
      success: false,
      error: `match_status must be one of: ${[...VALID_STATUSES].join(', ')}`,
    };
  }
  const update: Record<string, unknown> = {};
  if (input.matched_opera_ref !== undefined) {
    update.matched_opera_ref = input.matched_opera_ref ?? null;
  }
  if (input.match_status) {
    update.match_status = input.match_status;
  }
  if (Object.keys(update).length === 0) {
    return { success: false, error: 'No fields to update' };
  }
  try {
    const updated = await appDb('statement_lines').where({ id: lineId }).update(update);
    return { success: true, updated: Number(updated) > 0 };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}

// ---------------------------------------------------------------------
// delete lines for a statement (used when re-extracting)
// ---------------------------------------------------------------------

export interface DeleteStatementLinesResponse {
  success: boolean;
  deleted: number;
  error?: string;
}

export async function deleteStatementLines(
  appDb: Knex,
  statementId: number,
): Promise<DeleteStatementLinesResponse> {
  if (!Number.isFinite(statementId) || statementId <= 0) {
    return {
      success: false,
      deleted: 0,
      error: 'statement_id is required (positive number)',
    };
  }
  try {
    const deleted = await appDb('statement_lines')
      .where({ statement_id: statementId })
      .delete();
    return { success: true, deleted: Number(deleted) };
  } catch (err: any) {
    return { success: false, deleted: 0, error: err?.message ?? String(err) };
  }
}

// ---------------------------------------------------------------------
// statement_opera_only list (statement-side items missing in Opera)
// ---------------------------------------------------------------------

export interface ListOperaOnlyResponse {
  success: boolean;
  items: StatementOperaOnly[];
  count: number;
  total_amount: number;
  error?: string;
}

export async function listOperaOnlyItems(
  appDb: Knex,
  statementId: number,
): Promise<ListOperaOnlyResponse> {
  if (!Number.isFinite(statementId) || statementId <= 0) {
    return {
      success: false,
      items: [],
      count: 0,
      total_amount: 0,
      error: 'statement_id is required (positive number)',
    };
  }
  try {
    const rows = (await appDb('statement_opera_only')
      .where({ statement_id: statementId })
      .orderBy('id', 'asc')) as unknown as Array<{
      id: number;
      statement_id: number;
      reference: string | null;
      amount: number | null;
      reason: string | null;
    }>;
    const items: StatementOperaOnly[] = rows.map((r) => ({
      id: r.id,
      statement_id: r.statement_id,
      reference: r.reference ?? '',
      amount: Number(r.amount ?? 0),
      reason: r.reason ?? '',
    }));
    const total = items.reduce((s, i) => s + i.amount, 0);
    return {
      success: true,
      items,
      count: items.length,
      total_amount: r2(total),
    };
  } catch (err: any) {
    return {
      success: false,
      items: [],
      count: 0,
      total_amount: 0,
      error: err?.message ?? String(err),
    };
  }
}
