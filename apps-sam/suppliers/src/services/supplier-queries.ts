/**
 * Supplier queries — open/resolve workflow.
 *
 * Faithful ports of:
 *   - get_supplier_queries          (routes.py:699-807)
 *   - resolve_supplier_query        (routes.py:809-843)
 *   - auto_resolve_supplier_queries (routes.py:845-963)
 *   - get_overdue_supplier_queries  (routes.py:1237-1320)
 *   - send_supplier_query_reminder  (routes.py:965-1235 — mock-only stub)
 *
 * Auto-resolve runs deterministic rules: queries that match a recently-
 * posted Opera transaction (by reference + amount within 1p) get
 * marked as resolved with a note. The Opera lookup is wrapped so a
 * mock connector keeps tests deterministic.
 */
import type { Knex } from 'knex';

export interface SupplierQuery {
  id: number;
  supplier_code: string;
  statement_id: number | null;
  line_id: number | null;
  reference: string;
  amount: number;
  query_type: string;
  status: 'open' | 'resolved' | 'cancelled';
  description: string;
  resolution_notes: string | null;
  resolved_by: string | null;
  resolved_at: string | null;
  created_at: string;
  reminder_sent_at: string | null;
  reminder_count: number;
}

export interface ListQueriesOptions {
  supplierCode?: string | null;
  status?: 'open' | 'resolved' | 'cancelled' | null;
  limit?: number;
}

export interface ListQueriesResponse {
  success: boolean;
  queries: SupplierQuery[];
  count: number;
  error?: string;
}

function toIsoOrNull(value: Date | string | null | undefined): string | null {
  if (!value) return null;
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) return null;
    return value.toISOString();
  }
  return String(value);
}

interface QueryRowRaw {
  id: number;
  supplier_code: string;
  statement_id: number | null;
  line_id: number | null;
  reference: string | null;
  amount: number | string | null;
  query_type: string | null;
  status: string | null;
  description: string | null;
  resolution_notes: string | null;
  resolved_by: string | null;
  resolved_at: Date | string | null;
  created_at: Date | string | null;
  reminder_sent_at: Date | string | null;
  reminder_count: number | string | null;
}

function mapQueryRow(row: QueryRowRaw): SupplierQuery {
  const status = (row.status ?? 'open') as SupplierQuery['status'];
  return {
    id: Number(row.id),
    supplier_code: row.supplier_code,
    statement_id:
      row.statement_id !== null && row.statement_id !== undefined
        ? Number(row.statement_id)
        : null,
    line_id:
      row.line_id !== null && row.line_id !== undefined
        ? Number(row.line_id)
        : null,
    reference: row.reference ?? '',
    amount: Number(row.amount ?? 0),
    query_type: row.query_type ?? '',
    status,
    description: row.description ?? '',
    resolution_notes: row.resolution_notes,
    resolved_by: row.resolved_by,
    resolved_at: toIsoOrNull(row.resolved_at),
    created_at: toIsoOrNull(row.created_at) ?? '',
    reminder_sent_at: toIsoOrNull(row.reminder_sent_at),
    reminder_count: Number(row.reminder_count ?? 0),
  };
}

export async function listQueries(
  appDb: Knex,
  opts: ListQueriesOptions = {},
): Promise<ListQueriesResponse> {
  try {
    const limit = opts.limit ?? 200;
    let query = appDb('supplier_queries')
      .orderBy('created_at', 'desc')
      .limit(limit);
    if (opts.supplierCode) {
      query = query.where({ supplier_code: opts.supplierCode });
    }
    if (opts.status) {
      query = query.andWhere({ status: opts.status });
    }
    const rows = (await query) as unknown as QueryRowRaw[];
    return {
      success: true,
      queries: rows.map(mapQueryRow),
      count: rows.length,
    };
  } catch (err: any) {
    return {
      success: false,
      queries: [],
      count: 0,
      error: err?.message ?? String(err),
    };
  }
}

export interface ResolveQueryInput {
  queryId: number;
  resolvedBy: string;
  notes?: string | null;
}

export interface ResolveQueryResponse {
  success: boolean;
  query?: SupplierQuery;
  error?: string;
}

export async function resolveQuery(
  appDb: Knex,
  input: ResolveQueryInput,
): Promise<ResolveQueryResponse> {
  if (!Number.isFinite(input.queryId) || input.queryId <= 0) {
    return { success: false, error: 'Invalid query_id' };
  }
  try {
    const updated = await appDb('supplier_queries')
      .where({ id: input.queryId })
      .andWhere({ status: 'open' })
      .update({
        status: 'resolved',
        resolved_by: input.resolvedBy,
        resolved_at: appDb.fn.now(),
        resolution_notes: input.notes ?? null,
      });
    if (!updated) {
      return { success: false, error: 'Query not found or already resolved' };
    }
    const row = (await appDb('supplier_queries')
      .where({ id: input.queryId })
      .first()) as QueryRowRaw | undefined;
    return {
      success: true,
      query: row ? mapQueryRow(row) : undefined,
    };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}

export interface OperaPaymentLookup {
  /**
   * Returns true when a posted purchase ledger transaction matches
   * the given supplier + reference + amount within 1p. Implemented
   * by the route layer against operaDb; tests pass a mock.
   */
  hasMatchingPosting(opts: {
    supplierCode: string;
    reference: string;
    amountPounds: number;
  }): Promise<boolean>;
}

export interface AutoResolveResponse {
  success: boolean;
  resolved_count: number;
  scanned_count: number;
  error?: string;
}

export async function autoResolveQueries(
  appDb: Knex,
  lookup: OperaPaymentLookup,
  resolvedBy: string,
): Promise<AutoResolveResponse> {
  try {
    const open = (await appDb('supplier_queries')
      .where({ status: 'open' })
      .select(
        'id',
        'supplier_code',
        'reference',
        'amount',
      )) as unknown as Array<{
      id: number;
      supplier_code: string;
      reference: string | null;
      amount: number | string | null;
    }>;

    let resolved = 0;
    for (const q of open) {
      const ref = (q.reference ?? '').trim();
      if (!ref) continue;
      const amount = Number(q.amount ?? 0);
      const matched = await lookup.hasMatchingPosting({
        supplierCode: q.supplier_code,
        reference: ref,
        amountPounds: amount,
      });
      if (!matched) continue;
      await appDb('supplier_queries').where({ id: q.id }).update({
        status: 'resolved',
        resolved_by: resolvedBy,
        resolved_at: appDb.fn.now(),
        resolution_notes: 'auto-resolved: matching posting found in Opera',
      });
      resolved += 1;
    }

    return {
      success: true,
      resolved_count: resolved,
      scanned_count: open.length,
    };
  } catch (err: any) {
    return {
      success: false,
      resolved_count: 0,
      scanned_count: 0,
      error: err?.message ?? String(err),
    };
  }
}

export interface OverdueResponse extends ListQueriesResponse {
  threshold_days: number;
}

export async function listOverdueQueries(
  appDb: Knex,
  thresholdDays = 7,
): Promise<OverdueResponse> {
  try {
    const cutoff = new Date(
      Date.now() - thresholdDays * 24 * 60 * 60 * 1000,
    ).toISOString();
    const rows = (await appDb('supplier_queries')
      .where({ status: 'open' })
      .andWhere('created_at', '<', cutoff)
      .orderBy('created_at', 'asc')) as unknown as QueryRowRaw[];
    return {
      success: true,
      queries: rows.map(mapQueryRow),
      count: rows.length,
      threshold_days: thresholdDays,
    };
  } catch (err: any) {
    return {
      success: false,
      queries: [],
      count: 0,
      threshold_days: thresholdDays,
      error: err?.message ?? String(err),
    };
  }
}

export interface SendReminderInput {
  queryId: number;
  triggeredBy: string;
}

export interface SendReminderResponse {
  success: boolean;
  query?: SupplierQuery;
  error?: string;
}

/**
 * Bumps reminder count + sets reminder_sent_at. The actual email send
 * is handled by the route layer using ctx.email — this function
 * commits the audit row.
 */
export async function recordReminderSent(
  appDb: Knex,
  input: SendReminderInput,
): Promise<SendReminderResponse> {
  if (!Number.isFinite(input.queryId) || input.queryId <= 0) {
    return { success: false, error: 'Invalid query_id' };
  }
  try {
    const updated = await appDb('supplier_queries')
      .where({ id: input.queryId })
      .andWhere({ status: 'open' })
      .update({
        reminder_sent_at: appDb.fn.now(),
        reminder_count: appDb.raw('COALESCE(reminder_count, 0) + 1'),
      });
    if (!updated) {
      return { success: false, error: 'Query not found or already resolved' };
    }
    const row = (await appDb('supplier_queries')
      .where({ id: input.queryId })
      .first()) as QueryRowRaw | undefined;
    return { success: true, query: row ? mapQueryRow(row) : undefined };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}
