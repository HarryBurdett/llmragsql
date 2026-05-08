/**
 * Supplier statements — list and detail.
 *
 * Reads the per-app `supplier_statements` table (header) and joins
 * `statement_lines` for the detail view. Greenfield TS port (the
 * Python suppliers app is incomplete).
 *
 * Statement headers come from the AI extraction pipeline; line items
 * are populated when the user accepts the extraction.
 */
import type { Knex } from 'knex';

function dateToYmd(d: Date | string | null): string {
  if (!d) return '';
  if (d instanceof Date) {
    if (Number.isNaN(d.getTime())) return '';
    return d.toISOString().slice(0, 10);
  }
  return String(d).slice(0, 10);
}

function dateToIso(d: Date | string | null): string {
  if (!d) return '';
  if (d instanceof Date) {
    if (Number.isNaN(d.getTime())) return '';
    return d.toISOString();
  }
  return String(d);
}

export interface StatementHeader {
  id: number;
  supplier_code: string;
  statement_date: string;
  opening_balance: number;
  closing_balance: number;
  source: string;
  source_ref: string;
  pdf_path: string;
  imported_at: string;
}

export interface StatementLine {
  id: number;
  statement_id: number;
  line_date: string;
  reference: string;
  description: string;
  amount: number;
  matched_opera_ref: string;
  match_status: string;
}

// ---------------------------------------------------------------------
// list (header only)
// ---------------------------------------------------------------------

export interface ListStatementsOptions {
  supplierCode?: string | null;
  fromDate?: string | null;
  toDate?: string | null;
  limit?: number;
}

export interface ListStatementsResponse {
  success: boolean;
  statements: StatementHeader[];
  count: number;
  error?: string;
}

export async function listStatements(
  appDb: Knex,
  opts: ListStatementsOptions = {},
): Promise<ListStatementsResponse> {
  try {
    const limit = opts.limit ?? 100;
    let query = appDb('supplier_statements')
      .orderBy('statement_date', 'desc')
      .orderBy('imported_at', 'desc')
      .limit(limit);

    if (opts.supplierCode) {
      query = query.where({ supplier_code: opts.supplierCode });
    }
    if (opts.fromDate) {
      query = query.andWhere('statement_date', '>=', opts.fromDate);
    }
    if (opts.toDate) {
      query = query.andWhere('statement_date', '<=', opts.toDate);
    }

    const rows = (await query) as unknown as Array<{
      id: number;
      supplier_code: string;
      statement_date: Date | string | null;
      opening_balance: number | null;
      closing_balance: number | null;
      source: string | null;
      source_ref: string | null;
      pdf_path: string | null;
      imported_at: Date | string;
    }>;

    const statements: StatementHeader[] = rows.map((r) => ({
      id: r.id,
      supplier_code: r.supplier_code,
      statement_date: dateToYmd(r.statement_date),
      opening_balance: Number(r.opening_balance ?? 0),
      closing_balance: Number(r.closing_balance ?? 0),
      source: r.source ?? '',
      source_ref: r.source_ref ?? '',
      pdf_path: r.pdf_path ?? '',
      imported_at: dateToIso(r.imported_at),
    }));

    return { success: true, statements, count: statements.length };
  } catch (err: any) {
    return {
      success: false,
      statements: [],
      count: 0,
      error: err?.message ?? String(err),
    };
  }
}

// ---------------------------------------------------------------------
// get statement detail (header + lines + opera-only)
// ---------------------------------------------------------------------

export interface OperaOnlyItem {
  id: number;
  statement_id: number;
  reference: string;
  amount: number;
  reason: string;
}

export interface StatementDetail {
  header: StatementHeader;
  lines: StatementLine[];
  opera_only: OperaOnlyItem[];
}

export interface GetStatementResponse {
  success: boolean;
  statement?: StatementDetail;
  error?: string;
}

export async function getStatement(
  appDb: Knex,
  statementId: number,
): Promise<GetStatementResponse> {
  if (!Number.isFinite(statementId) || statementId <= 0) {
    return { success: false, error: 'Invalid statement_id' };
  }

  try {
    const headerRow = (await appDb('supplier_statements')
      .where({ id: statementId })
      .first()) as
      | {
          id: number;
          supplier_code: string;
          statement_date: Date | string | null;
          opening_balance: number | null;
          closing_balance: number | null;
          source: string | null;
          source_ref: string | null;
          pdf_path: string | null;
          imported_at: Date | string;
        }
      | undefined;

    if (!headerRow) {
      return { success: false, error: `Statement ${statementId} not found` };
    }

    const linesRows = (await appDb('statement_lines')
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

    const operaOnlyRows = (await appDb('statement_opera_only')
      .where({ statement_id: statementId })) as unknown as Array<{
      id: number;
      statement_id: number;
      reference: string | null;
      amount: number | null;
      reason: string | null;
    }>;

    const detail: StatementDetail = {
      header: {
        id: headerRow.id,
        supplier_code: headerRow.supplier_code,
        statement_date: dateToYmd(headerRow.statement_date),
        opening_balance: Number(headerRow.opening_balance ?? 0),
        closing_balance: Number(headerRow.closing_balance ?? 0),
        source: headerRow.source ?? '',
        source_ref: headerRow.source_ref ?? '',
        pdf_path: headerRow.pdf_path ?? '',
        imported_at: dateToIso(headerRow.imported_at),
      },
      lines: linesRows.map((r) => ({
        id: r.id,
        statement_id: r.statement_id,
        line_date: dateToYmd(r.line_date),
        reference: r.reference ?? '',
        description: r.description ?? '',
        amount: Number(r.amount ?? 0),
        matched_opera_ref: r.matched_opera_ref ?? '',
        match_status: r.match_status ?? '',
      })),
      opera_only: operaOnlyRows.map((r) => ({
        id: r.id,
        statement_id: r.statement_id,
        reference: r.reference ?? '',
        amount: Number(r.amount ?? 0),
        reason: r.reason ?? '',
      })),
    };

    return { success: true, statement: detail };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}
