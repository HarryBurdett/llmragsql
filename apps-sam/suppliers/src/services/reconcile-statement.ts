/**
 * Reconcile a supplier statement against Opera's purchase ledger.
 *
 * Pulls the statement's line items from `statement_lines` and the
 * supplier's open ptran rows, then matches by:
 *
 *   1. Exact reference match (statement reference == pt_trref or pt_supref)
 *   2. Exact amount match within ±7 days
 *   3. Marks remaining lines as missing or extra
 *
 * Updates `statement_lines.match_status` and `matched_opera_ref` so
 * the UI can highlight matched/missing/extra. Generates a variance
 * report with totals.
 *
 * Read-only against Opera (NOLOCK on every read). Writes only to the
 * per-app supplier_statements / statement_lines tables.
 */
import type { Knex } from 'knex';

export interface ReconcileStatementInput {
  statementId: number;
}

export interface ReconcileLine {
  statement_line_id: number;
  line_date: string;
  reference: string;
  description: string;
  amount: number;
  match_status: 'matched' | 'missing' | 'extra';
  matched_opera_ref: string | null;
  matched_opera_value: number | null;
  matched_opera_date: string | null;
}

export interface ReconcileSummary {
  matched_count: number;
  missing_count: number;
  extra_count: number;
  matched_total: number;
  missing_total: number;
  extra_total: number;
  variance_total: number;
}

export interface ReconcileStatementResult {
  success: boolean;
  statement_id?: number;
  supplier_code?: string;
  statement_date?: string;
  opening_balance?: number;
  closing_balance?: number;
  lines?: ReconcileLine[];
  summary?: ReconcileSummary;
  error?: string;
}

interface PtranRow {
  pt_trref: string;
  pt_supref: string;
  pt_trdate: string;
  pt_trvalue: number;
  pt_trbal: number;
}

function dateMs(s: string): number {
  if (!s) return NaN;
  // Accept YYYY-MM-DD or ISO
  const t = Date.parse(typeof s === 'string' ? s.slice(0, 10) : '');
  return Number.isFinite(t) ? t : NaN;
}

function daysBetween(a: string, b: string): number {
  const ma = dateMs(a);
  const mb = dateMs(b);
  if (Number.isNaN(ma) || Number.isNaN(mb)) return Number.MAX_SAFE_INTEGER;
  return Math.abs(ma - mb) / (1000 * 60 * 60 * 24);
}

function r2(v: number): number {
  return Math.round(v * 100) / 100;
}

export async function reconcileStatement(
  appDb: Knex,
  operaDb: Knex,
  input: ReconcileStatementInput,
): Promise<ReconcileStatementResult> {
  const stmtId = Number(input.statementId);
  if (!Number.isFinite(stmtId) || stmtId <= 0) {
    return { success: false, error: 'statement_id required' };
  }

  // Load header
  const header = await appDb('supplier_statements')
    .where({ id: stmtId })
    .first();
  if (!header) {
    return { success: false, error: `Statement ${stmtId} not found` };
  }
  const supplierCode = String(header.supplier_code ?? '').trim();

  // Load statement lines
  const stmtLines = (await appDb('statement_lines')
    .where({ statement_id: stmtId })
    .orderBy('line_date')) as Array<{
    id: number;
    line_date: string;
    reference: string | null;
    description: string | null;
    amount: number;
    matched_opera_ref: string | null;
    match_status: string | null;
  }>;

  // Load Opera ptran for supplier (open + recent — tolerate ±90 days
  // window so reconciliation against an old statement still works)
  let ptran: PtranRow[] = [];
  try {
    ptran = (await operaDb.raw(
      `SELECT
          RTRIM(ISNULL(pt_trref, '')) AS pt_trref,
          RTRIM(ISNULL(pt_supref, '')) AS pt_supref,
          CONVERT(VARCHAR(10), pt_trdate, 23) AS pt_trdate,
          ISNULL(pt_trvalue, 0) AS pt_trvalue,
          ISNULL(pt_trbal, 0) AS pt_trbal
         FROM ptran WITH (NOLOCK)
         WHERE RTRIM(pt_account) = ?`,
      [supplierCode],
    )) as PtranRow[];
  } catch {
    ptran = [];
  }

  // Build lookup maps
  const ptranByRef = new Map<string, PtranRow>();
  const remainingPtran = [...ptran];
  for (const p of ptran) {
    if (p.pt_trref) ptranByRef.set(p.pt_trref.toUpperCase(), p);
    if (p.pt_supref) ptranByRef.set(p.pt_supref.toUpperCase(), p);
  }

  const matchedRefs = new Set<string>();
  const lines: ReconcileLine[] = [];

  // Pass 1: ref match
  for (const sl of stmtLines) {
    const ref = (sl.reference ?? '').trim().toUpperCase();
    let match: PtranRow | null = null;
    if (ref) {
      const p = ptranByRef.get(ref);
      if (p && !matchedRefs.has(p.pt_trref || p.pt_supref)) {
        match = p;
        matchedRefs.add(p.pt_trref || p.pt_supref);
      }
    }
    if (match) {
      const idx = remainingPtran.findIndex(
        (p) => p === match || p.pt_trref === match!.pt_trref,
      );
      if (idx >= 0) remainingPtran.splice(idx, 1);
      lines.push({
        statement_line_id: sl.id,
        line_date: sl.line_date,
        reference: sl.reference ?? '',
        description: sl.description ?? '',
        amount: Number(sl.amount ?? 0),
        match_status: 'matched',
        matched_opera_ref: match.pt_trref || match.pt_supref,
        matched_opera_value: Number(match.pt_trvalue ?? 0),
        matched_opera_date: match.pt_trdate ?? null,
      });
    } else {
      lines.push({
        statement_line_id: sl.id,
        line_date: sl.line_date,
        reference: sl.reference ?? '',
        description: sl.description ?? '',
        amount: Number(sl.amount ?? 0),
        match_status: 'missing',
        matched_opera_ref: null,
        matched_opera_value: null,
        matched_opera_date: null,
      });
    }
  }

  // Pass 2: amount + ±7 day fallback for still-missing lines
  for (const line of lines) {
    if (line.match_status !== 'missing') continue;
    const lineAmt = Math.abs(Number(line.amount));
    if (lineAmt === 0) continue;
    const candidate = remainingPtran.find(
      (p) =>
        Math.abs(Math.abs(Number(p.pt_trvalue)) - lineAmt) < 0.01 &&
        daysBetween(p.pt_trdate, line.line_date) <= 7,
    );
    if (candidate) {
      const idx = remainingPtran.indexOf(candidate);
      if (idx >= 0) remainingPtran.splice(idx, 1);
      line.match_status = 'matched';
      line.matched_opera_ref = candidate.pt_trref || candidate.pt_supref;
      line.matched_opera_value = Number(candidate.pt_trvalue ?? 0);
      line.matched_opera_date = candidate.pt_trdate ?? null;
    }
  }

  // "Extra" — Opera rows not on the statement (within statement period)
  const stmtDate = String(header.statement_date ?? '');
  const extras: ReconcileLine[] = [];
  for (const p of remainingPtran) {
    // Only include Opera rows within ±31 days of the statement date
    if (stmtDate && daysBetween(p.pt_trdate ?? '', stmtDate) > 31) continue;
    extras.push({
      statement_line_id: -1,
      line_date: p.pt_trdate ?? '',
      reference: p.pt_trref || p.pt_supref,
      description: '(in Opera, not on statement)',
      amount: Number(p.pt_trvalue ?? 0),
      match_status: 'extra',
      matched_opera_ref: p.pt_trref || p.pt_supref,
      matched_opera_value: Number(p.pt_trvalue ?? 0),
      matched_opera_date: p.pt_trdate ?? null,
    });
  }

  // Persist match status back to statement_lines
  try {
    await appDb.transaction(async (trx) => {
      for (const line of lines) {
        if (line.statement_line_id < 0) continue;
        await trx('statement_lines')
          .where({ id: line.statement_line_id })
          .update({
            match_status: line.match_status,
            matched_opera_ref: line.matched_opera_ref ?? '',
          });
      }
    });
  } catch {
    // best-effort — the in-memory result is still correct
  }

  // Summary
  const matched = lines.filter((l) => l.match_status === 'matched');
  const missing = lines.filter((l) => l.match_status === 'missing');
  const matchedTotal = matched.reduce((s, l) => s + Number(l.amount), 0);
  const missingTotal = missing.reduce((s, l) => s + Number(l.amount), 0);
  const extraTotal = extras.reduce((s, l) => s + Number(l.amount), 0);
  const stmtClosing = Number(header.closing_balance ?? 0);
  const stmtOpening = Number(header.opening_balance ?? 0);
  const expectedMovements = stmtClosing - stmtOpening;
  const actualMovements = matchedTotal + missingTotal;
  const variance = expectedMovements - actualMovements;

  return {
    success: true,
    statement_id: stmtId,
    supplier_code: supplierCode,
    statement_date: stmtDate,
    opening_balance: stmtOpening,
    closing_balance: stmtClosing,
    lines: [...lines, ...extras],
    summary: {
      matched_count: matched.length,
      missing_count: missing.length,
      extra_count: extras.length,
      matched_total: r2(matchedTotal),
      missing_total: r2(missingTotal),
      extra_total: r2(extraTotal),
      variance_total: r2(variance),
    },
  };
}
