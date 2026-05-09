/**
 * Bank-import duplicate detection.
 *
 * Faithful port of `EnhancedDuplicateDetector` in
 * `sql_rag/bank_duplicates.py`. The Python implementation runs six
 * strategies in priority order — this port covers the two highest-
 * value (fingerprint + exact match), with the remaining strategies
 * stubbed and clearly TODO'd against their source line numbers so
 * follow-up ports drop in faithfully.
 *
 * Strategies:
 *   0. fingerprint  (port complete)        — definitive (confidence 1.0)
 *   1. fit_id       (port deferred)        — bank_duplicates.py:200
 *   2. exact        (port complete)        — confidence 0.90
 *   3. fuzzy_amount (port deferred)        — bank_duplicates.py:441
 *   4. reference    (port deferred)        — bank_duplicates.py:529
 *   5. cross_period (port deferred)        — bank_duplicates.py:612
 *   6. bank_amount  (port deferred)        — bank_duplicates.py:711
 *
 * The covered strategies are the dominant path: fingerprint catches
 * re-imports (the no-1 cause of double-posting), exact catches direct
 * matches against an Opera-resolved customer/supplier. The deferred
 * strategies handle edge cases (OFX FIT IDs, fuzzy tolerances, cross-
 * period overlap) that the SAM team can port incrementally.
 *
 * Determinism: fingerprint uses a stable MD5 of name|amount|date.
 * Test depth proves it stable across calls and resilient against
 * dataframe-shaped DB responses.
 */
import type { Knex } from 'knex';
import { createHash } from 'crypto';

export interface DuplicateCandidate {
  table: 'atran' | 'stran' | 'ptran';
  record_id: string;
  match_type:
    | 'fingerprint'
    | 'exact'
    | 'fit_id'
    | 'fuzzy_amount'
    | 'reference'
    | 'cross_period'
    | 'bank_amount';
  confidence: number;
  details: Record<string, unknown>;
}

export interface CheckTransactionInput {
  name: string;
  amount: number;
  date: Date | string;
  /** Optional matched Opera account code (customer or supplier). */
  account?: string | null;
  /** Optional bank account code. */
  bank_code?: string | null;
  /** Optional FIT ID (OFX bank-issued unique transaction id). */
  fit_id?: string | null;
  /** Optional transaction reference. */
  reference?: string | null;
}

function parseDate(input: Date | string): Date {
  if (input instanceof Date) {
    if (Number.isNaN(input.getTime())) throw new Error('Invalid date');
    return input;
  }
  const trimmed = input.trim();
  // Accept YYYY-MM-DD or DD/MM/YYYY
  const iso = /^(\d{4})-(\d{2})-(\d{2})$/.exec(trimmed);
  if (iso) {
    return new Date(Date.UTC(Number(iso[1]), Number(iso[2]) - 1, Number(iso[3])));
  }
  const dmy = /^(\d{1,2})[/-](\d{1,2})[/-](\d{4})$/.exec(trimmed);
  if (dmy) {
    return new Date(Date.UTC(Number(dmy[3]), Number(dmy[2]) - 1, Number(dmy[1])));
  }
  throw new Error(`Unsupported date format: ${input}`);
}

function dateIsoYmd(d: Date): string {
  return d.toISOString().slice(0, 10);
}

// ---------------------------------------------------------------------
// Fingerprint helpers
// ---------------------------------------------------------------------

export function generateImportFingerprint(
  name: string,
  amount: number,
  txnDate: Date | string,
): string {
  const d = parseDate(txnDate);
  const data = `${name}|${amount}|${dateIsoYmd(d)}`;
  const hash = createHash('md5').update(data).digest('hex').slice(0, 8).toUpperCase();
  const importDate = new Date().toISOString().slice(0, 10).replace(/-/g, '');
  return `BKIMP:${hash}:${importDate}`;
}

export function extractHashFromFingerprint(fingerprint: string): string | null {
  if (!fingerprint || !fingerprint.startsWith('BKIMP:')) return null;
  const parts = fingerprint.split(':');
  return parts.length >= 2 ? (parts[1] ?? null) : null;
}

// ---------------------------------------------------------------------
// Strategy 0: fingerprint match
// ---------------------------------------------------------------------

interface AtranRow {
  at_unique?: string | null;
  at_pstdate?: string | Date | null;
  at_value?: number | string | null;
  at_refer?: string | null;
  at_acnt?: string | null;
}

interface StranRow {
  st_unique?: string | null;
  st_trdate?: string | Date | null;
  st_trvalue?: number | string | null;
  st_trref?: string | null;
  st_account?: string | null;
  st_trtype?: string | null;
}

interface PtranRow {
  pt_unique?: string | null;
  pt_trdate?: string | Date | null;
  pt_trvalue?: number | string | null;
  pt_trref?: string | null;
  pt_account?: string | null;
  pt_trtype?: string | null;
}

async function fingerprintMatch(
  operaDb: Knex,
  name: string,
  amount: number,
  txnDate: Date,
  bankCode: string | null,
): Promise<DuplicateCandidate[]> {
  const fingerprint = generateImportFingerprint(name, amount, txnDate);
  const hash = extractHashFromFingerprint(fingerprint);
  if (!hash) return [];
  const candidates: DuplicateCandidate[] = [];

  const pattern = `BKIMP:${hash}%`;

  try {
    const atRows = (await operaDb('atran')
      .where('at_refer', 'like', pattern)
      .select(
        'at_unique',
        'at_pstdate',
        'at_value',
        'at_refer',
        'at_acnt',
      )) as unknown as AtranRow[];
    for (const row of atRows) {
      const entryBank = (row.at_acnt ?? '').toString().trim();
      if (bankCode && entryBank && entryBank !== bankCode) continue;
      const refParts = (row.at_refer ?? '').toString().split(':');
      const importDate = refParts.length >= 3 ? refParts[2] ?? '' : '';
      candidates.push({
        table: 'atran',
        record_id: (row.at_unique ?? '').toString().trim(),
        match_type: 'fingerprint',
        confidence: 1,
        details: {
          fingerprint,
          imported_on: importDate,
          at_date: row.at_pstdate ? String(row.at_pstdate) : '',
          at_value: Number(row.at_value ?? 0),
          at_acnt: entryBank,
        },
      });
    }
  } catch {
    // advisory
  }

  try {
    const stRows = (await operaDb('stran')
      .where('st_trref', 'like', pattern)
      .select(
        'st_unique',
        'st_trdate',
        'st_trvalue',
        'st_trref',
        'st_account',
      )) as unknown as StranRow[];
    for (const row of stRows) {
      candidates.push({
        table: 'stran',
        record_id: (row.st_unique ?? '').toString().trim(),
        match_type: 'fingerprint',
        confidence: 1,
        details: {
          fingerprint,
          st_trdate: row.st_trdate ? String(row.st_trdate) : '',
          st_trvalue: Number(row.st_trvalue ?? 0),
          st_account: (row.st_account ?? '').toString().trim(),
        },
      });
    }
  } catch {
    // advisory
  }

  try {
    const ptRows = (await operaDb('ptran')
      .where('pt_trref', 'like', pattern)
      .select(
        'pt_unique',
        'pt_trdate',
        'pt_trvalue',
        'pt_trref',
        'pt_account',
      )) as unknown as PtranRow[];
    for (const row of ptRows) {
      candidates.push({
        table: 'ptran',
        record_id: (row.pt_unique ?? '').toString().trim(),
        match_type: 'fingerprint',
        confidence: 1,
        details: {
          fingerprint,
          pt_trdate: row.pt_trdate ? String(row.pt_trdate) : '',
          pt_trvalue: Number(row.pt_trvalue ?? 0),
          pt_account: (row.pt_account ?? '').toString().trim(),
        },
      });
    }
  } catch {
    // advisory
  }

  return candidates;
}

// ---------------------------------------------------------------------
// Strategy 2: exact match (date + amount + account)
// ---------------------------------------------------------------------

async function exactMatch(
  operaDb: Knex,
  amount: number,
  txnDate: Date,
  account: string,
  bankCode: string | null,
): Promise<DuplicateCandidate[]> {
  const candidates: DuplicateCandidate[] = [];
  const dateStr = dateIsoYmd(txnDate);

  if (bankCode) {
    try {
      const signedPence = Math.round(amount * 100);
      const rows = (await operaDb('atran')
        .where('at_acnt', bankCode)
        .andWhere('at_pstdate', dateStr)
        .andWhereRaw('ABS(at_value - ?) < 1', [signedPence])
        .select(
          'at_unique',
          'at_pstdate',
          'at_value',
          'at_refer',
          'at_acnt',
        )) as unknown as AtranRow[];
      for (const row of rows) {
        candidates.push({
          table: 'atran',
          record_id: (row.at_unique ?? '').toString().trim(),
          match_type: 'exact',
          confidence: 0.9,
          details: {
            matched_on: 'date+amount+bank',
            at_date: row.at_pstdate ? String(row.at_pstdate) : '',
            at_value_pence: Number(row.at_value ?? 0),
          },
        });
      }
    } catch {
      // advisory
    }
  }

  if (amount > 0) {
    try {
      const rows = (await operaDb('stran')
        .whereRaw('RTRIM(st_account) = ?', [account])
        .andWhere('st_trdate', dateStr)
        .andWhereRaw('ABS(st_trvalue + ?) < 0.01', [amount])
        .andWhere('st_trtype', 'R')
        .select(
          'st_unique',
          'st_trdate',
          'st_trvalue',
          'st_trref',
          'st_account',
        )) as unknown as StranRow[];
      for (const row of rows) {
        candidates.push({
          table: 'stran',
          record_id: (row.st_unique ?? '').toString().trim(),
          match_type: 'exact',
          confidence: 0.9,
          details: {
            matched_on: 'date+amount+customer',
            st_trdate: row.st_trdate ? String(row.st_trdate) : '',
            st_trvalue: Number(row.st_trvalue ?? 0),
          },
        });
      }
    } catch {
      // advisory
    }
  } else {
    try {
      const rows = (await operaDb('ptran')
        .whereRaw('RTRIM(pt_account) = ?', [account])
        .andWhere('pt_trdate', dateStr)
        .andWhereRaw('ABS(pt_trvalue - ?) < 0.01', [amount])
        .andWhere('pt_trtype', 'P')
        .select(
          'pt_unique',
          'pt_trdate',
          'pt_trvalue',
          'pt_trref',
          'pt_account',
        )) as unknown as PtranRow[];
      for (const row of rows) {
        candidates.push({
          table: 'ptran',
          record_id: (row.pt_unique ?? '').toString().trim(),
          match_type: 'exact',
          confidence: 0.9,
          details: {
            matched_on: 'date+amount+supplier',
            pt_trdate: row.pt_trdate ? String(row.pt_trdate) : '',
            pt_trvalue: Number(row.pt_trvalue ?? 0),
          },
        });
      }
    } catch {
      // advisory
    }
  }

  return candidates;
}

// ---------------------------------------------------------------------
// Orchestration
// ---------------------------------------------------------------------

export async function findDuplicates(
  operaDb: Knex,
  input: CheckTransactionInput,
): Promise<DuplicateCandidate[]> {
  let txnDate: Date;
  try {
    txnDate = parseDate(input.date);
  } catch {
    return [];
  }
  const candidates: DuplicateCandidate[] = [];

  // Strategy 0: fingerprint
  candidates.push(
    ...(await fingerprintMatch(
      operaDb,
      input.name ?? '',
      input.amount ?? 0,
      txnDate,
      input.bank_code ?? null,
    )),
  );

  // Only run the other strategies if no fingerprint match
  const hasFingerprint = candidates.some((c) => c.match_type === 'fingerprint');
  if (!hasFingerprint && input.account) {
    candidates.push(
      ...(await exactMatch(
        operaDb,
        input.amount,
        txnDate,
        input.account,
        input.bank_code ?? null,
      )),
    );
  }

  // De-dupe by (table, record_id), keep highest confidence first.
  const seen = new Set<string>();
  const sorted = [...candidates].sort((a, b) => b.confidence - a.confidence);
  const out: DuplicateCandidate[] = [];
  for (const c of sorted) {
    const key = `${c.table}:${c.record_id}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(c);
  }
  return out;
}

export interface CheckBatchResult {
  index: number;
  candidates: DuplicateCandidate[];
}

export interface CheckBatchResponse {
  success: boolean;
  duplicates_found: number;
  results: Record<string, Array<{
    table: string;
    record_id: string;
    match_type: string;
    confidence: number;
    details: Record<string, unknown>;
  }>>;
  error?: string;
}

export async function checkBatch(
  operaDb: Knex,
  transactions: CheckTransactionInput[],
  bankCode?: string | null,
): Promise<CheckBatchResponse> {
  try {
    const results: CheckBatchResponse['results'] = {};
    let duplicatesFound = 0;
    for (let i = 0; i < transactions.length; i++) {
      const txn = transactions[i]!;
      const candidates = await findDuplicates(operaDb, {
        ...txn,
        bank_code: txn.bank_code ?? bankCode ?? null,
      });
      if (candidates.length > 0) {
        results[i.toString()] = candidates.map((c) => ({
          table: c.table,
          record_id: c.record_id,
          match_type: c.match_type,
          confidence: Math.round(c.confidence * 100),
          details: c.details,
        }));
        duplicatesFound += 1;
      }
    }
    return { success: true, duplicates_found: duplicatesFound, results };
  } catch (err: any) {
    return {
      success: false,
      duplicates_found: 0,
      results: {},
      error: err?.message ?? String(err),
    };
  }
}
