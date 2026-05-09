/**
 * GoCardless mandate-match suggestion.
 *
 * Faithful port of suggest_mandate_match
 * (apps/gocardless/api/routes.py:7644-7718). Returns up to 5
 * candidate Opera customers ranked by similarity to a GoCardless
 * customer name.
 *
 * Scoring tiers (matches Python exactly):
 *   1. Exact match after normalisation              → 1.0
 *   2. Containment (one normalised name contains
 *      the other)                                   → 0.85
 *   3. Fuzzy ratio via Ratcliff/Obershelp algorithm → 0..1
 *
 * Threshold: candidates below 0.5 are dropped. Final list sorted
 * by score desc, with GC-tagged customers (sn_analsys='GC') tied
 * at the top, then name asc.
 *
 * Normalisation: uppercase, trim, strip common company suffixes
 * ` LTD`, ` LIMITED`, ` PLC`, ` INC`, ` LLC`, ` CO`, ` COMPANY`,
 * `.` (single trailing dot). Note this is a SUPERSET of the
 * normalisation used in `mandates.ts:normaliseCompanyName` —
 * Python's suggest_mandate_match also strips a trailing period.
 */
import type { Knex } from 'knex';

export interface MatchSuggestion {
  account: string;
  name: string;
  score: number;
  is_gc: boolean;
}

export interface SuggestMandateMatchResponse {
  success: boolean;
  suggestions: MatchSuggestion[];
  gc_name: string;
  error?: string;
}

const SUFFIXES = [
  ' LTD',
  ' LIMITED',
  ' PLC',
  ' INC',
  ' LLC',
  ' CO',
  ' COMPANY',
];

export function normaliseSuggestName(name: string | null | undefined): string {
  if (!name) return '';
  let n = name.toUpperCase().trim();
  for (const suffix of SUFFIXES) {
    if (n.endsWith(suffix)) {
      n = n.slice(0, n.length - suffix.length);
      break;
    }
  }
  // Python also strips a trailing single '.' character (its suffix
  // list contains '.' after the company-name suffixes).
  if (n.endsWith('.')) n = n.slice(0, -1);
  return n.trim();
}

// ---------------------------------------------------------------------
// Ratcliff/Obershelp ratio — port of Python's difflib.SequenceMatcher
// ---------------------------------------------------------------------

interface MatchBlock {
  a: number;
  b: number;
  size: number;
}

/**
 * Find the longest contiguous matching subsequence between a[alo:ahi]
 * and b[blo:bhi]. Returns a Match block; size=0 if none found.
 *
 * Faithful port of `SequenceMatcher.find_longest_match` (CPython
 * Lib/difflib.py). We don't implement the autojunk heuristic — for
 * short company names it has no effect. b2j builds a position
 * lookup once per call.
 */
function findLongestMatch(
  a: string,
  b: string,
  alo: number,
  ahi: number,
  blo: number,
  bhi: number,
): MatchBlock {
  const b2j = new Map<string, number[]>();
  for (let i = blo; i < bhi; i++) {
    const ch = b[i]!;
    const arr = b2j.get(ch);
    if (arr) arr.push(i);
    else b2j.set(ch, [i]);
  }

  let besti = alo;
  let bestj = blo;
  let bestsize = 0;
  let j2len = new Map<number, number>();

  for (let i = alo; i < ahi; i++) {
    const newJ2len = new Map<number, number>();
    const positions = b2j.get(a[i]!);
    if (positions) {
      for (const j of positions) {
        if (j < blo) continue;
        if (j >= bhi) break;
        const k = (j2len.get(j - 1) ?? 0) + 1;
        newJ2len.set(j, k);
        if (k > bestsize) {
          besti = i - k + 1;
          bestj = j - k + 1;
          bestsize = k;
        }
      }
    }
    j2len = newJ2len;
  }
  return { a: besti, b: bestj, size: bestsize };
}

function getMatchingBlocks(a: string, b: string): MatchBlock[] {
  // Recursive / queue-based to mirror Python's behaviour — collect
  // all maximal-match triples, append a sentinel, then return
  // (Python's matching_blocks).
  const queue: Array<[number, number, number, number]> = [
    [0, a.length, 0, b.length],
  ];
  const matches: MatchBlock[] = [];
  while (queue.length > 0) {
    const [alo, ahi, blo, bhi] = queue.pop()!;
    const m = findLongestMatch(a, b, alo, ahi, blo, bhi);
    if (m.size > 0) {
      matches.push(m);
      if (alo < m.a && blo < m.b) {
        queue.push([alo, m.a, blo, m.b]);
      }
      if (m.a + m.size < ahi && m.b + m.size < bhi) {
        queue.push([m.a + m.size, ahi, m.b + m.size, bhi]);
      }
    }
  }
  matches.sort((x, y) => x.a - y.a || x.b - y.b);
  return matches;
}

export function sequenceMatcherRatio(a: string, b: string): number {
  if (!a && !b) return 1.0;
  const total = a.length + b.length;
  if (total === 0) return 0;
  const matches = getMatchingBlocks(a, b);
  let matched = 0;
  for (const m of matches) matched += m.size;
  return (2 * matched) / total;
}

// ---------------------------------------------------------------------
// Service entry point
// ---------------------------------------------------------------------

interface CustomerRow {
  account: string | null;
  name: string | null;
  analsys: string | null;
  balance: number | string | null;
}

export async function suggestMandateMatch(
  operaDb: Knex,
  gcName: string,
): Promise<SuggestMandateMatchResponse> {
  const trimmedName = (gcName ?? '').trim();
  try {
    const rows = (await operaDb('sname')
      .where({ sn_stop: 0 })
      .orderBy('sn_name', 'asc')
      .select(
        operaDb.raw('RTRIM(sn_account) AS account'),
        operaDb.raw('RTRIM(sn_name) AS name'),
        operaDb.raw('RTRIM(sn_analsys) AS analsys'),
        operaDb.raw('sn_currbal AS balance'),
      )) as unknown as CustomerRow[];
    if (!rows || rows.length === 0) {
      return { success: true, suggestions: [], gc_name: trimmedName };
    }
    const gcNorm = normaliseSuggestName(trimmedName);
    if (!gcNorm) {
      return { success: true, suggestions: [], gc_name: trimmedName };
    }

    const candidates: MatchSuggestion[] = [];
    for (const row of rows) {
      const account = (row.account ?? '').trim();
      const name = (row.name ?? '').trim();
      if (!account || !name) continue;
      const operaNorm = normaliseSuggestName(name);
      let score: number;
      if (gcNorm === operaNorm) {
        score = 1.0;
      } else if (
        operaNorm.length > 0 &&
        (gcNorm.includes(operaNorm) || operaNorm.includes(gcNorm))
      ) {
        score = 0.85;
      } else {
        score = sequenceMatcherRatio(gcNorm, operaNorm);
      }
      if (score >= 0.5) {
        candidates.push({
          account,
          name,
          score: Math.round(score * 1000) / 1000,
          is_gc:
            (row.analsys ?? '').toString().trim().toUpperCase() === 'GC',
        });
      }
    }

    candidates.sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      // GC-tagged customers tied at the top
      if (a.is_gc !== b.is_gc) return a.is_gc ? -1 : 1;
      return a.name.localeCompare(b.name);
    });

    return {
      success: true,
      suggestions: candidates.slice(0, 5),
      gc_name: trimmedName,
    };
  } catch {
    // Match Python: dashboard always loads, soft success on error
    return { success: true, suggestions: [], gc_name: trimmedName };
  }
}
