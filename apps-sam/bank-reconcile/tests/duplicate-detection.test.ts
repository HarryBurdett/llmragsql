import { describe, it, expect } from 'vitest';
import {
  generateImportFingerprint,
  extractHashFromFingerprint,
  findDuplicates,
  checkBatch,
} from '../src/services/duplicate-detection.js';

interface AtranRow {
  at_unique: string;
  at_pstdate: string;
  at_value: number;
  at_refer: string;
  at_acnt: string;
}

interface StranRow {
  st_unique: string;
  st_trdate: string;
  st_trvalue: number;
  st_trref: string;
  st_account: string;
  st_trtype: string;
}

interface PtranRow {
  pt_unique: string;
  pt_trdate: string;
  pt_trvalue: number;
  pt_trref: string;
  pt_account: string;
  pt_trtype: string;
}

interface State {
  atran: AtranRow[];
  stran: StranRow[];
  ptran: PtranRow[];
}

function makeOperaDb(state: State): any {
  function tableBuilder(table: string) {
    let pattern: string | null = null;
    let bankCodeFilter: string | null = null;
    let dateFilter: string | null = null;
    let typeFilter: string | null = null;
    let accountFilter: string | null = null;
    let rawAmountValue: number | null = null;
    let rawAmountKind: 'sub' | 'add' | null = null;

    const builder: any = {
      where: (col: any, op?: any, val?: any) => {
        if (typeof col === 'string') {
          if ((col === 'at_refer' || col === 'st_trref' || col === 'pt_trref') && op === 'like') {
            pattern = val.toString();
          } else if (col === 'at_acnt') {
            bankCodeFilter = op;
          } else if (col === 'at_pstdate' || col === 'st_trdate' || col === 'pt_trdate') {
            dateFilter = op;
          } else if (col === 'st_trtype' || col === 'pt_trtype') {
            typeFilter = op;
          }
        }
        return builder;
      },
      andWhere: (col: any, op?: any, val?: any) => builder.where(col, op, val),
      whereRaw: (sql: string, params: any[]) => {
        if (sql.includes('RTRIM(st_account)') || sql.includes('RTRIM(pt_account)')) {
          accountFilter = params?.[0] ?? null;
        }
        return builder;
      },
      andWhereRaw: (sql: string, params: any[]) => {
        if (sql.includes('ABS(at_value - ?)')) {
          rawAmountValue = params?.[0] ?? null;
          rawAmountKind = 'sub';
        } else if (sql.includes('ABS(st_trvalue + ?)')) {
          rawAmountValue = params?.[0] ?? null;
          rawAmountKind = 'add';
        } else if (sql.includes('ABS(pt_trvalue - ?)')) {
          rawAmountValue = params?.[0] ?? null;
          rawAmountKind = 'sub';
        }
        return builder;
      },
      select: () => builder,
      then: async (resolve: any) => {
        const matchPattern = (s: string) => {
          if (!pattern) return true;
          const prefix = pattern.replace(/%$/, '');
          return s.startsWith(prefix);
        };
        if (table === 'atran') {
          const filtered = state.atran.filter((r) => {
            if (pattern && !matchPattern(r.at_refer)) return false;
            if (bankCodeFilter && r.at_acnt !== bankCodeFilter) return false;
            if (dateFilter && r.at_pstdate !== dateFilter) return false;
            if (
              rawAmountValue !== null &&
              rawAmountKind === 'sub' &&
              Math.abs(r.at_value - rawAmountValue) >= 1
            )
              return false;
            return true;
          });
          return resolve(filtered);
        }
        if (table === 'stran') {
          const filtered = state.stran.filter((r) => {
            if (pattern && !matchPattern(r.st_trref)) return false;
            if (accountFilter && r.st_account.trim() !== accountFilter) return false;
            if (dateFilter && r.st_trdate !== dateFilter) return false;
            if (typeFilter && r.st_trtype !== typeFilter) return false;
            if (
              rawAmountValue !== null &&
              rawAmountKind === 'add' &&
              Math.abs(r.st_trvalue + rawAmountValue) >= 0.01
            )
              return false;
            return true;
          });
          return resolve(filtered);
        }
        if (table === 'ptran') {
          const filtered = state.ptran.filter((r) => {
            if (pattern && !matchPattern(r.pt_trref)) return false;
            if (accountFilter && r.pt_account.trim() !== accountFilter) return false;
            if (dateFilter && r.pt_trdate !== dateFilter) return false;
            if (typeFilter && r.pt_trtype !== typeFilter) return false;
            if (
              rawAmountValue !== null &&
              rawAmountKind === 'sub' &&
              Math.abs(r.pt_trvalue - rawAmountValue) >= 0.01
            )
              return false;
            return true;
          });
          return resolve(filtered);
        }
        return resolve([]);
      },
    };
    return builder;
  }

  const db: any = (table: string) => tableBuilder(table);
  return db;
}

// ---------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------

describe('generateImportFingerprint', () => {
  it('produces a stable BKIMP:HASH:DATE format', () => {
    const fp = generateImportFingerprint('Acme', 100, '2026-04-30');
    expect(fp).toMatch(/^BKIMP:[A-F0-9]{8}:\d{8}$/);
  });

  it('is deterministic for the same inputs', () => {
    const a = generateImportFingerprint('Acme', 100, '2026-04-30');
    const b = generateImportFingerprint('Acme', 100, '2026-04-30');
    // Same date components even if generated on the same import day
    expect(a.split(':').slice(0, 2).join(':')).toBe(
      b.split(':').slice(0, 2).join(':'),
    );
  });

  it('changes when the name changes', () => {
    const a = generateImportFingerprint('Acme', 100, '2026-04-30');
    const b = generateImportFingerprint('Beta', 100, '2026-04-30');
    expect(a.split(':')[1]).not.toBe(b.split(':')[1]);
  });
});

describe('extractHashFromFingerprint', () => {
  it('returns the hash portion', () => {
    const fp = 'BKIMP:A7F3B2C1:20260206';
    expect(extractHashFromFingerprint(fp)).toBe('A7F3B2C1');
  });
  it('returns null for non-BKIMP strings', () => {
    expect(extractHashFromFingerprint('something else')).toBeNull();
  });
  it('returns null for empty', () => {
    expect(extractHashFromFingerprint('')).toBeNull();
  });
});

// ---------------------------------------------------------------------
// findDuplicates — fingerprint
// ---------------------------------------------------------------------

describe('findDuplicates (fingerprint)', () => {
  it('detects already-imported transaction in atran', async () => {
    const fp = generateImportFingerprint('Acme', 100, '2026-04-30');
    const hash = fp.split(':')[1]!;
    const state: State = {
      atran: [
        {
          at_unique: 'A-1',
          at_pstdate: '2026-04-30',
          at_value: 10000,
          at_refer: `BKIMP:${hash}:20260430`,
          at_acnt: 'BC010',
        },
      ],
      stran: [],
      ptran: [],
    };
    const result = await findDuplicates(makeOperaDb(state), {
      name: 'Acme',
      amount: 100,
      date: '2026-04-30',
      bank_code: 'BC010',
    });
    expect(result.length).toBe(1);
    expect(result[0]?.match_type).toBe('fingerprint');
    expect(result[0]?.confidence).toBe(1);
    expect(result[0]?.table).toBe('atran');
  });

  it('skips fingerprint matches in a different bank account', async () => {
    const fp = generateImportFingerprint('Acme', 100, '2026-04-30');
    const hash = fp.split(':')[1]!;
    const state: State = {
      atran: [
        {
          at_unique: 'A-1',
          at_pstdate: '2026-04-30',
          at_value: 10000,
          at_refer: `BKIMP:${hash}:20260430`,
          at_acnt: 'BC020', // different bank
        },
      ],
      stran: [],
      ptran: [],
    };
    const result = await findDuplicates(makeOperaDb(state), {
      name: 'Acme',
      amount: 100,
      date: '2026-04-30',
      bank_code: 'BC010',
    });
    expect(result.length).toBe(0);
  });

  it('returns empty when no fingerprint or exact match', async () => {
    const result = await findDuplicates(
      makeOperaDb({ atran: [], stran: [], ptran: [] }),
      {
        name: 'Acme',
        amount: 100,
        date: '2026-04-30',
        bank_code: 'BC010',
      },
    );
    expect(result.length).toBe(0);
  });
});

// ---------------------------------------------------------------------
// findDuplicates — exact match
// ---------------------------------------------------------------------

describe('findDuplicates (exact)', () => {
  it('matches stran for a positive (receipt) amount with account', async () => {
    const state: State = {
      atran: [],
      stran: [
        {
          st_unique: 'S-1',
          st_trdate: '2026-04-30',
          st_trvalue: -100, // sales receipt stored negative
          st_trref: 'something',
          st_account: 'A001',
          st_trtype: 'R',
        },
      ],
      ptran: [],
    };
    const result = await findDuplicates(makeOperaDb(state), {
      name: 'Acme',
      amount: 100,
      date: '2026-04-30',
      account: 'A001',
    });
    expect(result.length).toBe(1);
    expect(result[0]?.match_type).toBe('exact');
    expect(result[0]?.table).toBe('stran');
    expect(result[0]?.confidence).toBe(0.9);
  });

  it('matches ptran for a negative (payment) amount with account', async () => {
    const state: State = {
      atran: [],
      stran: [],
      ptran: [
        {
          pt_unique: 'P-1',
          pt_trdate: '2026-04-30',
          pt_trvalue: -100,
          pt_trref: 'something',
          pt_account: 'B001',
          pt_trtype: 'P',
        },
      ],
    };
    const result = await findDuplicates(makeOperaDb(state), {
      name: 'Energy Co',
      amount: -100,
      date: '2026-04-30',
      account: 'B001',
    });
    expect(result.length).toBe(1);
    expect(result[0]?.match_type).toBe('exact');
    expect(result[0]?.table).toBe('ptran');
  });

  it('opposite-sign transactions are NOT exact matches', async () => {
    const state: State = {
      atran: [],
      stran: [
        {
          st_unique: 'S-1',
          st_trdate: '2026-04-30',
          st_trvalue: -100,
          st_trref: 'something',
          st_account: 'A001',
          st_trtype: 'R',
        },
      ],
      ptran: [],
    };
    const result = await findDuplicates(makeOperaDb(state), {
      name: 'Acme',
      amount: -100, // payment, but stran is a receipt
      date: '2026-04-30',
      account: 'A001',
    });
    // Negative amount routes to ptran exact, not stran. ptran is empty so no match.
    expect(result.filter((r) => r.match_type === 'exact').length).toBe(0);
  });

  it('skips exact match when fingerprint already matched', async () => {
    const fp = generateImportFingerprint('Acme', 100, '2026-04-30');
    const hash = fp.split(':')[1]!;
    const state: State = {
      atran: [
        {
          at_unique: 'A-1',
          at_pstdate: '2026-04-30',
          at_value: 10000,
          at_refer: `BKIMP:${hash}:20260430`,
          at_acnt: 'BC010',
        },
      ],
      stran: [
        {
          st_unique: 'S-1',
          st_trdate: '2026-04-30',
          st_trvalue: -100,
          st_trref: 'something',
          st_account: 'A001',
          st_trtype: 'R',
        },
      ],
      ptran: [],
    };
    const result = await findDuplicates(makeOperaDb(state), {
      name: 'Acme',
      amount: 100,
      date: '2026-04-30',
      bank_code: 'BC010',
      account: 'A001',
    });
    // Only fingerprint, not exact (skipped because fingerprint matched)
    expect(result.every((r) => r.match_type === 'fingerprint')).toBe(true);
  });
});

// ---------------------------------------------------------------------
// checkBatch
// ---------------------------------------------------------------------

describe('checkBatch', () => {
  it('flags only transactions with candidates', async () => {
    const fp1 = generateImportFingerprint('Acme', 100, '2026-04-30');
    const hash = fp1.split(':')[1]!;
    const state: State = {
      atran: [
        {
          at_unique: 'A-1',
          at_pstdate: '2026-04-30',
          at_value: 10000,
          at_refer: `BKIMP:${hash}:20260430`,
          at_acnt: 'BC010',
        },
      ],
      stran: [],
      ptran: [],
    };
    const result = await checkBatch(
      makeOperaDb(state),
      [
        { name: 'Acme', amount: 100, date: '2026-04-30' },
        { name: 'Beta', amount: 50, date: '2026-04-30' },
      ],
      'BC010',
    );
    expect(result.success).toBe(true);
    expect(result.duplicates_found).toBe(1);
    expect(result.results['0']?.length).toBe(1);
    expect(result.results['0']?.[0]?.confidence).toBe(100);
    expect(result.results['1']).toBeUndefined();
  });
});
