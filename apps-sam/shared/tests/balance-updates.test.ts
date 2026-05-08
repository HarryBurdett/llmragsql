import { describe, it, expect } from 'vitest';
import {
  updateNbankBalance,
  updateNacntBalance,
  getNacntType,
} from '../src/opera/balance-updates.js';

interface MockState {
  nbankRows: Record<string, { nk_curbal: number }>;
  nacntRows: Record<
    string,
    {
      na_type: string;
      na_subt: string;
      na_ptddr: number;
      na_ytddr: number;
      na_ptdcr: number;
      na_ytdcr: number;
      periodBals: Record<string, number>;
    }
  >;
  nhistRows: Array<{
    id: number;
    nh_nacnt: string;
    nh_ntype: string;
    nh_nsubt: string;
    nh_ncntr: string;
    nh_year: number;
    nh_period: number;
    nh_bal: number;
    nh_ptddr: number;
    nh_ptdcr: number;
  }>;
  nextNhistId: number;
  // nextid table emulation
  nextidValues: Record<string, number>;
  nsubtRows: Record<string, { balance: number }>; // key: type|subt
  ntypeRows: Record<string, { balance: number }>; // key: type
  capturedSql: string[];
  capturedParams: unknown[][];
}

function makeMockTrx(state: MockState): any {
  return {
    raw: (sql: string, params?: unknown[]) => {
      state.capturedSql.push(sql);
      state.capturedParams.push(params ?? []);

      // UPDATE nbank
      if (sql.includes('UPDATE nbank') && sql.includes('nk_curbal')) {
        const delta = Number((params ?? [])[0]);
        const acnt = String((params ?? [])[1]).trim();
        const row = state.nbankRows[acnt];
        if (!row) return Promise.resolve({ rowCount: 0 });
        row.nk_curbal += delta;
        return Promise.resolve({ rowCount: 1 });
      }
      // SELECT nacnt na_type/na_subt
      if (sql.includes('SELECT na_type, na_subt FROM nacnt')) {
        const acnt = String((params ?? [])[0]).trim();
        const row = state.nacntRows[acnt];
        return Promise.resolve(
          row ? [{ na_type: row.na_type, na_subt: row.na_subt }] : [],
        );
      }
      // UPDATE nacnt
      if (sql.includes('UPDATE nacnt')) {
        const acnt = String((params ?? [])[params!.length - 1]).trim();
        const row = state.nacntRows[acnt];
        if (!row) return Promise.resolve({ rowCount: 0 });
        const periodColMatch = /na_balc(\d{2})/.exec(sql);
        const periodCol = periodColMatch?.[0] ?? '';
        if (sql.includes('na_ptddr = ISNULL(na_ptddr')) {
          const v = Number((params ?? [])[0]);
          row.na_ptddr += v;
          row.na_ytddr += v;
          row.periodBals[periodCol] =
            (row.periodBals[periodCol] ?? 0) + v;
        } else {
          const absV = Number((params ?? [])[0]);
          const v = Number((params ?? [])[2]);
          row.na_ptdcr += absV;
          row.na_ytdcr += absV;
          row.periodBals[periodCol] =
            (row.periodBals[periodCol] ?? 0) + v;
        }
        return Promise.resolve({ rowCount: 1 });
      }
      // SELECT TOP 1 id FROM nhist (find existing)
      if (sql.includes('FROM nhist WITH (UPDLOCK')) {
        const acnt = String((params ?? [])[0]).trim();
        const ntype = String((params ?? [])[1]);
        const nsubt = String((params ?? [])[2]);
        const ncntr = String((params ?? [])[3]);
        const year = Number((params ?? [])[4]);
        const period = Number((params ?? [])[5]);
        const found = state.nhistRows.find(
          (r) =>
            r.nh_nacnt.trim() === acnt &&
            r.nh_ntype === ntype &&
            r.nh_nsubt === nsubt &&
            r.nh_ncntr === ncntr &&
            r.nh_year === year &&
            r.nh_period === period,
        );
        return Promise.resolve(found ? [{ id: found.id }] : []);
      }
      // UPDATE nhist
      if (sql.includes('UPDATE nhist') && sql.includes('id = ?')) {
        const id = Number((params ?? [])[2]);
        const row = state.nhistRows.find((r) => r.id === id);
        if (!row) return Promise.resolve({ rowCount: 0 });
        const v = Number((params ?? [])[0]);
        row.nh_bal += v;
        if (sql.includes('nh_ptddr')) {
          row.nh_ptddr += v;
        } else {
          row.nh_ptdcr += v; // stored negative
        }
        return Promise.resolve({ rowCount: 1 });
      }
      // INSERT nhist
      if (sql.includes('INSERT INTO nhist')) {
        const id = Number((params ?? [])[0]);
        state.nhistRows.push({
          id,
          nh_ntype: String((params ?? [])[1]),
          nh_nsubt: String((params ?? [])[2]),
          nh_nacnt: String((params ?? [])[3]),
          nh_ncntr: String((params ?? [])[4]),
          nh_year: Number((params ?? [])[5]),
          nh_period: Number((params ?? [])[6]),
          nh_bal: Number((params ?? [])[7]),
          nh_ptddr: Number((params ?? [])[8]),
          nh_ptdcr: Number((params ?? [])[9]),
        });
        return Promise.resolve({ rowCount: 1 });
      }
      // SELECT nextid
      if (sql.includes('FROM nextid') && sql.includes('UPDLOCK')) {
        const tn = String((params ?? [])[0]);
        const v = state.nextidValues[tn];
        return Promise.resolve(v != null ? [{ nextid: v }] : []);
      }
      if (sql.includes('UPDATE nextid')) {
        const newVal = Number((params ?? [])[0]);
        const tn = String((params ?? [])[1]);
        state.nextidValues[tn] = newVal;
        return Promise.resolve({ rowCount: 1 });
      }
      // UPDATE nsubt
      if (sql.includes('UPDATE nsubt')) {
        const v = Number((params ?? [])[0]);
        const subt = String((params ?? [])[1]);
        const type = String((params ?? [])[2]);
        const key = `${type}|${subt}`;
        if (!state.nsubtRows[key]) state.nsubtRows[key] = { balance: 0 };
        state.nsubtRows[key].balance += v;
        return Promise.resolve({ rowCount: 1 });
      }
      // UPDATE ntype
      if (sql.includes('UPDATE ntype')) {
        const v = Number((params ?? [])[0]);
        const type = String((params ?? [])[1]);
        if (!state.ntypeRows[type]) state.ntypeRows[type] = { balance: 0 };
        state.ntypeRows[type].balance += v;
        return Promise.resolve({ rowCount: 1 });
      }
      return Promise.resolve([]);
    },
  };
}

function emptyState(): MockState {
  return {
    nbankRows: {},
    nacntRows: {},
    nhistRows: [],
    nextNhistId: 1,
    nextidValues: { nhist: 1000 },
    nsubtRows: {},
    ntypeRows: {},
    capturedSql: [],
    capturedParams: [],
  };
}

describe('updateNbankBalance', () => {
  it('adds amount in pence to nk_curbal (receipt)', async () => {
    const state = emptyState();
    state.nbankRows.BC010 = { nk_curbal: 50000 };
    await updateNbankBalance(makeMockTrx(state), 'BC010', 12.5);
    expect(state.nbankRows.BC010!.nk_curbal).toBe(51250); // +1250 pence
  });

  it('subtracts on negative amount (payment)', async () => {
    const state = emptyState();
    state.nbankRows.BC010 = { nk_curbal: 50000 };
    await updateNbankBalance(makeMockTrx(state), 'BC010', -10);
    expect(state.nbankRows.BC010!.nk_curbal).toBe(49000);
  });

  it('throws when bank account not found', async () => {
    const state = emptyState();
    await expect(
      updateNbankBalance(makeMockTrx(state), 'UNKNOWN', 100),
    ).rejects.toThrow(/not found in nbank/);
  });

  it('uses ROWLOCK on the UPDATE', async () => {
    const state = emptyState();
    state.nbankRows.BC010 = { nk_curbal: 0 };
    await updateNbankBalance(makeMockTrx(state), 'BC010', 1);
    expect(state.capturedSql[0]).toMatch(/UPDATE nbank WITH \(ROWLOCK\)/);
  });
});

describe('getNacntType', () => {
  it('returns na_type/na_subt and caches', async () => {
    const state = emptyState();
    state.nacntRows.BC010 = {
      na_type: 'A',
      na_subt: 'CB',
      na_ptddr: 0,
      na_ytddr: 0,
      na_ptdcr: 0,
      na_ytdcr: 0,
      periodBals: {},
    };
    const trx = makeMockTrx(state);
    const t1 = await getNacntType(trx, 'BC010');
    const t2 = await getNacntType(trx, 'BC010');
    expect(t1).toEqual({ na_type: 'A', na_subt: 'CB' });
    expect(t2).toEqual({ na_type: 'A', na_subt: 'CB' });
    // Only one DB hit thanks to per-trx cache
    const dbHits = state.capturedSql.filter((s) =>
      s.includes('SELECT na_type, na_subt FROM nacnt'),
    ).length;
    expect(dbHits).toBe(1);
  });

  it('returns null when account not in nacnt', async () => {
    const t = await getNacntType(makeMockTrx(emptyState()), 'NOPE');
    expect(t).toBeNull();
  });
});

describe('updateNacntBalance', () => {
  function seed(state: MockState) {
    state.nacntRows.BC010 = {
      na_type: 'A',
      na_subt: 'CB',
      na_ptddr: 0,
      na_ytddr: 0,
      na_ptdcr: 0,
      na_ytdcr: 0,
      periodBals: {},
    };
  }

  it('debit (+ve): updates ptddr, ytddr, period balance, nhist, nsubt, ntype', async () => {
    const state = emptyState();
    seed(state);
    await updateNacntBalance(makeMockTrx(state), 'BC010', 100, {
      period: 4,
      year: 2026,
    });
    const row = state.nacntRows.BC010!;
    expect(row.na_ptddr).toBe(100);
    expect(row.na_ytddr).toBe(100);
    expect(row.na_ptdcr).toBe(0);
    expect(row.periodBals.na_balc04).toBe(100);
    expect(state.nhistRows).toHaveLength(1);
    expect(state.nhistRows[0]?.nh_bal).toBe(100);
    expect(state.nhistRows[0]?.nh_ptddr).toBe(100);
    expect(state.nhistRows[0]?.nh_ptdcr).toBe(0);
    expect(state.nsubtRows['A|CB']?.balance).toBe(100);
    expect(state.ntypeRows.A?.balance).toBe(100);
  });

  it('credit (-ve): updates ptdcr, ytdcr (positive magnitude), nhist nh_ptdcr stays negative', async () => {
    const state = emptyState();
    seed(state);
    await updateNacntBalance(makeMockTrx(state), 'BC010', -250, {
      period: 5,
      year: 2026,
    });
    const row = state.nacntRows.BC010!;
    expect(row.na_ptdcr).toBe(250); // positive magnitude
    expect(row.na_ytdcr).toBe(250);
    expect(row.na_ptddr).toBe(0);
    expect(row.periodBals.na_balc05).toBe(-250); // signed net
    expect(state.nhistRows[0]?.nh_bal).toBe(-250);
    expect(state.nhistRows[0]?.nh_ptddr).toBe(0);
    expect(state.nhistRows[0]?.nh_ptdcr).toBe(-250); // Opera's negative convention
    expect(state.nsubtRows['A|CB']?.balance).toBe(-250);
    expect(state.ntypeRows.A?.balance).toBe(-250);
  });

  it('updates existing nhist row in place (no new INSERT)', async () => {
    const state = emptyState();
    seed(state);
    state.nhistRows.push({
      id: 50,
      nh_nacnt: 'BC010   ',
      nh_ntype: 'A',
      nh_nsubt: 'CB',
      nh_ncntr: '    ',
      nh_year: 2026,
      nh_period: 4,
      nh_bal: 1000,
      nh_ptddr: 1000,
      nh_ptdcr: 0,
    });
    await updateNacntBalance(makeMockTrx(state), 'BC010', 100, {
      period: 4,
      year: 2026,
    });
    expect(state.nhistRows).toHaveLength(1); // no new INSERT
    expect(state.nhistRows[0]?.nh_bal).toBe(1100);
    expect(state.nhistRows[0]?.nh_ptddr).toBe(1100);
  });

  it('throws when nacnt account not found', async () => {
    const state = emptyState();
    await expect(
      updateNacntBalance(makeMockTrx(state), 'UNKNOWN', 100, {
        period: 1,
        year: 2026,
      }),
    ).rejects.toThrow(/not exist in nacnt/);
  });

  it('skips silently for invalid period (matches Python warning+return)', async () => {
    const state = emptyState();
    seed(state);
    await updateNacntBalance(makeMockTrx(state), 'BC010', 100, {
      period: 99,
      year: 2026,
    });
    // No DB activity at all
    expect(state.capturedSql).toHaveLength(0);
  });

  it('uses zero-padded period column (na_balc04 not na_balc4)', async () => {
    const state = emptyState();
    seed(state);
    await updateNacntBalance(makeMockTrx(state), 'BC010', 1, {
      period: 4,
      year: 2026,
    });
    const nacntSql = state.capturedSql.find((s) => s.includes('UPDATE nacnt'));
    expect(nacntSql).toMatch(/na_balc04/);
    expect(nacntSql).not.toMatch(/na_balc4(?!\d)/);
  });
});
