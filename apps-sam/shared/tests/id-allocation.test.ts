import { describe, it, expect } from 'vitest';
import {
  getNextJournal,
  getNextId,
  incrementAtypeEntry,
} from '../src/opera/id-allocation.js';

interface MockState {
  npNexjrnl?: number | null;
  // nextid rows: tablename → next value
  nextidRows: Record<string, number>;
  // atype rows: cbtype → ay_entry
  atypeRows: Record<string, string | null>;
  // aentry rows: { cbtype, entry } pairs that already exist
  aentryRows: Array<{ cbtype: string; entry: string }>;
  capturedSql: string[];
  capturedParams: unknown[][];
}

function makeMockTrx(state: MockState): any {
  return {
    raw: (sql: string, params?: unknown[]) => {
      state.capturedSql.push(sql);
      state.capturedParams.push(params ?? []);

      // SELECT np_nexjrnl
      if (sql.includes('np_nexjrnl FROM nparm')) {
        return Promise.resolve(
          state.npNexjrnl != null ? [{ np_nexjrnl: state.npNexjrnl }] : [],
        );
      }
      // UPDATE nparm
      if (sql.includes('UPDATE nparm') && sql.includes('np_nexjrnl')) {
        state.npNexjrnl = Number((params ?? [])[0]);
        return Promise.resolve({ rowCount: 1 });
      }
      // SELECT nextid
      if (sql.includes('FROM nextid') && sql.includes('UPDLOCK')) {
        const tablename = String((params ?? [])[0]);
        const v = state.nextidRows[tablename];
        return Promise.resolve(v != null ? [{ nextid: v }] : []);
      }
      // UPDATE nextid
      if (sql.includes('UPDATE nextid')) {
        const newVal = Number((params ?? [])[0]);
        const tablename = String((params ?? [])[1]);
        state.nextidRows[tablename] = newVal;
        return Promise.resolve({ rowCount: 1 });
      }
      // SELECT ay_entry FROM atype
      if (sql.includes('SELECT ay_entry FROM atype')) {
        const cbtype = String((params ?? [])[0]);
        const v = state.atypeRows[cbtype];
        if (v === undefined) return Promise.resolve([]);
        return Promise.resolve([{ ay_entry: v }]);
      }
      // SELECT 1 FROM aentry — check existence
      if (sql.includes('FROM aentry')) {
        const cbtype = String((params ?? [])[0]);
        const entry = String((params ?? [])[1]);
        const exists = state.aentryRows.some(
          (r) => r.cbtype === cbtype && r.entry === entry,
        );
        return Promise.resolve(exists ? [{ x: 1 }] : []);
      }
      // UPDATE atype
      if (sql.includes('UPDATE atype')) {
        const newEntry = String((params ?? [])[0]);
        const cbtype = String((params ?? [])[1]);
        state.atypeRows[cbtype] = newEntry;
        return Promise.resolve({ rowCount: 1 });
      }
      return Promise.resolve([]);
    },
  };
}

describe('getNextJournal', () => {
  it('returns current value and advances by count', async () => {
    const state: MockState = {
      npNexjrnl: 100,
      nextidRows: {},
      atypeRows: {},
      aentryRows: [],
      capturedSql: [],
      capturedParams: [],
    };
    const trx = makeMockTrx(state);
    const j = await getNextJournal(trx);
    expect(j).toBe(100);
    expect(state.npNexjrnl).toBe(101);
  });

  it('allocates a range of journal numbers', async () => {
    const state: MockState = {
      npNexjrnl: 100,
      nextidRows: {},
      atypeRows: {},
      aentryRows: [],
      capturedSql: [],
      capturedParams: [],
    };
    const j = await getNextJournal(makeMockTrx(state), 5);
    expect(j).toBe(100);
    expect(state.npNexjrnl).toBe(105);
  });

  it('defaults to 1 when nparm row missing', async () => {
    const state: MockState = {
      npNexjrnl: null,
      nextidRows: {},
      atypeRows: {},
      aentryRows: [],
      capturedSql: [],
      capturedParams: [],
    };
    const j = await getNextJournal(makeMockTrx(state));
    expect(j).toBe(1);
    expect(state.npNexjrnl).toBe(2);
  });

  it('uses UPDLOCK + ROWLOCK on read', async () => {
    const state: MockState = {
      npNexjrnl: 1,
      nextidRows: {},
      atypeRows: {},
      aentryRows: [],
      capturedSql: [],
      capturedParams: [],
    };
    await getNextJournal(makeMockTrx(state));
    expect(state.capturedSql[0]).toMatch(/WITH \(UPDLOCK, ROWLOCK\)/);
  });
});

describe('getNextId', () => {
  it('returns and advances per tablename', async () => {
    const state: MockState = {
      npNexjrnl: 0,
      nextidRows: { stran: 5000, ptran: 6000, ntran: 7000 },
      atypeRows: {},
      aentryRows: [],
      capturedSql: [],
      capturedParams: [],
    };
    const trx = makeMockTrx(state);
    const stranId = await getNextId(trx, 'stran');
    const ptranId = await getNextId(trx, 'ptran', 3);
    expect(stranId).toBe(5000);
    expect(ptranId).toBe(6000);
    expect(state.nextidRows.stran).toBe(5001);
    expect(state.nextidRows.ptran).toBe(6003);
  });

  it('throws when no row found for table', async () => {
    const state: MockState = {
      npNexjrnl: 0,
      nextidRows: {},
      atypeRows: {},
      aentryRows: [],
      capturedSql: [],
      capturedParams: [],
    };
    await expect(getNextId(makeMockTrx(state), 'unknown')).rejects.toThrow(
      /No nextid row found/,
    );
  });
});

describe('incrementAtypeEntry', () => {
  it('returns the current entry and advances ay_entry by 1', async () => {
    const state: MockState = {
      npNexjrnl: 0,
      nextidRows: {},
      atypeRows: { P1: 'P100008024' },
      aentryRows: [],
      capturedSql: [],
      capturedParams: [],
    };
    const e = await incrementAtypeEntry(makeMockTrx(state), 'P1');
    expect(e).toBe('P100008024');
    expect(state.atypeRows.P1).toBe('P100008025');
  });

  it('walks forward when entry already exists in aentry', async () => {
    const state: MockState = {
      npNexjrnl: 0,
      nextidRows: {},
      atypeRows: { P1: 'P100008024' },
      // First two are already taken — should walk to ...026
      aentryRows: [
        { cbtype: 'P1', entry: 'P100008024' },
        { cbtype: 'P1', entry: 'P100008025' },
      ],
      capturedSql: [],
      capturedParams: [],
    };
    const e = await incrementAtypeEntry(makeMockTrx(state), 'P1');
    expect(e).toBe('P100008026');
    // ay_entry advanced to one past what we used
    expect(state.atypeRows.P1).toBe('P100008027');
  });

  it('throws when cbtype not in atype', async () => {
    const state: MockState = {
      npNexjrnl: 0,
      nextidRows: {},
      atypeRows: {},
      aentryRows: [],
      capturedSql: [],
      capturedParams: [],
    };
    await expect(
      incrementAtypeEntry(makeMockTrx(state), 'NOPE'),
    ).rejects.toThrow(/not found in atype/);
  });

  it('throws after 100 sequential collisions', async () => {
    const state: MockState = {
      npNexjrnl: 0,
      nextidRows: {},
      atypeRows: { P1: 'P100008024' },
      // Block 200 entries forward — guarantees the 100-attempt cap fires.
      aentryRows: Array.from({ length: 200 }, (_, i) => ({
        cbtype: 'P1',
        entry: `P1${(8024 + i).toString().padStart(8, '0')}`,
      })),
      capturedSql: [],
      capturedParams: [],
    };
    await expect(
      incrementAtypeEntry(makeMockTrx(state), 'P1'),
    ).rejects.toThrow(/100 attempts/);
  });

  it('falls back to {cbtype}00000000 when ay_entry is null/empty', async () => {
    const state: MockState = {
      npNexjrnl: 0,
      nextidRows: {},
      atypeRows: { GC: null },
      aentryRows: [],
      capturedSql: [],
      capturedParams: [],
    };
    const e = await incrementAtypeEntry(makeMockTrx(state), 'GC');
    expect(e).toBe('GC00000000');
    expect(state.atypeRows.GC).toBe('GC00000001');
  });
});
