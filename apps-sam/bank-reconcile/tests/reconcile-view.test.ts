import { describe, it, expect } from 'vitest';
import { getReconcileBankView } from '../src/services/reconcile-view.js';

interface MockState {
  /** RTRIM(nk_acnt) → bank row */
  nbank: Record<
    string,
    {
      nk_desc: string;
      nk_sort: string;
      nk_number: string;
      /** in pence */
      nk_curbal: number;
    }
  >;
  /** Per-bank atran current-year movements */
  atranCurrent: Record<
    string,
    {
      entry_count: number;
      txn_count: number;
      receipts_pence: number;
      payments_pence: number;
      net_pence: number;
    }
  >;
  /** Per-bank atran all-time totals */
  atranAll: Record<string, { entry_count: number; net_pence: number }>;
  /** nacnt for the bank account */
  nacnt: Record<
    string,
    { description: string; na_prydr: number; na_prycr: number }
  >;
  /** ntran current-year totals */
  ntranCurrent: Record<
    string,
    { debits: number; credits: number; net: number }
  >;
  /** anoml posted/pending counts */
  anomlSummary: Record<
    string,
    Array<{ status: 'Posted' | 'Pending'; count: number; total: number }>
  >;
  /** anoml pending detail */
  anomlPending: Record<
    string,
    Array<{
      nominal_account: string;
      source: string;
      date: string;
      value: number;
      reference: string;
      comment: string;
    }>
  >;
  currentYear: number;
}

function makeOperaDb(state: MockState): any {
  return {
    raw: (sql: string, params?: unknown[]) => {
      // current_year discriminator
      if (
        sql.includes('MAX(nt_year) AS current_year FROM ntran')
      ) {
        return Promise.resolve([{ current_year: state.currentYear }]);
      }
      // nbank lookup
      if (sql.includes('FROM nbank') && sql.includes('balance_pence')) {
        const code = String((params ?? [])[0] ?? '');
        const b = state.nbank[code];
        if (!b) return Promise.resolve([]);
        return Promise.resolve([
          {
            code,
            description: b.nk_desc,
            sort_code: b.nk_sort,
            account_number: b.nk_number,
            balance_pence: b.nk_curbal,
          },
        ]);
      }
      // atran current-year
      if (
        sql.includes('FROM atran') &&
        sql.includes('YEAR(at_pstdate) = ?')
      ) {
        const code = String((params ?? [])[0] ?? '');
        const c = state.atranCurrent[code];
        return Promise.resolve(c ? [c] : [{}]);
      }
      // atran all-time
      if (
        sql.includes('FROM atran') &&
        sql.includes('SUM(at_value) AS net_pence')
      ) {
        const code = String((params ?? [])[0] ?? '');
        const a = state.atranAll[code];
        return Promise.resolve(a ? [a] : [{}]);
      }
      // nacnt
      if (sql.includes('FROM nacnt')) {
        const code = String((params ?? [])[0] ?? '');
        const n = state.nacnt[code];
        if (!n) return Promise.resolve([]);
        return Promise.resolve([
          {
            description: n.description,
            na_prydr: n.na_prydr,
            na_prycr: n.na_prycr,
          },
        ]);
      }
      // ntran current-year
      if (
        sql.includes('FROM ntran') &&
        sql.includes('SUM(nt_value)')
      ) {
        const code = String((params ?? [])[0] ?? '');
        const t = state.ntranCurrent[code];
        return Promise.resolve(t ? [t] : [{}]);
      }
      // anoml summary
      if (sql.includes('FROM anoml') && sql.includes('GROUP BY')) {
        const code = String((params ?? [])[0] ?? '');
        const s = state.anomlSummary[code] ?? [];
        return Promise.resolve(
          s.map((r) => ({ status: r.status, cnt: r.count, total: r.total })),
        );
      }
      // anoml pending detail
      if (sql.includes('FROM anoml') && sql.includes('ORDER BY ax_date')) {
        const code = String((params ?? [])[0] ?? '');
        return Promise.resolve(state.anomlPending[code] ?? []);
      }
      return Promise.resolve([]);
    },
  };
}

describe('getReconcileBankView', () => {
  it('rejects bad bank code', async () => {
    const db = makeOperaDb({
      nbank: {},
      atranCurrent: {},
      atranAll: {},
      nacnt: {},
      ntranCurrent: {},
      anomlSummary: {},
      anomlPending: {},
      currentYear: 2026,
    });
    const r = await getReconcileBankView(db, "BC';--");
    expect(r.success).toBe(false);
  });

  it('returns 404-like error when bank not in nbank', async () => {
    const db = makeOperaDb({
      nbank: {},
      atranCurrent: {},
      atranAll: {},
      nacnt: {},
      ntranCurrent: {},
      anomlSummary: {},
      anomlPending: {},
      currentYear: 2026,
    });
    const r = await getReconcileBankView(db, 'BC010');
    expect(r.success).toBe(false);
    if (!r.success) expect(r.error).toMatch(/not found/);
  });

  it('reports RECONCILED when all three sources agree', async () => {
    const db = makeOperaDb({
      nbank: {
        BC010: {
          nk_desc: 'Main',
          nk_sort: '00-00-00',
          nk_number: '12345678',
          nk_curbal: 100000, // £1000
        },
      },
      atranCurrent: {
        BC010: {
          entry_count: 5,
          txn_count: 5,
          receipts_pence: 200000,
          payments_pence: 100000,
          net_pence: 100000, // £1000 movements
        },
      },
      atranAll: {
        BC010: { entry_count: 10, net_pence: 100000 },
      },
      nacnt: {
        BC010: { description: 'Main Account', na_prydr: 0, na_prycr: 0 }, // bf = 0
      },
      ntranCurrent: {
        BC010: { debits: 2000, credits: 1000, net: 1000 },
      },
      anomlSummary: {
        BC010: [],
      },
      anomlPending: { BC010: [] },
      currentYear: 2026,
    });
    const r = await getReconcileBankView(db, 'BC010');
    expect(r.success).toBe(true);
    if (r.success) {
      expect(r.status).toBe('RECONCILED');
      expect(r.cashbook.expected_closing).toBe(1000);
      expect(r.bank_master.balance_pounds).toBe(1000);
      expect(r.nominal_ledger.total_balance).toBe(1000);
    }
  });

  it('reports UNRECONCILED with detail when sources disagree', async () => {
    const db = makeOperaDb({
      nbank: {
        BC010: {
          nk_desc: 'Main',
          nk_sort: '00-00-00',
          nk_number: '12345678',
          nk_curbal: 100000,
        },
      },
      atranCurrent: {
        BC010: {
          entry_count: 5,
          txn_count: 5,
          receipts_pence: 200000,
          payments_pence: 100000,
          net_pence: 110000, // £1100 movements (£100 over)
        },
      },
      atranAll: {
        BC010: { entry_count: 10, net_pence: 110000 },
      },
      nacnt: {
        BC010: { description: 'Main', na_prydr: 0, na_prycr: 0 },
      },
      ntranCurrent: { BC010: { debits: 2000, credits: 1000, net: 1000 } },
      anomlSummary: { BC010: [] },
      anomlPending: { BC010: [] },
      currentYear: 2026,
    });
    const r = await getReconcileBankView(db, 'BC010');
    expect(r.success).toBe(true);
    if (r.success) {
      expect(r.status).toBe('UNRECONCILED');
      expect(r.message).toMatch(/MORE than/);
    }
  });

  it('lists pending anoml transactions with source descriptions', async () => {
    const db = makeOperaDb({
      nbank: {
        BC010: {
          nk_desc: 'Main',
          nk_sort: '',
          nk_number: '',
          nk_curbal: 0,
        },
      },
      atranCurrent: {
        BC010: {
          entry_count: 0,
          txn_count: 0,
          receipts_pence: 0,
          payments_pence: 0,
          net_pence: 0,
        },
      },
      atranAll: { BC010: { entry_count: 0, net_pence: 0 } },
      nacnt: { BC010: { description: 'X', na_prydr: 0, na_prycr: 0 } },
      ntranCurrent: { BC010: { debits: 0, credits: 0, net: 0 } },
      anomlSummary: {
        BC010: [{ status: 'Pending', count: 2, total: 50 }],
      },
      anomlPending: {
        BC010: [
          {
            nominal_account: 'BC010',
            source: 'S',
            date: '2026-04-15',
            value: 30,
            reference: 'TEST',
            comment: 'COM',
          },
          {
            nominal_account: 'BC010',
            source: 'P',
            date: '2026-04-10',
            value: 20,
            reference: 'TEST2',
            comment: '',
          },
        ],
      },
      currentYear: 2026,
    });
    const r = await getReconcileBankView(db, 'BC010');
    expect(r.success).toBe(true);
    if (r.success) {
      const xfer = r.cashbook.transfer_file.pending_transfer;
      expect(xfer.count).toBe(2);
      expect(xfer.transactions).toHaveLength(2);
      expect(xfer.transactions[0]?.source_desc).toBe('Sales');
      expect(xfer.transactions[1]?.source_desc).toBe('Purchase');
    }
  });
});
