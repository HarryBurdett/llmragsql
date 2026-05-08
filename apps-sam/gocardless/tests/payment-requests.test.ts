import { describe, it, expect } from 'vitest';
import { listPaymentRequests } from '../src/services/payment-requests.js';

interface RequestRow {
  id: number;
  payment_id: string;
  mandate_id: string;
  opera_account: string;
  amount: number;
  amount_pence: number | null;
  currency: string;
  status: string;
  reference: string;
  charge_date: string;
  payout_id: string;
  invoice_refs: string;
  opera_receipt_ref: string;
  error_message: string;
  created_at: string;
  updated_at: string;
}

interface MandateRow {
  opera_account: string;
  opera_name: string;
}

interface MockState {
  requests: RequestRow[];
  mandates: MandateRow[];
}

function makeAppDb(state: MockState): any {
  const db: any = (table: string) => {
    let conds: Record<string, unknown> = {};
    let inCol: string | null = null;
    let inVals: unknown[] | null = null;
    let limitN = Infinity;
    let order: { col: string; dir: 'asc' | 'desc' } | null = null;
    let selectedCols: string[] | null = null;
    if (table === 'gocardless_payment_requests') {
      const builder: any = {
        where: (cond: Record<string, unknown>) => {
          Object.assign(conds, cond);
          return builder;
        },
        orderBy: (col: string, dir: 'asc' | 'desc' = 'asc') => {
          order = { col, dir };
          return builder;
        },
        limit: (n: number) => {
          limitN = n;
          return builder;
        },
        then: (cb: (rows: RequestRow[]) => unknown) => {
          let rows = state.requests.filter((r) =>
            Object.entries(conds).every(([k, v]) => (r as any)[k] === v),
          );
          if (order) {
            rows = [...rows].sort((a, b) => {
              const cmp = String((a as any)[order!.col]).localeCompare(
                String((b as any)[order!.col]),
              );
              return order!.dir === 'desc' ? -cmp : cmp;
            });
          }
          return Promise.resolve(cb(rows.slice(0, limitN)));
        },
      };
      return builder;
    }
    if (table === 'gocardless_mandates') {
      const builder: any = {
        whereIn: (col: string, vals: unknown[]) => {
          inCol = col;
          inVals = vals;
          return builder;
        },
        select: (...cols: string[]) => {
          selectedCols = cols;
          const rows = state.mandates.filter(
            (m) =>
              !inCol || (inVals && inVals.includes((m as any)[inCol!])),
          );
          return Promise.resolve(rows);
        },
      };
      return builder;
    }
    throw new Error(`Unexpected table: ${table}`);
  };
  db.fn = { now: () => new Date() };
  return db;
}

function emptyRequest(over: Partial<RequestRow> = {}): RequestRow {
  return {
    id: 1,
    payment_id: 'PR_X',
    mandate_id: 'MD_X',
    opera_account: 'CUST01',
    amount: 100,
    amount_pence: 10000,
    currency: 'GBP',
    status: 'pending',
    reference: '',
    charge_date: '2026-04-15',
    payout_id: '',
    invoice_refs: '',
    opera_receipt_ref: '',
    error_message: '',
    created_at: '2026-04-15T10:00:00Z',
    updated_at: '2026-04-15T10:00:00Z',
    ...over,
  };
}

describe('listPaymentRequests', () => {
  it('returns requests in created_at desc order', async () => {
    const state: MockState = {
      requests: [
        emptyRequest({ id: 1, created_at: '2026-04-10T10:00:00Z' }),
        emptyRequest({ id: 2, created_at: '2026-04-15T10:00:00Z' }),
        emptyRequest({ id: 3, created_at: '2026-04-12T10:00:00Z' }),
      ],
      mandates: [],
    };
    const result = await listPaymentRequests(makeAppDb(state));
    expect(result.success).toBe(true);
    expect(result.count).toBe(3);
    expect(result.requests[0]?.id).toBe(2);
  });

  it('filters by status', async () => {
    const state: MockState = {
      requests: [
        emptyRequest({ id: 1, status: 'pending' }),
        emptyRequest({ id: 2, status: 'paid_out' }),
      ],
      mandates: [],
    };
    const result = await listPaymentRequests(makeAppDb(state), {
      status: 'paid_out',
    });
    expect(result.count).toBe(1);
    expect(result.requests[0]?.status).toBe('paid_out');
  });

  it('filters by opera_account', async () => {
    const state: MockState = {
      requests: [
        emptyRequest({ id: 1, opera_account: 'A' }),
        emptyRequest({ id: 2, opera_account: 'B' }),
      ],
      mandates: [],
    };
    const result = await listPaymentRequests(makeAppDb(state), {
      operaAccount: 'A',
    });
    expect(result.count).toBe(1);
  });

  it('respects limit', async () => {
    const state: MockState = {
      requests: Array.from({ length: 5 }, (_, i) =>
        emptyRequest({ id: i + 1, created_at: `2026-04-${10 + i}` }),
      ),
      mandates: [],
    };
    const result = await listPaymentRequests(makeAppDb(state), { limit: 2 });
    expect(result.count).toBe(2);
  });

  it('enriches with customer_name from mandates', async () => {
    const state: MockState = {
      requests: [
        emptyRequest({ id: 1, opera_account: 'A' }),
        emptyRequest({ id: 2, opera_account: 'B' }),
      ],
      mandates: [
        { opera_account: 'A', opera_name: 'Acme Ltd' },
        { opera_account: 'B', opera_name: 'Beta Co' },
      ],
    };
    const result = await listPaymentRequests(makeAppDb(state));
    const aReq = result.requests.find((r) => r.opera_account === 'A');
    expect(aReq?.customer_name).toBe('Acme Ltd');
  });

  it('falls back to opera_account when mandate not found', async () => {
    const state: MockState = {
      requests: [emptyRequest({ id: 1, opera_account: 'CUST01' })],
      mandates: [],
    };
    const result = await listPaymentRequests(makeAppDb(state));
    expect(result.requests[0]?.customer_name).toBe('CUST01');
  });
});
