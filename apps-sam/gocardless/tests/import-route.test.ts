import { describe, it, expect } from 'vitest';
import { importGocardlessRoute } from '../src/services/import-route.js';

/**
 * Validation tests for the `/api/gocardless/import` orchestration layer.
 *
 * Pins the wrapping behaviour in front of `importGocardlessBatch`:
 *   - SQL-injection guards on bank_code / cbtype / fees nominal
 *   - Idempotency gate (duplicate payout_id refused)
 *   - Mandate verification (mandate_id → opera_account check)
 *   - Bank existence in nbank
 *
 * The bank-lock + Opera writes are exercised by the integration suite.
 * These unit tests fail fast at the validation gates so we never touch
 * the transactional path.
 */

interface MandateRow {
  mandate_id: string;
  opera_account: string;
}
interface ImportRow {
  payout_id: string | null;
  target_system: string;
}

interface AppState {
  mandates: MandateRow[];
  imports: ImportRow[];
  settings: Record<string, string | null>;
}

function makeAppDb(state: AppState): any {
  const db: any = (table: string) => {
    if (table === 'gocardless_imports') {
      const cond: Record<string, unknown> = {};
      const builder: any = {
        where: (c: any) => {
          Object.assign(cond, c);
          return builder;
        },
        andWhere: (c: any) => {
          Object.assign(cond, c);
          return builder;
        },
        first: () =>
          Promise.resolve(
            state.imports.find((r) =>
              Object.entries(cond).every(
                ([k, v]) => (r as any)[k] === v,
              ),
            ),
          ),
        insert: () => Promise.resolve([1]),
      };
      return builder;
    }
    if (table === 'gocardless_mandates') {
      const builder: any = {
        select: () => Promise.resolve(state.mandates),
        where: () => builder,
      };
      return builder;
    }
    if (table === 'settings') {
      let target = '';
      const builder: any = {
        where: (c: any) => {
          target = String(c.key ?? '');
          return builder;
        },
        first: () => {
          const v = state.settings[target];
          return Promise.resolve(v !== undefined ? { value: v } : undefined);
        },
        select: () =>
          Promise.resolve(
            Object.entries(state.settings).map(([key, value]) => ({
              key,
              value,
            })),
          ),
      };
      return builder;
    }
    if (table === 'import_locks') {
      // Allow lock acquisition by always returning empty (no existing lock)
      const builder: any = {
        where: () => builder,
        first: () => Promise.resolve(undefined),
        delete: () => Promise.resolve(0),
        insert: () => Promise.resolve([1]),
      };
      return builder;
    }
    throw new Error(`Unexpected table: ${table}`);
  };
  db.fn = { now: () => new Date() };
  return db;
}

interface OperaState {
  /** sname rows keyed by sn_account */
  customers: Record<string, { sn_name: string }>;
  /** existing nbank rows (each entry: nk_acnt, nk_sort, nk_number) */
  banks: Array<{ nk_acnt: string; nk_sort: string; nk_number: string }>;
}

function makeOperaDb(state: OperaState): any {
  return {
    raw: (sql: string, params?: unknown[]) => {
      // bankExists check
      if (
        sql.includes('FROM nbank') &&
        sql.includes('RTRIM(nk_acnt) = ?')
      ) {
        const code = String((params ?? [])[0] ?? '').trim();
        const found = state.banks.find((b) => b.nk_acnt.trim() === code);
        return Promise.resolve(found ? [{ nk_acnt: found.nk_acnt }] : []);
      }
      // resolveDestinationBank — full nbank dump
      if (sql.includes('FROM nbank') && sql.includes('nk_sort')) {
        return Promise.resolve(
          state.banks.map((b) => ({
            nk_acnt: b.nk_acnt,
            nk_sort: b.nk_sort,
            nk_number: b.nk_number,
          })),
        );
      }
      return Promise.resolve([]);
    },
  };
}

describe('importGocardlessRoute — validation', () => {
  it('rejects bad bank_code', async () => {
    const result = await importGocardlessRoute(
      makeAppDb({ mandates: [], imports: [], settings: {} }),
      makeOperaDb({ customers: {}, banks: [] }),
      {
        bankCode: "BC';--",
        postDate: '2026-04-15',
        payments: [{ customer_account: 'CUST01', amount: 100 }],
      },
    );
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/bank/);
  });

  it('rejects empty payments', async () => {
    const result = await importGocardlessRoute(
      makeAppDb({ mandates: [], imports: [], settings: {} }),
      makeOperaDb({ customers: {}, banks: [] }),
      {
        bankCode: 'BC010',
        postDate: '2026-04-15',
        payments: [],
      },
    );
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/No payments/);
  });

  it('refuses duplicate payout_id', async () => {
    const result = await importGocardlessRoute(
      makeAppDb({
        mandates: [],
        imports: [{ payout_id: 'PO_X', target_system: 'opera_se' }],
        settings: {},
      }),
      makeOperaDb({ customers: {}, banks: [] }),
      {
        bankCode: 'BC010',
        postDate: '2026-04-15',
        payments: [{ customer_account: 'CUST01', amount: 100 }],
        payoutId: 'PO_X',
      },
    );
    expect(result.success).toBe(false);
    expect(result.duplicate_payout).toBe(true);
  });

  it('rejects payment with missing amount', async () => {
    const result = await importGocardlessRoute(
      makeAppDb({ mandates: [], imports: [], settings: {} }),
      makeOperaDb({ customers: {}, banks: [] }),
      {
        bankCode: 'BC010',
        postDate: '2026-04-15',
        payments: [{ customer_account: 'CUST01', amount: 0 }],
      },
    );
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/Missing amount/);
  });

  it('blocks when mandate is linked to a different account', async () => {
    const result = await importGocardlessRoute(
      makeAppDb({
        mandates: [
          { mandate_id: 'MD_X', opera_account: 'CUST_REAL' },
        ],
        imports: [],
        settings: {},
      }),
      makeOperaDb({
        customers: { CUST_REAL: { sn_name: 'Real' } },
        banks: [{ nk_acnt: 'BC010', nk_sort: '00-00-00', nk_number: '99' }],
      }),
      {
        bankCode: 'BC010',
        postDate: '2026-04-15',
        payments: [
          {
            customer_account: 'CUST_WRONG',
            customer_name: 'Wrong',
            amount: 100,
            mandate_id: 'MD_X',
          },
        ],
      },
    );
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/mandate MD_X belongs to account CUST_REAL/);
  });

  it('returns missing-bank error when posting bank not in nbank', async () => {
    const result = await importGocardlessRoute(
      makeAppDb({ mandates: [], imports: [], settings: {} }),
      makeOperaDb({
        customers: {},
        banks: [], // no banks at all
      }),
      {
        bankCode: 'BC999',
        postDate: '2026-04-15',
        payments: [{ customer_account: 'CUST01', amount: 100 }],
      },
    );
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/does not exist in this company's bank accounts/);
  });
});
