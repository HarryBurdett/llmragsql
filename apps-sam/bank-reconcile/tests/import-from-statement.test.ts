import { describe, it, expect } from 'vitest';
import { importFromStatementPreview } from '../src/services/import-from-statement.js';

interface AliasRow {
  id: number;
  bank_code: string;
  payee_pattern: string;
  match_type: string;
  opera_account: string;
  confidence: number;
  direction: 'receipt' | 'payment' | 'either';
}

interface AppState {
  aliases: AliasRow[];
}

interface OperaState {
  customers: Record<string, string>;
  suppliers: Record<string, string>;
}

function makeAppDb(state: AppState): any {
  return (table: string) => {
    if (table !== 'bank_import_aliases') {
      throw new Error(`Unexpected: ${table}`);
    }
    let bank = '';
    const builder: any = {
      where: (cond: any) => {
        bank = String(cond.bank_code ?? '');
        return builder;
      },
      select: () =>
        Promise.resolve(state.aliases.filter((a) => a.bank_code === bank)),
    };
    return builder;
  };
}

function makeOperaDb(state: OperaState): any {
  return {
    raw: (sql: string) => {
      if (sql.includes('FROM sname')) {
        return Promise.resolve(
          Object.entries(state.customers).map(([account, name]) => ({
            account,
            name,
          })),
        );
      }
      if (sql.includes('FROM pname')) {
        return Promise.resolve(
          Object.entries(state.suppliers).map(([account, name]) => ({
            account,
            name,
          })),
        );
      }
      return Promise.resolve([]);
    },
  };
}

describe('importFromStatementPreview', () => {
  it('rejects bad bank code', async () => {
    const r = await importFromStatementPreview(
      makeAppDb({ aliases: [] }),
      makeOperaDb({ customers: {}, suppliers: {} }),
      "BC';--",
      [{ date: '2026-04-15', amount: 100 }],
    );
    expect(r.success).toBe(false);
  });

  it('returns empty groups when no transactions supplied', async () => {
    const r = await importFromStatementPreview(
      makeAppDb({ aliases: [] }),
      makeOperaDb({ customers: {}, suppliers: {} }),
      'BC010',
      [],
    );
    expect(r.success).toBe(true);
    expect(r.total_transactions).toBe(0);
    expect(r.summary?.unmatched).toBe(0);
  });

  it('matches a receipt to a customer alias', async () => {
    const r = await importFromStatementPreview(
      makeAppDb({
        aliases: [
          {
            id: 1,
            bank_code: 'BC010',
            payee_pattern: 'ACME LTD',
            match_type: 'customer',
            opera_account: 'A046',
            confidence: 0.95,
            direction: 'either',
          },
        ],
      }),
      makeOperaDb({
        customers: { A046: 'Acme Ltd' },
        suppliers: {},
      }),
      'BC010',
      [
        {
          date: '2026-04-15',
          description: 'BACS from ACME LTD',
          amount: 100,
        },
      ],
    );
    expect(r.success).toBe(true);
    expect(r.matched_receipts).toHaveLength(1);
    expect(r.matched_receipts?.[0]?.action).toBe('sales_receipt');
    expect(r.matched_receipts?.[0]?.matched_account).toBe('A046');
  });

  it('matches a payment to a supplier alias', async () => {
    const r = await importFromStatementPreview(
      makeAppDb({
        aliases: [
          {
            id: 2,
            bank_code: 'BC010',
            payee_pattern: 'WIDGET CO',
            match_type: 'supplier',
            opera_account: 'W034',
            confidence: 0.9,
            direction: 'either',
          },
        ],
      }),
      makeOperaDb({
        customers: {},
        suppliers: { W034: 'Widget Co' },
      }),
      'BC010',
      [
        {
          date: '2026-04-15',
          description: 'PAY WIDGET CO',
          amount: -250,
        },
      ],
    );
    expect(r.success).toBe(true);
    expect(r.matched_payments).toHaveLength(1);
    expect(r.matched_payments?.[0]?.action).toBe('purchase_payment');
  });

  it('respects direction=receipt restriction', async () => {
    const r = await importFromStatementPreview(
      makeAppDb({
        aliases: [
          {
            id: 3,
            bank_code: 'BC010',
            payee_pattern: 'BANK CHARGES',
            match_type: 'nominal',
            opera_account: '7901',
            confidence: 0.85,
            direction: 'payment',
          },
        ],
      }),
      makeOperaDb({ customers: {}, suppliers: {} }),
      'BC010',
      [
        // Direction='payment' but this is positive → should NOT match
        { date: '2026-04-15', description: 'BANK CHARGES', amount: 5 },
        // negative → SHOULD match
        { date: '2026-04-15', description: 'BANK CHARGES', amount: -5 },
      ],
    );
    expect(r.success).toBe(true);
    expect(r.matched_payments?.[0]?.matched_account).toBe('7901');
    expect(r.unmatched).toHaveLength(1);
  });

  it('flags unmatched transactions with deferred-matcher reason', async () => {
    const r = await importFromStatementPreview(
      makeAppDb({ aliases: [] }),
      makeOperaDb({ customers: {}, suppliers: {} }),
      'BC010',
      [{ date: '2026-04-15', description: 'UNKNOWN', amount: 50 }],
    );
    expect(r.unmatched).toHaveLength(1);
    expect(r.unmatched?.[0]?.skip_reason).toMatch(/AI fuzzy/);
  });
});
