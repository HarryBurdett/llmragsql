/**
 * Tests for reconcileTrialBalance.
 */
import { describe, it, expect } from 'vitest';
import { reconcileTrialBalance } from '../src/services/reconcile-trial-balance.js';

function makeMockKnex(canned: {
  currentYear?: number;
  accounts?: Array<{
    account: string;
    description: string;
    type: string;
    prior_debits: number;
    prior_credits: number;
    ytd_debits: number;
    ytd_credits: number;
    current_debits: number;
    current_credits: number;
    current_net: number;
  }>;
}): any {
  const db: any = () => ({
    select: () => ({}),
    first: async () => null,
  });
  db.raw = async (sql: string, _bindings?: unknown[]) => {
    if (sql.includes('MAX(nt_year)')) {
      return [{ current_year: canned.currentYear ?? 2026 }];
    }
    if (sql.includes('na_acnt') && sql.includes('LEFT JOIN')) {
      return canned.accounts ?? [];
    }
    return [];
  };
  return db;
}

describe('reconcileTrialBalance', () => {
  it('returns BALANCED when debits = credits everywhere', async () => {
    const db = makeMockKnex({
      currentYear: 2026,
      accounts: [
        {
          account: '1100',
          description: 'Debtors Control',
          type: 'A',
          prior_debits: 1000,
          prior_credits: 0,
          ytd_debits: 500,
          ytd_credits: 0,
          current_debits: 500,
          current_credits: 0,
          current_net: 500,
        },
        {
          account: '2100',
          description: 'Creditors Control',
          type: 'L',
          prior_debits: 0,
          prior_credits: 1000,
          ytd_debits: 0,
          ytd_credits: 500,
          current_debits: 0,
          current_credits: 500,
          current_net: -500,
        },
      ],
    });

    const result = await reconcileTrialBalance(db);

    expect(result.success).toBe(true);
    expect(result.status).toBe('BALANCED');
    expect(result.summary?.brought_forward.balanced).toBe(true);
    expect(result.summary?.current_year.balanced).toBe(true);
    expect(result.summary?.closing.balanced).toBe(true);
    expect(result.accounts).toHaveLength(2);
    expect(result.message).toMatch(/correct/);
  });

  it('returns UNBALANCED when debits != credits', async () => {
    const db = makeMockKnex({
      currentYear: 2026,
      accounts: [
        {
          account: '1100',
          description: 'Debtors',
          type: 'A',
          prior_debits: 1000,
          prior_credits: 0,
          ytd_debits: 0,
          ytd_credits: 0,
          current_debits: 0,
          current_credits: 0,
          current_net: 0,
        },
        // Imbalanced — only £500 credit balance to offset £1000 debit
        {
          account: '2100',
          description: 'Creditors',
          type: 'L',
          prior_debits: 0,
          prior_credits: 500,
          ytd_debits: 0,
          ytd_credits: 0,
          current_debits: 0,
          current_credits: 0,
          current_net: 0,
        },
      ],
    });

    const result = await reconcileTrialBalance(db);

    expect(result.success).toBe(true);
    expect(result.status).toBe('UNBALANCED');
    expect(result.summary?.brought_forward.balanced).toBe(false);
    expect(result.summary?.brought_forward.variance).toBe(500);
    expect(result.message).toMatch(/B\/F: £500/);
  });

  it('maps account types to type names correctly', async () => {
    const db = makeMockKnex({
      currentYear: 2026,
      accounts: [
        {
          account: '1100',
          description: 'Debtors',
          type: 'A',
          prior_debits: 100,
          prior_credits: 0,
          ytd_debits: 0,
          ytd_credits: 0,
          current_debits: 0,
          current_credits: 0,
          current_net: 0,
        },
        {
          account: '2100',
          description: 'Creditors',
          type: 'L',
          prior_debits: 0,
          prior_credits: 100,
          ytd_debits: 0,
          ytd_credits: 0,
          current_debits: 0,
          current_credits: 0,
          current_net: 0,
        },
      ],
    });

    const result = await reconcileTrialBalance(db);

    expect(result.accounts[0]?.type_name).toBe('Asset');
    expect(result.accounts[1]?.type_name).toBe('Liability');
  });

  it('returns success=false on query failure', async () => {
    const db: any = {
      raw: async () => {
        throw new Error('connection lost');
      },
    };

    const result = await reconcileTrialBalance(db);
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/connection lost/);
  });
});
