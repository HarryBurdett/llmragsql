import { describe, it, expect } from 'vitest';
import {
  getAgedDebtSummary,
  getAgedDebtBySupplier,
} from '../src/services/aged-debt.js';

function makeMockOpera(rows: unknown[]): any {
  const db: any = () => ({});
  db.raw = async () => rows;
  return db;
}

describe('getAgedDebtSummary', () => {
  it('returns buckets with totals + grand total', async () => {
    const db = makeMockOpera([
      { age_band: 'Current (0-30 days)', count: 5, total: 15000 },
      { age_band: '31-60 days', count: 2, total: 5000 },
      { age_band: '61-90 days', count: 1, total: 1500 },
      { age_band: 'Over 90 days', count: 0, total: 0 },
    ]);
    const result = await getAgedDebtSummary(db);
    expect(result.success).toBe(true);
    expect(result.buckets).toHaveLength(4);
    expect(result.total).toBe(21500);
    expect(result.count).toBe(8);
  });

  it('returns empty buckets when no aged debt', async () => {
    const db = makeMockOpera([]);
    const result = await getAgedDebtSummary(db);
    expect(result.success).toBe(true);
    expect(result.buckets).toEqual([]);
    expect(result.total).toBe(0);
  });

  it('handles query errors', async () => {
    const db: any = {
      raw: async () => {
        throw new Error('connection lost');
      },
    };
    const result = await getAgedDebtSummary(db);
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/connection lost/);
  });
});

describe('getAgedDebtBySupplier', () => {
  it('returns per-supplier breakdown rounded to 2dp', async () => {
    const db = makeMockOpera([
      {
        account: 'SUPP001',
        name: 'Acme Ltd',
        current_0_30: 1000.5,
        days_31_60: 250.25,
        days_61_90: 0,
        over_90: 0,
        total: 1250.75,
      },
      {
        account: 'SUPP002',
        name: 'Beta Co',
        current_0_30: 500,
        days_31_60: 0,
        days_61_90: 100,
        over_90: 0,
        total: 600,
      },
    ]);
    const result = await getAgedDebtBySupplier(db);
    expect(result.success).toBe(true);
    expect(result.suppliers).toHaveLength(2);
    expect(result.suppliers[0]?.account).toBe('SUPP001');
    expect(result.suppliers[0]?.total).toBe(1250.75);
    expect(result.suppliers[1]?.days_61_90).toBe(100);
  });

  it('returns empty array when no suppliers have outstanding ptran', async () => {
    const db = makeMockOpera([]);
    const result = await getAgedDebtBySupplier(db);
    expect(result.success).toBe(true);
    expect(result.suppliers).toEqual([]);
  });

  it('handles query errors', async () => {
    const db: any = {
      raw: async () => {
        throw new Error('table missing');
      },
    };
    const result = await getAgedDebtBySupplier(db);
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/table missing/);
  });
});
