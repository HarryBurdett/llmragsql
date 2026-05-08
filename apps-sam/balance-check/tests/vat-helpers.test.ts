/**
 * Tests for VAT helpers — port of `apps/balance_check/logic/vat_reconcile.py`.
 *
 * Pin the UK quarter calculator and the VAT-codes-with-rates picker
 * since both are tricky and easy to get wrong on date arithmetic.
 */
import { describe, it, expect } from 'vitest';
import {
  getVatQuarterDates,
  fetchVatCodesWithRates,
  fetchZvtranAggregate,
  fetchNvatAggregate,
} from '../src/services/vat-helpers.js';

describe('getVatQuarterDates', () => {
  it('Q1 (Jan-Mar) — month 2', () => {
    const result = getVatQuarterDates(new Date(2026, 1, 15)); // Feb 15
    expect(result.current_quarter).toBe('Q1 2026');
    expect(result.quarter_start).toBe('2026-01-01');
    expect(result.quarter_end).toBe('2026-03-31');
  });

  it('Q2 (Apr-Jun) — month 5', () => {
    const result = getVatQuarterDates(new Date(2026, 4, 10)); // May 10
    expect(result.current_quarter).toBe('Q2 2026');
    expect(result.quarter_start).toBe('2026-04-01');
    expect(result.quarter_end).toBe('2026-06-30');
  });

  it('Q3 (Jul-Sep) — month 8', () => {
    const result = getVatQuarterDates(new Date(2026, 7, 20)); // Aug 20
    expect(result.current_quarter).toBe('Q3 2026');
    expect(result.quarter_start).toBe('2026-07-01');
    expect(result.quarter_end).toBe('2026-09-30');
  });

  it('Q4 (Oct-Dec) — month 10', () => {
    const result = getVatQuarterDates(new Date(2026, 10, 5)); // Nov 5
    expect(result.current_quarter).toBe('Q4 2026');
    expect(result.quarter_start).toBe('2026-10-01');
    expect(result.quarter_end).toBe('2026-12-31');
  });

  it('returns 4 quarters total — current + previous 3', () => {
    const result = getVatQuarterDates(new Date(2026, 4, 10)); // May 10 (Q2)
    expect(result.quarters).toHaveLength(4);
    expect(result.quarters[0]?.is_current).toBe(true);
    expect(result.quarters[0]?.name).toBe('Q2 2026');
    expect(result.quarters[1]?.name).toBe('Q1 2026');
    expect(result.quarters[2]?.name).toBe('Q4 2025');
    expect(result.quarters[3]?.name).toBe('Q3 2025');
  });

  it('handles year rollover when reference is Q1', () => {
    const result = getVatQuarterDates(new Date(2026, 0, 15)); // Jan 15
    expect(result.quarters[1]?.name).toBe('Q4 2025');
    expect(result.quarters[2]?.name).toBe('Q3 2025');
    expect(result.quarters[3]?.name).toBe('Q2 2025');
  });

  it('uses today when reference is null', () => {
    const result = getVatQuarterDates(null);
    // Just check it produces a valid structure
    expect(result.quarters).toHaveLength(4);
    expect(result.current_quarter).toMatch(/^Q[1-4] \d{4}$/);
    expect(result.quarter_start).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    expect(result.quarter_end).toMatch(/^\d{4}-\d{2}-\d{2}$/);
  });
});

describe('fetchVatCodesWithRates', () => {
  function makeMock(rows: any[]): any {
    const db: any = () => ({});
    db.raw = async () => rows;
    return db;
  }

  it('returns vat codes with applicable rate (rate1 active, rate2 not effective yet)', async () => {
    const refDate = new Date(2026, 4, 1); // May 1, 2026
    const db = makeMock([
      {
        tx_code: '1',
        tx_desc: 'Standard 20%',
        tx_rate1: 20,
        tx_rate1dy: new Date(2020, 0, 1), // Jan 2020 — effective
        tx_rate2: 22,
        tx_rate2dy: new Date(2027, 0, 1), // Jan 2027 — future
        tx_trantyp: 'S',
        tx_nominal: '2200',
      },
    ]);

    const result = await fetchVatCodesWithRates(db, refDate);
    expect(result.vat_codes[0]?.rate).toBe(20);
    expect(result.output_nominal_accounts.has('2200')).toBe(true);
  });

  it('uses rate2 when both effective and rate2 is more recent', async () => {
    const refDate = new Date(2026, 4, 1);
    const db = makeMock([
      {
        tx_code: '1',
        tx_desc: 'Standard',
        tx_rate1: 17.5,
        tx_rate1dy: new Date(2010, 0, 1),
        tx_rate2: 20,
        tx_rate2dy: new Date(2011, 0, 4),
        tx_trantyp: 'S',
        tx_nominal: '2200',
      },
    ]);

    const result = await fetchVatCodesWithRates(db, refDate);
    expect(result.vat_codes[0]?.rate).toBe(20);
  });

  it('separates output (S) and input (P) nominal accounts', async () => {
    const refDate = new Date(2026, 4, 1);
    const db = makeMock([
      {
        tx_code: '1',
        tx_desc: 'Sales VAT',
        tx_rate1: 20,
        tx_rate1dy: null,
        tx_rate2: null,
        tx_rate2dy: null,
        tx_trantyp: 'S',
        tx_nominal: '2200',
      },
      {
        tx_code: '2',
        tx_desc: 'Purchases VAT',
        tx_rate1: 20,
        tx_rate1dy: null,
        tx_rate2: null,
        tx_rate2dy: null,
        tx_trantyp: 'P',
        tx_nominal: '2201',
      },
    ]);

    const result = await fetchVatCodesWithRates(db, refDate);
    expect(result.output_nominal_accounts.has('2200')).toBe(true);
    expect(result.output_nominal_accounts.has('2201')).toBe(false);
    expect(result.input_nominal_accounts.has('2201')).toBe(true);
    expect(result.input_nominal_accounts.has('2200')).toBe(false);
  });
});

describe('fetchZvtranAggregate', () => {
  it('aggregates by code, sums vat_amount, returns total', async () => {
    const db: any = () => ({});
    db.raw = async () => [
      { vat_code: '1', transaction_count: 5, vat_amount: 100, net_amount: 500 },
      { vat_code: '2', transaction_count: 3, vat_amount: 60, net_amount: 300 },
    ];

    const result = await fetchZvtranAggregate(db, {
      vattype: 'S',
      quarterStart: '2026-04-01',
      quarterEnd: '2026-06-30',
    });

    expect(result.total_vat).toBe(160);
    expect(result.by_code).toHaveLength(2);
    expect(result.by_code[0]?.vat_code).toBe('1');
    expect(result.by_code[0]?.vat_amount).toBe(100);
    expect(result.by_code[0]?.net_amount).toBe(500);
  });

  it('omits net_amount when includeNet=false', async () => {
    const db: any = () => ({});
    db.raw = async () => [{ vat_code: '1', transaction_count: 5, vat_amount: 100, net_amount: 500 }];

    const result = await fetchZvtranAggregate(db, {
      vattype: 'S',
      quarterStart: '2026-04-01',
      quarterEnd: '2026-06-30',
      includeNet: false,
    });

    expect(result.by_code[0]?.net_amount).toBeUndefined();
  });
});

describe('fetchNvatAggregate', () => {
  it('aggregates committed VAT (no net_amount returned)', async () => {
    const db: any = () => ({});
    db.raw = async () => [
      { vat_code: '1', transaction_count: 10, vat_amount: 200 },
      { vat_code: '2', transaction_count: 5, vat_amount: 50 },
    ];

    const result = await fetchNvatAggregate(db, {
      vattype: 'S',
      periodStart: '2026-04-01',
      periodEnd: '2026-06-30',
    });

    expect(result.total_vat).toBe(250);
    expect(result.by_code).toHaveLength(2);
    expect(result.by_code[0]?.vat_amount).toBe(200);
  });
});
