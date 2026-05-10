import { describe, it, expect } from 'vitest';
import { getVatRate } from '../src/opera/vat-rates.js';

interface MockState {
  // Map of (code|trantyp) → row, or just (code) → row when trantyp omitted
  rows: Array<{
    tx_code: string;
    tx_trantyp: 'S' | 'P' | string;
    tx_rate1?: number | null;
    tx_rate2?: number | null;
    tx_rate1dy?: Date | string | null;
    tx_rate2dy?: Date | string | null;
    tx_nominal?: string | null;
    tx_desc?: string | null;
  }>;
  fallbackNominal?: string;
}

function makeOperaDb(state: MockState): any {
  return {
    raw: (sql: string, params?: unknown[]) => {
      // Fallback "any nominal" lookup
      if (sql.includes('SELECT TOP 1 tx_nominal')) {
        return Promise.resolve(
          state.fallbackNominal
            ? [{ tx_nominal: state.fallbackNominal }]
            : [],
        );
      }
      // Strict: code + trantyp + ctrytyp='H'
      if (sql.includes("AND tx_trantyp = ?")) {
        const code = String((params ?? [])[0]);
        const trantyp = String((params ?? [])[1]);
        const found = state.rows.find(
          (r) =>
            r.tx_code.trim() === code && r.tx_trantyp === trantyp,
        );
        return Promise.resolve(found ? [found] : []);
      }
      // Loose: code + ctrytyp='H' (no trantyp)
      if (sql.includes('FROM ztax') && sql.includes('RTRIM(tx_code) = ?')) {
        const code = String((params ?? [])[0]);
        const found = state.rows.find((r) => r.tx_code.trim() === code);
        return Promise.resolve(found ? [found] : []);
      }
      return Promise.resolve([]);
    },
  };
}

describe('getVatRate', () => {
  it('returns rate1 when no rate2 effective date', async () => {
    const state: MockState = {
      rows: [
        {
          tx_code: '2',
          tx_trantyp: 'P',
          tx_rate1: 20,
          tx_rate2: null,
          tx_rate1dy: null,
          tx_rate2dy: null,
          tx_nominal: '7770',
          tx_desc: 'Standard rate',
        },
      ],
    };
    const result = await getVatRate(makeOperaDb(state), '2', 'P');
    expect(result.found).toBe(true);
    expect(result.rate).toBe(20);
    expect(result.nominal).toBe('7770');
  });

  it('uses rate2 when as_of_date >= rate2 effective date', async () => {
    const state: MockState = {
      rows: [
        {
          tx_code: '2',
          tx_trantyp: 'P',
          tx_rate1: 17.5,
          tx_rate2: 20,
          tx_rate1dy: '2008-01-01',
          tx_rate2dy: '2011-01-04',
          tx_nominal: '7770',
        },
      ],
    };
    const result = await getVatRate(
      makeOperaDb(state),
      '2',
      'P',
      new Date('2026-04-15'),
    );
    expect(result.rate).toBe(20);
  });

  it('uses rate1 when as_of_date < rate2 effective date', async () => {
    const state: MockState = {
      rows: [
        {
          tx_code: '2',
          tx_trantyp: 'P',
          tx_rate1: 17.5,
          tx_rate2: 20,
          tx_rate1dy: '2008-01-01',
          tx_rate2dy: '2011-01-04',
          tx_nominal: '7770',
        },
      ],
    };
    const result = await getVatRate(
      makeOperaDb(state),
      '2',
      'P',
      new Date('2010-06-15'),
    );
    expect(result.rate).toBe(17.5);
  });

  it('falls back to no-trantyp lookup when strict miss', async () => {
    const state: MockState = {
      rows: [
        {
          tx_code: '1',
          tx_trantyp: 'S',
          tx_rate1: 20,
          tx_nominal: '2200',
        },
      ],
    };
    const result = await getVatRate(makeOperaDb(state), '1', 'P');
    expect(result.found).toBe(true);
    expect(result.rate).toBe(20);
  });

  it('returns 0/empty/found=false with fallback nominal when code not found', async () => {
    const state: MockState = {
      rows: [],
      fallbackNominal: '7770',
    };
    const result = await getVatRate(makeOperaDb(state), 'UNKNOWN', 'P');
    expect(result.found).toBe(false);
    expect(result.rate).toBe(0);
    expect(result.nominal).toBe('7770');
  });

  it('rejects empty code without DB hits', async () => {
    const state: MockState = { rows: [] };
    const result = await getVatRate(makeOperaDb(state), '', 'P');
    expect(result.found).toBe(false);
    expect(result.nominal).toBe('');
  });
});
