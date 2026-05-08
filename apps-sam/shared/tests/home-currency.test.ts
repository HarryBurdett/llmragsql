import { describe, it, expect } from 'vitest';
import {
  getHomeCurrency,
  clearHomeCurrencyCache,
} from '../src/opera/home-currency.js';

interface MockState {
  rows: Array<{ xc_curr: string | null; xc_desc: string | null }>;
  throwOn?: boolean;
  callCount: number;
}

function makeOperaDb(state: MockState): any {
  return {
    raw: (_sql: string) => {
      state.callCount++;
      if (state.throwOn) return Promise.reject(new Error('zxchg missing'));
      return Promise.resolve(state.rows);
    },
  };
}

describe('getHomeCurrency', () => {
  it('returns the row where xc_home=1', async () => {
    const state: MockState = {
      rows: [{ xc_curr: 'GBP', xc_desc: 'Sterling' }],
      callCount: 0,
    };
    const db = makeOperaDb(state);
    const result = await getHomeCurrency(db);

    expect(result).toEqual({
      code: 'GBP',
      description: 'Sterling',
      found: true,
    });
  });

  it('trims whitespace from currency code and description', async () => {
    const state: MockState = {
      rows: [{ xc_curr: '  USD  ', xc_desc: '  US Dollar  ' }],
      callCount: 0,
    };
    const result = await getHomeCurrency(makeOperaDb(state));
    expect(result.code).toBe('USD');
    expect(result.description).toBe('US Dollar');
  });

  it('returns default when no row found', async () => {
    const state: MockState = { rows: [], callCount: 0 };
    const result = await getHomeCurrency(makeOperaDb(state));
    expect(result).toEqual({
      code: 'GBP',
      description: 'Sterling (default)',
      found: false,
    });
  });

  it('returns default when query throws', async () => {
    const state: MockState = { rows: [], throwOn: true, callCount: 0 };
    const result = await getHomeCurrency(makeOperaDb(state));
    expect(result.found).toBe(false);
    expect(result.code).toBe('GBP');
  });

  it('caches result per db instance', async () => {
    const state: MockState = {
      rows: [{ xc_curr: 'GBP', xc_desc: 'Sterling' }],
      callCount: 0,
    };
    const db = makeOperaDb(state);
    await getHomeCurrency(db);
    await getHomeCurrency(db);
    await getHomeCurrency(db);
    expect(state.callCount).toBe(1); // only first call hit DB
  });

  it('cache is cleared by clearHomeCurrencyCache', async () => {
    const state: MockState = {
      rows: [{ xc_curr: 'GBP', xc_desc: 'Sterling' }],
      callCount: 0,
    };
    const db = makeOperaDb(state);
    await getHomeCurrency(db);
    clearHomeCurrencyCache(db);
    await getHomeCurrency(db);
    expect(state.callCount).toBe(2);
  });
});
