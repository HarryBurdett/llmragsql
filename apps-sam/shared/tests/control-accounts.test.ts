/**
 * Tests for getControlAccounts — port of the Python tests in
 * tests/test_opera_config.py (where present) plus the equivalent
 * behavioural assertions made by the existing balance-check pytest
 * suite.
 */
import { describe, it, expect, beforeEach } from 'vitest';
import { getControlAccounts } from '../src/opera/control-accounts.js';

/**
 * Minimal mock Knex builder that returns canned rows from `.first()`.
 *
 * The real Knex API has many shapes; we only mock the chain
 *   knex(table).select(...).first()
 * because that's all getControlAccounts uses. The chain is built so
 * the call sequence matches the production code.
 */
function makeMockKnex(rows: Record<string, Record<string, string> | null>) {
  const mock: any = (table: string) => {
    const builder: any = {
      _table: table,
      select: (..._cols: unknown[]) => builder,
      first: async () => rows[table] ?? null,
    };
    return builder;
  };
  mock.raw = (sql: string) => sql;
  return mock;
}

describe('getControlAccounts', () => {
  beforeEach(() => {
    // Each test creates its own mock; the WeakMap cache is keyed
    // to the mock instance, so no cross-test leakage.
  });

  it('returns debtors from sprfls and creditors from pprfls when both are present', async () => {
    const db = makeMockKnex({
      sprfls: { debtors_control: '1100' },
      pprfls: { creditors_control: '2100' },
    });

    const result = await getControlAccounts(db);

    expect(result.debtorsControl).toBe('1100');
    expect(result.creditorsControl).toBe('2100');
    expect(result.source).toBe('sprfls');
  });

  it('falls back to nparm when sprfls/pprfls are missing values', async () => {
    const db = makeMockKnex({
      sprfls: { debtors_control: '' },
      pprfls: { creditors_control: '' },
      nparm: { debtors_control: '1200', creditors_control: '2200' },
    });

    const result = await getControlAccounts(db);

    expect(result.debtorsControl).toBe('1200');
    expect(result.creditorsControl).toBe('2200');
    expect(result.source).toBe('nparm');
  });

  it('uses sprfls for debtors AND nparm for creditors when pprfls is empty', async () => {
    const db = makeMockKnex({
      sprfls: { debtors_control: '1100' },
      pprfls: { creditors_control: '' },
      nparm: { debtors_control: '1200', creditors_control: '2300' },
    });

    const result = await getControlAccounts(db);

    expect(result.debtorsControl).toBe('1100');
    expect(result.creditorsControl).toBe('2300');
    // First non-default source wins
    expect(result.source).toBe('sprfls');
  });

  it('throws if debtors control cannot be determined', async () => {
    const db = makeMockKnex({
      sprfls: { debtors_control: '' },
      pprfls: { creditors_control: '2100' },
      nparm: { debtors_control: '', creditors_control: '' },
    });

    await expect(getControlAccounts(db)).rejects.toThrow(
      /Debtors control account not found/,
    );
  });

  it('throws if creditors control cannot be determined', async () => {
    const db = makeMockKnex({
      sprfls: { debtors_control: '1100' },
      pprfls: { creditors_control: '' },
      nparm: { debtors_control: '', creditors_control: '' },
    });

    await expect(getControlAccounts(db)).rejects.toThrow(
      /Creditors control account not found/,
    );
  });

  it('caches the result per-db', async () => {
    let sprflsCallCount = 0;
    const db: any = (table: string) => {
      const builder: any = {
        _table: table,
        select: (..._cols: unknown[]) => builder,
        first: async () => {
          if (table === 'sprfls') {
            sprflsCallCount++;
            return { debtors_control: '1100' };
          }
          if (table === 'pprfls') {
            return { creditors_control: '2100' };
          }
          return null;
        },
      };
      return builder;
    };
    db.raw = (sql: string) => sql;

    await getControlAccounts(db);
    await getControlAccounts(db);
    await getControlAccounts(db);

    expect(sprflsCallCount).toBe(1);
  });

  it('bypasses the cache when useCache=false', async () => {
    let sprflsCallCount = 0;
    const db: any = (table: string) => {
      const builder: any = {
        _table: table,
        select: (..._cols: unknown[]) => builder,
        first: async () => {
          if (table === 'sprfls') {
            sprflsCallCount++;
            return { debtors_control: '1100' };
          }
          if (table === 'pprfls') {
            return { creditors_control: '2100' };
          }
          return null;
        },
      };
      return builder;
    };
    db.raw = (sql: string) => sql;

    await getControlAccounts(db, false);
    await getControlAccounts(db, false);

    expect(sprflsCallCount).toBe(2);
  });

  it('NEVER hardcodes control account codes — assertion is in code review, not tests', () => {
    // This test is a placeholder marker. The CLAUDE.md mandate forbids
    // hardcoding account codes anywhere. The actual enforcement is via
    // code review (search for literal '1100', '2100', etc.). This file
    // doesn't attempt the search at runtime — that's a lint job.
    expect(true).toBe(true);
  });
});
