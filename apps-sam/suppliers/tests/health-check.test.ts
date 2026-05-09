import { describe, it, expect } from 'vitest';
import { runSuppliersHealthCheck } from '../src/services/health-check.js';

interface PnameRow {
  code: string;
}

interface AppRow {
  supplier_code: string;
}

interface MockState {
  pname: PnameRow[];
  statements: AppRow[];
  config: AppRow[];
  /** When true, supplier_statements table read throws (simulates table-not-yet-created). */
  statementsThrows?: boolean;
  /** When true, supplier_config table read throws. */
  configThrows?: boolean;
}

function makeOperaDb(state: MockState): any {
  const db: any = (_table: string) => {
    throw new Error('Opera table reads should go through .raw');
  };
  db.raw = async (sql: string) => {
    if (sql.includes('pname')) return state.pname;
    throw new Error(`Unexpected raw SQL: ${sql}`);
  };
  return db;
}

function makeAppDb(state: MockState): any {
  const db: any = (table: string) => {
    let throwsOnRead = false;
    if (table === 'supplier_statements' && state.statementsThrows) {
      throwsOnRead = true;
    }
    if (table === 'supplier_config' && state.configThrows) {
      throwsOnRead = true;
    }
    const builder: any = {
      whereNotNull: () => builder,
      distinct: () => builder,
      select: async (..._cols: string[]) => {
        if (throwsOnRead) throw new Error('table missing');
        if (table === 'supplier_statements') return state.statements;
        if (table === 'supplier_config') return state.config;
        throw new Error(`Unexpected table: ${table}`);
      },
    };
    return builder;
  };
  db.fn = { now: () => '__NOW__' };
  return db;
}

describe('runSuppliersHealthCheck', () => {
  it('reports all-passed when local codes match Opera', async () => {
    const state: MockState = {
      pname: [{ code: 'SUPP01' }, { code: 'SUPP02' }],
      statements: [{ supplier_code: 'SUPP01' }, { supplier_code: 'SUPP02' }],
      config: [],
    };
    const result = await runSuppliersHealthCheck({
      operaDb: makeOperaDb(state),
      appDb: makeAppDb(state),
    });
    expect(result.app).toBe('suppliers');
    expect(result.healthy).toBe(true);
    expect(result.checks.find((c) => c.name === 'Supplier statement history')?.passed).toBe(true);
    expect(result.metadata.opera_supplier_count).toBe(2);
  });

  it('flags orphan supplier codes from local data', async () => {
    const state: MockState = {
      pname: [{ code: 'SUPP01' }],
      statements: [
        { supplier_code: 'SUPP01' },
        { supplier_code: 'GHOST01' },
      ],
      config: [],
    };
    const result = await runSuppliersHealthCheck({
      operaDb: makeOperaDb(state),
      appDb: makeAppDb(state),
    });
    const check = result.checks.find((c) => c.name === 'Supplier statement history');
    expect(check?.passed).toBe(false);
    expect(check?.orphan_count).toBe(1);
    expect(check?.orphans?.[0]?.supplier_code).toBe('GHOST01');
    expect(result.healthy).toBe(true); // warnings don't break healthy
  });

  it('falls back to supplier_config when supplier_statements is empty', async () => {
    const state: MockState = {
      pname: [{ code: 'SUPP01' }],
      statements: [],
      config: [{ supplier_code: 'SUPP01' }],
    };
    const result = await runSuppliersHealthCheck({
      operaDb: makeOperaDb(state),
      appDb: makeAppDb(state),
    });
    expect(result.checks.find((c) => c.name === 'Supplier statement history')?.passed).toBe(true);
  });

  it('reports skipped status when neither local table is provisioned', async () => {
    const state: MockState = {
      pname: [{ code: 'SUPP01' }],
      statements: [],
      config: [],
      statementsThrows: true,
      configThrows: true,
    };
    const result = await runSuppliersHealthCheck({
      operaDb: makeOperaDb(state),
      appDb: makeAppDb(state),
    });
    const check = result.checks.find((c) => c.name === 'Supplier statement history');
    expect(check?.passed).toBe(true);
    expect(check?.severity).toBe('info');
    expect(check?.description).toMatch(/Skipped/);
  });

  it('flags Opera connection error when pname returns no codes', async () => {
    const state: MockState = {
      pname: [],
      statements: [{ supplier_code: 'SUPP01' }],
      config: [],
    };
    const result = await runSuppliersHealthCheck({
      operaDb: makeOperaDb(state),
      appDb: makeAppDb(state),
    });
    const operaCheck = result.checks.find((c) => c.name === 'Opera connection');
    expect(operaCheck?.passed).toBe(false);
    expect(operaCheck?.severity).toBe('error');
    expect(result.healthy).toBe(false); // error breaks healthy
  });

  it('caps orphan list at 50 entries', async () => {
    const state: MockState = {
      pname: [{ code: 'KEEP' }],
      statements: Array.from({ length: 75 }, (_, i) => ({
        supplier_code: `GHOST${i}`,
      })),
      config: [],
    };
    const result = await runSuppliersHealthCheck({
      operaDb: makeOperaDb(state),
      appDb: makeAppDb(state),
    });
    const check = result.checks.find((c) => c.name === 'Supplier statement history');
    expect(check?.orphan_count).toBe(75);
    expect(check?.orphans?.length).toBe(50);
  });

  it('skips local check when appDb omitted', async () => {
    const state: MockState = {
      pname: [{ code: 'SUPP01' }],
      statements: [],
      config: [],
    };
    const result = await runSuppliersHealthCheck({
      operaDb: makeOperaDb(state),
    });
    const check = result.checks.find((c) => c.name === 'Supplier statement history');
    expect(check?.description).toMatch(/Skipped/);
  });
});
