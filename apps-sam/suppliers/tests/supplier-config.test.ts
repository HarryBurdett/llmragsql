import { describe, it, expect } from 'vitest';
import {
  getSupplierConfig,
  saveSupplierConfig,
} from '../src/services/supplier-config.js';

interface MockState {
  rows: Map<
    string,
    { supplier_code: string; config_json: string | null; updated_at: Date }
  >;
}

function makeAppDb(state: MockState): any {
  const db: any = (table: string) => {
    if (table !== 'supplier_config') {
      throw new Error(`Unexpected table: ${table}`);
    }
    let whereCode = '';
    const builder: any = {
      where: (col: Record<string, unknown>) => {
        if (typeof col.supplier_code === 'string') whereCode = col.supplier_code;
        return builder;
      },
      first: async () => state.rows.get(whereCode) ?? null,
      update: async (patch: Record<string, unknown>) => {
        const existing = state.rows.get(whereCode);
        if (existing) {
          state.rows.set(whereCode, {
            ...existing,
            config_json: String(patch.config_json),
            updated_at: new Date(),
          });
          return 1;
        }
        return 0;
      },
      insert: async (row: Record<string, unknown>) => {
        state.rows.set(String(row.supplier_code), {
          supplier_code: String(row.supplier_code),
          config_json: String(row.config_json ?? ''),
          updated_at: new Date(),
        });
        return [1];
      },
    };
    return builder;
  };
  db.fn = { now: () => new Date() };
  return db;
}

describe('getSupplierConfig', () => {
  it('returns defaults when no row exists', async () => {
    const db = makeAppDb({ rows: new Map() });
    const result = await getSupplierConfig(db, 'SUPP001');
    expect(result.success).toBe(true);
    expect(result.config?.match_tolerance_pence).toBe(1);
    expect(result.config?.auto_process).toBe(false);
  });

  it('merges stored config with defaults', async () => {
    const state: MockState = {
      rows: new Map([
        [
          'SUPP001',
          {
            supplier_code: 'SUPP001',
            config_json: JSON.stringify({ auto_process: true }),
            updated_at: new Date(),
          },
        ],
      ]),
    };
    const db = makeAppDb(state);
    const result = await getSupplierConfig(db, 'SUPP001');
    expect(result.config?.auto_process).toBe(true);
    // Defaults still present
    expect(result.config?.match_tolerance_pence).toBe(1);
  });

  it('falls back to defaults when stored JSON is invalid', async () => {
    const state: MockState = {
      rows: new Map([
        [
          'SUPP001',
          {
            supplier_code: 'SUPP001',
            config_json: 'not-json',
            updated_at: new Date(),
          },
        ],
      ]),
    };
    const db = makeAppDb(state);
    const result = await getSupplierConfig(db, 'SUPP001');
    expect(result.success).toBe(true);
    expect(result.config?.match_tolerance_pence).toBe(1);
  });

  it('rejects empty supplier_code', async () => {
    const db = makeAppDb({ rows: new Map() });
    const result = await getSupplierConfig(db, '');
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/required/);
  });
});

describe('saveSupplierConfig', () => {
  it('rejects empty supplier_code', async () => {
    const db = makeAppDb({ rows: new Map() });
    const result = await saveSupplierConfig(db, {
      supplier_code: '',
      config: {},
    });
    expect(result.success).toBe(false);
  });

  it('rejects non-object config', async () => {
    const db = makeAppDb({ rows: new Map() });
    const result = await saveSupplierConfig(db, {
      supplier_code: 'SUPP001',
      config: [] as any,
    });
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/JSON object/);
  });

  it('inserts when no existing row', async () => {
    const state: MockState = { rows: new Map() };
    const db = makeAppDb(state);
    const result = await saveSupplierConfig(db, {
      supplier_code: 'SUPP001',
      config: { auto_process: true },
    });
    expect(result.success).toBe(true);
    expect(state.rows.has('SUPP001')).toBe(true);
  });

  it('updates existing row', async () => {
    const state: MockState = {
      rows: new Map([
        [
          'SUPP001',
          {
            supplier_code: 'SUPP001',
            config_json: JSON.stringify({ auto_process: false }),
            updated_at: new Date(),
          },
        ],
      ]),
    };
    const db = makeAppDb(state);
    const result = await saveSupplierConfig(db, {
      supplier_code: 'SUPP001',
      config: { auto_process: true, statement_template_hint: 'long' },
    });
    expect(result.success).toBe(true);
    const stored = JSON.parse(state.rows.get('SUPP001')!.config_json!);
    expect(stored.auto_process).toBe(true);
    expect(stored.statement_template_hint).toBe('long');
  });
});
