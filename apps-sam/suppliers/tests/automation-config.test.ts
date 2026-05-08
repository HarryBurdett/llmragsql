import { describe, it, expect } from 'vitest';
import {
  getAutomationConfig,
  saveAutomationConfig,
} from '../src/services/automation-config.js';

interface MockState {
  rows: Map<
    string,
    {
      supplier_code: string;
      auto_process: boolean;
      frequency: string;
      matching_rules_json: string;
      updated_at: Date;
    }
  >;
}

function makeAppDb(state: MockState): any {
  const db: any = (table: string) => {
    if (table !== 'supplier_automation_config') {
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
            auto_process: Boolean(patch.auto_process ?? existing.auto_process),
            frequency: String(patch.frequency ?? existing.frequency),
            matching_rules_json: String(
              patch.matching_rules_json ?? existing.matching_rules_json,
            ),
            updated_at: new Date(),
          });
          return 1;
        }
        return 0;
      },
      insert: async (row: Record<string, unknown>) => {
        state.rows.set(String(row.supplier_code), {
          supplier_code: String(row.supplier_code),
          auto_process: Boolean(row.auto_process),
          frequency: String(row.frequency),
          matching_rules_json: String(row.matching_rules_json ?? '{}'),
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

describe('getAutomationConfig', () => {
  it('returns defaults when no row exists', async () => {
    const db = makeAppDb({ rows: new Map() });
    const result = await getAutomationConfig(db, 'SUPP001');
    expect(result.success).toBe(true);
    expect(result.config?.auto_process).toBe(false);
    expect(result.config?.frequency).toBe('on_demand');
    expect(result.config?.matching_rules).toEqual({});
  });

  it('returns the stored config when row exists', async () => {
    const state: MockState = {
      rows: new Map([
        [
          'SUPP001',
          {
            supplier_code: 'SUPP001',
            auto_process: true,
            frequency: 'monthly',
            matching_rules_json: JSON.stringify({ ref_pattern: 'INV.*' }),
            updated_at: new Date(),
          },
        ],
      ]),
    };
    const db = makeAppDb(state);
    const result = await getAutomationConfig(db, 'SUPP001');
    expect(result.config?.auto_process).toBe(true);
    expect(result.config?.frequency).toBe('monthly');
    expect(result.config?.matching_rules).toEqual({ ref_pattern: 'INV.*' });
  });

  it("falls back to 'on_demand' when stored frequency is invalid", async () => {
    const state: MockState = {
      rows: new Map([
        [
          'SUPP001',
          {
            supplier_code: 'SUPP001',
            auto_process: false,
            frequency: 'unknown',
            matching_rules_json: '{}',
            updated_at: new Date(),
          },
        ],
      ]),
    };
    const db = makeAppDb(state);
    const result = await getAutomationConfig(db, 'SUPP001');
    expect(result.config?.frequency).toBe('on_demand');
  });

  it('rejects empty supplier_code', async () => {
    const db = makeAppDb({ rows: new Map() });
    const result = await getAutomationConfig(db, '');
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/required/);
  });
});

describe('saveAutomationConfig', () => {
  it('rejects empty supplier_code', async () => {
    const db = makeAppDb({ rows: new Map() });
    const result = await saveAutomationConfig(db, {
      supplier_code: '',
    });
    expect(result.success).toBe(false);
  });

  it('rejects invalid frequency', async () => {
    const db = makeAppDb({ rows: new Map() });
    const result = await saveAutomationConfig(db, {
      supplier_code: 'SUPP001',
      frequency: 'every-thursday',
    });
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/frequency must/);
  });

  it('rejects matching_rules array', async () => {
    const db = makeAppDb({ rows: new Map() });
    const result = await saveAutomationConfig(db, {
      supplier_code: 'SUPP001',
      matching_rules: [] as any,
    });
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/JSON object/);
  });

  it('inserts new row with provided fields', async () => {
    const state: MockState = { rows: new Map() };
    const db = makeAppDb(state);
    const result = await saveAutomationConfig(db, {
      supplier_code: 'SUPP001',
      auto_process: true,
      frequency: 'weekly',
    });
    expect(result.success).toBe(true);
    expect(state.rows.has('SUPP001')).toBe(true);
    expect(state.rows.get('SUPP001')?.auto_process).toBe(true);
  });

  it('partial-merges existing config', async () => {
    const state: MockState = {
      rows: new Map([
        [
          'SUPP001',
          {
            supplier_code: 'SUPP001',
            auto_process: false,
            frequency: 'monthly',
            matching_rules_json: JSON.stringify({ existing: 'rule' }),
            updated_at: new Date(),
          },
        ],
      ]),
    };
    const db = makeAppDb(state);
    const result = await saveAutomationConfig(db, {
      supplier_code: 'SUPP001',
      auto_process: true,
    });
    expect(result.success).toBe(true);
    // auto_process updated; frequency + matching_rules preserved
    expect(result.config?.auto_process).toBe(true);
    expect(result.config?.frequency).toBe('monthly');
    expect(result.config?.matching_rules).toEqual({ existing: 'rule' });
  });
});
