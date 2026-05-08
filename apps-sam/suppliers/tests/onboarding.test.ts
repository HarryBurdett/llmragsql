import { describe, it, expect } from 'vitest';
import {
  getOnboardingState,
  listOnboardingStates,
  updateOnboardingState,
} from '../src/services/onboarding.js';

interface MockState {
  rows: Map<
    string,
    {
      supplier_code: string;
      stage: string;
      notes: string;
      updated_at: Date;
    }
  >;
}

function makeAppDb(state: MockState): any {
  const db: any = (table: string) => {
    if (table !== 'supplier_onboarding') {
      throw new Error(`Unexpected table: ${table}`);
    }
    let filters: Record<string, unknown> = {};
    const builder: any = {
      where: (col: Record<string, unknown>) => {
        Object.assign(filters, col);
        return builder;
      },
      orderBy: () => builder,
      first: async () => {
        if (filters.supplier_code) {
          return state.rows.get(String(filters.supplier_code)) ?? null;
        }
        return null;
      },
      then: (cb: (rows: unknown[]) => unknown) => {
        const rows = [...state.rows.values()].filter((r) =>
          Object.keys(filters).every((k) => (r as any)[k] === filters[k]),
        );
        return Promise.resolve(cb(rows));
      },
      update: async (patch: Record<string, unknown>) => {
        const code = String(filters.supplier_code);
        const existing = state.rows.get(code);
        if (existing) {
          state.rows.set(code, {
            ...existing,
            stage: String(patch.stage ?? existing.stage),
            notes: String(patch.notes ?? existing.notes),
            updated_at: new Date(),
          });
          return 1;
        }
        return 0;
      },
      insert: async (row: Record<string, unknown>) => {
        state.rows.set(String(row.supplier_code), {
          supplier_code: String(row.supplier_code),
          stage: String(row.stage),
          notes: String(row.notes ?? ''),
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

describe('getOnboardingState', () => {
  it("returns 'discovered' default when no row exists", async () => {
    const db = makeAppDb({ rows: new Map() });
    const result = await getOnboardingState(db, 'SUPP001');
    expect(result.success).toBe(true);
    expect(result.state?.stage).toBe('discovered');
    expect(result.state?.notes).toBe('');
  });

  it('returns the stored state', async () => {
    const state: MockState = {
      rows: new Map([
        [
          'SUPP001',
          {
            supplier_code: 'SUPP001',
            stage: 'live',
            notes: 'Fully automated since April',
            updated_at: new Date(),
          },
        ],
      ]),
    };
    const db = makeAppDb(state);
    const result = await getOnboardingState(db, 'SUPP001');
    expect(result.state?.stage).toBe('live');
    expect(result.state?.notes).toMatch(/Fully automated/);
  });

  it("falls back to 'discovered' when stored stage is invalid", async () => {
    const state: MockState = {
      rows: new Map([
        [
          'SUPP001',
          {
            supplier_code: 'SUPP001',
            stage: 'unknown-stage',
            notes: '',
            updated_at: new Date(),
          },
        ],
      ]),
    };
    const db = makeAppDb(state);
    const result = await getOnboardingState(db, 'SUPP001');
    expect(result.state?.stage).toBe('discovered');
  });

  it('rejects empty supplier_code', async () => {
    const db = makeAppDb({ rows: new Map() });
    const result = await getOnboardingState(db, '');
    expect(result.success).toBe(false);
  });
});

describe('listOnboardingStates', () => {
  it('returns all when no stage filter', async () => {
    const state: MockState = {
      rows: new Map([
        [
          'SUPP001',
          {
            supplier_code: 'SUPP001',
            stage: 'live',
            notes: '',
            updated_at: new Date(),
          },
        ],
        [
          'SUPP002',
          {
            supplier_code: 'SUPP002',
            stage: 'testing',
            notes: '',
            updated_at: new Date(),
          },
        ],
      ]),
    };
    const db = makeAppDb(state);
    const result = await listOnboardingStates(db);
    expect(result.count).toBe(2);
  });

  it('filters by stage', async () => {
    const state: MockState = {
      rows: new Map([
        [
          'SUPP001',
          {
            supplier_code: 'SUPP001',
            stage: 'live',
            notes: '',
            updated_at: new Date(),
          },
        ],
        [
          'SUPP002',
          {
            supplier_code: 'SUPP002',
            stage: 'testing',
            notes: '',
            updated_at: new Date(),
          },
        ],
      ]),
    };
    const db = makeAppDb(state);
    const result = await listOnboardingStates(db, { stage: 'live' });
    expect(result.count).toBe(1);
    expect(result.states[0]?.supplier_code).toBe('SUPP001');
  });

  it('rejects invalid stage', async () => {
    const db = makeAppDb({ rows: new Map() });
    const result = await listOnboardingStates(db, { stage: 'bogus' as any });
    expect(result.success).toBe(false);
  });
});

describe('updateOnboardingState', () => {
  it('rejects invalid stage', async () => {
    const db = makeAppDb({ rows: new Map() });
    const result = await updateOnboardingState(db, {
      supplier_code: 'SUPP001',
      stage: 'invalid',
    });
    expect(result.success).toBe(false);
  });

  it('inserts new row with stage and notes', async () => {
    const state: MockState = { rows: new Map() };
    const db = makeAppDb(state);
    const result = await updateOnboardingState(db, {
      supplier_code: 'SUPP001',
      stage: 'configured',
      notes: 'Set up automation rules',
    });
    expect(result.success).toBe(true);
    expect(state.rows.get('SUPP001')?.stage).toBe('configured');
  });

  it('partial-merges existing state', async () => {
    const state: MockState = {
      rows: new Map([
        [
          'SUPP001',
          {
            supplier_code: 'SUPP001',
            stage: 'live',
            notes: 'Existing notes',
            updated_at: new Date(),
          },
        ],
      ]),
    };
    const db = makeAppDb(state);
    const result = await updateOnboardingState(db, {
      supplier_code: 'SUPP001',
      stage: 'paused',
    });
    expect(result.state?.stage).toBe('paused');
    expect(result.state?.notes).toBe('Existing notes'); // preserved
  });
});
