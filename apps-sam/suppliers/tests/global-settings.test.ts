import { describe, it, expect } from 'vitest';
import {
  getGlobalSupplierSettings,
  updateGlobalSupplierSettings,
  SUPPLIER_SETTINGS_DEFAULTS,
} from '../src/services/global-settings.js';

interface SettingsRow {
  id: number;
  key: string;
  value: string;
}

interface MockState {
  rows: SettingsRow[];
}

function makeAppDb(state: MockState): any {
  const db: any = (table: string) => {
    if (table !== 'settings') throw new Error(`Unexpected table: ${table}`);
    let conds: Record<string, unknown> = {};
    let likeKey: string | null = null;
    const matches = () =>
      state.rows.filter(
        (r) =>
          Object.entries(conds).every(([k, v]) => (r as any)[k] === v) &&
          (!likeKey || r.key.startsWith(likeKey.replace(/%/g, ''))),
      );
    const builder: any = {
      where: (col: any, op?: any, val?: any) => {
        if (typeof col === 'string') {
          if (op === 'like' && typeof val === 'string') {
            likeKey = val;
          } else if (op !== undefined && val !== undefined) {
            // unsupported op
            conds[col] = op;
          } else {
            conds[col] = op;
          }
        } else {
          Object.assign(conds, col);
        }
        return builder;
      },
      first: async () => matches()[0],
      select: async (..._cols: string[]) => matches(),
      update: async (data: Record<string, unknown>) => {
        let count = 0;
        for (const r of matches()) {
          Object.assign(r, data);
          count++;
        }
        return count;
      },
      insert: async (row: Record<string, unknown>) => {
        const id = (state.rows[state.rows.length - 1]?.id ?? 0) + 1;
        state.rows.push({
          id,
          key: String(row.key ?? ''),
          value: String(row.value ?? ''),
        });
        return [id];
      },
    };
    return builder;
  };
  db.fn = { now: () => '__NOW__' };
  return db;
}

// ---------------------------------------------------------------------
// getGlobalSupplierSettings
// ---------------------------------------------------------------------

describe('getGlobalSupplierSettings', () => {
  it('returns full default set when no rows stored', async () => {
    const state: MockState = { rows: [] };
    const result = await getGlobalSupplierSettings(makeAppDb(state));
    expect(result.success).toBe(true);
    const keys = Object.keys(result.settings);
    expect(keys.length).toBe(Object.keys(SUPPLIER_SETTINGS_DEFAULTS).length);
    expect(result.settings.acknowledgment_delay_minutes?.value).toBe('0');
    expect(result.settings.processing_sla_hours?.value).toBe('24');
    expect(result.settings.send_acknowledgement?.value).toBe('true');
  });

  it('overlays stored values on the defaults', async () => {
    const state: MockState = {
      rows: [
        { id: 1, key: 'global:processing_sla_hours', value: '8' },
        { id: 2, key: 'global:send_acknowledgement', value: 'false' },
      ],
    };
    const result = await getGlobalSupplierSettings(makeAppDb(state));
    expect(result.settings.processing_sla_hours?.value).toBe('8');
    expect(result.settings.send_acknowledgement?.value).toBe('false');
    // Unset key still falls back to default
    expect(result.settings.query_response_days?.value).toBe('7');
  });

  it('always returns the description from defaults', async () => {
    const state: MockState = {
      rows: [{ id: 1, key: 'global:processing_sla_hours', value: '99' }],
    };
    const result = await getGlobalSupplierSettings(makeAppDb(state));
    expect(result.settings.processing_sla_hours?.description).toMatch(/Target time/);
  });

  it('ignores rows with non-global prefix', async () => {
    const state: MockState = {
      rows: [
        { id: 1, key: 'something_else', value: 'x' },
      ],
    };
    const result = await getGlobalSupplierSettings(makeAppDb(state));
    expect(result.settings.processing_sla_hours?.value).toBe('24');
  });
});

// ---------------------------------------------------------------------
// updateGlobalSupplierSettings
// ---------------------------------------------------------------------

describe('updateGlobalSupplierSettings', () => {
  it('inserts new keys', async () => {
    const state: MockState = { rows: [] };
    const result = await updateGlobalSupplierSettings(makeAppDb(state), {
      processing_sla_hours: '8',
    });
    expect(result.success).toBe(true);
    expect(state.rows[0]?.key).toBe('global:processing_sla_hours');
    expect(state.rows[0]?.value).toBe('8');
  });

  it('updates existing keys', async () => {
    const state: MockState = {
      rows: [
        { id: 1, key: 'global:processing_sla_hours', value: '24' },
      ],
    };
    await updateGlobalSupplierSettings(makeAppDb(state), {
      processing_sla_hours: '12',
    });
    expect(state.rows).toHaveLength(1);
    expect(state.rows[0]?.value).toBe('12');
  });

  it('coerces non-string values to strings', async () => {
    const state: MockState = { rows: [] };
    await updateGlobalSupplierSettings(makeAppDb(state), {
      processing_sla_hours: 8,
      send_acknowledgement: false,
    });
    const sla = state.rows.find(
      (r) => r.key === 'global:processing_sla_hours',
    );
    const ack = state.rows.find(
      (r) => r.key === 'global:send_acknowledgement',
    );
    expect(sla?.value).toBe('8');
    expect(ack?.value).toBe('false');
  });

  it('rejects follow_up_reminder_days <= query_response_days', async () => {
    const state: MockState = { rows: [] };
    const result = await updateGlobalSupplierSettings(makeAppDb(state), {
      follow_up_reminder_days: '5',
      query_response_days: '7',
    });
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/must be greater/);
    expect(state.rows).toHaveLength(0);
  });

  it('compares against stored value when only one is supplied', async () => {
    const state: MockState = {
      rows: [
        { id: 1, key: 'global:query_response_days', value: '10' },
      ],
    };
    const result = await updateGlobalSupplierSettings(makeAppDb(state), {
      follow_up_reminder_days: '7',
    });
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/10 days/);
  });

  it('accepts valid follow-up > response window', async () => {
    const state: MockState = { rows: [] };
    const result = await updateGlobalSupplierSettings(makeAppDb(state), {
      follow_up_reminder_days: '14',
      query_response_days: '7',
    });
    expect(result.success).toBe(true);
  });

  it('skips unknown keys silently', async () => {
    const state: MockState = { rows: [] };
    const result = await updateGlobalSupplierSettings(makeAppDb(state), {
      unknown_key_xyz: 'value',
      processing_sla_hours: '8',
    });
    expect(result.success).toBe(true);
    expect(state.rows).toHaveLength(1);
    expect(state.rows[0]?.key).toBe('global:processing_sla_hours');
  });
});
