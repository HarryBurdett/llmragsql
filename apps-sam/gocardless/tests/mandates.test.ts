import { describe, it, expect } from 'vitest';
import {
  listMandates,
  listUnlinkedMandates,
} from '../src/services/mandates.js';

interface MandateRow {
  id: number;
  mandate_id: string;
  opera_account: string;
  opera_name: string;
  gocardless_name: string;
  gocardless_customer_id: string;
  mandate_status: string;
  scheme: string;
  email: string;
  created_at: string;
  updated_at: string;
}

interface MockState {
  rows: MandateRow[];
}

function makeAppDb(state: MockState): any {
  const db: any = (table: string) => {
    if (table !== 'gocardless_mandates') {
      throw new Error(`Unexpected table: ${table}`);
    }
    let conds: Record<string, unknown> = {};
    const builder: any = {
      where: (cond: Record<string, unknown>) => {
        Object.assign(conds, cond);
        return builder;
      },
      then: (cb: (rows: MandateRow[]) => unknown) => {
        const rows = state.rows.filter((r) =>
          Object.entries(conds).every(([k, v]) => (r as any)[k] === v),
        );
        return Promise.resolve(cb(rows));
      },
    };
    return builder;
  };
  db.fn = { now: () => new Date() };
  return db;
}

function emptyMandate(over: Partial<MandateRow> = {}): MandateRow {
  return {
    id: 1,
    mandate_id: 'MD_X',
    opera_account: 'CUST01',
    opera_name: 'Acme Ltd',
    gocardless_name: '',
    gocardless_customer_id: '',
    mandate_status: 'active',
    scheme: 'bacs',
    email: '',
    created_at: '2026-04-15',
    updated_at: '2026-04-15',
    ...over,
  };
}

describe('listMandates', () => {
  it('returns mandates sorted alphabetically by opera_name', async () => {
    const state: MockState = {
      rows: [
        emptyMandate({ id: 1, mandate_id: 'M1', opera_name: 'Beta' }),
        emptyMandate({ id: 2, mandate_id: 'M2', opera_name: 'Acme' }),
        emptyMandate({ id: 3, mandate_id: 'M3', opera_name: 'cATco' }),
      ],
    };
    const result = await listMandates(makeAppDb(state));
    expect(result.success).toBe(true);
    expect(result.count).toBe(3);
    expect(result.mandates[0]?.opera_name).toBe('Acme');
    expect(result.mandates[1]?.opera_name).toBe('Beta');
    expect(result.mandates[2]?.opera_name).toBe('cATco');
  });

  it('filters by status', async () => {
    const state: MockState = {
      rows: [
        emptyMandate({ id: 1, mandate_status: 'active' }),
        emptyMandate({ id: 2, mandate_status: 'cancelled' }),
      ],
    };
    const result = await listMandates(makeAppDb(state), { status: 'active' });
    expect(result.count).toBe(1);
    expect(result.mandates[0]?.mandate_status).toBe('active');
  });

  it('dedups __UNLINKED__ rows when a linked version of the same mandate_id exists', async () => {
    const state: MockState = {
      rows: [
        emptyMandate({
          id: 1,
          mandate_id: 'MD_DUP',
          opera_account: '__UNLINKED__',
          opera_name: 'Acme (raw)',
        }),
        emptyMandate({
          id: 2,
          mandate_id: 'MD_DUP',
          opera_account: 'CUST01',
          opera_name: 'Acme Ltd',
        }),
      ],
    };
    const result = await listMandates(makeAppDb(state));
    expect(result.count).toBe(1);
    expect(result.mandates[0]?.opera_account).toBe('CUST01');
  });

  it('keeps __UNLINKED__ rows when no linked version exists for that mandate_id', async () => {
    const state: MockState = {
      rows: [
        emptyMandate({
          id: 1,
          mandate_id: 'MD_LONELY',
          opera_account: '__UNLINKED__',
          opera_name: 'Lonely',
        }),
      ],
    };
    const result = await listMandates(makeAppDb(state));
    expect(result.count).toBe(1);
  });
});

describe('listUnlinkedMandates', () => {
  it('returns only __UNLINKED__ rows', async () => {
    const state: MockState = {
      rows: [
        emptyMandate({ id: 1, opera_account: 'CUST01' }),
        emptyMandate({
          id: 2,
          opera_account: '__UNLINKED__',
          opera_name: 'Bravo',
        }),
        emptyMandate({
          id: 3,
          opera_account: '__UNLINKED__',
          opera_name: 'Alpha',
        }),
      ],
    };
    const result = await listUnlinkedMandates(makeAppDb(state));
    expect(result.success).toBe(true);
    expect(result.count).toBe(2);
    // Sorted alphabetically by opera_name
    expect(result.mandates[0]?.opera_name).toBe('Alpha');
    expect(result.mandates[1]?.opera_name).toBe('Bravo');
  });
});
