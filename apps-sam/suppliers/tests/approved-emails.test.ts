import { describe, it, expect } from 'vitest';
import {
  listApprovedEmails,
  approveEmail,
  revokeEmail,
  isEmailApproved,
} from '../src/services/approved-emails.js';

interface MockState {
  rows: Array<Record<string, unknown> & { id: number }>;
  nextId: number;
}

function makeAppDb(state: MockState): any {
  const db: any = (table: string) => {
    if (table !== 'supplier_approved_emails') {
      throw new Error(`Unexpected table: ${table}`);
    }
    let filters: Record<string, unknown> = {};
    const builder: any = {
      where: (col: Record<string, unknown> | string, val?: unknown) => {
        if (typeof col === 'object') Object.assign(filters, col);
        else if (val !== undefined) filters[col] = val;
        return builder;
      },
      orderBy: () => builder,
      first: async () => {
        return state.rows.find((r) =>
          Object.keys(filters).every((k) => r[k] === filters[k]),
        );
      },
      then: (cb: (rows: unknown[]) => unknown) => {
        const rows = state.rows.filter((r) =>
          Object.keys(filters).every((k) => r[k] === filters[k]),
        );
        return Promise.resolve(cb(rows));
      },
      delete: async () => {
        const before = state.rows.length;
        state.rows = state.rows.filter(
          (r) => !Object.keys(filters).every((k) => r[k] === filters[k]),
        );
        return before - state.rows.length;
      },
      insert: (row: Record<string, unknown>) => {
        const id = state.nextId++;
        state.rows.push({ id, ...row, approved_at: new Date() } as any);
        return {
          returning: () => Promise.resolve([{ id }]),
        };
      },
    };
    return builder;
  };
  return db;
}

describe('approveEmail', () => {
  it('rejects invalid email format', async () => {
    const db = makeAppDb({ rows: [], nextId: 1 });
    const result = await approveEmail(db, {
      supplier_code: 'SUPP001',
      email_address: 'not-an-email',
    });
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/valid email/);
  });

  it('rejects missing fields', async () => {
    const db = makeAppDb({ rows: [], nextId: 1 });
    const result = await approveEmail(db, {
      supplier_code: '',
      email_address: 'a@b.com',
    });
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/required/);
  });

  it('inserts a new approval', async () => {
    const state: MockState = { rows: [], nextId: 1 };
    const db = makeAppDb(state);
    const result = await approveEmail(db, {
      supplier_code: 'SUPP001',
      email_address: 'ap@acme.com',
    });
    expect(result.success).toBe(true);
    expect(result.approved?.id).toBe(1);
    expect(state.rows).toHaveLength(1);
  });

  it('is idempotent — re-approving returns existing row', async () => {
    const state: MockState = {
      rows: [
        {
          id: 5,
          supplier_code: 'SUPP001',
          email_address: 'ap@acme.com',
          approved_at: new Date('2026-04-15'),
        } as any,
      ],
      nextId: 6,
    };
    const db = makeAppDb(state);
    const result = await approveEmail(db, {
      supplier_code: 'SUPP001',
      email_address: 'ap@acme.com',
    });
    expect(result.success).toBe(true);
    expect(result.approved?.id).toBe(5);
    expect(result.message).toMatch(/already approved/);
    expect(state.rows).toHaveLength(1);
  });
});

describe('listApprovedEmails', () => {
  it('returns approvals scoped to the supplier', async () => {
    const state: MockState = {
      rows: [
        {
          id: 1,
          supplier_code: 'SUPP001',
          email_address: 'a@x.com',
          approved_at: '2026-04-15T10:00:00Z',
        } as any,
        {
          id: 2,
          supplier_code: 'SUPP002',
          email_address: 'b@y.com',
          approved_at: '2026-04-15T10:00:00Z',
        } as any,
      ],
      nextId: 3,
    };
    const db = makeAppDb(state);
    const result = await listApprovedEmails(db, 'SUPP001');
    expect(result.count).toBe(1);
    expect(result.emails[0]?.email_address).toBe('a@x.com');
  });
});

describe('revokeEmail', () => {
  it('removes the record by id', async () => {
    const state: MockState = {
      rows: [{ id: 1, supplier_code: 'SUPP001', email_address: 'a@b.com' } as any],
      nextId: 2,
    };
    const db = makeAppDb(state);
    const result = await revokeEmail(db, 1);
    expect(result.success).toBe(true);
    expect(state.rows).toHaveLength(0);
  });

  it("returns 'not found' when id doesn't exist", async () => {
    const db = makeAppDb({ rows: [], nextId: 1 });
    const result = await revokeEmail(db, 999);
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/not found/);
  });
});

describe('isEmailApproved', () => {
  it('returns true when approval exists', async () => {
    const state: MockState = {
      rows: [
        {
          id: 1,
          supplier_code: 'SUPP001',
          email_address: 'ap@acme.com',
        } as any,
      ],
      nextId: 2,
    };
    const db = makeAppDb(state);
    const result = await isEmailApproved(db, 'SUPP001', 'ap@acme.com');
    expect(result).toBe(true);
  });

  it("returns false when no match", async () => {
    const db = makeAppDb({ rows: [], nextId: 1 });
    const result = await isEmailApproved(db, 'SUPP001', 'ap@acme.com');
    expect(result).toBe(false);
  });
});
