import { describe, it, expect } from 'vitest';
import {
  listContacts,
  addContact,
  deleteContact,
} from '../src/services/contacts.js';

interface MockState {
  rows: Array<Record<string, unknown> & { id: number }>;
  nextId: number;
}

function makeAppDb(state: MockState): any {
  const db: any = (table: string) => {
    if (table !== 'supplier_contacts_ext') {
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
        const fullRow = { id, ...row, updated_at: new Date() };
        state.rows.push(fullRow as any);
        return {
          returning: () => Promise.resolve([{ id }]),
        };
      },
    };
    return builder;
  };
  return db;
}

describe('addContact', () => {
  it('rejects empty supplier_code', async () => {
    const db = makeAppDb({ rows: [], nextId: 1 });
    const result = await addContact(db, {
      supplier_code: '',
      contact_email: 'x@y.com',
    });
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/required/);
  });

  it('rejects empty contact_email', async () => {
    const db = makeAppDb({ rows: [], nextId: 1 });
    const result = await addContact(db, {
      supplier_code: 'SUPP001',
      contact_email: '',
    });
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/required/);
  });

  it('rejects malformed email', async () => {
    const db = makeAppDb({ rows: [], nextId: 1 });
    const result = await addContact(db, {
      supplier_code: 'SUPP001',
      contact_email: 'not-an-email',
    });
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/valid email/);
  });

  it('inserts a valid contact and returns it', async () => {
    const state: MockState = { rows: [], nextId: 1 };
    const db = makeAppDb(state);
    const result = await addContact(db, {
      supplier_code: 'SUPP001',
      contact_email: 'ap@acme.com',
      contact_name: 'Jane Doe',
      contact_role: 'Accounts Payable',
    });
    expect(result.success).toBe(true);
    expect(result.contact?.id).toBe(1);
    expect(result.contact?.contact_email).toBe('ap@acme.com');
    expect(state.rows).toHaveLength(1);
  });
});

describe('listContacts', () => {
  it('returns contacts for the supplier', async () => {
    const state: MockState = {
      rows: [
        {
          id: 1,
          supplier_code: 'SUPP001',
          contact_email: 'a@x.com',
          contact_name: 'A',
          contact_role: 'AP',
          updated_at: '2026-04-15T10:00:00Z',
        },
        {
          id: 2,
          supplier_code: 'SUPP001',
          contact_email: 'b@x.com',
          contact_name: 'B',
          contact_role: 'Manager',
          updated_at: '2026-04-15T10:00:00Z',
        },
        {
          id: 3,
          supplier_code: 'SUPP002',
          contact_email: 'c@y.com',
          contact_name: 'C',
          contact_role: 'AP',
          updated_at: '2026-04-15T10:00:00Z',
        },
      ],
      nextId: 4,
    };
    const db = makeAppDb(state);
    const result = await listContacts(db, 'SUPP001');
    expect(result.count).toBe(2);
    expect(result.contacts.every((c) => c.supplier_code === 'SUPP001')).toBe(true);
  });

  it('returns empty when no contacts', async () => {
    const db = makeAppDb({ rows: [], nextId: 1 });
    const result = await listContacts(db, 'SUPP001');
    expect(result.count).toBe(0);
    expect(result.contacts).toEqual([]);
  });
});

describe('deleteContact', () => {
  it('returns success when contact exists', async () => {
    const state: MockState = {
      rows: [{ id: 1, supplier_code: 'SUPP001' } as any],
      nextId: 2,
    };
    const db = makeAppDb(state);
    const result = await deleteContact(db, 1);
    expect(result.success).toBe(true);
    expect(state.rows).toHaveLength(0);
  });

  it('returns error when contact not found', async () => {
    const db = makeAppDb({ rows: [], nextId: 1 });
    const result = await deleteContact(db, 999);
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/not found/);
  });
});
