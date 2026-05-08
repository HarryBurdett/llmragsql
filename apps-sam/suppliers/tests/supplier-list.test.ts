import { describe, it, expect } from 'vitest';
import { listSuppliers, getSupplier } from '../src/services/supplier-list.js';

function makeMockOpera(rows: unknown[]): any {
  const db: any = () => ({});
  db.raw = async () => rows;
  return db;
}

describe('listSuppliers', () => {
  it('returns active suppliers excluding dormant by default', async () => {
    const db = makeMockOpera([
      {
        account: 'SUPP001',
        name: 'Acme Ltd',
        current_balance: 1500.5,
        dormant: 0,
        email: 'ap@acme.com',
        phone: '01234567890',
      },
      {
        account: 'SUPP002 ',
        name: ' Beta Suppliers ',
        current_balance: 0,
        dormant: 0,
        email: '',
        phone: '',
      },
    ]);
    const result = await listSuppliers(db);
    expect(result.success).toBe(true);
    expect(result.suppliers).toHaveLength(2);
    expect(result.suppliers[0]?.account).toBe('SUPP001');
    expect(result.suppliers[0]?.dormant).toBe(false);
    expect(result.suppliers[0]?.current_balance).toBe(1500.5);
    expect(result.suppliers[1]?.account).toBe('SUPP002');
    expect(result.suppliers[1]?.name).toBe('Beta Suppliers');
    expect(result.count).toBe(2);
  });

  it('marks dormant suppliers correctly when included', async () => {
    const db = makeMockOpera([
      {
        account: 'OLDSUPP',
        name: 'Retired',
        current_balance: 0,
        dormant: 1,
        email: '',
        phone: '',
      },
    ]);
    const result = await listSuppliers(db, { includeDormant: true });
    expect(result.suppliers[0]?.dormant).toBe(true);
  });

  it('returns empty list when no suppliers', async () => {
    const db = makeMockOpera([]);
    const result = await listSuppliers(db);
    expect(result.success).toBe(true);
    expect(result.count).toBe(0);
  });

  it('handles query errors gracefully', async () => {
    const db: any = {
      raw: async () => {
        throw new Error('connection broken');
      },
    };
    const result = await listSuppliers(db);
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/connection broken/);
  });
});

describe('getSupplier', () => {
  it('returns supplier when code matches', async () => {
    const db = makeMockOpera([
      {
        account: 'SUPP001',
        name: 'Acme Ltd',
        current_balance: 500,
        dormant: 0,
        email: 'ap@acme.com',
        phone: '0123',
        address: '1 Main St, , , , SW1A 1AA',
      },
    ]);
    const result = await getSupplier(db, 'SUPP001');
    expect(result.success).toBe(true);
    expect(result.supplier?.account).toBe('SUPP001');
    expect(result.supplier?.email).toBe('ap@acme.com');
    // Empty address segments collapsed
    expect(result.supplier?.address).toBe('1 Main St, SW1A 1AA');
  });

  it('returns 404-style response when supplier not found', async () => {
    const db = makeMockOpera([]);
    const result = await getSupplier(db, 'GHOST');
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/'GHOST' not found/);
  });
});
