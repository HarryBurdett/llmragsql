/**
 * Tests for vatDiagnostic.
 */
import { describe, it, expect } from 'vitest';
import { vatDiagnostic } from '../src/services/vat-diagnostic.js';

function makeMockKnex(canned: {
  zvtran?: Record<string, unknown> | Error;
  nvat?: Record<string, unknown> | Error;
  ztax?: Record<string, unknown> | Error;
  ntran?: Record<string, unknown> | Error;
}): any {
  const db: any = () => ({});
  db.raw = async (sql: string) => {
    let key: keyof typeof canned;
    if (sql.includes('zvtran')) key = 'zvtran';
    else if (sql.includes('nvat')) key = 'nvat';
    else if (sql.includes('ztax')) key = 'ztax';
    else if (sql.includes('ntran')) key = 'ntran';
    else return [];

    const v = canned[key];
    if (v instanceof Error) throw v;
    if (v) return [v];
    return [];
  };
  return db;
}

describe('vatDiagnostic', () => {
  it('returns all table summaries when queries succeed', async () => {
    const db = makeMockKnex({
      zvtran: { total_rows: 100, uncommitted: 5, committed: 95, total_vat: 12345.67 },
      nvat: { total_rows: 200, total_vat: 22345.67, vat_types: 2 },
      ztax: { total_codes: 10 },
      ntran: { current_year: 2026, total_rows: 5000 },
    });

    const result = await vatDiagnostic(db);

    expect(result.tables.zvtran).toMatchObject({ total_rows: 100 });
    expect(result.tables.nvat).toMatchObject({ total_rows: 200 });
    expect(result.tables.ztax).toMatchObject({ total_codes: 10 });
    expect(result.tables.ntran).toMatchObject({ current_year: 2026 });
  });

  it('isolates per-table errors — one failure does not break others', async () => {
    const db = makeMockKnex({
      zvtran: new Error('zvtran missing'),
      nvat: { total_rows: 200 },
      ztax: { total_codes: 10 },
      ntran: { current_year: 2026 },
    });

    const result = await vatDiagnostic(db);

    expect(result.tables.zvtran).toMatchObject({ error: 'zvtran missing' });
    expect(result.tables.nvat).toMatchObject({ total_rows: 200 });
    expect(result.tables.ztax).toMatchObject({ total_codes: 10 });
    expect(result.tables.ntran).toMatchObject({ current_year: 2026 });
  });

  it('returns "no data" placeholder when query returns empty array', async () => {
    const db: any = () => ({});
    db.raw = async () => [];

    const result = await vatDiagnostic(db);

    expect(result.tables.zvtran).toMatchObject({ error: 'no data' });
    expect(result.tables.nvat).toMatchObject({ error: 'no data' });
    expect(result.tables.ztax).toMatchObject({ error: 'no data' });
    expect(result.tables.ntran).toMatchObject({ error: 'no data' });
  });
});
