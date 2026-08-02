/**
 * One-off: list the transactions making up the difference between the
 * VAT control nominal (ntran) and the VAT files (zvtran + nvat) for
 * Cloudsis (Opera SE Company C) from 2026-01-01 onwards.
 *
 * Run:
 *   cd /Users/maccb/llmragsql/apps-sam
 *   node_modules/.bin/tsx standalone-runner/src/vat-control-reconcile.ts
 */
import { readFileSync, writeFileSync } from 'node:fs';
import knex, { Knex } from 'knex';

const CUTOFF = '2026-01-01';
const COMPANY_DB = 'Opera3SECompany00C';
const OUT_DIR = '/tmp';

function loadDb(): Knex {
  const recs = JSON.parse(readFileSync('/Users/maccb/llmragsql/systems.json', 'utf-8'));
  const d = recs.find((r: any) => r.is_default).database;
  return knex({
    client: 'mssql',
    connection: {
      host: d.server,
      port: parseInt(d.port, 10),
      user: d.username,
      password: d.password,
      database: COMPANY_DB,
      options: { encrypt: false, trustServerCertificate: true },
    },
    pool: { min: 0, max: 5 },
  });
}

const r2 = (n: number) => Math.round(n * 100) / 100;
const fmtDate = (v: any) => {
  if (!v) return '';
  const d = v instanceof Date ? v : new Date(v);
  return isNaN(d.getTime()) ? String(v) : d.toISOString().slice(0, 10);
};
const toCsv = (rows: Record<string, any>[]) => {
  if (rows.length === 0) return '';
  const cols = Object.keys(rows[0]);
  const esc = (v: any) => {
    if (v === null || v === undefined) return '';
    const s = String(v);
    return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
  };
  return [cols.join(','), ...rows.map((r) => cols.map((c) => esc(r[c])).join(','))].join('\n');
};

async function main() {
  const db = loadDb();
  try {
    const ztaxRows = (await db.raw(`
      SELECT DISTINCT RTRIM(tx_nominal) AS nominal
      FROM ztax WITH (NOLOCK)
      WHERE tx_ctrytyp = 'H' AND tx_nominal IS NOT NULL AND RTRIM(tx_nominal) <> ''
    `)) as Array<{ nominal: string }>;
    const vatNominals = ztaxRows.map((r) => r.nominal);
    console.log(`VAT control nominals: ${vatNominals.join(', ')}`);

    const ntranRows = (await db.raw(
      `
      SELECT RTRIM(nt_acnt) AS nt_acnt, nt_entr, nt_year, nt_period, nt_value,
             RTRIM(nt_ref) AS nt_ref, RTRIM(nt_trnref) AS nt_trnref,
             RTRIM(nt_posttyp) AS nt_posttyp, nt_jrnl, nt_rvrse,
             RTRIM(nt_inp) AS nt_inp, RTRIM(nt_cmnt) AS nt_cmnt
      FROM ntran WITH (NOLOCK)
      WHERE nt_acnt IN (${vatNominals.map(() => '?').join(',')})
        AND nt_entr >= ?
      ORDER BY nt_entr, nt_jrnl
      `,
      [...vatNominals, CUTOFF],
    )) as Array<any>;
    console.log(`ntran rows (VAT control, >=${CUTOFF}): ${ntranRows.length}`);

    const zvtranRows = (await db.raw(
      `
      SELECT va_taxdate, va_trdate, RTRIM(va_trref) AS va_trref, RTRIM(va_trtype) AS va_trtype,
             RTRIM(va_account) AS va_account, RTRIM(va_laccnt) AS va_laccnt,
             RTRIM(va_anvat) AS va_anvat,
             RTRIM(va_vattype) AS va_vattype, va_vatval, va_trvalue, va_vatrate,
             va_box1, va_box4, va_done, va_reverse
      FROM zvtran WITH (NOLOCK)
      WHERE va_taxdate >= ?
      ORDER BY va_taxdate
      `,
      [CUTOFF],
    )) as Array<any>;
    console.log(`zvtran rows (>=${CUTOFF}): ${zvtranRows.length}`);

    const nvatRows = (await db.raw(
      `
      SELECT RTRIM(nv_acnt) AS nv_acnt, nv_taxdate, nv_value, nv_vatval,
             RTRIM(nv_vatcode) AS nv_vatcode, RTRIM(nv_vattype) AS nv_vattype,
             RTRIM(nv_ref) AS nv_ref, RTRIM(nv_comment) AS nv_comment
      FROM nvat WITH (NOLOCK)
      WHERE nv_taxdate >= ?
      ORDER BY nv_taxdate
      `,
      [CUTOFF],
    )) as Array<any>;
    console.log(`nvat rows (>=${CUTOFF}): ${nvatRows.length}`);

    const ntranSum = ntranRows.reduce((s, r) => s + Number(r.nt_value || 0), 0);
    const zvOut = zvtranRows.filter((r) => (r.va_vattype || '').trim() === 'S')
      .reduce((s, r) => s + Number(r.va_vatval || 0), 0);
    const zvIn = zvtranRows.filter((r) => (r.va_vattype || '').trim() === 'P')
      .reduce((s, r) => s + Number(r.va_vatval || 0), 0);
    const nvOut = nvatRows.filter((r) => (r.nv_vattype || '').trim() === 'S')
      .reduce((s, r) => s + Number(r.nv_vatval || 0), 0);
    const nvIn = nvatRows.filter((r) => (r.nv_vattype || '').trim() === 'P')
      .reduce((s, r) => s + Number(r.nv_vatval || 0), 0);

    const ntranLiability = r2(-ntranSum);
    const zvNet = r2(zvOut - zvIn);
    const nvNet = r2(nvOut - nvIn);
    const vatFilesLiability = r2(zvNet + nvNet);
    const variance = r2(ntranLiability - vatFilesLiability);

    console.log('');
    console.log('=== SUMMARY ===');
    console.log(`ntran VAT control: sum(nt_value)=${r2(ntranSum).toFixed(2)}  liability=-sum=${ntranLiability.toFixed(2)}`);
    console.log(`zvtran: output=${r2(zvOut).toFixed(2)}  input=${r2(zvIn).toFixed(2)}  net=${zvNet.toFixed(2)}`);
    console.log(`nvat:   output=${r2(nvOut).toFixed(2)}  input=${r2(nvIn).toFixed(2)}  net=${nvNet.toFixed(2)}`);
    console.log(`VAT files liability (zv+nv): ${vatFilesLiability.toFixed(2)}`);
    console.log(`VARIANCE (ntran-liability − VAT-files-liability): ${variance.toFixed(2)}`);
    console.log('');

    const byPosttyp = new Map<string, { count: number; sum: number }>();
    for (const r of ntranRows) {
      const k = (r.nt_posttyp || '').trim() || '(blank)';
      const cur = byPosttyp.get(k) ?? { count: 0, sum: 0 };
      cur.count++;
      cur.sum += Number(r.nt_value || 0);
      byPosttyp.set(k, cur);
    }
    console.log('ntran VAT control by nt_posttyp:');
    for (const [k, v] of [...byPosttyp.entries()].sort((a, b) => Math.abs(b[1].sum) - Math.abs(a[1].sum))) {
      console.log(`  ${k.padEnd(8)} count=${String(v.count).padStart(5)}  sum=${r2(v.sum).toFixed(2)}`);
    }
    console.log('');

    const zvBuckets = new Map<string, { count: number; sum: number }>();
    for (const r of zvtranRows) {
      const k = `${(r.va_vattype || '').trim()}/done=${r.va_done ? 1 : 0}`;
      const cur = zvBuckets.get(k) ?? { count: 0, sum: 0 };
      cur.count++;
      cur.sum += Number(r.va_vatval || 0);
      zvBuckets.set(k, cur);
    }
    console.log('zvtran by vattype/done:');
    for (const [k, v] of zvBuckets) console.log(`  ${k.padEnd(15)} count=${v.count}  sum_vatval=${r2(v.sum).toFixed(2)}`);
    console.log('');

    const nvBuckets = new Map<string, { count: number; sum: number }>();
    for (const r of nvatRows) {
      const k = (r.nv_vattype || '').trim();
      const cur = nvBuckets.get(k) ?? { count: 0, sum: 0 };
      cur.count++;
      cur.sum += Number(r.nv_vatval || 0);
      nvBuckets.set(k, cur);
    }
    console.log('nvat by vattype:');
    for (const [k, v] of nvBuckets) console.log(`  ${k.padEnd(4)} count=${v.count}  sum_vatval=${r2(v.sum).toFixed(2)}`);
    console.log('');

    // Match ntran ↔ zvtran by (taxdate ≈ entr) AND |vatval| ≈ |value|.
    // No journal/unique key exists in zvtran on this schema.
    const dayKey = (d: any) => fmtDate(d);
    const zvByDate = new Map<string, number[]>();
    zvtranRows.forEach((r, i) => {
      const k = dayKey(r.va_taxdate);
      if (!zvByDate.has(k)) zvByDate.set(k, []);
      zvByDate.get(k)!.push(i);
    });
    const usedZv = new Set<number>();
    const ntranEnriched = ntranRows.map((nt) => {
      const date = dayKey(nt.nt_entr);
      const val = Number(nt.nt_value || 0);
      let match: any = null;
      const candIdxs = zvByDate.get(date) ?? [];
      for (const ci of candIdxs) {
        if (usedZv.has(ci)) continue;
        const c = zvtranRows[ci];
        if (Math.abs(Math.abs(val) - Math.abs(Number(c.va_vatval || 0))) < 0.01) {
          match = c;
          usedZv.add(ci);
          break;
        }
      }
      return {
        nt_acnt: nt.nt_acnt,
        nt_entr: fmtDate(nt.nt_entr),
        nt_year: nt.nt_year,
        nt_period: nt.nt_period,
        nt_jrnl: nt.nt_jrnl,
        nt_posttyp: nt.nt_posttyp,
        nt_ref: nt.nt_ref,
        nt_trnref: nt.nt_trnref,
        nt_value: r2(val),
        nt_rvrse: nt.nt_rvrse,
        nt_inp: nt.nt_inp,
        match_status: match ? 'MATCHED' : 'UNMATCHED',
        matched_va_trref: match?.va_trref ?? '',
        matched_va_vatval: match ? r2(Number(match.va_vatval || 0)) : '',
        matched_va_vattype: match ? (match.va_vattype || '').trim() : '',
        matched_va_done: match ? match.va_done : '',
      };
    });

    const unmatchedZv = zvtranRows
      .map((r, i) => ({ r, i }))
      .filter(({ i }) => !usedZv.has(i))
      .map(({ r }) => ({
        va_taxdate: fmtDate(r.va_taxdate),
        va_trref: r.va_trref,
        va_trtype: r.va_trtype,
        va_account: r.va_account,
        va_anvat: r.va_anvat,
        va_vattype: (r.va_vattype || '').trim(),
        va_vatval: r2(Number(r.va_vatval || 0)),
        va_trvalue: r2(Number(r.va_trvalue || 0)),
        va_vatrate: r.va_vatrate,
        va_done: r.va_done,
        va_reverse: r.va_reverse,
      }));

    const stamp = new Date().toISOString().slice(0, 10);
    const ntranCsv = `${OUT_DIR}/vat-control-ntran-C-${stamp}.csv`;
    const unmatchedZvCsv = `${OUT_DIR}/vat-files-unmatched-C-${stamp}.csv`;
    const nvatCsv = `${OUT_DIR}/nvat-rows-C-${stamp}.csv`;
    writeFileSync(ntranCsv, toCsv(ntranEnriched));
    writeFileSync(unmatchedZvCsv, toCsv(unmatchedZv));
    writeFileSync(nvatCsv, toCsv(nvatRows.map((r) => ({
      nv_acnt: r.nv_acnt,
      nv_taxdate: fmtDate(r.nv_taxdate),
      nv_value: r2(Number(r.nv_value || 0)),
      nv_vatval: r2(Number(r.nv_vatval || 0)),
      nv_vatcode: r.nv_vatcode,
      nv_vattype: r.nv_vattype,
      nv_ref: r.nv_ref,
      nv_comment: r.nv_comment,
    }))));

    const suspicious = ntranEnriched.filter((r) => r.match_status === 'UNMATCHED');
    const susSum = suspicious.reduce((s, r) => s + Number(r.nt_value || 0), 0);
    const unmZvSum = unmatchedZv.reduce(
      (s, r) => s + (r.va_vattype === 'S' ? Number(r.va_vatval) : -Number(r.va_vatval)),
      0,
    );

    console.log(`Unmatched ntran rows: ${suspicious.length}  sum(nt_value)=${r2(susSum).toFixed(2)}`);
    console.log(`Unmatched zvtran rows: ${unmatchedZv.length}  net(S−P)=${r2(unmZvSum).toFixed(2)}`);
    console.log('');
    console.log('--- Top 40 unmatched ntran rows (|value| desc) ---');
    suspicious
      .sort((a, b) => Math.abs(b.nt_value) - Math.abs(a.nt_value))
      .slice(0, 40)
      .forEach((r) => {
        console.log(
          `  ${r.nt_entr}  jrnl=${String(r.nt_jrnl).padStart(7)}  posttyp=${(r.nt_posttyp || '').padEnd(2)}  value=${r.nt_value.toFixed(2).padStart(12)}  ref=${(r.nt_ref || '').padEnd(10)}  trnref=${(r.nt_trnref || '').slice(0, 40)}`,
        );
      });
    console.log('');
    console.log('--- Top 40 unmatched zvtran rows (|vatval| desc) ---');
    unmatchedZv
      .sort((a, b) => Math.abs(b.va_vatval) - Math.abs(a.va_vatval))
      .slice(0, 40)
      .forEach((r) => {
        console.log(
          `  ${r.va_taxdate}  type=${r.va_vattype}  done=${r.va_done ? 1 : 0}  vatval=${r.va_vatval.toFixed(2).padStart(10)}  trref=${(r.va_trref || '').padEnd(10)}  acct=${(r.va_account || '').padEnd(8)}  anvat=${(r.va_anvat || '')}`,
        );
      });
    console.log('');
    console.log('CSVs:');
    console.log(`  ${ntranCsv}`);
    console.log(`  ${unmatchedZvCsv}`);
    console.log(`  ${nvatCsv}`);
  } finally {
    await db.destroy();
  }
}

main().catch((e) => {
  console.error('FAILED:', e);
  process.exit(1);
});
