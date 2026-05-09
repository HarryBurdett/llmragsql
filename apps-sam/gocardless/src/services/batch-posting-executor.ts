/**
 * GoCardless batch posting executor — the SQL-write body for
 * `POST /api/gocardless/import`.
 *
 * Faithful port of the inner posting body of
 * `OperaSQLImport.import_gocardless_batch`
 * (sql_rag/opera_sql_import.py:6017-7017). Implements the
 * `BatchPostingExecutor` contract from `import-batch.ts` so the
 * route layer can wire it up directly.
 *
 * Per CLAUDE.md "complete data updates": every ntran INSERT is
 * followed by `updateNacntBalance`; every cashbook receipt updates
 * `nbank.nk_curbal` via `updateNbankBalance`; customer balance is
 * adjusted via sname.sn_currbal. Locking matches Python: NOLOCK on
 * reads, ROWLOCK on writes, UPDLOCK on sequence allocation.
 *
 * Behaviour notes / scope:
 *   - cbtype defaults to first batched-receipt atype if not supplied
 *   - Customer info (sn_name / sn_region / sn_terrtry / sn_custype)
 *     loaded once per customer via in-batch caching
 *   - aentry header inserts in pence with ae_complet=1 when
 *     completeBatch, otherwise 0 (leaves for review in Opera)
 *   - For each payment:
 *       1. atran (pence, at_type=4 sales receipt)
 *       2. stran (pounds, st_trtype='R')
 *       3. nbank.nk_curbal += amount (always)
 *       4. ntran debit/credit pair + nacnt updates (only when
 *          completeBatch and post_to_nominal)
 *       5. anoml debit/credit pair (only when completeBatch and
 *          post_to_transfer_file)
 *       6. sname.sn_currbal -= amount (always)
 *   - Fees + VAT split + bank-transfer auto-leg are TODO'd in this
 *     port (see Python lines 6519+ for fees, 6800+ for transfer).
 *     They're separate flows that the SAM team can layer on.
 */
import type { Knex } from 'knex';
import {
  getControlAccounts,
  getNacntType,
  getNextId,
  getNextJournal,
  getPeriodForDate,
  generateOperaUniqueIds,
  incrementAtypeEntry,
  insertNjmemo,
  updateNacntBalance,
  updateNbankBalance,
  type NacntType,
} from '@sqlrag/sam-shared';
import type {
  BatchPostingExecutor,
  ValidatedPayment,
  ValidatedRequest,
} from './import-batch.js';

interface CustomerInfo {
  account: string;
  name: string;
  region: string;
  terr: string;
  type: string;
  controlAccount: string;
}

const DEFAULT_REGION = 'K';
const DEFAULT_TERR = '001';
const DEFAULT_TYPE = 'DD1';

async function resolveCustomerControlAccount(
  trx: Knex,
  customerAccount: string,
  defaults: { sl_control: string },
): Promise<string> {
  try {
    const rows = (await trx.raw(
      `SELECT RTRIM(ISNULL(sp.sc_dbtctrl, '')) AS control_account
       FROM sname s WITH (NOLOCK)
       LEFT JOIN sprfls sp WITH (NOLOCK) ON RTRIM(s.sn_cprfl) = RTRIM(sp.sc_code)
       WHERE RTRIM(s.sn_account) = ?`,
      [customerAccount],
    )) as unknown as Array<{ control_account: string | null }>;
    if (Array.isArray(rows) && rows.length > 0) {
      const ctl = (rows[0]?.control_account ?? '').trim();
      if (ctl) return ctl;
    }
  } catch {
    // fall through to default
  }
  return defaults.sl_control;
}

async function loadCustomerInfo(
  trx: Knex,
  payments: ValidatedPayment[],
  defaults: { sl_control: string },
): Promise<Map<string, CustomerInfo>> {
  const out = new Map<string, CustomerInfo>();
  const seen = new Set<string>();
  for (const p of payments) {
    const acct = p.customer_account.trim();
    if (seen.has(acct)) continue;
    seen.add(acct);
    const rows = (await trx.raw(
      `SELECT TOP 1 sn_name, sn_region, sn_terrtry, sn_custype
       FROM sname WITH (NOLOCK)
       WHERE RTRIM(sn_account) = ?`,
      [acct],
    )) as unknown as Array<{
      sn_name: string | null;
      sn_region: string | null;
      sn_terrtry: string | null;
      sn_custype: string | null;
    }>;
    if (!Array.isArray(rows) || rows.length === 0) {
      throw new Error(`Customer account '${acct}' not found in sname`);
    }
    const r = rows[0]!;
    const controlAccount = await resolveCustomerControlAccount(
      trx,
      acct,
      defaults,
    );
    out.set(acct, {
      account: acct,
      name: (r.sn_name ?? '').trim(),
      region: (r.sn_region ?? '').trim() || DEFAULT_REGION,
      terr: (r.sn_terrtry ?? '').trim() || DEFAULT_TERR,
      type: (r.sn_custype ?? '').trim() || DEFAULT_TYPE,
      controlAccount,
    });
  }
  return out;
}

async function resolveCbtype(
  trx: Knex,
  preferred: string | null,
): Promise<{ cbtype: string; description: string }> {
  if (preferred) {
    const rows = (await trx.raw(
      `SELECT TOP 1 RTRIM(ay_desc) AS ay_desc
       FROM atype WITH (NOLOCK)
       WHERE RTRIM(ay_cbtype) = ? AND ay_type = 'R' AND ay_batched = 1`,
      [preferred],
    )) as unknown as Array<{ ay_desc: string | null }>;
    if (!Array.isArray(rows) || rows.length === 0) {
      throw new Error(
        `cbtype '${preferred}' is not a batched receipt type in atype`,
      );
    }
    return {
      cbtype: preferred,
      description: (rows[0]?.ay_desc ?? '').toString().trim() || 'Cheque',
    };
  }

  const gcRows = (await trx.raw(
    `SELECT TOP 1 RTRIM(ay_cbtype) AS ay_cbtype, RTRIM(ay_desc) AS ay_desc
     FROM atype WITH (NOLOCK)
     WHERE ay_type = 'R' AND ay_batched = 1
       AND (ay_desc LIKE '%GoCardless%' OR ay_desc LIKE '%gocardless%')`,
  )) as unknown as Array<{ ay_cbtype: string | null; ay_desc: string | null }>;
  if (Array.isArray(gcRows) && gcRows.length > 0 && gcRows[0]?.ay_cbtype) {
    return {
      cbtype: (gcRows[0].ay_cbtype ?? '').toString().trim(),
      description: (gcRows[0].ay_desc ?? '').toString().trim() || 'GoCardless',
    };
  }

  const rows = (await trx.raw(
    `SELECT TOP 1 RTRIM(ay_cbtype) AS ay_cbtype, RTRIM(ay_desc) AS ay_desc
     FROM atype WITH (NOLOCK)
     WHERE ay_type = 'R' AND ay_batched = 1`,
  )) as unknown as Array<{ ay_cbtype: string | null; ay_desc: string | null }>;
  if (!Array.isArray(rows) || rows.length === 0 || !rows[0]?.ay_cbtype) {
    throw new Error('No batched Receipt type codes found in atype table');
  }
  return {
    cbtype: (rows[0].ay_cbtype ?? '').toString().trim(),
    description: (rows[0].ay_desc ?? '').toString().trim() || 'Cheque',
  };
}

function nowMs(): {
  date: string;
  time: string;
  iso: string;
} {
  const now = new Date();
  const pad = (n: number) => n.toString().padStart(2, '0');
  const date = `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(
    now.getDate(),
  )}`;
  const time = `${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(
    now.getSeconds(),
  )}`;
  return { date, time, iso: `${date} ${time}` };
}

function pence(amountPounds: number): number {
  return Math.round(amountPounds * 100);
}

// ---------------------------------------------------------------------
// Inserts — kept short, parameter-bound
// ---------------------------------------------------------------------

async function insertAentry(
  trx: Knex,
  args: {
    aentryId: number;
    bankAccount: string;
    cbtype: string;
    entryNumber: string;
    postDate: string;
    reference: string;
    totalPence: number;
    completeBatch: boolean;
    inputBy: string;
    nowDate: string;
    nowTime: string;
    nowIso: string;
  },
): Promise<void> {
  await trx.raw(
    `INSERT INTO aentry (
      id, ae_acnt, ae_cntr, ae_cbtype, ae_entry, ae_reclnum,
      ae_lstdate, ae_frstat, ae_tostat, ae_statln, ae_entref,
      ae_value, ae_recbal, ae_remove, ae_tmpstat, ae_complet,
      ae_postgrp, sq_crdate, sq_crtime, sq_cruser, ae_comment,
      ae_payid, ae_batchid, ae_brwptr, datecreated, datemodified, state
    ) VALUES (
      ?, ?, '    ', ?, ?, 0,
      ?, 0, 0, 0, ?,
      ?, 0, 0, 0, ?,
      0, ?, ?, ?, 'GoCardless batch import',
      0, 0, '  ', ?, ?, 1
    )`,
    [
      args.aentryId,
      args.bankAccount,
      args.cbtype,
      args.entryNumber,
      args.postDate,
      args.reference.slice(0, 20),
      args.totalPence,
      args.completeBatch ? 1 : 0,
      args.nowDate,
      args.nowTime.slice(0, 8),
      args.inputBy.slice(0, 8),
      args.nowIso,
      args.nowIso,
    ],
  );
}

async function insertAtran(
  trx: Knex,
  args: {
    atranId: number;
    bankAccount: string;
    cbtype: string;
    entryNumber: string;
    inputBy: string;
    postDate: string;
    amountPence: number;
    customerAccount: string;
    customerName: string;
    description: string;
    atranUnique: string;
    reference: string;
    nowIso: string;
  },
): Promise<void> {
  await trx.raw(
    `INSERT INTO atran (
      id, at_acnt, at_cntr, at_cbtype, at_entry, at_inputby,
      at_type, at_pstdate, at_sysdate, at_tperiod, at_value,
      at_disc, at_fcurr, at_fcexch, at_fcmult, at_fcdec,
      at_account, at_name, at_comment, at_payee, at_payname,
      at_sort, at_number, at_remove, at_chqprn, at_chqlst,
      at_bacprn, at_ccdprn, at_ccdno, at_payslp, at_pysprn,
      at_cash, at_remit, at_unique, at_postgrp, at_ccauth,
      at_refer, at_srcco, at_ecb, at_ecbtype, at_atpycd,
      at_bsref, at_bsname, at_vattycd, at_project, at_job,
      at_bic, at_iban, at_memo, datecreated, datemodified, state
    ) VALUES (
      ?, ?, '    ', ?, ?, ?,
      4, ?, ?, 1, ?,
      0, '   ', 1.0, 0, 2,
      ?, ?, ?, '        ', '',
      '        ', '         ', 0, 0, 0,
      0, 0, '', 0, 0,
      0, 0, ?, 0, '0       ',
      ?, 'I', 0, ' ', '      ',
      '', '', '  ', '        ', '        ',
      '', '', '', ?, ?, 1
    )`,
    [
      args.atranId,
      args.bankAccount,
      args.cbtype,
      args.entryNumber,
      args.inputBy.slice(0, 8),
      args.postDate,
      args.postDate,
      args.amountPence,
      args.customerAccount,
      args.customerName.slice(0, 35),
      args.description.slice(0, 35),
      args.atranUnique,
      args.reference.slice(0, 20),
      args.nowIso,
      args.nowIso,
    ],
  );
}

async function insertStran(
  trx: Knex,
  args: {
    stranId: number;
    customerAccount: string;
    postDate: string;
    reference: string;
    amountPounds: number;
    memo: string;
    cbtype: string;
    entryNumber: string;
    stranUnique: string;
    region: string;
    terr: string;
    type: string;
    nowIso: string;
  },
): Promise<void> {
  await trx.raw(
    `INSERT INTO stran (
      id, st_account, st_trdate, st_trref, st_custref, st_trtype,
      st_trvalue, st_vatval, st_trbal, st_paid, st_crdate,
      st_advance, st_memo, st_payflag, st_set1day, st_set1,
      st_set2day, st_set2, st_dueday, st_fcurr, st_fcrate,
      st_fcdec, st_fcval, st_fcbal, st_fcmult, st_dispute,
      st_edi, st_editx, st_edivn, st_txtrep, st_binrep,
      st_advallc, st_cbtype, st_entry, st_unique, st_region,
      st_terr, st_type, st_fadval, st_delacc, st_euro,
      st_payadvl, st_eurind, st_origcur, st_fullamt, st_fullcb,
      st_fullnar, st_cash, st_rcode, st_ruser, st_revchrg,
      st_nlpdate, st_adjsv, st_fcvat, st_taxpoin,
      datecreated, datemodified, state
    ) VALUES (
      ?, ?, ?, ?, 'GoCardless', 'R',
      ?, 0, ?, ' ', ?,
      'N', ?, 0, 0, 0,
      0, 0, ?, '   ', 0,
      0, 0, 0, 0, 0,
      0, 0, 0, '', 0,
      0, ?, ?, ?, ?,
      ?, ?, 0, ?, 0,
      0, ' ', '   ', 0, '  ',
      '          ', 0, '    ', '        ', 0,
      ?, 0, 0, ?,
      ?, ?, 1
    )`,
    [
      args.stranId,
      args.customerAccount,
      args.postDate,
      args.reference.slice(0, 20),
      -args.amountPounds,
      -args.amountPounds,
      args.postDate,
      args.memo.slice(0, 200),
      args.postDate,
      args.cbtype,
      args.entryNumber,
      args.stranUnique,
      args.region.slice(0, 3),
      args.terr.slice(0, 3),
      args.type.slice(0, 3),
      args.customerAccount,
      args.postDate,
      args.postDate,
      args.nowIso,
      args.nowIso,
    ],
  );
}

async function insertNtranPair(
  trx: Knex,
  args: {
    idStart: number;
    bankAccount: string;
    bankType: NacntType;
    salesLedgerControl: string;
    controlType: NacntType;
    journal: number;
    inputBy: string;
    comment: string;
    trnref: string;
    postDate: string;
    amountPounds: number;
    year: number;
    period: number;
    pstid: string;
    nowIso: string;
  },
): Promise<void> {
  // Bank DEBIT (positive value = receipt)
  await trx.raw(
    `INSERT INTO ntran (
      id, nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
      nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
      nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
      nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
      nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
      nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
      nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
      nt_distrib, datecreated, datemodified, state
    ) VALUES (
      ?, ?, '    ', ?, ?, ?,
      '', ?, 'A', ?, ?,
      ?, ?, ?, ?, 0,
      0, 0, '   ', 0, 0,
      0, 0, 'I', '', '        ',
      '        ', 'S', 0, ?, 0,
      0, 0, 0, 0, 0,
      0, ?, ?, 1
    )`,
    [
      args.idStart,
      args.bankAccount,
      args.bankType.na_type,
      args.bankType.na_subt,
      args.journal,
      args.inputBy.slice(0, 10),
      args.comment,
      args.trnref,
      args.postDate,
      args.amountPounds,
      args.year,
      args.period,
      args.pstid,
      args.nowIso,
      args.nowIso,
    ],
  );

  // Debtors-control CREDIT (negative value)
  await trx.raw(
    `INSERT INTO ntran (
      id, nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
      nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
      nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
      nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
      nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
      nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
      nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
      nt_distrib, datecreated, datemodified, state
    ) VALUES (
      ?, ?, '    ', ?, ?, ?,
      '', ?, 'A', ?, ?,
      ?, ?, ?, ?, 0,
      0, 0, '   ', 0, 0,
      0, 0, 'I', '', '        ',
      '        ', 'S', 0, ?, 0,
      0, 0, 0, 0, 0,
      0, ?, ?, 1
    )`,
    [
      args.idStart + 1,
      args.salesLedgerControl,
      args.controlType.na_type,
      args.controlType.na_subt,
      args.journal,
      args.inputBy.slice(0, 10),
      args.comment,
      args.trnref,
      args.postDate,
      -args.amountPounds,
      args.year,
      args.period,
      args.pstid,
      args.nowIso,
      args.nowIso,
    ],
  );
}

async function insertAnomlPair(
  trx: Knex,
  args: {
    idStart: number;
    bankAccount: string;
    salesLedgerControl: string;
    postDate: string;
    amountPounds: number;
    reference: string;
    comment: string;
    doneFlag: string;
    atranUnique: string;
    journal: number;
    nowIso: string;
  },
): Promise<void> {
  // Bank leg
  await trx.raw(
    `INSERT INTO anoml (
      id, ax_nacnt, ax_ncntr, ax_source, ax_date, ax_value, ax_tref,
      ax_comment, ax_done, ax_fcurr, ax_fvalue, ax_fcrate, ax_fcmult, ax_fcdec,
      ax_srcco, ax_unique, ax_project, ax_job, ax_jrnl, ax_nlpdate,
      datecreated, datemodified, state
    ) VALUES (
      ?, ?, '    ', 'S', ?, ?, ?,
      ?, ?, '   ', 0, 0, 0, 0,
      'I', ?, '        ', '        ', ?, ?,
      ?, ?, 1
    )`,
    [
      args.idStart,
      args.bankAccount,
      args.postDate,
      args.amountPounds,
      args.reference.slice(0, 20),
      args.comment.slice(0, 50),
      args.doneFlag,
      args.atranUnique,
      args.journal,
      args.postDate,
      args.nowIso,
      args.nowIso,
    ],
  );

  // Debtors-control leg
  await trx.raw(
    `INSERT INTO anoml (
      id, ax_nacnt, ax_ncntr, ax_source, ax_date, ax_value, ax_tref,
      ax_comment, ax_done, ax_fcurr, ax_fvalue, ax_fcrate, ax_fcmult, ax_fcdec,
      ax_srcco, ax_unique, ax_project, ax_job, ax_jrnl, ax_nlpdate,
      datecreated, datemodified, state
    ) VALUES (
      ?, ?, '    ', 'S', ?, ?, ?,
      ?, ?, '   ', 0, 0, 0, 0,
      'I', ?, '        ', '        ', ?, ?,
      ?, ?, 1
    )`,
    [
      args.idStart + 1,
      args.salesLedgerControl,
      args.postDate,
      -args.amountPounds,
      args.reference.slice(0, 20),
      args.comment.slice(0, 50),
      args.doneFlag,
      args.atranUnique,
      args.journal,
      args.postDate,
      args.nowIso,
      args.nowIso,
    ],
  );
}

// ---------------------------------------------------------------------
// Public executor
// ---------------------------------------------------------------------

export const gocardlessBatchPostingExecutor: BatchPostingExecutor = {
  async postBatch(operaDb, request): Promise<{
    success: boolean;
    records_imported: number;
    batch_ref?: string | null;
    warnings: string[];
    errors: string[];
  }> {
    const warnings: string[] = [];
    let recordsImported = 0;
    let batchRef: string | null = null;

    try {
      const controlAccounts = await getControlAccounts(operaDb);
      const slControl = controlAccounts.debtorsControl;

      await operaDb.transaction(async (trx) => {
        const now = nowMs();
        const { period, year } = await getPeriodForDate(trx, request.postDate);

        const customerInfo = await loadCustomerInfo(trx, request.payments, {
          sl_control: slControl,
        });

        const cbtypeChoice = await resolveCbtype(trx, request.cbtype);
        const cbtype = cbtypeChoice.cbtype;
        const cbtypeDesc = cbtypeChoice.description;

        // Allocate sequences inside the transaction so retries are safe
        const uniqueIds = generateOperaUniqueIds(request.payments.length * 2);
        const entryNumber = await incrementAtypeEntry(trx, cbtype);
        batchRef = entryNumber;
        const aentryId = await getNextId(trx, 'aentry');
        // One journal per payment when complete_batch=true; otherwise we
        // still allocate one for consistency.
        const journalCount = request.completeBatch
          ? request.payments.length
          : 1;
        let nextJournal = await getNextJournal(trx, journalCount);

        const totalPence = request.payments.reduce(
          (acc, p) => acc + pence(p.amount),
          0,
        );

        await insertAentry(trx, {
          aentryId,
          bankAccount: request.postingBank,
          cbtype,
          entryNumber,
          postDate: request.postDateString,
          reference: request.reference,
          totalPence,
          completeBatch: request.completeBatch,
          inputBy: 'GOCARDLS',
          nowDate: now.date,
          nowTime: now.time,
          nowIso: now.iso,
        });

        for (let i = 0; i < request.payments.length; i++) {
          const p = request.payments[i]!;
          const cust = customerInfo.get(p.customer_account.trim());
          if (!cust) {
            throw new Error(
              `Customer info missing for ${p.customer_account} — should have been validated already`,
            );
          }
          const amountPounds = Number(p.amount);
          const amountPence = pence(amountPounds);

          const atranUnique = uniqueIds[i * 2]!;
          const ntranPstid = uniqueIds[i * 2 + 1]!;
          const atranId = await getNextId(trx, 'atran');
          const stranId = await getNextId(trx, 'stran');

          await insertAtran(trx, {
            atranId,
            bankAccount: request.postingBank,
            cbtype,
            entryNumber,
            inputBy: 'GOCARDLS',
            postDate: request.postDateString,
            amountPence,
            customerAccount: cust.account,
            customerName: cust.name,
            description: p.description,
            atranUnique,
            reference: request.reference,
            nowIso: now.iso,
          });

          await insertStran(trx, {
            stranId,
            customerAccount: cust.account,
            postDate: request.postDateString,
            reference: request.reference,
            amountPounds,
            memo: `GoCardless - ${p.description}`,
            cbtype,
            entryNumber,
            stranUnique: atranUnique, // shared with atran by design
            region: cust.region,
            terr: cust.terr,
            type: cust.type,
            nowIso: now.iso,
          });

          // Bank balance update — always
          await updateNbankBalance(trx, request.postingBank, amountPounds);

          if (request.completeBatch) {
            const bankType =
              (await getNacntType(trx, request.postingBank)) ??
              ({ na_type: 'B ', na_subt: 'BC' } as NacntType);
            const controlType =
              (await getNacntType(trx, cust.controlAccount)) ??
              ({ na_type: 'B ', na_subt: 'BB' } as NacntType);
            const ntranIdStart = await getNextId(trx, 'ntran', 2);
            const ntranComment = (p.description || '').padEnd(50).slice(0, 50);
            const ntranTrnref = (
              cust.name.slice(0, 30).padEnd(30) + 'GoCardless (RT)     '
            ).slice(0, 50);

            await insertNtranPair(trx, {
              idStart: ntranIdStart,
              bankAccount: request.postingBank,
              bankType,
              salesLedgerControl: cust.controlAccount,
              controlType,
              journal: nextJournal,
              inputBy: 'GOCARDLS',
              comment: ntranComment,
              trnref: ntranTrnref,
              postDate: request.postDateString,
              amountPounds,
              year,
              period,
              pstid: ntranPstid,
              nowIso: now.iso,
            });
            await updateNacntBalance(trx, request.postingBank, amountPounds, {
              period,
              year,
            });
            await updateNacntBalance(
              trx,
              cust.controlAccount,
              -amountPounds,
              { period, year },
            );
            await insertNjmemo(
              trx,
              nextJournal,
              'Cashbook Ledger Transfer (RT)',
            );

            const anomlIdStart = await getNextId(trx, 'anoml', 2);
            const anomlComment = (cust.name.slice(0, 30).padEnd(30) + cbtypeDesc).slice(0, 40);
            await insertAnomlPair(trx, {
              idStart: anomlIdStart,
              bankAccount: request.postingBank,
              salesLedgerControl: cust.controlAccount,
              postDate: request.postDateString,
              amountPounds,
              reference: request.reference,
              comment: anomlComment,
              doneFlag: 'Y', // post-to-NL completed
              atranUnique,
              journal: nextJournal,
              nowIso: now.iso,
            });
            nextJournal += 1;
          }

          // Customer balance — always
          await trx.raw(
            `UPDATE sname WITH (ROWLOCK)
             SET sn_currbal = ISNULL(sn_currbal, 0) - ?,
                 sn_nextpay = ISNULL(sn_nextpay, 0) + 1,
                 datemodified = GETDATE()
             WHERE RTRIM(sn_account) = ?`,
            [amountPounds, cust.account],
          );

          recordsImported += 1;
        }

        // TODO follow-up port: fees split (Python lines ~6519-6700) and
        // bank-transfer auto-leg (lines ~6800-7000). Both run after the
        // payment loop. Documented as known follow-ups in handoff doc.
        if (request.goCardlessFees > 0) {
          warnings.push(
            `GoCardless fees of £${request.goCardlessFees.toFixed(
              2,
            )} not yet posted by this executor. Fees-split port pending — post manually as a separate cashbook payment until then.`,
          );
        }
        if (request.destinationBank) {
          warnings.push(
            `Destination-bank auto-transfer to ${request.destinationBank} not yet posted by this executor. Bank-transfer port pending — post manually until then.`,
          );
        }
      });

      return {
        success: true,
        records_imported: recordsImported,
        batch_ref: batchRef,
        warnings,
        errors: [],
      };
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      return {
        success: false,
        records_imported: 0,
        batch_ref: null,
        warnings,
        errors: [msg],
      };
    }
  },
};
