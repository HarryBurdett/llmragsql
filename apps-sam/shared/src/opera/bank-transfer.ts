/**
 * Opera bank-transfer between two cashbook accounts.
 *
 * Faithful port of `OperaSQLImport.import_bank_transfer`
 * (sql_rag/opera_sql_import.py:9152-9651).
 *
 * Creates paired entries:
 *   - 2× aentry (one per bank, opposite signs)
 *   - 2× atran  (at_type=8, at_account=counterpart)
 *   - 2× ntran  (when post_to_nominal — opposite signs)
 *   - 2× anoml  (transfer file records, ax_source='A')
 *   - nbank balance updates for both
 *   - nacnt balance updates for both (when post_to_nominal)
 *
 * Locking:
 *   - Banks are processed in alphabetical order to prevent A→B / B→A
 *     deadlocks across concurrent transfers
 *   - WITH (ROWLOCK) on every UPDATE
 *   - Sequence allocation via UPDLOCK helpers (getNextJournal, etc.)
 *   - Single MSSQL transaction so a failure rolls back both legs
 */
import type { Knex } from 'knex';
import {
  validateBankCode,
  validateCbtype,
  SqlInputValidationError,
} from './sql-input-validators.js';
import { getPeriodPostingDecision } from './period-validation.js';
import {
  generateOperaUniqueId,
  generateOperaUniqueIds,
} from './unique-id.js';
import {
  getNextId,
  getNextJournal,
  incrementAtypeEntry,
} from './id-allocation.js';
import {
  getNacntType,
  updateNacntBalance,
  updateNbankBalance,
  insertNjmemo,
} from './balance-updates.js';

export interface BankTransferOptions {
  sourceBank: string;
  destBank: string;
  amountPounds: number; // positive
  reference: string; // max 20 chars
  postDate: string; // YYYY-MM-DD
  comment?: string;
  inputBy?: string; // max 8 chars
  cbtype?: string; // optional transfer type code (T1, etc.)
}

export interface BankTransferResult {
  success: boolean;
  source_entry?: string;
  dest_entry?: string;
  source_bank?: string;
  dest_bank?: string;
  amount?: number;
  journal_number?: number | null;
  shared_unique?: string;
  posting_mode?: string;
  tables_updated?: string[];
  message?: string;
  error?: string;
}

interface NbankRow {
  nk_acnt: string;
  nk_desc: string;
  nk_fcurr: string | null;
  nk_sort: string | null;
  nk_number: string | null;
}

async function readBank(
  operaDb: Knex,
  code: string,
): Promise<NbankRow | null> {
  const rows = (await operaDb.raw(
    `SELECT TOP 1
        RTRIM(nk_acnt) AS nk_acnt,
        RTRIM(ISNULL(nk_desc, '')) AS nk_desc,
        nk_fcurr,
        RTRIM(ISNULL(nk_sort, '')) AS nk_sort,
        RTRIM(ISNULL(nk_number, '')) AS nk_number
       FROM nbank WITH (NOLOCK)
       WHERE RTRIM(nk_acnt) = ?`,
    [code.trim()],
  )) as NbankRow[];
  return Array.isArray(rows) && rows.length > 0 ? rows[0]! : null;
}

async function getDefaultTransferCbtype(
  operaDb: Knex,
): Promise<string | null> {
  const rows = (await operaDb.raw(
    `SELECT TOP 1 ay_cbtype
       FROM atype WITH (NOLOCK)
       WHERE ay_type = 'T'
       ORDER BY ay_cbtype`,
  )) as Array<{ ay_cbtype: string }>;
  if (!Array.isArray(rows) || rows.length === 0 || !rows[0]?.ay_cbtype)
    return null;
  return rows[0].ay_cbtype.trim();
}

function pad(value: string, length: number): string {
  return value.length >= length ? value.slice(0, length) : value.padEnd(length, ' ');
}

function nowParts(): {
  nowStr: string;
  dateStr: string;
  timeStr: string;
} {
  const d = new Date();
  const iso = d.toISOString();
  return {
    nowStr: iso.replace('T', ' ').slice(0, 19),
    dateStr: iso.slice(0, 10),
    timeStr: iso.slice(11, 19),
  };
}

const AT_TYPE_TRANSFER = 8;

export async function importBankTransfer(
  operaDb: Knex,
  opts: BankTransferOptions,
): Promise<BankTransferResult> {
  const amount = Number(opts.amountPounds);
  if (!Number.isFinite(amount) || amount <= 0) {
    return { success: false, error: 'Transfer amount must be positive' };
  }
  let source: string;
  let dest: string;
  try {
    source = validateBankCode(opts.sourceBank);
    dest = validateBankCode(opts.destBank);
  } catch (e) {
    if (e instanceof SqlInputValidationError) {
      return { success: false, error: e.message };
    }
    throw e;
  }
  if (source.toUpperCase() === dest.toUpperCase()) {
    return {
      success: false,
      error: 'Source and destination bank accounts must be different',
    };
  }

  const sourceBank = await readBank(operaDb, source);
  if (!sourceBank) {
    return {
      success: false,
      error: `Source bank account '${source}' not found in nbank`,
    };
  }
  if ((sourceBank.nk_fcurr ?? '').trim()) {
    return {
      success: false,
      error: `Source bank '${source}' is a foreign currency account - transfers not supported`,
    };
  }
  const destBank = await readBank(operaDb, dest);
  if (!destBank) {
    return {
      success: false,
      error: `Destination bank account '${dest}' not found in nbank`,
    };
  }
  if ((destBank.nk_fcurr ?? '').trim()) {
    return {
      success: false,
      error: `Destination bank '${dest}' is a foreign currency account - transfers not supported`,
    };
  }

  // Resolve transfer cbtype
  let transferType = (opts.cbtype ?? '').trim();
  if (!transferType) {
    const auto = await getDefaultTransferCbtype(operaDb);
    if (!auto) {
      return {
        success: false,
        error: "No Transfer type codes (ay_type='T') found in atype table",
      };
    }
    transferType = auto;
  }
  try {
    transferType = validateCbtype(transferType);
  } catch (e) {
    if (e instanceof SqlInputValidationError) {
      return { success: false, error: e.message };
    }
    throw e;
  }

  // Period decision (NL — bank transfers post to nominal)
  const decision = await getPeriodPostingDecision(operaDb, opts.postDate, 'NL');
  if (!decision.can_post) {
    return { success: false, error: decision.error_message ?? 'Period rejected' };
  }
  const period = decision.transaction_period;
  const year = decision.transaction_year;

  const reference = (opts.reference ?? '').slice(0, 20);
  const inputBy = (opts.inputBy ?? 'SQLRAG').slice(0, 8);
  const comment = (opts.comment ?? '')
    .replace(/[\r\n]/g, ' ')
    .slice(0, 40);
  const sourceName = sourceBank.nk_desc;
  const destName = destBank.nk_desc;
  const destSort = (destBank.nk_sort ?? '').slice(0, 8);
  const destNumber = (destBank.nk_number ?? '').slice(0, 9);

  const amountPence = Math.round(amount * 100);
  const ntranComment = pad(reference.slice(0, 50), 50);
  const sourceTrnref = pad(destName.slice(0, 30), 30) + 'Transfer  (RT)     ';
  const destTrnref = pad(sourceName.slice(0, 30), 30) + 'Transfer  (RT)     ';

  // Lock-ordering: alphabetical
  const banksOrdered = [source, dest].slice().sort();
  const firstBank = banksOrdered[0]!;
  const sourceIsFirst = source === firstBank;

  const { nowStr, dateStr, timeStr } = nowParts();

  let sourceEntry = '';
  let destEntry = '';
  let nextJournal = 0;
  let sharedUnique = '';

  try {
    await operaDb.transaction(async (trx) => {
      sharedUnique = generateOperaUniqueId();
      const [pstidSource, pstidDest] = generateOperaUniqueIds(2) as [
        string,
        string,
      ];

      // Allocate entry numbers in lock order
      const firstEntry = await incrementAtypeEntry(trx, transferType);
      const secondEntry = await incrementAtypeEntry(trx, transferType);
      sourceEntry = sourceIsFirst ? firstEntry : secondEntry;
      destEntry = sourceIsFirst ? secondEntry : firstEntry;

      nextJournal = await getNextJournal(trx);

      // 1+2: aentry source/dest
      const aentryIdStart = await getNextId(trx, 'aentry', 2);
      const aentrySourceId = aentryIdStart;
      const aentryDestId = aentryIdStart + 1;

      await trx.raw(
        `INSERT INTO aentry (
            id,
            ae_acnt, ae_cntr, ae_cbtype, ae_entry, ae_reclnum,
            ae_lstdate, ae_frstat, ae_tostat, ae_statln, ae_entref,
            ae_value, ae_recbal, ae_remove, ae_tmpstat, ae_complet,
            ae_postgrp, sq_crdate, sq_crtime, sq_cruser, ae_comment,
            ae_payid, ae_batchid, ae_brwptr, datecreated, datemodified, state
          ) VALUES (
            ?,
            ?, '    ', ?, ?, 0,
            ?, 0, 0, 0, ?,
            ?, 0, 0, 0, 1,
            0, ?, ?, ?, ?,
            0, 0, '  ', ?, ?, 1
          )`,
        [
          aentrySourceId,
          source,
          transferType,
          sourceEntry,
          opts.postDate,
          reference,
          -amountPence,
          dateStr,
          timeStr.slice(0, 8),
          inputBy,
          comment,
          nowStr,
          nowStr,
        ],
      );
      await trx.raw(
        `INSERT INTO aentry (
            id,
            ae_acnt, ae_cntr, ae_cbtype, ae_entry, ae_reclnum,
            ae_lstdate, ae_frstat, ae_tostat, ae_statln, ae_entref,
            ae_value, ae_recbal, ae_remove, ae_tmpstat, ae_complet,
            ae_postgrp, sq_crdate, sq_crtime, sq_cruser, ae_comment,
            ae_payid, ae_batchid, ae_brwptr, datecreated, datemodified, state
          ) VALUES (
            ?,
            ?, '    ', ?, ?, 0,
            ?, 0, 0, 0, ?,
            ?, 0, 0, 0, 1,
            0, ?, ?, ?, ?,
            0, 0, '  ', ?, ?, 1
          )`,
        [
          aentryDestId,
          dest,
          transferType,
          destEntry,
          opts.postDate,
          reference,
          amountPence,
          dateStr,
          timeStr.slice(0, 8),
          inputBy,
          comment,
          nowStr,
          nowStr,
        ],
      );

      // 3+4: atran source/dest (at_type=8)
      const atranIdStart = await getNextId(trx, 'atran', 2);
      await trx.raw(
        `INSERT INTO atran (
            id,
            at_acnt, at_cntr, at_cbtype, at_entry, at_inputby,
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
            ?,
            ?, '    ', ?, ?, ?,
            ?, ?, ?, 1, ?,
            0, '   ', 1.0, 0, 2,
            ?, ?, ?, '        ', '',
            ?, ?, 0, 0, 0,
            0, 0, '', 0, 0,
            0, 0, ?, 0, '0       ',
            ?, 'I', 0, ' ', '      ',
            '', '', '  ', '        ', '        ',
            '', '', '', ?, ?, 1
          )`,
        [
          atranIdStart,
          source,
          transferType,
          sourceEntry,
          inputBy,
          AT_TYPE_TRANSFER,
          opts.postDate,
          opts.postDate,
          -amountPence,
          dest,
          destName.slice(0, 35),
          comment,
          destSort,
          destNumber,
          sharedUnique,
          reference,
          nowStr,
          nowStr,
        ],
      );
      await trx.raw(
        `INSERT INTO atran (
            id,
            at_acnt, at_cntr, at_cbtype, at_entry, at_inputby,
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
            ?,
            ?, '    ', ?, ?, ?,
            ?, ?, ?, 1, ?,
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
          atranIdStart + 1,
          dest,
          transferType,
          destEntry,
          inputBy,
          AT_TYPE_TRANSFER,
          opts.postDate,
          opts.postDate,
          amountPence,
          source,
          sourceName.slice(0, 35),
          comment,
          sharedUnique,
          reference,
          nowStr,
          nowStr,
        ],
      );

      // 5: nominal postings (in lock order)
      if (decision.post_to_nominal) {
        const firstValue = sourceIsFirst ? -amount : amount;
        const secondValue = sourceIsFirst ? amount : -amount;
        const firstTrnref = sourceIsFirst ? sourceTrnref : destTrnref;
        const secondTrnref = sourceIsFirst ? destTrnref : sourceTrnref;
        const firstPstid = sourceIsFirst ? pstidSource : pstidDest;
        const secondPstid = sourceIsFirst ? pstidDest : pstidSource;

        const firstBankCode = sourceIsFirst ? source : dest;
        const secondBankCode = sourceIsFirst ? dest : source;

        const firstBankType = (await getNacntType(trx, firstBankCode)) ?? {
          na_type: 'B ',
          na_subt: 'BC',
        };
        const secondBankType = (await getNacntType(trx, secondBankCode)) ?? {
          na_type: 'B ',
          na_subt: 'BC',
        };

        const ntranIdStart = await getNextId(trx, 'ntran', 2);
        await trx.raw(
          `INSERT INTO ntran (
              id,
              nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
              nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
              nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
              nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
              nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
              nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
              nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
              nt_distrib, datecreated, datemodified, state
            ) VALUES (
              ?,
              ?, '    ', ?, ?, ?,
              '', ?, 'A', ?, ?,
              ?, ?, ?, ?, 0,
              0, 0, '   ', 0, 0,
              0, 0, 'I', '', '        ',
              '        ', 'T', 0, ?, 0,
              0, 0, 0, 0, 0,
              0, ?, ?, 1
            )`,
          [
            ntranIdStart,
            firstBankCode,
            firstBankType.na_type,
            firstBankType.na_subt,
            nextJournal,
            inputBy.slice(0, 10),
            ntranComment,
            firstTrnref,
            opts.postDate,
            firstValue,
            year,
            period,
            firstPstid,
            nowStr,
            nowStr,
          ],
        );
        await updateNacntBalance(trx, firstBankCode, firstValue, {
          period,
          year,
        });

        await trx.raw(
          `INSERT INTO ntran (
              id,
              nt_acnt, nt_cntr, nt_type, nt_subt, nt_jrnl,
              nt_ref, nt_inp, nt_trtype, nt_cmnt, nt_trnref,
              nt_entr, nt_value, nt_year, nt_period, nt_rvrse,
              nt_prevyr, nt_consol, nt_fcurr, nt_fvalue, nt_fcrate,
              nt_fcmult, nt_fcdec, nt_srcco, nt_cdesc, nt_project,
              nt_job, nt_posttyp, nt_pstgrp, nt_pstid, nt_srcnlid,
              nt_recurr, nt_perpost, nt_rectify, nt_recjrnl, nt_vatanal,
              nt_distrib, datecreated, datemodified, state
            ) VALUES (
              ?,
              ?, '    ', ?, ?, ?,
              '', ?, 'A', ?, ?,
              ?, ?, ?, ?, 0,
              0, 0, '   ', 0, 0,
              0, 0, 'I', '', '        ',
              '        ', 'T', 0, ?, 0,
              0, 0, 0, 0, 0,
              0, ?, ?, 1
            )`,
          [
            ntranIdStart + 1,
            secondBankCode,
            secondBankType.na_type,
            secondBankType.na_subt,
            nextJournal,
            inputBy.slice(0, 10),
            ntranComment,
            secondTrnref,
            opts.postDate,
            secondValue,
            year,
            period,
            secondPstid,
            nowStr,
            nowStr,
          ],
        );
        await updateNacntBalance(trx, secondBankCode, secondValue, {
          period,
          year,
        });

        await insertNjmemo(trx, nextJournal, 'Cashbook Ledger Transfer (RT)');
      }

      // nbank balance updates (always, in lock order via signed amounts)
      await updateNbankBalance(trx, source, -amount);
      await updateNbankBalance(trx, dest, amount);

      // 6: anoml records
      if (decision.post_to_transfer_file) {
        const doneFlag = decision.transfer_file_done_flag;
        const jrnlNum = decision.post_to_nominal ? nextJournal : 0;
        const anomlIdStart = await getNextId(trx, 'anoml', 2);

        const firstBankCode = sourceIsFirst ? source : dest;
        const secondBankCode = sourceIsFirst ? dest : source;
        const firstAnomlValue = sourceIsFirst ? -amount : amount;
        const secondAnomlValue = sourceIsFirst ? amount : -amount;

        await trx.raw(
          `INSERT INTO anoml (
              id,
              ax_nacnt, ax_ncntr, ax_source, ax_date, ax_value, ax_tref,
              ax_comment, ax_done, ax_fcurr, ax_fvalue, ax_fcrate, ax_fcmult, ax_fcdec,
              ax_srcco, ax_unique, ax_project, ax_job, ax_jrnl, ax_nlpdate,
              datecreated, datemodified, state
            ) VALUES (
              ?,
              ?, '    ', 'A', ?, ?, ?,
              ?, ?, '   ', ?, 1.0, 0, 2.0,
              'I', ?, '        ', '        ', ?, ?,
              ?, ?, 1
            )`,
          [
            anomlIdStart,
            firstBankCode,
            opts.postDate,
            firstAnomlValue,
            reference,
            ntranComment.slice(0, 40),
            doneFlag,
            Math.round(firstAnomlValue * 100),
            sharedUnique,
            jrnlNum,
            opts.postDate,
            nowStr,
            nowStr,
          ],
        );
        await trx.raw(
          `INSERT INTO anoml (
              id,
              ax_nacnt, ax_ncntr, ax_source, ax_date, ax_value, ax_tref,
              ax_comment, ax_done, ax_fcurr, ax_fvalue, ax_fcrate, ax_fcmult, ax_fcdec,
              ax_srcco, ax_unique, ax_project, ax_job, ax_jrnl, ax_nlpdate,
              datecreated, datemodified, state
            ) VALUES (
              ?,
              ?, '    ', 'A', ?, ?, ?,
              ?, ?, '   ', ?, 1.0, 0, 2.0,
              'I', ?, '        ', '        ', ?, ?,
              ?, ?, 1
            )`,
          [
            anomlIdStart + 1,
            secondBankCode,
            opts.postDate,
            secondAnomlValue,
            reference,
            ntranComment.slice(0, 40),
            doneFlag,
            Math.round(secondAnomlValue * 100),
            sharedUnique,
            jrnlNum,
            opts.postDate,
            nowStr,
            nowStr,
          ],
        );
      }
    });
  } catch (e: any) {
    return { success: false, error: e?.message ?? String(e) };
  }

  const tablesUpdated = ['aentry (2)', 'atran (2)'];
  if (decision.post_to_nominal) {
    tablesUpdated.push('ntran (2)', 'nbank (2)', 'nacnt (2)');
  } else {
    tablesUpdated.push('nbank (2)');
  }
  if (decision.post_to_transfer_file) {
    tablesUpdated.push('anoml (2)');
  }
  const postingMode = decision.post_to_nominal
    ? 'Current period - posted to nominal'
    : 'Different period - transfer file only';

  return {
    success: true,
    source_entry: sourceEntry,
    dest_entry: destEntry,
    source_bank: source,
    dest_bank: dest,
    amount,
    journal_number: decision.post_to_nominal ? nextJournal : null,
    shared_unique: sharedUnique,
    posting_mode: postingMode,
    tables_updated: tablesUpdated,
    message: `Transfer created: ${source} -> ${dest} for £${amount.toFixed(2)}`,
  };
}
