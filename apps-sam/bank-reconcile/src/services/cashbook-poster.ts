/**
 * Unified cashbook poster — handles all four daily Opera posting flows:
 *
 *   - Sales receipt    (at_type 4) — port of `import_sales_receipt`
 *   - Sales refund     (at_type 3) — port of `import_sales_refund`
 *   - Purchase payment (at_type 5) — port of `import_purchase_payment`
 *   - Purchase refund  (at_type 6) — port of `import_purchase_refund`
 *
 * Each call writes:
 *   - 1× aentry  (cashbook header, signed pence)
 *   - 1× atran   (cashbook line, signed pence, at_type per variant)
 *   - 2× ntran   (when posting decision allows — bank + ledger control)
 *   - 2× anoml   (when posting decision allows — transfer file)
 *   - 1× stran/ptran (sub-ledger row)
 *   - nbank balance update (always, signed)
 *   - nacnt balance updates (when post_to_nominal)
 *   - sname/pname balance + counter update
 *
 * Locking:
 *   - Every UPDATE uses WITH (ROWLOCK)
 *   - Sequence allocation goes through UPDLOCK helpers
 *   - Single MSSQL transaction per call
 *
 * Faithful port of `OperaSQLImport.import_sales_receipt`,
 * `import_sales_refund`, `import_purchase_payment`,
 * `import_purchase_refund` from `sql_rag/opera_sql_import.py`.
 */
import type { Knex } from 'knex';
import {
  validateBankCode,
  validateAccountCode,
  validateCbtype,
  SqlInputValidationError,
  getPeriodPostingDecision,
  getCustomerControlAccount,
  getSupplierControlAccount,
  getNextJournal,
  getNextId,
  getNacntType,
  updateNacntBalance,
  updateNbankBalance,
  insertNjmemo,
  incrementAtypeEntry,
  generateOperaUniqueIds,
} from '@sqlrag/sam-shared';

export type CashbookKind =
  | 'sales_receipt'
  | 'sales_refund'
  | 'purchase_payment'
  | 'purchase_refund';

export interface CashbookPostInput {
  kind: CashbookKind;
  bankAccount: string;
  /** Customer account for sales_*, supplier account for purchase_* */
  partnerAccount: string;
  amountPounds: number; // POSITIVE — sign is derived from kind
  reference: string;
  postDate: string; // YYYY-MM-DD
  inputBy?: string;
  /** Override of the resolved control account */
  controlAccount?: string;
  paymentMethod?: string;
  cbtype?: string;
  validateOnly?: boolean;
  comment?: string;
}

export interface CashbookPostResult {
  success: boolean;
  records_processed: number;
  records_failed?: number;
  records_imported?: number;
  entry_number?: string;
  transaction_ref?: string;
  errors?: string[];
  warnings?: string[];
}

interface KindConfig {
  /** at_type numeric value */
  atType: number;
  /** Required atype category for cbtype (R or P) */
  requiredCategory: 'R' | 'P';
  /** Sub-ledger ('SL' or 'PL') for period decision */
  ledger: 'SL' | 'PL';
  /** Sign on the cashbook line (1 = receipt, -1 = payment) */
  cashSign: 1 | -1;
  /** Sign on the sub-ledger row (R/F + sign) */
  subTrtype: 'R' | 'F' | 'P';
  subSign: 1 | -1;
  /** anoml ax_source field */
  axSource: 'S' | 'P';
  /** ntran nt_posttyp field */
  ntPosttyp: 'S' | 'P';
  /** True for sales (uses sname/stran), false for purchase (pname/ptran) */
  isSales: boolean;
  /** Default trnref payment phrase */
  trnrefPhrase: string;
  /** Default ax_comment phrase */
  axCommentPhrase: string;
  /** Sub-ledger memo prefix */
  memoPrefix: string;
  /** sub-ledger balance delta sign (+1 or -1) */
  balanceSign: 1 | -1;
  /** ntran control sign — debtors-side (sales) or creditors-side (purchase) */
  ctrlSign: 1 | -1;
}

const KIND_CONFIG: Record<CashbookKind, KindConfig> = {
  sales_receipt: {
    atType: 4,
    requiredCategory: 'R',
    ledger: 'SL',
    cashSign: 1,
    subTrtype: 'R',
    subSign: -1,
    axSource: 'S',
    ntPosttyp: 'S',
    isSales: true,
    trnrefPhrase: 'BACS       (RT)     ',
    axCommentPhrase: 'BACS',
    memoPrefix: 'Payment received',
    balanceSign: -1, // sn_currbal -= amount
    ctrlSign: -1, // CR debtors
  },
  sales_refund: {
    atType: 3,
    requiredCategory: 'P',
    ledger: 'SL',
    cashSign: -1,
    subTrtype: 'F',
    subSign: 1,
    axSource: 'S',
    ntPosttyp: 'S',
    isSales: true,
    trnrefPhrase: 'BACS       (RT)     ',
    axCommentPhrase: 'Refund',
    memoPrefix: 'Refund to customer',
    balanceSign: 1, // sn_currbal += amount
    ctrlSign: 1, // DR debtors
  },
  purchase_payment: {
    atType: 5,
    requiredCategory: 'P',
    ledger: 'PL',
    cashSign: -1,
    subTrtype: 'P',
    subSign: -1,
    axSource: 'P',
    ntPosttyp: 'P',
    isSales: false,
    trnrefPhrase: 'Direct Cr (RT)     ',
    axCommentPhrase: 'Direct Cr',
    memoPrefix: 'Payment to supplier',
    balanceSign: -1, // pn_currbal -= amount
    ctrlSign: 1, // DR creditors (reduce liability)
  },
  purchase_refund: {
    atType: 6,
    requiredCategory: 'R',
    ledger: 'PL',
    cashSign: 1,
    subTrtype: 'F',
    subSign: 1,
    axSource: 'P',
    ntPosttyp: 'P',
    isSales: false,
    trnrefPhrase: 'Direct Cr (RT)     ',
    axCommentPhrase: 'Refund',
    memoPrefix: 'Refund from supplier',
    balanceSign: 1, // pn_currbal += amount
    ctrlSign: -1, // CR creditors (rebuild liability)
  },
};

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

interface PartnerInfo {
  name: string;
  region: string; // sales only
  terr: string; // sales only
  type: string; // sales only
}

async function lookupPartner(
  operaDb: Knex,
  account: string,
  isSales: boolean,
): Promise<PartnerInfo | null> {
  if (isSales) {
    const rows = (await operaDb.raw(
      `SELECT sn_name, sn_region, sn_terrtry, sn_custype FROM sname WITH (NOLOCK)
         WHERE RTRIM(sn_account) = ?`,
      [account],
    )) as Array<{
      sn_name: string | null;
      sn_region: string | null;
      sn_terrtry: string | null;
      sn_custype: string | null;
    }>;
    if (Array.isArray(rows) && rows.length > 0) {
      const r = rows[0]!;
      return {
        name: (r.sn_name ?? '').trim(),
        region: (r.sn_region ?? '').trim() || 'K',
        terr: (r.sn_terrtry ?? '').trim() || '001',
        type: (r.sn_custype ?? '').trim() || 'DD1',
      };
    }
    return null;
  }
  const rows = (await operaDb.raw(
    `SELECT pn_name FROM pname WITH (NOLOCK)
       WHERE RTRIM(pn_account) = ?`,
    [account],
  )) as Array<{ pn_name: string | null }>;
  if (Array.isArray(rows) && rows.length > 0 && rows[0]?.pn_name) {
    return {
      name: rows[0].pn_name.trim(),
      region: '',
      terr: '',
      type: '',
    };
  }
  return null;
}

async function lookupAtype(
  operaDb: Knex,
  cbtype: string,
): Promise<{ category: string; desc: string } | null> {
  const rows = (await operaDb.raw(
    `SELECT ay_cbtype, ay_desc, ay_type
       FROM atype WITH (NOLOCK)
       WHERE RTRIM(ay_cbtype) = ?`,
    [cbtype],
  )) as Array<{ ay_desc: string | null; ay_type: string | null }>;
  if (!Array.isArray(rows) || rows.length === 0) return null;
  return {
    category: (rows[0]?.ay_type ?? '').trim(),
    desc: (rows[0]?.ay_desc ?? '').trim(),
  };
}

async function findDefaultCbtype(
  operaDb: Knex,
  category: 'R' | 'P',
): Promise<string | null> {
  const rows = (await operaDb.raw(
    `SELECT TOP 1 ay_cbtype FROM atype WITH (NOLOCK)
       WHERE ay_type = ?
         AND ay_batched = 0
       ORDER BY ay_cbtype`,
    [category],
  )) as Array<{ ay_cbtype: string | null }>;
  if (Array.isArray(rows) && rows.length > 0 && rows[0]?.ay_cbtype) {
    return rows[0].ay_cbtype.trim();
  }
  return null;
}

export async function postCashbookEntry(
  operaDb: Knex,
  input: CashbookPostInput,
): Promise<CashbookPostResult> {
  const cfg = KIND_CONFIG[input.kind];
  if (!cfg) {
    return {
      success: false,
      records_processed: 1,
      records_failed: 1,
      errors: [`Unknown cashbook kind: ${String(input.kind)}`],
    };
  }

  const amount = Number(input.amountPounds);
  if (!Number.isFinite(amount) || amount <= 0) {
    return {
      success: false,
      records_processed: 1,
      records_failed: 1,
      errors: ['Amount must be positive'],
    };
  }

  // -- Validate boundary inputs
  let bankAccount: string;
  try {
    bankAccount = validateBankCode(input.bankAccount);
  } catch (e) {
    if (e instanceof SqlInputValidationError) {
      return {
        success: false,
        records_processed: 1,
        records_failed: 1,
        errors: [e.message],
      };
    }
    throw e;
  }
  let cbtype = (input.cbtype ?? '').trim();
  if (!cbtype) {
    const auto = await findDefaultCbtype(operaDb, cfg.requiredCategory);
    if (!auto) {
      return {
        success: false,
        records_processed: 1,
        records_failed: 1,
        errors: [
          `No ${cfg.requiredCategory === 'R' ? 'Receipt' : 'Payment'} type codes found in atype table`,
        ],
      };
    }
    cbtype = auto;
  }
  try {
    cbtype = validateCbtype(cbtype);
  } catch (e) {
    if (e instanceof SqlInputValidationError) {
      return {
        success: false,
        records_processed: 1,
        records_failed: 1,
        errors: [e.message],
      };
    }
    throw e;
  }
  const atypeRow = await lookupAtype(operaDb, cbtype);
  if (!atypeRow) {
    return {
      success: false,
      records_processed: 1,
      records_failed: 1,
      errors: [`Type code '${cbtype}' not found in atype table`],
    };
  }
  if (atypeRow.category !== cfg.requiredCategory) {
    const want = cfg.requiredCategory === 'R' ? 'Receipt' : 'Payment';
    return {
      success: false,
      records_processed: 1,
      records_failed: 1,
      errors: [
        `Type '${cbtype}' is category '${atypeRow.category}', but '${cfg.requiredCategory}' (${want}) is required`,
      ],
    };
  }

  // -- Partner lookup + control account
  const partnerAccount = (input.partnerAccount ?? '').trim();
  if (!partnerAccount) {
    return {
      success: false,
      records_processed: 1,
      records_failed: 1,
      errors: [
        cfg.isSales
          ? 'Customer account required'
          : 'Supplier account required',
      ],
    };
  }
  const partner = await lookupPartner(operaDb, partnerAccount, cfg.isSales);
  if (!partner) {
    const label = cfg.isSales ? 'Customer' : 'Supplier';
    return {
      success: false,
      records_processed: 1,
      records_failed: 1,
      errors: [`${label} account '${partnerAccount}' not found`],
    };
  }
  const partnerName = partner.name;

  let controlAccount = (input.controlAccount ?? '').trim();
  if (!controlAccount) {
    controlAccount = cfg.isSales
      ? await getCustomerControlAccount(operaDb, partnerAccount)
      : await getSupplierControlAccount(operaDb, partnerAccount);
  } else {
    try {
      controlAccount = validateAccountCode(controlAccount);
    } catch (e) {
      if (e instanceof SqlInputValidationError) {
        return {
          success: false,
          records_processed: 1,
          records_failed: 1,
          errors: [e.message],
        };
      }
      throw e;
    }
  }

  if (input.validateOnly) {
    return {
      success: true,
      records_processed: 1,
      records_imported: 1,
      warnings: ['Validation passed - no records inserted (validate_only=true)'],
    };
  }

  // -- Period decision
  const decision = await getPeriodPostingDecision(operaDb, input.postDate, cfg.ledger);
  if (!decision.can_post) {
    return {
      success: false,
      records_processed: 1,
      records_failed: 1,
      errors: [decision.error_message ?? 'Period rejected'],
    };
  }
  const period = decision.transaction_period;
  const year = decision.transaction_year;

  // -- Compute amounts
  const amountPence = Math.round(amount * 100);
  const aentryValue = cfg.cashSign * amountPence;
  const atranValue = cfg.cashSign * amountPence;
  const ntranBankValue = cfg.cashSign * amount;
  const ntranCtrlValue = cfg.ctrlSign * amount;
  const subValue = cfg.subSign * amount;
  const balanceDelta = cfg.balanceSign * amount;
  const nbankDelta = cfg.cashSign * amount;
  const reference = (input.reference ?? '').slice(0, 20);
  const inputBy = (input.inputBy ?? 'IMPORT').slice(0, 8);
  const paymentMethod = (input.paymentMethod ?? cfg.axCommentPhrase).slice(0, 20);
  const safeComment = (input.comment ?? '')
    .replace(/[\r\n]/g, ' ')
    .slice(0, 200);

  const ntranComment = pad((safeComment || reference).slice(0, 50), 50);
  const ntranTrnref = pad(partnerName.slice(0, 30), 30) + cfg.trnrefPhrase;
  const axCommentLedger = (
    pad(partnerName.slice(0, 30), 30) + paymentMethod
  ).slice(0, 50);

  const { nowStr, dateStr, timeStr } = nowParts();

  let entryNumber = '';
  let nextJournal = 0;

  try {
    await operaDb.transaction(async (trx) => {
      const [, atranUnique, ntranPstidA, ntranPstidB] = generateOperaUniqueIds(4) as [
        string,
        string,
        string,
        string,
      ];
      const stranUnique = atranUnique;
      entryNumber = await incrementAtypeEntry(trx, cbtype);
      nextJournal = await getNextJournal(trx);

      const aentryRowId = await getNextId(trx, 'aentry');
      const atranRowId = await getNextId(trx, 'atran');
      const subRowId = await getNextId(trx, cfg.isSales ? 'stran' : 'ptran');

      // 1. aentry
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
            ?, 0, 0, 0, 1,
            0, ?, ?, ?, ?,
            0, 0, '  ', ?, ?, 1
          )`,
        [
          aentryRowId,
          bankAccount,
          cbtype,
          entryNumber,
          input.postDate,
          reference,
          aentryValue,
          dateStr,
          timeStr.slice(0, 8),
          inputBy,
          safeComment.slice(0, 40),
          nowStr,
          nowStr,
        ],
      );

      // 2. atran
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
          atranRowId,
          bankAccount,
          cbtype,
          entryNumber,
          inputBy,
          cfg.atType,
          input.postDate,
          input.postDate,
          atranValue,
          partnerAccount,
          partnerName.slice(0, 35),
          safeComment.slice(0, 50),
          atranUnique,
          reference,
          nowStr,
          nowStr,
        ],
      );

      // 3. ntran (when post_to_nominal)
      if (decision.post_to_nominal) {
        const bankType = (await getNacntType(trx, bankAccount)) ?? {
          na_type: 'B ',
          na_subt: 'BC',
        };
        const ctrlType = (await getNacntType(trx, controlAccount)) ?? {
          na_type: cfg.isSales ? 'B ' : 'C ',
          na_subt: cfg.isSales ? 'BB' : 'CA',
        };
        const ntranIdStart = await getNextId(trx, 'ntran', 2);

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
              '        ', ?, 0, ?, 0,
              0, 0, 0, 0, 0,
              0, ?, ?, 1
            )`,
          [
            ntranIdStart,
            bankAccount,
            bankType.na_type,
            bankType.na_subt,
            nextJournal,
            inputBy.slice(0, 10),
            ntranComment,
            ntranTrnref,
            input.postDate,
            ntranBankValue,
            year,
            period,
            cfg.ntPosttyp,
            ntranPstidA,
            nowStr,
            nowStr,
          ],
        );
        await updateNacntBalance(trx, bankAccount, ntranBankValue, {
          period,
          year,
        });

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
              '        ', ?, 0, ?, 0,
              0, 0, 0, 0, 0,
              0, ?, ?, 1
            )`,
          [
            ntranIdStart + 1,
            controlAccount,
            ctrlType.na_type,
            ctrlType.na_subt,
            nextJournal,
            inputBy.slice(0, 10),
            ntranComment,
            ntranTrnref,
            input.postDate,
            ntranCtrlValue,
            year,
            period,
            cfg.ntPosttyp,
            ntranPstidB,
            nowStr,
            nowStr,
          ],
        );
        await updateNacntBalance(trx, controlAccount, ntranCtrlValue, {
          period,
          year,
        });

        await insertNjmemo(trx, nextJournal, 'Cashbook Ledger Transfer (RT)');
      }

      // nbank balance (always)
      await updateNbankBalance(trx, bankAccount, nbankDelta);

      // 4. anoml (transfer file)
      if (decision.post_to_transfer_file) {
        const doneFlag = decision.transfer_file_done_flag;
        const jrnlNum = decision.post_to_nominal ? nextJournal : 0;
        const anomlIdStart = await getNextId(trx, 'anoml', 2);

        await trx.raw(
          `INSERT INTO anoml (
              id, ax_nacnt, ax_ncntr, ax_source, ax_date, ax_value, ax_tref,
              ax_comment, ax_done, ax_fcurr, ax_fvalue, ax_fcrate, ax_fcmult, ax_fcdec,
              ax_srcco, ax_unique, ax_project, ax_job, ax_jrnl, ax_nlpdate,
              datecreated, datemodified, state
            ) VALUES (
              ?, ?, '    ', ?, ?, ?, ?,
              ?, ?, '   ', 0, 0, 0, 0,
              'I', ?, '        ', '        ', ?, ?,
              ?, ?, 1
            )`,
          [
            anomlIdStart,
            bankAccount,
            cfg.axSource,
            input.postDate,
            ntranBankValue,
            reference,
            axCommentLedger,
            doneFlag,
            atranUnique,
            jrnlNum,
            input.postDate,
            nowStr,
            nowStr,
          ],
        );
        await trx.raw(
          `INSERT INTO anoml (
              id, ax_nacnt, ax_ncntr, ax_source, ax_date, ax_value, ax_tref,
              ax_comment, ax_done, ax_fcurr, ax_fvalue, ax_fcrate, ax_fcmult, ax_fcdec,
              ax_srcco, ax_unique, ax_project, ax_job, ax_jrnl, ax_nlpdate,
              datecreated, datemodified, state
            ) VALUES (
              ?, ?, '    ', ?, ?, ?, ?,
              ?, ?, '   ', 0, 0, 0, 0,
              'I', ?, '        ', '        ', ?, ?,
              ?, ?, 1
            )`,
          [
            anomlIdStart + 1,
            controlAccount,
            cfg.axSource,
            input.postDate,
            ntranCtrlValue,
            reference,
            axCommentLedger,
            doneFlag,
            atranUnique,
            jrnlNum,
            input.postDate,
            nowStr,
            nowStr,
          ],
        );
      }

      // 5. stran or ptran
      if (cfg.isSales) {
        const memo = `${cfg.memoPrefix} - ${reference}`.slice(0, 200);
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
              ?, ?, ?, ?, ?, ?,
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
            subRowId,
            partnerAccount,
            input.postDate,
            reference,
            paymentMethod,
            cfg.subTrtype,
            subValue,
            subValue,
            input.postDate,
            memo,
            input.postDate,
            cbtype,
            entryNumber,
            stranUnique,
            partner.region.slice(0, 3),
            partner.terr.slice(0, 3),
            partner.type.slice(0, 3),
            partnerAccount,
            input.postDate,
            input.postDate,
            nowStr,
            nowStr,
          ],
        );
        await trx.raw(
          `UPDATE sname WITH (ROWLOCK)
              SET sn_currbal = sn_currbal + ?,
                  sn_nextpay = sn_nextpay + 1,
                  datemodified = ?
              WHERE RTRIM(sn_account) = ?`,
          [balanceDelta, nowStr, partnerAccount],
        );
      } else {
        await trx.raw(
          `INSERT INTO ptran (
              id, pt_account, pt_trdate, pt_trref, pt_supref, pt_trtype,
              pt_trvalue, pt_vatval, pt_trbal, pt_paid, pt_crdate,
              pt_advance, pt_payflag, pt_set1day, pt_set1, pt_set2day,
              pt_set2, pt_held, pt_fcurr, pt_fcrate, pt_fcdec,
              pt_fcval, pt_fcbal, pt_adval, pt_fadval, pt_fcmult,
              pt_cbtype, pt_entry, pt_unique, pt_suptype, pt_euro,
              pt_payadvl, pt_origcur, pt_eurind, pt_revchrg, pt_nlpdate,
              pt_adjsv, pt_vatset1, pt_vatset2, pt_pyroute, pt_fcvat,
              datecreated, datemodified, state
            ) VALUES (
              ?, ?, ?, ?, ?, ?,
              ?, 0, ?, ' ', ?,
              'N', 0, 0, 0, 0,
              0, ' ', '   ', 0, 0,
              0, 0, 0, 0, 0,
              ?, ?, ?, '   ', 0,
              0, '   ', ' ', 0, ?,
              0, 0, 0, 0, 0,
              ?, ?, 1
            )`,
          [
            subRowId,
            partnerAccount,
            input.postDate,
            reference,
            paymentMethod,
            cfg.subTrtype,
            subValue,
            subValue,
            input.postDate,
            cbtype,
            entryNumber,
            atranUnique,
            input.postDate,
            nowStr,
            nowStr,
          ],
        );
        await trx.raw(
          `UPDATE pname WITH (ROWLOCK)
              SET pn_currbal = pn_currbal + ?,
                  pn_nextpay = pn_nextpay + 1,
                  datemodified = ?
              WHERE RTRIM(pn_account) = ?`,
          [balanceDelta, nowStr, partnerAccount],
        );
      }
    });
  } catch (e: any) {
    return {
      success: false,
      records_processed: 1,
      records_failed: 1,
      errors: [e?.message ?? String(e)],
    };
  }

  const tablesUpdated = [
    'aentry',
    'atran',
    cfg.isSales ? 'stran' : 'ptran',
    cfg.isSales ? 'sname' : 'pname',
  ];
  if (decision.post_to_nominal) tablesUpdated.splice(2, 0, 'ntran (2)');
  if (decision.post_to_transfer_file) tablesUpdated.push('anoml (2)');
  const postingMode = decision.post_to_nominal
    ? 'Current period - posted to nominal'
    : 'Different period - transfer file only (pending NL post)';

  return {
    success: true,
    records_processed: 1,
    records_imported: 1,
    entry_number: entryNumber,
    transaction_ref: reference,
    warnings: [
      `Entry number: ${entryNumber}`,
      `Journal number: ${nextJournal}`,
      `Amount: £${amount.toFixed(2)}`,
      `Posting mode: ${postingMode}`,
      `Tables updated: ${tablesUpdated.join(', ')}`,
    ],
  };
}
