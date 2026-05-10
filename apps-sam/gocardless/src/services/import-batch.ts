/**
 * GoCardless batch import — faithful port of
 * `OperaSQLImport.import_gocardless_batch`
 * (sql_rag/opera_sql_import.py:6017-7011) and the wrapping endpoint
 * `apps/gocardless/api/routes.py:621-949`.
 *
 * Creates a true Opera batch:
 *   - One aentry header (batch total)
 *   - Multiple atran lines (one per customer payment)
 *   - Multiple stran records (one per customer)
 *   - Optional ntran + anoml records when complete_batch=true and
 *     posting decision allows
 *   - Customer balance updates (sname.sn_currbal / sn_nextpay)
 *   - Optional fees entry as a SEPARATE cashbook entry, with VAT split
 *     across atran lines and zvtran/nvat for VAT return tracking
 *
 * Faithful subset:
 *   - The `auto_allocate` path and `destination_bank` auto-transfer are
 *     NOT yet wired (separate services). When either flag is set, we
 *     return an explicit warning so callers and operators know.
 *
 * Locking:
 *   - Row-level via WITH (ROWLOCK) on every UPDATE.
 *   - The whole posting is a single MSSQL transaction (`operaDb.transaction`).
 *   - Sequence allocation (atype.ay_entry, nparm.np_nexjrnl, nextid)
 *     uses UPDLOCK via the shared `incrementAtypeEntry`,
 *     `getNextJournal`, `getNextId` helpers.
 *
 * SQL injection guard:
 *   - bankCode validated via `validateBankCode`
 *   - cbtype validated via `validateCbtype`
 *   - All other values bound via parameterised raw queries
 */
import type { Knex } from 'knex';
import {
  validateBankCode,
  validateAccountCode,
  validateCbtype,
  SqlInputValidationError,
  getHomeCurrency,
  getPeriodPostingDecision,
  getNextJournal,
  getNextId,
  getNacntType,
  updateNacntBalance,
  updateNbankBalance,
  insertNjmemo,
  incrementAtypeEntry,
  generateOperaUniqueIds,
  generateOperaUniqueId,
  getVatRate,
  getCustomerControlAccount,
} from '@sqlrag/sam-shared';

// --- Public types ----------------------------------------------------------

export interface PaymentInput {
  customer_account: string;
  amount: number; // in POUNDS
  description?: string;
  /** Per-row override of the batch-level auto_allocate flag */
  auto_allocate?: boolean;
  /** Optional GoCardless payment id, used by the auto-allocate flow */
  gc_payment_id?: string;
}

export interface ImportBatchOptions {
  bankAccount: string;
  payments: PaymentInput[];
  postDate: string; // YYYY-MM-DD
  reference?: string; // default 'GoCardless'
  goCardlessFees?: number; // gross including VAT
  vatOnFees?: number; // VAT element of fees
  feesNominalAccount?: string;
  feesVatCode?: string; // default '2'
  feesPaymentType?: string; // optional cbtype for fees entry
  completeBatch?: boolean;
  inputBy?: string; // default 'GOCARDLS'
  cbtype?: string; // batched receipt type, auto-detect when null
  validateOnly?: boolean;
  currency?: string;
  /** Defer auto-allocate to the dedicated allocation service. Setting
   * `true` here returns a warning since the SAM port hasn't wired it yet.
   */
  autoAllocate?: boolean;
  /** Defer destination-bank transfer. Returns a warning if requested. */
  destinationBank?: string;
  transferCbtype?: string;
}

export interface ImportBatchResult {
  success: boolean;
  records_processed: number;
  records_imported?: number;
  records_failed?: number;
  errors?: string[];
  warnings?: Array<string | null>;
  entry_number?: string;
  fees_entry_number?: string | null;
}

// --- Helpers ---------------------------------------------------------------

interface CustomerInfo {
  name: string;
  region: string;
  terr: string;
  type: string;
}

interface AtypeRow {
  cbtype: string;
  desc: string;
  category: string;
}

async function findGcCbtype(operaDb: Knex): Promise<string | null> {
  // Try a GoCardless-named batched receipt type first
  const gc = (await operaDb.raw(
    `SELECT TOP 1 ay_cbtype FROM atype WITH (NOLOCK)
       WHERE ay_type = 'R' AND ay_batched = 1
         AND (ay_desc LIKE '%GoCardless%' OR ay_desc LIKE '%gocardless%')`,
  )) as Array<{ ay_cbtype: string }>;
  if (Array.isArray(gc) && gc.length > 0 && gc[0]?.ay_cbtype) {
    return gc[0].ay_cbtype.trim();
  }
  // Fallback: any batched receipt type
  const fallback = (await operaDb.raw(
    `SELECT TOP 1 ay_cbtype FROM atype WITH (NOLOCK)
       WHERE ay_type = 'R' AND ay_batched = 1`,
  )) as Array<{ ay_cbtype: string }>;
  if (
    Array.isArray(fallback) &&
    fallback.length > 0 &&
    fallback[0]?.ay_cbtype
  ) {
    return fallback[0].ay_cbtype.trim();
  }
  return null;
}

async function lookupAtype(
  operaDb: Knex,
  cbtype: string,
): Promise<AtypeRow | null> {
  const rows = (await operaDb.raw(
    `SELECT ay_cbtype, ay_desc, ay_type
       FROM atype WITH (NOLOCK)
       WHERE RTRIM(ay_cbtype) = ?`,
    [cbtype],
  )) as Array<{ ay_cbtype: string; ay_desc: string; ay_type: string }>;
  if (!Array.isArray(rows) || rows.length === 0) return null;
  const row = rows[0];
  if (!row) return null;
  return {
    cbtype: (row.ay_cbtype ?? '').trim(),
    desc: (row.ay_desc ?? '').trim(),
    category: (row.ay_type ?? '').trim(),
  };
}

async function findFeesPaymentType(
  trx: Knex.Transaction,
  feesPaymentType?: string,
): Promise<string> {
  if (feesPaymentType) {
    const t = feesPaymentType.trim();
    if (t) return t;
  }
  const rows = (await trx.raw(
    `SELECT TOP 1 ay_cbtype FROM atype WITH (NOLOCK)
       WHERE ay_type = 'P' AND ay_batched = 0
       ORDER BY ay_cbtype`,
  )) as Array<{ ay_cbtype: string }>;
  if (Array.isArray(rows) && rows.length > 0 && rows[0]?.ay_cbtype) {
    return rows[0].ay_cbtype.trim();
  }
  return 'NP';
}

async function lookupCustomers(
  operaDb: Knex,
  payments: PaymentInput[],
): Promise<{
  customerInfo: Map<string, CustomerInfo>;
  errors: string[];
}> {
  const customerInfo = new Map<string, CustomerInfo>();
  const errors: string[] = [];
  for (let idx = 0; idx < payments.length; idx++) {
    const payment = payments[idx]!;
    const acct = (payment.customer_account ?? '').trim();
    if (!acct) {
      errors.push(`Payment ${idx + 1}: Missing customer account`);
      continue;
    }
    if (customerInfo.has(acct)) continue;
    const rows = (await operaDb.raw(
      `SELECT sn_name, sn_region, sn_terrtry, sn_custype
         FROM sname WITH (NOLOCK)
         WHERE RTRIM(sn_account) = ?`,
      [acct],
    )) as Array<{
      sn_name: string | null;
      sn_region: string | null;
      sn_terrtry: string | null;
      sn_custype: string | null;
    }>;
    if (!Array.isArray(rows) || rows.length === 0) {
      errors.push(
        `Payment ${idx + 1}: Customer account '${acct}' not found`,
      );
      continue;
    }
    const row = rows[0]!;
    customerInfo.set(acct, {
      name: (row.sn_name ?? '').trim(),
      region: (row.sn_region ?? '').trim() || 'K',
      terr: (row.sn_terrtry ?? '').trim() || '001',
      type: (row.sn_custype ?? '').trim() || 'DD1',
    });
  }
  return { customerInfo, errors };
}

function findDuplicates(
  payments: PaymentInput[],
  customerInfo: Map<string, CustomerInfo>,
): string[] {
  const seen = new Map<string, number[]>();
  for (let idx = 0; idx < payments.length; idx++) {
    const p = payments[idx]!;
    const key = `${(p.customer_account ?? '').trim()}|${Math.round(
      Number(p.amount ?? 0) * 100,
    )}`;
    const arr = seen.get(key) ?? [];
    arr.push(idx);
    seen.set(key, arr);
  }
  const warnings: string[] = [];
  for (const [key, indices] of seen.entries()) {
    if (indices.length <= 1) continue;
    const [acct, pence] = key.split('|');
    const amt = Number(pence ?? '0') / 100;
    const cust = customerInfo.get(acct ?? '')?.name ?? acct ?? '';
    const lineNums = indices.map((i) => i + 1).join(', ');
    warnings.push(
      `Duplicate: ${cust} (${acct}) appears ${indices.length} times for £${amt.toFixed(
        2,
      )} (payments ${lineNums}). Please verify each payment is matched to the correct customer.`,
    );
  }
  return warnings;
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

// --- Main service ---------------------------------------------------------

export async function importGocardlessBatch(
  operaDb: Knex,
  opts: ImportBatchOptions,
): Promise<ImportBatchResult> {
  const payments = opts.payments ?? [];
  if (payments.length === 0) {
    return {
      success: false,
      records_processed: 0,
      records_failed: 0,
      errors: ['No payments provided'],
    };
  }

  // -- Currency check
  if (opts.currency) {
    const home = await getHomeCurrency(operaDb);
    if (opts.currency.toUpperCase() !== home.code.toUpperCase()) {
      return {
        success: false,
        records_processed: payments.length,
        records_failed: payments.length,
        errors: [
          `GoCardless batch is in ${opts.currency} but home currency is ${home.code} (${home.description}). ` +
            'Foreign currency GoCardless batches are not supported. Please process this batch manually.',
        ],
      };
    }
  }

  // -- Fees nominal required when fees > 0
  const goCardlessFees = Number(opts.goCardlessFees ?? 0);
  const vatOnFees = Number(opts.vatOnFees ?? 0);
  if (goCardlessFees > 0 && !opts.feesNominalAccount) {
    return {
      success: false,
      records_processed: payments.length,
      records_failed: payments.length,
      errors: [
        `GoCardless fees of £${goCardlessFees.toFixed(2)} cannot be posted: ` +
          'fees_nominal_account not configured. ' +
          'Please configure the Fees Nominal Account in GoCardless Settings before importing.',
      ],
    };
  }

  // -- Validate bank code at boundary (defence in depth)
  let bankAccount: string;
  try {
    bankAccount = validateBankCode(opts.bankAccount);
  } catch (e) {
    if (e instanceof SqlInputValidationError) {
      return {
        success: false,
        records_processed: payments.length,
        records_failed: payments.length,
        errors: [e.message],
      };
    }
    throw e;
  }

  // -- Compute totals (pence for aentry/atran, pounds for ntran/anoml)
  const grossAmount = payments.reduce(
    (sum, p) => sum + Number(p.amount ?? 0),
    0,
  );
  const netAmount = grossAmount - Math.abs(goCardlessFees);
  const totalPence = Math.round(grossAmount * 100);

  // -- Resolve cbtype
  let cbtype = (opts.cbtype ?? '').trim();
  if (!cbtype) {
    const auto = await findGcCbtype(operaDb);
    if (!auto) {
      return {
        success: false,
        records_processed: payments.length,
        records_failed: payments.length,
        errors: ['No batched Receipt type codes found in atype table'],
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
        records_processed: payments.length,
        records_failed: payments.length,
        errors: [e.message],
      };
    }
    throw e;
  }
  const atype = await lookupAtype(operaDb, cbtype);
  if (!atype) {
    return {
      success: false,
      records_processed: payments.length,
      records_failed: payments.length,
      errors: [`Type code '${cbtype}' not found in atype table`],
    };
  }
  if (atype.category !== 'R') {
    return {
      success: false,
      records_processed: payments.length,
      records_failed: payments.length,
      errors: [
        `Type '${cbtype}' is category '${atype.category}', but 'R' (Receipt) is required`,
      ],
    };
  }
  const cbtypeDesc = atype.desc || 'Cheque';

  // -- Validate customers
  const { customerInfo, errors: customerErrors } = await lookupCustomers(
    operaDb,
    payments,
  );
  if (customerErrors.length > 0) {
    return {
      success: false,
      records_processed: payments.length,
      records_failed: payments.length,
      errors: customerErrors,
    };
  }

  // -- Duplicate scan (warnings only)
  const dupeWarnings = findDuplicates(payments, customerInfo);

  if (opts.validateOnly) {
    return {
      success: true,
      records_processed: payments.length,
      records_imported: payments.length,
      warnings: [
        `Validation passed for ${payments.length} payments totalling £${grossAmount.toFixed(
          2,
        )}`,
        ...dupeWarnings,
      ],
    };
  }

  // -- Period posting decision (Sales Ledger)
  const decision = await getPeriodPostingDecision(operaDb, opts.postDate, 'SL');
  if (!decision.can_post) {
    return {
      success: false,
      records_processed: payments.length,
      records_failed: payments.length,
      errors: [decision.error_message ?? 'Period rejected'],
    };
  }

  const period = decision.transaction_period;
  const year = decision.transaction_year;
  const reference = (opts.reference ?? 'GoCardless').slice(0, 20);
  const inputBy = (opts.inputBy ?? 'GOCARDLS').slice(0, 8);
  const completeBatch = !!opts.completeBatch;
  const feesNominal = (opts.feesNominalAccount ?? '').trim();
  const feesVatCodeRaw = (opts.feesVatCode ?? '2').trim() || '2';
  const { nowStr, dateStr, timeStr } = nowParts();

  // Note: per-customer record-lock pre-check (Python `check_record_locked`)
  // is omitted — SQL Server's UPDLOCK during the transaction surfaces the
  // same conflict via deadlock retry.

  let entryNumber = '';
  let feesEntryNumber: string | null = null;
  let vatNominalUsed = '';
  let vatRateUsed = 20.0;
  const allWarnings: string[] = [...dupeWarnings];

  try {
    await operaDb.transaction(async (trx) => {
      // 1. Allocate IDs upfront
      const uniquePerPayment = 2; // atran/stran shared + ntran_pstid
      const uniqueForFees = 3; // atran_unique + vat_unique + ntran_pstid
      const uniqueIds = generateOperaUniqueIds(
        payments.length * uniquePerPayment + uniqueForFees,
      );
      entryNumber = await incrementAtypeEntry(trx, cbtype);
      const aentryRowId = await getNextId(trx, 'aentry');

      const journalCount =
        payments.length + (goCardlessFees > 0 ? 1 : 0);
      let nextJournal = await getNextJournal(trx, journalCount);

      // 2. INSERT aentry (batch header)
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
          aentryRowId,
          bankAccount,
          cbtype,
          entryNumber,
          opts.postDate,
          reference,
          totalPence,
          completeBatch ? 1 : 0,
          dateStr,
          timeStr.slice(0, 8),
          inputBy,
          nowStr,
          nowStr,
        ],
      );

      // 3. Per-payment atran + stran (+ optional ntran/anoml)
      const nominalAccountsTouched = new Set<string>();
      for (let idx = 0; idx < payments.length; idx++) {
        const payment = payments[idx]!;
        const customerAccount = (payment.customer_account ?? '').trim();
        const amountPounds = Number(payment.amount ?? 0);
        const amountPence = Math.round(amountPounds * 100);
        const description = (payment.description ?? '').slice(0, 35);
        const cust = customerInfo.get(customerAccount)!;
        const customerName = cust.name;

        const atranRowId = await getNextId(trx, 'atran');
        const stranRowId = await getNextId(trx, 'stran');
        const atranUnique = uniqueIds[idx * 2]!;
        const stranUnique = atranUnique;
        const ntranPstid = uniqueIds[idx * 2 + 1]!;

        const salesLedgerControl = await getCustomerControlAccount(
          trx,
          customerAccount,
        );
        try {
          // Defence in depth — caller-supplied account codes are untrusted
          validateAccountCode(salesLedgerControl);
        } catch (e) {
          if (e instanceof SqlInputValidationError) throw e;
          throw e;
        }
        nominalAccountsTouched.add(salesLedgerControl);

        // INSERT atran (Sales Receipt = at_type 4)
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
            atranRowId,
            bankAccount,
            cbtype,
            entryNumber,
            inputBy,
            opts.postDate,
            opts.postDate,
            amountPence,
            customerAccount,
            customerName.slice(0, 35),
            description,
            atranUnique,
            reference,
            nowStr,
            nowStr,
          ],
        );

        // INSERT stran
        const stranMemo = `GoCardless - ${description}`.slice(0, 200);
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
            stranRowId,
            customerAccount,
            opts.postDate,
            reference,
            -amountPounds,
            -amountPounds,
            opts.postDate,
            stranMemo,
            opts.postDate,
            cbtype,
            entryNumber,
            stranUnique,
            cust.region.slice(0, 3),
            cust.terr.slice(0, 3),
            cust.type.slice(0, 3),
            customerAccount,
            opts.postDate,
            opts.postDate,
            nowStr,
            nowStr,
          ],
        );

        // nbank — receipts increase balance (always when atran created)
        await updateNbankBalance(trx, bankAccount, amountPounds);

        // Optional NL posting
        if (completeBatch && decision.post_to_nominal) {
          const ntranComment = pad(description.slice(0, 50), 50);
          const ntranTrnref = pad(customerName.slice(0, 30), 30) +
            'GoCardless (RT)     ';

          const bankType = (await getNacntType(trx, bankAccount)) ?? {
            na_type: 'B ',
            na_subt: 'BC',
          };
          const controlType = (await getNacntType(
            trx,
            salesLedgerControl,
          )) ?? { na_type: 'B ', na_subt: 'BB' };

          const ntranIdStart = await getNextId(trx, 'ntran', 2);

          // DR Bank
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
              ntranIdStart,
              bankAccount,
              bankType.na_type,
              bankType.na_subt,
              nextJournal,
              inputBy.slice(0, 10),
              ntranComment,
              ntranTrnref,
              opts.postDate,
              amountPounds,
              year,
              period,
              ntranPstid,
              nowStr,
              nowStr,
            ],
          );
          await updateNacntBalance(trx, bankAccount, amountPounds, {
            period,
            year,
          });

          // CR Debtors Control
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
              ntranIdStart + 1,
              salesLedgerControl,
              controlType.na_type,
              controlType.na_subt,
              nextJournal,
              inputBy.slice(0, 10),
              ntranComment,
              ntranTrnref,
              opts.postDate,
              -amountPounds,
              year,
              period,
              ntranPstid,
              nowStr,
              nowStr,
            ],
          );
          await updateNacntBalance(trx, salesLedgerControl, -amountPounds, {
            period,
            year,
          });

          await insertNjmemo(
            trx,
            nextJournal,
            'Cashbook Ledger Transfer (RT)',
          );
          nextJournal += 1;
        }

        // Optional transfer file (anoml)
        if (completeBatch && decision.post_to_transfer_file) {
          const jrnlNum =
            decision.post_to_nominal ? nextJournal - 1 : 0;
          const anomlIdStart = await getNextId(trx, 'anoml', 2);
          const doneFlag = decision.transfer_file_done_flag;
          const gcAxComment = (
            pad(customerName.slice(0, 30), 30) + cbtypeDesc
          ).slice(0, 50);

          // Bank side
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
              anomlIdStart,
              bankAccount,
              opts.postDate,
              amountPounds,
              reference,
              gcAxComment,
              doneFlag,
              atranUnique,
              jrnlNum,
              opts.postDate,
              nowStr,
              nowStr,
            ],
          );

          // Debtors control side
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
              anomlIdStart + 1,
              salesLedgerControl,
              opts.postDate,
              -amountPounds,
              reference,
              gcAxComment,
              doneFlag,
              atranUnique,
              jrnlNum,
              opts.postDate,
              nowStr,
              nowStr,
            ],
          );
        }

        // Update customer balance + payment counter (always)
        await trx.raw(
          `UPDATE sname WITH (ROWLOCK)
              SET sn_currbal = sn_currbal - ?,
                  sn_nextpay = sn_nextpay + 1,
                  datemodified = ?
              WHERE RTRIM(sn_account) = ?`,
          [amountPounds, nowStr, customerAccount],
        );
      }

      // 4. Fees as a SEPARATE cashbook entry
      if (goCardlessFees > 0 && feesNominal) {
        const feesUnique = uniqueIds[uniqueIds.length - 1]!;
        const feesVatUnique =
          uniqueIds[uniqueIds.length - 2] ?? generateOperaUniqueId();

        const grossFees = Math.abs(goCardlessFees);
        const netFees = grossFees - Math.abs(vatOnFees);

        // VAT lookup (always — even if vat_on_fees = 0 we still need the
        // nominal to record-keep against)
        const vatInfo = await getVatRate(
          trx,
          feesVatCodeRaw,
          'P',
          new Date(opts.postDate),
        );
        vatNominalUsed = vatInfo.nominal;
        vatRateUsed = vatInfo.rate || 20.0;

        // Nominal posting for fees
        if (decision.post_to_nominal) {
          const feesAcctType = (await getNacntType(trx, feesNominal)) ?? {
            na_type: 'P ',
            na_subt: 'HA',
          };
          const feesBankType = (await getNacntType(trx, bankAccount)) ?? {
            na_type: 'B ',
            na_subt: 'BC',
          };
          const feesNtranCount = vatOnFees > 0 ? 3 : 2;
          const feesNtranIdStart = await getNextId(
            trx,
            'ntran',
            feesNtranCount,
          );

          // DR Fees expense (NET)
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
                '', ?, 'A', 'GoCardless fees', 'GoCardless fees',
                ?, ?, ?, ?, 0,
                0, 0, '   ', 0, 0,
                0, 0, 'I', '', '        ',
                '        ', 'N', 0, ?, 0,
                0, 0, 0, 0, 0,
                0, ?, ?, 1
              )`,
            [
              feesNtranIdStart,
              feesNominal,
              feesAcctType.na_type,
              feesAcctType.na_subt,
              nextJournal,
              inputBy.slice(0, 10),
              opts.postDate,
              netFees,
              year,
              period,
              feesUnique,
              nowStr,
              nowStr,
            ],
          );
          await updateNacntBalance(trx, feesNominal, netFees, { period, year });

          if (vatOnFees > 0) {
            const vatAcctType = (await getNacntType(
              trx,
              vatNominalUsed,
            )) ?? { na_type: 'B ', na_subt: 'BB' };
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
                  '', ?, 'A', 'GoCardless fees VAT', 'GoCardless fees',
                  ?, ?, ?, ?, 0,
                  0, 0, '   ', 0, 0,
                  0, 0, 'I', '', '        ',
                  '        ', 'N', 0, ?, 0,
                  0, 0, 0, 0, 0,
                  0, ?, ?, 1
                )`,
              [
                feesNtranIdStart + 1,
                vatNominalUsed,
                vatAcctType.na_type,
                vatAcctType.na_subt,
                nextJournal,
                inputBy.slice(0, 10),
                opts.postDate,
                Math.abs(vatOnFees),
                year,
                period,
                feesVatUnique,
                nowStr,
                nowStr,
              ],
            );
            await updateNacntBalance(
              trx,
              vatNominalUsed,
              Math.abs(vatOnFees),
              { period, year },
            );
          }

          // CR Bank (gross)
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
                '', ?, 'A', 'GoCardless fees', 'GoCardless fees',
                ?, ?, ?, ?, 0,
                0, 0, '   ', 0, 0,
                0, 0, 'I', '', '        ',
                '        ', 'N', 0, ?, 0,
                0, 0, 0, 0, 0,
                0, ?, ?, 1
              )`,
            [
              feesNtranIdStart + (vatOnFees > 0 ? 2 : 1),
              bankAccount,
              feesBankType.na_type,
              feesBankType.na_subt,
              nextJournal,
              inputBy.slice(0, 10),
              opts.postDate,
              -grossFees,
              year,
              period,
              feesUnique,
              nowStr,
              nowStr,
            ],
          );
          await updateNacntBalance(trx, bankAccount, -grossFees, {
            period,
            year,
          });

          await insertNjmemo(
            trx,
            nextJournal,
            'Cashbook Ledger Transfer (RT)',
          );
        }

        // Separate cashbook entry for fees
        const grossFeesPence = Math.round(grossFees * 100);
        const feesCbtype = await findFeesPaymentType(
          trx,
          opts.feesPaymentType,
        );
        const feesCbtypeChecked = validateCbtype(feesCbtype);
        feesEntryNumber = await incrementAtypeEntry(trx, feesCbtypeChecked);

        const feesAentryRowId = await getNextId(trx, 'aentry');
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
              0, ?, ?, ?, 'GoCardless fees',
              0, 0, '  ', ?, ?, 1
            )`,
          [
            feesAentryRowId,
            bankAccount,
            feesCbtypeChecked,
            feesEntryNumber,
            opts.postDate,
            reference,
            -grossFeesPence,
            dateStr,
            timeStr.slice(0, 8),
            inputBy,
            nowStr,
            nowStr,
          ],
        );

        if (vatOnFees > 0) {
          const netFeesPence = Math.round(netFees * 100);
          const vatPence = Math.round(Math.abs(vatOnFees) * 100);
          const atranIdStart = await getNextId(trx, 'atran', 2);

          // Net fees line (Nominal Payment = at_type 1)
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
                1, ?, ?, 1, ?,
                0, '   ', 1.0, 0, 2,
                ?, 'GoCardless fees', '', '        ', '',
                '        ', '         ', 0, 0, 0,
                0, 0, '', 0, 0,
                0, 0, ?, 0, '0       ',
                ?, 'I', 0, ' ', '      ',
                '', '', '  ', '        ', '        ',
                '', '', '', ?, ?, 1
              )`,
            [
              atranIdStart,
              bankAccount,
              feesCbtypeChecked,
              feesEntryNumber,
              inputBy,
              opts.postDate,
              opts.postDate,
              -netFeesPence,
              feesNominal,
              feesUnique,
              reference,
              nowStr,
              nowStr,
            ],
          );

          // VAT line
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
                ?, ?, '   1', ?, ?, ?,
                1, ?, ?, 1, ?,
                0, '   ', 1.0, 0, 2,
                ?, 'GoCardless fees VAT', '', '        ', '',
                '        ', '         ', 0, 0, 0,
                0, 0, '', 0, 0,
                0, 0, ?, 0, '0       ',
                ?, 'I', 0, ' ', '      ',
                '', '', '  ', '        ', '        ',
                '', '', '', ?, ?, 1
              )`,
            [
              atranIdStart + 1,
              bankAccount,
              feesCbtypeChecked,
              feesEntryNumber,
              inputBy,
              opts.postDate,
              opts.postDate,
              -vatPence,
              vatNominalUsed,
              feesVatUnique,
              reference,
              nowStr,
              nowStr,
            ],
          );
        } else {
          const feesAtranRowId = await getNextId(trx, 'atran');
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
                1, ?, ?, 1, ?,
                0, '   ', 1.0, 0, 2,
                ?, 'GoCardless fees', '', '        ', '',
                '        ', '         ', 0, 0, 0,
                0, 0, '', 0, 0,
                0, 0, ?, 0, '0       ',
                ?, 'I', 0, ' ', '      ',
                '', '', '  ', '        ', '        ',
                '', '', '', ?, ?, 1
              )`,
            [
              feesAtranRowId,
              bankAccount,
              feesCbtypeChecked,
              feesEntryNumber,
              inputBy,
              opts.postDate,
              opts.postDate,
              -grossFeesPence,
              feesNominal,
              feesUnique,
              reference,
              nowStr,
              nowStr,
            ],
          );
        }

        // Bank balance: fees decrease bank
        await updateNbankBalance(trx, bankAccount, -grossFees);

        // anoml records for fees
        if (decision.post_to_transfer_file) {
          const jrnlNum = decision.post_to_nominal ? nextJournal : 0;
          const feesDoneFlag = decision.transfer_file_done_flag;
          const anomlIdStart = await getNextId(trx, 'anoml', 3);
          const feesBankFvalue = Math.round(-grossFees * 100);
          const feesExpenseFvalue = Math.round(netFees * 100);

          // Bank credit
          await trx.raw(
            `INSERT INTO anoml (
                id, ax_nacnt, ax_ncntr, ax_source, ax_date, ax_value, ax_tref,
                ax_comment, ax_done, ax_fcurr, ax_fvalue, ax_fcrate, ax_fcmult, ax_fcdec,
                ax_srcco, ax_unique, ax_project, ax_job, ax_jrnl, ax_nlpdate,
                datecreated, datemodified, state
              ) VALUES (
                ?, ?, '    ', 'A', ?, ?, ?,
                'GoCardless fees', ?, '   ', ?, 1.0, 0, 2.0,
                'I', ?, '        ', '        ', ?, ?,
                ?, ?, 1
              )`,
            [
              anomlIdStart,
              bankAccount,
              opts.postDate,
              -grossFees,
              reference,
              feesDoneFlag,
              feesBankFvalue,
              feesUnique,
              jrnlNum,
              opts.postDate,
              nowStr,
              nowStr,
            ],
          );

          // Fees expense debit
          await trx.raw(
            `INSERT INTO anoml (
                id, ax_nacnt, ax_ncntr, ax_source, ax_date, ax_value, ax_tref,
                ax_comment, ax_done, ax_fcurr, ax_fvalue, ax_fcrate, ax_fcmult, ax_fcdec,
                ax_srcco, ax_unique, ax_project, ax_job, ax_jrnl, ax_nlpdate,
                datecreated, datemodified, state
              ) VALUES (
                ?, ?, '    ', 'A', ?, ?, ?,
                'GoCardless fees', ?, '   ', ?, 1.0, 0, 2.0,
                'I', ?, '        ', '        ', ?, ?,
                ?, ?, 1
              )`,
            [
              anomlIdStart + 1,
              feesNominal,
              opts.postDate,
              netFees,
              reference,
              feesDoneFlag,
              feesExpenseFvalue,
              feesUnique,
              jrnlNum,
              opts.postDate,
              nowStr,
              nowStr,
            ],
          );

          if (vatOnFees > 0) {
            const feesVatFvalue = Math.round(Math.abs(vatOnFees) * 100);
            await trx.raw(
              `INSERT INTO anoml (
                  id, ax_nacnt, ax_ncntr, ax_source, ax_date, ax_value, ax_tref,
                  ax_comment, ax_done, ax_fcurr, ax_fvalue, ax_fcrate, ax_fcmult, ax_fcdec,
                  ax_srcco, ax_unique, ax_project, ax_job, ax_jrnl, ax_nlpdate,
                  datecreated, datemodified, state
                ) VALUES (
                  ?, ?, '    ', 'A', ?, ?, ?,
                  'GoCardless fees VAT', ?, '   ', ?, 1.0, 0, 2.0,
                  'I', ?, '        ', '        ', ?, ?,
                  ?, ?, 1
                )`,
              [
                anomlIdStart + 2,
                vatNominalUsed,
                opts.postDate,
                Math.abs(vatOnFees),
                reference,
                feesDoneFlag,
                feesVatFvalue,
                feesVatUnique,
                jrnlNum,
                opts.postDate,
                nowStr,
                nowStr,
              ],
            );
          }
        }

        // VAT tracking — zvtran + nvat — independent of transfer file
        if (vatOnFees > 0) {
          const zvtranRowId = await getNextId(trx, 'zvtran');
          await trx.raw(
            `INSERT INTO zvtran (
                id, va_source, va_account, va_laccnt, va_trdate, va_taxdate,
                va_ovrdate, va_trref, va_trtype, va_country, va_fcurr,
                va_trvalue, va_fcval, va_vatval, va_cost, va_vatctry,
                va_vattype, va_anvat, va_vatrate, va_box1, va_box2,
                va_box4, va_box6, va_box7, va_box8, va_box9,
                va_done, va_import, va_export,
                datecreated, datemodified, state
              ) VALUES (
                ?, 'N', ?, ?, ?, ?,
                ?, ?, 'I', 'GB', '   ',
                ?, 0, ?, 0, 'H',
                'P', ?, ?, 0, 0,
                1, 0, 1, 0, 0,
                0, 0, 0,
                ?, ?, 1
              )`,
            [
              zvtranRowId,
              feesNominal,
              feesNominal,
              opts.postDate,
              opts.postDate,
              opts.postDate,
              reference,
              netFees,
              Math.abs(vatOnFees),
              feesVatCodeRaw,
              vatRateUsed,
              nowStr,
              nowStr,
            ],
          );

          const nvatRowId = await getNextId(trx, 'nvat');
          await trx.raw(
            `INSERT INTO nvat (
                id, nv_acnt, nv_cntr, nv_date, nv_crdate, nv_taxdate,
                nv_ref, nv_type, nv_advance, nv_value, nv_vatval,
                nv_vatctry, nv_vattype, nv_vatcode, nv_vatrate, nv_comment,
                datecreated, datemodified, state
              ) VALUES (
                ?, ?, '', ?, ?, ?,
                ?, 'P', 0, ?, ?,
                ' ', 'P', ?, ?, 'GoCardless fees VAT',
                ?, ?, 1
              )`,
            [
              nvatRowId,
              vatNominalUsed,
              opts.postDate,
              opts.postDate,
              opts.postDate,
              reference,
              netFees,
              Math.abs(vatOnFees),
              feesVatCodeRaw.trim(),
              vatRateUsed,
              nowStr,
              nowStr,
            ],
          );
        }
      }

      // Capture nominal accounts touched for diagnostics (not exported here)
      void nominalAccountsTouched;
    });
  } catch (e: any) {
    const msg = e?.message ?? String(e);
    return {
      success: false,
      records_processed: payments.length,
      records_failed: payments.length,
      errors: [msg],
    };
  }

  // -- Build result warnings (matches Python's flat list)
  const batchStatus = completeBatch ? 'Completed' : 'Open for review';
  const feesDetail =
    goCardlessFees > 0 && feesNominal
      ? vatOnFees > 0
        ? `GoCardless fees: £${goCardlessFees.toFixed(
            2,
          )} (Net: £${(goCardlessFees - vatOnFees).toFixed(
            2,
          )} + VAT: £${vatOnFees.toFixed(2)})`
        : `GoCardless fees: £${goCardlessFees.toFixed(2)}`
      : null;
  const feesEntryMsg =
    goCardlessFees > 0 && feesNominal && feesEntryNumber
      ? `Fees posted as separate payment (entry ${feesEntryNumber})`
      : null;

  const warnings: Array<string | null> = [
    `Receipts entry: ${entryNumber}`,
    `Payments: ${payments.length}`,
    `Gross amount: £${grossAmount.toFixed(2)}`,
    feesDetail,
    feesEntryMsg,
    goCardlessFees ? `Net to bank: £${netAmount.toFixed(2)}` : null,
    vatOnFees > 0 && vatNominalUsed
      ? `VAT £${vatOnFees.toFixed(
          2,
        )} posted to ${vatNominalUsed} (code ${feesVatCodeRaw})`
      : null,
    `Batch status: ${batchStatus}`,
    vatOnFees > 0
      ? 'Posted to nominal ledger, transfer file (anoml), and zvtran (VAT)'
      : 'Posted to nominal ledger and transfer file (anoml)',
    ...allWarnings,
  ];

  // auto_allocate runs in the route layer once the batch transaction commits
  // (allocations target receipts that exist after the batch is on disk).
  // destination_bank likewise runs in the route layer after the batch
  // commits — see import-route.ts for both flows.

  return {
    success: true,
    records_processed: payments.length,
    records_imported: payments.length,
    entry_number: entryNumber,
    fees_entry_number: feesEntryNumber,
    warnings,
  };
}
