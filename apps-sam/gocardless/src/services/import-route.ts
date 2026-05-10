/**
 * GoCardless `/api/gocardless/import` route orchestration.
 *
 * Faithful port of the wrapping endpoint
 * `apps/gocardless/api/routes.py:621-949`. Layered on top of
 * `importGocardlessBatch` (the Opera writer) — adds:
 *   - SQL-injection guards on bank_code / cbtype / fees nominal
 *   - Idempotency gate (refuses if payout_id already imported)
 *   - Mandate verification (mandate_id → opera_account check;
 *     refuses if posting account differs from mandate-linked account)
 *   - Destination bank resolution from sort code + account number
 *   - GC control bank vs destination bank routing
 *   - Bank existence check in nbank for both posting + destination
 *   - Bank-level import lock (per-app `import_locks`)
 *   - On success, write to `gocardless_imports` history
 *
 * Returned response shape mirrors the Python route:
 *   { success, message?, payments_imported?, complete?, details?,
 *     duplicate_payout?, error? }
 */
import type { Knex } from 'knex';
import {
  validateBankCode,
  validateAccountCode,
  validateCbtype,
  SqlInputValidationError,
  importBankTransfer,
} from '@sqlrag/sam-shared';
import {
  importGocardlessBatch,
  type PaymentInput,
  type ImportBatchResult,
} from './import-batch.js';
import { isPayoutImported } from './import-idempotency.js';
import {
  withImportLock,
  ImportLockError,
} from './import-lock.js';
import { loadSettings } from './settings.js';
import { autoAllocateReceipt } from './auto-allocate-receipt.js';

// --- Public types --------------------------------------------------

export interface ImportPayloadPayment {
  customer_account: string;
  customer_name?: string;
  opera_customer_name?: string;
  amount: number | string;
  description?: string;
  auto_allocate?: boolean;
  gc_payment_id?: string;
  mandate_id?: string;
}

export interface ImportRouteInput {
  bankCode: string;
  postDate: string;
  reference?: string;
  completeBatch?: boolean;
  cbtype?: string;
  goCardlessFees?: number;
  vatOnFees?: number;
  feesNominalAccount?: string;
  feesVatCode?: string;
  feesPaymentType?: string;
  currency?: string;
  payoutId?: string;
  source?: string;
  destBankAccount?: string;
  destBankSortCode?: string;
  payments: ImportPayloadPayment[];
  /** Defaults to 'opera_se' — used when writing the history row */
  targetSystem?: string;
}

export interface ImportRouteResult {
  success: boolean;
  message?: string;
  payments_imported?: number;
  payments_processed?: number;
  complete?: boolean;
  details?: string[];
  duplicate_payout?: boolean;
  error?: string;
}

// --- Helpers -------------------------------------------------------

function normalize(s: string): string {
  return (s ?? '').replace(/[\s-]/g, '').trim();
}

async function resolveDestinationBank(
  operaDb: Knex,
  fallbackBank: string,
  destSort?: string,
  destAccount?: string,
): Promise<string> {
  if (!destSort && !destAccount) return fallbackBank;
  try {
    const rows = (await operaDb.raw(
      `SELECT RTRIM(nk_acnt) AS nk_acnt,
              RTRIM(ISNULL(nk_sort, '')) AS nk_sort,
              RTRIM(ISNULL(nk_number, '')) AS nk_number
         FROM nbank WITH (NOLOCK)`,
    )) as Array<{ nk_acnt: string; nk_sort: string; nk_number: string }>;
    if (!Array.isArray(rows) || rows.length === 0) return fallbackBank;
    const ns = normalize(destSort ?? '');
    const na = normalize(destAccount ?? '');
    for (const row of rows) {
      const dbSort = normalize(row.nk_sort);
      const dbAcct = normalize(row.nk_number);
      const sortMatch = !!(ns && dbSort && ns === dbSort);
      const acctMatch = !!(
        na &&
        dbAcct &&
        (dbAcct.endsWith(na) || na.endsWith(dbAcct) || dbAcct === na)
      );
      if (sortMatch && acctMatch) return row.nk_acnt;
      if (sortMatch && !na) return row.nk_acnt;
    }
  } catch {
    // best-effort
  }
  return fallbackBank;
}

async function bankExists(operaDb: Knex, code: string): Promise<boolean> {
  const rows = (await operaDb.raw(
    `SELECT TOP 1 nk_acnt FROM nbank WITH (NOLOCK)
       WHERE RTRIM(nk_acnt) = ?`,
    [code.trim()],
  )) as Array<{ nk_acnt: string }>;
  return Array.isArray(rows) && rows.length > 0;
}

interface MandateRow {
  mandate_id: string;
  opera_account: string;
}

async function loadMandateMap(
  appDb: Knex,
): Promise<Map<string, string>> {
  const rows = (await appDb('gocardless_mandates')
    .select('mandate_id', 'opera_account')) as MandateRow[];
  const map = new Map<string, string>();
  for (const r of rows ?? []) {
    const mid = (r.mandate_id ?? '').trim();
    const acct = (r.opera_account ?? '').trim();
    if (mid && acct && acct !== '__UNLINKED__') {
      map.set(mid, acct);
    }
  }
  return map;
}

async function recordImportHistory(
  appDb: Knex,
  data: {
    payoutId?: string;
    source: string;
    targetSystem: string;
    bankReference: string;
    grossAmount: number;
    netAmount: number;
    gocardlessFees: number;
    vatOnFees: number;
    paymentCount: number;
    payments: ImportPayloadPayment[];
    batchRef?: string | null;
    importedBy: string;
    postDate: string;
  },
): Promise<void> {
  try {
    const paymentsJson = JSON.stringify(
      data.payments.map((p) => ({
        customer_account: p.customer_account,
        gc_customer_name: p.customer_name ?? '',
        opera_customer_name: p.opera_customer_name ?? '',
        amount: typeof p.amount === 'string' ? Number(p.amount) : p.amount,
        description: p.description ?? '',
      })),
    );
    await appDb('gocardless_imports').insert({
      payout_id: data.payoutId ?? null,
      source: data.source,
      target_system: data.targetSystem,
      bank_reference: data.bankReference,
      gross_amount: data.grossAmount,
      net_amount: data.netAmount,
      gocardless_fees: data.gocardlessFees,
      vat_on_fees: data.vatOnFees,
      payment_count: data.paymentCount,
      payments_json: paymentsJson,
      batch_ref: data.batchRef ?? null,
      import_date: new Date().toISOString(),
      imported_by: data.importedBy,
      post_date: data.postDate,
    });
  } catch {
    // history write failure is non-fatal — Python logs and continues
  }
}

// --- Main service ---------------------------------------------------

export async function importGocardlessRoute(
  appDb: Knex,
  operaDb: Knex,
  input: ImportRouteInput,
): Promise<ImportRouteResult> {
  // -- Validate boundary inputs
  let bankCode: string;
  let cbtype: string | undefined;
  let feesNominal: string | undefined;
  try {
    bankCode = validateBankCode(input.bankCode);
    cbtype = input.cbtype ? validateCbtype(input.cbtype) : undefined;
    feesNominal = input.feesNominalAccount
      ? validateAccountCode(input.feesNominalAccount)
      : undefined;
  } catch (e) {
    if (e instanceof SqlInputValidationError) {
      return { success: false, error: e.message };
    }
    throw e;
  }

  if (!input.payments || input.payments.length === 0) {
    return { success: false, error: 'No payments provided' };
  }

  // -- Idempotency
  const payoutId = (input.payoutId ?? '').trim();
  if (payoutId) {
    const already = await isPayoutImported(appDb, payoutId);
    if (already) {
      return {
        success: false,
        error:
          `Payout ${payoutId} has already been imported. Refusing to ` +
          'post the same payout twice. If you genuinely need to re-post, ' +
          'reverse the original first.',
        duplicate_payout: true,
      };
    }
  }

  // -- Validate per-payment shape
  const validated: PaymentInput[] = [];
  const validatedRaw: ImportPayloadPayment[] = [];
  for (let idx = 0; idx < input.payments.length; idx++) {
    const p = input.payments[idx]!;
    const acct = (p.customer_account ?? '').trim();
    const amt = typeof p.amount === 'string' ? Number(p.amount) : p.amount;
    if (!acct) {
      return {
        success: false,
        error: `Payment ${idx + 1}: Missing customer_account`,
      };
    }
    if (!amt || Number.isNaN(amt)) {
      return {
        success: false,
        error: `Payment ${idx + 1}: Missing amount`,
      };
    }
    validated.push({
      customer_account: acct,
      amount: amt,
      description: (p.description ?? '').slice(0, 35),
      ...(p.auto_allocate !== undefined ? { auto_allocate: p.auto_allocate } : {}),
      ...(p.gc_payment_id ? { gc_payment_id: p.gc_payment_id } : {}),
    });
    validatedRaw.push({ ...p, customer_account: acct, amount: amt });
  }

  // -- Mandate verification
  const mandateMap = await loadMandateMap(appDb);
  for (let idx = 0; idx < validatedRaw.length; idx++) {
    const vp = validatedRaw[idx]!;
    const mandateId = (vp.mandate_id ?? '').trim();
    if (!mandateId) continue;
    const expected = mandateMap.get(mandateId);
    if (expected && expected !== vp.customer_account) {
      return {
        success: false,
        error:
          `Payment ${idx + 1}: mandate ${mandateId} belongs to account ${expected}, ` +
          `but is being posted to ${vp.customer_account} (${vp.customer_name ?? ''}). ` +
          'Please correct the customer match before importing.',
      };
    }
  }

  // -- Settings: control bank + transfer cbtype
  const settings = await loadSettings(appDb).catch(() => null);
  const gcBank = (settings?.gocardless_bank_code ?? '').trim();
  const transferCbtype =
    (settings?.gocardless_transfer_cbtype ?? '').trim() || undefined;

  // -- Destination bank resolution
  const resolvedDest = await resolveDestinationBank(
    operaDb,
    bankCode,
    input.destBankSortCode,
    input.destBankAccount,
  );
  const destinationBank =
    gcBank && resolvedDest.trim() !== gcBank ? resolvedDest : undefined;
  const postingBank = gcBank || resolvedDest;

  // -- Bank existence checks
  const banksToCheck = [postingBank];
  if (destinationBank) banksToCheck.push(destinationBank);
  for (const b of banksToCheck) {
    if (!(await bankExists(operaDb, b))) {
      const label =
        b === postingBank ? 'GC Control bank' : 'Destination bank';
      return {
        success: false,
        error:
          `${label} '${b}' does not exist in this company's bank accounts. ` +
          'Please update GoCardless Settings with valid bank codes for this company.',
      };
    }
  }

  // -- Acquire bank-level lock & import
  let result: ImportBatchResult;
  try {
    result = await withImportLock(
      appDb,
      postingBank,
      { locked_by: 'api', endpoint: 'gocardless-import' },
      () =>
        importGocardlessBatch(operaDb, {
          bankAccount: postingBank,
          payments: validated,
          postDate: input.postDate,
          ...(input.reference !== undefined ? { reference: input.reference } : {}),
          ...(input.goCardlessFees !== undefined
            ? { goCardlessFees: input.goCardlessFees }
            : {}),
          ...(input.vatOnFees !== undefined
            ? { vatOnFees: input.vatOnFees }
            : {}),
          ...(feesNominal ? { feesNominalAccount: feesNominal } : {}),
          ...(input.feesVatCode !== undefined
            ? { feesVatCode: input.feesVatCode }
            : {}),
          ...(input.feesPaymentType !== undefined
            ? { feesPaymentType: input.feesPaymentType }
            : {}),
          ...(input.completeBatch !== undefined
            ? { completeBatch: input.completeBatch }
            : {}),
          ...(cbtype ? { cbtype } : {}),
          ...(input.currency !== undefined
            ? { currency: input.currency }
            : {}),
          autoAllocate: true,
          ...(destinationBank ? { destinationBank } : {}),
          ...(transferCbtype ? { transferCbtype } : {}),
        }),
    );
  } catch (e) {
    if (e instanceof ImportLockError) {
      return { success: false, error: e.message };
    }
    throw e;
  }

  // -- On failure
  if (!result.success) {
    return {
      success: false,
      error: (result.errors ?? ['Import failed']).join('; '),
      payments_processed: result.records_processed,
    };
  }

  // -- On success: write history
  const grossAmount = validated.reduce((s, p) => s + Number(p.amount ?? 0), 0);
  const goCardlessFees = Number(input.goCardlessFees ?? 0);
  const netAmount = grossAmount - goCardlessFees;
  await recordImportHistory(appDb, {
    ...(payoutId ? { payoutId } : {}),
    source: (input.source ?? 'api').trim() || 'api',
    targetSystem: (input.targetSystem ?? 'opera_se').trim() || 'opera_se',
    bankReference: (input.reference ?? 'GoCardless').trim(),
    grossAmount,
    netAmount,
    gocardlessFees: goCardlessFees,
    vatOnFees: Number(input.vatOnFees ?? 0),
    paymentCount: validated.length,
    payments: validatedRaw,
    ...(result.entry_number ? { batchRef: result.entry_number } : {}),
    importedBy: 'GOCARDLS',
    postDate: input.postDate,
  });

  const details = (result.warnings ?? []).filter(
    (w): w is string => typeof w === 'string' && w.length > 0,
  );

  // -- Auto-allocate per-payment (Python's "auto_allocate=True" loop)
  const allocationDetails: string[] = [];
  for (const p of validatedRaw) {
    if (p.auto_allocate === false) {
      allocationDetails.push(
        `Allocation disabled for ${p.customer_account}: posted on account`,
      );
      continue;
    }
    try {
      const alloc = await autoAllocateReceipt(operaDb, appDb, {
        customerAccount: p.customer_account,
        receiptRef: (input.reference ?? 'GoCardless').trim(),
        receiptAmount: Number(p.amount),
        allocationDate: input.postDate,
        bankAccount: postingBank,
        ...(p.description ? { description: p.description } : {}),
        ...(p.gc_payment_id ? { gcPaymentId: p.gc_payment_id } : {}),
      });
      if (alloc.success) {
        const note =
          alloc.allocation_method === 'payment_request'
            ? ' (payment_request)'
            : '';
        allocationDetails.push(
          `Auto-allocated ${p.customer_account}: £${alloc.allocated_amount.toFixed(
            2,
          )} to ${alloc.allocations.length} invoice(s)${note}`,
        );
      } else {
        allocationDetails.push(
          `Allocation skipped for ${p.customer_account}: ${alloc.message}`,
        );
      }
    } catch (e: any) {
      allocationDetails.push(
        `Allocation failed for ${p.customer_account}: ${e?.message ?? String(e)}`,
      );
    }
  }
  const successfulAllocs = allocationDetails.filter((d) =>
    d.includes('Auto-allocated'),
  );
  if (successfulAllocs.length > 0) {
    details.push(`Auto-allocation: ${successfulAllocs.length} receipt(s) allocated`);
  }
  details.push(...allocationDetails);

  // -- Auto-transfer net to destination bank (Python's destination_bank flow)
  if (destinationBank) {
    const netAmount = grossAmount - goCardlessFees;
    try {
      const xfer = await importBankTransfer(operaDb, {
        sourceBank: postingBank,
        destBank: destinationBank,
        amountPounds: netAmount,
        reference: (input.reference ?? 'GoCardless').slice(0, 20),
        postDate: input.postDate,
        comment: 'GoCardless payout transfer',
        inputBy: 'GOCARDLS',
        ...(transferCbtype ? { cbtype: transferCbtype } : {}),
      });
      if (xfer.success) {
        details.push(
          `Net £${netAmount.toFixed(2)} transferred from ${postingBank} to ${destinationBank}`,
        );
      } else {
        details.push(
          `Transfer to ${destinationBank} failed: ${xfer.error} — post manually`,
        );
      }
    } catch (e: any) {
      details.push(
        `Transfer to ${destinationBank} failed: ${e?.message ?? String(e)} — post manually`,
      );
    }
  }

  return {
    success: true,
    message: `Successfully imported ${validated.length} payments`,
    payments_imported: result.records_imported ?? validated.length,
    complete: !!input.completeBatch,
    details,
  };
}
