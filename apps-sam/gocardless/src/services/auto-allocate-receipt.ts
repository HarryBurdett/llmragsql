/**
 * Auto-allocate a sales receipt to outstanding invoices.
 *
 * Faithful port of `OperaSQLImport.auto_allocate_receipt`
 * (sql_rag/opera_sql_import.py:7017-7425).
 *
 * Allocation rules (priority order, matches Python):
 *   0. If gc_payment_id is supplied → look up the original payment
 *      request's stored invoice_refs; allocate to those that are
 *      still outstanding.
 *   1. If invoice references found in the description (e.g. "INV12345")
 *      AND their total matches the receipt exactly → allocate to those.
 *   2. If receipt amount equals total outstanding on account AND there
 *      is at least one invoice → allocate to ALL invoices (clears the
 *      account, no ambiguity).
 *
 * Does NOT allocate based on amount alone — too risky with duplicates.
 *
 * Locking: all writes inside a single Opera SQL transaction. salloc
 * payflag allocation uses UPDLOCK on the customer's existing salloc
 * rows so concurrent allocations get serialised.
 */
import type { Knex } from 'knex';
import { getNextId } from '@sqlrag/sam-shared';

export interface AutoAllocateInput {
  customerAccount: string;
  receiptRef: string;
  receiptAmount: number; // POUNDS, positive
  allocationDate: string; // YYYY-MM-DD
  bankAccount?: string;
  description?: string;
  /** GoCardless payment id — triggers Rule 0 (payment request lookup) */
  gcPaymentId?: string;
}

export interface AllocationLine {
  ref: string;
  custref: string;
  amount: number;
  full_allocation: boolean;
  unique: string;
  stran_id: number;
}

export interface AutoAllocateResult {
  success: boolean;
  allocated_amount: number;
  allocations: AllocationLine[];
  message: string;
  receipt_fully_allocated?: boolean;
  allocation_method?:
    | 'payment_request'
    | 'invoice_reference'
    | 'clears_account'
    | 'single_invoice_match'
    | null;
}

interface PaymentRequest {
  invoice_refs?: string[] | null;
}

/**
 * Loader for the payment-request invoice_refs. Falls back to null
 * when the table doesn't exist or returns nothing.
 */
async function loadPaymentRequest(
  appDb: Knex | null,
  gcPaymentId: string,
): Promise<PaymentRequest | null> {
  if (!appDb || !gcPaymentId) return null;
  try {
    const row = (await appDb('gocardless_payment_requests')
      .where({ gc_payment_id: gcPaymentId })
      .first()) as
      | { invoice_refs: string | string[] | null }
      | undefined;
    if (!row) return null;
    let refs = row.invoice_refs;
    if (typeof refs === 'string') {
      try {
        refs = JSON.parse(refs);
      } catch {
        refs = null;
      }
    }
    if (!Array.isArray(refs) || refs.length === 0) return null;
    return { invoice_refs: refs.map((s) => String(s).trim()).filter(Boolean) };
  } catch {
    return null;
  }
}

interface ReceiptRow {
  id: number;
  st_trref: string;
  st_trvalue: number;
  st_trbal: number;
  st_paid: string;
  st_custref: string | null;
  st_unique: string | null;
  st_trdate: string;
}

interface InvoiceRow {
  id: number;
  st_trref: string;
  st_trvalue: number;
  st_trbal: number;
  st_custref: string | null;
  st_trdate: string;
  st_unique: string | null;
}

const INV_REF_RE = /INV\d+/g;

export async function autoAllocateReceipt(
  operaDb: Knex,
  appDb: Knex | null,
  input: AutoAllocateInput,
): Promise<AutoAllocateResult> {
  const customerAccount = (input.customerAccount ?? '').trim();
  const receiptRef = (input.receiptRef ?? '').trim();
  const receiptAmount = Number(input.receiptAmount ?? 0);
  const bankAccount = (input.bankAccount ?? '').trim();
  const description = input.description ?? '';
  const gcPaymentId = (input.gcPaymentId ?? '').trim();

  const result: AutoAllocateResult = {
    success: false,
    allocated_amount: 0,
    allocations: [],
    message: '',
    allocation_method: null,
  };

  if (!customerAccount || !receiptRef) {
    result.message = 'Missing customer account or receipt reference';
    return result;
  }

  try {
    // Locate the receipt row (closest match to receipt_amount)
    const receiptRows = (await operaDb.raw(
      `SELECT id, st_trref, st_trvalue, st_trbal, st_paid, st_custref, st_unique, st_trdate
         FROM stran WITH (NOLOCK)
         WHERE st_account = ?
           AND RTRIM(st_trref) = ?
           AND st_trtype = 'R'
           AND st_trbal < 0
         ORDER BY ABS(ABS(st_trbal) - ?) ASC`,
      [customerAccount, receiptRef, receiptAmount],
    )) as ReceiptRow[];

    if (!Array.isArray(receiptRows) || receiptRows.length === 0) {
      result.message = `Receipt ${receiptRef} not found or already allocated`;
      return result;
    }
    const receipt = receiptRows[0]!;
    const receiptBalance = Math.abs(Number(receipt.st_trbal));
    const receiptUnique = (receipt.st_unique ?? '').trim();
    const receiptStranId = Number(receipt.id);
    if (receiptBalance <= 0) {
      result.message = 'Receipt already fully allocated';
      return result;
    }

    // Outstanding invoices
    const invoiceRows = (await operaDb.raw(
      `SELECT id, st_trref, st_trvalue, st_trbal, st_custref, st_trdate, st_unique
         FROM stran WITH (NOLOCK)
         WHERE st_account = ?
           AND st_trtype = 'I'
           AND st_trbal > 0
         ORDER BY st_trdate ASC, st_trref ASC`,
      [customerAccount],
    )) as InvoiceRow[];

    if (!Array.isArray(invoiceRows) || invoiceRows.length === 0) {
      result.message = 'No outstanding invoices found for customer';
      return result;
    }

    let invoicesToAllocate: AllocationLine[] = [];
    let allocationMethod: AutoAllocateResult['allocation_method'] = null;

    const totalOutstanding = Number(
      invoiceRows
        .reduce((s, r) => s + Number(r.st_trbal), 0)
        .toFixed(2),
    );
    const receiptRounded = Number(receiptAmount.toFixed(2));

    // RULE 0: payment-request invoice refs
    if (gcPaymentId) {
      const pr = await loadPaymentRequest(appDb, gcPaymentId);
      const refs = pr?.invoice_refs ?? [];
      if (refs.length > 0) {
        const refsUpper = refs.map((r) => r.toUpperCase());
        const candidate: AllocationLine[] = [];
        for (const ref of refsUpper) {
          const inv = invoiceRows.find(
            (i) => (i.st_trref ?? '').trim().toUpperCase() === ref,
          );
          if (inv && Number(inv.st_trbal) > 0.005) {
            candidate.push({
              ref: inv.st_trref.trim(),
              custref: (inv.st_custref ?? '').trim(),
              amount: Number(inv.st_trbal),
              full_allocation: true,
              unique: (inv.st_unique ?? '').trim(),
              stran_id: Number(inv.id),
            });
          }
        }
        if (candidate.length > 0) {
          const totalPr = Number(
            candidate.reduce((s, c) => s + c.amount, 0).toFixed(2),
          );
          if (receiptRounded >= totalPr) {
            invoicesToAllocate = candidate;
            allocationMethod = 'payment_request';
          } else {
            // Partial — allocate oldest first up to receipt amount
            let remaining = receiptRounded;
            for (const line of candidate) {
              if (remaining <= 0.005) break;
              const allocAmt = Math.min(line.amount, remaining);
              line.full_allocation = Math.abs(allocAmt - line.amount) < 0.01;
              line.amount = allocAmt;
              remaining -= allocAmt;
            }
            invoicesToAllocate = candidate.filter((c) => c.amount > 0.005);
            allocationMethod = 'payment_request';
          }
        }
      }
    }

    // RULE 1: invoice refs in description
    let invMatches: string[] = [];
    if (!allocationMethod && description) {
      invMatches = (description.toUpperCase().match(INV_REF_RE) ?? []).slice();
      if (invMatches.length > 0) {
        for (const ref of invMatches) {
          const inv = invoiceRows.find(
            (i) => (i.st_trref ?? '').trim().toUpperCase() === ref,
          );
          if (inv && Number(inv.st_trbal) > 0) {
            invoicesToAllocate.push({
              ref: inv.st_trref.trim(),
              custref: (inv.st_custref ?? '').trim(),
              amount: Number(inv.st_trbal),
              full_allocation: true,
              unique: (inv.st_unique ?? '').trim(),
              stran_id: Number(inv.id),
            });
          }
        }
        if (invoicesToAllocate.length > 0) {
          const total = Number(
            invoicesToAllocate.reduce((s, l) => s + l.amount, 0).toFixed(2),
          );
          if (receiptRounded === total) {
            allocationMethod = 'invoice_reference';
          } else {
            const details = invoicesToAllocate.map(
              (l) => `${l.ref} (£${l.amount.toFixed(2)})`,
            );
            result.message =
              `Invoice reference(s) found but amounts do not match: ` +
              `receipt £${receiptRounded.toFixed(2)} vs invoice total ` +
              `£${total.toFixed(2)}. Found: ${JSON.stringify(details)}`;
            return result;
          }
        }
      }
    }

    // RULE 2: clears whole account (or single invoice exact match)
    if (!allocationMethod) {
      if (
        receiptRounded === totalOutstanding &&
        invoiceRows.length >= 1
      ) {
        invoicesToAllocate = invoiceRows
          .filter((i) => Number(i.st_trbal) > 0)
          .map((i) => ({
            ref: i.st_trref.trim(),
            custref: (i.st_custref ?? '').trim(),
            amount: Number(i.st_trbal),
            full_allocation: true,
            unique: (i.st_unique ?? '').trim(),
            stran_id: Number(i.id),
          }));
        allocationMethod =
          invoiceRows.length >= 2 ? 'clears_account' : 'single_invoice_match';
      } else {
        result.message = invMatches.length
          ? `Invoice reference(s) ${JSON.stringify(invMatches)} not found in outstanding invoices`
          : `Cannot auto-allocate: no invoice reference in description and ` +
            `receipt £${receiptRounded.toFixed(2)} does not clear account total ` +
            `£${totalOutstanding.toFixed(2)}`;
        return result;
      }
    }

    // Calculate amounts to commit
    const totalInvoiceAmount = Number(
      invoicesToAllocate.reduce((s, l) => s + l.amount, 0).toFixed(2),
    );
    const totalToAllocate =
      allocationMethod === 'payment_request' &&
      receiptRounded > totalInvoiceAmount
        ? totalInvoiceAmount
        : receiptAmount;
    const receiptFullyAllocated = !(
      allocationMethod === 'payment_request' &&
      receiptRounded > totalInvoiceAmount
    );

    const allocDateStr = input.allocationDate;
    const nowStr = new Date()
      .toISOString()
      .replace('T', ' ')
      .slice(0, 19);

    await operaDb.transaction(async (trx) => {
      // Next al_payflag for this customer (UPDLOCK so concurrent
      // allocations don't race)
      const payflagRows = (await trx.raw(
        `SELECT ISNULL(MAX(al_payflag), 0) AS max_payflag
           FROM salloc WITH (UPDLOCK, ROWLOCK)
           WHERE al_account = ?`,
        [customerAccount],
      )) as Array<{ max_payflag: number | null }>;
      const nextPayflag =
        Number(payflagRows[0]?.max_payflag ?? 0) + 1;

      const newReceiptBal = receiptBalance - totalToAllocate;
      const receiptPaidFlag = receiptFullyAllocated ? 'A' : ' ';

      // UPDATE the receipt
      if (receiptFullyAllocated) {
        await trx.raw(
          `UPDATE stran WITH (ROWLOCK)
              SET st_trbal = ?,
                  st_paid = ?,
                  st_payday = ?,
                  st_payflag = ?,
                  datemodified = ?
              WHERE st_account = ?
                AND RTRIM(st_trref) = ?
                AND st_trtype = 'R'
                AND RTRIM(st_unique) = ?`,
          [
            -newReceiptBal,
            receiptPaidFlag,
            allocDateStr,
            nextPayflag,
            nowStr,
            customerAccount,
            receiptRef,
            receiptUnique,
          ],
        );
      } else {
        await trx.raw(
          `UPDATE stran WITH (ROWLOCK)
              SET st_trbal = ?,
                  st_paid = ?,
                  st_payflag = ?,
                  datemodified = ?
              WHERE st_account = ?
                AND RTRIM(st_trref) = ?
                AND st_trtype = 'R'
                AND RTRIM(st_unique) = ?`,
          [
            -newReceiptBal,
            receiptPaidFlag,
            nextPayflag,
            nowStr,
            customerAccount,
            receiptRef,
            receiptUnique,
          ],
        );
      }

      // Insert salloc for the receipt (only when fully allocated)
      if (receiptFullyAllocated) {
        const allocRef2 =
          allocationMethod === 'payment_request'
            ? 'AUTO:GC_REQ'
            : allocationMethod === 'invoice_reference'
              ? 'AUTO:INV_REF'
              : 'AUTO:CLR_ACCT';
        const sallocId = await getNextId(trx, 'salloc');
        await trx.raw(
          `INSERT INTO salloc (
              id,
              al_account, al_date, al_ref1, al_ref2, al_type, al_val,
              al_payind, al_payflag, al_payday, al_fcurr, al_fval, al_fdec,
              al_advind, al_acnt, al_cntr, al_preprd, al_unique, al_adjsv,
              datecreated, datemodified, state
            ) VALUES (
              ?,
              ?, ?, ?, ?, 'R', ?,
              'A', ?, ?, '   ', 0, 0,
              0, ?, '    ', 0, ?, 0,
              ?, ?, 1
            )`,
          [
            sallocId,
            customerAccount,
            receipt.st_trdate ?? allocDateStr,
            receiptRef,
            allocRef2,
            -receiptBalance,
            nextPayflag,
            allocDateStr,
            bankAccount,
            receiptStranId,
            nowStr,
            nowStr,
          ],
        );
      }

      // Apply allocation to each invoice
      for (const line of invoicesToAllocate) {
        const invCurrent = (await trx.raw(
          `SELECT st_trbal, st_trdate FROM stran WITH (NOLOCK)
              WHERE st_account = ?
                AND RTRIM(st_trref) = ?
                AND st_trtype = 'I'`,
          [customerAccount, line.ref],
        )) as Array<{ st_trbal: number; st_trdate: string }>;
        if (!Array.isArray(invCurrent) || invCurrent.length === 0) continue;
        const inv = invCurrent[0]!;
        const newInvBal = Number(inv.st_trbal) - line.amount;
        const invPaidFlag = newInvBal < 0.01 ? 'P' : ' ';
        const invDate = inv.st_trdate;

        if (newInvBal < 0.01) {
          await trx.raw(
            `UPDATE stran WITH (ROWLOCK)
                SET st_trbal = ?,
                    st_paid = ?,
                    st_payday = ?,
                    st_payflag = ?,
                    st_lastrec = ?,
                    datemodified = ?
                WHERE st_account = ?
                  AND RTRIM(st_trref) = ?
                  AND st_trtype = 'I'`,
            [
              newInvBal,
              invPaidFlag,
              allocDateStr,
              nextPayflag,
              invDate,
              nowStr,
              customerAccount,
              line.ref,
            ],
          );

          const sallocInvId = await getNextId(trx, 'salloc');
          await trx.raw(
            `INSERT INTO salloc (
                id,
                al_account, al_date, al_ref1, al_ref2, al_type, al_val,
                al_payind, al_payflag, al_payday, al_fcurr, al_fval, al_fdec,
                al_advind, al_acnt, al_cntr, al_preprd, al_unique, al_adjsv,
                datecreated, datemodified, state
              ) VALUES (
                ?,
                ?, ?, ?, ?, 'I', ?,
                'A', ?, ?, '   ', 0, 0,
                0, ?, '    ', 0, ?, 0,
                ?, ?, 1
              )`,
            [
              sallocInvId,
              customerAccount,
              invDate,
              line.ref,
              line.custref.slice(0, 20),
              line.amount,
              nextPayflag,
              allocDateStr,
              bankAccount,
              line.stran_id,
              nowStr,
              nowStr,
            ],
          );
        } else {
          await trx.raw(
            `UPDATE stran WITH (ROWLOCK)
                SET st_trbal = ?,
                    st_paid = ?,
                    st_payflag = ?,
                    datemodified = ?
                WHERE st_account = ?
                  AND RTRIM(st_trref) = ?
                  AND st_trtype = 'I'`,
            [
              newInvBal,
              invPaidFlag,
              nextPayflag,
              nowStr,
              customerAccount,
              line.ref,
            ],
          );
        }
      }

      // sname.sn_lastrec
      await trx.raw(
        `UPDATE sname WITH (ROWLOCK)
            SET sn_lastrec = ?,
                datemodified = ?
            WHERE RTRIM(sn_account) = ?`,
        [allocDateStr, nowStr, customerAccount],
      );
    });

    result.success = true;
    result.allocated_amount = totalToAllocate;
    result.allocations = invoicesToAllocate;
    result.receipt_fully_allocated = receiptFullyAllocated;
    result.allocation_method = allocationMethod;
    result.message =
      allocationMethod === 'payment_request'
        ? `Allocated £${totalToAllocate.toFixed(2)} to ${invoicesToAllocate.length} invoice(s) from payment request`
        : allocationMethod === 'invoice_reference'
          ? `Allocated £${totalToAllocate.toFixed(2)} to ${invoicesToAllocate.length} invoice(s) by reference`
          : `Allocated £${totalToAllocate.toFixed(2)} to ${invoicesToAllocate.length} invoice(s) - clears account`;

    return result;
  } catch (e: any) {
    return {
      ...result,
      message: `Allocation failed: ${e?.message ?? String(e)}`,
    };
  }
}
