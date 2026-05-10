/**
 * Bank-statement import preview — port of `import_from_statement`
 * (apps/bank_reconcile/api/routes.py:1826-1929).
 *
 * Takes a list of already-extracted bank-statement transactions and
 * matches each one to an Opera customer / supplier / nominal using the
 * alias table that the Python `BankStatementImporter._match_transaction`
 * builds up over time.
 *
 * This is a PREVIEW endpoint — it returns matched/unmatched groups so
 * the operator can review and confirm before any Opera write happens.
 * The actual posting happens via the cashbook-poster service once the
 * operator confirms.
 *
 * Faithful to Python within scope: the alias lookup path is fully
 * ported. The AI-driven fuzzy matching, repeat-entry-check, and
 * bank-transfer-check from Python's `_match_transaction` are NOT yet
 * ported — they need their own dedicated services and depend on
 * SAM's llm service. We document each as a deferred follow-up; for
 * already-aliased payees the match will succeed exactly as in Python.
 *
 * Locking: read-only against Opera (NOLOCK on any reads). No locks
 * needed on the per-app DB (alias reads are best-effort).
 */
import type { Knex } from 'knex';
import { validateBankCode, SqlInputValidationError } from '@sqlrag/sam-shared';

export interface StatementTxnInput {
  date: string; // YYYY-MM-DD or ISO string
  description?: string;
  reference?: string;
  amount: number; // signed: negative = debit (payment), positive = credit (receipt)
  type?: string;
}

export type MatchAction =
  | 'sales_receipt'
  | 'sales_refund'
  | 'purchase_payment'
  | 'purchase_refund'
  | 'nominal_receipt'
  | 'nominal_payment'
  | 'bank_transfer'
  | null;

export interface MatchedTxn {
  row: number;
  date: string;
  name: string;
  reference: string;
  amount: number;
  action: MatchAction;
  match_type: 'alias' | 'unmatched';
  matched_account: string | null;
  matched_name: string | null;
  match_score: number | null;
  skip_reason: string | null;
}

export interface ImportPreviewResult {
  success: boolean;
  total_transactions?: number;
  matched_receipts?: MatchedTxn[];
  matched_payments?: MatchedTxn[];
  unmatched?: MatchedTxn[];
  summary?: {
    receipts: number;
    payments: number;
    unmatched: number;
  };
  warnings?: string[];
  error?: string;
}

interface AliasRow {
  id: number;
  bank_code: string;
  payee_pattern: string;
  match_type: string | null;
  opera_account: string | null;
  confidence: number | null;
  direction: string | null;
}

async function loadAliases(
  appDb: Knex,
  bankCode: string,
): Promise<AliasRow[]> {
  try {
    return (await appDb('bank_import_aliases')
      .where({ bank_code: bankCode })
      .select(
        'id',
        'bank_code',
        'payee_pattern',
        'match_type',
        'opera_account',
        'confidence',
        'direction',
      )) as AliasRow[];
  } catch {
    return [];
  }
}

interface NameLookups {
  customers: Map<string, string>; // sn_account -> sn_name
  suppliers: Map<string, string>;
}

async function loadAccountNames(operaDb: Knex): Promise<NameLookups> {
  const customers = new Map<string, string>();
  const suppliers = new Map<string, string>();
  try {
    const sRows = (await operaDb.raw(
      `SELECT RTRIM(sn_account) AS account, RTRIM(ISNULL(sn_name, '')) AS name
         FROM sname WITH (NOLOCK)
         WHERE ISNULL(sn_dormant, 0) = 0`,
    )) as Array<{ account: string; name: string }>;
    for (const r of sRows ?? []) customers.set(r.account, r.name);
  } catch {
    // best-effort
  }
  try {
    const pRows = (await operaDb.raw(
      `SELECT RTRIM(pn_account) AS account, RTRIM(ISNULL(pn_name, '')) AS name
         FROM pname WITH (NOLOCK)
         WHERE ISNULL(pn_dormant, 0) = 0`,
    )) as Array<{ account: string; name: string }>;
    for (const r of pRows ?? []) suppliers.set(r.account, r.name);
  } catch {
    // best-effort
  }
  return { customers, suppliers };
}

function actionFor(
  matchType: string | null,
  isReceipt: boolean,
): MatchAction {
  const t = (matchType ?? '').toLowerCase();
  if (t === 'customer') {
    return isReceipt ? 'sales_receipt' : 'sales_refund';
  }
  if (t === 'supplier') {
    return isReceipt ? 'purchase_refund' : 'purchase_payment';
  }
  if (t === 'nominal') {
    return isReceipt ? 'nominal_receipt' : 'nominal_payment';
  }
  return null;
}

function aliasMatches(
  alias: AliasRow,
  description: string,
  isReceipt: boolean,
): boolean {
  const pattern = (alias.payee_pattern ?? '').trim();
  if (!pattern) return false;
  const direction = (alias.direction ?? 'either').toLowerCase();
  if (direction === 'receipt' && !isReceipt) return false;
  if (direction === 'payment' && isReceipt) return false;
  return description.toUpperCase().includes(pattern.toUpperCase());
}

export async function importFromStatementPreview(
  appDb: Knex,
  operaDb: Knex,
  bankCode: string,
  transactions: StatementTxnInput[],
): Promise<ImportPreviewResult> {
  let code: string;
  try {
    code = validateBankCode(bankCode);
  } catch (e) {
    if (e instanceof SqlInputValidationError) {
      return { success: false, error: e.message };
    }
    throw e;
  }

  if (!Array.isArray(transactions) || transactions.length === 0) {
    return {
      success: true,
      total_transactions: 0,
      matched_receipts: [],
      matched_payments: [],
      unmatched: [],
      summary: { receipts: 0, payments: 0, unmatched: 0 },
    };
  }

  const aliases = await loadAliases(appDb, code);
  const names = await loadAccountNames(operaDb);

  const matchedReceipts: MatchedTxn[] = [];
  const matchedPayments: MatchedTxn[] = [];
  const unmatched: MatchedTxn[] = [];

  for (let i = 0; i < transactions.length; i++) {
    const txn = transactions[i]!;
    const amount = Number(txn.amount ?? 0);
    const isReceipt = amount > 0;
    const description = (txn.description ?? '').trim();
    const reference = (txn.reference || description).slice(0, 30);
    const date =
      typeof txn.date === 'string'
        ? txn.date.slice(0, 10)
        : String(txn.date ?? '');

    let alias: AliasRow | null = null;
    for (const a of aliases) {
      if (aliasMatches(a, description, isReceipt)) {
        alias = a;
        break;
      }
    }

    const matchedAccount = alias?.opera_account?.trim() ?? null;
    const matchedName = matchedAccount
      ? (names.customers.get(matchedAccount) ??
          names.suppliers.get(matchedAccount) ??
          null)
      : null;
    const action = matchedAccount
      ? actionFor(alias!.match_type ?? '', isReceipt)
      : null;

    const result: MatchedTxn = {
      row: i + 1,
      date,
      name: description.slice(0, 100),
      reference,
      amount,
      action,
      match_type: alias ? 'alias' : 'unmatched',
      matched_account: matchedAccount,
      matched_name: matchedName,
      match_score: alias?.confidence ?? null,
      skip_reason: alias
        ? null
        : 'No alias match (AI fuzzy matching not yet ported in SAM build)',
    };

    if (action === 'sales_receipt' || action === 'sales_refund') {
      matchedReceipts.push(result);
    } else if (action === 'purchase_payment' || action === 'purchase_refund') {
      matchedPayments.push(result);
    } else if (matchedAccount && isReceipt) {
      matchedReceipts.push(result);
    } else if (matchedAccount && !isReceipt) {
      matchedPayments.push(result);
    } else {
      unmatched.push(result);
    }
  }

  return {
    success: true,
    total_transactions: transactions.length,
    matched_receipts: matchedReceipts,
    matched_payments: matchedPayments,
    unmatched,
    summary: {
      receipts: matchedReceipts.length,
      payments: matchedPayments.length,
      unmatched: unmatched.length,
    },
    warnings: [
      'AI fuzzy matching is not yet wired in the SAM port — matches that depend on payee fuzzy similarity (rather than learned aliases) will currently show as unmatched. Operator can manually pick the account in the UI; the choice will be persisted as an alias for next time.',
    ],
  };
}
