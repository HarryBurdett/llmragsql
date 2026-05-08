/**
 * Sub-ledger ↔ Nominal Ledger transaction variance analysis.
 *
 * Faithful port of the variance-analysis section inside
 * `reconcile_creditors` (lines ~285-720 of `apps/balance_check/api/routes.py`)
 * and the matching section inside `reconcile_debtors`. The two endpoints
 * use identical matching logic — extracted here once to mirror the
 * existing `sub_ledger_reconcile.py` extraction pattern.
 *
 * Matching strategy (4 priority tiers — preserved exactly):
 *   1. Reference match (nt_cmnt = pt_trref / st_trref) — most reliable
 *      - Generic refs ('rec', 'pay', 'contra', etc.) require exact value (£0.10 tol)
 *      - Specific refs allow 10% or £10 tolerance
 *   2. Date + value + supplier-account
 *   3. Date + value
 *   4. Value + supplier-account (£0.02 tolerance)
 *
 * NB: behaviour preserved exactly — sign conventions, rounding, supplier
 * name lookups, all match the Python implementation.
 */
import type { Knex } from 'knex';

function r2(n: number): number {
  return Math.round(n * 100) / 100;
}

function dateToStr(d: Date | string | null): string {
  if (!d) return '';
  if (d instanceof Date) {
    const yr = d.getFullYear();
    const mo = String(d.getMonth() + 1).padStart(2, '0');
    const da = String(d.getDate()).padStart(2, '0');
    return `${yr}-${mo}-${da}`;
  }
  return String(d);
}

const GENERIC_REFS = new Set<string>([
  'rec',
  'pay',
  'contra',
  'refund',
  'adjustment',
  'adj',
  'jnl',
  'journal',
]);

export interface VarianceAnalysisItem {
  source: string;
  date: string;
  reference: string;
  supplier: string;
  type: string;
  value: number;
  note: string;
  category?: 'NL_ONLY' | 'PL_ONLY' | 'VALUE_DIFF' | 'OTHER';
  // Value-difference items carry these extra fields
  nl_value?: number;
  pl_value?: number;
  pl_balance?: number;
  match_type?: string;
}

export interface VarianceAnalysisResult {
  items: VarianceAnalysisItem[];
  count: number;
  value_diff_count: number;
  nl_only_count: number;
  pl_only_count: number;
  small_balance_count: number;
  summary: {
    nl_only: { count: number; total: number; description: string };
    pl_only: { count: number; total: number; description: string };
    value_differences: { count: number; total: number; description: string };
  };
  nl_total_check: number;
  pl_total_check: number;
}

export interface VarianceAnalysisOptions {
  /** Side identification — 'creditors' or 'debtors' — controls labels and master spec lookups. */
  side: 'creditors' | 'debtors';
  /** Control-account codes that have been identified for the NL side. */
  controlAccountCodes: string[];
  /** Current fiscal year (used to filter PL/SL transactions). */
  currentYear: number;
  /** Variance amount (PL - NL) — used for "exact match to variance" detection. */
  varianceAmount: number;
  /**
   * Whether to filter NL transactions to current year only.
   *
   * - Creditors: TRUE — Python filters by nt_year = current_year.
   * - Debtors: FALSE — Python pulls ALL years; old SL transactions need matching too
   *   (per comment in routes.py: "We need all years to properly match against SL
   *   transactions that may be old").
   */
  filterNlByCurrentYear: boolean;
  /**
   * Whether to run the analysis regardless of variance magnitude.
   *
   * - Creditors: FALSE — Python only populates the analysis when
   *   `variance_abs >= 0.01`; otherwise returns the empty shape.
   * - Debtors: TRUE — Python comment "Provide drill-down into
   *   transactions even when reconciled" — always runs.
   */
  alwaysRun: boolean;
}

/**
 * Run the full sub-ledger ↔ NL variance analysis. Returns nothing if the
 * variance is below £0.01 — matches the Python early-out behaviour.
 *
 * The empty result still has the right shape so the caller can stitch it
 * into the response without conditionals.
 */
export async function analyseVariance(
  db: Knex,
  opts: VarianceAnalysisOptions,
): Promise<VarianceAnalysisResult> {
  const empty: VarianceAnalysisResult = {
    items: [],
    count: 0,
    value_diff_count: 0,
    nl_only_count: 0,
    pl_only_count: 0,
    small_balance_count: 0,
    summary: {
      nl_only: { count: 0, total: 0, description: '' },
      pl_only: { count: 0, total: 0, description: '' },
      value_differences: { count: 0, total: 0, description: '' },
    },
    nl_total_check: 0,
    pl_total_check: 0,
  };

  const varianceAbs = Math.abs(opts.varianceAmount);
  if (varianceAbs < 0.01 && !opts.alwaysRun) return empty;

  const isCreditors = opts.side === 'creditors';
  const masterTable = isCreditors ? 'pname' : 'sname';
  const masterAccountField = isCreditors ? 'pn_account' : 'sn_account';
  const masterNameField = isCreditors ? 'pn_name' : 'sn_name';
  const txnTable = isCreditors ? 'ptran' : 'stran';
  const txnRefField = isCreditors ? 'pt_trref' : 'st_trref';
  const txnBalField = isCreditors ? 'pt_trbal' : 'st_trbal';
  const txnValField = isCreditors ? 'pt_trvalue' : 'st_trvalue';
  const txnDateField = isCreditors ? 'pt_trdate' : 'st_trdate';
  const txnAccountField = isCreditors ? 'pt_account' : 'st_account';
  const txnTypeField = isCreditors ? 'pt_trtype' : 'st_trtype';
  const txnSupRefField = isCreditors ? 'pt_supref' : 'st_yourref';
  const partyLabel = isCreditors ? 'supplier' : 'customer';
  const ledgerLabel = isCreditors ? 'Purchase Ledger' : 'Sales Ledger';
  const ledgerShortPL = isCreditors ? 'PL' : 'SL';
  const partyAccountLookup = isCreditors ? 'Supplier' : 'Customer';

  // Get NL transactions
  // Creditors: current year only.
  // Debtors: all years (matches old SL transactions still on the ledger).
  const controlInClause = opts.controlAccountCodes.map((c) => `'${c}'`).join(',');
  const yearFilter = opts.filterNlByCurrentYear ? 'AND nt_year = ?' : '';
  const nlTransactionsSql = `
    SELECT
      RTRIM(nt_cmnt) AS reference,
      nt_value AS nl_value,
      nt_entr AS date,
      nt_year AS year,
      nt_type AS type,
      RTRIM(nt_ref) AS nl_ref,
      RTRIM(nt_trnref) AS description
    FROM ntran
    WHERE nt_acnt IN (${controlInClause})
      ${yearFilter}
    ORDER BY nt_entr, nt_cmnt
  `;
  let nlTrans: Array<{
    reference: string | null;
    nl_value: number | null;
    date: Date | string | null;
    year: number | null;
    type: string | null;
    nl_ref: string | null;
    description: string | null;
  }> = [];
  try {
    const bindings = opts.filterNlByCurrentYear ? [opts.currentYear] : [];
    nlTrans = (await db.raw(nlTransactionsSql, bindings)) as unknown as typeof nlTrans;
    if (!Array.isArray(nlTrans)) nlTrans = [];
  } catch {
    nlTrans = [];
  }

  // Get party (supplier/customer) names for matching
  const partyNamesSql = `
    SELECT RTRIM(${masterAccountField}) AS account, RTRIM(${masterNameField}) AS name
    FROM ${masterTable}
  `;
  const partyNameToAccount = new Map<string, string>();
  const partyAccountToName = new Map<string, string>();
  try {
    const partyRows = (await db.raw(partyNamesSql)) as unknown as Array<{
      account: string | null;
      name: string | null;
    }>;
    for (const row of Array.isArray(partyRows) ? partyRows : []) {
      const acc = row.account ? String(row.account).trim() : '';
      const name = row.name ? String(row.name).trim().toUpperCase() : '';
      if (acc && name) {
        partyNameToAccount.set(name, acc);
        partyAccountToName.set(acc, name);
      }
    }
  } catch {
    // empty maps; matching will fall through
  }

  // Get PL/SL transactions for current year only, for active accounts
  const plTransactionsSql = `
    SELECT
      RTRIM(${txnRefField}) AS reference,
      ${txnBalField} AS pl_balance,
      ${txnValField} AS pl_value,
      ${txnDateField} AS date,
      RTRIM(${txnAccountField}) AS supplier,
      ${txnTypeField} AS type,
      RTRIM(${txnSupRefField}) AS supplier_ref
    FROM ${txnTable}
    WHERE ${txnBalField} <> 0
      AND RTRIM(${txnAccountField}) IN (SELECT RTRIM(${masterAccountField}) FROM ${masterTable})
      AND YEAR(${txnDateField}) = ?
    ORDER BY ${txnDateField}, ${txnRefField}
  `;
  let plTrans: Array<{
    reference: string | null;
    pl_balance: number | null;
    pl_value: number | null;
    date: Date | string | null;
    supplier: string | null;
    type: string | null;
    supplier_ref: string | null;
  }> = [];
  try {
    plTrans = (await db.raw(plTransactionsSql, [opts.currentYear])) as unknown as typeof plTrans;
    if (!Array.isArray(plTrans)) plTrans = [];
  } catch {
    plTrans = [];
  }

  // Build NL entries with matching keys
  interface NlEntry {
    value: number;
    date: string;
    reference: string;
    year: number;
    type: string;
    matched: boolean;
    date_val_key: string;
    date_val_supplier_key: string;
    abs_val: number;
    supplier_name: string;
    supplier_account: string;
  }

  const nlEntries: NlEntry[] = [];
  let nlTotalCheck = 0;

  for (const txn of nlTrans) {
    const ref = txn.reference ? String(txn.reference).trim() : '';
    const nlValue = Number(txn.nl_value ?? 0);
    const nlDateStr = dateToStr(txn.date ?? null);

    const description = txn.description ? String(txn.description).trim() : '';
    const nlSupplierName = description ? description.slice(0, 30).trim().toUpperCase() : '';

    let nlSupplierAccount = partyNameToAccount.get(nlSupplierName) ?? '';
    // Partial match if no exact (supplier name might be truncated)
    if (!nlSupplierAccount && nlSupplierName) {
      for (const [name, acc] of partyNameToAccount.entries()) {
        if (name.startsWith(nlSupplierName) || nlSupplierName.startsWith(name)) {
          nlSupplierAccount = acc;
          break;
        }
      }
    }

    const absVal = Math.abs(nlValue);
    const dateValKey = `${nlDateStr}|${absVal.toFixed(2)}`;
    const dateValSupplierKey = `${nlDateStr}|${absVal.toFixed(2)}|${nlSupplierAccount}`;

    nlEntries.push({
      value: nlValue,
      date: nlDateStr,
      reference: ref,
      year: Number(txn.year ?? 0),
      type: String(txn.type ?? ''),
      matched: false,
      date_val_key: dateValKey,
      date_val_supplier_key: dateValSupplierKey,
      abs_val: absVal,
      supplier_name: nlSupplierName,
      supplier_account: nlSupplierAccount,
    });
    nlTotalCheck += nlValue;
  }

  // Build PL entries with matching keys
  interface PlEntry {
    balance: number;
    value: number;
    date: string;
    reference: string;
    supplier: string;
    type: string;
    supplier_ref: string;
    date_val_key: string;
    date_val_supplier_key: string;
    abs_val: number;
    matched: boolean;
  }

  const plEntries: PlEntry[] = [];
  let plTotalCheck = 0;

  for (const txn of plTrans) {
    const ref = txn.reference ? String(txn.reference).trim() : '';
    const plBal = Number(txn.pl_balance ?? 0);
    const plValue = Number(txn.pl_value ?? 0);
    const supplier = txn.supplier ? String(txn.supplier).trim() : '';
    const trType = txn.type ? String(txn.type).trim() : '';
    const supRef = txn.supplier_ref ? String(txn.supplier_ref).trim() : '';
    const plDateStr = dateToStr(txn.date ?? null);

    const absVal = Math.abs(plValue);
    const dateValKey = `${plDateStr}|${absVal.toFixed(2)}`;
    const dateValSupplierKey = `${plDateStr}|${absVal.toFixed(2)}|${supplier}`;

    plEntries.push({
      balance: plBal,
      value: plValue,
      date: plDateStr,
      reference: ref,
      supplier,
      type: trType,
      supplier_ref: supRef,
      date_val_key: dateValKey,
      date_val_supplier_key: dateValSupplierKey,
      abs_val: absVal,
      matched: false,
    });
    plTotalCheck += plBal;
  }

  // Build NL reference lookup
  const nlByReference = new Map<string, NlEntry[]>();
  for (const nl of nlEntries) {
    if (nl.reference) {
      const list = nlByReference.get(nl.reference);
      if (list) list.push(nl);
      else nlByReference.set(nl.reference, [nl]);
    }
  }

  const valueDiffItems: VarianceAnalysisItem[] = [];

  // Match PL → NL using 4-strategy priority
  for (const plEntry of plEntries) {
    const plRef = plEntry.reference;
    const plAbs = plEntry.abs_val;
    const isGenericRef = plRef ? GENERIC_REFS.has(plRef.toLowerCase()) : false;

    let nlData: NlEntry | null = null;
    let matchType = '';

    // Strategy 1: reference match
    if (plRef) {
      const candidates = nlByReference.get(plRef);
      if (candidates) {
        for (const nl of candidates) {
          if (nl.matched) continue;
          const valueDiff = Math.abs(nl.abs_val - plAbs);
          const tolerance = isGenericRef ? 0.1 : Math.max(10.0, plAbs * 0.1);
          if (valueDiff <= tolerance) {
            nlData = nl;
            matchType = 'reference';
            break;
          }
        }
      }
    }

    // Strategy 2: date + value + supplier
    if (!nlData) {
      for (const nl of nlEntries) {
        if (!nl.matched && nl.date_val_supplier_key === plEntry.date_val_supplier_key) {
          nlData = nl;
          matchType = 'date_value_supplier';
          break;
        }
      }
    }

    // Strategy 3: date + value
    if (!nlData) {
      for (const nl of nlEntries) {
        if (!nl.matched && nl.date_val_key === plEntry.date_val_key) {
          nlData = nl;
          matchType = 'date_value';
          break;
        }
      }
    }

    // Strategy 4: value + supplier (£0.02 tolerance)
    if (!nlData) {
      for (const nl of nlEntries) {
        if (nl.matched) continue;
        if (Math.abs(nl.abs_val - plAbs) < 0.02 && nl.supplier_account === plEntry.supplier) {
          nlData = nl;
          matchType = 'value_supplier';
          break;
        }
      }
    }

    if (nlData) {
      nlData.matched = true;
      plEntry.matched = true;

      const nlAbs = nlData.abs_val;
      const actualDiff = r2(nlAbs - plAbs);
      if (Math.abs(actualDiff) >= 0.01) {
        valueDiffItems.push({
          source: 'Value Difference',
          date: plEntry.date,
          reference: plEntry.reference,
          supplier: plEntry.supplier,
          type: plEntry.type,
          value: actualDiff,
          nl_value: r2(nlData.value),
          pl_value: r2(plEntry.value),
          pl_balance: r2(plEntry.balance),
          match_type: matchType,
          note: `NL: £${nlAbs.toFixed(2)} vs ${ledgerShortPL}: £${plAbs.toFixed(2)} (diff: £${Math.abs(actualDiff).toFixed(2)})`,
        });
      }
    }
  }

  // NL-only (in NL but not in PL)
  const nlOnlyItems: VarianceAnalysisItem[] = [];
  for (const nl of nlEntries) {
    if (!nl.matched && Math.abs(nl.value) >= 0.01) {
      const supplierInfo = nl.supplier_account || nl.supplier_name || '';
      let note = `In NL (year ${nl.year}) but no matching ${ledgerShortPL} entry`;
      if (nl.supplier_name && !nl.supplier_account) {
        note += ` - ${partyAccountLookup} name '${nl.supplier_name}' not found in ${masterTable}`;
      }
      nlOnlyItems.push({
        source: 'Nominal Ledger Only',
        date: nl.date,
        reference: nl.reference,
        supplier: supplierInfo,
        type: nl.type || 'NL',
        value: r2(nl.value),
        note,
      });
    }
  }

  // PL-only (in PL but not in NL)
  let plOnlyItems: VarianceAnalysisItem[] = [];
  for (const pl of plEntries) {
    if (!pl.matched && Math.abs(pl.balance) >= 0.01) {
      plOnlyItems.push({
        source: `${ledgerLabel} Only`,
        date: pl.date,
        reference: pl.reference,
        supplier: pl.supplier,
        type: pl.type,
        value: r2(pl.balance),
        note: `In ${ledgerShortPL} but no matching NL entry`,
      });
    }
  }

  // Look for items that exactly match the variance or are small balances
  const exactMatchRefs = new Set<string>();
  const smallBalanceRefs = new Set<string>();
  const varianceItems: VarianceAnalysisItem[] = [];

  for (const txn of plTrans) {
    const plBal = Number(txn.pl_balance ?? 0);
    const ref = txn.reference ? String(txn.reference).trim() : '';
    const trDate = dateToStr(txn.date ?? null);

    if (Math.abs(Math.abs(plBal) - varianceAbs) < 0.02) {
      exactMatchRefs.add(ref);
      varianceItems.push({
        source: 'Exact Match',
        date: trDate,
        reference: ref,
        supplier: txn.supplier ? String(txn.supplier).trim() : '',
        type: txn.type ? String(txn.type).trim() : '',
        value: r2(plBal),
        note: `Balance £${plBal.toFixed(2)} matches variance £${varianceAbs.toFixed(2)}`,
      });
    } else if (Math.abs(plBal) >= 0.01 && Math.abs(plBal) < 1.0) {
      smallBalanceRefs.add(ref);
      varianceItems.push({
        source: 'Small Balance',
        date: trDate,
        reference: ref,
        supplier: txn.supplier ? String(txn.supplier).trim() : '',
        type: txn.type ? String(txn.type).trim() : '',
        value: r2(plBal),
        note: 'Small balance - possible rounding',
      });
    }
  }

  // Remove small balance/exact match items from PL-only
  const varianceRefs = new Set<string>([...exactMatchRefs, ...smallBalanceRefs]);
  plOnlyItems = plOnlyItems.filter((item) => !varianceRefs.has(item.reference));

  // Build display items (categorised)
  const displayItems: VarianceAnalysisItem[] = [];
  for (const item of nlOnlyItems) displayItems.push({ ...item, category: 'NL_ONLY' });
  for (const item of plOnlyItems) displayItems.push({ ...item, category: 'PL_ONLY' });
  for (const item of valueDiffItems) displayItems.push({ ...item, category: 'VALUE_DIFF' });
  for (const item of varianceItems) displayItems.push({ ...item, category: 'OTHER' });

  const nlOnlyTotal = nlOnlyItems.reduce((sum, item) => sum + item.value, 0);
  const plOnlyTotal = plOnlyItems.reduce((sum, item) => sum + item.value, 0);
  const valueDiffTotal = valueDiffItems.reduce((sum, item) => sum + item.value, 0);

  // Suppress unused-var warning for partyLabel — kept for parity with Python labels.
  void partyLabel;

  return {
    items: displayItems,
    count: displayItems.length,
    value_diff_count: valueDiffItems.length,
    nl_only_count: nlOnlyItems.length,
    pl_only_count: plOnlyItems.length,
    small_balance_count: varianceItems.length,
    summary: {
      nl_only: {
        count: nlOnlyItems.length,
        total: r2(nlOnlyTotal),
        description: `Entries in Nominal Ledger with no matching ${ledgerLabel} entry`,
      },
      pl_only: {
        count: plOnlyItems.length,
        total: r2(plOnlyTotal),
        description: `Entries in ${ledgerLabel} with no matching Nominal Ledger entry`,
      },
      value_differences: {
        count: valueDiffItems.length,
        total: r2(valueDiffTotal),
        description: 'Matched entries with different values',
      },
    },
    nl_total_check: r2(nlTotalCheck),
    pl_total_check: r2(plTotalCheck),
  };
}
