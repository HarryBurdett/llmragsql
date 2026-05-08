/**
 * Pure helper functions for sub-ledger ↔ control-account reconciliation.
 *
 * Faithful port of `apps/balance_check/logic/sub_ledger_reconcile.py`.
 *
 * The balance-check route handlers `reconcileCreditors` and `reconcileDebtors`
 * share the same overall shape (7 phases — outstanding totals, breakdown by
 * type, master totals, master-vs-txn variance, transfer-file pending list,
 * transfer-file summary, control-account YTD movement). They differ only in
 * which table/field names to use. This module extracts the shared shape into
 * pure helpers parameterised by a LedgerSpec.
 *
 * READ-ONLY queries with NOLOCK hints (consistent with CLAUDE.md locking
 * rules for read paths). Returns plain objects so the route handler can
 * assemble the JSON response.
 *
 * Behaviour preserved exactly — SQL copied verbatim from the Python helpers,
 * parameterised only on the LedgerSpec field names.
 */
import type { Knex } from 'knex';

/**
 * Identifies a sub-ledger triple (master table, txn table, transfer file).
 *
 * Field names spelled exactly as they appear in the Opera schema snapshot.
 */
export interface LedgerSpec {
  // Sub-ledger transaction table (ptran / stran)
  txnTable: string;
  txnAccountField: string; // pt_account / st_account
  txnBalanceField: string; // pt_trbal / st_trbal
  txnTypeField: string; // pt_trtype / st_trtype

  // Master table (pname / sname)
  masterTable: string;
  masterAccountField: string; // pn_account / sn_account
  masterNameField: string; // pn_name / sn_name
  masterBalanceField: string; // pn_currbal / sn_currbal

  // Transfer file (pnoml / snoml)
  transferTable: string; // pnoml / snoml
  transferDoneField: string; // px_done / sx_done
}

export const CREDITORS: LedgerSpec = {
  txnTable: 'ptran',
  txnAccountField: 'pt_account',
  txnBalanceField: 'pt_trbal',
  txnTypeField: 'pt_trtype',
  masterTable: 'pname',
  masterAccountField: 'pn_account',
  masterNameField: 'pn_name',
  masterBalanceField: 'pn_currbal',
  transferTable: 'pnoml',
  transferDoneField: 'px_done',
};

export const DEBTORS: LedgerSpec = {
  txnTable: 'stran',
  txnAccountField: 'st_account',
  txnBalanceField: 'st_trbal',
  txnTypeField: 'st_trtype',
  masterTable: 'sname',
  masterAccountField: 'sn_account',
  masterNameField: 'sn_name',
  masterBalanceField: 'sn_currbal',
  transferTable: 'snoml',
  transferDoneField: 'sx_done',
};

/**
 * Round to 2 decimal places — Python's `round(x, 2)`.
 */
function r2(n: number): number {
  return Math.round(n * 100) / 100;
}

export interface OutstandingResult {
  total_outstanding: number;
  transaction_count: number;
}

/**
 * Total outstanding balance + count for the sub-ledger.
 *
 * Excludes orphan rows (where the account no longer exists in the
 * master table) so the figure compares like-for-like with the
 * master-balance check.
 */
export async function fetchOutstanding(
  db: Knex,
  spec: LedgerSpec,
): Promise<OutstandingResult> {
  const sql = `
    SELECT
      COUNT(*) AS transaction_count,
      SUM(${spec.txnBalanceField}) AS total_outstanding
    FROM ${spec.txnTable} WITH (NOLOCK)
    WHERE ${spec.txnBalanceField} <> 0
      AND RTRIM(${spec.txnAccountField}) IN (
        SELECT RTRIM(${spec.masterAccountField}) FROM ${spec.masterTable} WITH (NOLOCK)
      )
  `;
  const rows = (await db.raw(sql)) as unknown as Array<{
    transaction_count: number | null;
    total_outstanding: number | null;
  }>;
  const row = Array.isArray(rows) ? rows[0] : null;
  return {
    total_outstanding: Number(row?.total_outstanding ?? 0),
    transaction_count: Number(row?.transaction_count ?? 0),
  };
}

export interface BreakdownRow {
  type: string;
  description: string;
  count: number;
  total: number;
}

/**
 * Breakdown of outstanding balance by transaction type.
 */
export async function fetchBreakdownByType(
  db: Knex,
  spec: LedgerSpec,
  typeDescriptions: Record<string, string>,
): Promise<BreakdownRow[]> {
  const sql = `
    SELECT
      ${spec.txnTypeField} AS type,
      COUNT(*) AS count,
      SUM(${spec.txnBalanceField}) AS total
    FROM ${spec.txnTable} WITH (NOLOCK)
    WHERE ${spec.txnBalanceField} <> 0
      AND RTRIM(${spec.txnAccountField}) IN (
        SELECT RTRIM(${spec.masterAccountField}) FROM ${spec.masterTable} WITH (NOLOCK)
      )
    GROUP BY ${spec.txnTypeField}
    ORDER BY ${spec.txnTypeField}
  `;
  const rows = (await db.raw(sql)) as unknown as Array<{
    type: string | null;
    count: number | null;
    total: number | null;
  }>;
  return (Array.isArray(rows) ? rows : []).map((row) => {
    const typeCode = row.type ? String(row.type).trim() : 'Unknown';
    return {
      type: typeCode,
      description: typeDescriptions[typeCode] ?? typeCode,
      count: Number(row.count ?? 0),
      total: r2(Number(row.total ?? 0)),
    };
  });
}

export interface MasterTotalsResult {
  total_balance: number;
  count: number;
}

/**
 * Total balance and count from the master table (only non-zero balances).
 */
export async function fetchMasterTotals(
  db: Knex,
  spec: LedgerSpec,
): Promise<MasterTotalsResult> {
  const label = spec.masterTable === 'pname' ? 'supplier_count' : 'customer_count';
  const sql = `
    SELECT
      COUNT(*) AS ${label},
      SUM(${spec.masterBalanceField}) AS total_balance
    FROM ${spec.masterTable} WITH (NOLOCK)
    WHERE ${spec.masterBalanceField} <> 0
  `;
  const rows = (await db.raw(sql)) as unknown as Array<{
    [k: string]: number | null;
  }>;
  const row = Array.isArray(rows) ? rows[0] : null;
  return {
    total_balance: Number(row?.total_balance ?? 0),
    count: Number((row?.[label] as number | null) ?? 0),
  };
}

export interface MasterTxnVarianceRow {
  account: string;
  name: string;
  master_balance: number;
  transaction_balance: number;
  variance: number;
}

/**
 * Per-account rows where master balance disagrees with txn-table balance.
 *
 * Used to highlight specific accounts that need investigation when the
 * sub-ledger ↔ control-account reconciliation breaks. Variance threshold
 * is 0.01 (one penny) — this is a finance system, no tolerance.
 */
export async function fetchMasterTxnVariance(
  db: Knex,
  spec: LedgerSpec,
): Promise<MasterTxnVarianceRow[]> {
  const sql = `
    SELECT
      m.${spec.masterAccountField} AS account,
      RTRIM(m.${spec.masterNameField}) AS name,
      m.${spec.masterBalanceField} AS master_balance,
      COALESCE(t.txn_balance, 0) AS transaction_balance,
      m.${spec.masterBalanceField} - COALESCE(t.txn_balance, 0) AS variance
    FROM ${spec.masterTable} m WITH (NOLOCK)
    LEFT JOIN (
      SELECT ${spec.txnAccountField}, SUM(${spec.txnBalanceField}) AS txn_balance
      FROM ${spec.txnTable} WITH (NOLOCK)
      GROUP BY ${spec.txnAccountField}
    ) t ON RTRIM(m.${spec.masterAccountField}) = RTRIM(t.${spec.txnAccountField})
    WHERE ABS(m.${spec.masterBalanceField} - COALESCE(t.txn_balance, 0)) >= 0.01
    ORDER BY ABS(m.${spec.masterBalanceField} - COALESCE(t.txn_balance, 0)) DESC
  `;
  let rows: Array<{
    account: string | null;
    name: string | null;
    master_balance: number | null;
    transaction_balance: number | null;
    variance: number | null;
  }> = [];
  try {
    const result = (await db.raw(sql)) as unknown as typeof rows;
    rows = Array.isArray(result) ? result : [];
  } catch {
    rows = [];
  }
  return rows.map((row) => ({
    account: row.account ? String(row.account).trim() : '',
    name: row.name ?? '',
    master_balance: r2(Number(row.master_balance ?? 0)),
    transaction_balance: r2(Number(row.transaction_balance ?? 0)),
    variance: r2(Number(row.variance ?? 0)),
  }));
}

export interface TransferFilePendingRow {
  nominal_account: string;
  type: string;
  date: string;
  value: number;
  reference: string;
  comment: string;
}

/**
 * Transactions sitting in the transfer file (pnoml/snoml) waiting to post to NL.
 *
 * Done flag = 'Y' means already posted; anything else is pending.
 * Column naming differs between pnoml (px_*) and snoml (sx_*) so the
 * SELECT spells columns explicitly.
 */
export async function fetchTransferFilePending(
  db: Knex,
  spec: LedgerSpec,
): Promise<TransferFilePendingRow[]> {
  let sql: string;
  if (spec.transferTable === 'pnoml') {
    sql = `
      SELECT
        px_nacnt AS nominal_account,
        px_type AS type,
        px_date AS date,
        px_value AS value,
        px_tref AS reference,
        px_comment AS comment,
        px_done AS status
      FROM pnoml WITH (NOLOCK)
      WHERE px_done <> 'Y' OR px_done IS NULL
      ORDER BY px_date DESC
    `;
  } else {
    // snoml
    sql = `
      SELECT
        sx_nacnt AS nominal_account,
        sx_type AS type,
        sx_date AS date,
        sx_value AS value,
        sx_tref AS reference,
        sx_comment AS comment,
        sx_done AS status
      FROM snoml WITH (NOLOCK)
      WHERE sx_done <> 'Y' OR sx_done IS NULL
      ORDER BY sx_date DESC
    `;
  }
  let rows: Array<{
    nominal_account: string | null;
    type: string | null;
    date: Date | string | null;
    value: number | null;
    reference: string | null;
    comment: string | null;
  }> = [];
  try {
    const result = (await db.raw(sql)) as unknown as typeof rows;
    rows = Array.isArray(result) ? result : [];
  } catch {
    rows = [];
  }
  return rows.map((row) => {
    let dateStr = '';
    if (row.date instanceof Date) {
      const yr = row.date.getFullYear();
      const mo = String(row.date.getMonth() + 1).padStart(2, '0');
      const da = String(row.date.getDate()).padStart(2, '0');
      dateStr = `${yr}-${mo}-${da}`;
    } else if (row.date) {
      dateStr = String(row.date);
    }
    return {
      nominal_account: row.nominal_account ? String(row.nominal_account).trim() : '',
      type: row.type ? String(row.type).trim() : '',
      date: dateStr,
      value: r2(Number(row.value ?? 0)),
      reference: row.reference ? String(row.reference).trim() : '',
      comment: row.comment ? String(row.comment).trim() : '',
    };
  });
}

export interface TransferFileSummaryResult {
  posted: { count: number; total: number };
  pending: { count: number; total: number };
}

/**
 * Posted vs pending counts/totals from the transfer file.
 */
export async function fetchTransferFileSummary(
  db: Knex,
  spec: LedgerSpec,
): Promise<TransferFileSummaryResult> {
  let sql: string;
  if (spec.transferTable === 'pnoml') {
    sql = `
      SELECT
        CASE WHEN px_done = 'Y' THEN 'Posted' ELSE 'Pending' END AS status,
        COUNT(*) AS count,
        SUM(px_value) AS total
      FROM pnoml WITH (NOLOCK)
      GROUP BY CASE WHEN px_done = 'Y' THEN 'Posted' ELSE 'Pending' END
    `;
  } else {
    sql = `
      SELECT
        CASE WHEN sx_done = 'Y' THEN 'Posted' ELSE 'Pending' END AS status,
        COUNT(*) AS count,
        SUM(sx_value) AS total
      FROM snoml WITH (NOLOCK)
      GROUP BY CASE WHEN sx_done = 'Y' THEN 'Posted' ELSE 'Pending' END
    `;
  }
  let rows: Array<{ status: string | null; count: number | null; total: number | null }> = [];
  try {
    const result = (await db.raw(sql)) as unknown as typeof rows;
    rows = Array.isArray(result) ? result : [];
  } catch {
    rows = [];
  }
  let posted = { count: 0, total: 0 };
  let pending = { count: 0, total: 0 };
  for (const row of rows) {
    if (row.status === 'Posted') {
      posted = { count: Number(row.count ?? 0), total: r2(Number(row.total ?? 0)) };
    } else {
      pending = { count: Number(row.count ?? 0), total: r2(Number(row.total ?? 0)) };
    }
  }
  return { posted, pending };
}
