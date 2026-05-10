/**
 * Opera period validation — controls which dates are allowed for new
 * postings.
 *
 * Faithful port of the period helpers in `sql_rag/opera_config.py`:
 *   - get_period_for_date           (nclndd lookup)
 *   - get_current_period_info       (nparm — np_year, np_perno, np_periods)
 *   - get_period_status             (nclndd ledger-specific status)
 *   - is_open_period_accounting_enabled (seqco.co_opanl with nparm fallback)
 *   - is_real_time_update_enabled   (seqco.co_rtupdnl)
 *   - validate_posting_period       (high-level orchestration)
 *   - get_ledger_type_for_transaction (transaction-type → ledger code)
 *
 * Used by GoCardless validate-date, the bank-reconcile import flows,
 * and any service that needs to gate writes on Opera's
 * period-status rules.
 *
 * Period status values:
 *   0 = Open    (writes allowed)
 *   1 = Blocked
 *   2 = Closed
 */
import type { Knex } from 'knex';

export type LedgerType = 'NL' | 'SL' | 'PL' | 'ST' | 'WG' | 'FA';

const STATUS_FIELD: Record<LedgerType, string> = {
  NL: 'ncd_nlstat',
  SL: 'ncd_slstat',
  PL: 'ncd_plstat',
  ST: 'ncd_ststat',
  WG: 'ncd_wgstat',
  FA: 'ncd_fastat',
};

const LEDGER_NAMES: Record<LedgerType, string> = {
  NL: 'Nominal Ledger',
  SL: 'Sales Ledger',
  PL: 'Purchase Ledger',
  ST: 'Stock',
  WG: 'Wages',
  FA: 'Fixed Assets',
};

export interface PeriodInfo {
  np_year: number | null;
  np_perno: number | null;
  np_periods: number; // default 12
}

export interface PeriodValidationResult {
  is_valid: boolean;
  error_message?: string | null;
  year: number;
  period: number;
  open_period_accounting: boolean;
}

/**
 * Mirrors the Python `PeriodPostingDecision` dataclass in
 * `sql_rag/opera_config.py`.
 *
 * Drives how a transaction is written:
 *   - `can_post=false` → reject (use `error_message` for UX)
 *   - `post_to_nominal=true` → write ntran + nacnt updates
 *   - `post_to_transfer_file=true` → write anoml; `done_flag` is the ax_done value
 */
export interface PeriodPostingDecision {
  can_post: boolean;
  post_to_nominal: boolean;
  post_to_transfer_file: boolean;
  /** `'Y'` (posted to NL) or `' '` (pending transfer). */
  transfer_file_done_flag: ' ' | 'Y';
  error_message: string | null;
  current_year: number | null;
  current_period: number | null;
  transaction_year: number;
  transaction_period: number;
}

// ---------------------------------------------------------------------
// Date parsing
// ---------------------------------------------------------------------

function parseDate(input: Date | string): Date {
  if (input instanceof Date) {
    if (Number.isNaN(input.getTime())) {
      throw new Error(`Invalid Date object`);
    }
    return input;
  }
  const trimmed = input.trim();
  // Match strict YYYY-MM-DD; reject other formats (mirrors Python's strptime)
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(trimmed);
  if (!m) {
    throw new Error(`Invalid date format: ${input}. Use YYYY-MM-DD`);
  }
  const [, y, mo, d] = m;
  // UTC date so timezone-shifted ISO strings don't drift
  const dt = new Date(Date.UTC(Number(y), Number(mo) - 1, Number(d)));
  if (Number.isNaN(dt.getTime())) {
    throw new Error(`Invalid date: ${input}`);
  }
  return dt;
}

function toIsoDate(d: Date): string {
  return d.toISOString().slice(0, 10);
}

// ---------------------------------------------------------------------
// get_period_for_date — nclndd lookup, fallback to calendar month
// ---------------------------------------------------------------------

export async function getPeriodForDate(
  operaDb: Knex,
  postDate: Date | string,
): Promise<{ period: number; year: number }> {
  const d = parseDate(postDate);
  const iso = toIsoDate(d);

  try {
    const rows = (await operaDb.raw(
      `SELECT TOP 1 ncd_period, ncd_year
       FROM nclndd WITH (NOLOCK)
       WHERE ncd_stdate <= ? AND ncd_endate >= ?`,
      [iso, iso],
    )) as unknown as Array<{ ncd_period: number; ncd_year: number }>;
    if (Array.isArray(rows) && rows.length > 0 && rows[0]) {
      return {
        period: Number(rows[0].ncd_period),
        year: Number(rows[0].ncd_year),
      };
    }
  } catch {
    // fall through to calendar-month fallback
  }

  // Calendar-month fallback (matches Python)
  return { period: d.getUTCMonth() + 1, year: d.getUTCFullYear() };
}

// ---------------------------------------------------------------------
// get_current_period_info — nparm
// ---------------------------------------------------------------------

export async function getCurrentPeriodInfo(operaDb: Knex): Promise<PeriodInfo> {
  try {
    const rows = (await operaDb.raw(
      `SELECT TOP 1 np_year, np_perno, np_periods
       FROM nparm WITH (NOLOCK)`,
    )) as unknown as Array<{
      np_year: number | null;
      np_perno: number | null;
      np_periods: number | null;
    }>;
    if (Array.isArray(rows) && rows.length > 0 && rows[0]) {
      const r = rows[0];
      return {
        np_year: r.np_year ? Number(r.np_year) : null,
        np_perno: r.np_perno ? Number(r.np_perno) : null,
        np_periods: r.np_periods ? Number(r.np_periods) : 12,
      };
    }
  } catch {
    // fall through
  }
  return { np_year: null, np_perno: null, np_periods: 12 };
}

// ---------------------------------------------------------------------
// get_period_status — ledger-specific status from nclndd
// ---------------------------------------------------------------------

export async function getPeriodStatus(
  operaDb: Knex,
  year: number,
  period: number,
  ledgerType: LedgerType,
): Promise<number | null> {
  const field = STATUS_FIELD[ledgerType];
  if (!field) {
    throw new Error(
      `Invalid ledger_type: ${ledgerType}. Must be one of ${Object.keys(STATUS_FIELD).join(', ')}`,
    );
  }
  try {
    const rows = (await operaDb.raw(
      `SELECT ${field} AS period_status
       FROM nclndd WITH (NOLOCK)
       WHERE ncd_year = ? AND ncd_period = ?`,
      [year, period],
    )) as unknown as Array<{ period_status: number | null }>;
    if (Array.isArray(rows) && rows.length > 0 && rows[0]?.period_status != null) {
      return Number(rows[0].period_status);
    }
  } catch {
    // fall through
  }
  return null;
}

// ---------------------------------------------------------------------
// is_open_period_accounting_enabled — seqco.co_opanl with nparm fallback
// ---------------------------------------------------------------------

export async function isOpenPeriodAccountingEnabled(
  operaDb: Knex,
): Promise<boolean> {
  // Primary: seqco in the system DB
  try {
    const rows = (await operaDb.raw(
      `SELECT co_opanl
       FROM Opera3SESystem.dbo.seqco WITH (NOLOCK)
       WHERE co_code = RIGHT(DB_NAME(), 1)`,
    )) as unknown as Array<{ co_opanl: number | boolean | null }>;
    if (Array.isArray(rows) && rows.length > 0 && rows[0]) {
      return !!rows[0].co_opanl;
    }
  } catch {
    // fall through to nparm fallback
  }

  // Fallback: nparm.np_opawarn (older SE installs)
  try {
    const rows = (await operaDb.raw(
      `SELECT TOP 1 np_opawarn FROM nparm WITH (NOLOCK)`,
    )) as unknown as Array<{ np_opawarn: number | boolean | null }>;
    if (Array.isArray(rows) && rows.length > 0 && rows[0]) {
      return !!rows[0].np_opawarn;
    }
  } catch {
    // fall through
  }

  // Default to disabled (stricter mode) — same as Python
  return false;
}

// ---------------------------------------------------------------------
// is_real_time_update_enabled — seqco.co_rtupdnl
// ---------------------------------------------------------------------

export async function isRealTimeUpdateEnabled(operaDb: Knex): Promise<boolean> {
  try {
    const rows = (await operaDb.raw(
      `SELECT co_rtupdnl
       FROM Opera3SESystem.dbo.seqco WITH (NOLOCK)
       WHERE co_code = RIGHT(DB_NAME(), 1)`,
    )) as unknown as Array<{ co_rtupdnl: number | boolean | null }>;
    if (Array.isArray(rows) && rows.length > 0 && rows[0]) {
      return !!rows[0].co_rtupdnl;
    }
  } catch {
    // Default to disabled (batch-transfer mode) — same as Python
  }
  return false;
}

// ---------------------------------------------------------------------
// validate_posting_period — high-level orchestration
// ---------------------------------------------------------------------

export async function validatePostingPeriod(
  operaDb: Knex,
  postDate: Date | string,
  ledgerType: LedgerType = 'NL',
): Promise<PeriodValidationResult> {
  const { period, year } = await getPeriodForDate(operaDb, postDate);
  const opaEnabled = await isOpenPeriodAccountingEnabled(operaDb);

  if (opaEnabled) {
    // Always check NL first (master gatekeeper)
    const nlStatus = await getPeriodStatus(operaDb, year, period, 'NL');
    if (nlStatus === null) {
      return {
        is_valid: false,
        error_message: `Period ${period}/${year} not found in calendar (nclndd)`,
        year,
        period,
        open_period_accounting: true,
      };
    }
    if (nlStatus !== 0) {
      const desc = nlStatus === 2 ? 'closed' : 'blocked';
      return {
        is_valid: false,
        error_message: `Nominal Ledger is ${desc} for period ${period}/${year} — all ledgers blocked`,
        year,
        period,
        open_period_accounting: true,
      };
    }

    // Sub-ledger check (if not NL)
    if (ledgerType !== 'NL') {
      const subStatus = await getPeriodStatus(operaDb, year, period, ledgerType);
      if (subStatus !== null && subStatus !== 0) {
        const desc = subStatus === 2 ? 'closed' : 'blocked';
        return {
          is_valid: false,
          error_message: `${LEDGER_NAMES[ledgerType]} is ${desc} for period ${period}/${year}`,
          year,
          period,
          open_period_accounting: true,
        };
      }
    }

    return { is_valid: true, year, period, open_period_accounting: true };
  }

  // OPA OFF: only the current period is allowed
  const current = await getCurrentPeriodInfo(operaDb);
  if (current.np_year === null || current.np_perno === null) {
    // Can't determine — allow (matches Python's permissive fallback)
    return { is_valid: true, year, period, open_period_accounting: false };
  }
  if (year !== current.np_year || period !== current.np_perno) {
    return {
      is_valid: false,
      error_message: `Period ${period}/${year} is blocked. Current period is ${current.np_perno}/${current.np_year}. Open Period Accounting is disabled.`,
      year,
      period,
      open_period_accounting: false,
    };
  }
  return { is_valid: true, year, period, open_period_accounting: false };
}

// ---------------------------------------------------------------------
// getPeriodPostingDecision — port of get_period_posting_decision
// (sql_rag/opera_config.py:848-1037). Returns the structured decision
// the Opera writers use to choose between immediate NL posting and
// transfer-file-only posting.
// ---------------------------------------------------------------------

export async function getPeriodPostingDecision(
  operaDb: Knex,
  postDate: Date | string,
  ledgerType: LedgerType = 'NL',
): Promise<PeriodPostingDecision> {
  const { period: txnPeriod, year: txnYear } = await getPeriodForDate(
    operaDb,
    postDate,
  );
  const current = await getCurrentPeriodInfo(operaDb);
  const opaEnabled = await isOpenPeriodAccountingEnabled(operaDb);
  const realTimeUpdate = await isRealTimeUpdateEnabled(operaDb);

  const currentYear = current.np_year;
  const currentPeriod = current.np_perno;

  // Could not read current period — Python falls back to transfer-only
  if (currentYear === null || currentPeriod === null) {
    return {
      can_post: true,
      post_to_nominal: false,
      post_to_transfer_file: true,
      transfer_file_done_flag: ' ',
      error_message: null,
      current_year: currentYear,
      current_period: currentPeriod,
      transaction_year: txnYear,
      transaction_period: txnPeriod,
    };
  }

  // Step 1: OPA gating — NL is master gatekeeper
  if (opaEnabled) {
    const nlStatus = await getPeriodStatus(operaDb, txnYear, txnPeriod, 'NL');
    if (nlStatus === null) {
      return {
        can_post: false,
        post_to_nominal: false,
        post_to_transfer_file: false,
        transfer_file_done_flag: ' ',
        error_message: `Period ${txnPeriod}/${txnYear} not found in calendar (nclndd)`,
        current_year: currentYear,
        current_period: currentPeriod,
        transaction_year: txnYear,
        transaction_period: txnPeriod,
      };
    }
    if (nlStatus !== 0) {
      const desc = nlStatus === 2 ? 'closed' : 'blocked';
      return {
        can_post: false,
        post_to_nominal: false,
        post_to_transfer_file: false,
        transfer_file_done_flag: ' ',
        error_message: `Period ${txnPeriod}/${txnYear} is ${desc} for NL — all ledgers blocked`,
        current_year: currentYear,
        current_period: currentPeriod,
        transaction_year: txnYear,
        transaction_period: txnPeriod,
      };
    }

    // Sub-ledger check for SL/PL/etc.
    if (ledgerType !== 'NL') {
      const subStatus = await getPeriodStatus(
        operaDb,
        txnYear,
        txnPeriod,
        ledgerType,
      );
      if (subStatus === null) {
        return {
          can_post: false,
          post_to_nominal: false,
          post_to_transfer_file: false,
          transfer_file_done_flag: ' ',
          error_message: `Period ${txnPeriod}/${txnYear} not found in calendar for ${ledgerType}`,
          current_year: currentYear,
          current_period: currentPeriod,
          transaction_year: txnYear,
          transaction_period: txnPeriod,
        };
      }
      if (subStatus !== 0) {
        const desc = subStatus === 2 ? 'closed' : 'blocked';
        return {
          can_post: false,
          post_to_nominal: false,
          post_to_transfer_file: false,
          transfer_file_done_flag: ' ',
          error_message: `Period ${txnPeriod}/${txnYear} is ${desc} for ${ledgerType}`,
          current_year: currentYear,
          current_period: currentPeriod,
          transaction_year: txnYear,
          transaction_period: txnPeriod,
        };
      }
    }
  } else {
    // OPA OFF: Only current period allowed
    if (txnYear !== currentYear || txnPeriod !== currentPeriod) {
      return {
        can_post: false,
        post_to_nominal: false,
        post_to_transfer_file: false,
        transfer_file_done_flag: ' ',
        error_message:
          `Period ${txnPeriod}/${txnYear} is blocked. ` +
          `Current period is ${currentPeriod}/${currentYear}. ` +
          `Open Period Accounting is disabled.`,
        current_year: currentYear,
        current_period: currentPeriod,
        transaction_year: txnYear,
        transaction_period: txnPeriod,
      };
    }
  }

  // Step 2: Real Time Update OFF → transfer file only, done=' '
  if (!realTimeUpdate) {
    return {
      can_post: true,
      post_to_nominal: false,
      post_to_transfer_file: true,
      transfer_file_done_flag: ' ',
      error_message: null,
      current_year: currentYear,
      current_period: currentPeriod,
      transaction_year: txnYear,
      transaction_period: txnPeriod,
    };
  }

  // Step 3: Real Time Update ON
  // Period >= current → post to NL immediately
  if (
    txnYear > currentYear ||
    (txnYear === currentYear && txnPeriod >= currentPeriod)
  ) {
    return {
      can_post: true,
      post_to_nominal: true,
      post_to_transfer_file: true,
      transfer_file_done_flag: 'Y',
      error_message: null,
      current_year: currentYear,
      current_period: currentPeriod,
      transaction_year: txnYear,
      transaction_period: txnPeriod,
    };
  }

  // Backdated within same financial year → still post immediately
  if (txnYear === currentYear) {
    return {
      can_post: true,
      post_to_nominal: true,
      post_to_transfer_file: true,
      transfer_file_done_flag: 'Y',
      error_message: null,
      current_year: currentYear,
      current_period: currentPeriod,
      transaction_year: txnYear,
      transaction_period: txnPeriod,
    };
  }

  // Prior financial year → reject (mirrors Python)
  return {
    can_post: false,
    post_to_nominal: false,
    post_to_transfer_file: false,
    transfer_file_done_flag: ' ',
    error_message:
      `Cannot post to prior financial year (${txnYear}). ` +
      `Current financial year is ${currentYear}. ` +
      `Please change the posting date to the current year.`,
    current_year: currentYear,
    current_period: currentPeriod,
    transaction_year: txnYear,
    transaction_period: txnPeriod,
  };
}

// ---------------------------------------------------------------------
// transaction_type → ledger mapping (mirrors Python)
// ---------------------------------------------------------------------

export function getLedgerTypeForTransaction(
  transactionType: string,
): LedgerType {
  const map: Record<string, LedgerType> = {
    sales_receipt: 'SL',
    sales_refund: 'SL',
    sales_invoice: 'SL',
    sales_credit: 'SL',
    purchase_payment: 'PL',
    purchase_refund: 'PL',
    purchase_invoice: 'PL',
    purchase_credit: 'PL',
    nominal_payment: 'NL',
    nominal_receipt: 'NL',
    bank_transfer: 'NL',
  };
  return map[transactionType] ?? 'NL';
}
