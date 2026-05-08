/**
 * Reconcile summary service — faithful port of `reconcile_summary()` in
 * `apps/balance_check/api/routes.py` (the Python implementation, lines
 * 1649-1970).
 *
 * Quick overview of all reconciliation checks — shows at a glance whether
 * everything balances. Read-only against Opera SQL.
 *
 * The four checks (debtors, creditors, cashbook, VAT) are each wrapped in
 * try/catch so a failure in one doesn't break the others — each appears
 * in `checks` with `reconciled: false` + `error` if it failed.
 */
import type { Knex } from 'knex';
import { getControlAccounts } from '@sqlrag/sam-shared';
import type {
  DetailRow,
  ReconcileCheck,
  ReconcileSummaryResponse,
  VarianceRow,
} from '../types.js';

/**
 * Round to two decimals — matches Python's `round(x, 2)`.
 */
function r2(n: number): number {
  return Math.round(n * 100) / 100;
}

/**
 * Format the current timestamp as the Python code does:
 *   datetime.now().strftime('%Y-%m-%d %H:%M:%S')
 */
function formatNow(): string {
  const now = new Date();
  const pad = (n: number, w = 2) => String(n).padStart(w, '0');
  return (
    `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())} ` +
    `${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`
  );
}

export async function reconcileSummary(
  db: Knex,
): Promise<ReconcileSummaryResponse> {
  const summary: ReconcileSummaryResponse = {
    success: true,
    reconciliation_date: formatNow(),
    checks: [],
    all_reconciled: true,
    total_checks: 0,
    passed_checks: 0,
    failed_checks: 0,
  };

  try {
    // Get control accounts (NEVER hardcode — see CLAUDE.md mandate)
    const controlAccounts = await getControlAccounts(db);

    // Get current fiscal year — used to filter ntran sums to "current year only"
    // so the year-opening B/Fwd row + this year's transactions = full closing
    // balance, regardless of whether prior-year detail rows are still in ntran.
    const fyResult = await db('ntran')
      .select(db.raw('MAX(nt_year) AS current_year'))
      .first();
    const fiscalCurrentYear = Number(fyResult?.current_year) || new Date().getFullYear();

    // ========== 1. DEBTORS CHECK ==========
    summary.checks.push(
      await runDebtorsCheck(db, controlAccounts.debtorsControl, fiscalCurrentYear),
    );

    // ========== 2. CREDITORS CHECK ==========
    summary.checks.push(
      await runCreditorsCheck(db, controlAccounts.creditorsControl, fiscalCurrentYear),
    );

    // ========== 3. CASHBOOK CHECK ==========
    summary.checks.push(await runCashbookCheck(db, fiscalCurrentYear));

    // ========== 4. VAT CHECK ==========
    summary.checks.push(await runVatCheck(db));

    // Calculate overall status
    summary.total_checks = summary.checks.length;
    summary.passed_checks = summary.checks.filter((c) => c.reconciled).length;
    summary.failed_checks = summary.total_checks - summary.passed_checks;
    summary.all_reconciled = summary.failed_checks === 0;

    return summary;
  } catch (err: any) {
    return {
      ...summary,
      success: false,
      error: err?.message ?? String(err),
    };
  }
}

async function runDebtorsCheck(
  db: Knex,
  debtorsControl: string,
  fiscalCurrentYear: number,
): Promise<ReconcileCheck> {
  try {
    // Sales Ledger total — exclude orphan stran rows (account no longer in sname).
    const slRow = await db('stran')
      .with('valid_accounts', db('sname').select(db.raw('RTRIM(sn_account) AS acct')))
      .select(db.raw('SUM(st_trbal) AS total'))
      .where('st_trbal', '<>', 0)
      .whereRaw('RTRIM(st_account) IN (SELECT RTRIM(sn_account) FROM sname WITH (NOLOCK))')
      .first();
    const slTotal = Number(slRow?.total ?? 0);

    // Customer master total
    const snameRow = await db('sname')
      .select(db.raw('SUM(sn_currbal) AS total'))
      .where('sn_currbal', '<>', 0)
      .first();
    const snameTotal = Number(snameRow?.total ?? 0);

    // NL Debtors control — current fiscal year only
    const nlRow = await db('ntran')
      .select(db.raw('SUM(nt_value) AS total'))
      .where('nt_acnt', debtorsControl)
      .andWhere('nt_year', fiscalCurrentYear)
      .first();
    const nlDebtorsTotal = Number(nlRow?.total ?? 0);

    // Variances — exact to the penny (no tolerance: this is a finance system).
    const slVsSnameVariance = Math.abs(slTotal - snameTotal);
    const slVsSnameOk = r2(slVsSnameVariance) === 0;

    const slVsNlVariance = Math.abs(slTotal - nlDebtorsTotal);
    const slVsNlOk = r2(slVsNlVariance) === 0;

    const debtorsOk = slVsSnameOk && slVsNlOk;

    const details: DetailRow[] = [
      { label: 'Sales Ledger (stran)', value: r2(slTotal) },
      { label: 'Customer Master (sname)', value: r2(snameTotal) },
      { label: `Nominal (${debtorsControl})`, value: r2(nlDebtorsTotal) },
    ];
    const variances: VarianceRow[] = [
      { label: 'SL vs Master', value: r2(slVsSnameVariance), ok: slVsSnameOk },
      { label: 'SL vs NL', value: r2(slVsNlVariance), ok: slVsNlOk },
    ];

    return {
      name: 'Debtors',
      icon: 'users',
      reconciled: debtorsOk,
      details,
      variances,
    };
  } catch (err: any) {
    return {
      name: 'Debtors',
      icon: 'users',
      reconciled: false,
      error: err?.message ?? String(err),
    };
  }
}

async function runCreditorsCheck(
  db: Knex,
  creditorsControl: string,
  fiscalCurrentYear: number,
): Promise<ReconcileCheck> {
  try {
    // Purchase Ledger total — exclude orphan ptran rows
    const plRow = await db('ptran')
      .select(db.raw('SUM(pt_trbal) AS total'))
      .where('pt_trbal', '<>', 0)
      .whereRaw('RTRIM(pt_account) IN (SELECT RTRIM(pn_account) FROM pname WITH (NOLOCK))')
      .first();
    const plTotal = Number(plRow?.total ?? 0);

    // Supplier master total
    const pnameRow = await db('pname')
      .select(db.raw('SUM(pn_currbal) AS total'))
      .where('pn_currbal', '<>', 0)
      .first();
    const pnameTotal = Number(pnameRow?.total ?? 0);

    // NL Creditors control (negate for comparison — NL is opposite sign).
    const nlRow = await db('ntran')
      .select(db.raw('SUM(nt_value) AS total'))
      .where('nt_acnt', creditorsControl)
      .andWhere('nt_year', fiscalCurrentYear)
      .first();
    const nlCreditorsTotal = -Number(nlRow?.total ?? 0);

    // Variances — exact to the penny.
    const plVsPnameVariance = Math.abs(plTotal - pnameTotal);
    const plVsPnameOk = r2(plVsPnameVariance) === 0;

    const plVsNlVariance = Math.abs(plTotal - nlCreditorsTotal);
    const plVsNlOk = r2(plVsNlVariance) === 0;

    const creditorsOk = plVsPnameOk && plVsNlOk;

    const details: DetailRow[] = [
      { label: 'Purchase Ledger (ptran)', value: r2(plTotal) },
      { label: 'Supplier Master (pname)', value: r2(pnameTotal) },
      { label: `Nominal (${creditorsControl})`, value: r2(nlCreditorsTotal) },
    ];
    const variances: VarianceRow[] = [
      { label: 'PL vs Master', value: r2(plVsPnameVariance), ok: plVsPnameOk },
      { label: 'PL vs NL', value: r2(plVsNlVariance), ok: plVsNlOk },
    ];

    return {
      name: 'Creditors',
      icon: 'building',
      reconciled: creditorsOk,
      details,
      variances,
    };
  } catch (err: any) {
    return {
      name: 'Creditors',
      icon: 'building',
      reconciled: false,
      error: err?.message ?? String(err),
    };
  }
}

async function runCashbookCheck(
  db: Knex,
  fiscalCurrentYear: number,
): Promise<ReconcileCheck> {
  try {
    // Get bank accounts — nk_acnt is both the bank code AND the nominal code
    const banks = await db('nbank').select('nk_acnt', 'nk_curbal');

    let bankMasterTotal = 0;
    let nlBankTotal = 0;

    for (const bank of banks) {
      const bankCode = String(bank.nk_acnt ?? '').trim();
      // nk_curbal is in pence
      const masterBal = Number(bank.nk_curbal ?? 0) / 100.0;
      bankMasterTotal += masterBal;

      // In Opera, bank account code IS the nominal code (e.g., BC010).
      // Filter to current fiscal year — B/Fwd opener + this year's
      // transactions = full closing balance, no double-count.
      const nlRow = await db('ntran')
        .select(db.raw('SUM(nt_value) AS total'))
        .where('nt_acnt', bankCode)
        .andWhere('nt_year', fiscalCurrentYear)
        .first();
      const nlBal = Number(nlRow?.total ?? 0);
      nlBankTotal += nlBal;
    }

    // Bank vs NL — exact to the penny.
    const bankVariance = Math.abs(bankMasterTotal - nlBankTotal);
    const cashbookOk = r2(bankVariance) === 0;

    const details: DetailRow[] = [
      { label: 'Bank Master (nbank)', value: r2(bankMasterTotal) },
      { label: 'Nominal Ledger', value: r2(nlBankTotal) },
    ];
    const variances: VarianceRow[] = [
      { label: 'Bank vs NL', value: r2(bankVariance), ok: cashbookOk },
    ];

    return {
      name: 'Cashbook',
      icon: 'book',
      reconciled: cashbookOk,
      details,
      variances,
    };
  } catch (err: any) {
    return {
      name: 'Cashbook',
      icon: 'book',
      reconciled: false,
      error: err?.message ?? String(err),
    };
  }
}

async function runVatCheck(db: Knex): Promise<ReconcileCheck> {
  try {
    // Get VAT nominal accounts from ztax
    const ztaxRows = await db('ztax')
      .distinct('tx_nominal', 'tx_trantyp')
      .where('tx_ctrytyp', 'H')
      .andWhereNot('tx_nominal', null)
      .andWhereNot('tx_nominal', '');

    const outputNominals = new Set<string>();
    const inputNominals = new Set<string>();
    for (const row of ztaxRows) {
      const nominal = String(row.tx_nominal ?? '').trim();
      const vatType = String(row.tx_trantyp ?? '').trim();
      if (nominal) {
        if (vatType === 'S') outputNominals.add(nominal);
        else if (vatType === 'P') inputNominals.add(nominal);
      }
    }

    // Get current calendar year — derived from the latest calendar-year
    // of nominal activity (NOT fiscal year — nvat is filtered by
    // YEAR(nv_date) which is calendar).
    const cyRow = await db('ntran')
      .select(db.raw('MAX(YEAR(nt_entr)) AS current_year'))
      .first();
    const currentYear = Number(cyRow?.current_year) || new Date().getFullYear();

    // nvat output VAT — calendar year via YEAR(nv_date)
    const nvatOutputRow = await db('nvat')
      .select(db.raw('SUM(nv_vatval) AS total'))
      .where('nv_vattype', 'S')
      .andWhereRaw('YEAR(nv_date) = ?', [currentYear])
      .first();
    const nvatOutput = Number(nvatOutputRow?.total ?? 0);

    // nvat input VAT — calendar year
    const nvatInputRow = await db('nvat')
      .select(db.raw('SUM(nv_vatval) AS total'))
      .where('nv_vattype', 'P')
      .andWhereRaw('YEAR(nv_date) = ?', [currentYear])
      .first();
    const nvatInput = Number(nvatInputRow?.total ?? 0);

    const nvatNet = nvatOutput - nvatInput;

    // NL VAT totals — sum across all output + input nominals
    let nlVatTotal = 0;
    const allVatNominals = new Set<string>([...outputNominals, ...inputNominals]);
    for (const acnt of allVatNominals) {
      const nlRow = await db('ntran')
        .select(db.raw('SUM(nt_value) AS total'))
        .where('nt_acnt', acnt)
        .andWhereRaw('YEAR(nt_entr) = ?', [currentYear])
        .first();
      // Negate — NL is opposite sign to nvat
      nlVatTotal += -Number(nlRow?.total ?? 0);
    }

    // VAT variance — exact to the penny.
    const vatVariance = Math.abs(nvatNet - nlVatTotal);
    const vatOk = r2(vatVariance) === 0;

    const details: DetailRow[] = [
      { label: 'Output VAT (nvat)', value: r2(nvatOutput) },
      { label: 'Input VAT (nvat)', value: r2(nvatInput) },
      { label: 'Net VAT (nvat)', value: r2(nvatNet) },
      { label: 'Nominal Ledger', value: r2(nlVatTotal) },
    ];
    const variances: VarianceRow[] = [
      { label: 'nvat vs NL', value: r2(vatVariance), ok: vatOk },
    ];

    return {
      name: 'VAT',
      icon: 'receipt',
      reconciled: vatOk,
      details,
      variances,
    };
  } catch (err: any) {
    return {
      name: 'VAT',
      icon: 'receipt',
      reconciled: false,
      error: err?.message ?? String(err),
    };
  }
}
