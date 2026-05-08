/**
 * Opera SQL helpers — shared across all plugins.
 */

export {
  getControlAccounts,
  clearControlAccountsCache,
} from './control-accounts.js';
export type { OperaControlAccounts } from './control-accounts.js';

export { fetchVatCodesWithRates } from './vat-rates.js';
export type { VatCodeRow, VatCodesWithRatesResult } from './vat-rates.js';

export {
  getPeriodForDate,
  getCurrentPeriodInfo,
  getPeriodStatus,
  isOpenPeriodAccountingEnabled,
  isRealTimeUpdateEnabled,
  validatePostingPeriod,
  getLedgerTypeForTransaction,
} from './period-validation.js';
export type {
  LedgerType,
  PeriodInfo,
  PeriodValidationResult,
} from './period-validation.js';
