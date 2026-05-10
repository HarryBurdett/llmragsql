/**
 * Opera SQL helpers — shared across all plugins.
 */

export {
  getControlAccounts,
  getCustomerControlAccount,
  getSupplierControlAccount,
  clearControlAccountsCache,
} from './control-accounts.js';
export type { OperaControlAccounts } from './control-accounts.js';

export { fetchVatCodesWithRates, getVatRate } from './vat-rates.js';
export type {
  VatCodeRow,
  VatCodesWithRatesResult,
  VatRateLookup,
} from './vat-rates.js';

export {
  getPeriodForDate,
  getCurrentPeriodInfo,
  getPeriodStatus,
  isOpenPeriodAccountingEnabled,
  isRealTimeUpdateEnabled,
  validatePostingPeriod,
  getPeriodPostingDecision,
  getLedgerTypeForTransaction,
} from './period-validation.js';

export { getHomeCurrency, clearHomeCurrencyCache } from './home-currency.js';
export type { HomeCurrency } from './home-currency.js';

export {
  SqlInputValidationError,
  validateBankCode,
  validateAccountCode,
  validateEntryNumber,
  validateCbtype,
  validatePaymentRef,
  validateReference,
  validateBatchNumber,
} from './sql-input-validators.js';

export {
  getNextJournal,
  getNextId,
  incrementAtypeEntry,
} from './id-allocation.js';

export {
  updateNbankBalance,
  updateNacntBalance,
  getNacntType,
  insertNjmemo,
} from './balance-updates.js';
export type { NacntType, UpdateNacntBalanceOptions } from './balance-updates.js';

export {
  generateOperaUniqueId,
  generateOperaUniqueIds,
} from './unique-id.js';

export { importBankTransfer } from './bank-transfer.js';
export type {
  BankTransferOptions,
  BankTransferResult,
} from './bank-transfer.js';
export type {
  LedgerType,
  PeriodInfo,
  PeriodValidationResult,
  PeriodPostingDecision,
} from './period-validation.js';
