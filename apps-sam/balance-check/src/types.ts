/**
 * Type definitions for balance-check responses.
 *
 * Match the exact shape returned by the Python endpoints — these are
 * the contract the frontend depends on.
 */

export interface DetailRow {
  label: string;
  value: number;
}

export interface VarianceRow {
  label: string;
  value: number;
  ok: boolean;
}

export interface ReconcileCheck {
  name: 'Debtors' | 'Creditors' | 'Cashbook' | 'VAT';
  icon: 'users' | 'building' | 'book' | 'receipt';
  reconciled: boolean;
  details?: DetailRow[];
  variances?: VarianceRow[];
  error?: string;
}

export interface ReconcileSummaryResponse {
  success: boolean;
  reconciliation_date: string;
  checks: ReconcileCheck[];
  all_reconciled: boolean;
  total_checks: number;
  passed_checks: number;
  failed_checks: number;
  error?: string;
}
