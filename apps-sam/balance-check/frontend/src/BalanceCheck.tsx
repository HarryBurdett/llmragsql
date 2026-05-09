/**
 * Balance Check plugin entry component.
 *
 * Read-only summary dashboard against Opera. Hits the four core
 * reconcile endpoints in parallel (cashbook + debtors + creditors +
 * trial balance) and surfaces the variance — the "are we in balance?"
 * tile that drives all of accounts month-end.
 *
 * Legacy reference pages:
 *   frontend/src/pages/Reconcile.tsx              (parent)
 *   frontend/src/pages/CashbookOptions.tsx        (cashbook drilldown)
 *   frontend/src/pages/CreditorsReconcile.tsx     (creditors drilldown)
 *   frontend/src/pages/VATReconcile.tsx           (VAT)
 *
 * Endpoints used:
 *   - GET /api/reconcile/summary
 *   - GET /api/reconcile/creditors
 *   - GET /api/reconcile/debtors
 *   - GET /api/reconcile/trial-balance
 *   - GET /api/reconcile/vat
 */
import { useEffect, useState } from 'react';
import type { SamPluginContext } from './sam';

interface ReconcileSummaryResponse {
  success: boolean;
  cashbook?: { atran_total: number; nbank_total: number; nominal_total: number; difference: number };
  debtors?: { sales_total: number; nominal_total: number; difference: number };
  creditors?: { purchase_total: number; nominal_total: number; difference: number };
  vat?: { sales: number; purchases: number; net: number; nominal: number; difference: number };
  trial_balance?: { debit_total: number; credit_total: number; difference: number };
  error?: string;
}

export default function BalanceCheck({
  context,
}: {
  context: SamPluginContext;
}) {
  const [summary, setSummary] = useState<ReconcileSummaryResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refresh = async () => {
    setLoading(true);
    setError(null);
    try {
      const result = await context.api.fetch<ReconcileSummaryResponse>(
        '/api/reconcile/summary',
      );
      setSummary(result);
      if (!result.success) setError(result.error ?? 'Summary failed');
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [context.api]);

  return (
    <div className="balance-check-app space-y-4">
      <header className="flex items-baseline justify-between">
        <div>
          <h1 className="text-2xl font-bold">Balance Check</h1>
          <p className="text-sm text-gray-500">
            Internal Opera control reconciliation —{' '}
            {context.currentCompany?.name ?? '—'}
          </p>
        </div>
        <button
          onClick={refresh}
          disabled={loading}
          className="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:bg-gray-300"
        >
          {loading ? 'Refreshing…' : 'Refresh'}
        </button>
      </header>

      {error && (
        <div className="rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700">
          {error}
        </div>
      )}

      {summary && summary.success && (
        <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
          {summary.cashbook && (
            <BalanceTile
              title="Cashbook"
              rows={[
                ['atran total', summary.cashbook.atran_total],
                ['nbank total', summary.cashbook.nbank_total],
                ['Nominal', summary.cashbook.nominal_total],
              ]}
              difference={summary.cashbook.difference}
            />
          )}
          {summary.debtors && (
            <BalanceTile
              title="Debtors"
              rows={[
                ['Sales ledger', summary.debtors.sales_total],
                ['Nominal', summary.debtors.nominal_total],
              ]}
              difference={summary.debtors.difference}
            />
          )}
          {summary.creditors && (
            <BalanceTile
              title="Creditors"
              rows={[
                ['Purchase ledger', summary.creditors.purchase_total],
                ['Nominal', summary.creditors.nominal_total],
              ]}
              difference={summary.creditors.difference}
            />
          )}
          {summary.trial_balance && (
            <BalanceTile
              title="Trial Balance"
              rows={[
                ['Debits', summary.trial_balance.debit_total],
                ['Credits', summary.trial_balance.credit_total],
              ]}
              difference={summary.trial_balance.difference}
            />
          )}
          {summary.vat && (
            <BalanceTile
              title="VAT"
              rows={[
                ['Sales VAT', summary.vat.sales],
                ['Purchase VAT', summary.vat.purchases],
                ['Net', summary.vat.net],
                ['Nominal', summary.vat.nominal],
              ]}
              difference={summary.vat.difference}
            />
          )}
        </div>
      )}
    </div>
  );
}

function BalanceTile({
  title,
  rows,
  difference,
}: {
  title: string;
  rows: Array<[string, number]>;
  difference: number;
}) {
  const inBalance = Math.abs(difference) < 0.01;
  return (
    <div className="rounded-xl border border-gray-200 bg-white p-5 shadow-sm">
      <div className="flex items-baseline justify-between">
        <h3 className="text-sm font-semibold text-gray-700">{title}</h3>
        <span
          className={
            inBalance
              ? 'rounded-full bg-green-100 px-2 py-0.5 text-xs text-green-700'
              : 'rounded-full bg-red-100 px-2 py-0.5 text-xs text-red-700'
          }
        >
          {inBalance ? 'In balance' : `Δ £${difference.toFixed(2)}`}
        </span>
      </div>
      <dl className="mt-3 space-y-1 text-sm">
        {rows.map(([label, value]) => (
          <div key={label} className="flex justify-between">
            <dt className="text-gray-500">{label}</dt>
            <dd className="font-mono">£{value.toFixed(2)}</dd>
          </div>
        ))}
      </dl>
    </div>
  );
}
