/**
 * Balance Check plugin entry component.
 *
 * Mounts the four ported reconciliation pages from the legacy
 * frontend (~2,400 LOC across CreditorsReconcile, DebtorsReconcile,
 * TrialBalanceCheck, CashbookReconcile) inside SAM's plugin shell.
 * Tab switcher routes between them.
 *
 * See bank-reconcile/BankReconcile.tsx for the QueryClientProvider
 * pattern.
 */
import { useEffect, useMemo, useState } from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { SamPluginContext } from './sam';
import { setSamContext } from './api-shim';
import CreditorsReconcile from './CreditorsReconcile';
import DebtorsReconcile from './DebtorsReconcile';
import TrialBalanceCheck from './TrialBalanceCheck';
import CashbookReconcile from './CashbookReconcile';

type Tab = 'creditors' | 'debtors' | 'trial-balance' | 'cashbook';

export default function BalanceCheck({
  context,
}: {
  context: SamPluginContext;
}) {
  const [tab, setTab] = useState<Tab>('creditors');

  useEffect(() => {
    setSamContext(context);
  }, [context]);

  const queryClient = useMemo(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: { retry: 1, refetchOnWindowFocus: false },
        },
      }),
    [],
  );

  return (
    <QueryClientProvider client={queryClient}>
      <div className="balance-check-app space-y-4">
        <nav className="flex gap-2 border-b border-gray-200 px-4">
          {(
            ['creditors', 'debtors', 'trial-balance', 'cashbook'] as const
          ).map((t) => (
            <button
              key={t}
              onClick={() => setTab(t)}
              className={
                tab === t
                  ? 'border-b-2 border-blue-600 px-4 py-2 text-sm font-medium text-blue-600'
                  : 'px-4 py-2 text-sm text-gray-500 hover:text-gray-700'
              }
            >
              {t === 'creditors'
                ? 'Creditors'
                : t === 'debtors'
                  ? 'Debtors'
                  : t === 'trial-balance'
                    ? 'Trial Balance'
                    : 'Cashbook'}
            </button>
          ))}
        </nav>

        {tab === 'creditors' && <CreditorsReconcile />}
        {tab === 'debtors' && <DebtorsReconcile />}
        {tab === 'trial-balance' && <TrialBalanceCheck />}
        {tab === 'cashbook' && <CashbookReconcile />}
      </div>
    </QueryClientProvider>
  );
}
