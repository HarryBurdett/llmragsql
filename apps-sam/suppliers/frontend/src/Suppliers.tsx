/**
 * Suppliers plugin entry component.
 *
 * Mounts the four ported supplier pages from the legacy frontend
 * (~3,000 LOC across SupplierAccount, SupplierDashboard,
 * SupplierReconciliations, SupplierStatementQueue) inside SAM's
 * plugin shell. Tab switcher routes between them — the legacy
 * `frontend/src/pages/SupplierAccount.tsx` was the deepest page
 * (1,342 LOC, customer-facing reconciliation UI).
 *
 * See bank-reconcile/BankReconcile.tsx for the QueryClientProvider
 * pattern.
 */
import { useEffect, useMemo, useState } from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { SamPluginContext } from './sam';
import { setSamContext } from './api-shim';
import SupplierAccount from './SupplierAccount';
import SupplierDashboard from './SupplierDashboard';
import SupplierReconciliations from './SupplierReconciliations';
import SupplierStatementQueue from './SupplierStatementQueue';

type Tab = 'dashboard' | 'queue' | 'reconciliations' | 'account';

export default function Suppliers({
  context,
}: {
  context: SamPluginContext;
}) {
  const [tab, setTab] = useState<Tab>('dashboard');

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
      <div className="suppliers-app space-y-4">
        <nav className="flex gap-2 border-b border-gray-200 px-4">
          {(['dashboard', 'queue', 'reconciliations', 'account'] as const).map(
            (t) => (
              <button
                key={t}
                onClick={() => setTab(t)}
                className={
                  tab === t
                    ? 'border-b-2 border-blue-600 px-4 py-2 text-sm font-medium text-blue-600'
                    : 'px-4 py-2 text-sm text-gray-500 hover:text-gray-700'
                }
              >
                {t === 'dashboard'
                  ? 'Dashboard'
                  : t === 'queue'
                    ? 'Queue'
                    : t === 'reconciliations'
                      ? 'Reconciliations'
                      : 'Account'}
              </button>
            ),
          )}
        </nav>

        {tab === 'dashboard' && <SupplierDashboard />}
        {tab === 'queue' && <SupplierStatementQueue />}
        {tab === 'reconciliations' && <SupplierReconciliations />}
        {tab === 'account' && <SupplierAccount />}
      </div>
    </QueryClientProvider>
  );
}
