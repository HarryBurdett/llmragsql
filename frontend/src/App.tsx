import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { Layout } from './components/Layout';
import { Archive } from './pages/Archive';
import { Dashboard } from './pages/Dashboard';
import { CreditControl } from './pages/CreditControl';
import { Cashflow } from './pages/Cashflow';
import { TrialBalance } from './pages/TrialBalance';
import { StatutoryAccounts } from './pages/StatutoryAccounts';
import { SalesDashboards } from './pages/SalesDashboards';
import { CreditorsControl } from './pages/CreditorsControl';
import { Email } from './pages/Email';
import { Ask } from './pages/Ask';
import { Settings } from './pages/Settings';
import { Reconcile } from './pages/Reconcile';
import { Imports } from './pages/Imports';
import { LockMonitor } from './pages/LockMonitor';
import { GoCardlessImport } from './pages/GoCardlessImport';
import { BankStatementReconcile } from './pages/BankStatementReconcile';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      staleTime: 30000,
    },
  },
});

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <Layout>
          <Routes>
            {/* Main pages */}
            <Route path="/" element={<Archive />} />
            <Route path="/archive" element={<Archive />} />
            <Route path="/settings" element={<Settings />} />
            <Route path="/lock-monitor" element={<LockMonitor />} />

            {/* Cashbook routes */}
            <Route path="/cashbook/bank-rec" element={<Imports bankRecOnly />} />
            <Route path="/cashbook/gocardless" element={<GoCardlessImport />} />
            <Route path="/cashbook/statement-reconcile" element={<BankStatementReconcile />} />
            {/* Redirect old bank-rec URL */}
            <Route path="/bank-rec" element={<Navigate to="/cashbook/bank-rec" replace />} />

            {/* Archive routes - existing features */}
            <Route path="/archive/dashboard" element={<Dashboard />} />
            <Route path="/archive/sales-dashboards" element={<SalesDashboards />} />
            <Route path="/archive/debtors-control" element={<CreditControl />} />
            <Route path="/archive/creditors-control" element={<CreditorsControl />} />
            <Route path="/archive/cashflow" element={<Cashflow />} />
            <Route path="/archive/trial-balance" element={<TrialBalance />} />
            <Route path="/archive/statutory-accounts" element={<StatutoryAccounts />} />
            <Route path="/archive/reconcile" element={<Reconcile />} />
            <Route path="/archive/imports" element={<Imports />} />
            <Route path="/archive/email" element={<Email />} />
            <Route path="/archive/ask" element={<Ask />} />

            {/* Redirects for old URLs */}
            <Route path="/sales-dashboards" element={<Navigate to="/archive/sales-dashboards" replace />} />
            <Route path="/debtors-control" element={<Navigate to="/archive/debtors-control" replace />} />
            <Route path="/creditors-control" element={<Navigate to="/archive/creditors-control" replace />} />
            <Route path="/cashflow" element={<Navigate to="/archive/cashflow" replace />} />
            <Route path="/trial-balance" element={<Navigate to="/archive/trial-balance" replace />} />
            <Route path="/statutory-accounts" element={<Navigate to="/archive/statutory-accounts" replace />} />
            <Route path="/reconcile" element={<Navigate to="/archive/reconcile" replace />} />
            <Route path="/imports" element={<Navigate to="/archive/imports" replace />} />
            <Route path="/email" element={<Navigate to="/archive/email" replace />} />
            <Route path="/ask" element={<Navigate to="/archive/ask" replace />} />
          </Routes>
        </Layout>
      </BrowserRouter>
    </QueryClientProvider>
  );
}

export default App;
