import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { Layout } from './components/Layout';
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
            <Route path="/" element={<Dashboard />} />
            <Route path="/sales-dashboards" element={<SalesDashboards />} />
            <Route path="/debtors-control" element={<CreditControl />} />
            <Route path="/creditors-control" element={<CreditorsControl />} />
            <Route path="/cashflow" element={<Cashflow />} />
            <Route path="/trial-balance" element={<TrialBalance />} />
            <Route path="/statutory-accounts" element={<StatutoryAccounts />} />
            <Route path="/reconcile" element={<Reconcile />} />
            <Route path="/email" element={<Email />} />
            <Route path="/ask" element={<Ask />} />
            <Route path="/settings" element={<Settings />} />
          </Routes>
        </Layout>
      </BrowserRouter>
    </QueryClientProvider>
  );
}

export default App;
