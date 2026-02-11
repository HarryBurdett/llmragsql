import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { Layout } from './components/Layout';
import { CompanyRequiredModal } from './components/CompanyRequiredModal';
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
import { DebtorsReconcile } from './pages/DebtorsReconcile';
import { CreditorsReconcile } from './pages/CreditorsReconcile';
import { CashbookReconcile } from './pages/CashbookReconcile';
import { VATReconcile } from './pages/VATReconcile';
import { ReconcileSummary } from './pages/ReconcileSummary';
import { TrialBalanceCheck } from './pages/TrialBalanceCheck';
import { SupplierDashboard } from './pages/SupplierDashboard';
import { SupplierStatementQueue } from './pages/SupplierStatementQueue';
import { SupplierReconciliations } from './pages/SupplierReconciliations';
import { SupplierStatementHistory } from './pages/SupplierStatementHistory';
import { SupplierQueries } from './pages/SupplierQueries';
import { SupplierCommunications } from './pages/SupplierCommunications';
import { SupplierDirectory } from './pages/SupplierDirectory';
import { SupplierSecurity } from './pages/SupplierSecurity';
import { SupplierSettings } from './pages/SupplierSettings';
import { SupplierAccount } from './pages/SupplierAccount';
import { UserActivity } from './pages/UserActivity';
import { PensionExport } from './pages/PensionExport';
import { PayrollSettings } from './pages/PayrollSettings';
import { Projects } from './pages/Projects';
import { Stock } from './pages/Stock';
import { SalesOrders } from './pages/SalesOrders';
import { PurchaseOrders } from './pages/PurchaseOrders';
import { BillOfMaterials } from './pages/BillOfMaterials';

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
        <CompanyRequiredModal>
          <Layout>
          <Routes>
            {/* Main pages */}
            <Route path="/" element={<Archive />} />
            <Route path="/archive" element={<Archive />} />
            <Route path="/settings" element={<Settings />} />

            {/* System routes */}
            <Route path="/system/projects" element={<Projects />} />
            <Route path="/system/projects/:projectId" element={<Projects />} />
            <Route path="/system/lock-monitor" element={<LockMonitor />} />
            {/* Redirect old lock-monitor URL */}
            <Route path="/lock-monitor" element={<Navigate to="/system/lock-monitor" replace />} />

            {/* Stock routes */}
            <Route path="/stock" element={<Stock />} />
            <Route path="/stock/products" element={<Stock />} />

            {/* SOP routes */}
            <Route path="/sop" element={<SalesOrders />} />
            <Route path="/sop/documents" element={<SalesOrders />} />

            {/* POP routes */}
            <Route path="/pop" element={<PurchaseOrders />} />
            <Route path="/pop/orders" element={<PurchaseOrders />} />

            {/* BOM routes */}
            <Route path="/bom" element={<BillOfMaterials />} />
            <Route path="/bom/assemblies" element={<BillOfMaterials />} />
            <Route path="/bom/works-orders" element={<BillOfMaterials />} />

            {/* Cashbook routes */}
            <Route path="/cashbook/bank-rec" element={<Imports bankRecOnly />} />
            <Route path="/cashbook/gocardless" element={<GoCardlessImport />} />
            <Route path="/cashbook/statement-reconcile" element={<BankStatementReconcile />} />
            {/* Redirect old bank-rec URL */}
            <Route path="/bank-rec" element={<Navigate to="/cashbook/bank-rec" replace />} />

            {/* Reconcile routes - control account balance checks */}
            <Route path="/reconcile/summary" element={<ReconcileSummary />} />
            <Route path="/reconcile/trial-balance" element={<TrialBalanceCheck />} />
            <Route path="/reconcile/debtors" element={<DebtorsReconcile />} />
            <Route path="/reconcile/creditors" element={<CreditorsReconcile />} />
            <Route path="/reconcile/cashbook" element={<CashbookReconcile />} />
            <Route path="/reconcile/vat" element={<VATReconcile />} />
            {/* Redirect old banks URL */}
            <Route path="/reconcile/banks" element={<Navigate to="/reconcile/cashbook" replace />} />

            {/* Utilities routes */}
            <Route path="/utilities/user-activity" element={<UserActivity />} />

            {/* Payroll routes */}
            <Route path="/payroll/pension-export" element={<PensionExport />} />
            <Route path="/payroll/settings" element={<PayrollSettings />} />

            {/* AP Automation routes */}
            <Route path="/supplier/dashboard" element={<SupplierDashboard />} />
            <Route path="/supplier/statements/queue" element={<SupplierStatementQueue />} />
            <Route path="/supplier/statements/reconciliations" element={<SupplierReconciliations />} />
            <Route path="/supplier/statements/history" element={<SupplierStatementHistory />} />
            <Route path="/supplier/queries/open" element={<SupplierQueries />} />
            <Route path="/supplier/queries/overdue" element={<SupplierQueries />} />
            <Route path="/supplier/queries/resolved" element={<SupplierQueries />} />
            <Route path="/supplier/communications" element={<SupplierCommunications />} />
            <Route path="/supplier/directory" element={<SupplierDirectory />} />
            <Route path="/supplier/security/alerts" element={<SupplierSecurity />} />
            <Route path="/supplier/security/audit" element={<SupplierSecurity />} />
            <Route path="/supplier/security/senders" element={<SupplierSecurity />} />
            <Route path="/supplier/settings" element={<SupplierSettings />} />
            <Route path="/supplier/account" element={<SupplierAccount />} />

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
        </CompanyRequiredModal>
      </BrowserRouter>
    </QueryClientProvider>
  );
}

export default App;
