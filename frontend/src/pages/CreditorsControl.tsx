import { useState, useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  Search,
  DollarSign,
  Clock,
  Users,
  RefreshCw,
  Phone,
  Mail,
  FileText,
  Building,
  AlertTriangle,
  Calendar,
  X,
  Printer,
} from 'lucide-react';
import apiClient from '../api/client';
import type {
  CreditorsDashboardResponse,
  CreditorsReportResponse,
  CreditorRecord,
  TopSupplier,
  StatementTransaction,
} from '../api/client';
import { PageHeader, Card, LoadingState, StatusBadge } from '../components/ui';

type ViewMode = 'dashboard' | 'report' | 'supplier';

export function CreditorsControl() {
  const [viewMode, setViewMode] = useState<ViewMode>('dashboard');
  const [searchQuery, setSearchQuery] = useState('');
  const [debouncedSearch, setDebouncedSearch] = useState('');
  const [selectedSupplier, setSelectedSupplier] = useState<string | null>(null);
  const [showTransactions, setShowTransactions] = useState(false);
  const [showStatement, setShowStatement] = useState(false);
  const [includePaid, setIncludePaid] = useState(false);

  // Debounce search query - only search after 300ms of no typing
  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedSearch(searchQuery);
    }, 300);
    return () => clearTimeout(timer);
  }, [searchQuery]);

  // Fetch dashboard data - cached for 30s
  const dashboardQuery = useQuery<CreditorsDashboardResponse>({
    queryKey: ['creditorsDashboard'],
    queryFn: async () => {
      const response = await apiClient.creditorsDashboard();
      return response.data;
    },
    staleTime: 30000,
    refetchInterval: 60000,
    refetchOnWindowFocus: false,
  });

  // Fetch creditors report - cached for 2 minutes
  const reportQuery = useQuery<CreditorsReportResponse>({
    queryKey: ['creditorsReport'],
    queryFn: async () => {
      const response = await apiClient.creditorsReport();
      return response.data;
    },
    enabled: viewMode === 'report',
    staleTime: 2 * 60 * 1000,
    gcTime: 5 * 60 * 1000,
  });

  // Search suppliers - uses debounced query, cached for 30s
  const searchResults = useQuery({
    queryKey: ['searchSuppliers', debouncedSearch],
    queryFn: async () => {
      const response = await apiClient.searchSuppliers(debouncedSearch);
      return response.data;
    },
    enabled: debouncedSearch.length >= 2,
    staleTime: 30000,
    gcTime: 60000,
  });

  // Supplier details - cached for 5 minutes (doesn't change often)
  const supplierQuery = useQuery({
    queryKey: ['supplierDetails', selectedSupplier],
    queryFn: async () => {
      const response = await apiClient.supplierDetails(selectedSupplier!);
      return response.data;
    },
    enabled: !!selectedSupplier,
    staleTime: 5 * 60 * 1000,
    gcTime: 10 * 60 * 1000,
  });

  // Supplier transactions - cached for 2 minutes
  const transactionsQuery = useQuery({
    queryKey: ['supplierTransactions', selectedSupplier, includePaid],
    queryFn: async () => {
      const response = await apiClient.supplierTransactions(selectedSupplier!, includePaid);
      return response.data;
    },
    enabled: !!selectedSupplier && showTransactions,
    staleTime: 2 * 60 * 1000,
    gcTime: 5 * 60 * 1000,
  });

  // Supplier statement - cached for 2 minutes
  const statementQuery = useQuery({
    queryKey: ['supplierStatement', selectedSupplier],
    queryFn: async () => {
      const response = await apiClient.supplierStatement(selectedSupplier!);
      return response.data;
    },
    enabled: !!selectedSupplier && showStatement,
    staleTime: 2 * 60 * 1000,
    gcTime: 5 * 60 * 1000,
  });

  const formatCurrency = (value: number): string => {
    return `£${Math.abs(value).toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  };

  const formatDate = (dateStr: string): string => {
    if (!dateStr) return '-';
    return new Date(dateStr).toLocaleDateString('en-GB');
  };

  const handleSelectSupplier = (account: string) => {
    setSelectedSupplier(account);
    setViewMode('supplier');
    setShowTransactions(true);
    setShowStatement(false);
    setSearchQuery('');
  };

  const handleBackToDashboard = () => {
    setSelectedSupplier(null);
    setViewMode('dashboard');
    setShowTransactions(false);
    setShowStatement(false);
  };

  const dashboard = dashboardQuery.data;
  const report = reportQuery.data;
  const supplier = supplierQuery.data?.supplier;

  // Filter report data based on search
  const filteredReport = report?.data?.filter((r: CreditorRecord) =>
    searchQuery === '' ||
    r.account.toLowerCase().includes(searchQuery.toLowerCase()) ||
    r.supplier.toLowerCase().includes(searchQuery.toLowerCase())
  ) || [];

  return (
    <div className="space-y-6">
      {/* Header */}
      <PageHeader
        icon={Building}
        title="Credit Control"
        subtitle="Manage supplier accounts and outstanding invoices"
      >
        <div className="flex gap-2">
          {viewMode === 'supplier' && (
            <button
              onClick={handleBackToDashboard}
              className="btn btn-secondary text-sm flex items-center"
            >
              <X className="h-4 w-4 mr-2" />
              Back
            </button>
          )}
          {viewMode !== 'supplier' && (
            <div className="flex bg-gray-100 rounded-lg p-1">
              <button
                onClick={() => setViewMode('dashboard')}
                className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                  viewMode === 'dashboard'
                    ? 'bg-white shadow text-blue-600'
                    : 'text-gray-600 hover:text-gray-900'
                }`}
              >
                Dashboard
              </button>
              <button
                onClick={() => setViewMode('report')}
                className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                  viewMode === 'report'
                    ? 'bg-white shadow text-blue-600'
                    : 'text-gray-600 hover:text-gray-900'
                }`}
              >
                Creditors Report
              </button>
            </div>
          )}
          <button
            onClick={() => {
              dashboardQuery.refetch();
              reportQuery.refetch();
            }}
            className="btn btn-secondary text-sm flex items-center"
          >
            <RefreshCw className={`h-4 w-4 mr-2 ${dashboardQuery.isRefetching ? 'animate-spin' : ''}`} />
            Refresh
          </button>
        </div>
      </PageHeader>

      {/* Search Bar */}
      {viewMode !== 'supplier' && (
        <div className="relative">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-5 w-5 text-gray-400" />
          <input
            type="text"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            placeholder="Search suppliers by account code or name..."
            className="input pl-10 w-full"
          />
          {/* Search Results Dropdown */}
          {searchQuery.length >= 2 && searchResults.data?.suppliers && searchResults.data.suppliers.length > 0 && (
            <div className="absolute z-10 w-full mt-1 bg-white border border-gray-200 rounded-xl shadow-lg max-h-60 overflow-y-auto">
              {searchResults.data.suppliers.map((s) => (
                <button
                  key={s.account}
                  onClick={() => handleSelectSupplier(s.account)}
                  className="w-full px-4 py-3 text-left hover:bg-gray-50 border-b border-gray-100 last:border-b-0"
                >
                  <div className="flex justify-between items-center">
                    <div>
                      <span className="font-medium text-gray-900">{s.supplier_name}</span>
                      <span className="text-gray-500 ml-2 text-sm">{s.account}</span>
                    </div>
                    <span className={`font-medium ${s.balance > 0 ? 'text-red-600' : 'text-emerald-600'}`}>
                      {formatCurrency(s.balance)}
                    </span>
                  </div>
                </button>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Dashboard View */}
      {viewMode === 'dashboard' && dashboard && (
        <>
          {/* Metrics Cards */}
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
            {/* Total Creditors */}
            <Card>
              <div className="flex items-center justify-between mb-3">
                <DollarSign className="h-8 w-8 text-blue-500" />
                <StatusBadge variant="info">{dashboard.metrics?.total_creditors?.count || 0} suppliers</StatusBadge>
              </div>
              <p className="text-sm text-gray-500">Total Outstanding</p>
              <p className="text-2xl font-bold text-gray-900">{formatCurrency(dashboard.metrics?.total_creditors?.value || 0)}</p>
            </Card>

            {/* Overdue */}
            <Card>
              <div className="flex items-center justify-between mb-3">
                <AlertTriangle className="h-8 w-8 text-red-500" />
                <StatusBadge variant="danger">{dashboard.metrics?.overdue_invoices?.count || 0} invoices</StatusBadge>
              </div>
              <p className="text-sm text-gray-500">Overdue</p>
              <p className="text-2xl font-bold text-gray-900">{formatCurrency(dashboard.metrics?.overdue_invoices?.value || 0)}</p>
            </Card>

            {/* Due in 7 Days */}
            <Card>
              <div className="flex items-center justify-between mb-3">
                <Clock className="h-8 w-8 text-amber-500" />
                <StatusBadge variant="warning">{dashboard.metrics?.due_7_days?.count || 0} invoices</StatusBadge>
              </div>
              <p className="text-sm text-gray-500">Due in 7 Days</p>
              <p className="text-2xl font-bold text-gray-900">{formatCurrency(dashboard.metrics?.due_7_days?.value || 0)}</p>
            </Card>

            {/* Due in 30 Days */}
            <Card>
              <div className="flex items-center justify-between mb-3">
                <Calendar className="h-8 w-8 text-amber-500" />
                <StatusBadge variant="warning">{dashboard.metrics?.due_30_days?.count || 0} invoices</StatusBadge>
              </div>
              <p className="text-sm text-gray-500">Due in 30 Days</p>
              <p className="text-2xl font-bold text-gray-900">{formatCurrency(dashboard.metrics?.due_30_days?.value || 0)}</p>
            </Card>

            {/* Recent Payments */}
            <Card>
              <div className="flex items-center justify-between mb-3">
                <DollarSign className="h-8 w-8 text-emerald-500" />
                <StatusBadge variant="success">{dashboard.metrics?.recent_payments?.count || 0} payments</StatusBadge>
              </div>
              <p className="text-sm text-gray-500">Payments (7 Days)</p>
              <p className="text-2xl font-bold text-gray-900">{formatCurrency(dashboard.metrics?.recent_payments?.value || 0)}</p>
            </Card>
          </div>

          {/* Top Suppliers */}
          <Card title="Top Suppliers by Balance" icon={Users}>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-200">
                    <th className="text-left py-3 font-medium text-gray-500">Account</th>
                    <th className="text-left py-3 font-medium text-gray-500">Supplier</th>
                    <th className="text-right py-3 font-medium text-gray-500">Balance</th>
                    <th className="text-left py-3 font-medium text-gray-500">Contact</th>
                    <th className="text-center py-3 font-medium text-gray-500">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {dashboard.top_suppliers?.map((s: TopSupplier) => (
                    <tr key={s.account} className="border-b border-gray-100 hover:bg-gray-50">
                      <td className="py-3">
                        <button
                          onClick={() => handleSelectSupplier(s.account)}
                          className="text-blue-600 hover:text-blue-800 font-medium"
                        >
                          {s.account}
                        </button>
                      </td>
                      <td className="py-3">{s.supplier}</td>
                      <td className="py-3 text-right font-medium text-red-600">{formatCurrency(s.balance)}</td>
                      <td className="py-3">
                        {s.contact && <span className="text-gray-600">{s.contact}</span>}
                        {s.phone && (
                          <a href={`tel:${s.phone}`} className="ml-2 text-blue-600 hover:text-blue-800">
                            <Phone className="h-4 w-4 inline" />
                          </a>
                        )}
                      </td>
                      <td className="py-3 text-center">
                        <button
                          onClick={() => handleSelectSupplier(s.account)}
                          className="btn btn-secondary btn-sm"
                        >
                          View
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Card>
        </>
      )}

      {/* Report View */}
      {viewMode === 'report' && (
        <Card title="Aged Creditors Report" icon={FileText}>
          {report && (
            <span className="text-sm text-gray-500">{filteredReport.length} suppliers</span>
          )}

          {reportQuery.isLoading ? (
            <LoadingState message="Loading creditors report..." />
          ) : (
            <>
              {/* Totals Row */}
              {report?.totals && (
                <div className="grid grid-cols-5 gap-4 mb-4 p-4 bg-gray-50 rounded-xl">
                  <div>
                    <p className="text-sm text-gray-500">Total Balance</p>
                    <p className="text-xl font-bold text-gray-900">{formatCurrency(report.totals.balance)}</p>
                  </div>
                  <div>
                    <p className="text-sm text-gray-500">Current</p>
                    <p className="text-lg font-semibold text-emerald-600">{formatCurrency(report.totals.current)}</p>
                  </div>
                  <div>
                    <p className="text-sm text-gray-500">1 Month</p>
                    <p className="text-lg font-semibold text-amber-600">{formatCurrency(report.totals.month_1)}</p>
                  </div>
                  <div>
                    <p className="text-sm text-gray-500">2 Months</p>
                    <p className="text-lg font-semibold text-amber-600">{formatCurrency(report.totals.month_2)}</p>
                  </div>
                  <div>
                    <p className="text-sm text-gray-500">3+ Months</p>
                    <p className="text-lg font-semibold text-red-600">{formatCurrency(report.totals.month_3_plus)}</p>
                  </div>
                </div>
              )}

              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-gray-200">
                      <th className="text-left py-3 font-medium text-gray-500">Account</th>
                      <th className="text-left py-3 font-medium text-gray-500">Supplier</th>
                      <th className="text-right py-3 font-medium text-gray-500">Balance</th>
                      <th className="text-right py-3 font-medium text-gray-500">Current</th>
                      <th className="text-right py-3 font-medium text-gray-500">1 Month</th>
                      <th className="text-right py-3 font-medium text-gray-500">2 Months</th>
                      <th className="text-right py-3 font-medium text-gray-500">3+ Months</th>
                      <th className="text-center py-3 font-medium text-gray-500">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredReport.map((r: CreditorRecord) => (
                      <tr key={r.account} className="border-b border-gray-100 hover:bg-gray-50">
                        <td className="py-3">
                          <button
                            onClick={() => handleSelectSupplier(r.account)}
                            className="text-blue-600 hover:text-blue-800 font-medium"
                          >
                            {r.account}
                          </button>
                        </td>
                        <td className="py-3">{r.supplier}</td>
                        <td className="py-3 text-right font-medium">{formatCurrency(r.balance)}</td>
                        <td className="py-3 text-right text-emerald-600">{formatCurrency(r.current_period)}</td>
                        <td className="py-3 text-right text-amber-600">{formatCurrency(r.month_1)}</td>
                        <td className="py-3 text-right text-amber-600">{formatCurrency(r.month_2)}</td>
                        <td className="py-3 text-right text-red-600">{formatCurrency(r.month_3_plus)}</td>
                        <td className="py-3 text-center">
                          <button
                            onClick={() => handleSelectSupplier(r.account)}
                            className="btn btn-secondary btn-sm"
                          >
                            View
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          )}
        </Card>
      )}

      {/* Supplier Detail View */}
      {viewMode === 'supplier' && selectedSupplier && (
        <div className="space-y-6">
          {/* Supplier Header */}
          {supplier && (
            <Card>
              <div className="flex items-start justify-between">
                <div className="flex items-start gap-4">
                  <div className="p-3 bg-blue-100 rounded-xl">
                    <Building className="h-8 w-8 text-blue-600" />
                  </div>
                  <div>
                    <h3 className="text-xl font-bold text-gray-900">{supplier.supplier_name}</h3>
                    <p className="text-gray-500">Account: {supplier.account}</p>
                    <div className="mt-2 text-sm text-gray-600">
                      {supplier.address1 && <p>{supplier.address1}</p>}
                      {supplier.address2 && <p>{supplier.address2}</p>}
                      {supplier.address3 && <p>{supplier.address3}</p>}
                      {supplier.address4 && <p>{supplier.address4}</p>}
                      {supplier.postcode && <p>{supplier.postcode}</p>}
                    </div>
                    <div className="mt-3 flex gap-4">
                      {supplier.phone && (
                        <a href={`tel:${supplier.phone}`} className="flex items-center gap-1 text-blue-600 hover:text-blue-800">
                          <Phone className="h-4 w-4" />
                          {supplier.phone}
                        </a>
                      )}
                      {supplier.email && (
                        <a href={`mailto:${supplier.email}`} className="flex items-center gap-1 text-blue-600 hover:text-blue-800">
                          <Mail className="h-4 w-4" />
                          {supplier.email}
                        </a>
                      )}
                    </div>
                  </div>
                </div>
                <div className="text-right">
                  <p className="text-sm text-gray-500">Current Balance</p>
                  <p className={`text-3xl font-bold ${supplier.balance > 0 ? 'text-red-600' : 'text-emerald-600'}`}>
                    {formatCurrency(supplier.balance)}
                  </p>
                  <p className="text-sm text-gray-500 mt-2">YTD Turnover</p>
                  <p className="text-lg font-semibold text-gray-700">{formatCurrency(supplier.turnover_ytd || 0)}</p>
                </div>
              </div>
            </Card>
          )}

          {/* View Toggle */}
          <div className="flex gap-2">
            <button
              onClick={() => { setShowTransactions(true); setShowStatement(false); }}
              className={`btn ${showTransactions && !showStatement ? 'btn-primary' : 'btn-secondary'}`}
            >
              <FileText className="h-4 w-4 mr-2" />
              Transactions
            </button>
            <button
              onClick={() => { setShowStatement(true); setShowTransactions(false); }}
              className={`btn ${showStatement ? 'btn-primary' : 'btn-secondary'}`}
            >
              <Printer className="h-4 w-4 mr-2" />
              Statement
            </button>
          </div>

          {/* Transactions View */}
          {showTransactions && !showStatement && (
            <Card>
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-base font-semibold text-gray-900">Outstanding Invoices</h3>
                <label className="flex items-center gap-2 text-sm">
                  <input
                    type="checkbox"
                    checked={includePaid}
                    onChange={(e) => setIncludePaid(e.target.checked)}
                    className="rounded"
                  />
                  Include paid transactions
                </label>
              </div>

              {transactionsQuery.isLoading ? (
                <LoadingState message="Loading transactions..." />
              ) : (
                <>
                  {/* Summary */}
                  {transactionsQuery.data?.summary && (
                    <div className="grid grid-cols-4 gap-4 mb-4 p-4 bg-gray-50 rounded-xl">
                      <div>
                        <p className="text-sm text-gray-500">Total Invoices</p>
                        <p className="text-lg font-semibold">{formatCurrency(transactionsQuery.data.summary.total_invoices)}</p>
                      </div>
                      <div>
                        <p className="text-sm text-gray-500">Total Credits</p>
                        <p className="text-lg font-semibold text-emerald-600">{formatCurrency(transactionsQuery.data.summary.total_credits)}</p>
                      </div>
                      <div>
                        <p className="text-sm text-gray-500">Total Payments</p>
                        <p className="text-lg font-semibold text-blue-600">{formatCurrency(transactionsQuery.data.summary.total_payments)}</p>
                      </div>
                      <div>
                        <p className="text-sm text-gray-500">Balance</p>
                        <p className="text-lg font-semibold text-red-600">{formatCurrency(transactionsQuery.data.summary.balance)}</p>
                      </div>
                    </div>
                  )}

                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b border-gray-200">
                          <th className="text-left py-3 font-medium text-gray-500">Date</th>
                          <th className="text-left py-3 font-medium text-gray-500">Reference</th>
                          <th className="text-left py-3 font-medium text-gray-500">Type</th>
                          <th className="text-left py-3 font-medium text-gray-500">Description</th>
                          <th className="text-right py-3 font-medium text-gray-500">Value</th>
                          <th className="text-right py-3 font-medium text-gray-500">Balance</th>
                          <th className="text-left py-3 font-medium text-gray-500">Due Date</th>
                          <th className="text-center py-3 font-medium text-gray-500">Status</th>
                        </tr>
                      </thead>
                      <tbody>
                        {transactionsQuery.data?.transactions?.map((t, idx) => (
                          <tr key={`${t.reference}-${idx}`} className="border-b border-gray-100 hover:bg-gray-50">
                            <td className="py-3">{formatDate(t.date)}</td>
                            <td className="py-3 font-medium">{t.reference}</td>
                            <td className="py-3">
                              <StatusBadge variant={
                                t.type === 'Invoice' ? 'info' :
                                t.type === 'Credit Note' ? 'success' :
                                t.type === 'Payment' ? 'info' :
                                'neutral'
                              }>
                                {t.type}
                              </StatusBadge>
                            </td>
                            <td className="py-3">{t.description}</td>
                            <td className="py-3 text-right font-medium">{formatCurrency(t.value)}</td>
                            <td className="py-3 text-right font-medium">{formatCurrency(t.balance)}</td>
                            <td className="py-3">{formatDate(t.due_date)}</td>
                            <td className="py-3 text-center">
                              {t.days_overdue > 0 ? (
                                <StatusBadge variant="danger">
                                  {t.days_overdue} days overdue
                                </StatusBadge>
                              ) : t.balance > 0 ? (
                                <StatusBadge variant="warning">Outstanding</StatusBadge>
                              ) : (
                                <StatusBadge variant="success">Paid</StatusBadge>
                              )}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </>
              )}
            </Card>
          )}

          {/* Statement View */}
          {showStatement && (
            <Card>
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-base font-semibold text-gray-900">Outstanding Items Statement</h3>
                <button
                  onClick={() => window.print()}
                  className="btn btn-secondary btn-sm flex items-center gap-2"
                >
                  <Printer className="h-4 w-4" />
                  Print
                </button>
              </div>

              {statementQuery.isLoading ? (
                <LoadingState message="Loading statement..." />
              ) : statementQuery.data && (
                <>
                  {/* Statement Header */}
                  <div className="mb-6 p-4 bg-gray-50 rounded-xl">
                    <div className="flex justify-between">
                      <div>
                        <h4 className="font-bold text-lg">{statementQuery.data.supplier?.supplier_name}</h4>
                        <p className="text-gray-600">{statementQuery.data.supplier?.address1}</p>
                        {statementQuery.data.supplier?.address2 && <p className="text-gray-600">{statementQuery.data.supplier.address2}</p>}
                        {statementQuery.data.supplier?.postcode && <p className="text-gray-600">{statementQuery.data.supplier.postcode}</p>}
                      </div>
                      <div className="text-right">
                        <p className="text-sm text-gray-500">Statement Period</p>
                        <p className="font-medium">
                          {formatDate(statementQuery.data.period?.from_date || '')} - {formatDate(statementQuery.data.period?.to_date || '')}
                        </p>
                      </div>
                    </div>
                  </div>

                  {/* Opening Balance */}
                  <div className="flex justify-between py-2 border-b border-gray-200 font-medium">
                    <span>Opening Balance</span>
                    <span>{formatCurrency(statementQuery.data.opening_balance)}</span>
                  </div>

                  {/* Transactions */}
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b border-gray-200">
                          <th className="text-left py-3 font-medium text-gray-500">Date</th>
                          <th className="text-left py-3 font-medium text-gray-500">Reference</th>
                          <th className="text-left py-3 font-medium text-gray-500">Description</th>
                          <th className="text-left py-3 font-medium text-gray-500">Due Date</th>
                          <th className="text-right py-3 font-medium text-gray-500">Original</th>
                          <th className="text-right py-3 font-medium text-gray-500">Outstanding</th>
                        </tr>
                      </thead>
                      <tbody>
                        {statementQuery.data.transactions?.map((t: StatementTransaction, idx: number) => (
                          <tr key={`${t.reference}-${idx}`} className="border-b border-gray-100">
                            <td className="py-3">{formatDate(t.date)}</td>
                            <td className="py-3">{t.reference}</td>
                            <td className="py-3">{t.description}</td>
                            <td className="py-3">{t.due_date ? formatDate(t.due_date) : '-'}</td>
                            <td className="py-3 text-right">{formatCurrency(t.debit || t.credit || 0)}</td>
                            <td className="py-3 text-right font-medium">{formatCurrency(t.balance)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>

                  {/* Total Outstanding */}
                  <div className="mt-4">
                    <div className="flex justify-between py-3 border-t-2 border-gray-300 font-bold text-lg">
                      <span>Total Outstanding</span>
                      <span className={statementQuery.data.closing_balance > 0 ? 'text-red-600' : 'text-emerald-600'}>
                        {formatCurrency(statementQuery.data.closing_balance)}
                      </span>
                    </div>
                  </div>
                </>
              )}
            </Card>
          )}
        </div>
      )}
    </div>
  );
}

export default CreditorsControl;
