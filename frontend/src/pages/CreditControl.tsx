import { useState } from 'react';
import { useQuery, useMutation } from '@tanstack/react-query';
import {
  Search, AlertTriangle, DollarSign, Clock, Users, RefreshCw,
  ChevronDown, ChevronUp, Phone, Mail, StopCircle, TrendingUp,
  CheckCircle, FileText, MessageSquare, CreditCard, Calendar,
  AlertCircle, Wallet, FileQuestion, History, UserCheck
} from 'lucide-react';
import apiClient from '../api/client';
import type { CreditControlQueryResponse, PriorityAction, DebtorsReportResponse } from '../api/client';
import { PageHeader, Card, LoadingState, Alert, EmptyState, StatusBadge } from '../components/ui';

// Query categories for the debtors control agent
const QUERY_CATEGORIES = {
  pre_action: {
    label: 'Pre-Action Checks',
    color: 'blue',
    queries: [
      { label: 'Unallocated Cash', question: 'Show unallocated cash on accounts', icon: Wallet },
      { label: 'Pending Credits', question: 'Show pending credit notes', icon: CreditCard },
      { label: 'Disputed Invoices', question: 'Show disputed invoices', icon: FileQuestion },
      { label: 'Account Status', question: 'Show account status flags', icon: AlertCircle },
    ]
  },
  ledger: {
    label: 'Ledger State',
    color: 'purple',
    queries: [
      { label: 'Overdue by Age', question: 'Show overdue invoices by age bracket', icon: Clock },
      { label: 'Balance Aging', question: 'Show customer balance aging summary', icon: History },
      { label: 'Overdue Invoices', question: 'Show me overdue invoices', icon: AlertTriangle },
    ]
  },
  customer: {
    label: 'Customer Context',
    color: 'teal',
    queries: [
      { label: 'Payment History', question: 'Show payment history and patterns', icon: History },
      { label: 'Customer Notes', question: 'Show customer notes and contact log', icon: FileText },
      { label: 'Customer Segments', question: 'Show customer segments and priorities', icon: UserCheck },
    ]
  },
  promise: {
    label: 'Promise Tracking',
    color: 'orange',
    queries: [
      { label: 'Promises Due', question: 'Show promises due today or overdue', icon: Calendar },
      { label: 'Broken Promises', question: 'Show customers with broken promises', icon: AlertCircle },
    ]
  },
  monitoring: {
    label: 'Monitoring',
    color: 'red',
    queries: [
      { label: 'Over Credit Limit', question: 'Which customers are over their credit limit?', icon: AlertTriangle },
      { label: 'Top Debtors', question: 'Who owes us the most money?', icon: DollarSign },
      { label: 'Accounts On Stop', question: 'Which accounts are on stop?', icon: StopCircle },
      { label: 'Old Unallocated', question: 'Show unallocated cash over 7 days', icon: Wallet },
      { label: 'Recent Payments', question: 'Who paid recently?', icon: TrendingUp },
      { label: 'Aged Debt', question: 'Show aged debt summary', icon: Clock },
    ]
  }
};

// Simple quick queries for the main view
const QUICK_QUERIES = [
  { label: 'Over Credit Limit', question: 'Which customers are over their credit limit?', icon: AlertTriangle, color: 'red' },
  { label: 'Top Debtors', question: 'Who owes us the most money?', icon: DollarSign, color: 'amber' },
  { label: 'Accounts On Stop', question: 'Which accounts are on stop?', icon: Users, color: 'amber' },
  { label: 'Overdue Invoices', question: 'Show me overdue invoices', icon: Clock, color: 'blue' },
  { label: 'Recent Payments', question: 'Who paid recently?', icon: RefreshCw, color: 'emerald' },
  { label: 'Aged Debt', question: 'Show aged debt summary', icon: Clock, color: 'blue' },
];

type ViewMode = 'dashboard' | 'query' | 'agent' | 'debtors';

interface ActionTask {
  id: string;
  type: 'call' | 'email' | 'stop' | 'note' | 'review';
  label: string;
  icon: typeof Phone;
  color: string;
}

const ACTION_TASKS: ActionTask[] = [
  { id: 'call', type: 'call', label: 'Log Call', icon: Phone, color: 'blue' },
  { id: 'email', type: 'email', label: 'Send Reminder', icon: Mail, color: 'green' },
  { id: 'stop', type: 'stop', label: 'Put On Stop', icon: StopCircle, color: 'red' },
  { id: 'note', type: 'note', label: 'Add Note', icon: FileText, color: 'purple' },
  { id: 'review', type: 'review', label: 'Mark Reviewed', icon: CheckCircle, color: 'teal' },
];

export function CreditControl() {
  const [viewMode, setViewMode] = useState<ViewMode>('dashboard');
  const [question, setQuestion] = useState('');
  const [result, setResult] = useState<CreditControlQueryResponse | null>(null);
  const [showSQL, setShowSQL] = useState(false);
  const [selectedAccount, setSelectedAccount] = useState<PriorityAction | null>(null);
  const [actionLog, setActionLog] = useState<{account: string; action: string; time: Date}[]>([]);

  // Fetch dashboard data
  const dashboardQuery = useQuery({
    queryKey: ['creditControlDashboard'],
    queryFn: () => apiClient.creditControlDashboard(),
    refetchInterval: 60000, // Refresh every minute
  });

  // Fetch debtors report data
  const debtorsQuery = useQuery({
    queryKey: ['debtorsReport'],
    queryFn: () => apiClient.debtorsReport(),
    enabled: viewMode === 'debtors',
  });

  const queryMutation = useMutation({
    mutationFn: (q: string) => apiClient.creditControlQuery(q),
    onSuccess: (response) => {
      setResult(response.data);
      setViewMode('query');
    },
  });

  const loadDataMutation = useMutation({
    mutationFn: () => apiClient.loadCreditControlData(),
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    queryMutation.mutate(question);
  };

  const handleQuickQuery = (q: string) => {
    setQuestion(q);
    queryMutation.mutate(q);
  };

  const handleAction = (action: ActionTask, account: PriorityAction) => {
    // Log the action (in a real app, this would call an API)
    setActionLog(prev => [...prev, {
      account: account.account,
      action: action.label,
      time: new Date()
    }]);

    // Show feedback
    alert(`Action "${action.label}" logged for ${account.customer.trim()} (${account.account})`);
  };

  const formatCurrency = (value: number): string => {
    return `£${value.toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  };

  const formatValue = (value: unknown): string => {
    if (value === null || value === undefined) return '-';
    if (typeof value === 'number') {
      if (Math.abs(value) >= 1 && value % 1 !== 0) {
        return formatCurrency(value);
      }
      return value.toLocaleString('en-GB');
    }
    if (typeof value === 'string') {
      if (value.match(/^\d{4}-\d{2}-\d{2}/)) {
        return new Date(value).toLocaleDateString('en-GB');
      }
      return value.trim();
    }
    if (typeof value === 'boolean') return value ? 'Yes' : 'No';
    return String(value);
  };

  const getColumnHeader = (key: string): string => {
    return key
      .replace(/_/g, ' ')
      .replace(/\b\w/g, (l) => l.toUpperCase());
  };

  const getPriorityBadge = (reason: string) => {
    switch (reason) {
      case 'ON_STOP':
        return <StatusBadge variant="danger">On Stop</StatusBadge>;
      case 'OVER_LIMIT':
        return <StatusBadge variant="warning">Over Limit</StatusBadge>;
      default:
        return <StatusBadge variant="warning">High Balance</StatusBadge>;
    }
  };

  const dashboard = dashboardQuery.data?.data;

  return (
    <div className="space-y-6">
      {/* Header */}
      <PageHeader
        icon={Users}
        title="Debtors Control"
        subtitle="Query live customer data with natural language"
      >
        <div className="flex gap-2">
          {/* View Mode Toggle */}
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
              onClick={() => setViewMode('debtors')}
              className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                viewMode === 'debtors'
                  ? 'bg-white shadow text-blue-600'
                  : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              Debtors Report
            </button>
            <button
              onClick={() => setViewMode('agent')}
              className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                viewMode === 'agent'
                  ? 'bg-white shadow text-blue-600'
                  : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              Agent Queries
            </button>
            <button
              onClick={() => setViewMode('query')}
              className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                viewMode === 'query'
                  ? 'bg-white shadow text-blue-600'
                  : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              Search
            </button>
          </div>
          <button
            onClick={() => {
              loadDataMutation.mutate();
              dashboardQuery.refetch();
            }}
            disabled={loadDataMutation.isPending}
            className="btn btn-secondary text-sm flex items-center"
          >
            <RefreshCw className={`h-4 w-4 mr-2 ${loadDataMutation.isPending ? 'animate-spin' : ''}`} />
            {loadDataMutation.isPending ? 'Loading...' : 'Refresh'}
          </button>
        </div>
      </PageHeader>

      {/* Dashboard View */}
      {viewMode === 'dashboard' && (
        <>
          {/* Metrics Cards */}
          {dashboard?.metrics && (
            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
              {/* Total Debt */}
              <Card>
                <div className="flex items-center justify-between mb-3">
                  <DollarSign className="h-8 w-8 text-blue-500" />
                  <StatusBadge variant="info">{dashboard.metrics.total_debt?.count || 0} accounts</StatusBadge>
                </div>
                <p className="text-sm text-gray-500">Total Outstanding</p>
                <p className="text-2xl font-bold text-gray-900">{formatCurrency(dashboard.metrics.total_debt?.value || 0)}</p>
              </Card>

              {/* Over Credit Limit */}
              <Card
                className="cursor-pointer hover:shadow-md transition-shadow"
              >
                <div
                  onClick={() => handleQuickQuery('Which customers are over their credit limit?')}
                >
                  <div className="flex items-center justify-between mb-3">
                    <AlertTriangle className="h-8 w-8 text-red-500" />
                    <StatusBadge variant="danger">{dashboard.metrics.over_credit_limit?.count || 0} accounts</StatusBadge>
                  </div>
                  <p className="text-sm text-gray-500">Over Credit Limit</p>
                  <p className="text-2xl font-bold text-gray-900">{formatCurrency(dashboard.metrics.over_credit_limit?.value || 0)}</p>
                </div>
              </Card>

              {/* Accounts On Stop */}
              <Card
                className="cursor-pointer hover:shadow-md transition-shadow"
              >
                <div
                  onClick={() => handleQuickQuery('Which accounts are on stop?')}
                >
                  <div className="flex items-center justify-between mb-3">
                    <StopCircle className="h-8 w-8 text-amber-500" />
                    <StatusBadge variant="warning">{dashboard.metrics.accounts_on_stop?.count || 0} accounts</StatusBadge>
                  </div>
                  <p className="text-sm text-gray-500">Accounts On Stop</p>
                  <p className="text-2xl font-bold text-gray-900">{formatCurrency(dashboard.metrics.accounts_on_stop?.value || 0)}</p>
                </div>
              </Card>

              {/* Overdue Invoices */}
              <Card
                className="cursor-pointer hover:shadow-md transition-shadow"
              >
                <div
                  onClick={() => handleQuickQuery('Show me overdue invoices')}
                >
                  <div className="flex items-center justify-between mb-3">
                    <Clock className="h-8 w-8 text-blue-500" />
                    <StatusBadge variant="info">{dashboard.metrics.overdue_invoices?.count || 0} invoices</StatusBadge>
                  </div>
                  <p className="text-sm text-gray-500">Overdue Invoices</p>
                  <p className="text-2xl font-bold text-gray-900">{formatCurrency(dashboard.metrics.overdue_invoices?.value || 0)}</p>
                </div>
              </Card>

              {/* Recent Payments */}
              <Card
                className="cursor-pointer hover:shadow-md transition-shadow"
              >
                <div
                  onClick={() => handleQuickQuery('Who paid recently?')}
                >
                  <div className="flex items-center justify-between mb-3">
                    <TrendingUp className="h-8 w-8 text-emerald-500" />
                    <StatusBadge variant="success">{dashboard.metrics.recent_payments?.count || 0} payments</StatusBadge>
                  </div>
                  <p className="text-sm text-gray-500">Payments (7 days)</p>
                  <p className="text-2xl font-bold text-gray-900">{formatCurrency(dashboard.metrics.recent_payments?.value || 0)}</p>
                </div>
              </Card>
            </div>
          )}

          {/* Secondary Metrics Row */}
          {dashboard?.metrics && (
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              {/* Promises Due */}
              {dashboard.metrics.promises_due && (
                <Card
                  className="border-2 border-amber-200 cursor-pointer hover:border-amber-400 transition-colors"
                >
                  <div
                    onClick={() => handleQuickQuery('Show promises due today or overdue')}
                    className="flex items-center gap-3"
                  >
                    <Calendar className="h-8 w-8 text-amber-500" />
                    <div>
                      <p className="text-xs text-gray-500">Promises Due</p>
                      <p className="text-lg font-bold text-gray-900">{formatCurrency(dashboard.metrics.promises_due.value)}</p>
                      <p className="text-xs text-amber-600">{dashboard.metrics.promises_due.count} promises</p>
                    </div>
                  </div>
                </Card>
              )}

              {/* Disputed */}
              {dashboard.metrics.disputed && (
                <Card
                  className="border-2 border-red-200 cursor-pointer hover:border-red-400 transition-colors"
                >
                  <div
                    onClick={() => handleQuickQuery('Show disputed invoices')}
                    className="flex items-center gap-3"
                  >
                    <FileQuestion className="h-8 w-8 text-red-500" />
                    <div>
                      <p className="text-xs text-gray-500">In Dispute</p>
                      <p className="text-lg font-bold text-gray-900">{formatCurrency(dashboard.metrics.disputed.value)}</p>
                      <p className="text-xs text-red-600">{dashboard.metrics.disputed.count} invoices</p>
                    </div>
                  </div>
                </Card>
              )}

              {/* Unallocated Cash */}
              {dashboard.metrics.unallocated_cash && (
                <Card
                  className="border-2 border-blue-200 cursor-pointer hover:border-blue-400 transition-colors"
                >
                  <div
                    onClick={() => handleQuickQuery('Show unallocated cash on accounts')}
                    className="flex items-center gap-3"
                  >
                    <Wallet className="h-8 w-8 text-blue-500" />
                    <div>
                      <p className="text-xs text-gray-500">Unallocated Cash</p>
                      <p className="text-lg font-bold text-gray-900">{formatCurrency(dashboard.metrics.unallocated_cash.value)}</p>
                      <p className="text-xs text-blue-600">{dashboard.metrics.unallocated_cash.count} receipts</p>
                    </div>
                  </div>
                </Card>
              )}
            </div>
          )}

          {/* Priority Actions Section */}
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            {/* Priority Accounts List */}
            <div className="lg:col-span-2">
              <Card title="Priority Actions" icon={AlertTriangle}>
                {dashboardQuery.isLoading ? (
                  <LoadingState message="Loading priority actions..." />
                ) : dashboard?.priority_actions && dashboard.priority_actions.length > 0 ? (
                  <div className="space-y-3">
                    {dashboard.priority_actions.map((account, idx) => (
                      <div
                        key={idx}
                        className={`p-4 rounded-xl border-2 transition-all cursor-pointer ${
                          selectedAccount?.account === account.account
                            ? 'border-blue-500 bg-blue-50'
                            : 'border-gray-200 hover:border-gray-300 bg-white'
                        }`}
                        onClick={() => setSelectedAccount(selectedAccount?.account === account.account ? null : account)}
                      >
                        <div className="flex justify-between items-start">
                          <div>
                            <div className="flex items-center gap-2">
                              <span className="font-medium text-gray-900">{account.customer?.trim()}</span>
                              {getPriorityBadge(account.priority_reason)}
                            </div>
                            <p className="text-sm text-gray-500 mt-1">
                              Account: {account.account} | Contact: {account.contact?.trim() || 'N/A'}
                            </p>
                          </div>
                          <div className="text-right">
                            <p className="text-lg font-bold text-gray-900">{formatCurrency(account.balance)}</p>
                            <p className="text-xs text-gray-500">Limit: {formatCurrency(account.credit_limit || 0)}</p>
                          </div>
                        </div>

                        {/* Action buttons when selected */}
                        {selectedAccount?.account === account.account && (
                          <div className="mt-4 pt-4 border-t border-gray-200">
                            <p className="text-xs text-gray-500 mb-2">Quick Actions:</p>
                            <div className="flex flex-wrap gap-2">
                              {ACTION_TASKS.map((action) => {
                                const Icon = action.icon;
                                return (
                                  <button
                                    key={action.id}
                                    onClick={(e) => {
                                      e.stopPropagation();
                                      handleAction(action, account);
                                    }}
                                    className={`flex items-center gap-1 px-3 py-1.5 rounded-md text-xs font-medium transition-colors
                                      bg-${action.color}-100 text-${action.color}-700 hover:bg-${action.color}-200`}
                                    style={{
                                      backgroundColor: action.color === 'blue' ? '#dbeafe' :
                                                      action.color === 'green' ? '#dcfce7' :
                                                      action.color === 'red' ? '#fee2e2' :
                                                      action.color === 'purple' ? '#f3e8ff' : '#ccfbf1',
                                      color: action.color === 'blue' ? '#1d4ed8' :
                                             action.color === 'green' ? '#15803d' :
                                             action.color === 'red' ? '#b91c1c' :
                                             action.color === 'purple' ? '#7e22ce' : '#0f766e'
                                    }}
                                  >
                                    <Icon className="h-3 w-3" />
                                    {action.label}
                                  </button>
                                );
                              })}
                            </div>
                            {account.phone && (
                              <p className="mt-2 text-sm text-gray-600">
                                <Phone className="h-3 w-3 inline mr-1" />
                                {account.phone.trim()}
                              </p>
                            )}
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                ) : (
                  <EmptyState
                    icon={CheckCircle}
                    title="No priority actions required"
                  />
                )}
              </Card>
            </div>

            {/* Action Log / Recent Activity */}
            <Card title="Recent Activity" icon={MessageSquare}>
              {actionLog.length > 0 ? (
                <div className="space-y-3">
                  {actionLog.slice(-5).reverse().map((log, idx) => (
                    <div key={idx} className="flex items-start gap-3 p-2 bg-gray-50 rounded-xl">
                      <MessageSquare className="h-4 w-4 text-blue-500 mt-0.5" />
                      <div>
                        <p className="text-sm font-medium text-gray-900">{log.action}</p>
                        <p className="text-xs text-gray-500">
                          {log.account} - {log.time.toLocaleTimeString('en-GB')}
                        </p>
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <EmptyState
                  icon={MessageSquare}
                  title="No recent activity"
                  message="Actions will appear here"
                />
              )}
            </Card>
          </div>
        </>
      )}

      {/* Debtors Report View */}
      {viewMode === 'debtors' && (
        <>
          <Card>
            <div className="flex justify-between items-center mb-4">
              <div>
                <h3 className="text-base font-semibold text-gray-900">Aged Debtors Report</h3>
                <p className="text-sm text-gray-500">Customer balances by aging period</p>
              </div>
              <button
                onClick={() => debtorsQuery.refetch()}
                disabled={debtorsQuery.isFetching}
                className="btn btn-secondary text-sm flex items-center"
              >
                <RefreshCw className={`h-4 w-4 mr-2 ${debtorsQuery.isFetching ? 'animate-spin' : ''}`} />
                Refresh
              </button>
            </div>

            {debtorsQuery.isLoading ? (
              <LoadingState message="Loading debtors report..." />
            ) : debtorsQuery.data?.data?.success === false ? (
              <Alert variant="error" title="Error loading report">
                {(debtorsQuery.data?.data as DebtorsReportResponse)?.error}
              </Alert>
            ) : (
              <>
                {/* Totals Summary Cards */}
                {debtorsQuery.data?.data?.totals && (
                  <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-6">
                    <div className="p-4 bg-blue-50 rounded-xl border border-blue-200">
                      <p className="text-xs text-blue-600 font-medium">Total Balance</p>
                      <p className="text-xl font-bold text-blue-900">
                        {formatCurrency(debtorsQuery.data.data.totals.balance)}
                      </p>
                    </div>
                    <div className="p-4 bg-emerald-50 rounded-xl border border-emerald-200">
                      <p className="text-xs text-emerald-600 font-medium">Current</p>
                      <p className="text-xl font-bold text-emerald-900">
                        {formatCurrency(debtorsQuery.data.data.totals.current)}
                      </p>
                    </div>
                    <div className="p-4 bg-amber-50 rounded-xl border border-amber-200">
                      <p className="text-xs text-amber-600 font-medium">1 Month</p>
                      <p className="text-xl font-bold text-amber-900">
                        {formatCurrency(debtorsQuery.data.data.totals.month_1)}
                      </p>
                    </div>
                    <div className="p-4 bg-amber-50 rounded-xl border border-amber-200">
                      <p className="text-xs text-amber-600 font-medium">2 Month</p>
                      <p className="text-xl font-bold text-amber-900">
                        {formatCurrency(debtorsQuery.data.data.totals.month_2)}
                      </p>
                    </div>
                    <div className="p-4 bg-red-50 rounded-xl border border-red-200">
                      <p className="text-xs text-red-600 font-medium">3 Month+</p>
                      <p className="text-xl font-bold text-red-900">
                        {formatCurrency(debtorsQuery.data.data.totals.month_3_plus)}
                      </p>
                    </div>
                  </div>
                )}

                {/* Debtors Table */}
                {debtorsQuery.data?.data?.data && debtorsQuery.data.data.data.length > 0 ? (
                  <div className="overflow-x-auto">
                    <table className="min-w-full divide-y divide-gray-200">
                      <thead className="bg-gray-50">
                        <tr>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Account</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Customer</th>
                          <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Balance</th>
                          <th className="px-4 py-3 text-right text-xs font-medium text-emerald-600 uppercase tracking-wider">Current</th>
                          <th className="px-4 py-3 text-right text-xs font-medium text-amber-600 uppercase tracking-wider">1 Month</th>
                          <th className="px-4 py-3 text-right text-xs font-medium text-amber-600 uppercase tracking-wider">2 Month</th>
                          <th className="px-4 py-3 text-right text-xs font-medium text-red-600 uppercase tracking-wider">3 Month+</th>
                          <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Credit Limit</th>
                          <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
                        </tr>
                      </thead>
                      <tbody className="bg-white divide-y divide-gray-200">
                        {debtorsQuery.data.data.data.map((debtor, idx) => (
                          <tr key={idx} className="hover:bg-gray-50">
                            <td className="px-4 py-3 text-sm font-medium text-gray-900">{debtor.account?.trim()}</td>
                            <td className="px-4 py-3 text-sm text-gray-900">{debtor.customer?.trim()}</td>
                            <td className="px-4 py-3 text-sm text-right font-mono font-semibold text-gray-900">
                              {formatCurrency(debtor.balance || 0)}
                            </td>
                            <td className="px-4 py-3 text-sm text-right font-mono text-emerald-700">
                              {formatCurrency(debtor.current_period || 0)}
                            </td>
                            <td className="px-4 py-3 text-sm text-right font-mono text-amber-700">
                              {formatCurrency(debtor.month_1 || 0)}
                            </td>
                            <td className="px-4 py-3 text-sm text-right font-mono text-amber-700">
                              {formatCurrency(debtor.month_2 || 0)}
                            </td>
                            <td className="px-4 py-3 text-sm text-right font-mono text-red-700">
                              {formatCurrency(debtor.month_3_plus || 0)}
                            </td>
                            <td className="px-4 py-3 text-sm text-right font-mono text-gray-600">
                              {formatCurrency(debtor.credit_limit || 0)}
                            </td>
                            <td className="px-4 py-3 text-sm text-center">
                              {debtor.on_stop ? (
                                <StatusBadge variant="danger">On Stop</StatusBadge>
                              ) : debtor.balance > (debtor.credit_limit || 0) && (debtor.credit_limit || 0) > 0 ? (
                                <StatusBadge variant="warning">Over Limit</StatusBadge>
                              ) : (
                                <StatusBadge variant="success">OK</StatusBadge>
                              )}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                      {/* Totals Footer Row */}
                      <tfoot className="bg-gray-100 font-semibold">
                        <tr>
                          <td className="px-4 py-3 text-sm text-gray-900" colSpan={2}>TOTALS ({debtorsQuery.data.data.count} accounts)</td>
                          <td className="px-4 py-3 text-sm text-right font-mono text-gray-900">
                            {formatCurrency(debtorsQuery.data.data.totals?.balance || 0)}
                          </td>
                          <td className="px-4 py-3 text-sm text-right font-mono text-emerald-700">
                            {formatCurrency(debtorsQuery.data.data.totals?.current || 0)}
                          </td>
                          <td className="px-4 py-3 text-sm text-right font-mono text-amber-700">
                            {formatCurrency(debtorsQuery.data.data.totals?.month_1 || 0)}
                          </td>
                          <td className="px-4 py-3 text-sm text-right font-mono text-amber-700">
                            {formatCurrency(debtorsQuery.data.data.totals?.month_2 || 0)}
                          </td>
                          <td className="px-4 py-3 text-sm text-right font-mono text-red-700">
                            {formatCurrency(debtorsQuery.data.data.totals?.month_3_plus || 0)}
                          </td>
                          <td className="px-4 py-3 text-sm" colSpan={2}></td>
                        </tr>
                      </tfoot>
                    </table>
                  </div>
                ) : (
                  <EmptyState
                    icon={Users}
                    title="No debtors found"
                  />
                )}
              </>
            )}
          </Card>
        </>
      )}

      {/* Query View - Original functionality */}
      {viewMode === 'query' && (
        <>
          {/* Quick Query Buttons */}
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
            {QUICK_QUERIES.map((q) => {
              const Icon = q.icon;
              return (
                <button
                  key={q.label}
                  onClick={() => handleQuickQuery(q.question)}
                  disabled={queryMutation.isPending}
                  className={`p-3 rounded-xl border-2 text-left transition-all hover:shadow-md
                    ${result?.query_type === q.label.toLowerCase().replace(/ /g, '_')
                      ? `border-${q.color}-500 bg-${q.color}-50`
                      : 'border-gray-200 hover:border-gray-300 bg-white'}`}
                >
                  <Icon className={`h-5 w-5 text-${q.color}-500 mb-1`} />
                  <span className="text-sm font-medium text-gray-700">{q.label}</span>
                </button>
              );
            })}
          </div>

          {/* Search Box */}
          <form onSubmit={handleSubmit}>
            <Card>
              <div className="flex gap-4">
                <div className="flex-1 relative">
                  <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-5 w-5 text-gray-400" />
                  <input
                    type="text"
                    value={question}
                    onChange={(e) => setQuestion(e.target.value)}
                    placeholder="Ask a debtors control question... (e.g., 'Who owes us money?')"
                    className="input pl-10"
                  />
                </div>
                <button
                  type="submit"
                  disabled={queryMutation.isPending}
                  className="btn btn-primary px-6"
                >
                  {queryMutation.isPending ? 'Searching...' : 'Search'}
                </button>
              </div>
            </Card>
          </form>

          {/* Results */}
          {result && (
            <Card>
              {/* Summary Header */}
              <div className="flex justify-between items-start mb-4">
                <div>
                  <h3 className="text-base font-semibold text-gray-900">{result.description}</h3>
                  <p className="text-sm text-gray-600 mt-1">{result.summary}</p>
                </div>
                <StatusBadge variant="info">{result.count} records</StatusBadge>
              </div>

              {/* SQL Toggle */}
              {result.sql_used && (
                <button
                  onClick={() => setShowSQL(!showSQL)}
                  className="flex items-center text-sm text-gray-500 hover:text-gray-700 mb-4"
                >
                  {showSQL ? <ChevronUp className="h-4 w-4 mr-1" /> : <ChevronDown className="h-4 w-4 mr-1" />}
                  {showSQL ? 'Hide SQL' : 'Show SQL'}
                </button>
              )}
              {showSQL && result.sql_used && (
                <pre className="bg-gray-800 text-green-400 p-4 rounded-xl text-sm overflow-x-auto mb-4">
                  {result.sql_used}
                </pre>
              )}

              {/* Results Table */}
              {result.data && result.data.length > 0 ? (
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        {Object.keys(result.data[0]).map((key) => (
                          <th
                            key={key}
                            className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                          >
                            {getColumnHeader(key)}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {result.data.map((row, rowIndex) => (
                        <tr key={rowIndex} className="hover:bg-gray-50">
                          {Object.entries(row).map(([key, value], colIndex) => (
                            <td
                              key={colIndex}
                              className={`px-4 py-3 text-sm whitespace-nowrap ${
                                typeof value === 'number' ? 'text-right font-mono' : 'text-gray-900'
                              } ${
                                key === 'status' && value === 'OVER LIMIT' ? 'text-red-600 font-semibold' :
                                key === 'status' && value === 'ON STOP' ? 'text-amber-600 font-semibold' :
                                key === 'on_stop' && value ? 'text-red-600' : ''
                              }`}
                            >
                              {formatValue(value)}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <p className="text-sm text-gray-500 text-center py-8">No results found</p>
              )}
            </Card>
          )}

          {/* Error */}
          {result && !result.success && result.error && (
            <Alert variant="error" title="Error">
              {result.error}
            </Alert>
          )}
        </>
      )}

      {/* Agent Queries View - All 15 query categories */}
      {viewMode === 'agent' && (
        <>
          <div className="space-y-6">
            {Object.entries(QUERY_CATEGORIES).map(([key, category]) => (
              <Card key={key}>
                <h3 className={`text-base font-semibold mb-4 text-${category.color}-700`}>
                  {category.label}
                </h3>
                <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-3">
                  {category.queries.map((q) => {
                    const Icon = q.icon;
                    return (
                      <button
                        key={q.label}
                        onClick={() => handleQuickQuery(q.question)}
                        disabled={queryMutation.isPending}
                        className={`p-3 rounded-xl border-2 text-left transition-all hover:shadow-md
                          border-gray-200 hover:border-${category.color}-300 bg-white`}
                      >
                        <Icon className={`h-5 w-5 text-${category.color}-500 mb-1`} />
                        <span className="text-sm font-medium text-gray-700">{q.label}</span>
                      </button>
                    );
                  })}
                </div>
              </Card>
            ))}
          </div>

          {/* Search Box for Agent */}
          <form onSubmit={handleSubmit}>
            <Card>
              <div className="flex gap-4">
                <div className="flex-1 relative">
                  <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-5 w-5 text-gray-400" />
                  <input
                    type="text"
                    value={question}
                    onChange={(e) => setQuestion(e.target.value)}
                    placeholder="Ask any debtors control question or search for a customer/invoice..."
                    className="input pl-10"
                  />
                </div>
                <button
                  type="submit"
                  disabled={queryMutation.isPending}
                  className="btn btn-primary px-6"
                >
                  {queryMutation.isPending ? 'Searching...' : 'Search'}
                </button>
              </div>
            </Card>
          </form>

          {/* Results */}
          {result && (
            <Card>
              <div className="flex justify-between items-start mb-4">
                <div>
                  <div className="flex items-center gap-2">
                    <h3 className="text-base font-semibold text-gray-900">{result.description}</h3>
                    <StatusBadge variant="neutral">
                      {(result as { category?: string }).category || 'general'}
                    </StatusBadge>
                  </div>
                  <p className="text-sm text-gray-600 mt-1">{result.summary}</p>
                </div>
                <StatusBadge variant="info">{result.count} records</StatusBadge>
              </div>

              {result.sql_used && (
                <button
                  onClick={() => setShowSQL(!showSQL)}
                  className="flex items-center text-sm text-gray-500 hover:text-gray-700 mb-4"
                >
                  {showSQL ? <ChevronUp className="h-4 w-4 mr-1" /> : <ChevronDown className="h-4 w-4 mr-1" />}
                  {showSQL ? 'Hide SQL' : 'Show SQL'}
                </button>
              )}
              {showSQL && result.sql_used && (
                <pre className="bg-gray-800 text-green-400 p-4 rounded-xl text-sm overflow-x-auto mb-4">
                  {result.sql_used}
                </pre>
              )}

              {result.data && result.data.length > 0 ? (
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        {Object.keys(result.data[0]).map((key) => (
                          <th
                            key={key}
                            className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                          >
                            {getColumnHeader(key)}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {result.data.map((row, rowIndex) => (
                        <tr key={rowIndex} className="hover:bg-gray-50">
                          {Object.entries(row).map(([, value], colIndex) => (
                            <td
                              key={colIndex}
                              className={`px-4 py-3 text-sm whitespace-nowrap ${
                                typeof value === 'number' ? 'text-right font-mono' : 'text-gray-900'
                              }`}
                            >
                              {formatValue(value)}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <p className="text-sm text-gray-500 text-center py-8">No results found</p>
              )}
            </Card>
          )}
        </>
      )}

      {/* Load Data Success */}
      {loadDataMutation.isSuccess && (
        <Alert variant="success">
          Debtors control data loaded successfully!
        </Alert>
      )}
    </div>
  );
}
