import { useState } from 'react';
import { authFetch } from '../api/client';
import { useQuery } from '@tanstack/react-query';
import {
  CheckCircle,
  XCircle,
  RefreshCw,
  Database,
  Search,
} from 'lucide-react';
import { PageHeader, LoadingState, Alert, Card, SectionHeader } from '../components/ui';

interface TrialBalanceAccount {
  account: string;
  description: string;
  type: string;
  type_name: string;
  bf_balance: number;
  current_debits: number;
  current_credits: number;
  current_net: number;
  closing_balance: number;
}

interface TrialBalanceSummary {
  brought_forward: {
    debits: number;
    credits: number;
    variance: number;
    balanced: boolean;
  };
  current_year: {
    debits: number;
    credits: number;
    variance: number;
    balanced: boolean;
  };
  closing: {
    debits: number;
    credits: number;
    variance: number;
    balanced: boolean;
  };
  account_count: number;
}

interface TrialBalanceResponse {
  success: boolean;
  reconciliation_date: string;
  current_year: number;
  summary: TrialBalanceSummary;
  accounts: TrialBalanceAccount[];
  status: string;
  message: string;
  error?: string;
}

export function TrialBalanceCheck() {
  const [showAccounts, setShowAccounts] = useState(false);
  const [searchFilter, setSearchFilter] = useState('');
  const [typeFilter, setTypeFilter] = useState<string>('all');

  const tbQuery = useQuery<TrialBalanceResponse>({
    queryKey: ['trialBalanceCheck'],
    queryFn: async () => {
      const response = await authFetch('http://localhost:8000/api/reconcile/trial-balance');
      return response.json();
    },
    refetchOnWindowFocus: false,
  });

  const formatCurrency = (value: number | undefined | null) => {
    if (value === undefined || value === null) return '£0.00';
    return new Intl.NumberFormat('en-GB', {
      style: 'currency',
      currency: 'GBP',
    }).format(value);
  };

  const data = tbQuery.data;
  const isLoading = tbQuery.isLoading;
  const isBalanced = data?.status === 'BALANCED';

  // Filter accounts
  const filteredAccounts = data?.accounts?.filter(acc => {
    const matchesSearch = !searchFilter ||
      acc.account.toLowerCase().includes(searchFilter.toLowerCase()) ||
      acc.description.toLowerCase().includes(searchFilter.toLowerCase());
    const matchesType = typeFilter === 'all' || acc.type === typeFilter;
    return matchesSearch && matchesType;
  }) || [];

  // Get unique types for filter
  const accountTypes = [...new Set(data?.accounts?.map(a => a.type) || [])].sort();

  return (
    <div className="space-y-6">
      {/* Header */}
      <PageHeader
        icon={Database}
        title="Trial Balance Check"
        subtitle={`${data?.current_year ? `Year ${data.current_year} - ` : ''}Verify nominal ledger debits equal credits`}
      >
        <button
          onClick={() => tbQuery.refetch()}
          disabled={isLoading}
          className="flex items-center gap-2 px-4 py-2 bg-gray-100 hover:bg-gray-200 rounded-lg transition-colors disabled:opacity-50"
        >
          <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </PageHeader>

      {/* Loading State */}
      {isLoading && (
        <LoadingState message="Checking trial balance..." size="lg" />
      )}

      {/* Error State */}
      {tbQuery.error && (
        <Alert variant="error" title="Error loading data">
          {(tbQuery.error as Error).message}
        </Alert>
      )}

      {/* Data Display */}
      {data && !isLoading && (
        <div className="space-y-6">
          {/* Status Banner */}
          <Alert variant={isBalanced ? 'success' : 'error'} title={data.status}>
            <div className="flex items-center justify-between">
              <span>{data.message}</span>
              <span className="text-xs text-gray-500">
                As at: {data.reconciliation_date}
              </span>
            </div>
          </Alert>

          {/* Summary Cards */}
          <div className="grid grid-cols-3 gap-4">
            {/* Brought Forward */}
            <Card>
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-base font-semibold text-gray-900">Brought Forward</h3>
                {data.summary.brought_forward.balanced ? (
                  <CheckCircle className="h-5 w-5 text-emerald-500" />
                ) : (
                  <XCircle className="h-5 w-5 text-red-500" />
                )}
              </div>
              <div className="space-y-2">
                <div className="flex justify-between text-sm">
                  <span className="text-gray-600">Debit Balances</span>
                  <span className="font-medium text-emerald-700">{formatCurrency(data.summary.brought_forward.debits)}</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-gray-600">Credit Balances</span>
                  <span className="font-medium text-red-700">{formatCurrency(data.summary.brought_forward.credits)}</span>
                </div>
                <div className="border-t pt-2 flex justify-between text-sm">
                  <span className="text-gray-600">Variance</span>
                  <span className={`font-bold ${
                    data.summary.brought_forward.balanced ? 'text-emerald-600' : 'text-red-600'
                  }`}>
                    {formatCurrency(data.summary.brought_forward.variance)}
                  </span>
                </div>
              </div>
            </Card>

            {/* Current Year */}
            <Card>
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-base font-semibold text-gray-900">Current Year</h3>
                {data.summary.current_year.balanced ? (
                  <CheckCircle className="h-5 w-5 text-emerald-500" />
                ) : (
                  <XCircle className="h-5 w-5 text-red-500" />
                )}
              </div>
              <div className="space-y-2">
                <div className="flex justify-between text-sm">
                  <span className="text-gray-600">Total Debits</span>
                  <span className="font-medium text-emerald-700">{formatCurrency(data.summary.current_year.debits)}</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-gray-600">Total Credits</span>
                  <span className="font-medium text-red-700">{formatCurrency(data.summary.current_year.credits)}</span>
                </div>
                <div className="border-t pt-2 flex justify-between text-sm">
                  <span className="text-gray-600">Variance</span>
                  <span className={`font-bold ${
                    data.summary.current_year.balanced ? 'text-emerald-600' : 'text-red-600'
                  }`}>
                    {formatCurrency(data.summary.current_year.variance)}
                  </span>
                </div>
              </div>
            </Card>

            {/* Closing */}
            <Card>
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-base font-semibold text-gray-900">Closing Balance</h3>
                {data.summary.closing.balanced ? (
                  <CheckCircle className="h-5 w-5 text-emerald-500" />
                ) : (
                  <XCircle className="h-5 w-5 text-red-500" />
                )}
              </div>
              <div className="space-y-2">
                <div className="flex justify-between text-sm">
                  <span className="text-gray-600">Debit Balances</span>
                  <span className="font-medium text-emerald-700">{formatCurrency(data.summary.closing.debits)}</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-gray-600">Credit Balances</span>
                  <span className="font-medium text-red-700">{formatCurrency(data.summary.closing.credits)}</span>
                </div>
                <div className="border-t pt-2 flex justify-between text-sm">
                  <span className="text-gray-600">Variance</span>
                  <span className={`font-bold ${
                    data.summary.closing.balanced ? 'text-emerald-600' : 'text-red-600'
                  }`}>
                    {formatCurrency(data.summary.closing.variance)}
                  </span>
                </div>
              </div>
            </Card>
          </div>

          {/* Account Details */}
          <Card padding={false}>
            <SectionHeader
              title="Account Details"
              icon={Database}
              badge={`${data.summary.account_count} accounts`}
              badgeVariant="neutral"
              expanded={showAccounts}
              onToggle={() => setShowAccounts(!showAccounts)}
            />

            {showAccounts && (
              <div className="p-4 border-t">
                {/* Filters */}
                <div className="flex gap-4 mb-4">
                  <div className="relative flex-1">
                    <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
                    <input
                      type="text"
                      placeholder="Search accounts..."
                      value={searchFilter}
                      onChange={(e) => setSearchFilter(e.target.value)}
                      className="w-full pl-10 pr-4 py-2 border border-gray-200 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    />
                  </div>
                  <select
                    value={typeFilter}
                    onChange={(e) => setTypeFilter(e.target.value)}
                    className="px-4 py-2 border border-gray-200 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  >
                    <option value="all">All Types</option>
                    {accountTypes.map(type => (
                      <option key={type} value={type}>{type}</option>
                    ))}
                  </select>
                </div>

                {/* Table */}
                <div className="overflow-x-auto max-h-96 overflow-y-auto">
                  <table className="w-full text-sm">
                    <thead className="sticky top-0 bg-gray-50">
                      <tr className="text-gray-500 text-xs uppercase tracking-wider">
                        <th className="text-left py-3 px-4 font-semibold">Account</th>
                        <th className="text-left py-3 px-4 font-semibold">Description</th>
                        <th className="text-center py-3 px-4 font-semibold">Type</th>
                        <th className="text-right py-3 px-4 font-semibold">B/F</th>
                        <th className="text-right py-3 px-4 font-semibold text-emerald-700">Debits</th>
                        <th className="text-right py-3 px-4 font-semibold text-red-700">Credits</th>
                        <th className="text-right py-3 px-4 font-semibold">Net</th>
                        <th className="text-right py-3 px-4 font-semibold">Closing</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100">
                      {filteredAccounts.map((acc, idx) => (
                        <tr key={idx} className="hover:bg-gray-50">
                          <td className="py-2 px-4 font-mono text-sm">{acc.account}</td>
                          <td className="py-2 px-4 text-sm text-gray-700 truncate max-w-xs">{acc.description}</td>
                          <td className="py-2 px-4 text-center">
                            <span className="px-2 py-0.5 text-xs font-medium bg-gray-100 text-gray-700 rounded">
                              {acc.type}
                            </span>
                          </td>
                          <td className={`py-2 px-4 text-right text-sm ${acc.bf_balance >= 0 ? 'text-gray-900' : 'text-red-600'}`}>
                            {formatCurrency(acc.bf_balance)}
                          </td>
                          <td className="py-2 px-4 text-right text-sm text-emerald-700 bg-emerald-50/50">
                            {formatCurrency(acc.current_debits)}
                          </td>
                          <td className="py-2 px-4 text-right text-sm text-red-700 bg-red-50/50">
                            {formatCurrency(acc.current_credits)}
                          </td>
                          <td className={`py-2 px-4 text-right text-sm ${acc.current_net >= 0 ? 'text-gray-900' : 'text-red-600'}`}>
                            {formatCurrency(acc.current_net)}
                          </td>
                          <td className={`py-2 px-4 text-right text-sm font-medium ${acc.closing_balance >= 0 ? 'text-gray-900' : 'text-red-600'}`}>
                            {formatCurrency(acc.closing_balance)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                    <tfoot className="sticky bottom-0 bg-gray-100 font-semibold">
                      <tr>
                        <td colSpan={4} className="py-3 px-4 text-sm">Totals ({filteredAccounts.length} accounts)</td>
                        <td className="py-3 px-4 text-right text-sm text-emerald-700">
                          {formatCurrency(filteredAccounts.reduce((sum, a) => sum + a.current_debits, 0))}
                        </td>
                        <td className="py-3 px-4 text-right text-sm text-red-700">
                          {formatCurrency(filteredAccounts.reduce((sum, a) => sum + a.current_credits, 0))}
                        </td>
                        <td className="py-3 px-4 text-right text-sm">
                          {formatCurrency(filteredAccounts.reduce((sum, a) => sum + a.current_net, 0))}
                        </td>
                        <td className="py-3 px-4 text-right text-sm">
                          {formatCurrency(filteredAccounts.reduce((sum, a) => sum + a.closing_balance, 0))}
                        </td>
                      </tr>
                    </tfoot>
                  </table>
                </div>
              </div>
            )}
          </Card>
        </div>
      )}
    </div>
  );
}

export default TrialBalanceCheck;
