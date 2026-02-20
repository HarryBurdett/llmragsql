import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  CheckCircle,
  AlertTriangle,
  RefreshCw,
  Users,
  FileText,
  Database,
  ArrowRight,
  TrendingUp,
  Clock,
} from 'lucide-react';
import apiClient from '../api/client';
import type { ReconciliationResponse } from '../api/client';
import { PageHeader, Card, LoadingState, Alert, SectionHeader } from '../components/ui';

export function DebtorsReconcile() {
  const [expandedSections, setExpandedSections] = useState<Set<string>>(
    new Set(['summary', 'variance', 'sales_ledger', 'nominal_ledger'])
  );

  const toggleSection = (section: string) => {
    const newExpanded = new Set(expandedSections);
    if (newExpanded.has(section)) {
      newExpanded.delete(section);
    } else {
      newExpanded.add(section);
    }
    setExpandedSections(newExpanded);
  };

  const debtorsQuery = useQuery<ReconciliationResponse>({
    queryKey: ['reconcileDebtors'],
    queryFn: async () => {
      const response = await apiClient.reconcileDebtors();
      return response.data;
    },
    refetchOnWindowFocus: false,
  });

  const formatCurrency = (value: number | undefined | null) => {
    if (value === undefined || value === null) return '£0.00';
    const prefix = value < 0 ? '-' : '';
    return prefix + new Intl.NumberFormat('en-GB', {
      style: 'currency',
      currency: 'GBP',
    }).format(Math.abs(value));
  };

  const data = debtorsQuery.data;
  const isLoading = debtorsQuery.isLoading;
  const error = debtorsQuery.error;

  // Extract values from nested response structure
  const salesLedgerTotal = data?.sales_ledger?.total_outstanding || 0;
  const nominalLedgerTotal = data?.nominal_ledger?.total_balance || 0;
  const variance = data?.variance?.amount || 0;
  const isReconciled = data?.variance?.reconciled || Math.abs(variance) < 0.01;

  return (
    <div className="space-y-6">
      {/* Header */}
      <PageHeader
        icon={Users}
        title="Debtors Reconciliation"
        subtitle="Sales Ledger vs Nominal Ledger control account"
      >
        <button
          onClick={() => debtorsQuery.refetch()}
          disabled={isLoading}
          className="flex items-center gap-2 px-4 py-2 bg-white border border-gray-200 hover:bg-gray-50 rounded-lg transition-colors text-sm font-medium text-gray-700 disabled:opacity-50"
        >
          <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </PageHeader>

      {/* Quick Stats */}
      {data && (
        <div className="grid grid-cols-3 gap-4">
          <Card>
            <div className="text-center">
              <div className="text-2xl font-bold text-gray-900">{formatCurrency(salesLedgerTotal)}</div>
              <div className="text-sm text-gray-500 mt-1">Sales Ledger</div>
            </div>
          </Card>
          <Card>
            <div className="text-center">
              <div className="text-2xl font-bold text-gray-900">{formatCurrency(nominalLedgerTotal)}</div>
              <div className="text-sm text-gray-500 mt-1">Nominal Control</div>
            </div>
          </Card>
          <Card className={isReconciled ? 'border-emerald-200 bg-emerald-50' : 'border-red-200 bg-red-50'}>
            <div className="text-center">
              <div className={`text-2xl font-bold ${isReconciled ? 'text-emerald-700' : 'text-red-700'}`}>{formatCurrency(variance)}</div>
              <div className="text-sm text-gray-500 mt-1">Variance</div>
            </div>
          </Card>
        </div>
      )}

      {/* Loading State */}
      {isLoading && (
        <Card>
          <LoadingState message="Loading reconciliation data..." />
        </Card>
      )}

      {/* Error State */}
      {error && (
        <Alert variant="error" title="Error loading data">
          {(error as Error).message}
        </Alert>
      )}

      {/* Data Display */}
      {data && !isLoading && (
        <div className="space-y-4">
          {/* Reconciliation Status */}
          <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
            <SectionHeader
              title="Reconciliation Status"
              icon={isReconciled ? CheckCircle : AlertTriangle}
              expanded={expandedSections.has('summary')}
              onToggle={() => toggleSection('summary')}
            />
            {expandedSections.has('summary') && (
              <div className="p-6 border-t border-gray-100">
                <div className="flex items-center justify-center gap-4 mb-6">
                  {isReconciled ? (
                    <div className="flex items-center gap-3 px-6 py-3 bg-emerald-100 text-emerald-800 rounded-xl">
                      <CheckCircle className="h-6 w-6" />
                      <span className="text-lg font-semibold">Reconciled</span>
                    </div>
                  ) : (
                    <div className="flex items-center gap-3 px-6 py-3 bg-red-100 text-red-800 rounded-xl">
                      <AlertTriangle className="h-6 w-6" />
                      <span className="text-lg font-semibold">
                        Variance of {formatCurrency(Math.abs(variance))}
                      </span>
                    </div>
                  )}
                </div>

                <div className="grid grid-cols-3 gap-6">
                  <div className="text-center p-6 bg-blue-50 rounded-xl border border-blue-100">
                    <FileText className="h-8 w-8 text-blue-600 mx-auto mb-2" />
                    <p className="text-sm text-gray-600 mb-1">Sales Ledger (stran)</p>
                    <p className="text-2xl font-bold text-blue-700">
                      {formatCurrency(salesLedgerTotal)}
                    </p>
                    <p className="text-xs text-gray-500 mt-1">
                      {data.sales_ledger?.transaction_count || 0} transactions
                    </p>
                  </div>

                  <div className="flex items-center justify-center">
                    <div className="flex flex-col items-center">
                      <ArrowRight className="h-8 w-8 text-gray-400" />
                      <span className="text-sm text-gray-500 mt-1">should equal</span>
                    </div>
                  </div>

                  <div className="text-center p-6 bg-emerald-50 rounded-xl border border-emerald-100">
                    <Database className="h-8 w-8 text-emerald-600 mx-auto mb-2" />
                    <p className="text-sm text-gray-600 mb-1">Nominal Control (nacnt)</p>
                    <p className="text-2xl font-bold text-emerald-700">
                      {formatCurrency(nominalLedgerTotal)}
                    </p>
                    <p className="text-xs text-gray-500 mt-1">
                      {data.nominal_ledger?.control_accounts?.length || 0} control account(s)
                    </p>
                  </div>
                </div>

                {/* Pending Transfers Warning */}
                {data.sales_ledger?.pending_transfer && data.sales_ledger.pending_transfer.count > 0 && (
                  <Alert variant="warning" title="Pending Transfer File Entries" className="mt-4">
                    {data.sales_ledger.pending_transfer.count} entries totalling{' '}
                    {formatCurrency(data.sales_ledger.pending_transfer.total)} not yet posted to Nominal Ledger
                  </Alert>
                )}
              </div>
            )}
          </div>

          {/* Sales Ledger Details */}
          <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
            <SectionHeader
              title="Sales Ledger Breakdown"
              icon={FileText}
              badge={data.sales_ledger?.transaction_count}
              expanded={expandedSections.has('sales_ledger')}
              onToggle={() => toggleSection('sales_ledger')}
            />
            {expandedSections.has('sales_ledger') && (
              <div className="p-6 border-t border-gray-100">
                {data.sales_ledger?.breakdown_by_type && data.sales_ledger.breakdown_by_type.length > 0 ? (
                  <div className="overflow-x-auto">
                    <table className="w-full">
                      <thead>
                        <tr className="bg-gray-50 text-gray-500 text-xs uppercase tracking-wider">
                          <th className="text-left py-3 px-4 font-semibold rounded-l-lg">Type</th>
                          <th className="text-left py-3 px-4 font-semibold">Description</th>
                          <th className="text-right py-3 px-4 font-semibold">Count</th>
                          <th className="text-right py-3 px-4 font-semibold rounded-r-lg">Total</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-gray-100">
                        {data.sales_ledger.breakdown_by_type.map((item, idx) => (
                          <tr key={idx} className="hover:bg-gray-50 transition-colors">
                            <td className="py-3 px-4">
                              <span className="inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                                {item.type}
                              </span>
                            </td>
                            <td className="py-3 px-4 text-sm text-gray-700">{item.description}</td>
                            <td className="py-3 px-4 text-right text-sm text-gray-600">{item.count}</td>
                            <td className="py-3 px-4 text-right text-sm font-medium text-gray-900">
                              {formatCurrency(item.total)}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                      <tfoot>
                        <tr className="bg-gray-50 font-semibold">
                          <td colSpan={2} className="py-3 px-4 text-sm rounded-l-lg">Total</td>
                          <td className="py-3 px-4 text-right text-sm">
                            {data.sales_ledger.transaction_count}
                          </td>
                          <td className="py-3 px-4 text-right text-sm rounded-r-lg">
                            {formatCurrency(data.sales_ledger.total_outstanding)}
                          </td>
                        </tr>
                      </tfoot>
                    </table>
                  </div>
                ) : (
                  <p className="text-sm text-gray-500 text-center py-4">No transaction data available</p>
                )}

                {/* Customer Master Check */}
                {data.sales_ledger?.customer_master_check && (
                  <div className="mt-4 p-4 bg-gray-50 rounded-xl">
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="text-sm font-medium text-gray-700">Customer Master (sname) Check</p>
                        <p className="text-xs text-gray-500">
                          {data.sales_ledger.customer_master_check.customer_count} customers with balances
                        </p>
                      </div>
                      <div className="text-right">
                        <p className="font-semibold text-gray-900">
                          {formatCurrency(data.sales_ledger.customer_master_check.total)}
                        </p>
                        {data.sales_ledger.customer_master_check.matches_stran ? (
                          <span className="text-xs text-emerald-600 flex items-center gap-1 justify-end">
                            <CheckCircle className="h-3 w-3" /> Matches stran
                          </span>
                        ) : (
                          <span className="text-xs text-amber-600 flex items-center gap-1 justify-end">
                            <AlertTriangle className="h-3 w-3" /> Differs from stran
                          </span>
                        )}
                      </div>
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>

          {/* Nominal Ledger Details */}
          <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
            <SectionHeader
              title="Nominal Ledger Control Accounts"
              icon={Database}
              badge={data.nominal_ledger?.control_accounts?.length}
              expanded={expandedSections.has('nominal_ledger')}
              onToggle={() => toggleSection('nominal_ledger')}
            />
            {expandedSections.has('nominal_ledger') && (
              <div className="p-6 border-t border-gray-100">
                {data.nominal_ledger?.control_accounts && data.nominal_ledger.control_accounts.length > 0 ? (
                  <div className="overflow-x-auto">
                    <table className="w-full">
                      <thead>
                        <tr className="bg-gray-50 text-gray-500 text-xs uppercase tracking-wider">
                          <th className="text-left py-3 px-4 font-semibold rounded-l-lg">Account</th>
                          <th className="text-left py-3 px-4 font-semibold">Description</th>
                          <th className="text-right py-3 px-4 font-semibold">B/F</th>
                          <th className="text-right py-3 px-4 font-semibold text-green-700">Debits</th>
                          <th className="text-right py-3 px-4 font-semibold text-red-700">Credits</th>
                          <th className="text-right py-3 px-4 font-semibold">Net</th>
                          <th className="text-right py-3 px-4 font-semibold rounded-r-lg">Balance</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-gray-100">
                        {data.nominal_ledger.control_accounts.map((acc, idx) => (
                          <tr key={idx} className="hover:bg-gray-50 transition-colors">
                            <td className="py-3 px-4 font-mono text-sm">{acc.account}</td>
                            <td className="py-3 px-4 text-sm text-gray-700">{acc.description}</td>
                            <td className="py-3 px-4 text-right text-sm text-gray-600">
                              {formatCurrency(acc.brought_forward)}
                            </td>
                            <td className="py-3 px-4 text-right text-sm text-green-700 bg-green-50/50">
                              {formatCurrency(acc.current_year_debits || 0)}
                            </td>
                            <td className="py-3 px-4 text-right text-sm text-red-700 bg-red-50/50">
                              {formatCurrency(acc.current_year_credits || 0)}
                            </td>
                            <td className="py-3 px-4 text-right text-sm text-gray-600">
                              {formatCurrency(acc.current_year_net)}
                            </td>
                            <td className="py-3 px-4 text-right text-sm font-semibold text-gray-900">
                              {formatCurrency(acc.closing_balance)}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                      <tfoot>
                        <tr className="bg-gray-50 font-semibold">
                          <td colSpan={3} className="py-3 px-4 text-sm rounded-l-lg">Total</td>
                          <td className="py-3 px-4 text-right text-sm text-green-700">
                            {formatCurrency(data.nominal_ledger.control_accounts.reduce((sum, acc) => sum + (acc.current_year_debits || 0), 0))}
                          </td>
                          <td className="py-3 px-4 text-right text-sm text-red-700">
                            {formatCurrency(data.nominal_ledger.control_accounts.reduce((sum, acc) => sum + (acc.current_year_credits || 0), 0))}
                          </td>
                          <td className="py-3 px-4 text-right text-sm"></td>
                          <td className="py-3 px-4 text-right text-sm rounded-r-lg">
                            {formatCurrency(data.nominal_ledger.total_balance)}
                          </td>
                        </tr>
                      </tfoot>
                    </table>
                  </div>
                ) : (
                  <p className="text-sm text-gray-500 text-center py-4">No control accounts found</p>
                )}

                <p className="text-xs text-gray-500 mt-4">
                  Source: {data.nominal_ledger?.source}
                </p>
              </div>
            )}
          </div>

          {/* Aged Analysis */}
          {data.aged_analysis && data.aged_analysis.length > 0 && (
            <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
              <SectionHeader
                title="Aged Analysis"
                icon={TrendingUp}
                expanded={expandedSections.has('aged')}
                onToggle={() => toggleSection('aged')}
              />
              {expandedSections.has('aged') && (
                <div className="p-6 border-t border-gray-100">
                  <div className="grid grid-cols-5 gap-4">
                    {data.aged_analysis.map((band, idx) => (
                      <div key={idx} className="text-center p-4 bg-gray-50 rounded-xl">
                        <p className="text-sm text-gray-600 mb-1">{band.age_band}</p>
                        <p className="text-lg font-bold text-gray-900">
                          {formatCurrency(band.total)}
                        </p>
                        <p className="text-xs text-gray-500">{band.count} items</p>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Top Customers */}
          {data.top_suppliers && data.top_suppliers.length > 0 && (
            <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
              <SectionHeader
                title="Top Customers by Balance"
                icon={Users}
                badge={data.top_suppliers.length}
                expanded={expandedSections.has('top')}
                onToggle={() => toggleSection('top')}
              />
              {expandedSections.has('top') && (
                <div className="p-6 border-t border-gray-100">
                  <div className="overflow-x-auto">
                    <table className="w-full">
                      <thead>
                        <tr className="bg-gray-50 text-gray-500 text-xs uppercase tracking-wider">
                          <th className="text-left py-3 px-4 font-semibold rounded-l-lg">Account</th>
                          <th className="text-left py-3 px-4 font-semibold">Name</th>
                          <th className="text-right py-3 px-4 font-semibold rounded-r-lg">Balance</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-gray-100">
                        {data.top_suppliers.map((cust, idx) => (
                          <tr key={idx} className="hover:bg-gray-50 transition-colors">
                            <td className="py-3 px-4 font-mono text-sm">{cust.account}</td>
                            <td className="py-3 px-4 text-sm text-gray-700">{cust.name}</td>
                            <td className="py-3 px-4 text-right text-sm font-semibold text-gray-900">
                              {formatCurrency(cust.outstanding)}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export default DebtorsReconcile;
