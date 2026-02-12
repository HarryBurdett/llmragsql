import { useState } from 'react';
import { authFetch } from '../api/client';
import { useQuery } from '@tanstack/react-query';
import {
  CheckCircle,
  XCircle,
  AlertTriangle,
  RefreshCw,
  Building,
  ChevronDown,
  ChevronRight,
  FileText,
  Database,
  ArrowRight,
  TrendingUp,
  Clock,
} from 'lucide-react';
import apiClient from '../api/client';
import type { ReconciliationResponse } from '../api/client';

export function CreditorsReconcile() {
  const [expandedSections, setExpandedSections] = useState<Set<string>>(
    new Set(['summary', 'variance', 'purchase_ledger', 'nominal_ledger'])
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

  const creditorsQuery = useQuery<ReconciliationResponse>({
    queryKey: ['reconcileCreditors'],
    queryFn: async () => {
      const response = await apiClient.reconcileCreditors();
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

  const SectionHeader = ({
    title,
    section,
    icon: Icon,
    badge,
  }: {
    title: string;
    section: string;
    icon: React.ComponentType<{ className?: string }>;
    badge?: string | number;
  }) => (
    <button
      onClick={() => toggleSection(section)}
      className="w-full flex items-center justify-between p-4 bg-slate-50 hover:bg-slate-100 rounded-xl transition-colors"
    >
      <div className="flex items-center gap-3">
        <div className="p-2 bg-white rounded-lg shadow-sm">
          <Icon className="h-5 w-5 text-emerald-600" />
        </div>
        <span className="font-semibold text-slate-900">{title}</span>
        {badge !== undefined && (
          <span className="px-2 py-0.5 text-xs font-medium bg-emerald-100 text-emerald-700 rounded-full">
            {badge}
          </span>
        )}
      </div>
      {expandedSections.has(section) ? (
        <ChevronDown className="h-5 w-5 text-slate-400" />
      ) : (
        <ChevronRight className="h-5 w-5 text-slate-400" />
      )}
    </button>
  );

  const data = creditorsQuery.data;
  const isLoading = creditorsQuery.isLoading;
  const error = creditorsQuery.error;

  // Extract values from nested response structure
  const purchaseLedgerTotal = data?.purchase_ledger?.total_outstanding || 0;
  const nominalLedgerTotal = data?.nominal_ledger?.total_balance || 0;
  const variance = data?.variance?.amount || 0;
  const isReconciled = data?.variance?.reconciled || Math.abs(variance) < 0.01;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="relative overflow-hidden bg-gradient-to-r from-emerald-600 via-teal-600 to-emerald-700 rounded-2xl p-8 text-white shadow-lg">
        <div className="absolute inset-0 bg-grid-white/10 [mask-image:linear-gradient(0deg,transparent,black)]" />
        <div className="relative flex justify-between items-start">
          <div>
            <div className="flex items-center gap-3 mb-2">
              <div className="p-2 bg-white/20 rounded-lg backdrop-blur-sm">
                <Building className="h-6 w-6" />
              </div>
              <h1 className="text-3xl font-bold">Creditors Reconciliation</h1>
            </div>
            <p className="text-emerald-100 text-lg">
              Purchase Ledger vs Nominal Ledger control account
            </p>
          </div>
          <button
            onClick={() => creditorsQuery.refetch()}
            disabled={isLoading}
            className="flex items-center gap-2 px-4 py-2 bg-white/20 hover:bg-white/30 rounded-xl backdrop-blur-sm transition-all duration-200 text-sm font-medium disabled:opacity-50"
          >
            <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
            Refresh
          </button>
        </div>

        {/* Quick Stats */}
        {data && (
          <div className="relative mt-6 grid grid-cols-3 gap-4">
            <div className="bg-white/10 backdrop-blur-sm rounded-xl p-4">
              <div className="text-2xl font-bold">{formatCurrency(purchaseLedgerTotal)}</div>
              <div className="text-emerald-200 text-sm">Purchase Ledger</div>
            </div>
            <div className="bg-white/10 backdrop-blur-sm rounded-xl p-4">
              <div className="text-2xl font-bold">{formatCurrency(nominalLedgerTotal)}</div>
              <div className="text-emerald-200 text-sm">Nominal Control</div>
            </div>
            <div className={`backdrop-blur-sm rounded-xl p-4 ${
              isReconciled ? 'bg-emerald-400/30' : 'bg-red-500/30'
            }`}>
              <div className="text-2xl font-bold">{formatCurrency(variance)}</div>
              <div className="text-sm opacity-80">Variance</div>
            </div>
          </div>
        )}
      </div>

      {/* Loading State */}
      {isLoading && (
        <div className="bg-white rounded-2xl shadow-sm border border-slate-100 p-12">
          <div className="flex flex-col items-center">
            <RefreshCw className="h-8 w-8 animate-spin text-emerald-600 mb-4" />
            <p className="text-slate-500 font-medium">Loading reconciliation data...</p>
          </div>
        </div>
      )}

      {/* Error State */}
      {error && (
        <div className="bg-red-50 border border-red-200 rounded-2xl p-6">
          <div className="flex items-center gap-4">
            <div className="p-3 bg-red-100 rounded-xl">
              <XCircle className="h-6 w-6 text-red-600" />
            </div>
            <div>
              <h3 className="font-semibold text-red-900">Error loading data</h3>
              <p className="text-red-700 text-sm">{(error as Error).message}</p>
            </div>
          </div>
        </div>
      )}

      {/* Data Display */}
      {data && !isLoading && (
        <div className="space-y-4">
          {/* Reconciliation Status */}
          <div className="bg-white rounded-2xl shadow-sm border border-slate-100 overflow-hidden">
            <SectionHeader
              title="Reconciliation Status"
              section="summary"
              icon={isReconciled ? CheckCircle : AlertTriangle}
            />
            {expandedSections.has('summary') && (
              <div className="p-6 border-t border-slate-100">
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
                  <div className="text-center p-6 bg-emerald-50 rounded-xl border border-emerald-100">
                    <FileText className="h-8 w-8 text-emerald-600 mx-auto mb-2" />
                    <p className="text-sm text-slate-600 mb-1">Purchase Ledger (ptran)</p>
                    <p className="text-2xl font-bold text-emerald-700">
                      {formatCurrency(purchaseLedgerTotal)}
                    </p>
                    <p className="text-xs text-slate-500 mt-1">
                      {data.purchase_ledger?.transaction_count || 0} transactions
                    </p>
                  </div>

                  <div className="flex items-center justify-center">
                    <div className="flex flex-col items-center">
                      <ArrowRight className="h-8 w-8 text-slate-400" />
                      <span className="text-sm text-slate-500 mt-1">should equal</span>
                    </div>
                  </div>

                  <div className="text-center p-6 bg-blue-50 rounded-xl border border-blue-100">
                    <Database className="h-8 w-8 text-blue-600 mx-auto mb-2" />
                    <p className="text-sm text-slate-600 mb-1">Nominal Control (nacnt)</p>
                    <p className="text-2xl font-bold text-blue-700">
                      {formatCurrency(nominalLedgerTotal)}
                    </p>
                    <p className="text-xs text-slate-500 mt-1">
                      {data.nominal_ledger?.control_accounts?.length || 0} control account(s)
                    </p>
                  </div>
                </div>

                {/* Pending Transfers Warning */}
                {data.purchase_ledger?.pending_transfer && data.purchase_ledger.pending_transfer.count > 0 && (
                  <div className="mt-4 p-4 bg-amber-50 border border-amber-200 rounded-xl">
                    <div className="flex items-center gap-3">
                      <Clock className="h-5 w-5 text-amber-600" />
                      <div>
                        <p className="font-medium text-amber-800">Pending Transfer File Entries</p>
                        <p className="text-sm text-amber-700">
                          {data.purchase_ledger.pending_transfer.count} entries totalling{' '}
                          {formatCurrency(data.purchase_ledger.pending_transfer.total)} not yet posted to Nominal Ledger
                        </p>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>

          {/* Purchase Ledger Details */}
          <div className="bg-white rounded-2xl shadow-sm border border-slate-100 overflow-hidden">
            <SectionHeader
              title="Purchase Ledger Breakdown"
              section="purchase_ledger"
              icon={FileText}
              badge={data.purchase_ledger?.transaction_count}
            />
            {expandedSections.has('purchase_ledger') && (
              <div className="p-6 border-t border-slate-100">
                {data.purchase_ledger?.breakdown_by_type && data.purchase_ledger.breakdown_by_type.length > 0 ? (
                  <div className="overflow-x-auto">
                    <table className="w-full">
                      <thead>
                        <tr className="bg-slate-50 text-slate-500 text-xs uppercase tracking-wider">
                          <th className="text-left py-3 px-4 font-semibold rounded-l-lg">Type</th>
                          <th className="text-left py-3 px-4 font-semibold">Description</th>
                          <th className="text-right py-3 px-4 font-semibold">Count</th>
                          <th className="text-right py-3 px-4 font-semibold rounded-r-lg">Total</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-100">
                        {data.purchase_ledger.breakdown_by_type.map((item, idx) => (
                          <tr key={idx} className="hover:bg-slate-50 transition-colors">
                            <td className="py-3 px-4">
                              <span className="inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium bg-emerald-100 text-emerald-800">
                                {item.type}
                              </span>
                            </td>
                            <td className="py-3 px-4 text-slate-700">{item.description}</td>
                            <td className="py-3 px-4 text-right text-slate-600">{item.count}</td>
                            <td className="py-3 px-4 text-right font-medium text-slate-900">
                              {formatCurrency(item.total)}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                      <tfoot>
                        <tr className="bg-slate-50 font-semibold">
                          <td colSpan={2} className="py-3 px-4 rounded-l-lg">Total</td>
                          <td className="py-3 px-4 text-right">
                            {data.purchase_ledger.transaction_count}
                          </td>
                          <td className="py-3 px-4 text-right rounded-r-lg">
                            {formatCurrency(data.purchase_ledger.total_outstanding)}
                          </td>
                        </tr>
                      </tfoot>
                    </table>
                  </div>
                ) : (
                  <p className="text-slate-500 text-center py-4">No transaction data available</p>
                )}

                {/* Supplier Master Check */}
                {data.purchase_ledger?.supplier_master_check && (
                  <div className="mt-4 p-4 bg-slate-50 rounded-xl">
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="text-sm font-medium text-slate-700">Supplier Master (pname) Check</p>
                        <p className="text-xs text-slate-500">
                          {data.purchase_ledger.supplier_master_check.supplier_count} suppliers with balances
                        </p>
                      </div>
                      <div className="text-right">
                        <p className="font-semibold text-slate-900">
                          {formatCurrency(data.purchase_ledger.supplier_master_check.total)}
                        </p>
                        {data.purchase_ledger.supplier_master_check.matches_ptran ? (
                          <span className="text-xs text-emerald-600 flex items-center gap-1 justify-end">
                            <CheckCircle className="h-3 w-3" /> Matches ptran
                          </span>
                        ) : (
                          <span className="text-xs text-amber-600 flex items-center gap-1 justify-end">
                            <AlertTriangle className="h-3 w-3" /> Differs from ptran
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
          <div className="bg-white rounded-2xl shadow-sm border border-slate-100 overflow-hidden">
            <SectionHeader
              title="Nominal Ledger Control Accounts"
              section="nominal_ledger"
              icon={Database}
              badge={data.nominal_ledger?.control_accounts?.length}
            />
            {expandedSections.has('nominal_ledger') && (
              <div className="p-6 border-t border-slate-100">
                {data.nominal_ledger?.control_accounts && data.nominal_ledger.control_accounts.length > 0 ? (
                  <div className="overflow-x-auto">
                    <table className="w-full">
                      <thead>
                        <tr className="bg-slate-50 text-slate-500 text-xs uppercase tracking-wider">
                          <th className="text-left py-3 px-4 font-semibold rounded-l-lg">Account</th>
                          <th className="text-left py-3 px-4 font-semibold">Description</th>
                          <th className="text-right py-3 px-4 font-semibold">B/F</th>
                          <th className="text-right py-3 px-4 font-semibold text-green-700">Debits</th>
                          <th className="text-right py-3 px-4 font-semibold text-red-700">Credits</th>
                          <th className="text-right py-3 px-4 font-semibold">Net</th>
                          <th className="text-right py-3 px-4 font-semibold rounded-r-lg">Balance</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-100">
                        {data.nominal_ledger.control_accounts.map((acc, idx) => (
                          <tr key={idx} className="hover:bg-slate-50 transition-colors">
                            <td className="py-3 px-4 font-mono text-sm">{acc.account}</td>
                            <td className="py-3 px-4 text-slate-700">{acc.description}</td>
                            <td className="py-3 px-4 text-right text-slate-600">
                              {formatCurrency(acc.brought_forward)}
                            </td>
                            <td className="py-3 px-4 text-right text-green-700 bg-green-50/50">
                              {formatCurrency(acc.current_year_debits || 0)}
                            </td>
                            <td className="py-3 px-4 text-right text-red-700 bg-red-50/50">
                              {formatCurrency(acc.current_year_credits || 0)}
                            </td>
                            <td className="py-3 px-4 text-right text-slate-600">
                              {formatCurrency(acc.current_year_net)}
                            </td>
                            <td className="py-3 px-4 text-right font-semibold text-slate-900">
                              {formatCurrency(acc.closing_balance)}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                      <tfoot>
                        <tr className="bg-slate-50 font-semibold">
                          <td colSpan={3} className="py-3 px-4 rounded-l-lg">Total</td>
                          <td className="py-3 px-4 text-right text-green-700">
                            {formatCurrency(data.nominal_ledger.control_accounts.reduce((sum, acc) => sum + (acc.current_year_debits || 0), 0))}
                          </td>
                          <td className="py-3 px-4 text-right text-red-700">
                            {formatCurrency(data.nominal_ledger.control_accounts.reduce((sum, acc) => sum + (acc.current_year_credits || 0), 0))}
                          </td>
                          <td className="py-3 px-4 text-right"></td>
                          <td className="py-3 px-4 text-right rounded-r-lg">
                            {formatCurrency(data.nominal_ledger.total_balance)}
                          </td>
                        </tr>
                      </tfoot>
                    </table>
                  </div>
                ) : (
                  <p className="text-slate-500 text-center py-4">No control accounts found</p>
                )}

                <p className="text-xs text-slate-500 mt-4">
                  Source: {data.nominal_ledger?.source}
                </p>
              </div>
            )}
          </div>

          {/* Aged Analysis */}
          {data.aged_analysis && data.aged_analysis.length > 0 && (
            <div className="bg-white rounded-2xl shadow-sm border border-slate-100 overflow-hidden">
              <SectionHeader
                title="Aged Analysis"
                section="aged"
                icon={TrendingUp}
              />
              {expandedSections.has('aged') && (
                <div className="p-6 border-t border-slate-100">
                  <div className="grid grid-cols-5 gap-4">
                    {data.aged_analysis.map((band, idx) => (
                      <div key={idx} className="text-center p-4 bg-slate-50 rounded-xl">
                        <p className="text-sm text-slate-600 mb-1">{band.age_band}</p>
                        <p className="text-lg font-bold text-slate-900">
                          {formatCurrency(band.total)}
                        </p>
                        <p className="text-xs text-slate-500">{band.count} items</p>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Top Suppliers */}
          {data.top_suppliers && data.top_suppliers.length > 0 && (
            <div className="bg-white rounded-2xl shadow-sm border border-slate-100 overflow-hidden">
              <SectionHeader
                title="Top Suppliers by Balance"
                section="top"
                icon={Building}
                badge={data.top_suppliers.length}
              />
              {expandedSections.has('top') && (
                <div className="p-6 border-t border-slate-100">
                  <div className="overflow-x-auto">
                    <table className="w-full">
                      <thead>
                        <tr className="bg-slate-50 text-slate-500 text-xs uppercase tracking-wider">
                          <th className="text-left py-3 px-4 font-semibold rounded-l-lg">Account</th>
                          <th className="text-left py-3 px-4 font-semibold">Name</th>
                          <th className="text-right py-3 px-4 font-semibold rounded-r-lg">Balance</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-100">
                        {data.top_suppliers.map((supplier, idx) => (
                          <tr key={idx} className="hover:bg-slate-50 transition-colors">
                            <td className="py-3 px-4 font-mono text-sm">{supplier.account}</td>
                            <td className="py-3 px-4 text-slate-700">{supplier.name}</td>
                            <td className="py-3 px-4 text-right font-semibold text-slate-900">
                              {formatCurrency(supplier.outstanding)}
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

export default CreditorsReconcile;
