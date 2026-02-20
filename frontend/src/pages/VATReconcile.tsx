import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  CheckCircle,
  XCircle,
  AlertTriangle,
  RefreshCw,
  Receipt,
  ChevronDown,
  ChevronRight,
  TrendingUp,
  TrendingDown,
  Database,
  Percent,
  Calendar,
  Clock,
  FileText,
} from 'lucide-react';
import apiClient from '../api/client';
import { PageHeader, LoadingState, Alert } from '../components/ui';

interface VATCodeItem {
  code: string;
  description: string;
  rate: number;
  type: string;
  nominal_account: string;
}

interface VATByCode {
  vat_code: string;
  transaction_count: number;
  vat_amount: number;
  gross_amount?: number;
  net_amount: number;
}

interface NominalAccount {
  account: string;
  description: string;
  type: string;
  brought_forward: number;
  current_year_debits: number;
  current_year_credits: number;
  current_year_net: number;
  closing_balance: number;
}

interface NominalMovement {
  account: string;
  description: string;
  type: string;
  debits: number;
  credits: number;
  net: number;
  transaction_count: number;
}

interface QuarterInfo {
  current_quarter: string;
  quarter_start: string;
  quarter_end: string;
  quarters: Array<{
    name: string;
    start: string;
    end: string;
    is_current: boolean;
  }>;
}

interface VATReconciliationResponse {
  success: boolean;
  reconciliation_date: string;
  quarter_info: QuarterInfo;
  vat_codes: VATCodeItem[];
  current_quarter: {
    output_vat: {
      source: string;
      total_vat: number;
      by_code: VATByCode[];
      quarter: string;
    };
    input_vat: {
      source: string;
      total_vat: number;
      by_code: VATByCode[];
      quarter: string;
    };
    uncommitted: {
      source: string;
      quarter: string;
      period_start: string;
      period_end: string;
      output_vat: {
        total: number;
        by_code: VATByCode[];
      };
      input_vat: {
        total: number;
        by_code: VATByCode[];
      };
      net_liability: number;
      description: string;
    };
    nominal_movements: {
      source: string;
      quarter: string;
      period_start: string;
      period_end: string;
      accounts: NominalMovement[];
      output_vat_total: number;
      input_vat_total: number;
      net_movement: number;
    };
  };
  year_to_date: {
    output_vat: {
      source: string;
      total_vat: number;
      by_code: VATByCode[];
      current_year: number;
    };
    input_vat: {
      source: string;
      total_vat: number;
      by_code: VATByCode[];
      current_year: number;
    };
    nominal_accounts: {
      source: string;
      accounts: NominalAccount[];
      total_balance: number;
      current_year: number;
    };
  };
  variance: {
    quarter: {
      uncommitted_output: number;
      uncommitted_input: number;
      uncommitted_net: number;
      nl_output_movement: number;
      nl_input_movement: number;
      nl_net_movement: number;
      variance_amount: number;
      variance_absolute: number;
      reconciled: boolean;
    };
    year_to_date: {
      nvat_output_total: number;
      nvat_input_total: number;
      nvat_net_liability: number;
      nominal_ledger_balance: number;
      variance_amount: number;
      variance_absolute: number;
      reconciled: boolean;
    };
  };
  status: string;
  message: string;
  error?: string;
}

type ViewMode = 'quarter' | 'ytd';

export function VATReconcile() {
  const [expandedSections, setExpandedSections] = useState<Set<string>>(new Set(['summary', 'uncommitted', 'quarter_nl']));
  const [viewMode, setViewMode] = useState<ViewMode>('quarter');

  const toggleSection = (section: string) => {
    const newExpanded = new Set(expandedSections);
    if (newExpanded.has(section)) {
      newExpanded.delete(section);
    } else {
      newExpanded.add(section);
    }
    setExpandedSections(newExpanded);
  };

  const vatQuery = useQuery<VATReconciliationResponse>({
    queryKey: ['reconcileVAT'],
    queryFn: async () => {
      const response = await apiClient.reconcileVat();
      return response.data;
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

  const formatPercent = (value: number | undefined | null) => {
    if (value === undefined || value === null) return '0%';
    return `${value}%`;
  };

  const formatDate = (dateStr: string | undefined) => {
    if (!dateStr) return '';
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' });
  };

  const SectionHeader = ({ title, section, icon: Icon, badge, badgeColor }: {
    title: string;
    section: string;
    icon: React.ComponentType<{ className?: string }>;
    badge?: string | number;
    badgeColor?: string;
  }) => (
    <button
      onClick={() => toggleSection(section)}
      className="w-full flex items-center justify-between p-4 bg-gray-50 hover:bg-gray-100 rounded-lg transition-colors"
    >
      <div className="flex items-center gap-3">
        <Icon className="h-5 w-5 text-violet-600" />
        <span className="font-semibold text-gray-900">{title}</span>
        {badge !== undefined && (
          <span className={`px-2 py-0.5 text-xs font-medium rounded-full ${badgeColor || 'bg-violet-100 text-violet-700'}`}>
            {badge}
          </span>
        )}
      </div>
      {expandedSections.has(section) ? (
        <ChevronDown className="h-5 w-5 text-gray-400" />
      ) : (
        <ChevronRight className="h-5 w-5 text-gray-400" />
      )}
    </button>
  );

  const data = vatQuery.data;
  const isLoading = vatQuery.isLoading;
  const isReconciled = data?.variance?.quarter?.reconciled;

  return (
    <div className="space-y-6">
      {/* Header */}
      <PageHeader icon={Receipt} title="VAT Reconciliation" subtitle={`${data?.quarter_info?.current_quarter || 'Current Quarter'} - Uncommitted VAT vs Nominal Ledger`}>
        <div className="flex items-center gap-3">
          <div className="flex bg-gray-100 rounded-lg p-1">
            <button
              onClick={() => setViewMode('quarter')}
              className={`px-3 py-1.5 text-sm font-medium rounded-md transition-colors ${
                viewMode === 'quarter' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-600 hover:bg-gray-200'
              }`}
            >
              <span className="flex items-center gap-1.5">
                <Calendar className="h-4 w-4" />
                Quarter
              </span>
            </button>
            <button
              onClick={() => setViewMode('ytd')}
              className={`px-3 py-1.5 text-sm font-medium rounded-md transition-colors ${
                viewMode === 'ytd' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-600 hover:bg-gray-200'
              }`}
            >
              <span className="flex items-center gap-1.5">
                <FileText className="h-4 w-4" />
                Year to Date
              </span>
            </button>
          </div>
          <button
            onClick={() => vatQuery.refetch()}
            disabled={isLoading}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50"
          >
            <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
            Refresh
          </button>
        </div>
      </PageHeader>

      {/* Loading State */}
      {isLoading && (
        <LoadingState message="Loading VAT reconciliation data..." size="lg" />
      )}

      {/* Error State */}
      {vatQuery.error && (
        <Alert variant="error" title="Error loading data">{(vatQuery.error as Error).message}</Alert>
      )}

      {/* Data Display */}
      {data && !isLoading && (
        <div className="space-y-4">
          {/* Status Banner */}
          <div className={`rounded-lg p-4 flex items-center justify-between ${
            isReconciled ? 'bg-green-50 border border-green-200' : 'bg-amber-50 border border-amber-200'
          }`}>
            <div className="flex items-center gap-3">
              {isReconciled ? (
                <CheckCircle className="h-6 w-6 text-green-600" />
              ) : (
                <AlertTriangle className="h-6 w-6 text-amber-600" />
              )}
              <div>
                <span className={`font-semibold ${isReconciled ? 'text-green-800' : 'text-amber-800'}`}>
                  {data.status}
                </span>
                {data.message && (
                  <p className={`text-sm mt-0.5 ${isReconciled ? 'text-green-700' : 'text-amber-700'}`}>
                    {data.message}
                  </p>
                )}
              </div>
            </div>
            <div className="text-right text-sm text-gray-500">
              <div>As at: {data.reconciliation_date}</div>
              {data.quarter_info && (
                <div className="text-xs mt-1">
                  {formatDate(data.quarter_info.quarter_start)} - {formatDate(data.quarter_info.quarter_end)}
                </div>
              )}
            </div>
          </div>

          {/* ========== QUARTER VIEW ========== */}
          {viewMode === 'quarter' && (
            <>
              {/* Quarter Summary Section */}
              <div className="bg-white rounded-lg shadow">
                <SectionHeader title="Quarter VAT Summary" section="summary" icon={Receipt} badge={data.quarter_info?.current_quarter} />
                {expandedSections.has('summary') && (
                  <div className="p-6 border-t">
                    <div className="grid grid-cols-4 gap-4">
                      {/* Output VAT (Uncommitted) */}
                      <div className="text-center p-4 bg-green-50 rounded-lg border border-green-100">
                        <div className="flex items-center justify-center gap-2 mb-2">
                          <TrendingUp className="h-4 w-4 text-green-600" />
                          <p className="text-sm font-medium text-green-700">Output VAT</p>
                        </div>
                        <p className="text-2xl font-bold text-green-800">
                          {formatCurrency(data.current_quarter?.uncommitted?.output_vat?.total)}
                        </p>
                        <p className="text-xs text-green-600 mt-1">
                          Uncommitted (zvtran)
                        </p>
                      </div>

                      {/* Input VAT (Uncommitted) */}
                      <div className="text-center p-4 bg-red-50 rounded-lg border border-red-100">
                        <div className="flex items-center justify-center gap-2 mb-2">
                          <TrendingDown className="h-4 w-4 text-red-600" />
                          <p className="text-sm font-medium text-red-700">Input VAT</p>
                        </div>
                        <p className="text-2xl font-bold text-red-800">
                          {formatCurrency(data.current_quarter?.uncommitted?.input_vat?.total)}
                        </p>
                        <p className="text-xs text-red-600 mt-1">
                          Uncommitted (zvtran)
                        </p>
                      </div>

                      {/* Net VAT Liability */}
                      <div className="text-center p-4 bg-violet-50 rounded-lg border border-violet-100">
                        <div className="flex items-center justify-center gap-2 mb-2">
                          <Receipt className="h-4 w-4 text-violet-600" />
                          <p className="text-sm font-medium text-violet-700">Net VAT Due</p>
                        </div>
                        <p className="text-2xl font-bold text-violet-800">
                          {formatCurrency(data.current_quarter?.uncommitted?.net_liability)}
                        </p>
                        <p className="text-xs text-violet-600 mt-1">
                          Output - Input
                        </p>
                      </div>

                      {/* NL Movement */}
                      <div className="text-center p-4 bg-blue-50 rounded-lg border border-blue-100">
                        <div className="flex items-center justify-center gap-2 mb-2">
                          <Database className="h-4 w-4 text-blue-600" />
                          <p className="text-sm font-medium text-blue-700">NL Movement</p>
                        </div>
                        <p className="text-2xl font-bold text-blue-800">
                          {formatCurrency(data.current_quarter?.nominal_movements?.net_movement)}
                        </p>
                        <p className="text-xs text-blue-600 mt-1">
                          Nominal Ledger
                        </p>
                      </div>
                    </div>

                    {/* Variance */}
                    {data.variance?.quarter && (
                      <div className={`mt-4 p-4 rounded-lg border ${
                        data.variance.quarter.reconciled
                          ? 'bg-green-50 border-green-200'
                          : 'bg-red-50 border-red-200'
                      }`}>
                        <div className="flex items-center justify-between">
                          <div className="flex items-center gap-2">
                            {data.variance.quarter.reconciled ? (
                              <CheckCircle className="h-5 w-5 text-green-600" />
                            ) : (
                              <XCircle className="h-5 w-5 text-red-600" />
                            )}
                            <span className="font-medium">Quarter Variance (Uncommitted vs NL)</span>
                          </div>
                          <span className={`text-lg font-bold ${
                            data.variance.quarter.reconciled ? 'text-green-700' : 'text-red-700'
                          }`}>
                            {formatCurrency(data.variance.quarter.variance_amount)}
                          </span>
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </div>

              {/* Uncommitted VAT Details */}
              <div className="bg-white rounded-lg shadow">
                <SectionHeader
                  title="Uncommitted VAT (Not yet on VAT Return)"
                  section="uncommitted"
                  icon={Clock}
                  badge={`${(data.current_quarter?.uncommitted?.output_vat?.by_code?.length || 0) + (data.current_quarter?.uncommitted?.input_vat?.by_code?.length || 0)} codes`}
                  badgeColor="bg-amber-100 text-amber-700"
                />
                {expandedSections.has('uncommitted') && (
                  <div className="p-6 border-t">
                    <p className="text-sm text-gray-500 mb-4">
                      {data.current_quarter?.uncommitted?.description} | Period: {formatDate(data.current_quarter?.uncommitted?.period_start)} to {formatDate(data.current_quarter?.uncommitted?.period_end)}
                    </p>

                    <div className="grid grid-cols-2 gap-6">
                      {/* Output VAT */}
                      <div>
                        <h4 className="font-medium text-green-700 mb-3 flex items-center gap-2">
                          <TrendingUp className="h-4 w-4" />
                          Output VAT (Sales)
                        </h4>
                        {data.current_quarter?.uncommitted?.output_vat?.by_code && data.current_quarter.uncommitted.output_vat.by_code.length > 0 ? (
                          <table className="w-full text-sm">
                            <thead>
                              <tr className="bg-green-50 text-green-700">
                                <th className="text-left py-2 px-3 rounded-l-lg">Code</th>
                                <th className="text-right py-2 px-3">Txns</th>
                                <th className="text-right py-2 px-3">Net</th>
                                <th className="text-right py-2 px-3 rounded-r-lg">VAT</th>
                              </tr>
                            </thead>
                            <tbody className="divide-y divide-gray-100">
                              {data.current_quarter.uncommitted.output_vat.by_code.map((item, idx) => (
                                <tr key={idx}>
                                  <td className="py-2 px-3 font-mono">{item.vat_code || 'N/A'}</td>
                                  <td className="py-2 px-3 text-right text-gray-600">{item.transaction_count}</td>
                                  <td className="py-2 px-3 text-right">{formatCurrency(item.net_amount)}</td>
                                  <td className="py-2 px-3 text-right font-semibold text-green-700">{formatCurrency(item.vat_amount)}</td>
                                </tr>
                              ))}
                            </tbody>
                            <tfoot>
                              <tr className="bg-green-50 font-semibold">
                                <td colSpan={3} className="py-2 px-3 rounded-l-lg">Total</td>
                                <td className="py-2 px-3 text-right text-green-700 rounded-r-lg">
                                  {formatCurrency(data.current_quarter.uncommitted.output_vat.total)}
                                </td>
                              </tr>
                            </tfoot>
                          </table>
                        ) : (
                          <p className="text-gray-500 text-sm">No uncommitted output VAT</p>
                        )}
                      </div>

                      {/* Input VAT */}
                      <div>
                        <h4 className="font-medium text-red-700 mb-3 flex items-center gap-2">
                          <TrendingDown className="h-4 w-4" />
                          Input VAT (Purchases)
                        </h4>
                        {data.current_quarter?.uncommitted?.input_vat?.by_code && data.current_quarter.uncommitted.input_vat.by_code.length > 0 ? (
                          <table className="w-full text-sm">
                            <thead>
                              <tr className="bg-red-50 text-red-700">
                                <th className="text-left py-2 px-3 rounded-l-lg">Code</th>
                                <th className="text-right py-2 px-3">Txns</th>
                                <th className="text-right py-2 px-3">Net</th>
                                <th className="text-right py-2 px-3 rounded-r-lg">VAT</th>
                              </tr>
                            </thead>
                            <tbody className="divide-y divide-gray-100">
                              {data.current_quarter.uncommitted.input_vat.by_code.map((item, idx) => (
                                <tr key={idx}>
                                  <td className="py-2 px-3 font-mono">{item.vat_code || 'N/A'}</td>
                                  <td className="py-2 px-3 text-right text-gray-600">{item.transaction_count}</td>
                                  <td className="py-2 px-3 text-right">{formatCurrency(item.net_amount)}</td>
                                  <td className="py-2 px-3 text-right font-semibold text-red-700">{formatCurrency(item.vat_amount)}</td>
                                </tr>
                              ))}
                            </tbody>
                            <tfoot>
                              <tr className="bg-red-50 font-semibold">
                                <td colSpan={3} className="py-2 px-3 rounded-l-lg">Total</td>
                                <td className="py-2 px-3 text-right text-red-700 rounded-r-lg">
                                  {formatCurrency(data.current_quarter.uncommitted.input_vat.total)}
                                </td>
                              </tr>
                            </tfoot>
                          </table>
                        ) : (
                          <p className="text-gray-500 text-sm">No uncommitted input VAT</p>
                        )}
                      </div>
                    </div>

                    <p className="text-xs text-gray-500 mt-4">
                      Source: {data.current_quarter?.uncommitted?.source}
                    </p>
                  </div>
                )}
              </div>

              {/* Quarter NL Movements */}
              <div className="bg-white rounded-lg shadow">
                <SectionHeader
                  title="Nominal Ledger VAT Movements (Quarter)"
                  section="quarter_nl"
                  icon={Database}
                  badge={data.current_quarter?.nominal_movements?.accounts?.length}
                />
                {expandedSections.has('quarter_nl') && (
                  <div className="p-6 border-t">
                    {data.current_quarter?.nominal_movements?.accounts && data.current_quarter.nominal_movements.accounts.length > 0 ? (
                      <div className="overflow-x-auto">
                        <table className="w-full">
                          <thead>
                            <tr className="bg-gray-50 text-gray-500 text-xs uppercase tracking-wider">
                              <th className="text-left py-3 px-4 font-semibold rounded-l-lg">Account</th>
                              <th className="text-left py-3 px-4 font-semibold">Description</th>
                              <th className="text-center py-3 px-4 font-semibold">Type</th>
                              <th className="text-right py-3 px-4 font-semibold">Txns</th>
                              <th className="text-right py-3 px-4 font-semibold text-green-700">Debits</th>
                              <th className="text-right py-3 px-4 font-semibold text-red-700">Credits</th>
                              <th className="text-right py-3 px-4 font-semibold rounded-r-lg">Net</th>
                            </tr>
                          </thead>
                          <tbody className="divide-y divide-gray-100">
                            {data.current_quarter.nominal_movements.accounts.map((acc, idx) => (
                              <tr key={idx} className="hover:bg-gray-50 transition-colors">
                                <td className="py-3 px-4 font-mono text-sm">{acc.account}</td>
                                <td className="py-3 px-4 text-gray-700">{acc.description}</td>
                                <td className="py-3 px-4 text-center">
                                  <span className={`px-2 py-1 text-xs font-medium rounded ${
                                    acc.type === 'Output' ? 'bg-green-100 text-green-700' :
                                    acc.type === 'Input' ? 'bg-red-100 text-red-700' :
                                    'bg-gray-100 text-gray-700'
                                  }`}>
                                    {acc.type}
                                  </span>
                                </td>
                                <td className="py-3 px-4 text-right text-gray-600">{acc.transaction_count}</td>
                                <td className="py-3 px-4 text-right text-green-700 bg-green-50/50">
                                  {formatCurrency(acc.debits)}
                                </td>
                                <td className="py-3 px-4 text-right text-red-700 bg-red-50/50">
                                  {formatCurrency(acc.credits)}
                                </td>
                                <td className="py-3 px-4 text-right font-semibold text-gray-900">
                                  {formatCurrency(acc.net)}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                          <tfoot>
                            <tr className="bg-gray-50 font-semibold">
                              <td colSpan={4} className="py-3 px-4 rounded-l-lg">Totals</td>
                              <td className="py-3 px-4 text-right text-green-700">
                                {formatCurrency(data.current_quarter.nominal_movements.accounts.reduce((sum, acc) => sum + acc.debits, 0))}
                              </td>
                              <td className="py-3 px-4 text-right text-red-700">
                                {formatCurrency(data.current_quarter.nominal_movements.accounts.reduce((sum, acc) => sum + acc.credits, 0))}
                              </td>
                              <td className="py-3 px-4 text-right rounded-r-lg">
                                {formatCurrency(data.current_quarter.nominal_movements.net_movement)}
                              </td>
                            </tr>
                          </tfoot>
                        </table>
                      </div>
                    ) : (
                      <p className="text-gray-500 text-center py-4">No VAT nominal movements for current quarter</p>
                    )}
                    <p className="text-xs text-gray-500 mt-4">
                      Source: {data.current_quarter?.nominal_movements?.source} | Period: {formatDate(data.current_quarter?.nominal_movements?.period_start)} to {formatDate(data.current_quarter?.nominal_movements?.period_end)}
                    </p>
                  </div>
                )}
              </div>
            </>
          )}

          {/* ========== YEAR TO DATE VIEW ========== */}
          {viewMode === 'ytd' && (
            <>
              {/* YTD Summary Section */}
              <div className="bg-white rounded-lg shadow">
                <SectionHeader title="Year to Date VAT Summary" section="ytd_summary" icon={Receipt} badge={data.year_to_date?.output_vat?.current_year} />
                {expandedSections.has('ytd_summary') && (
                  <div className="p-6 border-t">
                    <div className="grid grid-cols-4 gap-4">
                      {/* Output VAT */}
                      <div className="text-center p-4 bg-green-50 rounded-lg border border-green-100">
                        <div className="flex items-center justify-center gap-2 mb-2">
                          <TrendingUp className="h-4 w-4 text-green-600" />
                          <p className="text-sm font-medium text-green-700">Output VAT</p>
                        </div>
                        <p className="text-2xl font-bold text-green-800">
                          {formatCurrency(data.year_to_date?.output_vat?.total_vat)}
                        </p>
                        <p className="text-xs text-green-600 mt-1">
                          VAT collected on sales
                        </p>
                      </div>

                      {/* Input VAT */}
                      <div className="text-center p-4 bg-red-50 rounded-lg border border-red-100">
                        <div className="flex items-center justify-center gap-2 mb-2">
                          <TrendingDown className="h-4 w-4 text-red-600" />
                          <p className="text-sm font-medium text-red-700">Input VAT</p>
                        </div>
                        <p className="text-2xl font-bold text-red-800">
                          {formatCurrency(data.year_to_date?.input_vat?.total_vat)}
                        </p>
                        <p className="text-xs text-red-600 mt-1">
                          VAT paid on purchases
                        </p>
                      </div>

                      {/* Net Liability (nvat) */}
                      <div className="text-center p-4 bg-violet-50 rounded-lg border border-violet-100">
                        <div className="flex items-center justify-center gap-2 mb-2">
                          <Receipt className="h-4 w-4 text-violet-600" />
                          <p className="text-sm font-medium text-violet-700">Net VAT (nvat)</p>
                        </div>
                        <p className="text-2xl font-bold text-violet-800">
                          {formatCurrency(data.variance?.year_to_date?.nvat_net_liability)}
                        </p>
                        <p className="text-xs text-violet-600 mt-1">
                          Output - Input
                        </p>
                      </div>

                      {/* Nominal Ledger Balance */}
                      <div className="text-center p-4 bg-blue-50 rounded-lg border border-blue-100">
                        <div className="flex items-center justify-center gap-2 mb-2">
                          <Database className="h-4 w-4 text-blue-600" />
                          <p className="text-sm font-medium text-blue-700">NL Balance</p>
                        </div>
                        <p className="text-2xl font-bold text-blue-800">
                          {formatCurrency(data.year_to_date?.nominal_accounts?.total_balance)}
                        </p>
                        <p className="text-xs text-blue-600 mt-1">
                          Nominal ledger VAT accounts
                        </p>
                      </div>
                    </div>

                    {/* YTD Variance */}
                    {data.variance?.year_to_date && (
                      <div className={`mt-4 p-4 rounded-lg border ${
                        data.variance.year_to_date.reconciled
                          ? 'bg-green-50 border-green-200'
                          : 'bg-red-50 border-red-200'
                      }`}>
                        <div className="flex items-center justify-between">
                          <div className="flex items-center gap-2">
                            {data.variance.year_to_date.reconciled ? (
                              <CheckCircle className="h-5 w-5 text-green-600" />
                            ) : (
                              <XCircle className="h-5 w-5 text-red-600" />
                            )}
                            <span className="font-medium">YTD Variance (nvat vs NL)</span>
                          </div>
                          <span className={`text-lg font-bold ${
                            data.variance.year_to_date.reconciled ? 'text-green-700' : 'text-red-700'
                          }`}>
                            {formatCurrency(data.variance.year_to_date.variance_amount)}
                          </span>
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </div>

              {/* Output VAT Details */}
              <div className="bg-white rounded-lg shadow">
                <SectionHeader
                  title="Output VAT (Sales) - YTD"
                  section="ytd_output"
                  icon={TrendingUp}
                  badge={data.year_to_date?.output_vat?.by_code?.length}
                />
                {expandedSections.has('ytd_output') && (
                  <div className="p-6 border-t">
                    {data.year_to_date?.output_vat?.by_code && data.year_to_date.output_vat.by_code.length > 0 ? (
                      <div className="overflow-x-auto">
                        <table className="w-full">
                          <thead>
                            <tr className="bg-gray-50 text-gray-500 text-xs uppercase tracking-wider">
                              <th className="text-left py-3 px-4 font-semibold rounded-l-lg">VAT Code</th>
                              <th className="text-right py-3 px-4 font-semibold">Transactions</th>
                              <th className="text-right py-3 px-4 font-semibold">Net Amount</th>
                              <th className="text-right py-3 px-4 font-semibold text-green-700">VAT Amount</th>
                              <th className="text-right py-3 px-4 font-semibold rounded-r-lg">Gross Amount</th>
                            </tr>
                          </thead>
                          <tbody className="divide-y divide-gray-100">
                            {data.year_to_date.output_vat.by_code.map((item, idx) => (
                              <tr key={idx} className="hover:bg-gray-50 transition-colors">
                                <td className="py-3 px-4 font-mono text-sm">{item.vat_code || 'N/A'}</td>
                                <td className="py-3 px-4 text-right text-gray-600">{item.transaction_count}</td>
                                <td className="py-3 px-4 text-right">{formatCurrency(item.net_amount)}</td>
                                <td className="py-3 px-4 text-right font-semibold text-green-700 bg-green-50/50">
                                  {formatCurrency(item.vat_amount)}
                                </td>
                                <td className="py-3 px-4 text-right">{formatCurrency(item.gross_amount)}</td>
                              </tr>
                            ))}
                          </tbody>
                          <tfoot>
                            <tr className="bg-gray-50 font-semibold">
                              <td className="py-3 px-4 rounded-l-lg">Total</td>
                              <td className="py-3 px-4 text-right">
                                {data.year_to_date.output_vat.by_code.reduce((sum, item) => sum + item.transaction_count, 0)}
                              </td>
                              <td className="py-3 px-4 text-right">
                                {formatCurrency(data.year_to_date.output_vat.by_code.reduce((sum, item) => sum + item.net_amount, 0))}
                              </td>
                              <td className="py-3 px-4 text-right text-green-700">
                                {formatCurrency(data.year_to_date.output_vat.total_vat)}
                              </td>
                              <td className="py-3 px-4 text-right rounded-r-lg">
                                {formatCurrency(data.year_to_date.output_vat.by_code.reduce((sum, item) => sum + (item.gross_amount || 0), 0))}
                              </td>
                            </tr>
                          </tfoot>
                        </table>
                      </div>
                    ) : (
                      <p className="text-gray-500 text-center py-4">No output VAT transactions for current year</p>
                    )}
                    <p className="text-xs text-gray-500 mt-4">
                      Source: {data.year_to_date?.output_vat?.source} | Year: {data.year_to_date?.output_vat?.current_year}
                    </p>
                  </div>
                )}
              </div>

              {/* Input VAT Details */}
              <div className="bg-white rounded-lg shadow">
                <SectionHeader
                  title="Input VAT (Purchases) - YTD"
                  section="ytd_input"
                  icon={TrendingDown}
                  badge={data.year_to_date?.input_vat?.by_code?.length}
                />
                {expandedSections.has('ytd_input') && (
                  <div className="p-6 border-t">
                    {data.year_to_date?.input_vat?.by_code && data.year_to_date.input_vat.by_code.length > 0 ? (
                      <div className="overflow-x-auto">
                        <table className="w-full">
                          <thead>
                            <tr className="bg-gray-50 text-gray-500 text-xs uppercase tracking-wider">
                              <th className="text-left py-3 px-4 font-semibold rounded-l-lg">VAT Code</th>
                              <th className="text-right py-3 px-4 font-semibold">Transactions</th>
                              <th className="text-right py-3 px-4 font-semibold">Net Amount</th>
                              <th className="text-right py-3 px-4 font-semibold text-red-700">VAT Amount</th>
                              <th className="text-right py-3 px-4 font-semibold rounded-r-lg">Gross Amount</th>
                            </tr>
                          </thead>
                          <tbody className="divide-y divide-gray-100">
                            {data.year_to_date.input_vat.by_code.map((item, idx) => (
                              <tr key={idx} className="hover:bg-gray-50 transition-colors">
                                <td className="py-3 px-4 font-mono text-sm">{item.vat_code || 'N/A'}</td>
                                <td className="py-3 px-4 text-right text-gray-600">{item.transaction_count}</td>
                                <td className="py-3 px-4 text-right">{formatCurrency(item.net_amount)}</td>
                                <td className="py-3 px-4 text-right font-semibold text-red-700 bg-red-50/50">
                                  {formatCurrency(item.vat_amount)}
                                </td>
                                <td className="py-3 px-4 text-right">{formatCurrency(item.gross_amount)}</td>
                              </tr>
                            ))}
                          </tbody>
                          <tfoot>
                            <tr className="bg-gray-50 font-semibold">
                              <td className="py-3 px-4 rounded-l-lg">Total</td>
                              <td className="py-3 px-4 text-right">
                                {data.year_to_date.input_vat.by_code.reduce((sum, item) => sum + item.transaction_count, 0)}
                              </td>
                              <td className="py-3 px-4 text-right">
                                {formatCurrency(data.year_to_date.input_vat.by_code.reduce((sum, item) => sum + item.net_amount, 0))}
                              </td>
                              <td className="py-3 px-4 text-right text-red-700">
                                {formatCurrency(data.year_to_date.input_vat.total_vat)}
                              </td>
                              <td className="py-3 px-4 text-right rounded-r-lg">
                                {formatCurrency(data.year_to_date.input_vat.by_code.reduce((sum, item) => sum + (item.gross_amount || 0), 0))}
                              </td>
                            </tr>
                          </tfoot>
                        </table>
                      </div>
                    ) : (
                      <p className="text-gray-500 text-center py-4">No input VAT transactions for current year</p>
                    )}
                    <p className="text-xs text-gray-500 mt-4">
                      Source: {data.year_to_date?.input_vat?.source} | Year: {data.year_to_date?.input_vat?.current_year}
                    </p>
                  </div>
                )}
              </div>

              {/* Nominal Ledger VAT Accounts */}
              <div className="bg-white rounded-lg shadow">
                <SectionHeader
                  title="Nominal Ledger VAT Accounts - YTD"
                  section="ytd_nominal"
                  icon={Database}
                  badge={data.year_to_date?.nominal_accounts?.accounts?.length}
                />
                {expandedSections.has('ytd_nominal') && (
                  <div className="p-6 border-t">
                    {data.year_to_date?.nominal_accounts?.accounts && data.year_to_date.nominal_accounts.accounts.length > 0 ? (
                      <div className="overflow-x-auto">
                        <table className="w-full">
                          <thead>
                            <tr className="bg-gray-50 text-gray-500 text-xs uppercase tracking-wider">
                              <th className="text-left py-3 px-4 font-semibold rounded-l-lg">Account</th>
                              <th className="text-left py-3 px-4 font-semibold">Description</th>
                              <th className="text-center py-3 px-4 font-semibold">Type</th>
                              <th className="text-right py-3 px-4 font-semibold">B/F</th>
                              <th className="text-right py-3 px-4 font-semibold text-green-700">Debits</th>
                              <th className="text-right py-3 px-4 font-semibold text-red-700">Credits</th>
                              <th className="text-right py-3 px-4 font-semibold">Net</th>
                              <th className="text-right py-3 px-4 font-semibold rounded-r-lg">Balance</th>
                            </tr>
                          </thead>
                          <tbody className="divide-y divide-gray-100">
                            {data.year_to_date.nominal_accounts.accounts.map((acc, idx) => (
                              <tr key={idx} className="hover:bg-gray-50 transition-colors">
                                <td className="py-3 px-4 font-mono text-sm">{acc.account}</td>
                                <td className="py-3 px-4 text-gray-700">{acc.description}</td>
                                <td className="py-3 px-4 text-center">
                                  <span className={`px-2 py-1 text-xs font-medium rounded ${
                                    acc.type === 'Output' ? 'bg-green-100 text-green-700' :
                                    acc.type === 'Input' ? 'bg-red-100 text-red-700' :
                                    'bg-gray-100 text-gray-700'
                                  }`}>
                                    {acc.type}
                                  </span>
                                </td>
                                <td className="py-3 px-4 text-right text-gray-600">
                                  {formatCurrency(acc.brought_forward)}
                                </td>
                                <td className="py-3 px-4 text-right text-green-700 bg-green-50/50">
                                  {formatCurrency(acc.current_year_debits)}
                                </td>
                                <td className="py-3 px-4 text-right text-red-700 bg-red-50/50">
                                  {formatCurrency(acc.current_year_credits)}
                                </td>
                                <td className="py-3 px-4 text-right text-gray-600">
                                  {formatCurrency(acc.current_year_net)}
                                </td>
                                <td className="py-3 px-4 text-right font-semibold text-gray-900">
                                  {formatCurrency(acc.closing_balance)}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                          <tfoot>
                            <tr className="bg-gray-50 font-semibold">
                              <td colSpan={4} className="py-3 px-4 rounded-l-lg">Total</td>
                              <td className="py-3 px-4 text-right text-green-700">
                                {formatCurrency(data.year_to_date.nominal_accounts.accounts.reduce((sum, acc) => sum + acc.current_year_debits, 0))}
                              </td>
                              <td className="py-3 px-4 text-right text-red-700">
                                {formatCurrency(data.year_to_date.nominal_accounts.accounts.reduce((sum, acc) => sum + acc.current_year_credits, 0))}
                              </td>
                              <td className="py-3 px-4 text-right"></td>
                              <td className="py-3 px-4 text-right rounded-r-lg">
                                {formatCurrency(data.year_to_date.nominal_accounts.total_balance)}
                              </td>
                            </tr>
                          </tfoot>
                        </table>
                      </div>
                    ) : (
                      <p className="text-gray-500 text-center py-4">No VAT nominal accounts found</p>
                    )}
                    <p className="text-xs text-gray-500 mt-4">
                      Source: {data.year_to_date?.nominal_accounts?.source} | Year: {data.year_to_date?.nominal_accounts?.current_year}
                    </p>
                  </div>
                )}
              </div>
            </>
          )}

          {/* VAT Codes Reference (shown in both views) */}
          <div className="bg-white rounded-lg shadow">
            <SectionHeader
              title="VAT Codes Reference"
              section="codes"
              icon={Percent}
              badge={data.vat_codes?.length}
            />
            {expandedSections.has('codes') && (
              <div className="p-6 border-t">
                {data.vat_codes && data.vat_codes.length > 0 ? (
                  <div className="overflow-x-auto">
                    <table className="w-full">
                      <thead>
                        <tr className="bg-gray-50 text-gray-500 text-xs uppercase tracking-wider">
                          <th className="text-left py-3 px-4 font-semibold rounded-l-lg">Code</th>
                          <th className="text-left py-3 px-4 font-semibold">Description</th>
                          <th className="text-center py-3 px-4 font-semibold">Type</th>
                          <th className="text-right py-3 px-4 font-semibold">Rate</th>
                          <th className="text-left py-3 px-4 font-semibold rounded-r-lg">Nominal Account</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-gray-100">
                        {data.vat_codes.map((code, idx) => (
                          <tr key={idx} className="hover:bg-gray-50 transition-colors">
                            <td className="py-3 px-4 font-mono text-sm font-medium">{code.code}</td>
                            <td className="py-3 px-4 text-gray-700">{code.description}</td>
                            <td className="py-3 px-4 text-center">
                              <span className={`px-2 py-1 text-xs font-medium rounded ${
                                code.type === 'S' ? 'bg-green-100 text-green-700' :
                                code.type === 'P' ? 'bg-red-100 text-red-700' :
                                'bg-gray-100 text-gray-700'
                              }`}>
                                {code.type === 'S' ? 'Sales' : code.type === 'P' ? 'Purchase' : code.type}
                              </span>
                            </td>
                            <td className="py-3 px-4 text-right font-medium">
                              {formatPercent(code.rate)}
                            </td>
                            <td className="py-3 px-4 font-mono text-sm text-gray-600">{code.nominal_account || '-'}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <p className="text-gray-500 text-center py-4">No VAT codes found</p>
                )}
                <p className="text-xs text-gray-500 mt-4">
                  Source: ztax (VAT Codes Table)
                </p>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

export default VATReconcile;
