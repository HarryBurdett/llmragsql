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
} from 'lucide-react';
import apiClient from '../api/client';

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
  gross_amount: number;
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

interface VATReconciliationResponse {
  success: boolean;
  reconciliation_date: string;
  vat_codes: VATCodeItem[];
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
  variance: {
    nvat_output_total: number;
    nvat_input_total: number;
    nvat_net_liability: number;
    nominal_ledger_balance: number;
    variance_amount: number;
    variance_absolute: number;
    reconciled: boolean;
  };
  status: string;
  message: string;
  error?: string;
}

export function VATReconcile() {
  const [expandedSections, setExpandedSections] = useState<Set<string>>(new Set(['summary', 'output', 'input']));

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

  const SectionHeader = ({ title, section, icon: Icon, badge }: { title: string; section: string; icon: React.ComponentType<{ className?: string }>; badge?: string | number }) => (
    <button
      onClick={() => toggleSection(section)}
      className="w-full flex items-center justify-between p-4 bg-gray-50 hover:bg-gray-100 rounded-lg transition-colors"
    >
      <div className="flex items-center gap-3">
        <Icon className="h-5 w-5 text-violet-600" />
        <span className="font-semibold text-gray-900">{title}</span>
        {badge !== undefined && (
          <span className="px-2 py-0.5 text-xs font-medium bg-violet-100 text-violet-700 rounded-full">
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
  const isReconciled = data?.status === 'RECONCILED';

  return (
    <div className="space-y-6">
      {/* Header with gradient */}
      <div className="bg-gradient-to-r from-violet-600 to-purple-600 rounded-xl shadow-lg p-6 text-white">
        <div className="flex justify-between items-center">
          <div>
            <h1 className="text-2xl font-bold flex items-center gap-3">
              <div className="p-2 bg-white/20 rounded-lg backdrop-blur-sm">
                <Receipt className="h-6 w-6" />
              </div>
              VAT Reconciliation
            </h1>
            <p className="text-violet-100 mt-2">Output VAT vs Input VAT vs Nominal Ledger</p>
          </div>
          <button
            onClick={() => vatQuery.refetch()}
            disabled={isLoading}
            className="flex items-center gap-2 px-4 py-2 bg-white/20 hover:bg-white/30 backdrop-blur-sm rounded-lg transition-colors disabled:opacity-50"
          >
            <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
            Refresh
          </button>
        </div>
      </div>

      {/* Loading State */}
      {isLoading && (
        <div className="bg-white rounded-lg shadow p-8 text-center">
          <RefreshCw className="h-8 w-8 animate-spin text-violet-600 mx-auto mb-4" />
          <p className="text-gray-600">Loading VAT reconciliation data...</p>
        </div>
      )}

      {/* Error State */}
      {vatQuery.error && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4">
          <div className="flex items-center gap-2 text-red-800">
            <XCircle className="h-5 w-5" />
            <span className="font-medium">Error loading data</span>
          </div>
          <p className="text-red-600 mt-1">{(vatQuery.error as Error).message}</p>
        </div>
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
            <span className="text-sm text-gray-500">
              As at: {data.reconciliation_date}
            </span>
          </div>

          {/* Summary Section */}
          <div className="bg-white rounded-lg shadow">
            <SectionHeader title="VAT Summary" section="summary" icon={Receipt} />
            {expandedSections.has('summary') && (
              <div className="p-6 border-t">
                <div className="grid grid-cols-4 gap-4">
                  {/* Output VAT */}
                  <div className="text-center p-4 bg-green-50 rounded-lg border border-green-100">
                    <div className="flex items-center justify-center gap-2 mb-2">
                      <TrendingUp className="h-4 w-4 text-green-600" />
                      <p className="text-sm font-medium text-green-700">Output VAT</p>
                    </div>
                    <p className="text-2xl font-bold text-green-800">
                      {formatCurrency(data.output_vat?.total_vat)}
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
                      {formatCurrency(data.input_vat?.total_vat)}
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
                      {formatCurrency(data.variance?.nvat_net_liability)}
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
                      {formatCurrency(data.nominal_accounts?.total_balance)}
                    </p>
                    <p className="text-xs text-blue-600 mt-1">
                      Nominal ledger VAT accounts
                    </p>
                  </div>
                </div>

                {/* Variance */}
                {data.variance && (
                  <div className={`mt-4 p-4 rounded-lg border ${
                    data.variance.reconciled
                      ? 'bg-green-50 border-green-200'
                      : 'bg-red-50 border-red-200'
                  }`}>
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        {data.variance.reconciled ? (
                          <CheckCircle className="h-5 w-5 text-green-600" />
                        ) : (
                          <XCircle className="h-5 w-5 text-red-600" />
                        )}
                        <span className="font-medium">Variance (nvat vs NL)</span>
                      </div>
                      <span className={`text-lg font-bold ${
                        data.variance.reconciled ? 'text-green-700' : 'text-red-700'
                      }`}>
                        {formatCurrency(data.variance.variance_amount)}
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
              title="Output VAT (Sales)"
              section="output"
              icon={TrendingUp}
              badge={data.output_vat?.by_code?.length}
            />
            {expandedSections.has('output') && (
              <div className="p-6 border-t">
                {data.output_vat?.by_code && data.output_vat.by_code.length > 0 ? (
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
                        {data.output_vat.by_code.map((item, idx) => (
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
                            {data.output_vat.by_code.reduce((sum, item) => sum + item.transaction_count, 0)}
                          </td>
                          <td className="py-3 px-4 text-right">
                            {formatCurrency(data.output_vat.by_code.reduce((sum, item) => sum + item.net_amount, 0))}
                          </td>
                          <td className="py-3 px-4 text-right text-green-700">
                            {formatCurrency(data.output_vat.total_vat)}
                          </td>
                          <td className="py-3 px-4 text-right rounded-r-lg">
                            {formatCurrency(data.output_vat.by_code.reduce((sum, item) => sum + item.gross_amount, 0))}
                          </td>
                        </tr>
                      </tfoot>
                    </table>
                  </div>
                ) : (
                  <p className="text-gray-500 text-center py-4">No output VAT transactions for current year</p>
                )}
                <p className="text-xs text-gray-500 mt-4">
                  Source: {data.output_vat?.source} | Year: {data.output_vat?.current_year}
                </p>
              </div>
            )}
          </div>

          {/* Input VAT Details */}
          <div className="bg-white rounded-lg shadow">
            <SectionHeader
              title="Input VAT (Purchases)"
              section="input"
              icon={TrendingDown}
              badge={data.input_vat?.by_code?.length}
            />
            {expandedSections.has('input') && (
              <div className="p-6 border-t">
                {data.input_vat?.by_code && data.input_vat.by_code.length > 0 ? (
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
                        {data.input_vat.by_code.map((item, idx) => (
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
                            {data.input_vat.by_code.reduce((sum, item) => sum + item.transaction_count, 0)}
                          </td>
                          <td className="py-3 px-4 text-right">
                            {formatCurrency(data.input_vat.by_code.reduce((sum, item) => sum + item.net_amount, 0))}
                          </td>
                          <td className="py-3 px-4 text-right text-red-700">
                            {formatCurrency(data.input_vat.total_vat)}
                          </td>
                          <td className="py-3 px-4 text-right rounded-r-lg">
                            {formatCurrency(data.input_vat.by_code.reduce((sum, item) => sum + item.gross_amount, 0))}
                          </td>
                        </tr>
                      </tfoot>
                    </table>
                  </div>
                ) : (
                  <p className="text-gray-500 text-center py-4">No input VAT transactions for current year</p>
                )}
                <p className="text-xs text-gray-500 mt-4">
                  Source: {data.input_vat?.source} | Year: {data.input_vat?.current_year}
                </p>
              </div>
            )}
          </div>

          {/* Nominal Ledger VAT Accounts */}
          <div className="bg-white rounded-lg shadow">
            <SectionHeader
              title="Nominal Ledger VAT Accounts"
              section="nominal"
              icon={Database}
              badge={data.nominal_accounts?.accounts?.length}
            />
            {expandedSections.has('nominal') && (
              <div className="p-6 border-t">
                {data.nominal_accounts?.accounts && data.nominal_accounts.accounts.length > 0 ? (
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
                        {data.nominal_accounts.accounts.map((acc, idx) => (
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
                            {formatCurrency(data.nominal_accounts.accounts.reduce((sum, acc) => sum + acc.current_year_debits, 0))}
                          </td>
                          <td className="py-3 px-4 text-right text-red-700">
                            {formatCurrency(data.nominal_accounts.accounts.reduce((sum, acc) => sum + acc.current_year_credits, 0))}
                          </td>
                          <td className="py-3 px-4 text-right"></td>
                          <td className="py-3 px-4 text-right rounded-r-lg">
                            {formatCurrency(data.nominal_accounts.total_balance)}
                          </td>
                        </tr>
                      </tfoot>
                    </table>
                  </div>
                ) : (
                  <p className="text-gray-500 text-center py-4">No VAT nominal accounts found</p>
                )}
                <p className="text-xs text-gray-500 mt-4">
                  Source: {data.nominal_accounts?.source} | Year: {data.nominal_accounts?.current_year}
                </p>
              </div>
            )}
          </div>

          {/* VAT Codes Reference */}
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
