import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  CheckCircle,
  XCircle,
  AlertTriangle,
  RefreshCw,
  BookOpen,
  Landmark,
  ChevronDown,
  ChevronRight,
} from 'lucide-react';
import apiClient from '../api/client';
import type { BankAccountsResponse, BankReconciliationResponse } from '../api/client';

export function CashbookReconcile() {
  const [selectedBank, setSelectedBank] = useState<string | null>(null);
  const [expandedSections, setExpandedSections] = useState<Set<string>>(new Set(['summary']));

  const toggleSection = (section: string) => {
    const newExpanded = new Set(expandedSections);
    if (newExpanded.has(section)) {
      newExpanded.delete(section);
    } else {
      newExpanded.add(section);
    }
    setExpandedSections(newExpanded);
  };

  // Fetch bank accounts list
  const banksQuery = useQuery<BankAccountsResponse>({
    queryKey: ['reconcileBanks'],
    queryFn: async () => {
      const response = await apiClient.reconcileBanks();
      return response.data;
    },
    refetchOnWindowFocus: false,
  });

  // Auto-select first bank when loaded
  if (banksQuery.data?.banks?.length && !selectedBank) {
    setSelectedBank(banksQuery.data.banks[0].account_code);
  }

  // Fetch bank reconciliation for selected bank
  const bankQuery = useQuery<BankReconciliationResponse>({
    queryKey: ['reconcileBank', selectedBank],
    queryFn: async () => {
      const response = await apiClient.reconcileBank(selectedBank!);
      return response.data;
    },
    enabled: !!selectedBank,
    refetchOnWindowFocus: false,
  });

  const formatCurrency = (value: number | undefined | null) => {
    if (value === undefined || value === null) return '£0.00';
    return new Intl.NumberFormat('en-GB', {
      style: 'currency',
      currency: 'GBP',
    }).format(value);
  };

  const SectionHeader = ({ title, section, icon: Icon }: { title: string; section: string; icon: React.ComponentType<{ className?: string }> }) => (
    <button
      onClick={() => toggleSection(section)}
      className="w-full flex items-center justify-between p-4 bg-gray-50 hover:bg-gray-100 rounded-lg transition-colors"
    >
      <div className="flex items-center gap-3">
        <Icon className="h-5 w-5 text-green-600" />
        <span className="font-semibold text-gray-900">{title}</span>
      </div>
      {expandedSections.has(section) ? (
        <ChevronDown className="h-5 w-5 text-gray-400" />
      ) : (
        <ChevronRight className="h-5 w-5 text-gray-400" />
      )}
    </button>
  );

  const data = bankQuery.data;
  const isLoading = bankQuery.isLoading || banksQuery.isLoading;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
            <BookOpen className="h-6 w-6 text-green-600" />
            Cashbook Reconciliation
          </h1>
          <p className="text-gray-500 mt-1">Bank/Cashbook balance vs Nominal Ledger control account</p>
        </div>
        <button
          onClick={() => {
            banksQuery.refetch();
            bankQuery.refetch();
          }}
          disabled={isLoading}
          className="flex items-center gap-2 px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50"
        >
          <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </div>

      {/* Bank Account Tabs */}
      <div className="flex flex-wrap gap-2">
        {banksQuery.data?.banks?.map((bank) => (
          <button
            key={bank.account_code}
            onClick={() => setSelectedBank(bank.account_code)}
            className={`px-4 py-2 rounded-lg font-medium transition-colors ${
              selectedBank === bank.account_code
                ? 'bg-green-600 text-white'
                : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
            }`}
            title={bank.description}
          >
            <div className="flex items-center gap-2">
              <Landmark className="h-4 w-4" />
              {bank.account_code}
            </div>
          </button>
        ))}
      </div>

      {/* Selected Bank Description */}
      {selectedBank && banksQuery.data?.banks && (
        <div className="text-sm text-gray-600">
          Selected: <span className="font-medium">{banksQuery.data.banks.find(b => b.account_code === selectedBank)?.description}</span>
        </div>
      )}

      {/* Loading State */}
      {isLoading && (
        <div className="bg-white rounded-lg shadow p-8 text-center">
          <RefreshCw className="h-8 w-8 animate-spin text-green-600 mx-auto mb-4" />
          <p className="text-gray-600">Loading reconciliation data...</p>
        </div>
      )}

      {/* Error State */}
      {bankQuery.error && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4">
          <div className="flex items-center gap-2 text-red-800">
            <XCircle className="h-5 w-5" />
            <span className="font-medium">Error loading data</span>
          </div>
          <p className="text-red-600 mt-1">{(bankQuery.error as Error).message}</p>
        </div>
      )}

      {/* Data Display */}
      {data && !isLoading && (
        <div className="space-y-4">
          {/* Summary Section */}
          <div className="bg-white rounded-lg shadow">
            <SectionHeader title="Summary" section="summary" icon={CheckCircle} />
            {expandedSections.has('summary') && (
              <div className="p-4 border-t">
                <div className="grid grid-cols-3 gap-6">
                  <div className="text-center p-4 bg-green-50 rounded-lg">
                    <p className="text-sm text-gray-600">Cashbook Balance</p>
                    <p className="text-2xl font-bold text-green-700">
                      {formatCurrency(data.cashbook_balance)}
                    </p>
                  </div>
                  <div className="text-center p-4 bg-blue-50 rounded-lg">
                    <p className="text-sm text-gray-600">Nominal Control Balance</p>
                    <p className="text-2xl font-bold text-blue-700">
                      {formatCurrency(data.nominal_balance)}
                    </p>
                  </div>
                  <div className={`text-center p-4 rounded-lg ${
                    Math.abs((data.variance as number) || 0) < 0.01 ? 'bg-green-50' : 'bg-red-50'
                  }`}>
                    <p className="text-sm text-gray-600">Variance</p>
                    <p className={`text-2xl font-bold ${
                      Math.abs((data.variance as number) || 0) < 0.01 ? 'text-green-700' : 'text-red-700'
                    }`}>
                      {formatCurrency(data.variance)}
                    </p>
                  </div>
                </div>

                {/* Status indicator */}
                <div className="mt-4 flex justify-center">
                  {Math.abs((data.variance as number) || 0) < 0.01 ? (
                    <div className="flex items-center gap-2 text-green-700 bg-green-100 px-4 py-2 rounded-full">
                      <CheckCircle className="h-5 w-5" />
                      <span className="font-medium">Reconciled</span>
                    </div>
                  ) : (
                    <div className="flex items-center gap-2 text-red-700 bg-red-100 px-4 py-2 rounded-full">
                      <AlertTriangle className="h-5 w-5" />
                      <span className="font-medium">Variance Detected</span>
                    </div>
                  )}
                </div>

                {/* Additional Info */}
                <div className="mt-6 grid grid-cols-2 gap-4 text-sm">
                  <div className="p-3 bg-gray-50 rounded">
                    <span className="text-gray-500">Bank Account:</span>
                    <span className="ml-2 font-medium">{data.bank_code}</span>
                  </div>
                  <div className="p-3 bg-gray-50 rounded">
                    <span className="text-gray-500">Control Account:</span>
                    <span className="ml-2 font-medium">{data.control_account || 'N/A'}</span>
                  </div>
                  <div className="p-3 bg-gray-50 rounded">
                    <span className="text-gray-500">Last Reconciled:</span>
                    <span className="ml-2 font-medium">{data.last_reconciled_date || 'Never'}</span>
                  </div>
                  <div className="p-3 bg-gray-50 rounded">
                    <span className="text-gray-500">Unreconciled Entries:</span>
                    <span className="ml-2 font-medium">{data.unreconciled_count || 0}</span>
                  </div>
                </div>
              </div>
            )}
          </div>

          {/* Unreconciled Entries */}
          {data.unreconciled_entries && data.unreconciled_entries.length > 0 && (
            <div className="bg-white rounded-lg shadow">
              <SectionHeader title={`Unreconciled Entries (${data.unreconciled_entries.length})`} section="unreconciled" icon={AlertTriangle} />
              {expandedSections.has('unreconciled') && (
                <div className="p-4 border-t">
                  <div className="overflow-x-auto max-h-96 overflow-y-auto">
                    <table className="min-w-full divide-y divide-gray-200">
                      <thead className="bg-gray-50 sticky top-0">
                        <tr>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Date</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Reference</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Details</th>
                          <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Amount</th>
                        </tr>
                      </thead>
                      <tbody className="bg-white divide-y divide-gray-200">
                        {data.unreconciled_entries.map((entry: any, idx: number) => (
                          <tr key={idx} className="hover:bg-gray-50">
                            <td className="px-4 py-3 text-sm">{entry.date}</td>
                            <td className="px-4 py-3 text-sm font-mono">{entry.reference}</td>
                            <td className="px-4 py-3 text-sm">{entry.details}</td>
                            <td className={`px-4 py-3 text-sm text-right font-medium ${
                              entry.amount < 0 ? 'text-red-600' : 'text-green-600'
                            }`}>
                              {formatCurrency(entry.amount)}
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

      {/* No banks message */}
      {!isLoading && (!banksQuery.data?.banks || banksQuery.data.banks.length === 0) && (
        <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-6 text-center">
          <Landmark className="h-8 w-8 text-yellow-500 mx-auto mb-2" />
          <p className="text-yellow-800 font-medium">No bank accounts found</p>
          <p className="text-yellow-600 text-sm mt-1">Configure bank accounts in Opera to use this feature</p>
        </div>
      )}
    </div>
  );
}

export default CashbookReconcile;
