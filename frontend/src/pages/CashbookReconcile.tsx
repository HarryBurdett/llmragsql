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
  ArrowRightLeft,
  Building2,
  Calculator,
  FileText,
} from 'lucide-react';
import apiClient from '../api/client';
import type { BankAccountsResponse, BankReconciliationResponse } from '../api/client';

export function CashbookReconcile() {
  const [selectedBank, setSelectedBank] = useState<string | null>(null);
  const [expandedSections, setExpandedSections] = useState<Set<string>>(new Set(['summary', 'variance']));

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

  const SectionHeader = ({ title, section, icon: Icon, badge }: { title: string; section: string; icon: React.ComponentType<{ className?: string }>; badge?: string | number }) => (
    <button
      onClick={() => toggleSection(section)}
      className="w-full flex items-center justify-between p-4 bg-gray-50 hover:bg-gray-100 rounded-lg transition-colors"
    >
      <div className="flex items-center gap-3">
        <Icon className="h-5 w-5 text-teal-600" />
        <span className="font-semibold text-gray-900">{title}</span>
        {badge !== undefined && (
          <span className="px-2 py-0.5 text-xs font-medium bg-teal-100 text-teal-700 rounded-full">
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

  const data = bankQuery.data;
  const isLoading = bankQuery.isLoading || banksQuery.isLoading;

  // Extract values from nested API response
  const bankMasterBalance = data?.bank_master?.balance_pounds || 0;
  const cashbookExpected = data?.cashbook?.expected_closing || 0;
  const nominalBalance = data?.nominal_ledger?.total_balance || 0;
  const isReconciled = data?.status === 'RECONCILED';
  const pendingCount = data?.cashbook?.transfer_file?.pending_transfer?.count || 0;
  const pendingTotal = data?.cashbook?.transfer_file?.pending_transfer?.total || 0;

  return (
    <div className="space-y-6">
      {/* Header with gradient */}
      <div className="bg-gradient-to-r from-teal-600 to-emerald-600 rounded-xl shadow-lg p-6 text-white">
        <div className="flex justify-between items-center">
          <div>
            <h1 className="text-2xl font-bold flex items-center gap-3">
              <div className="p-2 bg-white/20 rounded-lg backdrop-blur-sm">
                <BookOpen className="h-6 w-6" />
              </div>
              Cashbook Reconciliation
            </h1>
            <p className="text-teal-100 mt-2">Bank/Cashbook balance vs Nominal Ledger control account</p>
          </div>
          <button
            onClick={() => {
              banksQuery.refetch();
              bankQuery.refetch();
            }}
            disabled={isLoading}
            className="flex items-center gap-2 px-4 py-2 bg-white/20 hover:bg-white/30 backdrop-blur-sm rounded-lg transition-colors disabled:opacity-50"
          >
            <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
            Refresh
          </button>
        </div>
      </div>

      {/* Bank Account Tabs */}
      <div className="flex flex-wrap gap-2">
        {banksQuery.data?.banks?.map((bank) => (
          <button
            key={bank.account_code}
            onClick={() => setSelectedBank(bank.account_code)}
            className={`px-4 py-2 rounded-lg font-medium transition-all duration-200 ${
              selectedBank === bank.account_code
                ? 'bg-teal-600 text-white shadow-lg shadow-teal-200'
                : 'bg-white text-gray-700 hover:bg-gray-50 border border-gray-200 hover:border-teal-300'
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
        <div className="text-sm text-gray-600 bg-white rounded-lg p-3 shadow-sm border border-gray-100">
          Selected: <span className="font-medium text-gray-900">{banksQuery.data.banks.find(b => b.account_code === selectedBank)?.description}</span>
          {data?.bank_account && (
            <span className="ml-4 text-gray-500">
              Sort: {data.bank_account.sort_code || 'N/A'} | Account: {data.bank_account.account_number || 'N/A'}
            </span>
          )}
        </div>
      )}

      {/* Loading State */}
      {isLoading && (
        <div className="bg-white rounded-lg shadow p-8 text-center">
          <RefreshCw className="h-8 w-8 animate-spin text-teal-600 mx-auto mb-4" />
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

          {/* Summary Section - Three Sources */}
          <div className="bg-white rounded-lg shadow">
            <SectionHeader title="Balance Summary" section="summary" icon={Calculator} />
            {expandedSections.has('summary') && (
              <div className="p-4 border-t">
                <div className="grid grid-cols-3 gap-4">
                  {/* Cashbook Expected */}
                  <div className="text-center p-4 bg-teal-50 rounded-lg border border-teal-100">
                    <div className="flex items-center justify-center gap-2 mb-2">
                      <BookOpen className="h-4 w-4 text-teal-600" />
                      <p className="text-sm font-medium text-teal-700">Cashbook Expected</p>
                    </div>
                    <p className="text-2xl font-bold text-teal-800">
                      {formatCurrency(cashbookExpected)}
                    </p>
                    <p className="text-xs text-teal-600 mt-1">
                      Movements + B/F
                    </p>
                  </div>

                  {/* Bank Master Balance */}
                  <div className="text-center p-4 bg-blue-50 rounded-lg border border-blue-100">
                    <div className="flex items-center justify-center gap-2 mb-2">
                      <Landmark className="h-4 w-4 text-blue-600" />
                      <p className="text-sm font-medium text-blue-700">Bank Master (nbank)</p>
                    </div>
                    <p className="text-2xl font-bold text-blue-800">
                      {formatCurrency(bankMasterBalance)}
                    </p>
                    <p className="text-xs text-blue-600 mt-1">
                      nk_curbal
                    </p>
                  </div>

                  {/* Nominal Ledger Balance */}
                  <div className="text-center p-4 bg-purple-50 rounded-lg border border-purple-100">
                    <div className="flex items-center justify-center gap-2 mb-2">
                      <Building2 className="h-4 w-4 text-purple-600" />
                      <p className="text-sm font-medium text-purple-700">Nominal Ledger</p>
                    </div>
                    <p className="text-2xl font-bold text-purple-800">
                      {formatCurrency(nominalBalance)}
                    </p>
                    <p className="text-xs text-purple-600 mt-1">
                      {data.nominal_ledger?.account || data.bank_code}
                    </p>
                  </div>
                </div>

                {/* Additional Cashbook Details */}
                {data.cashbook && (
                  <div className="mt-4 grid grid-cols-4 gap-3 text-sm">
                    <div className="p-3 bg-gray-50 rounded-lg">
                      <span className="text-gray-500">Current Year:</span>
                      <span className="ml-2 font-medium">{data.cashbook.current_year}</span>
                    </div>
                    <div className="p-3 bg-gray-50 rounded-lg">
                      <span className="text-gray-500">Entries:</span>
                      <span className="ml-2 font-medium">{data.cashbook.current_year_entries || 0}</span>
                    </div>
                    <div className="p-3 bg-green-50 rounded-lg">
                      <span className="text-gray-500">Receipts:</span>
                      <span className="ml-2 font-medium text-green-700">{formatCurrency(data.cashbook.current_year_receipts)}</span>
                    </div>
                    <div className="p-3 bg-red-50 rounded-lg">
                      <span className="text-gray-500">Payments:</span>
                      <span className="ml-2 font-medium text-red-700">{formatCurrency(data.cashbook.current_year_payments)}</span>
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>

          {/* Variance Details Section */}
          <div className="bg-white rounded-lg shadow">
            <SectionHeader title="Variance Analysis" section="variance" icon={ArrowRightLeft} />
            {expandedSections.has('variance') && data.variance && (
              <div className="p-4 border-t space-y-4">
                {/* Cashbook vs Bank Master */}
                {data.variance.cashbook_vs_bank_master && (
                  <div className={`p-4 rounded-lg border ${
                    data.variance.cashbook_vs_bank_master.reconciled
                      ? 'bg-green-50 border-green-200'
                      : 'bg-red-50 border-red-200'
                  }`}>
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        {data.variance.cashbook_vs_bank_master.reconciled ? (
                          <CheckCircle className="h-5 w-5 text-green-600" />
                        ) : (
                          <XCircle className="h-5 w-5 text-red-600" />
                        )}
                        <span className="font-medium">Cashbook vs Bank Master</span>
                      </div>
                      <span className={`text-lg font-bold ${
                        data.variance.cashbook_vs_bank_master.reconciled ? 'text-green-700' : 'text-red-700'
                      }`}>
                        {formatCurrency(data.variance.cashbook_vs_bank_master.amount)}
                      </span>
                    </div>
                    <div className="mt-2 grid grid-cols-2 gap-4 text-sm">
                      <div>
                        <span className="text-gray-500">Cashbook Expected:</span>
                        <span className="ml-2 font-medium">{formatCurrency(data.variance.cashbook_vs_bank_master.cashbook_expected)}</span>
                      </div>
                      <div>
                        <span className="text-gray-500">Bank Master:</span>
                        <span className="ml-2 font-medium">{formatCurrency(data.variance.cashbook_vs_bank_master.bank_master)}</span>
                      </div>
                    </div>
                  </div>
                )}

                {/* Bank Master vs Nominal */}
                {data.variance.bank_master_vs_nominal && (
                  <div className={`p-4 rounded-lg border ${
                    data.variance.bank_master_vs_nominal.reconciled
                      ? 'bg-green-50 border-green-200'
                      : 'bg-red-50 border-red-200'
                  }`}>
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        {data.variance.bank_master_vs_nominal.reconciled ? (
                          <CheckCircle className="h-5 w-5 text-green-600" />
                        ) : (
                          <XCircle className="h-5 w-5 text-red-600" />
                        )}
                        <span className="font-medium">Bank Master vs Nominal Ledger</span>
                      </div>
                      <span className={`text-lg font-bold ${
                        data.variance.bank_master_vs_nominal.reconciled ? 'text-green-700' : 'text-red-700'
                      }`}>
                        {formatCurrency(data.variance.bank_master_vs_nominal.amount)}
                      </span>
                    </div>
                    <div className="mt-2 grid grid-cols-2 gap-4 text-sm">
                      <div>
                        <span className="text-gray-500">Bank Master:</span>
                        <span className="ml-2 font-medium">{formatCurrency(data.variance.bank_master_vs_nominal.bank_master)}</span>
                      </div>
                      <div>
                        <span className="text-gray-500">Nominal Ledger:</span>
                        <span className="ml-2 font-medium">{formatCurrency(data.variance.bank_master_vs_nominal.nominal_ledger)}</span>
                      </div>
                    </div>
                  </div>
                )}

                {/* Cashbook vs Nominal */}
                {data.variance.cashbook_vs_nominal && (
                  <div className={`p-4 rounded-lg border ${
                    data.variance.cashbook_vs_nominal.reconciled
                      ? 'bg-green-50 border-green-200'
                      : 'bg-red-50 border-red-200'
                  }`}>
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        {data.variance.cashbook_vs_nominal.reconciled ? (
                          <CheckCircle className="h-5 w-5 text-green-600" />
                        ) : (
                          <XCircle className="h-5 w-5 text-red-600" />
                        )}
                        <span className="font-medium">Cashbook vs Nominal Ledger</span>
                      </div>
                      <span className={`text-lg font-bold ${
                        data.variance.cashbook_vs_nominal.reconciled ? 'text-green-700' : 'text-red-700'
                      }`}>
                        {formatCurrency(data.variance.cashbook_vs_nominal.amount)}
                      </span>
                    </div>
                    <div className="mt-2 grid grid-cols-2 gap-4 text-sm">
                      <div>
                        <span className="text-gray-500">Cashbook Expected:</span>
                        <span className="ml-2 font-medium">{formatCurrency(data.variance.cashbook_vs_nominal.cashbook_expected)}</span>
                      </div>
                      <div>
                        <span className="text-gray-500">Nominal Ledger:</span>
                        <span className="ml-2 font-medium">{formatCurrency(data.variance.cashbook_vs_nominal.nominal_ledger)}</span>
                      </div>
                    </div>
                  </div>
                )}

                {/* Summary section */}
                {data.variance.summary && (
                  <div className="p-4 bg-gray-50 rounded-lg text-sm">
                    <h4 className="font-medium text-gray-700 mb-2">Reconciliation Summary</h4>
                    <div className="grid grid-cols-3 gap-4">
                      <div>
                        <span className="text-gray-500">CB Movements:</span>
                        <span className="ml-2 font-medium">{formatCurrency(data.variance.summary.cashbook_movements)}</span>
                      </div>
                      <div>
                        <span className="text-gray-500">Prior Year B/F:</span>
                        <span className="ml-2 font-medium">{formatCurrency(data.variance.summary.prior_year_bf)}</span>
                      </div>
                      <div>
                        <span className="text-gray-500">Transfer Pending:</span>
                        <span className="ml-2 font-medium">{formatCurrency(data.variance.summary.transfer_file_pending)}</span>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>

          {/* Transfer File (Pending Transactions) */}
          {pendingCount > 0 && (
            <div className="bg-white rounded-lg shadow">
              <SectionHeader
                title="Pending Transfer to Nominal"
                section="pending"
                icon={FileText}
                badge={pendingCount}
              />
              {expandedSections.has('pending') && data.cashbook?.transfer_file?.pending_transfer && (
                <div className="p-4 border-t">
                  <div className="mb-3 p-3 bg-amber-50 border border-amber-200 rounded-lg flex items-center gap-2">
                    <AlertTriangle className="h-4 w-4 text-amber-600" />
                    <span className="text-sm text-amber-800">
                      {pendingCount} transaction(s) totalling {formatCurrency(pendingTotal)} pending transfer to Nominal Ledger
                    </span>
                  </div>
                  <div className="overflow-x-auto max-h-72 overflow-y-auto">
                    <table className="min-w-full divide-y divide-gray-200">
                      <thead className="bg-gray-50 sticky top-0">
                        <tr>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Date</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Source</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Reference</th>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Comment</th>
                          <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Amount</th>
                        </tr>
                      </thead>
                      <tbody className="bg-white divide-y divide-gray-200">
                        {data.cashbook.transfer_file.pending_transfer.transactions.map((txn, idx) => (
                          <tr key={idx} className="hover:bg-gray-50">
                            <td className="px-4 py-3 text-sm whitespace-nowrap">{txn.date}</td>
                            <td className="px-4 py-3 text-sm">
                              <span className="px-2 py-1 bg-gray-100 rounded text-xs font-medium">
                                {txn.source_desc || txn.source}
                              </span>
                            </td>
                            <td className="px-4 py-3 text-sm font-mono">{txn.reference}</td>
                            <td className="px-4 py-3 text-sm text-gray-600">{txn.comment}</td>
                            <td className={`px-4 py-3 text-sm text-right font-medium ${
                              txn.value < 0 ? 'text-red-600' : 'text-green-600'
                            }`}>
                              {formatCurrency(txn.value)}
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

          {/* Nominal Ledger Details */}
          {data.nominal_ledger && (
            <div className="bg-white rounded-lg shadow">
              <SectionHeader title="Nominal Ledger Details" section="nominal" icon={Building2} />
              {expandedSections.has('nominal') && (
                <div className="p-4 border-t">
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                    <div className="p-3 bg-gray-50 rounded-lg">
                      <span className="text-gray-500">Account:</span>
                      <span className="ml-2 font-medium">{data.nominal_ledger.account}</span>
                    </div>
                    <div className="p-3 bg-gray-50 rounded-lg">
                      <span className="text-gray-500">Description:</span>
                      <span className="ml-2 font-medium">{data.nominal_ledger.description}</span>
                    </div>
                    <div className="p-3 bg-gray-50 rounded-lg">
                      <span className="text-gray-500">Current Year:</span>
                      <span className="ml-2 font-medium">{data.nominal_ledger.current_year}</span>
                    </div>
                    <div className="p-3 bg-gray-50 rounded-lg">
                      <span className="text-gray-500">B/F Balance:</span>
                      <span className="ml-2 font-medium">{formatCurrency(data.nominal_ledger.brought_forward)}</span>
                    </div>
                    <div className="p-3 bg-green-50 rounded-lg">
                      <span className="text-gray-500">YTD Debits:</span>
                      <span className="ml-2 font-medium text-green-700">{formatCurrency(data.nominal_ledger.current_year_debits)}</span>
                    </div>
                    <div className="p-3 bg-red-50 rounded-lg">
                      <span className="text-gray-500">YTD Credits:</span>
                      <span className="ml-2 font-medium text-red-700">{formatCurrency(data.nominal_ledger.current_year_credits)}</span>
                    </div>
                    <div className="p-3 bg-blue-50 rounded-lg">
                      <span className="text-gray-500">YTD Net:</span>
                      <span className="ml-2 font-medium text-blue-700">{formatCurrency(data.nominal_ledger.current_year_net)}</span>
                    </div>
                    <div className="p-3 bg-purple-50 rounded-lg">
                      <span className="text-gray-500">Closing Balance:</span>
                      <span className="ml-2 font-medium text-purple-700">{formatCurrency(data.nominal_ledger.closing_balance)}</span>
                    </div>
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
