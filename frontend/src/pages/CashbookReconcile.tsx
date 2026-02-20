import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  CheckCircle,
  XCircle,
  AlertTriangle,
  RefreshCw,
  BookOpen,
  Landmark,
  ArrowRightLeft,
  Building2,
  Calculator,
  FileText,
} from 'lucide-react';
import apiClient from '../api/client';
import type { BankAccountsResponse, BankReconciliationResponse } from '../api/client';
import { PageHeader, Card, LoadingState, Alert, SectionHeader, EmptyState } from '../components/ui';

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
      {/* Header */}
      <PageHeader
        icon={BookOpen}
        title="Cashbook Reconciliation"
        subtitle="Bank/Cashbook balance vs Nominal Ledger control account"
      >
        <button
          onClick={() => {
            banksQuery.refetch();
            bankQuery.refetch();
          }}
          disabled={isLoading}
          className="flex items-center gap-2 px-4 py-2 bg-white border border-gray-200 hover:bg-gray-50 rounded-lg transition-colors text-sm font-medium text-gray-700 disabled:opacity-50"
        >
          <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </PageHeader>

      {/* Bank Account Tabs */}
      <div className="flex flex-wrap gap-2">
        {banksQuery.data?.banks?.map((bank) => (
          <button
            key={bank.account_code}
            onClick={() => setSelectedBank(bank.account_code)}
            className={`px-4 py-2 rounded-lg font-medium transition-all duration-200 ${
              selectedBank === bank.account_code
                ? 'bg-blue-600 text-white shadow-lg shadow-blue-200'
                : 'bg-white text-gray-700 hover:bg-gray-50 border border-gray-200 hover:border-blue-300'
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
        <div className="text-sm text-gray-600 bg-white rounded-xl p-3 shadow-sm border border-gray-200">
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
        <Card>
          <LoadingState message="Loading reconciliation data..." />
        </Card>
      )}

      {/* Error State */}
      {bankQuery.error && (
        <Alert variant="error" title="Error loading data">
          {(bankQuery.error as Error).message}
        </Alert>
      )}

      {/* Data Display */}
      {data && !isLoading && (
        <div className="space-y-4">
          {/* Status Banner */}
          {isReconciled ? (
            <Alert variant="success" title={data.status}>
              {data.message && <span>{data.message}</span>}
              <span className="float-right text-xs text-gray-500">As at: {data.reconciliation_date}</span>
            </Alert>
          ) : (
            <Alert variant="warning" title={data.status}>
              {data.message && <span>{data.message}</span>}
              <span className="float-right text-xs text-gray-500">As at: {data.reconciliation_date}</span>
            </Alert>
          )}

          {/* Summary Section - Three Sources */}
          <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
            <SectionHeader
              title="Balance Summary"
              icon={Calculator}
              expanded={expandedSections.has('summary')}
              onToggle={() => toggleSection('summary')}
            />
            {expandedSections.has('summary') && (
              <div className="p-6 border-t border-gray-100">
                <div className="grid grid-cols-3 gap-4">
                  {/* Cashbook Expected */}
                  <div className="text-center p-4 bg-blue-50 rounded-xl border border-blue-100">
                    <div className="flex items-center justify-center gap-2 mb-2">
                      <BookOpen className="h-4 w-4 text-blue-600" />
                      <p className="text-sm font-medium text-blue-700">Cashbook Expected</p>
                    </div>
                    <p className="text-2xl font-bold text-blue-800">
                      {formatCurrency(cashbookExpected)}
                    </p>
                    <p className="text-xs text-blue-600 mt-1">
                      Movements + B/F
                    </p>
                  </div>

                  {/* Bank Master Balance */}
                  <div className="text-center p-4 bg-emerald-50 rounded-xl border border-emerald-100">
                    <div className="flex items-center justify-center gap-2 mb-2">
                      <Landmark className="h-4 w-4 text-emerald-600" />
                      <p className="text-sm font-medium text-emerald-700">Bank Master (nbank)</p>
                    </div>
                    <p className="text-2xl font-bold text-emerald-800">
                      {formatCurrency(bankMasterBalance)}
                    </p>
                    <p className="text-xs text-emerald-600 mt-1">
                      nk_curbal
                    </p>
                  </div>

                  {/* Nominal Ledger Balance */}
                  <div className="text-center p-4 bg-gray-50 rounded-xl border border-gray-200">
                    <div className="flex items-center justify-center gap-2 mb-2">
                      <Building2 className="h-4 w-4 text-gray-600" />
                      <p className="text-sm font-medium text-gray-700">Nominal Ledger</p>
                    </div>
                    <p className="text-2xl font-bold text-gray-800">
                      {formatCurrency(nominalBalance)}
                    </p>
                    <p className="text-xs text-gray-600 mt-1">
                      {data.nominal_ledger?.account || data.bank_code}
                    </p>
                  </div>
                </div>

                {/* Additional Cashbook Details */}
                {data.cashbook && (
                  <div className="mt-4 grid grid-cols-4 gap-3 text-sm">
                    <div className="p-3 bg-gray-50 rounded-xl">
                      <span className="text-gray-500">Current Year:</span>
                      <span className="ml-2 font-medium">{data.cashbook.current_year}</span>
                    </div>
                    <div className="p-3 bg-gray-50 rounded-xl">
                      <span className="text-gray-500">Entries:</span>
                      <span className="ml-2 font-medium">{data.cashbook.current_year_entries || 0}</span>
                    </div>
                    <div className="p-3 bg-emerald-50 rounded-xl">
                      <span className="text-gray-500">Receipts:</span>
                      <span className="ml-2 font-medium text-emerald-700">{formatCurrency(data.cashbook.current_year_receipts)}</span>
                    </div>
                    <div className="p-3 bg-red-50 rounded-xl">
                      <span className="text-gray-500">Payments:</span>
                      <span className="ml-2 font-medium text-red-700">{formatCurrency(data.cashbook.current_year_payments)}</span>
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>

          {/* Variance Details Section */}
          <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
            <SectionHeader
              title="Variance Analysis"
              icon={ArrowRightLeft}
              expanded={expandedSections.has('variance')}
              onToggle={() => toggleSection('variance')}
            />
            {expandedSections.has('variance') && data.variance && (
              <div className="p-6 border-t border-gray-100 space-y-4">
                {/* Cashbook vs Bank Master */}
                {data.variance.cashbook_vs_bank_master && (
                  <div className={`p-4 rounded-xl border ${
                    data.variance.cashbook_vs_bank_master.reconciled
                      ? 'bg-emerald-50 border-emerald-200'
                      : 'bg-red-50 border-red-200'
                  }`}>
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        {data.variance.cashbook_vs_bank_master.reconciled ? (
                          <CheckCircle className="h-5 w-5 text-emerald-600" />
                        ) : (
                          <XCircle className="h-5 w-5 text-red-600" />
                        )}
                        <span className="font-medium text-sm">Cashbook vs Bank Master</span>
                      </div>
                      <span className={`text-lg font-bold ${
                        data.variance.cashbook_vs_bank_master.reconciled ? 'text-emerald-700' : 'text-red-700'
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
                  <div className={`p-4 rounded-xl border ${
                    data.variance.bank_master_vs_nominal.reconciled
                      ? 'bg-emerald-50 border-emerald-200'
                      : 'bg-red-50 border-red-200'
                  }`}>
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        {data.variance.bank_master_vs_nominal.reconciled ? (
                          <CheckCircle className="h-5 w-5 text-emerald-600" />
                        ) : (
                          <XCircle className="h-5 w-5 text-red-600" />
                        )}
                        <span className="font-medium text-sm">Bank Master vs Nominal Ledger</span>
                      </div>
                      <span className={`text-lg font-bold ${
                        data.variance.bank_master_vs_nominal.reconciled ? 'text-emerald-700' : 'text-red-700'
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
                  <div className={`p-4 rounded-xl border ${
                    data.variance.cashbook_vs_nominal.reconciled
                      ? 'bg-emerald-50 border-emerald-200'
                      : 'bg-red-50 border-red-200'
                  }`}>
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        {data.variance.cashbook_vs_nominal.reconciled ? (
                          <CheckCircle className="h-5 w-5 text-emerald-600" />
                        ) : (
                          <XCircle className="h-5 w-5 text-red-600" />
                        )}
                        <span className="font-medium text-sm">Cashbook vs Nominal Ledger</span>
                      </div>
                      <span className={`text-lg font-bold ${
                        data.variance.cashbook_vs_nominal.reconciled ? 'text-emerald-700' : 'text-red-700'
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
                  <div className="p-4 bg-gray-50 rounded-xl text-sm">
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
            <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
              <SectionHeader
                title="Pending Transfer to Nominal"
                icon={FileText}
                badge={pendingCount}
                badgeVariant="warning"
                expanded={expandedSections.has('pending')}
                onToggle={() => toggleSection('pending')}
              />
              {expandedSections.has('pending') && data.cashbook?.transfer_file?.pending_transfer && (
                <div className="p-6 border-t border-gray-100">
                  <Alert variant="warning" className="mb-3">
                    {pendingCount} transaction(s) totalling {formatCurrency(pendingTotal)} pending transfer to Nominal Ledger
                  </Alert>
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
                              txn.value < 0 ? 'text-red-600' : 'text-emerald-600'
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
            <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
              <SectionHeader
                title="Nominal Ledger Details"
                icon={Building2}
                expanded={expandedSections.has('nominal')}
                onToggle={() => toggleSection('nominal')}
              />
              {expandedSections.has('nominal') && (
                <div className="p-6 border-t border-gray-100">
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                    <div className="p-3 bg-gray-50 rounded-xl">
                      <span className="text-gray-500">Account:</span>
                      <span className="ml-2 font-medium">{data.nominal_ledger.account}</span>
                    </div>
                    <div className="p-3 bg-gray-50 rounded-xl">
                      <span className="text-gray-500">Description:</span>
                      <span className="ml-2 font-medium">{data.nominal_ledger.description}</span>
                    </div>
                    <div className="p-3 bg-gray-50 rounded-xl">
                      <span className="text-gray-500">Current Year:</span>
                      <span className="ml-2 font-medium">{data.nominal_ledger.current_year}</span>
                    </div>
                    <div className="p-3 bg-gray-50 rounded-xl">
                      <span className="text-gray-500">B/F Balance:</span>
                      <span className="ml-2 font-medium">{formatCurrency(data.nominal_ledger.brought_forward)}</span>
                    </div>
                    <div className="p-3 bg-emerald-50 rounded-xl">
                      <span className="text-gray-500">YTD Debits:</span>
                      <span className="ml-2 font-medium text-emerald-700">{formatCurrency(data.nominal_ledger.current_year_debits)}</span>
                    </div>
                    <div className="p-3 bg-red-50 rounded-xl">
                      <span className="text-gray-500">YTD Credits:</span>
                      <span className="ml-2 font-medium text-red-700">{formatCurrency(data.nominal_ledger.current_year_credits)}</span>
                    </div>
                    <div className="p-3 bg-blue-50 rounded-xl">
                      <span className="text-gray-500">YTD Net:</span>
                      <span className="ml-2 font-medium text-blue-700">{formatCurrency(data.nominal_ledger.current_year_net)}</span>
                    </div>
                    <div className="p-3 bg-gray-50 rounded-xl">
                      <span className="text-gray-500">Closing Balance:</span>
                      <span className="ml-2 font-medium text-gray-700">{formatCurrency(data.nominal_ledger.closing_balance)}</span>
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
        <Card>
          <EmptyState
            icon={Landmark}
            title="No bank accounts found"
            message="Configure bank accounts in Opera to use this feature"
          />
        </Card>
      )}
    </div>
  );
}

export default CashbookReconcile;
