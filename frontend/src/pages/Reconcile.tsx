import { useState, useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  CheckCircle,
  XCircle,
  AlertTriangle,
  RefreshCw,
  DollarSign,
  FileText,
  Users,
  Building,
  ChevronDown,
  ChevronRight,
  Landmark,
} from 'lucide-react';
import apiClient from '../api/client';
import type { ReconciliationResponse, BankAccountsResponse, BankReconciliationResponse } from '../api/client';

type ReconciliationType = 'creditors' | 'debtors' | string; // string for bank codes

export function Reconcile() {
  const [reconciliationType, setReconciliationType] = useState<ReconciliationType>('creditors');
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

  // Fetch creditors reconciliation
  const creditorsQuery = useQuery<ReconciliationResponse>({
    queryKey: ['reconcileCreditors'],
    queryFn: async () => {
      const response = await apiClient.reconcileCreditors();
      return response.data;
    },
    enabled: reconciliationType === 'creditors',
    refetchOnWindowFocus: false,
  });

  // Fetch debtors reconciliation
  const debtorsQuery = useQuery<ReconciliationResponse>({
    queryKey: ['reconcileDebtors'],
    queryFn: async () => {
      const response = await apiClient.reconcileDebtors();
      return response.data;
    },
    enabled: reconciliationType === 'debtors',
    refetchOnWindowFocus: false,
  });

  // Fetch bank reconciliation (for bank codes)
  const isBankCode = reconciliationType !== 'creditors' && reconciliationType !== 'debtors';
  const bankQuery = useQuery<BankReconciliationResponse>({
    queryKey: ['reconcileBank', reconciliationType],
    queryFn: async () => {
      const response = await apiClient.reconcileBank(reconciliationType);
      return response.data;
    },
    enabled: isBankCode,
    refetchOnWindowFocus: false,
  });

  const getCurrentQuery = () => {
    if (reconciliationType === 'creditors') return creditorsQuery;
    if (reconciliationType === 'debtors') return debtorsQuery;
    return bankQuery;
  };

  const currentQuery = getCurrentQuery();

  const formatCurrency = (value: number | undefined) => {
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
        <Icon className="h-5 w-5 text-gray-600" />
        <span className="font-semibold text-gray-800">{title}</span>
      </div>
      {expandedSections.has(section) ? (
        <ChevronDown className="h-5 w-5 text-gray-500" />
      ) : (
        <ChevronRight className="h-5 w-5 text-gray-500" />
      )}
    </button>
  );

  // Get data based on current type
  const ledgerData = reconciliationType === 'creditors' ? creditorsQuery.data :
                     reconciliationType === 'debtors' ? debtorsQuery.data : null;
  const bankData = isBankCode ? bankQuery.data : null;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Ledger Reconciliation</h1>
          <p className="text-gray-600 mt-1">
            Compare sub-ledger balances with nominal ledger control accounts
          </p>
        </div>
        <button
          onClick={() => currentQuery.refetch()}
          disabled={currentQuery.isLoading}
          className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50"
        >
          <RefreshCw className={`h-4 w-4 ${currentQuery.isLoading ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </div>

      {/* Reconciliation Type Toggle */}
      <div className="flex flex-wrap gap-2">
        <button
          onClick={() => setReconciliationType('creditors')}
          className={`px-4 py-2 rounded-lg font-medium transition-colors ${
            reconciliationType === 'creditors'
              ? 'bg-blue-600 text-white'
              : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
          }`}
        >
          <div className="flex items-center gap-2">
            <Building className="h-4 w-4" />
            Creditors
          </div>
        </button>
        <button
          onClick={() => setReconciliationType('debtors')}
          className={`px-4 py-2 rounded-lg font-medium transition-colors ${
            reconciliationType === 'debtors'
              ? 'bg-blue-600 text-white'
              : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
          }`}
        >
          <div className="flex items-center gap-2">
            <Users className="h-4 w-4" />
            Debtors
          </div>
        </button>

        {/* Bank Account Tabs */}
        {banksQuery.data?.banks?.map((bank) => (
          <button
            key={bank.account_code}
            onClick={() => setReconciliationType(bank.account_code)}
            className={`px-4 py-2 rounded-lg font-medium transition-colors ${
              reconciliationType === bank.account_code
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

      {/* Loading State */}
      {currentQuery.isLoading && (
        <div className="flex items-center justify-center py-12">
          <RefreshCw className="h-8 w-8 animate-spin text-blue-600" />
          <span className="ml-3 text-gray-600">Running reconciliation...</span>
        </div>
      )}

      {/* Error State */}
      {currentQuery.isError && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4">
          <div className="flex items-center gap-2 text-red-700">
            <XCircle className="h-5 w-5" />
            <span>Failed to load reconciliation data</span>
          </div>
        </div>
      )}

      {/* Creditors/Debtors Results */}
      {ledgerData && !currentQuery.isLoading && !isBankCode && (
        <LedgerReconciliationView
          data={ledgerData}
          reconciliationType={reconciliationType as 'creditors' | 'debtors'}
          expandedSections={expandedSections}
          toggleSection={toggleSection}
          formatCurrency={formatCurrency}
          SectionHeader={SectionHeader}
        />
      )}

      {/* Bank Reconciliation Results */}
      {bankData && !currentQuery.isLoading && isBankCode && (
        <BankReconciliationView
          data={bankData}
          expandedSections={expandedSections}
          toggleSection={toggleSection}
          formatCurrency={formatCurrency}
          SectionHeader={SectionHeader}
        />
      )}
    </div>
  );
}

// Ledger Reconciliation View (Creditors/Debtors)
function LedgerReconciliationView({
  data,
  reconciliationType,
  expandedSections,
  toggleSection,
  formatCurrency,
  SectionHeader
}: {
  data: ReconciliationResponse;
  reconciliationType: 'creditors' | 'debtors';
  expandedSections: Set<string>;
  toggleSection: (section: string) => void;
  formatCurrency: (value: number | undefined) => string;
  SectionHeader: React.ComponentType<{ title: string; section: string; icon: React.ComponentType<{ className?: string }> }>;
}) {
  return (
    <div className="space-y-4">
      {/* Status Banner */}
      <div
        className={`p-6 rounded-lg border-2 ${
          data.status === 'RECONCILED'
            ? 'bg-green-50 border-green-300'
            : 'bg-red-50 border-red-300'
        }`}
      >
        <div className="flex items-center gap-4">
          {data.status === 'RECONCILED' ? (
            <CheckCircle className="h-12 w-12 text-green-600" />
          ) : (
            <XCircle className="h-12 w-12 text-red-600" />
          )}
          <div>
            <h2 className={`text-2xl font-bold ${data.status === 'RECONCILED' ? 'text-green-800' : 'text-red-800'}`}>
              {data.status}
            </h2>
            <p className={`text-lg ${data.status === 'RECONCILED' ? 'text-green-700' : 'text-red-700'}`}>
              {data.message}
            </p>
            <p className="text-sm text-gray-600 mt-1">
              As at: {data.reconciliation_date}
            </p>
          </div>
        </div>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        {/* Sub-Ledger Total */}
        <div className="bg-white rounded-lg shadow p-6">
          <div className="flex items-center gap-3 mb-2">
            <FileText className="h-5 w-5 text-blue-600" />
            <span className="text-sm text-gray-600">
              {reconciliationType === 'creditors' ? 'Purchase Ledger' : 'Sales Ledger'}
            </span>
          </div>
          <p className="text-2xl font-bold text-gray-900">
            {formatCurrency(
              reconciliationType === 'creditors'
                ? data.purchase_ledger?.total_outstanding
                : data.sales_ledger?.total_outstanding
            )}
          </p>
          <p className="text-sm text-gray-500 mt-1">
            {reconciliationType === 'creditors'
              ? `${data.purchase_ledger?.transaction_count || 0} transactions`
              : `${data.sales_ledger?.transaction_count || 0} transactions`}
          </p>
        </div>

        {/* Posted to NL (from transfer file) */}
        <div className="bg-white rounded-lg shadow p-6 border-l-4 border-green-500">
          <div className="flex items-center gap-3 mb-2">
            <CheckCircle className="h-5 w-5 text-green-600" />
            <span className="text-sm text-gray-600">Posted to NL</span>
          </div>
          <p className="text-2xl font-bold text-green-700">
            {formatCurrency(
              reconciliationType === 'creditors'
                ? (data.purchase_ledger as any)?.transfer_file?.posted_to_nl?.total
                : (data.sales_ledger as any)?.transfer_file?.posted_to_nl?.total
            )}
          </p>
          <p className="text-sm text-gray-500 mt-1">
            {reconciliationType === 'creditors'
              ? `${(data.purchase_ledger as any)?.transfer_file?.posted_to_nl?.count || 0} transactions`
              : `${(data.sales_ledger as any)?.transfer_file?.posted_to_nl?.count || 0} transactions`}
          </p>
        </div>

        {/* Pending in Transfer File */}
        {data.variance?.has_pending_transfers && (
          <div className="bg-amber-50 rounded-lg shadow p-6 border-l-4 border-amber-500">
            <div className="flex items-center gap-3 mb-2">
              <AlertTriangle className="h-5 w-5 text-amber-600" />
              <span className="text-sm text-gray-600">Pending in Transfer File</span>
            </div>
            <p className="text-2xl font-bold text-amber-700">
              {formatCurrency(
                reconciliationType === 'creditors'
                  ? (data.purchase_ledger as any)?.transfer_file?.pending_transfer?.total
                  : (data.sales_ledger as any)?.transfer_file?.pending_transfer?.total
              )}
            </p>
            <p className="text-sm text-amber-600 mt-1">
              {reconciliationType === 'creditors'
                ? `${(data.purchase_ledger as any)?.transfer_file?.pending_transfer?.count || 0} awaiting posting`
                : `${(data.sales_ledger as any)?.transfer_file?.pending_transfer?.count || 0} awaiting posting`}
            </p>
          </div>
        )}

        {/* Nominal Ledger Total */}
        <div className="bg-white rounded-lg shadow p-6">
          <div className="flex items-center gap-3 mb-2">
            <Building className="h-5 w-5 text-purple-600" />
            <span className="text-sm text-gray-600">Nominal Ledger Control</span>
          </div>
          <p className="text-2xl font-bold text-gray-900">
            {formatCurrency(data.nominal_ledger?.total_balance)}
          </p>
          <p className="text-sm text-gray-500 mt-1">
            {data.nominal_ledger?.control_accounts?.length || 0} control account(s)
          </p>
        </div>

        {/* Variance */}
        <div className={`rounded-lg shadow p-6 ${data.variance?.reconciled ? 'bg-green-50' : 'bg-red-50'}`}>
          <div className="flex items-center gap-3 mb-2">
            <AlertTriangle className={`h-5 w-5 ${data.variance?.reconciled ? 'text-green-600' : 'text-red-600'}`} />
            <span className="text-sm text-gray-600">Variance (Posted vs NL)</span>
          </div>
          <p className={`text-2xl font-bold ${data.variance?.reconciled ? 'text-green-700' : 'text-red-700'}`}>
            {formatCurrency(data.variance?.absolute)}
          </p>
          <p className="text-sm text-gray-500 mt-1">
            {data.variance?.reconciled ? 'Balanced' : 'Out of balance'}
          </p>
        </div>
      </div>

      {/* Sub-Ledger Details */}
      <div className="bg-white rounded-lg shadow overflow-hidden">
        <SectionHeader
          title={reconciliationType === 'creditors' ? 'Purchase Ledger Details' : 'Sales Ledger Details'}
          section="subledger"
          icon={FileText}
        />
        {expandedSections.has('subledger') && (
          <div className="p-4 space-y-4">
            {/* Breakdown by Type */}
            <div>
              <h4 className="font-medium text-gray-700 mb-2">Breakdown by Transaction Type</h4>
              <table className="w-full text-sm">
                <thead>
                  <tr className="bg-gray-50">
                    <th className="text-left p-2">Type</th>
                    <th className="text-left p-2">Description</th>
                    <th className="text-right p-2">Count</th>
                    <th className="text-right p-2">Total</th>
                  </tr>
                </thead>
                <tbody>
                  {(reconciliationType === 'creditors'
                    ? data.purchase_ledger?.breakdown_by_type
                    : data.sales_ledger?.breakdown_by_type
                  )?.map((item, idx) => (
                    <tr key={idx} className="border-t">
                      <td className="p-2 font-mono">{item.type}</td>
                      <td className="p-2">{item.description}</td>
                      <td className="p-2 text-right">{item.count}</td>
                      <td className="p-2 text-right font-medium">{formatCurrency(item.total)}</td>
                    </tr>
                  ))}
                </tbody>
                <tfoot>
                  <tr className="border-t-2 font-bold bg-gray-50">
                    <td className="p-2" colSpan={2}>Total</td>
                    <td className="p-2 text-right">
                      {reconciliationType === 'creditors'
                        ? data.purchase_ledger?.transaction_count
                        : data.sales_ledger?.transaction_count}
                    </td>
                    <td className="p-2 text-right">
                      {formatCurrency(
                        reconciliationType === 'creditors'
                          ? data.purchase_ledger?.total_outstanding
                          : data.sales_ledger?.total_outstanding
                      )}
                    </td>
                  </tr>
                </tfoot>
              </table>
            </div>

            {/* Transfer File Status */}
            {data.variance?.has_pending_transfers && (
              <div className="bg-amber-50 p-4 rounded-lg border border-amber-200">
                <h4 className="font-medium text-amber-800 mb-2 flex items-center gap-2">
                  <AlertTriangle className="h-4 w-4" />
                  Transfer File Status ({reconciliationType === 'creditors' ? 'pnoml' : 'snoml'})
                </h4>
                <div className="grid grid-cols-2 gap-4 text-sm">
                  <div>
                    <span className="text-gray-600">Posted to Nominal Ledger:</span>
                    <p className="font-medium text-green-700">
                      {formatCurrency(
                        reconciliationType === 'creditors'
                          ? (data.purchase_ledger as any)?.transfer_file?.posted_to_nl?.total
                          : (data.sales_ledger as any)?.transfer_file?.posted_to_nl?.total
                      )}
                      {' '}({reconciliationType === 'creditors'
                        ? (data.purchase_ledger as any)?.transfer_file?.posted_to_nl?.count
                        : (data.sales_ledger as any)?.transfer_file?.posted_to_nl?.count} entries)
                    </p>
                  </div>
                  <div>
                    <span className="text-gray-600">Pending in Transfer File:</span>
                    <p className="font-medium text-amber-700">
                      {formatCurrency(
                        reconciliationType === 'creditors'
                          ? (data.purchase_ledger as any)?.transfer_file?.pending_transfer?.total
                          : (data.sales_ledger as any)?.transfer_file?.pending_transfer?.total
                      )}
                      {' '}({reconciliationType === 'creditors'
                        ? (data.purchase_ledger as any)?.transfer_file?.pending_transfer?.count
                        : (data.sales_ledger as any)?.transfer_file?.pending_transfer?.count} entries)
                    </p>
                  </div>
                </div>
                <p className="text-xs text-amber-600 mt-2 mb-3">
                  These entries are in the {reconciliationType === 'creditors' ? 'pnoml' : 'snoml'} transfer file awaiting posting to the Nominal Ledger.
                </p>

                {/* Pending Transactions Detail */}
                <button
                  onClick={() => toggleSection('pendingTransactions')}
                  className="w-full flex items-center justify-between p-2 bg-amber-100 hover:bg-amber-200 rounded transition-colors text-sm"
                >
                  <span className="font-medium text-amber-800">
                    View Pending Entries ({reconciliationType === 'creditors'
                      ? (data.purchase_ledger as any)?.transfer_file?.pending_transfer?.count
                      : (data.sales_ledger as any)?.transfer_file?.pending_transfer?.count})
                  </span>
                  {expandedSections.has('pendingTransactions') ? (
                    <ChevronDown className="h-4 w-4 text-amber-600" />
                  ) : (
                    <ChevronRight className="h-4 w-4 text-amber-600" />
                  )}
                </button>

                {expandedSections.has('pendingTransactions') && (
                  <div className="mt-3 overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="bg-amber-100">
                          <th className="text-left p-2">Date</th>
                          <th className="text-left p-2">Nominal Account</th>
                          <th className="text-left p-2">Type</th>
                          <th className="text-left p-2">Reference</th>
                          <th className="text-left p-2">Comment</th>
                          <th className="text-right p-2">Value</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(reconciliationType === 'creditors'
                          ? (data.purchase_ledger as any)?.transfer_file?.pending_transfer?.transactions
                          : (data.sales_ledger as any)?.transfer_file?.pending_transfer?.transactions
                        )?.map((txn: any, idx: number) => (
                          <tr key={idx} className="border-t border-amber-200">
                            <td className="p-2 whitespace-nowrap">{txn.date}</td>
                            <td className="p-2 font-mono">{txn.nominal_account}</td>
                            <td className="p-2">
                              <span className="px-2 py-0.5 bg-amber-200 rounded text-xs font-medium">
                                {txn.type}
                              </span>
                            </td>
                            <td className="p-2 font-mono">{txn.reference}</td>
                            <td className="p-2 text-gray-600 max-w-[200px] truncate" title={txn.comment}>
                              {txn.comment}
                            </td>
                            <td className="p-2 text-right font-medium">{formatCurrency(txn.value)}</td>
                          </tr>
                        ))}
                      </tbody>
                      <tfoot>
                        <tr className="border-t-2 border-amber-300 font-bold bg-amber-100">
                          <td className="p-2" colSpan={5}>Total Pending</td>
                          <td className="p-2 text-right">
                            {formatCurrency(
                              reconciliationType === 'creditors'
                                ? (data.purchase_ledger as any)?.transfer_file?.pending_transfer?.total
                                : (data.sales_ledger as any)?.transfer_file?.pending_transfer?.total
                            )}
                          </td>
                        </tr>
                      </tfoot>
                    </table>
                  </div>
                )}
              </div>
            )}

            {/* Master File Check */}
            <div className="bg-gray-50 p-4 rounded-lg">
              <h4 className="font-medium text-gray-700 mb-2">
                {reconciliationType === 'creditors' ? 'Supplier Master Check' : 'Customer Master Check'}
              </h4>
              {reconciliationType === 'creditors' ? (
                <div className="flex items-center gap-4">
                  {data.purchase_ledger?.supplier_master_check?.matches_ptran ? (
                    <CheckCircle className="h-5 w-5 text-green-600" />
                  ) : (
                    <XCircle className="h-5 w-5 text-red-600" />
                  )}
                  <div>
                    <p className="text-sm">
                      <span className="font-medium">pname total:</span>{' '}
                      {formatCurrency(data.purchase_ledger?.supplier_master_check?.total)}
                    </p>
                    <p className="text-sm text-gray-600">
                      {data.purchase_ledger?.supplier_master_check?.supplier_count} suppliers with balance
                    </p>
                  </div>
                </div>
              ) : (
                <div className="flex items-center gap-4">
                  {data.sales_ledger?.customer_master_check?.matches_stran ? (
                    <CheckCircle className="h-5 w-5 text-green-600" />
                  ) : (
                    <XCircle className="h-5 w-5 text-red-600" />
                  )}
                  <div>
                    <p className="text-sm">
                      <span className="font-medium">sname total:</span>{' '}
                      {formatCurrency(data.sales_ledger?.customer_master_check?.total)}
                    </p>
                    <p className="text-sm text-gray-600">
                      {data.sales_ledger?.customer_master_check?.customer_count} customers with balance
                    </p>
                  </div>
                </div>
              )}
            </div>
          </div>
        )}
      </div>

      {/* Nominal Ledger Details */}
      <div className="bg-white rounded-lg shadow overflow-hidden">
        <SectionHeader
          title={`Nominal Ledger Control Account (${(data.nominal_ledger as any)?.current_year || 'Current Year'})`}
          section="nominal"
          icon={Building}
        />
        {expandedSections.has('nominal') && (
          <div className="p-4">
            <p className="text-sm text-gray-600 mb-3">
              Showing current year ({(data.nominal_ledger as any)?.current_year}) transactions only for reconciliation
            </p>
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50">
                  <th className="text-left p-2">Account</th>
                  <th className="text-left p-2">Description</th>
                  <th className="text-right p-2">Prior Year B/F</th>
                  <th className="text-right p-2">Current Year Dr</th>
                  <th className="text-right p-2">Current Year Cr</th>
                  <th className="text-right p-2">Current Year Net</th>
                  <th className="text-right p-2">Balance</th>
                </tr>
              </thead>
              <tbody>
                {data.nominal_ledger?.control_accounts?.map((acc: any, idx: number) => (
                  <tr key={idx} className="border-t">
                    <td className="p-2 font-mono">{acc.account}</td>
                    <td className="p-2">{acc.description}</td>
                    <td className="p-2 text-right text-gray-500">{formatCurrency(acc.brought_forward)}</td>
                    <td className="p-2 text-right">{formatCurrency(acc.current_year_debits)}</td>
                    <td className="p-2 text-right">{formatCurrency(acc.current_year_credits)}</td>
                    <td className="p-2 text-right">{formatCurrency(acc.current_year_net)}</td>
                    <td className="p-2 text-right font-bold">{formatCurrency(acc.closing_balance)}</td>
                  </tr>
                ))}
              </tbody>
              <tfoot>
                <tr className="border-t-2 font-bold bg-gray-50">
                  <td className="p-2" colSpan={6}>Total (Current Year)</td>
                  <td className="p-2 text-right">{formatCurrency(data.nominal_ledger?.total_balance)}</td>
                </tr>
              </tfoot>
            </table>
          </div>
        )}
      </div>

      {/* Aged Analysis (Creditors only) */}
      {reconciliationType === 'creditors' && data.aged_analysis && (
        <div className="bg-white rounded-lg shadow overflow-hidden">
          <SectionHeader title="Aged Analysis" section="aged" icon={DollarSign} />
          {expandedSections.has('aged') && (
            <div className="p-4">
              <table className="w-full text-sm">
                <thead>
                  <tr className="bg-gray-50">
                    <th className="text-left p-2">Age Band</th>
                    <th className="text-right p-2">Count</th>
                    <th className="text-right p-2">Total</th>
                  </tr>
                </thead>
                <tbody>
                  {data.aged_analysis.map((band, idx) => (
                    <tr key={idx} className="border-t">
                      <td className="p-2">{band.age_band}</td>
                      <td className="p-2 text-right">{band.count}</td>
                      <td className="p-2 text-right font-medium">{formatCurrency(band.total)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      {/* Top Suppliers (Creditors only) */}
      {reconciliationType === 'creditors' && data.top_suppliers && (
        <div className="bg-white rounded-lg shadow overflow-hidden">
          <SectionHeader title="Top 10 Suppliers by Outstanding" section="suppliers" icon={Users} />
          {expandedSections.has('suppliers') && (
            <div className="p-4">
              <table className="w-full text-sm">
                <thead>
                  <tr className="bg-gray-50">
                    <th className="text-left p-2">Account</th>
                    <th className="text-left p-2">Supplier</th>
                    <th className="text-right p-2">Invoices</th>
                    <th className="text-right p-2">Outstanding</th>
                  </tr>
                </thead>
                <tbody>
                  {data.top_suppliers.map((supplier, idx) => (
                    <tr key={idx} className="border-t">
                      <td className="p-2 font-mono">{supplier.account}</td>
                      <td className="p-2">{supplier.name}</td>
                      <td className="p-2 text-right">{supplier.invoice_count}</td>
                      <td className="p-2 text-right font-medium">{formatCurrency(supplier.outstanding)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// Bank Reconciliation View
function BankReconciliationView({
  data,
  expandedSections,
  toggleSection,
  formatCurrency,
  SectionHeader
}: {
  data: BankReconciliationResponse;
  expandedSections: Set<string>;
  toggleSection: (section: string) => void;
  formatCurrency: (value: number | undefined) => string;
  SectionHeader: React.ComponentType<{ title: string; section: string; icon: React.ComponentType<{ className?: string }> }>;
}) {
  if (!data.success) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-4">
        <div className="flex items-center gap-2 text-red-700">
          <XCircle className="h-5 w-5" />
          <span>{data.error || 'Failed to load bank reconciliation'}</span>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* Status Banner */}
      <div
        className={`p-6 rounded-lg border-2 ${
          data.status === 'RECONCILED'
            ? 'bg-green-50 border-green-300'
            : 'bg-red-50 border-red-300'
        }`}
      >
        <div className="flex items-center gap-4">
          {data.status === 'RECONCILED' ? (
            <CheckCircle className="h-12 w-12 text-green-600" />
          ) : (
            <XCircle className="h-12 w-12 text-red-600" />
          )}
          <div>
            <h2 className={`text-2xl font-bold ${data.status === 'RECONCILED' ? 'text-green-800' : 'text-red-800'}`}>
              {data.status}
            </h2>
            <p className={`text-lg ${data.status === 'RECONCILED' ? 'text-green-700' : 'text-red-700'}`}>
              {data.message}
            </p>
            <p className="text-sm text-gray-600 mt-1">
              {data.bank_account.description} ({data.bank_account.code})
              {data.bank_account.sort_code && ` - ${data.bank_account.sort_code}`}
              {data.bank_account.account_number && ` ${data.bank_account.account_number}`}
            </p>
            <p className="text-sm text-gray-500">
              As at: {data.reconciliation_date}
            </p>
          </div>
        </div>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        {/* Cashbook Balance */}
        <div className="bg-white rounded-lg shadow p-6">
          <div className="flex items-center gap-3 mb-2">
            <FileText className="h-5 w-5 text-blue-600" />
            <span className="text-sm text-gray-600">Cashbook Balance</span>
          </div>
          <p className="text-2xl font-bold text-gray-900">
            {formatCurrency(data.cashbook.total_balance)}
          </p>
          <p className="text-sm text-gray-500 mt-1">
            {data.cashbook.entry_count} entries
          </p>
        </div>

        {/* Posted to NL */}
        <div className="bg-white rounded-lg shadow p-6 border-l-4 border-green-500">
          <div className="flex items-center gap-3 mb-2">
            <CheckCircle className="h-5 w-5 text-green-600" />
            <span className="text-sm text-gray-600">Posted to NL</span>
          </div>
          <p className="text-2xl font-bold text-green-700">
            {formatCurrency(data.cashbook.transfer_file.posted_to_nl.total)}
          </p>
          <p className="text-sm text-gray-500 mt-1">
            {data.cashbook.transfer_file.posted_to_nl.count} entries
          </p>
        </div>

        {/* Pending in Transfer File */}
        {data.variance.has_pending_transfers && (
          <div className="bg-amber-50 rounded-lg shadow p-6 border-l-4 border-amber-500">
            <div className="flex items-center gap-3 mb-2">
              <AlertTriangle className="h-5 w-5 text-amber-600" />
              <span className="text-sm text-gray-600">Pending (anoml)</span>
            </div>
            <p className="text-2xl font-bold text-amber-700">
              {formatCurrency(data.cashbook.transfer_file.pending_transfer.total)}
            </p>
            <p className="text-sm text-amber-600 mt-1">
              {data.cashbook.transfer_file.pending_transfer.count} awaiting posting
            </p>
          </div>
        )}

        {/* Nominal Ledger Balance */}
        <div className="bg-white rounded-lg shadow p-6">
          <div className="flex items-center gap-3 mb-2">
            <Landmark className="h-5 w-5 text-purple-600" />
            <span className="text-sm text-gray-600">Nominal Ledger</span>
          </div>
          <p className="text-2xl font-bold text-gray-900">
            {formatCurrency(data.nominal_ledger.total_balance)}
          </p>
          <p className="text-sm text-gray-500 mt-1">
            {data.nominal_ledger.account}
          </p>
        </div>

        {/* Variance */}
        <div className={`rounded-lg shadow p-6 ${data.variance.reconciled ? 'bg-green-50' : 'bg-red-50'}`}>
          <div className="flex items-center gap-3 mb-2">
            <AlertTriangle className={`h-5 w-5 ${data.variance.reconciled ? 'text-green-600' : 'text-red-600'}`} />
            <span className="text-sm text-gray-600">Variance</span>
          </div>
          <p className={`text-2xl font-bold ${data.variance.reconciled ? 'text-green-700' : 'text-red-700'}`}>
            {formatCurrency(data.variance.absolute)}
          </p>
          <p className="text-sm text-gray-500 mt-1">
            {data.variance.reconciled ? 'Balanced' : 'Out of balance'}
          </p>
        </div>
      </div>

      {/* Cashbook Details */}
      <div className="bg-white rounded-lg shadow overflow-hidden">
        <SectionHeader
          title="Cashbook Details"
          section="cashbook"
          icon={FileText}
        />
        {expandedSections.has('cashbook') && (
          <div className="p-4 space-y-4">
            <div className="grid grid-cols-2 gap-4 text-sm">
              <div>
                <span className="text-gray-600">Source:</span>
                <p className="font-medium">{data.cashbook.source}</p>
              </div>
              <div>
                <span className="text-gray-600">Total Balance:</span>
                <p className="font-medium">{formatCurrency(data.cashbook.total_balance)}</p>
              </div>
            </div>

            {/* Transfer File Status */}
            {data.variance.has_pending_transfers && (
              <div className="bg-amber-50 p-4 rounded-lg border border-amber-200">
                <h4 className="font-medium text-amber-800 mb-2 flex items-center gap-2">
                  <AlertTriangle className="h-4 w-4" />
                  Transfer File Status (anoml)
                </h4>
                <div className="grid grid-cols-2 gap-4 text-sm">
                  <div>
                    <span className="text-gray-600">Posted to Nominal Ledger:</span>
                    <p className="font-medium text-green-700">
                      {formatCurrency(data.cashbook.transfer_file.posted_to_nl.total)}
                      {' '}({data.cashbook.transfer_file.posted_to_nl.count} entries)
                    </p>
                  </div>
                  <div>
                    <span className="text-gray-600">Pending in Transfer File:</span>
                    <p className="font-medium text-amber-700">
                      {formatCurrency(data.cashbook.transfer_file.pending_transfer.total)}
                      {' '}({data.cashbook.transfer_file.pending_transfer.count} entries)
                    </p>
                  </div>
                </div>

                {/* Pending Transactions Detail */}
                <button
                  onClick={() => toggleSection('pendingTransactions')}
                  className="w-full flex items-center justify-between p-2 mt-3 bg-amber-100 hover:bg-amber-200 rounded transition-colors text-sm"
                >
                  <span className="font-medium text-amber-800">
                    View Pending Entries ({data.cashbook.transfer_file.pending_transfer.count})
                  </span>
                  {expandedSections.has('pendingTransactions') ? (
                    <ChevronDown className="h-4 w-4 text-amber-600" />
                  ) : (
                    <ChevronRight className="h-4 w-4 text-amber-600" />
                  )}
                </button>

                {expandedSections.has('pendingTransactions') && (
                  <div className="mt-3 overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="bg-amber-100">
                          <th className="text-left p-2">Date</th>
                          <th className="text-left p-2">Nominal Account</th>
                          <th className="text-left p-2">Source</th>
                          <th className="text-left p-2">Reference</th>
                          <th className="text-left p-2">Comment</th>
                          <th className="text-right p-2">Value</th>
                        </tr>
                      </thead>
                      <tbody>
                        {data.cashbook.transfer_file.pending_transfer.transactions.map((txn, idx) => (
                          <tr key={idx} className="border-t border-amber-200">
                            <td className="p-2 whitespace-nowrap">{txn.date}</td>
                            <td className="p-2 font-mono">{txn.nominal_account}</td>
                            <td className="p-2">
                              <span className="px-2 py-0.5 bg-amber-200 rounded text-xs font-medium">
                                {txn.source_desc}
                              </span>
                            </td>
                            <td className="p-2 font-mono">{txn.reference}</td>
                            <td className="p-2 text-gray-600 max-w-[200px] truncate" title={txn.comment}>
                              {txn.comment}
                            </td>
                            <td className="p-2 text-right font-medium">{formatCurrency(txn.value)}</td>
                          </tr>
                        ))}
                      </tbody>
                      <tfoot>
                        <tr className="border-t-2 border-amber-300 font-bold bg-amber-100">
                          <td className="p-2" colSpan={5}>Total Pending</td>
                          <td className="p-2 text-right">
                            {formatCurrency(data.cashbook.transfer_file.pending_transfer.total)}
                          </td>
                        </tr>
                      </tfoot>
                    </table>
                  </div>
                )}
              </div>
            )}
          </div>
        )}
      </div>

      {/* Nominal Ledger Details */}
      <div className="bg-white rounded-lg shadow overflow-hidden">
        <SectionHeader
          title={`Nominal Ledger (${data.nominal_ledger.current_year || 'Current Year'})`}
          section="nominal"
          icon={Landmark}
        />
        {expandedSections.has('nominal') && (
          <div className="p-4">
            <table className="w-full text-sm">
              <tbody>
                <tr className="border-b">
                  <td className="p-2 text-gray-600">Account</td>
                  <td className="p-2 font-mono font-medium">{data.nominal_ledger.account}</td>
                </tr>
                <tr className="border-b">
                  <td className="p-2 text-gray-600">Description</td>
                  <td className="p-2 font-medium">{data.nominal_ledger.description}</td>
                </tr>
                {data.nominal_ledger.brought_forward !== undefined && (
                  <tr className="border-b">
                    <td className="p-2 text-gray-600">Brought Forward</td>
                    <td className="p-2 font-medium">{formatCurrency(data.nominal_ledger.brought_forward)}</td>
                  </tr>
                )}
                {data.nominal_ledger.current_year_debits !== undefined && (
                  <tr className="border-b">
                    <td className="p-2 text-gray-600">Current Year Debits</td>
                    <td className="p-2 font-medium">{formatCurrency(data.nominal_ledger.current_year_debits)}</td>
                  </tr>
                )}
                {data.nominal_ledger.current_year_credits !== undefined && (
                  <tr className="border-b">
                    <td className="p-2 text-gray-600">Current Year Credits</td>
                    <td className="p-2 font-medium">{formatCurrency(data.nominal_ledger.current_year_credits)}</td>
                  </tr>
                )}
                {data.nominal_ledger.current_year_net !== undefined && (
                  <tr className="border-b">
                    <td className="p-2 text-gray-600">Current Year Net</td>
                    <td className="p-2 font-medium">{formatCurrency(data.nominal_ledger.current_year_net)}</td>
                  </tr>
                )}
                <tr className="bg-gray-50">
                  <td className="p-2 text-gray-800 font-medium">Closing Balance</td>
                  <td className="p-2 font-bold text-lg">{formatCurrency(data.nominal_ledger.closing_balance || data.nominal_ledger.total_balance)}</td>
                </tr>
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
