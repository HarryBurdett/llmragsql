import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  RefreshCw,
  FileText,
  TrendingUp,
  TrendingDown,
  Building,
  Wallet,
  CreditCard,
  PiggyBank,
  ChevronDown,
  ChevronRight,
  AlertTriangle,
} from 'lucide-react';
import apiClient from '../api/client';
import type { StatutoryAccountsResponse } from '../api/client';

export function StatutoryAccounts() {
  const [selectedYear] = useState(2026);
  const [expandedSections, setExpandedSections] = useState<Set<string>>(
    new Set(['turnover', 'cost_of_sales', 'admin', 'fixed_assets', 'current_assets', 'current_liabilities', 'capital'])
  );

  const accountsQuery = useQuery({
    queryKey: ['statutoryAccounts', selectedYear],
    queryFn: () => apiClient.statutoryAccounts(selectedYear),
  });

  const formatCurrency = (value: number): string => {
    const absValue = Math.abs(value);
    const formatted = `£${absValue.toLocaleString('en-GB', { minimumFractionDigits: 0, maximumFractionDigits: 0 })}`;
    return value < 0 ? `(${formatted})` : formatted;
  };

  const toggleSection = (section: string) => {
    setExpandedTypes(prev => {
      const newSet = new Set(prev);
      if (newSet.has(section)) {
        newSet.delete(section);
      } else {
        newSet.add(section);
      }
      return newSet;
    });
  };

  const setExpandedTypes = setExpandedSections;

  const data = accountsQuery.data?.data as StatutoryAccountsResponse | undefined;
  const pnl = data?.profit_and_loss;
  const bs = data?.balance_sheet;

  const SectionHeader = ({ title, total, section, icon: Icon }: { title: string; total: number; section: string; icon: typeof FileText }) => (
    <tr
      className="bg-gray-50 cursor-pointer hover:bg-gray-100"
      onClick={() => toggleSection(section)}
    >
      <td className="px-4 py-2 font-semibold text-gray-700" colSpan={2}>
        <div className="flex items-center gap-2">
          {expandedSections.has(section) ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
          <Icon className="h-4 w-4 text-gray-500" />
          {title}
        </div>
      </td>
      <td className="px-4 py-2 text-right font-semibold font-mono text-gray-900">
        {formatCurrency(total)}
      </td>
    </tr>
  );

  const LineItem = ({ code, description, value, indent = false }: { code: string; description: string; value: number; indent?: boolean }) => (
    <tr className="hover:bg-gray-50">
      <td className={`px-4 py-1 text-sm text-gray-500 ${indent ? 'pl-12' : 'pl-8'}`}>{code}</td>
      <td className="px-4 py-1 text-sm text-gray-700">{description}</td>
      <td className="px-4 py-1 text-sm text-right font-mono text-gray-600">
        {formatCurrency(Math.abs(value))}
      </td>
    </tr>
  );

  const SubtotalRow = ({ label, value, bold = false }: { label: string; value: number; bold?: boolean }) => (
    <tr className={bold ? 'bg-gray-100' : ''}>
      <td className="px-4 py-2" colSpan={2}>
        <span className={`${bold ? 'font-bold text-gray-900' : 'font-medium text-gray-700'} pl-4`}>{label}</span>
      </td>
      <td className={`px-4 py-2 text-right font-mono ${bold ? 'font-bold text-gray-900 border-t-2 border-b-2 border-gray-400' : 'font-semibold text-gray-800'}`}>
        {formatCurrency(value)}
      </td>
    </tr>
  );

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-start">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Statutory Accounts</h2>
          <p className="text-gray-600 mt-1">UK GAAP format accounts for the year ended {selectedYear}</p>
        </div>
        <button
          onClick={() => accountsQuery.refetch()}
          disabled={accountsQuery.isFetching}
          className="btn btn-secondary text-sm flex items-center"
        >
          <RefreshCw className={`h-4 w-4 mr-2 ${accountsQuery.isFetching ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </div>

      {accountsQuery.isLoading ? (
        <div className="flex items-center justify-center py-12">
          <RefreshCw className="h-8 w-8 animate-spin text-blue-500" />
        </div>
      ) : data?.success === false ? (
        <div className="card text-center py-8 text-red-500">
          <AlertTriangle className="h-8 w-8 mx-auto mb-2" />
          <p>Error loading accounts: {data?.error}</p>
        </div>
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Profit and Loss Account */}
          <div className="card">
            <div className="flex items-center gap-2 mb-4 pb-3 border-b">
              <TrendingUp className="h-6 w-6 text-green-600" />
              <h3 className="text-lg font-bold text-gray-900">Profit and Loss Account</h3>
            </div>
            <p className="text-sm text-gray-500 mb-4">For the year ended 31 December {selectedYear}</p>

            <div className="overflow-x-auto">
              <table className="min-w-full">
                <thead>
                  <tr className="border-b">
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase w-20">Code</th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">Description</th>
                    <th className="px-4 py-2 text-right text-xs font-medium text-gray-500 uppercase w-32">£</th>
                  </tr>
                </thead>
                <tbody>
                  {/* Turnover */}
                  <SectionHeader title="Turnover" total={pnl?.turnover?.total || 0} section="turnover" icon={TrendingUp} />
                  {expandedSections.has('turnover') && pnl?.turnover?.items?.map((item, idx) => (
                    <LineItem key={idx} code={item.code} description={item.description} value={item.value} />
                  ))}

                  {/* Cost of Sales */}
                  <SectionHeader title="Cost of Sales" total={-(pnl?.cost_of_sales?.total || 0)} section="cost_of_sales" icon={TrendingDown} />
                  {expandedSections.has('cost_of_sales') && pnl?.cost_of_sales?.items?.map((item, idx) => (
                    <LineItem key={idx} code={item.code} description={item.description} value={item.value} />
                  ))}

                  {/* Gross Profit */}
                  <SubtotalRow label="Gross Profit" value={pnl?.gross_profit || 0} />

                  {/* Administrative Expenses */}
                  <SectionHeader title="Administrative Expenses" total={-(pnl?.administrative_expenses?.total || 0)} section="admin" icon={FileText} />
                  {expandedSections.has('admin') && pnl?.administrative_expenses?.items?.map((item, idx) => (
                    <LineItem key={idx} code={item.code} description={item.description} value={item.value} />
                  ))}

                  {/* Operating Profit */}
                  <SubtotalRow label="Operating Profit" value={pnl?.operating_profit || 0} />

                  {/* Profit Before Tax */}
                  <SubtotalRow label="Profit Before Taxation" value={pnl?.profit_before_tax || 0} bold />
                </tbody>
              </table>
            </div>
          </div>

          {/* Balance Sheet */}
          <div className="card">
            <div className="flex items-center gap-2 mb-4 pb-3 border-b">
              <Building className="h-6 w-6 text-blue-600" />
              <h3 className="text-lg font-bold text-gray-900">Balance Sheet</h3>
            </div>
            <p className="text-sm text-gray-500 mb-4">As at 31 December {selectedYear}</p>

            <div className="overflow-x-auto">
              <table className="min-w-full">
                <thead>
                  <tr className="border-b">
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase w-20">Code</th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">Description</th>
                    <th className="px-4 py-2 text-right text-xs font-medium text-gray-500 uppercase w-32">£</th>
                  </tr>
                </thead>
                <tbody>
                  {/* Fixed Assets */}
                  <SectionHeader title="Fixed Assets" total={bs?.fixed_assets?.total || 0} section="fixed_assets" icon={Building} />
                  {expandedSections.has('fixed_assets') && bs?.fixed_assets?.items?.map((item, idx) => (
                    <LineItem key={idx} code={item.code} description={item.description} value={item.value} />
                  ))}

                  {/* Current Assets */}
                  <SectionHeader title="Current Assets" total={bs?.current_assets?.total || 0} section="current_assets" icon={Wallet} />
                  {expandedSections.has('current_assets') && bs?.current_assets?.items?.map((item, idx) => (
                    <LineItem key={idx} code={item.code} description={item.description} value={item.value} />
                  ))}

                  {/* Current Liabilities */}
                  <SectionHeader title="Creditors: amounts falling due within one year" total={-(bs?.current_liabilities?.total || 0)} section="current_liabilities" icon={CreditCard} />
                  {expandedSections.has('current_liabilities') && bs?.current_liabilities?.items?.map((item, idx) => (
                    <LineItem key={idx} code={item.code} description={item.description} value={item.value} />
                  ))}

                  {/* Net Current Assets */}
                  <SubtotalRow label="Net Current Assets" value={bs?.net_current_assets || 0} />

                  {/* Total Assets Less Current Liabilities */}
                  <SubtotalRow label="Total Assets Less Current Liabilities" value={bs?.total_assets_less_current_liabilities || 0} />

                  {/* Capital and Reserves */}
                  <SectionHeader title="Capital and Reserves" total={bs?.capital_and_reserves?.total || 0} section="capital" icon={PiggyBank} />
                  {expandedSections.has('capital') && bs?.capital_and_reserves?.items?.map((item, idx) => (
                    <LineItem key={idx} code={item.code} description={item.description} value={item.value} />
                  ))}

                  {/* Shareholders' Funds */}
                  <SubtotalRow label="Shareholders' Funds" value={bs?.capital_and_reserves?.total || 0} bold />
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* Notes */}
      <div className="card bg-gray-50">
        <h4 className="font-semibold text-gray-700 mb-2">Notes</h4>
        <ul className="text-sm text-gray-600 space-y-1">
          <li>• These accounts are prepared in accordance with UK GAAP (FRS 102)</li>
          <li>• Figures are derived from the nominal ledger transactions (ntran) for {selectedYear}</li>
          <li>• Click on section headers to expand/collapse account details</li>
        </ul>
      </div>
    </div>
  );
}

export default StatutoryAccounts;
