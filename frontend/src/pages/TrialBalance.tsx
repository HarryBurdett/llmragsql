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
  ShoppingCart,
  Briefcase,
  MoreHorizontal,
  CheckCircle,
  AlertTriangle,
} from 'lucide-react';
import apiClient from '../api/client';
import type { TrialBalanceResponse } from '../api/client';

const TYPE_ICONS: Record<string, typeof FileText> = {
  'A': Building,      // Fixed Assets
  'B': Wallet,        // Current Assets
  'C': CreditCard,    // Current Liabilities
  'D': PiggyBank,     // Capital & Reserves
  'E': TrendingUp,    // Sales
  'F': ShoppingCart,  // Cost of Sales
  'G': Briefcase,     // Overheads
  'H': MoreHorizontal, // Other
};

const TYPE_COLORS: Record<string, string> = {
  'A': 'blue',
  'B': 'green',
  'C': 'red',
  'D': 'purple',
  'E': 'emerald',
  'F': 'orange',
  'G': 'amber',
  'H': 'gray',
};

export function TrialBalance() {
  const [selectedYear] = useState(2026);
  const [expandedTypes, setExpandedTypes] = useState<Set<string>>(new Set(['E', 'F', 'G'])); // Default expand P&L types

  const trialBalanceQuery = useQuery({
    queryKey: ['trialBalance', selectedYear],
    queryFn: () => apiClient.trialBalance(selectedYear),
  });

  const formatCurrency = (value: number): string => {
    return `£${Math.abs(value).toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  };

  const toggleType = (type: string) => {
    setExpandedTypes(prev => {
      const newSet = new Set(prev);
      if (newSet.has(type)) {
        newSet.delete(type);
      } else {
        newSet.add(type);
      }
      return newSet;
    });
  };

  const data = trialBalanceQuery.data?.data as TrialBalanceResponse | undefined;

  // Group data by account type
  const groupedData = data?.data?.reduce((acc, record) => {
    const type = record.account_type?.trim() || '?';
    if (!acc[type]) {
      acc[type] = [];
    }
    acc[type].push(record);
    return acc;
  }, {} as Record<string, typeof data.data>) || {};

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-start">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Trial Balance</h2>
          <p className="text-gray-600 mt-1">Summary trial balance from the nominal ledger for {selectedYear}</p>
        </div>
        <button
          onClick={() => trialBalanceQuery.refetch()}
          disabled={trialBalanceQuery.isFetching}
          className="btn btn-secondary text-sm flex items-center"
        >
          <RefreshCw className={`h-4 w-4 mr-2 ${trialBalanceQuery.isFetching ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </div>

      {trialBalanceQuery.isLoading ? (
        <div className="flex items-center justify-center py-12">
          <RefreshCw className="h-8 w-8 animate-spin text-blue-500" />
        </div>
      ) : data?.success === false ? (
        <div className="card text-center py-8 text-red-500">
          <AlertTriangle className="h-8 w-8 mx-auto mb-2" />
          <p>Error loading trial balance: {data?.error}</p>
        </div>
      ) : (
        <>
          {/* Summary Cards */}
          {data?.totals && (
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="card bg-gradient-to-br from-green-500 to-green-600 text-white">
                <div className="flex items-center justify-between">
                  <TrendingUp className="h-8 w-8 opacity-80" />
                  <span className="text-xs bg-white/20 px-2 py-1 rounded-full">
                    {data.count} accounts
                  </span>
                </div>
                <div className="mt-3">
                  <p className="text-sm opacity-80">Total Debits</p>
                  <p className="text-2xl font-bold">{formatCurrency(data.totals.debit)}</p>
                </div>
              </div>

              <div className="card bg-gradient-to-br from-red-500 to-red-600 text-white">
                <div className="flex items-center justify-between">
                  <TrendingDown className="h-8 w-8 opacity-80" />
                </div>
                <div className="mt-3">
                  <p className="text-sm opacity-80">Total Credits</p>
                  <p className="text-2xl font-bold">{formatCurrency(data.totals.credit)}</p>
                </div>
              </div>

              <div className={`card ${Math.abs(data.totals.difference) < 0.01 ? 'bg-gradient-to-br from-blue-500 to-blue-600' : 'bg-gradient-to-br from-yellow-500 to-yellow-600'} text-white`}>
                <div className="flex items-center justify-between">
                  {Math.abs(data.totals.difference) < 0.01 ? (
                    <CheckCircle className="h-8 w-8 opacity-80" />
                  ) : (
                    <AlertTriangle className="h-8 w-8 opacity-80" />
                  )}
                </div>
                <div className="mt-3">
                  <p className="text-sm opacity-80">Difference</p>
                  <p className="text-2xl font-bold">{formatCurrency(data.totals.difference)}</p>
                  {Math.abs(data.totals.difference) < 0.01 && (
                    <p className="text-xs mt-1 opacity-80">Trial Balance is in balance</p>
                  )}
                </div>
              </div>
            </div>
          )}

          {/* Summary by Type */}
          {data?.by_type && (
            <div className="card">
              <h3 className="text-lg font-semibold text-gray-900 mb-4">Summary by Account Type</h3>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                {Object.entries(data.by_type)
                  .sort(([a], [b]) => a.localeCompare(b))
                  .map(([type, summary]) => {
                    const Icon = TYPE_ICONS[type.trim()] || FileText;
                    const color = TYPE_COLORS[type.trim()] || 'gray';
                    return (
                      <button
                        key={type}
                        onClick={() => toggleType(type.trim())}
                        className={`p-4 rounded-lg border-2 text-left transition-all hover:shadow-md ${
                          expandedTypes.has(type.trim())
                            ? `border-${color}-500 bg-${color}-50`
                            : 'border-gray-200 bg-white'
                        }`}
                        style={{
                          borderColor: expandedTypes.has(type.trim()) ? undefined : '#e5e7eb',
                          backgroundColor: expandedTypes.has(type.trim()) ? `var(--${color}-50, #f0fdf4)` : 'white'
                        }}
                      >
                        <div className="flex items-center gap-2 mb-2">
                          <Icon className={`h-5 w-5 text-${color}-600`} />
                          <span className="text-sm font-medium text-gray-700">{summary.name}</span>
                        </div>
                        <div className="text-xs text-gray-500">
                          <p>Dr: {formatCurrency(summary.debit)}</p>
                          <p>Cr: {formatCurrency(summary.credit)}</p>
                          <p className="mt-1 text-gray-400">{summary.count} accounts</p>
                        </div>
                      </button>
                    );
                  })}
              </div>
            </div>
          )}

          {/* Detailed Trial Balance Table */}
          <div className="card">
            <div className="flex justify-between items-center mb-4">
              <h3 className="text-lg font-semibold text-gray-900">Detailed Trial Balance</h3>
              <span className="text-sm text-gray-500">
                Click account types above to expand/collapse
              </span>
            </div>

            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Account</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Description</th>
                    <th className="px-4 py-3 text-right text-xs font-medium text-green-600 uppercase tracking-wider">Debit</th>
                    <th className="px-4 py-3 text-right text-xs font-medium text-red-600 uppercase tracking-wider">Credit</th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {Object.entries(groupedData)
                    .sort(([a], [b]) => a.localeCompare(b))
                    .map(([type, records]) => {
                      const typeName = data?.by_type?.[type]?.name || `Type ${type}`;
                      const isExpanded = expandedTypes.has(type.trim());
                      const typeDebit = records.reduce((sum, r) => sum + (r.debit || 0), 0);
                      const typeCredit = records.reduce((sum, r) => sum + (r.credit || 0), 0);
                      const Icon = TYPE_ICONS[type.trim()] || FileText;

                      return (
                        <>
                          {/* Type Header Row */}
                          <tr
                            key={`header-${type}`}
                            className="bg-gray-100 cursor-pointer hover:bg-gray-200"
                            onClick={() => toggleType(type.trim())}
                          >
                            <td className="px-4 py-2" colSpan={2}>
                              <div className="flex items-center gap-2">
                                <Icon className="h-4 w-4 text-gray-600" />
                                <span className="font-semibold text-gray-700">{typeName}</span>
                                <span className="text-xs text-gray-500">({records.length} accounts)</span>
                                <span className="text-xs text-gray-400 ml-2">
                                  {isExpanded ? '▼' : '▶'}
                                </span>
                              </div>
                            </td>
                            <td className="px-4 py-2 text-right font-mono font-semibold text-green-700">
                              {typeDebit > 0 ? formatCurrency(typeDebit) : ''}
                            </td>
                            <td className="px-4 py-2 text-right font-mono font-semibold text-red-700">
                              {typeCredit > 0 ? formatCurrency(typeCredit) : ''}
                            </td>
                          </tr>

                          {/* Detail Rows */}
                          {isExpanded && records.map((record, idx) => (
                            <tr key={`${type}-${idx}`} className="hover:bg-gray-50">
                              <td className="px-4 py-2 pl-8 text-sm text-gray-600">{record.account_code?.trim()}</td>
                              <td className="px-4 py-2 text-sm text-gray-900">{record.description}</td>
                              <td className="px-4 py-2 text-sm text-right font-mono text-green-700">
                                {record.debit > 0 ? formatCurrency(record.debit) : ''}
                              </td>
                              <td className="px-4 py-2 text-sm text-right font-mono text-red-700">
                                {record.credit > 0 ? formatCurrency(record.credit) : ''}
                              </td>
                            </tr>
                          ))}
                        </>
                      );
                    })}
                </tbody>
                {/* Totals Footer */}
                <tfoot className="bg-gray-100 font-bold">
                  <tr>
                    <td className="px-4 py-3 text-gray-900" colSpan={2}>TOTALS</td>
                    <td className="px-4 py-3 text-right font-mono text-green-800">
                      {data?.totals ? formatCurrency(data.totals.debit) : ''}
                    </td>
                    <td className="px-4 py-3 text-right font-mono text-red-800">
                      {data?.totals ? formatCurrency(data.totals.credit) : ''}
                    </td>
                  </tr>
                </tfoot>
              </table>
            </div>
          </div>
        </>
      )}
    </div>
  );
}

export default TrialBalance;
