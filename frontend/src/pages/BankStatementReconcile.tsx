import { useState, useMemo } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  CheckCircle,
  RefreshCw,
  Landmark,
  CheckSquare,
  Square,
  ArrowUpDown,
  Search,
} from 'lucide-react';
import apiClient from '../api/client';
import type {
  BankAccountsResponse,
  BankReconciliationStatusResponse,
  UnreconciledEntriesResponse,
  MarkReconciledResponse,
} from '../api/client';

export function BankStatementReconcile() {
  const queryClient = useQueryClient();
  const [selectedBank, setSelectedBank] = useState<string>('BC010');
  const [selectedEntries, setSelectedEntries] = useState<Set<string>>(new Set());
  const [statementNumber, setStatementNumber] = useState<string>('');
  const [statementDate, setStatementDate] = useState<string>(
    new Date().toISOString().split('T')[0]
  );
  const [statementBalance, setStatementBalance] = useState<string>('0.00');
  const [searchTerm, setSearchTerm] = useState<string>('');
  const [sortField, setSortField] = useState<'ae_entry' | 'value_pounds' | 'ae_lstdate'>('ae_lstdate');
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('asc');

  // Fetch bank accounts
  const banksQuery = useQuery<BankAccountsResponse>({
    queryKey: ['reconcileBanks'],
    queryFn: async () => {
      const response = await apiClient.reconcileBanks();
      return response.data;
    },
  });

  // Fetch reconciliation status
  const statusQuery = useQuery<BankReconciliationStatusResponse>({
    queryKey: ['bankRecStatus', selectedBank],
    queryFn: async () => {
      const response = await apiClient.getBankReconciliationStatus(selectedBank);
      return response.data;
    },
    enabled: !!selectedBank,
  });

  // Fetch unreconciled entries
  const entriesQuery = useQuery<UnreconciledEntriesResponse>({
    queryKey: ['unreconciledEntries', selectedBank],
    queryFn: async () => {
      const response = await apiClient.getUnreconciledEntries(selectedBank);
      return response.data;
    },
    enabled: !!selectedBank,
  });

  // Mark reconciled mutation
  const markReconciledMutation = useMutation<MarkReconciledResponse, Error, void>({
    mutationFn: async () => {
      const entries = Array.from(selectedEntries).map((entry, index) => ({
        entry_number: entry,
        statement_line: (index + 1) * 10,
      }));
      const response = await apiClient.markEntriesReconciled(selectedBank, {
        entries,
        statement_number: parseInt(statementNumber) || (statusQuery.data?.last_stmt_no || 0) + 1,
        statement_date: statementDate,
        reconciliation_date: statementDate,
      });
      return response.data;
    },
    onSuccess: () => {
      setSelectedEntries(new Set());
      queryClient.invalidateQueries({ queryKey: ['bankRecStatus', selectedBank] });
      queryClient.invalidateQueries({ queryKey: ['unreconciledEntries', selectedBank] });
    },
  });

  const formatCurrency = (value: number | undefined | null) => {
    if (value === undefined || value === null) return '';
    return Math.abs(value).toLocaleString('en-GB', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  };

  const formatDate = (dateStr: string | null) => {
    if (!dateStr) return '';
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-GB'); // DD/MM/YYYY
  };

  const toggleEntry = (entryNumber: string) => {
    const newSelected = new Set(selectedEntries);
    if (newSelected.has(entryNumber)) {
      newSelected.delete(entryNumber);
    } else {
      newSelected.add(entryNumber);
    }
    setSelectedEntries(newSelected);
  };

  const toggleAll = () => {
    if (filteredEntries.length === selectedEntries.size) {
      setSelectedEntries(new Set());
    } else {
      setSelectedEntries(new Set(filteredEntries.map(e => e.ae_entry)));
    }
  };

  const handleSort = (field: 'ae_entry' | 'value_pounds' | 'ae_lstdate') => {
    if (sortField === field) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortDirection('asc');
    }
  };

  // Filter and sort entries
  const filteredEntries = useMemo(() => {
    let entries = entriesQuery.data?.entries || [];

    // Filter by search term
    if (searchTerm) {
      const term = searchTerm.toLowerCase();
      entries = entries.filter(
        e =>
          e.ae_entry.toLowerCase().includes(term) ||
          e.ae_entref?.toLowerCase().includes(term) ||
          e.ae_comment?.toLowerCase().includes(term)
      );
    }

    // Sort
    entries = [...entries].sort((a, b) => {
      let aVal: string | number = a[sortField] ?? '';
      let bVal: string | number = b[sortField] ?? '';

      if (sortField === 'value_pounds') {
        aVal = a.value_pounds;
        bVal = b.value_pounds;
      } else if (sortField === 'ae_lstdate') {
        aVal = a.ae_lstdate || '';
        bVal = b.ae_lstdate || '';
      }

      if (typeof aVal === 'string') {
        aVal = aVal.toLowerCase();
        bVal = (bVal as string).toLowerCase();
      }

      if (sortDirection === 'asc') {
        return aVal > bVal ? 1 : -1;
      } else {
        return aVal < bVal ? 1 : -1;
      }
    });

    return entries;
  }, [entriesQuery.data?.entries, searchTerm, sortField, sortDirection]);

  // Calculate totals
  const totals = useMemo(() => {
    const selected = filteredEntries.filter(e => selectedEntries.has(e.ae_entry));
    const reconciled = selected.reduce((sum, e) => sum + e.value_pounds, 0);
    const stmtBal = parseFloat(statementBalance) || 0;
    const difference = stmtBal - reconciled;

    return {
      reconciled,
      statementBalance: stmtBal,
      difference,
    };
  }, [filteredEntries, selectedEntries, statementBalance]);

  // Calculate running balance for display
  const entriesWithBalance = useMemo(() => {
    let runningBalance = statusQuery.data?.reconciled_balance || 0;
    return filteredEntries.map((entry) => {
      if (selectedEntries.has(entry.ae_entry)) {
        runningBalance += entry.value_pounds;
      }
      return {
        ...entry,
        runningBalance,
        lineNumber: selectedEntries.has(entry.ae_entry)
          ? (Array.from(selectedEntries).indexOf(entry.ae_entry) + 1) * 10
          : null,
      };
    });
  }, [filteredEntries, selectedEntries, statusQuery.data?.reconciled_balance]);

  const bankDescription = banksQuery.data?.banks?.find(b => b.account_code === selectedBank)?.description || '';

  return (
    <div className="p-6 max-w-7xl mx-auto">
      {/* Header */}
      <div className="mb-4">
        <h1 className="text-xl font-bold text-gray-900 flex items-center gap-2">
          <Landmark className="w-6 h-6 text-blue-600" />
          Reconcile: {bankDescription || selectedBank} - Unreconciled Entries (Sterling)
        </h1>
      </div>

      {/* Bank Selector Row */}
      <div className="bg-gray-100 border border-gray-300 rounded p-3 mb-4">
        <div className="flex items-center gap-6 flex-wrap">
          <div className="flex items-center gap-2">
            <label className="text-sm font-medium text-gray-700">Description:</label>
            <select
              value={selectedBank}
              onChange={e => {
                setSelectedBank(e.target.value);
                setSelectedEntries(new Set());
              }}
              className="border border-gray-400 rounded px-2 py-1 min-w-[250px] bg-white"
            >
              {banksQuery.data?.banks?.map(bank => (
                <option key={bank.account_code} value={bank.account_code}>
                  {bank.description || bank.account_code}
                </option>
              ))}
            </select>
          </div>

          <div className="flex items-center gap-2">
            <label className="text-sm font-medium text-gray-700">Statement #:</label>
            <input
              type="number"
              value={statementNumber}
              onChange={e => setStatementNumber(e.target.value)}
              placeholder={String((statusQuery.data?.last_stmt_no || 0) + 1)}
              className="border border-gray-400 rounded px-2 py-1 w-24 bg-white"
            />
          </div>

          <div className="flex items-center gap-2">
            <label className="text-sm font-medium text-gray-700">Date:</label>
            <input
              type="date"
              value={statementDate}
              onChange={e => setStatementDate(e.target.value)}
              className="border border-gray-400 rounded px-2 py-1 bg-white"
            />
          </div>

          <div className="flex items-center gap-2">
            <label className="text-sm font-medium text-gray-700">Statement Balance:</label>
            <input
              type="number"
              step="0.01"
              value={statementBalance}
              onChange={e => setStatementBalance(e.target.value)}
              className="border border-gray-400 rounded px-2 py-1 w-28 bg-white text-right"
            />
          </div>

          <button
            onClick={() => {
              statusQuery.refetch();
              entriesQuery.refetch();
            }}
            className="p-1 text-gray-600 hover:text-gray-800 hover:bg-gray-200 rounded"
            title="Refresh"
          >
            <RefreshCw className={`w-4 h-4 ${statusQuery.isFetching ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      {/* Search */}
      <div className="mb-2 flex items-center gap-4">
        <div className="relative">
          <Search className="w-4 h-4 absolute left-2 top-1/2 transform -translate-y-1/2 text-gray-400" />
          <input
            type="text"
            placeholder="Search..."
            value={searchTerm}
            onChange={e => setSearchTerm(e.target.value)}
            className="pl-8 pr-3 py-1 border border-gray-400 rounded w-48 text-sm"
          />
        </div>
        <button
          onClick={toggleAll}
          className="px-2 py-1 text-xs border border-gray-400 rounded hover:bg-gray-100 flex items-center gap-1"
        >
          {filteredEntries.length === selectedEntries.size ? (
            <CheckSquare className="w-3 h-3" />
          ) : (
            <Square className="w-3 h-3" />
          )}
          {filteredEntries.length === selectedEntries.size ? 'Untick All' : 'Tick All'}
        </button>
      </div>

      {/* Entries Table - Opera Style */}
      <div className="border border-gray-400 bg-white">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-200 border-b border-gray-400">
              <th className="w-8 px-1 py-2 text-center border-r border-gray-300"></th>
              <th
                className="px-2 py-2 text-left font-medium text-gray-700 border-r border-gray-300 cursor-pointer hover:bg-gray-300"
                onClick={() => handleSort('ae_lstdate')}
              >
                <div className="flex items-center gap-1">
                  Date
                  <ArrowUpDown className="w-3 h-3" />
                </div>
              </th>
              <th
                className="px-2 py-2 text-left font-medium text-gray-700 border-r border-gray-300 cursor-pointer hover:bg-gray-300"
                onClick={() => handleSort('ae_entry')}
              >
                <div className="flex items-center gap-1">
                  Reference
                  <ArrowUpDown className="w-3 h-3" />
                </div>
              </th>
              <th className="px-2 py-2 text-left font-medium text-gray-700 border-r border-gray-300">
                Cashbook Type
              </th>
              <th
                className="px-2 py-2 text-right font-medium text-gray-700 border-r border-gray-300 cursor-pointer hover:bg-gray-300"
                onClick={() => handleSort('value_pounds')}
              >
                <div className="flex items-center justify-end gap-1">
                  Payments
                  <ArrowUpDown className="w-3 h-3" />
                </div>
              </th>
              <th className="px-2 py-2 text-right font-medium text-gray-700 border-r border-gray-300">
                Receipts
              </th>
              <th className="px-2 py-2 text-right font-medium text-gray-700 border-r border-gray-300">
                Balance
              </th>
              <th className="px-2 py-2 text-right font-medium text-gray-700 w-16">
                Line
              </th>
            </tr>
          </thead>
          <tbody>
            {entriesQuery.isLoading ? (
              <tr>
                <td colSpan={8} className="px-4 py-8 text-center text-gray-500">
                  <RefreshCw className="w-5 h-5 animate-spin mx-auto mb-2" />
                  Loading...
                </td>
              </tr>
            ) : filteredEntries.length === 0 ? (
              <tr>
                <td colSpan={8} className="px-4 py-8 text-center text-gray-500">
                  <CheckCircle className="w-5 h-5 mx-auto mb-2 text-green-500" />
                  No unreconciled entries
                </td>
              </tr>
            ) : (
              entriesWithBalance.map((entry, idx) => {
                const isSelected = selectedEntries.has(entry.ae_entry);
                const isPayment = entry.value_pounds < 0;

                return (
                  <tr
                    key={entry.ae_entry}
                    className={`border-b border-gray-200 cursor-pointer ${
                      isSelected ? 'bg-blue-100' : idx % 2 === 0 ? 'bg-white' : 'bg-gray-50'
                    } hover:bg-blue-50`}
                    onClick={() => toggleEntry(entry.ae_entry)}
                  >
                    <td className="px-1 py-1 text-center border-r border-gray-200">
                      <input
                        type="checkbox"
                        checked={isSelected}
                        onChange={() => toggleEntry(entry.ae_entry)}
                        onClick={e => e.stopPropagation()}
                        className="rounded border-gray-400"
                      />
                    </td>
                    <td className="px-2 py-1 border-r border-gray-200 text-gray-700">
                      {formatDate(entry.ae_lstdate)}
                    </td>
                    <td className="px-2 py-1 border-r border-gray-200 text-gray-900">
                      {entry.ae_entref?.trim() || entry.ae_entry}
                    </td>
                    <td className="px-2 py-1 border-r border-gray-200 text-gray-700">
                      {entry.ae_cbtype?.trim() || '-'}
                    </td>
                    <td className="px-2 py-1 border-r border-gray-200 text-right text-gray-900">
                      {isPayment ? formatCurrency(entry.value_pounds) : ''}
                    </td>
                    <td className="px-2 py-1 border-r border-gray-200 text-right text-gray-900">
                      {!isPayment ? formatCurrency(entry.value_pounds) : ''}
                    </td>
                    <td className="px-2 py-1 border-r border-gray-200 text-right text-gray-900">
                      {isSelected ? formatCurrency(entry.runningBalance) : ''}
                    </td>
                    <td className="px-2 py-1 text-right text-gray-900">
                      {entry.lineNumber || ''}
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>

      {/* Footer - Opera Style */}
      <div className="bg-gray-100 border border-t-0 border-gray-400 px-4 py-3 flex justify-between items-center">
        <div className="flex gap-8">
          <div>
            <span className="text-sm text-gray-600">Statement Balance: </span>
            <span className="font-medium">{formatCurrency(totals.statementBalance)}</span>
          </div>
          <div>
            <span className="text-sm text-gray-600">Reconciled: </span>
            <span className="font-medium">{formatCurrency(totals.reconciled)}</span>
          </div>
          <div>
            <span className="text-sm text-gray-600">Difference: </span>
            <span className={`font-medium ${Math.abs(totals.difference) < 0.01 ? 'text-green-600' : 'text-red-600'}`}>
              {formatCurrency(totals.difference)}
            </span>
          </div>
        </div>

        <div className="flex gap-2">
          <button
            onClick={() => markReconciledMutation.mutate()}
            disabled={selectedEntries.size === 0 || markReconciledMutation.isPending}
            className="px-4 py-1.5 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2 text-sm"
          >
            {markReconciledMutation.isPending ? (
              <RefreshCw className="w-4 h-4 animate-spin" />
            ) : (
              <CheckCircle className="w-4 h-4" />
            )}
            Post (F5)
          </button>
          <button
            onClick={() => setSelectedEntries(new Set())}
            className="px-4 py-1.5 border border-gray-400 rounded hover:bg-gray-100 text-sm"
          >
            Cancel
          </button>
        </div>
      </div>

      {/* Status Messages */}
      {markReconciledMutation.isSuccess && (
        <div className="mt-3 p-3 bg-green-50 border border-green-200 text-green-800 rounded text-sm">
          {markReconciledMutation.data?.message}
        </div>
      )}

      {markReconciledMutation.isError && (
        <div className="mt-3 p-3 bg-red-50 border border-red-200 text-red-800 rounded text-sm">
          Error: {markReconciledMutation.error?.message}
        </div>
      )}

      {/* Summary Info */}
      <div className="mt-4 text-xs text-gray-500">
        <span>{filteredEntries.length} unreconciled entries</span>
        {selectedEntries.size > 0 && (
          <span className="ml-4">{selectedEntries.size} selected for reconciliation</span>
        )}
      </div>
    </div>
  );
}

export default BankStatementReconcile;
