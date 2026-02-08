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
  const [searchTerm, setSearchTerm] = useState<string>('');
  const [sortField, setSortField] = useState<'ae_entry' | 'value_pounds' | 'ae_lstdate'>('ae_entry');
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('desc');

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
    if (value === undefined || value === null) return '£0.00';
    return new Intl.NumberFormat('en-GB', {
      style: 'currency',
      currency: 'GBP',
    }).format(value);
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
      setSortDirection('desc');
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

  // Calculate selected total
  const selectedTotal = useMemo(() => {
    return filteredEntries
      .filter(e => selectedEntries.has(e.ae_entry))
      .reduce((sum, e) => sum + e.value_pounds, 0);
  }, [filteredEntries, selectedEntries]);

  return (
    <div className="p-6 max-w-7xl mx-auto">
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900 flex items-center gap-3">
          <Landmark className="w-7 h-7 text-blue-600" />
          Bank Statement Reconciliation
        </h1>
        <p className="text-gray-600 mt-1">
          Mark cashbook entries as reconciled against your bank statement
        </p>
      </div>

      {/* Bank Selector */}
      <div className="bg-white rounded-lg shadow p-4 mb-6">
        <div className="flex items-center gap-4 flex-wrap">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Bank Account
            </label>
            <select
              value={selectedBank}
              onChange={e => {
                setSelectedBank(e.target.value);
                setSelectedEntries(new Set());
              }}
              className="border border-gray-300 rounded-md px-3 py-2 min-w-[200px]"
            >
              {banksQuery.data?.banks?.map(bank => (
                <option key={bank.account_code} value={bank.account_code}>
                  {bank.account_code} - {bank.description}
                </option>
              ))}
            </select>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Statement Number
            </label>
            <input
              type="number"
              value={statementNumber}
              onChange={e => setStatementNumber(e.target.value)}
              placeholder={`Next: ${(statusQuery.data?.last_stmt_no || 0) + 1}`}
              className="border border-gray-300 rounded-md px-3 py-2 w-32"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Statement Date
            </label>
            <input
              type="date"
              value={statementDate}
              onChange={e => setStatementDate(e.target.value)}
              className="border border-gray-300 rounded-md px-3 py-2"
            />
          </div>

          <button
            onClick={() => {
              statusQuery.refetch();
              entriesQuery.refetch();
            }}
            className="mt-6 p-2 text-gray-600 hover:text-gray-800 hover:bg-gray-100 rounded"
            title="Refresh"
          >
            <RefreshCw className={`w-5 h-5 ${statusQuery.isFetching ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      {/* Status Cards */}
      {statusQuery.data && (
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
          <div className="bg-white rounded-lg shadow p-4">
            <div className="text-sm text-gray-600">Current Balance</div>
            <div className="text-xl font-bold text-gray-900">
              {formatCurrency(statusQuery.data.current_balance)}
            </div>
          </div>
          <div className="bg-white rounded-lg shadow p-4">
            <div className="text-sm text-gray-600">Reconciled Balance</div>
            <div className="text-xl font-bold text-green-600">
              {formatCurrency(statusQuery.data.reconciled_balance)}
            </div>
          </div>
          <div className="bg-white rounded-lg shadow p-4">
            <div className="text-sm text-gray-600">Unreconciled</div>
            <div className="text-xl font-bold text-orange-600">
              {formatCurrency(statusQuery.data.unreconciled_total)}
            </div>
            <div className="text-xs text-gray-500">
              {statusQuery.data.unreconciled_count} entries
            </div>
          </div>
          <div className="bg-white rounded-lg shadow p-4">
            <div className="text-sm text-gray-600">Last Statement</div>
            <div className="text-xl font-bold text-gray-900">
              #{statusQuery.data.last_stmt_no || 'N/A'}
            </div>
            <div className="text-xs text-gray-500">
              {statusQuery.data.last_stmt_date
                ? new Date(statusQuery.data.last_stmt_date).toLocaleDateString()
                : 'N/A'}
            </div>
          </div>
        </div>
      )}

      {/* Action Bar */}
      <div className="bg-white rounded-lg shadow p-4 mb-4">
        <div className="flex items-center justify-between flex-wrap gap-4">
          <div className="flex items-center gap-4">
            <div className="relative">
              <Search className="w-4 h-4 absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400" />
              <input
                type="text"
                placeholder="Search entries..."
                value={searchTerm}
                onChange={e => setSearchTerm(e.target.value)}
                className="pl-9 pr-3 py-2 border border-gray-300 rounded-md w-64"
              />
            </div>
            <div className="text-sm text-gray-600">
              {selectedEntries.size > 0 && (
                <span className="font-medium">
                  {selectedEntries.size} selected ({formatCurrency(selectedTotal)})
                </span>
              )}
            </div>
          </div>

          <div className="flex items-center gap-2">
            <button
              onClick={toggleAll}
              className="px-3 py-2 text-sm border border-gray-300 rounded-md hover:bg-gray-50 flex items-center gap-2"
            >
              {filteredEntries.length === selectedEntries.size ? (
                <CheckSquare className="w-4 h-4" />
              ) : (
                <Square className="w-4 h-4" />
              )}
              {filteredEntries.length === selectedEntries.size ? 'Deselect All' : 'Select All'}
            </button>

            <button
              onClick={() => markReconciledMutation.mutate()}
              disabled={selectedEntries.size === 0 || markReconciledMutation.isPending}
              className="px-4 py-2 bg-green-600 text-white rounded-md hover:bg-green-700 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
            >
              {markReconciledMutation.isPending ? (
                <RefreshCw className="w-4 h-4 animate-spin" />
              ) : (
                <CheckCircle className="w-4 h-4" />
              )}
              Mark Reconciled
            </button>
          </div>
        </div>

        {markReconciledMutation.isSuccess && (
          <div className="mt-3 p-3 bg-green-50 text-green-800 rounded-md text-sm">
            {markReconciledMutation.data?.message}
          </div>
        )}

        {markReconciledMutation.isError && (
          <div className="mt-3 p-3 bg-red-50 text-red-800 rounded-md text-sm">
            Error: {markReconciledMutation.error?.message}
          </div>
        )}
      </div>

      {/* Entries Table */}
      <div className="bg-white rounded-lg shadow overflow-hidden">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-4 py-3 text-left">
                <input
                  type="checkbox"
                  checked={filteredEntries.length > 0 && filteredEntries.length === selectedEntries.size}
                  onChange={toggleAll}
                  className="rounded border-gray-300"
                />
              </th>
              <th
                className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => handleSort('ae_entry')}
              >
                <div className="flex items-center gap-1">
                  Entry
                  <ArrowUpDown className="w-3 h-3" />
                </div>
              </th>
              <th
                className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => handleSort('value_pounds')}
              >
                <div className="flex items-center justify-end gap-1">
                  Amount
                  <ArrowUpDown className="w-3 h-3" />
                </div>
              </th>
              <th
                className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => handleSort('ae_lstdate')}
              >
                <div className="flex items-center gap-1">
                  Date
                  <ArrowUpDown className="w-3 h-3" />
                </div>
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Reference
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Type
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {entriesQuery.isLoading ? (
              <tr>
                <td colSpan={6} className="px-4 py-8 text-center text-gray-500">
                  <RefreshCw className="w-6 h-6 animate-spin mx-auto mb-2" />
                  Loading entries...
                </td>
              </tr>
            ) : filteredEntries.length === 0 ? (
              <tr>
                <td colSpan={6} className="px-4 py-8 text-center text-gray-500">
                  <CheckCircle className="w-6 h-6 mx-auto mb-2 text-green-500" />
                  No unreconciled entries
                </td>
              </tr>
            ) : (
              filteredEntries.map(entry => (
                <tr
                  key={entry.ae_entry}
                  className={`hover:bg-gray-50 cursor-pointer ${
                    selectedEntries.has(entry.ae_entry) ? 'bg-blue-50' : ''
                  }`}
                  onClick={() => toggleEntry(entry.ae_entry)}
                >
                  <td className="px-4 py-3">
                    <input
                      type="checkbox"
                      checked={selectedEntries.has(entry.ae_entry)}
                      onChange={() => toggleEntry(entry.ae_entry)}
                      onClick={e => e.stopPropagation()}
                      className="rounded border-gray-300"
                    />
                  </td>
                  <td className="px-4 py-3 text-sm font-mono text-gray-900">
                    {entry.ae_entry}
                  </td>
                  <td
                    className={`px-4 py-3 text-sm text-right font-medium ${
                      entry.value_pounds >= 0 ? 'text-green-600' : 'text-red-600'
                    }`}
                  >
                    {formatCurrency(entry.value_pounds)}
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-600">
                    {entry.ae_lstdate
                      ? new Date(entry.ae_lstdate).toLocaleDateString()
                      : '-'}
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-600">
                    {entry.ae_entref?.trim() || '-'}
                  </td>
                  <td className="px-4 py-3 text-sm">
                    <span
                      className={`px-2 py-1 rounded text-xs font-medium ${
                        entry.ae_cbtype?.startsWith('P')
                          ? 'bg-red-100 text-red-800'
                          : 'bg-green-100 text-green-800'
                      }`}
                    >
                      {entry.ae_cbtype?.trim() || '-'}
                    </span>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>

        {/* Table Footer */}
        {filteredEntries.length > 0 && (
          <div className="px-4 py-3 bg-gray-50 border-t border-gray-200 flex justify-between items-center">
            <div className="text-sm text-gray-600">
              Showing {filteredEntries.length} unreconciled entries
            </div>
            <div className="text-sm font-medium text-gray-900">
              Total: {formatCurrency(filteredEntries.reduce((sum, e) => sum + e.value_pounds, 0))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default BankStatementReconcile;
