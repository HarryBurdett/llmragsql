import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  MessageSquare,
  Clock,
  AlertTriangle,
  CheckCircle,
  RefreshCw,
  Search,
  Calendar,
  Building,
} from 'lucide-react';
import apiClient from '../api/client';
import type { SupplierQueriesResponse } from '../api/client';

type StatusFilter = 'all' | 'open' | 'overdue' | 'resolved';

export function SupplierQueries() {
  const queryClient = useQueryClient();
  const [statusFilter, setStatusFilter] = useState<StatusFilter>('open');
  const [searchQuery, setSearchQuery] = useState('');

  const queriesQuery = useQuery<SupplierQueriesResponse>({
    queryKey: ['supplierQueries', statusFilter === 'all' ? undefined : statusFilter],
    queryFn: async () => {
      const response = await apiClient.supplierQueries(
        statusFilter === 'all' ? undefined : statusFilter
      );
      return response.data;
    },
    refetchInterval: 60000,
  });

  const resolveMutation = useMutation({
    mutationFn: async (queryId: number) => {
      const response = await apiClient.supplierQueryResolve(queryId);
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['supplierQueries'] });
      queryClient.invalidateQueries({ queryKey: ['supplierStatementDashboard'] });
    },
  });

  const queries = queriesQuery.data?.queries || [];
  const counts = queriesQuery.data?.counts || { open: 0, overdue: 0, resolved: 0 };

  const filteredQueries = queries.filter(q => {
    if (!searchQuery) return true;
    const search = searchQuery.toLowerCase();
    return (
      q.supplier_name?.toLowerCase().includes(search) ||
      q.supplier_code?.toLowerCase().includes(search) ||
      q.reference?.toLowerCase().includes(search) ||
      q.description?.toLowerCase().includes(search)
    );
  });

  const formatDate = (dateStr: string | null): string => {
    if (!dateStr) return '-';
    return new Date(dateStr).toLocaleDateString('en-GB', {
      day: '2-digit',
      month: 'short',
      year: 'numeric',
    });
  };

  const formatCurrency = (value: number | null): string => {
    if (value === null) return '-';
    return `£${Math.abs(value).toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  };

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'open':
        return 'bg-amber-100 text-amber-700 border border-amber-200';
      case 'overdue':
        return 'bg-red-100 text-red-700 border border-red-200';
      case 'resolved':
        return 'bg-emerald-100 text-emerald-700 border border-emerald-200';
      default:
        return 'bg-slate-100 text-slate-600 border border-slate-200';
    }
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-amber-100 rounded-lg">
            <MessageSquare className="h-6 w-6 text-amber-600" />
          </div>
          <div>
            <h1 className="text-2xl font-bold text-slate-900">Supplier Queries</h1>
            <p className="text-sm text-slate-500">Track and manage outstanding supplier queries</p>
          </div>
        </div>
        <button
          onClick={() => queriesQuery.refetch()}
          disabled={queriesQuery.isFetching}
          className="flex items-center gap-2 px-4 py-2 bg-slate-100 hover:bg-slate-200 rounded-lg transition-colors"
        >
          <RefreshCw className={`h-4 w-4 ${queriesQuery.isFetching ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </div>

      {/* Status Tabs */}
      <div className="flex gap-2 border-b border-slate-200 pb-4">
        <button
          onClick={() => setStatusFilter('all')}
          className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
            statusFilter === 'all'
              ? 'bg-slate-900 text-white'
              : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
          }`}
        >
          All ({counts.open + counts.overdue + counts.resolved})
        </button>
        <button
          onClick={() => setStatusFilter('open')}
          className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
            statusFilter === 'open'
              ? 'bg-amber-500 text-white'
              : 'bg-amber-50 text-amber-700 hover:bg-amber-100'
          }`}
        >
          <Clock className="h-4 w-4" />
          Open ({counts.open})
        </button>
        <button
          onClick={() => setStatusFilter('overdue')}
          className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
            statusFilter === 'overdue'
              ? 'bg-red-500 text-white'
              : 'bg-red-50 text-red-700 hover:bg-red-100'
          }`}
        >
          <AlertTriangle className="h-4 w-4" />
          Overdue ({counts.overdue})
        </button>
        <button
          onClick={() => setStatusFilter('resolved')}
          className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
            statusFilter === 'resolved'
              ? 'bg-emerald-500 text-white'
              : 'bg-emerald-50 text-emerald-700 hover:bg-emerald-100'
          }`}
        >
          <CheckCircle className="h-4 w-4" />
          Resolved ({counts.resolved})
        </button>
      </div>

      {/* Search */}
      <div className="relative">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-5 w-5 text-slate-400" />
        <input
          type="text"
          placeholder="Search by supplier, reference, or description..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          className="w-full pl-10 pr-4 py-2 border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-amber-500 focus:border-transparent"
        />
      </div>

      {/* Loading State */}
      {queriesQuery.isLoading && (
        <div className="flex items-center justify-center py-12">
          <RefreshCw className="h-8 w-8 text-slate-400 animate-spin" />
        </div>
      )}

      {/* Queries Table */}
      {!queriesQuery.isLoading && (
        <div className="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-slate-50 border-b border-slate-200">
                <tr>
                  <th className="text-left py-3 px-4 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                    Supplier
                  </th>
                  <th className="text-left py-3 px-4 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                    Query Type
                  </th>
                  <th className="text-left py-3 px-4 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                    Reference
                  </th>
                  <th className="text-right py-3 px-4 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                    Amount
                  </th>
                  <th className="text-left py-3 px-4 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                    Sent
                  </th>
                  <th className="text-center py-3 px-4 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                    Days Out
                  </th>
                  <th className="text-center py-3 px-4 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                    Status
                  </th>
                  <th className="text-right py-3 px-4 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100">
                {filteredQueries.length === 0 ? (
                  <tr>
                    <td colSpan={8} className="py-12 text-center text-slate-400">
                      <MessageSquare className="h-12 w-12 mx-auto mb-3 opacity-50" />
                      <p className="font-medium">No queries found</p>
                      <p className="text-sm">
                        {statusFilter === 'all'
                          ? 'No queries have been raised yet'
                          : `No ${statusFilter} queries`}
                      </p>
                    </td>
                  </tr>
                ) : (
                  filteredQueries.map((query) => (
                    <tr key={query.query_id} className="hover:bg-slate-50 transition-colors">
                      <td className="py-3 px-4">
                        <div className="flex items-center gap-2">
                          <Building className="h-4 w-4 text-slate-400" />
                          <div>
                            <p className="font-medium text-slate-900">{query.supplier_name}</p>
                            <p className="text-xs text-slate-500">{query.supplier_code}</p>
                          </div>
                        </div>
                      </td>
                      <td className="py-3 px-4">
                        <span className="text-sm text-slate-700">{query.query_type}</span>
                      </td>
                      <td className="py-3 px-4">
                        <div>
                          <p className="text-sm font-medium text-slate-700">{query.reference || '-'}</p>
                          <p className="text-xs text-slate-500 truncate max-w-[200px]">{query.description}</p>
                        </div>
                      </td>
                      <td className="py-3 px-4 text-right">
                        <span className="font-medium text-slate-900">
                          {query.debit ? formatCurrency(query.debit) : query.credit ? formatCurrency(query.credit) : '-'}
                        </span>
                      </td>
                      <td className="py-3 px-4">
                        <div className="flex items-center gap-1 text-sm text-slate-600">
                          <Calendar className="h-4 w-4" />
                          {formatDate(query.query_sent_at)}
                        </div>
                      </td>
                      <td className="py-3 px-4 text-center">
                        <span className={`font-semibold ${query.status === 'overdue' ? 'text-red-600' : 'text-slate-600'}`}>
                          {Math.round(query.days_outstanding || 0)}
                        </span>
                      </td>
                      <td className="py-3 px-4 text-center">
                        <span className={`inline-flex px-2.5 py-1 rounded-full text-xs font-medium ${getStatusBadge(query.status)}`}>
                          {query.status}
                        </span>
                      </td>
                      <td className="py-3 px-4 text-right">
                        {query.status !== 'resolved' && (
                          <button
                            onClick={() => resolveMutation.mutate(query.query_id)}
                            disabled={resolveMutation.isPending}
                            className="text-sm text-emerald-600 hover:text-emerald-700 font-medium"
                          >
                            Mark Resolved
                          </button>
                        )}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

export default SupplierQueries;
