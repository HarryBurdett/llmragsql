import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  Scale,
  RefreshCw,
  Search,
  Calendar,
  Building,
  CheckCircle,
  Clock,
  Eye,
} from 'lucide-react';
import { Link } from 'react-router-dom';
import apiClient from '../api/client';
import type { SupplierStatementQueueResponse } from '../api/client';

export function SupplierReconciliations() {
  const queryClient = useQueryClient();
  const [searchQuery, setSearchQuery] = useState('');

  const reconciliationsQuery = useQuery<SupplierStatementQueueResponse>({
    queryKey: ['supplierReconciliations'],
    queryFn: async () => {
      const response = await apiClient.supplierStatementReconciliations();
      return response.data;
    },
    refetchInterval: 30000,
  });

  const approveMutation = useMutation({
    mutationFn: async (statementId: number) => {
      const response = await apiClient.supplierStatementApprove(statementId, 'User');
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['supplierReconciliations'] });
      queryClient.invalidateQueries({ queryKey: ['supplierStatementDashboard'] });
    },
  });

  const statements = reconciliationsQuery.data?.statements || [];

  const filteredStatements = statements.filter(s => {
    if (!searchQuery) return true;
    const search = searchQuery.toLowerCase();
    return (
      s.supplier_name?.toLowerCase().includes(search) ||
      s.supplier_code?.toLowerCase().includes(search)
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

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'reconciled':
        return 'bg-emerald-100 text-emerald-700 border border-emerald-200';
      case 'queued':
        return 'bg-violet-100 text-violet-700 border border-violet-200';
      default:
        return 'bg-slate-100 text-slate-600 border border-slate-200';
    }
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-violet-100 rounded-lg">
            <Scale className="h-6 w-6 text-violet-600" />
          </div>
          <div>
            <h1 className="text-2xl font-bold text-slate-900">Reconciliations</h1>
            <p className="text-sm text-slate-500">Review and approve reconciled statements</p>
          </div>
        </div>
        <button
          onClick={() => reconciliationsQuery.refetch()}
          disabled={reconciliationsQuery.isFetching}
          className="flex items-center gap-2 px-4 py-2 bg-slate-100 hover:bg-slate-200 rounded-lg transition-colors"
        >
          <RefreshCw className={`h-4 w-4 ${reconciliationsQuery.isFetching ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </div>

      {/* Search */}
      <div className="relative">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-5 w-5 text-slate-400" />
        <input
          type="text"
          placeholder="Search by supplier..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          className="w-full pl-10 pr-4 py-2 border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-violet-500 focus:border-transparent"
        />
      </div>

      {/* Loading State */}
      {reconciliationsQuery.isLoading && (
        <div className="flex items-center justify-center py-12">
          <RefreshCw className="h-8 w-8 text-slate-400 animate-spin" />
        </div>
      )}

      {/* Reconciliations Table */}
      {!reconciliationsQuery.isLoading && (
        <div className="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-slate-50 border-b border-slate-200">
                <tr>
                  <th className="text-left py-3 px-4 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                    Supplier
                  </th>
                  <th className="text-left py-3 px-4 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                    Statement Date
                  </th>
                  <th className="text-left py-3 px-4 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                    Received
                  </th>
                  <th className="text-left py-3 px-4 text-xs font-semibold text-slate-500 uppercase tracking-wider">
                    Processed
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
                {filteredStatements.length === 0 ? (
                  <tr>
                    <td colSpan={6} className="py-12 text-center text-slate-400">
                      <Scale className="h-12 w-12 mx-auto mb-3 opacity-50" />
                      <p className="font-medium">No pending reconciliations</p>
                      <p className="text-sm">Processed statements awaiting approval will appear here</p>
                    </td>
                  </tr>
                ) : (
                  filteredStatements.map((statement) => (
                    <tr key={statement.id} className="hover:bg-slate-50 transition-colors">
                      <td className="py-3 px-4">
                        <div className="flex items-center gap-2">
                          <Building className="h-4 w-4 text-slate-400" />
                          <div>
                            <p className="font-medium text-slate-900">{statement.supplier_name}</p>
                            <p className="text-xs text-slate-500">{statement.supplier_code}</p>
                          </div>
                        </div>
                      </td>
                      <td className="py-3 px-4">
                        <div className="flex items-center gap-1 text-sm text-slate-600">
                          <Calendar className="h-4 w-4" />
                          {formatDate(statement.statement_date)}
                        </div>
                      </td>
                      <td className="py-3 px-4 text-sm text-slate-600">
                        {formatDate(statement.received_date)}
                      </td>
                      <td className="py-3 px-4 text-sm text-slate-600">
                        {formatDate(statement.processed_at)}
                      </td>
                      <td className="py-3 px-4 text-center">
                        <span className={`inline-flex items-center gap-1 px-2.5 py-1 rounded-full text-xs font-medium ${getStatusBadge(statement.status)}`}>
                          {statement.status === 'queued' ? (
                            <Clock className="h-3 w-3" />
                          ) : (
                            <CheckCircle className="h-3 w-3" />
                          )}
                          {statement.status === 'queued' ? 'Pending Approval' : 'Reconciled'}
                        </span>
                      </td>
                      <td className="py-3 px-4 text-right">
                        <div className="flex items-center justify-end gap-2">
                          <Link
                            to={`/supplier/statements/queue?view=${statement.id}`}
                            className="p-2 text-slate-400 hover:text-slate-600 hover:bg-slate-100 rounded-lg transition-colors"
                            title="View Details"
                          >
                            <Eye className="h-4 w-4" />
                          </Link>
                          {statement.status === 'queued' && (
                            <button
                              onClick={() => approveMutation.mutate(statement.id)}
                              disabled={approveMutation.isPending}
                              className="px-3 py-1.5 bg-emerald-600 text-white text-sm font-medium rounded-lg hover:bg-emerald-700 transition-colors disabled:opacity-50"
                            >
                              Approve
                            </button>
                          )}
                        </div>
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

export default SupplierReconciliations;
