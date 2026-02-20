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
import { PageHeader, LoadingState, EmptyState, StatusBadge, Card } from '../components/ui';

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

  return (
    <div className="space-y-6">
      {/* Header */}
      <PageHeader icon={Scale} title="Reconciliations" subtitle="Review and approve reconciled statements">
        <button
          onClick={() => reconciliationsQuery.refetch()}
          disabled={reconciliationsQuery.isFetching}
          className="flex items-center gap-2 px-4 py-2 bg-gray-100 hover:bg-gray-200 rounded-lg transition-colors"
        >
          <RefreshCw className={`h-4 w-4 ${reconciliationsQuery.isFetching ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </PageHeader>

      {/* Search */}
      <div className="relative">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-5 w-5 text-gray-400" />
        <input
          type="text"
          placeholder="Search by supplier..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          className="w-full pl-10 pr-4 py-2 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
        />
      </div>

      {/* Loading State */}
      {reconciliationsQuery.isLoading && (
        <LoadingState message="Loading reconciliations..." />
      )}

      {/* Reconciliations Table */}
      {!reconciliationsQuery.isLoading && (
        <Card padding={false}>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-gray-50 border-b border-gray-200">
                <tr>
                  <th className="text-left py-3 px-4 text-xs font-semibold text-gray-500 uppercase tracking-wider">
                    Supplier
                  </th>
                  <th className="text-left py-3 px-4 text-xs font-semibold text-gray-500 uppercase tracking-wider">
                    Statement Date
                  </th>
                  <th className="text-left py-3 px-4 text-xs font-semibold text-gray-500 uppercase tracking-wider">
                    Received
                  </th>
                  <th className="text-left py-3 px-4 text-xs font-semibold text-gray-500 uppercase tracking-wider">
                    Processed
                  </th>
                  <th className="text-center py-3 px-4 text-xs font-semibold text-gray-500 uppercase tracking-wider">
                    Status
                  </th>
                  <th className="text-right py-3 px-4 text-xs font-semibold text-gray-500 uppercase tracking-wider">
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {filteredStatements.length === 0 ? (
                  <tr>
                    <td colSpan={6} className="py-12 text-center">
                      <EmptyState icon={Scale} title="No pending reconciliations" message="Processed statements awaiting approval will appear here" />
                    </td>
                  </tr>
                ) : (
                  filteredStatements.map((statement) => (
                    <tr key={statement.id} className="hover:bg-gray-50 transition-colors">
                      <td className="py-3 px-4">
                        <div className="flex items-center gap-2">
                          <Building className="h-4 w-4 text-gray-400" />
                          <div>
                            <p className="text-sm font-medium text-gray-900">{statement.supplier_name}</p>
                            <p className="text-xs text-gray-500">{statement.supplier_code}</p>
                          </div>
                        </div>
                      </td>
                      <td className="py-3 px-4">
                        <div className="flex items-center gap-1 text-sm text-gray-600">
                          <Calendar className="h-4 w-4" />
                          {formatDate(statement.statement_date)}
                        </div>
                      </td>
                      <td className="py-3 px-4 text-sm text-gray-600">
                        {formatDate(statement.received_date)}
                      </td>
                      <td className="py-3 px-4 text-sm text-gray-600">
                        {formatDate(statement.processed_at)}
                      </td>
                      <td className="py-3 px-4 text-center">
                        <StatusBadge variant={statement.status === 'queued' ? 'warning' : 'success'}>
                          <span className="flex items-center gap-1">
                            {statement.status === 'queued' ? (
                              <Clock className="h-3 w-3" />
                            ) : (
                              <CheckCircle className="h-3 w-3" />
                            )}
                            {statement.status === 'queued' ? 'Pending Approval' : 'Reconciled'}
                          </span>
                        </StatusBadge>
                      </td>
                      <td className="py-3 px-4 text-right">
                        <div className="flex items-center justify-end gap-2">
                          <Link
                            to={`/supplier/statements/queue?view=${statement.id}`}
                            className="p-2 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-lg transition-colors"
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
        </Card>
      )}
    </div>
  );
}

export default SupplierReconciliations;
