import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  Archive,
  RefreshCw,
  Search,
  Calendar,
  Building,
  CheckCircle,
} from 'lucide-react';
import apiClient from '../api/client';
import type { SupplierStatementQueueResponse } from '../api/client';
import { PageHeader, LoadingState, EmptyState, Card } from '../components/ui';

export function SupplierStatementHistory() {
  const [searchQuery, setSearchQuery] = useState('');
  const [days, setDays] = useState(90);

  const historyQuery = useQuery<SupplierStatementQueueResponse>({
    queryKey: ['supplierStatementHistory', days],
    queryFn: async () => {
      const response = await apiClient.supplierStatementHistory(days);
      return response.data;
    },
  });

  const statements = historyQuery.data?.statements || [];

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

  const formatCurrency = (value: number | null): string => {
    if (value === null || value === undefined) return '-';
    return `£${Math.abs(value).toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <PageHeader icon={Archive} title="Statement History" subtitle="Completed and sent supplier statements">
        <select
          value={days}
          onChange={(e) => setDays(Number(e.target.value))}
          className="px-3 py-2 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          <option value={30}>Last 30 days</option>
          <option value={90}>Last 90 days</option>
          <option value={180}>Last 6 months</option>
          <option value={365}>Last year</option>
        </select>
        <button
          onClick={() => historyQuery.refetch()}
          disabled={historyQuery.isFetching}
          className="flex items-center gap-2 px-4 py-2 bg-gray-100 hover:bg-gray-200 rounded-lg transition-colors"
        >
          <RefreshCw className={`h-4 w-4 ${historyQuery.isFetching ? 'animate-spin' : ''}`} />
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
      {historyQuery.isLoading && (
        <LoadingState message="Loading statement history..." />
      )}

      {/* History Table */}
      {!historyQuery.isLoading && (
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
                    Sent
                  </th>
                  <th className="text-center py-3 px-4 text-xs font-semibold text-gray-500 uppercase tracking-wider">
                    Lines
                  </th>
                  <th className="text-center py-3 px-4 text-xs font-semibold text-gray-500 uppercase tracking-wider">
                    Matched
                  </th>
                  <th className="text-right py-3 px-4 text-xs font-semibold text-gray-500 uppercase tracking-wider">
                    Balance
                  </th>
                  <th className="text-left py-3 px-4 text-xs font-semibold text-gray-500 uppercase tracking-wider">
                    Approved By
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {filteredStatements.length === 0 ? (
                  <tr>
                    <td colSpan={8} className="py-12 text-center">
                      <EmptyState icon={Archive} title="No history found" message="Completed statements will appear here" />
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
                        {formatDate(statement.sent_at)}
                      </td>
                      <td className="py-3 px-4 text-center">
                        <span className="text-sm font-medium text-gray-700">{statement.line_count || 0}</span>
                      </td>
                      <td className="py-3 px-4 text-center">
                        <div className="flex items-center justify-center gap-1">
                          <CheckCircle className="h-4 w-4 text-emerald-500" />
                          <span className="text-sm font-medium text-emerald-600">
                            {statement.matched_count || 0}
                          </span>
                        </div>
                      </td>
                      <td className="py-3 px-4 text-right text-sm font-medium text-gray-900">
                        {formatCurrency(statement.closing_balance)}
                      </td>
                      <td className="py-3 px-4 text-sm text-gray-600">
                        {statement.approved_by || '-'}
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

export default SupplierStatementHistory;
