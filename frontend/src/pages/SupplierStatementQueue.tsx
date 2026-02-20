import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  FileText,
  Search,
  Filter,
  RefreshCw,
  Eye,
  Play,
  Clock,
  CheckCircle,
  XCircle,
  AlertTriangle,
  Mail,
  Calendar,
  Building,
  ChevronDown,
  Inbox,
} from 'lucide-react';
import { Link } from 'react-router-dom';
import apiClient from '../api/client';
import type { SupplierStatementQueueItem, SupplierStatementQueueResponse } from '../api/client';
import { PageHeader, Card, LoadingState, EmptyState, Alert } from '../components/ui';

type StatusFilter = 'all' | 'received' | 'processing' | 'reconciled' | 'queued' | 'sent' | 'error';

const STATUS_CONFIG: Record<string, { label: string; color: string; icon: typeof Clock; bg: string }> = {
  received: {
    label: 'Received',
    color: 'text-blue-700',
    bg: 'bg-blue-100 border-blue-200',
    icon: Inbox,
  },
  processing: {
    label: 'Processing',
    color: 'text-amber-700',
    bg: 'bg-amber-100 border-amber-200',
    icon: RefreshCw,
  },
  reconciled: {
    label: 'Reconciled',
    color: 'text-emerald-700',
    bg: 'bg-emerald-100 border-emerald-200',
    icon: CheckCircle,
  },
  queued: {
    label: 'Awaiting Approval',
    color: 'text-violet-700',
    bg: 'bg-violet-100 border-violet-200',
    icon: Clock,
  },
  approved: {
    label: 'Approved',
    color: 'text-indigo-700',
    bg: 'bg-indigo-100 border-indigo-200',
    icon: CheckCircle,
  },
  sent: {
    label: 'Sent',
    color: 'text-gray-600',
    bg: 'bg-gray-100 border-gray-200',
    icon: Mail,
  },
  error: {
    label: 'Error',
    color: 'text-red-700',
    bg: 'bg-red-100 border-red-200',
    icon: XCircle,
  },
};

export function SupplierStatementQueue() {
  const queryClient = useQueryClient();
  const [statusFilter, setStatusFilter] = useState<StatusFilter>('all');
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedStatement, setSelectedStatement] = useState<SupplierStatementQueueItem | null>(null);
  const [showFilters, setShowFilters] = useState(false);

  // Fetch statements
  const statementsQuery = useQuery<SupplierStatementQueueResponse>({
    queryKey: ['supplierStatementQueue', statusFilter === 'all' ? undefined : statusFilter],
    queryFn: async () => {
      const response = await apiClient.supplierStatementQueue(
        statusFilter === 'all' ? undefined : statusFilter
      );
      return response.data;
    },
    refetchInterval: 30000, // Refresh every 30 seconds
  });

  // Process statement mutation
  const processMutation = useMutation({
    mutationFn: async (statementId: number) => {
      const response = await apiClient.supplierStatementProcess(statementId);
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['supplierStatementQueue'] });
      queryClient.invalidateQueries({ queryKey: ['supplierStatementDashboard'] });
    },
  });

  const statements = statementsQuery.data?.statements || [];

  // Filter statements by search query
  const filteredStatements = statements.filter((s) => {
    if (!searchQuery) return true;
    const query = searchQuery.toLowerCase();
    return (
      s.supplier_name?.toLowerCase().includes(query) ||
      s.supplier_code?.toLowerCase().includes(query) ||
      s.sender_email?.toLowerCase().includes(query)
    );
  });

  // Group statements by status for summary
  const statusCounts = statements.reduce((acc, s) => {
    acc[s.status] = (acc[s.status] || 0) + 1;
    return acc;
  }, {} as Record<string, number>);

  const formatDate = (dateStr: string | null): string => {
    if (!dateStr) return '-';
    return new Date(dateStr).toLocaleDateString('en-GB', {
      day: '2-digit',
      month: 'short',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  };

  const formatShortDate = (dateStr: string | null): string => {
    if (!dateStr) return '-';
    return new Date(dateStr).toLocaleDateString('en-GB', {
      day: '2-digit',
      month: 'short',
    });
  };

  const formatCurrency = (value: number | null): string => {
    if (value === null) return '-';
    return `£${Math.abs(value).toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  };

  const getStatusConfig = (status: string) => {
    return STATUS_CONFIG[status] || STATUS_CONFIG.received;
  };

  const getTimeSince = (dateStr: string): string => {
    const date = new Date(dateStr);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMins / 60);
    const diffDays = Math.floor(diffHours / 24);

    if (diffMins < 60) return `${diffMins}m ago`;
    if (diffHours < 24) return `${diffHours}h ago`;
    if (diffDays < 7) return `${diffDays}d ago`;
    return formatShortDate(dateStr);
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <PageHeader
        icon={Inbox}
        title="Statement Queue"
        subtitle="Incoming supplier statements awaiting processing"
      >
        <Link
          to="/supplier/dashboard"
          className="flex items-center gap-2 px-4 py-2 bg-gray-100 hover:bg-gray-200 rounded-lg transition-colors text-sm font-medium text-gray-700"
        >
          Dashboard
        </Link>
        <button
          onClick={() => statementsQuery.refetch()}
          disabled={statementsQuery.isFetching}
          className="flex items-center gap-2 px-4 py-2 bg-gray-100 hover:bg-gray-200 rounded-lg transition-colors text-sm font-medium text-gray-700"
        >
          <RefreshCw className={`h-4 w-4 ${statementsQuery.isFetching ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </PageHeader>

      {/* Status Summary Pills */}
      <div className="flex flex-wrap gap-2">
        <button
          onClick={() => setStatusFilter('all')}
          className={`px-4 py-2 rounded-lg text-sm font-medium transition-all ${
            statusFilter === 'all'
              ? 'bg-blue-600 text-white'
              : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
          }`}
        >
          All ({statements.length})
        </button>
        {Object.entries(STATUS_CONFIG).map(([key, config]) => {
          const count = statusCounts[key] || 0;
          if (count === 0 && key !== statusFilter) return null;
          return (
            <button
              key={key}
              onClick={() => setStatusFilter(key as StatusFilter)}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-all flex items-center gap-2 ${
                statusFilter === key
                  ? 'bg-blue-600 text-white'
                  : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
              }`}
            >
              <config.icon className="h-4 w-4" />
              {config.label} ({count})
            </button>
          );
        })}
      </div>

      {/* Search and Filters */}
      <Card>
        <div className="flex items-center gap-4">
          <div className="flex-1 relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-5 w-5 text-gray-400" />
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="Search by supplier name, code, or email..."
              className="w-full pl-10 pr-4 py-2.5 bg-gray-50 border border-gray-200 rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all"
            />
          </div>
          <button
            onClick={() => setShowFilters(!showFilters)}
            className={`flex items-center gap-2 px-4 py-2.5 rounded-xl border transition-all ${
              showFilters
                ? 'bg-blue-50 border-blue-200 text-blue-700'
                : 'bg-gray-50 border-gray-200 text-gray-600 hover:bg-gray-100'
            }`}
          >
            <Filter className="h-4 w-4" />
            Filters
            <ChevronDown className={`h-4 w-4 transition-transform ${showFilters ? 'rotate-180' : ''}`} />
          </button>
        </div>

        {/* Expanded Filters */}
        {showFilters && (
          <div className="mt-4 pt-4 border-t border-gray-100 grid grid-cols-3 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Date Range</label>
              <select className="w-full px-3 py-2 bg-gray-50 border border-gray-200 rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500">
                <option value="all">All time</option>
                <option value="today">Today</option>
                <option value="week">This week</option>
                <option value="month">This month</option>
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Sort By</label>
              <select className="w-full px-3 py-2 bg-gray-50 border border-gray-200 rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500">
                <option value="received_desc">Newest first</option>
                <option value="received_asc">Oldest first</option>
                <option value="supplier">Supplier name</option>
                <option value="balance">Balance (high to low)</option>
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Priority</label>
              <select className="w-full px-3 py-2 bg-gray-50 border border-gray-200 rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500">
                <option value="all">All priorities</option>
                <option value="high">High priority</option>
                <option value="normal">Normal</option>
              </select>
            </div>
          </div>
        )}
      </Card>

      {/* Loading State */}
      {statementsQuery.isLoading && (
        <Card>
          <LoadingState message="Loading statements..." size="lg" />
        </Card>
      )}

      {/* Error State */}
      {statementsQuery.isError && (
        <Alert variant="error" title="Error loading statements">
          {statementsQuery.error instanceof Error ? statementsQuery.error.message : 'Failed to load data'}
        </Alert>
      )}

      {/* Statement List */}
      {!statementsQuery.isLoading && !statementsQuery.isError && (
        <Card padding={false}>
          {filteredStatements.length === 0 ? (
            <EmptyState
              icon={FileText}
              title="No Statements Found"
              message={
                searchQuery
                  ? `No statements match "${searchQuery}"`
                  : statusFilter !== 'all'
                  ? `No statements with status "${STATUS_CONFIG[statusFilter]?.label || statusFilter}"`
                  : 'No statements have been received yet. Statements will appear here when suppliers send them.'
              }
            />
          ) : (
            <div className="divide-y divide-gray-100">
              {filteredStatements.map((statement) => {
                const statusConfig = getStatusConfig(statement.status);
                const StatusIcon = statusConfig.icon;

                return (
                  <div
                    key={statement.id}
                    className="p-5 hover:bg-gray-50 transition-colors"
                  >
                    <div className="flex items-start justify-between gap-4">
                      {/* Left: Supplier Info */}
                      <div className="flex items-start gap-4 flex-1">
                        <div className="p-3 bg-gray-100 rounded-xl">
                          <Building className="h-6 w-6 text-gray-500" />
                        </div>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-3 mb-1">
                            <h3 className="font-semibold text-gray-900 truncate">
                              {statement.supplier_name || statement.supplier_code}
                            </h3>
                            <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium border ${statusConfig.bg} ${statusConfig.color}`}>
                              <StatusIcon className="h-3.5 w-3.5" />
                              {statusConfig.label}
                            </span>
                          </div>
                          <div className="flex items-center gap-4 text-sm text-gray-500">
                            <span className="flex items-center gap-1">
                              <Calendar className="h-4 w-4" />
                              Statement: {formatShortDate(statement.statement_date)}
                            </span>
                            <span className="flex items-center gap-1">
                              <Mail className="h-4 w-4" />
                              {statement.sender_email || 'Unknown sender'}
                            </span>
                            <span className="text-gray-400">
                              Received {getTimeSince(statement.received_date)}
                            </span>
                          </div>
                          {statement.error_message && (
                            <div className="mt-2 flex items-center gap-2 text-sm text-red-600">
                              <AlertTriangle className="h-4 w-4" />
                              {statement.error_message}
                            </div>
                          )}
                        </div>
                      </div>

                      {/* Middle: Stats */}
                      <div className="flex items-center gap-6 text-center">
                        <div>
                          <div className="text-lg font-semibold text-gray-900">
                            {statement.line_count || 0}
                          </div>
                          <div className="text-xs text-gray-500">Lines</div>
                        </div>
                        {statement.matched_count > 0 && (
                          <div>
                            <div className="text-lg font-semibold text-emerald-600">
                              {statement.matched_count}
                            </div>
                            <div className="text-xs text-gray-500">Matched</div>
                          </div>
                        )}
                        {statement.query_count > 0 && (
                          <div>
                            <div className="text-lg font-semibold text-amber-600">
                              {statement.query_count}
                            </div>
                            <div className="text-xs text-gray-500">Queries</div>
                          </div>
                        )}
                        <div>
                          <div className="text-lg font-semibold text-gray-900">
                            {formatCurrency(statement.closing_balance)}
                          </div>
                          <div className="text-xs text-gray-500">Balance</div>
                        </div>
                      </div>

                      {/* Right: Actions */}
                      <div className="flex items-center gap-2">
                        <button
                          onClick={() => setSelectedStatement(statement)}
                          className="flex items-center gap-2 px-3 py-2 text-sm font-medium text-gray-600 hover:text-gray-900 hover:bg-gray-100 rounded-lg transition-colors"
                        >
                          <Eye className="h-4 w-4" />
                          View
                        </button>
                        {statement.status === 'received' && (
                          <button
                            onClick={() => processMutation.mutate(statement.id)}
                            disabled={processMutation.isPending}
                            className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 rounded-lg transition-colors disabled:opacity-50"
                          >
                            {processMutation.isPending ? (
                              <RefreshCw className="h-4 w-4 animate-spin" />
                            ) : (
                              <Play className="h-4 w-4" />
                            )}
                            Process
                          </button>
                        )}
                        {statement.status === 'queued' && (
                          <Link
                            to={`/supplier/statements/reconciliations?id=${statement.id}`}
                            className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-white bg-violet-600 hover:bg-violet-700 rounded-lg transition-colors"
                          >
                            <CheckCircle className="h-4 w-4" />
                            Review
                          </Link>
                        )}
                        {statement.status === 'error' && (
                          <button
                            onClick={() => processMutation.mutate(statement.id)}
                            disabled={processMutation.isPending}
                            className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-white bg-amber-600 hover:bg-amber-700 rounded-lg transition-colors disabled:opacity-50"
                          >
                            <RefreshCw className="h-4 w-4" />
                            Retry
                          </button>
                        )}
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </Card>
      )}

      {/* Statement Detail Modal */}
      {selectedStatement && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-2xl shadow-2xl max-w-2xl w-full max-h-[90vh] overflow-hidden">
            {/* Modal Header */}
            <div className="p-6 border-b border-gray-100">
              <div className="flex items-start justify-between">
                <div>
                  <h2 className="text-xl font-bold text-gray-900">
                    {selectedStatement.supplier_name || selectedStatement.supplier_code}
                  </h2>
                  <p className="text-gray-500">
                    Statement dated {formatShortDate(selectedStatement.statement_date)}
                  </p>
                </div>
                <button
                  onClick={() => setSelectedStatement(null)}
                  className="p-2 hover:bg-gray-100 rounded-lg transition-colors"
                >
                  <XCircle className="h-5 w-5 text-gray-400" />
                </button>
              </div>
            </div>

            {/* Modal Content */}
            <div className="p-6 overflow-y-auto max-h-[60vh]">
              <div className="grid grid-cols-2 gap-4 mb-6">
                <div className="bg-gray-50 rounded-xl p-4">
                  <div className="text-sm text-gray-500 mb-1">Status</div>
                  <div className={`inline-flex items-center gap-2 font-medium ${getStatusConfig(selectedStatement.status).color}`}>
                    {(() => {
                      const StatusIcon = getStatusConfig(selectedStatement.status).icon;
                      return <StatusIcon className="h-5 w-5" />;
                    })()}
                    {getStatusConfig(selectedStatement.status).label}
                  </div>
                </div>
                <div className="bg-gray-50 rounded-xl p-4">
                  <div className="text-sm text-gray-500 mb-1">Closing Balance</div>
                  <div className="text-xl font-bold text-gray-900">
                    {formatCurrency(selectedStatement.closing_balance)}
                  </div>
                </div>
              </div>

              <div className="space-y-4">
                <div className="flex items-center justify-between py-3 border-b border-gray-100">
                  <span className="text-gray-500">Supplier Code</span>
                  <span className="font-medium text-gray-900">{selectedStatement.supplier_code}</span>
                </div>
                <div className="flex items-center justify-between py-3 border-b border-gray-100">
                  <span className="text-gray-500">Received</span>
                  <span className="font-medium text-gray-900">{formatDate(selectedStatement.received_date)}</span>
                </div>
                <div className="flex items-center justify-between py-3 border-b border-gray-100">
                  <span className="text-gray-500">Sender Email</span>
                  <span className="font-medium text-gray-900">{selectedStatement.sender_email || '-'}</span>
                </div>
                <div className="flex items-center justify-between py-3 border-b border-gray-100">
                  <span className="text-gray-500">Opening Balance</span>
                  <span className="font-medium text-gray-900">{formatCurrency(selectedStatement.opening_balance)}</span>
                </div>
                <div className="flex items-center justify-between py-3 border-b border-gray-100">
                  <span className="text-gray-500">Line Items</span>
                  <span className="font-medium text-gray-900">{selectedStatement.line_count || 0}</span>
                </div>
                {selectedStatement.matched_count > 0 && (
                  <div className="flex items-center justify-between py-3 border-b border-gray-100">
                    <span className="text-gray-500">Matched Items</span>
                    <span className="font-medium text-emerald-600">{selectedStatement.matched_count}</span>
                  </div>
                )}
                {selectedStatement.query_count > 0 && (
                  <div className="flex items-center justify-between py-3 border-b border-gray-100">
                    <span className="text-gray-500">Queries Raised</span>
                    <span className="font-medium text-amber-600">{selectedStatement.query_count}</span>
                  </div>
                )}
                {selectedStatement.acknowledged_at && (
                  <div className="flex items-center justify-between py-3 border-b border-gray-100">
                    <span className="text-gray-500">Acknowledged</span>
                    <span className="font-medium text-gray-900">{formatDate(selectedStatement.acknowledged_at)}</span>
                  </div>
                )}
                {selectedStatement.processed_at && (
                  <div className="flex items-center justify-between py-3 border-b border-gray-100">
                    <span className="text-gray-500">Processed</span>
                    <span className="font-medium text-gray-900">{formatDate(selectedStatement.processed_at)}</span>
                  </div>
                )}
                {selectedStatement.approved_by && (
                  <div className="flex items-center justify-between py-3 border-b border-gray-100">
                    <span className="text-gray-500">Approved By</span>
                    <span className="font-medium text-gray-900">{selectedStatement.approved_by}</span>
                  </div>
                )}
                {selectedStatement.sent_at && (
                  <div className="flex items-center justify-between py-3 border-b border-gray-100">
                    <span className="text-gray-500">Response Sent</span>
                    <span className="font-medium text-gray-900">{formatDate(selectedStatement.sent_at)}</span>
                  </div>
                )}
                {selectedStatement.error_message && (
                  <div className="py-3">
                    <span className="text-gray-500 block mb-2">Error</span>
                    <div className="bg-red-50 border border-red-200 rounded-lg p-3 text-sm text-red-700">
                      {selectedStatement.error_message}
                    </div>
                  </div>
                )}
              </div>
            </div>

            {/* Modal Footer */}
            <div className="p-6 border-t border-gray-100 bg-gray-50 flex justify-end gap-3">
              <button
                onClick={() => setSelectedStatement(null)}
                className="px-4 py-2 text-sm font-medium text-gray-600 hover:text-gray-900 transition-colors"
              >
                Close
              </button>
              {selectedStatement.status === 'received' && (
                <button
                  onClick={() => {
                    processMutation.mutate(selectedStatement.id);
                    setSelectedStatement(null);
                  }}
                  className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 rounded-lg transition-colors"
                >
                  <Play className="h-4 w-4" />
                  Process Statement
                </button>
              )}
              {selectedStatement.status === 'queued' && (
                <Link
                  to={`/supplier/statements/reconciliations?id=${selectedStatement.id}`}
                  className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-white bg-violet-600 hover:bg-violet-700 rounded-lg transition-colors"
                >
                  <CheckCircle className="h-4 w-4" />
                  Review & Approve
                </Link>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default SupplierStatementQueue;
