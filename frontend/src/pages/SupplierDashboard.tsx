import { useQuery } from '@tanstack/react-query';
import {
  FileText,
  CheckCircle,
  Clock,
  MessageSquare,
  RefreshCw,
  Shield,
  XCircle,
  ArrowRight,
  Mail,
  Calendar,
  Building,
  Activity,
  Zap,
  BarChart3,
  Bell,
} from 'lucide-react';
import { Link } from 'react-router-dom';
import apiClient from '../api/client';
import type { SupplierStatementDashboardResponse } from '../api/client';
import { PageHeader, Card, LoadingState, EmptyState, Alert } from '../components/ui';

export function SupplierDashboard() {
  const dashboardQuery = useQuery<SupplierStatementDashboardResponse>({
    queryKey: ['supplierStatementDashboard'],
    queryFn: async () => {
      const response = await apiClient.supplierStatementDashboard();
      return response.data;
    },
    refetchInterval: 60000, // Refresh every minute
  });

  const dashboard = dashboardQuery.data;

  const formatDate = (dateStr: string | null): string => {
    if (!dateStr) return '-';
    return new Date(dateStr).toLocaleDateString('en-GB', {
      day: '2-digit',
      month: 'short',
      hour: '2-digit',
      minute: '2-digit',
    });
  };

  const formatCurrency = (value: number): string => {
    return `£${Math.abs(value).toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  };

  const getStatusBadge = (status: string) => {
    const styles: Record<string, string> = {
      received: 'bg-blue-100 text-blue-700 border border-blue-200',
      processing: 'bg-amber-100 text-amber-700 border border-amber-200',
      reconciled: 'bg-emerald-100 text-emerald-700 border border-emerald-200',
      queued: 'bg-violet-100 text-violet-700 border border-violet-200',
      sent: 'bg-gray-100 text-gray-600 border border-gray-200',
      error: 'bg-red-100 text-red-700 border border-red-200',
    };
    return styles[status?.toLowerCase()] || 'bg-gray-100 text-gray-600 border border-gray-200';
  };

  const getQueryStatusBadge = (status: string) => {
    const styles: Record<string, string> = {
      open: 'bg-amber-100 text-amber-700 border border-amber-200',
      overdue: 'bg-red-100 text-red-700 border border-red-200',
      resolved: 'bg-emerald-100 text-emerald-700 border border-emerald-200',
    };
    return styles[status?.toLowerCase()] || 'bg-gray-100 text-gray-600 border border-gray-200';
  };

  // Count total alerts
  const totalAlerts =
    (dashboard?.alerts?.security_alerts?.length || 0) +
    (dashboard?.alerts?.overdue_queries?.length || 0) +
    (dashboard?.alerts?.failed_processing?.length || 0);

  return (
    <div className="space-y-6">
      {/* Header */}
      <PageHeader
        icon={Zap}
        title="Supplier Statement Automation"
        subtitle="Intelligent processing, reconciliation & supplier communications"
      >
        <button
          onClick={() => dashboardQuery.refetch()}
          disabled={dashboardQuery.isFetching}
          className="flex items-center gap-2 px-4 py-2 bg-gray-100 hover:bg-gray-200 rounded-lg transition-colors text-sm font-medium text-gray-700"
        >
          <RefreshCw className={`h-4 w-4 ${dashboardQuery.isFetching ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </PageHeader>

      {/* Quick Stats Row */}
      {dashboard && (
        <div className="grid grid-cols-4 gap-4">
          <Card>
            <div className="text-3xl font-bold text-gray-900">{dashboard.kpis?.statements_week || 0}</div>
            <div className="text-sm text-gray-500">This Week</div>
          </Card>
          <Card>
            <div className="text-3xl font-bold text-gray-900">{dashboard.kpis?.pending_approvals || 0}</div>
            <div className="text-sm text-gray-500">Pending Approval</div>
          </Card>
          <Card>
            <div className="text-3xl font-bold text-gray-900">{dashboard.kpis?.open_queries || 0}</div>
            <div className="text-sm text-gray-500">Open Queries</div>
          </Card>
          <Card>
            <div className="text-3xl font-bold text-gray-900">
              {dashboard.kpis?.match_rate_percent ? `${dashboard.kpis.match_rate_percent}%` : '-'}
            </div>
            <div className="text-sm text-gray-500">Match Rate</div>
          </Card>
        </div>
      )}

      {/* Loading State */}
      {dashboardQuery.isLoading && (
        <LoadingState message="Loading dashboard..." size="lg" />
      )}

      {/* Error State */}
      {dashboardQuery.isError && (
        <Alert variant="error" title="Error loading dashboard">
          {dashboardQuery.error instanceof Error ? dashboardQuery.error.message : 'Failed to load data'}
        </Alert>
      )}

      {dashboard && (
        <>
          {/* KPI Cards Grid */}
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-5">
            {/* Statements Today */}
            <Card>
              <div className="flex items-center justify-between mb-4">
                <div className="p-2.5 bg-blue-50 rounded-xl">
                  <FileText className="h-5 w-5 text-blue-600" />
                </div>
                <span className="text-xs font-medium px-2 py-1 bg-blue-50 text-blue-600 rounded-full">
                  {dashboard.kpis?.statements_today || 0} today
                </span>
              </div>
              <div className="text-3xl font-bold text-gray-900">
                {dashboard.kpis?.statements_week || 0}
              </div>
              <div className="text-sm text-gray-500">Statements This Week</div>
            </Card>

            {/* Pending Approvals */}
            <Link to="/supplier/statements/reconciliations">
              <Card>
                <div className="flex items-center justify-between mb-4">
                  <div className="p-2.5 bg-blue-50 rounded-xl">
                    <Clock className="h-5 w-5 text-blue-600" />
                  </div>
                  <ArrowRight className="h-4 w-4 text-gray-300" />
                </div>
                <div className="text-3xl font-bold text-gray-900">
                  {dashboard.kpis?.pending_approvals || 0}
                </div>
                <div className="text-sm text-gray-500">Pending Approvals</div>
              </Card>
            </Link>

            {/* Open Queries */}
            <Link to="/supplier/queries/open">
              <Card>
                <div className="flex items-center justify-between mb-4">
                  <div className="p-2.5 bg-amber-50 rounded-xl">
                    <MessageSquare className="h-5 w-5 text-amber-600" />
                  </div>
                  {dashboard.kpis?.overdue_queries > 0 && (
                    <span className="text-xs font-medium px-2 py-1 bg-red-50 text-red-600 rounded-full">
                      {dashboard.kpis.overdue_queries} overdue
                    </span>
                  )}
                </div>
                <div className="text-3xl font-bold text-gray-900">
                  {dashboard.kpis?.open_queries || 0}
                </div>
                <div className="text-sm text-gray-500">Open Queries</div>
              </Card>
            </Link>

            {/* Processing Time */}
            <Card>
              <div className="flex items-center justify-between mb-4">
                <div className="p-2.5 bg-emerald-50 rounded-xl">
                  <Activity className="h-5 w-5 text-emerald-600" />
                </div>
              </div>
              <div className="text-3xl font-bold text-gray-900">
                {dashboard.kpis?.avg_processing_hours ? `${dashboard.kpis.avg_processing_hours}h` : '-'}
              </div>
              <div className="text-sm text-gray-500">Avg Processing Time</div>
            </Card>

            {/* Match Rate */}
            <Card>
              <div className="flex items-center justify-between mb-4">
                <div className="p-2.5 bg-blue-50 rounded-xl">
                  <BarChart3 className="h-5 w-5 text-blue-600" />
                </div>
              </div>
              <div className="text-3xl font-bold text-gray-900">
                {dashboard.kpis?.match_rate_percent ? `${dashboard.kpis.match_rate_percent}%` : '-'}
              </div>
              <div className="text-sm text-gray-500">Match Rate</div>
            </Card>
          </div>

          {/* Alerts Section */}
          {totalAlerts > 0 && (
            <Alert variant="warning" title={`Alerts Requiring Attention - ${totalAlerts} issue${totalAlerts !== 1 ? 's' : ''} need your review`}>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-3">
                {/* Security Alerts */}
                {dashboard.alerts?.security_alerts?.length > 0 && (
                  <Card>
                    <div className="flex items-center gap-3 mb-3">
                      <Shield className="h-5 w-5 text-red-600" />
                      <span className="text-base font-semibold text-gray-900">Security Alerts</span>
                    </div>
                    <p className="text-sm text-gray-600 mb-3">
                      {dashboard.alerts.security_alerts.length} bank detail change{dashboard.alerts.security_alerts.length !== 1 ? 's' : ''} pending verification
                    </p>
                    <Link
                      to="/supplier/security/alerts"
                      className="inline-flex items-center gap-2 text-sm font-medium text-red-600 hover:text-red-700 transition-colors"
                    >
                      Review now <ArrowRight className="h-4 w-4" />
                    </Link>
                  </Card>
                )}

                {/* Overdue Queries */}
                {dashboard.alerts?.overdue_queries?.length > 0 && (
                  <Card>
                    <div className="flex items-center gap-3 mb-3">
                      <Clock className="h-5 w-5 text-amber-600" />
                      <span className="text-base font-semibold text-gray-900">Overdue Queries</span>
                    </div>
                    <p className="text-sm text-gray-600 mb-3">
                      {dashboard.alerts.overdue_queries.length} quer{dashboard.alerts.overdue_queries.length !== 1 ? 'ies' : 'y'} past response deadline
                    </p>
                    <Link
                      to="/supplier/queries/overdue"
                      className="inline-flex items-center gap-2 text-sm font-medium text-amber-600 hover:text-amber-700 transition-colors"
                    >
                      View queries <ArrowRight className="h-4 w-4" />
                    </Link>
                  </Card>
                )}

                {/* Failed Processing */}
                {dashboard.alerts?.failed_processing?.length > 0 && (
                  <Card>
                    <div className="flex items-center gap-3 mb-3">
                      <XCircle className="h-5 w-5 text-red-600" />
                      <span className="text-base font-semibold text-gray-900">Failed Processing</span>
                    </div>
                    <p className="text-sm text-gray-600 mb-3">
                      {dashboard.alerts.failed_processing.length} statement{dashboard.alerts.failed_processing.length !== 1 ? 's' : ''} failed extraction
                    </p>
                    <Link
                      to="/supplier/statements/queue"
                      className="inline-flex items-center gap-2 text-sm font-medium text-red-600 hover:text-red-700 transition-colors"
                    >
                      Review errors <ArrowRight className="h-4 w-4" />
                    </Link>
                  </Card>
                )}
              </div>
            </Alert>
          )}

          {/* Activity Grid */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Recent Statements */}
            <Card title="Recent Statements" icon={Mail} padding={false}>
              <div className="px-5 pb-2 pt-0 flex justify-end -mt-2">
                <Link to="/supplier/statements/queue" className="text-sm font-medium text-blue-600 hover:text-blue-700 transition-colors">
                  View all
                </Link>
              </div>
              <div className="divide-y divide-gray-100">
                {dashboard.recent_statements?.length === 0 ? (
                  <EmptyState icon={FileText} title="No recent statements" message="Statements will appear here when received" />
                ) : (
                  dashboard.recent_statements?.slice(0, 5).map((statement) => (
                    <div
                      key={statement.id}
                      className="flex items-center justify-between p-4 hover:bg-gray-50 transition-colors"
                    >
                      <div className="flex items-center gap-3">
                        <div className="p-2 bg-gray-100 rounded-lg">
                          <Building className="h-4 w-4 text-gray-500" />
                        </div>
                        <div>
                          <p className="text-sm font-medium text-gray-900">{statement.supplier_name}</p>
                          <p className="text-xs text-gray-500 flex items-center gap-1">
                            <Calendar className="h-3 w-3" />
                            {formatDate(statement.received_date)}
                          </p>
                        </div>
                      </div>
                      <div className="text-right">
                        <span className={`inline-flex px-2.5 py-1 rounded-full text-xs font-medium ${getStatusBadge(statement.status)}`}>
                          {statement.status}
                        </span>
                        {statement.closing_balance !== null && (
                          <p className="text-sm font-medium text-gray-600 mt-1">
                            {formatCurrency(statement.closing_balance)}
                          </p>
                        )}
                      </div>
                    </div>
                  ))
                )}
              </div>
            </Card>

            {/* Recent Queries */}
            <Card title="Recent Queries" icon={MessageSquare} padding={false}>
              <div className="px-5 pb-2 pt-0 flex justify-end -mt-2">
                <Link to="/supplier/queries/open" className="text-sm font-medium text-blue-600 hover:text-blue-700 transition-colors">
                  View all
                </Link>
              </div>
              <div className="divide-y divide-gray-100">
                {dashboard.recent_queries?.length === 0 ? (
                  <EmptyState icon={MessageSquare} title="No recent queries" message="Queries will appear here when raised" />
                ) : (
                  dashboard.recent_queries?.slice(0, 5).map((query) => (
                    <div
                      key={query.id}
                      className="flex items-center justify-between p-4 hover:bg-gray-50 transition-colors"
                    >
                      <div className="flex items-center gap-3">
                        <div className="p-2 bg-gray-100 rounded-lg">
                          <Building className="h-4 w-4 text-gray-500" />
                        </div>
                        <div>
                          <p className="text-sm font-medium text-gray-900">{query.supplier_name}</p>
                          <p className="text-xs text-gray-500 truncate max-w-[180px]">
                            {query.query_type}: {query.reference || 'General'}
                          </p>
                        </div>
                      </div>
                      <div className="text-right">
                        <span className={`inline-flex px-2.5 py-1 rounded-full text-xs font-medium ${getQueryStatusBadge(query.status)}`}>
                          {query.status}
                        </span>
                        <p className="text-xs text-gray-500 mt-1">{query.days_outstanding}d ago</p>
                      </div>
                    </div>
                  ))
                )}
              </div>
            </Card>
          </div>

          {/* Latest Responses */}
          <Card title="Latest Responses Sent" icon={CheckCircle} padding={false}>
            <div className="px-5 pb-2 pt-0 flex justify-end -mt-2">
              <Link to="/supplier/communications" className="text-sm font-medium text-blue-600 hover:text-blue-700 transition-colors">
                View all communications
              </Link>
            </div>

            {dashboard.recent_responses?.length === 0 ? (
              <EmptyState icon={Mail} title="No recent responses" message="Sent responses will appear here" />
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead>
                    <tr className="bg-gray-50 text-gray-500 text-xs uppercase tracking-wider">
                      <th className="text-left py-3 px-5 font-semibold">Supplier</th>
                      <th className="text-left py-3 px-5 font-semibold">Statement Date</th>
                      <th className="text-left py-3 px-5 font-semibold">Sent</th>
                      <th className="text-left py-3 px-5 font-semibold">Approved By</th>
                      <th className="text-center py-3 px-5 font-semibold">Queries</th>
                      <th className="text-right py-3 px-5 font-semibold">Balance</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100">
                    {dashboard.recent_responses?.slice(0, 5).map((response) => (
                      <tr key={response.id} className="hover:bg-gray-50 transition-colors">
                        <td className="py-4 px-5">
                          <Link
                            to={`/supplier/directory?account=${response.supplier_code}`}
                            className="font-medium text-blue-600 hover:text-blue-700 transition-colors"
                          >
                            {response.supplier_name}
                          </Link>
                        </td>
                        <td className="py-4 px-5 text-gray-600">{formatDate(response.statement_date)}</td>
                        <td className="py-4 px-5 text-gray-600">{formatDate(response.sent_at)}</td>
                        <td className="py-4 px-5 text-gray-600">{response.approved_by || '-'}</td>
                        <td className="py-4 px-5 text-center">
                          {response.queries_count > 0 ? (
                            <span className="inline-flex px-2.5 py-1 bg-amber-100 text-amber-700 rounded-full text-xs font-medium border border-amber-200">
                              {response.queries_count}
                            </span>
                          ) : (
                            <span className="text-emerald-600 font-medium">None</span>
                          )}
                        </td>
                        <td className="py-4 px-5 text-right font-medium text-gray-900">
                          {response.balance !== null ? formatCurrency(response.balance) : '-'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </Card>
        </>
      )}

      {/* Empty State */}
      {!dashboardQuery.isLoading && !dashboardQuery.isError && !dashboard && (
        <Card>
          <EmptyState icon={FileText} title="No Data Available" message="The supplier statement automation system hasn't processed any statements yet. Once statements are received and processed, they will appear here.">
            <Link
              to="/supplier/statements/queue"
              className="inline-flex items-center gap-2 px-5 py-2.5 bg-blue-600 text-white font-medium rounded-xl hover:bg-blue-700 transition-colors shadow-sm"
            >
              View Statement Queue <ArrowRight className="h-4 w-4" />
            </Link>
          </EmptyState>
        </Card>
      )}
    </div>
  );
}

export default SupplierDashboard;
