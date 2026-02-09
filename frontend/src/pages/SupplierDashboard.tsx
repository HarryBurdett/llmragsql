import { useQuery } from '@tanstack/react-query';
import {
  FileText,
  CheckCircle,
  Clock,
  AlertTriangle,
  MessageSquare,
  RefreshCw,
  TrendingUp,
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
      sent: 'bg-slate-100 text-slate-600 border border-slate-200',
      error: 'bg-red-100 text-red-700 border border-red-200',
    };
    return styles[status?.toLowerCase()] || 'bg-slate-100 text-slate-600 border border-slate-200';
  };

  const getQueryStatusBadge = (status: string) => {
    const styles: Record<string, string> = {
      open: 'bg-amber-100 text-amber-700 border border-amber-200',
      overdue: 'bg-red-100 text-red-700 border border-red-200',
      resolved: 'bg-emerald-100 text-emerald-700 border border-emerald-200',
    };
    return styles[status?.toLowerCase()] || 'bg-slate-100 text-slate-600 border border-slate-200';
  };

  // Count total alerts
  const totalAlerts =
    (dashboard?.alerts?.security_alerts?.length || 0) +
    (dashboard?.alerts?.overdue_queries?.length || 0) +
    (dashboard?.alerts?.failed_processing?.length || 0);

  return (
    <div className="space-y-8">
      {/* Header with gradient accent */}
      <div className="relative overflow-hidden bg-gradient-to-r from-indigo-600 via-purple-600 to-indigo-700 rounded-2xl p-8 text-white shadow-lg">
        <div className="absolute inset-0 bg-grid-white/10 [mask-image:linear-gradient(0deg,transparent,black)]" />
        <div className="relative flex justify-between items-start">
          <div>
            <div className="flex items-center gap-3 mb-2">
              <div className="p-2 bg-white/20 rounded-lg backdrop-blur-sm">
                <Zap className="h-6 w-6" />
              </div>
              <h1 className="text-3xl font-bold">Supplier Statement Automation</h1>
            </div>
            <p className="text-indigo-100 text-lg">
              Intelligent processing, reconciliation & supplier communications
            </p>
          </div>
          <button
            onClick={() => dashboardQuery.refetch()}
            disabled={dashboardQuery.isFetching}
            className="flex items-center gap-2 px-4 py-2 bg-white/20 hover:bg-white/30 rounded-xl backdrop-blur-sm transition-all duration-200 text-sm font-medium"
          >
            <RefreshCw className={`h-4 w-4 ${dashboardQuery.isFetching ? 'animate-spin' : ''}`} />
            Refresh
          </button>
        </div>

        {/* Quick Stats Row */}
        {dashboard && (
          <div className="relative mt-6 grid grid-cols-4 gap-4">
            <div className="bg-white/10 backdrop-blur-sm rounded-xl p-4">
              <div className="text-3xl font-bold">{dashboard.kpis?.statements_week || 0}</div>
              <div className="text-indigo-200 text-sm">This Week</div>
            </div>
            <div className="bg-white/10 backdrop-blur-sm rounded-xl p-4">
              <div className="text-3xl font-bold">{dashboard.kpis?.pending_approvals || 0}</div>
              <div className="text-indigo-200 text-sm">Pending Approval</div>
            </div>
            <div className="bg-white/10 backdrop-blur-sm rounded-xl p-4">
              <div className="text-3xl font-bold">{dashboard.kpis?.open_queries || 0}</div>
              <div className="text-indigo-200 text-sm">Open Queries</div>
            </div>
            <div className="bg-white/10 backdrop-blur-sm rounded-xl p-4">
              <div className="text-3xl font-bold">
                {dashboard.kpis?.match_rate_percent ? `${dashboard.kpis.match_rate_percent}%` : '-'}
              </div>
              <div className="text-indigo-200 text-sm">Match Rate</div>
            </div>
          </div>
        )}
      </div>

      {/* Loading State */}
      {dashboardQuery.isLoading && (
        <div className="flex flex-col items-center justify-center py-16">
          <div className="relative">
            <div className="w-16 h-16 border-4 border-indigo-200 rounded-full animate-pulse" />
            <RefreshCw className="absolute inset-0 m-auto h-8 w-8 text-indigo-600 animate-spin" />
          </div>
          <p className="mt-4 text-slate-500 font-medium">Loading dashboard...</p>
        </div>
      )}

      {/* Error State */}
      {dashboardQuery.isError && (
        <div className="bg-red-50 border border-red-200 rounded-2xl p-6 shadow-sm">
          <div className="flex items-center gap-4">
            <div className="p-3 bg-red-100 rounded-xl">
              <XCircle className="h-6 w-6 text-red-600" />
            </div>
            <div>
              <h3 className="font-semibold text-red-900">Error loading dashboard</h3>
              <p className="text-red-700 text-sm">
                {dashboardQuery.error instanceof Error ? dashboardQuery.error.message : 'Failed to load data'}
              </p>
            </div>
          </div>
        </div>
      )}

      {dashboard && (
        <>
          {/* KPI Cards Grid */}
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-5">
            {/* Statements Today */}
            <div className="group relative bg-white rounded-2xl p-6 shadow-sm hover:shadow-lg transition-all duration-300 border border-slate-100 overflow-hidden">
              <div className="absolute inset-0 bg-gradient-to-br from-blue-500 to-blue-600 opacity-0 group-hover:opacity-100 transition-opacity duration-300" />
              <div className="relative z-10 group-hover:text-white transition-colors duration-300">
                <div className="flex items-center justify-between mb-4">
                  <div className="p-2.5 bg-blue-100 group-hover:bg-white/20 rounded-xl transition-colors duration-300">
                    <FileText className="h-5 w-5 text-blue-600 group-hover:text-white transition-colors duration-300" />
                  </div>
                  <span className="text-xs font-medium px-2 py-1 bg-blue-50 group-hover:bg-white/20 text-blue-600 group-hover:text-white rounded-full transition-colors duration-300">
                    {dashboard.kpis?.statements_today || 0} today
                  </span>
                </div>
                <div className="text-3xl font-bold text-slate-900 group-hover:text-white transition-colors duration-300">
                  {dashboard.kpis?.statements_week || 0}
                </div>
                <div className="text-sm text-slate-500 group-hover:text-white/80 transition-colors duration-300">
                  Statements This Week
                </div>
              </div>
            </div>

            {/* Pending Approvals */}
            <Link
              to="/supplier/statements/reconciliations"
              className="group relative bg-white rounded-2xl p-6 shadow-sm hover:shadow-lg transition-all duration-300 border border-slate-100 overflow-hidden"
            >
              <div className="absolute inset-0 bg-gradient-to-br from-violet-500 to-purple-600 opacity-0 group-hover:opacity-100 transition-opacity duration-300" />
              <div className="relative z-10 group-hover:text-white transition-colors duration-300">
                <div className="flex items-center justify-between mb-4">
                  <div className="p-2.5 bg-violet-100 group-hover:bg-white/20 rounded-xl transition-colors duration-300">
                    <Clock className="h-5 w-5 text-violet-600 group-hover:text-white transition-colors duration-300" />
                  </div>
                  <ArrowRight className="h-4 w-4 text-slate-300 group-hover:text-white group-hover:translate-x-1 transition-all duration-300" />
                </div>
                <div className="text-3xl font-bold text-slate-900 group-hover:text-white transition-colors duration-300">
                  {dashboard.kpis?.pending_approvals || 0}
                </div>
                <div className="text-sm text-slate-500 group-hover:text-white/80 transition-colors duration-300">
                  Pending Approvals
                </div>
              </div>
            </Link>

            {/* Open Queries */}
            <Link
              to="/supplier/queries/open"
              className="group relative bg-white rounded-2xl p-6 shadow-sm hover:shadow-lg transition-all duration-300 border border-slate-100 overflow-hidden"
            >
              <div className="absolute inset-0 bg-gradient-to-br from-amber-500 to-orange-500 opacity-0 group-hover:opacity-100 transition-opacity duration-300" />
              <div className="relative z-10 group-hover:text-white transition-colors duration-300">
                <div className="flex items-center justify-between mb-4">
                  <div className="p-2.5 bg-amber-100 group-hover:bg-white/20 rounded-xl transition-colors duration-300">
                    <MessageSquare className="h-5 w-5 text-amber-600 group-hover:text-white transition-colors duration-300" />
                  </div>
                  {dashboard.kpis?.overdue_queries > 0 && (
                    <span className="text-xs font-medium px-2 py-1 bg-red-100 group-hover:bg-white/30 text-red-600 group-hover:text-white rounded-full transition-colors duration-300">
                      {dashboard.kpis.overdue_queries} overdue
                    </span>
                  )}
                </div>
                <div className="text-3xl font-bold text-slate-900 group-hover:text-white transition-colors duration-300">
                  {dashboard.kpis?.open_queries || 0}
                </div>
                <div className="text-sm text-slate-500 group-hover:text-white/80 transition-colors duration-300">
                  Open Queries
                </div>
              </div>
            </Link>

            {/* Processing Time */}
            <div className="group relative bg-white rounded-2xl p-6 shadow-sm hover:shadow-lg transition-all duration-300 border border-slate-100 overflow-hidden">
              <div className="absolute inset-0 bg-gradient-to-br from-emerald-500 to-teal-600 opacity-0 group-hover:opacity-100 transition-opacity duration-300" />
              <div className="relative z-10 group-hover:text-white transition-colors duration-300">
                <div className="flex items-center justify-between mb-4">
                  <div className="p-2.5 bg-emerald-100 group-hover:bg-white/20 rounded-xl transition-colors duration-300">
                    <Activity className="h-5 w-5 text-emerald-600 group-hover:text-white transition-colors duration-300" />
                  </div>
                </div>
                <div className="text-3xl font-bold text-slate-900 group-hover:text-white transition-colors duration-300">
                  {dashboard.kpis?.avg_processing_hours ? `${dashboard.kpis.avg_processing_hours}h` : '-'}
                </div>
                <div className="text-sm text-slate-500 group-hover:text-white/80 transition-colors duration-300">
                  Avg Processing Time
                </div>
              </div>
            </div>

            {/* Match Rate */}
            <div className="group relative bg-white rounded-2xl p-6 shadow-sm hover:shadow-lg transition-all duration-300 border border-slate-100 overflow-hidden">
              <div className="absolute inset-0 bg-gradient-to-br from-cyan-500 to-blue-600 opacity-0 group-hover:opacity-100 transition-opacity duration-300" />
              <div className="relative z-10 group-hover:text-white transition-colors duration-300">
                <div className="flex items-center justify-between mb-4">
                  <div className="p-2.5 bg-cyan-100 group-hover:bg-white/20 rounded-xl transition-colors duration-300">
                    <BarChart3 className="h-5 w-5 text-cyan-600 group-hover:text-white transition-colors duration-300" />
                  </div>
                </div>
                <div className="text-3xl font-bold text-slate-900 group-hover:text-white transition-colors duration-300">
                  {dashboard.kpis?.match_rate_percent ? `${dashboard.kpis.match_rate_percent}%` : '-'}
                </div>
                <div className="text-sm text-slate-500 group-hover:text-white/80 transition-colors duration-300">
                  Match Rate
                </div>
              </div>
            </div>
          </div>

          {/* Alerts Section */}
          {totalAlerts > 0 && (
            <div className="bg-gradient-to-r from-red-50 to-orange-50 border border-red-100 rounded-2xl p-6 shadow-sm">
              <div className="flex items-center gap-3 mb-5">
                <div className="p-2.5 bg-red-100 rounded-xl">
                  <Bell className="h-5 w-5 text-red-600" />
                </div>
                <div>
                  <h3 className="text-lg font-semibold text-slate-900">Alerts Requiring Attention</h3>
                  <p className="text-sm text-slate-500">{totalAlerts} issue{totalAlerts !== 1 ? 's' : ''} need your review</p>
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                {/* Security Alerts */}
                {dashboard.alerts?.security_alerts?.length > 0 && (
                  <div className="bg-white rounded-xl p-4 border border-red-200 shadow-sm hover:shadow-md transition-shadow">
                    <div className="flex items-center gap-3 mb-3">
                      <Shield className="h-5 w-5 text-red-600" />
                      <span className="font-semibold text-slate-900">Security Alerts</span>
                    </div>
                    <p className="text-sm text-slate-600 mb-3">
                      {dashboard.alerts.security_alerts.length} bank detail change{dashboard.alerts.security_alerts.length !== 1 ? 's' : ''} pending verification
                    </p>
                    <Link
                      to="/supplier/security/alerts"
                      className="inline-flex items-center gap-2 text-sm font-medium text-red-600 hover:text-red-700 transition-colors"
                    >
                      Review now <ArrowRight className="h-4 w-4" />
                    </Link>
                  </div>
                )}

                {/* Overdue Queries */}
                {dashboard.alerts?.overdue_queries?.length > 0 && (
                  <div className="bg-white rounded-xl p-4 border border-amber-200 shadow-sm hover:shadow-md transition-shadow">
                    <div className="flex items-center gap-3 mb-3">
                      <Clock className="h-5 w-5 text-amber-600" />
                      <span className="font-semibold text-slate-900">Overdue Queries</span>
                    </div>
                    <p className="text-sm text-slate-600 mb-3">
                      {dashboard.alerts.overdue_queries.length} quer{dashboard.alerts.overdue_queries.length !== 1 ? 'ies' : 'y'} past response deadline
                    </p>
                    <Link
                      to="/supplier/queries/overdue"
                      className="inline-flex items-center gap-2 text-sm font-medium text-amber-600 hover:text-amber-700 transition-colors"
                    >
                      View queries <ArrowRight className="h-4 w-4" />
                    </Link>
                  </div>
                )}

                {/* Failed Processing */}
                {dashboard.alerts?.failed_processing?.length > 0 && (
                  <div className="bg-white rounded-xl p-4 border border-orange-200 shadow-sm hover:shadow-md transition-shadow">
                    <div className="flex items-center gap-3 mb-3">
                      <XCircle className="h-5 w-5 text-orange-600" />
                      <span className="font-semibold text-slate-900">Failed Processing</span>
                    </div>
                    <p className="text-sm text-slate-600 mb-3">
                      {dashboard.alerts.failed_processing.length} statement{dashboard.alerts.failed_processing.length !== 1 ? 's' : ''} failed extraction
                    </p>
                    <Link
                      to="/supplier/statements/queue"
                      className="inline-flex items-center gap-2 text-sm font-medium text-orange-600 hover:text-orange-700 transition-colors"
                    >
                      Review errors <ArrowRight className="h-4 w-4" />
                    </Link>
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Activity Grid */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Recent Statements */}
            <div className="bg-white rounded-2xl shadow-sm border border-slate-100 overflow-hidden">
              <div className="flex items-center justify-between p-5 border-b border-slate-100">
                <div className="flex items-center gap-3">
                  <div className="p-2 bg-blue-100 rounded-lg">
                    <Mail className="h-5 w-5 text-blue-600" />
                  </div>
                  <h3 className="font-semibold text-slate-900">Recent Statements</h3>
                </div>
                <Link to="/supplier/statements/queue" className="text-sm font-medium text-indigo-600 hover:text-indigo-700 transition-colors">
                  View all
                </Link>
              </div>

              <div className="divide-y divide-slate-100">
                {dashboard.recent_statements?.length === 0 ? (
                  <div className="text-center py-12 text-slate-400">
                    <FileText className="h-12 w-12 mx-auto mb-3 opacity-50" />
                    <p className="font-medium">No recent statements</p>
                    <p className="text-sm">Statements will appear here when received</p>
                  </div>
                ) : (
                  dashboard.recent_statements?.slice(0, 5).map((statement) => (
                    <div
                      key={statement.id}
                      className="flex items-center justify-between p-4 hover:bg-slate-50 transition-colors"
                    >
                      <div className="flex items-center gap-3">
                        <div className="p-2 bg-slate-100 rounded-lg">
                          <Building className="h-4 w-4 text-slate-500" />
                        </div>
                        <div>
                          <p className="font-medium text-slate-900">{statement.supplier_name}</p>
                          <p className="text-xs text-slate-500 flex items-center gap-1">
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
                          <p className="text-sm font-medium text-slate-600 mt-1">
                            {formatCurrency(statement.closing_balance)}
                          </p>
                        )}
                      </div>
                    </div>
                  ))
                )}
              </div>
            </div>

            {/* Recent Queries */}
            <div className="bg-white rounded-2xl shadow-sm border border-slate-100 overflow-hidden">
              <div className="flex items-center justify-between p-5 border-b border-slate-100">
                <div className="flex items-center gap-3">
                  <div className="p-2 bg-amber-100 rounded-lg">
                    <MessageSquare className="h-5 w-5 text-amber-600" />
                  </div>
                  <h3 className="font-semibold text-slate-900">Recent Queries</h3>
                </div>
                <Link to="/supplier/queries/open" className="text-sm font-medium text-indigo-600 hover:text-indigo-700 transition-colors">
                  View all
                </Link>
              </div>

              <div className="divide-y divide-slate-100">
                {dashboard.recent_queries?.length === 0 ? (
                  <div className="text-center py-12 text-slate-400">
                    <MessageSquare className="h-12 w-12 mx-auto mb-3 opacity-50" />
                    <p className="font-medium">No recent queries</p>
                    <p className="text-sm">Queries will appear here when raised</p>
                  </div>
                ) : (
                  dashboard.recent_queries?.slice(0, 5).map((query) => (
                    <div
                      key={query.id}
                      className="flex items-center justify-between p-4 hover:bg-slate-50 transition-colors"
                    >
                      <div className="flex items-center gap-3">
                        <div className="p-2 bg-slate-100 rounded-lg">
                          <Building className="h-4 w-4 text-slate-500" />
                        </div>
                        <div>
                          <p className="font-medium text-slate-900">{query.supplier_name}</p>
                          <p className="text-xs text-slate-500 truncate max-w-[180px]">
                            {query.query_type}: {query.reference || 'General'}
                          </p>
                        </div>
                      </div>
                      <div className="text-right">
                        <span className={`inline-flex px-2.5 py-1 rounded-full text-xs font-medium ${getQueryStatusBadge(query.status)}`}>
                          {query.status}
                        </span>
                        <p className="text-xs text-slate-500 mt-1">{query.days_outstanding}d ago</p>
                      </div>
                    </div>
                  ))
                )}
              </div>
            </div>
          </div>

          {/* Latest Responses */}
          <div className="bg-white rounded-2xl shadow-sm border border-slate-100 overflow-hidden">
            <div className="flex items-center justify-between p-5 border-b border-slate-100">
              <div className="flex items-center gap-3">
                <div className="p-2 bg-emerald-100 rounded-lg">
                  <CheckCircle className="h-5 w-5 text-emerald-600" />
                </div>
                <h3 className="font-semibold text-slate-900">Latest Responses Sent</h3>
              </div>
              <Link to="/supplier/communications" className="text-sm font-medium text-indigo-600 hover:text-indigo-700 transition-colors">
                View all communications
              </Link>
            </div>

            {dashboard.recent_responses?.length === 0 ? (
              <div className="text-center py-12 text-slate-400">
                <Mail className="h-12 w-12 mx-auto mb-3 opacity-50" />
                <p className="font-medium">No recent responses</p>
                <p className="text-sm">Sent responses will appear here</p>
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead>
                    <tr className="bg-slate-50 text-slate-500 text-xs uppercase tracking-wider">
                      <th className="text-left py-3 px-5 font-semibold">Supplier</th>
                      <th className="text-left py-3 px-5 font-semibold">Statement Date</th>
                      <th className="text-left py-3 px-5 font-semibold">Sent</th>
                      <th className="text-left py-3 px-5 font-semibold">Approved By</th>
                      <th className="text-center py-3 px-5 font-semibold">Queries</th>
                      <th className="text-right py-3 px-5 font-semibold">Balance</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100">
                    {dashboard.recent_responses?.slice(0, 5).map((response) => (
                      <tr key={response.id} className="hover:bg-slate-50 transition-colors">
                        <td className="py-4 px-5">
                          <Link
                            to={`/supplier/directory?account=${response.supplier_code}`}
                            className="font-medium text-indigo-600 hover:text-indigo-700 transition-colors"
                          >
                            {response.supplier_name}
                          </Link>
                        </td>
                        <td className="py-4 px-5 text-slate-600">{formatDate(response.statement_date)}</td>
                        <td className="py-4 px-5 text-slate-600">{formatDate(response.sent_at)}</td>
                        <td className="py-4 px-5 text-slate-600">{response.approved_by || '-'}</td>
                        <td className="py-4 px-5 text-center">
                          {response.queries_count > 0 ? (
                            <span className="inline-flex px-2.5 py-1 bg-amber-100 text-amber-700 rounded-full text-xs font-medium border border-amber-200">
                              {response.queries_count}
                            </span>
                          ) : (
                            <span className="text-emerald-600 font-medium">None</span>
                          )}
                        </td>
                        <td className="py-4 px-5 text-right font-medium text-slate-900">
                          {response.balance !== null ? formatCurrency(response.balance) : '-'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </>
      )}

      {/* Empty State */}
      {!dashboardQuery.isLoading && !dashboardQuery.isError && !dashboard && (
        <div className="bg-white rounded-2xl shadow-sm border border-slate-100 text-center py-16 px-8">
          <div className="inline-flex p-4 bg-indigo-100 rounded-2xl mb-6">
            <FileText className="h-12 w-12 text-indigo-600" />
          </div>
          <h3 className="text-xl font-semibold text-slate-900 mb-2">No Data Available</h3>
          <p className="text-slate-500 mb-6 max-w-md mx-auto">
            The supplier statement automation system hasn't processed any statements yet.
            Once statements are received and processed, they will appear here.
          </p>
          <Link
            to="/supplier/statements/queue"
            className="inline-flex items-center gap-2 px-5 py-2.5 bg-indigo-600 text-white font-medium rounded-xl hover:bg-indigo-700 transition-colors shadow-sm"
          >
            View Statement Queue <ArrowRight className="h-4 w-4" />
          </Link>
        </div>
      )}
    </div>
  );
}

export default SupplierDashboard;
