import { useQuery } from '@tanstack/react-query';
import {
  Truck, FileText, AlertTriangle, ShieldAlert, Clock,
  CheckCircle, XCircle, TrendingUp, MessageSquare,
  Mail, Send, RefreshCw,
} from 'lucide-react';
import { Link } from 'react-router-dom';
import { authFetch, friendlyError } from '../api/client';
import { PageHeader, Card, LoadingState, Alert } from '../components/ui';

// ─── Types ───────────────────────────────────────────────────

interface DashboardData {
  statements_this_month: number;
  pending_approvals: number;
  open_queries: number;
  overdue_queries: number;
  match_rate: number;
  alerts: DashboardAlert[];
  recent_statements: RecentStatement[];
  recent_responses: RecentResponse[];
}

interface DashboardAlert {
  type: 'security' | 'overdue' | 'error';
  message: string;
  timestamp: string;
  supplier_name?: string;
}

interface AgedCreditor {
  current: number;
  days_30: number;
  days_60: number;
  days_90: number;
  days_120_plus: number;
  total: number;
}

interface RecentStatement {
  supplier_name: string;
  received_date: string;
  amount: number;
  status: string;
}

interface RecentResponse {
  supplier_name: string;
  sent_date: string;
  type: string;
  status: string;
}

// ─── Helpers ─────────────────────────────────────────────────

function formatCurrency(value: number | undefined | null): string {
  if (value === undefined || value === null) return '\u00A30.00';
  return new Intl.NumberFormat('en-GB', { style: 'currency', currency: 'GBP' }).format(value);
}

function formatDate(dateStr: string): string {
  if (!dateStr) return '';
  const d = new Date(dateStr);
  return d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' });
}

function formatDateTime(dateStr: string): string {
  if (!dateStr) return '';
  const d = new Date(dateStr);
  return d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit' });
}

// ─── Data fetching ───────────────────────────────────────────

async function fetchDashboard(): Promise<DashboardData> {
  const res = await authFetch('/api/supplier-statements/dashboard');
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(body.error || body.detail || 'Failed to load dashboard data');
  }
  return res.json();
}

async function fetchAgedCreditors(): Promise<AgedCreditor> {
  const res = await authFetch('/api/creditors/aged');
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(body.error || body.detail || 'Failed to load aged creditors');
  }
  return res.json();
}

// ─── KPI Card ────────────────────────────────────────────────

function KpiCard({ label, value, suffix, icon: Icon, color, highlight }: {
  label: string;
  value: number | string;
  suffix?: string;
  icon: React.ComponentType<{ className?: string }>;
  color: string;
  highlight?: 'amber' | 'red' | null;
}) {
  const colorMap: Record<string, { bg: string; icon: string }> = {
    blue: { bg: 'bg-blue-50', icon: 'text-blue-600' },
    amber: { bg: 'bg-amber-50', icon: 'text-amber-600' },
    red: { bg: 'bg-red-50', icon: 'text-red-600' },
    emerald: { bg: 'bg-emerald-50', icon: 'text-emerald-600' },
  };
  const c = colorMap[color] || colorMap.blue;

  const highlightBorder = highlight === 'red'
    ? 'border-red-300 ring-1 ring-red-100'
    : highlight === 'amber'
      ? 'border-amber-300 ring-1 ring-amber-100'
      : 'border-gray-200';

  return (
    <div className={`bg-white rounded-xl border shadow-sm p-5 ${highlightBorder}`}>
      <div className="flex items-center justify-between mb-3">
        <span className="text-xs font-medium text-gray-500 uppercase tracking-wider">{label}</span>
        <div className={`p-2 rounded-lg ${c.bg}`}>
          <Icon className={`h-4 w-4 ${c.icon}`} />
        </div>
      </div>
      <div className="flex items-baseline gap-1">
        <span className="text-2xl font-bold text-gray-900">{value}</span>
        {suffix && <span className="text-sm text-gray-500">{suffix}</span>}
      </div>
    </div>
  );
}

// ─── Aged Creditors Bar ──────────────────────────────────────

function AgedCreditorsChart({ data }: { data: AgedCreditor }) {
  const buckets = [
    { label: 'Current', value: data.current, color: 'bg-emerald-500' },
    { label: '30 days', value: data.days_30, color: 'bg-blue-500' },
    { label: '60 days', value: data.days_60, color: 'bg-amber-500' },
    { label: '90 days', value: data.days_90, color: 'bg-orange-500' },
    { label: '120+ days', value: data.days_120_plus, color: 'bg-red-500' },
  ];

  const maxVal = Math.max(...buckets.map(b => b.value), 1);

  return (
    <div className="space-y-3">
      {buckets.map((bucket) => (
        <div key={bucket.label} className="flex items-center gap-3">
          <span className="text-xs text-gray-500 w-16 text-right flex-shrink-0">{bucket.label}</span>
          <div className="flex-1 bg-gray-100 rounded-full h-5 overflow-hidden">
            <div
              className={`h-full ${bucket.color} rounded-full transition-all duration-500`}
              style={{ width: `${Math.max((bucket.value / maxVal) * 100, bucket.value > 0 ? 2 : 0)}%` }}
            />
          </div>
          <span className="text-xs font-medium text-gray-700 w-24 text-right flex-shrink-0">
            {formatCurrency(bucket.value)}
          </span>
        </div>
      ))}
      <div className="pt-2 border-t border-gray-100 flex justify-between items-center">
        <span className="text-sm font-medium text-gray-700">Total Outstanding</span>
        <span className="text-sm font-bold text-gray-900">{formatCurrency(data.total)}</span>
      </div>
    </div>
  );
}

// ─── Alert Item ──────────────────────────────────────────────

function AlertItem({ alert }: { alert: DashboardAlert }) {
  const typeConfig: Record<string, { icon: React.ComponentType<{ className?: string }>; color: string; bg: string }> = {
    security: { icon: ShieldAlert, color: 'text-red-600', bg: 'bg-red-50' },
    overdue: { icon: Clock, color: 'text-amber-600', bg: 'bg-amber-50' },
    error: { icon: XCircle, color: 'text-red-600', bg: 'bg-red-50' },
  };

  const c = typeConfig[alert.type] || typeConfig.error;
  const Icon = c.icon;

  return (
    <div className={`flex items-start gap-3 p-3 rounded-lg ${c.bg}`}>
      <Icon className={`h-4 w-4 mt-0.5 flex-shrink-0 ${c.color}`} />
      <div className="flex-1 min-w-0">
        <p className="text-sm text-gray-900">{alert.message}</p>
        <div className="flex items-center gap-2 mt-1">
          {alert.supplier_name && (
            <span className="text-xs text-gray-500">{alert.supplier_name}</span>
          )}
          <span className="text-xs text-gray-400">{formatDateTime(alert.timestamp)}</span>
        </div>
      </div>
    </div>
  );
}

// ─── Main Component ──────────────────────────────────────────

export default function SupplierDashboard() {
  const {
    data: dashboard,
    isLoading: dashLoading,
    error: dashError,
    refetch: refetchDash,
  } = useQuery<DashboardData>({
    queryKey: ['supplier-dashboard'],
    queryFn: fetchDashboard,
    refetchInterval: 60_000,
  });

  const {
    data: aged,
    isLoading: agedLoading,
    error: agedError,
  } = useQuery<AgedCreditor>({
    queryKey: ['aged-creditors'],
    queryFn: fetchAgedCreditors,
    refetchInterval: 300_000,
  });

  const isLoading = dashLoading || agedLoading;

  return (
    <div className="space-y-6">
      {/* Header */}
      <PageHeader
        icon={Truck}
        title="Supplier Dashboard"
        subtitle="Automation overview and aged creditor analysis"
      >
        <button
          onClick={() => refetchDash()}
          className="flex items-center gap-1.5 px-3 py-1.5 text-sm text-gray-600 bg-white border border-gray-200 rounded-lg hover:bg-gray-50 transition-colors"
        >
          <RefreshCw className={`h-3.5 w-3.5 ${dashLoading ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </PageHeader>

      {/* Errors */}
      {dashError && (
        <Alert variant="error" title="Dashboard unavailable">
          {friendlyError((dashError as Error).message)}
        </Alert>
      )}
      {agedError && (
        <Alert variant="error" title="Aged creditors unavailable">
          {friendlyError((agedError as Error).message)}
        </Alert>
      )}

      {/* Loading */}
      {isLoading && !dashboard && !aged && (
        <LoadingState message="Loading supplier dashboard..." />
      )}

      {/* KPI Cards */}
      {dashboard && (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          <KpiCard
            label="Statements This Month"
            value={dashboard.statements_this_month}
            icon={FileText}
            color="blue"
          />
          <KpiCard
            label="Pending Approvals"
            value={dashboard.pending_approvals}
            icon={Clock}
            color="amber"
            highlight={dashboard.pending_approvals > 0 ? 'amber' : null}
          />
          <KpiCard
            label="Open Queries"
            value={dashboard.open_queries}
            icon={MessageSquare}
            color="red"
            highlight={dashboard.overdue_queries > 0 ? 'red' : null}
          />
          <KpiCard
            label="Match Rate"
            value={dashboard.match_rate.toFixed(1)}
            suffix="%"
            icon={TrendingUp}
            color="emerald"
          />
        </div>
      )}

      {/* Main content grid */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Aged Creditors */}
        <Card title="Aged Creditors Summary" icon={TrendingUp}>
          {agedLoading && !aged ? (
            <LoadingState message="Loading aged creditors..." size="sm" />
          ) : aged ? (
            <AgedCreditorsChart data={aged} />
          ) : null}
        </Card>

        {/* Alerts */}
        <Card title="Alerts" icon={AlertTriangle}>
          {dashboard?.alerts && dashboard.alerts.length > 0 ? (
            <div className="space-y-2">
              {dashboard.alerts.map((alert, i) => (
                <AlertItem key={i} alert={alert} />
              ))}
            </div>
          ) : dashboard && !dashLoading ? (
            <div className="flex flex-col items-center py-6 text-gray-400">
              <CheckCircle className="h-8 w-8 mb-2 text-emerald-400" />
              <p className="text-sm">No active alerts</p>
            </div>
          ) : null}
        </Card>
      </div>

      {/* Recent Activity */}
      {dashboard && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Recent Statements Received */}
          <Card title="Recent Statements" icon={Mail}>
            {dashboard.recent_statements.length > 0 ? (
              <div className="divide-y divide-gray-100">
                {dashboard.recent_statements.map((stmt, i) => (
                  <div key={i} className="flex items-center justify-between py-2.5 first:pt-0 last:pb-0">
                    <div className="min-w-0 flex-1">
                      <p className="text-sm font-medium text-gray-900 truncate">{stmt.supplier_name}</p>
                      <p className="text-xs text-gray-500">{formatDate(stmt.received_date)}</p>
                    </div>
                    <div className="text-right flex-shrink-0 ml-3">
                      <p className="text-sm font-medium text-gray-900">{formatCurrency(stmt.amount)}</p>
                      <span className={`inline-block text-xs px-1.5 py-0.5 rounded-full ${
                        stmt.status === 'reconciled' ? 'bg-emerald-50 text-emerald-700' :
                        stmt.status === 'pending' ? 'bg-amber-50 text-amber-700' :
                        stmt.status === 'processing' ? 'bg-blue-50 text-blue-700' :
                        'bg-gray-50 text-gray-600'
                      }`}>
                        {stmt.status}
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-sm text-gray-400 text-center py-4">No recent statements</p>
            )}
            <div className="mt-4 pt-3 border-t border-gray-100">
              <Link
                to="/supplier/statements/queue"
                className="text-xs font-medium text-blue-600 hover:text-blue-700 transition-colors"
              >
                View all statements &rarr;
              </Link>
            </div>
          </Card>

          {/* Recent Responses Sent */}
          <Card title="Recent Responses" icon={Send}>
            {dashboard.recent_responses.length > 0 ? (
              <div className="divide-y divide-gray-100">
                {dashboard.recent_responses.map((resp, i) => (
                  <div key={i} className="flex items-center justify-between py-2.5 first:pt-0 last:pb-0">
                    <div className="min-w-0 flex-1">
                      <p className="text-sm font-medium text-gray-900 truncate">{resp.supplier_name}</p>
                      <p className="text-xs text-gray-500">{formatDate(resp.sent_date)}</p>
                    </div>
                    <div className="text-right flex-shrink-0 ml-3">
                      <span className="text-xs text-gray-600">{resp.type}</span>
                      <span className={`ml-2 inline-block text-xs px-1.5 py-0.5 rounded-full ${
                        resp.status === 'sent' ? 'bg-emerald-50 text-emerald-700' :
                        resp.status === 'draft' ? 'bg-gray-50 text-gray-600' :
                        resp.status === 'approved' ? 'bg-blue-50 text-blue-700' :
                        'bg-gray-50 text-gray-600'
                      }`}>
                        {resp.status}
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-sm text-gray-400 text-center py-4">No recent responses</p>
            )}
            <div className="mt-4 pt-3 border-t border-gray-100">
              <Link
                to="/supplier/queries/open"
                className="text-xs font-medium text-blue-600 hover:text-blue-700 transition-colors"
              >
                View open queries &rarr;
              </Link>
            </div>
          </Card>
        </div>
      )}
    </div>
  );
}

export { SupplierDashboard };
