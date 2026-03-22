import { useQuery } from '@tanstack/react-query';
import {
  CheckCircle, XCircle, RefreshCw, Scale, Users, Building2,
  BookOpen, Receipt, ChevronRight, AlertTriangle, Database,
  Cpu, Brain, Server, Activity,
} from 'lucide-react';
import { Link } from 'react-router-dom';
import apiClient, { authFetch } from '../api/client';
import { PageHeader, Card, LoadingState } from '../components/ui';

// ─── Types ───────────────────────────────────────────────────

interface ReconcileDetail { label: string; value: number }
interface ReconcileVariance { label: string; value: number; ok: boolean }
interface ReconcileCheck {
  name: string; icon: string; reconciled: boolean;
  details?: ReconcileDetail[]; variances?: ReconcileVariance[]; error?: string;
}
interface ReconcileSummaryResponse {
  success: boolean; reconciliation_date: string; checks: ReconcileCheck[];
  all_reconciled: boolean; total_checks: number; passed_checks: number; failed_checks: number;
}
interface SystemStatus {
  sql_connector: boolean; vector_db: boolean; llm: boolean; config_loaded: boolean;
}

// ─── Helpers ─────────────────────────────────────────────────

const iconMap: Record<string, React.ComponentType<{ className?: string }>> = {
  users: Users, building: Building2, book: BookOpen, receipt: Receipt,
};

const linkMap: Record<string, string> = {
  Debtors: '/reconcile/debtors', Creditors: '/reconcile/creditors',
  Cashbook: '/reconcile/cashbook', VAT: '/reconcile/vat',
};

function formatCurrency(value: number | undefined | null): string {
  if (value === undefined || value === null) return '£0.00';
  return new Intl.NumberFormat('en-GB', { style: 'currency', currency: 'GBP' }).format(value);
}

// ─── Sub-components ──────────────────────────────────────────

function ServiceIndicator({ label, ok, icon: Icon }: {
  label: string; ok: boolean; icon: React.ComponentType<{ className?: string }>;
}) {
  return (
    <div className={`flex items-center gap-3 px-4 py-3 rounded-lg border ${
      ok ? 'bg-emerald-50 border-emerald-200' : 'bg-red-50 border-red-200'
    }`}>
      <Icon className={`h-5 w-5 ${ok ? 'text-emerald-600' : 'text-red-500'}`} />
      <div className="flex-1 min-w-0">
        <p className="text-sm font-medium text-gray-900">{label}</p>
        <p className={`text-xs ${ok ? 'text-emerald-600' : 'text-red-600'}`}>
          {ok ? 'Connected' : 'Unavailable'}
        </p>
      </div>
      {ok ? (
        <CheckCircle className="h-5 w-5 text-emerald-500 flex-shrink-0" />
      ) : (
        <XCircle className="h-5 w-5 text-red-500 flex-shrink-0" />
      )}
    </div>
  );
}

function ReconcileCard({ check }: { check: ReconcileCheck }) {
  const Icon = iconMap[check.icon] || Scale;
  const detailLink = linkMap[check.name];

  return (
    <div className={`bg-white border rounded-lg overflow-hidden border-l-4 ${
      check.reconciled ? 'border-l-emerald-500 border-gray-200' : 'border-l-red-500 border-gray-200'
    }`}>
      {/* Header */}
      <div className={`px-4 py-3 flex items-center justify-between ${
        check.reconciled ? 'bg-emerald-50' : 'bg-red-50'
      }`}>
        <div className="flex items-center gap-2.5">
          <Icon className={`h-4.5 w-4.5 ${check.reconciled ? 'text-emerald-600' : 'text-red-600'}`} />
          <span className="text-sm font-semibold text-gray-900">{check.name}</span>
        </div>
        {check.reconciled ? (
          <CheckCircle className="h-5 w-5 text-emerald-500" />
        ) : check.error ? (
          <AlertTriangle className="h-5 w-5 text-amber-500" />
        ) : (
          <XCircle className="h-5 w-5 text-red-500" />
        )}
      </div>

      {/* Body */}
      <div className="px-4 py-3">
        {check.error ? (
          <p className="text-xs text-red-600">{check.error}</p>
        ) : (
          <>
            {check.details && (
              <div className="space-y-1.5">
                {check.details.map((d, i) => (
                  <div key={i} className="flex justify-between text-xs">
                    <span className="text-gray-500">{d.label}</span>
                    <span className="font-medium text-gray-800">{formatCurrency(d.value)}</span>
                  </div>
                ))}
              </div>
            )}
            {check.variances && (
              <div className="border-t mt-2 pt-2 space-y-1.5">
                {check.variances.map((v, i) => (
                  <div key={i} className="flex justify-between items-center text-xs">
                    <span className="text-gray-500">{v.label}</span>
                    <span className={`font-medium flex items-center gap-1 ${
                      v.ok ? 'text-emerald-600' : 'text-red-600'
                    }`}>
                      {v.ok ? <CheckCircle className="h-3.5 w-3.5" /> : <XCircle className="h-3.5 w-3.5" />}
                      {formatCurrency(v.value)}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </>
        )}

        {detailLink && (
          <Link
            to={detailLink}
            className="mt-3 flex items-center justify-center gap-1.5 w-full py-1.5 bg-gray-50 hover:bg-gray-100 rounded text-xs font-medium text-gray-600 transition-colors"
          >
            View Details <ChevronRight className="h-3.5 w-3.5" />
          </Link>
        )}
      </div>
    </div>
  );
}

// ─── Main Page ───────────────────────────────────────────────

export function SystemDashboard() {
  // System services status
  const statusQuery = useQuery<SystemStatus>({
    queryKey: ['systemStatus'],
    queryFn: async () => {
      const res = await apiClient.status();
      return res.data;
    },
    refetchInterval: 30_000,
  });

  // Reconciliation summary
  const reconcileQuery = useQuery<ReconcileSummaryResponse>({
    queryKey: ['reconcileSummary'],
    queryFn: async () => {
      const response = await authFetch('/api/reconcile/summary');
      return response.json();
    },
    refetchOnWindowFocus: false,
  });

  const status = statusQuery.data;
  const reconcile = reconcileQuery.data;

  const isRefreshing = statusQuery.isFetching || reconcileQuery.isFetching;
  const isLoading = statusQuery.isLoading || reconcileQuery.isLoading;

  return (
    <div className="space-y-6">
      <PageHeader icon={Activity} title="System Dashboard" subtitle="Service health and control account status">
        <button
          onClick={() => { statusQuery.refetch(); reconcileQuery.refetch(); }}
          disabled={isRefreshing}
          className="flex items-center gap-2 px-4 py-2 bg-gray-100 hover:bg-gray-200 rounded-lg transition-colors disabled:opacity-50 text-sm"
        >
          <RefreshCw className={`h-4 w-4 ${isRefreshing ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </PageHeader>

      {isLoading && <LoadingState message="Loading system status..." />}

      {/* ─── Service Health ─── */}
      {status && (
        <Card title="Service Health" icon={Server}>
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
            <ServiceIndicator label="Database" ok={status.sql_connector} icon={Database} />
            <ServiceIndicator label="Vector Store" ok={status.vector_db} icon={Cpu} />
            <ServiceIndicator label="AI Engine" ok={status.llm} icon={Brain} />
            <ServiceIndicator label="Configuration" ok={status.config_loaded} icon={Server} />
          </div>
          {(!status.sql_connector || !status.vector_db || !status.llm || !status.config_loaded) && (
            <div className="mt-3 p-3 bg-amber-50 border border-amber-200 rounded-lg flex items-start gap-2">
              <AlertTriangle className="h-4 w-4 text-amber-500 flex-shrink-0 mt-0.5" />
              <p className="text-sm text-amber-800">
                One or more services are unavailable. Some features may not work until the issues are resolved.
              </p>
            </div>
          )}
        </Card>
      )}

      {/* ─── Balance Checks ─── */}
      {reconcile && (
        <Card
          title="Control Account Balances"
          icon={Scale}
        >
          {/* Overall status banner */}
          <div className={`mb-4 px-4 py-2.5 rounded-lg flex items-center justify-between ${
            reconcile.all_reconciled ? 'bg-emerald-50 border border-emerald-200' : 'bg-amber-50 border border-amber-200'
          }`}>
            <div className="flex items-center gap-2">
              {reconcile.all_reconciled ? (
                <CheckCircle className="h-5 w-5 text-emerald-500" />
              ) : (
                <AlertTriangle className="h-5 w-5 text-amber-500" />
              )}
              <span className={`text-sm font-medium ${
                reconcile.all_reconciled ? 'text-emerald-800' : 'text-amber-800'
              }`}>
                {reconcile.all_reconciled
                  ? 'All control accounts reconciled'
                  : `${reconcile.failed_checks} of ${reconcile.total_checks} checks require attention`}
              </span>
            </div>
            <span className="text-xs text-gray-500">
              {reconcile.passed_checks}/{reconcile.total_checks} passed
            </span>
          </div>

          {/* Check cards */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {reconcile.checks.map((check) => (
              <ReconcileCard key={check.name} check={check} />
            ))}
          </div>

          {reconcile.reconciliation_date && (
            <p className="text-xs text-gray-400 mt-3 text-right">
              Last checked: {reconcile.reconciliation_date}
            </p>
          )}
        </Card>
      )}
    </div>
  );
}

export default SystemDashboard;
