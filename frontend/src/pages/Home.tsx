import { useQuery } from '@tanstack/react-query';
import { Link } from 'react-router-dom';
import {
  Landmark, CreditCard, Scale, TrendingUp, TrendingDown,
  ArrowRight, CheckCircle, AlertTriangle, Database, Cpu, Brain,
  Truck, FileText, Activity
} from 'lucide-react';
import apiClient from '../api/client';
import { useAuth } from '../context/AuthContext';

function formatCurrency(value: number): string {
  const abs = Math.abs(value);
  if (abs >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}m`;
  if (abs >= 1_000) return `${(value / 1_000).toFixed(1)}k`;
  return value.toFixed(0);
}

export function Home() {
  const { user, hasPermission } = useAuth();
  const currentYear = new Date().getFullYear();

  const { data: statusData } = useQuery({
    queryKey: ['status'],
    queryFn: () => apiClient.status(),
  });

  const { data: reconcileData } = useQuery({
    queryKey: ['reconcileSummary'],
    queryFn: () => apiClient.getReconcileSummary(),
    retry: false,
  });

  const { data: financeData } = useQuery({
    queryKey: ['financeSummary', currentYear],
    queryFn: () => apiClient.dashboardFinanceSummary(currentYear),
    retry: false,
  });

  const status = statusData?.data;
  const reconcile = reconcileData?.data;
  const finance = financeData?.data;
  const pl = finance?.profit_and_loss;
  const ratios = finance?.ratios;

  // Count reconciliation issues
  const reconcileChecks = reconcile?.checks || [];
  const reconciledCount = reconcileChecks.filter((c: any) => c.reconciled).length;
  const totalChecks = reconcileChecks.length;

  return (
    <div className="space-y-6">
      {/* Welcome */}
      <div>
        <h1 className="text-2xl font-bold text-gray-900">
          {getGreeting()}, {user?.display_name?.split(' ')[0] || 'there'}
        </h1>
        <p className="text-sm text-gray-500 mt-1">Here's your overview for today</p>
      </div>

      {/* System Health Strip */}
      <div className="flex items-center gap-4 px-4 py-2.5 bg-white border border-gray-200 rounded-lg">
        <span className="text-xs font-medium text-gray-500 uppercase tracking-wide">System</span>
        <StatusDot label="Database" ok={status?.sql_connector} />
        <StatusDot label="Vectors" ok={status?.vector_db} />
        <StatusDot label="AI" ok={status?.llm} />
      </div>

      {/* Finance Summary Cards */}
      {pl && (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          <MetricCard
            label="Revenue"
            value={`£${formatCurrency(pl.sales)}`}
            icon={TrendingUp}
            color="blue"
          />
          <MetricCard
            label="Gross Profit"
            value={`£${formatCurrency(pl.gross_profit)}`}
            sub={ratios ? `${ratios.gross_margin_percent}% margin` : undefined}
            icon={TrendingUp}
            color="emerald"
          />
          <MetricCard
            label="Overheads"
            value={`£${formatCurrency(pl.overheads)}`}
            icon={TrendingDown}
            color="amber"
          />
          <MetricCard
            label="Operating Profit"
            value={`£${formatCurrency(pl.operating_profit)}`}
            sub={ratios ? `${ratios.operating_margin_percent}% margin` : undefined}
            icon={pl.operating_profit >= 0 ? TrendingUp : TrendingDown}
            color={pl.operating_profit >= 0 ? 'emerald' : 'red'}
          />
        </div>
      )}

      {/* Balance Check Overview */}
      {reconcileChecks.length > 0 && (
        <div className="bg-white border border-gray-200 rounded-lg p-5">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-2">
              <Scale className="h-5 w-5 text-gray-400" />
              <h2 className="text-sm font-semibold text-gray-900">Balance Checks</h2>
            </div>
            <Link to="/reconcile/summary" className="text-xs text-blue-600 hover:text-blue-800 flex items-center gap-1">
              View detail <ArrowRight className="h-3 w-3" />
            </Link>
          </div>
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
            {reconcileChecks.map((check: any) => (
              <div
                key={check.name}
                className={`flex items-center gap-3 p-3 rounded-lg border ${
                  check.reconciled
                    ? 'border-emerald-200 bg-emerald-50'
                    : 'border-amber-200 bg-amber-50'
                }`}
              >
                {check.reconciled
                  ? <CheckCircle className="h-4 w-4 text-emerald-600 flex-shrink-0" />
                  : <AlertTriangle className="h-4 w-4 text-amber-600 flex-shrink-0" />}
                <div>
                  <p className="text-sm font-medium text-gray-900">{check.name}</p>
                  <p className={`text-xs ${check.reconciled ? 'text-emerald-700' : 'text-amber-700'}`}>
                    {check.reconciled ? 'Balanced' : 'Variance'}
                  </p>
                </div>
              </div>
            ))}
          </div>
          {totalChecks > 0 && (
            <p className="text-xs text-gray-500 mt-3">
              {reconciledCount} of {totalChecks} checks balanced
            </p>
          )}
        </div>
      )}

      {/* Quick Actions */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {hasPermission('cashbook') && (
          <QuickAction
            to="/cashbook/bank-hub"
            icon={Landmark}
            title="Bank Statements"
            description="Import and reconcile bank statements"
            color="blue"
          />
        )}
        {hasPermission('cashbook') && (
          <QuickAction
            to="/cashbook/gocardless"
            icon={CreditCard}
            title="GoCardless"
            description="Import Direct Debit collections"
            color="purple"
          />
        )}
        {hasPermission('ap_automation') && (
          <QuickAction
            to="/supplier/dashboard"
            icon={Truck}
            title="Suppliers"
            description="AP automation and statement reconciliation"
            color="amber"
          />
        )}
      </div>
    </div>
  );
}

function getGreeting(): string {
  const hour = new Date().getHours();
  if (hour < 12) return 'Good morning';
  if (hour < 17) return 'Good afternoon';
  return 'Good evening';
}

function StatusDot({ label, ok }: { label: string; ok?: boolean }) {
  return (
    <div className="flex items-center gap-1.5">
      <div className={`w-2 h-2 rounded-full ${ok ? 'bg-emerald-500' : ok === false ? 'bg-red-500' : 'bg-gray-300'}`} />
      <span className="text-xs text-gray-600">{label}</span>
    </div>
  );
}

const colorMap: Record<string, { bg: string; icon: string; text: string }> = {
  blue:    { bg: 'bg-blue-50',    icon: 'text-blue-600',    text: 'text-blue-700' },
  emerald: { bg: 'bg-emerald-50', icon: 'text-emerald-600', text: 'text-emerald-700' },
  amber:   { bg: 'bg-amber-50',   icon: 'text-amber-600',   text: 'text-amber-700' },
  red:     { bg: 'bg-red-50',     icon: 'text-red-600',     text: 'text-red-700' },
  purple:  { bg: 'bg-purple-50',  icon: 'text-purple-600',  text: 'text-purple-700' },
};

function MetricCard({ label, value, sub, icon: Icon, color }: {
  label: string; value: string; sub?: string;
  icon: React.ComponentType<{ className?: string }>; color: string;
}) {
  const c = colorMap[color] || colorMap.blue;
  return (
    <div className="bg-white border border-gray-200 rounded-lg p-4">
      <div className="flex items-center justify-between mb-2">
        <span className="text-xs font-medium text-gray-500 uppercase tracking-wide">{label}</span>
        <div className={`p-1.5 rounded-lg ${c.bg}`}>
          <Icon className={`h-4 w-4 ${c.icon}`} />
        </div>
      </div>
      <p className="text-xl font-bold text-gray-900">{value}</p>
      {sub && <p className={`text-xs mt-0.5 ${c.text}`}>{sub}</p>}
    </div>
  );
}

function QuickAction({ to, icon: Icon, title, description, color }: {
  to: string; icon: React.ComponentType<{ className?: string }>;
  title: string; description: string; color: string;
}) {
  const c = colorMap[color] || colorMap.blue;
  return (
    <Link
      to={to}
      className="group flex items-start gap-4 p-4 bg-white border border-gray-200 rounded-lg hover:border-gray-300 hover:shadow-sm transition-all"
    >
      <div className={`p-2.5 rounded-lg ${c.bg} group-hover:scale-105 transition-transform`}>
        <Icon className={`h-5 w-5 ${c.icon}`} />
      </div>
      <div className="flex-1 min-w-0">
        <p className="text-sm font-semibold text-gray-900 group-hover:text-blue-700 transition-colors">{title}</p>
        <p className="text-xs text-gray-500 mt-0.5">{description}</p>
      </div>
      <ArrowRight className="h-4 w-4 text-gray-300 group-hover:text-blue-500 transition-colors mt-1" />
    </Link>
  );
}

export default Home;
