import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  BarChart3,
  TrendingUp,
  TrendingDown,
  Users,
  DollarSign,
  PieChart,
  AlertTriangle,
  ChevronDown,
  ChevronUp,
  Activity,
  Target,
  RefreshCw,
  UserMinus,
  UserPlus,
  Zap,
  Calendar,
  ArrowRight,
  Shield,
} from 'lucide-react';
import apiClient from '../api/client';
import type {
  ExecutiveSummaryResponse,
  RevenueByCategoryDetailedResponse,
  NewVsExistingRevenueResponse,
  CustomerChurnAnalysisResponse,
  ForwardIndicatorsResponse,
  MonthlyComparisonResponse,
  DashboardCustomerConcentrationResponse,
  DashboardTopCustomersResponse,
} from '../api/client';

// ============ Utility Functions ============

function formatCurrency(value: number): string {
  return new Intl.NumberFormat('en-GB', {
    style: 'currency',
    currency: 'GBP',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value);
}

function formatPercent(value: number, includeSign = true): string {
  const sign = includeSign && value > 0 ? '+' : '';
  return `${sign}${value.toFixed(1)}%`;
}

function formatCompact(value: number): string {
  if (value >= 1000000) return `£${(value / 1000000).toFixed(1)}m`;
  if (value >= 1000) return `£${(value / 1000).toFixed(0)}k`;
  return formatCurrency(value);
}

// ============ Reusable Components ============

interface KPICardProps {
  title: string;
  value: string | number;
  subtitle?: string;
  comparison?: string;
  trend?: 'up' | 'down' | 'neutral' | 'flat';
  trendValue?: string;
  icon?: React.ReactNode;
  size?: 'normal' | 'large';
  variant?: 'default' | 'success' | 'warning' | 'danger';
}

function KPICard({
  title, value, subtitle, comparison, trend, trendValue, icon,
  size = 'normal', variant = 'default'
}: KPICardProps) {
  const bgColors = {
    default: 'bg-white',
    success: 'bg-green-50 border-green-200',
    warning: 'bg-yellow-50 border-yellow-200',
    danger: 'bg-red-50 border-red-200',
  };

  return (
    <div className={`card ${bgColors[variant]} ${size === 'large' ? 'p-6' : ''}`}>
      <div className="flex items-start justify-between">
        <div className="flex-1">
          <p className="text-sm text-gray-500 font-medium">{title}</p>
          <p className={`font-bold text-gray-900 mt-1 ${size === 'large' ? 'text-3xl' : 'text-2xl'}`}>
            {value}
          </p>
          {subtitle && <p className="text-sm text-gray-500 mt-1">{subtitle}</p>}
          {comparison && <p className="text-xs text-gray-400 mt-1">{comparison}</p>}
        </div>
        <div className="flex flex-col items-end">
          {icon && <div className="text-gray-400 mb-2">{icon}</div>}
          {trend && trendValue && (
            <div className={`flex items-center text-sm font-medium ${
              trend === 'up' ? 'text-green-600' :
              trend === 'down' ? 'text-red-600' : 'text-gray-500'
            }`}>
              {trend === 'up' && <TrendingUp className="h-4 w-4 mr-1" />}
              {trend === 'down' && <TrendingDown className="h-4 w-4 mr-1" />}
              {trendValue}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

interface SectionHeaderProps {
  title: string;
  subtitle?: string;
  icon?: React.ReactNode;
}

function SectionHeader({ title, subtitle, icon }: SectionHeaderProps) {
  return (
    <div className="flex items-center gap-3 mb-4">
      {icon && <div className="text-blue-600">{icon}</div>}
      <div>
        <h3 className="text-lg font-semibold text-gray-900">{title}</h3>
        {subtitle && <p className="text-sm text-gray-500">{subtitle}</p>}
      </div>
    </div>
  );
}

// ============ Executive Summary Section ============
// What a Sales Director should see first - at-a-glance performance

function ExecutiveSummarySection({ year }: { year: number }) {
  const { data, isLoading } = useQuery<ExecutiveSummaryResponse>({
    queryKey: ['executive-summary', year],
    queryFn: async () => {
      const response = await apiClient.dashboardExecutiveSummary(year);
      return response.data;
    },
  });

  if (isLoading) {
    return <div className="text-center py-8">Loading executive summary...</div>;
  }

  const kpis = data?.kpis;
  const period = data?.period;

  if (!kpis) {
    return <div className="text-center py-8 text-gray-500">No data available</div>;
  }

  const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  const currentMonthName = monthNames[(period?.current_month || 1) - 1];

  return (
    <div className="space-y-6">
      {/* Primary KPIs - The headline numbers */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <KPICard
          title={`${currentMonthName} ${year} Revenue`}
          value={formatCurrency(kpis.current_month.value)}
          comparison={`vs ${formatCurrency(kpis.current_month.prior_year)} LY`}
          trend={kpis.current_month.trend === 'flat' ? 'neutral' : kpis.current_month.trend}
          trendValue={formatPercent(kpis.current_month.yoy_change_percent)}
          icon={<Calendar className="h-6 w-6" />}
          size="large"
          variant={kpis.current_month.yoy_change_percent >= 0 ? 'success' : 'danger'}
        />
        <KPICard
          title={`Q${period?.current_quarter} ${year} to Date`}
          value={formatCurrency(kpis.quarter_to_date.value)}
          comparison={`vs ${formatCurrency(kpis.quarter_to_date.prior_year)} LY`}
          trend={kpis.quarter_to_date.trend === 'flat' ? 'neutral' : kpis.quarter_to_date.trend}
          trendValue={formatPercent(kpis.quarter_to_date.yoy_change_percent)}
          icon={<Activity className="h-6 w-6" />}
          size="large"
          variant={kpis.quarter_to_date.yoy_change_percent >= 0 ? 'success' : 'danger'}
        />
        <KPICard
          title={`YTD ${year} Revenue`}
          value={formatCurrency(kpis.year_to_date.value)}
          comparison={`vs ${formatCurrency(kpis.year_to_date.prior_year)} same period LY`}
          trend={kpis.year_to_date.trend === 'flat' ? 'neutral' : kpis.year_to_date.trend}
          trendValue={formatPercent(kpis.year_to_date.yoy_change_percent)}
          icon={<DollarSign className="h-6 w-6" />}
          size="large"
          variant={kpis.year_to_date.yoy_change_percent >= 0 ? 'success' : 'danger'}
        />
        <KPICard
          title="Rolling 12 Months"
          value={formatCurrency(kpis.rolling_12_months.value)}
          comparison={`vs ${formatCurrency(kpis.rolling_12_months.prior_period)} prior period`}
          trend={kpis.rolling_12_months.trend === 'flat' ? 'neutral' : kpis.rolling_12_months.trend}
          trendValue={formatPercent(kpis.rolling_12_months.change_percent)}
          icon={<RefreshCw className="h-6 w-6" />}
          size="large"
        />
      </div>

      {/* Secondary metrics - Run rate and projections */}
      <div className="card bg-gradient-to-r from-blue-50 to-indigo-50">
        <div className="flex items-center gap-2 mb-4">
          <Target className="h-5 w-5 text-blue-600" />
          <h4 className="font-semibold text-gray-900">Full Year Projection</h4>
        </div>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
          <div>
            <p className="text-sm text-gray-500">Monthly Run Rate</p>
            <p className="text-xl font-bold text-gray-900">{formatCurrency(kpis.monthly_run_rate)}</p>
            <p className="text-xs text-gray-400">Based on last 3 months</p>
          </div>
          <div>
            <p className="text-sm text-gray-500">Annual Run Rate</p>
            <p className="text-xl font-bold text-gray-900">{formatCurrency(kpis.annual_run_rate)}</p>
          </div>
          <div>
            <p className="text-sm text-gray-500">Projected Full Year</p>
            <p className="text-xl font-bold text-blue-700">{formatCurrency(kpis.projected_full_year)}</p>
            <p className="text-xs text-gray-400">Based on YTD trend</p>
          </div>
          <div>
            <p className="text-sm text-gray-500">vs Prior Year</p>
            <p className={`text-xl font-bold ${kpis.projection_vs_prior_percent >= 0 ? 'text-green-600' : 'text-red-600'}`}>
              {formatPercent(kpis.projection_vs_prior_percent)}
            </p>
            <p className="text-xs text-gray-400">Prior: {formatCurrency(kpis.prior_full_year)}</p>
          </div>
        </div>
      </div>
    </div>
  );
}

// ============ Monthly Comparison Section ============
// Detailed month-by-month YoY comparison

function MonthlyComparisonSection({ year }: { year: number }) {
  const { data, isLoading } = useQuery<MonthlyComparisonResponse>({
    queryKey: ['monthly-comparison', year],
    queryFn: async () => {
      const response = await apiClient.dashboardMonthlyComparison(year);
      return response.data;
    },
  });

  if (isLoading) return <div className="text-center py-4">Loading...</div>;

  const months = data?.months || [];
  const maxRevenue = Math.max(...months.map(m => Math.max(m.current_year, m.previous_year)));

  return (
    <div className="card">
      <SectionHeader
        title="Monthly Revenue Comparison"
        subtitle="Current year vs same month last year"
        icon={<BarChart3 className="h-5 w-5" />}
      />

      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-200 text-gray-500">
              <th className="text-left py-3 px-2 font-medium">Month</th>
              <th className="text-right py-3 px-2 font-medium">{year}</th>
              <th className="py-3 px-4 font-medium">Comparison</th>
              <th className="text-right py-3 px-2 font-medium">{year - 1}</th>
              <th className="text-right py-3 px-2 font-medium">YoY Change</th>
              <th className="text-right py-3 px-2 font-medium">YTD {year}</th>
              <th className="text-right py-3 px-2 font-medium">YTD Var</th>
            </tr>
          </thead>
          <tbody>
            {months.map((m) => (
              <tr key={m.month} className="border-b border-gray-100 hover:bg-gray-50">
                <td className="py-3 px-2 font-medium">{m.month_name}</td>
                <td className="py-3 px-2 text-right font-medium">{formatCompact(m.current_year)}</td>
                <td className="py-3 px-4">
                  <div className="flex items-center gap-1">
                    {/* Current year bar */}
                    <div className="flex-1 flex justify-end">
                      <div
                        className="bg-blue-500 h-4 rounded-l"
                        style={{ width: `${(m.current_year / maxRevenue) * 100}%`, minWidth: m.current_year > 0 ? '4px' : '0' }}
                      />
                    </div>
                    <div className="w-px h-6 bg-gray-300" />
                    {/* Previous year bar */}
                    <div className="flex-1">
                      <div
                        className="bg-gray-300 h-4 rounded-r"
                        style={{ width: `${(m.previous_year / maxRevenue) * 100}%`, minWidth: m.previous_year > 0 ? '4px' : '0' }}
                      />
                    </div>
                  </div>
                </td>
                <td className="py-3 px-2 text-right text-gray-500">{formatCompact(m.previous_year)}</td>
                <td className={`py-3 px-2 text-right font-medium ${
                  m.yoy_change_percent > 0 ? 'text-green-600' :
                  m.yoy_change_percent < 0 ? 'text-red-600' : 'text-gray-500'
                }`}>
                  {m.previous_year > 0 ? formatPercent(m.yoy_change_percent) : '-'}
                </td>
                <td className="py-3 px-2 text-right">{formatCompact(m.ytd_current)}</td>
                <td className={`py-3 px-2 text-right font-medium ${
                  m.ytd_variance >= 0 ? 'text-green-600' : 'text-red-600'
                }`}>
                  {m.ytd_variance >= 0 ? '+' : ''}{formatCompact(m.ytd_variance)}
                </td>
              </tr>
            ))}
          </tbody>
          <tfoot>
            <tr className="bg-gray-50 font-bold">
              <td className="py-3 px-2">Full Year</td>
              <td className="py-3 px-2 text-right">{formatCurrency(data?.totals.current_year || 0)}</td>
              <td className="py-3 px-4"></td>
              <td className="py-3 px-2 text-right text-gray-500">{formatCurrency(data?.totals.previous_year || 0)}</td>
              <td className={`py-3 px-2 text-right ${
                (data?.totals.current_year || 0) >= (data?.totals.previous_year || 0) ? 'text-green-600' : 'text-red-600'
              }`}>
                {data?.totals.previous_year ? formatPercent(
                  ((data.totals.current_year - data.totals.previous_year) / data.totals.previous_year) * 100
                ) : '-'}
              </td>
              <td className="py-3 px-2 text-right"></td>
              <td className="py-3 px-2 text-right"></td>
            </tr>
          </tfoot>
        </table>
      </div>

      {/* Legend */}
      <div className="flex items-center gap-4 mt-4 text-sm text-gray-500">
        <div className="flex items-center gap-2">
          <div className="w-4 h-3 bg-blue-500 rounded" />
          <span>{year}</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-4 h-3 bg-gray-300 rounded" />
          <span>{year - 1}</span>
        </div>
      </div>
    </div>
  );
}

// ============ Revenue by Category Section ============
// Breakdown by sales category: Recurring, Consultancy, Cloud, Software

function RevenueCategorySection({ year }: { year: number }) {
  const [showTrend, setShowTrend] = useState(false);

  const { data, isLoading } = useQuery<RevenueByCategoryDetailedResponse>({
    queryKey: ['revenue-by-category-detailed', year],
    queryFn: async () => {
      const response = await apiClient.dashboardRevenueByCategoryDetailed(year);
      return response.data;
    },
  });

  if (isLoading) return <div className="text-center py-4">Loading...</div>;

  const categories = data?.categories || [];
  const summary = data?.summary;

  const colors = [
    { bg: 'bg-blue-500', text: 'text-blue-600', light: 'bg-blue-100' },
    { bg: 'bg-green-500', text: 'text-green-600', light: 'bg-green-100' },
    { bg: 'bg-purple-500', text: 'text-purple-600', light: 'bg-purple-100' },
    { bg: 'bg-orange-500', text: 'text-orange-600', light: 'bg-orange-100' },
    { bg: 'bg-pink-500', text: 'text-pink-600', light: 'bg-pink-100' },
    { bg: 'bg-cyan-500', text: 'text-cyan-600', light: 'bg-cyan-100' },
    { bg: 'bg-yellow-500', text: 'text-yellow-600', light: 'bg-yellow-100' },
    { bg: 'bg-gray-500', text: 'text-gray-600', light: 'bg-gray-100' },
  ];

  return (
    <div className="card">
      <div className="flex items-center justify-between mb-4">
        <SectionHeader
          title="Revenue by Category"
          subtitle="Performance by revenue stream"
          icon={<PieChart className="h-5 w-5" />}
        />
        <button
          onClick={() => setShowTrend(!showTrend)}
          className="text-sm text-blue-600 hover:text-blue-800 flex items-center gap-1"
        >
          {showTrend ? 'Hide' : 'Show'} Monthly Trend
          {showTrend ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
        </button>
      </div>

      {/* Summary bar */}
      <div className="mb-6">
        <div className="flex justify-between text-sm mb-2">
          <span className="font-medium">{year}: {formatCurrency(summary?.total_current || 0)}</span>
          <span className={`font-medium ${(summary?.total_change_percent || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
            {formatPercent(summary?.total_change_percent || 0)} vs {year - 1}
          </span>
        </div>
        <div className="h-8 flex rounded-lg overflow-hidden">
          {categories.map((cat, idx) => (
            <div
              key={cat.category}
              className={`${colors[idx % colors.length].bg} relative group transition-all hover:opacity-80`}
              style={{ width: `${cat.percent_of_total}%` }}
              title={`${cat.category}: ${formatCurrency(cat.current_year)} (${cat.percent_of_total}%)`}
            >
              {cat.percent_of_total > 8 && (
                <span className="absolute inset-0 flex items-center justify-center text-white text-xs font-medium">
                  {cat.percent_of_total.toFixed(0)}%
                </span>
              )}
            </div>
          ))}
        </div>
        <div className="flex flex-wrap gap-3 mt-3">
          {categories.map((cat, idx) => (
            <div key={cat.category} className="flex items-center text-sm">
              <div className={`w-3 h-3 rounded mr-1.5 ${colors[idx % colors.length].bg}`} />
              <span className="text-gray-600">{cat.category}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Detailed table */}
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-200 text-gray-500">
              <th className="text-left py-2 font-medium">Category</th>
              <th className="text-right py-2 font-medium">{year}</th>
              <th className="text-right py-2 font-medium">Mix %</th>
              <th className="text-right py-2 font-medium">{year - 1}</th>
              <th className="text-right py-2 font-medium">Change</th>
              <th className="text-center py-2 font-medium">Trend</th>
            </tr>
          </thead>
          <tbody>
            {categories.map((cat, idx) => (
              <>
                <tr key={cat.category} className="border-b border-gray-100 hover:bg-gray-50">
                  <td className="py-3">
                    <div className="flex items-center gap-2">
                      <div className={`w-2 h-2 rounded ${colors[idx % colors.length].bg}`} />
                      <span className="font-medium">{cat.category}</span>
                    </div>
                  </td>
                  <td className="py-3 text-right font-medium">{formatCurrency(cat.current_year)}</td>
                  <td className="py-3 text-right text-gray-500">{cat.percent_of_total.toFixed(1)}%</td>
                  <td className="py-3 text-right text-gray-500">{formatCurrency(cat.previous_year)}</td>
                  <td className={`py-3 text-right font-medium ${
                    cat.change_percent > 0 ? 'text-green-600' :
                    cat.change_percent < 0 ? 'text-red-600' : 'text-gray-500'
                  }`}>
                    {formatPercent(cat.change_percent)}
                  </td>
                  <td className="py-3 text-center">
                    {cat.trend === 'up' && <TrendingUp className="h-4 w-4 text-green-500 inline" />}
                    {cat.trend === 'down' && <TrendingDown className="h-4 w-4 text-red-500 inline" />}
                    {cat.trend === 'stable' && <span className="text-gray-400">—</span>}
                  </td>
                </tr>
                {/* Monthly trend row (collapsible) */}
                {showTrend && (
                  <tr key={`${cat.category}-trend`} className="bg-gray-50">
                    <td colSpan={6} className="py-2 px-4">
                      <div className="flex items-center gap-1">
                        {cat.monthly_trend.map((mt) => {
                          const maxVal = Math.max(...cat.monthly_trend.map(x => Math.max(x.current, x.previous)));
                          const currHeight = maxVal > 0 ? (mt.current / maxVal) * 24 : 0;
                          const prevHeight = maxVal > 0 ? (mt.previous / maxVal) * 24 : 0;
                          return (
                            <div key={mt.month} className="flex-1 flex items-end gap-px h-8" title={`M${mt.month}: ${formatCompact(mt.current)} vs ${formatCompact(mt.previous)}`}>
                              <div className={`flex-1 ${colors[idx % colors.length].bg} rounded-t`} style={{ height: currHeight }} />
                              <div className="flex-1 bg-gray-300 rounded-t" style={{ height: prevHeight }} />
                            </div>
                          );
                        })}
                      </div>
                      <div className="flex justify-between text-xs text-gray-400 mt-1">
                        <span>Jan</span>
                        <span>Dec</span>
                      </div>
                    </td>
                  </tr>
                )}
              </>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ============ New vs Existing Business Section ============
// Critical for understanding growth sources

function NewVsExistingSection({ year }: { year: number }) {
  const { data, isLoading } = useQuery<NewVsExistingRevenueResponse>({
    queryKey: ['new-vs-existing', year],
    queryFn: async () => {
      const response = await apiClient.dashboardNewVsExistingRevenue(year);
      return response.data;
    },
  });

  if (isLoading) return <div className="text-center py-4">Loading...</div>;

  const newBiz = data?.new_business;
  const existing = data?.existing_business;

  const newThisYear = newBiz?.this_year;
  const newLastYear = newBiz?.last_year_acquired;

  return (
    <div className="card">
      <SectionHeader
        title="New vs Existing Business"
        subtitle="Where is growth coming from?"
        icon={<UserPlus className="h-5 w-5" />}
      />

      {/* Visual breakdown */}
      <div className="mb-6">
        <div className="h-10 flex rounded-lg overflow-hidden">
          <div
            className="bg-green-500 flex items-center justify-center text-white text-sm font-medium"
            style={{ width: `${newThisYear?.percent_of_total || 0}%` }}
            title={`New this year: ${formatCurrency(newThisYear?.revenue || 0)}`}
          >
            {(newThisYear?.percent_of_total || 0) > 5 && `${newThisYear?.percent_of_total.toFixed(0)}%`}
          </div>
          <div
            className="bg-blue-400 flex items-center justify-center text-white text-sm font-medium"
            style={{ width: `${newLastYear?.percent_of_total || 0}%` }}
            title={`Acquired last year: ${formatCurrency(newLastYear?.revenue || 0)}`}
          >
            {(newLastYear?.percent_of_total || 0) > 5 && `${newLastYear?.percent_of_total.toFixed(0)}%`}
          </div>
          <div
            className="bg-gray-400 flex items-center justify-center text-white text-sm font-medium"
            style={{ width: `${existing?.percent_of_total || 0}%` }}
            title={`Existing customers: ${formatCurrency(existing?.revenue || 0)}`}
          >
            {(existing?.percent_of_total || 0) > 10 && `${existing?.percent_of_total.toFixed(0)}%`}
          </div>
        </div>
        <div className="flex gap-4 mt-3 text-sm">
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded bg-green-500" />
            <span>New {year}</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded bg-blue-400" />
            <span>Acquired {year - 1}</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded bg-gray-400" />
            <span>Existing</span>
          </div>
        </div>
      </div>

      {/* Detailed breakdown */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="bg-green-50 p-4 rounded-lg border border-green-200">
          <div className="flex items-center gap-2 mb-2">
            <UserPlus className="h-5 w-5 text-green-600" />
            <span className="font-semibold text-green-800">New Customers ({year})</span>
          </div>
          <p className="text-2xl font-bold text-green-700">{formatCurrency(newThisYear?.revenue || 0)}</p>
          <p className="text-sm text-green-600 mt-1">{newThisYear?.customers || 0} customers</p>
          <p className="text-sm text-green-600">{formatCurrency(newThisYear?.avg_per_customer || 0)} avg/customer</p>
        </div>

        <div className="bg-blue-50 p-4 rounded-lg border border-blue-200">
          <div className="flex items-center gap-2 mb-2">
            <Users className="h-5 w-5 text-blue-600" />
            <span className="font-semibold text-blue-800">Won in {year - 1}</span>
          </div>
          <p className="text-2xl font-bold text-blue-700">{formatCurrency(newLastYear?.revenue || 0)}</p>
          <p className="text-sm text-blue-600 mt-1">{newLastYear?.customers || 0} customers</p>
          <p className="text-sm text-blue-600">{formatCurrency(newLastYear?.avg_per_customer || 0)} avg/customer</p>
        </div>

        <div className="bg-gray-50 p-4 rounded-lg border border-gray-200">
          <div className="flex items-center gap-2 mb-2">
            <Shield className="h-5 w-5 text-gray-600" />
            <span className="font-semibold text-gray-800">Existing Base</span>
          </div>
          <p className="text-2xl font-bold text-gray-700">{formatCurrency(existing?.revenue || 0)}</p>
          <p className="text-sm text-gray-600 mt-1">{existing?.customers || 0} customers</p>
          <p className="text-sm text-gray-600">{formatCurrency(existing?.avg_per_customer || 0)} avg/customer</p>
        </div>
      </div>
    </div>
  );
}

// ============ Customer Churn & Retention Section ============
// Customer health indicators

function ChurnAnalysisSection({ year }: { year: number }) {
  const [showDetails, setShowDetails] = useState(false);

  const { data, isLoading } = useQuery<CustomerChurnAnalysisResponse>({
    queryKey: ['customer-churn', year],
    queryFn: async () => {
      const response = await apiClient.dashboardCustomerChurnAnalysis(year);
      return response.data;
    },
  });

  if (isLoading) return <div className="text-center py-4">Loading...</div>;

  const summary = data?.summary;

  return (
    <div className="card">
      <SectionHeader
        title="Customer Retention & Churn"
        subtitle="Customer health indicators"
        icon={<UserMinus className="h-5 w-5" />}
      />

      {/* Key metrics */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
        <div className={`p-4 rounded-lg ${
          (summary?.retention_rate || 0) >= 90 ? 'bg-green-50 border border-green-200' :
          (summary?.retention_rate || 0) >= 80 ? 'bg-yellow-50 border border-yellow-200' :
          'bg-red-50 border border-red-200'
        }`}>
          <p className="text-sm text-gray-600">Retention Rate</p>
          <p className={`text-2xl font-bold ${
            (summary?.retention_rate || 0) >= 90 ? 'text-green-700' :
            (summary?.retention_rate || 0) >= 80 ? 'text-yellow-700' : 'text-red-700'
          }`}>
            {summary?.retention_rate.toFixed(1)}%
          </p>
        </div>

        <div className="p-4 rounded-lg bg-red-50 border border-red-200">
          <p className="text-sm text-gray-600">Churned</p>
          <p className="text-xl font-bold text-red-700">{summary?.churned_count || 0}</p>
          <p className="text-sm text-red-600">{formatCurrency(summary?.churned_revenue || 0)} lost</p>
        </div>

        <div className="p-4 rounded-lg bg-orange-50 border border-orange-200">
          <p className="text-sm text-gray-600">At Risk</p>
          <p className="text-xl font-bold text-orange-700">{summary?.at_risk_count || 0}</p>
          <p className="text-sm text-orange-600">{formatCurrency(summary?.at_risk_revenue || 0)} at risk</p>
        </div>

        <div className="p-4 rounded-lg bg-green-50 border border-green-200">
          <p className="text-sm text-gray-600">Growing</p>
          <p className="text-xl font-bold text-green-700">{summary?.growing_count || 0}</p>
          <p className="text-sm text-green-600">Stable: {summary?.stable_count || 0}</p>
        </div>
      </div>

      {/* Detail toggle */}
      <button
        onClick={() => setShowDetails(!showDetails)}
        className="text-sm text-blue-600 hover:text-blue-800 flex items-center gap-1 mb-4"
      >
        {showDetails ? 'Hide' : 'Show'} Customer Details
        {showDetails ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
      </button>

      {showDetails && (
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {/* Churned customers */}
          <div>
            <h4 className="font-medium text-red-700 mb-2">Churned Customers (Top 10)</h4>
            <div className="space-y-2 max-h-60 overflow-y-auto">
              {data?.churned_customers.map((c) => (
                <div key={c.account} className="text-sm p-2 bg-red-50 rounded">
                  <p className="font-medium truncate">{c.customer_name}</p>
                  <p className="text-red-600">{formatCurrency(c.last_year_revenue)} LY</p>
                </div>
              ))}
            </div>
          </div>

          {/* At risk */}
          <div>
            <h4 className="font-medium text-orange-700 mb-2">At Risk (Down &gt;50%)</h4>
            <div className="space-y-2 max-h-60 overflow-y-auto">
              {data?.at_risk_customers.map((c) => (
                <div key={c.account} className="text-sm p-2 bg-orange-50 rounded">
                  <p className="font-medium truncate">{c.customer_name}</p>
                  <p className="text-orange-600">{formatPercent(c.change_percent)} ({formatCurrency(c.current_revenue)})</p>
                </div>
              ))}
            </div>
          </div>

          {/* Growing */}
          <div>
            <h4 className="font-medium text-green-700 mb-2">Growing Customers</h4>
            <div className="space-y-2 max-h-60 overflow-y-auto">
              {data?.growing_customers.map((c) => (
                <div key={c.account} className="text-sm p-2 bg-green-50 rounded">
                  <p className="font-medium truncate">{c.customer_name}</p>
                  <p className="text-green-600">{formatPercent(c.change_percent)} ({formatCurrency(c.current_revenue)})</p>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ============ Forward Looking Indicators Section ============
// Predictions and risk flags

function ForwardIndicatorsSection({ year }: { year: number }) {
  const { data, isLoading } = useQuery<ForwardIndicatorsResponse>({
    queryKey: ['forward-indicators', year],
    queryFn: async () => {
      const response = await apiClient.dashboardForwardIndicators(year);
      return response.data;
    },
  });

  if (isLoading) return <div className="text-center py-4">Loading...</div>;

  const runRates = data?.run_rates;
  const projections = data?.projections;
  const trend = data?.trend;
  const risks = data?.risk_flags || [];

  return (
    <div className="card">
      <SectionHeader
        title="Forward-Looking Indicators"
        subtitle="Run rates, projections and risk flags"
        icon={<Zap className="h-5 w-5" />}
      />

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Run rates */}
        <div>
          <h4 className="font-medium text-gray-700 mb-3">Run Rates</h4>
          <div className="space-y-3">
            <div className="flex justify-between items-center p-3 bg-gray-50 rounded">
              <span className="text-sm text-gray-600">3-Month Average</span>
              <div className="text-right">
                <p className="font-semibold">{formatCurrency(runRates?.monthly_3m_avg || 0)}/mo</p>
                <p className="text-xs text-gray-500">{formatCurrency(runRates?.annual_3m_basis || 0)} annualized</p>
              </div>
            </div>
            <div className="flex justify-between items-center p-3 bg-gray-50 rounded">
              <span className="text-sm text-gray-600">6-Month Average</span>
              <div className="text-right">
                <p className="font-semibold">{formatCurrency(runRates?.monthly_6m_avg || 0)}/mo</p>
                <p className="text-xs text-gray-500">{formatCurrency(runRates?.annual_6m_basis || 0)} annualized</p>
              </div>
            </div>
            <div className="flex justify-between items-center p-3 bg-gray-50 rounded">
              <span className="text-sm text-gray-600">YTD Average</span>
              <div className="text-right">
                <p className="font-semibold">{formatCurrency(runRates?.monthly_ytd_avg || 0)}/mo</p>
                <p className="text-xs text-gray-500">{formatCurrency(runRates?.annual_ytd_basis || 0)} annualized</p>
              </div>
            </div>
          </div>

          {/* Trend indicator */}
          <div className={`mt-4 p-3 rounded-lg ${
            trend?.direction === 'accelerating' ? 'bg-green-50 border border-green-200' :
            trend?.direction === 'decelerating' ? 'bg-red-50 border border-red-200' :
            'bg-gray-50 border border-gray-200'
          }`}>
            <div className="flex items-center gap-2">
              {trend?.direction === 'accelerating' && <TrendingUp className="h-5 w-5 text-green-600" />}
              {trend?.direction === 'decelerating' && <TrendingDown className="h-5 w-5 text-red-600" />}
              {trend?.direction === 'stable' && <ArrowRight className="h-5 w-5 text-gray-600" />}
              <span className={`font-medium capitalize ${
                trend?.direction === 'accelerating' ? 'text-green-700' :
                trend?.direction === 'decelerating' ? 'text-red-700' : 'text-gray-700'
              }`}>
                {trend?.direction} Trend
              </span>
            </div>
            <p className="text-sm text-gray-600 mt-1">
              Recent 3mo: {formatCurrency(trend?.recent_3_months || 0)} vs Prior 3mo: {formatCurrency(trend?.prior_3_months || 0)}
            </p>
          </div>
        </div>

        {/* Projections and risks */}
        <div>
          <h4 className="font-medium text-gray-700 mb-3">Full Year Projections</h4>
          <div className="space-y-3">
            <div className="flex justify-between items-center p-3 bg-blue-50 rounded border border-blue-200">
              <span className="text-sm text-blue-700">Conservative</span>
              <span className="font-semibold text-blue-700">{formatCurrency(projections?.conservative || 0)}</span>
            </div>
            <div className="flex justify-between items-center p-3 bg-blue-100 rounded border border-blue-300">
              <span className="text-sm text-blue-800 font-medium">Midpoint Forecast</span>
              <span className="font-bold text-blue-800">{formatCurrency(projections?.midpoint || 0)}</span>
            </div>
            <div className="flex justify-between items-center p-3 bg-blue-50 rounded border border-blue-200">
              <span className="text-sm text-blue-700">Optimistic</span>
              <span className="font-semibold text-blue-700">{formatCurrency(projections?.optimistic || 0)}</span>
            </div>
            <div className="flex justify-between items-center p-3 bg-gray-50 rounded">
              <span className="text-sm text-gray-600">Prior Year Actual</span>
              <span className="font-semibold">{formatCurrency(projections?.prior_year_actual || 0)}</span>
            </div>
            <div className={`text-center p-2 rounded ${
              (projections?.vs_prior_year_percent || 0) >= 0 ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'
            }`}>
              <span className="font-medium">
                Projection vs Prior Year: {formatPercent(projections?.vs_prior_year_percent || 0)}
              </span>
            </div>
          </div>

          {/* Risk flags */}
          {risks.length > 0 && (
            <div className="mt-4">
              <h4 className="font-medium text-gray-700 mb-2 flex items-center gap-2">
                <AlertTriangle className="h-4 w-4 text-orange-500" />
                Risk Flags
              </h4>
              <div className="space-y-2">
                {risks.map((risk, idx) => (
                  <div
                    key={idx}
                    className={`p-2 rounded text-sm ${
                      risk.severity === 'high' ? 'bg-red-100 text-red-700 border border-red-200' :
                      risk.severity === 'medium' ? 'bg-orange-100 text-orange-700 border border-orange-200' :
                      'bg-yellow-100 text-yellow-700 border border-yellow-200'
                    }`}
                  >
                    {risk.message}
                  </div>
                ))}
              </div>
            </div>
          )}

          {risks.length === 0 && (
            <div className="mt-4 p-3 bg-green-50 rounded-lg border border-green-200 text-center">
              <Shield className="h-5 w-5 text-green-600 inline mr-2" />
              <span className="text-green-700 font-medium">No risk flags detected</span>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// ============ Customer Concentration Section ============
// Uses existing endpoint

function CustomerConcentrationSection({ year }: { year: number }) {
  const [showAllCustomers, setShowAllCustomers] = useState(false);

  const { data: concentration } = useQuery<DashboardCustomerConcentrationResponse>({
    queryKey: ['customer-concentration', year],
    queryFn: async () => {
      const response = await apiClient.dashboardCustomerConcentration(year);
      return response.data;
    },
  });

  const { data: topCustomers } = useQuery<DashboardTopCustomersResponse>({
    queryKey: ['top-customers', year],
    queryFn: async () => {
      const response = await apiClient.dashboardTopCustomers(year, 20);
      return response.data;
    },
  });

  const conc = concentration?.concentration;
  const customers = topCustomers?.customers || [];

  return (
    <div className="card">
      <SectionHeader
        title="Customer Concentration"
        subtitle="Revenue dependency analysis"
        icon={<Users className="h-5 w-5" />}
      />

      {/* Concentration metrics */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-6">
        <div className="p-3 bg-gray-50 rounded text-center">
          <p className="text-sm text-gray-500">Total Customers</p>
          <p className="text-xl font-bold">{conc?.total_customers || 0}</p>
        </div>
        <div className="p-3 bg-gray-50 rounded text-center">
          <p className="text-sm text-gray-500">Top 1</p>
          <p className="text-xl font-bold">{conc?.top_1_percent || 0}%</p>
        </div>
        <div className="p-3 bg-gray-50 rounded text-center">
          <p className="text-sm text-gray-500">Top 5</p>
          <p className="text-xl font-bold">{conc?.top_5_percent || 0}%</p>
        </div>
        <div className="p-3 bg-gray-50 rounded text-center">
          <p className="text-sm text-gray-500">Top 10</p>
          <p className="text-xl font-bold">{conc?.top_10_percent || 0}%</p>
        </div>
        <div className={`p-3 rounded text-center ${
          conc?.risk_level === 'high' ? 'bg-red-100 border border-red-200' :
          conc?.risk_level === 'medium' ? 'bg-yellow-100 border border-yellow-200' :
          'bg-green-100 border border-green-200'
        }`}>
          <p className="text-sm text-gray-500">Risk Level</p>
          <p className={`text-xl font-bold capitalize ${
            conc?.risk_level === 'high' ? 'text-red-700' :
            conc?.risk_level === 'medium' ? 'text-yellow-700' : 'text-green-700'
          }`}>
            {conc?.risk_level || 'Low'}
          </p>
        </div>
      </div>

      {/* Top customers table */}
      <div className="flex items-center justify-between mb-3">
        <h4 className="font-medium text-gray-700">Top Customers by Revenue</h4>
        <button
          onClick={() => setShowAllCustomers(!showAllCustomers)}
          className="text-sm text-blue-600 hover:text-blue-800 flex items-center"
        >
          {showAllCustomers ? 'Show Less' : 'Show All 20'}
          {showAllCustomers ? <ChevronUp className="h-4 w-4 ml-1" /> : <ChevronDown className="h-4 w-4 ml-1" />}
        </button>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-200 text-gray-500">
              <th className="text-left py-2">#</th>
              <th className="text-left py-2">Customer</th>
              <th className="text-right py-2">{year}</th>
              <th className="text-right py-2">{year - 1}</th>
              <th className="text-right py-2">% Total</th>
              <th className="text-right py-2">Cumulative</th>
              <th className="text-center py-2">Trend</th>
            </tr>
          </thead>
          <tbody>
            {(showAllCustomers ? customers : customers.slice(0, 10)).map((c, idx) => (
              <tr key={c.account_code} className="border-b border-gray-100 hover:bg-gray-50">
                <td className="py-2 text-gray-500">{idx + 1}</td>
                <td className="py-2">
                  <span className="font-medium">{c.customer_name}</span>
                  <span className="text-gray-400 ml-2 text-xs">{c.account_code}</span>
                </td>
                <td className="py-2 text-right font-medium">{formatCurrency(c.current_year)}</td>
                <td className="py-2 text-right text-gray-500">{formatCurrency(c.previous_year)}</td>
                <td className="py-2 text-right">{c.percent_of_total.toFixed(1)}%</td>
                <td className="py-2 text-right text-gray-500">{c.cumulative_percent.toFixed(1)}%</td>
                <td className="py-2 text-center">
                  {c.trend === 'up' && <TrendingUp className="h-4 w-4 text-green-500 inline" />}
                  {c.trend === 'down' && <TrendingDown className="h-4 w-4 text-red-500 inline" />}
                  {c.trend === 'stable' && <span className="text-gray-400">—</span>}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ============ Main Dashboard Component ============

type DashboardView = 'executive' | 'performance' | 'categories' | 'customers' | 'forward';

export function SalesDashboards() {
  const [activeView, setActiveView] = useState<DashboardView>('executive');
  const [year, setYear] = useState<number | null>(null);

  // Fetch available years
  const { data: yearsData } = useQuery({
    queryKey: ['available-years'],
    queryFn: async () => {
      const response = await apiClient.dashboardAvailableYears();
      return response.data;
    },
  });

  const availableYears = yearsData?.years || [];
  const defaultYear = yearsData?.default_year || 2024;

  if (year === null && defaultYear) {
    setYear(defaultYear);
  }

  const selectedYear = year || defaultYear;

  const views = [
    { id: 'executive' as DashboardView, label: 'Executive Summary', icon: Activity, description: 'At-a-glance KPIs' },
    { id: 'performance' as DashboardView, label: 'Revenue Performance', icon: BarChart3, description: 'Monthly comparison' },
    { id: 'categories' as DashboardView, label: 'Category Analysis', icon: PieChart, description: 'Revenue breakdown' },
    { id: 'customers' as DashboardView, label: 'Customer Health', icon: Users, description: 'Retention & churn' },
    { id: 'forward' as DashboardView, label: 'Forward Indicators', icon: Target, description: 'Projections & risks' },
  ];

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
        <div className="flex items-center gap-3">
          <BarChart3 className="h-8 w-8 text-blue-600" />
          <div>
            <h1 className="text-2xl font-bold text-gray-900">Sales Dashboard</h1>
            <p className="text-sm text-gray-600">Intsys UK Performance Analytics</p>
          </div>
        </div>
        <div className="flex items-center gap-4">
          {/* Year Selector */}
          <select
            value={selectedYear}
            onChange={(e) => setYear(parseInt(e.target.value))}
            className="select w-32"
          >
            {availableYears.length > 0 ? (
              availableYears.map((y) => (
                <option key={y.year} value={y.year}>
                  {y.year}
                </option>
              ))
            ) : (
              <>
                <option value={2024}>2024</option>
                <option value={2023}>2023</option>
              </>
            )}
          </select>
        </div>
      </div>

      {/* Navigation Tabs */}
      <div className="flex flex-wrap gap-2 border-b border-gray-200 pb-4">
        {views.map((view) => {
          const Icon = view.icon;
          const isActive = activeView === view.id;
          return (
            <button
              key={view.id}
              onClick={() => setActiveView(view.id)}
              className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-all ${
                isActive
                  ? 'bg-blue-600 text-white shadow-md'
                  : 'bg-white text-gray-600 hover:bg-gray-100 border border-gray-200'
              }`}
            >
              <Icon className="h-4 w-4" />
              <span>{view.label}</span>
            </button>
          );
        })}
      </div>

      {/* Dashboard Content */}
      <div className="space-y-6">
        {activeView === 'executive' && (
          <>
            <ExecutiveSummarySection year={selectedYear} />
            <MonthlyComparisonSection year={selectedYear} />
          </>
        )}

        {activeView === 'performance' && (
          <>
            <MonthlyComparisonSection year={selectedYear} />
            <NewVsExistingSection year={selectedYear} />
          </>
        )}

        {activeView === 'categories' && (
          <>
            <RevenueCategorySection year={selectedYear} />
          </>
        )}

        {activeView === 'customers' && (
          <>
            <CustomerConcentrationSection year={selectedYear} />
            <ChurnAnalysisSection year={selectedYear} />
          </>
        )}

        {activeView === 'forward' && (
          <>
            <ForwardIndicatorsSection year={selectedYear} />
          </>
        )}
      </div>
    </div>
  );
}

export default SalesDashboards;
