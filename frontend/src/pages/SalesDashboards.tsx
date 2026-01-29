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
} from 'lucide-react';
import apiClient from '../api/client';
import type {
  DashboardCeoKpisResponse,
  DashboardRevenueOverTimeResponse,
  DashboardRevenueCompositionResponse,
  DashboardTopCustomersResponse,
  DashboardCustomerConcentrationResponse,
  DashboardCustomerLifecycleResponse,
  DashboardMarginByCategoryResponse,
} from '../api/client';

type DashboardView = 'ceo' | 'revenue' | 'customers' | 'margin';

interface KPICardProps {
  title: string;
  value: string | number;
  subtitle?: string;
  trend?: 'up' | 'down' | 'neutral';
  trendValue?: string;
  icon?: React.ReactNode;
}

function KPICard({ title, value, subtitle, trend, trendValue, icon }: KPICardProps) {
  return (
    <div className="card">
      <div className="flex items-start justify-between">
        <div>
          <p className="text-sm text-gray-500">{title}</p>
          <p className="text-2xl font-bold text-gray-900 mt-1">{value}</p>
          {subtitle && <p className="text-sm text-gray-500 mt-1">{subtitle}</p>}
        </div>
        <div className="flex flex-col items-end">
          {icon && <div className="text-gray-400">{icon}</div>}
          {trend && trendValue && (
            <div className={`flex items-center mt-2 text-sm ${
              trend === 'up' ? 'text-green-600' : trend === 'down' ? 'text-red-600' : 'text-gray-500'
            }`}>
              {trend === 'up' ? <TrendingUp className="h-4 w-4 mr-1" /> :
               trend === 'down' ? <TrendingDown className="h-4 w-4 mr-1" /> : null}
              {trendValue}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function formatCurrency(value: number): string {
  return new Intl.NumberFormat('en-GB', {
    style: 'currency',
    currency: 'GBP',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value);
}

function formatPercent(value: number): string {
  return `${value >= 0 ? '+' : ''}${value.toFixed(1)}%`;
}

// CEO View Component
function CEOView({ year }: { year: number }) {
  const { data: kpis, isLoading: kpisLoading } = useQuery<DashboardCeoKpisResponse>({
    queryKey: ['ceo-kpis', year],
    queryFn: async () => {
      const response = await apiClient.dashboardCeoKpis(year);
      return response.data;
    },
  });

  const { data: revenueData } = useQuery<DashboardRevenueOverTimeResponse>({
    queryKey: ['revenue-over-time', year],
    queryFn: async () => {
      const response = await apiClient.dashboardRevenueOverTime(year);
      return response.data;
    },
  });

  if (kpisLoading) {
    return <div className="text-center py-8">Loading...</div>;
  }

  const k = kpis?.kpis || {
    mtd: 0,
    qtd: 0,
    ytd: 0,
    yoy_growth_percent: 0,
    avg_monthly_3m: 0,
    avg_monthly_6m: 0,
    avg_monthly_12m: 0,
    active_customers: 0,
    revenue_per_customer: 0,
    year: 2026,
    month: 1,
    quarter: 1,
  };

  return (
    <div className="space-y-6">
      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KPICard
          title="YTD Revenue"
          value={formatCurrency(k.ytd || 0)}
          trend={(k.yoy_growth_percent || 0) > 0 ? 'up' : (k.yoy_growth_percent || 0) < 0 ? 'down' : 'neutral'}
          trendValue={`${formatPercent(k.yoy_growth_percent || 0)} YoY`}
          icon={<DollarSign className="h-6 w-6" />}
        />
        <KPICard
          title="MTD Revenue"
          value={formatCurrency(k.mtd || 0)}
          subtitle={`Q${k.quarter || 1} to date: ${formatCurrency(k.qtd || 0)}`}
          icon={<Activity className="h-6 w-6" />}
        />
        <KPICard
          title="Active Customers"
          value={k.active_customers || 0}
          subtitle={`Rev/customer: ${formatCurrency(k.revenue_per_customer || 0)}`}
          icon={<Users className="h-6 w-6" />}
        />
        <KPICard
          title="Avg Monthly (12m)"
          value={formatCurrency(k.avg_monthly_12m || 0)}
          subtitle={`3m: ${formatCurrency(k.avg_monthly_3m || 0)}`}
          icon={<BarChart3 className="h-6 w-6" />}
        />
      </div>

      {/* Monthly Revenue Chart */}
      <div className="card">
        <h3 className="text-lg font-semibold mb-4">Monthly Revenue</h3>
        {revenueData?.months && (
          <div className="space-y-2">
            <div className="flex text-xs text-gray-500 mb-2">
              <div className="w-16">Month</div>
              <div className="flex-1 text-right">{year}</div>
              <div className="flex-1 text-right">{year - 1}</div>
              <div className="w-20 text-right">Change</div>
            </div>
            {revenueData.months.map((m: any) => {
              const change = m.previous_total > 0
                ? ((m.current_total - m.previous_total) / m.previous_total * 100)
                : 0;
              return (
                <div key={m.month} className="flex items-center text-sm border-b border-gray-100 py-2">
                  <div className="w-16 font-medium">{m.month_name}</div>
                  <div className="flex-1">
                    <div className="flex items-center justify-end">
                      <div className="w-32 bg-gray-200 rounded-full h-4 mr-2">
                        <div
                          className="bg-blue-600 h-4 rounded-full"
                          style={{
                            width: `${Math.min(100, (m.current_total / (revenueData.months.reduce((max: number, x: any) =>
                              Math.max(max, x.current_total, x.previous_total), 0) || 1)) * 100)}%`
                          }}
                        />
                      </div>
                      <span className="w-20 text-right">{formatCurrency(m.current_total)}</span>
                    </div>
                  </div>
                  <div className="flex-1">
                    <div className="flex items-center justify-end">
                      <div className="w-32 bg-gray-200 rounded-full h-4 mr-2">
                        <div
                          className="bg-gray-400 h-4 rounded-full"
                          style={{
                            width: `${Math.min(100, (m.previous_total / (revenueData.months.reduce((max: number, x: any) =>
                              Math.max(max, x.current_total, x.previous_total), 0) || 1)) * 100)}%`
                          }}
                        />
                      </div>
                      <span className="w-20 text-right">{formatCurrency(m.previous_total)}</span>
                    </div>
                  </div>
                  <div className={`w-20 text-right ${change > 0 ? 'text-green-600' : change < 0 ? 'text-red-600' : ''}`}>
                    {m.previous_total > 0 ? formatPercent(change) : '-'}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}

// Revenue Composition Component
function RevenueComposition({ year }: { year: number }) {
  const { data, isLoading } = useQuery<DashboardRevenueCompositionResponse>({
    queryKey: ['revenue-composition', year],
    queryFn: async () => {
      const response = await apiClient.dashboardRevenueComposition(year);
      return response.data;
    },
  });

  if (isLoading) {
    return <div className="text-center py-8">Loading...</div>;
  }

  const categories = data?.categories || [];
  const total = data?.current_total || 0;

  // Colors for categories
  const colors = [
    'bg-blue-500', 'bg-green-500', 'bg-yellow-500', 'bg-purple-500',
    'bg-red-500', 'bg-indigo-500', 'bg-pink-500', 'bg-gray-500'
  ];

  return (
    <div className="space-y-6">
      {/* Summary Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KPICard
          title={`Total Revenue ${year}`}
          value={formatCurrency(total)}
          trend={(data?.current_total || 0) > (data?.previous_total || 0) ? 'up' : 'down'}
          trendValue={`vs ${formatCurrency(data?.previous_total || 0)} prev`}
          icon={<DollarSign className="h-6 w-6" />}
        />
      </div>

      {/* Category Breakdown */}
      <div className="card">
        <h3 className="text-lg font-semibold mb-4">Revenue by Category</h3>

        {/* Stacked Bar Visualization */}
        <div className="mb-6">
          <div className="h-8 flex rounded-lg overflow-hidden">
            {categories.map((cat: any, idx: number) => (
              <div
                key={cat.category}
                className={`${colors[idx % colors.length]} relative group`}
                style={{ width: `${cat.current_percent}%` }}
                title={`${cat.category}: ${formatCurrency(cat.current_year)} (${cat.current_percent}%)`}
              >
                {cat.current_percent > 10 && (
                  <span className="absolute inset-0 flex items-center justify-center text-white text-xs font-medium">
                    {cat.current_percent.toFixed(0)}%
                  </span>
                )}
              </div>
            ))}
          </div>
          <div className="flex flex-wrap gap-3 mt-3">
            {categories.map((cat: any, idx: number) => (
              <div key={cat.category} className="flex items-center text-sm">
                <div className={`w-3 h-3 rounded mr-1 ${colors[idx % colors.length]}`} />
                <span className="text-gray-600">{cat.category}</span>
              </div>
            ))}
          </div>
        </div>

        {/* Detailed Table */}
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-200">
                <th className="text-left py-2 font-medium text-gray-500">Category</th>
                <th className="text-right py-2 font-medium text-gray-500">{year}</th>
                <th className="text-right py-2 font-medium text-gray-500">% Mix</th>
                <th className="text-right py-2 font-medium text-gray-500">{year - 1}</th>
                <th className="text-right py-2 font-medium text-gray-500">% Mix</th>
                <th className="text-right py-2 font-medium text-gray-500">Change</th>
              </tr>
            </thead>
            <tbody>
              {categories.map((cat: any) => (
                <tr key={cat.category} className="border-b border-gray-100">
                  <td className="py-2 font-medium">{cat.category}</td>
                  <td className="py-2 text-right">{formatCurrency(cat.current_year)}</td>
                  <td className="py-2 text-right text-gray-500">{cat.current_percent.toFixed(1)}%</td>
                  <td className="py-2 text-right">{formatCurrency(cat.previous_year)}</td>
                  <td className="py-2 text-right text-gray-500">{cat.previous_percent.toFixed(1)}%</td>
                  <td className={`py-2 text-right ${
                    cat.change_percent > 0 ? 'text-green-600' : cat.change_percent < 0 ? 'text-red-600' : ''
                  }`}>
                    {formatPercent(cat.change_percent)}
                  </td>
                </tr>
              ))}
            </tbody>
            <tfoot>
              <tr className="font-bold">
                <td className="py-2">Total</td>
                <td className="py-2 text-right">{formatCurrency(data?.current_total || 0)}</td>
                <td className="py-2 text-right">100%</td>
                <td className="py-2 text-right">{formatCurrency(data?.previous_total || 0)}</td>
                <td className="py-2 text-right">100%</td>
                <td className="py-2 text-right"></td>
              </tr>
            </tfoot>
          </table>
        </div>
      </div>
    </div>
  );
}

// Customer Analysis Component
function CustomerAnalysis({ year }: { year: number }) {
  const [showAllCustomers, setShowAllCustomers] = useState(false);

  const { data: topCustomers, isLoading: customersLoading } = useQuery<DashboardTopCustomersResponse>({
    queryKey: ['top-customers', year],
    queryFn: async () => {
      const response = await apiClient.dashboardTopCustomers(year, 20);
      return response.data;
    },
  });

  const { data: concentration } = useQuery<DashboardCustomerConcentrationResponse>({
    queryKey: ['customer-concentration', year],
    queryFn: async () => {
      const response = await apiClient.dashboardCustomerConcentration(year);
      return response.data;
    },
  });

  const { data: lifecycle } = useQuery<DashboardCustomerLifecycleResponse>({
    queryKey: ['customer-lifecycle', year],
    queryFn: async () => {
      const response = await apiClient.dashboardCustomerLifecycle(year);
      return response.data;
    },
  });

  if (customersLoading) {
    return <div className="text-center py-8">Loading...</div>;
  }

  const customers = topCustomers?.customers || [];
  const conc = concentration?.concentration || {
    total_customers: 0,
    total_revenue: 0,
    top_1_percent: 0,
    top_3_percent: 0,
    top_5_percent: 0,
    top_10_percent: 0,
    risk_level: 'low' as const,
  };
  const ages = lifecycle?.age_bands || {
    less_than_1_year: { count: 0, revenue: 0 },
    '1_to_3_years': { count: 0, revenue: 0 },
    '3_to_5_years': { count: 0, revenue: 0 },
    over_5_years: { count: 0, revenue: 0 },
  };

  return (
    <div className="space-y-6">
      {/* Concentration Risk */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
        <KPICard
          title="Total Customers"
          value={conc.total_customers || 0}
          icon={<Users className="h-6 w-6" />}
        />
        <KPICard
          title="Top 1 Customer"
          value={`${conc.top_1_percent || 0}%`}
          subtitle="of revenue"
        />
        <KPICard
          title="Top 5 Customers"
          value={`${conc.top_5_percent || 0}%`}
          subtitle="of revenue"
        />
        <KPICard
          title="Top 10 Customers"
          value={`${conc.top_10_percent || 0}%`}
          subtitle="of revenue"
        />
        <div className={`card ${
          conc.risk_level === 'high' ? 'bg-red-50 border-red-200' :
          conc.risk_level === 'medium' ? 'bg-yellow-50 border-yellow-200' :
          'bg-green-50 border-green-200'
        }`}>
          <div className="flex items-center gap-2">
            {conc.risk_level === 'high' || conc.risk_level === 'medium' ? (
              <AlertTriangle className={`h-6 w-6 ${conc.risk_level === 'high' ? 'text-red-500' : 'text-yellow-500'}`} />
            ) : null}
            <div>
              <p className="text-sm text-gray-500">Concentration Risk</p>
              <p className={`text-xl font-bold capitalize ${
                conc.risk_level === 'high' ? 'text-red-700' :
                conc.risk_level === 'medium' ? 'text-yellow-700' :
                'text-green-700'
              }`}>
                {conc.risk_level || 'Low'}
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Customer Lifecycle */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KPICard
          title="New Customers"
          value={lifecycle?.new_customers || 0}
          subtitle={`This year (${year})`}
          trend="up"
          icon={<Users className="h-6 w-6" />}
        />
        <KPICard
          title="Lost/Dormant"
          value={lifecycle?.lost_customers || 0}
          subtitle="No activity this year"
          trend="down"
        />
      </div>

      {/* Revenue by Customer Age */}
      <div className="card">
        <h3 className="text-lg font-semibold mb-4">Revenue by Customer Tenure</h3>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <div className="bg-blue-50 p-4 rounded-lg">
            <p className="text-sm text-blue-600">Less than 1 year</p>
            <p className="text-xl font-bold text-blue-800">{formatCurrency(ages.less_than_1_year?.revenue || 0)}</p>
            <p className="text-sm text-blue-600">{ages.less_than_1_year?.count || 0} customers</p>
          </div>
          <div className="bg-green-50 p-4 rounded-lg">
            <p className="text-sm text-green-600">1-3 years</p>
            <p className="text-xl font-bold text-green-800">{formatCurrency(ages['1_to_3_years']?.revenue || 0)}</p>
            <p className="text-sm text-green-600">{ages['1_to_3_years']?.count || 0} customers</p>
          </div>
          <div className="bg-yellow-50 p-4 rounded-lg">
            <p className="text-sm text-yellow-600">3-5 years</p>
            <p className="text-xl font-bold text-yellow-800">{formatCurrency(ages['3_to_5_years']?.revenue || 0)}</p>
            <p className="text-sm text-yellow-600">{ages['3_to_5_years']?.count || 0} customers</p>
          </div>
          <div className="bg-purple-50 p-4 rounded-lg">
            <p className="text-sm text-purple-600">5+ years</p>
            <p className="text-xl font-bold text-purple-800">{formatCurrency(ages.over_5_years?.revenue || 0)}</p>
            <p className="text-sm text-purple-600">{ages.over_5_years?.count || 0} customers</p>
          </div>
        </div>
      </div>

      {/* Top Customers Table */}
      <div className="card">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold">Top Customers</h3>
          <button
            onClick={() => setShowAllCustomers(!showAllCustomers)}
            className="text-sm text-blue-600 hover:text-blue-800 flex items-center"
          >
            {showAllCustomers ? (
              <>Show Less <ChevronUp className="h-4 w-4 ml-1" /></>
            ) : (
              <>Show All <ChevronDown className="h-4 w-4 ml-1" /></>
            )}
          </button>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-200">
                <th className="text-left py-2 font-medium text-gray-500">#</th>
                <th className="text-left py-2 font-medium text-gray-500">Customer</th>
                <th className="text-right py-2 font-medium text-gray-500">{year}</th>
                <th className="text-right py-2 font-medium text-gray-500">{year - 1}</th>
                <th className="text-right py-2 font-medium text-gray-500">% Total</th>
                <th className="text-right py-2 font-medium text-gray-500">Cumulative</th>
                <th className="text-center py-2 font-medium text-gray-500">Trend</th>
              </tr>
            </thead>
            <tbody>
              {(showAllCustomers ? customers : customers.slice(0, 10)).map((c: any, idx: number) => (
                <tr key={c.account_code} className="border-b border-gray-100">
                  <td className="py-2 text-gray-500">{idx + 1}</td>
                  <td className="py-2">
                    <span className="font-medium">{c.customer_name}</span>
                    <span className="text-gray-500 ml-2 text-xs">{c.account_code}</span>
                  </td>
                  <td className="py-2 text-right font-medium">{formatCurrency(c.current_year)}</td>
                  <td className="py-2 text-right text-gray-500">{formatCurrency(c.previous_year)}</td>
                  <td className="py-2 text-right">{c.percent_of_total.toFixed(1)}%</td>
                  <td className="py-2 text-right text-gray-500">{c.cumulative_percent.toFixed(1)}%</td>
                  <td className="py-2 text-center">
                    {c.trend === 'up' && <TrendingUp className="h-4 w-4 text-green-500 inline" />}
                    {c.trend === 'down' && <TrendingDown className="h-4 w-4 text-red-500 inline" />}
                    {c.trend === 'stable' && <span className="text-gray-400">-</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

// Margin Analysis Component
function MarginAnalysis({ year }: { year: number }) {
  const { data, isLoading } = useQuery<DashboardMarginByCategoryResponse>({
    queryKey: ['margin-by-category', year],
    queryFn: async () => {
      const response = await apiClient.dashboardMarginByCategory(year);
      return response.data;
    },
  });

  if (isLoading) {
    return <div className="text-center py-8">Loading...</div>;
  }

  const categories = data?.categories || [];
  const totals = data?.totals || {
    revenue: 0,
    cost_of_sales: 0,
    gross_profit: 0,
    gross_margin_percent: 0,
  };

  return (
    <div className="space-y-6">
      {/* Summary Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KPICard
          title="Total Revenue"
          value={formatCurrency(totals.revenue || 0)}
          icon={<DollarSign className="h-6 w-6" />}
        />
        <KPICard
          title="Cost of Sales"
          value={formatCurrency(totals.cost_of_sales || 0)}
        />
        <KPICard
          title="Gross Profit"
          value={formatCurrency(totals.gross_profit || 0)}
          trend={(totals.gross_margin_percent || 0) > 50 ? 'up' : 'down'}
        />
        <KPICard
          title="Gross Margin %"
          value={`${totals.gross_margin_percent?.toFixed(1) || 0}%`}
          icon={<PieChart className="h-6 w-6" />}
        />
      </div>

      {/* Margin by Category */}
      <div className="card">
        <h3 className="text-lg font-semibold mb-4">Gross Margin by Category</h3>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-200">
                <th className="text-left py-2 font-medium text-gray-500">Category</th>
                <th className="text-right py-2 font-medium text-gray-500">Revenue</th>
                <th className="text-right py-2 font-medium text-gray-500">Cost of Sales</th>
                <th className="text-right py-2 font-medium text-gray-500">Gross Profit</th>
                <th className="text-right py-2 font-medium text-gray-500">Margin %</th>
                <th className="py-2 font-medium text-gray-500">Margin Bar</th>
              </tr>
            </thead>
            <tbody>
              {categories.map((cat: any) => (
                <tr key={cat.category} className="border-b border-gray-100">
                  <td className="py-2 font-medium">{cat.category}</td>
                  <td className="py-2 text-right">{formatCurrency(cat.revenue)}</td>
                  <td className="py-2 text-right text-gray-500">{formatCurrency(cat.cost_of_sales)}</td>
                  <td className="py-2 text-right">{formatCurrency(cat.gross_profit)}</td>
                  <td className={`py-2 text-right font-medium ${
                    cat.gross_margin_percent > 60 ? 'text-green-600' :
                    cat.gross_margin_percent > 40 ? 'text-yellow-600' :
                    'text-red-600'
                  }`}>
                    {cat.gross_margin_percent.toFixed(1)}%
                  </td>
                  <td className="py-2">
                    <div className="w-24 bg-gray-200 rounded-full h-4">
                      <div
                        className={`h-4 rounded-full ${
                          cat.gross_margin_percent > 60 ? 'bg-green-500' :
                          cat.gross_margin_percent > 40 ? 'bg-yellow-500' :
                          'bg-red-500'
                        }`}
                        style={{ width: `${Math.min(100, cat.gross_margin_percent)}%` }}
                      />
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
            <tfoot>
              <tr className="font-bold bg-gray-50">
                <td className="py-2">Total</td>
                <td className="py-2 text-right">{formatCurrency(totals.revenue || 0)}</td>
                <td className="py-2 text-right">{formatCurrency(totals.cost_of_sales || 0)}</td>
                <td className="py-2 text-right">{formatCurrency(totals.gross_profit || 0)}</td>
                <td className="py-2 text-right">{totals.gross_margin_percent?.toFixed(1) || 0}%</td>
                <td className="py-2">
                  <div className="w-24 bg-gray-200 rounded-full h-4">
                    <div
                      className="bg-blue-600 h-4 rounded-full"
                      style={{ width: `${Math.min(100, totals.gross_margin_percent || 0)}%` }}
                    />
                  </div>
                </td>
              </tr>
            </tfoot>
          </table>
        </div>
      </div>
    </div>
  );
}

// Main Component
export function SalesDashboards() {
  const [activeView, setActiveView] = useState<DashboardView>('ceo');
  const [year, setYear] = useState(2026);

  const views = [
    { id: 'ceo' as DashboardView, label: 'CEO View', icon: Activity },
    { id: 'revenue' as DashboardView, label: 'Revenue Composition', icon: PieChart },
    { id: 'customers' as DashboardView, label: 'Customer Analysis', icon: Users },
    { id: 'margin' as DashboardView, label: 'Margin Analysis', icon: BarChart3 },
  ];

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
        <div className="flex items-center gap-3">
          <BarChart3 className="h-8 w-8 text-blue-600" />
          <div>
            <h1 className="text-2xl font-bold text-gray-900">Live Opera Dashboards</h1>
            <p className="text-sm text-gray-600">Sales Performance Analytics</p>
          </div>
        </div>
        <div className="flex items-center gap-4">
          <select
            value={year}
            onChange={(e) => setYear(parseInt(e.target.value))}
            className="select w-32"
          >
            <option value={2026}>2026</option>
            <option value={2025}>2025</option>
            <option value={2024}>2024</option>
            <option value={2023}>2023</option>
          </select>
        </div>
      </div>

      {/* View Tabs */}
      <div className="flex flex-wrap gap-2">
        {views.map((view) => {
          const Icon = view.icon;
          return (
            <button
              key={view.id}
              onClick={() => setActiveView(view.id)}
              className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                activeView === view.id
                  ? 'bg-blue-600 text-white'
                  : 'bg-white text-gray-600 hover:bg-gray-100 border border-gray-200'
              }`}
            >
              <Icon className="h-4 w-4" />
              {view.label}
            </button>
          );
        })}
      </div>

      {/* Dashboard Content */}
      {activeView === 'ceo' && <CEOView year={year} />}
      {activeView === 'revenue' && <RevenueComposition year={year} />}
      {activeView === 'customers' && <CustomerAnalysis year={year} />}
      {activeView === 'margin' && <MarginAnalysis year={year} />}
    </div>
  );
}

export default SalesDashboards;
