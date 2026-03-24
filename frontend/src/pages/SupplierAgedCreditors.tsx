import { useState, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  RefreshCw,
  Building2,
  Search,
  ChevronDown,
  ChevronRight,
  ArrowUpDown,
  ArrowUp,
  ArrowDown,
  TrendingUp,
} from 'lucide-react';
import { authFetch } from '../api/client';
import { PageHeader, Card, Alert, LoadingState } from '../components/ui';

// --- Types ---

interface SupplierAging {
  account: string;
  name: string;
  current: number;
  days30: number;
  days60: number;
  days90: number;
  days120: number;
  total: number;
}

interface AgedCreditorsResponse {
  success: boolean;
  summary: {
    current: number;
    days30: number;
    days60: number;
    days90: number;
    days120: number;
    total: number;
  };
  suppliers: SupplierAging[];
  error?: string;
}

interface SupplierInvoice {
  reference: string;
  date: string;
  due_date: string;
  original: number;
  balance: number;
  age_days: number;
  age_bucket: string;
}

interface SupplierDetailResponse {
  success: boolean;
  account: string;
  name: string;
  invoices: SupplierInvoice[];
  error?: string;
}

interface TrendPoint {
  period: string;
  current: number;
  days30: number;
  days60: number;
  days90: number;
  days120: number;
}

interface TrendResponse {
  success: boolean;
  trend: TrendPoint[];
  error?: string;
}

type SortField = 'account' | 'name' | 'current' | 'days30' | 'days60' | 'days90' | 'days120' | 'total';
type SortDirection = 'asc' | 'desc';

// --- Helpers ---

const formatCurrency = (value: number | undefined | null): string => {
  if (value === undefined || value === null) return '\u00a30.00';
  return new Intl.NumberFormat('en-GB', {
    style: 'currency',
    currency: 'GBP',
  }).format(value);
};

const formatCompactCurrency = (value: number): string => {
  if (Math.abs(value) >= 1_000_000) {
    return `\u00a3${(value / 1_000_000).toFixed(1)}M`;
  }
  if (Math.abs(value) >= 1_000) {
    return `\u00a3${(value / 1_000).toFixed(1)}K`;
  }
  return formatCurrency(value);
};

// --- Main Component ---

export default function SupplierAgedCreditors() {
  const [searchTerm, setSearchTerm] = useState('');
  const [sortField, setSortField] = useState<SortField>('total');
  const [sortDirection, setSortDirection] = useState<SortDirection>('desc');
  const [expandedAccount, setExpandedAccount] = useState<string | null>(null);

  // Fetch aged creditors summary + supplier list
  const agedQuery = useQuery<AgedCreditorsResponse>({
    queryKey: ['creditorsAged'],
    queryFn: async () => {
      const response = await authFetch('/api/creditors/aged');
      if (!response.ok) {
        throw new Error(`Failed to load aged creditors: ${response.statusText}`);
      }
      return response.json();
    },
    refetchOnWindowFocus: false,
  });

  // Fetch trend data
  const trendQuery = useQuery<TrendResponse>({
    queryKey: ['creditorsAgedTrend'],
    queryFn: async () => {
      const response = await authFetch('/api/creditors/aged/trend');
      if (!response.ok) {
        throw new Error(`Failed to load trend data: ${response.statusText}`);
      }
      return response.json();
    },
    refetchOnWindowFocus: false,
  });

  // Fetch detail for expanded supplier
  const detailQuery = useQuery<SupplierDetailResponse>({
    queryKey: ['creditorsAgedDetail', expandedAccount],
    queryFn: async () => {
      if (!expandedAccount) throw new Error('No account selected');
      const response = await authFetch(`/api/creditors/aged/${encodeURIComponent(expandedAccount)}`);
      if (!response.ok) {
        throw new Error(`Failed to load supplier detail: ${response.statusText}`);
      }
      return response.json();
    },
    enabled: !!expandedAccount,
    refetchOnWindowFocus: false,
  });

  const data = agedQuery.data;
  const isLoading = agedQuery.isLoading;

  // Sorting
  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDirection(prev => (prev === 'asc' ? 'desc' : 'asc'));
    } else {
      setSortField(field);
      setSortDirection(field === 'name' || field === 'account' ? 'asc' : 'desc');
    }
  };

  // Filter + sort suppliers
  const filteredSuppliers = useMemo(() => {
    if (!data?.suppliers) return [];
    let list = data.suppliers;

    if (searchTerm.trim()) {
      const term = searchTerm.toLowerCase();
      list = list.filter(
        s => s.name.toLowerCase().includes(term) || s.account.toLowerCase().includes(term)
      );
    }

    return [...list].sort((a, b) => {
      const aVal = a[sortField];
      const bVal = b[sortField];
      if (typeof aVal === 'string' && typeof bVal === 'string') {
        return sortDirection === 'asc'
          ? aVal.localeCompare(bVal)
          : bVal.localeCompare(aVal);
      }
      const numA = aVal as number;
      const numB = bVal as number;
      return sortDirection === 'asc' ? numA - numB : numB - numA;
    });
  }, [data?.suppliers, searchTerm, sortField, sortDirection]);

  // Toggle row expansion
  const toggleExpand = (account: string) => {
    setExpandedAccount(prev => (prev === account ? null : account));
  };

  // Sort icon helper
  const SortIcon = ({ field }: { field: SortField }) => {
    if (sortField !== field) return <ArrowUpDown className="h-3 w-3 opacity-40" />;
    return sortDirection === 'asc' ? (
      <ArrowUp className="h-3 w-3" />
    ) : (
      <ArrowDown className="h-3 w-3" />
    );
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <PageHeader
        icon={Building2}
        title="Aged Creditors"
        subtitle={
          data && !isLoading
            ? `${filteredSuppliers.length} supplier${filteredSuppliers.length !== 1 ? 's' : ''} with outstanding balances`
            : 'Loading supplier aging analysis...'
        }
      >
        <button
          onClick={() => {
            agedQuery.refetch();
            trendQuery.refetch();
          }}
          disabled={isLoading}
          className="flex items-center gap-2 px-4 py-2 bg-gray-100 hover:bg-gray-200 rounded-lg transition-colors disabled:opacity-50 text-sm"
        >
          <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </PageHeader>

      {/* Loading */}
      {isLoading && <LoadingState message="Loading aged creditors..." />}

      {/* Error */}
      {agedQuery.error && (
        <Alert variant="error" title="Error loading aged creditors">
          {(agedQuery.error as Error).message}
        </Alert>
      )}

      {/* Summary Cards */}
      {data && !isLoading && (
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
          <SummaryCard
            label="Current"
            amount={data.summary.current}
            colorClass="text-emerald-700"
            bgClass="bg-emerald-50"
            borderClass="border-emerald-200"
          />
          <SummaryCard
            label="30 Days"
            amount={data.summary.days30}
            colorClass="text-amber-700"
            bgClass="bg-amber-50"
            borderClass="border-amber-200"
          />
          <SummaryCard
            label="60 Days"
            amount={data.summary.days60}
            colorClass="text-amber-700"
            bgClass="bg-amber-50"
            borderClass="border-amber-200"
          />
          <SummaryCard
            label="90 Days"
            amount={data.summary.days90}
            colorClass="text-red-700"
            bgClass="bg-red-50"
            borderClass="border-red-200"
          />
          <SummaryCard
            label="120+ Days"
            amount={data.summary.days120}
            colorClass="text-red-700"
            bgClass="bg-red-50"
            borderClass="border-red-200"
          />
          <SummaryCard
            label="Total"
            amount={data.summary.total}
            colorClass="text-gray-900"
            bgClass="bg-gray-50"
            borderClass="border-gray-300"
            bold
          />
        </div>
      )}

      {/* Trend Chart */}
      {trendQuery.data?.trend && trendQuery.data.trend.length > 0 && (
        <Card>
          <div className="flex items-center gap-2 mb-4">
            <TrendingUp className="h-5 w-5 text-gray-500" />
            <h3 className="text-sm font-semibold text-gray-900">Aging Trend (Last 6 Months)</h3>
          </div>
          <TrendChart data={trendQuery.data.trend} />
        </Card>
      )}

      {/* Supplier Table */}
      {data && !isLoading && (
        <Card padding={false}>
          {/* Search Bar */}
          <div className="p-4 border-b border-gray-200">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-gray-400" />
              <input
                type="text"
                placeholder="Search by supplier name or account..."
                value={searchTerm}
                onChange={e => setSearchTerm(e.target.value)}
                className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
            </div>
          </div>

          {/* Table */}
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 border-b border-gray-200">
                  <th className="w-8 px-4 py-3" />
                  {([
                    ['account', 'Account'],
                    ['name', 'Supplier Name'],
                    ['current', 'Current'],
                    ['days30', '30 Days'],
                    ['days60', '60 Days'],
                    ['days90', '90 Days'],
                    ['days120', '120+ Days'],
                    ['total', 'Total'],
                  ] as [SortField, string][]).map(([field, label]) => (
                    <th
                      key={field}
                      onClick={() => handleSort(field)}
                      className={`px-4 py-3 font-medium text-gray-600 cursor-pointer hover:bg-gray-100 select-none ${
                        field === 'account' || field === 'name' ? 'text-left' : 'text-right'
                      }`}
                    >
                      <div className={`flex items-center gap-1 ${
                        field !== 'account' && field !== 'name' ? 'justify-end' : ''
                      }`}>
                        {label}
                        <SortIcon field={field} />
                      </div>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {filteredSuppliers.length === 0 && (
                  <tr>
                    <td colSpan={9} className="px-4 py-8 text-center text-gray-500">
                      {searchTerm
                        ? 'No suppliers match your search.'
                        : 'No outstanding creditors found.'}
                    </td>
                  </tr>
                )}
                {filteredSuppliers.map(supplier => (
                  <SupplierRow
                    key={supplier.account}
                    supplier={supplier}
                    isExpanded={expandedAccount === supplier.account}
                    onToggle={() => toggleExpand(supplier.account)}
                    detailData={
                      expandedAccount === supplier.account ? detailQuery.data : undefined
                    }
                    detailLoading={
                      expandedAccount === supplier.account && detailQuery.isLoading
                    }
                    detailError={
                      expandedAccount === supplier.account && detailQuery.error
                        ? (detailQuery.error as Error).message
                        : undefined
                    }
                  />
                ))}
              </tbody>
            </table>
          </div>

          {/* Footer */}
          {filteredSuppliers.length > 0 && (
            <div className="px-4 py-3 border-t border-gray-200 bg-gray-50 text-xs text-gray-500">
              Showing {filteredSuppliers.length} of {data.suppliers.length} suppliers
            </div>
          )}
        </Card>
      )}
    </div>
  );
}

// --- Sub-Components ---

function SummaryCard({
  label,
  amount,
  colorClass,
  bgClass,
  borderClass,
  bold,
}: {
  label: string;
  amount: number;
  colorClass: string;
  bgClass: string;
  borderClass: string;
  bold?: boolean;
}) {
  return (
    <div className={`rounded-lg border ${borderClass} ${bgClass} p-4`}>
      <p className="text-xs font-medium text-gray-500 uppercase tracking-wide">{label}</p>
      <p className={`mt-1 text-lg ${bold ? 'font-bold' : 'font-semibold'} ${colorClass}`}>
        {formatCurrency(amount)}
      </p>
    </div>
  );
}

function SupplierRow({
  supplier,
  isExpanded,
  onToggle,
  detailData,
  detailLoading,
  detailError,
}: {
  supplier: SupplierAging;
  isExpanded: boolean;
  onToggle: () => void;
  detailData?: SupplierDetailResponse;
  detailLoading: boolean;
  detailError?: string;
}) {
  const ageBucketColor = (value: number, bucket: 'current' | '30' | '60' | '90' | '120') => {
    if (value === 0) return 'text-gray-400';
    switch (bucket) {
      case 'current':
        return 'text-emerald-700';
      case '30':
      case '60':
        return 'text-amber-700';
      case '90':
      case '120':
        return 'text-red-700';
    }
  };

  return (
    <>
      <tr
        onClick={onToggle}
        className="border-b border-gray-100 hover:bg-blue-50/50 cursor-pointer transition-colors"
      >
        <td className="px-4 py-3">
          {isExpanded ? (
            <ChevronDown className="h-4 w-4 text-gray-400" />
          ) : (
            <ChevronRight className="h-4 w-4 text-gray-400" />
          )}
        </td>
        <td className="px-4 py-3 font-mono text-gray-600">{supplier.account}</td>
        <td className="px-4 py-3 font-medium text-gray-900">{supplier.name}</td>
        <td className={`px-4 py-3 text-right ${ageBucketColor(supplier.current, 'current')}`}>
          {formatCurrency(supplier.current)}
        </td>
        <td className={`px-4 py-3 text-right ${ageBucketColor(supplier.days30, '30')}`}>
          {formatCurrency(supplier.days30)}
        </td>
        <td className={`px-4 py-3 text-right ${ageBucketColor(supplier.days60, '60')}`}>
          {formatCurrency(supplier.days60)}
        </td>
        <td className={`px-4 py-3 text-right ${ageBucketColor(supplier.days90, '90')}`}>
          {formatCurrency(supplier.days90)}
        </td>
        <td className={`px-4 py-3 text-right ${ageBucketColor(supplier.days120, '120')}`}>
          {formatCurrency(supplier.days120)}
        </td>
        <td className="px-4 py-3 text-right font-semibold text-gray-900">
          {formatCurrency(supplier.total)}
        </td>
      </tr>

      {/* Expanded detail row */}
      {isExpanded && (
        <tr>
          <td colSpan={9} className="bg-gray-50 px-8 py-4">
            {detailLoading && (
              <div className="flex items-center gap-2 text-sm text-gray-500">
                <RefreshCw className="h-4 w-4 animate-spin" />
                Loading invoices...
              </div>
            )}
            {detailError && (
              <div className="text-sm text-red-600">{detailError}</div>
            )}
            {detailData?.invoices && detailData.invoices.length > 0 && (
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-xs text-gray-500 uppercase tracking-wide">
                    <th className="pb-2 pr-4">Reference</th>
                    <th className="pb-2 pr-4">Date</th>
                    <th className="pb-2 pr-4">Due Date</th>
                    <th className="pb-2 pr-4 text-right">Original</th>
                    <th className="pb-2 pr-4 text-right">Balance</th>
                    <th className="pb-2 text-right">Age (Days)</th>
                  </tr>
                </thead>
                <tbody>
                  {detailData.invoices.map((inv, idx) => (
                    <tr key={idx} className="border-t border-gray-200">
                      <td className="py-2 pr-4 font-mono">{inv.reference}</td>
                      <td className="py-2 pr-4">{inv.date}</td>
                      <td className="py-2 pr-4">{inv.due_date}</td>
                      <td className="py-2 pr-4 text-right">{formatCurrency(inv.original)}</td>
                      <td className="py-2 pr-4 text-right font-medium">{formatCurrency(inv.balance)}</td>
                      <td className={`py-2 text-right font-medium ${
                        inv.age_days <= 30
                          ? 'text-emerald-700'
                          : inv.age_days <= 60
                            ? 'text-amber-700'
                            : 'text-red-700'
                      }`}>
                        {inv.age_days}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
            {detailData?.invoices && detailData.invoices.length === 0 && (
              <p className="text-sm text-gray-500">No outstanding invoices found.</p>
            )}
          </td>
        </tr>
      )}
    </>
  );
}

// --- Trend Chart (inline SVG) ---

function TrendChart({ data }: { data: TrendPoint[] }) {
  if (data.length === 0) return null;

  const chartWidth = 700;
  const chartHeight = 200;
  const padding = { top: 20, right: 20, bottom: 40, left: 60 };
  const plotWidth = chartWidth - padding.left - padding.right;
  const plotHeight = chartHeight - padding.top - padding.bottom;

  // Compute totals per period to find max
  const totals = data.map(d => d.current + d.days30 + d.days60 + d.days90 + d.days120);
  const maxTotal = Math.max(...totals, 1);

  const barWidth = Math.min(60, plotWidth / data.length - 8);
  const barGap = (plotWidth - barWidth * data.length) / (data.length + 1);

  const buckets: { key: keyof TrendPoint; color: string; label: string }[] = [
    { key: 'current', color: '#059669', label: 'Current' },
    { key: 'days30', color: '#d97706', label: '30d' },
    { key: 'days60', color: '#ea580c', label: '60d' },
    { key: 'days90', color: '#dc2626', label: '90d' },
    { key: 'days120', color: '#991b1b', label: '120+' },
  ];

  // Y-axis ticks
  const yTicks = 4;
  const yStep = maxTotal / yTicks;

  return (
    <div className="w-full overflow-x-auto">
      <svg
        viewBox={`0 0 ${chartWidth} ${chartHeight}`}
        className="w-full max-w-[700px]"
        style={{ minWidth: 400 }}
      >
        {/* Y-axis grid lines and labels */}
        {Array.from({ length: yTicks + 1 }).map((_, i) => {
          const value = yStep * i;
          const y = padding.top + plotHeight - (value / maxTotal) * plotHeight;
          return (
            <g key={`y-${i}`}>
              <line
                x1={padding.left}
                y1={y}
                x2={chartWidth - padding.right}
                y2={y}
                stroke="#e5e7eb"
                strokeWidth={1}
              />
              <text
                x={padding.left - 8}
                y={y + 4}
                textAnchor="end"
                className="fill-gray-400"
                fontSize={10}
              >
                {formatCompactCurrency(value)}
              </text>
            </g>
          );
        })}

        {/* Stacked bars */}
        {data.map((point, i) => {
          const x = padding.left + barGap * (i + 1) + barWidth * i;
          let cumY = 0;

          return (
            <g key={point.period}>
              {buckets.map(bucket => {
                const val = point[bucket.key] as number;
                const barH = (val / maxTotal) * plotHeight;
                const y = padding.top + plotHeight - cumY - barH;
                cumY += barH;

                if (val === 0) return null;

                return (
                  <rect
                    key={bucket.key}
                    x={x}
                    y={y}
                    width={barWidth}
                    height={Math.max(barH, 0)}
                    fill={bucket.color}
                    rx={2}
                  >
                    <title>
                      {point.period} - {bucket.label}: {formatCurrency(val)}
                    </title>
                  </rect>
                );
              })}
              {/* X-axis label */}
              <text
                x={x + barWidth / 2}
                y={chartHeight - padding.bottom + 16}
                textAnchor="middle"
                className="fill-gray-500"
                fontSize={10}
              >
                {point.period}
              </text>
            </g>
          );
        })}

        {/* Axes */}
        <line
          x1={padding.left}
          y1={padding.top}
          x2={padding.left}
          y2={padding.top + plotHeight}
          stroke="#9ca3af"
          strokeWidth={1}
        />
        <line
          x1={padding.left}
          y1={padding.top + plotHeight}
          x2={chartWidth - padding.right}
          y2={padding.top + plotHeight}
          stroke="#9ca3af"
          strokeWidth={1}
        />
      </svg>

      {/* Legend */}
      <div className="flex items-center justify-center gap-4 mt-2">
        {buckets.map(bucket => (
          <div key={bucket.key} className="flex items-center gap-1.5 text-xs text-gray-600">
            <span
              className="inline-block w-3 h-3 rounded-sm"
              style={{ backgroundColor: bucket.color }}
            />
            {bucket.label}
          </div>
        ))}
      </div>
    </div>
  );
}

export { SupplierAgedCreditors };
