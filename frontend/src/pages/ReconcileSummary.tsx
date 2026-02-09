import { useQuery } from '@tanstack/react-query';
import {
  CheckCircle,
  XCircle,
  RefreshCw,
  Scale,
  Users,
  Building2,
  BookOpen,
  Receipt,
  ChevronRight,
  AlertTriangle,
} from 'lucide-react';
import { Link } from 'react-router-dom';
import apiClient from '../api/client';

interface ReconcileDetail {
  label: string;
  value: number;
}

interface ReconcileVariance {
  label: string;
  value: number;
  ok: boolean;
}

interface ReconcileCheck {
  name: string;
  icon: string;
  reconciled: boolean;
  details?: ReconcileDetail[];
  variances?: ReconcileVariance[];
  error?: string;
}

interface ReconcileSummaryResponse {
  success: boolean;
  reconciliation_date: string;
  checks: ReconcileCheck[];
  all_reconciled: boolean;
  total_checks: number;
  passed_checks: number;
  failed_checks: number;
  error?: string;
}

const iconMap: Record<string, React.ComponentType<{ className?: string }>> = {
  users: Users,
  building: Building2,
  book: BookOpen,
  receipt: Receipt,
};

const linkMap: Record<string, string> = {
  Debtors: '/reconcile/debtors',
  Creditors: '/reconcile/creditors',
  Cashbook: '/reconcile/cashbook',
  VAT: '/reconcile/vat',
};

export function ReconcileSummary() {
  const summaryQuery = useQuery<ReconcileSummaryResponse>({
    queryKey: ['reconcileSummary'],
    queryFn: async () => {
      const response = await apiClient.get<ReconcileSummaryResponse>('/reconcile/summary');
      return response.data;
    },
    refetchOnWindowFocus: false,
  });

  const formatCurrency = (value: number | undefined | null) => {
    if (value === undefined || value === null) return '£0.00';
    return new Intl.NumberFormat('en-GB', {
      style: 'currency',
      currency: 'GBP',
    }).format(value);
  };

  const data = summaryQuery.data;
  const isLoading = summaryQuery.isLoading;

  return (
    <div className="space-y-6">
      {/* Header with gradient */}
      <div className={`rounded-xl shadow-lg p-6 text-white ${
        data?.all_reconciled
          ? 'bg-gradient-to-r from-green-600 to-emerald-600'
          : data && !isLoading
            ? 'bg-gradient-to-r from-amber-600 to-orange-600'
            : 'bg-gradient-to-r from-slate-600 to-slate-700'
      }`}>
        <div className="flex justify-between items-center">
          <div>
            <h1 className="text-2xl font-bold flex items-center gap-3">
              <div className="p-2 bg-white/20 rounded-lg backdrop-blur-sm">
                <Scale className="h-6 w-6" />
              </div>
              Reconciliation Summary
            </h1>
            <p className="text-white/80 mt-2">
              {data?.all_reconciled
                ? 'All systems reconciled'
                : data && !isLoading
                  ? `${data.failed_checks} of ${data.total_checks} checks require attention`
                  : 'Checking reconciliation status...'}
            </p>
          </div>
          <div className="flex items-center gap-4">
            {data && !isLoading && (
              <div className="text-right">
                <div className="text-3xl font-bold">
                  {data.passed_checks}/{data.total_checks}
                </div>
                <div className="text-sm text-white/80">Checks Passed</div>
              </div>
            )}
            <button
              onClick={() => summaryQuery.refetch()}
              disabled={isLoading}
              className="flex items-center gap-2 px-4 py-2 bg-white/20 hover:bg-white/30 backdrop-blur-sm rounded-lg transition-colors disabled:opacity-50"
            >
              <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
              Refresh
            </button>
          </div>
        </div>
      </div>

      {/* Loading State */}
      {isLoading && (
        <div className="bg-white rounded-lg shadow p-8 text-center">
          <RefreshCw className="h-8 w-8 animate-spin text-slate-600 mx-auto mb-4" />
          <p className="text-gray-600">Running reconciliation checks...</p>
        </div>
      )}

      {/* Error State */}
      {summaryQuery.error && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4">
          <div className="flex items-center gap-2 text-red-800">
            <XCircle className="h-5 w-5" />
            <span className="font-medium">Error loading data</span>
          </div>
          <p className="text-red-600 mt-1">{(summaryQuery.error as Error).message}</p>
        </div>
      )}

      {/* Reconciliation Cards */}
      {data && !isLoading && (
        <div className="grid grid-cols-2 gap-4">
          {data.checks.map((check) => {
            const Icon = iconMap[check.icon] || Scale;
            const detailLink = linkMap[check.name];

            return (
              <div
                key={check.name}
                className={`bg-white rounded-lg shadow-lg overflow-hidden border-l-4 ${
                  check.reconciled
                    ? 'border-l-green-500'
                    : 'border-l-red-500'
                }`}
              >
                {/* Card Header */}
                <div className={`p-4 ${
                  check.reconciled ? 'bg-green-50' : 'bg-red-50'
                }`}>
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3">
                      <div className={`p-2 rounded-lg ${
                        check.reconciled ? 'bg-green-100' : 'bg-red-100'
                      }`}>
                        <Icon className={`h-5 w-5 ${
                          check.reconciled ? 'text-green-600' : 'text-red-600'
                        }`} />
                      </div>
                      <div>
                        <h3 className="font-semibold text-gray-900">{check.name}</h3>
                        <p className={`text-sm ${
                          check.reconciled ? 'text-green-600' : 'text-red-600'
                        }`}>
                          {check.error
                            ? 'Error'
                            : check.reconciled
                              ? 'Reconciled'
                              : 'Variance Found'}
                        </p>
                      </div>
                    </div>
                    {check.reconciled ? (
                      <CheckCircle className="h-8 w-8 text-green-500" />
                    ) : (
                      <XCircle className="h-8 w-8 text-red-500" />
                    )}
                  </div>
                </div>

                {/* Card Body */}
                <div className="p-4">
                  {check.error ? (
                    <div className="flex items-center gap-2 text-red-600 text-sm">
                      <AlertTriangle className="h-4 w-4" />
                      {check.error}
                    </div>
                  ) : (
                    <>
                      {/* Details */}
                      {check.details && (
                        <div className="space-y-2 mb-4">
                          {check.details.map((detail, idx) => (
                            <div key={idx} className="flex justify-between text-sm">
                              <span className="text-gray-600">{detail.label}</span>
                              <span className="font-medium text-gray-900">
                                {formatCurrency(detail.value)}
                              </span>
                            </div>
                          ))}
                        </div>
                      )}

                      {/* Variances */}
                      {check.variances && (
                        <div className="border-t pt-3 space-y-2">
                          {check.variances.map((variance, idx) => (
                            <div key={idx} className="flex justify-between items-center text-sm">
                              <span className="text-gray-600">{variance.label}</span>
                              <span className={`font-medium flex items-center gap-1 ${
                                variance.ok ? 'text-green-600' : 'text-red-600'
                              }`}>
                                {variance.ok ? (
                                  <CheckCircle className="h-4 w-4" />
                                ) : (
                                  <XCircle className="h-4 w-4" />
                                )}
                                {formatCurrency(variance.value)}
                              </span>
                            </div>
                          ))}
                        </div>
                      )}
                    </>
                  )}

                  {/* View Details Link */}
                  {detailLink && (
                    <Link
                      to={detailLink}
                      className="mt-4 flex items-center justify-center gap-2 w-full py-2 px-4 bg-gray-100 hover:bg-gray-200 rounded-lg text-sm font-medium text-gray-700 transition-colors"
                    >
                      View Details
                      <ChevronRight className="h-4 w-4" />
                    </Link>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* Footer */}
      {data && !isLoading && (
        <div className="text-center text-sm text-gray-500">
          Last checked: {data.reconciliation_date}
        </div>
      )}
    </div>
  );
}

export default ReconcileSummary;
