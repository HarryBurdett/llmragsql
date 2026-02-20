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
import { authFetch } from '../api/client';
import { PageHeader, Card, Alert, LoadingState, StatusBadge } from '../components/ui';

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
      const response = await authFetch('http://localhost:8000/api/reconcile/summary');
      return response.json();
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
      {/* Header */}
      <PageHeader icon={Scale} title="Reconciliation Summary" subtitle={
        data?.all_reconciled
          ? 'All systems reconciled'
          : data && !isLoading
            ? `${data.failed_checks} of ${data.total_checks} checks require attention`
            : 'Checking reconciliation status...'
      }>
        <div className="flex items-center gap-3">
          {data && !isLoading && (
            <>
              <StatusBadge variant={data.all_reconciled ? 'success' : 'warning'}>
                {data.passed_checks}/{data.total_checks} Passed
              </StatusBadge>
            </>
          )}
          <button
            onClick={() => summaryQuery.refetch()}
            disabled={isLoading}
            className="flex items-center gap-2 px-4 py-2 bg-gray-100 hover:bg-gray-200 rounded-lg transition-colors disabled:opacity-50 text-sm"
          >
            <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
            Refresh
          </button>
        </div>
      </PageHeader>

      {/* Loading State */}
      {isLoading && (
        <LoadingState message="Running reconciliation checks..." />
      )}

      {/* Error State */}
      {summaryQuery.error && (
        <Alert variant="error" title="Error loading data">
          {(summaryQuery.error as Error).message}
        </Alert>
      )}

      {/* Reconciliation Cards */}
      {data && !isLoading && (
        <div className="grid grid-cols-2 gap-4">
          {data.checks.map((check) => {
            const Icon = iconMap[check.icon] || Scale;
            const detailLink = linkMap[check.name];

            return (
              <Card key={check.name} className={`overflow-hidden border-l-4 ${
                check.reconciled
                  ? 'border-l-emerald-500'
                  : 'border-l-red-500'
              }`} padding={false}>
                {/* Card Header */}
                <div className={`p-4 ${
                  check.reconciled ? 'bg-emerald-50' : 'bg-red-50'
                }`}>
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3">
                      <div className={`p-2 rounded-lg ${
                        check.reconciled ? 'bg-emerald-100' : 'bg-red-100'
                      }`}>
                        <Icon className={`h-5 w-5 ${
                          check.reconciled ? 'text-emerald-600' : 'text-red-600'
                        }`} />
                      </div>
                      <div>
                        <h3 className="text-sm font-semibold text-gray-900">{check.name}</h3>
                        <StatusBadge variant={check.error ? 'danger' : check.reconciled ? 'success' : 'danger'}>
                          {check.error ? 'Error' : check.reconciled ? 'Reconciled' : 'Variance Found'}
                        </StatusBadge>
                      </div>
                    </div>
                    {check.reconciled ? (
                      <CheckCircle className="h-8 w-8 text-emerald-500" />
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
                                variance.ok ? 'text-emerald-600' : 'text-red-600'
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
              </Card>
            );
          })}
        </div>
      )}

      {/* Footer */}
      {data && !isLoading && (
        <div className="text-center text-xs text-gray-500">
          Last checked: {data.reconciliation_date}
        </div>
      )}
    </div>
  );
}

export default ReconcileSummary;
