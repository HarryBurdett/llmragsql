import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  FileText, RefreshCw, Clock, CheckCircle, Send, Eye, Play,
  Inbox
} from 'lucide-react';
import { authFetch } from '../api/client';
import { PageHeader, Card, StatusBadge, Alert } from '../components/ui';

type TabType = 'all' | 'received' | 'processing' | 'reconciled' | 'approved' | 'sent';

interface SupplierStatement {
  id: number;
  supplier_account: string;
  supplier_name: string;
  supplier_code: string;
  statement_date: string;
  status: string;
  received_date: string;
  match_rate: number | null;
  total_items: number;
  matched_count: number;
  query_count: number;
  closing_balance: number | null;
  sender_email: string | null;
  error_message: string | null;
  acknowledged_at: string | null;
  processed_at: string | null;
  approved_by: string | null;
  sent_at: string | null;
  line_count: number;
}

interface StatementsResponse {
  statements: SupplierStatement[];
  total: number;
}

const STATUS_VARIANT: Record<string, 'info' | 'success' | 'warning' | 'danger' | 'neutral'> = {
  received: 'info',
  processing: 'warning',
  reconciled: 'success',
  approved: 'success',
  sent: 'neutral',
  error: 'danger',
};

function formatDate(dateStr: string | null): string {
  if (!dateStr) return '-';
  return new Date(dateStr).toLocaleDateString('en-GB', {
    day: '2-digit', month: 'short', year: 'numeric',
  });
}

function formatMatchRate(rate: number | null): string {
  if (rate === null || rate === undefined) return '-';
  return `${Math.round(rate * 100)}%`;
}

export default function SupplierStatementQueue() {
  const [activeTab, setActiveTab] = useState<TabType>('all');
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  const tabs: { id: TabType; label: string; icon: React.ComponentType<{ className?: string }> }[] = [
    { id: 'all', label: 'All', icon: Inbox },
    { id: 'received', label: 'Received', icon: FileText },
    { id: 'processing', label: 'Processing', icon: Clock },
    { id: 'reconciled', label: 'Reconciled', icon: CheckCircle },
    { id: 'approved', label: 'Approved', icon: CheckCircle },
    { id: 'sent', label: 'Sent', icon: Send },
  ];

  const { data, isLoading, refetch } = useQuery({
    queryKey: ['supplier-statements', activeTab],
    queryFn: async () => {
      const url = activeTab === 'all'
        ? '/api/supplier-statements'
        : `/api/supplier-statements?status=${activeTab}`;
      const res = await authFetch(url);
      if (!res.ok) throw new Error('Failed to fetch supplier statements');
      const json = await res.json();
      if (json.error) throw new Error(json.error);
      return json as StatementsResponse;
    },
    staleTime: 30000,
    refetchInterval: 30000,
  });

  const handleProcess = async (id: number) => {
    setError(null);
    setSuccess(null);
    try {
      const res = await authFetch(`/api/supplier-statements/${id}/process`, { method: 'POST' });
      const json = await res.json();
      if (!res.ok || json.error) throw new Error(json.error || 'Failed to process statement');
      setSuccess(json.message || 'Statement processing started');
      refetch();
    } catch (e: any) {
      setError(e.message);
    }
  };

  const handleApprove = async (id: number) => {
    setError(null);
    setSuccess(null);
    try {
      const res = await authFetch(`/api/supplier-statements/${id}/approve`, { method: 'POST' });
      const json = await res.json();
      if (!res.ok || json.error) throw new Error(json.error || 'Failed to approve statement');
      setSuccess(json.message || 'Statement approved');
      refetch();
    } catch (e: any) {
      setError(e.message);
    }
  };

  const statements = data?.statements || [];

  return (
    <div className="space-y-6">
      <PageHeader icon={FileText} title="Statement Queue" subtitle="Process and reconcile supplier statements">
        <button
          onClick={() => refetch()}
          className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50"
        >
          <RefreshCw className="w-4 h-4" />
          Refresh
        </button>
      </PageHeader>

      {error && (
        <Alert variant="error" title="Error" onDismiss={() => setError(null)}>
          {error}
        </Alert>
      )}
      {success && (
        <Alert variant="success" onDismiss={() => setSuccess(null)}>
          {success}
        </Alert>
      )}

      <Card padding={false} className="overflow-hidden">
        {/* Tabs */}
        <div className="border-b border-gray-200">
          <nav className="flex">
            {tabs.map(tab => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`flex items-center gap-2 px-6 py-3 text-sm font-medium border-b-2 -mb-px ${
                  activeTab === tab.id
                    ? 'border-green-500 text-green-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                }`}
              >
                <tab.icon className="w-4 h-4" />
                {tab.label}
              </button>
            ))}
          </nav>
        </div>

        {/* Tab Content */}
        <div className="p-4">
          {isLoading ? (
            <div className="flex items-center justify-center py-12">
              <RefreshCw className="w-6 h-6 text-gray-400 animate-spin" />
            </div>
          ) : statements.length === 0 ? (
            <div className="text-center py-12 text-gray-500">
              No statements found{activeTab !== 'all' ? ` with status "${activeTab}"` : ''}
            </div>
          ) : (
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Supplier</th>
                  <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Date</th>
                  <th className="px-3 py-3 text-center text-xs font-medium text-gray-500 uppercase">Status</th>
                  <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Received</th>
                  <th className="px-3 py-3 text-center text-xs font-medium text-gray-500 uppercase">Match Rate</th>
                  <th className="px-3 py-3 text-center text-xs font-medium text-gray-500 uppercase">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {statements.map(stmt => (
                  <tr
                    key={stmt.id}
                    className="hover:bg-gray-50 cursor-pointer"
                    onClick={() => window.location.href = `/suppliers/statements/${stmt.id}`}
                  >
                    <td className="px-3 py-3">
                      <div className="text-sm font-medium text-gray-900">{stmt.supplier_name || stmt.supplier_code}</div>
                      <div className="text-xs text-gray-500">{stmt.supplier_code}</div>
                    </td>
                    <td className="px-3 py-3 text-sm text-gray-700">
                      {formatDate(stmt.statement_date)}
                    </td>
                    <td className="px-3 py-3 text-center">
                      <StatusBadge variant={STATUS_VARIANT[stmt.status] || 'neutral'}>
                        {stmt.status.charAt(0).toUpperCase() + stmt.status.slice(1)}
                      </StatusBadge>
                    </td>
                    <td className="px-3 py-3 text-sm text-gray-500">
                      {formatDate(stmt.received_date)}
                    </td>
                    <td className="px-3 py-3 text-center">
                      {stmt.match_rate !== null ? (
                        <span className={`text-sm font-medium ${
                          stmt.match_rate >= 0.9 ? 'text-green-600' :
                          stmt.match_rate >= 0.7 ? 'text-amber-600' :
                          'text-red-600'
                        }`}>
                          {formatMatchRate(stmt.match_rate)}
                        </span>
                      ) : (
                        <span className="text-sm text-gray-400">-</span>
                      )}
                    </td>
                    <td className="px-3 py-3 text-center">
                      <div className="flex items-center justify-center gap-2" onClick={e => e.stopPropagation()}>
                        {stmt.status === 'received' && (
                          <button
                            onClick={() => handleProcess(stmt.id)}
                            className="flex items-center gap-1 px-2.5 py-1 text-xs font-medium text-blue-700 bg-blue-50 border border-blue-200 rounded-lg hover:bg-blue-100"
                            title="Process statement"
                          >
                            <Play className="w-3 h-3" />
                            Process
                          </button>
                        )}
                        {stmt.status === 'reconciled' && (
                          <button
                            onClick={() => handleApprove(stmt.id)}
                            className="flex items-center gap-1 px-2.5 py-1 text-xs font-medium text-green-700 bg-green-50 border border-green-200 rounded-lg hover:bg-green-100"
                            title="Approve statement"
                          >
                            <CheckCircle className="w-3 h-3" />
                            Approve
                          </button>
                        )}
                        <button
                          onClick={() => window.location.href = `/suppliers/statements/${stmt.id}`}
                          className="flex items-center gap-1 px-2.5 py-1 text-xs font-medium text-gray-700 bg-gray-50 border border-gray-200 rounded-lg hover:bg-gray-100"
                          title="View details"
                        >
                          <Eye className="w-3 h-3" />
                          View
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </Card>
    </div>
  );
}

export { SupplierStatementQueue };
