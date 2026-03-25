import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  MessageSquare, Clock, AlertTriangle, CheckCircle, RefreshCw,
  Send, Building, Calendar, HelpCircle
} from 'lucide-react';
import { authFetch } from '../api/client';
import { PageHeader, Card, StatusBadge, Alert } from '../components/ui';
import { HelpPanel } from '../components/HelpPanel';
import { useHelp } from '../hooks/useHelp';

type TabType = 'open' | 'overdue' | 'resolved';

interface SupplierQuery {
  query_id: number;
  supplier_code: string;
  supplier_name: string;
  reference: string | null;
  query_type: string;
  description: string | null;
  debit: number | null;
  credit: number | null;
  query_sent_at: string | null;
  days_outstanding: number | null;
  status: string;
  resolved_at: string | null;
}

interface QueriesResponse {
  queries: SupplierQuery[];
  counts: { open: number; overdue: number; resolved: number };
}

function formatDate(dateStr: string | null): string {
  if (!dateStr) return '-';
  return new Date(dateStr).toLocaleDateString('en-GB', {
    day: '2-digit', month: 'short', year: 'numeric',
  });
}

export default function SupplierQueries() {
  const { showHelp, setShowHelp } = useHelp();
  const [activeTab, setActiveTab] = useState<TabType>('open');
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  const tabs: { id: TabType; label: string; icon: React.ComponentType<{ className?: string }> }[] = [
    { id: 'open', label: 'Open', icon: Clock },
    { id: 'overdue', label: 'Overdue', icon: AlertTriangle },
    { id: 'resolved', label: 'Resolved', icon: CheckCircle },
  ];

  const { data, isLoading, refetch } = useQuery({
    queryKey: ['supplier-queries', activeTab],
    queryFn: async () => {
      const url = activeTab === 'overdue'
        ? '/api/supplier-queries/overdue'
        : `/api/supplier-queries?status=${activeTab}`;
      const res = await authFetch(url);
      if (!res.ok) throw new Error('Failed to fetch supplier queries');
      const json = await res.json();
      if (json.error) throw new Error(json.error);
      return json as QueriesResponse;
    },
    staleTime: 30000,
    refetchInterval: 60000,
  });

  const handleResolve = async (queryId: number) => {
    setError(null);
    setSuccess(null);
    try {
      const res = await authFetch(`/api/supplier-queries/${queryId}/resolve`, { method: 'POST' });
      const json = await res.json();
      if (!res.ok || json.error) throw new Error(json.error || 'Failed to resolve query');
      setSuccess(json.message || 'Query marked as resolved');
      refetch();
    } catch (e: any) {
      setError(e.message);
    }
  };

  const handleSendReminder = async (queryId: number) => {
    setError(null);
    setSuccess(null);
    try {
      const res = await authFetch(`/api/supplier-queries/${queryId}/remind`, { method: 'POST' });
      const json = await res.json();
      if (!res.ok || json.error) throw new Error(json.error || 'Failed to send reminder');
      setSuccess(json.message || 'Reminder sent');
      refetch();
    } catch (e: any) {
      setError(e.message);
    }
  };

  const queries = data?.queries || [];

  return (
    <div className="space-y-6">
      <PageHeader icon={MessageSquare} title="Supplier Queries" subtitle="Track and manage outstanding supplier queries">
        <button
          onClick={() => setShowHelp(prev => !prev)}
          className={`flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-lg transition-colors ${
            showHelp ? 'bg-blue-600 text-white' : 'text-gray-600 bg-white border border-gray-200 hover:bg-gray-50'
          }`}
          title="Toggle help (F1)"
        >
          <HelpCircle className="h-4 w-4" />
          Help
        </button>
        <button
          onClick={() => refetch()}
          className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50"
        >
          <RefreshCw className="w-4 h-4" />
          Refresh
        </button>
      </PageHeader>

      <HelpPanel
        isOpen={showHelp}
        onClose={() => setShowHelp(false)}
        sections={[
          { title: 'Open Queries', content: 'Items where a question has been sent to the supplier (e.g. missing invoice copy, amount mismatch). Waiting for their response.' },
          { title: 'Overdue', content: 'Queries that are past the response deadline configured in Supplier Settings. These may need a follow-up reminder.' },
          { title: 'Resolved', content: 'Queries that have been answered by the supplier and closed. Use the Resolve button to close a query once the answer is received.' },
        ]}
      />

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
          ) : queries.length === 0 ? (
            <div className="text-center py-12 text-gray-500">
              No {activeTab} queries found
            </div>
          ) : (
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Supplier</th>
                  <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Invoice Ref</th>
                  <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Query Type</th>
                  <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Sent Date</th>
                  <th className="px-3 py-3 text-center text-xs font-medium text-gray-500 uppercase">Age (days)</th>
                  <th className="px-3 py-3 text-center text-xs font-medium text-gray-500 uppercase">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {queries.map(q => (
                  <tr key={q.query_id} className="hover:bg-gray-50">
                    <td className="px-3 py-3">
                      <div className="flex items-center gap-2">
                        <Building className="h-4 w-4 text-gray-400 flex-shrink-0" />
                        <div>
                          <div className="text-sm font-medium text-gray-900">{q.supplier_name}</div>
                          <div className="text-xs text-gray-500">{q.supplier_code}</div>
                        </div>
                      </div>
                    </td>
                    <td className="px-3 py-3">
                      <div className="text-sm font-medium text-gray-700">{q.reference || '-'}</div>
                      {q.description && (
                        <div className="text-xs text-gray-500 truncate max-w-[200px]">{q.description}</div>
                      )}
                    </td>
                    <td className="px-3 py-3">
                      <StatusBadge variant={
                        q.query_type === 'missing_invoice' ? 'warning' :
                        q.query_type === 'amount_mismatch' ? 'danger' :
                        q.query_type === 'duplicate' ? 'info' :
                        'neutral'
                      }>
                        {q.query_type.replace(/_/g, ' ')}
                      </StatusBadge>
                    </td>
                    <td className="px-3 py-3">
                      <div className="flex items-center gap-1 text-sm text-gray-600">
                        <Calendar className="h-3.5 w-3.5" />
                        {formatDate(q.query_sent_at)}
                      </div>
                    </td>
                    <td className="px-3 py-3 text-center">
                      <span className={`text-sm font-semibold ${
                        (q.days_outstanding || 0) > 14 ? 'text-red-600' :
                        (q.days_outstanding || 0) > 7 ? 'text-amber-600' :
                        'text-gray-600'
                      }`}>
                        {Math.round(q.days_outstanding || 0)}
                      </span>
                    </td>
                    <td className="px-3 py-3 text-center">
                      <div className="flex items-center justify-center gap-2">
                        {q.status !== 'resolved' && (
                          <>
                            <button
                              onClick={() => handleResolve(q.query_id)}
                              className="flex items-center gap-1 px-2.5 py-1 text-xs font-medium text-green-700 bg-green-50 border border-green-200 rounded-lg hover:bg-green-100"
                              title="Mark as resolved"
                            >
                              <CheckCircle className="w-3 h-3" />
                              Resolve
                            </button>
                            <button
                              onClick={() => handleSendReminder(q.query_id)}
                              className="flex items-center gap-1 px-2.5 py-1 text-xs font-medium text-blue-700 bg-blue-50 border border-blue-200 rounded-lg hover:bg-blue-100"
                              title="Send reminder to supplier"
                            >
                              <Send className="w-3 h-3" />
                              Send Reminder
                            </button>
                          </>
                        )}
                        {q.status === 'resolved' && (
                          <span className="text-xs text-gray-400">Resolved {formatDate(q.resolved_at)}</span>
                        )}
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

export { SupplierQueries };
