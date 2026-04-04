import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import {
  FileText, RefreshCw, Clock, CheckCircle, Send, Eye, Play,
  Inbox, HelpCircle, AlertTriangle
} from 'lucide-react';
import { authFetch } from '../api/client';
import { PageHeader, Card, StatusBadge, Alert } from '../components/ui';
import { HelpPanel } from '../components/HelpPanel';
import { useHelp } from '../hooks/useHelp';

type TabType = 'all' | 'differences' | 'agreed';

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
  const s = dateStr.replace('T', ' ').split(' ')[0];
  const parts = s.split('-');
  if (parts.length === 3 && parts[0].length === 4) {
    return `${parts[2]}/${parts[1]}/${parts[0]}`;
  }
  return dateStr;
}

function formatMatchRate(rate: number | null): string {
  if (rate === null || rate === undefined) return '-';
  return `${Math.round(rate * 100)}%`;
}

export default function SupplierStatementQueue() {
  const navigate = useNavigate();
  const { showHelp, setShowHelp } = useHelp();
  const [activeTab, setActiveTab] = useState<TabType>('differences');
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [approvingId, setApprovingId] = useState<number | null>(null);

  const tabs: { id: TabType; label: string; icon: React.ComponentType<{ className?: string }> }[] = [
    { id: 'all', label: 'All', icon: Inbox },
    { id: 'differences', label: 'Needs Review', icon: AlertTriangle },
    { id: 'agreed', label: 'Agreed', icon: CheckCircle },
  ];

  const { data, isLoading, refetch } = useQuery({
    queryKey: ['supplier-statements', activeTab],
    queryFn: async () => {
      // Always fetch all — filter client-side by agreed/differences
      const res = await authFetch('/api/supplier-statements');
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
    setApprovingId(id);
    try {
      const res = await authFetch(`/api/supplier-statements/${id}/approve`, { method: 'POST' });
      const json = await res.json();
      if (!res.ok || json.error || json.detail) throw new Error(json.detail || json.error || 'Failed to approve statement');
      setSuccess(`${json.message}${json.recipient ? ' — sent to ' + json.recipient : ''}`);
      refetch();
    } catch (e: any) {
      setError(e.message);
    } finally {
      setApprovingId(null);
    }
  };

  const allStatements = data?.statements || [];
  const statements = activeTab === 'all'
    ? allStatements
    : activeTab === 'differences'
    ? allStatements.filter(s => (s.query_count || 0) > 0)
    : allStatements.filter(s => (s.query_count || 0) === 0);

  return (
    <div className="space-y-6">
      <PageHeader icon={FileText} title="Statement Queue" subtitle="Process and reconcile supplier statements">
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
          { title: 'Statement Queue', content: 'Incoming supplier statements are automatically extracted from email and queued here for processing.' },
          { title: 'Status', content: 'Received = new and unprocessed. Processing = currently being reconciled. Reconciled = ready for review. Approved = response has been sent to the supplier.' },
          { title: 'Processing', content: 'Click Process to extract line items and reconcile against Opera. The system matches each item automatically and flags any discrepancies.' },
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
          ) : statements.length === 0 ? (
            <div className="text-center py-12 text-gray-500">
              No statements found{activeTab !== 'all' ? ` with status "${activeTab}"` : ''}
            </div>
          ) : (
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Supplier</th>
                  <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Statement Date</th>
                  <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Received</th>
                  <th className="px-3 py-3 text-center text-xs font-medium text-gray-500 uppercase">Result</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {statements.map(stmt => (
                  <tr
                    key={stmt.id}
                    className="hover:bg-gray-50 cursor-pointer"
                    onClick={() => navigate(`/supplier/statements/${stmt.id}`)}
                  >
                    <td className="px-3 py-3">
                      <div className="text-sm font-medium text-gray-900">{stmt.supplier_name || stmt.supplier_code}</div>
                      <div className="text-xs text-gray-500">{stmt.supplier_code}</div>
                    </td>
                    <td className="px-3 py-3 text-sm text-gray-700">
                      {formatDate(stmt.statement_date)}
                    </td>
                    <td className="px-3 py-3 text-sm text-gray-500">
                      {formatDate(stmt.received_date)}
                    </td>
                    <td className="px-3 py-3 text-center">
                      {(stmt.query_count || 0) === 0 ? (
                        <span className="inline-flex items-center gap-1 text-xs font-medium text-green-700 bg-green-50 px-2 py-0.5 rounded">
                          <CheckCircle className="w-3 h-3" /> Agreed
                        </span>
                      ) : (
                        <span className="inline-flex items-center gap-1 text-xs font-medium text-red-700 bg-red-50 px-2 py-0.5 rounded">
                          <AlertTriangle className="w-3 h-3" /> {stmt.query_count} {stmt.query_count === 1 ? 'query' : 'queries'}
                        </span>
                      )}
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
