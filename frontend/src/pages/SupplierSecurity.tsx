import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  Shield,
  RefreshCw,
  AlertTriangle,
  FileText,
  Users,
  CheckCircle,
  XCircle,
  Building,
  Mail,
  Trash2,
  Plus,
} from 'lucide-react';
import { useLocation } from 'react-router-dom';
import apiClient from '../api/client';
import type {
  SupplierSecurityAlertsResponse,
  SupplierSecurityAuditResponse,
  SupplierApprovedSendersResponse,
} from '../api/client';
import { PageHeader, LoadingState, EmptyState, Alert, Card } from '../components/ui';

type Tab = 'alerts' | 'audit' | 'senders';

export function SupplierSecurity() {
  const location = useLocation();
  const queryClient = useQueryClient();

  // Determine initial tab from URL
  const getInitialTab = (): Tab => {
    if (location.pathname.includes('/audit')) return 'audit';
    if (location.pathname.includes('/senders')) return 'senders';
    return 'alerts';
  };

  const [activeTab, setActiveTab] = useState<Tab>(getInitialTab());
  const [days, setDays] = useState(90);
  const [newSenderSupplier, setNewSenderSupplier] = useState('');
  const [newSenderEmail, setNewSenderEmail] = useState('');

  // Queries
  const alertsQuery = useQuery<SupplierSecurityAlertsResponse>({
    queryKey: ['supplierSecurityAlerts'],
    queryFn: async () => {
      const response = await apiClient.supplierSecurityAlerts();
      return response.data;
    },
    enabled: activeTab === 'alerts',
  });

  const auditQuery = useQuery<SupplierSecurityAuditResponse>({
    queryKey: ['supplierSecurityAudit', days],
    queryFn: async () => {
      const response = await apiClient.supplierSecurityAudit(days);
      return response.data;
    },
    enabled: activeTab === 'audit',
  });

  const sendersQuery = useQuery<SupplierApprovedSendersResponse>({
    queryKey: ['supplierApprovedSenders'],
    queryFn: async () => {
      const response = await apiClient.supplierApprovedSenders();
      return response.data;
    },
    enabled: activeTab === 'senders',
  });

  // Mutations
  const verifyMutation = useMutation({
    mutationFn: async (alertId: number) => {
      const response = await apiClient.supplierSecurityAlertVerify(alertId, 'User');
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['supplierSecurityAlerts'] });
      queryClient.invalidateQueries({ queryKey: ['supplierSecurityAudit'] });
    },
  });

  const addSenderMutation = useMutation({
    mutationFn: async ({ supplierCode, email }: { supplierCode: string; email: string }) => {
      const response = await apiClient.supplierApprovedSenderAdd(supplierCode, email);
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['supplierApprovedSenders'] });
      setNewSenderSupplier('');
      setNewSenderEmail('');
    },
  });

  const removeSenderMutation = useMutation({
    mutationFn: async (senderId: number) => {
      const response = await apiClient.supplierApprovedSenderRemove(senderId);
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['supplierApprovedSenders'] });
    },
  });

  const formatDate = (dateStr: string | null): string => {
    if (!dateStr) return '-';
    return new Date(dateStr).toLocaleDateString('en-GB', {
      day: '2-digit',
      month: 'short',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  };

  const alerts = alertsQuery.data?.alerts || [];
  const auditEntries = auditQuery.data?.entries || [];
  const senders = sendersQuery.data?.senders || [];

  return (
    <div className="space-y-6">
      {/* Header */}
      <PageHeader icon={Shield} title="Security" subtitle="Monitor supplier changes and manage approved senders" />

      {/* Tabs */}
      <div className="flex gap-2 border-b border-gray-200 pb-4">
        <button
          onClick={() => setActiveTab('alerts')}
          className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
            activeTab === 'alerts'
              ? 'bg-red-500 text-white'
              : 'bg-red-50 text-red-700 hover:bg-red-100'
          }`}
        >
          <AlertTriangle className="h-4 w-4" />
          Alerts ({alerts.length})
        </button>
        <button
          onClick={() => setActiveTab('audit')}
          className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
            activeTab === 'audit'
              ? 'bg-gray-900 text-white'
              : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
          }`}
        >
          <FileText className="h-4 w-4" />
          Audit Log
        </button>
        <button
          onClick={() => setActiveTab('senders')}
          className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
            activeTab === 'senders'
              ? 'bg-blue-500 text-white'
              : 'bg-blue-50 text-blue-700 hover:bg-blue-100'
          }`}
        >
          <Users className="h-4 w-4" />
          Approved Senders
        </button>
      </div>

      {/* Alerts Tab */}
      {activeTab === 'alerts' && (
        <div className="space-y-4">
          {alertsQuery.isLoading && (
            <LoadingState message="Loading security alerts..." />
          )}

          {!alertsQuery.isLoading && alerts.length === 0 && (
            <Alert variant="success" title="No pending alerts">
              All supplier changes have been verified
            </Alert>
          )}

          {!alertsQuery.isLoading && alerts.map((alert) => (
            <Card key={alert.id} className="border-red-200">
              <div className="flex items-start justify-between">
                <div className="flex items-start gap-3">
                  <div className="p-2 bg-red-100 rounded-lg">
                    <AlertTriangle className="h-5 w-5 text-red-600" />
                  </div>
                  <div>
                    <div className="flex items-center gap-2">
                      <Building className="h-4 w-4 text-gray-400" />
                      <span className="text-sm font-semibold text-gray-900">{alert.supplier_name}</span>
                      <span className="text-xs text-gray-500">({alert.supplier_code})</span>
                    </div>
                    <p className="text-sm font-semibold text-red-700 mt-1">
                      {alert.field_name} Changed
                    </p>
                    <div className="mt-2 grid grid-cols-2 gap-4 text-sm">
                      <div>
                        <span className="text-gray-500">Previous: </span>
                        <span className="text-gray-700">{alert.old_value || '(empty)'}</span>
                      </div>
                      <div>
                        <span className="text-gray-500">New: </span>
                        <span className="font-medium text-red-700">{alert.new_value || '(empty)'}</span>
                      </div>
                    </div>
                    <p className="text-xs text-gray-500 mt-2">
                      Changed by: {alert.changed_by || 'Unknown'} at {formatDate(alert.changed_at)}
                    </p>
                  </div>
                </div>
                <button
                  onClick={() => verifyMutation.mutate(alert.id)}
                  disabled={verifyMutation.isPending}
                  className="px-4 py-2 bg-emerald-600 text-white text-sm font-medium rounded-lg hover:bg-emerald-700 transition-colors"
                >
                  Verify
                </button>
              </div>
            </Card>
          ))}
        </div>
      )}

      {/* Audit Tab */}
      {activeTab === 'audit' && (
        <div className="space-y-4">
          <div className="flex justify-end">
            <select
              value={days}
              onChange={(e) => setDays(Number(e.target.value))}
              className="px-3 py-2 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value={30}>Last 30 days</option>
              <option value={90}>Last 90 days</option>
              <option value={180}>Last 6 months</option>
              <option value={365}>Last year</option>
            </select>
          </div>

          {auditQuery.isLoading && (
            <LoadingState message="Loading audit log..." />
          )}

          {!auditQuery.isLoading && (
            <Card padding={false}>
              <table className="w-full">
                <thead className="bg-gray-50 border-b border-gray-200">
                  <tr>
                    <th className="text-left py-3 px-4 text-xs font-semibold text-gray-500 uppercase">Supplier</th>
                    <th className="text-left py-3 px-4 text-xs font-semibold text-gray-500 uppercase">Field</th>
                    <th className="text-left py-3 px-4 text-xs font-semibold text-gray-500 uppercase">Previous</th>
                    <th className="text-left py-3 px-4 text-xs font-semibold text-gray-500 uppercase">New</th>
                    <th className="text-left py-3 px-4 text-xs font-semibold text-gray-500 uppercase">Changed</th>
                    <th className="text-center py-3 px-4 text-xs font-semibold text-gray-500 uppercase">Verified</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {auditEntries.length === 0 ? (
                    <tr>
                      <td colSpan={6} className="py-12 text-center">
                        <EmptyState icon={FileText} title="No audit entries" />
                      </td>
                    </tr>
                  ) : (
                    auditEntries.map((entry) => (
                      <tr key={entry.id} className="hover:bg-gray-50">
                        <td className="py-3 px-4">
                          <p className="text-sm font-medium text-gray-900">{entry.supplier_name}</p>
                          <p className="text-xs text-gray-500">{entry.supplier_code}</p>
                        </td>
                        <td className="py-3 px-4 text-sm text-gray-700">{entry.field_name}</td>
                        <td className="py-3 px-4 text-sm text-gray-600">{entry.old_value || '-'}</td>
                        <td className="py-3 px-4 text-sm text-gray-900 font-medium">{entry.new_value || '-'}</td>
                        <td className="py-3 px-4">
                          <p className="text-sm text-gray-600">{formatDate(entry.changed_at)}</p>
                          <p className="text-xs text-gray-400">{entry.changed_by || 'Unknown'}</p>
                        </td>
                        <td className="py-3 px-4 text-center">
                          {entry.verified ? (
                            <CheckCircle className="h-5 w-5 text-emerald-500 mx-auto" />
                          ) : (
                            <XCircle className="h-5 w-5 text-red-500 mx-auto" />
                          )}
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </Card>
          )}
        </div>
      )}

      {/* Approved Senders Tab */}
      {activeTab === 'senders' && (
        <div className="space-y-4">
          {/* Add New Sender */}
          <Card title="Add Approved Sender" icon={Plus}>
            <div className="flex gap-3">
              <input
                type="text"
                placeholder="Supplier Code"
                value={newSenderSupplier}
                onChange={(e) => setNewSenderSupplier(e.target.value)}
                className="flex-1 px-3 py-2 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
              <input
                type="email"
                placeholder="Email Address"
                value={newSenderEmail}
                onChange={(e) => setNewSenderEmail(e.target.value)}
                className="flex-1 px-3 py-2 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
              <button
                onClick={() => addSenderMutation.mutate({ supplierCode: newSenderSupplier, email: newSenderEmail })}
                disabled={!newSenderSupplier || !newSenderEmail || addSenderMutation.isPending}
                className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 transition-colors"
              >
                <Plus className="h-4 w-4" />
                Add
              </button>
            </div>
          </Card>

          {sendersQuery.isLoading && (
            <LoadingState message="Loading approved senders..." />
          )}

          {!sendersQuery.isLoading && (
            <Card padding={false}>
              <table className="w-full">
                <thead className="bg-gray-50 border-b border-gray-200">
                  <tr>
                    <th className="text-left py-3 px-4 text-xs font-semibold text-gray-500 uppercase">Supplier</th>
                    <th className="text-left py-3 px-4 text-xs font-semibold text-gray-500 uppercase">Email</th>
                    <th className="text-left py-3 px-4 text-xs font-semibold text-gray-500 uppercase">Domain</th>
                    <th className="text-left py-3 px-4 text-xs font-semibold text-gray-500 uppercase">Added</th>
                    <th className="text-right py-3 px-4 text-xs font-semibold text-gray-500 uppercase">Actions</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {senders.length === 0 ? (
                    <tr>
                      <td colSpan={5} className="py-12 text-center">
                        <EmptyState icon={Users} title="No approved senders" message="Add sender email addresses above" />
                      </td>
                    </tr>
                  ) : (
                    senders.map((sender) => (
                      <tr key={sender.id} className="hover:bg-gray-50">
                        <td className="py-3 px-4">
                          <p className="text-sm font-medium text-gray-900">{sender.supplier_name}</p>
                          <p className="text-xs text-gray-500">{sender.supplier_code}</p>
                        </td>
                        <td className="py-3 px-4">
                          <div className="flex items-center gap-2">
                            <Mail className="h-4 w-4 text-gray-400" />
                            <span className="text-sm text-gray-700">{sender.email_address}</span>
                          </div>
                        </td>
                        <td className="py-3 px-4 text-sm text-gray-600">{sender.email_domain}</td>
                        <td className="py-3 px-4">
                          <p className="text-sm text-gray-600">{formatDate(sender.added_at)}</p>
                          <p className="text-xs text-gray-400">{sender.added_by || 'Unknown'}</p>
                        </td>
                        <td className="py-3 px-4 text-right">
                          <button
                            onClick={() => removeSenderMutation.mutate(sender.id)}
                            disabled={removeSenderMutation.isPending}
                            className="text-red-600 hover:text-red-700"
                          >
                            <Trash2 className="h-4 w-4" />
                          </button>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </Card>
          )}
        </div>
      )}
    </div>
  );
}

export default SupplierSecurity;
