import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  Shield, AlertTriangle, CheckCircle, RefreshCw, Clock,
  Building, Mail, Flag, Phone, HelpCircle
} from 'lucide-react';
import { authFetch } from '../api/client';
import { PageHeader, Card, StatusBadge, Alert } from '../components/ui';
import { HelpPanel } from '../components/HelpPanel';
import { useHelp } from '../hooks/useHelp';

type TabType = 'pending' | 'verified' | 'all';

interface SecurityAlert {
  id: number;
  supplier_code: string;
  supplier_name: string;
  field_name: string;
  old_value: string | null;
  new_value: string | null;
  changed_at: string;
  changed_by: string | null;
  status: string;
  verified_at: string | null;
  verified_by: string | null;
}

interface AlertsResponse {
  alerts: SecurityAlert[];
  total: number;
}

interface EmailFlag {
  id: number;
  supplier_code: string;
  supplier_name: string;
  email_subject: string;
  sender: string;
  flag_reason: string;
  flagged_at: string;
  reviewed: boolean;
}

interface EmailFlagsResponse {
  flags: EmailFlag[];
  total: number;
}

function formatDate(dateStr: string | null): string {
  if (!dateStr) return '-';
  return new Date(dateStr).toLocaleDateString('en-GB', {
    day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit',
  });
}

export default function SupplierSecurity() {
  const { showHelp, setShowHelp } = useHelp();
  const [activeTab, setActiveTab] = useState<TabType>('pending');
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [phoneConfirmed, setPhoneConfirmed] = useState<Set<number>>(new Set());

  const tabs: { id: TabType; label: string; icon: React.ComponentType<{ className?: string }> }[] = [
    { id: 'pending', label: 'Pending', icon: AlertTriangle },
    { id: 'verified', label: 'Verified', icon: CheckCircle },
    { id: 'all', label: 'All', icon: Shield },
  ];

  // Fetch security alerts
  const { data: alertsData, isLoading: alertsLoading, refetch: refetchAlerts } = useQuery({
    queryKey: ['supplier-security-alerts', activeTab],
    queryFn: async () => {
      const res = await authFetch('/api/supplier-security/alerts');
      if (!res.ok) throw new Error('Failed to fetch security alerts');
      const json = await res.json();
      if (json.error) throw new Error(json.error);
      return json as AlertsResponse;
    },
    staleTime: 30000,
  });

  // Fetch email flags
  const { data: flagsData, isLoading: flagsLoading } = useQuery({
    queryKey: ['supplier-security-email-flags'],
    queryFn: async () => {
      const res = await authFetch('/api/supplier-security/email-flags');
      if (!res.ok) throw new Error('Failed to fetch email flags');
      const json = await res.json();
      if (json.error) throw new Error(json.error);
      return json as EmailFlagsResponse;
    },
    staleTime: 60000,
  });

  const handleVerify = async (alertId: number) => {
    setError(null);
    setSuccess(null);
    try {
      const res = await authFetch(`/api/supplier-security/alerts/${alertId}/verify`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ phone_confirmed: true }),
      });
      const json = await res.json();
      if (!res.ok || json.error) throw new Error(json.error || 'Failed to verify alert');
      setSuccess(json.message || 'Alert verified successfully');
      setPhoneConfirmed(prev => {
        const next = new Set(prev);
        next.delete(alertId);
        return next;
      });
      refetchAlerts();
    } catch (e: any) {
      setError(e.message);
    }
  };

  const togglePhoneConfirm = (alertId: number) => {
    setPhoneConfirmed(prev => {
      const next = new Set(prev);
      if (next.has(alertId)) {
        next.delete(alertId);
      } else {
        next.add(alertId);
      }
      return next;
    });
  };

  // Filter alerts based on active tab
  const allAlerts = alertsData?.alerts || [];
  const filteredAlerts = activeTab === 'all'
    ? allAlerts
    : allAlerts.filter(a =>
        activeTab === 'pending' ? a.status !== 'verified' : a.status === 'verified'
      );

  const emailFlags = flagsData?.flags || [];

  return (
    <div className="space-y-6">
      <PageHeader icon={Shield} title="Security Alerts" subtitle="Monitor supplier changes and verify suspicious activity">
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
          onClick={() => refetchAlerts()}
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
          { title: 'Security Alerts', content: 'Triggered automatically when supplier bank details change in Opera. Each change must be verified before payments resume.' },
          { title: 'Verification', content: 'Tick the phone confirmation checkbox after calling the supplier to confirm the change, then click Verify to mark the alert as safe.' },
          { title: 'Email Flags', content: 'Emails flagged because they mention bank details or payment instructions. These may indicate a potential fraud attempt and should be reviewed carefully.' },
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
          {alertsLoading ? (
            <div className="flex items-center justify-center py-12">
              <RefreshCw className="w-6 h-6 text-gray-400 animate-spin" />
            </div>
          ) : filteredAlerts.length === 0 ? (
            <div className="text-center py-12 text-gray-500">
              {activeTab === 'pending'
                ? 'No pending alerts - all supplier changes have been verified'
                : `No ${activeTab} alerts found`}
            </div>
          ) : (
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Supplier</th>
                  <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Field Changed</th>
                  <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Old Value</th>
                  <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">New Value</th>
                  <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Changed At</th>
                  <th className="px-3 py-3 text-center text-xs font-medium text-gray-500 uppercase">Status</th>
                  <th className="px-3 py-3 text-center text-xs font-medium text-gray-500 uppercase">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {filteredAlerts.map(alert => (
                  <tr key={alert.id} className="hover:bg-gray-50">
                    <td className="px-3 py-3">
                      <div className="flex items-center gap-2">
                        <Building className="h-4 w-4 text-gray-400 flex-shrink-0" />
                        <div>
                          <div className="text-sm font-medium text-gray-900">{alert.supplier_name}</div>
                          <div className="text-xs text-gray-500">{alert.supplier_code}</div>
                        </div>
                      </div>
                    </td>
                    <td className="px-3 py-3">
                      <span className="text-sm font-medium text-red-700">{alert.field_name}</span>
                    </td>
                    <td className="px-3 py-3">
                      <span className="text-sm text-gray-600">{alert.old_value || '(empty)'}</span>
                    </td>
                    <td className="px-3 py-3">
                      <span className="text-sm font-medium text-gray-900">{alert.new_value || '(empty)'}</span>
                    </td>
                    <td className="px-3 py-3">
                      <div className="text-sm text-gray-600">{formatDate(alert.changed_at)}</div>
                      {alert.changed_by && (
                        <div className="text-xs text-gray-400">by {alert.changed_by}</div>
                      )}
                    </td>
                    <td className="px-3 py-3 text-center">
                      <StatusBadge variant={alert.status === 'verified' ? 'success' : 'danger'}>
                        {alert.status === 'verified' ? 'Verified' : 'Pending'}
                      </StatusBadge>
                    </td>
                    <td className="px-3 py-3 text-center">
                      {alert.status !== 'verified' ? (
                        <div className="flex flex-col items-center gap-2">
                          <label className="flex items-center gap-1.5 text-xs text-gray-600 cursor-pointer">
                            <input
                              type="checkbox"
                              checked={phoneConfirmed.has(alert.id)}
                              onChange={() => togglePhoneConfirm(alert.id)}
                              className="rounded border-gray-300 text-green-600 focus:ring-green-500"
                            />
                            <Phone className="w-3 h-3" />
                            Phone confirmed
                          </label>
                          <button
                            onClick={() => handleVerify(alert.id)}
                            disabled={!phoneConfirmed.has(alert.id)}
                            className={`flex items-center gap-1 px-2.5 py-1 text-xs font-medium rounded-lg ${
                              phoneConfirmed.has(alert.id)
                                ? 'text-green-700 bg-green-50 border border-green-200 hover:bg-green-100'
                                : 'text-gray-400 bg-gray-50 border border-gray-200 cursor-not-allowed'
                            }`}
                            title={phoneConfirmed.has(alert.id) ? 'Verify change' : 'Please confirm by phone first'}
                          >
                            <CheckCircle className="w-3 h-3" />
                            Verify
                          </button>
                        </div>
                      ) : (
                        <span className="text-xs text-gray-400">
                          {alert.verified_by && `by ${alert.verified_by}`}
                          {alert.verified_at && ` on ${formatDate(alert.verified_at)}`}
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

      {/* Email Flags Section */}
      <Card padding={false} className="overflow-hidden">
        <div className="px-4 py-3 border-b border-gray-200 bg-gray-50">
          <div className="flex items-center gap-2">
            <Flag className="w-4 h-4 text-amber-500" />
            <h3 className="text-sm font-semibold text-gray-700">Email Flags</h3>
            <span className="text-xs text-gray-500">({emailFlags.length})</span>
          </div>
        </div>
        <div className="p-4">
          {flagsLoading ? (
            <div className="flex items-center justify-center py-8">
              <RefreshCw className="w-5 h-5 text-gray-400 animate-spin" />
            </div>
          ) : emailFlags.length === 0 ? (
            <div className="text-center py-8 text-gray-500 text-sm">
              No flagged emails
            </div>
          ) : (
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Supplier</th>
                  <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Email Subject</th>
                  <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Sender</th>
                  <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Flag Reason</th>
                  <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Flagged At</th>
                  <th className="px-3 py-3 text-center text-xs font-medium text-gray-500 uppercase">Reviewed</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {emailFlags.map(flag => (
                  <tr key={flag.id} className="hover:bg-gray-50">
                    <td className="px-3 py-3">
                      <div className="text-sm font-medium text-gray-900">{flag.supplier_name}</div>
                      <div className="text-xs text-gray-500">{flag.supplier_code}</div>
                    </td>
                    <td className="px-3 py-3">
                      <div className="text-sm text-gray-700 truncate max-w-[250px]">{flag.email_subject}</div>
                    </td>
                    <td className="px-3 py-3">
                      <div className="flex items-center gap-1 text-sm text-gray-600">
                        <Mail className="w-3.5 h-3.5" />
                        {flag.sender}
                      </div>
                    </td>
                    <td className="px-3 py-3">
                      <StatusBadge variant="warning">{flag.flag_reason}</StatusBadge>
                    </td>
                    <td className="px-3 py-3 text-sm text-gray-500">
                      {formatDate(flag.flagged_at)}
                    </td>
                    <td className="px-3 py-3 text-center">
                      {flag.reviewed ? (
                        <CheckCircle className="w-4 h-4 text-green-500 mx-auto" />
                      ) : (
                        <Clock className="w-4 h-4 text-amber-500 mx-auto" />
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

export { SupplierSecurity };
