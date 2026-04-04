import { useState, useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  Settings, RefreshCw, Save, Clock, Mail, AlertTriangle,
  Calendar, Banknote, Users, FileText, HelpCircle
} from 'lucide-react';
import { authFetch } from '../api/client';
import { PageHeader, Card, Alert } from '../components/ui';
import { HelpPanel } from '../components/HelpPanel';
import { useHelp } from '../hooks/useHelp';
import type { LucideIcon } from 'lucide-react';

interface SettingConfig {
  key: string;
  label: string;
  description: string;
  icon: LucideIcon;
  type: 'number' | 'text' | 'email' | 'toggle' | 'date';
  prefix?: string;
  suffix?: string;
  placeholder?: string;
}

interface SettingsGroup {
  title: string;
  description: string;
  settings: SettingConfig[];
}

interface SettingsResponse {
  settings: Record<string, any>;
}

const SETTINGS_GROUPS: SettingsGroup[] = [
  {
    title: 'Timing',
    description: 'Configure processing delays and SLA targets',
    settings: [
      {
        key: 'acknowledgment_delay_minutes',
        label: 'Acknowledgment Delay',
        description: 'Minutes to wait before sending receipt acknowledgment (0 = immediate)',
        icon: Clock,
        type: 'number',
        suffix: 'minutes',
      },
      {
        key: 'processing_sla_hours',
        label: 'Processing SLA',
        description: 'Target hours to process a received statement',
        icon: Clock,
        type: 'number',
        suffix: 'hours',
      },
      {
        key: 'query_response_days',
        label: 'Query Response Deadline',
        description: 'Expected days for supplier to respond to queries',
        icon: Calendar,
        type: 'number',
        suffix: 'days',
      },
      {
        key: 'follow_up_reminder_days',
        label: 'Follow-up Reminder',
        description: 'Days before sending a follow-up reminder for unanswered queries',
        icon: Calendar,
        type: 'number',
        suffix: 'days',
      },
      {
        key: 'next_payment_run_date',
        label: 'Next Payment Run Date',
        description: 'Scheduled date for the next supplier payment run — included in automated responses to suppliers',
        icon: Calendar,
        type: 'date',
      },
    ],
  },
  {
    title: 'Thresholds',
    description: 'Set limits for automatic processing decisions',
    settings: [
      {
        key: 'large_discrepancy_threshold',
        label: 'Large Discrepancy Threshold',
        description: 'Amount above which discrepancies require manual review',
        icon: Banknote,
        type: 'number',
        prefix: '\u00A3',
      },
      {
        key: 'require_approval_above',
        label: 'Require Approval Above',
        description: 'Require manual approval for responses with variance above this amount',
        icon: Banknote,
        type: 'number',
        prefix: '\u00A3',
      },
      {
        key: 'old_statement_threshold_days',
        label: 'Old Statement Threshold',
        description: 'Days after which a received statement is flagged as old',
        icon: AlertTriangle,
        type: 'number',
        suffix: 'days',
      },
      {
        key: 'payment_notification_days',
        label: 'Payment Notification Period',
        description: 'Only include payments made within this many days in responses',
        icon: Calendar,
        type: 'number',
        suffix: 'days',
      },
    ],
  },
  {
    title: 'Notifications',
    description: 'Configure email notifications and alerts',
    settings: [
      {
        key: 'security_alert_recipients',
        label: 'Security Alert Recipients',
        description: 'Email addresses for bank detail change alerts (comma-separated)',
        icon: Mail,
        type: 'email',
        placeholder: 'email1@company.com, email2@company.com',
      },
      {
        key: 'response_cc_email',
        label: 'CC All Responses To',
        description: 'Email address to CC on all sent responses (for audit)',
        icon: Mail,
        type: 'email',
        placeholder: 'accounts@company.com',
      },
    ],
  },
  {
    title: 'Remittance',
    description: 'Configure remittance advice generation and sending',
    settings: [
      {
        key: 'auto_acknowledge',
        label: 'Auto-Acknowledge Receipt',
        description: 'Automatically send acknowledgment when a statement is received',
        icon: Mail,
        type: 'toggle',
      },
      {
        key: 'auto_process',
        label: 'Auto-Process Statements',
        description: 'Automatically reconcile statements when received',
        icon: RefreshCw,
        type: 'toggle',
      },
      {
        key: 'auto_respond_if_reconciled',
        label: 'Auto-Respond if Fully Reconciled',
        description: 'Send response immediately if all items match (no queries)',
        icon: Mail,
        type: 'toggle',
      },
      {
        key: 'auto_respond_with_queries',
        label: 'Auto-Respond with Queries',
        description: 'Send response immediately even when there are queries (otherwise requires approval)',
        icon: AlertTriangle,
        type: 'toggle',
      },
    ],
  },
  {
    title: 'Onboarding',
    description: 'Configure new supplier onboarding behaviour',
    settings: [
      {
        key: 'auto_create_supplier_from_email',
        label: 'Auto-Create from Email',
        description: 'Automatically create a supplier contact record when a new sender emails a statement',
        icon: Users,
        type: 'toggle',
      },
      {
        key: 'require_sender_approval',
        label: 'Require Sender Approval',
        description: 'New sender email addresses require approval before statements are processed',
        icon: AlertTriangle,
        type: 'toggle',
      },
      {
        key: 'default_statement_format',
        label: 'Default Statement Format',
        description: 'Expected format for incoming statements (pdf, csv, or auto-detect)',
        icon: FileText,
        type: 'text',
        placeholder: 'auto',
      },
    ],
  },
];

export default function SupplierSettings() {
  const { showHelp, setShowHelp } = useHelp();
  const [formValues, setFormValues] = useState<Record<string, string>>({});
  const [hasChanges, setHasChanges] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  const { data, isLoading, refetch } = useQuery({
    queryKey: ['supplier-settings'],
    queryFn: async () => {
      const res = await authFetch('/api/supplier-settings');
      if (!res.ok) throw new Error('Failed to fetch settings');
      const json = await res.json();
      if (json.error) throw new Error(json.error);
      return json as SettingsResponse;
    },
    staleTime: 60000,
  });

  // Initialize form values from query data
  useEffect(() => {
    if (data?.settings) {
      const initial: Record<string, string> = {};
      Object.entries(data.settings).forEach(([key, val]) => {
        initial[key] = typeof val === 'object' && val !== null ? val.value : String(val ?? '');
      });
      setFormValues(initial);
      setHasChanges(false);
    }
  }, [data]);

  const handleChange = (key: string, value: string) => {
    setFormValues(prev => ({ ...prev, [key]: value }));
    setHasChanges(true);
  };

  const handleSave = async () => {
    setError(null);
    setSuccess(null);
    setSaving(true);
    try {
      const res = await authFetch('/api/supplier-settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(formValues),
      });
      const json = await res.json();
      if (!res.ok || json.error) throw new Error(json.error || 'Failed to save settings');
      setSuccess('Settings saved successfully');
      setHasChanges(false);
      refetch();
    } catch (e: any) {
      setError(e.message);
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="space-y-6">
      <PageHeader icon={Settings} title="Supplier Settings" subtitle="Configure supplier statement automation behaviour">
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
        <button
          onClick={handleSave}
          disabled={!hasChanges || saving}
          className={`flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-lg ${
            hasChanges
              ? 'bg-blue-600 text-white hover:bg-blue-700'
              : 'bg-gray-100 text-gray-400 cursor-not-allowed'
          }`}
        >
          <Save className="w-4 h-4" />
          {saving ? 'Saving...' : 'Save Changes'}
        </button>
      </PageHeader>

      <HelpPanel
        isOpen={showHelp}
        onClose={() => setShowHelp(false)}
        sections={[
          { title: 'Timing', content: 'Acknowledgment delay (0 = immediate), processing SLA target, query response deadline, and follow-up reminder interval.' },
          { title: 'Thresholds', content: 'Large discrepancy amount that triggers manual review. Approval required above a set amount. Old statement days threshold for flagging stale statements.' },
          { title: 'Notifications', content: 'Security alert recipients receive emails when supplier bank details change. CC on responses sends a copy of every outgoing response for audit.' },
          { title: 'Remittance', content: 'Auto-send remittance advice after payment. Configure format (email or PDF) and CC address for copies.' },
          { title: 'Onboarding', content: 'Auto-detect new suppliers from incoming emails. Require bank detail verification before processing the first payment to a new supplier.' },
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

      {isLoading ? (
        <div className="flex items-center justify-center py-12">
          <RefreshCw className="w-6 h-6 text-gray-400 animate-spin" />
        </div>
      ) : (
        <div className="space-y-8">
          {SETTINGS_GROUPS.map(group => (
            <div key={group.title}>
              <div className="mb-4">
                <h2 className="text-base font-semibold text-gray-900">{group.title}</h2>
                <p className="text-sm text-gray-500">{group.description}</p>
              </div>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {group.settings.map(setting => {
                  const Icon = setting.icon;
                  const currentValue = formValues[setting.key] || '';
                  const isToggle = setting.type === 'toggle';
                  const isDate = setting.type === 'date';
                  const isChecked = currentValue === 'true' || currentValue === '1';

                  return (
                    <Card key={setting.key}>
                      <div className="flex items-start gap-3">
                        <div className="p-2 bg-gray-100 rounded-lg flex-shrink-0">
                          <Icon className="h-5 w-5 text-gray-600" />
                        </div>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center justify-between">
                            <label className="block text-sm font-semibold text-gray-900 mb-1">
                              {setting.label}
                            </label>
                            {isToggle && (
                              <button
                                onClick={() => handleChange(setting.key, isChecked ? 'false' : 'true')}
                                className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors ${
                                  isChecked ? 'bg-blue-600' : 'bg-gray-200'
                                }`}
                              >
                                <span
                                  className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${
                                    isChecked ? 'translate-x-6' : 'translate-x-1'
                                  }`}
                                />
                              </button>
                            )}
                          </div>
                          <p className="text-sm text-gray-500 mb-3">{setting.description}</p>
                          {!isToggle && (
                            <div className="flex items-center gap-2">
                              {setting.prefix && (
                                <span className="text-sm text-gray-500">{setting.prefix}</span>
                              )}
                              {isDate ? (
                                <input
                                  type="date"
                                  value={currentValue}
                                  onChange={e => handleChange(setting.key, e.target.value)}
                                  className="flex-1 px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                                />
                              ) : (
                                <input
                                  type={setting.type === 'number' ? 'number' : 'text'}
                                  value={currentValue}
                                  onChange={e => handleChange(setting.key, e.target.value)}
                                  placeholder={setting.placeholder}
                                  className="flex-1 px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                                />
                              )}
                              {setting.suffix && (
                                <span className="text-sm text-gray-500">{setting.suffix}</span>
                              )}
                            </div>
                          )}
                        </div>
                      </div>
                    </Card>
                  );
                })}
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Help Text */}
      <Alert variant="info" title="About These Settings">
        <ul className="space-y-1 text-sm">
          <li><strong>Timing:</strong> Controls how quickly statements are acknowledged and processed.</li>
          <li><strong>Thresholds:</strong> Large discrepancies are flagged for manual review before responding.</li>
          <li><strong>Notifications:</strong> Bank detail changes will be emailed to security alert recipients.</li>
          <li><strong>Remittance:</strong> Auto-respond settings control whether responses need manual approval.</li>
          <li><strong>Onboarding:</strong> Governs how new supplier senders are handled.</li>
        </ul>
      </Alert>
    </div>
  );
}

export { SupplierSettings };
