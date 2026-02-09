import { useState, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  Settings,
  RefreshCw,
  Save,
  Clock,
  Mail,
  AlertTriangle,
  Calendar,
  Banknote,
} from 'lucide-react';
import apiClient from '../api/client';
import type { SupplierSettingsResponse } from '../api/client';

export function SupplierSettings() {
  const queryClient = useQueryClient();
  const [formValues, setFormValues] = useState<Record<string, string>>({});
  const [hasChanges, setHasChanges] = useState(false);

  const settingsQuery = useQuery<SupplierSettingsResponse>({
    queryKey: ['supplierSettings'],
    queryFn: async () => {
      const response = await apiClient.supplierSettings();
      return response.data;
    },
  });

  const updateMutation = useMutation({
    mutationFn: async (settings: Record<string, string>) => {
      const response = await apiClient.supplierSettingsUpdate(settings);
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['supplierSettings'] });
      setHasChanges(false);
    },
  });

  // Initialize form values from query
  useEffect(() => {
    if (settingsQuery.data?.settings) {
      const initialValues: Record<string, string> = {};
      Object.entries(settingsQuery.data.settings).forEach(([key, val]) => {
        initialValues[key] = typeof val === 'object' ? val.value : val;
      });
      setFormValues(initialValues);
    }
  }, [settingsQuery.data]);

  const handleChange = (key: string, value: string) => {
    setFormValues(prev => ({ ...prev, [key]: value }));
    setHasChanges(true);
  };

  const handleSave = () => {
    updateMutation.mutate(formValues);
  };

  const settingsGroups = [
    {
      title: 'Automatic Response Settings',
      description: 'Control when responses are sent automatically without manual approval',
      settings: [
        {
          key: 'auto_acknowledge',
          label: 'Auto-Acknowledge Receipt',
          description: 'Automatically send acknowledgment when statement is received',
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
          description: 'Send response immediately even if there are queries (otherwise requires approval)',
          icon: AlertTriangle,
          type: 'toggle',
        },
        {
          key: 'require_approval_above',
          label: 'Require Approval Above',
          description: 'Require manual approval for responses with variance above this amount',
          icon: Banknote,
          type: 'number',
          prefix: '£',
        },
      ],
    },
    {
      title: 'Timing Settings',
      description: 'Configure delays and SLAs for statement processing',
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
          description: 'Days before sending follow-up reminder',
          icon: Calendar,
          type: 'number',
          suffix: 'days',
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
          description: 'Amount (£) above which discrepancies require manual review',
          icon: Banknote,
          type: 'number',
          prefix: '£',
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
          description: 'Only notify payments made within this many days',
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
  ];

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-slate-100 rounded-lg">
            <Settings className="h-6 w-6 text-slate-600" />
          </div>
          <div>
            <h1 className="text-2xl font-bold text-slate-900">Automation Settings</h1>
            <p className="text-sm text-slate-500">Configure supplier statement automation behavior</p>
          </div>
        </div>
        <div className="flex items-center gap-3">
          <button
            onClick={() => settingsQuery.refetch()}
            disabled={settingsQuery.isFetching}
            className="flex items-center gap-2 px-4 py-2 bg-slate-100 hover:bg-slate-200 rounded-lg transition-colors"
          >
            <RefreshCw className={`h-4 w-4 ${settingsQuery.isFetching ? 'animate-spin' : ''}`} />
            Refresh
          </button>
          <button
            onClick={handleSave}
            disabled={!hasChanges || updateMutation.isPending}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg font-medium transition-colors ${
              hasChanges
                ? 'bg-indigo-600 text-white hover:bg-indigo-700'
                : 'bg-slate-100 text-slate-400 cursor-not-allowed'
            }`}
          >
            <Save className="h-4 w-4" />
            {updateMutation.isPending ? 'Saving...' : 'Save Changes'}
          </button>
        </div>
      </div>

      {/* Loading State */}
      {settingsQuery.isLoading && (
        <div className="flex items-center justify-center py-12">
          <RefreshCw className="h-8 w-8 text-slate-400 animate-spin" />
        </div>
      )}

      {/* Success Message */}
      {updateMutation.isSuccess && (
        <div className="bg-emerald-50 border border-emerald-200 rounded-lg p-4 text-emerald-700">
          Settings saved successfully
        </div>
      )}

      {/* Settings Form - Grouped */}
      {!settingsQuery.isLoading && (
        <div className="space-y-8">
          {settingsGroups.map((group) => (
            <div key={group.title} className="space-y-4">
              <div>
                <h2 className="text-lg font-semibold text-slate-900">{group.title}</h2>
                <p className="text-sm text-slate-500">{group.description}</p>
              </div>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {group.settings.map((setting) => {
                  const Icon = setting.icon;
                  const currentValue = formValues[setting.key] || '';
                  const isToggle = setting.type === 'toggle';
                  const isChecked = currentValue === 'true' || currentValue === '1';

                  return (
                    <div
                      key={setting.key}
                      className="bg-white rounded-xl shadow-sm border border-slate-200 p-5"
                    >
                      <div className="flex items-start gap-3">
                        <div className="p-2 bg-slate-100 rounded-lg">
                          <Icon className="h-5 w-5 text-slate-600" />
                        </div>
                        <div className="flex-1">
                          <div className="flex items-center justify-between">
                            <label className="block font-medium text-slate-900 mb-1">
                              {setting.label}
                            </label>
                            {isToggle && (
                              <button
                                onClick={() => handleChange(setting.key, isChecked ? 'false' : 'true')}
                                className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors ${
                                  isChecked ? 'bg-indigo-600' : 'bg-slate-200'
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
                          <p className="text-sm text-slate-500 mb-3">
                            {setting.description}
                          </p>
                          {!isToggle && (
                            <div className="flex items-center gap-2">
                              {setting.prefix && (
                                <span className="text-slate-500">{setting.prefix}</span>
                              )}
                              <input
                                type={setting.type === 'number' ? 'number' : 'text'}
                                value={currentValue}
                                onChange={(e) => handleChange(setting.key, e.target.value)}
                                placeholder={setting.placeholder}
                                className="flex-1 px-3 py-2 border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-500"
                              />
                              {setting.suffix && (
                                <span className="text-slate-500">{setting.suffix}</span>
                              )}
                            </div>
                          )}
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Help Text */}
      <div className="bg-blue-50 border border-blue-200 rounded-xl p-4">
        <h3 className="font-medium text-blue-900 mb-2">About These Settings</h3>
        <ul className="text-sm text-blue-700 space-y-1">
          <li>• <strong>Acknowledgment Delay:</strong> Set to 0 for immediate acknowledgment, or add delay for batch processing.</li>
          <li>• <strong>Large Discrepancy Threshold:</strong> Transactions above this amount will be flagged for manual review before responding.</li>
          <li>• <strong>Security Alerts:</strong> Bank detail changes will be emailed to these addresses for verification.</li>
        </ul>
      </div>
    </div>
  );
}

export default SupplierSettings;
