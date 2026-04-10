import { useState, useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  Settings, RefreshCw, Save, Clock, Mail, AlertTriangle,
  Calendar, Banknote, Users, FileText, HelpCircle, Code2
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
  type: 'number' | 'text' | 'email' | 'toggle' | 'date' | 'textarea';
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
        description: 'Days allowed for supplier to respond to a query before it is flagged as overdue',
        icon: Calendar,
        type: 'number',
        suffix: 'days',
      },
      {
        key: 'follow_up_reminder_days',
        label: 'Follow-up Reminder Interval',
        description: 'Days after query sent before each follow-up reminder (must be greater than response deadline)',
        icon: Calendar,
        type: 'number',
        suffix: 'days',
      },
      {
        key: 'max_follow_up_reminders',
        label: 'Maximum Reminders',
        description: 'Number of follow-up reminders before query is escalated for manual action (tone escalates with each)',
        icon: AlertTriangle,
        type: 'number',
        suffix: 'reminders',
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
        key: 'test_mode_email',
        label: 'Test Mode — Redirect All Emails',
        description: 'When set, ALL outbound supplier emails are sent to this address instead of the supplier contact. Clear to disable test mode.',
        icon: AlertTriangle,
        type: 'email',
        placeholder: 'charlieb@intsysuk.com',
      },
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
    title: 'Automation',
    description: 'Control what the system does automatically',
    settings: [
      {
        key: 'auto_process',
        label: 'Auto-Process Statements',
        description: 'Automatically reconcile statements when received',
        icon: RefreshCw,
        type: 'toggle',
      },
    ],
  },
  {
    title: 'Communications',
    description: 'Choose which emails suppliers receive. Suppliers want payment — queries hold it up. Only communicate when they need to act.',
    settings: [
      {
        key: 'send_acknowledgement',
        label: 'Send Receipt Acknowledgement',
        description: 'Confirm receipt of their statement (lets them know it was received and is being processed)',
        icon: Mail,
        type: 'toggle',
      },
      {
        key: 'send_agreed_response',
        label: 'Send Agreed Confirmation',
        description: 'Confirm balance is agreed with payment schedule (reassures them payment is on track)',
        icon: Mail,
        type: 'toggle',
      },
      {
        key: 'send_query_response',
        label: 'Send Query Notification',
        description: 'Notify supplier of items holding up payment — this is the critical communication that gets queries resolved',
        icon: AlertTriangle,
        type: 'toggle',
      },
      {
        key: 'send_follow_up_reminders',
        label: 'Send Follow-up Reminders',
        description: 'Automatically chase unanswered queries with escalating reminders (payment cannot be released until resolved)',
        icon: AlertTriangle,
        type: 'toggle',
      },
      {
        key: 'auto_respond_if_reconciled',
        label: 'Auto-Send (No Approval Required)',
        description: 'Send enabled communications automatically without waiting for manual approval',
        icon: RefreshCw,
        type: 'toggle',
      },
      {
        key: 'require_approval_for_queries',
        label: 'Require Approval for Queries',
        description: 'Hold query notifications for manual review before sending (recommended for large variances)',
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
  {
    title: 'Email Templates',
    description: 'Customise the emails sent to suppliers. Write plain text — the system formats it as a professional email automatically.',
    settings: [
      {
        key: 'email_template_subject_agreed',
        label: 'Subject — Balance Agreed',
        description: 'Subject line when all items match',
        icon: Mail,
        type: 'text',
        placeholder: 'Statement Confirmed — {supplier_name} — {statement_date}',
      },
      {
        key: 'email_template_subject_query',
        label: 'Subject — With Queries',
        description: 'Subject line when there are outstanding queries',
        icon: Mail,
        type: 'text',
        placeholder: 'Statement Response — {supplier_name} — {statement_date}',
      },
      {
        key: 'email_template_agreed',
        label: 'Email Body — Balance Agreed',
        description: 'Sent when all items match. Use {contact_name}, {supplier_name}, {statement_date}, {their_balance}, {payment_schedule}, {company_sign_off}. The system inserts payment details automatically.',
        icon: FileText,
        type: 'textarea',
      },
      {
        key: 'email_template_query',
        label: 'Email Body — With Queries',
        description: 'Sent when items need attention. Use the same fields plus {query_count} and {query_table} (the list of queried items is inserted automatically).',
        icon: FileText,
        type: 'textarea',
      },
      {
        key: 'response_sign_off',
        label: 'Sign-off',
        description: 'Signature block appended to all emails (used as {company_sign_off})',
        icon: FileText,
        type: 'text',
        placeholder: 'Regards, Accounts Department',
      },
      {
        key: 'response_company_name',
        label: 'Company Name',
        description: 'Company name shown in bold below the sign-off',
        icon: FileText,
        type: 'text',
        placeholder: 'Your Company Ltd',
      },
    ],
  },
];

// Sample data for template preview — realistic example so user sees exactly what the email looks like
const PREVIEW_MERGE_DATA: Record<string, string> = {
  contact_name: 'Sarah Johnson',
  supplier_name: 'ABC Office Supplies Ltd',
  statement_date: '31/03/2026',
  their_balance: '£4,287.50',
  our_balance: '£3,945.00',
  difference: '<span style="color:#721c24;font-weight:bold;">£342.50</span>',
  agreed_count: '12',
  query_count: '2',
  query_table: `<table style="border-collapse:collapse;width:100%;margin:12px 0;">
    <tr style="background:#f8f9fa;"><th style="border:1px solid #dee2e6;padding:8px;text-align:left;">Reference</th><th style="border:1px solid #dee2e6;padding:8px;text-align:left;">Query</th><th style="border:1px solid #dee2e6;padding:8px;text-align:right;">Amount</th></tr>
    <tr><td style="border:1px solid #dee2e6;padding:8px;">INV-8842</td><td style="border:1px solid #dee2e6;padding:8px;">Invoice not found in our records</td><td style="border:1px solid #dee2e6;padding:8px;text-align:right;">£210.00</td></tr>
    <tr><td style="border:1px solid #dee2e6;padding:8px;">CN-1205</td><td style="border:1px solid #dee2e6;padding:8px;">Credit note not received</td><td style="border:1px solid #dee2e6;padding:8px;text-align:right;">-£132.50</td></tr>
  </table>`,
  payment_table: `<table style="border-collapse:collapse;width:100%;margin:12px 0;">
    <tr style="background:#f8f9fa;"><th style="border:1px solid #dee2e6;padding:8px;text-align:left;">Date</th><th style="border:1px solid #dee2e6;padding:8px;text-align:left;">Reference</th><th style="border:1px solid #dee2e6;padding:8px;text-align:right;">Amount</th></tr>
    <tr><td style="border:1px solid #dee2e6;padding:8px;">15/03/2026</td><td style="border:1px solid #dee2e6;padding:8px;">BACS PMT</td><td style="border:1px solid #dee2e6;padding:8px;text-align:right;">£2,150.00</td></tr>
  </table>`,
  payment_schedule: '<p style="background:#e8f5e9;padding:10px;border-radius:4px;">Your agreed balance is scheduled for payment on <strong>Friday 18 April 2026</strong>.</p>',
  company_sign_off: 'Regards,<br>Accounts Department<br><b>Intsys UK Ltd</b>',
};

function renderPreview(template: string, mergeData: Record<string, string>): string {
  // Convert plain text to HTML paragraphs (same logic as backend)
  let html = template;
  if (!html.includes('<p>') && !html.includes('<div>') && !html.includes('<br')) {
    const lines = html.trim().split('\n');
    const htmlLines: string[] = [];
    for (const line of lines) {
      const stripped = line.trim();
      if (!stripped) continue;
      if (['{query_table}', '{payment_table}', '{payment_schedule}'].includes(stripped)) {
        htmlLines.push(stripped);
      } else {
        htmlLines.push(`<p>${stripped}</p>`);
      }
    }
    html = htmlLines.join('\n');
  }
  // Replace merge fields
  for (const [field, value] of Object.entries(mergeData)) {
    html = html.replaceAll(`{${field}}`, value);
  }
  return `<div style="font-family:Arial,sans-serif;font-size:14px;color:#333;line-height:1.6;max-width:640px;padding:16px;">${html}</div>`;
}

export default function SupplierSettings() {
  const { showHelp, setShowHelp } = useHelp();
  const [formValues, setFormValues] = useState<Record<string, string>>({});
  const [hasChanges, setHasChanges] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [previewKey, setPreviewKey] = useState<string | null>(null);

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
          {
            title: 'Email Template Merge Fields',
            content: [
              '{contact_name} — First named contact from Opera (falls back to supplier name)',
              '{supplier_name} — Supplier name from Opera',
              '{statement_date} — Date of the supplier statement',
              '{their_balance} — Closing balance on the statement (£X,XXX.XX)',
              '{our_balance} — Outstanding balance per our Purchase Ledger (£X,XXX.XX)',
              '{difference} — Difference between the two balances (coloured green/red)',
              '{agreed_count} — Number of items that matched',
              '{query_count} — Number of items requiring attention',
              '{query_table} — Auto-generated HTML table of queried items (do not edit)',
              '{payment_table} — Auto-generated HTML table of recent payments (do not edit)',
              '{payment_schedule} — Upcoming payment run date text (do not edit)',
              '{company_sign_off} — Sign-off text from the "Response Sign-Off" setting',
            ].join('\n'),
          },
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
                  const isTextarea = setting.type === 'textarea';
                  const isChecked = currentValue === 'true' || currentValue === '1';

                  return (
                    <Card key={setting.key} className={isTextarea ? 'md:col-span-2' : ''}>
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
                              {isTextarea ? (
                                <div className="flex-1 space-y-2">
                                  <textarea
                                    value={currentValue}
                                    onChange={e => handleChange(setting.key, e.target.value)}
                                    rows={8}
                                    placeholder={setting.key === 'email_template_agreed'
                                      ? 'Dear {contact_name},\n\nThank you for your statement dated {statement_date}.\n\nWe confirm the balance of {their_balance} is agreed.\n\n{payment_schedule}\n\n{company_sign_off}'
                                      : setting.key === 'email_template_query'
                                      ? 'Dear {contact_name},\n\nThank you for your statement dated {statement_date}.\n\nPayment will be processed for all agreed items. However, the following items require your attention as they affect the outstanding balance:\n\n{query_table}\n\nPlease respond at your earliest convenience so that we can resolve these items and reconcile our records.\n\n{payment_schedule}\n\n{company_sign_off}'
                                      : ''}
                                    className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent resize-y"
                                  />
                                  <button
                                    type="button"
                                    onClick={() => setPreviewKey(previewKey === setting.key ? null : setting.key)}
                                    className="px-3 py-1 text-xs bg-gray-100 text-gray-700 rounded hover:bg-gray-200 border border-gray-200"
                                  >
                                    {previewKey === setting.key ? 'Hide Preview' : 'Preview'}
                                  </button>
                                  {previewKey === setting.key && (
                                    <div className="border border-gray-300 rounded-lg bg-white shadow-sm">
                                      <div className="px-3 py-1.5 bg-gray-50 border-b border-gray-200 rounded-t-lg">
                                        <span className="text-xs text-gray-500">Email preview with sample data</span>
                                      </div>
                                      <div
                                        className="p-4 overflow-auto max-h-96"
                                        dangerouslySetInnerHTML={{
                                          __html: renderPreview(
                                            currentValue || (setting.key === 'email_template_agreed'
                                              ? 'Dear {contact_name},\n\nThank you for your statement dated {statement_date}.\n\nWe confirm the balance of {their_balance} is agreed.\n\n{payment_schedule}\n\n{company_sign_off}'
                                              : setting.key === 'email_template_query'
                                              ? 'Dear {contact_name},\n\nThank you for your statement dated {statement_date}.\n\nPayment will be processed for all agreed items. However, the following items require your attention as they affect the outstanding balance:\n\n{query_table}\n\nPlease respond at your earliest convenience so that we can resolve these items and reconcile our records.\n\n{payment_schedule}\n\n{company_sign_off}'
                                              : ''),
                                            {
                                              ...PREVIEW_MERGE_DATA,
                                              company_sign_off: (formValues['response_sign_off'] || 'Regards,<br>Accounts Department')
                                                + (formValues['response_company_name'] ? `<br><b>${formValues['response_company_name']}</b>` : ''),
                                            }
                                          )
                                        }}
                                      />
                                    </div>
                                  )}
                                </div>
                              ) : isDate ? (
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
          <li><strong>Timing:</strong> Controls response deadlines and reminder intervals.</li>
          <li><strong>Thresholds:</strong> Large discrepancies are flagged for manual review.</li>
          <li><strong>Communications:</strong> Choose which emails suppliers receive. Each can be toggled independently.</li>
          <li><strong>Email Templates:</strong> Write plain text with merge fields like {'{supplier_name}'} — click Preview to see exactly what the email will look like.</li>
        </ul>
      </Alert>
    </div>
  );
}

export { SupplierSettings };
