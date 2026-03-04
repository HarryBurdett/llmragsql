import { useState, useEffect } from 'react';
import { Save, RefreshCw, CheckCircle, CreditCard, Settings as SettingsIcon } from 'lucide-react';
import { authFetch } from '../api/client';
import { PageHeader, Card } from '../components/ui';

// GoCardless Settings - link to GoCardless Import page (single source of truth)
function GoCardlessSettings() {
  const [apiKeyConfigured, setApiKeyConfigured] = useState(false);
  const [apiKeyHint, setApiKeyHint] = useState('');
  const [dataSource, setDataSource] = useState('');
  const [defaultBankCode, setDefaultBankCode] = useState('');
  const [gcBankCode, setGcBankCode] = useState('');

  useEffect(() => {
    authFetch('/api/gocardless/settings')
      .then(res => res.json())
      .then(data => {
        if (data.success && data.settings) {
          setApiKeyConfigured(data.settings.api_key_configured || false);
          setApiKeyHint(data.settings.api_key_hint || '');
          setDataSource(data.settings.data_source || 'api');
          setDefaultBankCode(data.settings.default_bank_code || '');
          setGcBankCode(data.settings.gocardless_bank_code || '');
        }
      })
      .catch(err => console.error('Failed to load GoCardless settings:', err));
  }, []);

  return (
    <Card title="GoCardless Import Settings" icon={CreditCard}>
      <div className="flex items-center justify-end mb-4">
        <a
          href="/cashbook/gocardless"
          className="btn btn-primary flex items-center gap-2"
        >
          <SettingsIcon className="h-4 w-4" />
          Open GoCardless Settings
        </a>
      </div>

      <p className="text-sm text-gray-600 mb-4">
        GoCardless settings are managed from the GoCardless Import page to keep all configuration in one place.
      </p>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div className="bg-gray-50 p-3 rounded-lg">
          <p className="text-xs text-gray-500 mb-1">API Connection</p>
          <p className={`text-sm font-medium ${apiKeyConfigured ? 'text-green-600' : 'text-gray-400'}`}>
            {apiKeyConfigured ? `Configured (${apiKeyHint})` : 'Not configured'}
          </p>
        </div>
        <div className="bg-gray-50 p-3 rounded-lg">
          <p className="text-xs text-gray-500 mb-1">Data Source</p>
          <p className="text-sm font-medium text-gray-900">
            {dataSource === 'api' ? 'API' : dataSource === 'email' ? 'Email' : '-'}
          </p>
        </div>
        <div className="bg-gray-50 p-3 rounded-lg">
          <p className="text-xs text-gray-500 mb-1">Default Bank</p>
          <p className="text-sm font-medium text-gray-900">{defaultBankCode || '-'}</p>
        </div>
        <div className="bg-gray-50 p-3 rounded-lg">
          <p className="text-xs text-gray-500 mb-1">Control Bank</p>
          <p className="text-sm font-medium text-gray-900">{gcBankCode || 'None'}</p>
        </div>
      </div>
    </Card>
  );
}

// Recurring Entries Settings
function RecurringEntriesSettings() {
  const [mode, setMode] = useState<'process' | 'warn'>('process');
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    authFetch('/api/recurring-entries/config')
      .then(res => res.json())
      .then(data => {
        if (data.success) setMode(data.mode || 'process');
      })
      .catch(err => console.error('Failed to load recurring entries config:', err));
  }, []);

  const handleSave = async () => {
    setSaving(true);
    setSaved(false);
    try {
      const res = await authFetch(`/api/recurring-entries/config?mode=${mode}`, { method: 'PUT' });
      const data = await res.json();
      if (data.success) {
        setSaved(true);
        setTimeout(() => setSaved(false), 2000);
      }
    } catch (err) {
      console.error('Failed to save recurring entries config:', err);
    } finally {
      setSaving(false);
    }
  };

  return (
    <Card title="Recurring Entries" icon={RefreshCw}>
      <div className="flex items-center justify-end mb-4">
        <button onClick={handleSave} disabled={saving} className="btn btn-primary flex items-center gap-2">
          {saved ? <CheckCircle className="h-4 w-4" /> : <Save className="h-4 w-4" />}
          {saving ? 'Saving...' : saved ? 'Saved' : 'Save'}
        </button>
      </div>
      <p className="text-sm text-gray-600 mb-4">
        When processing a bank statement, the system checks for outstanding recurring entries due for the selected bank.
        Recurring entries are created and maintained in Opera — this setting controls how they are handled during import.
      </p>
      <div className="max-w-md">
        <label className="block text-sm font-medium text-gray-700 mb-1">Processing Mode</label>
        <select
          value={mode}
          onChange={e => setMode(e.target.value as 'process' | 'warn')}
          className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm"
        >
          <option value="process">Process directly — post recurring entries before import</option>
          <option value="warn">Warn only — remind to run in Opera</option>
        </select>
        <p className="text-xs text-gray-500 mt-1.5">
          {mode === 'process'
            ? 'Shows a selection dialog before analysis, allowing you to post due recurring entries directly.'
            : 'Shows a warning banner reminding you to process recurring entries in Opera before importing.'}
        </p>
      </div>
    </Card>
  );
}

export default function CashbookOptions() {
  return (
    <div>
      <PageHeader title="Cashbook Options" />
      <div className="space-y-6">
        <GoCardlessSettings />
        <RecurringEntriesSettings />
      </div>
    </div>
  );
}
