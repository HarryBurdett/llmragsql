import { useState, useEffect } from 'react';
import { Save, RefreshCw, CheckCircle, BookOpen } from 'lucide-react';
import { authFetch } from '../api/client';
import { PageHeader, Card } from '../components/ui';

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
      <PageHeader icon={BookOpen} title="Cashbook Options" />
      <div className="space-y-6">
        <RecurringEntriesSettings />
      </div>
    </div>
  );
}
