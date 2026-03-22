import { useState, useEffect } from 'react';
import { Save, RefreshCw, CheckCircle, BookOpen, FolderOpen } from 'lucide-react';
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

// Bank Statement Folder Settings (shared by email and folder import)
function StatementFolderSettings() {
  const [baseFolder, setBaseFolder] = useState('');
  const [archiveFolder, setArchiveFolder] = useState('');
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    authFetch('/api/bank-import/folder-settings')
      .then(res => res.json())
      .then(data => {
        if (data.success) {
          setBaseFolder(data.base_folder || '');
          setArchiveFolder(data.archive_folder || '');
        }
      })
      .catch(err => console.error('Failed to load folder settings:', err));
  }, []);

  const handleSave = async () => {
    setSaving(true);
    setSaved(false);
    setError(null);
    try {
      const res = await authFetch('/api/bank-import/folder-settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          base_folder: baseFolder,
          archive_folder: archiveFolder,
        })
      });
      const data = await res.json();
      if (data.success) {
        setSaved(true);
        setTimeout(() => setSaved(false), 2000);
      } else {
        setError(data.error || 'Failed to save');
      }
    } catch (err) {
      console.error('Failed to save folder settings:', err);
      setError('Failed to save settings');
    } finally {
      setSaving(false);
    }
  };

  return (
    <Card title="Statement Folders" icon={FolderOpen}>
      <div className="flex items-center justify-end mb-4">
        <button onClick={handleSave} disabled={saving} className="btn btn-primary flex items-center gap-2">
          {saved ? <CheckCircle className="h-4 w-4" /> : <Save className="h-4 w-4" />}
          {saving ? 'Saving...' : saved ? 'Saved' : 'Save'}
        </button>
      </div>
      <p className="text-sm text-gray-600 mb-4">
        Define where bank statement PDFs are stored. This applies to both email downloads and folder imports.
        Bank-specific subfolders are created automatically based on the Opera bank account
        (e.g. <code className="text-xs bg-gray-100 px-1 rounded">BB010-barclays-current</code>).
      </p>

      {error && (
        <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded-lg flex items-start gap-2">
          <span className="text-red-500">&#9888;</span>
          <p className="text-sm text-red-800 flex-1">{error}</p>
          <button onClick={() => setError(null)} className="text-red-400 hover:text-red-600">&times;</button>
        </div>
      )}

      <div className="space-y-4 max-w-lg">
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Base Folder</label>
          <input
            type="text"
            value={baseFolder}
            onChange={e => setBaseFolder(e.target.value)}
            placeholder="/path/to/bank-statements"
            className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm"
          />
          <p className="text-xs text-gray-500 mt-1">
            Root folder for bank statement PDFs. Bank-specific subfolders are created inside this automatically.
          </p>
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Archive Folder</label>
          <input
            type="text"
            value={archiveFolder}
            onChange={e => setArchiveFolder(e.target.value)}
            placeholder="/path/to/bank-statements/archive"
            className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm"
          />
          <p className="text-xs text-gray-500 mt-1">
            Where imported statements are moved after successful processing.
            Organised into bank and year-month subfolders automatically.
          </p>
        </div>
      </div>
    </Card>
  );
}

export default function CashbookOptions() {
  return (
    <div>
      <PageHeader icon={BookOpen} title="Bank Rec Settings" />
      <div className="space-y-6">
        <StatementFolderSettings />
        <RecurringEntriesSettings />
      </div>
    </div>
  );
}
