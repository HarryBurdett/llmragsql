import { useState, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Settings, Save, Loader2, CheckCircle, FolderOpen } from 'lucide-react';
import { authFetch } from '../api/client';
import { PageHeader, Card, LoadingState, Alert } from '../components/ui';

const API_BASE = 'http://localhost:8000/api';

const PROVIDERS = [
  { key: 'nest', name: 'NEST' },
  { key: 'aviva', name: 'Aviva' },
  { key: 'scottish_widows', name: 'Scottish Widows' },
  { key: 'smart_pension', name: 'Smart Pension (PAPDIS)' },
  { key: 'peoples_pension', name: "People's Pension" },
  { key: 'royal_london', name: 'Royal London' },
  { key: 'standard_life', name: 'Standard Life' },
  { key: 'legal_general', name: 'Legal & General' },
  { key: 'aegon', name: 'Aegon' }
];

export function PayrollSettings() {
  const queryClient = useQueryClient();
  const [provider, setProvider] = useState('');
  const [exportFolder, setExportFolder] = useState('');
  const [saved, setSaved] = useState(false);

  // Fetch current config
  const { data: configData, isLoading } = useQuery({
    queryKey: ['pensionConfig'],
    queryFn: async () => {
      const res = await authFetch(`${API_BASE}/pension/config`);
      return res.json();
    },
  });

  // Set form values from config
  useEffect(() => {
    if (configData) {
      setProvider(configData.pension_provider || '');
      setExportFolder(configData.export_folder || '');
    }
  }, [configData]);

  // Save mutation
  const saveMutation = useMutation({
    mutationFn: async () => {
      const res = await authFetch(`${API_BASE}/pension/config`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          pension_provider: provider,
          pension_export_folder: exportFolder
        })
      });
      return res.json();
    },
    onSuccess: () => {
      setSaved(true);
      queryClient.invalidateQueries({ queryKey: ['pensionConfig'] });
      setTimeout(() => setSaved(false), 3000);
    }
  });

  const handleSave = () => {
    saveMutation.mutate();
  };

  if (isLoading) {
    return <LoadingState message="Loading settings..." />;
  }

  return (
    <div className="max-w-2xl mx-auto space-y-6">
      <PageHeader
        icon={Settings}
        title="Payroll Parameters"
        subtitle={`Configure pension export settings for ${configData?.company_name || 'company'}`}
      />

      <Card title="Pension Export Settings">
        <div className="space-y-6">
          {/* Provider */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Pension Provider
            </label>
            <select
              value={provider}
              onChange={(e) => setProvider(e.target.value)}
              className="w-full p-2 border rounded-lg"
            >
              <option value="">-- Select Provider --</option>
              {PROVIDERS.map((p) => (
                <option key={p.key} value={p.key}>
                  {p.name}
                </option>
              ))}
            </select>
            <p className="text-xs text-gray-500 mt-1">
              The pension provider determines the export file format
            </p>
          </div>

          {/* Export Folder */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2 flex items-center gap-2">
              <FolderOpen className="w-4 h-4" />
              Export Folder
            </label>
            <input
              type="text"
              value={exportFolder}
              onChange={(e) => setExportFolder(e.target.value)}
              className="w-full p-2 border rounded-lg font-mono text-sm"
              placeholder="/path/to/exports"
            />
            <p className="text-xs text-gray-500 mt-1">
              Full path where pension export files will be saved
            </p>
          </div>

          {/* Save Button */}
          <div className="flex items-center gap-4 pt-4 border-t">
            <button
              onClick={handleSave}
              disabled={saveMutation.isPending}
              className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg disabled:opacity-50"
            >
              {saveMutation.isPending ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Save className="w-4 h-4" />
              )}
              Save Settings
            </button>

            {saved && (
              <span className="flex items-center gap-1 text-emerald-600 text-sm">
                <CheckCircle className="w-4 h-4" />
                Saved
              </span>
            )}

            {saveMutation.isError && (
              <Alert variant="error" className="flex-1">Error saving settings</Alert>
            )}
          </div>
        </div>
      </Card>
    </div>
  );
}

export default PayrollSettings;
