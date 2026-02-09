import { useState, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Settings, Save, Loader2, CheckCircle, FolderOpen } from 'lucide-react';

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
      const res = await fetch(`${API_BASE}/pension/config`);
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
      const res = await fetch(`${API_BASE}/pension/config`, {
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
    return (
      <div className="p-6 flex items-center justify-center">
        <Loader2 className="w-6 h-6 animate-spin text-blue-600" />
      </div>
    );
  }

  return (
    <div className="p-6 max-w-2xl mx-auto">
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
          <Settings className="w-6 h-6" />
          Payroll Parameters
        </h1>
        <p className="text-gray-600 mt-1">
          Configure pension export settings for {configData?.company_name || 'company'}
        </p>
      </div>

      <div className="bg-white rounded-lg shadow-sm border p-6 space-y-6">
        <div>
          <h2 className="text-lg font-semibold mb-4">Pension Export Settings</h2>

          {/* Provider */}
          <div className="mb-4">
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
          <div className="mb-4">
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
            <span className="flex items-center gap-1 text-green-600">
              <CheckCircle className="w-4 h-4" />
              Saved
            </span>
          )}

          {saveMutation.isError && (
            <span className="text-red-600">
              Error saving settings
            </span>
          )}
        </div>
      </div>
    </div>
  );
}

export default PayrollSettings;
