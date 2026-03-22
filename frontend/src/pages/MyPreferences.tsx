import { useState, useEffect } from 'react';
import { useQuery, useMutation } from '@tanstack/react-query';
import { Save, CheckCircle, Monitor, Mic, Building2, Cog } from 'lucide-react';
import { authFetch } from '../api/client';
import { useAuth } from '../context/AuthContext';
import { PageHeader, Card, Alert } from '../components/ui';

export function MyPreferences() {
  const { user, updateUser } = useAuth();

  const [uiMode, setUiMode] = useState<'classic' | 'launcher'>('classic');
  const [voiceEnabled, setVoiceEnabled] = useState(false);
  const [defaultCompany, setDefaultCompany] = useState('');
  const [defaultSystem, setDefaultSystem] = useState('');
  const [saved, setSaved] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Load current preferences
  const { data: prefs } = useQuery({
    queryKey: ['myPreferences'],
    queryFn: async () => {
      const res = await authFetch('/api/auth/preferences');
      return res.json();
    },
  });

  // Load available companies
  const { data: companiesData } = useQuery({
    queryKey: ['companies'],
    queryFn: async () => {
      const res = await authFetch('/api/companies');
      return res.json();
    },
  });

  // Load available systems
  const { data: systemsData } = useQuery({
    queryKey: ['systems'],
    queryFn: async () => {
      const res = await authFetch('/api/systems');
      return res.json();
    },
  });

  // Populate form when prefs load
  useEffect(() => {
    if (prefs?.success) {
      setUiMode(prefs.ui_mode || 'classic');
      setVoiceEnabled(prefs.voice_enabled || false);
      setDefaultCompany(prefs.default_company || '');
      setDefaultSystem(prefs.default_system || '');
    }
  }, [prefs]);

  const companies = companiesData?.companies || [];
  const systems = systemsData?.systems || systemsData || [];

  const saveMutation = useMutation({
    mutationFn: async (data: Record<string, unknown>) => {
      const res = await authFetch('/api/auth/preferences', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
      });
      return res.json();
    },
    onSuccess: (data) => {
      if (data.success) {
        setSaved(true);
        setError(null);
        setTimeout(() => setSaved(false), 3000);
        // Update AuthContext in-memory (also persists to localStorage)
        updateUser({
          ui_mode: data.ui_mode,
          voice_enabled: data.voice_enabled,
          default_company: data.default_company,
          default_system: data.default_system,
        });
      } else {
        setError(data.error || 'Failed to save preferences');
      }
    },
    onError: () => {
      setError('Failed to connect to server');
    },
  });

  const handleSave = () => {
    saveMutation.mutate({
      ui_mode: uiMode,
      voice_enabled: voiceEnabled,
      default_company: defaultCompany || null,
      default_system: defaultSystem || null,
    });
  };

  return (
    <div className="max-w-2xl mx-auto">
      <PageHeader
        title="My Preferences"
        subtitle={`Logged in as ${user?.display_name || user?.username || 'Unknown'}`}
        icon={Cog}
      />

      <div className="space-y-6">
        {/* Interface */}
        <Card title="Interface" icon={Monitor}>
          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Home Screen</label>
              <div className="flex gap-3">
                <button
                  onClick={() => setUiMode('classic')}
                  className={`flex-1 p-3 rounded-lg border-2 text-center transition-all ${
                    uiMode === 'classic'
                      ? 'border-blue-500 bg-blue-50 text-blue-700'
                      : 'border-gray-200 hover:border-gray-300 text-gray-600'
                  }`}
                >
                  <div className="text-sm font-medium">Classic</div>
                  <div className="text-xs mt-1 opacity-70">Menu navigation</div>
                </button>
                <button
                  onClick={() => setUiMode('launcher')}
                  className={`flex-1 p-3 rounded-lg border-2 text-center transition-all ${
                    uiMode === 'launcher'
                      ? 'border-blue-500 bg-blue-50 text-blue-700'
                      : 'border-gray-200 hover:border-gray-300 text-gray-600'
                  }`}
                >
                  <div className="text-sm font-medium">Launcher</div>
                  <div className="text-xs mt-1 opacity-70">App tiles</div>
                </button>
              </div>
            </div>

            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <Mic className="h-4 w-4 text-gray-500" />
                <div>
                  <div className="text-sm font-medium text-gray-700">Voice Control</div>
                  <div className="text-xs text-gray-500">Speak commands to navigate and enter data</div>
                </div>
              </div>
              <button
                onClick={() => setVoiceEnabled(!voiceEnabled)}
                className={`relative w-11 h-6 rounded-full transition-colors ${voiceEnabled ? 'bg-blue-500' : 'bg-gray-300'}`}
              >
                <span className={`absolute top-0.5 left-0.5 w-5 h-5 bg-white rounded-full shadow transition-transform ${voiceEnabled ? 'translate-x-5' : ''}`} />
              </button>
            </div>
          </div>
        </Card>

        {/* Defaults */}
        <Card title="Defaults" icon={Building2}>
          <div className="space-y-4">
            {companies.length > 0 && (
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Default Company</label>
                <select
                  value={defaultCompany}
                  onChange={(e) => setDefaultCompany(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                >
                  <option value="">No preference (use last active)</option>
                  {companies.map((c: { id: string; name: string }) => (
                    <option key={c.id} value={c.id}>{c.name}</option>
                  ))}
                </select>
                <p className="text-xs text-gray-500 mt-1">Company selected automatically on login</p>
              </div>
            )}

            {Array.isArray(systems) && systems.length > 0 && (
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Default System</label>
                <select
                  value={defaultSystem}
                  onChange={(e) => setDefaultSystem(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                >
                  <option value="">No preference (use last active)</option>
                  {systems.map((s: { id: string; name: string }) => (
                    <option key={s.id} value={s.id}>{s.name}</option>
                  ))}
                </select>
                <p className="text-xs text-gray-500 mt-1">System profile selected automatically on login</p>
              </div>
            )}
          </div>
        </Card>

        {/* Save */}
        <div className="flex items-center gap-3">
          <button
            onClick={handleSave}
            disabled={saveMutation.isPending}
            className="btn btn-primary flex items-center gap-2"
          >
            <Save className="h-4 w-4" />
            {saveMutation.isPending ? 'Saving...' : 'Save Preferences'}
          </button>
          {saved && (
            <span className="text-green-600 text-sm flex items-center gap-1">
              <CheckCircle className="h-4 w-4" />
              Saved
            </span>
          )}
        </div>

        {error && (
          <Alert variant="error" onDismiss={() => setError(null)}>
            {error}
          </Alert>
        )}
      </div>
    </div>
  );
}
