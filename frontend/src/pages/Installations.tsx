import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { Monitor, Plus, Pencil, Trash2, Star, CheckCircle, AlertCircle, X, Server, ArrowRight } from 'lucide-react';
import apiClient from '../api/client';
import type { SystemProfile } from '../api/client';
import { PageHeader, Card } from '../components/ui';

export function Installations() {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const [editingSystem, setEditingSystem] = useState<SystemProfile | null>(null);
  const [editName, setEditName] = useState('');
  const [newSystemName, setNewSystemName] = useState('');
  const [showAddSystem, setShowAddSystem] = useState(false);
  const [showSettingsPrompt, setShowSettingsPrompt] = useState<string | null>(null);
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null);

  const { data: systemsData, refetch } = useQuery({
    queryKey: ['systems'],
    queryFn: () => apiClient.getSystems(),
  });

  const systems = systemsData?.data?.systems || [];
  const activeSystemId = systemsData?.data?.active_system_id;

  const invalidate = () => {
    refetch();
    queryClient.invalidateQueries({ queryKey: ['activeSystem'] });
  };

  const handleAdd = async () => {
    if (!newSystemName.trim()) return;
    try {
      // Clone settings from the active installation so the new one has working defaults
      const activeSystem = systems.find(s => s.id === activeSystemId);
      const response = await apiClient.createSystem({
        name: newSystemName.trim(),
        database: activeSystem?.database || {},
        opera: activeSystem?.opera || {},
        is_default: systems.length === 0,
      });
      if (response.data.success) {
        setNewSystemName('');
        setShowAddSystem(false);
        setMessage({ type: 'success', text: `"${newSystemName.trim()}" created with current settings. Connect to it, then update via Settings.` });
        invalidate();
      } else {
        setMessage({ type: 'error', text: response.data.error || 'Failed to create' });
      }
    } catch {
      setMessage({ type: 'error', text: 'Failed to create installation' });
    }
  };

  const handleDelete = async (sys: SystemProfile) => {
    try {
      const response = await apiClient.deleteSystem(sys.id);
      if (response.data.success) {
        setMessage({ type: 'success', text: `"${sys.name}" deleted` });
        invalidate();
      } else {
        setMessage({ type: 'error', text: response.data.error || 'Failed to delete' });
      }
    } catch {
      setMessage({ type: 'error', text: 'Failed to delete' });
    }
  };

  const handleSetDefault = async (sys: SystemProfile) => {
    try {
      await apiClient.updateSystem(sys.id, {
        name: sys.name,
        database: sys.database,
        opera: sys.opera,
        is_default: true,
      });
      setMessage({ type: 'success', text: `"${sys.name}" set as default` });
      invalidate();
    } catch {
      setMessage({ type: 'error', text: 'Failed to set default' });
    }
  };

  const handleRename = async (sys: SystemProfile) => {
    if (!editName.trim() || editName.trim() === sys.name) {
      setEditingSystem(null);
      return;
    }
    try {
      await apiClient.updateSystem(sys.id, {
        name: editName.trim(),
        database: sys.database,
        opera: sys.opera,
        is_default: sys.is_default,
      });
      setEditingSystem(null);
      setMessage({ type: 'success', text: 'Renamed' });
      invalidate();
    } catch {
      setMessage({ type: 'error', text: 'Failed to rename' });
    }
  };

  const handleActivate = async (sys: SystemProfile) => {
    try {
      const response = await apiClient.activateSystem(sys.id);
      if (response.data.success) {
        invalidate();
        // Refresh config-dependent queries
        queryClient.invalidateQueries({ queryKey: ['config'] });
        queryClient.invalidateQueries({ queryKey: ['operaConfig'] });
        queryClient.invalidateQueries({ queryKey: ['companies'] });
        // Show prompt to configure settings
        setMessage(null);
        setShowSettingsPrompt(sys.name);
      }
    } catch {
      setMessage({ type: 'error', text: 'Failed to switch installation' });
    }
  };

  return (
    <div className="space-y-6">
      <PageHeader
        icon={Monitor}
        title="Installations"
        subtitle="Manage the Opera installations this system can connect to"
      />

      <Card>
        <div className="space-y-4">
          {/* Installation list */}
          {systems.length === 0 ? (
            <p className="text-sm text-gray-500 py-4 text-center">No installations configured.</p>
          ) : (
            <div className="divide-y divide-gray-100">
              {systems.map((sys: SystemProfile) => {
                const isActive = sys.id === activeSystemId;
                const isEditing = editingSystem?.id === sys.id;

                return (
                  <div key={sys.id} className={`flex items-center justify-between py-3 px-1 ${isActive ? 'bg-blue-50/50 -mx-1 px-2 rounded-lg' : ''}`}>
                    <div className="flex items-center gap-3 min-w-0 flex-1">
                      <span
                        className={`w-2.5 h-2.5 rounded-full flex-shrink-0 ${isActive ? 'bg-green-500' : 'bg-gray-300'}`}
                        title={isActive ? 'Active' : 'Inactive'}
                      />
                      {isEditing ? (
                        <input
                          type="text"
                          className="input py-1 px-2 text-sm w-56"
                          value={editName}
                          onChange={(e) => setEditName(e.target.value)}
                          autoFocus
                          onBlur={() => handleRename(sys)}
                          onKeyDown={(e) => {
                            if (e.key === 'Enter') handleRename(sys);
                            if (e.key === 'Escape') setEditingSystem(null);
                          }}
                        />
                      ) : (
                        <div className="min-w-0">
                          <div className="flex items-center gap-2">
                            <span className="text-sm font-medium text-gray-900">{sys.name}</span>
                            {sys.is_default && (
                              <span className="text-xs text-amber-600 font-medium bg-amber-50 px-1.5 py-0.5 rounded">Default</span>
                            )}
                            {isActive && (
                              <span className="text-xs text-green-700 font-medium bg-green-100 px-1.5 py-0.5 rounded">Active</span>
                            )}
                          </div>
                          <div className="flex items-center gap-2 mt-0.5 text-xs text-gray-400">
                            {sys.opera?.version && (
                              <span>{sys.opera.version === 'sql_se' ? 'Opera SQL SE' : 'Opera 3'}</span>
                            )}
                            {sys.database?.server && (
                              <>
                                <span className="text-gray-300">|</span>
                                <span>{sys.database.server}</span>
                              </>
                            )}
                            {sys.database?.database && (
                              <>
                                <span className="text-gray-300">|</span>
                                <span>{sys.database.database}</span>
                              </>
                            )}
                          </div>
                        </div>
                      )}
                    </div>

                    <div className="flex items-center gap-1 flex-shrink-0 ml-3">
                      {!isActive && (
                        <button
                          onClick={() => handleActivate(sys)}
                          className="px-2.5 py-1 text-xs font-medium text-blue-600 bg-blue-50 rounded-md hover:bg-blue-100 transition-colors"
                        >
                          Connect
                        </button>
                      )}
                      {!sys.is_default && (
                        <button
                          onClick={() => handleSetDefault(sys)}
                          title="Set as default"
                          className="p-1.5 rounded text-gray-400 hover:text-amber-500 hover:bg-amber-50 transition-colors"
                        >
                          <Star className="h-3.5 w-3.5" />
                        </button>
                      )}
                      <button
                        onClick={() => { setEditingSystem(sys); setEditName(sys.name); }}
                        title="Rename"
                        className="p-1.5 rounded text-gray-400 hover:text-blue-600 hover:bg-blue-50 transition-colors"
                      >
                        <Pencil className="h-3.5 w-3.5" />
                      </button>
                      {!isActive && systems.length > 1 && (
                        <button
                          onClick={() => handleDelete(sys)}
                          title="Delete"
                          className="p-1.5 rounded text-gray-400 hover:text-red-600 hover:bg-red-50 transition-colors"
                        >
                          <Trash2 className="h-3.5 w-3.5" />
                        </button>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          )}

          {/* Add new */}
          {showAddSystem ? (
            <div className="flex items-center gap-2 pt-2 border-t border-gray-100">
              <input
                type="text"
                className="input py-1.5 px-3 text-sm flex-1"
                placeholder="Installation name (e.g. Training Server, Opera 3 Live)"
                value={newSystemName}
                onChange={(e) => setNewSystemName(e.target.value)}
                autoFocus
                onKeyDown={(e) => {
                  if (e.key === 'Enter') handleAdd();
                  if (e.key === 'Escape') { setShowAddSystem(false); setNewSystemName(''); }
                }}
              />
              <button
                onClick={handleAdd}
                disabled={!newSystemName.trim()}
                className="px-3 py-1.5 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 disabled:opacity-50 transition-colors"
              >
                Add
              </button>
              <button
                onClick={() => { setShowAddSystem(false); setNewSystemName(''); }}
                className="px-3 py-1.5 text-sm font-medium text-gray-600 bg-gray-100 rounded-lg hover:bg-gray-200 transition-colors"
              >
                Cancel
              </button>
            </div>
          ) : (
            <div className="pt-2 border-t border-gray-100">
              <button
                onClick={() => setShowAddSystem(true)}
                className="flex items-center gap-1.5 text-sm text-blue-600 hover:text-blue-700 font-medium"
              >
                <Plus className="h-3.5 w-3.5" />
                Add Installation
              </button>
            </div>
          )}

          {/* Message */}
          {message && (
            <div className={`flex items-center gap-2 text-sm ${message.type === 'success' ? 'text-green-600' : 'text-red-600'}`}>
              {message.type === 'success' ? <CheckCircle className="h-4 w-4" /> : <AlertCircle className="h-4 w-4" />}
              <span>{message.text}</span>
              <button onClick={() => setMessage(null)} className="ml-auto text-gray-400 hover:text-gray-600">
                <X className="h-3.5 w-3.5" />
              </button>
            </div>
          )}

          {/* Settings prompt after activation */}
          {showSettingsPrompt && (
            <div className="bg-amber-50 border border-amber-200 rounded-lg p-4">
              <div className="flex items-start gap-3">
                <AlertCircle className="h-5 w-5 text-amber-500 mt-0.5 flex-shrink-0" />
                <div className="flex-1">
                  <p className="text-sm font-medium text-amber-800">
                    Connected to "{showSettingsPrompt}"
                  </p>
                  <p className="text-sm text-amber-700 mt-1">
                    Review the database and Opera settings to ensure they point to the correct installation.
                  </p>
                  <div className="flex items-center gap-2 mt-3">
                    <button
                      onClick={() => navigate('/settings')}
                      className="px-3 py-1.5 text-sm font-medium text-white bg-amber-600 rounded-lg hover:bg-amber-700 transition-colors flex items-center gap-1.5"
                    >
                      Go to Settings
                      <ArrowRight className="h-3.5 w-3.5" />
                    </button>
                    <button
                      onClick={() => setShowSettingsPrompt(null)}
                      className="px-3 py-1.5 text-sm font-medium text-amber-700 bg-amber-100 rounded-lg hover:bg-amber-200 transition-colors"
                    >
                      Dismiss
                    </button>
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      </Card>

      {/* Help text */}
      <div className="bg-gray-50 border border-gray-200 rounded-xl p-5">
        <div className="flex items-start gap-3">
          <Server className="h-5 w-5 text-gray-400 mt-0.5 flex-shrink-0" />
          <div className="text-sm text-gray-600 space-y-1.5">
            <p className="font-medium text-gray-700">How installations work</p>
            <p>Each installation points to a different Opera system (e.g. production, training, or a different Opera version).</p>
            <p>Select an installation on the login screen or use the <strong>Connect</strong> button above. The <strong>Settings</strong> page configures the database and Opera connection for whichever installation is currently active.</p>
            <p>The <strong>Default</strong> installation is pre-selected on the login screen.</p>
          </div>
        </div>
      </div>
    </div>
  );
}

export default Installations;
