import { useState, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  Monitor, Plus, Pencil, Trash2, Star, CheckCircle, AlertCircle, X,
  Server, Save, ChevronDown, ChevronRight, Database, TestTube
} from 'lucide-react';
import apiClient from '../api/client';
import type { SystemProfile, DatabaseConfig, OperaConfig } from '../api/client';
import { PageHeader, Card } from '../components/ui';

interface SystemFormState {
  // Database
  dbType: string;
  dbServer: string;
  dbPort: string;
  dbDatabase: string;
  dbUsername: string;
  dbPassword: string;
  useWindowsAuth: boolean;
  ssl: boolean;
  // Opera
  operaVersion: 'sql_se' | 'opera3';
  opera3ServerPath: string;
  opera3BasePath: string;
  opera3CompanyCode: string;
  opera3ShareUser: string;
  opera3SharePassword: string;
  opera3AgentUrl: string;
  opera3AgentKey: string;
}

function systemToForm(sys: SystemProfile): SystemFormState {
  return {
    dbType: sys.database?.type || 'mssql',
    dbServer: sys.database?.server || '',
    dbPort: sys.database?.port || '1433',
    dbDatabase: sys.database?.database || '',
    dbUsername: sys.database?.username || '',
    dbPassword: sys.database?.password || '',
    useWindowsAuth: sys.database?.use_windows_auth === 'True',
    ssl: sys.database?.ssl === 'true',
    operaVersion: (sys.opera?.version as 'sql_se' | 'opera3') || 'sql_se',
    opera3ServerPath: sys.opera?.opera3_server_path || '',
    opera3BasePath: sys.opera?.opera3_base_path || '',
    opera3ShareUser: sys.opera?.opera3_share_user || '',
    opera3SharePassword: sys.opera?.opera3_share_password || '',
    opera3CompanyCode: sys.opera?.opera3_company_code || '',
    opera3AgentUrl: sys.opera?.opera3_agent_url || '',
    opera3AgentKey: sys.opera?.opera3_agent_key || '',
  };
}

function formToSystemData(form: SystemFormState) {
  return {
    database: {
      type: form.dbType,
      server: form.dbServer,
      port: form.dbPort,
      database: form.dbDatabase,
      username: form.dbUsername,
      password: form.dbPassword,
      use_windows_auth: form.useWindowsAuth ? 'True' : 'False',
      ssl: form.ssl ? 'true' : 'false',
      trust_server_certificate: 'true',
      pool_size: '5',
      max_overflow: '10',
      pool_timeout: '30',
      connection_timeout: '30',
      command_timeout: '60',
    },
    opera: {
      version: form.operaVersion,
      opera3_server_path: form.opera3ServerPath,
      opera3_base_path: form.opera3BasePath,
      opera3_company_code: form.opera3CompanyCode,
      opera3_share_user: form.opera3ShareUser,
      opera3_share_password: form.opera3SharePassword,
      opera3_agent_url: form.opera3AgentUrl,
      opera3_agent_key: form.opera3AgentKey,
    },
  };
}

export function Installations() {
  const queryClient = useQueryClient();
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [editingNameId, setEditingNameId] = useState<string | null>(null);
  const [editName, setEditName] = useState('');
  const [newSystemName, setNewSystemName] = useState('');
  const [showAddSystem, setShowAddSystem] = useState(false);
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null);
  const [form, setForm] = useState<SystemFormState | null>(null);

  const { data: systemsData, refetch } = useQuery({
    queryKey: ['systems'],
    queryFn: () => apiClient.getSystems(),
  });

  const systems = systemsData?.data?.systems || [];
  const activeSystemId = systemsData?.data?.active_system_id;

  // Auto-expand the active (or default) installation on first load
  useEffect(() => {
    if (systems.length > 0 && expandedId === null) {
      const active = systems.find(s => s.id === activeSystemId);
      const dflt = systems.find(s => s.is_default);
      const pick = active || dflt || systems[0];
      if (pick) setExpandedId(pick.id);
    }
  }, [systems.length, activeSystemId]); // eslint-disable-line react-hooks/exhaustive-deps

  // Load form when expanding an installation
  useEffect(() => {
    if (expandedId) {
      const sys = systems.find(s => s.id === expandedId);
      if (sys) setForm(systemToForm(sys));
    } else {
      setForm(null);
    }
  }, [expandedId, systems.length]); // eslint-disable-line react-hooks/exhaustive-deps

  const invalidate = () => {
    refetch();
    queryClient.invalidateQueries({ queryKey: ['activeSystem'] });
  };

  // Mutations for saving via the existing config endpoints (applies to active system)
  const dbMutation = useMutation({
    mutationFn: (data: DatabaseConfig) => apiClient.updateDatabaseConfig(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['config'] });
      invalidate();
    },
  });

  const operaMutation = useMutation({
    mutationFn: (data: OperaConfig) => apiClient.updateOperaConfig(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['operaConfig'] });
      invalidate();
    },
  });

  const operaTestMutation = useMutation({
    mutationFn: (data: OperaConfig) => apiClient.testOperaConnection(data),
  });

  // Opera 3 companies detected automatically from data path at login

  const handleAdd = async () => {
    if (!newSystemName.trim()) return;
    try {
      const activeSystem = systems.find(s => s.id === activeSystemId);
      const response = await apiClient.createSystem({
        name: newSystemName.trim(),
        database: activeSystem?.database || {},
        opera: activeSystem?.opera || {},
        is_default: systems.length === 0,
      });
      if (response.data.success) {
        const newId = response.data.system?.id;
        setNewSystemName('');
        setShowAddSystem(false);
        setMessage({ type: 'success', text: `"${newSystemName.trim()}" created` });
        invalidate();
        // Auto-expand the new installation for editing
        if (newId) setTimeout(() => setExpandedId(newId), 100);
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
        if (expandedId === sys.id) setExpandedId(null);
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
      setEditingNameId(null);
      return;
    }
    try {
      await apiClient.updateSystem(sys.id, {
        name: editName.trim(),
        database: sys.database,
        opera: sys.opera,
        is_default: sys.is_default,
      });
      setEditingNameId(null);
      setMessage({ type: 'success', text: 'Renamed' });
      invalidate();
    } catch {
      setMessage({ type: 'error', text: 'Failed to rename' });
    }
  };

  const handleActivate = async (sys: SystemProfile) => {
    try {
      const proceed = window.confirm(
        `Switch to "${sys.name}"?\n\nYou will be logged out and need to log in again to ensure the correct data is loaded.`
      );
      if (!proceed) return;

      const response = await apiClient.activateSystem(sys.id);
      if (response.data.success) {
        // Installation switch — force logout and fresh login for clean state
        sessionStorage.clear();
        localStorage.removeItem('auth_token');
        localStorage.removeItem('auth_user');
        localStorage.removeItem('auth_permissions');
        queryClient.clear();
        window.location.href = '/login';
        return;
      } else {
        setMessage({ type: 'error', text: response.data.error || response.data.detail || 'Failed to switch installation' });
      }
    } catch (err: any) {
      const detail = err?.response?.data?.error || err?.response?.data?.detail || 'Failed to switch installation';
      setMessage({ type: 'error', text: detail });
    }
  };

  const handleSaveSettings = async (sys: SystemProfile) => {
    if (!form) return;

    const isActive = sys.id === activeSystemId;
    const data = formToSystemData(form);

    if (isActive) {
      // For active installation, use the config endpoints which also reinitialise the connector
      try {
        await apiClient.updateDatabaseConfig({
          type: form.dbType,
          server: form.dbServer,
          port: form.dbPort ? parseInt(form.dbPort) : undefined,
          database: form.dbDatabase,
          username: form.dbUsername,
          password: form.dbPassword,
          use_windows_auth: form.useWindowsAuth,
          ssl: form.ssl,
        });
        await apiClient.updateOperaConfig({
          version: form.operaVersion,
          opera3_server_path: form.opera3ServerPath,
          opera3_base_path: form.opera3BasePath,
          opera3_company_code: form.opera3CompanyCode,
          opera3_share_user: form.opera3ShareUser,
          opera3_share_password: form.opera3SharePassword,
          opera3_agent_url: form.opera3AgentUrl,
          opera3_agent_key: form.opera3AgentKey,
        });
        // If Opera version changed on the active installation, force re-login
        const previousVersion = sys.opera?.version;
        if (previousVersion && previousVersion !== form.operaVersion) {
          sessionStorage.clear();
          localStorage.removeItem('auth_token');
          localStorage.removeItem('auth_user');
          localStorage.removeItem('auth_permissions');
          queryClient.clear();
          alert(`Opera version changed from ${previousVersion} to ${form.operaVersion}. You will be redirected to login.`);
          window.location.href = '/login';
          return;
        }
        setMessage({ type: 'success', text: `"${sys.name}" settings saved` });
        invalidate();
        queryClient.invalidateQueries({ queryKey: ['config'] });
        queryClient.invalidateQueries({ queryKey: ['operaConfig'] });
      } catch {
        setMessage({ type: 'error', text: 'Failed to save settings' });
      }
    } else {
      // For non-active installations, just update the systems.json entry
      try {
        await apiClient.updateSystem(sys.id, {
          name: sys.name,
          ...data,
          is_default: sys.is_default,
        });
        setMessage({ type: 'success', text: `"${sys.name}" settings saved` });
        invalidate();
      } catch {
        setMessage({ type: 'error', text: 'Failed to save settings' });
      }
    }
  };

  const handleTestConnection = () => {
    if (!form) return;
    operaTestMutation.mutate({
      version: form.operaVersion,
      opera3_server_path: form.opera3ServerPath,
      opera3_base_path: form.opera3BasePath,
      opera3_company_code: form.opera3CompanyCode,
      opera3_share_user: form.opera3ShareUser,
      opera3_share_password: form.opera3SharePassword,
      opera3_agent_url: form.opera3AgentUrl,
      opera3_agent_key: form.opera3AgentKey,
    });
  };

  const updateForm = (updates: Partial<SystemFormState>) => {
    if (form) setForm({ ...form, ...updates });
  };

  return (
    <div className="space-y-6">
      <PageHeader
        icon={Monitor}
        title="Installations"
        subtitle="Manage the Opera installations this system can connect to"
      />

      {/* Installation list */}
      {systems.length === 0 ? (
        <Card>
          <p className="text-sm text-gray-500 py-4 text-center">No installations configured.</p>
        </Card>
      ) : (
        <div className="space-y-3">
          {systems.map((sys: SystemProfile) => {
            const isActive = sys.id === activeSystemId;
            const isExpanded = expandedId === sys.id;
            const isEditingName = editingNameId === sys.id;

            return (
              <Card key={sys.id} className={isActive ? 'ring-2 ring-blue-200' : ''}>
                {/* Header row */}
                <div className="flex items-center justify-between">
                  <div
                    className="flex items-center gap-3 min-w-0 flex-1 cursor-pointer"
                    onClick={() => setExpandedId(isExpanded ? null : sys.id)}
                  >
                    {isExpanded
                      ? <ChevronDown className="h-4 w-4 text-gray-400 flex-shrink-0" />
                      : <ChevronRight className="h-4 w-4 text-gray-400 flex-shrink-0" />
                    }
                    <span
                      className={`w-2.5 h-2.5 rounded-full flex-shrink-0 ${isActive ? 'bg-green-500' : 'bg-gray-300'}`}
                    />
                    {isEditingName ? (
                      <input
                        type="text"
                        className="input py-1 px-2 text-sm w-56"
                        value={editName}
                        onChange={(e) => setEditName(e.target.value)}
                        autoFocus
                        onClick={(e) => e.stopPropagation()}
                        onBlur={() => handleRename(sys)}
                        onKeyDown={(e) => {
                          if (e.key === 'Enter') handleRename(sys);
                          if (e.key === 'Escape') setEditingNameId(null);
                        }}
                      />
                    ) : (
                      <div className="min-w-0">
                        <div className="flex items-center gap-2">
                          <span className="text-sm font-semibold text-gray-900">{sys.name}</span>
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
                      onClick={() => { setEditingNameId(sys.id); setEditName(sys.name); }}
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

                {/* Expanded settings panel */}
                {isExpanded && form && (
                  <div className="mt-4 pt-4 border-t border-gray-100 space-y-5">
                    {/* Opera Version */}
                    <div>
                      <label className="label">Opera Version</label>
                      <select
                        className="select"
                        value={form.operaVersion}
                        onChange={(e) => updateForm({ operaVersion: e.target.value as 'sql_se' | 'opera3' })}
                      >
                        <option value="sql_se">Opera SQL SE (SQL Server)</option>
                        <option value="opera3">Opera 3 (FoxPro/DBF)</option>
                      </select>
                    </div>

                    {/* SQL SE Database Settings */}
                    {form.operaVersion === 'sql_se' && (
                      <div className="space-y-3">
                        <h4 className="text-sm font-medium text-gray-700 flex items-center gap-1.5">
                          <Database className="h-4 w-4 text-gray-400" />
                          SQL Server Connection
                        </h4>
                        <div className="grid grid-cols-3 gap-3">
                          <div className="col-span-2">
                            <label className="label">Server</label>
                            <input
                              type="text"
                              className="input"
                              placeholder="IP address or hostname"
                              value={form.dbServer}
                              onChange={(e) => updateForm({ dbServer: e.target.value })}
                            />
                          </div>
                          <div>
                            <label className="label">Port</label>
                            <input
                              type="text"
                              className="input"
                              placeholder="1433"
                              value={form.dbPort}
                              onChange={(e) => updateForm({ dbPort: e.target.value })}
                            />
                          </div>
                        </div>
                        <div>
                          <label className="label">Database Name</label>
                          <input
                            type="text"
                            className="input"
                            placeholder="Opera3SECompany00X"
                            value={form.dbDatabase}
                            onChange={(e) => updateForm({ dbDatabase: e.target.value })}
                          />
                        </div>
                        <div className="flex items-center">
                          <input
                            type="checkbox"
                            id={`winAuth-${sys.id}`}
                            checked={form.useWindowsAuth}
                            onChange={(e) => updateForm({ useWindowsAuth: e.target.checked })}
                            className="mr-2"
                          />
                          <label htmlFor={`winAuth-${sys.id}`} className="text-sm text-gray-700">
                            Use Windows Authentication
                          </label>
                        </div>
                        {!form.useWindowsAuth && (
                          <div className="grid grid-cols-2 gap-3">
                            <div>
                              <label className="label">Username</label>
                              <input
                                type="text"
                                className="input"
                                placeholder="Username"
                                value={form.dbUsername}
                                onChange={(e) => updateForm({ dbUsername: e.target.value })}
                              />
                            </div>
                            <div>
                              <label className="label">Password</label>
                              <input
                                type="password"
                                className="input"
                                placeholder="Password"
                                value={form.dbPassword}
                                onChange={(e) => updateForm({ dbPassword: e.target.value })}
                              />
                            </div>
                          </div>
                        )}
                        <div className="flex items-center">
                          <input
                            type="checkbox"
                            id={`ssl-${sys.id}`}
                            checked={form.ssl}
                            onChange={(e) => updateForm({ ssl: e.target.checked })}
                            className="mr-2"
                          />
                          <label htmlFor={`ssl-${sys.id}`} className="text-sm text-gray-700">
                            Use SSL/TLS
                          </label>
                        </div>
                      </div>
                    )}

                    {/* Opera 3 Settings */}
                    {form.operaVersion === 'opera3' && (
                      <div className="space-y-3">
                        <h4 className="text-sm font-medium text-gray-700 flex items-center gap-1.5">
                          <Server className="h-4 w-4 text-gray-400" />
                          Opera 3 Connection
                        </h4>
                        <div>
                          <label className="label">Server Path</label>
                          <input
                            type="text"
                            className="input"
                            placeholder="\\\\SERVER\\O3 Server VFP"
                            value={form.opera3ServerPath}
                            onChange={(e) => updateForm({ opera3ServerPath: e.target.value })}
                          />
                          <p className="text-xs text-gray-500 mt-1">UNC or network path to the Opera 3 server</p>
                        </div>
                        <div className="grid grid-cols-2 gap-3">
                          <div>
                            <label className="label">Share Username</label>
                            <input
                              type="text"
                              className="input"
                              placeholder="domain\\username"
                              value={form.opera3ShareUser || ''}
                              onChange={(e) => updateForm({ opera3ShareUser: e.target.value })}
                            />
                          </div>
                          <div>
                            <label className="label">Share Password</label>
                            <input
                              type="password"
                              className="input"
                              placeholder="Password"
                              value={form.opera3SharePassword || ''}
                              onChange={(e) => updateForm({ opera3SharePassword: e.target.value })}
                            />
                          </div>
                        </div>
                        <div>
                          <label className="label">Local Data Path</label>
                          <input
                            type="text"
                            className="input"
                            placeholder="C:\Apps\O3 Server VFP"
                            value={form.opera3BasePath}
                            onChange={(e) => updateForm({ opera3BasePath: e.target.value })}
                          />
                        </div>
                        <p className="text-xs text-gray-500">Companies are detected automatically from the data path and available at login.</p>

                            {/* Write Agent */}
                            <div className="space-y-3 mt-4 pt-4 border-t border-gray-100">
                              <h4 className="text-sm font-medium text-gray-700">Write Agent</h4>
                              <p className="text-xs text-gray-500">
                                The Write Agent runs on the Opera 3 server to safely post transactions. Without it, Opera 3 is read-only.
                              </p>
                              <div className="grid grid-cols-2 gap-3">
                                <div>
                                  <label className="label">Write Agent URL</label>
                                  <input
                                    type="text"
                                    className="input"
                                    placeholder="http://172.17.172.214:9000"
                                    value={form.opera3AgentUrl}
                                    onChange={(e) => updateForm({ opera3AgentUrl: e.target.value })}
                                  />
                                </div>
                                <div>
                                  <label className="label">Write Agent Key</label>
                                  <input
                                    type="password"
                                    className="input"
                                    placeholder="Shared secret from installer"
                                    value={form.opera3AgentKey}
                                    onChange={(e) => updateForm({ opera3AgentKey: e.target.value })}
                                  />
                                </div>
                              </div>

                              <div className="flex items-center gap-3">
                                <button
                                  onClick={async () => {
                                    try {
                                      const res = await apiClient.testWriteAgent({
                                        version: form.operaVersion,
                                        opera3_agent_url: form.opera3AgentUrl,
                                        opera3_agent_key: form.opera3AgentKey,
                                      });
                                      if (res.data.success) {
                                        setMessage({ type: 'success', text: res.data.message || 'Write Agent connected' });
                                      } else {
                                        setMessage({ type: 'error', text: res.data.error || 'Write Agent test failed' });
                                      }
                                    } catch {
                                      setMessage({ type: 'error', text: 'Failed to test Write Agent connection' });
                                    }
                                  }}
                                  disabled={!form.opera3AgentUrl}
                                  className="px-3 py-1.5 text-sm bg-blue-50 text-blue-700 rounded hover:bg-blue-100 disabled:opacity-50"
                                >
                                  Test Connection
                                </button>
                              </div>

                              <details className="mt-2">
                                <summary className="text-sm font-medium text-blue-600 cursor-pointer hover:text-blue-800">
                                  Setup Instructions
                                </summary>
                                <div className="mt-2 p-3 bg-blue-50 rounded text-sm text-gray-700 space-y-2">
                                  <p><strong>The Write Agent</strong> is a service that runs on the Opera 3 server to safely post transactions to the FoxPro database. It must be installed on the same server as the Opera 3 data files.</p>
                                  <p className="font-medium">Setup:</p>
                                  <ol className="list-decimal list-inside space-y-1 ml-2">
                                    <li>Copy the <code className="bg-white px-1 rounded">opera3-write-agent</code> folder to the Opera 3 server</li>
                                    <li>Run <code className="bg-white px-1 rounded">install.bat</code> as Administrator</li>
                                    <li>The installer displays the URL and Key</li>
                                    <li>Enter the URL and Key in the fields above</li>
                                    <li>Click <strong>Test Connection</strong> to verify</li>
                                  </ol>
                                  <div className="mt-2 pt-2 border-t border-blue-100">
                                    <p className="text-xs text-gray-500">
                                      <strong>Without the Write Agent:</strong> Opera 3 is read-only in this application.
                                      Bank statement viewing, reporting, and analysis work without it.
                                      Posting transactions, importing, and reconciliation require the Write Agent.
                                    </p>
                                  </div>
                                  <div className="mt-1">
                                    <p className="text-xs text-gray-500">
                                      <strong>How it works:</strong> The Write Agent uses proper FoxPro file locking (record-level)
                                      to safely write transactions without interfering with Opera 3 users.
                                      It includes crash recovery, post-write verification, and an audit trail.
                                    </p>
                                  </div>
                                </div>
                              </details>
                            </div>
                      </div>
                    )}

                    {/* Actions */}
                    <div className="flex items-center gap-3 pt-2 border-t border-gray-100">
                      <button
                        onClick={() => handleSaveSettings(sys)}
                        disabled={dbMutation.isPending || operaMutation.isPending}
                        className="btn btn-primary flex items-center"
                      >
                        <Save className="h-4 w-4 mr-2" />
                        {dbMutation.isPending || operaMutation.isPending ? 'Saving...' : 'Save'}
                      </button>
                      {isActive && (
                        <button
                          onClick={handleTestConnection}
                          disabled={operaTestMutation.isPending}
                          className="btn btn-secondary flex items-center"
                        >
                          <TestTube className="h-4 w-4 mr-2" />
                          {operaTestMutation.isPending ? 'Testing...' : 'Test Connection'}
                        </button>
                      )}
                      {!isActive && (
                        <p className="text-xs text-gray-400">Connect to this installation first to test the connection.</p>
                      )}
                    </div>

                    {/* Test result */}
                    {operaTestMutation.isSuccess && isActive && (
                      <div className={`p-3 rounded-md text-sm ${
                        operaTestMutation.data?.data?.success
                          ? 'bg-green-50 text-green-800'
                          : 'bg-red-50 text-red-800'
                      }`}>
                        {operaTestMutation.data?.data?.success ? (
                          <div className="flex items-center">
                            <CheckCircle className="h-4 w-4 mr-2" />
                            {operaTestMutation.data?.data?.message}
                          </div>
                        ) : (
                          <div className="flex items-center">
                            <AlertCircle className="h-4 w-4 mr-2" />
                            {operaTestMutation.data?.data?.error || 'Connection test failed'}
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                )}
              </Card>
            );
          })}
        </div>
      )}

      {/* Add new / Messages */}
      <div className="space-y-3">
        {showAddSystem ? (
          <div className="flex items-center gap-2">
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
          <button
            onClick={() => setShowAddSystem(true)}
            className="flex items-center gap-1.5 text-sm text-blue-600 hover:text-blue-700 font-medium"
          >
            <Plus className="h-3.5 w-3.5" />
            Add Installation
          </button>
        )}

        {message && (
          <div className={`flex items-center gap-2 text-sm ${message.type === 'success' ? 'text-green-600' : 'text-red-600'}`}>
            {message.type === 'success' ? <CheckCircle className="h-4 w-4" /> : <AlertCircle className="h-4 w-4" />}
            <span>{message.text}</span>
            <button onClick={() => setMessage(null)} className="ml-auto text-gray-400 hover:text-gray-600">
              <X className="h-3.5 w-3.5" />
            </button>
          </div>
        )}
      </div>
    </div>
  );
}

export default Installations;
