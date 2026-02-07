import { useState, useEffect, useCallback } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  Lock,
  Play,
  Square,
  RefreshCw,
  AlertTriangle,
  Clock,
  Users,
  Table,
  X,
  Activity,
  Database,
  FolderOpen
} from 'lucide-react';

const API_BASE = 'http://localhost:8000/api';

type MonitorType = 'sql-server' | 'opera3';
type DataSource = 'opera-sql' | 'opera3';

interface LockEvent {
  blocked_session?: number;
  blocking_session?: number;
  blocked_user?: string;
  blocking_user?: string;
  table_name: string;
  lock_type: string;
  wait_time_ms?: number;
  blocked_query?: string;
  blocking_query?: string;
  timestamp?: string;
  // Opera 3 specific
  file_name?: string;
  process?: string;
  process_id?: number;
  user?: string;
}

interface TableStat {
  table_name: string;
  block_count?: number;
  lock_count?: number;
  total_wait_ms?: number;
  avg_wait_ms?: number;
}

interface UserStat {
  user: string;
  block_count?: number;
  access_count?: number;
  total_wait_ms?: number;
  users_blocked?: number;
  tables_accessed?: number;
}

interface HourlyStat {
  hour: number;
  event_count: number;
  avg_wait_ms?: number;
}

interface LockSummary {
  total_events: number;
  unique_tables?: number;
  unique_files?: number;
  unique_processes?: number;
  total_wait_time_ms?: number;
  avg_wait_time_ms?: number;
  max_wait_time_ms?: number;
  most_blocked_tables?: TableStat[];
  most_locked_files?: TableStat[];
  most_blocking_users?: UserStat[];
  most_active_processes?: UserStat[];
  hourly_distribution: HourlyStat[];
  recent_events: LockEvent[];
}

interface Monitor {
  name: string;
  type: MonitorType;
  is_monitoring: boolean;
  data_path?: string;
}

type ConnectionType = 'sql' | 'foxpro';

interface ConnectionForm {
  description: string;
  connectionType: ConnectionType;
  // SQL Server fields
  server: string;
  port: string;
  database: string;
  username: string;
  password: string;
  useWindowsAuth: boolean;
  // FoxPro fields
  dataPath: string;
}

interface Company {
  code: string;
  name: string;
  data_path?: string;
}

export function LockMonitor() {
  // Get configured data source from settings
  const { data: operaConfigData } = useQuery({
    queryKey: ['opera-config'],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/opera-config`);
      return res.json();
    },
  });
  const dataSource: DataSource = operaConfigData?.version === 'opera3' ? 'opera3' : 'opera-sql';
  const isOpera3Mode = dataSource === 'opera3';

  const [monitors, setMonitors] = useState<Monitor[]>([]);
  const [selectedMonitor, setSelectedMonitor] = useState<Monitor | null>(null);
  const [currentLocks, setCurrentLocks] = useState<LockEvent[]>([]);
  const [summary, setSummary] = useState<LockSummary | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [showConnectForm, setShowConnectForm] = useState(false);
  const [summaryHours, setSummaryHours] = useState(24);
  const [autoRefresh, setAutoRefresh] = useState(false);

  const [connectionForm, setConnectionForm] = useState<ConnectionForm>(() => ({
    description: '',
    connectionType: isOpera3Mode ? 'foxpro' : 'sql',
    // SQL fields from localStorage
    server: localStorage.getItem('lockMonitor_sql_server') || '',
    port: localStorage.getItem('lockMonitor_sql_port') || '1433',
    database: localStorage.getItem('lockMonitor_sql_database') || '',
    username: localStorage.getItem('lockMonitor_sql_username') || '',
    password: '',
    useWindowsAuth: localStorage.getItem('lockMonitor_sql_useWindowsAuth') === 'true',
    // FoxPro fields from localStorage
    dataPath: localStorage.getItem('lockMonitor_foxpro_dataPath') || ''
  }));

  // Company selection state
  const [connectionTested, setConnectionTested] = useState(false);
  const [testingConnection, setTestingConnection] = useState(false);
  const [availableCompanies, setAvailableCompanies] = useState<Company[]>([]);
  const [selectedCompany, setSelectedCompany] = useState<Company | null>(null);

  // Auto-populate FoxPro data path from settings when form opens
  useEffect(() => {
    if (showConnectForm && operaConfigData && connectionForm.connectionType === 'foxpro' && !connectionForm.dataPath) {
      const serverPath = operaConfigData.opera3_server_path;
      const basePath = operaConfigData.opera3_base_path;
      if (serverPath) {
        setConnectionForm(prev => ({ ...prev, dataPath: serverPath }));
      } else if (basePath) {
        setConnectionForm(prev => ({ ...prev, dataPath: basePath }));
      }
    }
  }, [showConnectForm, operaConfigData, connectionForm.connectionType, connectionForm.dataPath]);

  // Reset form when opening
  const openConnectionForm = () => {
    setConnectionForm(prev => ({
      ...prev,
      description: '',
      connectionType: isOpera3Mode ? 'foxpro' : 'sql'
    }));
    setConnectionTested(false);
    setAvailableCompanies([]);
    setSelectedCompany(null);
    setShowConnectForm(true);
    setError(null);
  };

  // Reset company state when connection type or details change
  const resetConnectionTest = () => {
    setConnectionTested(false);
    setAvailableCompanies([]);
    setSelectedCompany(null);
  };

  // Test connection and fetch companies
  const handleTestConnection = async () => {
    setTestingConnection(true);
    setError(null);
    try {
      if (connectionForm.connectionType === 'foxpro') {
        // Test FoxPro connection and list companies
        const params = new URLSearchParams({ base_path: connectionForm.dataPath });
        const res = await fetch(`${API_BASE}/opera3-lock-monitor/list-companies?${params}`, { method: 'POST' });
        const data = await res.json();
        if (data.success && data.companies.length > 0) {
          setAvailableCompanies(data.companies);
          setConnectionTested(true);
        } else {
          setError(data.error || 'No companies found');
        }
      } else {
        // Test SQL Server connection
        const params = new URLSearchParams({
          server: connectionForm.server,
          port: connectionForm.port || '1433',
          database: connectionForm.database
        });
        if (!connectionForm.useWindowsAuth) {
          params.append('username', connectionForm.username);
          params.append('password', connectionForm.password);
        }
        const res = await fetch(`${API_BASE}/lock-monitor/test-connection?${params}`, { method: 'POST' });
        const data = await res.json();
        if (data.success) {
          // For SQL, the database IS the company
          setSelectedCompany(data.company);
          setConnectionTested(true);
        } else {
          setError(data.error || 'Connection failed');
        }
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Connection test failed');
    } finally {
      setTestingConnection(false);
    }
  };

  // Persist connection settings
  useEffect(() => {
    if (connectionForm.server) localStorage.setItem('lockMonitor_sql_server', connectionForm.server);
    if (connectionForm.port) localStorage.setItem('lockMonitor_sql_port', connectionForm.port);
    if (connectionForm.database) localStorage.setItem('lockMonitor_sql_database', connectionForm.database);
    if (connectionForm.username) localStorage.setItem('lockMonitor_sql_username', connectionForm.username);
    localStorage.setItem('lockMonitor_sql_useWindowsAuth', String(connectionForm.useWindowsAuth));
    if (connectionForm.dataPath) localStorage.setItem('lockMonitor_foxpro_dataPath', connectionForm.dataPath);
  }, [connectionForm]);

  // Fetch monitors for the configured data source only
  const fetchMonitors = useCallback(async () => {
    try {
      const apiEndpoint = isOpera3Mode ? 'opera3-lock-monitor' : 'lock-monitor';
      const res = await fetch(`${API_BASE}/${apiEndpoint}/list`);
      const data = await res.json();

      const fetchedMonitors: Monitor[] = [];
      if (data.success) {
        data.monitors.forEach((m: any) => {
          fetchedMonitors.push({
            name: m.name,
            type: isOpera3Mode ? 'opera3' : 'sql-server',
            is_monitoring: m.is_monitoring,
            data_path: m.data_path
          });
        });
      }

      setMonitors(fetchedMonitors);

      if (fetchedMonitors.length > 0 && !selectedMonitor) {
        setSelectedMonitor(fetchedMonitors[0]);
      }
    } catch (err) {
      console.error('Failed to fetch monitors:', err);
    }
  }, [selectedMonitor, isOpera3Mode]);

  const getApiBase = () => {
    return isOpera3Mode ? 'opera3-lock-monitor' : 'lock-monitor';
  };

  const fetchCurrentLocks = useCallback(async () => {
    if (!selectedMonitor) return;
    try {
      const apiBase = getApiBase();
      const res = await fetch(`${API_BASE}/${apiBase}/${selectedMonitor.name}/current`);
      const data = await res.json();
      if (data.success) {
        setCurrentLocks(data.events);
      }
    } catch (err) {
      console.error('Failed to fetch current locks:', err);
    }
  }, [selectedMonitor, isOpera3Mode]);

  const fetchSummary = useCallback(async () => {
    if (!selectedMonitor) return;
    setLoading(true);
    try {
      const apiBase = getApiBase();
      const res = await fetch(`${API_BASE}/${apiBase}/${selectedMonitor.name}/summary?hours=${summaryHours}`);
      const data = await res.json();
      if (data.success) {
        setSummary(data.summary);
        setError(null);
      } else {
        setError(data.error);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch summary');
    } finally {
      setLoading(false);
    }
  }, [selectedMonitor, summaryHours, isOpera3Mode]);

  useEffect(() => {
    fetchMonitors();
  }, [fetchMonitors]);

  useEffect(() => {
    if (selectedMonitor) {
      fetchCurrentLocks();
      fetchSummary();
    }
  }, [selectedMonitor, fetchCurrentLocks, fetchSummary]);

  useEffect(() => {
    if (!autoRefresh || !selectedMonitor) return;
    const interval = setInterval(() => {
      fetchCurrentLocks();
    }, 5000);
    return () => clearInterval(interval);
  }, [autoRefresh, selectedMonitor, fetchCurrentLocks]);

  const handleConnect = async () => {
    if (!connectionTested || !selectedCompany) {
      setError('Please test connection and select a company first');
      return;
    }

    setLoading(true);
    setError(null);
    try {
      if (connectionForm.connectionType === 'foxpro') {
        // Connect to FoxPro (Opera 3) - use selected company's data path
        const companyDataPath = selectedCompany.data_path || connectionForm.dataPath;
        const params = new URLSearchParams({
          name: connectionForm.description,
          data_path: companyDataPath
        });
        const res = await fetch(`${API_BASE}/opera3-lock-monitor/connect?${params}`, { method: 'POST' });
        const data = await res.json();
        if (data.success) {
          setShowConnectForm(false);
          fetchMonitors();
          setSelectedMonitor({ name: connectionForm.description, type: 'opera3', is_monitoring: false, data_path: companyDataPath });
        } else {
          setError(data.error);
        }
      } else {
        // Connect to SQL Server (Opera SE)
        const params = new URLSearchParams({
          name: connectionForm.description,
          server: connectionForm.server,
          database: connectionForm.database
        });
        if (connectionForm.port && connectionForm.port !== '1433') {
          params.append('port', connectionForm.port);
        }
        if (!connectionForm.useWindowsAuth) {
          params.append('username', connectionForm.username);
          params.append('password', connectionForm.password);
        }
        const res = await fetch(`${API_BASE}/lock-monitor/connect?${params}`, { method: 'POST' });
        const data = await res.json();
        if (data.success) {
          setShowConnectForm(false);
          fetchMonitors();
          setSelectedMonitor({ name: connectionForm.description, type: 'sql-server', is_monitoring: false });
        } else {
          setError(data.error);
        }
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Connection failed');
    } finally {
      setLoading(false);
    }
  };

  const handleStartMonitoring = async () => {
    if (!selectedMonitor) return;
    setLoading(true);
    try {
      const apiBase = getApiBase();
      const res = await fetch(`${API_BASE}/${apiBase}/${selectedMonitor.name}/start?poll_interval=5`, {
        method: 'POST'
      });
      const data = await res.json();
      if (data.success) {
        fetchMonitors();
      } else {
        setError(data.error);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to start monitoring');
    } finally {
      setLoading(false);
    }
  };

  const handleStopMonitoring = async () => {
    if (!selectedMonitor) return;
    setLoading(true);
    try {
      const apiBase = getApiBase();
      const res = await fetch(`${API_BASE}/${apiBase}/${selectedMonitor.name}/stop`, { method: 'POST' });
      const data = await res.json();
      if (data.success) {
        fetchMonitors();
      } else {
        setError(data.error);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to stop monitoring');
    } finally {
      setLoading(false);
    }
  };

  const handleRemoveMonitor = async (monitor: Monitor) => {
    if (!confirm(`Remove monitor "${monitor.name}"?`)) return;
    try {
      const apiBase = getApiBase();
      const res = await fetch(`${API_BASE}/${apiBase}/${monitor.name}`, { method: 'DELETE' });
      const data = await res.json();
      if (data.success) {
        if (selectedMonitor?.name === monitor.name) {
          setSelectedMonitor(null);
          setSummary(null);
          setCurrentLocks([]);
        }
        fetchMonitors();
      }
    } catch (err) {
      console.error('Failed to remove monitor:', err);
    }
  };

  const isMonitoring = selectedMonitor?.is_monitoring || false;
  const isOpera3 = selectedMonitor?.type === 'opera3';

  const formatDuration = (ms: number | undefined) => {
    if (ms === undefined) return '-';
    if (ms < 1000) return `${ms}ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
    return `${(ms / 60000).toFixed(1)}m`;
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
            <Lock className="h-6 w-6" />
            Lock Monitor
          </h1>
          <p className="text-gray-600 mt-1">
            Monitor {isOpera3Mode ? 'file' : 'record'} locking conflicts
            <span className={`ml-2 px-2 py-0.5 rounded text-xs ${isOpera3Mode ? 'bg-green-100 text-green-700' : 'bg-blue-100 text-blue-700'}`}>
              {isOpera3Mode ? 'Opera 3' : 'Opera SE'}
            </span>
          </p>
        </div>
        <button
          onClick={openConnectionForm}
          className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg"
        >
          <Database className="h-4 w-4" />
          Add Connection
        </button>
      </div>

      {/* Connection Form */}
      {showConnectForm && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl p-6 w-full max-w-md">
            <div className="flex justify-between items-center mb-4">
              <h2 className="text-lg font-semibold flex items-center gap-2">
                {connectionForm.connectionType === 'sql' ? <Database className="h-5 w-5" /> : <FolderOpen className="h-5 w-5" />}
                Add Connection
              </h2>
              <button onClick={() => { setShowConnectForm(false); setError(null); }} className="text-gray-500 hover:text-gray-700">
                <X className="h-5 w-5" />
              </button>
            </div>

            <div className="space-y-4">
              {/* Description */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Description</label>
                <input
                  type="text"
                  value={connectionForm.description}
                  onChange={e => setConnectionForm({ ...connectionForm, description: e.target.value })}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md"
                  placeholder="e.g., Opera 3 Production, Opera SE Test"
                />
              </div>

              {/* Connection Type Dropdown */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Connection Type</label>
                <select
                  value={connectionForm.connectionType}
                  onChange={e => { setConnectionForm({ ...connectionForm, connectionType: e.target.value as ConnectionType }); resetConnectionTest(); }}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md bg-white"
                >
                  <option value="sql">SQL Server (Opera SE)</option>
                  <option value="foxpro">FoxPro (Opera 3)</option>
                </select>
              </div>

              {/* SQL Server Fields */}
              {connectionForm.connectionType === 'sql' && (
                <>
                  <div className="grid grid-cols-3 gap-3">
                    <div className="col-span-2">
                      <label className="block text-sm font-medium text-gray-700 mb-1">Server</label>
                      <input
                        type="text"
                        value={connectionForm.server}
                        onChange={e => { setConnectionForm({ ...connectionForm, server: e.target.value }); resetConnectionTest(); }}
                        className="w-full px-3 py-2 border border-gray-300 rounded-md"
                        placeholder="e.g., localhost\\SQLEXPRESS"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">Port</label>
                      <input
                        type="text"
                        value={connectionForm.port}
                        onChange={e => { setConnectionForm({ ...connectionForm, port: e.target.value }); resetConnectionTest(); }}
                        className="w-full px-3 py-2 border border-gray-300 rounded-md"
                        placeholder="1433"
                      />
                    </div>
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Database</label>
                    <input
                      type="text"
                      value={connectionForm.database}
                      onChange={e => { setConnectionForm({ ...connectionForm, database: e.target.value }); resetConnectionTest(); }}
                      className="w-full px-3 py-2 border border-gray-300 rounded-md"
                      placeholder="e.g., Opera3"
                    />
                  </div>
                  <div className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      id="windowsAuth"
                      checked={connectionForm.useWindowsAuth}
                      onChange={e => { setConnectionForm({ ...connectionForm, useWindowsAuth: e.target.checked }); resetConnectionTest(); }}
                      className="h-4 w-4 text-blue-600"
                    />
                    <label htmlFor="windowsAuth" className="text-sm text-gray-700">Use Windows Authentication</label>
                  </div>
                  {!connectionForm.useWindowsAuth && (
                    <>
                      <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">Username</label>
                        <input
                          type="text"
                          value={connectionForm.username}
                          onChange={e => { setConnectionForm({ ...connectionForm, username: e.target.value }); resetConnectionTest(); }}
                          className="w-full px-3 py-2 border border-gray-300 rounded-md"
                        />
                      </div>
                      <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">Password</label>
                        <input
                          type="password"
                          value={connectionForm.password}
                          onChange={e => { setConnectionForm({ ...connectionForm, password: e.target.value }); resetConnectionTest(); }}
                          className="w-full px-3 py-2 border border-gray-300 rounded-md"
                        />
                      </div>
                    </>
                  )}
                </>
              )}

              {/* FoxPro Fields */}
              {connectionForm.connectionType === 'foxpro' && (
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Base Path</label>
                  <input
                    type="text"
                    value={connectionForm.dataPath}
                    onChange={e => { setConnectionForm({ ...connectionForm, dataPath: e.target.value }); resetConnectionTest(); }}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md"
                    placeholder="e.g., C:\\Apps\\O3 Server VFP"
                  />
                  <p className="text-xs text-gray-500 mt-1">Path to the Opera 3 installation folder</p>
                </div>
              )}

              {/* Test Connection Button */}
              {!connectionTested && (
                <button
                  onClick={handleTestConnection}
                  disabled={testingConnection || (connectionForm.connectionType === 'sql' ? (!connectionForm.server || !connectionForm.database) : !connectionForm.dataPath)}
                  className="w-full px-4 py-2 bg-gray-600 text-white rounded-md hover:bg-gray-700 disabled:bg-gray-400 flex items-center justify-center gap-2"
                >
                  {testingConnection ? (
                    <>
                      <RefreshCw className="h-4 w-4 animate-spin" />
                      Testing Connection...
                    </>
                  ) : (
                    'Test Connection'
                  )}
                </button>
              )}

              {/* Company Selection (after successful test) */}
              {connectionTested && connectionForm.connectionType === 'foxpro' && availableCompanies.length > 0 && (
                <div className="p-3 bg-green-50 border border-green-200 rounded-md">
                  <label className="block text-sm font-medium text-green-800 mb-2">Select Company</label>
                  <select
                    value={selectedCompany?.code || ''}
                    onChange={e => {
                      const company = availableCompanies.find(c => c.code === e.target.value);
                      setSelectedCompany(company || null);
                    }}
                    className="w-full px-3 py-2 border border-green-300 rounded-md bg-white"
                  >
                    <option value="">-- Select a company --</option>
                    {availableCompanies.map(c => (
                      <option key={c.code} value={c.code}>{c.name} ({c.code})</option>
                    ))}
                  </select>
                </div>
              )}

              {/* SQL Company Info (after successful test) */}
              {connectionTested && connectionForm.connectionType === 'sql' && selectedCompany && (
                <div className="p-3 bg-green-50 border border-green-200 rounded-md">
                  <div className="flex items-center gap-2 text-green-800">
                    <Database className="h-4 w-4" />
                    <span className="font-medium">Connection Successful</span>
                  </div>
                  <p className="text-sm text-green-700 mt-1">
                    Company: <strong>{selectedCompany.name}</strong>
                  </p>
                </div>
              )}

              {error && (
                <div className="p-3 bg-red-50 border border-red-200 rounded-md text-red-700 text-sm max-h-32 overflow-y-auto">
                  <pre className="whitespace-pre-wrap break-words">{error}</pre>
                </div>
              )}
              <div className="flex justify-end gap-3 pt-4">
                <button onClick={() => { setShowConnectForm(false); setError(null); }} className="px-4 py-2 border border-gray-300 rounded-md hover:bg-gray-50">
                  Cancel
                </button>
                {connectionTested && selectedCompany && (
                  <button
                    onClick={handleConnect}
                    disabled={loading || !connectionForm.description}
                    className={`px-4 py-2 text-white rounded-md disabled:bg-gray-400 ${connectionForm.connectionType === 'sql' ? 'bg-blue-600 hover:bg-blue-700' : 'bg-green-600 hover:bg-green-700'}`}
                  >
                    {loading ? 'Adding...' : 'Add Monitor'}
                  </button>
                )}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Monitor Selection */}
      {monitors.length > 0 && (
        <div className="bg-white rounded-lg shadow p-4">
          <div className="flex items-center gap-4 flex-wrap">
            <span className="text-sm font-medium text-gray-700">Connections:</span>
            {monitors.map(m => (
              <div
                key={`${m.type}-${m.name}`}
                className={`flex items-center gap-2 px-3 py-1.5 rounded-lg cursor-pointer ${
                  selectedMonitor?.name === m.name && selectedMonitor?.type === m.type
                    ? m.type === 'sql-server' ? 'bg-blue-100 border-2 border-blue-500' : 'bg-green-100 border-2 border-green-500'
                    : 'bg-gray-100 border-2 border-transparent hover:bg-gray-200'
                }`}
                onClick={() => setSelectedMonitor(m)}
              >
                {m.type === 'sql-server' ? <Database className="h-4 w-4" /> : <FolderOpen className="h-4 w-4" />}
                <span className="font-medium">{m.name}</span>
                <span className={`text-xs px-1.5 py-0.5 rounded ${m.type === 'sql-server' ? 'bg-blue-200 text-blue-700' : 'bg-green-200 text-green-700'}`}>
                  {m.type === 'sql-server' ? 'SQL' : 'O3'}
                </span>
                {m.is_monitoring && (
                  <span className="flex items-center gap-1 text-xs text-green-600">
                    <Activity className="h-3 w-3 animate-pulse" />
                  </span>
                )}
                <button
                  onClick={e => { e.stopPropagation(); handleRemoveMonitor(m); }}
                  className="ml-1 text-gray-400 hover:text-red-500"
                >
                  <X className="h-4 w-4" />
                </button>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* No monitors */}
      {monitors.length === 0 && (
        <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-6 text-center">
          <AlertTriangle className="h-8 w-8 text-yellow-500 mx-auto mb-2" />
          <p className="text-yellow-800 font-medium">No connections configured</p>
          <p className="text-yellow-600 text-sm mt-1">Add a SQL Server (Opera SE) or Opera 3 (FoxPro) connection to monitor</p>
        </div>
      )}

      {/* Control Panel */}
      {selectedMonitor && monitors.length > 0 && (
        <div className="bg-white rounded-lg shadow p-4">
          <div className="flex items-center justify-between flex-wrap gap-4">
            <div className="flex items-center gap-4">
              <button
                onClick={isMonitoring ? handleStopMonitoring : handleStartMonitoring}
                disabled={loading}
                className={`flex items-center gap-2 px-4 py-2 rounded-lg font-medium ${
                  isMonitoring
                    ? 'bg-red-100 text-red-700 hover:bg-red-200'
                    : 'bg-green-100 text-green-700 hover:bg-green-200'
                }`}
              >
                {isMonitoring ? <><Square className="h-4 w-4" /> Stop</> : <><Play className="h-4 w-4" /> Start</>}
              </button>

              <button
                onClick={() => {
                  fetchCurrentLocks();
                  fetchSummary();
                }}
                disabled={loading}
                className="flex items-center gap-2 px-4 py-2 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200"
              >
                <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
                Refresh
              </button>

              <label className="flex items-center gap-2 text-sm">
                <input
                  type="checkbox"
                  checked={autoRefresh}
                  onChange={e => setAutoRefresh(e.target.checked)}
                  className="h-4 w-4 text-blue-600"
                />
                Auto-refresh (5s)
              </label>
            </div>

            <div className="flex items-center gap-2">
              <label className="text-sm text-gray-600">Period:</label>
              <select
                value={summaryHours}
                onChange={e => setSummaryHours(Number(e.target.value))}
                className="px-3 py-1.5 border border-gray-300 rounded-md text-sm"
              >
                <option value={1}>1 hour</option>
                <option value={6}>6 hours</option>
                <option value={24}>24 hours</option>
                <option value={72}>3 days</option>
                <option value={168}>1 week</option>
              </select>
            </div>
          </div>
        </div>
      )}

      {/* Current Locks */}
      {selectedMonitor && (
        <div className="bg-white rounded-lg shadow">
          <div className="p-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold flex items-center gap-2">
              <AlertTriangle className="h-5 w-5 text-orange-500" />
              {isOpera3 ? 'Current File Access' : 'Current Blocking'} ({currentLocks.length})
            </h2>
          </div>
          {currentLocks.length === 0 ? (
            <div className="p-6 text-center text-gray-500">
              <Lock className="h-8 w-8 mx-auto mb-2 text-gray-300" />
              {isOpera3 ? 'No file locks detected' : 'No blocking detected'}
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead className="bg-gray-50">
                  <tr>
                    {isOpera3 ? (
                      <>
                        <th className="text-left p-3">Table</th>
                        <th className="text-left p-3">File</th>
                        <th className="text-left p-3">Process</th>
                        <th className="text-left p-3">User</th>
                        <th className="text-left p-3">Lock Type</th>
                      </>
                    ) : (
                      <>
                        <th className="text-left p-3">Blocked User</th>
                        <th className="text-left p-3">Blocking User</th>
                        <th className="text-left p-3">Table</th>
                        <th className="text-left p-3">Lock Type</th>
                        <th className="text-right p-3">Wait Time</th>
                      </>
                    )}
                  </tr>
                </thead>
                <tbody>
                  {currentLocks.map((lock, idx) => (
                    <tr key={idx} className="border-t border-gray-100 hover:bg-gray-50">
                      {isOpera3 ? (
                        <>
                          <td className="p-3 font-mono text-xs">{lock.table_name}</td>
                          <td className="p-3 text-xs">{lock.file_name}</td>
                          <td className="p-3">
                            <span className="font-medium">{lock.process}</span>
                            {lock.process_id && <span className="text-gray-400 text-xs ml-1">(#{lock.process_id})</span>}
                          </td>
                          <td className="p-3">{lock.user || '-'}</td>
                          <td className="p-3">{lock.lock_type}</td>
                        </>
                      ) : (
                        <>
                          <td className="p-3">
                            <span className="font-medium">{lock.blocked_user}</span>
                            <span className="text-gray-400 text-xs ml-1">(#{lock.blocked_session})</span>
                          </td>
                          <td className="p-3">
                            <span className="font-medium text-red-600">{lock.blocking_user}</span>
                            <span className="text-gray-400 text-xs ml-1">(#{lock.blocking_session})</span>
                          </td>
                          <td className="p-3 font-mono text-xs">{lock.table_name}</td>
                          <td className="p-3">{lock.lock_type}</td>
                          <td className="p-3 text-right font-medium text-orange-600">{formatDuration(lock.wait_time_ms)}</td>
                        </>
                      )}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      {/* Summary Statistics */}
      {summary && (
        <>
          {/* Overview Cards */}
          <div className={`grid ${isOpera3 ? 'grid-cols-3' : 'grid-cols-4'} gap-4`}>
            <div className="bg-white rounded-lg shadow p-4">
              <div className="text-sm text-gray-500">Total Events</div>
              <div className="text-2xl font-bold">{summary.total_events.toLocaleString()}</div>
            </div>
            <div className="bg-white rounded-lg shadow p-4">
              <div className="text-sm text-gray-500">{isOpera3 ? 'Files Accessed' : 'Tables Affected'}</div>
              <div className="text-2xl font-bold">{isOpera3 ? summary.unique_files : summary.unique_tables}</div>
            </div>
            {isOpera3 ? (
              <div className="bg-white rounded-lg shadow p-4">
                <div className="text-sm text-gray-500">Processes</div>
                <div className="text-2xl font-bold">{summary.unique_processes}</div>
              </div>
            ) : (
              <>
                <div className="bg-white rounded-lg shadow p-4">
                  <div className="text-sm text-gray-500">Avg Wait Time</div>
                  <div className="text-2xl font-bold">{formatDuration(summary.avg_wait_time_ms)}</div>
                </div>
                <div className="bg-white rounded-lg shadow p-4">
                  <div className="text-sm text-gray-500">Max Wait Time</div>
                  <div className="text-2xl font-bold text-red-600">{formatDuration(summary.max_wait_time_ms)}</div>
                </div>
              </>
            )}
          </div>

          {/* Tables/Files and Users/Processes */}
          <div className="grid grid-cols-2 gap-6">
            <div className="bg-white rounded-lg shadow">
              <div className="p-4 border-b border-gray-200">
                <h3 className="font-semibold flex items-center gap-2">
                  <Table className="h-4 w-4" />
                  {isOpera3 ? 'Most Accessed Files' : 'Most Blocked Tables'}
                </h3>
              </div>
              {((isOpera3 ? summary.most_locked_files : summary.most_blocked_tables) || []).length === 0 ? (
                <div className="p-4 text-gray-500 text-center">No data</div>
              ) : (
                <div className="divide-y divide-gray-100">
                  {(isOpera3 ? summary.most_locked_files : summary.most_blocked_tables)?.map((t, idx) => (
                    <div key={idx} className="p-3 flex justify-between items-center">
                      <span className="font-mono text-sm">{t.table_name}</span>
                      <div className="text-right">
                        <span className="font-bold">{t.lock_count || t.block_count}</span>
                        <span className="text-gray-400 text-xs ml-1">{isOpera3 ? 'accesses' : 'blocks'}</span>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            <div className="bg-white rounded-lg shadow">
              <div className="p-4 border-b border-gray-200">
                <h3 className="font-semibold flex items-center gap-2">
                  <Users className="h-4 w-4" />
                  {isOpera3 ? 'Most Active Processes' : 'Top Blocking Users'}
                </h3>
              </div>
              {((isOpera3 ? summary.most_active_processes : summary.most_blocking_users) || []).length === 0 ? (
                <div className="p-4 text-gray-500 text-center">No data</div>
              ) : (
                <div className="divide-y divide-gray-100">
                  {(isOpera3 ? summary.most_active_processes : summary.most_blocking_users)?.map((u, idx) => (
                    <div key={idx} className="p-3 flex justify-between items-center">
                      <span className="font-medium">{u.user}</span>
                      <div className="text-right">
                        <span className={`font-bold ${isOpera3 ? '' : 'text-red-600'}`}>{u.access_count || u.block_count}</span>
                        <span className="text-gray-400 text-xs ml-1">{isOpera3 ? 'accesses' : 'blocks'}</span>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>

          {/* Hourly Distribution */}
          {summary.hourly_distribution.length > 0 && (
            <div className="bg-white rounded-lg shadow">
              <div className="p-4 border-b border-gray-200">
                <h3 className="font-semibold flex items-center gap-2">
                  <Clock className="h-4 w-4" />
                  Hourly Distribution
                </h3>
              </div>
              <div className="p-4">
                <div className="flex items-end gap-1 h-32">
                  {Array.from({ length: 24 }, (_, hour) => {
                    const stat = summary.hourly_distribution.find(h => h.hour === hour);
                    const maxCount = Math.max(...summary.hourly_distribution.map(h => h.event_count), 1);
                    const height = stat ? (stat.event_count / maxCount) * 100 : 0;
                    return (
                      <div key={hour} className="flex-1 flex flex-col items-center">
                        <div
                          className={`w-full rounded-t ${height > 0 ? (isOpera3 ? 'bg-green-500' : 'bg-blue-500') : 'bg-gray-100'}`}
                          style={{ height: `${Math.max(height, 2)}%` }}
                          title={`${hour}:00 - ${stat?.event_count || 0} events`}
                        />
                        {hour % 4 === 0 && <span className="text-xs text-gray-400 mt-1">{hour}</span>}
                      </div>
                    );
                  })}
                </div>
              </div>
            </div>
          )}

          {/* Recent Events */}
          <div className="bg-white rounded-lg shadow">
            <div className="p-4 border-b border-gray-200">
              <h3 className="font-semibold">Recent Events</h3>
            </div>
            {summary.recent_events.length === 0 ? (
              <div className="p-6 text-center text-gray-500">No events recorded</div>
            ) : (
              <div className="overflow-x-auto max-h-96 overflow-y-auto">
                <table className="w-full text-sm">
                  <thead className="bg-gray-50 sticky top-0">
                    <tr>
                      <th className="text-left p-3">Time</th>
                      {isOpera3 ? (
                        <>
                          <th className="text-left p-3">Table</th>
                          <th className="text-left p-3">Process</th>
                          <th className="text-left p-3">User</th>
                        </>
                      ) : (
                        <>
                          <th className="text-left p-3">Blocked</th>
                          <th className="text-left p-3">Blocking</th>
                          <th className="text-left p-3">Table</th>
                          <th className="text-right p-3">Wait</th>
                        </>
                      )}
                    </tr>
                  </thead>
                  <tbody>
                    {summary.recent_events.map((event, idx) => (
                      <tr key={idx} className="border-t border-gray-100 hover:bg-gray-50">
                        <td className="p-3 text-xs text-gray-500">
                          {event.timestamp ? new Date(event.timestamp).toLocaleString() : '-'}
                        </td>
                        {isOpera3 ? (
                          <>
                            <td className="p-3 font-mono text-xs">{event.table_name}</td>
                            <td className="p-3">{event.process}</td>
                            <td className="p-3">{event.user || '-'}</td>
                          </>
                        ) : (
                          <>
                            <td className="p-3">{event.blocked_user}</td>
                            <td className="p-3 text-red-600">{event.blocking_user}</td>
                            <td className="p-3 font-mono text-xs">{event.table_name}</td>
                            <td className="p-3 text-right">{formatDuration(event.wait_time_ms)}</td>
                          </>
                        )}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </>
      )}
    </div>
  );
}
