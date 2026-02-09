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
  FolderOpen,
  Pencil,
  Server,
  Monitor,
  Cpu
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
  // Enhanced SQL Server details
  database_name?: string;
  schema_name?: string;
  index_name?: string;
  resource_type?: string;
  resource_description?: string;
  lock_mode?: string;
  blocking_lock_mode?: string;
  // Service/Application identification
  blocked_program?: string;
  blocking_program?: string;
  blocked_host?: string;
  blocking_host?: string;
  blocking_host_process_id?: number;
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

interface ProgramStat {
  program: string;
  block_count: number;
  total_wait_ms: number;
  avg_wait_ms: number;
  tables_blocked: string[];
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
  most_blocking_programs?: ProgramStat[];
  hourly_distribution: HourlyStat[];
  recent_events: LockEvent[];
}

interface DatabaseConnection {
  session_id: number;
  login_name: string;
  host_name: string;
  program_name: string;
  status: string;
  login_time: string;
  last_request_start_time: string;
  cpu_time: number;
  memory_usage: number;
  open_transaction_count: number;
  client_interface_name: string;
}

interface Monitor {
  name: string;
  type: MonitorType;
  is_monitoring: boolean;
  data_path?: string;
  connected?: boolean;
  needs_password?: boolean;
  server?: string;
  database?: string;
  username?: string;
  use_windows_auth?: boolean;
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

  // Service/connection tracking state
  const [connections, setConnections] = useState<DatabaseConnection[]>([]);
  const [showConnections, setShowConnections] = useState(false);
  const [connectionsLoading, setConnectionsLoading] = useState(false);

  // Pre-restore state
  const [showPreRestore, setShowPreRestore] = useState(false);
  const [preRestoreLoading, setPreRestoreLoading] = useState(false);
  const [preRestoreResult, setPreRestoreResult] = useState<{
    success: boolean;
    message: string;
    details?: any;
  } | null>(null);
  const [databaseMode, setDatabaseMode] = useState<'MULTI_USER' | 'SINGLE_USER' | 'UNKNOWN'>('UNKNOWN');

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
  const [editingMonitor, setEditingMonitor] = useState<Monitor | null>(null);

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

  // Reset form when opening for new connection
  const openConnectionForm = () => {
    setEditingMonitor(null);
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

  // Edit an existing connection
  const handleEditMonitor = (monitor: Monitor) => {
    setEditingMonitor(monitor);
    setConnectionForm({
      description: monitor.name,
      connectionType: monitor.type === 'opera3' ? 'foxpro' : 'sql',
      server: monitor.server || '',
      port: '1433',
      database: monitor.database || '',
      username: monitor.username || '',
      password: '',
      useWindowsAuth: monitor.use_windows_auth || false,
      dataPath: monitor.data_path || ''
    });
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
        // Test SQL Server connection - list available databases
        const params = new URLSearchParams({
          server: connectionForm.server,
          port: connectionForm.port || '1433'
        });
        if (!connectionForm.useWindowsAuth) {
          params.append('username', connectionForm.username);
          params.append('password', connectionForm.password);
        }
        const res = await fetch(`${API_BASE}/lock-monitor/test-connection?${params}`, { method: 'POST' });
        const data = await res.json();
        if (data.success && data.databases && data.databases.length > 0) {
          // Show database selection dropdown
          setAvailableCompanies(data.databases);
          setConnectionTested(true);
        } else {
          setError(data.error || 'No databases found');
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
            data_path: m.data_path,
            connected: m.connected !== false,
            needs_password: m.needs_password || false,
            server: m.server,
            database: m.database,
            username: m.username,
            use_windows_auth: m.use_windows_auth
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

  // Fetch current database connections (SQL Server only)
  const fetchConnections = useCallback(async () => {
    if (!selectedMonitor || selectedMonitor.type !== 'sql-server') return;
    setConnectionsLoading(true);
    try {
      const res = await fetch(`${API_BASE}/lock-monitor/${selectedMonitor.name}/connections`);
      const data = await res.json();
      if (data.success) {
        setConnections(data.connections || []);
      } else {
        console.error('Failed to fetch connections:', data.error);
      }
    } catch (err) {
      console.error('Failed to fetch connections:', err);
    } finally {
      setConnectionsLoading(false);
    }
  }, [selectedMonitor]);

  // Kill all database connections
  const killConnections = async () => {
    if (!selectedMonitor || selectedMonitor.type !== 'sql-server') return;
    setPreRestoreLoading(true);
    setPreRestoreResult(null);
    try {
      const res = await fetch(`${API_BASE}/lock-monitor/${selectedMonitor.name}/kill-connections`, {
        method: 'POST'
      });
      const data = await res.json();
      setPreRestoreResult({
        success: data.success,
        message: data.message || data.error,
        details: data
      });
      if (data.success) {
        fetchConnections();
      }
    } catch (err) {
      setPreRestoreResult({
        success: false,
        message: err instanceof Error ? err.message : 'Failed to kill connections'
      });
    } finally {
      setPreRestoreLoading(false);
    }
  };

  // Set database to single user mode
  const setSingleUserMode = async () => {
    if (!selectedMonitor || selectedMonitor.type !== 'sql-server') return;
    if (!confirm('This will disconnect ALL users and prevent new connections. Continue?')) return;
    setPreRestoreLoading(true);
    setPreRestoreResult(null);
    try {
      const res = await fetch(`${API_BASE}/lock-monitor/${selectedMonitor.name}/set-single-user`, {
        method: 'POST'
      });
      const data = await res.json();
      setPreRestoreResult({
        success: data.success,
        message: data.message || data.error,
        details: data
      });
      if (data.success) {
        setDatabaseMode('SINGLE_USER');
        fetchConnections();
      }
    } catch (err) {
      setPreRestoreResult({
        success: false,
        message: err instanceof Error ? err.message : 'Failed to set single user mode'
      });
    } finally {
      setPreRestoreLoading(false);
    }
  };

  // Set database back to multi user mode
  const setMultiUserMode = async () => {
    if (!selectedMonitor || selectedMonitor.type !== 'sql-server') return;
    setPreRestoreLoading(true);
    setPreRestoreResult(null);
    try {
      const res = await fetch(`${API_BASE}/lock-monitor/${selectedMonitor.name}/set-multi-user`, {
        method: 'POST'
      });
      const data = await res.json();
      setPreRestoreResult({
        success: data.success,
        message: data.message || data.error,
        details: data
      });
      if (data.success) {
        setDatabaseMode('MULTI_USER');
        fetchConnections();
      }
    } catch (err) {
      setPreRestoreResult({
        success: false,
        message: err instanceof Error ? err.message : 'Failed to set multi user mode'
      });
    } finally {
      setPreRestoreLoading(false);
    }
  };

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

  // Fetch connections when modal is opened
  useEffect(() => {
    if (showConnections && selectedMonitor?.type === 'sql-server') {
      fetchConnections();
    }
  }, [showConnections, selectedMonitor, fetchConnections]);

  const handleConnect = async () => {
    if (!connectionTested || !selectedCompany) {
      setError('Please test connection and select a company first');
      return;
    }

    setLoading(true);
    setError(null);
    try {
      // If editing and the name changed, delete the old monitor first
      if (editingMonitor && editingMonitor.name !== connectionForm.description) {
        const apiBase = editingMonitor.type === 'opera3' ? 'opera3-lock-monitor' : 'lock-monitor';
        await fetch(`${API_BASE}/${apiBase}/${editingMonitor.name}`, { method: 'DELETE' });
      }

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
          setEditingMonitor(null);
          fetchMonitors();
          setSelectedMonitor({ name: connectionForm.description, type: 'opera3', is_monitoring: false, data_path: companyDataPath });
        } else {
          setError(data.error);
        }
      } else {
        // Connect to SQL Server (Opera SE) - use selected database
        const params = new URLSearchParams({
          name: connectionForm.description,
          server: connectionForm.server,
          database: selectedCompany.code  // Use selected database
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
          setEditingMonitor(null);
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

  // Reconnect a saved monitor - use the edit function
  const handleReconnectMonitor = (monitor: Monitor) => {
    handleEditMonitor(monitor);
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
                {editingMonitor ? 'Edit Connection' : 'Add Connection'}
              </h2>
              <button onClick={() => { setShowConnectForm(false); setError(null); setEditingMonitor(null); }} className="text-gray-500 hover:text-gray-700">
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
                  disabled={testingConnection || (connectionForm.connectionType === 'sql' ? !connectionForm.server : !connectionForm.dataPath)}
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

              {/* Database/Company Selection (after successful test) */}
              {connectionTested && availableCompanies.length > 0 && (
                <div className="p-3 bg-green-50 border border-green-200 rounded-md">
                  <label className="block text-sm font-medium text-green-800 mb-2">
                    {connectionForm.connectionType === 'sql' ? 'Select Database' : 'Select Company'}
                  </label>
                  <select
                    value={selectedCompany?.code || ''}
                    onChange={e => {
                      const company = availableCompanies.find(c => c.code === e.target.value);
                      setSelectedCompany(company || null);
                    }}
                    className="w-full px-3 py-2 border border-green-300 rounded-md bg-white"
                  >
                    <option value="">-- Select {connectionForm.connectionType === 'sql' ? 'a database' : 'a company'} --</option>
                    {availableCompanies.map(c => (
                      <option key={c.code} value={c.code}>{c.name}</option>
                    ))}
                  </select>
                  <p className="text-xs text-green-600 mt-1">
                    {availableCompanies.length} {connectionForm.connectionType === 'sql' ? 'databases' : 'companies'} found
                  </p>
                </div>
              )}

              {error && (
                <div className="p-3 bg-red-50 border border-red-200 rounded-md text-red-700 text-sm max-h-32 overflow-y-auto">
                  <pre className="whitespace-pre-wrap break-words">{error}</pre>
                </div>
              )}
              <div className="flex justify-end gap-3 pt-4">
                <button onClick={() => { setShowConnectForm(false); setError(null); setEditingMonitor(null); }} className="px-4 py-2 border border-gray-300 rounded-md hover:bg-gray-50">
                  Cancel
                </button>
                {connectionTested && selectedCompany && (
                  <button
                    onClick={handleConnect}
                    disabled={loading || !connectionForm.description}
                    className={`px-4 py-2 text-white rounded-md disabled:bg-gray-400 ${connectionForm.connectionType === 'sql' ? 'bg-blue-600 hover:bg-blue-700' : 'bg-green-600 hover:bg-green-700'}`}
                  >
                    {loading ? (editingMonitor ? 'Updating...' : 'Adding...') : (editingMonitor ? 'Update Monitor' : 'Add Monitor')}
                  </button>
                )}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Connections Modal - Shows all active database connections */}
      {showConnections && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl w-full max-w-5xl max-h-[80vh] overflow-hidden">
            <div className="flex justify-between items-center p-4 border-b border-gray-200">
              <h2 className="text-lg font-semibold flex items-center gap-2">
                <Server className="h-5 w-5 text-purple-600" />
                Active Database Connections
                <span className="text-sm font-normal text-gray-500">
                  ({connections.length} connections)
                </span>
              </h2>
              <div className="flex items-center gap-2">
                <button
                  onClick={fetchConnections}
                  disabled={connectionsLoading}
                  className="flex items-center gap-1 px-3 py-1.5 text-sm bg-gray-100 hover:bg-gray-200 rounded-md"
                >
                  <RefreshCw className={`h-4 w-4 ${connectionsLoading ? 'animate-spin' : ''}`} />
                  Refresh
                </button>
                <button
                  onClick={() => setShowConnections(false)}
                  className="text-gray-500 hover:text-gray-700"
                >
                  <X className="h-5 w-5" />
                </button>
              </div>
            </div>

            <div className="p-4 bg-yellow-50 border-b border-yellow-200">
              <p className="text-sm text-yellow-800">
                <strong>Pre-Restore Check:</strong> Before running exclusive operations like database restores,
                review this list to identify services that need to be stopped. Each connection must be closed
                for exclusive access.
              </p>
            </div>

            {/* Connection summary by program */}
            {connections.length > 0 && (
              <div className="p-4 border-b border-gray-200 bg-gray-50">
                <h3 className="text-sm font-medium text-gray-700 mb-2">Connections by Service/Program:</h3>
                <div className="flex flex-wrap gap-2">
                  {Object.entries(
                    connections.reduce((acc, conn) => {
                      const prog = conn.program_name || 'Unknown';
                      acc[prog] = (acc[prog] || 0) + 1;
                      return acc;
                    }, {} as Record<string, number>)
                  )
                    .sort((a, b) => b[1] - a[1])
                    .map(([program, count]) => (
                      <span
                        key={program}
                        className="px-3 py-1 bg-white border border-gray-200 rounded-full text-sm"
                      >
                        <span className="font-medium">{program}</span>
                        <span className="ml-1 text-gray-500">({count})</span>
                      </span>
                    ))}
                </div>
              </div>
            )}

            <div className="overflow-auto max-h-[50vh]">
              {connectionsLoading ? (
                <div className="p-8 text-center text-gray-500">
                  <RefreshCw className="h-8 w-8 animate-spin mx-auto mb-2" />
                  Loading connections...
                </div>
              ) : connections.length === 0 ? (
                <div className="p-8 text-center text-gray-500">
                  <Server className="h-8 w-8 mx-auto mb-2 text-gray-300" />
                  No active connections found
                </div>
              ) : (
                <table className="w-full text-sm">
                  <thead className="bg-gray-50 sticky top-0">
                    <tr>
                      <th className="text-left p-3">Session</th>
                      <th className="text-left p-3">Program/Service</th>
                      <th className="text-left p-3">Host</th>
                      <th className="text-left p-3">Login</th>
                      <th className="text-left p-3">Status</th>
                      <th className="text-left p-3">Last Request</th>
                      <th className="text-right p-3">Open Trans</th>
                    </tr>
                  </thead>
                  <tbody>
                    {connections.map((conn) => (
                      <tr
                        key={conn.session_id}
                        className={`border-t border-gray-100 hover:bg-gray-50 ${
                          conn.open_transaction_count > 0 ? 'bg-orange-50' : ''
                        }`}
                      >
                        <td className="p-3 font-mono text-xs">{conn.session_id}</td>
                        <td className="p-3">
                          <div className="flex items-center gap-2">
                            <Cpu className="h-4 w-4 text-gray-400" />
                            <span className="font-medium">{conn.program_name || 'Unknown'}</span>
                          </div>
                          {conn.client_interface_name && (
                            <div className="text-xs text-gray-400">{conn.client_interface_name}</div>
                          )}
                        </td>
                        <td className="p-3">
                          <div className="flex items-center gap-1">
                            <Monitor className="h-3 w-3 text-gray-400" />
                            {conn.host_name || '-'}
                          </div>
                        </td>
                        <td className="p-3 text-gray-600">{conn.login_name}</td>
                        <td className="p-3">
                          <span
                            className={`px-2 py-0.5 rounded text-xs ${
                              conn.status === 'running'
                                ? 'bg-green-100 text-green-700'
                                : conn.status === 'sleeping'
                                ? 'bg-gray-100 text-gray-600'
                                : 'bg-yellow-100 text-yellow-700'
                            }`}
                          >
                            {conn.status}
                          </span>
                        </td>
                        <td className="p-3 text-xs text-gray-500">
                          {conn.last_request_start_time
                            ? new Date(conn.last_request_start_time).toLocaleString()
                            : '-'}
                        </td>
                        <td className="p-3 text-right">
                          {conn.open_transaction_count > 0 ? (
                            <span className="px-2 py-0.5 bg-orange-100 text-orange-700 rounded text-xs font-medium">
                              {conn.open_transaction_count}
                            </span>
                          ) : (
                            <span className="text-gray-400">0</span>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>

            <div className="p-4 border-t border-gray-200 bg-gray-50 text-xs text-gray-500">
              <strong>Tip:</strong> Programs with open transactions (highlighted in orange) should be
              closed first. Common services to check: Opera client apps, SSMS, SQL Agent jobs, IIS app pools.
            </div>
          </div>
        </div>
      )}

      {/* Pre-Restore Modal */}
      {showPreRestore && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl w-full max-w-3xl max-h-[80vh] overflow-hidden">
            <div className="flex justify-between items-center p-4 border-b border-red-200 bg-red-50">
              <h2 className="text-lg font-semibold flex items-center gap-2 text-red-800">
                <AlertTriangle className="h-5 w-5" />
                Pre-Restore Check
                <span className="text-sm font-normal text-red-600 ml-2">
                  {databaseMode !== 'UNKNOWN' && `(${databaseMode})`}
                </span>
              </h2>
              <button
                onClick={() => setShowPreRestore(false)}
                className="text-gray-500 hover:text-gray-700"
              >
                <X className="h-5 w-5" />
              </button>
            </div>

            <div className="p-4 bg-yellow-50 border-b border-yellow-200">
              <p className="text-sm text-yellow-800">
                <strong>Warning:</strong> These actions will disconnect users from the database.
                Use before running restore or other exclusive operations.
              </p>
            </div>

            {/* Current Connections Summary */}
            <div className="p-4 border-b border-gray-200">
              <h3 className="font-medium text-gray-700 mb-3 flex items-center gap-2">
                <Server className="h-4 w-4" />
                Active Connections ({connections.length})
              </h3>
              {connections.length > 0 ? (
                <div className="space-y-2 max-h-40 overflow-y-auto">
                  {Object.entries(
                    connections.reduce((acc, conn) => {
                      const prog = conn.program_name || 'Unknown';
                      if (!acc[prog]) acc[prog] = { count: 0, hosts: new Set(), sessions: [] };
                      acc[prog].count++;
                      acc[prog].hosts.add(conn.host_name || 'Unknown');
                      acc[prog].sessions.push(conn.session_id);
                      return acc;
                    }, {} as Record<string, { count: number; hosts: Set<string>; sessions: number[] }>)
                  ).map(([program, info]) => (
                    <div key={program} className="flex items-center justify-between p-2 bg-gray-50 rounded">
                      <div className="flex items-center gap-2">
                        <Cpu className="h-4 w-4 text-gray-400" />
                        <span className="font-medium">{program}</span>
                        <span className="text-xs text-gray-500">({Array.from(info.hosts).join(', ')})</span>
                      </div>
                      <span className="px-2 py-1 bg-red-100 text-red-700 rounded text-sm font-medium">
                        {info.count} connection{info.count !== 1 ? 's' : ''}
                      </span>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="text-center text-gray-500 py-4">
                  <Server className="h-8 w-8 mx-auto mb-2 text-gray-300" />
                  No active connections
                </div>
              )}
              <button
                onClick={fetchConnections}
                disabled={connectionsLoading}
                className="mt-3 flex items-center gap-1 px-3 py-1.5 text-sm bg-gray-100 hover:bg-gray-200 rounded-md"
              >
                <RefreshCw className={`h-4 w-4 ${connectionsLoading ? 'animate-spin' : ''}`} />
                Refresh
              </button>
            </div>

            {/* Action Buttons */}
            <div className="p-4 space-y-3">
              <div className="grid grid-cols-2 gap-3">
                <button
                  onClick={killConnections}
                  disabled={preRestoreLoading || connections.length === 0}
                  className="flex items-center justify-center gap-2 p-3 bg-orange-100 text-orange-700 rounded-lg hover:bg-orange-200 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  <X className="h-5 w-5" />
                  <div className="text-left">
                    <div className="font-medium">Kill Other Connections</div>
                    <div className="text-xs opacity-75">Keeps your session active</div>
                  </div>
                </button>

                <button
                  onClick={setSingleUserMode}
                  disabled={preRestoreLoading || databaseMode === 'SINGLE_USER'}
                  className="flex items-center justify-center gap-2 p-3 bg-red-100 text-red-700 rounded-lg hover:bg-red-200 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  <Lock className="h-5 w-5" />
                  <div className="text-left">
                    <div className="font-medium">Single User Mode</div>
                    <div className="text-xs opacity-75">Kill all & block new connections</div>
                  </div>
                </button>
              </div>

              <button
                onClick={setMultiUserMode}
                disabled={preRestoreLoading || databaseMode !== 'SINGLE_USER'}
                className="w-full flex items-center justify-center gap-2 p-3 bg-green-100 text-green-700 rounded-lg hover:bg-green-200 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                <Play className="h-5 w-5" />
                <div className="text-left">
                  <div className="font-medium">Restore Multi-User Mode</div>
                  <div className="text-xs opacity-75">Re-enable normal database access</div>
                </div>
              </button>
            </div>

            {/* Result Message */}
            {preRestoreResult && (
              <div className={`p-4 border-t ${preRestoreResult.success ? 'bg-green-50 border-green-200' : 'bg-red-50 border-red-200'}`}>
                <div className={`font-medium ${preRestoreResult.success ? 'text-green-800' : 'text-red-800'}`}>
                  {preRestoreResult.success ? '✓' : '✗'} {preRestoreResult.message}
                </div>
                {preRestoreResult.details?.killed_sessions && preRestoreResult.details.killed_sessions.length > 0 && (
                  <div className="mt-2 text-sm text-gray-600">
                    <strong>Sessions terminated:</strong>
                    <ul className="mt-1 space-y-1 max-h-32 overflow-y-auto">
                      {preRestoreResult.details.killed_sessions.map((s: any, i: number) => (
                        <li key={i} className="flex items-center gap-2">
                          <span className="text-red-500">✗</span>
                          <span>{s.program}</span>
                          <span className="text-gray-400">on {s.host}</span>
                          <span className="text-gray-400">(session {s.session_id})</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
            )}

            {preRestoreLoading && (
              <div className="p-4 border-t border-gray-200 flex items-center justify-center gap-2 text-gray-600">
                <RefreshCw className="h-5 w-5 animate-spin" />
                Processing...
              </div>
            )}

            <div className="p-4 border-t border-gray-200 bg-gray-50 text-xs text-gray-500">
              <strong>After restore:</strong> Click "Restore Multi-User Mode" to re-enable normal database access,
              then restart Opera SE Server service if needed.
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
                  !m.connected
                    ? 'bg-yellow-50 border-2 border-yellow-300'
                    : selectedMonitor?.name === m.name && selectedMonitor?.type === m.type
                    ? m.type === 'sql-server' ? 'bg-blue-100 border-2 border-blue-500' : 'bg-green-100 border-2 border-green-500'
                    : 'bg-gray-100 border-2 border-transparent hover:bg-gray-200'
                }`}
                onClick={() => m.connected ? setSelectedMonitor(m) : handleReconnectMonitor(m)}
                title={!m.connected ? 'Click to reconnect (password required)' : ''}
              >
                {m.type === 'sql-server' ? <Database className="h-4 w-4" /> : <FolderOpen className="h-4 w-4" />}
                <span className={`font-medium ${!m.connected ? 'text-yellow-700' : ''}`}>{m.name}</span>
                <span className={`text-xs px-1.5 py-0.5 rounded ${m.type === 'sql-server' ? 'bg-blue-200 text-blue-700' : 'bg-green-200 text-green-700'}`}>
                  {m.type === 'sql-server' ? 'SQL' : 'O3'}
                </span>
                {!m.connected && (
                  <span className="text-xs px-1.5 py-0.5 rounded bg-yellow-200 text-yellow-700">
                    Reconnect
                  </span>
                )}
                {m.connected && m.is_monitoring && (
                  <span className="flex items-center gap-1 text-xs text-green-600">
                    <Activity className="h-3 w-3 animate-pulse" />
                  </span>
                )}
                <button
                  onClick={e => { e.stopPropagation(); handleEditMonitor(m); }}
                  className="ml-1 text-gray-400 hover:text-blue-500"
                  title="Edit connection"
                >
                  <Pencil className="h-3.5 w-3.5" />
                </button>
                <button
                  onClick={e => { e.stopPropagation(); handleRemoveMonitor(m); }}
                  className="ml-1 text-gray-400 hover:text-red-500"
                  title="Remove connection"
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
      {selectedMonitor && selectedMonitor.connected !== false && monitors.length > 0 && (
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

              {/* View Connections button - SQL Server only */}
              {selectedMonitor?.type === 'sql-server' && (
                <>
                  <button
                    onClick={() => setShowConnections(true)}
                    className="flex items-center gap-2 px-4 py-2 bg-purple-100 text-purple-700 rounded-lg hover:bg-purple-200"
                  >
                    <Server className="h-4 w-4" />
                    View Connections
                  </button>
                  <button
                    onClick={() => { setShowPreRestore(true); setPreRestoreResult(null); fetchConnections(); }}
                    className="flex items-center gap-2 px-4 py-2 bg-red-100 text-red-700 rounded-lg hover:bg-red-200"
                  >
                    <AlertTriangle className="h-4 w-4" />
                    Pre-Restore
                  </button>
                </>
              )}
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
            <div className="flex justify-between items-center">
              <h2 className="text-lg font-semibold flex items-center gap-2">
                <AlertTriangle className="h-5 w-5 text-orange-500" />
                {isOpera3 ? 'Current File Access' : 'Current Blocking'} ({currentLocks.length})
              </h2>
              <span className="text-xs text-gray-500 bg-gray-100 px-2 py-1 rounded">
                Read-only monitoring - does not affect data entry
              </span>
            </div>
          </div>
          {currentLocks.length === 0 ? (
            <div className="p-6 text-center text-gray-500">
              <Lock className="h-8 w-8 mx-auto mb-2 text-gray-300" />
              {isOpera3 ? 'No file locks detected' : 'No blocking detected - all clear'}
            </div>
          ) : (
            <div className="divide-y divide-gray-200">
              {!isOpera3 && currentLocks.map((lock, idx) => (
                <div key={idx} className="p-4 bg-red-50 border-l-4 border-red-500">
                  <div className="flex justify-between items-start mb-3">
                    <div>
                      <span className="text-red-800 font-semibold text-lg">
                        {lock.table_name}
                      </span>
                      {lock.schema_name && (
                        <span className="text-red-600 text-sm ml-2">({lock.schema_name})</span>
                      )}
                      {lock.database_name && (
                        <span className="text-gray-500 text-xs ml-2">[{lock.database_name}]</span>
                      )}
                    </div>
                    <span className="bg-red-600 text-white px-3 py-1 rounded-full text-sm font-medium">
                      Waiting {formatDuration(lock.wait_time_ms)}
                    </span>
                  </div>

                  <div className="grid grid-cols-2 gap-4 mb-3">
                    <div className="bg-white p-3 rounded border border-red-200">
                      <div className="text-xs text-gray-500 mb-1">BLOCKED USER</div>
                      <div className="font-medium text-red-700">{lock.blocked_user}</div>
                      <div className="text-xs text-gray-400">Session #{lock.blocked_session}</div>
                      {lock.blocked_program && lock.blocked_program !== 'Unknown' && (
                        <div className="mt-1 text-xs text-gray-500 flex items-center gap-1">
                          <Cpu className="h-3 w-3" />
                          {lock.blocked_program}
                        </div>
                      )}
                      {lock.blocked_host && lock.blocked_host !== 'Unknown' && (
                        <div className="text-xs text-gray-400 flex items-center gap-1">
                          <Monitor className="h-3 w-3" />
                          {lock.blocked_host}
                        </div>
                      )}
                    </div>
                    <div className="bg-white p-3 rounded border border-orange-200">
                      <div className="text-xs text-gray-500 mb-1">BLOCKING USER</div>
                      <div className="font-medium text-orange-700">{lock.blocking_user}</div>
                      <div className="text-xs text-gray-400">Session #{lock.blocking_session}</div>
                      {lock.blocking_program && lock.blocking_program !== 'Unknown' && (
                        <div className="mt-1 inline-flex items-center gap-1 px-2 py-0.5 bg-purple-100 text-purple-700 rounded text-xs">
                          <Cpu className="h-3 w-3" />
                          {lock.blocking_program}
                        </div>
                      )}
                      {lock.blocking_host && lock.blocking_host !== 'Unknown' && (
                        <div className="text-xs text-gray-400 flex items-center gap-1 mt-1">
                          <Monitor className="h-3 w-3" />
                          {lock.blocking_host}
                          {lock.blocking_host_process_id ? ` (PID: ${lock.blocking_host_process_id})` : ''}
                        </div>
                      )}
                    </div>
                  </div>

                  <div className="grid grid-cols-3 gap-2 text-sm mb-3">
                    <div>
                      <span className="text-gray-500">Lock Type:</span>
                      <span className="ml-1 font-medium">{lock.resource_type || lock.lock_type}</span>
                    </div>
                    <div>
                      <span className="text-gray-500">Lock Mode:</span>
                      <span className="ml-1 font-medium">{lock.lock_mode}</span>
                      {lock.blocking_lock_mode && (
                        <span className="text-gray-400 ml-1">(blocked by {lock.blocking_lock_mode})</span>
                      )}
                    </div>
                    {lock.index_name && (
                      <div>
                        <span className="text-gray-500">Index:</span>
                        <span className="ml-1 font-medium">{lock.index_name}</span>
                      </div>
                    )}
                  </div>

                  {lock.resource_description && (
                    <div className="text-xs bg-gray-100 p-2 rounded mb-3">
                      <span className="text-gray-500">Resource:</span>
                      <code className="ml-1 text-gray-700">{lock.resource_description}</code>
                    </div>
                  )}

                  {(lock.blocked_query || lock.blocking_query) && (
                    <div className="grid grid-cols-2 gap-2 text-xs">
                      {lock.blocked_query && (
                        <div className="bg-white p-2 rounded border">
                          <div className="text-gray-500 mb-1">Blocked Query:</div>
                          <code className="text-gray-700 break-all">{lock.blocked_query.substring(0, 200)}...</code>
                        </div>
                      )}
                      {lock.blocking_query && (
                        <div className="bg-white p-2 rounded border">
                          <div className="text-gray-500 mb-1">Blocking Query:</div>
                          <code className="text-gray-700 break-all">{lock.blocking_query.substring(0, 200)}...</code>
                        </div>
                      )}
                    </div>
                  )}
                </div>
              ))}

              {/* Opera 3 FoxPro locks - simple table view */}
              {isOpera3 && (
                <table className="w-full text-sm">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="text-left p-3">Table</th>
                      <th className="text-left p-3">File</th>
                      <th className="text-left p-3">Process</th>
                      <th className="text-left p-3">User</th>
                      <th className="text-left p-3">Lock Type</th>
                    </tr>
                  </thead>
                  <tbody>
                    {currentLocks.map((lock, idx) => (
                      <tr key={idx} className="border-t border-gray-100 hover:bg-gray-50">
                        <td className="p-3 font-mono text-xs">{lock.table_name}</td>
                        <td className="p-3 text-xs">{lock.file_name}</td>
                        <td className="p-3">
                          <span className="font-medium">{lock.process}</span>
                          {lock.process_id && <span className="text-gray-400 text-xs ml-1">(#{lock.process_id})</span>}
                        </td>
                        <td className="p-3">{lock.user || '-'}</td>
                        <td className="p-3">{lock.lock_type}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
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

          {/* Most Blocking Programs - SQL Server only */}
          {!isOpera3 && summary.most_blocking_programs && summary.most_blocking_programs.length > 0 && (
            <div className="bg-white rounded-lg shadow border-2 border-purple-200">
              <div className="p-4 border-b border-purple-200 bg-purple-50">
                <h3 className="font-semibold flex items-center gap-2 text-purple-800">
                  <Server className="h-4 w-4" />
                  Services/Programs Causing Locks
                  <span className="text-xs font-normal text-purple-600 ml-2">
                    (Services to investigate)
                  </span>
                </h3>
              </div>
              <div className="divide-y divide-gray-100">
                {summary.most_blocking_programs.map((prog, idx) => (
                  <div key={idx} className="p-4">
                    <div className="flex justify-between items-start mb-2">
                      <div className="flex items-center gap-2">
                        <Cpu className="h-4 w-4 text-purple-500" />
                        <span className="font-semibold text-purple-800">{prog.program}</span>
                      </div>
                      <div className="text-right">
                        <span className="font-bold text-red-600 text-lg">{prog.block_count}</span>
                        <span className="text-gray-400 text-xs ml-1">blocks</span>
                      </div>
                    </div>
                    <div className="flex flex-wrap gap-4 text-sm text-gray-600">
                      <div>
                        <span className="text-gray-400">Total wait:</span>
                        <span className="ml-1 font-medium">{formatDuration(prog.total_wait_ms)}</span>
                      </div>
                      <div>
                        <span className="text-gray-400">Avg wait:</span>
                        <span className="ml-1 font-medium">{formatDuration(prog.avg_wait_ms)}</span>
                      </div>
                    </div>
                    {prog.tables_blocked && prog.tables_blocked.length > 0 && (
                      <div className="mt-2 flex flex-wrap gap-1">
                        <span className="text-xs text-gray-400">Tables blocked:</span>
                        {prog.tables_blocked.slice(0, 5).map((table, tidx) => (
                          <span key={tidx} className="text-xs px-2 py-0.5 bg-gray-100 rounded font-mono">
                            {table}
                          </span>
                        ))}
                        {prog.tables_blocked.length > 5 && (
                          <span className="text-xs text-gray-400">+{prog.tables_blocked.length - 5} more</span>
                        )}
                      </div>
                    )}
                  </div>
                ))}
              </div>
              <div className="p-3 bg-purple-50 border-t border-purple-200 text-xs text-purple-700">
                <strong>Action:</strong> Review and consider stopping or reconfiguring these services to reduce locking.
                Click "View Connections" to see active sessions for each program.
              </div>
            </div>
          )}

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
                          <th className="text-left p-3">Blocking Program</th>
                          <th className="text-left p-3">Host</th>
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
                            <td className="p-3">
                              {event.blocking_program && event.blocking_program !== 'Unknown' ? (
                                <span className="inline-flex items-center gap-1 px-2 py-0.5 bg-purple-100 text-purple-700 rounded text-xs">
                                  <Cpu className="h-3 w-3" />
                                  {event.blocking_program}
                                </span>
                              ) : (
                                <span className="text-gray-400 text-xs">-</span>
                              )}
                            </td>
                            <td className="p-3 text-xs text-gray-600">
                              {event.blocking_host && event.blocking_host !== 'Unknown'
                                ? event.blocking_host
                                : '-'}
                            </td>
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
