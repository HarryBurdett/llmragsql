import { useState, useEffect, useCallback } from 'react';
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

interface SQLConnectionForm {
  name: string;
  server: string;
  database: string;
  username: string;
  password: string;
  useWindowsAuth: boolean;
}

interface Opera3ConnectionForm {
  name: string;
  dataPath: string;
}

export function LockMonitor() {
  const [monitors, setMonitors] = useState<Monitor[]>([]);
  const [selectedMonitor, setSelectedMonitor] = useState<Monitor | null>(null);
  const [showAllSystems, setShowAllSystems] = useState(true);  // Default to showing all
  const [currentLocks, setCurrentLocks] = useState<LockEvent[]>([]);
  const [allSystemsLocks, setAllSystemsLocks] = useState<{source: string; type: MonitorType; locks: LockEvent[]}[]>([]);
  const [summary, setSummary] = useState<LockSummary | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [showConnectForm, setShowConnectForm] = useState<MonitorType | null>(null);
  const [summaryHours, setSummaryHours] = useState(24);
  const [autoRefresh, setAutoRefresh] = useState(false);

  const [sqlForm, setSqlForm] = useState<SQLConnectionForm>(() => ({
    name: localStorage.getItem('lockMonitor_sql_name') || '',
    server: localStorage.getItem('lockMonitor_sql_server') || '',
    database: localStorage.getItem('lockMonitor_sql_database') || '',
    username: localStorage.getItem('lockMonitor_sql_username') || '',
    password: '',
    useWindowsAuth: localStorage.getItem('lockMonitor_sql_useWindowsAuth') === 'true'
  }));

  const [opera3Form, setOpera3Form] = useState<Opera3ConnectionForm>(() => ({
    name: localStorage.getItem('lockMonitor_opera3_name') || '',
    dataPath: localStorage.getItem('lockMonitor_opera3_dataPath') || ''
  }));

  // Persist connection settings
  useEffect(() => {
    if (sqlForm.name) localStorage.setItem('lockMonitor_sql_name', sqlForm.name);
    if (sqlForm.server) localStorage.setItem('lockMonitor_sql_server', sqlForm.server);
    if (sqlForm.database) localStorage.setItem('lockMonitor_sql_database', sqlForm.database);
    if (sqlForm.username) localStorage.setItem('lockMonitor_sql_username', sqlForm.username);
    localStorage.setItem('lockMonitor_sql_useWindowsAuth', String(sqlForm.useWindowsAuth));
  }, [sqlForm]);

  useEffect(() => {
    if (opera3Form.name) localStorage.setItem('lockMonitor_opera3_name', opera3Form.name);
    if (opera3Form.dataPath) localStorage.setItem('lockMonitor_opera3_dataPath', opera3Form.dataPath);
  }, [opera3Form]);

  const fetchMonitors = useCallback(async () => {
    try {
      // Fetch both SQL Server and Opera 3 monitors
      const [sqlRes, opera3Res] = await Promise.all([
        fetch(`${API_BASE}/lock-monitor/list`),
        fetch(`${API_BASE}/opera3-lock-monitor/list`)
      ]);

      const sqlData = await sqlRes.json();
      const opera3Data = await opera3Res.json();

      const allMonitors: Monitor[] = [];

      if (sqlData.success) {
        sqlData.monitors.forEach((m: any) => {
          allMonitors.push({
            name: m.name,
            type: 'sql-server',
            is_monitoring: m.is_monitoring
          });
        });
      }

      if (opera3Data.success) {
        opera3Data.monitors.forEach((m: any) => {
          allMonitors.push({
            name: m.name,
            type: 'opera3',
            is_monitoring: m.is_monitoring,
            data_path: m.data_path
          });
        });
      }

      setMonitors(allMonitors);

      if (allMonitors.length > 0 && !selectedMonitor) {
        setSelectedMonitor(allMonitors[0]);
      }
    } catch (err) {
      console.error('Failed to fetch monitors:', err);
    }
  }, [selectedMonitor]);

  const getApiBase = (monitor: Monitor) => {
    return monitor.type === 'sql-server' ? 'lock-monitor' : 'opera3-lock-monitor';
  };

  const fetchCurrentLocks = useCallback(async () => {
    if (!selectedMonitor) return;
    try {
      const apiBase = getApiBase(selectedMonitor);
      const res = await fetch(`${API_BASE}/${apiBase}/${selectedMonitor.name}/current`);
      const data = await res.json();
      if (data.success) {
        setCurrentLocks(data.events);
      }
    } catch (err) {
      console.error('Failed to fetch current locks:', err);
    }
  }, [selectedMonitor]);

  // Fetch locks from ALL monitors (combined view)
  const fetchAllSystemsLocks = useCallback(async () => {
    if (monitors.length === 0) return;
    try {
      const results: {source: string; type: MonitorType; locks: LockEvent[]}[] = [];

      for (const monitor of monitors) {
        try {
          const apiBase = getApiBase(monitor);
          const res = await fetch(`${API_BASE}/${apiBase}/${monitor.name}/current`);
          const data = await res.json();
          if (data.success && data.events && data.events.length > 0) {
            results.push({
              source: monitor.name,
              type: monitor.type,
              locks: data.events.map((e: LockEvent) => ({ ...e, _source: monitor.name, _type: monitor.type }))
            });
          }
        } catch (err) {
          console.error(`Failed to fetch locks from ${monitor.name}:`, err);
        }
      }

      setAllSystemsLocks(results);
    } catch (err) {
      console.error('Failed to fetch all systems locks:', err);
    }
  }, [monitors]);

  const fetchSummary = useCallback(async () => {
    if (!selectedMonitor) return;
    setLoading(true);
    try {
      const apiBase = getApiBase(selectedMonitor);
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
  }, [selectedMonitor, summaryHours]);

  useEffect(() => {
    fetchMonitors();
  }, [fetchMonitors]);

  useEffect(() => {
    if (showAllSystems) {
      fetchAllSystemsLocks();
    } else if (selectedMonitor) {
      fetchCurrentLocks();
      fetchSummary();
    }
  }, [selectedMonitor, showAllSystems, fetchCurrentLocks, fetchSummary, fetchAllSystemsLocks]);

  useEffect(() => {
    if (!autoRefresh) return;
    const interval = setInterval(() => {
      if (showAllSystems) {
        fetchAllSystemsLocks();
      } else if (selectedMonitor) {
        fetchCurrentLocks();
      }
    }, 5000);
    return () => clearInterval(interval);
  }, [autoRefresh, selectedMonitor, showAllSystems, fetchCurrentLocks, fetchAllSystemsLocks]);

  const handleConnectSQL = async () => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams({
        name: sqlForm.name,
        server: sqlForm.server,
        database: sqlForm.database
      });
      if (!sqlForm.useWindowsAuth) {
        params.append('username', sqlForm.username);
        params.append('password', sqlForm.password);
      }

      const res = await fetch(`${API_BASE}/lock-monitor/connect?${params}`, { method: 'POST' });
      const data = await res.json();

      if (data.success) {
        setShowConnectForm(null);
        fetchMonitors();
        setSelectedMonitor({ name: sqlForm.name, type: 'sql-server', is_monitoring: false });
      } else {
        setError(data.error);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Connection failed');
    } finally {
      setLoading(false);
    }
  };

  const handleConnectOpera3 = async () => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams({
        name: opera3Form.name,
        data_path: opera3Form.dataPath
      });

      const res = await fetch(`${API_BASE}/opera3-lock-monitor/connect?${params}`, { method: 'POST' });
      const data = await res.json();

      if (data.success) {
        setShowConnectForm(null);
        fetchMonitors();
        setSelectedMonitor({ name: opera3Form.name, type: 'opera3', is_monitoring: false, data_path: opera3Form.dataPath });
      } else {
        setError(data.error);
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
      const apiBase = getApiBase(selectedMonitor);
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
      const apiBase = getApiBase(selectedMonitor);
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
      const apiBase = getApiBase(monitor);
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
          <p className="text-gray-600 mt-1">Monitor record/file locking conflicts</p>
        </div>
        <div className="flex gap-2">
          <button
            onClick={() => setShowConnectForm('sql-server')}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
          >
            <Database className="h-4 w-4" />
            Add SQL Server
          </button>
          <button
            onClick={() => setShowConnectForm('opera3')}
            className="flex items-center gap-2 px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700"
          >
            <FolderOpen className="h-4 w-4" />
            Add Opera 3
          </button>
        </div>
      </div>

      {/* SQL Server Connection Form */}
      {showConnectForm === 'sql-server' && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl p-6 w-full max-w-md">
            <div className="flex justify-between items-center mb-4">
              <h2 className="text-lg font-semibold flex items-center gap-2">
                <Database className="h-5 w-5" />
                Connect to SQL Server (Opera SE)
              </h2>
              <button onClick={() => { setShowConnectForm(null); setError(null); }} className="text-gray-500 hover:text-gray-700">
                <X className="h-5 w-5" />
              </button>
            </div>

            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Connection Name</label>
                <input
                  type="text"
                  value={sqlForm.name}
                  onChange={e => setSqlForm({ ...sqlForm, name: e.target.value })}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md"
                  placeholder="e.g., Production Opera"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Server</label>
                <input
                  type="text"
                  value={sqlForm.server}
                  onChange={e => setSqlForm({ ...sqlForm, server: e.target.value })}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md"
                  placeholder="e.g., localhost\\SQLEXPRESS"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Database</label>
                <input
                  type="text"
                  value={sqlForm.database}
                  onChange={e => setSqlForm({ ...sqlForm, database: e.target.value })}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md"
                  placeholder="e.g., Opera3"
                />
              </div>
              <div className="flex items-center gap-2">
                <input
                  type="checkbox"
                  id="windowsAuth"
                  checked={sqlForm.useWindowsAuth}
                  onChange={e => setSqlForm({ ...sqlForm, useWindowsAuth: e.target.checked })}
                  className="h-4 w-4 text-blue-600"
                />
                <label htmlFor="windowsAuth" className="text-sm text-gray-700">Use Windows Authentication</label>
              </div>
              {!sqlForm.useWindowsAuth && (
                <>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Username</label>
                    <input
                      type="text"
                      value={sqlForm.username}
                      onChange={e => setSqlForm({ ...sqlForm, username: e.target.value })}
                      className="w-full px-3 py-2 border border-gray-300 rounded-md"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Password</label>
                    <input
                      type="password"
                      value={sqlForm.password}
                      onChange={e => setSqlForm({ ...sqlForm, password: e.target.value })}
                      className="w-full px-3 py-2 border border-gray-300 rounded-md"
                    />
                  </div>
                </>
              )}
              {error && (
                <div className="p-3 bg-red-50 border border-red-200 rounded-md text-red-700 text-sm">{error}</div>
              )}
              <div className="flex justify-end gap-3 pt-4">
                <button onClick={() => { setShowConnectForm(null); setError(null); }} className="px-4 py-2 border border-gray-300 rounded-md hover:bg-gray-50">
                  Cancel
                </button>
                <button
                  onClick={handleConnectSQL}
                  disabled={loading || !sqlForm.name || !sqlForm.server || !sqlForm.database}
                  className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:bg-gray-400"
                >
                  {loading ? 'Connecting...' : 'Connect'}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Opera 3 Connection Form */}
      {showConnectForm === 'opera3' && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl p-6 w-full max-w-md">
            <div className="flex justify-between items-center mb-4">
              <h2 className="text-lg font-semibold flex items-center gap-2">
                <FolderOpen className="h-5 w-5" />
                Connect to Opera 3 (FoxPro)
              </h2>
              <button onClick={() => { setShowConnectForm(null); setError(null); }} className="text-gray-500 hover:text-gray-700">
                <X className="h-5 w-5" />
              </button>
            </div>

            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Connection Name</label>
                <input
                  type="text"
                  value={opera3Form.name}
                  onChange={e => setOpera3Form({ ...opera3Form, name: e.target.value })}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md"
                  placeholder="e.g., Opera 3 Company"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Data Path</label>
                <input
                  type="text"
                  value={opera3Form.dataPath}
                  onChange={e => setOpera3Form({ ...opera3Form, dataPath: e.target.value })}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md"
                  placeholder="e.g., C:\\Apps\\O3 Server VFP\\Company"
                />
                <p className="text-xs text-gray-500 mt-1">Path to the folder containing DBF files</p>
              </div>
              {error && (
                <div className="p-3 bg-red-50 border border-red-200 rounded-md text-red-700 text-sm">{error}</div>
              )}
              <div className="flex justify-end gap-3 pt-4">
                <button onClick={() => { setShowConnectForm(null); setError(null); }} className="px-4 py-2 border border-gray-300 rounded-md hover:bg-gray-50">
                  Cancel
                </button>
                <button
                  onClick={handleConnectOpera3}
                  disabled={loading || !opera3Form.name || !opera3Form.dataPath}
                  className="px-4 py-2 bg-green-600 text-white rounded-md hover:bg-green-700 disabled:bg-gray-400"
                >
                  {loading ? 'Connecting...' : 'Connect'}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Monitor Selection */}
      {monitors.length > 0 && (
        <div className="bg-white rounded-lg shadow p-4">
          <div className="flex items-center gap-4 flex-wrap">
            <span className="text-sm font-medium text-gray-700">View:</span>
            {/* All Systems Option */}
            <div
              className={`flex items-center gap-2 px-3 py-1.5 rounded-lg cursor-pointer ${
                showAllSystems
                  ? 'bg-purple-100 border-2 border-purple-500'
                  : 'bg-gray-100 border-2 border-transparent hover:bg-gray-200'
              }`}
              onClick={() => { setShowAllSystems(true); setSelectedMonitor(null); }}
            >
              <Activity className="h-4 w-4" />
              <span className="font-medium">All Systems</span>
              <span className="text-xs px-1.5 py-0.5 rounded bg-purple-200 text-purple-700">
                {monitors.length}
              </span>
            </div>
            {/* Individual Monitors */}
            {monitors.map(m => (
              <div
                key={`${m.type}-${m.name}`}
                className={`flex items-center gap-2 px-3 py-1.5 rounded-lg cursor-pointer ${
                  !showAllSystems && selectedMonitor?.name === m.name && selectedMonitor?.type === m.type
                    ? m.type === 'sql-server' ? 'bg-blue-100 border-2 border-blue-500' : 'bg-green-100 border-2 border-green-500'
                    : 'bg-gray-100 border-2 border-transparent hover:bg-gray-200'
                }`}
                onClick={() => { setShowAllSystems(false); setSelectedMonitor(m); }}
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
      {(selectedMonitor || showAllSystems) && monitors.length > 0 && (
        <div className="bg-white rounded-lg shadow p-4">
          <div className="flex items-center justify-between flex-wrap gap-4">
            <div className="flex items-center gap-4">
              {!showAllSystems && selectedMonitor && (
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
              )}

              <button
                onClick={() => {
                  if (showAllSystems) {
                    fetchAllSystemsLocks();
                  } else {
                    fetchCurrentLocks();
                    fetchSummary();
                  }
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

            {!showAllSystems && (
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
            )}
          </div>
        </div>
      )}

      {/* All Systems Combined View */}
      {showAllSystems && monitors.length > 0 && (
        <div className="bg-white rounded-lg shadow">
          <div className="p-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold flex items-center gap-2">
              <Activity className="h-5 w-5 text-purple-500" />
              Current Locks - All Systems
              <span className="text-sm font-normal text-gray-500">
                ({allSystemsLocks.reduce((sum, s) => sum + s.locks.length, 0)} total)
              </span>
            </h2>
          </div>
          {allSystemsLocks.length === 0 || allSystemsLocks.every(s => s.locks.length === 0) ? (
            <div className="p-6 text-center text-gray-500">
              <Lock className="h-8 w-8 mx-auto mb-2 text-gray-300" />
              No locks detected across any system
            </div>
          ) : (
            <div className="divide-y divide-gray-200">
              {allSystemsLocks.filter(s => s.locks.length > 0).map(system => (
                <div key={`${system.type}-${system.source}`} className="p-4">
                  <div className="flex items-center gap-2 mb-3">
                    {system.type === 'sql-server' ? (
                      <Database className="h-4 w-4 text-blue-600" />
                    ) : (
                      <FolderOpen className="h-4 w-4 text-green-600" />
                    )}
                    <span className="font-medium">{system.source}</span>
                    <span className={`text-xs px-1.5 py-0.5 rounded ${
                      system.type === 'sql-server' ? 'bg-blue-100 text-blue-700' : 'bg-green-100 text-green-700'
                    }`}>
                      {system.type === 'sql-server' ? 'Opera SE' : 'Opera 3'}
                    </span>
                    <span className="text-sm text-gray-500">({system.locks.length} locks)</span>
                  </div>
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead className="bg-gray-50">
                        <tr>
                          {system.type === 'opera3' ? (
                            <>
                              <th className="text-left p-2 text-xs">Table</th>
                              <th className="text-left p-2 text-xs">File</th>
                              <th className="text-left p-2 text-xs">Process</th>
                              <th className="text-left p-2 text-xs">Lock Type</th>
                            </>
                          ) : (
                            <>
                              <th className="text-left p-2 text-xs">Blocked User</th>
                              <th className="text-left p-2 text-xs">Blocking User</th>
                              <th className="text-left p-2 text-xs">Table</th>
                              <th className="text-left p-2 text-xs">Wait Time</th>
                            </>
                          )}
                        </tr>
                      </thead>
                      <tbody>
                        {system.locks.slice(0, 10).map((lock, i) => (
                          <tr key={i} className="border-t border-gray-100">
                            {system.type === 'opera3' ? (
                              <>
                                <td className="p-2">{lock.table_name}</td>
                                <td className="p-2 text-gray-600">{lock.file_name}</td>
                                <td className="p-2">{lock.process}</td>
                                <td className="p-2">
                                  <span className="px-2 py-0.5 bg-orange-100 text-orange-700 rounded text-xs">
                                    {lock.lock_type}
                                  </span>
                                </td>
                              </>
                            ) : (
                              <>
                                <td className="p-2">{lock.blocked_user}</td>
                                <td className="p-2 text-red-600">{lock.blocking_user}</td>
                                <td className="p-2">{lock.table_name}</td>
                                <td className="p-2">{formatDuration(lock.wait_time_ms)}</td>
                              </>
                            )}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                    {system.locks.length > 10 && (
                      <div className="p-2 text-center text-sm text-gray-500">
                        ... and {system.locks.length - 10} more
                      </div>
                    )}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Current Locks (Single Monitor View) */}
      {!showAllSystems && selectedMonitor && (
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
